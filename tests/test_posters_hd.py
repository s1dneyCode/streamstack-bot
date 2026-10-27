"""
Unit tests for the slideshow's HD posters (RUN B5.7 · 0a): bot/posters_hd.py
makes an HD copy of the poster of every title that can reach a slideshow — in
a list (media_list_items) or chosen as a list's cover (media_lists.
cover_media_id) — from TMDb's original, fit inside 1280×1920 as a JPEG, hosts
it at media-images/posters-hd/{tmdb_id}/{stem}.jpg and writes its URL to
media.poster_hd_url by the row's id. poster_url and poster_path are never
written.

Fakes only: a Supabase client whose three tables apply the filters, keyset
bounds and writes the script sends (so a write by tmdb_id would visibly touch
two rows), whose storage records every upload behind the REAL
SupabaseClient.upload_image / get_public_url (the public URL ends in a bare
`?`, as the real one can); TMDb's image CDN answered in memory; images built
in memory with Pillow; a clock the test hands in. No network, no database.
Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import requests
from PIL import Image, features
from postgrest.exceptions import APIError

from bot import posters_hd as mod
from bot.supabase_client import SupabaseClient

START = datetime(2026, 10, 27, 20, 41, tzinfo=timezone.utc)
CDN = "https://image.tmdb.org/t/p/original/"
PUBLIC = "https://project.supabase.invalid/storage/v1/object/public/"
MISSING_COLUMN = APIError({"code": "42703", "message": "column media.poster_hd_url does not exist",
                           "hint": None, "details": None})
FAKE_ENV = {"SUPABASE_URL": "https://project.supabase.invalid", "SUPABASE_KEY": "fake-supabase-key"}
SUMMARY_FIELDS = ("candidates", "pending", "made", "resized", "fit_as_is", "converted", "missing", "errors",
                  "no_path", "gone", "written", "budget_hit", "bytes_uploaded", "avg_kb")


# ── images, in memory ─────────────────────────────────────────────────────

def _image(size, fmt="JPEG", mode="RGB", color=(180, 40, 60)) -> bytes:
    if mode == "RGBA" and len(color) == 3:
        color = (*color, 128)
    buf = io.BytesIO()
    Image.new(mode, size, color).save(buf, format=fmt)
    return buf.getvalue()


def _decoded(data: bytes):
    """(format, size, mode, info) of *data*."""
    with Image.open(io.BytesIO(data)) as img:
        return img.format, img.size, img.mode, dict(img.info)


BIG = _image((2000, 3000))       # TMDb's usual original: 2:3, larger than the box
FITS = _image((1000, 1500))      # already inside 1280×1920
SQUARE = _image((3000, 3000))
SMALL = _image((500, 750), color=(10, 200, 90))


# ── the fakes ─────────────────────────────────────────────────────────────

class _Query:
    """One PostgREST chain on a fake table. Applies what it is told."""

    def __init__(self, db, table):
        self.db, self.table_name, self.ops, self._negate = db, table, [], False

    def _op(self, name, *args, **kwargs):
        if self._negate:
            name, self._negate = f"not_{name}", False
        self.ops.append((name, args, kwargs))
        return self

    @property
    def not_(self):
        self._negate = True
        return self

    def select(self, *a, **kw): return self._op("select", *a, **kw)
    def order(self, *a, **kw): return self._op("order", *a, **kw)
    def limit(self, *a, **kw): return self._op("limit", *a, **kw)
    def gt(self, *a, **kw): return self._op("gt", *a, **kw)
    def in_(self, *a, **kw): return self._op("in_", *a, **kw)
    def is_(self, *a, **kw): return self._op("is_", *a, **kw)
    def update(self, *a, **kw): return self._op("update", *a, **kw)
    def eq(self, *a, **kw): return self._op("eq", *a, **kw)

    def named(self, name):
        return [o for o in self.ops if o[0] == name]

    def execute(self):
        self.db.executed.append(self)
        rows = self.db.tables[self.table_name]
        update = self.named("update")
        if update:
            fields = update[0][1][0]
            (col, value), = [o[1] for o in self.named("eq")]
            if value in self.db.fail_writes:
                raise APIError({"code": "57014", "message": "canceling statement", "hint": None, "details": None})
            hits = [r for r in rows if r[col] == value]
            for r in hits:
                r.update(fields)
            return types.SimpleNamespace(data=hits)
        cols = [c.strip() for c in self.named("select")[0][1][0].split(",")]
        if self.table_name in self.db.read_errors:
            raise self.db.read_errors[self.table_name]
        if self.table_name == "media" and self.db.missing_column and "poster_hd_url" in cols:
            raise MISSING_COLUMN
        found = list(rows)
        for _, (col, _null), _ in self.named("is_"):
            found = [r for r in found if r.get(col) is None]
        for _, (col, _null), _ in self.named("not_is_"):
            found = [r for r in found if r.get(col) is not None]
        for _, (col, value), _ in self.named("gt"):
            found = [r for r in found if r[col] > value]
        for _, (col, values), _ in self.named("in_"):
            found = [r for r in found if r[col] in values]
        for _, (col,), kw in self.named("order"):
            found.sort(key=lambda r: r[col], reverse=bool(kw.get("desc")))
        for _, (n,), _ in self.named("limit"):
            found = found[:n]
        return types.SimpleNamespace(data=[{c: r[c] for c in cols} for r in found])


class _Bucket:
    def __init__(self, db, name):
        self.db, self.name = db, name

    def upload(self, path, file, file_options=None):
        if self.db.fail_uploads:
            raise RuntimeError("storage refused the upload")
        self.db.uploads.append({"bucket": self.name, "path": path, "bytes": file, "options": dict(file_options or {})})

    def get_public_url(self, path):
        return f"{PUBLIC}{self.name}/{path}?"   # the bare `?` the real client can return


class _Db:
    """The house SupabaseClient as the script uses it: .client.table(...)
    chains, and upload_image / get_public_url — the REAL methods, over a
    fake storage."""

    def __init__(self, items=(), lists=(), media=(), missing_column=False, read_errors=None,
                 fail_writes=(), fail_uploads=False):
        self.tables = {"media_list_items": [dict(r) for r in items], "media_lists": [dict(r) for r in lists],
                       "media": [dict(r) for r in media]}
        self.missing_column, self.read_errors = missing_column, dict(read_errors or {})
        self.fail_writes, self.fail_uploads = set(fail_writes), fail_uploads
        self.executed, self.uploads = [], []
        self.client = self
        self.storage = types.SimpleNamespace(from_=lambda name: _Bucket(self, name))

    def table(self, name):
        assert name in self.tables, name
        return _Query(self, name)

    def upload_image(self, bucket, path, image_bytes, content_type="image/jpeg"):
        return SupabaseClient.upload_image(self, bucket, path, image_bytes, content_type)

    def get_public_url(self, bucket, path):
        return SupabaseClient.get_public_url(self, bucket, path)

    def reads(self, table):
        return [q for q in self.executed if q.table_name == table and q.named("select")]

    @property
    def writes(self):
        return [(q.named("update")[0][1][0], q.named("eq")[0][1]) for q in self.executed if q.named("update")]

    def media(self, media_id):
        return next(r for r in self.tables["media"] if r["id"] == media_id)


class _Cdn:
    """Stands in for requests.get: answers by URL, records (url, kwargs)."""

    def __init__(self, routes=None, default=(404, b"")):
        self.routes, self.default, self.calls = dict(routes or {}), default, []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        answer = self.routes.get(url, self.default)
        if isinstance(answer, BaseException):
            raise answer
        status, content = answer
        return types.SimpleNamespace(status_code=status, content=content)

    @property
    def urls(self):
        return [u for u, _ in self.calls]


class _Clock:
    """START plus `step` seconds per call."""

    def __init__(self, step=0.0):
        self.calls, self.step = 0, step

    def __call__(self):
        self.calls += 1
        return START + timedelta(seconds=self.step * (self.calls - 1))


def _media(i, tmdb_id=None, media_type="movie", poster_path=None, **fields):
    tmdb_id = tmdb_id if tmdb_id is not None else 1000 + i
    return {"id": f"m-{i:05d}", "tmdb_id": tmdb_id, "media_type": media_type,
            "poster_path": poster_path if poster_path is not None else f"https://image.tmdb.org/t/p/w500/p{i}.jpg",
            "poster_url": f"{PUBLIC}media-images/posters/{tmdb_id}.jpg?", "poster_hd_url": None,
            "tmdb_missing_at": None, **fields}


def _item(n, media_id, list_id="l-1"):
    return {"id": n, "list_id": list_id, "media_id": media_id, "owner_id": f"owner-{list_id}"}


def _list(list_id, cover=None):
    return {"id": list_id, "owner_id": f"owner-{list_id}", "cover_media_id": cover}


def _in_a_list(*media_rows, cdn_routes=None, **db_kwargs):
    """Each row in list l-1, and a CDN answering BIG for every row's original."""
    db = _Db(items=[_item(n, r["id"]) for n, r in enumerate(media_rows, start=1)], lists=[_list("l-1")],
             media=media_rows, **db_kwargs)
    routes = {CDN + r["poster_path"].rsplit("/", 1)[-1]: (200, BIG) for r in media_rows if r.get("poster_path")}
    routes.update(cdn_routes or {})
    return db, _Cdn(routes)


def _run(db, cdn, batch=300, budget_min=20, clock=None):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            stats = mod.run(db, fetch=cdn, clock=clock or _Clock(), batch=batch, budget_min=budget_min)
        except BaseException as exc:  # SystemExit included: the fail-fast is an exit
            stats = exc
    return stats, out.getvalue()


def _summary(out):
    lines = [l for l in out.splitlines() if l.startswith("[POSTER_HD] SUMMARY ")]
    return lines[-1] if lines else ""


def _tokens(out):
    return dict(t.split("=", 1) for t in _summary(out).split()[2:] if "=" in t)


# ── (1) the set: list items and covers, each title once ──────────────────

class SetTest(unittest.TestCase):

    def test_list_items_and_covers_together_each_title_once(self):
        one, two, three = _media(1), _media(2), _media(3)
        db = _Db(items=[_item(1, one["id"], "l-1"), _item(2, one["id"], "l-2"), _item(3, three["id"], "l-2")],
                 lists=[_list("l-1", cover=two["id"]), _list("l-2"), _list("l-3", cover=one["id"])],
                 media=[one, two, three])
        cdn = _Cdn({CDN + f"p{i}.jpg": (200, FITS) for i in (1, 2, 3)})
        stats, out = _run(db, cdn)

        self.assertEqual(sorted(cdn.urls), [CDN + "p1.jpg", CDN + "p2.jpg", CDN + "p3.jpg"])   # once each
        self.assertEqual(sorted(u["path"] for u in db.uploads),
                         ["posters-hd/1001/p1.jpg", "posters-hd/1002/p2.jpg", "posters-hd/1003/p3.jpg"])
        self.assertEqual(_tokens(out).get("candidates"), "3")
        self.assertEqual(_tokens(out).get("made"), "3")

    def test_a_title_gone_from_tmdb_is_left_out_and_counted(self):
        gone = _media(1, tmdb_missing_at="2026-10-20T08:00:00+00:00")
        db, cdn = _in_a_list(gone)
        _, out = _run(db, cdn)
        self.assertEqual(cdn.urls, [])
        self.assertEqual(db.uploads, [])
        self.assertEqual(_tokens(out).get("gone"), "1")
        # Positive control: the same row without the stamp is made.
        db, cdn = _in_a_list(_media(1))
        _, out = _run(db, cdn)
        self.assertEqual(cdn.urls, [CDN + "p1.jpg"])
        self.assertEqual(_tokens(out).get("gone"), "0")

    def test_the_set_is_read_in_keyset_pages_by_id_never_offsets(self):
        rows = [_media(i) for i in range(1, 6)]
        db = _Db(items=[_item(n, r["id"]) for n, r in enumerate(rows, start=1)],
                 lists=[_list(f"l-{n}", cover=rows[n - 1]["id"]) for n in range(1, 4)], media=rows)
        with mock.patch.object(mod, "PAGE_SIZE", 2):
            _run(db, _Cdn(default=(200, FITS)))

        for table, expected in (("media_list_items", [None, 2, 4]), ("media_lists", [None, "l-2"])):
            with self.subTest(table=table):
                reads = db.reads(table)
                bounds = [q.named("gt")[0][1][1] if q.named("gt") else None for q in reads]
                self.assertEqual(bounds, expected)
                for q in reads:
                    self.assertEqual(q.named("order")[0][1], ("id",))
                    self.assertEqual(q.named("order")[0][2], {})          # ascending: a desc keyset stops after a page
                    self.assertEqual(q.named("limit")[0][1], (2,))
        # The covers are asked for non-null cover_media_id only.
        self.assertTrue(all(q.named("not_is_") == [("not_is_", ("cover_media_id", "null"), {})]
                            for q in db.reads("media_lists")))

    def test_the_media_rows_are_read_by_id_in_chunks(self):
        rows = [_media(i) for i in range(1, 6)]
        db = _Db(items=[_item(n, r["id"]) for n, r in enumerate(rows, start=1)], lists=[], media=rows)
        with mock.patch.object(mod, "ID_CHUNK", 2):
            _run(db, _Cdn(default=(200, FITS)))

        chunks = [q.named("in_")[0][1] for q in db.reads("media")]
        self.assertEqual([c[0] for c in chunks], ["id", "id", "id"])
        self.assertEqual(sorted(i for c in chunks for i in c[1]), [r["id"] for r in rows])
        self.assertTrue(all(len(c[1]) <= 2 for c in chunks))


# ── (2) pending ───────────────────────────────────────────────────────────

class PendingTest(unittest.TestCase):

    def test_a_null_hd_url_is_pending(self):
        self.assertIs(mod.is_pending(_media(1)), True)

    def test_the_same_stem_is_not_pending_and_another_stem_is(self):
        row = _media(1, poster_path="https://image.tmdb.org/t/p/w500/abc.jpg",
                     poster_hd_url=f"{PUBLIC}media-images/posters-hd/1001/abc.jpg")
        self.assertIs(mod.is_pending(row), False)
        # Positive control: TMDb changed the poster since.
        row["poster_hd_url"] = f"{PUBLIC}media-images/posters-hd/1001/old.jpg"
        self.assertIs(mod.is_pending(row), True)

    def test_both_poster_path_shapes_give_the_same_stem(self):
        self.assertEqual(mod.file_stem("https://image.tmdb.org/t/p/w500/abc.jpg"), "abc")
        self.assertEqual(mod.file_stem("/abc.jpg"), "abc")
        for path in ("https://image.tmdb.org/t/p/w500/abc.jpg", "/abc.jpg"):
            with self.subTest(path=path):
                row = _media(1, poster_path=path, poster_hd_url=f"{PUBLIC}media-images/posters-hd/1001/abc.jpg")
                self.assertIs(mod.is_pending(row), False)

    def test_a_trailing_question_mark_does_not_make_it_pending(self):
        row = _media(1, poster_path="/abc.jpg", poster_hd_url=f"{PUBLIC}media-images/posters-hd/1001/abc.jpg?")
        self.assertIs(mod.is_pending(row), False)
        self.assertEqual(mod.file_stem(f"{PUBLIC}media-images/posters-hd/1001/abc.jpg?"), "abc")
        # Positive control: a different stem behind the `?` still is.
        row["poster_hd_url"] = f"{PUBLIC}media-images/posters-hd/1001/xyz.jpg?"
        self.assertIs(mod.is_pending(row), True)

    def test_a_title_already_made_is_not_fetched_again(self):
        db, cdn = _in_a_list(_media(1), _media(2))
        first, out = _run(db, cdn)
        self.assertEqual(_tokens(out).get("made"), "2")
        cdn.calls.clear()
        _, out = _run(db, cdn)          # the URLs the first run stored, `?` and all
        self.assertEqual(cdn.urls, [])
        self.assertEqual(_tokens(out).get("pending"), "0")
        # Positive control: TMDb changes one poster — only that one is made again.
        db.media("m-00002")["poster_path"] = "https://image.tmdb.org/t/p/w500/new2.jpg"
        cdn.routes[CDN + "new2.jpg"] = (200, FITS)
        _, out = _run(db, cdn)
        self.assertEqual(cdn.urls, [CDN + "new2.jpg"])
        self.assertEqual(_tokens(out).get("pending"), "1")

    def test_a_title_without_poster_path_is_skipped_and_counted(self):
        db, cdn = _in_a_list(_media(1, poster_path=""), _media(2))
        _, out = _run(db, cdn)
        self.assertEqual(cdn.urls, [CDN + "p2.jpg"])
        self.assertEqual(_tokens(out).get("no_path"), "1")

    def test_a_null_poster_path_is_skipped_and_counted_too(self):
        # The shape a title without a poster has in the database.
        empty = _media(1)
        empty["poster_path"] = None
        db, cdn = _in_a_list(empty, _media(2))
        stats, out = _run(db, cdn)
        self.assertIsInstance(stats, dict)
        self.assertEqual(cdn.urls, [CDN + "p2.jpg"])          # positive control: the other title is made
        self.assertEqual(_tokens(out).get("no_path"), "1")


# ── (3) the source ────────────────────────────────────────────────────────

class SourceTest(unittest.TestCase):

    def test_the_original_from_both_poster_path_shapes(self):
        for path in ("https://image.tmdb.org/t/p/w500/abc.jpg", "/abc.jpg"):
            with self.subTest(path=path):
                url = mod.source_url(path)
                self.assertEqual(url, "https://image.tmdb.org/t/p/original/abc.jpg")
                self.assertNotIn("w500", url)   # positive control above: the file is there

    def test_the_source_is_never_read_from_poster_url(self):
        row = _media(1, poster_path="/abc.jpg")
        db, cdn = _in_a_list(row)
        _run(db, cdn)
        self.assertEqual(cdn.urls, [CDN + "abc.jpg"])
        self.assertFalse([u for u in cdn.urls if "supabase" in u or "1001.jpg" in u])
        self.assertFalse([u for u in cdn.urls if "/w500/" in u])
        self.assertEqual(cdn.calls[0][1].get("timeout"), mod.FETCH_TIMEOUT_SECONDS)


# ── (4) the image ─────────────────────────────────────────────────────────

class ImageTest(unittest.TestCase):

    def _made(self, source: bytes, path="/abc.jpg"):
        db, cdn = _in_a_list(_media(1, poster_path=path), cdn_routes={CDN + path.rsplit("/", 1)[-1]: (200, source)})
        stats, out = _run(db, cdn)
        self.assertEqual(len(db.uploads), 1, out)
        return db.uploads[0]["bytes"], _tokens(out)

    def test_a_2000x3000_original_is_fit_to_1280x1920(self):
        data, tokens = self._made(BIG)
        fmt, size, mode, info = _decoded(data)
        self.assertEqual((fmt, size, mode), ("JPEG", (1280, 1920), "RGB"))
        self.assertTrue(info.get("progressive") or info.get("progression"))
        self.assertEqual(tokens.get("resized"), "1")
        self.assertNotEqual(data, BIG)      # positive control for the next test: re-encoded

    def test_a_jpeg_that_fits_is_uploaded_untouched(self):
        data, tokens = self._made(FITS)
        self.assertEqual(data, FITS)        # the downloaded bytes, never re-encoded
        self.assertEqual(tokens.get("fit_as_is"), "1")
        self.assertEqual(_decoded(data)[1], (1000, 1500))   # never upscaled

    def test_a_square_fits_inside_the_box(self):
        data, tokens = self._made(SQUARE)
        self.assertEqual(_decoded(data)[1], (1280, 1280))
        self.assertEqual(tokens.get("resized"), "1")

    def test_nothing_is_ever_larger_than_its_source(self):
        for size in ((2000, 3000), (1000, 1500), (3000, 3000), (1280, 1920), (1281, 1920), (1280, 1921),
                     (600, 900), (4000, 1000), (500, 4000)):
            with self.subTest(size=size):
                source = _image(size)
                data, kind = mod.hd_jpeg(source)
                self.assertIn(kind, (mod.RESIZED, mod.FIT_AS_IS, mod.CONVERTED))
                w, h = _decoded(data)[1]
                self.assertLessEqual(w, size[0])
                self.assertLessEqual(h, size[1])
                self.assertLessEqual(w, 1280)
                self.assertLessEqual(h, 1920)
                self.assertTrue(w == size[0] or h == 1920 or w == 1280, (w, h))   # fit, not shrunk further

    def test_a_jpeg_exactly_the_box_is_uploaded_untouched(self):
        exact = _image((1280, 1920))
        data, kind = mod.hd_jpeg(exact)
        self.assertEqual((kind, data == exact), (mod.FIT_AS_IS, True))
        # Positive control: one pixel over, on either side, is fit.
        for size in ((1281, 1920), (1280, 1921)):
            with self.subTest(size=size):
                data, kind = mod.hd_jpeg(_image(size))
                self.assertEqual(kind, mod.RESIZED)

    def test_the_fit_is_lanczos_and_the_jpeg_quality_85_progressive_optimized(self):
        real_save, real_thumbnail = Image.Image.save, Image.Image.thumbnail
        saves, thumbnails = [], []

        def save(img, fp, format=None, **params):
            saves.append((format, params))
            return real_save(img, fp, format, **params)

        def thumbnail(img, size, resample=None, reducing_gap=2.0):
            thumbnails.append((img.mode, tuple(size), resample))
            return real_thumbnail(img, size, resample, reducing_gap)

        palette = _image((2000, 3000), fmt="PNG", mode="P", color=7)
        with mock.patch.object(Image.Image, "save", save), mock.patch.object(Image.Image, "thumbnail", thumbnail):
            mod.hd_jpeg(BIG)
            mod.hd_jpeg(palette)
        self.assertEqual(saves, [("JPEG", {"quality": 85, "progressive": True, "optimize": True})] * 2)
        # LANCZOS for both, the palette image included: it is made RGB first.
        self.assertEqual(thumbnails, [("RGB", (1280, 1920), Image.Resampling.LANCZOS)] * 2)

    def test_an_mpo_that_fits_counts_as_a_jpeg(self):
        buf = io.BytesIO()
        first, second = Image.new("RGB", (600, 900), (1, 2, 3)), Image.new("RGB", (600, 900), (4, 5, 6))
        first.save(buf, format="MPO", save_all=True, append_images=[second])
        mpo = buf.getvalue()
        self.assertEqual(_decoded(mpo)[0], "MPO")
        data, kind = mod.hd_jpeg(mpo)
        self.assertEqual((kind, data == mpo), (mod.FIT_AS_IS, True))

    def test_a_png_or_webp_that_fits_becomes_a_jpeg_at_its_own_size(self):
        formats = ["PNG"] + (["WEBP"] if features.check("webp") else [])
        for fmt in formats:
            with self.subTest(fmt=fmt):
                source = _image((600, 900), fmt=fmt, mode="RGBA")
                self.assertEqual(_decoded(source)[2], "RGBA")
                data, tokens = self._made(source, path=f"/abc.{fmt.lower()}")
                self.assertEqual(_decoded(data)[:3], ("JPEG", (600, 900), "RGB"))
                self.assertEqual(tokens.get("converted"), "1")


# ── (5) the key ───────────────────────────────────────────────────────────

class KeyTest(unittest.TestCase):

    def test_the_key_is_versioned_by_tmdb_id_and_stem_in_media_images(self):
        db, cdn = _in_a_list(_media(1, tmdb_id=550, poster_path="https://image.tmdb.org/t/p/w500/pB8BM7pdSp6B6Ih7QZ4DrQ3PmJK.jpg"))
        _run(db, cdn)
        self.assertEqual(len(db.uploads), 1)
        upload = db.uploads[0]
        self.assertEqual(upload["bucket"], "media-images")
        self.assertEqual(upload["path"], "posters-hd/550/pB8BM7pdSp6B6Ih7QZ4DrQ3PmJK.jpg")
        self.assertEqual(upload["options"], {"content-type": "image/jpeg", "upsert": "true"})
        # The URL is stored exactly as upload_image returned it, `?` included.
        self.assertEqual(db.media("m-00001")["poster_hd_url"],
                         f"{PUBLIC}media-images/posters-hd/550/pB8BM7pdSp6B6Ih7QZ4DrQ3PmJK.jpg?")


# ── (6) the write ─────────────────────────────────────────────────────────

class WriteTest(unittest.TestCase):

    def test_one_patch_by_id_carrying_only_poster_hd_url(self):
        movie = _media(1, tmdb_id=1416, media_type="movie", poster_path="/movie.jpg")
        show = _media(2, tmdb_id=1416, media_type="tv", poster_path="/show.jpg")   # same number, not in a list
        db = _Db(items=[_item(1, movie["id"])], lists=[_list("l-1")], media=[movie, show])
        _run(db, _Cdn({CDN + "movie.jpg": (200, FITS)}))

        self.assertEqual(len(db.writes), 1)
        fields, (col, value) = db.writes[0]
        self.assertEqual(list(fields), ["poster_hd_url"])
        self.assertEqual((col, value), ("id", "m-00001"))
        self.assertNotEqual(col, "tmdb_id")
        # Positive control for the filter: the show sharing the number is untouched.
        self.assertIsNone(db.media("m-00002")["poster_hd_url"])
        self.assertIsNotNone(db.media("m-00001")["poster_hd_url"])
        # poster_url and poster_path are never written.
        self.assertEqual(db.media("m-00001")["poster_url"], movie["poster_url"])
        self.assertEqual(db.media("m-00001")["poster_path"], "/movie.jpg")


# ── (7) the outcomes ──────────────────────────────────────────────────────

class OutcomeTest(unittest.TestCase):

    def test_a_404_is_missing_with_no_upload_and_no_write(self):
        db, cdn = _in_a_list(_media(1), cdn_routes={CDN + "p1.jpg": (404, b"")})
        _, out = _run(db, cdn)
        self.assertEqual((db.uploads, db.writes), ([], []))
        self.assertEqual((_tokens(out).get("missing"), _tokens(out).get("errors")), ("1", "0"))
        self.assertEqual(db.media("m-00001")["poster_url"], _media(1)["poster_url"])

    def test_a_timeout_or_a_5xx_is_an_error_with_no_write(self):
        for answer, kind in ((requests.Timeout("read timed out"), "Timeout"), ((503, b"busy"), "HTTP 503")):
            with self.subTest(kind=kind):
                db, cdn = _in_a_list(_media(1), cdn_routes={CDN + "p1.jpg": answer})
                _, out = _run(db, cdn)
                self.assertEqual((db.uploads, db.writes), ([], []))
                self.assertEqual((_tokens(out).get("errors"), _tokens(out).get("missing")), ("1", "0"))
                self.assertIn(f"[POSTER_HD] ERROR tmdb=1001 type=movie: fetch failed ({kind})", out)

    def test_bytes_pillow_cannot_open_are_an_error_with_no_write(self):
        db, cdn = _in_a_list(_media(1), cdn_routes={CDN + "p1.jpg": (200, b"<html>not an image</html>")})
        _, out = _run(db, cdn)
        self.assertEqual((db.uploads, db.writes), ([], []))
        self.assertEqual(_tokens(out).get("errors"), "1")
        self.assertIn("[POSTER_HD] ERROR tmdb=1001 type=movie: image unreadable (UnidentifiedImageError)", out)

    def test_unreadable_bytes_say_what_they_start_like(self):
        # The exit ramp's «say which»: the format's name, never the bytes.
        for content, like in ((b"\x00\x00\x00\x1cftypavif" + b"\x00" * 40, "avif"),
                              (b"<!DOCTYPE html><html>", "html"), (b"\x01\x02\x03", "unknown")):
            with self.subTest(like=like):
                db, cdn = _in_a_list(_media(1), cdn_routes={CDN + "p1.jpg": (200, content)})
                _, out = _run(db, cdn)
                self.assertIn(f"image unreadable (UnidentifiedImageError) — it starts like {like}", out)
                self.assertEqual((db.uploads, db.writes), ([], []))

    def test_a_failed_upload_is_an_error_with_no_write(self):
        db, cdn = _in_a_list(_media(1), fail_uploads=True)
        _, out = _run(db, cdn)
        self.assertEqual(db.writes, [])
        self.assertEqual((_tokens(out).get("errors"), _tokens(out).get("made")), ("1", "0"))
        self.assertIn("[POSTER_HD] ERROR tmdb=1001 type=movie: upload failed", out)

    def test_a_failed_patch_is_an_error_and_the_run_goes_on(self):
        db, cdn = _in_a_list(_media(1), _media(2), fail_writes={"m-00001"})
        _, out = _run(db, cdn)
        tokens = _tokens(out)
        self.assertEqual((tokens.get("errors"), tokens.get("written"), tokens.get("made")), ("1", "1", "2"))
        self.assertIsNone(db.media("m-00001")["poster_hd_url"])
        self.assertIsNotNone(db.media("m-00002")["poster_hd_url"])     # the next title was made
        self.assertIn("[POSTER_HD] ERROR tmdb=1001 type=movie: write failed (APIError)", out)

    def test_a_failed_patch_still_counts_the_bytes_it_uploaded(self):
        db, cdn = _in_a_list(_media(1), fail_writes={"m-00001"})
        _, out = _run(db, cdn)
        tokens = _tokens(out)
        uploaded = len(db.uploads[0]["bytes"])
        self.assertEqual((tokens.get("made"), tokens.get("written")), ("1", "0"))
        self.assertEqual(tokens.get("bytes_uploaded"), str(uploaded))
        self.assertEqual(tokens.get("avg_kb"), f"{uploaded / 1024:.1f}")     # per file made, not per row written

    def test_an_upload_failure_leaks_no_list_owner_id_or_query(self):
        db, cdn = _in_a_list(_media(1), fail_uploads=True)
        _, out = _run(db, cdn)
        # The house upload_image says its own line: the storage path, no more.
        self.assertIn("[Supabase] Error uploading image to media-images/posters-hd/1001/p1.jpg: ", out)
        for leak in ("l-1", "owner-", "?", "m-0000"):
            with self.subTest(leak=leak):
                self.assertNotIn(leak, out)

    def test_the_log_names_no_title_list_owner_or_query_and_no_line_per_success(self):
        db, cdn = _in_a_list(_media(1), _media(2), _media(3), cdn_routes={CDN + "p2.jpg": (503, b"")})
        _, out = _run(db, cdn)
        error_lines = [l for l in out.splitlines() if "tmdb=" in l]
        self.assertEqual(error_lines, ["[POSTER_HD] ERROR tmdb=1002 type=movie: fetch failed (HTTP 503)"])
        for leak in ("l-1", "owner-", "?", "http", "m-0000"):
            with self.subTest(leak=leak):
                self.assertNotIn(leak, out)
        self.assertEqual(_tokens(out).get("made"), "2")   # the two successes, counted, not printed


# ── (8) the budget ────────────────────────────────────────────────────────

class BudgetTest(unittest.TestCase):

    def test_the_batch_cap_ends_the_run_with_its_summary(self):
        db, cdn = _in_a_list(_media(1), _media(2), _media(3))
        _, out = _run(db, cdn, batch=2)
        self.assertEqual(len(db.uploads), 2)
        self.assertEqual((_tokens(out).get("budget_hit"), _tokens(out).get("pending")), ("1", "3"))
        self.assertTrue(out.rstrip().splitlines()[-1].startswith("[POSTER_HD] SUMMARY "))
        # Positive control: a batch that holds them all is no budget hit.
        db, cdn = _in_a_list(_media(1), _media(2), _media(3))
        _, out = _run(db, cdn, batch=3)
        self.assertEqual((len(db.uploads), _tokens(out).get("budget_hit")), (3, "0"))

    def test_the_time_budget_ends_the_run_with_its_summary(self):
        db, cdn = _in_a_list(_media(1), _media(2), _media(3))
        _, out = _run(db, cdn, budget_min=1, clock=_Clock(step=25.0))
        self.assertLess(len(db.uploads), 3)
        self.assertGreater(len(db.uploads), 0)
        self.assertEqual(_tokens(out).get("budget_hit"), "1")
        self.assertTrue(out.rstrip().splitlines()[-1].startswith("[POSTER_HD] SUMMARY "))
        # Positive control: the same run on a clock that does not move finishes.
        db, cdn = _in_a_list(_media(1), _media(2), _media(3))
        _, out = _run(db, cdn, budget_min=1, clock=_Clock(step=0.0))
        self.assertEqual((len(db.uploads), _tokens(out).get("budget_hit")), (3, "0"))


# ── (9) the fail-fast ─────────────────────────────────────────────────────

class FailFastTest(unittest.TestCase):

    def test_a_missing_column_names_the_migration_and_does_nothing(self):
        db, cdn = _in_a_list(_media(1), missing_column=True)
        stats, out = _run(db, cdn)
        self.assertIsInstance(stats, SystemExit)
        self.assertEqual(stats.code, 1)
        self.assertIn("[POSTER_HD] ERROR: the server has no media.poster_hd_url — apply migration "
                      "20261027200000 before this code runs.", out)
        self.assertEqual((db.uploads, db.writes, cdn.urls), ([], [], []))
        # Positive control: the same set with the column present runs.
        db, cdn = _in_a_list(_media(1))
        stats, out = _run(db, cdn)
        self.assertIsInstance(stats, dict)
        self.assertEqual(len(db.uploads), 1)

    def test_a_42703_on_another_column_is_not_blamed_on_the_migration(self):
        other = APIError({"code": "42703", "message": "column media_list_items.media_id does not exist",
                          "hint": None, "details": None})
        db, cdn = _in_a_list(_media(1), read_errors={"media_list_items": other})
        stats, out = _run(db, cdn)
        self.assertIsInstance(stats, SystemExit)
        self.assertEqual(stats.code, 1)
        self.assertIn("[POSTER_HD] ERROR: the set's read failed (APIError); stopping, nothing written.", out)
        self.assertNotIn("20261027200000", out)
        self.assertEqual((db.uploads, db.writes, cdn.urls), ([], [], []))

    def test_any_other_failure_of_the_sets_read_is_a_red_run_that_writes_nothing(self):
        for table in ("media_list_items", "media_lists", "media"):
            with self.subTest(table=table):
                db, cdn = _in_a_list(_media(1), read_errors={table: RuntimeError("permission denied for table")})
                stats, out = _run(db, cdn)
                self.assertIsInstance(stats, SystemExit)
                self.assertEqual(stats.code, 1)
                self.assertIn("[POSTER_HD] ERROR: the set's read failed (RuntimeError); stopping, nothing written.", out)
                self.assertNotIn("permission denied", out)       # the kind, not the message
                self.assertEqual((db.uploads, db.writes, cdn.urls), ([], [], []))


# ── (10) the SUMMARY ──────────────────────────────────────────────────────

class SummaryTest(unittest.TestCase):

    def test_the_summary_carries_every_field_in_order(self):
        db, cdn = _in_a_list(_media(1), _media(2), _media(3, poster_path=""), _media(4),
                             _media(5, tmdb_missing_at="2026-10-20T08:00:00+00:00"),
                             cdn_routes={CDN + "p2.jpg": (200, FITS), CDN + "p4.jpg": (404, b"")})
        _, out = _run(db, cdn)
        line = _summary(out)
        self.assertTrue(line.startswith("[POSTER_HD] SUMMARY "), out)
        self.assertEqual([t.split("=", 1)[0] for t in line.split()[2:]], list(SUMMARY_FIELDS))
        tokens = _tokens(out)
        self.assertEqual({k: tokens[k] for k in SUMMARY_FIELDS if k not in ("bytes_uploaded", "avg_kb")},
                         {"candidates": "5", "pending": "3", "made": "2", "resized": "1", "fit_as_is": "1",
                          "converted": "0", "missing": "1", "errors": "0", "no_path": "1", "gone": "1",
                          "written": "2", "budget_hit": "0"})

    def test_avg_kb_comes_from_the_bytes_actually_uploaded(self):
        db, cdn = _in_a_list(_media(1), _media(2), _media(3),
                             cdn_routes={CDN + "p1.jpg": (200, FITS), CDN + "p2.jpg": (200, SMALL),
                                         CDN + "p3.jpg": (503, b"")})
        _, out = _run(db, cdn)
        tokens = _tokens(out)
        self.assertEqual(tokens.get("bytes_uploaded"), str(len(FITS) + len(SMALL)))
        self.assertEqual(tokens.get("avg_kb"), f"{(len(FITS) + len(SMALL)) / 2 / 1024:.1f}")
        # Positive control: nothing uploaded has no average.
        db, cdn = _in_a_list(_media(1), cdn_routes={CDN + "p1.jpg": (404, b"")})
        _, out = _run(db, cdn)
        self.assertEqual((_tokens(out).get("bytes_uploaded"), _tokens(out).get("avg_kb")), ("0", "-"))


    def test_avg_kb_counts_the_bytes_uploaded_not_the_bytes_downloaded(self):
        db, cdn = _in_a_list(_media(1), _media(2), cdn_routes={CDN + "p1.jpg": (200, BIG), CDN + "p2.jpg": (200, FITS)})
        _, out = _run(db, cdn)
        uploaded = sum(len(u["bytes"]) for u in db.uploads)
        self.assertNotEqual(uploaded, len(BIG) + len(FITS))      # the resized file is not the original
        self.assertEqual(_tokens(out).get("bytes_uploaded"), str(uploaded))
        self.assertEqual(_tokens(out).get("avg_kb"), f"{uploaded / 2 / 1024:.1f}")


# ── main: the environment ─────────────────────────────────────────────────

class MainTest(unittest.TestCase):

    def _main(self, env):
        seen = {}

        def fake_run(db, **kwargs):
            seen.update(kwargs, db=db)
            return {}

        out = io.StringIO()
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.object(mod, "SupabaseClient", lambda url, key: ("db", url, key)), \
                mock.patch.object(mod, "run", fake_run), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            try:
                mod.main(clock=_Clock())
                exited = None
            except SystemExit as exc:
                exited = exc
        return seen, exited, out.getvalue()

    def test_main_reads_the_batch_and_the_budget_with_the_scripts_defaults(self):
        seen, exited, _ = self._main(FAKE_ENV)
        self.assertIsNone(exited)
        self.assertEqual((seen.get("batch"), seen.get("budget_min")), (300, 20))
        self.assertEqual(seen.get("db"), ("db", FAKE_ENV["SUPABASE_URL"], FAKE_ENV["SUPABASE_KEY"]))
        seen, _, _ = self._main({**FAKE_ENV, "POSTERS_HD_BATCH": "40", "POSTERS_HD_TIME_BUDGET_MIN": "5"})
        self.assertEqual((seen.get("batch"), seen.get("budget_min")), (40, 5))
        self.assertEqual((mod.DEFAULT_POSTERS_HD_BATCH, mod.DEFAULT_POSTERS_HD_TIME_BUDGET_MIN), (300, 20))

    def test_main_needs_the_supabase_pair_and_no_tmdb_key(self):
        self.assertNotIn("TMDB_API_KEY", FAKE_ENV)
        seen, exited, _ = self._main(FAKE_ENV)            # no TMDB_API_KEY: runs
        self.assertIsNone(exited)
        self.assertIn("batch", seen)
        # Positive control: without SUPABASE_KEY it stops, naming it.
        seen, exited, out = self._main({"SUPABASE_URL": FAKE_ENV["SUPABASE_URL"]})
        self.assertIsInstance(exited, SystemExit)
        self.assertEqual(exited.code, 1)
        self.assertIn("[POSTER_HD] ERROR: Required environment variable 'SUPABASE_KEY' is not set.", out)
        self.assertEqual(seen, {})


if __name__ == "__main__":
    unittest.main()
