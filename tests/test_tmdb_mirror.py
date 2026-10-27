"""
Unit tests for the TMDb image mirror (RUN B5.8 · 0b): bot/tmdb_mirror.py
copies every image the app still loads from image.tmdb.org — backdrops at
w1280, title logos at w500, cast and crew photos at w185 — to
media-images/tmdb/{size}/{file} under TMDb's own file name, byte for byte, with
a one-year cache; a ledger on the server (public.tmdb_image_mirror) remembers
each file once, ok or missing.

Fakes only: a Supabase client whose two RPCs (tmdb_mirror_titles,
tmdb_mirror_missing) answer from in-memory titles and a ledger that the
script's upserts fill, whose storage records every upload behind the REAL
SupabaseClient.upload_image; TMDb's image CDN answered in memory with its
content types; images built in memory with Pillow; a clock and a sleep handed
in. No network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import threading
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import requests
from PIL import Image
from postgrest.exceptions import APIError
from requests.structures import CaseInsensitiveDict

from bot import tmdb_mirror as mod
from bot.supabase_client import SupabaseClient

START = datetime(2026, 10, 27, 23, 11, tzinfo=timezone.utc)
CDN = "https://image.tmdb.org/t/p/"
PUBLIC = "https://project.supabase.invalid/storage/v1/object/public/"
FAKE_ENV = {"SUPABASE_URL": "https://project.supabase.invalid", "SUPABASE_KEY": "fake-supabase-key"}
SUMMARY_FIELDS = ("pages", "titles", "wanted", "already", "copied", "missing", "errors", "skipped_size",
                  "w1280", "w500", "w185", "bytes_uploaded", "avg_kb", "files_per_s", "budget_hit", "sweep_done")


def _api_error(code, message="x"):
    return APIError({"code": code, "message": message, "hint": None, "details": None})


# ── images, in memory ─────────────────────────────────────────────────────

def _image(size, fmt="JPEG", mode="RGB", color=(30, 90, 160)) -> bytes:
    buf = io.BytesIO()
    Image.new(mode, size, color).save(buf, format=fmt)
    return buf.getvalue()


def _untouchable(data: bytes) -> bytes:
    """Bytes no encoder writes back — a marker after the image's end — so an
    upload equal to them was not re-encoded (a flat image re-saved at
    Pillow's defaults comes back byte for byte)."""
    return data + b"--only-tmdb-wrote-these-bytes--"


PHOTO = _untouchable(_image((185, 278)))
BACKDROP = _untouchable(_image((1280, 720), color=(200, 120, 20)))
LOGO_PNG = _untouchable(_image((500, 200), fmt="PNG", mode="RGBA", color=(255, 255, 255, 180)))
LOGO_WEBP = _untouchable(_image((500, 200), fmt="WEBP", mode="RGBA", color=(255, 255, 255, 180)))
LOGO_SVG = b'<svg xmlns="http://www.w3.org/2000/svg" width="10" height="4"></svg>'


# ── the fakes ─────────────────────────────────────────────────────────────

class _Call:
    def __init__(self, fn):
        self.fn = fn

    def execute(self):
        return types.SimpleNamespace(data=self.fn())


class _Table:
    """Any table: every operation is recorded; only the ledger's upsert does anything."""

    def __init__(self, db, name):
        self.db, self.name = db, name

    def _record(self, op, *args, **kwargs):
        self.db.table_ops.append((self.name, op))
        return self

    def select(self, *a, **kw): return self._record("select")
    def insert(self, *a, **kw): return self._record("insert")
    def update(self, *a, **kw): return self._record("update")
    def delete(self, *a, **kw): return self._record("delete")

    def upsert(self, rows, on_conflict=None, **kwargs):
        self._record("upsert")
        db = self.db

        def apply():
            db.threads.add(("upsert", threading.get_ident()))
            if db.upsert_error is not None:
                raise db.upsert_error
            db.upserts.append((self.name, [dict(r) for r in rows], on_conflict))
            for r in rows:
                db.ledger[(r["size"], r["file"])] = dict(r)
            return list(rows)

        return _Call(apply)


class _Bucket:
    def __init__(self, db, name):
        self.db, self.name = db, name

    def upload(self, path, file, file_options=None):
        if self.db.fail_uploads:
            raise RuntimeError("storage refused the upload")
        self.db.uploads.append({"bucket": self.name, "path": path, "bytes": file, "options": dict(file_options or {})})

    def get_public_url(self, path):
        return f"{PUBLIC}{self.name}/{path}?"


class _Db:
    """The house SupabaseClient as the mirror uses it: .client.rpc(...),
    .client.table(...), and upload_image — the REAL one, over a fake storage."""

    def __init__(self, titles=(), ledger=(), rpc_errors=None, upsert_error=None, fail_uploads=False,
                 missing_shape="text"):
        self.titles = sorted((dict(t) for t in titles), key=lambda t: t["id"])
        self.ledger = {(s, f): {"size": s, "file": f, "status": st, "bytes": None} for s, f, st in ledger}
        self.rpc_errors = {k: list(v) if isinstance(v, list) else [v] for k, v in (rpc_errors or {}).items()}
        self.upsert_error, self.fail_uploads, self.missing_shape = upsert_error, fail_uploads, missing_shape
        self.rpc_calls, self.upserts, self.table_ops, self.uploads, self.threads = [], [], [], [], set()
        self.client = self
        self.storage = types.SimpleNamespace(from_=lambda name: _Bucket(self, name))

    def rpc(self, name, params):
        def call():
            self.threads.add(("rpc", threading.get_ident()))
            self.rpc_calls.append((name, dict(params)))
            errors = self.rpc_errors.get(name)
            if errors:
                error = errors.pop(0)   # None: this call answers
                if error is not None:
                    raise error
            if name == "tmdb_mirror_titles":
                after, limit = params.get("p_after"), min(params.get("p_limit", 200), 500)
                return [dict(t) for t in self.titles if after is None or t["id"] > after][:limit]
            if name == "tmdb_mirror_missing":
                seen, out = set(), []
                for f in params["p_files"][:1000]:
                    if f not in seen and (params["p_size"], f) not in self.ledger:
                        seen.add(f)
                        out.append(f)
                return [{"tmdb_mirror_missing": f} for f in out] if self.missing_shape == "rows" else out
            raise AssertionError(f"unknown rpc {name}")
        return _Call(call)

    def table(self, name):
        return _Table(self, name)

    def upload_image(self, bucket, path, image_bytes, content_type="image/jpeg", **kwargs):
        return SupabaseClient.upload_image(self, bucket, path, image_bytes, content_type, **kwargs)

    def get_public_url(self, bucket, path):
        return SupabaseClient.get_public_url(self, bucket, path)

    def calls(self, name):
        return [p for n, p in self.rpc_calls if n == name]


class _Cdn:
    """Stands in for requests.get: answers by URL — (status, bytes, content
    type) or an exception — and records every URL asked."""

    def __init__(self, routes=None, default=(404, b"", "text/html")):
        self.routes, self.default, self.urls, self.lock = dict(routes or {}), default, [], threading.Lock()

    def __call__(self, url, **kwargs):
        with self.lock:
            self.urls.append(url)
        answer = self.routes.get(url, self.default)
        if isinstance(answer, BaseException):
            raise answer
        status, content, ctype = answer
        return types.SimpleNamespace(status_code=status, content=content,
                                     headers=CaseInsensitiveDict({"Content-Type": ctype} if ctype else {}))


class _Clock:
    """START plus `step` seconds per call; every value it gave is kept."""

    def __init__(self, step=0.0):
        self.step, self.values = step, []

    def __call__(self):
        value = START + timedelta(seconds=self.step * len(self.values))
        self.values.append(value)
        return value


def _title(i, backdrop=None, logo=None, photos=()):
    return {"id": f"00000000-0000-0000-0000-{i:012d}", "tmdb_id": 1000 + i, "backdrop_path": backdrop,
            "title_logo_url": logo, "profile_paths": list(photos)}


def _cdn_for(titles, extra=None):
    """A CDN that answers every file the titles name, at the size the mirror asks."""
    routes = {}
    for t in titles:
        if t.get("backdrop_path"):
            routes[f"{CDN}w1280/{t['backdrop_path'].lstrip('/')}"] = (200, BACKDROP, "image/jpeg")
        if t.get("title_logo_url") and "/w500/" in t["title_logo_url"]:
            name = t["title_logo_url"].rsplit("/", 1)[-1]
            routes[f"{CDN}w500/{name}"] = ((200, LOGO_SVG, "image/svg+xml") if name.endswith(".svg")
                                           else (200, LOGO_PNG, "image/png"))
        for p in t.get("profile_paths") or ():
            routes[f"{CDN}w185/{p.lstrip('/')}"] = (200, PHOTO, "image/jpeg")
    routes.update(extra or {})
    return _Cdn(routes)


def _run(db, cdn, *, workers=2, budget_min=25, max_files=20000, clock=None, sleep=None, make_uploader=None):
    out, sleeps = io.StringIO(), []
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            stats = mod.run(db, make_uploader=make_uploader or (lambda: db), fetch=cdn, clock=clock or _Clock(),
                            workers=workers, budget_min=budget_min, max_files=max_files,
                            sleep=sleep or sleeps.append)
        except BaseException as exc:  # SystemExit included: the fail-fast is an exit
            stats = exc
    return stats, out.getvalue()


def _summary(out):
    lines = [l for l in out.splitlines() if l.startswith("[TMDB_MIRROR] SUMMARY ")]
    return lines[-1] if lines else ""


def _tokens(out):
    return dict(t.split("=", 1) for t in _summary(out).split()[2:] if "=" in t)


# ── B5.7's fetch, reused: it carries the CDN's content type now ───────────

class FetchContentTypeTest(unittest.TestCase):

    def test_fetch_image_carries_the_cdns_content_type(self):
        from bot import posters_hd
        got = posters_hd.fetch_image("u", _Cdn({"u": (200, PHOTO, "image/png")}))
        self.assertEqual((got.outcome, got.content, got.content_type), (posters_hd.FETCH_OK, PHOTO, "image/png"))
        # Positive control: a response without headers (B5.7's fakes) has none, and nothing else changes.
        got = posters_hd.fetch_image("u", lambda url, **kw: types.SimpleNamespace(status_code=200, content=PHOTO))
        self.assertEqual((got.outcome, got.content, got.content_type), (posters_hd.FETCH_OK, PHOTO, None))


# ── (1) a page's wanted files ─────────────────────────────────────────────

class WantedTest(unittest.TestCase):

    def test_the_three_sizes_deduplicated_within_the_page(self):
        titles = [_title(1, backdrop="/b1.jpg", logo=f"{CDN}w500/l1.png", photos=["/p1.jpg", "/p2.jpg", "/p1.jpg"]),
                  _title(2, backdrop="/b1.jpg", photos=["/p1.jpg", "/p3.jpg"])]
        db, cdn = _Db(titles), _cdn_for(titles)
        _, out = _run(db, cdn)
        self.assertEqual(sorted(u["path"] for u in db.uploads),
                         ["tmdb/w1280/b1.jpg", "tmdb/w185/p1.jpg", "tmdb/w185/p2.jpg", "tmdb/w185/p3.jpg",
                          "tmdb/w500/l1.png"])
        self.assertEqual(len(cdn.urls), 5)                       # each file asked once
        self.assertEqual(_tokens(out).get("wanted"), "5")

    def test_a_logo_at_another_size_is_skipped_not_fetched(self):
        titles = [_title(1, logo=f"{CDN}original/l1.png"), _title(2, logo=f"{CDN}w300/l2.png"),
                  _title(3, logo=f"{CDN}w500/l3.png")]
        db, cdn = _Db(titles), _cdn_for(titles)
        _, out = _run(db, cdn)
        self.assertEqual(_tokens(out).get("skipped_size"), "2")
        self.assertFalse([u for u in cdn.urls if "l1.png" in u or "l2.png" in u])
        # Positive control: the w500 one is fetched and copied.
        self.assertEqual(cdn.urls, [f"{CDN}w500/l3.png"])
        self.assertEqual([u["path"] for u in db.uploads], ["tmdb/w500/l3.png"])

    def test_an_svg_logo_keeps_its_content_type(self):
        titles = [_title(1, logo=f"{CDN}w500/l1.svg")]
        db, cdn = _Db(titles), _cdn_for(titles)
        _run(db, cdn)
        self.assertEqual(len(db.uploads), 1)
        self.assertEqual(db.uploads[0]["options"]["content-type"], "image/svg+xml")
        self.assertEqual(db.uploads[0]["bytes"], LOGO_SVG)


# ── (2) the missing question ──────────────────────────────────────────────

class MissingQuestionTest(unittest.TestCase):

    def test_per_size_in_chunks_of_at_most_1000(self):
        photos = [f"/p{i:05d}.jpg" for i in range(2500)]
        titles = [_title(1, backdrop="/b1.jpg", photos=photos)]
        db, cdn = _Db(titles), _Cdn(default=(404, b"", "text/html"))
        _run(db, cdn)
        calls = db.calls("tmdb_mirror_missing")
        by_size = {}
        for c in calls:
            by_size.setdefault(c["p_size"], []).append(len(c["p_files"]))
        self.assertEqual(sorted(by_size), ["w1280", "w185"])
        self.assertEqual(by_size["w185"], [1000, 1000, 500])     # positive control: 2,500 → three calls
        self.assertEqual(by_size["w1280"], [1])
        self.assertTrue(all(n <= 1000 for c in calls for n in [len(c["p_files"])]))

    def test_files_the_ledger_holds_are_not_fetched_ok_or_missing(self):
        titles = [_title(1, backdrop="/b1.jpg", photos=["/p1.jpg", "/p2.jpg", "/p3.jpg"])]
        db = _Db(titles, ledger=[("w185", "p1.jpg", "ok"), ("w185", "p2.jpg", "missing")])
        cdn = _cdn_for(titles)
        _, out = _run(db, cdn)
        self.assertEqual(sorted(cdn.urls), [f"{CDN}w1280/b1.jpg", f"{CDN}w185/p3.jpg"])
        self.assertEqual((_tokens(out).get("wanted"), _tokens(out).get("already")), ("4", "2"))

    def test_the_rpcs_answer_as_rows_too(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db, cdn = _Db(titles, missing_shape="rows"), _cdn_for(titles)
        _run(db, cdn)
        self.assertEqual([u["path"] for u in db.uploads], ["tmdb/w185/p1.jpg"])


# ── (3) a copy: byte for byte, its content type, a year ───────────────────

class CopyTest(unittest.TestCase):

    def test_ok_is_one_upload_byte_for_byte_with_the_cdns_type_and_a_year(self):
        titles = [_title(1, backdrop="/b1.jpg")]
        db, cdn = _Db(titles), _cdn_for(titles)
        _run(db, cdn)
        self.assertEqual(len(db.uploads), 1)
        upload = db.uploads[0]
        self.assertEqual((upload["bucket"], upload["path"]), ("media-images", "tmdb/w1280/b1.jpg"))
        self.assertEqual(upload["bytes"], BACKDROP)
        # Positive control: a re-encode would show, even a default round trip in the same format.
        for original, fmt in ((BACKDROP, "JPEG"), (LOGO_PNG, "PNG"), (PHOTO, "JPEG")):
            with self.subTest(fmt=fmt):
                reencoded = io.BytesIO()
                Image.open(io.BytesIO(original)).save(reencoded, format=fmt)
                self.assertNotEqual(reencoded.getvalue(), original)
        self.assertEqual(upload["options"], {"content-type": "image/jpeg", "upsert": "true", "cache-control": "31536000"})

    def test_then_the_ledger_row_ok_with_its_bytes(self):
        titles = [_title(1, backdrop="/b1.jpg")]
        db, cdn = _Db(titles), _cdn_for(titles)
        _run(db, cdn)
        self.assertIn(("w1280", "b1.jpg"), db.ledger)
        self.assertEqual(db.ledger[("w1280", "b1.jpg")],
                         {"size": "w1280", "file": "b1.jpg", "status": "ok", "bytes": len(BACKDROP)})

    def test_every_kind_goes_up_byte_for_byte_with_its_type(self):
        titles = [_title(1, logo=f"{CDN}w500/l1.png", photos=["/p1.jpg"]), _title(2, logo=f"{CDN}w500/l2.webp")]
        db = _Db(titles)
        cdn = _cdn_for(titles, {f"{CDN}w500/l2.webp": (200, LOGO_WEBP, "image/webp")})
        _run(db, cdn)
        got = {u["path"]: (u["bytes"], u["options"]["content-type"]) for u in db.uploads}
        self.assertEqual(got, {"tmdb/w500/l1.png": (LOGO_PNG, "image/png"), "tmdb/w185/p1.jpg": (PHOTO, "image/jpeg"),
                               "tmdb/w500/l2.webp": (LOGO_WEBP, "image/webp")})

    def test_a_content_type_with_parameters_is_its_bare_type(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db = _Db(titles)
        cdn = _Cdn({f"{CDN}w185/p1.jpg": (200, PHOTO, "image/jpeg; charset=binary")})
        _run(db, cdn)
        self.assertEqual(len(db.uploads), 1)
        self.assertEqual(db.uploads[0]["options"]["content-type"], "image/jpeg")


# ── (4) the other outcomes ────────────────────────────────────────────────

class OutcomeTest(unittest.TestCase):

    def test_a_404_is_a_missing_row_and_no_upload(self):
        titles = [_title(1, photos=["/gone.jpg", "/p2.jpg"])]
        db, cdn = _Db(titles), _cdn_for(titles, {f"{CDN}w185/gone.jpg": (404, b"", "text/html")})
        _, out = _run(db, cdn)
        self.assertEqual([u["path"] for u in db.uploads], ["tmdb/w185/p2.jpg"])
        self.assertEqual(db.ledger[("w185", "gone.jpg")]["status"], "missing")
        self.assertEqual((_tokens(out).get("missing"), _tokens(out).get("copied")), ("1", "1"))
        # So it is not asked again next run.
        cdn.urls.clear()
        _run(db, cdn)
        self.assertEqual(cdn.urls, [])

    def test_an_error_is_no_upload_and_no_ledger_row_and_is_asked_again(self):
        for answer, kind in ((requests.Timeout("slow"), "Timeout"), ((503, b"", "text/html"), "HTTP 503")):
            with self.subTest(kind=kind):
                titles = [_title(1, photos=["/p1.jpg"])]
                db, cdn = _Db(titles), _Cdn({f"{CDN}w185/p1.jpg": answer})
                _, out = _run(db, cdn)
                self.assertEqual((db.uploads, db.ledger), ([], {}))
                self.assertEqual(_tokens(out).get("errors"), "1")
                self.assertIn(f"[TMDB_MIRROR] ERROR size=w185: fetch failed ({kind})", out)
                _run(db, cdn)
                self.assertEqual(len(cdn.urls), 2)               # asked again: nothing recorded

    def test_an_unexpected_content_type_is_not_uploaded_and_is_counted(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db, cdn = _Db(titles), _Cdn({f"{CDN}w185/p1.jpg": (200, b"<html>", "text/html")})
        _, out = _run(db, cdn)
        self.assertEqual((db.uploads, db.ledger), ([], {}))
        self.assertEqual(_tokens(out).get("errors"), "1")
        self.assertIn("[TMDB_MIRROR] ERROR size=w185: content type text/html is not an image this mirror keeps", out)

    def test_a_failed_upload_is_an_error_with_no_ledger_row(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db, cdn = _Db(titles, fail_uploads=True), _cdn_for(titles)
        _, out = _run(db, cdn)
        self.assertEqual(db.ledger, {})
        self.assertEqual((_tokens(out).get("errors"), _tokens(out).get("copied")), ("1", "0"))

    def test_the_log_names_no_file_title_or_query_and_no_line_per_success(self):
        titles = [_title(1, backdrop="/b1.jpg", photos=["/p1.jpg", "/p2.jpg"])]
        db, cdn = _Db(titles), _cdn_for(titles, {f"{CDN}w185/p2.jpg": (500, b"", "text/html")})
        _, out = _run(db, cdn)
        self.assertEqual([l for l in out.splitlines() if "ERROR" in l],
                         ["[TMDB_MIRROR] ERROR size=w185: fetch failed (HTTP 500)"])
        for leak in ("b1.jpg", "p1.jpg", "p2.jpg", "?", "1001"):
            with self.subTest(leak=leak):
                self.assertNotIn(leak, out)
        self.assertEqual(_tokens(out).get("copied"), "2")

    def test_error_lines_stop_at_twenty(self):
        photos = [f"/p{i:03d}.jpg" for i in range(25)]
        db, cdn = _Db([_title(1, photos=photos)]), _Cdn(default=(503, b"", "text/html"))
        _, out = _run(db, cdn)
        self.assertEqual(len([l for l in out.splitlines() if l.startswith("[TMDB_MIRROR] ERROR size=")]), 20)
        self.assertIn("[TMDB_MIRROR] … more errors not printed (the SUMMARY counts them)", out)
        self.assertEqual(_tokens(out).get("errors"), "25")


# ── (5) the ledger upsert ─────────────────────────────────────────────────

class LedgerTest(unittest.TestCase):

    def test_once_per_page_at_most_500_rows_a_call_on_size_and_file(self):
        photos = [f"/p{i:05d}.jpg" for i in range(1200)]
        titles = [_title(1, photos=photos)]
        db = _Db(titles)
        cdn = _Cdn(default=(200, PHOTO, "image/jpeg"))
        _run(db, cdn, workers=4)
        self.assertEqual([len(rows) for _, rows, _ in db.upserts], [500, 500, 200])
        self.assertEqual({(name, conflict) for name, _, conflict in db.upserts}, {("tmdb_image_mirror", "size,file")})
        self.assertEqual(len(db.ledger), 1200)

    def test_each_page_writes_its_rows_after_its_uploads(self):
        titles = [_title(i, photos=[f"/p{i}.jpg"]) for i in range(1, 4)]
        db, cdn = _Db(titles), _cdn_for(titles)
        with mock.patch.object(mod, "PAGE_TITLES", 1):
            _run(db, cdn)
        self.assertEqual([[r["file"] for r in rows] for _, rows, _ in db.upserts], [["p1.jpg"], ["p2.jpg"], ["p3.jpg"]])

    def test_a_page_with_nothing_new_writes_nothing(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db, cdn = _Db(titles, ledger=[("w185", "p1.jpg", "ok")]), _cdn_for(titles)
        _, out = _run(db, cdn)
        self.assertEqual((db.upserts, db.uploads), ([], []))
        self.assertEqual(_tokens(out).get("copied"), "0")


# ── (6) read-only on the catalogue ────────────────────────────────────────

class ReadOnlyTest(unittest.TestCase):

    def test_nothing_ever_touches_media_or_media_credits(self):
        titles = [_title(1, backdrop="/b1.jpg", photos=["/p1.jpg"]), _title(2, photos=["/gone.jpg"])]
        db, cdn = _Db(titles), _cdn_for(titles, {f"{CDN}w185/gone.jpg": (404, b"", "text/html")})
        _run(db, cdn)
        self.assertEqual([op for op in db.table_ops if op[0] in ("media", "media_credits")], [])
        self.assertTrue(all(op == ("tmdb_image_mirror", "upsert") for op in db.table_ops), db.table_ops)
        # Positive control: the catalogue was read, through the RPC.
        self.assertTrue(db.calls("tmdb_mirror_titles"))

    def test_the_rpcs_and_the_ledger_stay_on_the_main_thread_uploads_get_a_client_per_worker(self):
        titles = [_title(1, photos=[f"/p{i}.jpg" for i in range(40)])]
        db, cdn = _Db(titles), _cdn_for(titles)
        made, lock = [], threading.Lock()

        def make_uploader():
            with lock:
                made.append(threading.get_ident())
            return db

        main = threading.get_ident()
        _run(db, cdn, workers=4, make_uploader=make_uploader)
        self.assertEqual({t for _, t in db.threads}, {main})
        self.assertEqual(len(made), len(set(made)))              # one client per worker thread
        self.assertLessEqual(len(made), 4)
        self.assertNotIn(main, made)
        self.assertEqual(len(db.uploads), 40)


# ── (7) the budget and the sweep ──────────────────────────────────────────

class BudgetTest(unittest.TestCase):

    def test_pages_go_keyset_by_the_last_id_and_the_last_page_ends_the_sweep(self):
        titles = [_title(i, photos=[f"/p{i}.jpg"]) for i in range(1, 6)]
        db, cdn = _Db(titles), _cdn_for(titles)
        with mock.patch.object(mod, "PAGE_TITLES", 2):
            _, out = _run(db, cdn)
        self.assertEqual([c.get("p_after") for c in db.calls("tmdb_mirror_titles")],
                         [None, titles[1]["id"], titles[3]["id"]])
        self.assertTrue(all(c["p_limit"] == 2 for c in db.calls("tmdb_mirror_titles")))
        tokens = _tokens(out)
        self.assertEqual((tokens.get("pages"), tokens.get("titles"), tokens.get("sweep_done"), tokens.get("budget_hit")),
                         ("3", "5", "1", "0"))

    def test_a_full_last_page_asks_once_more_and_then_the_sweep_is_done(self):
        titles = [_title(i, photos=[f"/p{i}.jpg"]) for i in range(1, 5)]
        db, cdn = _Db(titles), _cdn_for(titles)
        with mock.patch.object(mod, "PAGE_TITLES", 2):
            _, out = _run(db, cdn)
        self.assertEqual(len(db.calls("tmdb_mirror_titles")), 3)
        self.assertEqual(_tokens(out).get("sweep_done"), "1")

    def test_the_time_budget_ends_the_run_with_its_summary(self):
        titles = [_title(i, photos=[f"/p{i}.jpg"]) for i in range(1, 6)]
        db, cdn = _Db(titles), _cdn_for(titles)
        with mock.patch.object(mod, "PAGE_TITLES", 1):
            _, out = _run(db, cdn, budget_min=1, clock=_Clock(step=20.0))
        tokens = _tokens(out)
        self.assertEqual((tokens.get("budget_hit"), tokens.get("sweep_done")), ("1", "0"))
        self.assertLess(len(db.uploads), 5)
        self.assertTrue(out.rstrip().splitlines()[-1].startswith("[TMDB_MIRROR] SUMMARY "))
        # Positive control: a clock that does not move finishes the sweep.
        db, cdn = _Db(titles), _cdn_for(titles)
        with mock.patch.object(mod, "PAGE_TITLES", 1):
            _, out = _run(db, cdn, budget_min=1, clock=_Clock(step=0.0))
        self.assertEqual((_tokens(out).get("budget_hit"), _tokens(out).get("sweep_done"), len(db.uploads)),
                         ("0", "1", 5))

    def test_max_files_ends_the_run_and_what_was_copied_is_in_the_ledger(self):
        titles = [_title(1, photos=[f"/p{i:02d}.jpg" for i in range(10)])]
        db, cdn = _Db(titles), _cdn_for(titles)
        _, out = _run(db, cdn, max_files=3)
        self.assertEqual(len(cdn.urls), 3)
        self.assertEqual(len(db.ledger), 3)
        self.assertEqual((_tokens(out).get("budget_hit"), _tokens(out).get("sweep_done")), ("1", "0"))
        # The next run goes on where the ledger says.
        _run(db, cdn, max_files=3)
        self.assertEqual(len(db.ledger), 6)


# ── (8) the fail-fast ─────────────────────────────────────────────────────

class FailFastTest(unittest.TestCase):

    def test_a_missing_rpc_or_table_names_the_migration_and_uploads_nothing(self):
        cases = (("tmdb_mirror_titles", "PGRST202"), ("tmdb_mirror_titles", "42883"),
                 ("tmdb_mirror_missing", "PGRST202"), ("tmdb_mirror_missing", "42P01"))
        for rpc, code in cases:
            with self.subTest(rpc=rpc, code=code):
                titles = [_title(1, photos=["/p1.jpg"])]
                db, cdn = _Db(titles, rpc_errors={rpc: _api_error(code)}), _cdn_for(titles)
                stats, out = _run(db, cdn)
                self.assertIsInstance(stats, SystemExit)
                self.assertEqual(stats.code, 1)
                self.assertIn("[TMDB_MIRROR] ERROR: the server has no public.", out)
                self.assertIn("apply migration tmdb_image_mirror before this code runs.", out)
                self.assertEqual((db.uploads, cdn.urls), ([], []))
        # Positive control: the same page with the migration applied copies.
        titles = [_title(1, photos=["/p1.jpg"])]
        db, cdn = _Db(titles), _cdn_for(titles)
        stats, _ = _run(db, cdn)
        self.assertIsInstance(stats, dict)
        self.assertEqual(len(db.uploads), 1)

    def test_a_missing_ledger_table_at_the_write_names_the_migration(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db, cdn = _Db(titles, upsert_error=_api_error("42P01", 'relation "public.tmdb_image_mirror" does not exist')), \
            _cdn_for(titles)
        stats, out = _run(db, cdn)
        self.assertIsInstance(stats, SystemExit)
        self.assertIn("apply migration tmdb_image_mirror", out)

    def test_another_rpc_failure_is_a_red_run_with_its_summary(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db, cdn = _Db(titles, rpc_errors={"tmdb_mirror_titles": RuntimeError("boom")}), _cdn_for(titles)
        stats, out = _run(db, cdn)
        # run() says it and returns; main() exits 1 after the SUMMARY (MainTest).
        self.assertIsInstance(stats, dict)
        self.assertEqual(stats.get("failed"), "tmdb_mirror_titles")
        self.assertIn("[TMDB_MIRROR] ERROR: tmdb_mirror_titles failed (RuntimeError); stopping with what landed", out)
        self.assertNotIn("boom", out)
        self.assertTrue(_summary(out))

    def test_a_transient_rpc_failure_is_retried(self):
        titles = [_title(1, photos=["/p1.jpg"])]
        db = _Db(titles, rpc_errors={"tmdb_mirror_titles": [_api_error("57014"), _api_error("503")]})
        cdn, sleeps = _cdn_for(titles), []
        stats, out = _run(db, cdn, sleep=sleeps.append)
        self.assertIsInstance(stats, dict)
        self.assertEqual(sleeps, [2, 4])
        self.assertEqual(len(db.uploads), 1)


# ── (9) the SUMMARY ───────────────────────────────────────────────────────

class SummaryTest(unittest.TestCase):

    def test_the_summary_carries_every_field_in_order(self):
        titles = [_title(1, backdrop="/b1.jpg", logo=f"{CDN}w500/l1.png", photos=["/p1.jpg", "/gone.jpg"]),
                  _title(2, logo=f"{CDN}original/l2.png", photos=["/p1.jpg", "/bad.jpg"])]
        db = _Db(titles, ledger=[("w185", "old.jpg", "ok")])
        cdn = _cdn_for(titles, {f"{CDN}w185/gone.jpg": (404, b"", "text/html"),
                                f"{CDN}w185/bad.jpg": (500, b"", "text/html")})
        _, out = _run(db, cdn)
        line = _summary(out)
        self.assertEqual([t.split("=", 1)[0] for t in line.split()[2:]], list(SUMMARY_FIELDS))
        tokens = _tokens(out)
        self.assertEqual({k: tokens[k] for k in ("pages", "titles", "wanted", "already", "copied", "missing", "errors",
                                                 "skipped_size", "w1280", "w500", "w185", "budget_hit", "sweep_done")},
                         {"pages": "1", "titles": "2", "wanted": "5", "already": "0", "copied": "3", "missing": "1",
                          "errors": "1", "skipped_size": "1", "w1280": "1", "w500": "1", "w185": "1",
                          "budget_hit": "0", "sweep_done": "1"})

    def test_avg_kb_and_files_per_s_come_from_what_was_uploaded(self):
        titles = [_title(1, backdrop="/b1.jpg", photos=["/p1.jpg", "/p2.jpg"])]
        db, cdn, clock = _Db(titles), _cdn_for(titles), _Clock(step=1.5)
        _, out = _run(db, cdn, clock=clock)
        self.assertEqual(len(db.uploads), 3)
        self.assertGreaterEqual(len(clock.values), 2)
        uploaded = sum(len(u["bytes"]) for u in db.uploads)
        elapsed = (clock.values[-1] - clock.values[0]).total_seconds()
        tokens = _tokens(out)
        self.assertEqual(tokens.get("bytes_uploaded"), str(uploaded))
        self.assertEqual(tokens.get("avg_kb"), f"{uploaded / 3 / 1024:.1f}")
        self.assertEqual(tokens.get("files_per_s"), f"{3 / elapsed:.1f}")
        # Positive control: nothing copied has neither.
        db, cdn = _Db(titles, ledger=[("w1280", "b1.jpg", "ok"), ("w185", "p1.jpg", "ok"), ("w185", "p2.jpg", "ok")]), \
            _cdn_for(titles)
        _, out = _run(db, cdn)
        self.assertEqual((_tokens(out).get("avg_kb"), _tokens(out).get("files_per_s")), ("-", "-"))


# ── what the review of 0b added before the commit ─────────────────────────

class _Uploader:
    """A per-worker client: records the thread that built it and every
    thread that uploads with it."""

    def __init__(self, db):
        self.db, self.built_in, self.used_in = db, threading.get_ident(), set()

    def upload_image(self, *args, **kwargs):
        self.used_in.add(threading.get_ident())
        return self.db.upload_image(*args, **kwargs)


class ReviewTest(unittest.TestCase):

    def test_each_worker_uploads_with_the_client_its_own_thread_built(self):
        photos = [f"/p{i:02d}.jpg" for i in range(48)]
        titles = [_title(1, photos=photos)]
        db, cdn, made, lock = _Db(titles), _cdn_for(titles), [], threading.Lock()
        slow = cdn.__call__

        def slow_cdn(url, **kwargs):                  # slow enough for the pool to use several threads
            threading.Event().wait(0.005)
            return slow(url, **kwargs)

        def make_uploader():
            uploader = _Uploader(db)
            with lock:
                made.append(uploader)
            return uploader

        _run(db, slow_cdn, workers=4, make_uploader=make_uploader)
        self.assertEqual(len(db.uploads), 48)
        self.assertGreater(len({t for u in made for t in u.used_in}), 1)   # positive control: several threads
        for uploader in made:
            self.assertEqual(uploader.used_in, {uploader.built_in})        # never shared across threads

    def test_the_time_budget_is_checked_inside_a_page_too(self):
        titles = [_title(1, photos=[f"/p{i:02d}.jpg" for i in range(40)])]
        db, cdn = _Db(titles), _cdn_for(titles)
        _, out = _run(db, cdn, workers=1, budget_min=1, clock=_Clock(step=20.0))
        tokens = _tokens(out)
        self.assertLess(len(db.uploads), 40)
        self.assertGreater(len(db.uploads), 0)
        self.assertEqual(len(db.ledger), len(db.uploads))                 # what landed is in the ledger
        self.assertEqual((tokens.get("budget_hit"), tokens.get("sweep_done")), ("1", "0"))

    def test_a_missing_ledger_table_at_the_write_names_the_migration_either_way(self):
        for code in ("42P01", "PGRST205"):
            with self.subTest(code=code):
                titles = [_title(1, photos=["/p1.jpg"])]
                db, cdn = _Db(titles, upsert_error=_api_error(code)), _cdn_for(titles)
                stats, out = _run(db, cdn)
                self.assertIsInstance(stats, SystemExit)
                self.assertIn("apply migration tmdb_image_mirror", out)
                # That page's uploads landed (the RPCs were there): the next run uploads them again.
                self.assertEqual(len(db.uploads), 1)

    def test_avg_kb_and_files_per_s_count_the_copies_only(self):
        titles = [_title(1, backdrop="/b1.jpg", photos=["/p1.jpg", "/gone.jpg", "/bad.jpg"])]
        db, clock = _Db(titles), _Clock(step=2.0)
        cdn = _cdn_for(titles, {f"{CDN}w185/gone.jpg": (404, b"", "text/html"),
                                f"{CDN}w185/bad.jpg": (500, b"", "text/html")})
        _, out = _run(db, cdn, clock=clock)
        tokens = _tokens(out)
        self.assertEqual((tokens.get("copied"), tokens.get("missing"), tokens.get("errors")), ("2", "1", "1"))
        uploaded = len(BACKDROP) + len(PHOTO)
        elapsed = (clock.values[-1] - clock.values[0]).total_seconds()
        self.assertEqual(tokens.get("avg_kb"), f"{uploaded / 2 / 1024:.1f}")
        self.assertEqual(tokens.get("files_per_s"), f"{2 / elapsed:.1f}")

    def test_a_run_an_rpc_stops_early_has_not_done_its_sweep(self):
        titles = [_title(i, photos=[f"/p{i}.jpg"]) for i in range(1, 4)]
        db = _Db(titles, rpc_errors={"tmdb_mirror_titles": [None, RuntimeError("boom")]})
        with mock.patch.object(mod, "PAGE_TITLES", 1):
            stats, out = _run(db, _cdn_for(titles))
        self.assertEqual(stats.get("failed"), "tmdb_mirror_titles")
        self.assertEqual((_tokens(out).get("pages"), _tokens(out).get("sweep_done")), ("1", "0"))

    def test_a_name_the_ledger_cannot_hold_is_never_asked_or_fetched(self):
        long_name = "x" * 117 + ".jpg"                       # 121 characters
        titles = [_title(1, photos=["/a.jp", f"/{long_name}", "/abcde.jpg"])]
        db, cdn = _Db(titles), _cdn_for(titles)
        _, out = _run(db, cdn)
        asked = [f for c in db.calls("tmdb_mirror_missing") for f in c["p_files"]]
        self.assertEqual(asked, ["abcde.jpg"])              # positive control: a storable name is asked
        self.assertEqual(cdn.urls, [f"{CDN}w185/abcde.jpg"])
        self.assertEqual(_tokens(out).get("errors"), "2")
        self.assertEqual(out.count("[TMDB_MIRROR] ERROR size=w185: a file name the ledger cannot hold"), 2)

    def test_a_logo_url_without_a_size_segment_is_skipped(self):
        titles = [_title(1, logo="https://example.invalid/logos/l1.png"), _title(2, logo=f"{CDN}w500/l2.png")]
        db, cdn = _Db(titles), _cdn_for(titles)
        _, out = _run(db, cdn)
        self.assertEqual(_tokens(out).get("skipped_size"), "1")
        self.assertEqual(cdn.urls, [f"{CDN}w500/l2.png"])

    def test_the_client_deduplicates_before_it_asks(self):
        titles = [_title(1, photos=["/p1.jpg", "/p1.jpg", "/p2.jpg"]), _title(2, photos=["/p2.jpg", "/p1.jpg"])]
        db, cdn = _Db(titles), _cdn_for(titles)
        _run(db, cdn)
        sent = [c["p_files"] for c in db.calls("tmdb_mirror_missing")]
        self.assertEqual(sent, [["p1.jpg", "p2.jpg"]])

    def test_failed_uploads_in_a_row_stop_the_run_red(self):
        titles = [_title(1, photos=[f"/p{i:02d}.jpg" for i in range(90)])]
        db, cdn = _Db(titles, fail_uploads=True), _cdn_for(titles)
        stats, out = _run(db, cdn, workers=2)
        self.assertEqual(stats.get("failed"), "files")
        self.assertIn("[TMDB_MIRROR] ERROR: 50 files in a row failed — the CDN or Storage is down", out)
        attempted = int(_tokens(out).get("errors"))
        self.assertGreaterEqual(attempted, 50)
        self.assertLess(attempted, 90)                       # it stopped before the page was spent
        # The house upload_image's own line per failure is bounded by the stop, and names no title.
        self.assertEqual(out.count("[Supabase] Error uploading image to media-images/tmdb/w185/"), attempted)
        # Positive control: errors broken by a copy do not stop it.
        mixed = {f"{CDN}w185/p{i:02d}.jpg": ((200, PHOTO, "image/jpeg") if i % 10 == 0 else (503, b"", "text/html"))
                 for i in range(90)}
        db = _Db(titles)
        stats, out = _run(db, _Cdn(mixed), workers=2)
        self.assertIsNone(stats.get("failed"))
        self.assertEqual(_tokens(out).get("errors"), "81")

    def test_the_file_cap_reached_at_a_page_boundary_asks_no_further_page(self):
        titles = [_title(i, photos=[f"/p{i}.jpg"]) for i in range(1, 4)]
        db, cdn = _Db(titles), _cdn_for(titles)
        with mock.patch.object(mod, "PAGE_TITLES", 1):
            _, out = _run(db, cdn, max_files=1)
        self.assertEqual(len(db.calls("tmdb_mirror_titles")), 1)      # the cap holds before the next page
        self.assertEqual(len(db.calls("tmdb_mirror_missing")), 1)
        self.assertEqual((len(db.uploads), _tokens(out).get("budget_hit")), (1, "1"))
        # Positive control: a cap of 2 reaches the second page.
        db, cdn = _Db(titles), _cdn_for(titles)
        with mock.patch.object(mod, "PAGE_TITLES", 1):
            _run(db, cdn, max_files=2)
        self.assertEqual(len(db.calls("tmdb_mirror_titles")), 2)

    def test_the_page_and_the_chunks_are_the_briefs(self):
        self.assertEqual((mod.PAGE_TITLES, mod.MISSING_CHUNK, mod.LEDGER_CHUNK), (200, 1000, 500))
        self.assertEqual(mod.CONTENT_TYPES, {"image/jpeg", "image/png", "image/svg+xml", "image/webp"})


# ── main: the environment ─────────────────────────────────────────────────

class MainTest(unittest.TestCase):

    def _main(self, env):
        seen = {}

        def fake_run(db, **kwargs):
            seen.update(kwargs, db=db, uploader=kwargs["make_uploader"]())   # built while the patch holds
            return {"failed": None}

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

    def test_main_reads_its_three_knobs_with_the_scripts_defaults(self):
        seen, exited, _ = self._main(FAKE_ENV)
        self.assertIsNone(exited)
        self.assertEqual((seen.get("workers"), seen.get("budget_min"), seen.get("max_files")), (6, 25, 20000))
        self.assertEqual(seen["uploader"], ("db", FAKE_ENV["SUPABASE_URL"], FAKE_ENV["SUPABASE_KEY"]))
        seen, _, _ = self._main({**FAKE_ENV, "TMDB_MIRROR_WORKERS": "3", "TMDB_MIRROR_TIME_BUDGET_MIN": "80",
                                 "TMDB_MIRROR_MAX_FILES": "500"})
        self.assertEqual((seen.get("workers"), seen.get("budget_min"), seen.get("max_files")), (3, 80, 500))

    def test_main_refuses_knobs_that_cannot_work_before_any_work(self):
        for key, value in (("TMDB_MIRROR_TIME_BUDGET_MIN", "81"), ("TMDB_MIRROR_TIME_BUDGET_MIN", "0"),
                           ("TMDB_MIRROR_WORKERS", "0"), ("TMDB_MIRROR_WORKERS", "17"), ("TMDB_MIRROR_MAX_FILES", "0")):
            with self.subTest(key=key, value=value):
                seen, exited, out = self._main({**FAKE_ENV, key: value})
                self.assertIsInstance(exited, SystemExit)
                self.assertEqual(exited.code, 1)
                self.assertIn(f"[TMDB_MIRROR] ERROR: {key}", out)
                self.assertEqual(seen, {})

    def test_main_needs_the_supabase_pair_and_no_tmdb_key(self):
        self.assertNotIn("TMDB_API_KEY", FAKE_ENV)
        seen, exited, _ = self._main(FAKE_ENV)
        self.assertIsNone(exited)
        seen, exited, out = self._main({"SUPABASE_URL": FAKE_ENV["SUPABASE_URL"]})
        self.assertIsInstance(exited, SystemExit)
        self.assertIn("[TMDB_MIRROR] ERROR: Required environment variable 'SUPABASE_KEY' is not set.", out)

    def test_a_failed_run_exits_1_after_its_summary(self):
        def failing_run(db, **kwargs):
            return {"failed": "tmdb_mirror_titles"}

        out = io.StringIO()
        with mock.patch.dict(os.environ, FAKE_ENV, clear=True), \
                mock.patch.object(mod, "SupabaseClient", lambda url, key: ("db", url, key)), \
                mock.patch.object(mod, "run", failing_run), contextlib.redirect_stdout(out):
            with self.assertRaises(SystemExit) as raised:
                mod.main(clock=_Clock())
        self.assertEqual(raised.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
