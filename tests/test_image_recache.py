"""
Unit tests for the re-cache sweep (RUN B5.10 · 0b): bot/image_recache.py walks
media-images by prefix through public.image_cache_page (keyset pages by name),
and every object still on no-cache whose key the policy knows is fetched from
its public URL and uploaded again under the same key, byte for byte, with the
policy's cache. A cursor in bot_state remembers the walk. On the first object
re-uploaded each run, a gate reads the SERVED Cache-Control back (supabase
storage #1109: changing the stored value is not changing what the app gets),
and the stored one, before any worker starts.

RUN B5.10.1 · 0a: what a run hands its workflow — budget_hit, drain_done,
retagged, refused and cursor, appended to $GITHUB_OUTPUT by main(), a red
run's too — so the workflow's relaunch step can chain the next run.

Fakes only, in the shape of tests/test_tmdb_mirror.py: a Supabase client whose
image_cache_page answers from in-memory objects in C order, whose storage
records every upload behind the REAL SupabaseClient.upload_image (and stores
max-age=<seconds> the way the server does), and whose bot_state is a dict; a
CDN that serves those objects with their Content-Type, ETag and the
Cache-Control it currently serves; a clock whose sleep advances it. No
network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import copy
import hashlib
import io
import os
import pathlib
import re
import tempfile
import threading
import types
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from unittest import mock

import httpx
import requests
from PIL import Image
from postgrest.exceptions import APIError
from requests.structures import CaseInsensitiveDict

from bot import image_recache as mod
from bot.supabase_client import SupabaseClient

START = datetime(2026, 11, 22, 16, 53, tzinfo=timezone.utc)
PUBLIC = "https://project.supabase.invalid/storage/v1/object/public/"
BUCKET_URL = PUBLIC + "media-images/"
FAKE_ENV = {"SUPABASE_URL": "https://project.supabase.invalid", "SUPABASE_KEY": "fake-supabase-key"}
YEAR, WEEK = 31536000, 604800
SUMMARY_FIELDS = ("prefixes", "pages", "seen", "already", "retagged", "skipped_shape", "fetch_error", "bad_type",
                  "size_mismatch", "etag_mismatch", "etag_unchecked", "errors", "bytes_down", "bytes_up",
                  "objects_per_s", "budget_hit", "drain_done", "cursor")
SOURCE = pathlib.Path(__file__).resolve().parent.parent / "bot" / "image_recache.py"


def _api_error(code, message="x"):
    return APIError({"code": code, "message": message, "hint": None, "details": None})


def _untouchable(data: bytes) -> bytes:
    """Bytes no encoder writes back (a marker after the image's end), so an
    upload equal to them was not re-encoded."""
    return data + b"--only-storage-wrote-these-bytes--"


def _jpeg(size=(40, 60), color=(30, 90, 160)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, color).save(buf, format="JPEG")
    return _untouchable(buf.getvalue())


JPEG = _jpeg()


def _data(i) -> bytes:
    return JPEG + f"#{i}".encode()


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


# ── the fakes ─────────────────────────────────────────────────────────────

class _Clock:
    """START plus `step` seconds per call; sleep() advances it too. Every
    value it gave and every sleep are kept."""

    def __init__(self, step=0.0):
        self.now, self.step, self.values, self.sleeps = START, step, [], []

    def __call__(self):
        value = self.now
        self.values.append(value)
        self.now += timedelta(seconds=self.step)
        return value

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


class _Call:
    def __init__(self, fn):
        self.fn = fn

    def execute(self):
        return types.SimpleNamespace(data=self.fn())


class _Bucket:
    def __init__(self, db, name):
        self.db, self.name = db, name

    def upload(self, path, file, file_options=None):
        db = self.db
        if self.name != "media-images":
            raise AssertionError(f"an upload to bucket {self.name}")
        with db.lock:
            db.threads.add(("upload", threading.get_ident()))
            db.upload_attempts[path] += 1
            fails = db.fail_uploads.get(path, 0)
            if fails < 0 or db.upload_attempts[path] <= fails or db.fail_every_upload:
                raise RuntimeError("storage refused the upload")
            options = dict(file_options or {})
            db.uploads.append({"bucket": self.name, "path": path, "bytes": file, "options": options})
            db.events.append(("upload", path))
            obj = db.objects[path]
            obj["data"] = file
            seconds = options.get("cache-control")
            if seconds is not None and db.stored_follows:
                obj["cache_control"] = db.stored_value or f"max-age={seconds}"
            if seconds is not None and db.served_follows:
                db.pending_served[path] = [db.served_value or f"max-age={seconds}", db.served_lag]

    def get_public_url(self, path):
        return f"{PUBLIC}{self.name}/{path}?"

    def __getattr__(self, op):                     # remove, move, copy, … — recorded, never done
        if op.startswith("__"):
            raise AttributeError(op)

        def record(*args, **kwargs):
            self.db.storage_ops.append(op)
        return record


class _Table:
    def __init__(self, db, name):
        db.table_ops.append(name)

    def __getattr__(self, op):
        if op.startswith("__"):
            raise AttributeError(op)

        def chain(*args, **kwargs):
            return self
        return chain


class _Db:
    """The house SupabaseClient as the sweep uses it: .client.rpc(...),
    get_bot_state / set_bot_state, get_public_url, and upload_image — the
    REAL one, over a fake storage."""

    def __init__(self, objects, *, cursor=None, served_follows=True, served_lag=0, stored_follows=True,
                 fail_uploads=None, fail_every_upload=False, rpc_errors=None, state_read_errors=None,
                 state_write_errors=None, sizes=None, served_value=None, stored_value=None):
        self.objects = {name: dict(o) for name, o in objects.items()}
        self.served_value, self.stored_value = served_value, stored_value
        self.served = {name: o["cache_control"] for name, o in self.objects.items()}
        self.pending_served = {}
        self.served_follows, self.served_lag, self.stored_follows = served_follows, served_lag, stored_follows
        self.fail_uploads, self.fail_every_upload = dict(fail_uploads or {}), fail_every_upload
        self.rpc_errors = list(rpc_errors or [])
        self.state_read_errors, self.state_write_errors = list(state_read_errors or []), list(state_write_errors or [])
        self.sizes = dict(sizes or {})
        self.state = {} if cursor is None else {mod.CURSOR_KEY: copy.deepcopy(cursor)}
        self.state_writes, self.rpc_calls, self.uploads, self.events = [], [], [], []
        self.table_ops, self.storage_ops, self.threads = [], [], set()
        self.upload_attempts, self.lock = Counter(), threading.Lock()
        self.client = self
        self.storage = types.SimpleNamespace(from_=lambda name: _Bucket(self, name))

    # PostgREST
    def rpc(self, name, params):
        def call():
            self.threads.add(("rpc", threading.get_ident()))
            self.rpc_calls.append((name, dict(params)))
            self.events.append(("rpc", dict(params)))
            if self.rpc_errors:
                error = self.rpc_errors.pop(0)     # None: this call answers
                if error is not None:
                    raise error
            if name != "image_cache_page":
                raise AssertionError(f"unknown rpc {name}")
            prefix, after, limit = params["p_prefix"], params.get("p_after", ""), params.get("p_limit", 1000)
            if not re.fullmatch(r"[A-Za-z0-9-]+/", prefix or ""):
                return []
            names = sorted(n for n in self.objects if n.startswith(prefix) and n > (after or ""))
            return [{"name": n, "bytes": self.sizes.get(n, len(self.objects[n]["data"])),
                     "mimetype": self.objects[n]["mimetype"], "cache_control": self.objects[n]["cache_control"]}
                    for n in names[:min(max(limit, 1), 1000)]]
        return _Call(call)

    def table(self, name):
        return _Table(self, name)

    # bot_state, as the house's get_bot_state / set_bot_state
    def get_bot_state(self, key):
        self.threads.add(("state", threading.get_ident()))
        if self.state_read_errors:
            error = self.state_read_errors.pop(0)
            if error is not None:
                raise error
        return copy.deepcopy(self.state.get(key))

    def set_bot_state(self, key, value):
        self.threads.add(("state", threading.get_ident()))
        if self.state_write_errors:
            error = self.state_write_errors.pop(0)
            if error is not None:
                raise error
        self.state[key] = copy.deepcopy(value)
        self.state_writes.append((key, copy.deepcopy(value)))
        self.events.append(("cursor", copy.deepcopy(value)))

    # Storage, through the real client methods
    def upload_image(self, bucket, path, image_bytes, content_type="image/jpeg", **kwargs):
        return SupabaseClient.upload_image(self, bucket, path, image_bytes, content_type, **kwargs)

    def get_public_url(self, bucket, path):
        return SupabaseClient.get_public_url(self, bucket, path)

    # what the CDN serves now
    def serve(self, name):
        with self.lock:
            pending = self.pending_served.get(name)
            if pending is not None:
                if pending[1] > 0:
                    pending[1] -= 1
                else:
                    self.served[name] = pending[0]
                    del self.pending_served[name]
            return self.served[name]

    def page_calls(self):
        return [p for n, p in self.rpc_calls if p.get("p_limit") != 1]

    def gate_calls(self):
        return [p for n, p in self.rpc_calls if p.get("p_limit") == 1]

    def cursors(self):
        return [v for k, v in self.state_writes if k == mod.CURSOR_KEY]


class _Cdn:
    """Stands in for requests.get on our public URLs: serves each object with
    its mimetype, the quoted MD5 of its bytes as ETag and the Cache-Control
    the CDN serves now; *overrides* per name (status, body, ctype, etag, or an
    exception). Records every name asked."""

    def __init__(self, db, overrides=None, after_upload=None):
        self.db, self.overrides, self.names, self.lock = db, dict(overrides or {}), [], threading.Lock()
        self.after_upload, self.timeouts = dict(after_upload or {}), []

    def __call__(self, url, timeout=None, **kwargs):
        assert url.startswith(BUCKET_URL), url
        name = url[len(BUCKET_URL):].rstrip("?")
        with self.lock:
            self.names.append(name)
            self.timeouts.append(timeout)
            self.db.events.append(("fetch", name))
        if name in self.after_upload and any(u["path"] == name for u in self.db.uploads):
            raise self.after_upload[name]                  # e.g. the gate's reads, after the upload
        override = self.overrides.get(name) or {}
        if isinstance(override, BaseException):
            raise override
        obj = self.db.objects[name]
        body = override.get("body", obj["data"])
        headers = {"Content-Type": override.get("ctype", obj["mimetype"]),
                   "ETag": override.get("etag", f'"{_md5(obj["data"])}"'),
                   "Cache-Control": self.db.serve(name)}
        return types.SimpleNamespace(status_code=override.get("status", 200), content=body,
                                     headers=CaseInsensitiveDict({k: v for k, v in headers.items() if v is not None}))

    def count(self, name):
        return self.names.count(name)


def _objects(names, cache_control="no-cache", mimetype="image/jpeg"):
    return {name: {"data": _data(i), "mimetype": mimetype, "cache_control": cache_control}
            for i, name in enumerate(names)}


def _run(db, cdn=None, *, prefixes=("posters/",), workers=2, budget_min=25, max_objects=40000, restart=False,
         clock=None, make_uploader=None):
    clock = clock or _Clock()
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            stats = mod.run(db, make_uploader=make_uploader or (lambda: db), fetch=cdn or _Cdn(db), clock=clock,
                            sleep=clock.sleep, prefixes=prefixes, workers=workers, budget_min=budget_min,
                            max_objects=max_objects, restart=restart)
        except BaseException as exc:   # SystemExit included: a refusal is an exit
            stats = exc
    return (stats if isinstance(stats, (dict, BaseException)) else {}), out.getvalue(), clock


def _summary(out):
    lines = [l for l in out.splitlines() if l.startswith("[IMAGE_RECACHE] SUMMARY ")]
    return lines[-1] if lines else ""


def _tokens(out):
    return dict(t.split("=", 1) for t in _summary(out).split()[2:] if "=" in t)


def _failed(stats):
    return stats.get("failed") if isinstance(stats, dict) else repr(stats)


def _gate_lines(out):
    return [l for l in out.splitlines() if l.startswith("[IMAGE_RECACHE] GATE ")]


POSTERS = [f"posters/{i}.jpg" for i in range(1, 8)]        # C order: 1 … 7


# ── (1) the walk ──────────────────────────────────────────────────────────

class WalkTest(unittest.TestCase):

    def test_pages_go_keyset_by_the_last_name_and_a_short_page_ends_the_prefix(self):
        db = _Db(_objects(POSTERS + ["seasons/1/1.jpg", "seasons/1/2.jpg"]))
        with mock.patch.object(mod, "PAGE_LIMIT", 3):
            stats, out, _ = _run(db, prefixes=("posters/", "seasons/"))
        self.assertIsNone(_failed(stats))
        self.assertEqual([(p["p_prefix"], p["p_after"], p["p_limit"]) for p in db.page_calls()],
                         [("posters/", "", 3), ("posters/", "posters/3.jpg", 3), ("posters/", "posters/6.jpg", 3),
                          ("seasons/", "", 3)])
        self.assertEqual(sorted(u["path"] for u in db.uploads), sorted(POSTERS + ["seasons/1/1.jpg", "seasons/1/2.jpg"]))

    def test_the_cursor_is_written_after_each_page_with_its_last_name_and_done_at_the_end(self):
        db = _Db(_objects(POSTERS + ["seasons/1/1.jpg", "seasons/1/2.jpg"]))
        with mock.patch.object(mod, "PAGE_LIMIT", 3):
            _, out, _ = _run(db, prefixes=("posters/", "seasons/"))
        cursors = db.cursors()
        self.assertEqual([(c.get("prefix"), c.get("after")) for c in cursors],
                         [("posters/", "posters/3.jpg"), ("posters/", "posters/6.jpg"), ("posters/", "posters/7.jpg"),
                          ("seasons/", "seasons/1/2.jpg")])
        self.assertEqual([c.get("done_at") for c in cursors[:-1]], [None, None, None])
        done_at = cursors[-1].get("done_at") if cursors else None
        self.assertIsInstance(done_at, str)
        self.assertEqual(datetime.fromisoformat(done_at).tzinfo, timezone.utc)
        self.assertEqual(set(cursors[-1]) if cursors else set(), {"prefix", "after", "done_at"})
        tokens = _tokens(out)
        self.assertEqual((tokens.get("drain_done"), tokens.get("cursor")), ("1", "seasons/:seasons/1/2.jpg"))

    def test_each_page_is_written_after_its_uploads(self):
        db = _Db(_objects(POSTERS))
        with mock.patch.object(mod, "PAGE_LIMIT", 3):
            _run(db)
        kinds = [(e[0], e[1].get("after") if e[0] == "cursor" else e[1]) for e in db.events if e[0] in ("upload", "cursor")]
        # Every upload of a page comes before that page's cursor, and after the previous one.
        self.assertEqual([k for k in kinds if k[0] == "cursor"],
                         [("cursor", "posters/3.jpg"), ("cursor", "posters/6.jpg"), ("cursor", "posters/7.jpg")])
        for name in POSTERS:
            with self.subTest(name=name):
                up = kinds.index(("upload", name))
                written = next(i for i, k in enumerate(kinds) if k[0] == "cursor" and k[1] >= name)
                self.assertLess(up, written)

    def test_the_rpc_is_called_by_name_with_named_parameters_and_pages_of_1000(self):
        db = _Db(_objects(POSTERS))
        _run(db)
        self.assertEqual({n for n, _ in db.rpc_calls}, {"image_cache_page"})
        self.assertTrue(db.rpc_calls)
        for _, params in db.rpc_calls:
            self.assertEqual(set(params), {"p_prefix", "p_after", "p_limit"})
        self.assertEqual(db.page_calls()[:1], [{"p_prefix": "posters/", "p_after": "", "p_limit": 1000}])
        self.assertEqual((mod.PAGE_LIMIT, mod.PAGE_RPC, mod.BUCKET, mod.CURSOR_KEY),
                         (1000, "image_cache_page", "media-images", "image_recache_cursor"))

    def test_the_prefixes_go_in_the_order_given(self):
        db = _Db(_objects(["carousels/9/0.jpg", "posters/1.jpg", "seasons/1/1.jpg"]))
        _run(db, prefixes=("seasons/", "carousels/", "posters/"))
        self.assertEqual([p["p_prefix"] for p in db.page_calls()], ["seasons/", "carousels/", "posters/"])
        self.assertEqual(mod.DEFAULT_IMAGE_RECACHE_PREFIXES, "posters/,seasons/,carousels/")


# ── (2) which rows are fetched ────────────────────────────────────────────

class RowTest(unittest.TestCase):

    def test_a_row_already_on_a_max_age_is_neither_fetched_nor_uploaded(self):
        objects = _objects(["posters/1.jpg"], cache_control="max-age=604800")
        objects.update(_objects(["posters/2.jpg"], cache_control="max-age=31536000"))
        objects.update(_objects(["posters/3.jpg"]))
        db = _Db(objects)
        cdn = _Cdn(db)
        _, out, _ = _run(db, cdn)
        self.assertEqual((cdn.count("posters/1.jpg"), cdn.count("posters/2.jpg")), (0, 0))
        tokens = _tokens(out)
        self.assertEqual((tokens.get("seen"), tokens.get("already"), tokens.get("retagged")), ("3", "2", "1"))
        # Positive control: the no-cache row is fetched and uploaded.
        self.assertGreaterEqual(cdn.count("posters/3.jpg"), 1)
        self.assertEqual([u["path"] for u in db.uploads], ["posters/3.jpg"])

    def test_a_shape_the_policy_does_not_know_is_skipped_and_counted(self):
        db = _Db(_objects(["posters/1.jpg", "posters/abc.jpg", "posters/readme.txt"]))
        cdn = _Cdn(db)
        _, out, _ = _run(db, cdn)
        self.assertEqual((cdn.count("posters/abc.jpg"), cdn.count("posters/readme.txt")), (0, 0))
        self.assertEqual(_tokens(out).get("skipped_shape"), "2")
        self.assertEqual([u["path"] for u in db.uploads], ["posters/1.jpg"])        # positive control


# ── (3) the body guards ───────────────────────────────────────────────────

class GuardTest(unittest.TestCase):

    def _one(self, override, name="posters/1.jpg", **db_kwargs):
        db = _Db(_objects([name]), **db_kwargs)
        cdn = _Cdn(db, {name: override})
        stats, out, _ = _run(db, cdn)
        return db, cdn, stats, out

    def test_a_status_other_than_200_is_a_fetch_error_and_no_upload(self):
        for status in (500, 404, 304, 206):
            with self.subTest(status=status):
                db, _, stats, out = self._one({"status": status})
                self.assertEqual((db.uploads, _tokens(out).get("fetch_error")), ([], "1"))
                self.assertIn(f"[IMAGE_RECACHE] ERROR posters/1.jpg: HTTP {status}", out)

    def test_a_fetch_that_raises_is_a_fetch_error_and_no_upload(self):
        db, _, _, out = self._one(requests.ConnectionError("down"))
        self.assertEqual((db.uploads, _tokens(out).get("fetch_error")), ([], "1"))
        self.assertIn("[IMAGE_RECACHE] ERROR posters/1.jpg: ConnectionError", out)

    def test_a_content_type_other_than_the_rows_is_refused(self):
        db, _, _, out = self._one({"ctype": "text/html", "body": b"<html>error</html>"})
        self.assertEqual((db.uploads, _tokens(out).get("bad_type")), ([], "1"))
        # Positive control: the row's own type, with a parameter, passes.
        db, _, _, out = self._one({"ctype": "image/jpeg; charset=binary"})
        self.assertEqual((len(db.uploads), _tokens(out).get("bad_type")), (1, "0"))
        self.assertEqual(db.uploads[0]["options"].get("content-type") if db.uploads else None, "image/jpeg")

    def test_a_body_of_another_length_is_refused(self):
        name = "posters/1.jpg"
        db, _, _, out = self._one({"body": _data(0) + b"x"})
        self.assertEqual((db.uploads, _tokens(out).get("size_mismatch")), ([], "1"))
        db, _, _, out = self._one({}, sizes={name: len(_data(0)) - 1})            # the row says another size
        self.assertEqual((db.uploads, _tokens(out).get("size_mismatch")), ([], "1"))
        # A truncated body, whose multipart ETag cannot catch it.
        db, _, _, out = self._one({"body": _data(0)[:-1], "etag": '"' + "a" * 32 + '-3"'})
        self.assertEqual((db.uploads, _tokens(out).get("size_mismatch")), ([], "1"))

    def test_a_body_whose_md5_is_not_its_etag_is_refused(self):
        db, _, _, out = self._one({"etag": '"' + "0" * 32 + '"'})
        self.assertEqual((db.uploads, _tokens(out).get("etag_mismatch")), ([], "1"))
        # Positive control: the right MD5, quoted or not, upper case or not, passes.
        for etag in (f'"{_md5(_data(0))}"', _md5(_data(0)), f'"{_md5(_data(0)).upper()}"'):
            with self.subTest(etag=etag):
                db, _, _, out = self._one({"etag": etag})
                self.assertEqual((len(db.uploads), _tokens(out).get("etag_mismatch")), (1, "0"))

    def test_a_multipart_etag_is_unchecked_and_the_upload_proceeds(self):
        db, _, _, out = self._one({"etag": '"' + "a" * 32 + '-3"'})
        tokens = _tokens(out)
        self.assertEqual((tokens.get("etag_unchecked"), tokens.get("retagged"), tokens.get("etag_mismatch")),
                         ("1", "1", "0"))
        self.assertEqual([u["bytes"] for u in db.uploads], [_data(0)])

    def test_an_etag_of_no_known_shape_is_no_proof_and_no_upload(self):
        for etag in (None, 'W/"' + _md5(_data(0)) + '"', '"not-an-md5"', '"' + "a" * 31 + '"',
                     '"' + "z" * 32 + '-3"', '"' + "a" * 31 + '-3"', '"' + "a" * 32 + '-"'):
            with self.subTest(etag=etag):
                db, _, _, out = self._one({"etag": etag})
                self.assertEqual((db.uploads, _tokens(out).get("etag_mismatch")), ([], "1"))

    def test_all_right_is_one_upload_of_the_same_bytes_with_the_policys_seconds(self):
        for name, seconds in (("posters/603.jpg", WEEK), ("posters/603/kvuN0aKiDPXSfmTg5ICh.jpg", YEAR),
                              ("seasons/1399/1.jpg", WEEK), ("carousels/603/0.jpg", WEEK)):
            with self.subTest(name=name):
                db = _Db(_objects([name]))
                _, out, _ = _run(db, prefixes=(name.split("/", 1)[0] + "/",))
                self.assertEqual(len(db.uploads), 1)
                upload = db.uploads[0] if db.uploads else {}
                self.assertEqual((upload.get("bucket"), upload.get("path"), upload.get("bytes")),
                                 ("media-images", name, _data(0)))
                self.assertEqual(upload.get("options"), {"content-type": "image/jpeg", "upsert": "true",
                                                         "cache-control": str(seconds)})
                self.assertEqual(db.objects[name]["cache_control"], f"max-age={seconds}")

    def test_the_bytes_are_never_re_encoded(self):
        db = _Db(_objects(["posters/1.jpg"]))
        _run(db)
        uploaded = db.uploads[0]["bytes"] if db.uploads else b""
        self.assertEqual(uploaded, _data(0))
        # Positive control: a re-encoded body would differ.
        buf = io.BytesIO()
        Image.open(io.BytesIO(_data(0))).save(buf, format="JPEG")
        self.assertNotEqual(buf.getvalue(), _data(0))

    def test_another_image_type_goes_up_with_its_own_type(self):
        db = _Db(_objects(["posters/1.jpg"], mimetype="image/png"))
        _run(db)
        self.assertEqual([u["options"].get("content-type") for u in db.uploads], ["image/png"])


# ── (4) the gate ──────────────────────────────────────────────────────────

class GateTest(unittest.TestCase):

    def test_a_served_header_that_never_changes_stops_the_run_after_one_object(self):
        db = _Db(_objects(POSTERS[:3]), served_follows=False)
        cdn = _Cdn(db)
        stats, out, clock = _run(db, cdn)
        self.assertEqual(_failed(stats), "gate")
        self.assertEqual([u["path"] for u in db.uploads], ["posters/1.jpg"])
        self.assertEqual(_gate_lines(out), ["[IMAGE_RECACHE] GATE served=no-cache stored=max-age=604800 for posters/1.jpg"])
        # Read every 5 s for 90 s: the fetch of the body, then 19 reads (0, 5, … 90).
        self.assertEqual(cdn.count("posters/1.jpg"), 1 + 19)
        self.assertEqual(clock.sleeps, [mod.GATE_POLL_SECONDS] * 18)
        self.assertEqual((mod.GATE_POLL_SECONDS, mod.GATE_WINDOW_SECONDS), (5, 90))
        # The SUMMARY comes after the GATE line, and no page's cursor is written.
        self.assertTrue(_summary(out))
        self.assertLess(out.index("[IMAGE_RECACHE] GATE "), out.index("[IMAGE_RECACHE] SUMMARY "))
        self.assertEqual(db.cursors(), [])

    def test_a_stored_value_that_did_not_change_stops_it_too(self):
        db = _Db(_objects(POSTERS[:3]), stored_follows=False)
        stats, out, clock = _run(db)
        self.assertEqual(_failed(stats), "gate")
        self.assertEqual([u["path"] for u in db.uploads], ["posters/1.jpg"])
        self.assertEqual(_gate_lines(out), ["[IMAGE_RECACHE] GATE served=max-age=604800 stored=no-cache for posters/1.jpg"])
        self.assertTrue(_summary(out))
        self.assertEqual(db.cursors(), [])

    def test_both_right_and_the_run_goes_on(self):
        db = _Db(_objects(POSTERS[:3]))
        stats, out, clock = _run(db)
        self.assertIsNone(_failed(stats))
        self.assertEqual(sorted(u["path"] for u in db.uploads), POSTERS[:3])
        self.assertEqual(_gate_lines(out),
                         ["[IMAGE_RECACHE] GATE served=max-age=604800 stored=max-age=604800 for posters/1.jpg"])
        self.assertEqual(clock.sleeps, [])

    def test_a_served_header_that_lags_passes_when_it_arrives(self):
        db = _Db(_objects(POSTERS[:3]), served_lag=3)
        stats, out, clock = _run(db)
        self.assertIsNone(_failed(stats))
        self.assertEqual(clock.sleeps, [5, 5, 5])
        self.assertEqual(len(db.uploads), 3)

    def test_the_gate_runs_once_a_run(self):
        db = _Db(_objects(POSTERS))
        cdn = _Cdn(db)
        with mock.patch.object(mod, "PAGE_LIMIT", 2):
            _, out, _ = _run(db, cdn)
        self.assertEqual(len(_gate_lines(out)), 1)
        self.assertEqual(len(db.gate_calls()), 1)
        self.assertEqual(cdn.count("posters/1.jpg"), 2)                          # its body, then one read
        self.assertEqual([cdn.count(n) for n in POSTERS[1:]], [1] * 6)            # positive control

    def test_the_stored_read_starts_just_before_the_object(self):
        cases = (({"posters/1.jpg": "no-cache"}, 1000, ""),                          # first of its prefix
                 ({"posters/1.jpg": "max-age=604800", "posters/2.jpg": "no-cache"}, 1000, "posters/1.jpg"),
                 ({"posters/1.jpg": "max-age=604800", "posters/2.jpg": "max-age=604800",
                   "posters/3.jpg": "no-cache"}, 2, "posters/2.jpg"))                # first of a later page
        for states, limit, after in cases:
            with self.subTest(after=after):
                objects = {n: {"data": _data(i), "mimetype": "image/jpeg", "cache_control": c}
                           for i, (n, c) in enumerate(states.items())}
                db = _Db(objects)
                with mock.patch.object(mod, "PAGE_LIMIT", limit):
                    _run(db)
                self.assertEqual(db.gate_calls(), [{"p_prefix": "posters/", "p_after": after, "p_limit": 1}])

    def test_no_worker_starts_before_the_gate_passes(self):
        db = _Db(_objects(POSTERS))
        _run(db, workers=4)
        uploads = [i for i, e in enumerate(db.events) if e[0] == "upload"]
        gate = [i for i, e in enumerate(db.events) if e[0] == "rpc" and e[1].get("p_limit") == 1]
        self.assertEqual(len(gate), 1)
        self.assertEqual(len(uploads), 7)
        self.assertLess(uploads[0], gate[0])                                     # the gate object first
        self.assertTrue(all(i > gate[0] for i in uploads[1:]))                   # every other one after it

    def test_no_object_to_upload_means_no_gate(self):
        db = _Db(_objects(POSTERS, cache_control="max-age=604800"))
        stats, out, _ = _run(db)
        self.assertIsNone(_failed(stats))
        self.assertEqual((_gate_lines(out), db.gate_calls()), ([], []))


# ── (5) failures ──────────────────────────────────────────────────────────

class ErrorTest(unittest.TestCase):

    def test_a_failed_upload_is_retried_once_at_the_end_of_the_run(self):
        db = _Db(_objects(POSTERS[:4]), fail_uploads={"posters/2.jpg": 1})
        stats, out, _ = _run(db, workers=1)
        self.assertIsNone(_failed(stats))
        self.assertEqual(db.upload_attempts["posters/2.jpg"], 2)
        self.assertEqual([e[1] for e in db.events if e[0] == "upload"],
                         ["posters/1.jpg", "posters/3.jpg", "posters/4.jpg", "posters/2.jpg"])
        tokens = _tokens(out)
        self.assertEqual((tokens.get("retagged"), tokens.get("errors")), ("4", "0"))

    def test_still_failing_is_counted_named_and_left_and_the_cursor_moves_on(self):
        db = _Db(_objects(POSTERS[:4]), fail_uploads={"posters/2.jpg": -1})
        stats, out, _ = _run(db, workers=1)
        self.assertIsNone(_failed(stats))                                        # exit 0: a plan, not a red run
        self.assertEqual(db.upload_attempts["posters/2.jpg"], 2)
        tokens = _tokens(out)
        self.assertEqual((tokens.get("retagged"), tokens.get("errors")), ("3", "1"))
        self.assertIn("[IMAGE_RECACHE]   posters/2.jpg", out)
        self.assertEqual(db.objects["posters/2.jpg"]["cache_control"], "no-cache")
        # The page's cursor waits just before it until its retry has run, then moves on past it.
        self.assertEqual([(c.get("after"), c.get("done_at") is None) for c in db.cursors()],
                         [("posters/1.jpg", True), ("posters/4.jpg", False)])

    def test_a_fetch_error_or_a_guard_is_not_retried(self):
        db = _Db(_objects(POSTERS[:3]))
        cdn = _Cdn(db, {"posters/2.jpg": {"status": 503}, "posters/3.jpg": {"ctype": "text/html"}})
        _run(db, cdn)
        self.assertEqual((cdn.count("posters/2.jpg"), cdn.count("posters/3.jpg")), (1, 1))

    def test_fifty_failures_in_a_row_stop_the_run_red_after_the_summary(self):
        names = [f"posters/{i:02d}.jpg" for i in range(90)]
        db = _Db(_objects(names), fail_every_upload=True)
        stats, out, _ = _run(db, workers=2)
        self.assertEqual(_failed(stats), "files")
        self.assertIn(f"[IMAGE_RECACHE] ERROR: {mod.ERRORS_IN_A_ROW_MAX} objects in a row failed", out)
        self.assertEqual(sum(db.upload_attempts.values()), 50)                    # stopped, and no retry round
        self.assertTrue(_summary(out))
        self.assertEqual(db.cursors(), [])                                       # the page is asked again next run
        # Positive control: failures broken by a success do not stop it (each fails once, then lands).
        db = _Db(_objects(names), fail_uploads={n: 1 for i, n in enumerate(names) if i % 10})
        stats, out, _ = _run(db, workers=2)
        self.assertIsNone(_failed(stats))
        self.assertEqual((_tokens(out).get("errors"), _tokens(out).get("retagged")), ("0", "90"))

    def test_a_page_read_that_keeps_failing_stops_red_with_what_landed(self):
        db = _Db(_objects(POSTERS), rpc_errors=[None, None, RuntimeError("boom")])   # page 1, gate, page 2
        with mock.patch.object(mod, "PAGE_LIMIT", 3):
            stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "image_cache_page")
        self.assertEqual([c.get("after") for c in db.cursors()], ["posters/3.jpg"])
        self.assertIn("[IMAGE_RECACHE] ERROR: image_cache_page failed (RuntimeError)", out)
        self.assertTrue(_summary(out))

    def test_a_transient_failure_is_retried_two_then_four_seconds_apart(self):
        db = _Db(_objects(POSTERS[:2]), rpc_errors=[httpx.ConnectTimeout("t"), _api_error("57014")],
                 state_write_errors=[_api_error("503")])
        stats, _, clock = _run(db)
        self.assertIsNone(_failed(stats))
        self.assertEqual(clock.sleeps, [2, 4, 2])
        self.assertEqual(len(db.uploads), 2)

    def test_a_missing_function_names_it(self):
        db = _Db(_objects(POSTERS[:2]), rpc_errors=[_api_error("PGRST202")])
        stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "image_cache_page")
        self.assertIn("public.image_cache_page", out)
        self.assertEqual(db.uploads, [])

    def test_a_cursor_write_that_keeps_failing_stops_red(self):
        db = _Db(_objects(POSTERS[:2]), state_write_errors=[RuntimeError("boom")])
        stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "bot_state")
        self.assertTrue(_summary(out))


# ── (6) the cursor ────────────────────────────────────────────────────────

class CursorTest(unittest.TestCase):
    DONE = {"prefix": "carousels/", "after": "carousels/9/7.jpg", "done_at": "2026-12-01T00:00:00+00:00"}

    def test_a_done_cursor_reads_no_page_and_ends_in_drain_done(self):
        db = _Db(_objects(POSTERS), cursor=self.DONE)
        stats, out, _ = _run(db, prefixes=("posters/", "seasons/", "carousels/"))
        self.assertIsNone(_failed(stats))
        self.assertEqual((db.rpc_calls, db.state_writes, db.uploads), ([], [], []))
        tokens = _tokens(out)
        self.assertEqual((tokens.get("drain_done"), tokens.get("pages"), tokens.get("cursor")),
                         ("1", "0", "carousels/:carousels/9/7.jpg"))

    def test_restart_resets_the_cursor_to_the_first_prefix(self):
        db = _Db(_objects(POSTERS), cursor=self.DONE)
        stats, out, _ = _run(db, prefixes=("posters/",), restart=True)
        self.assertIsNone(_failed(stats))
        self.assertEqual(db.cursors()[:1], [{"prefix": "posters/", "after": "", "done_at": None}])
        self.assertEqual(db.page_calls()[:1], [{"p_prefix": "posters/", "p_after": "", "p_limit": 1000}])
        self.assertEqual(len(db.uploads), 7)
        self.assertIsNotNone(db.cursors()[-1].get("done_at") if db.cursors() else None)

    def test_a_stored_cursor_resumes_at_its_prefix_and_name(self):
        db = _Db(_objects(POSTERS + ["seasons/1/1.jpg", "seasons/1/2.jpg"]),
                 cursor={"prefix": "seasons/", "after": "seasons/1/1.jpg", "done_at": None})
        _run(db, prefixes=("posters/", "seasons/"))
        self.assertEqual(db.page_calls()[:1], [{"p_prefix": "seasons/", "p_after": "seasons/1/1.jpg", "p_limit": 1000}])
        self.assertEqual([u["path"] for u in db.uploads], ["seasons/1/2.jpg"])

    def test_a_stored_prefix_absent_from_the_list_starts_at_the_first_and_says_so(self):
        db = _Db(_objects(POSTERS), cursor={"prefix": "tmdb/", "after": "tmdb/w185/x.jpg", "done_at": None})
        _, out, _ = _run(db, prefixes=("posters/",))
        self.assertIn("[IMAGE_RECACHE] the cursor's prefix tmdb/ is not in this run's list — starting at posters/", out)
        self.assertEqual(db.page_calls()[:1], [{"p_prefix": "posters/", "p_after": "", "p_limit": 1000}])

    def test_a_cursor_this_script_did_not_write_is_refused_unless_restart(self):
        for stored in ("garbage", {"prefix": 1, "after": "", "done_at": None}, {"prefix": "posters/"},
                       {"prefix": "posters/", "after": None, "done_at": None}, ["posters/", ""]):
            with self.subTest(stored=stored):
                db = _Db(_objects(POSTERS), cursor=stored)
                stats, out, _ = _run(db)
                self.assertEqual(_failed(stats), "cursor")
                self.assertEqual((db.rpc_calls, db.state_writes), ([], []))
                # Positive control: restart replaces it.
                db = _Db(_objects(POSTERS), cursor=stored)
                stats, _, _ = _run(db, restart=True)
                self.assertIsNone(_failed(stats))

    def test_a_cursor_read_that_fails_never_resets_it(self):
        db = _Db(_objects(POSTERS), cursor={"prefix": "posters/", "after": "posters/5.jpg", "done_at": None},
                 state_read_errors=[RuntimeError("boom")])
        stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "bot_state")
        self.assertEqual((db.rpc_calls, db.state_writes), ([], []))
        self.assertEqual(db.state[mod.CURSOR_KEY]["after"], "posters/5.jpg")
        # Positive control: a transient read failure is retried, and the run resumes.
        db = _Db(_objects(POSTERS), cursor={"prefix": "posters/", "after": "posters/5.jpg", "done_at": None},
                 state_read_errors=[httpx.ConnectTimeout("t")])
        stats, _, _ = _run(db)
        self.assertIsNone(_failed(stats))
        self.assertEqual(db.page_calls()[:1], [{"p_prefix": "posters/", "p_after": "posters/5.jpg", "p_limit": 1000}])

    def test_a_prefix_the_policy_does_not_know_is_refused_before_the_first_rpc(self):
        for prefixes in (("zzz/",), ("posters/", "zzz/"), ("posters",), ("posters/", "posters/"), ()):
            with self.subTest(prefixes=prefixes):
                db = _Db(_objects(POSTERS))
                stats, _, _ = _run(db, prefixes=prefixes)
                self.assertEqual(_failed(stats), "prefixes")
                self.assertEqual((db.rpc_calls, db.state_writes), ([], []))
        # Positive control: the three known prefixes are accepted.
        db = _Db(_objects(POSTERS))
        stats, _, _ = _run(db, prefixes=("posters/", "seasons/", "carousels/"))
        self.assertIsNone(_failed(stats))

    def test_parse_prefixes(self):
        self.assertEqual(mod.parse_prefixes("posters/,seasons/,carousels/"), ("posters/", "seasons/", "carousels/"))
        self.assertEqual(mod.parse_prefixes(" posters/ , seasons/ "), ("posters/", "seasons/"))
        for raw in ("zzz/", "posters", "", "posters/,", "posters/,posters/", "posters/;seasons/", "Posters/"):
            with self.subTest(raw=raw):
                self.assertIsNone(mod.parse_prefixes(raw))


# ── (7) the budget ────────────────────────────────────────────────────────

class BudgetTest(unittest.TestCase):

    def test_the_time_budget_ends_the_run_with_the_cursor_written_and_the_next_run_resumes(self):
        names = [f"posters/{i:02d}.jpg" for i in range(30)]
        db = _Db(_objects(names))
        stats, out, _ = _run(db, workers=1, budget_min=1, clock=_Clock(step=10.0))
        self.assertIsNone(_failed(stats))
        tokens = _tokens(out)
        self.assertEqual((tokens.get("budget_hit"), tokens.get("drain_done")), ("1", "0"))
        first = [u["path"] for u in db.uploads]
        self.assertTrue(0 < len(first) < 30, len(first))
        self.assertEqual([c.get("after") for c in db.cursors()][-1:], [max(first)] if first else [])
        # The next run starts where the cursor says, and uploads nothing twice.
        stats, out, _ = _run(db, workers=1)
        self.assertEqual(sorted(u["path"] for u in db.uploads), names)
        self.assertEqual(_tokens(out).get("drain_done"), "1")

    def test_max_objects_ends_the_run_and_counts_what_was_fetched(self):
        names = [f"posters/{i:02d}.jpg" for i in range(10)]
        db = _Db(_objects(names))
        cdn = _Cdn(db, {"posters/01.jpg": {"status": 500}})
        _, out, _ = _run(db, cdn, max_objects=3)
        tokens = _tokens(out)
        self.assertEqual((tokens.get("budget_hit"), tokens.get("retagged"), tokens.get("fetch_error")), ("1", "2", "1"))
        self.assertEqual([c.get("after") for c in db.cursors()], ["posters/02.jpg"])
        # Positive control: rows already done are free.
        objects = _objects([f"posters/{i:02d}.jpg" for i in range(10)], cache_control="max-age=604800")
        objects.update(_objects(["posters/90.jpg", "posters/91.jpg"]))
        db = _Db(objects)
        _, out, _ = _run(db, max_objects=2)
        tokens = _tokens(out)
        self.assertEqual((tokens.get("budget_hit"), tokens.get("retagged"), tokens.get("drain_done")), ("0", "2", "1"))

    def test_a_run_out_of_budget_reads_no_further_page(self):
        db = _Db(_objects(POSTERS))
        with mock.patch.object(mod, "PAGE_LIMIT", 3):
            _, out, _ = _run(db, max_objects=3)
        self.assertEqual(len(db.page_calls()), 1)
        self.assertEqual(_tokens(out).get("budget_hit"), "1")


# ── (8) the SUMMARY ───────────────────────────────────────────────────────

class SummaryTest(unittest.TestCase):

    def test_the_summary_carries_every_field_in_order(self):
        db = _Db(_objects(POSTERS[:2]))
        _, out, _ = _run(db, prefixes=("posters/", "seasons/"))
        line = _summary(out)
        self.assertTrue(line.startswith("[IMAGE_RECACHE] SUMMARY "), line)
        self.assertEqual([t.split("=", 1)[0] for t in line.split()[2:]], list(SUMMARY_FIELDS))
        self.assertEqual(_tokens(out).get("prefixes"), "posters/,seasons/")

    def test_bytes_down_equals_bytes_up_when_every_upload_lands(self):
        db = _Db(_objects(POSTERS))
        _, out, _ = _run(db)
        total = sum(len(_data(i)) for i in range(7))
        self.assertEqual((_tokens(out).get("bytes_down"), _tokens(out).get("bytes_up")), (str(total), str(total)))
        # Positive control: a refused body is fetched, not uploaded.
        db = _Db(_objects(POSTERS[:2]))
        _, out, _ = _run(db, _Cdn(db, {"posters/2.jpg": {"ctype": "text/html"}}))
        self.assertEqual((_tokens(out).get("bytes_down"), _tokens(out).get("bytes_up")),
                         (str(len(_data(0)) + len(_data(1))), str(len(_data(0)))))

    def test_objects_per_s_comes_from_the_clock(self):
        db = _Db(_objects(POSTERS[:4]))
        _, out, clock = _run(db, clock=_Clock(step=2.0))
        self.assertGreaterEqual(len(clock.values), 2)                            # the run reads its clock
        elapsed = (clock.values[-1] - clock.values[0]).total_seconds() if clock.values else 0
        self.assertGreater(elapsed, 0)
        self.assertEqual(_tokens(out).get("objects_per_s"), f"{4 / elapsed:.1f}")
        # Positive control: nothing re-uploaded is a dash.
        db = _Db(_objects(POSTERS[:4], cache_control="max-age=604800"))
        _, out, _ = _run(db, clock=_Clock(step=2.0))
        self.assertEqual(_tokens(out).get("objects_per_s"), "-")


# ── (9) what it never does, and the logs ──────────────────────────────────

class ReadOnlyTest(unittest.TestCase):

    def test_it_never_touches_a_table_or_deletes_and_writes_only_its_cursor(self):
        db = _Db(_objects(POSTERS))
        _run(db)
        self.assertEqual((db.table_ops, db.storage_ops), ([], []))
        self.assertEqual({k for k, _ in db.state_writes}, {mod.CURSOR_KEY})
        self.assertEqual(len(db.uploads), 7)                                     # positive control

    def test_the_module_calls_no_table_and_no_delete(self):
        code = SOURCE.read_text(encoding="utf-8") if SOURCE.exists() else ""
        body = "\n".join(l.split("#", 1)[0] for l in code.split('"""', 2)[-1].splitlines())
        for call in (".table(", ".remove(", ".delete(", ".move(", ".copy(", ".update(", ".insert(", ".upsert("):
            with self.subTest(call=call):
                self.assertNotIn(call, body)
        self.assertIn(".upload_image(", body)                                    # positive control: the scan reads code

    def test_the_rpcs_and_the_cursor_stay_on_the_main_thread_and_uploads_get_a_client_per_worker(self):
        db, made, lock = _Db(_objects([f"posters/{i:02d}.jpg" for i in range(40)])), [], threading.Lock()

        class Uploader:
            def __init__(self):
                self.built_in, self.used_in = threading.get_ident(), set()

            def upload_image(self, *args, **kwargs):
                self.used_in.add(threading.get_ident())
                threading.Event().wait(0.002)
                return db.upload_image(*args, **kwargs)

        def make_uploader():
            uploader = Uploader()
            with lock:
                made.append(uploader)
            return uploader

        _run(db, workers=4, make_uploader=make_uploader)
        main = threading.get_ident()
        self.assertEqual({t for kind, t in db.threads if kind in ("rpc", "state")}, {main})
        self.assertEqual(len(db.uploads), 40)
        self.assertGreater(len({t for u in made for t in u.used_in}), 1)         # positive control: several threads
        for uploader in made:
            self.assertEqual(uploader.used_in, {uploader.built_in})


class LogTest(unittest.TestCase):

    def test_no_line_per_success_and_no_url(self):
        db = _Db(_objects(POSTERS))
        _, out, _ = _run(db)
        named = [l for l in out.splitlines() if any(n in l for n in POSTERS[1:])]
        self.assertEqual(named, [l for l in named if l.startswith("[IMAGE_RECACHE] SUMMARY ")])
        self.assertNotIn("http", out)
        self.assertNotIn("?", out)

    def test_error_lines_name_the_key_and_the_kind_and_stop_at_fifty(self):
        names = [f"posters/{i:03d}.jpg" for i in range(120)]
        db = _Db(_objects(names))
        cdn = _Cdn(db, {n: {"status": 500} for i, n in enumerate(names) if i % 2})
        _, out, _ = _run(db, cdn)
        lines = [l for l in out.splitlines() if l.startswith("[IMAGE_RECACHE] ERROR posters/")]
        self.assertEqual(len(lines), mod.ERROR_LINES_MAX)
        self.assertEqual(lines[0], "[IMAGE_RECACHE] ERROR posters/001.jpg: HTTP 500")
        self.assertIn("more errors not printed", out)
        self.assertEqual(_tokens(out).get("fetch_error"), "60")

    def test_a_still_failing_list_stops_at_fifty_names(self):
        # 240 objects: every odd one's first upload fails; half of those fail again (60), the other
        # half land on the retry — so the retry round never fails 50 in a row.
        names = [f"posters/{i:03d}.jpg" for i in range(240)]
        db = _Db(_objects(names), fail_uploads={n: (-1 if i % 4 == 3 else 1) for i, n in enumerate(names) if i % 2})
        stats, out, _ = _run(db)
        self.assertIsNone(_failed(stats))
        self.assertIn("[IMAGE_RECACHE]   … and 10 more", out)
        self.assertEqual(_tokens(out).get("retagged"), "180")
        listed = [l for l in out.splitlines() if re.fullmatch(r"\[IMAGE_RECACHE\]   posters/\d{3}\.jpg", l)]
        self.assertEqual(len(listed), mod.FAILED_NAMES_MAX)
        self.assertEqual(_tokens(out).get("errors"), "60")


# ── the review (RUN B5.10): retries, the gate in the retry round, the list, the breaker, the rest ──

def _invariant(tokens) -> bool:
    """seen = already + skipped_shape + retagged + the refusals + errors."""
    parts = ("already", "skipped_shape", "retagged", "fetch_error", "bad_type", "size_mismatch", "etag_mismatch",
             "errors")
    return bool(tokens) and int(tokens["seen"]) == sum(int(tokens[p]) for p in parts)


class RetryHoldTest(unittest.TestCase):
    """A failed upload the run had no time to retry holds the cursor; the next run asks it again."""

    def test_a_failure_the_time_budget_left_unretried_holds_the_cursor_and_lands_next_run(self):
        names = [f"posters/{i:02d}.jpg" for i in range(30)]
        db = _Db(_objects(names), fail_uploads={"posters/03.jpg": 1})
        stats, out, _ = _run(db, workers=1, budget_min=1, clock=_Clock(step=10.0))
        self.assertIsNone(_failed(stats))
        tokens = _tokens(out)
        self.assertEqual((tokens.get("budget_hit"), tokens.get("errors")), ("1", "1"))
        self.assertIn("1 failed upload(s) not retried", out)
        self.assertEqual(db.upload_attempts["posters/03.jpg"], 1)
        # The cursor never passed it: it stays just before it.
        self.assertEqual([c.get("after") for c in db.cursors()][-1:], ["posters/02.jpg"])
        self.assertTrue(_invariant(tokens), tokens)
        # The next run asks it first, and it lands; nothing is uploaded twice.
        _run(db, workers=1)
        self.assertEqual(db.upload_attempts["posters/03.jpg"], 2)
        self.assertEqual(sorted(u["path"] for u in db.uploads), names)
        self.assertEqual(db.objects["posters/03.jpg"]["cache_control"], "max-age=604800")

    def test_the_hold_spans_prefixes(self):
        objects = _objects(["posters/1.jpg", "posters/2.jpg", "posters/3.jpg"])
        objects.update(_objects(["seasons/1/1.jpg", "seasons/1/2.jpg", "seasons/1/3.jpg"]))
        # posters page 1, the gate, posters page 2, seasons page 1 answer; seasons page 2 fails for good.
        db = _Db(objects, fail_uploads={"posters/2.jpg": 1}, rpc_errors=[None, None, None, None, RuntimeError("boom")])
        with mock.patch.object(mod, "PAGE_LIMIT", 2):
            stats, out, _ = _run(db, prefixes=("posters/", "seasons/"))
        self.assertEqual(_failed(stats), "image_cache_page")
        self.assertEqual(db.cursors(), [{"prefix": "posters/", "after": "posters/1.jpg", "done_at": None}])
        self.assertIn("[IMAGE_RECACHE]   posters/2.jpg", out)
        # Positive control: the walk did reach seasons/.
        self.assertIn("seasons/1/1.jpg", [u["path"] for u in db.uploads])

    def test_a_retry_that_lands_lets_the_cursor_go_on_to_done(self):
        db = _Db(_objects(POSTERS[:4]), fail_uploads={"posters/2.jpg": 1})
        _, out, _ = _run(db, workers=1)
        cursors = db.cursors()
        self.assertEqual([(c.get("after"), c.get("done_at") is None) for c in cursors],
                         [("posters/1.jpg", True), ("posters/4.jpg", False)])
        self.assertEqual(_tokens(out).get("drain_done"), "1")


class RetryGateTest(unittest.TestCase):
    """The retry round is gated too, when the walk uploaded nothing."""

    def test_when_every_first_upload_failed_the_retry_round_runs_the_gate_first(self):
        db = _Db(_objects(POSTERS[:3]), fail_uploads={n: 1 for n in POSTERS[:3]})
        stats, out, _ = _run(db, workers=4)
        self.assertIsNone(_failed(stats))
        self.assertEqual(len(_gate_lines(out)), 1)
        self.assertEqual(db.gate_calls(), [{"p_prefix": "posters/", "p_after": "", "p_limit": 1}])
        uploads = [i for i, e in enumerate(db.events) if e[0] == "upload"]
        gate = [i for i, e in enumerate(db.events) if e[0] == "rpc" and e[1].get("p_limit") == 1]
        self.assertEqual(len(uploads), 3)
        self.assertLess(uploads[0], gate[0] if gate else -1)
        self.assertTrue(all(i > gate[0] for i in uploads[1:]) if gate else False)

    def test_the_first_dispatch_with_max_objects_1_is_gated_even_through_its_retry(self):
        db = _Db(_objects(POSTERS[:3]), fail_uploads={"posters/1.jpg": 1})
        stats, out, _ = _run(db, max_objects=1)
        self.assertIsNone(_failed(stats))
        self.assertEqual(_gate_lines(out),
                         ["[IMAGE_RECACHE] GATE served=max-age=604800 stored=max-age=604800 for posters/1.jpg"])
        tokens = _tokens(out)
        self.assertEqual((tokens.get("retagged"), tokens.get("budget_hit")), ("1", "1"))
        self.assertEqual([c.get("after") for c in db.cursors()][-1:], ["posters/1.jpg"])

    def test_a_gate_that_fails_in_the_retry_round_stops_after_one_object(self):
        db = _Db(_objects(POSTERS[:3]), fail_uploads={n: 1 for n in POSTERS[:3]}, served_follows=False)
        stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "gate")
        self.assertEqual([u["path"] for u in db.uploads], ["posters/1.jpg"])
        self.assertEqual(db.cursors(), [{"prefix": "posters/", "after": "", "done_at": None}])   # held

    def test_fifty_retries_in_a_row_that_fail_stop_red_and_hold_the_cursor(self):
        names = [f"posters/{i:03d}.jpg" for i in range(120)]
        db = _Db(_objects(names), fail_uploads={n: -1 for i, n in enumerate(names) if i % 2})
        stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "files")
        self.assertEqual(db.cursors(), [{"prefix": "posters/", "after": "posters/000.jpg", "done_at": None}])
        self.assertTrue(_summary(out))


class DoneListTest(unittest.TestCase):
    """done_at is the end of a list: a list that goes on walks on."""

    def test_a_drain_done_short_of_this_runs_list_walks_on(self):
        done = {"prefix": "seasons/", "after": "seasons/9/9.jpg", "done_at": "2026-12-01T00:00:00+00:00"}
        db = _Db(_objects(["carousels/9/0.jpg", "carousels/9/1.jpg"]), cursor=done)
        stats, out, _ = _run(db, prefixes=("posters/", "seasons/", "carousels/"))
        self.assertIsNone(_failed(stats))
        self.assertEqual([(p["p_prefix"], p["p_after"]) for p in db.page_calls()],
                         [("seasons/", "seasons/9/9.jpg"), ("carousels/", "")])
        self.assertEqual(sorted(u["path"] for u in db.uploads), ["carousels/9/0.jpg", "carousels/9/1.jpg"])
        self.assertIn("the drain was done up to seasons/; this run's list goes on to carousels/", out)
        self.assertEqual(_tokens(out).get("drain_done"), "1")
        # Positive control: a list that ends at seasons/ is done.
        db = _Db(_objects(["carousels/9/0.jpg"]), cursor=done)
        _, out, _ = _run(db, prefixes=("posters/", "seasons/"))
        self.assertEqual((db.rpc_calls, _tokens(out).get("drain_done")), ([], "1"))


class BreakerTest(unittest.TestCase):
    """The breaker counts what an outage makes, in the pool too; a refusal is about its own object."""

    def _sixty(self, override=None, **db_kwargs):
        names = [f"posters/{i:03d}.jpg" for i in range(61)]
        db = _Db(_objects(names), **db_kwargs)
        cdn = _Cdn(db, {n: override for n in names[1:]} if override else None)
        stats, out, _ = _run(db, cdn, workers=2)
        return db, stats, out

    def test_refusals_in_a_row_do_not_trip_it(self):
        for override, counter in (({"ctype": "text/html"}, "bad_type"), ({"status": 404}, "fetch_error"),
                                  ({"etag": '"' + "0" * 32 + '"'}, "etag_mismatch")):
            with self.subTest(counter=counter):
                db, stats, out = self._sixty(override)
                self.assertIsNone(_failed(stats))
                self.assertEqual(_tokens(out).get(counter), "60")
                self.assertEqual(_tokens(out).get("drain_done"), "1")             # the cursor moves on

    def test_outages_in_a_row_after_the_gate_trip_it_in_the_pool(self):
        names = [f"posters/{i:03d}.jpg" for i in range(61)]
        cases = (("503", {n: {"status": 503} for n in names[1:]}, {}),
                 ("429", {n: {"status": 429} for n in names[1:]}, {}),
                 ("raise", {n: requests.ConnectionError("down") for n in names[1:]}, {}),
                 ("upload", None, {"fail_uploads": {n: -1 for n in names[1:]}}))
        for kind, overrides, db_kwargs in cases:
            with self.subTest(kind=kind):
                db = _Db(_objects(names), **db_kwargs)
                stats, out, _ = _run(db, _Cdn(db, overrides), workers=2)
                self.assertEqual(_failed(stats), "files")
                self.assertEqual(len(_gate_lines(out)), 1)                       # it tripped after the gate
                self.assertEqual(db.cursors(), [])


class GateReviewTest(unittest.TestCase):

    def test_a_served_max_age_of_another_length_fails_the_gate(self):
        db = _Db(_objects(POSTERS[:3]), served_value="max-age=3600")
        stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "gate")
        self.assertIn("its public URL still served max-age=3600 after 90 s", out)
        self.assertNotIn("the server stored", out)

    def test_a_stored_max_age_of_another_length_fails_the_gate(self):
        db = _Db(_objects(POSTERS[:3]), stored_value="max-age=3600")
        stats, out, clock = _run(db)
        self.assertEqual(_failed(stats), "gate")
        self.assertIn("the server stored max-age=3600", out)
        self.assertNotIn("still served", out)
        self.assertEqual(clock.sleeps, [])                                        # no wait: the served side was right

    def test_a_directive_among_others_passes(self):
        db = _Db(_objects(POSTERS[:3]), served_value="public, max-age=604800")
        stats, _, _ = _run(db)
        self.assertIsNone(_failed(stats))

    def test_a_gate_read_that_raises_names_no_url(self):
        url = f"{BUCKET_URL}posters/1.jpg?"
        db = _Db(_objects(POSTERS[:2]))
        cdn = _Cdn(db, after_upload={"posters/1.jpg": requests.ConnectionError(f"failed to reach {url}")})
        stats, out, _ = _run(db, cdn)
        self.assertEqual(_failed(stats), "gate")
        self.assertIn("GATE served=ConnectionError stored=max-age=604800 for posters/1.jpg", out)
        self.assertNotIn("http", out)
        self.assertNotIn("?", out)

    def test_a_fetch_that_raises_with_its_url_names_no_url(self):
        url = f"{BUCKET_URL}posters/2.jpg?"
        db = _Db(_objects(POSTERS[:2]))
        _, out, _ = _run(db, _Cdn(db, {"posters/2.jpg": requests.ConnectionError(f"failed to reach {url}")}))
        self.assertIn("[IMAGE_RECACHE] ERROR posters/2.jpg: ConnectionError", out)
        self.assertNotIn("http", out)
        self.assertNotIn("?", out)

    def test_the_gates_stored_read_gets_the_retry_and_a_lasting_failure_stops_red(self):
        db = _Db(_objects(POSTERS[:2]), rpc_errors=[None, httpx.ConnectTimeout("t")])
        stats, _, clock = _run(db)
        self.assertIsNone(_failed(stats))
        self.assertEqual(clock.sleeps, [2])
        objects = _objects(["posters/1.jpg", "posters/2.jpg", "posters/3.jpg"], cache_control="max-age=604800")
        objects.update(_objects(["posters/4.jpg", "posters/5.jpg"]))
        db = _Db(objects, rpc_errors=[None, RuntimeError("boom")])
        stats, out, _ = _run(db)
        self.assertEqual(_failed(stats), "image_cache_page")
        tokens = _tokens(out)
        self.assertEqual((tokens.get("seen"), tokens.get("already"), tokens.get("retagged")), ("4", "3", "1"))
        self.assertTrue(_invariant(tokens), tokens)


class RetryBoundTest(unittest.TestCase):

    def test_a_transient_failure_that_lasts_three_attempts_stops_red(self):
        db = _Db(_objects(POSTERS[:2]), rpc_errors=[httpx.ConnectTimeout("t")] * 3)
        stats, out, clock = _run(db)
        self.assertEqual(_failed(stats), "image_cache_page")
        self.assertEqual(clock.sleeps, [2, 4])
        self.assertTrue(_summary(out))
        db = _Db(_objects(POSTERS[:2]), state_write_errors=[_api_error("503")] * 3)
        stats, _, clock = _run(db)
        self.assertEqual(_failed(stats), "bot_state")
        self.assertEqual(clock.sleeps, [2, 4])

    def test_every_fetch_asks_thirty_seconds(self):
        db = _Db(_objects(POSTERS[:3]), served_lag=2)
        cdn = _Cdn(db)
        _run(db, cdn)
        self.assertEqual(len(cdn.timeouts), 3 + 3)                                # three bodies and three gate reads
        self.assertEqual(set(cdn.timeouts), {30})
        self.assertEqual(mod.FETCH_TIMEOUT_SECONDS, 30)


class CursorShapeTest(unittest.TestCase):

    def test_more_values_this_script_did_not_write_are_refused(self):
        for stored in ({"prefix": "posters/", "after": "zzz", "done_at": None},
                       {"prefix": "posters/", "after": ""},
                       {"prefix": "posters/", "after": "", "done_at": 1}):
            with self.subTest(stored=stored):
                db = _Db(_objects(POSTERS), cursor=stored)
                stats, _, _ = _run(db)
                self.assertEqual(_failed(stats), "cursor")
                self.assertEqual((db.rpc_calls, db.state_writes), ([], []))
        # Positive control: an after under its prefix resumes.
        db = _Db(_objects(POSTERS), cursor={"prefix": "posters/", "after": "posters/3.jpg", "done_at": None})
        stats, _, _ = _run(db)
        self.assertIsNone(_failed(stats))


class InvariantTest(unittest.TestCase):

    def test_seen_is_the_sum_on_a_gate_stop_a_breaker_and_a_cut(self):
        db = _Db(_objects(POSTERS[:3]), served_follows=False)
        _, out, _ = _run(db)
        self.assertTrue(_invariant(_tokens(out)), _tokens(out))
        self.assertEqual(_tokens(out).get("seen"), "1")
        names = [f"posters/{i:02d}.jpg" for i in range(90)]
        db = _Db(_objects(names), fail_every_upload=True)
        _, out, _ = _run(db)
        self.assertTrue(_invariant(_tokens(out)), _tokens(out))
        objects = _objects([f"posters/{i:02d}.jpg" for i in range(5)])
        objects.update(_objects(["posters/90.jpg", "posters/91.jpg"], cache_control="max-age=604800"))
        db = _Db(objects)
        _, out, _ = _run(db, max_objects=3)
        tokens = _tokens(out)
        self.assertTrue(_invariant(tokens), tokens)
        self.assertEqual((tokens.get("seen"), tokens.get("already")), ("3", "0"))   # the rows past the cut wait


# ── main: the environment ─────────────────────────────────────────────────

class MainTest(unittest.TestCase):

    def _main(self, env):
        seen = {}

        def fake_run(db, **kwargs):
            seen.update(kwargs, db=db, uploader=kwargs["make_uploader"]())
            return {"failed": None}

        out = io.StringIO()
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.object(mod, "SupabaseClient", lambda url, key: ("db", url, key), create=True), \
                mock.patch.object(mod, "run", fake_run), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            try:
                mod.main(clock=_Clock())
                exited = None
            except SystemExit as exc:
                exited = exc
        return seen, exited, out.getvalue()

    def test_main_reads_its_knobs_with_the_scripts_defaults(self):
        seen, exited, _ = self._main(FAKE_ENV)
        self.assertIsNone(exited)
        self.assertEqual((seen.get("prefixes"), seen.get("workers"), seen.get("budget_min"), seen.get("max_objects"),
                          seen.get("restart")), (("posters/", "seasons/", "carousels/"), 12, 25, 40000, False))
        self.assertEqual(seen.get("uploader"), ("db", FAKE_ENV["SUPABASE_URL"], FAKE_ENV["SUPABASE_KEY"]))
        seen, _, _ = self._main({**FAKE_ENV, "IMAGE_RECACHE_PREFIXES": "carousels/", "IMAGE_RECACHE_WORKERS": "16",
                                 "IMAGE_RECACHE_TIME_BUDGET_MIN": "80", "IMAGE_RECACHE_MAX_OBJECTS": "1",
                                 "IMAGE_RECACHE_RESTART": "TRUE"})
        self.assertEqual((seen.get("prefixes"), seen.get("workers"), seen.get("budget_min"), seen.get("max_objects"),
                          seen.get("restart")), (("carousels/",), 16, 80, 1, True))
        self.assertEqual((mod.DEFAULT_IMAGE_RECACHE_WORKERS, mod.DEFAULT_IMAGE_RECACHE_TIME_BUDGET_MIN,
                          mod.DEFAULT_IMAGE_RECACHE_MAX_OBJECTS, mod.MAX_IMAGE_RECACHE_WORKERS,
                          mod.MAX_IMAGE_RECACHE_TIME_BUDGET_MIN), (12, 25, 40000, 16, 80))   # B5.10.1 · 0c: 12

    def test_main_refuses_knobs_outside_their_range_before_any_work(self):
        for key, value in (("IMAGE_RECACHE_WORKERS", "0"), ("IMAGE_RECACHE_WORKERS", "17"),
                           ("IMAGE_RECACHE_TIME_BUDGET_MIN", "0"), ("IMAGE_RECACHE_TIME_BUDGET_MIN", "81"),
                           ("IMAGE_RECACHE_MAX_OBJECTS", "0"), ("IMAGE_RECACHE_MAX_OBJECTS", "lots"),
                           ("IMAGE_RECACHE_RESTART", "yes"), ("IMAGE_RECACHE_PREFIXES", "zzz/"),
                           ("IMAGE_RECACHE_PREFIXES", "posters/,zzz/"), ("IMAGE_RECACHE_PREFIXES", "")):
            with self.subTest(key=key, value=value):
                seen, exited, out = self._main({**FAKE_ENV, key: value})
                self.assertIsInstance(exited, SystemExit)
                self.assertEqual(getattr(exited, "code", None), 1)
                self.assertIn(f"[IMAGE_RECACHE] ERROR: {key}", out)
                self.assertEqual(seen, {})

    def test_main_needs_the_supabase_pair_and_no_tmdb_key(self):
        self.assertNotIn("TMDB_API_KEY", FAKE_ENV)
        seen, exited, _ = self._main(FAKE_ENV)
        self.assertIsNone(exited)
        self.assertTrue(seen)
        seen, exited, out = self._main({"SUPABASE_URL": FAKE_ENV["SUPABASE_URL"]})
        self.assertIsInstance(exited, SystemExit)
        self.assertIn("[IMAGE_RECACHE] ERROR: Required environment variable 'SUPABASE_KEY' is not set.", out)

    def test_a_failed_run_exits_1(self):
        out = io.StringIO()
        with mock.patch.dict(os.environ, FAKE_ENV, clear=True), \
                mock.patch.object(mod, "SupabaseClient", lambda url, key: ("db", url, key), create=True), \
                mock.patch.object(mod, "run", lambda db, **kwargs: {"failed": "gate"}), \
                contextlib.redirect_stdout(out):
            try:
                mod.main(clock=_Clock())
                code = None
            except SystemExit as exc:
                code = exc.code
        self.assertEqual(code, 1)


# ── (10) what the workflow reads: $GITHUB_OUTPUT (B5.10.1 · 0a) ───────────

OUTPUT_NAMES = ("budget_hit", "drain_done", "retagged", "refused", "cursor")
REFUSAL_TERMS = ("fetch_error", "bad_type", "size_mismatch", "etag_mismatch", "errors")


def _fake_stats(**over):
    """A run's stats as _summary returns them, every refusal term non-zero and
    distinct (1 + 2 + 3 + 4 + 5 = 15), so a dropped term changes the sum."""
    stats = {"pages": 3, "seen": 47, "already": 8, "skipped_shape": 6, "etag_unchecked": 7, "bytes_down": 900,
             "bytes_up": 800, "budget_hit": 1, "drain_done": 0, "failed": None, "retagged": 17, "fetch_error": 1,
             "bad_type": 2, "size_mismatch": 3, "etag_mismatch": 4, "errors": 5,
             "cursor": "posters/:posters/19877.jpg"}
    return {**stats, **over}


def _read_lines(path):
    with open(path, encoding="utf-8") as handle:
        return handle.read().splitlines()


def _outputs(stats) -> dict:
    """What write_outputs puts in a fresh file, as {name: value}."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "github_output")
        mod.write_outputs(stats, path)
        lines = _read_lines(path) if os.path.exists(path) else []
    return dict(line.split("=", 1) for line in lines if "=" in line)


class OutputsTest(unittest.TestCase):

    def test_the_five_lines_are_appended_after_what_the_file_already_holds(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "github_output")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("earlier=kept\n")
            mod.write_outputs(_fake_stats(), path)
            lines = _read_lines(path)
        self.assertEqual(lines[:1], ["earlier=kept"])                            # positive control: never truncated
        self.assertEqual(lines[1:], ["budget_hit=1", "drain_done=0", "retagged=17", "refused=15",
                                     "cursor=posters/:posters/19877.jpg"])

    def test_no_path_writes_nothing(self):
        with mock.patch.object(mod, "open", side_effect=AssertionError("a file was opened"), create=True) as opened:
            mod.write_outputs(_fake_stats(), None)
        self.assertFalse(opened.called)
        # Positive control beside it: a path writes the five lines.
        self.assertEqual(sorted(_outputs(_fake_stats())), sorted(OUTPUT_NAMES))

    def test_refused_is_every_refusal_plus_the_errors_and_nothing_else(self):
        stats = _fake_stats()
        self.assertTrue(all(stats[term] for term in REFUSAL_TERMS))
        self.assertEqual(len({stats[term] for term in REFUSAL_TERMS}), len(REFUSAL_TERMS))
        self.assertEqual(_outputs(stats).get("refused"), str(sum(stats[term] for term in REFUSAL_TERMS)))   # 15
        for term in REFUSAL_TERMS:
            with self.subTest(term=term):                                        # each term moves it
                self.assertEqual(_outputs(_fake_stats(**{term: stats[term] + 100})).get("refused"), "115")
        # Positive control: what is not a refusal does not move it.
        for other in ("already", "skipped_shape", "etag_unchecked", "retagged"):
            with self.subTest(other=other):
                self.assertEqual(_outputs(_fake_stats(**{other: stats[other] + 100})).get("refused"), "15")

    def test_the_cursor_is_the_summarys_cursor_field(self):
        objects = _objects(POSTERS[:3])
        objects.update(_objects(["posters/4.jpg"], cache_control="max-age=604800"))
        cases = (("a cut mid-prefix", _Db(_objects(POSTERS)), {"max_objects": 3}, "posters/:posters/3.jpg"),
                 ("a drain done", _Db(objects), {}, "posters/:posters/4.jpg"),
                 ("a gate stop", _Db(_objects(POSTERS[:2]), served_follows=False), {}, "posters/:"),
                 ("a resumed cursor", _Db(_objects(POSTERS),
                                          cursor={"prefix": "posters/", "after": "posters/5.jpg", "done_at": None}),
                  {"max_objects": 1}, "posters/:posters/6.jpg"))
        for label, db, knobs, expected in cases:
            with self.subTest(case=label):
                stats, out, _ = _run(db, **knobs)
                self.assertEqual(_tokens(out).get("cursor"), expected)           # positive control: the SUMMARY's
                self.assertEqual(stats.get("cursor"), _tokens(out).get("cursor"))
                self.assertEqual(_outputs(stats).get("cursor"), expected)

    # main(): the lines reach $GITHUB_OUTPUT, a red run's too

    def _main_with_output(self, *, run=None, db=None, env=None):
        """main() with a tmp GITHUB_OUTPUT: *run* replaces run(), or the real
        run() goes through *db* and its CDN. Returns the file's lines, the
        exit code (None when it returns) and the output."""
        out = io.StringIO()
        clock = _Clock()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "github_output")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("earlier=kept\n")
            patches = [mock.patch.dict(os.environ, {**FAKE_ENV, **(env or {}), "GITHUB_OUTPUT": path}, clear=True),
                       mock.patch.object(mod, "SupabaseClient",
                                         (lambda url, key: db) if db is not None else (lambda url, key: ("db", url, key)))]
            if run is not None:
                patches.append(mock.patch.object(mod, "run", run))
            with contextlib.ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                stack.enter_context(contextlib.redirect_stdout(out))
                stack.enter_context(contextlib.redirect_stderr(out))
                try:
                    mod.main(clock=clock, fetch=_Cdn(db) if db is not None else None, sleep=clock.sleep)
                    code = None
                except SystemExit as exc:
                    code = exc.code
            lines = _read_lines(path)
        return lines, code, out.getvalue()

    def test_main_appends_the_lines_after_run(self):
        lines, code, _ = self._main_with_output(run=lambda db, **kwargs: _fake_stats())
        self.assertIsNone(code)
        self.assertEqual(lines, ["earlier=kept", "budget_hit=1", "drain_done=0", "retagged=17", "refused=15",
                                 "cursor=posters/:posters/19877.jpg"])

    def test_a_red_run_writes_its_lines_and_still_exits_1(self):
        lines, code, _ = self._main_with_output(run=lambda db, **kwargs: _fake_stats(failed="gate", budget_hit=0))
        self.assertEqual(code, 1)
        self.assertEqual(lines[1:], ["budget_hit=0", "drain_done=0", "retagged=17", "refused=15",
                                     "cursor=posters/:posters/19877.jpg"])

    def test_main_through_the_fakes_writes_what_its_summary_says(self):
        db = _Db(_objects(POSTERS))
        lines, code, out = self._main_with_output(db=db, env={"IMAGE_RECACHE_PREFIXES": "posters/",
                                                              "IMAGE_RECACHE_MAX_OBJECTS": "3",
                                                              "IMAGE_RECACHE_WORKERS": "2"})
        self.assertIsNone(code)
        tokens = _tokens(out)
        self.assertEqual(lines, ["earlier=kept", "budget_hit=1", "drain_done=0", "retagged=3", "refused=0",
                                 "cursor=posters/:posters/3.jpg"])
        self.assertEqual([lines[1], lines[2], lines[3], lines[5]],
                         [f"budget_hit={tokens.get('budget_hit')}", f"drain_done={tokens.get('drain_done')}",
                          f"retagged={tokens.get('retagged')}", f"cursor={tokens.get('cursor')}"])
        self.assertEqual(len(db.uploads), 3)                                     # positive control: a real run

    def test_a_red_run_through_the_fakes_writes_its_lines_and_exits_1(self):
        db = _Db(_objects(POSTERS[:2]), served_follows=False)                    # the gate fails
        lines, code, out = self._main_with_output(db=db, env={"IMAGE_RECACHE_PREFIXES": "posters/"})
        self.assertEqual(code, 1)
        self.assertEqual(len(_gate_lines(out)), 1)
        self.assertEqual(lines, ["earlier=kept", "budget_hit=0", "drain_done=0", "retagged=1", "refused=0",
                                 f"cursor={_tokens(out).get('cursor')}"])


if __name__ == "__main__":
    unittest.main()
