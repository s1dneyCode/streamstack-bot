"""
Unit tests for the house cache policy of media-images (RUN B5.10 · 0a).

Every object an upload_image call wrote without cache_seconds carries
cacheControl = no-cache (625,395 of them on 22/11), which the app's image
cache reads as zero validity. bot/image_cache_policy.py says what each key
deserves, from its shape alone:
  - a key that names TMDb's FILE (posters/<id>/<stem>.jpg,
    posters-hd/<id>/<stem>.jpg, tmdb/w<n>/<file>) — a changed image is a new
    key — a year;
  - a key that names a SLOT (posters/<id>.jpg, carousels/<id>/<n>.jpg,
    seasons/<id>/<n>.jpg) — the bytes under it can change — 7 days, Fran's
    number: a changed image is at most a week stale on a device;
  - anything else: None, no guess.
Every upload_image call under bot/ passes cache_seconds, and the three files
that upload slots ask the policy for the very key they upload.

The callers are pinned at the wire: a REAL SupabaseClient, offline, whose
requests are caught at httpx's transport — tests/test_upload_image_cache.py's
_wire(), here with an answer per path (a storage upload gets storage's
answer, a PostgREST call gets rows) so a caller's database calls do not
fail. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import ast
import contextlib
import os
import pathlib
import tempfile
import types
import unittest
from unittest import mock

import httpx

from bot import backfill_seasons, image_cache_policy as policy, migrate_images, posters_hd, tmdb_mirror
from bot.supabase_client import SupabaseClient
from tests.test_upload_image_cache import FAKE_KEY, FAKE_URL, _form_field, _wire

BOT = pathlib.Path(__file__).resolve().parent.parent / "bot"
YEAR, WEEK = 31536000, 604800
DATA = b"\xff\xd8\xff\xe0fake-jpeg-bytes"
STORAGE = "/storage/v1/object/media-images/"
# The files that upload a slot, and the callers the scan must see in each (RUN B5.10's measure (a)).
SLOT_UPLOADERS = ("migrate_images.py", "enrich_new_titles.py", "backfill_seasons.py")
KNOWN_CALLERS = {"migrate_images.py": 3, "enrich_new_titles.py": 1, "backfill_seasons.py": 1,
                 "posters_hd.py": 1, "tmdb_mirror.py": 1}


# ── the scan ──────────────────────────────────────────────────────────────

def upload_calls(source: str) -> list[ast.Call]:
    """Every call of a function or method named upload_image in *source*
    (a definition is not a call)."""
    calls = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name == "upload_image":
                calls.append(node)
    return calls


def without_cache_seconds(path: pathlib.Path) -> list[int]:
    """The line of every upload_image call in the file that passes no cache_seconds."""
    return [c.lineno for c in upload_calls(path.read_text(encoding="utf-8"))
            if not any(k.arg == "cache_seconds" for k in c.keywords)]


def asks_the_policy_for_its_key(call: ast.Call) -> bool:
    """True when the call's cache_seconds is cache_seconds_for(<the key it
    uploads>): the path argument itself, or the name a := in it binds."""
    value = next((k.value for k in call.keywords if k.arg == "cache_seconds"), None)
    if not (isinstance(value, ast.Call) and getattr(value.func, "id", None) == "cache_seconds_for"
            and len(value.args) == 1 and not value.keywords and len(call.args) >= 2):
        return False
    key, asked = call.args[1], value.args[0]
    if isinstance(key, ast.NamedExpr):
        return isinstance(asked, ast.Name) and asked.id == key.target.id
    return ast.dump(asked) == ast.dump(key)


# ── the wire, with an answer per path ─────────────────────────────────────

@contextlib.contextmanager
def _routed(rows=None):
    """_wire()'s offline SupabaseClient and catch, with storage answered as
    storage and PostgREST answered with *rows* by table (else [])."""
    rows = rows or {}
    with _wire() as (db, caught):
        def handle_request(self, request):
            request.read()
            caught.append(request)
            path = request.url.path
            if "/storage/v1/object/" in path:
                return httpx.Response(200, json={"Key": path.split("/storage/v1/object/", 1)[1]}, request=request)
            return httpx.Response(200, json=rows.get(path.rsplit("/", 1)[-1], []), request=request)

        with mock.patch.object(httpx.HTTPTransport, "handle_request", handle_request):
            yield db, caught


def _uploads(caught) -> list[tuple]:
    """(key, the cacheControl form field, the cache-control header) of every storage upload caught."""
    return [(r.url.path.split(STORAGE, 1)[1], _form_field(r, "cacheControl"), r.headers.get("cache-control"))
            for r in caught if r.method == "POST" and STORAGE in r.url.path]


# ── (1) the table ─────────────────────────────────────────────────────────

class PolicyTableTest(unittest.TestCase):

    def test_each_shape_gets_its_number(self):
        for name, seconds in (("posters/603/kvuN0aKiDPXSfmTg5ICh.jpg", YEAR),   # refresh_poster: TMDb's stem
                              ("posters-hd/603/kvuN0aKiDPXSfmTg5ICh.jpg", YEAR),  # posters_hd: TMDb's stem
                              ("tmdb/w185/abc123.jpg", YEAR),                     # tmdb_mirror: TMDb's file
                              ("posters/603.jpg", WEEK),                          # migrate_poster: a slot
                              ("carousels/603/0.jpg", WEEK),                      # migrate_carousel: a slot
                              ("seasons/1399/1.jpg", WEEK)):                      # season posters: a slot
            with self.subTest(name=name):
                self.assertEqual(policy.cache_seconds_for(name), seconds)

    def test_every_mirror_size_and_kind_is_a_tmdb_file(self):
        for name in ("tmdb/w1280/xyz.jpg", "tmdb/w500/logo.png", "tmdb/w500/logo.svg", "tmdb/w500/logo.webp"):
            with self.subTest(name=name):
                self.assertEqual(policy.cache_seconds_for(name), YEAR)

    def test_the_two_numbers(self):
        self.assertEqual(policy.ONE_YEAR_CACHE_SECONDS, 365 * 24 * 3600)
        self.assertEqual(policy.SEVEN_DAYS_CACHE_SECONDS, 7 * 24 * 3600)
        self.assertEqual((policy.ONE_YEAR_CACHE_SECONDS, policy.SEVEN_DAYS_CACHE_SECONDS), (YEAR, WEEK))

    def test_an_unknown_shape_gets_none_and_the_known_one_beside_it_its_number(self):
        # Each unknown name, then the known one a character away (positive control).
        for unknown, known, seconds in (("posters/60a.jpg", "posters/603.jpg", WEEK),
                                        ("carousels/603/a.jpg", "carousels/603/1.jpg", WEEK),
                                        ("seasons/1399/1.jpgx", "seasons/1399/1.jpg", WEEK),
                                        ("poster/603.jpg", "posters/603.jpg", WEEK),
                                        ("posters/603/kvuN.jpe", "posters/603/kvuN.jpg", YEAR),
                                        ("posters-hd/60x/kvuN.jpg", "posters-hd/603/kvuN.jpg", YEAR),
                                        ("tmdb/x185/abc.jpg", "tmdb/w185/abc.jpg", YEAR)):
            with self.subTest(unknown=unknown):
                self.assertIsNone(policy.cache_seconds_for(unknown))
                self.assertEqual(policy.cache_seconds_for(known), seconds)

    def test_nothing_else_is_guessed(self):
        for name in ("", "posters/", "posters/.jpg", "carousels/603/0/1.jpg", "seasons/1399/1/2.jpg",
                     "tmdb/w185/", "tmdb/w185/a/b.jpg", "media-images/posters/603.jpg", "/posters/603.jpg",
                     "posters/603.jpg/", "other/603.jpg"):
            with self.subTest(name=name):
                self.assertIsNone(policy.cache_seconds_for(name))

    def test_the_known_prefixes_are_the_shapes_first_segments(self):
        self.assertEqual(policy.PREFIXES, ("posters/", "posters-hd/", "carousels/", "seasons/", "tmdb/"))
        # Each is a prefix image_cache_page accepts: letters, digits, '-', one trailing slash.
        for prefix in policy.PREFIXES:
            with self.subTest(prefix=prefix):
                self.assertRegex(prefix, r"^[A-Za-z0-9-]+/$")
        # Positive control: every known name lives under one of them, and posters/ is not posters-hd/.
        for name in ("posters/603.jpg", "posters-hd/603/kvuN.jpg", "carousels/603/0.jpg", "seasons/1399/1.jpg",
                     "tmdb/w185/abc123.jpg"):
            with self.subTest(name=name):
                self.assertEqual(sum(name.startswith(p) for p in policy.PREFIXES), 1)


# ── (2) where the year lives ──────────────────────────────────────────────

def _assigns(source: str, name: str) -> bool:
    return any(isinstance(n, ast.Assign) and any(getattr(t, "id", None) == name for t in n.targets)
               for n in ast.walk(ast.parse(source)))


def _imports_from(source: str, module: str, name: str) -> bool:
    return any(isinstance(n, ast.ImportFrom) and n.module == module and n.level == 1
               and any(a.name == name and a.asname is None for a in n.names)
               for n in ast.walk(ast.parse(source)))


class ConstantHomeTest(unittest.TestCase):

    def test_posters_hd_imports_the_year_from_the_policy_under_the_same_name(self):
        source = (BOT / "posters_hd.py").read_text(encoding="utf-8")
        self.assertFalse(_assigns(source, "ONE_YEAR_CACHE_SECONDS"))
        self.assertTrue(_imports_from(source, "image_cache_policy", "ONE_YEAR_CACHE_SECONDS"))
        # Positive control: the helpers see B5.8's assignment and an import.
        self.assertTrue(_assigns("ONE_YEAR_CACHE_SECONDS = 31536000\n", "ONE_YEAR_CACHE_SECONDS"))
        self.assertTrue(_imports_from("from .image_cache_policy import ONE_YEAR_CACHE_SECONDS\n",
                                      "image_cache_policy", "ONE_YEAR_CACHE_SECONDS"))

    def test_every_holder_of_the_year_holds_the_policys(self):
        for module in (posters_hd, tmdb_mirror):
            with self.subTest(module=module.__name__):
                self.assertEqual(module.ONE_YEAR_CACHE_SECONDS, policy.ONE_YEAR_CACHE_SECONDS)
        # tmdb_mirror still takes it from posters_hd, untouched.
        self.assertTrue(_imports_from((BOT / "tmdb_mirror.py").read_text(encoding="utf-8"), "posters_hd",
                                      "ONE_YEAR_CACHE_SECONDS"))

    def test_the_policy_is_pure(self):
        tree = ast.parse((BOT / "image_cache_policy.py").read_text(encoding="utf-8"))
        imported = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        imported += [("." * n.level) + (n.module or "") for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        self.assertEqual([m for m in imported if m not in ("re",)], [])
        # Positive control: an import of a client would be seen.
        tree = ast.parse("from .supabase_client import SupabaseClient\nimport requests\n")
        seen = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        seen += [("." * n.level) + (n.module or "") for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        self.assertEqual(seen, ["requests", ".supabase_client"])


# ── (3) every caller passes it ────────────────────────────────────────────

class CallScanTest(unittest.TestCase):

    def test_every_upload_call_under_bot_passes_cache_seconds(self):
        missing = {p.name: lines for p in sorted(BOT.glob("*.py")) if (lines := without_cache_seconds(p))}
        self.assertEqual(missing, {})

    def test_the_scan_sees_every_known_caller(self):
        found = {p.name: len(upload_calls(p.read_text(encoding="utf-8"))) for p in BOT.glob("*.py")}
        self.assertEqual({name: found.get(name) for name in KNOWN_CALLERS}, KNOWN_CALLERS)
        self.assertEqual(found.get("supabase_client.py"), 0)          # the definition is not a call

    def test_the_scan_catches_a_call_without_it_in_a_file_outside_the_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = pathlib.Path(tmp) / "fixture.py"
            fixture.write_text(
                "def f(db, b):\n"
                "    db.upload_image('media-images', 'posters/1.jpg', b)\n"
                "    db.upload_image(\n"
                "        'media-images',\n"
                "        'seasons/1/1.jpg',\n"
                "        b,\n"
                "    )\n"
                "    db.upload_image('media-images', 'tmdb/w185/a.jpg', b, 'image/jpeg', cache_seconds=31536000)\n",
                encoding="utf-8")
            self.assertEqual(without_cache_seconds(fixture), [2, 3])   # positive control: line 8 passes it

    def test_the_slot_uploaders_ask_the_policy_for_the_key_they_upload(self):
        for name in SLOT_UPLOADERS:
            calls = upload_calls((BOT / name).read_text(encoding="utf-8"))
            self.assertTrue(calls, name)
            for call in calls:
                with self.subTest(file=name, line=call.lineno):
                    self.assertTrue(asks_the_policy_for_its_key(call))

    def test_the_policy_check_refuses_a_literal_or_another_key(self):
        def check(src):
            return asks_the_policy_for_its_key(upload_calls(src)[0])

        self.assertFalse(check("db.upload_image(B, f'posters/{i}.jpg', b, cache_seconds=604800)"))
        self.assertFalse(check("db.upload_image(B, f'posters/{i}.jpg', b, cache_seconds=cache_seconds_for('x'))"))
        self.assertFalse(check("db.upload_image(B, (key := f'posters/{i}.jpg'), b, cache_seconds=cache_seconds_for(k))"))
        self.assertFalse(check("db.upload_image(B, f'posters/{i}.jpg', b)"))
        # Positive controls: the same expression, or the name a := binds.
        self.assertTrue(check("db.upload_image(B, f'posters/{i}.jpg', b, cache_seconds=cache_seconds_for(f'posters/{i}.jpg'))"))
        self.assertTrue(check("db.upload_image(B, (key := f'posters/{i}.jpg'), b, cache_seconds=cache_seconds_for(key))"))


# ── (4) the callers, at the wire ──────────────────────────────────────────

class CallerWireTest(unittest.TestCase):

    def test_the_wire_tells_a_cache_from_none(self):
        # Positive control for the pins below: an upload without cache_seconds sends no cacheControl
        # field (the one the server stores: no-cache), and storage3's own default header, not a max-age.
        with _routed() as (db, caught):
            db.upload_image("media-images", "posters/603.jpg", DATA)
        (key, field, header), = _uploads(caught)
        self.assertEqual((key, field), ("posters/603.jpg", None))
        self.assertFalse((header or "").startswith("max-age="), header)

    def test_migrate_poster_asks_seven_days_for_its_slot(self):
        with _routed() as (db, caught), mock.patch.object(migrate_images, "download_image", lambda url: DATA):
            url = migrate_images.migrate_poster(db, 603, "x", "https://image.tmdb.org/t/p/w500/abc.jpg")
        self.assertEqual(_uploads(caught), [("posters/603.jpg", "604800", "max-age=604800")])
        self.assertTrue(url.startswith(f"{FAKE_URL}/storage/v1/object/public/media-images/posters/603.jpg"))

    def test_refresh_poster_asks_a_year_for_its_tmdb_file(self):
        with _routed() as (db, caught), mock.patch.object(migrate_images, "download_image", lambda url: DATA):
            url = migrate_images.refresh_poster(db, 603, "m-1", "/kvuN0aKiDPXSfmTg5ICh.jpg")
        self.assertEqual(_uploads(caught), [("posters/603/kvuN0aKiDPXSfmTg5ICh.jpg", "31536000", "max-age=31536000")])
        self.assertIsNotNone(url)

    def test_migrate_carousel_asks_seven_days_for_each_slot(self):
        tmdb = types.SimpleNamespace(_get=lambda path: {"backdrops": [{"file_path": "/b0.jpg"}, {"file_path": "/b1.jpg"}]})
        with _routed() as (db, caught), mock.patch.object(migrate_images, "download_image", lambda url: DATA):
            urls = migrate_images.migrate_carousel(db, tmdb, 603, "x", "movie")
        self.assertEqual(_uploads(caught), [("carousels/603/0.jpg", "604800", "max-age=604800"),
                                            ("carousels/603/1.jpg", "604800", "max-age=604800")])
        self.assertEqual(len(urls), 2)

    def test_a_season_poster_asks_seven_days_for_its_slot(self):
        class FakeTmdb:
            def __init__(self, api_key):
                pass

            def get_seasons(self, tmdb_id):
                return [{"season_number": 1, "poster_path": "/s1.jpg"}, {"season_number": 2, "poster_path": "/s2.jpg"}]

            def get_season_episodes(self, tmdb_id, season_number):
                return None, []

        env = {"TMDB_API_KEY": "fake-tmdb-key", "SUPABASE_URL": FAKE_URL, "SUPABASE_KEY": FAKE_KEY}
        shows = {"media_seasons": [], "media": [{"id": "m-1", "tmdb_id": 1399, "title": "Fake Show",
                                                 "status": "Returning Series"}]}
        with _routed(shows) as (_, caught), \
                mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.object(backfill_seasons, "TmdbClient", FakeTmdb), \
                mock.patch.object(backfill_seasons, "download_image", lambda url: DATA), \
                mock.patch.object(backfill_seasons.time, "sleep", lambda s: None), \
                mock.patch.object(SupabaseClient, "upsert_seasons", lambda self, media_id, seasons: []), \
                mock.patch.object(SupabaseClient, "update_tv_runtime", lambda self, media_id: None):
            backfill_seasons.main()
        self.assertEqual(_uploads(caught), [("seasons/1399/1.jpg", "604800", "max-age=604800"),
                                            ("seasons/1399/2.jpg", "604800", "max-age=604800")])


if __name__ == "__main__":
    unittest.main()
