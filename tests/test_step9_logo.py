"""
Unit tests for the nightly's Step 9 logo fields — bot.main.step9_logo_fields —
and for SupabaseClient.upsert_media carrying title_logo_luminance beside
title_logo_url (B1's third writer).

Fakes only: a TMDb client that answers a canned logo URL (or None), a fake
measure that counts its calls, and a fake Supabase client that records the
upsert payload. No network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import ast
import contextlib
import inspect
import io
import pathlib
import types
import unittest

from bot import main as bot_main
from bot.logo_luminance import luminance_for_logo
from bot.supabase_client import SupabaseClient
from bot.tmdb import TmdbClient

TOL = 1e-9  # the luminance is passed through, not computed: any drift is a bug

LOGO_URL = "https://image.tmdb.invalid/t/p/w500/logo.png"
IMAGES = {"logos": [{"iso_639_1": "en", "file_path": "/logo.png"}]}  # opaque to the helper


class _FakeTmdb:
    def __init__(self, logo=LOGO_URL):
        self.logo = logo
        self.calls = []  # kwargs of every chooser call, its measure left out

    def get_title_logo(self, **kwargs):
        self.calls.append(kwargs)
        return self.logo

    def choose_title_logo(self, **kwargs):
        # B4: Step 9 calls the chooser. This fake follows the new call: its canned logo,
        # measured once by the measure Step 9 hands it — what get_title_logo and Step 9's
        # own measure did before.
        measure = kwargs.pop("measure")
        url = self.get_title_logo(**kwargs)
        return url, (measure(url) if url else None)


DARK = "https://image.tmdb.invalid/t/p/w500/dark.png"
LIGHT = "https://image.tmdb.invalid/t/p/w500/light.png"


class _ChooserTmdb:
    """B4: canned candidates, the REAL choose_title_logo."""

    choose_title_logo = TmdbClient.choose_title_logo

    def __init__(self, candidates):
        self.candidates, self.calls = list(candidates), []

    def title_logo_candidates(self, tmdb_id, media_type, data=None):
        self.calls.append({"tmdb_id": tmdb_id, "media_type": media_type, "data": data})
        return list(self.candidates)

    def get_title_logo(self, **kwargs):
        # The rule until B4 — the first candidate — for a Step 9 that still calls it.
        return self.candidates[0] if self.candidates else None


class _FakeMeasure:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        return self.value


# ── bot.main.step9_logo_fields ────────────────────────────────────────────

class StepNineLogoFieldsTest(unittest.TestCase):

    def test_returns_the_url_and_its_measured_luminance(self):
        tmdb, measure = _FakeTmdb(), _FakeMeasure(0.8312)

        fields = bot_main.step9_logo_fields(tmdb, 700, "movie", IMAGES, measure=measure)

        self.assertEqual(set(fields), {"title_logo_url", "title_logo_luminance"})
        self.assertEqual(fields["title_logo_url"], LOGO_URL)
        self.assertIsInstance(fields["title_logo_luminance"], float)
        self.assertAlmostEqual(fields["title_logo_luminance"], 0.8312, delta=TOL)
        # The logo is chosen exactly as Step 9 chose it: the prefetched images.
        self.assertEqual(tmdb.calls, [{"tmdb_id": 700, "media_type": "movie", "data": IMAGES}])
        self.assertEqual(measure.calls, [LOGO_URL])

    def test_no_logo_gives_two_nones_and_never_measures(self):
        measure = _FakeMeasure(0.8312)

        fields = bot_main.step9_logo_fields(_FakeTmdb(logo=None), 700, "movie", IMAGES, measure=measure)

        self.assertEqual(fields, {"title_logo_url": None, "title_logo_luminance": None})
        self.assertEqual(measure.calls, [])  # no fetch for a title without a logo
        # Control: with a logo, the same measure is called.
        bot_main.step9_logo_fields(_FakeTmdb(), 700, "movie", IMAGES, measure=measure)
        self.assertEqual(measure.calls, [LOGO_URL])

    def test_a_measured_zero_is_kept_not_turned_into_null(self):
        # 0.0 is the darkest logo of all — case D itself.
        fields = bot_main.step9_logo_fields(_FakeTmdb(), 700, "movie", IMAGES, measure=_FakeMeasure(0.0))

        self.assertIsNotNone(fields.get("title_logo_luminance"))
        self.assertAlmostEqual(fields["title_logo_luminance"], 0.0, delta=TOL)
        # Control: an unmeasured logo is the one that comes back None.
        fields = bot_main.step9_logo_fields(_FakeTmdb(), 700, "movie", IMAGES, measure=_FakeMeasure(None))
        self.assertEqual(fields["title_logo_url"], LOGO_URL)
        self.assertIsNone(fields["title_logo_luminance"])

    def test_measure_is_called_exactly_once_per_logo(self):
        measure = _FakeMeasure(0.5)
        logos = [LOGO_URL, None, "https://image.tmdb.invalid/t/p/w500/b.svg", None]

        for i, logo in enumerate(logos):
            bot_main.step9_logo_fields(_FakeTmdb(logo=logo), i, "tv", IMAGES, measure=measure)

        self.assertEqual(measure.calls, [LOGO_URL, "https://image.tmdb.invalid/t/p/w500/b.svg"])

    def test_the_nightly_measures_with_the_one_definition(self):
        # Step 9 calls the helper without `measure`, so the default is what runs.
        default = inspect.signature(bot_main.step9_logo_fields).parameters["measure"].default
        self.assertIs(default, luminance_for_logo)

    def test_an_empty_images_payload_is_passed_through_as_is(self):
        tmdb = _FakeTmdb(logo=None)

        bot_main.step9_logo_fields(tmdb, 700, "movie", {}, measure=_FakeMeasure(0.5))

        self.assertEqual(tmdb.calls, [{"tmdb_id": 700, "media_type": "movie", "data": {}}])

    def test_b4_step_9_writes_the_choosers_winner_and_its_luminance(self):
        values = {DARK: 0.011, LIGHT: 0.6}
        calls = []

        def by_url(url):
            calls.append(url)
            return values[url]

        tmdb = _ChooserTmdb([DARK, LIGHT])
        fields = bot_main.step9_logo_fields(tmdb, 700, "movie", IMAGES, measure=by_url)
        self.assertEqual(fields, {"title_logo_url": LIGHT, "title_logo_luminance": 0.6})
        self.assertEqual(calls, [DARK, LIGHT])                       # each measured once, none again
        self.assertEqual(tmdb.calls, [{"tmdb_id": 700, "media_type": "movie", "data": IMAGES}])

    def test_b4_step_9_a_light_first_candidate_is_one_fetch(self):
        calls = []

        def by_url(url):
            calls.append(url)
            return {DARK: 0.011, LIGHT: 0.6}[url]

        fields = bot_main.step9_logo_fields(_ChooserTmdb([LIGHT, DARK]), 700, "movie", IMAGES, measure=by_url)
        self.assertEqual(fields, {"title_logo_url": LIGHT, "title_logo_luminance": 0.6})
        self.assertEqual(calls, [LIGHT])

    def test_step9_record_spreads_the_helper_in_place_of_the_logo_key(self):
        # STRUCTURAL PIN, declared as such: main() is too wide to drive with
        # fakes (twelve TMDb list endpoints before Step 9, eight steps after),
        # so this one reads the source. Step 9's record must spread the helper
        # — with images_data and the default measure — exactly where the logo
        # key was, and carry no logo key of its own.
        tree = ast.parse(pathlib.Path(bot_main.__file__).read_text(encoding="utf-8"))
        main_fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
        records = [n.value for n in ast.walk(main_fn) if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "media_record" for t in n.targets)]
        self.assertEqual(len(records), 1)
        record = records[0]
        keys = [k.value if isinstance(k, ast.Constant) else None for k in record.keys]
        spreads = [ast.unparse(v) for k, v in zip(record.keys, record.values) if k is None]

        self.assertEqual(spreads, ["step9_logo_fields(tmdb, tmdb_id, media_type, images_data)"])
        self.assertNotIn("title_logo_url", keys)
        self.assertNotIn("title_logo_luminance", keys)
        # Control: the rest of the record is where it was — the spread sits
        # between runtime and certification, as the logo key did.
        self.assertEqual(keys.index(None), keys.index("runtime") + 1)
        self.assertEqual(keys.index("certification"), keys.index(None) + 1)
        self.assertEqual(keys[:3], ["tmdb_id", "title", "overview"])


# ── SupabaseClient.upsert_media ───────────────────────────────────────────

class _FakeTable:
    def __init__(self, client, name):
        self._client = client
        self._name = name
        self._is_select = False

    def upsert(self, payload, **kwargs):
        self._client.upserts.append((self._name, payload, kwargs))
        return self

    def select(self, *args, **kwargs):
        self._is_select = True
        return self

    def eq(self, *args, **kwargs):
        return self

    def maybe_single(self):
        return self

    def execute(self):
        return types.SimpleNamespace(data={"id": 42} if self._is_select else [])


class _FakeClient:
    def __init__(self):
        self.upserts = []

    def table(self, name):
        return _FakeTable(self, name)


def _upsert(media_dict: dict):
    """Run the real upsert_media against a fake client; return (payload, kwargs, id)."""
    db = SupabaseClient.__new__(SupabaseClient)  # no create_client: nothing real is built
    db.client = _FakeClient()
    with contextlib.redirect_stdout(io.StringIO()):
        row_id = db.upsert_media(media_dict)
    (table, payload, kwargs), = db.client.upserts
    assert table == "media"
    return payload, kwargs, row_id


def _record(**extra) -> dict:
    record = {
        "tmdb_id": 700, "title": "Dune", "overview": "…", "poster_path": "/p.jpg",
        "media_type": "movie", "release_date": "2021-10-22", "is_in_theatres": False,
        "is_streamable_now": False, "popularity": 50.0, "vote_count": 1000,
        "tmdb_score": 78, "status": "Released", "certification": "PG-13",
        "genres": ["Sci-Fi"], "original_language": "en", "runtime": 155,
    }
    record.update(extra)
    return record


class UpsertMediaLuminanceTest(unittest.TestCase):

    def test_the_payload_carries_the_luminance_the_dict_brings(self):
        payload, _, _ = _upsert(_record(title_logo_url=LOGO_URL, title_logo_luminance=0.8312))

        self.assertEqual(payload["title_logo_url"], LOGO_URL)
        self.assertIsInstance(payload["title_logo_luminance"], float)
        self.assertAlmostEqual(payload["title_logo_luminance"], 0.8312, delta=TOL)

    def test_a_measured_zero_reaches_the_payload_as_zero(self):
        # 0.0 is the darkest logo of all — case D. `... or None` would store NULL.
        payload, _, _ = _upsert(_record(title_logo_url=LOGO_URL, title_logo_luminance=0.0))

        self.assertIsNotNone(payload["title_logo_luminance"])
        self.assertAlmostEqual(payload["title_logo_luminance"], 0.0, delta=TOL)
        # Control: an absent luminance is the one that lands as None.
        payload, _, _ = _upsert(_record(title_logo_url=LOGO_URL))
        self.assertIsNone(payload["title_logo_luminance"])

    def test_parity_a_dict_without_the_keys_writes_null_to_both(self):
        # bulk_import / sync_changes / coverage_sweep pass no logo: the payload
        # writes NULL to both columns, as it already does for the logo.
        for media_type in ("movie", "tv"):
            with self.subTest(media_type=media_type):
                payload, _, _ = _upsert(_record(media_type=media_type))

                self.assertIn("title_logo_luminance", payload)
                self.assertIsNone(payload["title_logo_luminance"])
                # Control: the logo key, which upsert_media has always copied, is the same.
                self.assertIn("title_logo_url", payload)
                self.assertIsNone(payload["title_logo_url"])

    def test_the_rest_of_the_payload_is_as_before(self):
        # Control: what upsert_media did before this change still holds.
        payload, kwargs, row_id = _upsert(_record(title_logo_url=LOGO_URL, title_logo_luminance=0.5))

        self.assertEqual(kwargs, {"on_conflict": "tmdb_id"})
        self.assertEqual(row_id, 42)
        self.assertEqual(payload["certification"], "PG-13")
        self.assertEqual(payload["genres"], ["Sci-Fi"])
        self.assertEqual(payload["runtime"], 155)
        self.assertIn("popularity_score", payload)
        # A pre-release title still loses its RT score, as before.
        payload, _, _ = _upsert(_record(status="Rumored", rt_score=90))
        self.assertIsNone(payload["rt_score"])


if __name__ == "__main__":
    unittest.main()
