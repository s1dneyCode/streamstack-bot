"""
Unit tests for the two writers of media.title_logo_url that this run touches
— enrich_new_titles.py Step 5 and backfill_title_logos.py — and the
luminance each now writes beside the URL, in the same payload.

Each script's row handler is driven directly, and each main() once, with
fakes: a TMDb client that answers a canned logo URL, a Supabase client that
records every query chain, and luminance_for_logo swapped on the module for
a fake that counts its calls. No network, no database. Run from the repo
root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import sys
import types
import unittest
from unittest import mock

from bot import backfill_title_logos, enrich_new_titles

TOL = 1e-9  # the luminance is passed through, not computed: any drift is a bug

LOGO_URL = "https://image.tmdb.invalid/t/p/w500/logo.png"

# Fake credentials for the main() tests: shaped like the real ones, usable
# for nothing (the clients they would build are replaced by fakes anyway).
FAKE_ENV = {
    "TMDB_API_KEY": "fake-tmdb-key",
    "SUPABASE_URL": "https://project.supabase.invalid",
    "SUPABASE_KEY": "fake-supabase-key",
}


# ── Fakes ──────────────────────────────────────────────────────────────────

class _FakeQuery:
    """One PostgREST query chain: records every call, answers on execute()."""

    def __init__(self, db: "_FakeSupabase", table: str):
        self._db = db
        self.table = table
        self.ops = []  # [(name, args, kwargs)] in call order

    @property
    def not_(self):
        self.ops.append(("not_", (), {}))
        return self

    # Only the builder methods these scripts use: a misspelt or nonexistent
    # PostgREST method fails here as it would against supabase-py.
    METHODS = {"select", "eq", "is_", "range", "limit", "order", "update"}

    def __getattr__(self, name):
        if name not in self.METHODS:
            raise AttributeError(name)

        def record(*args, **kwargs):
            self.ops.append((name, args, kwargs))
            return self

        return record

    def op(self, name):
        return next((o for o in self.ops if o[0] == name), None)

    def execute(self):
        self._db.executed.append(self)
        rows = self._db.tables.get(self.table, [])
        rng = self.op("range")
        if rng:
            start, end = rng[1]
            rows = rows[start:end + 1]
        select = self.op("select")
        count = self._db.count if select and select[2].get("count") else None
        return types.SimpleNamespace(data=list(rows), count=count)


class _FakeSupabase:
    """Stands in for SupabaseClient: `.client.table(...)` chains are recorded."""

    def __init__(self, tables=None, count=0):
        self.client = self
        self.tables = tables or {}
        self.count = count
        self.executed = []

    def table(self, name):
        return _FakeQuery(self, name)

    def rpc(self, name, params):
        # B3: enrich_new_titles takes its queue from rpc("enrichment_queue");
        # serve the same canned rows the table read used to return.
        rows = self.tables.get("media", [])
        return types.SimpleNamespace(execute=lambda: types.SimpleNamespace(data=list(rows)))

    @property
    def updates(self):
        """[(table, payload, eq_args)] for every executed update, in order."""
        out = []
        for q in self.executed:
            upd = q.op("update")
            if upd:
                eq = q.op("eq")
                out.append((q.table, upd[1][0], eq[1] if eq else None))
        return out


class _FakeTmdb:
    def __init__(self, logo=LOGO_URL, error=None):
        self.logo = logo
        self.error = error
        self.logo_calls = []

    def get_title_logo(self, tmdb_id, media_type, data=None):
        self.logo_calls.append((tmdb_id, media_type))
        if self.error is not None:
            raise self.error
        return self.logo(tmdb_id) if callable(self.logo) else self.logo

    def _get(self, path, params=None):  # enrich Step 0's base detail
        return {}


class _FakeLuminance:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        return self.value


def _captured(fn, *args, **kwargs):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        result = fn(*args, **kwargs)
    return result, out.getvalue()


def _enrich_row(**overrides) -> dict:
    """A movie row that, in main(), reaches Step 5 without any other API call."""
    row = {
        "id": 7, "tmdb_id": 700, "title": "Dune", "media_type": "movie",
        "poster_path": "/p.jpg", "poster_url": "https://cdn.invalid/p.jpg",
        "release_date": "2021-10-22", "is_in_theatres": False,
        "is_streamable_now": False, "title_logo_url": None,
        "certification": "PG-13", "genres": ["Sci-Fi"], "runtime": 155,
    }
    row.update(overrides)
    return row


def _stats() -> dict:
    return {"logos": 0, "errors": 0}


class _PayloadTestCase(unittest.TestCase):

    def assertLogoPayload(self, payload: dict, url: str, luminance):
        """Exactly the two keys; the URL as chosen; the luminance as measured."""
        self.assertEqual(set(payload), {"title_logo_url", "title_logo_luminance"})
        self.assertEqual(payload["title_logo_url"], url)
        if luminance is None:
            self.assertIsNone(payload["title_logo_luminance"])
        else:
            self.assertIsInstance(payload["title_logo_luminance"], float)
            self.assertAlmostEqual(payload["title_logo_luminance"], luminance, delta=TOL)


# ── enrich_new_titles.py · Step 5 ─────────────────────────────────────────

class EnrichStep5Test(_PayloadTestCase):

    def _step5(self, row, tmdb, luminance):
        db = _FakeSupabase()
        stats = _stats()
        with mock.patch.object(enrich_new_titles, "luminance_for_logo", luminance):
            had_error, out = _captured(enrich_new_titles.enrich_title_logo, db, tmdb, row, stats)
        return db, stats, had_error, out

    def test_one_update_carries_the_url_and_its_measured_luminance(self):
        luminance = _FakeLuminance(0.8312)

        db, stats, had_error, out = self._step5(_enrich_row(), _FakeTmdb(), luminance)

        self.assertEqual(len(db.updates), 1)
        table, payload, eq = db.updates[0]
        self.assertEqual((table, eq), ("media", ("id", 7)))
        self.assertLogoPayload(payload, LOGO_URL, 0.8312)
        self.assertEqual(luminance.calls, [LOGO_URL])
        self.assertFalse(had_error)
        self.assertEqual(stats, {"logos": 1, "errors": 0})
        self.assertIn("[ENRICH]   logo: ok · luminance 0.83\n", out)

    def test_an_unmeasured_luminance_is_written_null_and_the_url_still_lands(self):
        db, stats, had_error, out = self._step5(_enrich_row(), _FakeTmdb(), _FakeLuminance(None))

        self.assertEqual(len(db.updates), 1)
        self.assertLogoPayload(db.updates[0][1], LOGO_URL, None)
        self.assertEqual(stats["logos"], 1)
        self.assertIn("[ENRICH]   logo: ok · luminance pending\n", out)
        # Control: the same row with a measure writes the number.
        db, _, _, _ = self._step5(_enrich_row(), _FakeTmdb(), _FakeLuminance(0.4))
        self.assertLogoPayload(db.updates[0][1], LOGO_URL, 0.4)

    def test_a_measured_zero_is_written_as_zero_not_null(self):
        # 0.0 is the darkest logo of all — case D itself. It must land as a
        # number: a truthiness slip (`luminance or None`) would store NULL.
        db, _, _, out = self._step5(_enrich_row(), _FakeTmdb(), _FakeLuminance(0.0))

        self.assertIsNotNone(db.updates[0][1]["title_logo_luminance"])
        self.assertLogoPayload(db.updates[0][1], LOGO_URL, 0.0)
        self.assertIn("[ENRICH]   logo: ok · luminance 0.00\n", out)
        # Control: an unmeasured logo is the one that is NULL and pending.
        db, _, _, out = self._step5(_enrich_row(), _FakeTmdb(), _FakeLuminance(None))
        self.assertIsNone(db.updates[0][1]["title_logo_luminance"])
        self.assertIn("luminance pending", out)

    def test_no_logo_on_tmdb_is_a_completed_attempt_not_an_error(self):
        # An empty result is not an exception: the row must still get its
        # enriched_at, or it is re-scanned on every run.
        luminance = _FakeLuminance(0.83)

        db, stats, had_error, out = self._step5(_enrich_row(), _FakeTmdb(logo=None), luminance)

        self.assertFalse(had_error)
        self.assertEqual(stats, {"logos": 0, "errors": 0})
        self.assertEqual(db.executed, [])
        self.assertEqual(luminance.calls, [])
        self.assertEqual(out, "")
        # Control: with a logo, the same fakes write and count it.
        db, stats, _, _ = self._step5(_enrich_row(), _FakeTmdb(), luminance)
        self.assertEqual(len(db.updates), 1)
        self.assertEqual(stats["logos"], 1)

    def test_a_title_that_already_has_a_logo_is_skipped_as_before(self):
        tmdb, luminance = _FakeTmdb(), _FakeLuminance(0.83)

        db, stats, had_error, out = self._step5(
            _enrich_row(title_logo_url="https://image.tmdb.invalid/t/p/w500/old.png"),
            tmdb, luminance,
        )

        self.assertEqual(db.executed, [])
        self.assertEqual(tmdb.logo_calls, [])
        self.assertEqual(luminance.calls, [])
        self.assertFalse(had_error)
        self.assertEqual(stats, {"logos": 0, "errors": 0})
        self.assertEqual(out, "")
        # Control: without a logo, the same fakes are used and written to.
        db, _, _, _ = self._step5(_enrich_row(), tmdb, luminance)
        self.assertEqual(len(db.updates), 1)

    def test_luminance_is_measured_once_per_written_logo_and_never_otherwise(self):
        luminance = _FakeLuminance(0.83)
        rows_and_logos = [
            (_enrich_row(id=1, tmdb_id=101), LOGO_URL),                    # written
            (_enrich_row(id=2, tmdb_id=102, title_logo_url=LOGO_URL), LOGO_URL),  # skipped
            (_enrich_row(id=3, tmdb_id=103), None),                        # TMDb has none
        ]
        written = []
        for row, logo in rows_and_logos:
            db, _, _, _ = self._step5(row, _FakeTmdb(logo=logo), luminance)
            written.extend(db.updates)

        self.assertEqual(luminance.calls, [LOGO_URL])
        self.assertEqual([eq for _, _, eq in written], [("id", 1)])

    def test_a_failure_is_counted_and_reported_as_before(self):
        db, stats, had_error, out = self._step5(
            _enrich_row(), _FakeTmdb(error=RuntimeError("boom")), _FakeLuminance(0.83)
        )

        self.assertTrue(had_error)
        self.assertEqual(stats, {"logos": 0, "errors": 1})
        self.assertEqual(db.updates, [])
        self.assertIn("[ENRICH]   logo: failed — boom\n", out)
        # Control: the same row without the failure reports no error.
        _, _, had_error, _ = self._step5(_enrich_row(), _FakeTmdb(), _FakeLuminance(0.83))
        self.assertFalse(had_error)

    def test_main_runs_step_5_through_the_handler_with_its_error_semantics(self):
        ok_row = _enrich_row(id=1, tmdb_id=101, title="Dune")
        bad_row = _enrich_row(id=2, tmdb_id=102, title="Arrival")
        db = _FakeSupabase(tables={
            "media": [ok_row, bad_row],
            "media_trailers": [{"media_id": 0}],   # Steps 2+3: already present
            "media_credits": [{"media_id": 0}],
            "streaming_availability": [],          # Step 9: no state change
        })

        def logo(tmdb_id):
            if tmdb_id == 102:
                raise RuntimeError("boom")
            return LOGO_URL

        tmdb = _FakeTmdb(logo=logo)
        luminance = _FakeLuminance(0.8312)
        with mock.patch.dict(os.environ, FAKE_ENV), \
                mock.patch.object(sys, "argv", ["enrich_new_titles"]), \
                mock.patch.object(enrich_new_titles, "SupabaseClient", lambda **kw: db), \
                mock.patch.object(enrich_new_titles, "TmdbClient", lambda **kw: tmdb), \
                mock.patch.object(enrich_new_titles, "luminance_for_logo", luminance), \
                mock.patch.object(enrich_new_titles.time, "sleep", lambda s: None):
            _, out = _captured(enrich_new_titles.main)

        logo_updates = [(eq, p) for t, p, eq in db.updates if "title_logo_url" in p]
        self.assertEqual(len(logo_updates), 1)
        self.assertEqual(logo_updates[0][0], ("id", 1))
        self.assertLogoPayload(logo_updates[0][1], LOGO_URL, 0.8312)
        stamped = [eq for t, p, eq in db.updates if "enriched_at" in p]
        # The row whose Step 5 raised is not stamped; the clean one is.
        self.assertEqual(stamped, [("id", 1)])
        self.assertIn("errors=1", out)


# ── backfill_title_logos.py ───────────────────────────────────────────────

class BackfillTitleLogosTest(_PayloadTestCase):

    def _row_handler(self, row, tmdb, luminance, i=1, total=3):
        db = _FakeSupabase()
        with mock.patch.object(backfill_title_logos, "luminance_for_logo", luminance):
            written, out = _captured(backfill_title_logos.backfill_title_logo, db, tmdb, row, i, total)
        return db, written, out

    @staticmethod
    def _row(**overrides) -> dict:
        row = {"id": 9, "tmdb_id": 900, "title": "Dune", "media_type": "movie"}
        row.update(overrides)
        return row

    def test_one_update_carries_the_url_and_its_measured_luminance(self):
        luminance = _FakeLuminance(0.8312)

        db, written, out = self._row_handler(self._row(), _FakeTmdb(), luminance)

        self.assertTrue(written)
        self.assertEqual(len(db.updates), 1)
        table, payload, eq = db.updates[0]
        self.assertEqual((table, eq), ("media", ("id", 9)))
        self.assertLogoPayload(payload, LOGO_URL, 0.8312)
        self.assertEqual(luminance.calls, [LOGO_URL])
        self.assertEqual(out, f"[LOGOS] 1/3 Dune: {LOGO_URL} · luminance 0.83\n")

    def test_an_unmeasured_luminance_is_written_null_and_the_url_still_lands(self):
        db, written, out = self._row_handler(self._row(), _FakeTmdb(), _FakeLuminance(None))

        self.assertTrue(written)
        self.assertLogoPayload(db.updates[0][1], LOGO_URL, None)
        self.assertEqual(out, f"[LOGOS] 1/3 Dune: {LOGO_URL} · luminance pending\n")
        # Control: with a measure, the number is written.
        db, _, _ = self._row_handler(self._row(), _FakeTmdb(), _FakeLuminance(0.4))
        self.assertLogoPayload(db.updates[0][1], LOGO_URL, 0.4)

    def test_a_measured_zero_is_written_as_zero_not_null(self):
        db, _, out = self._row_handler(self._row(), _FakeTmdb(), _FakeLuminance(0.0))

        self.assertIsNotNone(db.updates[0][1]["title_logo_luminance"])
        self.assertLogoPayload(db.updates[0][1], LOGO_URL, 0.0)
        self.assertEqual(out, f"[LOGOS] 1/3 Dune: {LOGO_URL} · luminance 0.00\n")
        # Control: an unmeasured logo is the one that is NULL and pending.
        db, _, out = self._row_handler(self._row(), _FakeTmdb(), _FakeLuminance(None))
        self.assertIsNone(db.updates[0][1]["title_logo_luminance"])
        self.assertIn("luminance pending", out)

    def test_no_logo_found_writes_nothing_and_measures_nothing(self):
        luminance = _FakeLuminance(0.83)

        db, written, out = self._row_handler(self._row(), _FakeTmdb(logo=None), luminance)

        self.assertFalse(written)
        self.assertEqual(db.executed, [])
        self.assertEqual(luminance.calls, [])
        self.assertEqual(out, "[LOGOS] 1/3 Dune: no logo found\n")
        # Control: when TMDb has a logo, the same fake is measured once.
        self._row_handler(self._row(), _FakeTmdb(), luminance)
        self.assertEqual(luminance.calls, [LOGO_URL])

    def test_main_selects_only_rows_without_a_logo_and_counts_as_before(self):
        rows = [self._row(id=1, tmdb_id=101, title="A"),
                self._row(id=2, tmdb_id=102, title="B"),
                self._row(id=3, tmdb_id=103, title="C")]
        db = _FakeSupabase(tables={"media": rows})
        tmdb = _FakeTmdb(logo=lambda tmdb_id: None if tmdb_id == 102 else LOGO_URL)
        luminance = _FakeLuminance(0.8312)
        sleeps = []
        with mock.patch.dict(os.environ, FAKE_ENV), \
                mock.patch.object(backfill_title_logos, "SupabaseClient", lambda **kw: db), \
                mock.patch.object(backfill_title_logos, "TmdbClient", lambda **kw: tmdb), \
                mock.patch.object(backfill_title_logos, "luminance_for_logo", luminance), \
                mock.patch.object(backfill_title_logos.time, "sleep", sleeps.append):
            _, out = _captured(backfill_title_logos.main)

        reads = [q for q in db.executed if q.op("select")]
        self.assertEqual(len(reads), 1)
        # The whole read chain, exactly as before this run: the columns, the
        # one filter (no .not_ in front of it), the first page's range.
        self.assertEqual(reads[0].ops, [
            ("select", ("id, tmdb_id, title, media_type",), {}),
            ("is_", ("title_logo_url", "null"), {}),
            ("range", (0, 999), {}),
        ])
        self.assertEqual([eq for _, _, eq in db.updates], [("id", 1), ("id", 3)])
        for _, payload, _ in db.updates:
            self.assertLogoPayload(payload, LOGO_URL, 0.8312)
        self.assertEqual(luminance.calls, [LOGO_URL, LOGO_URL])
        self.assertEqual(len(sleeps), 3)  # one per row, as before
        for seconds in sleeps:
            self.assertAlmostEqual(seconds, 0.25, delta=TOL)
        self.assertIn(
            "[LOGOS] Done. 3 titles processed — 2 updated, 1 with no logo found.\n", out
        )


if __name__ == "__main__":
    unittest.main()
