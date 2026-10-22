"""
Unit tests for bot.backfill_title_logo_luminance — the backfill that
measures the catalogue's existing title logos.

Driven with a fake Supabase client that records every query chain and
answers canned pages, a fake measure, a fake sleep and a fake clock. No
network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import itertools
import os
import types
import unittest
from unittest import mock

from bot import backfill_title_logo_luminance as backfill_mod

TOL = 1e-9  # the measure is passed through, not computed: any drift is a bug

FAKE_ENV = {
    "SUPABASE_URL": "https://project.supabase.invalid",
    "SUPABASE_KEY": "fake-supabase-key",
}


class _FakeQuery:
    def __init__(self, db: "_FakeSupabase", table: str):
        self._db = db
        self.table = table
        self.ops = []

    @property
    def not_(self):
        self.ops.append(("not_", (), {}))
        return self

    # Only the builder methods the script uses: a misspelt or nonexistent
    # PostgREST method fails here as it would against supabase-py.
    METHODS = {"select", "is_", "order", "range", "limit", "update", "eq"}

    def __getattr__(self, name):
        if name not in self.METHODS:
            raise AttributeError(name)

        def record(*args, **kwargs):
            self.ops.append((name, args, kwargs))
            return self

        return record

    def op(self, name):
        return next((o for o in self.ops if o[0] == name), None)

    @property
    def is_count(self) -> bool:
        select = self.op("select")
        return bool(select and select[2].get("count"))

    def execute(self):
        self._db.executed.append(self)
        upd, eq = self.op("update"), self.op("eq")
        if upd and eq and eq[1][1] in self._db.fail_update_ids:
            raise RuntimeError("simulated write failure")
        rows = self._db.rows
        rng = self.op("range")
        if rng:
            start, end = rng[1]
            rows = rows[start:end + 1]
        return types.SimpleNamespace(data=list(rows), count=self._db.left if self.is_count else None)


class _FakeSupabase:
    def __init__(self, rows, left=0, fail_update_ids=()):
        self.client = self
        self.rows = rows
        self.left = left
        self.fail_update_ids = set(fail_update_ids)
        self.executed = []

    def table(self, name):
        return _FakeQuery(self, name)

    @property
    def page_reads(self):
        return [q for q in self.executed if q.op("select") and not q.is_count]

    @property
    def updates(self):
        out = []
        for q in self.executed:
            upd = q.op("update")
            if upd:
                out.append((q.table, upd[1][0], q.op("eq")[1]))
        return out


class _FakeMeasure:
    def __init__(self, answers: dict):
        self.answers = answers  # url → value (None = unmeasurable)
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        return self.answers[url]


def _rows(n: int) -> list[dict]:
    return [{"id": i, "title_logo_url": f"https://image.tmdb.invalid/t/p/w500/{i}.png"}
            for i in range(1, n + 1)]


def _run(db, measure, clock=None, budget_seconds=None, sleeps=None):
    out = io.StringIO()
    kwargs = {}
    if budget_seconds is not None:
        kwargs["budget_seconds"] = budget_seconds
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        result = backfill_mod.backfill(
            db,
            measure=measure,
            sleep=(sleeps.append if sleeps is not None else (lambda s: None)),
            clock=clock or itertools.repeat(1_000_000.0).__next__,  # never 0: elapsed, not absolute
            **kwargs,
        )
    return result, out.getvalue()


def _filters(query) -> list:
    """The filter ops of a query chain, in order (select/order/range/limit dropped)."""
    return [(name, args) for name, args, _ in query.ops if name in ("not_", "is_", "eq", "neq")]


EXPECTED_FILTERS = [
    ("not_", ()), ("is_", ("title_logo_url", "null")),
    ("is_", ("title_logo_luminance", "null")),
]


class SelectionTest(unittest.TestCase):

    def test_selects_rows_with_a_logo_and_no_luminance(self):
        rows = _rows(1)
        db = _FakeSupabase(rows)

        _run(db, _FakeMeasure({rows[0]["title_logo_url"]: 0.5}))

        self.assertEqual(len(db.page_reads), 1)
        self.assertEqual(_filters(db.page_reads[0]), EXPECTED_FILTERS)
        # The "left" count uses the same selection, or it would count the wrong thing.
        counts = [q for q in db.executed if q.is_count]
        self.assertEqual(len(counts), 1)
        self.assertEqual(_filters(counts[0]), EXPECTED_FILTERS)
        self.assertEqual(db.page_reads[0].op("select")[1], ("id, title_logo_url",))
        self.assertEqual(counts[0].op("select")[2].get("count"), "exact")

    def test_every_page_is_read_before_the_first_update(self):
        rows = _rows(5)
        db = _FakeSupabase(rows)
        measure = _FakeMeasure({r["title_logo_url"]: 0.5 for r in rows})

        with mock.patch.object(backfill_mod, "PAGE_SIZE", 2):
            _run(db, measure)

        kinds = ["read" if q in db.page_reads else "update" if q.op("update") else "other"
                 for q in db.executed]
        self.assertEqual([q.op("range")[1] for q in db.page_reads], [(0, 1), (2, 3), (4, 5)])
        self.assertEqual(kinds.count("update"), 5)  # control: there ARE writes
        self.assertLess(max(i for i, k in enumerate(kinds) if k == "read"),
                        min(i for i, k in enumerate(kinds) if k == "update"))


class MeasureAndWriteTest(unittest.TestCase):

    def test_a_page_of_three_measures_three_writes_two_and_leaves_the_null_alone(self):
        rows = _rows(3)
        urls = [r["title_logo_url"] for r in rows]
        measure = _FakeMeasure({urls[0]: 0.6123, urls[1]: None, urls[2]: 0.2471})
        db = _FakeSupabase(rows, left=1)

        result, out = _run(db, measure)

        self.assertEqual(measure.calls, urls)
        self.assertEqual([(t, eq) for t, _, eq in db.updates], [("media", ("id", 1)), ("media", ("id", 3))])
        for (_, payload, _), expected in zip(db.updates, (0.6123, 0.2471)):
            self.assertEqual(set(payload), {"title_logo_luminance"})
            self.assertAlmostEqual(payload["title_logo_luminance"], expected, delta=TOL)
        # The unmeasurable row is never written — not as 0, not at all.
        self.assertNotIn(("id", 2), [eq for _, _, eq in db.updates])
        self.assertEqual(result, {"measured": 2, "unmeasurable": 1, "total": 3, "left": 1})
        # "Left" is read at the END, after the writes, or it reports the
        # backlog the run started with.
        kinds = ["count" if q.is_count else "update" if q.op("update") else "read" for q in db.executed]
        self.assertEqual(kinds.count("update"), 2)  # control: there are writes before it
        self.assertEqual(kinds[-1], "count")

    def test_a_measured_zero_is_written_not_left_null(self):
        # 0.0 is the darkest logo of all — case D itself. A truthiness slip
        # (`if not value`) would treat it as unmeasurable and never write it.
        rows = _rows(2)
        urls = [r["title_logo_url"] for r in rows]
        db = _FakeSupabase(rows)

        result, _ = _run(db, _FakeMeasure({urls[0]: 0.0, urls[1]: None}))

        self.assertEqual([eq for _, _, eq in db.updates], [("id", 1)])
        self.assertIsNotNone(db.updates[0][1].get("title_logo_luminance"))
        self.assertAlmostEqual(db.updates[0][1]["title_logo_luminance"], 0.0, delta=TOL)
        # Control: the None beside it is the one left alone and counted.
        self.assertEqual((result["measured"], result["unmeasurable"]), (1, 1))

    def test_the_summary_line_reports_the_counts_and_what_is_left(self):
        rows = _rows(3)
        urls = [r["title_logo_url"] for r in rows]
        db = _FakeSupabase(rows, left=1)

        _, out = _run(db, _FakeMeasure({urls[0]: 0.6, urls[1]: None, urls[2]: 0.2}))

        self.assertTrue(out.endswith(
            "[LUMINANCE] Done. measured 2 / unmeasurable 1 / total 3 · 1 still NULL\n"
        ), out)

    def test_it_sleeps_between_fetches(self):
        rows = _rows(3)
        urls = [r["title_logo_url"] for r in rows]
        sleeps = []

        # The middle fetch fails to measure: it was still a fetch.
        _run(_FakeSupabase(rows), _FakeMeasure({urls[0]: 0.5, urls[1]: None, urls[2]: 0.5}),
             sleeps=sleeps)

        self.assertEqual(len(sleeps), 3)  # one per fetch
        for seconds in sleeps:
            self.assertAlmostEqual(seconds, 0.05, delta=TOL)

    def test_progress_every_100(self):
        rows = _rows(250)
        answers = {r["title_logo_url"]: 0.5 for r in rows}
        answers[rows[99]["title_logo_url"]] = None  # row 100 is unmeasurable

        _, out = _run(_FakeSupabase(rows), _FakeMeasure(answers))

        progress = [l for l in out.splitlines() if l.startswith("[LUMINANCE] ") and "/250 " in l]
        self.assertEqual(progress, [
            "[LUMINANCE] 100/250 — measured 99, unmeasurable 1",
            "[LUMINANCE] 200/250 — measured 199, unmeasurable 1",
        ])


class FailedWriteTest(unittest.TestCase):

    def test_a_failed_write_is_logged_and_the_run_still_reaches_its_summary(self):
        rows = _rows(3)
        urls = [r["title_logo_url"] for r in rows]
        db = _FakeSupabase(rows, left=1, fail_update_ids={2})

        result, out = _run(db, _FakeMeasure({u: 0.5 for u in urls}))

        failures = [line for line in out.splitlines() if "write failed" in line]
        self.assertEqual(failures, [f"[LUMINANCE] {urls[1]}: write failed (RuntimeError)"])
        self.assertTrue(out.endswith(
            "[LUMINANCE] Done. measured 2 / unmeasurable 1 / total 3 · 1 still NULL\n"), out)
        self.assertEqual((result["measured"], result["unmeasurable"]), (2, 1))
        # Control: without the failure, all three are measured and nothing is logged.
        result, out = _run(_FakeSupabase(rows), _FakeMeasure({u: 0.5 for u in urls}))
        self.assertEqual(result["measured"], 3)
        self.assertNotIn("write failed", out)


class TimeBudgetTest(unittest.TestCase):

    def test_the_budget_stops_the_run_and_the_summary_still_prints(self):
        rows = _rows(5)
        measure = _FakeMeasure({r["title_logo_url"]: 0.5 for r in rows})
        clock = itertools.count(1_000_000, 10).__next__  # start, then +10 s per check

        result, out = _run(_FakeSupabase(rows, left=3), measure, clock=clock, budget_seconds=25)

        self.assertEqual(len(measure.calls), 2)
        self.assertIn("[LUMINANCE] Time budget reached after 2/5 — stopping; "
                      "dispatch again to continue.\n", out)
        self.assertTrue(out.endswith(
            "[LUMINANCE] Done. measured 2 / unmeasurable 0 / total 5 · 3 still NULL\n"), out)
        self.assertEqual(result["left"], 3)
        # Control: with room in the budget, every row is measured and no stop line.
        measure = _FakeMeasure({r["title_logo_url"]: 0.5 for r in rows})
        _, out = _run(_FakeSupabase(rows), measure, clock=itertools.count(1_000_000, 10).__next__,
                      budget_seconds=1000)
        self.assertEqual(len(measure.calls), 5)
        self.assertNotIn("Time budget reached", out)

    def test_backfill_applies_the_default_budget_when_none_is_passed(self):
        rows = _rows(3)
        measure = _FakeMeasure({r["title_logo_url"]: 0.5 for r in rows})
        t0 = 5000.0
        ticks = iter([t0, t0 + 170 * 60 - 1, t0 + 170 * 60 + 1])

        _, out = _run(_FakeSupabase(rows), measure, clock=lambda: next(ticks, t0 + 999_999))

        self.assertEqual(len(measure.calls), 1)  # control: it did start
        self.assertIn("[LUMINANCE] Time budget reached after 1/3", out)

    def test_the_default_budget_leaves_room_inside_the_jobs_180_minutes(self):
        self.assertLess(backfill_mod.TIME_BUDGET_SECONDS, 180 * 60)
        self.assertGreater(backfill_mod.TIME_BUDGET_SECONDS, 120 * 60)


class EnvAndMainTest(unittest.TestCase):

    def test_load_env_needs_only_the_supabase_pair(self):
        with mock.patch.dict(os.environ, FAKE_ENV, clear=True):
            self.assertEqual(backfill_mod.load_env(), FAKE_ENV)
        for missing in FAKE_ENV:
            with self.subTest(missing=missing):
                env = {k: v for k, v in FAKE_ENV.items() if k != missing}
                out = io.StringIO()
                with mock.patch.dict(os.environ, env, clear=True), \
                        contextlib.redirect_stdout(out), \
                        self.assertRaises(SystemExit) as cm:
                    backfill_mod.load_env()
                self.assertEqual(cm.exception.code, 1)
                self.assertEqual(
                    out.getvalue(),
                    f"[LUMINANCE] ERROR: Required environment variable '{missing}' is not set.\n",
                )

    def test_main_wires_the_real_measure_and_the_supabase_client(self):
        rows = _rows(2)
        db = _FakeSupabase(rows)
        measure = _FakeMeasure({rows[0]["title_logo_url"]: 0.7, rows[1]["title_logo_url"]: 0.1})
        made, sleeps = [], []
        out = io.StringIO()
        with mock.patch.dict(os.environ, FAKE_ENV), \
                mock.patch.object(backfill_mod, "SupabaseClient", lambda **kw: made.append(kw) or db), \
                mock.patch.object(backfill_mod, "luminance_for_logo", measure), \
                mock.patch.object(backfill_mod.time, "sleep", sleeps.append), \
                contextlib.redirect_stdout(out):
            backfill_mod.main()

        self.assertEqual(made, [{"url": FAKE_ENV["SUPABASE_URL"], "key": FAKE_ENV["SUPABASE_KEY"]}])
        self.assertEqual(len(sleeps), 2)  # the real throttle, once per fetch
        for seconds in sleeps:
            self.assertAlmostEqual(seconds, 0.05, delta=TOL)

        self.assertEqual(measure.calls, [r["title_logo_url"] for r in rows])
        self.assertEqual([eq for _, _, eq in db.updates], [("id", 1), ("id", 2)])
        self.assertIn("[LUMINANCE] Done. measured 2 / unmeasurable 0 / total 2", out.getvalue())


if __name__ == "__main__":
    unittest.main()
