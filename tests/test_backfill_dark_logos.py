"""
Unit tests for the dark-logos backfill (RUN B4 · 0c): the rows already under
the app's line (title_logo_luminance < logo_luminance.MIN_LOGO_LUMINANCE)
get the one chooser's logo when it is a different URL with a MEASURED
luminance higher than the stored one — one update, both keys, one line — and
nothing is written otherwise.

Fakes only: a Supabase client whose media table applies the filters, order,
ranges and writes the script sends; a TMDb client whose candidates are canned
per title (or raise); a measure that answers a value per URL and records its
calls; a clock the test hands in. The workflow is read as text. No network,
no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import pathlib
import re
import types
import unittest
from unittest import mock

import requests

from bot import backfill_dark_logos as mod
from bot.logo_luminance import MIN_LOGO_LUMINANCE, luminance_for_logo
from tests.test_workflow_posters_hd import secrets_named, step_env
from tests.test_workflow_tmdb_mirror import IMAGE_WRITERS

BACKFILLS = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "backfills.yml"
CDN = "https://image.tmdb.org/t/p/w500/"
FAKE_ENV = {"TMDB_API_KEY": "fake-tmdb-key", "SUPABASE_URL": "https://project.supabase.invalid",
            "SUPABASE_KEY": "fake-supabase-key"}


# ── the fakes ─────────────────────────────────────────────────────────────

class _Query:
    def __init__(self, db):
        self.db, self.ops = db, []

    def _op(self, name, *args, **kwargs):
        self.ops.append((name, args, kwargs))
        return self

    def select(self, *a, **kw): return self._op("select", *a, **kw)
    def lt(self, *a, **kw): return self._op("lt", *a, **kw)
    def order(self, *a, **kw): return self._op("order", *a, **kw)
    def range(self, *a, **kw): return self._op("range", *a, **kw)
    def limit(self, *a, **kw): return self._op("limit", *a, **kw)
    def update(self, *a, **kw): return self._op("update", *a, **kw)
    def eq(self, *a, **kw): return self._op("eq", *a, **kw)

    def named(self, name):
        return [o for o in self.ops if o[0] == name]

    def execute(self):
        self.db.executed.append(self)
        if self.named("update"):
            fields = self.named("update")[0][1][0]
            (col, value), = [o[1] for o in self.named("eq")]
            if value in self.db.fail_writes:
                raise RuntimeError("write failed")
            for r in self.db.rows:
                if r[col] == value:
                    r.update(fields)
            return types.SimpleNamespace(data=[], count=None)
        rows = list(self.db.rows)
        for _, (col, bound), _ in self.named("lt"):
            rows = [r for r in rows if r.get(col) is not None and r[col] < bound]
        if self.named("select")[0][2].get("count"):
            return types.SimpleNamespace(data=rows[:1], count=len(rows))
        for _, (col,), kw in self.named("order"):
            rows.sort(key=lambda r: r[col], reverse=bool(kw.get("desc")))
        for _, (start, end), _ in self.named("range"):
            rows = rows[start:end + 1]
        cols = [c.strip() for c in self.named("select")[0][1][0].split(",")]
        return types.SimpleNamespace(data=[{c: r.get(c) for c in cols} for r in rows], count=None)


class _Db:
    def __init__(self, rows, fail_writes=()):
        self.rows, self.fail_writes, self.executed = [dict(r) for r in rows], set(fail_writes), []
        self.client = self

    def table(self, name):
        assert name == "media", name
        return _Query(self)

    @property
    def writes(self):
        return [(q.named("update")[0][1][0], q.named("eq")[0][1]) for q in self.executed if q.named("update")]

    @property
    def reads(self):
        return [q for q in self.executed if q.named("select") and not q.named("select")[0][2].get("count")]


class _Tmdb:
    """Candidates per tmdb_id: a list of URLs, or an exception to raise."""

    def __init__(self, answers):
        self.answers, self.calls = dict(answers), []

    def title_logo_candidates(self, tmdb_id, media_type, data=None):
        self.calls.append((tmdb_id, media_type))
        answer = self.answers.get(tmdb_id, [])
        if isinstance(answer, BaseException):
            raise answer
        return list(answer)


class _Measure:
    def __init__(self, values):
        self.values, self.calls = dict(values), []

    def __call__(self, url):
        self.calls.append(url)
        return self.values.get(url)


class _Clock:
    """Seconds: `step` more on every call."""

    def __init__(self, step=0.0):
        self.t, self.step = 0.0, step

    def __call__(self):
        value = self.t
        self.t += self.step
        return value


def _row(i, lum, url=None, popularity=10.0, title=None):
    return {"id": f"m-{i:03d}", "tmdb_id": 1000 + i, "media_type": "movie", "title": title or f"Title {i}",
            "popularity": popularity, "title_logo_url": url or f"{CDN}stored{i}.png", "title_logo_luminance": lum}


def _404():
    response = requests.Response()
    response.status_code = 404
    return requests.HTTPError("404 Client Error", response=response)


def _run(db, tmdb, measure, clock=None, budget_seconds=mod.TIME_BUDGET_SECONDS):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        stats = mod.backfill(db, tmdb, measure=measure, clock=clock or _Clock(), budget_seconds=budget_seconds)
    return stats, out.getvalue()


def _tokens(out):
    line = next((l for l in out.splitlines() if l.startswith("[DARK_LOGOS] SUMMARY ")), "")
    return dict(t.split("=", 1) for t in line.split()[2:] if "=" in t)


# ── the selection ─────────────────────────────────────────────────────────

class SelectionTest(unittest.TestCase):

    def test_only_rows_known_to_be_dark_are_selected(self):
        rows = [_row(1, None), _row(2, 0.10), _row(3, 0.05), _row(4, 0.0), _row(5, 0.0999), _row(6, 0.6)]
        with contextlib.redirect_stdout(io.StringIO()):
            selected = mod.fetch_dark(_Db(rows))
        self.assertEqual(sorted(r["id"] for r in selected), ["m-003", "m-004", "m-005"])
        # A NULL is not known to be dark; 0.10 is at the line (positive control: 0.05 is in).
        self.assertNotIn("m-001", [r["id"] for r in selected])
        self.assertNotIn("m-002", [r["id"] for r in selected])
        self.assertEqual(MIN_LOGO_LUMINANCE, 0.10)

    def test_read_whole_before_any_write_in_popularity_order_with_id_breaking_ties(self):
        rows = [_row(1, 0.01, popularity=5), _row(2, 0.01, popularity=50), _row(3, 0.01, popularity=50),
                _row(4, 0.01, popularity=20), _row(5, 0.01, popularity=None)]
        db = _Db(rows)
        tmdb = _Tmdb({r["tmdb_id"]: [r["title_logo_url"], f"{CDN}light{r['tmdb_id']}.png"] for r in rows})
        measure = _Measure({f"{CDN}light{r['tmdb_id']}.png": 0.6 for r in rows})
        with mock.patch.object(mod, "PAGE_SIZE", 2):
            _run(db, tmdb, measure)
        self.assertEqual([t for t, _ in tmdb.calls], [1002, 1003, 1004, 1001, 1005])
        kinds = ["read" if q.named("select") and not q.named("select")[0][2].get("count") else
                 "write" if q.named("update") else "count" for q in db.executed]
        self.assertEqual(kinds[:3], ["read", "read", "read"])            # 5 rows in pages of 2
        self.assertNotIn("read", kinds[3:])
        self.assertEqual(kinds.count("write"), 5)


# ── what it writes, and what it does not ──────────────────────────────────

class WriteTest(unittest.TestCase):

    def test_a_lighter_measured_winner_is_one_update_with_both_keys_and_its_line(self):
        stored = f"{CDN}dune_dark.png"
        db = _Db([_row(1, 0.011, url=stored, title="Dune: Part Two")])
        measure = _Measure({f"{CDN}dune_light.png": 0.6})
        stats, out = _run(db, _Tmdb({1001: [stored, f"{CDN}dune_light.png"]}), measure)
        self.assertEqual(db.writes, [({"title_logo_url": f"{CDN}dune_light.png", "title_logo_luminance": 0.6},
                                      ("id", "m-001"))])
        self.assertIn(f"[DARK_LOGOS] Dune: Part Two: {stored} (0.011) → {CDN}dune_light.png (0.600)", out)
        self.assertEqual(measure.calls, [f"{CDN}dune_light.png"])          # the stored luminance stands in
        self.assertEqual((stats["to_light"], stats["to_lighter"]), (1, 0))

    def test_a_lighter_one_still_under_the_line_is_written_and_counted_apart(self):
        stored = f"{CDN}dark.png"
        db = _Db([_row(1, 0.011, url=stored)])
        stats, _ = _run(db, _Tmdb({1001: [stored, f"{CDN}mid.png"]}), _Measure({f"{CDN}mid.png": 0.05}))
        self.assertEqual(db.writes, [({"title_logo_url": f"{CDN}mid.png", "title_logo_luminance": 0.05},
                                      ("id", "m-001"))])
        self.assertEqual((stats["to_light"], stats["to_lighter"]), (0, 1))

    def test_nothing_is_written_unless_the_winner_is_another_url_measured_lighter(self):
        stored = f"{CDN}stored1.png"
        cases = (
            ("the same winner", [stored, f"{CDN}darker.png"], {f"{CDN}darker.png": 0.005}, "nothing_lighter"),
            ("a darker winner", [f"{CDN}x.png", f"{CDN}y.png"], {f"{CDN}x.png": 0.03, f"{CDN}y.png": 0.02},
             "nothing_lighter"),
            ("an unmeasured winner", [f"{CDN}u.png", f"{CDN}light.png"], {f"{CDN}light.png": 0.6}, "nothing_lighter"),
            ("no logos at TMDb", [], {}, "no_logos"),
            ("a 404", _404(), {}, "no_logos"),
        )
        for name, candidates, values, counter in cases:
            with self.subTest(case=name):
                db = _Db([_row(1, 0.05, url=stored)])
                stats, out = _run(db, _Tmdb({1001: candidates}), _Measure(values))
                self.assertEqual(db.writes, [])
                self.assertEqual(stats[counter], 1)
                self.assertEqual(db.rows[0]["title_logo_url"], stored)          # never NULL over a logo
                self.assertNotIn(" → ", out)
        # Positive control: a lighter, measured, different winner is written.
        db = _Db([_row(1, 0.05, url=stored)])
        _run(db, _Tmdb({1001: [stored, f"{CDN}light.png"]}), _Measure({f"{CDN}light.png": 0.6}))
        self.assertEqual(len(db.writes), 1)

    def test_an_error_writes_nothing_and_is_counted_with_its_line(self):
        db = _Db([_row(1, 0.05, title="Inception"), _row(2, 0.05)])
        stats, out = _run(db, _Tmdb({1001: RuntimeError("timed out"), 1002: [f"{CDN}stored2.png", f"{CDN}l.png"]}),
                          _Measure({f"{CDN}l.png": 0.6}))
        self.assertEqual(stats["errors"], 1)
        self.assertIn("[DARK_LOGOS] Inception: images call failed (RuntimeError)", out)
        self.assertEqual([eq for _, eq in db.writes], [("id", "m-002")])       # the run goes on

    def test_a_failed_write_is_an_error(self):
        db = _Db([_row(1, 0.05)], fail_writes={"m-001"})
        stats, out = _run(db, _Tmdb({1001: [f"{CDN}stored1.png", f"{CDN}l.png"]}), _Measure({f"{CDN}l.png": 0.6}))
        self.assertEqual((stats["errors"], stats["to_light"]), (1, 0))
        self.assertIn("write failed (RuntimeError)", out)

    def test_the_401_rule_is_not_swallowed(self):
        db = _Db([_row(1, 0.05)])
        with self.assertRaises(SystemExit):
            _run(db, _Tmdb({1001: SystemExit(1)}), _Measure({}))


# ── the budget, the summary, main ─────────────────────────────────────────

class BudgetAndSummaryTest(unittest.TestCase):

    def test_the_budget_stops_starting_rows_and_the_summary_still_prints(self):
        rows = [_row(i, 0.05) for i in range(1, 6)]
        db = _Db(rows)
        tmdb = _Tmdb({r["tmdb_id"]: [r["title_logo_url"], f"{CDN}l{i}.png"] for i, r in enumerate(rows)})
        stats, out = _run(db, tmdb, _Measure({}), clock=_Clock(step=60.0), budget_seconds=150)
        self.assertLess(len(tmdb.calls), 5)
        self.assertGreater(len(tmdb.calls), 0)
        self.assertIn("[DARK_LOGOS] Time budget reached", out)
        self.assertTrue(out.rstrip().splitlines()[-1].startswith("[DARK_LOGOS] SUMMARY "))
        # Positive control: a clock that does not move checks them all.
        db = _Db(rows)
        tmdb = _Tmdb({r["tmdb_id"]: [] for r in rows})
        _run(db, tmdb, _Measure({}), clock=_Clock(step=0.0), budget_seconds=150)
        self.assertEqual(len(tmdb.calls), 5)

    def test_the_summary_carries_every_count_and_what_is_still_under_the_line(self):
        rows = [_row(1, 0.011), _row(2, 0.011), _row(3, 0.05), _row(4, 0.05), _row(5, 0.05)]
        tmdb = _Tmdb({1001: [f"{CDN}stored1.png", f"{CDN}a.png"], 1002: [f"{CDN}stored2.png", f"{CDN}b.png"],
                      1003: [], 1004: RuntimeError("x"), 1005: [f"{CDN}stored5.png"]})
        db = _Db(rows)
        _, out = _run(db, tmdb, _Measure({f"{CDN}a.png": 0.6, f"{CDN}b.png": 0.03}))
        line = next((l for l in out.splitlines() if l.startswith("[DARK_LOGOS] SUMMARY ")), "")
        self.assertTrue(line, out)
        self.assertEqual([t.split("=", 1)[0] for t in line.split()[2:]],
                         ["checked", "to_light", "to_lighter", "nothing_lighter", "no_logos", "errors", "still_dark"])
        self.assertEqual(_tokens(out), {"checked": "5", "to_light": "1", "to_lighter": "1", "nothing_lighter": "1",
                                        "no_logos": "1", "errors": "1", "still_dark": "4"})

    def test_main_reads_its_env_and_measures_with_the_one_definition(self):
        seen = {}

        def fake_backfill(db, tmdb, **kwargs):
            seen.update(kwargs, db=db, tmdb=tmdb)
            return {}

        with mock.patch.dict(os.environ, FAKE_ENV, clear=True), \
                mock.patch.object(mod, "SupabaseClient", lambda url, key: ("db", url)), \
                mock.patch.object(mod, "TmdbClient", lambda api_key: ("tmdb", api_key)), \
                mock.patch.object(mod, "backfill", fake_backfill), contextlib.redirect_stdout(io.StringIO()):
            mod.main()
        self.assertEqual((seen.get("db"), seen.get("tmdb")), (("db", FAKE_ENV["SUPABASE_URL"]), ("tmdb", "fake-tmdb-key")))
        self.assertIs(seen.get("measure"), luminance_for_logo)
        # Positive control: without the TMDb key it stops, naming it.
        out = io.StringIO()
        with mock.patch.dict(os.environ, {k: v for k, v in FAKE_ENV.items() if k != "TMDB_API_KEY"}, clear=True), \
                contextlib.redirect_stdout(out):
            with self.assertRaises(SystemExit):
                mod.main()
        self.assertIn("[DARK_LOGOS] ERROR: Required environment variable 'TMDB_API_KEY' is not set.", out.getvalue())


# ── the workflow, and the mirror's census ─────────────────────────────────

def _job(text: str, name: str) -> str:
    """One job's block of backfills.yml."""
    if f"\n  {name}:\n" not in text:
        return ""
    return re.split(r"\n  [a-z0-9-]+:\n", text.split(f"\n  {name}:\n", 1)[1], maxsplit=1)[0]


class WorkflowTest(unittest.TestCase):

    def setUp(self):
        self.text = BACKFILLS.read_text(encoding="utf-8")

    def test_the_new_choice_is_in_the_inputs_options(self):
        options = re.findall(r"(?m)^          - ([a-z-]+)$", self.text.split("\n      role:", 1)[0])
        self.assertIn("dark-logos", options)
        self.assertIn("title-logo-luminance", options)                   # positive control: its siblings are read

    def test_its_job_runs_the_script_with_its_secrets_like_its_siblings(self):
        job = _job(self.text, "backfill-dark-logos")
        self.assertIn("    if: github.event.inputs.job == 'dark-logos'\n", job)
        self.assertRegex(job, r"(?m)^    timeout-minutes: 180$")
        self.assertEqual(re.findall(r"(?m)^\s+run: (.+)$", job), ["pip install -r requirements.txt",
                                                                  "python -m bot.backfill_dark_logos"])
        self.assertEqual(secrets_named(job), ["SUPABASE_KEY", "SUPABASE_URL", "TMDB_API_KEY"])
        self.assertEqual(sorted(step_env(job)), ["SUPABASE_KEY", "SUPABASE_URL", "TMDB_API_KEY"])
        # Positive control: the sibling job is parsed the same way.
        self.assertEqual(re.findall(r"(?m)^\s+run: (.+)$", _job(self.text, "backfill-title-logos")),
                         ["pip install -r requirements.txt", "python -m bot.backfill_title_logos"])

    def test_the_mirror_follows_it(self):
        # It writes title_logo_url, so TMDb Mirror's census holds it (Backfills is already a trigger).
        self.assertIn("backfill_dark_logos", IMAGE_WRITERS)


if __name__ == "__main__":
    unittest.main()
