"""
Unit tests for the IMDb-id backfill, rewritten (RUN B5.3 · 0b).

The May script read every imdb_id IS NULL row with offsets, wrote by
tmdb_id alone (a movie and a show sharing the number were both written),
slept 0.2 s a row and printed every title. The rewrite drains a server-side
queue — imdb_id IS NULL AND imdb_checked_at IS NULL AND tmdb_missing_at IS
NULL, migration 20261026120000 — in keyset pages by id, asks TMDb through
TmdbClient.lookup_imdb_id (which tells FOUND / NONE / MISSING / ERROR
apart), and writes by the row's id: the id and the stamp, or the stamp
alone. A time budget and a batch cap end the run with its SUMMARY.

Fakes only: a Supabase client whose `media` table applies the filters,
keyset bounds and writes the script sends (so a write by tmdb_id would
visibly touch two rows), a TMDb client with canned lookups, and a clock the
test hands in. For lookup_imdb_id itself, requests.get is routed in memory.
No network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import json
import os
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import requests
from postgrest.exceptions import APIError

from bot import backfill_imdb_ids as mod
from bot import tmdb as tmdb_mod
from bot.tmdb import IMDB_ERROR, IMDB_FOUND, IMDB_MISSING, IMDB_NONE, ImdbLookup

START = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
MISSING_COLUMN = APIError({"code": "42703", "message": "column media.imdb_checked_at does not exist",
                           "hint": None, "details": None})
FAKE_ENV = {"TMDB_API_KEY": "fake-tmdb-key", "SUPABASE_URL": "https://project.supabase.invalid",
            "SUPABASE_KEY": "fake-supabase-key"}


# ── the fakes ─────────────────────────────────────────────────────────────

class _Query:
    """One PostgREST chain on the fake table. Applies what it is told."""

    def __init__(self, table):
        self.t, self.ops = table, []

    def _op(self, name, *args, **kwargs):
        self.ops.append((name, args, kwargs))
        return self

    def select(self, *a, **kw): return self._op("select", *a, **kw)
    def is_(self, *a, **kw): return self._op("is_", *a, **kw)
    def gt(self, *a, **kw): return self._op("gt", *a, **kw)
    def order(self, *a, **kw): return self._op("order", *a, **kw)
    def limit(self, *a, **kw): return self._op("limit", *a, **kw)
    def update(self, *a, **kw): return self._op("update", *a, **kw)
    def eq(self, *a, **kw): return self._op("eq", *a, **kw)

    def named(self, name):
        return [o for o in self.ops if o[0] == name]

    def execute(self):
        self.t.executed.append(self)
        update = self.named("update")
        if update:
            fields = update[0][1][0]
            (col, value), = [o[1] for o in self.named("eq")]
            if value in self.t.fail_writes:
                raise APIError({"code": "57014", "message": "canceling statement", "hint": None, "details": None})
            hits = [r for r in self.t.rows if r[col] == value]
            for r in hits:
                r.update(fields)
            return types.SimpleNamespace(data=hits, count=None)
        if self.t.page_error is not None and not self.named("select")[0][2].get("count"):
            raise self.t.page_error
        rows = list(self.t.rows)
        for _, (col, _null), _ in self.named("is_"):
            rows = [r for r in rows if r.get(col) is None]
        for _, (col, value), _ in self.named("gt"):
            rows = [r for r in rows if r[col] > value]
        if self.named("order"):
            rows.sort(key=lambda r: r["id"])
        for _, (n,), _ in self.named("limit"):
            limited = rows[:n]
        if self.named("select")[0][2].get("count"):
            return types.SimpleNamespace(data=rows[:1], count=len(rows))
        rows = limited if self.named("limit") else rows
        cols = [c.strip() for c in self.named("select")[0][1][0].split(",")]
        return types.SimpleNamespace(data=[{c: r[c] for c in cols} for r in rows], count=None)


class _Table:
    def __init__(self, rows, page_error=None, fail_writes=(), count_error=False):
        self.rows, self.page_error, self.fail_writes = rows, page_error, set(fail_writes)
        self.count_error, self.executed = count_error, []
        self.client = self

    def table(self, name):
        assert name == "media", name
        query = _Query(self)
        if self.count_error:
            original = query.execute

            def execute():
                if query.named("select") and query.named("select")[0][2].get("count"):
                    raise RuntimeError("count failed")
                return original()

            query.execute = execute
        return query

    @property
    def page_reads(self):
        return [q for q in self.executed if q.named("select") and not q.named("select")[0][2].get("count")]

    @property
    def writes(self):
        return [(q.named("update")[0][1][0], q.named("eq")[0][1]) for q in self.executed if q.named("update")]


def _row(i, tmdb_id=None, media_type="tv", **fields):
    return {"id": f"m-{i:05d}", "tmdb_id": tmdb_id if tmdb_id is not None else 1000 + i, "media_type": media_type,
            "imdb_id": None, "imdb_checked_at": None, "tmdb_missing_at": None, **fields}


class _Tmdb:
    def __init__(self, answers=None, default=ImdbLookup(IMDB_NONE)):
        self.answers, self.default, self.asked = dict(answers or {}), default, []

    def lookup_imdb_id(self, tmdb_id, media_type):
        self.asked.append((tmdb_id, media_type))
        return self.answers.get((tmdb_id, media_type), self.default)


class _Clock:
    """START plus `step` seconds per call (or a fixed instant)."""

    def __init__(self, step=0.0):
        self.calls, self.step = 0, step

    def __call__(self):
        self.calls += 1
        return START + timedelta(seconds=self.step * (self.calls - 1))


def _last(out):
    """The run's last line (its SUMMARY), or '' when it printed nothing."""
    lines = out.splitlines()
    return lines[-1] if lines else ""


def _run(table, tmdb, batch=20000, budget_min=80, clock=None):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            stats = mod.backfill(table, tmdb, clock=clock or _Clock(), batch=batch, budget_min=budget_min)
        except BaseException as exc:  # SystemExit included: the fail-fast is an exit
            stats = exc
    return stats, out.getvalue()


# ── (1) the queue: keyset pages by id, never an offset ───────────────────

class QueueTest(unittest.TestCase):

    def test_pages_are_keyset_by_id_and_never_offsets(self):
        table = _Table([_row(i) for i in range(1, 2501)])
        with mock.patch.object(mod, "PAGE_SIZE", 1000):
            stats, _ = _run(table, _Tmdb())

        reads = table.page_reads
        self.assertEqual(len(reads), 3)                                  # 1000, 1000, 500
        self.assertEqual([q.named("gt") for q in reads][0], [])          # the first page has no bound
        self.assertEqual(reads[1].named("gt")[0][1], ("id", "m-01000"))  # then id > the last id seen
        self.assertEqual(reads[2].named("gt")[0][1], ("id", "m-02000"))
        for q in reads:
            self.assertFalse(q.named("range"))                           # never an offset
            self.assertEqual(q.named("order")[0][1], ("id",))
            self.assertEqual(q.named("limit")[0][1], (1000,))
            self.assertEqual(sorted(o[1][0] for o in q.named("is_")), ["imdb_checked_at", "imdb_id", "tmdb_missing_at"])
            self.assertEqual(q.named("select")[0][1], ("id, tmdb_id, media_type",))
        self.assertEqual(stats["scanned"], 2500)

    def test_a_titles_already_checked_or_missing_are_not_asked(self):
        rows = [_row(1), _row(2, imdb_checked_at="2026-10-01T00:00:00+00:00"),
                _row(3, tmdb_missing_at="2026-09-01T00:00:00+00:00"), _row(4, imdb_id="tt0000004")]
        tmdb = _Tmdb()
        _run(_Table(rows), tmdb)

        self.assertEqual(tmdb.asked, [(1001, "tv")])                     # control: the plain one is asked


# ── (2)(3) what each outcome writes, by id ───────────────────────────────

class OutcomeTest(unittest.TestCase):

    def test_found_writes_the_id_and_the_stamp_for_that_row_only(self):
        # Same tmdb_id, other type: TMDb's movie and tv ids are separate spaces.
        show, movie = _row(1, tmdb_id=1416, media_type="tv"), _row(2, tmdb_id=1416, media_type="movie",
                                                                    imdb_checked_at="2026-10-01T00:00:00+00:00")
        table = _Table([show, movie])
        _run(table, _Tmdb({(1416, "tv"): ImdbLookup(IMDB_FOUND, "tt0413573")}))

        self.assertEqual(table.writes, [({"imdb_id": "tt0413573", "imdb_checked_at": START.isoformat()},
                                         ("id", "m-00001"))])
        self.assertEqual((show["imdb_id"], show["imdb_checked_at"]), ("tt0413573", START.isoformat()))
        self.assertIsNone(movie["imdb_id"])                               # the other row is untouched
        self.assertEqual(movie["imdb_checked_at"], "2026-10-01T00:00:00+00:00")

    def test_none_stamps_only(self):
        table = _Table([_row(1)])
        stats, _ = _run(table, _Tmdb({(1001, "tv"): ImdbLookup(IMDB_NONE)}))

        self.assertEqual(table.writes, [({"imdb_checked_at": START.isoformat()}, ("id", "m-00001"))])
        self.assertEqual((stats["none"], stats["written"]), (1, 1))

    def test_missing_stamps_and_counts_and_leaves_tmdb_missing_at_alone(self):
        table = _Table([_row(1)])
        stats, _ = _run(table, _Tmdb({(1001, "tv"): ImdbLookup(IMDB_MISSING)}))

        self.assertEqual(table.writes, [({"imdb_checked_at": START.isoformat()}, ("id", "m-00001"))])
        self.assertEqual(stats["missing"], 1)
        self.assertIsNone(table.rows[0]["tmdb_missing_at"])               # its own writers set that

    def test_an_error_writes_nothing_and_the_run_goes_on(self):
        table = _Table([_row(1), _row(2)])
        stats, out = _run(table, _Tmdb({(1001, "tv"): ImdbLookup(IMDB_ERROR, reason="HTTPError 500")}))

        self.assertEqual(table.writes, [({"imdb_checked_at": START.isoformat()}, ("id", "m-00002"))])  # the next
        self.assertEqual((stats["errors"], stats["written"], stats["scanned"]), (1, 1, 2))
        self.assertIn("[IMDB_ID] ERROR tmdb=1001 type=tv: HTTPError 500", out)
        self.assertIsNone(table.rows[0]["imdb_checked_at"])               # left for the next run

    def test_a_failed_write_is_an_error_and_the_run_goes_on(self):
        table = _Table([_row(1), _row(2)], fail_writes={"m-00001"})
        stats, out = _run(table, _Tmdb())

        self.assertEqual((stats.get("errors"), stats.get("written")), (1, 1))
        self.assertIn("[IMDB_ID] ERROR tmdb=1001 type=tv: write failed (APIError)", out)

    def test_the_log_names_no_title_and_progress_comes_every_1000(self):
        table = _Table([{**_row(i), "title": f"Secret Title {i}"} for i in range(1, 2501)])
        _, out = _run(table, _Tmdb())

        self.assertNotIn("Secret Title", out)
        self.assertEqual(out.count("[IMDB_ID] progress:"), 2)            # at 1,000 and 2,000


# ── (4) the time budget and the batch cap ────────────────────────────────

class BudgetTest(unittest.TestCase):

    def test_the_time_budget_ends_the_run_with_its_summary(self):
        table = _Table([_row(i) for i in range(1, 51)])
        clock = _Clock(step=60.0)                                         # each read of the clock: one minute on
        stats, out = _run(table, _Tmdb(), budget_min=10, clock=clock)

        self.assertEqual(stats.get("budget_hit"), 1)
        self.assertLess(stats["scanned"], 50)
        self.assertIn("[IMDB_ID] time budget of 10 min reached after", out)
        self.assertIn("budget_hit=1", _last(out))
        # Control: a budget the run never reaches.
        stats, out = _run(_Table([_row(i) for i in range(1, 51)]), _Tmdb(), budget_min=80)
        self.assertEqual((stats.get("budget_hit"), stats["scanned"]), (0, 50))

    def test_the_batch_caps_the_run(self):
        table = _Table([_row(i) for i in range(1, 51)])
        with mock.patch.object(mod, "PAGE_SIZE", 20):
            stats, _ = _run(table, _Tmdb(), batch=30)

        self.assertEqual(stats["scanned"], 30)
        self.assertEqual(len(table.writes), 30)


# ── (5) the fail-fast and (6) the SUMMARY ────────────────────────────────

class FailFastAndSummaryTest(unittest.TestCase):

    def test_a_missing_column_names_the_migration_and_writes_nothing(self):
        table = _Table([_row(1)], page_error=MISSING_COLUMN)
        result, out = _run(table, _Tmdb())

        self.assertIsInstance(result, SystemExit)
        self.assertEqual(result.code, 1)
        self.assertEqual(table.writes, [])
        errors = [line for line in out.splitlines() if "ERROR" in line]
        self.assertEqual(len(errors), 1)
        self.assertIn("20261026120000", errors[0])
        self.assertIn("imdb_checked_at", errors[0])
        # Control: any other page error is not the fail-fast — it stops the run with its SUMMARY.
        other = APIError({"code": "57014", "message": "canceling statement", "hint": None, "details": None})
        stats, out = _run(_Table([_row(1)], page_error=other), _Tmdb())
        self.assertTrue(stats.get("queue_failed"))
        self.assertNotIn("20261026120000", out)
        self.assertIn("[IMDB_ID] SUMMARY", out)

    def test_the_summary_line_carries_every_token(self):
        rows = [_row(1), _row(2), _row(3), _row(4)]
        tmdb = _Tmdb({(1001, "tv"): ImdbLookup(IMDB_FOUND, "tt1"), (1002, "tv"): ImdbLookup(IMDB_NONE),
                      (1003, "tv"): ImdbLookup(IMDB_MISSING), (1004, "tv"): ImdbLookup(IMDB_ERROR, reason="x")})
        _, out = _run(_Table(rows), tmdb)

        self.assertEqual(_last(out),
                         "[IMDB_ID] SUMMARY scanned=4 found=1 none=1 missing=1 errors=1 written=3 "
                         "budget_hit=0 remaining=1")                      # the error's row stays queued

    def test_remaining_is_a_dash_when_the_count_fails(self):
        _, out = _run(_Table([_row(1)], count_error=True), _Tmdb())
        self.assertTrue(_last(out).endswith(" remaining=-"), _last(out))


class MainTest(unittest.TestCase):

    def test_main_reads_the_batch_and_the_budget_from_the_environment(self):
        seen = {}

        def fake_backfill(db, tmdb, *, clock, batch, budget_min):
            seen.update(batch=batch, budget_min=budget_min, now=clock())
            return {}

        for env, batch, budget in (({}, 20000, 80), ({"IMDB_BACKFILL_BATCH": "500", "IMDB_TIME_BUDGET_MIN": "5"}, 500, 5)):
            with self.subTest(env=env), mock.patch.dict(os.environ, {**FAKE_ENV, **env}, clear=True), \
                    mock.patch.object(mod, "SupabaseClient", lambda **kw: object()), \
                    mock.patch.object(mod, "TmdbClient", lambda **kw: object()), \
                    mock.patch.object(mod, "backfill", fake_backfill), contextlib.redirect_stdout(io.StringIO()):
                seen.clear()
                mod.main(clock=lambda: START)
                self.assertEqual((seen.get("batch"), seen.get("budget_min"), seen.get("now")), (batch, budget, START))


# ── TmdbClient.lookup_imdb_id: the four outcomes, apart ──────────────────

def _tmdb_response(status, body, url):
    r = requests.Response()
    r.status_code, r._content, r.url = status, json.dumps(body).encode("utf-8"), url
    r.reason = {200: "OK", 404: "Not Found", 500: "Internal Server Error"}.get(status, "")
    return r


@contextlib.contextmanager
def _tmdb(answers):
    """A real TmdbClient whose requests.get answers from *answers* (a list,
    one per call) and whose limiter and sleeps are inert."""
    calls, out = [], io.StringIO()

    def get(url, params=None, timeout=None):
        calls.append(url)
        full = requests.Request("GET", url, params=params).prepare().url
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        if isinstance(answer, Exception):
            raise answer
        return _tmdb_response(answer[0], answer[1], full)

    with mock.patch.object(tmdb_mod.requests, "get", get), mock.patch.object(tmdb_mod.time, "sleep", lambda s: None), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        client = tmdb_mod.TmdbClient(api_key="fake-tmdb-key")
        client._limiter = types.SimpleNamespace(acquire=lambda: None)
        client.calls, client.out = calls, out
        yield client


class LookupImdbIdTest(unittest.TestCase):

    def test_found_none_missing_and_error_are_told_apart(self):
        cases = (([(200, {"imdb_id": "tt0413573"})], IMDB_FOUND, "tt0413573", 1),
                 ([(200, {"imdb_id": None})], IMDB_NONE, None, 1),
                 ([(200, {"imdb_id": ""})], IMDB_NONE, None, 1),
                 ([(200, {})], IMDB_NONE, None, 1),
                 ([(200, {"imdb_id": "nm0000123"})], IMDB_NONE, None, 1),   # not an id we can use
                 ([(404, {"status_code": 34})], IMDB_MISSING, None, 1),     # not retried
                 ([(500, {})], IMDB_ERROR, None, 3),                         # after _get's three tries
                 ([requests.ConnectionError("reset")], IMDB_ERROR, None, 3))
        for answers, outcome, imdb_id, calls in cases:
            with self.subTest(answers=repr(answers)[:60]):
                with _tmdb(list(answers)) as client:
                    result = client.lookup_imdb_id(1416, "tv")
                self.assertEqual((result.outcome, result.imdb_id), (outcome, imdb_id))
                self.assertEqual(len(client.calls), calls)
                self.assertTrue(client.calls[0].endswith("/tv/1416/external_ids"))

    def test_an_error_says_its_kind_never_its_url(self):
        with _tmdb([(500, {})]) as client:
            result = client.lookup_imdb_id(1416, "tv")

        self.assertEqual(result.reason, "HTTPError 500")
        self.assertNotIn("fake-tmdb-key", client.out.getvalue())
        self.assertNotIn("api_key", client.out.getvalue())

    def test_the_old_get_imdb_id_answers_as_before(self):
        # backfill_rt_scores.py still calls it: an id, or None for anything else.
        for answers, expected in (([(200, {"imdb_id": "tt0413573"})], "tt0413573"), ([(404, {})], None),
                                  ([(500, {})], None)):
            with self.subTest(expected=expected), _tmdb(list(answers)) as client:
                self.assertEqual(client.get_imdb_id(1416, "tv"), expected)


class MissingIsACountNotALineTest(unittest.TestCase):
    """B5.4 · 0b: backfill #1 printed 82 `[TMDB] 404 for …/external_ids — not
    retrying (client error).` lines, one per MISSING title — an expected
    outcome the SUMMARY already counts, printed as if it were a failure."""

    def test_a_404_in_the_backfill_prints_no_tmdb_line_and_is_counted(self):
        with _tmdb([(404, {"status_code": 34})]) as client, mock.patch.object(mod, "PROGRESS_EVERY", 1):
            stats, out = _run(_Table([_row(1)]), client)

        self.assertNotIn("[TMDB] 404", out)
        self.assertNotIn("not retrying", out)
        self.assertEqual((stats["missing"], stats["errors"], stats["written"]), (1, 0, 1))
        self.assertIn("missing 1", [line for line in out.splitlines() if "[IMDB_ID] progress:" in line][0])
        self.assertIn(" missing=1 ", _last(out))

    def test_a_500_in_the_backfill_still_prints_its_line_and_is_an_error(self):
        # Positive control: only the 404 goes quiet.
        with _tmdb([(500, {})]) as client:
            stats, out = _run(_Table([_row(1)]), client)

        self.assertEqual(out.count("[TMDB] Request failed (attempt"), 3)
        self.assertIn("[IMDB_ID] ERROR tmdb=1001 type=tv: HTTPError 500", out)
        self.assertEqual((stats["errors"], stats["missing"], stats["written"]), (1, 0, 0))

    def test_another_client_error_in_the_backfill_still_prints_its_line(self):
        # Control: a 401 (a bad key) is not an outcome — its line stays.
        with _tmdb([(401, {})]) as client:
            stats, out = _run(_Table([_row(1)]), client)

        self.assertIn("[TMDB] 401 for https://api.themoviedb.org/3/tv/1001/external_ids — not retrying (client error).",
                      out)
        self.assertEqual((stats["errors"], stats["missing"]), (1, 0))

    def test_gets_404_line_outside_the_backfill_is_unchanged(self):
        for path in ("/movie/550/credits", "/tv/1416/external_ids"):
            with self.subTest(path=path), _tmdb([(404, {})]) as client:
                with self.assertRaises(requests.HTTPError):
                    client._get(path)
                self.assertEqual(client.out.getvalue(),
                                 f"[TMDB] 404 for https://api.themoviedb.org/3{path} — not retrying (client error).\n")
        # The same path through another caller still says it: the quiet is lookup_imdb_id's, not the path's.
        with _tmdb([(404, {})]) as client:
            self.assertIsNone(client.get_imdb_id(1416, "tv"))
        self.assertIn("[TMDB] 404 for https://api.themoviedb.org/3/tv/1416/external_ids — not retrying (client error).",
                      client.out.getvalue())


class TmdbRedactionTest(unittest.TestCase):

    def test_no_api_key_reaches_a_tmdb_failure_line_or_the_raised_error(self):
        # TMDb's key rides in the query string; requests quotes the full URL.
        for answers in ([(500, {})], [(401, {})], [requests.ConnectionError(
                "HTTPSConnectionPool(host='api.themoviedb.org'): Max retries exceeded with url: "
                "/3/tv/1416/external_ids?api_key=fake-tmdb-key")]):
            with self.subTest(answers=repr(answers)[:40]):
                with _tmdb(list(answers)) as client:
                    try:
                        client._get("/tv/1416/external_ids")
                    except requests.RequestException as exc:
                        raised = str(exc)
                text = client.out.getvalue() + raised
                self.assertNotIn("fake-tmdb-key", text)
                self.assertNotIn("api_key=", text)
                self.assertIn("/tv/1416/external_ids", text)              # positive control: the path stays


if __name__ == "__main__":
    unittest.main()
