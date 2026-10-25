"""
Unit tests for the server's three doors (RUN B5.1 · 0a, migration 20261025160000):

  - SupabaseClient.write_related → public.trakt_replace_related(p_media_id,
    p_rows, p_synced_at): one title's media_related rows AND its stamp in one
    RPC, one transaction — instead of a DELETE, an INSERT and a PATCH, where
    the stamp could land without its rows.
  - SupabaseClient.decay_trakt_heat → public.trakt_decay_heat(p_before,
    p_limit): Phase A's decay in calls of 200 rows while a call returns 200,
    at most 50 calls — instead of one UPDATE over every row ever stamped,
    which hit PostgREST's 8 s statement timeout on every run.
  - the queue RPC retries '57014' (statement timeout) like a 503, and a slice
    that fails for good stops the drain with a summary, not a traceback.

Fakes only: a Supabase client whose rpc() records its name and parameters as
sent and answers from a script, a Trakt client routed in memory. No network,
no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import types
import unittest
from unittest import mock

from postgrest.exceptions import APIError

from bot import supabase_client
from bot import sync_trakt
from bot.supabase_client import SupabaseClient
from tests.test_trakt_related import _Router, _client, _hit, _row
from tests.test_trakt_related_queue import FIX, _main, _rows
from datetime import datetime, timezone

RUN = "2026-10-25T03:00:00+00:00"
MISSING_WRITE = APIError({
    "code": "PGRST202",
    "message": "Could not find the function public.trakt_replace_related(p_media_id, p_rows, p_synced_at) "
               "in the schema cache",
    "hint": None, "details": None,
})
MISSING_DECAY = APIError({
    "code": "PGRST202",
    "message": "Could not find the function public.trakt_decay_heat(p_before, p_limit) in the schema cache",
    "hint": None, "details": None,
})
NO_SUCH_MEDIA = APIError({"code": "P0002", "message": "trakt_replace_related: no media row with id m-1",
                          "hint": None, "details": None})
TIMEOUT = APIError({"code": "57014", "message": "canceling statement due to statement timeout",
                    "hint": None, "details": None})


class _Rpc:
    """Stands in for supabase's client: rpc() records (name, params as sent)
    and answers from a script — the last answer repeats."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = []

    def rpc(self, name, params):
        self.calls.append((name, params))

        def execute():
            answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
            if isinstance(answer, BaseException):
                raise answer
            return types.SimpleNamespace(data=answer)

        return types.SimpleNamespace(execute=execute)


def _db(answers):
    db = SupabaseClient.__new__(SupabaseClient)  # no create_client: nothing real is built
    db.client = _Rpc(answers)
    return db


def _quiet(fn, *args):
    out, sleeps = io.StringIO(), []
    with mock.patch.object(supabase_client.time, "sleep", sleeps.append), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            result = fn(*args)
        except BaseException as exc:  # SystemExit included: the fail-fast is an exit
            result = exc
    return result, out.getvalue(), sleeps


ROWS = [{"related_tmdb_id": 438631, "related_media_type": "movie", "rank": 1},
        {"related_tmdb_id": 11, "related_media_type": "movie", "rank": 2}]


# ── SupabaseClient.write_related ──────────────────────────────────────────

class WriteRelatedTest(unittest.TestCase):

    def test_one_rpc_with_the_three_named_parameters(self):
        db = _db([2])

        result, _, _ = _quiet(db.write_related, "m-1", ROWS, RUN)

        self.assertEqual(result, 2)
        self.assertEqual(db.client.calls, [("trakt_replace_related",
                                            {"p_media_id": "m-1", "p_rows": ROWS, "p_synced_at": RUN})])
        self.assertIsInstance(db.client.calls[0][1]["p_rows"], list)   # a JSON array, never a string
        # Control: no related titles is an empty array, still one call, still the stamp.
        db = _db([0])
        result, _, _ = _quiet(db.write_related, "m-1", [], RUN)
        self.assertEqual((result, db.client.calls[0][1]["p_rows"]), (0, []))
        self.assertIsInstance(db.client.calls[0][1]["p_rows"], list)

    def test_each_row_carries_exactly_the_three_keys(self):
        db = _db([1])
        _quiet(db.write_related, "m-1", [{**ROWS[0], "source": "trakt", "synced_at": RUN}], RUN)

        self.assertEqual(len(db.client.calls), 1)
        self.assertEqual(db.client.calls[0][1]["p_rows"], [ROWS[0]])   # the server adds source and synced_at

    def test_it_returns_the_rows_the_server_wrote(self):
        # Trakt listed the same title twice: two rows sent, one written.
        db = _db([1])
        result, _, _ = _quiet(db.write_related, "m-1", [ROWS[0], {**ROWS[0], "rank": 2}], RUN)
        self.assertEqual(result, 1)

    def test_an_error_propagates_and_is_not_retried(self):
        for error in (NO_SUCH_MEDIA, MISSING_WRITE, TIMEOUT):
            with self.subTest(code=error.code):
                db = _db([error, 2])
                result, _, sleeps = _quiet(db.write_related, "m-1", ROWS, RUN)
                self.assertIs(result, error)
                self.assertEqual((len(db.client.calls), sleeps), (1, []))
        # Control: a healthy write returns its count.
        result, _, _ = _quiet(_db([2]).write_related, "m-1", ROWS, RUN)
        self.assertEqual(result, 2)


# ── Phase B writes each title through that one RPC ───────────────────────

class _WriteDb:
    """sync_trakt's db with ONLY the queue and write_related: any call to the
    old replace_media_related / stamp_trakt_related_synced is an AttributeError."""

    def __init__(self, rows, fail=None):
        self.rows = rows
        self.fail = dict(fail or {})   # media_id -> exception
        self.written = []              # (media_id, rows, synced_at) — every call
        self.landed = []               # media_id

    def get_titles_for_related_drain(self, limit, before=None):
        return [r for r in self.rows if r["id"] not in self.landed][:limit]

    def write_related(self, media_id, rows, synced_at):
        self.written.append((media_id, rows, synced_at))
        if media_id in self.fail:
            raise self.fail[media_id]
        self.landed.append(media_id)
        return len({r["related_tmdb_id"] for r in rows})   # the server folds a title listed twice


def _drain(router, db, batch=100):
    with _client(router) as client:
        try:
            stats = sync_trakt._run_phase_b(client, db, RUN, batch)
        except BaseException as exc:
            stats = exc
    return stats, client.out.getvalue()


class DrainWritesOnceTest(unittest.TestCase):

    def test_a_resolved_title_is_one_write_with_its_rows(self):
        router = _Router({
            "/search/tmdb/1416": (200, [_hit("show", 1407, 1416, "Grey's Anatomy")]),
            "/shows/1407/related": (200, [{"ids": {"tmdb": 2734}}, {"ids": {"trakt": 2}}, {"ids": {"tmdb": 78191}}]),
        })
        db = _WriteDb([_row(media_id="m-3", tmdb_id=1416, media_type="tv", title="Grey's Anatomy")])

        stats, _ = _drain(router, db)

        self.assertEqual(db.written, [("m-3", [
            {"related_tmdb_id": 2734, "related_media_type": "tv", "rank": 1},
            {"related_tmdb_id": 78191, "related_media_type": "tv", "rank": 2},
        ], RUN)])
        self.assertEqual((stats["errors"], stats["related_rows"]), (0, 2))

    def test_an_unresolved_title_is_one_write_with_zero_rows(self):
        router = _Router({"/search/tmdb/693134": (200, [])})
        db = _WriteDb([_row()])

        stats, _ = _drain(router, db)

        self.assertEqual(db.written, [("m-1", [], RUN)])
        self.assertEqual((stats["unresolved"], stats["errors"]), (1, 0))

    def test_related_rows_counts_what_the_server_wrote(self):
        # Trakt lists Dune twice: two rows sent, the server writes one.
        router = _Router({
            "/search/tmdb/693134": (200, [_hit("movie", 287071, 693134)]),
            "/movies/287071/related": (200, [{"ids": {"tmdb": 438631}}, {"ids": {"tmdb": 438631}}]),
        })
        db = _WriteDb([_row()])

        stats, _ = _drain(router, db)

        self.assertEqual([len(rows) for _, rows, _ in db.written], [2])
        self.assertEqual(stats["related_rows"], 1)

    def test_a_failed_write_is_an_error_and_the_next_title_still_writes(self):
        router = _Router({"/search/tmdb/1": (200, []), "/search/tmdb/2": (200, [])})
        db = _WriteDb([_row("m-1", 1), _row("m-2", 2)], fail={"m-1": NO_SUCH_MEDIA})

        stats, out = _drain(router, db, batch=2)

        self.assertEqual(stats["errors"], 1)
        self.assertEqual(db.landed, ["m-2"])                    # m-1 stays queued; m-2 is written
        self.assertIn("tmdb_id=1: related drain failed", out)

    def test_a_missing_write_function_fails_fast(self):
        router = _Router({f"/search/tmdb/{i}": (200, []) for i in (1, 2, 3)})
        db = _WriteDb([_row("m-1", 1), _row("m-2", 2), _row("m-3", 3)], fail={"m-1": MISSING_WRITE})

        result, out = _drain(router, db)

        self.assertIsInstance(result, SystemExit)
        self.assertEqual(result.code, 1)
        self.assertEqual([m for m, _, _ in db.written], ["m-1"])   # nothing else was written
        errors = [line for line in out.splitlines() if "ERROR" in line]
        self.assertEqual(len(errors), 1)
        self.assertIn("trakt_replace_related", errors[0])
        self.assertIn("20261025160000", errors[0])
        # Control: any other write error is one title's error, and the drain goes on.
        db = _WriteDb([_row("m-1", 1), _row("m-2", 2), _row("m-3", 3)], fail={"m-1": NO_SUCH_MEDIA})
        stats, out = _drain(router, db)
        self.assertEqual((stats["errors"], db.landed), (1, ["m-2", "m-3"]))
        self.assertNotIn("20261025160000", out)


# ── the queue: a statement timeout is transient; a dead slice leaves a summary ─

class QueueTimeoutTest(unittest.TestCase):

    def test_a_statement_timeout_is_retried_like_a_503(self):
        db = _db([TIMEOUT, _rows(2)])

        result, out, sleeps = _quiet(db.get_titles_for_related_drain, 1000, FIX)

        self.assertEqual(result, _rows(2))
        self.assertEqual((len(db.client.calls), sleeps), (2, [2]))
        self.assertIn("(code 57014)", out)
        # Control: three of them in a row propagate, after 2 s and 4 s.
        db = _db([TIMEOUT, TIMEOUT, TIMEOUT, _rows(2)])
        result, _, sleeps = _quiet(db.get_titles_for_related_drain, 1000, FIX)
        self.assertIs(result, TIMEOUT)
        self.assertEqual((len(db.client.calls), sleeps), (3, [2, 4]))

    def test_queue_rows_reach_the_drain_with_their_imdb_id(self):
        rows = [{**_rows(1)[0], "imdb_id": "tt15239678"}, _rows(1, start=1)[0]]   # one with, one without
        result, _, _ = _quiet(_db([rows]).get_titles_for_related_drain, 1000, FIX)
        self.assertEqual(result, rows)


class _SliceDb:
    """A queue whose asks answer from a script: rows, or an exception."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.asks = []
        self.landed = []

    def get_titles_for_related_drain(self, limit, before=None):
        self.asks.append(limit)
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def write_related(self, media_id, rows, synced_at):
        self.landed.append(media_id)
        return 0


class _NothingRelated:
    def resolve_id(self, media_type, tmdb_id):
        return None

    def get_related(self, media_type, trakt_id, limit=20):
        return []


class SliceFailureTest(unittest.TestCase):

    def _run(self, db):
        out = io.StringIO()
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10), contextlib.redirect_stdout(out):
            try:
                stats = sync_trakt._run_phase_b(_NothingRelated(), db, RUN, 30)
            except BaseException as exc:
                stats = exc
        return stats, out.getvalue()

    def test_a_slice_that_fails_for_good_stops_the_drain_with_what_landed(self):
        db = _SliceDb([_rows(10), TIMEOUT])

        stats, out = self._run(db)

        self.assertIsInstance(stats, dict)                      # not a traceback
        self.assertTrue(stats.get("queue_failed"))
        self.assertEqual(len(db.landed), 10)                    # the first slice stays written
        self.assertIn("[TRAKT SYNC] ERROR: the queue slice failed — ", out)
        self.assertIn("; stopping this run's drain with what landed", out)
        self.assertIn("57014", out)
        # Control: a healthy queue does not report a failed slice.
        stats, out = self._run(_SliceDb([_rows(10), _rows(5, start=10)]))
        self.assertFalse(stats.get("queue_failed"))
        self.assertNotIn("the queue slice failed", out)

    def test_a_missing_queue_function_is_still_the_fail_fast(self):
        missing = APIError({"code": "PGRST202", "message": "Could not find the function public.trakt_related_queue",
                            "hint": None, "details": None})
        stats, out = self._run(_SliceDb([missing]))

        self.assertIsInstance(stats, SystemExit)
        self.assertIn("20261024210000", out)
        self.assertNotIn("the queue slice failed", out)

    def test_main_prints_the_summary_and_exits_1_after_a_failed_slice(self):
        failed = {"scanned": 10, "resolved": 0, "unresolved": 10, "related_rows": 0, "errors": 0,
                  "stuck_at": None, "queue_failed": "APIError: canceling statement due to statement timeout"}

        code, _, out = _main(datetime(2026, 10, 25, 3, 0, tzinfo=timezone.utc), phase_b_stats=failed)

        self.assertEqual(code, 1)
        self.assertIn("SUMMARY", out)
        # Control: the same run without the failed slice ends green.
        code, _, _ = _main(datetime(2026, 10, 25, 3, 0, tzinfo=timezone.utc),
                           phase_b_stats={**failed, "queue_failed": None})
        self.assertIsNone(code)


# ── SupabaseClient.decay_trakt_heat and Phase A ──────────────────────────

def _decay_calls(db):
    return [c for c in db.client.calls if c[0] == "trakt_decay_heat"]


class DecayTest(unittest.TestCase):

    def test_it_calls_again_while_a_call_returns_its_limit(self):
        db = _db([200, 200, 19])

        result, _, _ = _quiet(db.decay_trakt_heat, RUN)

        self.assertEqual(result, (419, 3))
        self.assertEqual(db.client.calls, [("trakt_decay_heat", {"p_before": RUN, "p_limit": 200})] * 3)

    def test_a_call_that_decays_nothing_is_the_only_call(self):
        db = _db([0])

        result, _, _ = _quiet(db.decay_trakt_heat, RUN)

        self.assertEqual(result, (0, 1))
        self.assertEqual(len(db.client.calls), 1)

    def test_fifty_calls_is_the_ceiling(self):
        db = _db([200])   # a server that always answers its limit

        result, _, _ = _quiet(db.decay_trakt_heat, RUN)

        self.assertEqual(result, (10000, 50))
        self.assertEqual(len(db.client.calls), 50)

    def test_another_error_keeps_what_landed_and_returns(self):
        db = _db([200, TIMEOUT, 200])

        result, out, _ = _quiet(db.decay_trakt_heat, RUN)

        self.assertEqual(result, (200, 2))
        self.assertIn("[Supabase] Error decaying stale trakt heat after 200 title(s) in 2 call(s)", out)
        # Control: a healthy decay says nothing of an error.
        result, out, _ = _quiet(_db([19]).decay_trakt_heat, RUN)
        self.assertEqual(result, (19, 1))
        self.assertNotIn("Error", out)

    def test_a_missing_decay_function_propagates_from_the_client(self):
        result, _, _ = _quiet(_db([MISSING_DECAY]).decay_trakt_heat, RUN)
        self.assertIs(result, MISSING_DECAY)


class _NoLists:
    """A Trakt client whose three lists are empty: Phase A goes straight to the decay."""

    def get_most_watched(self, media_type, period, page, limit):
        return [], 1

    def get_trending(self, media_type, page, limit):
        return [], 1

    def get_anticipated(self, media_type, page, limit):
        return [], 1


class PhaseADecayTest(unittest.TestCase):

    def _phase_a(self, db):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            try:
                result = sync_trakt._run_phase_a(_NoLists(), db, RUN, 300)
            except BaseException as exc:
                result = exc
        return result, out.getvalue()

    def test_phase_a_says_how_much_heat_it_decayed(self):
        db = _db([200, 200, 19])

        stats, out = self._phase_a(db)

        self.assertIsInstance(stats, dict)
        self.assertIn("[TRAKT SYNC]   decayed heat on 419 title(s) in 3 call(s)", out)
        self.assertEqual(len(_decay_calls(db)), 3)

    def test_a_missing_decay_function_fails_phase_a_loudly(self):
        stats, out = self._phase_a(_db([MISSING_DECAY]))

        self.assertIsInstance(stats, SystemExit)
        self.assertEqual(stats.code, 1)
        errors = [line for line in out.splitlines() if "ERROR" in line]
        self.assertEqual(len(errors), 1)
        self.assertIn("trakt_decay_heat", errors[0])
        self.assertIn("20261025160000", errors[0])

    def test_any_other_decay_error_lets_the_run_go_on(self):
        stats, out = self._phase_a(_db([200, TIMEOUT]))

        self.assertIsInstance(stats, dict)                      # Phase B still runs after this
        self.assertIn("decayed heat on 200 title(s) in 2 call(s)", out)
        self.assertNotIn("20261025160000", out)


if __name__ == "__main__":
    unittest.main()
