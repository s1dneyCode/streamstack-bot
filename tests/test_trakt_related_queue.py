"""
Unit tests for the related drain's queue (RUN B5 · 0b): the titles come from
the server function public.trakt_related_queue(p_limit, p_before) — migration
20261024210000 — in the app's order, and the drain takes them in SLICES of at
most 1,000 (PostgREST's max-rows trims any response to 1,000 rows), asking
again after each full slice until the batch is filled or a slice comes back
short. p_before is the fix date: every stamp older than it was written by the
wrong /related path.

Fakes only: a Supabase client whose rpc() records its name and parameters and
recomputes the queue on every call (stamped titles drop out, as on the server),
a Trakt client that resolves every title, an injected clock and sleep. No
network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import types
import unittest
from datetime import datetime, timezone
from unittest import mock

import httpx
from postgrest.exceptions import APIError

from bot import supabase_client
from bot import sync_trakt
from bot.supabase_client import SupabaseClient

FIX = "2026-10-24T16:00:00Z"
RUN = "2026-10-25T03:00:00+00:00"
MISSING_FUNCTION = APIError({
    "code": "PGRST202",
    "message": "Could not find the function public.trakt_related_queue(p_before, p_limit) in the schema cache",
    "hint": None, "details": None,
})


def _rows(n: int, start: int = 0) -> list[dict]:
    return [{"id": f"m-{i}", "tmdb_id": 1000 + i, "media_type": "movie", "title": f"T{i}", "popularity": 1.0}
            for i in range(start, start + n)]


# ── SupabaseClient.get_titles_for_related_drain: the RPC ─────────────────

class _RpcClient:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.rpc_calls = []

    def rpc(self, name, params):
        self.rpc_calls.append((name, params))

        def execute():
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return types.SimpleNamespace(data=outcome)

        return types.SimpleNamespace(execute=execute)


def _drain_rpc(outcomes, limit=1000):
    db = SupabaseClient.__new__(SupabaseClient)  # no create_client: nothing real is built
    db.client = _RpcClient(outcomes)
    sleeps, out = [], io.StringIO()
    with mock.patch.object(supabase_client.time, "sleep", sleeps.append), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            result = db.get_titles_for_related_drain(limit, FIX)
        except BaseException as exc:
            result = exc
    return result, db.client, sleeps, out.getvalue()


class QueueRpcTest(unittest.TestCase):

    def test_the_rpc_is_called_with_its_two_named_parameters(self):
        rows = _rows(3)

        result, client, sleeps, _ = _drain_rpc([rows], limit=1000)

        self.assertEqual(result, rows)
        self.assertEqual(client.rpc_calls, [("trakt_related_queue", {"p_limit": 1000, "p_before": FIX})])
        self.assertEqual(sleeps, [])
        # The last slice of a batch asks for less: the limit is the caller's.
        _, client, _, _ = _drain_rpc([rows], limit=200)
        self.assertEqual(client.rpc_calls, [("trakt_related_queue", {"p_limit": 200, "p_before": FIX})])

    def test_an_empty_answer_is_an_empty_list(self):
        for data in (None, []):
            with self.subTest(data=data):
                result, _, _, _ = _drain_rpc([data])
                self.assertEqual(result, [])
                self.assertIsInstance(result, list)

    def test_a_transient_failure_is_retried_as_in_b31(self):
        cloudflare = APIError({"message": "JSON could not be generated", "code": 503, "hint": None, "details": "<html>"})
        dropped = httpx.RemoteProtocolError("Server disconnected without sending a response.")

        result, client, sleeps, _ = _drain_rpc([cloudflare, dropped, _rows(2)])

        self.assertEqual(result, _rows(2))
        self.assertEqual(len(client.rpc_calls), 3)
        self.assertEqual(sleeps, [2, 4])

    def test_the_retry_is_b31s_own(self):
        # Copied, not imported (enrich_new_titles must not move, and the client
        # cannot import it): this pin keeps the two sets the same.
        from bot import enrich_new_titles
        self.assertEqual(supabase_client._TRANSIENT_RPC_CODES, enrich_new_titles._TRANSIENT_QUEUE_CODES)
        self.assertEqual(supabase_client._TRANSIENT_RPC_ERRORS, enrich_new_titles._TRANSIENT_QUEUE_ERRORS)
        self.assertEqual(supabase_client._RPC_ATTEMPTS, enrich_new_titles._QUEUE_ATTEMPTS)
        self.assertEqual(supabase_client._RPC_RETRY_DELAYS, enrich_new_titles._QUEUE_RETRY_DELAYS)

    def test_each_transient_kind_is_retried_and_the_third_failure_propagates(self):
        def cf(status):
            return APIError({"message": "JSON could not be generated", "code": status, "hint": None, "details": "<html>"})

        def pg(code):
            return APIError({"code": code, "message": "connection", "hint": None, "details": None})

        kinds = [cf(s) for s in (503, 520, 521, 522, 523, 524)] + [pg(c) for c in
                 ("PGRST000", "PGRST001", "PGRST002", "PGRST003")] + [
                 httpx.ReadTimeout("timed out"), httpx.ConnectError("reset"),
                 httpx.RemoteProtocolError("Server disconnected without sending a response.")]
        for error in kinds:
            with self.subTest(error=f"{type(error).__name__} {getattr(error, 'code', '')}"):
                result, client, sleeps, _ = _drain_rpc([error, _rows(1)])
                self.assertEqual(result, _rows(1))
                self.assertEqual((len(client.rpc_calls), sleeps), (2, [2]))
        third = httpx.ReadTimeout("timed out")
        result, client, sleeps, _ = _drain_rpc([cf(503), cf(520), third, _rows(1)])
        self.assertIs(result, third)
        self.assertEqual((len(client.rpc_calls), sleeps), (3, [2, 4]))

    def test_a_missing_function_or_a_4xx_is_not_retried(self):
        denied = APIError({"code": "42501", "message": "permission denied", "hint": None, "details": None})
        for error in (MISSING_FUNCTION, denied):
            with self.subTest(code=error.code):
                result, client, sleeps, _ = _drain_rpc([error, _rows(2)])

                self.assertIs(result, error)
                self.assertEqual(len(client.rpc_calls), 1)
                self.assertEqual(sleeps, [])
        # Control: a transient failure, in the same harness, is retried.
        cloudflare = APIError({"message": "JSON could not be generated", "code": 503, "hint": None, "details": "<html>"})
        result, client, sleeps, _ = _drain_rpc([cloudflare, _rows(2)])
        self.assertEqual((result, len(client.rpc_calls), sleeps), (_rows(2), 2, [2]))


# ── sync_trakt Phase B: the slices ────────────────────────────────────────

class _QueueDb:
    """The server's queue, recomputed on every ask: stamped titles drop out."""

    def __init__(self, rows, stamps_land=True, error=None, stamp_fails=None):
        self.rows = rows
        self.stamps_land = stamps_land
        self.error = error
        # media_id -> how many of its stamps fail quietly (as the real
        # stamp_trakt_related_synced does: it logs and swallows); -1 = always.
        self.stamp_fails = dict(stamp_fails or {})
        self.asks = []      # (limit, before)
        self.stamped = []   # media_id
        self.replaced = []

    def get_titles_for_related_drain(self, limit, before=None):
        self.asks.append((limit, before))
        if self.error is not None:
            raise self.error
        done = set(self.stamped) if self.stamps_land else set()
        return [r for r in self.rows if r["id"] not in done][:limit]

    def replace_media_related(self, media_id, rows, synced_at):
        self.replaced.append(media_id)

    def stamp_trakt_related_synced(self, media_id, synced_at):
        left = self.stamp_fails.get(media_id, 0)
        if left:
            self.stamp_fails[media_id] = left - 1 if left > 0 else left
            return  # failed quietly: nothing recorded, no exception
        self.stamped.append(media_id)


class _ResolvingTrakt:
    def resolve_id(self, media_type, tmdb_id):
        return tmdb_id + 1

    def get_related(self, media_type, trakt_id, limit=20):
        return []


def _phase_b(db, batch):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            stats = sync_trakt._run_phase_b(_ResolvingTrakt(), db, RUN, batch)
        except BaseException as exc:  # SystemExit included: the fail-fast is an exit
            stats = exc
    return stats, out.getvalue()


class SliceTest(unittest.TestCase):

    def test_a_full_slice_is_followed_by_another_ask(self):
        db = _QueueDb(_rows(1300))

        stats, _ = _phase_b(db, batch=3000)

        self.assertEqual(db.asks, [(1000, FIX), (1000, FIX)])   # a full slice, then a short one
        self.assertEqual(stats["scanned"], 1300)
        self.assertEqual(len(db.stamped), 1300)

    def test_the_batch_caps_the_last_ask(self):
        db = _QueueDb(_rows(1300))

        stats, _ = _phase_b(db, batch=1200)

        self.assertEqual(db.asks, [(1000, FIX), (200, FIX)])
        self.assertEqual(stats["scanned"], 1200)

    def test_an_empty_queue_is_one_ask_and_nothing_done(self):
        db = _QueueDb([])

        stats, _ = _phase_b(db, batch=3000)

        self.assertEqual(db.asks, [(1000, FIX)])
        self.assertEqual(stats["scanned"], 0)

    def test_a_full_slice_already_stamped_stops_the_loop(self):
        # The stamps do not land: the server answers the same full slice again.
        db = _QueueDb(_rows(5), stamps_land=False)

        with mock.patch.object(sync_trakt, "RELATED_SLICE", 5):
            stats, out = _phase_b(db, batch=3000)

        self.assertEqual(stats["scanned"], 5)          # processed once
        self.assertEqual(len(db.asks), 2)              # it did ask again, and stopped
        self.assertEqual(stats["stuck_at"], "m-0")
        self.assertIn("m-0", out)
        self.assertIn("«T0»", out)
        # Control: with stamps landing, the same queue is drained and nothing is stuck.
        db = _QueueDb(_rows(5))
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 5):
            stats, _ = _phase_b(db, batch=3000)
        self.assertIsNone(stats["stuck_at"])

    def test_one_quietly_failed_stamp_is_drained_again_not_a_stuck_queue(self):
        db = _QueueDb(_rows(30), stamp_fails={"m-3": 1})
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, out = _phase_b(db, batch=3000)

        self.assertIsNone(stats["stuck_at"])
        self.assertEqual(sorted(set(db.stamped)), sorted(r["id"] for r in _rows(30)))  # every title, in the end
        self.assertIn("stamp did not land for media_id=m-3", out)
        self.assertNotIn("not landing", out)

    def test_a_quiet_failure_at_the_queues_tail_is_not_a_stuck_queue(self):
        # The queue ends on a full slice: the next ask answers only the title
        # whose stamp failed quietly. A short slice of repeats is the tail.
        db = _QueueDb(_rows(10), stamp_fails={"m-3": 1})
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, out = _phase_b(db, batch=3000)

        self.assertIsNone(stats["stuck_at"])
        self.assertEqual(db.asks, [(10, FIX), (10, FIX)])
        self.assertEqual(sorted(set(db.stamped)), sorted(r["id"] for r in _rows(10)))
        self.assertIn("stamp did not land for media_id=m-3", out)
        # Control: the same queue whose stamps never land comes back FULL — stuck.
        db = _QueueDb(_rows(10), stamps_land=False)
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, _ = _phase_b(db, batch=3000)
        self.assertEqual(stats["stuck_at"], "m-0")

    def test_a_quiet_failure_before_a_small_last_ask_is_not_a_stuck_queue(self):
        # batch 11 in slices of 10: the last ask is one title, and the head's
        # quietly failed stamp fills it.
        db = _QueueDb(_rows(30), stamp_fails={"m-0": 1})
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, _ = _phase_b(db, batch=11)

        self.assertIsNone(stats["stuck_at"])
        self.assertEqual(db.asks, [(10, FIX), (1, FIX)])
        self.assertIn("m-0", db.stamped)
        # Control: with no stamp landing, the second ask of batch 20 is a full slice — stuck.
        db = _QueueDb(_rows(30), stamps_land=False)
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, _ = _phase_b(db, batch=20)
        self.assertEqual(stats["stuck_at"], "m-0")

    def test_a_lost_stamp_whose_redrain_fails_is_not_a_stuck_queue(self):
        # m-3's first stamp is lost quietly; its drain AGAIN raises, so it is
        # not stamped at all — it heads the next slice as a plain failure.
        class _FailsSecondTry(_ResolvingTrakt):
            tries = 0

            def resolve_id(self, media_type, tmdb_id):
                if tmdb_id == 1003:
                    _FailsSecondTry.tries += 1
                    if _FailsSecondTry.tries == 2:
                        raise RuntimeError("Trakt 500 after its retries")
                return tmdb_id + 1

        db = _QueueDb(_rows(50), stamp_fails={"m-3": 1})
        out = io.StringIO()
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10), contextlib.redirect_stdout(out):
            stats = sync_trakt._run_phase_b(_FailsSecondTry(), db, RUN, 30)

        self.assertIsNone(stats["stuck_at"])
        self.assertEqual(len(db.asks), 3)
        self.assertIn("m-3", db.stamped)                       # its third try landed
        # Control: two of its stamps lost (no failure between) is a stuck title.
        db = _QueueDb(_rows(50), stamp_fails={"m-3": 2})
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, out = _phase_b(db, batch=30)
        self.assertEqual(stats["stuck_at"], "m-3")
        self.assertIn("not landing", out)

    def test_a_full_slice_with_nothing_new_stops_even_with_a_failure_in_it(self):
        # No stamp lands, and m-0 always fails: the second full slice holds only
        # titles this run already tried, nine of them stamped — stuck at once.
        class _FailsHead(_ResolvingTrakt):
            def resolve_id(self, media_type, tmdb_id):
                if tmdb_id == 1000:
                    raise RuntimeError("Trakt 500 after its retries")
                return tmdb_id + 1

        db = _QueueDb(_rows(50), stamps_land=False)
        out = io.StringIO()
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10), contextlib.redirect_stdout(out):
            stats = sync_trakt._run_phase_b(_FailsHead(), db, RUN, 20)

        self.assertEqual(stats["stuck_at"], "m-1")
        self.assertEqual(stats["scanned"], 10)                 # stopped before draining slice 2
        self.assertIn("not landing", out.getvalue())
        # Control: stamps landing, the same failing head — slice 2 brings new titles.
        db = _QueueDb(_rows(50))
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10), contextlib.redirect_stdout(io.StringIO()):
            stats = sync_trakt._run_phase_b(_FailsHead(), db, RUN, 20)
        self.assertIsNone(stats["stuck_at"])

    def test_a_full_slice_of_failures_is_not_a_stuck_queue(self):
        # Every title fails (Trakt down): nothing was stamped, so nothing was
        # lost — the per-title failures are counted, as they always were.
        class _AllFail(_ResolvingTrakt):
            def resolve_id(self, media_type, tmdb_id):
                raise RuntimeError("Trakt 500 after its retries")

        db = _QueueDb(_rows(10))
        out = io.StringIO()
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10), contextlib.redirect_stdout(out):
            stats = sync_trakt._run_phase_b(_AllFail(), db, RUN, 20)

        self.assertIsNone(stats["stuck_at"])
        self.assertEqual((stats["scanned"], stats["errors"], len(db.asks)), (20, 20, 2))
        self.assertNotIn("not landing", out.getvalue())
        # Control: the same full slice drained but never stamped on the server — stuck.
        db = _QueueDb(_rows(10), stamps_land=False)
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, _ = _phase_b(db, batch=20)
        self.assertEqual(stats["stuck_at"], "m-0")

    def test_a_full_slice_mostly_of_lost_stamps_is_drained_again(self):
        # Nine of slice 1's ten stamps are lost once: slice 2 is full, nine
        # repeats and one new title — not a stuck queue.
        db = _QueueDb(_rows(50), stamp_fails={f"m-{i}": 1 for i in range(9)})
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, out = _phase_b(db, batch=30)

        self.assertIsNone(stats["stuck_at"])
        self.assertEqual(out.count("draining it again"), 9)
        self.assertEqual(len(set(db.stamped)), 21)             # 30 attempts, 9 of them re-drains
        # Control: when none of them land, the full slice is stuck.
        db = _QueueDb(_rows(50), stamps_land=False)
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, _ = _phase_b(db, batch=30)
        self.assertEqual(stats["stuck_at"], "m-0")

    def test_a_title_back_after_its_second_drain_is_a_stuck_queue(self):
        db = _QueueDb(_rows(30), stamp_fails={"m-3": -1})  # its stamp never lands
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 10):
            stats, out = _phase_b(db, batch=3000)

        self.assertEqual(stats["stuck_at"], "m-3")
        self.assertEqual(len(db.asks), 3)  # drained again once, then stopped
        self.assertIn("not landing", out)

    def test_unresolved_titles_whose_stamps_do_not_land_are_caught_too(self):
        class _NothingResolves(_ResolvingTrakt):
            def resolve_id(self, media_type, tmdb_id):
                return None

        db = _QueueDb(_rows(5), stamps_land=False)
        out = io.StringIO()
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 5), contextlib.redirect_stdout(out):
            stats = sync_trakt._run_phase_b(_NothingResolves(), db, RUN, 3000)

        self.assertEqual(stats["stuck_at"], "m-0")

    def test_any_other_queue_error_propagates(self):
        denied = APIError({"code": "42501", "message": "permission denied", "hint": None, "details": None})
        db = _QueueDb(_rows(3), error=denied)

        result, out = _phase_b(db, batch=3000)

        self.assertIs(result, denied)  # not swallowed into an empty queue, not the fail-fast
        self.assertEqual(db.stamped, [])
        self.assertNotIn("20261024210000", out)
        # Control: the missing function's line does name the migration.
        _, out = _phase_b(_QueueDb(_rows(3), error=MISSING_FUNCTION), batch=3000)
        self.assertIn("20261024210000", out)

    def test_a_failed_title_heading_the_next_slice_is_not_a_stuck_queue(self):
        # The first title of a slice fails (not stamped), so it rightly heads
        # the next slice again: that is not the stamps failing to land.
        class _FailsFirst(_ResolvingTrakt):
            def resolve_id(self, media_type, tmdb_id):
                if tmdb_id == 1000:
                    raise RuntimeError("Trakt 500 after its retries")
                return tmdb_id + 1

        db = _QueueDb(_rows(5))
        out = io.StringIO()
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 5), contextlib.redirect_stdout(out):
            stats = sync_trakt._run_phase_b(_FailsFirst(), db, RUN, 3000)

        self.assertIsNone(stats["stuck_at"])
        self.assertEqual(len(db.asks), 2)                      # it asked again
        self.assertEqual(sorted(db.stamped), ["m-1", "m-2", "m-3", "m-4"])
        self.assertEqual(stats["errors"], 2)                   # m-0 tried in both slices
        self.assertNotIn("not landing", out.getvalue())
        self.assertNotIn("stamp did not land", out.getvalue())  # a failure is not a lost stamp
        # Control: a stamp that is lost quietly is named as one.
        db = _QueueDb(_rows(5), stamp_fails={"m-0": 1})
        with mock.patch.object(sync_trakt, "RELATED_SLICE", 5):
            _, said = _phase_b(db, batch=3000)
        self.assertIn("stamp did not land for media_id=m-0", said)

    def test_a_missing_function_exits_and_stamps_nothing(self):
        db = _QueueDb(_rows(3), error=MISSING_FUNCTION)

        result, out = _phase_b(db, batch=3000)

        self.assertIsInstance(result, SystemExit)
        self.assertEqual(result.code, 1)
        self.assertEqual(db.stamped, [])
        self.assertIn("20261024210000", out)
        self.assertEqual(len([l for l in out.splitlines() if "ERROR" in l]), 1)
        # Control: a healthy queue stamps.
        db = _QueueDb(_rows(3))
        _phase_b(db, batch=3000)
        self.assertEqual(len(db.stamped), 3)


# ── sync_trakt.main: the startup guard and the SUMMARY ───────────────────

FAKE_ENV = {"TRAKT_CLIENT_ID": "fake-trakt-client-id", "SUPABASE_URL": "https://project.supabase.invalid",
            "SUPABASE_KEY": "fake-supabase-key", "TRAKT_RELATED_BATCH": "3000"}
PHASE_A = {"updated": 4, "type_mismatches": 0, "unmatched_anticipated_ids": [], "api_calls": 18}


def _main(now: datetime, phase_b_stats=None):
    calls = []
    phase_b = phase_b_stats or {"scanned": 5, "resolved": 3, "unresolved": 2, "related_rows": 40,
                                "errors": 0, "stuck_at": None}
    out = io.StringIO()
    code = None
    with mock.patch.dict(os.environ, FAKE_ENV), \
            mock.patch.object(sync_trakt, "TraktClient", lambda **kw: object()), \
            mock.patch.object(sync_trakt, "SupabaseClient", lambda **kw: object()), \
            mock.patch.object(sync_trakt, "_run_phase_a", lambda *a: calls.append("A") or PHASE_A), \
            mock.patch.object(sync_trakt, "_run_phase_b", lambda *a: calls.append(("B",) + a[2:]) or phase_b), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            sync_trakt.main(clock=lambda: now)
        except SystemExit as exc:
            code = exc.code
    return code, calls, out.getvalue()


class StartupGuardTest(unittest.TestCase):

    def test_a_clock_before_the_fix_date_stops_at_startup(self):
        code, calls, out = _main(datetime(2026, 10, 24, 15, 0, tzinfo=timezone.utc))

        self.assertEqual(code, 1)
        self.assertEqual(calls, [])  # neither phase ran
        self.assertEqual(len([l for l in out.splitlines() if "ERROR" in l]), 1)
        # Control: after the fix date, both phases run with that clock's instant.
        code, calls, _ = _main(datetime(2026, 10, 25, 3, 0, tzinfo=timezone.utc))
        self.assertIsNone(code)
        self.assertEqual(calls, ["A", ("B", "2026-10-25T03:00:00+00:00", 3000)])

    def test_the_fix_instant_itself_is_refused(self):
        # The server compares strictly (synced_at < p_before): a stamp AT the
        # fix date would leave the queue — the guard refuses it anyway, so no
        # run ever stamps on the line.
        code, calls, _ = _main(datetime(2026, 10, 24, 16, 0, tzinfo=timezone.utc))
        self.assertEqual((code, calls), (1, []))
        code, calls, _ = _main(datetime(2026, 10, 24, 16, 0, 0, 1, tzinfo=timezone.utc))
        self.assertIsNone(code)
        self.assertEqual(calls[0], "A")

    def test_the_summary_says_the_new_counters(self):
        _, _, out = _main(datetime(2026, 10, 25, 3, 0, tzinfo=timezone.utc))

        summary = next(l for l in out.splitlines() if "SUMMARY" in l)
        self.assertIn("related_scanned=5 related_resolved=3 related_unresolved=2 related_rows=40 related_errors=0",
                      summary)

    def test_a_stuck_queue_prints_the_summary_and_exits_non_zero(self):
        stuck = {"scanned": 5, "resolved": 5, "unresolved": 0, "related_rows": 0, "errors": 0, "stuck_at": "m-0"}

        code, _, out = _main(datetime(2026, 10, 25, 3, 0, tzinfo=timezone.utc), phase_b_stats=stuck)

        self.assertEqual(code, 1)
        self.assertIn("SUMMARY", out)


if __name__ == "__main__":
    unittest.main()
