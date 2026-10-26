"""
Unit tests for the budget the Trakt client enforces on itself (RUN B5.2 · 0b).

Trakt sends this client no X-Ratelimit header (run #26), so it cannot be told
where the wall is; nine 429s cost run #26 2,700 of its ≈ 4,470 seconds. The
client now keeps the monotonic send times of its requests and, before every
attempt (retries included), waits until the oldest leaves the window when the
window already holds window_calls of them (default 420 per 300 s). After the
first 429 of a run the window's capacity drops to 75 % (never below 100). The
3.0/s token bucket stays underneath as the burst cap. The SUMMARY says the
budget, how many times it waited, and the fewest requests seen between two
429s.

Fakes only: requests.get routed in memory, a fake monotonic clock that every
request moves by `step` seconds, an injected sleeper that moves it by each wait
and records it, an inert token bucket; time.sleep is replaced by a recorder
that must stay empty. No network, no real clock, no real sleep. Run from the
repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import json
import types
import unittest
from datetime import datetime, timezone
from unittest import mock

import requests

from bot import trakt as trakt_mod
from tests.test_trakt_related import BASE, _NoLimiter, _Router
from tests.test_trakt_related_queue import _main
from tests.test_trakt_wall import _Mono

START = datetime(2026, 10, 26, 2, 17, tzinfo=timezone.utc)


@contextlib.contextmanager
def _paced(routes, step: float = 1.0, **budget):
    """A real TraktClient: HTTP routed, a fake monotonic clock moved `step` s by
    each request, every sleep through the injected sleeper (mono.sleep)."""
    router, mono, out, real_sleeps, sent_times = _Router(routes), _Mono(), io.StringIO(), [], []

    def get(url, headers=None, params=None, timeout=None):
        sent_times.append(mono.t)
        response = router(url, headers=headers, params=params, timeout=timeout)
        mono.t += step
        return response

    with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), \
            mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
            mock.patch.object(trakt_mod.requests, "get", get), \
            mock.patch.object(trakt_mod.time, "sleep", real_sleeps.append), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        client = trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=mono, sleeper=mono.sleep, **budget)
        client.out, client.mono, client.real_sleeps, client.router = out, mono, real_sleeps, router
        client.sent_times = sent_times
        yield client


def _calls(client, n):
    for _ in range(n):
        client.get_trending("movie")


class WindowTest(unittest.TestCase):

    def test_a_full_window_waits_exactly_until_the_oldest_leaves(self):
        with _paced({"/movies/trending": (200, [])}, window_calls=5, window_seconds=300) as client:
            _calls(client, 5)                                 # sent at t = 0, 1, 2, 3, 4
            self.assertEqual(client.mono.waits, [])           # five in the window: none waits
            _calls(client, 1)                                 # the sixth, at t = 5
            self.assertEqual(client.mono.waits, [295.0])      # until t = 300, when t = 0 leaves
            _calls(client, 1)                                 # at t = 301: t = 1 has left too
            self.assertEqual(client.mono.waits, [295.0])      # positive control: no second wait

        out = client.out.getvalue()
        self.assertEqual(out.count("[TRAKT] Budget:"), 1)
        self.assertIn("[TRAKT] Budget: 5/5 in the last 300 s — waiting 295 s", out)
        self.assertIn("trakt_budget_waits=1", client.rate_limit_summary())
        self.assertEqual(client.real_sleeps, [])              # the injected sleeper did every wait
        self.assertEqual(len(client.router.calls), 7)

    def test_a_retry_waits_for_the_budget_too(self):
        # Two calls fill a window of 2; the second one's 500 is retried 2 s
        # later (t = 4), when the window is still full: the retry waits.
        with _paced({"/movies/trending": [(200, []), (500, {}), (200, [])]}, window_calls=2) as client:
            _calls(client, 2)

        self.assertEqual(client.mono.waits, [2.0, 296.0])     # the retry's 2 s, then the budget's
        self.assertEqual(len(client.router.calls), 3)
        self.assertEqual(client.real_sleeps, [])

    def test_the_default_budget_is_420_requests_per_300_s(self):
        with _paced({"/movies/trending": (200, [])}, step=0.1) as client:
            _calls(client, 420)
            self.assertEqual(client.mono.waits, [])
            _calls(client, 1)

        self.assertEqual(len(client.mono.waits), 1)
        self.assertIn("[TRAKT] Budget: 420/420 in the last 300 s — waiting", client.out.getvalue())


class AdaptiveFloorTest(unittest.TestCase):

    def test_a_429_lowers_the_window_to_three_quarters(self):
        routes = {"/movies/trending": [(429, {}, {"Retry-After": "1"}), (200, [])]}
        with _paced(routes, step=0.1) as client:
            _calls(client, 1)                                 # a 429, then its retry: 2 requests
            self.assertIn("[TRAKT] Budget lowered to 315 after a 429", client.out.getvalue())
            _calls(client, 313)                               # 315 in the window
            self.assertEqual(client.mono.waits, [1.0])        # only the Retry-After so far
            _calls(client, 1)                                 # the 316th waits

        self.assertEqual(len(client.mono.waits), 2)
        out = client.out.getvalue()
        self.assertIn("[TRAKT] Budget: 315/315 in the last 300 s — waiting", out)
        self.assertEqual(out.count("Budget lowered"), 1)
        # Control: the same 316 requests with no 429 never wait (the window is 420).
        with _paced({"/movies/trending": (200, [])}, step=0.1) as client:
            _calls(client, 316)
        self.assertEqual(client.mono.waits, [])
        self.assertNotIn("Budget lowered", client.out.getvalue())

    def test_the_window_is_lowered_once_a_run(self):
        routes = {"/movies/trending": [(429, {}, {"Retry-After": "1"}), (200, []),
                                       (429, {}, {"Retry-After": "1"}), (200, [])]}
        with _paced(routes, step=0.1) as client:
            _calls(client, 2)                                 # two 429s
            _calls(client, 311)                               # 315 in the window
            self.assertEqual(client.mono.waits, [1.0, 1.0])
            _calls(client, 1)

        out = client.out.getvalue()
        self.assertEqual(out.count("Budget lowered to 315"), 1)
        self.assertIn("Budget: 315/315", out)                 # not 236: 75 % of 420, once

    def test_the_lowered_window_is_never_below_100(self):
        with _paced({"/movies/trending": [(429, {}, {"Retry-After": "1"}), (200, [])]}, window_calls=120) as client:
            _calls(client, 1)

        self.assertIn("Budget lowered to 100 after a 429", client.out.getvalue())   # not 90
        # Control: 75 % of 420 is above 100, and is what a 429 sets.
        with _paced({"/movies/trending": [(429, {}, {"Retry-After": "1"}), (200, [])]}) as client:
            _calls(client, 1)
        self.assertIn("Budget lowered to 315 after a 429", client.out.getvalue())

    def test_a_429_still_honours_retry_after_through_the_sleeper(self):
        with _paced({"/movies/trending": [(429, {}, {"Retry-After": "300"}), (200, [])]}) as client:
            _calls(client, 1)

        self.assertEqual(client.mono.waits, [300.0])
        self.assertEqual(client.real_sleeps, [])


def _limit(remaining):
    return {"X-Ratelimit": json.dumps({"name": "UNAUTHED_API_GET_LIMIT", "period": 300, "limit": 1000,
                                       "remaining": remaining, "until": "2026-10-26T02:22:00Z"})}


class SummaryTest(unittest.TestCase):

    def test_before_any_response_the_limit_is_unknown(self):
        with _paced({}) as client:
            pass

        self.assertEqual(client.rate_limit_summary(),
                         "trakt_limit=unknown trakt_pauses=0 trakt_429=0 "
                         "trakt_budget=420/300s trakt_budget_waits=0 trakt_wall_calls=-")

    def test_a_response_without_the_header_says_header_missing(self):
        with _paced({"/movies/trending": (200, [])}) as client:
            _calls(client, 1)
        self.assertIn("trakt_limit=header-missing ", client.rate_limit_summary())
        # Controls: a well-formed header names the window; a malformed one is not "missing".
        with _paced({"/movies/trending": (200, [], _limit(999))}) as client:
            _calls(client, 1)
        self.assertIn("trakt_limit=1000/300s ", client.rate_limit_summary())
        with _paced({"/movies/trending": (200, [], {"X-Ratelimit": "not json"})}) as client:
            _calls(client, 1)
        self.assertIn("trakt_limit=unknown ", client.rate_limit_summary())

    def test_the_summary_carries_the_budget_its_waits_and_the_wall(self):
        routes = {"/movies/trending": [(200, []), (200, []), (429, {}, {"Retry-After": "1"}), (200, [])]}
        with _paced(routes, window_calls=150, window_seconds=600) as client:
            _calls(client, 3)                                 # the third's first try is the 429: B = 3

        summary = client.rate_limit_summary()
        self.assertIn("trakt_budget=150/600s trakt_budget_waits=0 trakt_wall_calls=3", summary)
        self.assertTrue(summary.startswith("trakt_limit=header-missing trakt_pauses=0 trakt_429=1 "))


class MainWindowEnvTest(unittest.TestCase):

    def test_main_reads_the_window_from_the_environment(self):
        cases = (({"TRAKT_WINDOW_CALLS": None, "TRAKT_WINDOW_SECONDS": None}, 420, 300),
                 ({"TRAKT_WINDOW_CALLS": "300", "TRAKT_WINDOW_SECONDS": "600"}, 300, 600))
        for env, calls, seconds in cases:
            with self.subTest(env=env):
                seen = {}
                _main(START, env=env, seen=seen)
                self.assertEqual(seen["trakt"].get("window_calls"), calls)
                self.assertEqual(seen["trakt"].get("window_seconds"), seconds)
                self.assertTrue(callable(seen["trakt"].get("clock")))   # the clock still goes in

    def test_the_client_defaults_are_420_per_300_s(self):
        self.assertEqual((trakt_mod.DEFAULT_WINDOW_CALLS, trakt_mod.DEFAULT_WINDOW_SECONDS), (420, 300))

    def test_a_window_that_cannot_work_stops_the_run_before_phase_a(self):
        for env, word in (({"TRAKT_WINDOW_CALLS": "0"}, "TRAKT_WINDOW_CALLS"),
                          ({"TRAKT_WINDOW_CALLS": "-1"}, "TRAKT_WINDOW_CALLS"),
                          ({"TRAKT_WINDOW_SECONDS": "0"}, "TRAKT_WINDOW_SECONDS"),
                          ({"TRAKT_WINDOW_SECONDS": "601"}, "TRAKT_WINDOW_SECONDS")):
            with self.subTest(env=env):
                code, calls, out = _main(START, env=env)
                self.assertEqual((code, calls), (1, []))         # neither phase ran
                errors = [line for line in out.splitlines() if "ERROR" in line]
                self.assertEqual(len(errors), 1)
                self.assertIn(word, errors[0])
        # Control: the edges themselves are accepted.
        for env in ({"TRAKT_WINDOW_CALLS": "1", "TRAKT_WINDOW_SECONDS": "1"},
                    {"TRAKT_WINDOW_SECONDS": "600"}):
            with self.subTest(env=env):
                code, calls, _ = _main(START, env=env)
                self.assertIsNone(code)
                self.assertEqual(calls[0], "A")

    def test_the_client_refuses_a_window_that_cannot_work(self):
        for kwargs in ({"window_calls": 0}, {"window_seconds": 0}):
            with self.subTest(**kwargs), self.assertRaises(ValueError):
                trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=lambda: 0.0, **kwargs)
        trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=lambda: 0.0,
                              window_calls=1, window_seconds=1)   # control: the smallest that works


# ── B5.2 · 0b's review: the echo, the orders, the edges ─────────────────

class EchoTest(unittest.TestCase):

    def test_a_sub_second_echo_is_slept_but_neither_printed_nor_counted(self):
        # Once a window has filled, the next request is often ready a moment
        # before the slot it needs frees: that is the window's echo, not a
        # full window. Sent at 0 and 0.5; the third waits 9 s (t = 10); the
        # fourth, ready at 10.2, waits 0.3 s for t = 0.5 to leave.
        with _paced({"/movies/trending": (200, [])}, step=0.0, window_calls=2, window_seconds=10) as client:
            for t in (0.0, 0.5, 1.0):
                client.mono.t = t
                _calls(client, 1)
            client.mono.t += 0.2
            _calls(client, 1)

        self.assertEqual(len(client.mono.waits), 2)
        self.assertAlmostEqual(client.mono.waits[0], 9.0)
        self.assertAlmostEqual(client.mono.waits[1], 0.3)     # slept…
        out = client.out.getvalue()
        self.assertEqual(out.count("[TRAKT] Budget:"), 1)     # …but only the full window is said
        self.assertIn("[TRAKT] Budget: 2/2 in the last 10 s — waiting 9 s", out)
        self.assertIn("trakt_budget_waits=1 ", client.rate_limit_summary())
        self.assertNotIn("waiting 0 s", out)

    def test_a_long_drain_counts_only_its_full_windows(self):
        # 5,000 requests a little faster than the window allows, jittered:
        # the window fills about every 420 requests, and only those waits count.
        import random
        rng = random.Random(7)
        with _paced({"/movies/trending": (200, [])}, step=0.0) as client:
            for _ in range(5000):
                client.mono.t += rng.uniform(0.2, 0.52)
                _calls(client, 1)

        long_waits = [w for w in client.mono.waits if w >= 1.0]
        self.assertGreater(len(client.mono.waits), len(long_waits))   # there were echoes, and they were slept
        self.assertIn("trakt_budget_waits=%d " % len(long_waits), client.rate_limit_summary())
        self.assertEqual(client.out.getvalue().count("[TRAKT] Budget:"), len(long_waits))
        self.assertTrue(10 <= len(long_waits) <= 12, long_waits)       # ≈ 5,000 / 420
        # Never more than 420 sent in any 300 s, over the whole run. (1 µs of
        # slack: a fake clock that adds a computed wait can land a float's
        # hair short of the window's edge.)
        sends, start, busiest = client.sent_times, 0, 0
        for end, t in enumerate(sends):
            while t - sends[start] >= 300 - 1e-6:
                start += 1
            busiest = max(busiest, end - start + 1)
        self.assertEqual(len(sends), 5000)
        self.assertEqual(busiest, 420)


class OrderTest(unittest.TestCase):

    def test_the_header_pause_goes_through_the_sleeper_before_the_budget(self):
        routes = {"/movies/trending": [(200, [], _limit(999)), (200, [], _limit(25)), (200, [])]}
        clock = lambda: datetime(2026, 10, 26, 2, 21, tzinfo=timezone.utc)   # 60 s before _limit's until
        with _paced(routes, window_calls=2, clock=clock) as client:
            _calls(client, 3)

        self.assertEqual(client.mono.waits, [60.0, 238.0])    # the pause, then the budget from where it left
        self.assertEqual(client.real_sleeps, [])

    def test_the_budget_waits_before_the_token_bucket(self):
        with _paced({"/movies/trending": (200, [])}, window_calls=1) as client:
            acquired_at = []
            client._limiter = types.SimpleNamespace(acquire=lambda: acquired_at.append(client.mono()))
            _calls(client, 2)

        self.assertEqual(acquired_at, [0.0, 300.0])           # the second acquire comes after the 299 s wait

    def test_the_token_bucket_is_built_at_three_per_second(self):
        built = []

        class _Recording(_NoLimiter):
            def __init__(self, *args, **kwargs):
                built.append((args, kwargs))

        with mock.patch.object(trakt_mod, "RateLimiter", _Recording):
            trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=lambda: 0.0)
        self.assertEqual(built, [((), {"rate": 3.0})])

    def test_after_a_wait_the_window_counts_from_its_end_on_a_frozen_clock(self):
        sleeps, router = [], _Router({"/movies/trending": (200, [])})
        with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
                mock.patch.object(trakt_mod.requests, "get", router), contextlib.redirect_stdout(io.StringIO()):
            client = trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=lambda: 0.0,
                                           sleeper=sleeps.append, window_calls=2)
            _calls(client, 4)

        self.assertEqual(sleeps, [300.0])                     # the third waits; the fourth finds room


class FloorEdgesTest(unittest.TestCase):

    def test_a_lowered_window_fuller_than_its_capacity_waits_for_enough_to_leave(self):
        routes = {"/movies/trending": [(200, [])] * 199 + [(429, {}, {"Retry-After": "1"}), (200, [])]}
        with _paced(routes, window_calls=200) as client:
            _calls(client, 200)          # sent at t = 0..198; the 200th's 429 at 199 lowers to 150

        self.assertEqual(client.mono.waits, [1.0, 149.0])     # until t = 50 leaves, not t = 0
        self.assertIn("[TRAKT] Budget: 200/150 in the last 300 s — waiting 149 s", client.out.getvalue())

    def test_a_window_below_the_floor_is_not_raised_by_a_429(self):
        routes = {"/movies/trending": [(429, {}, {"Retry-After": "1"}), (200, [])]}
        with _paced(routes, window_calls=50) as client:
            _calls(client, 49)            # the 429 and its retry, then 48: 50 in the window
            self.assertEqual(client.mono.waits, [1.0])
            _calls(client, 1)

        out = client.out.getvalue()
        self.assertNotIn("Budget lowered", out)
        self.assertIn("[TRAKT] Budget: 50/50 in the last 300 s", out)

    def test_only_a_429_lowers_the_window(self):
        with _paced({"/movies/trending": [(500, {}), (200, [])]}) as client:
            _calls(client, 1)
        self.assertNotIn("Budget lowered", client.out.getvalue())
        # Control: a 429 does.
        with _paced({"/movies/trending": [(429, {}, {"Retry-After": "1"}), (200, [])]}) as client:
            _calls(client, 1)
        self.assertIn("Budget lowered to 315", client.out.getvalue())


class SummaryEdgesTest(unittest.TestCase):

    def test_the_summary_wall_is_the_fewest_requests_up_to_a_429(self):
        routes = {"/movies/trending": [(200, []), (429, {}, {"Retry-After": "1"}), (200, []), (200, []),
                                       (200, []), (200, []), (429, {}, {"Retry-After": "1"}), (200, [])]}
        with _paced(routes) as client:
            _calls(client, 6)             # 429s at requests 2 and 7: 2, then 5

        summary = client.rate_limit_summary()
        self.assertIn("trakt_429=2 ", summary)
        self.assertIn("trakt_wall_calls=2", summary)

    def test_requests_without_a_response_leave_the_limit_unknown(self):
        with _paced({}) as client:
            with mock.patch.object(trakt_mod.requests, "get", side_effect=requests.ConnectionError("reset")):
                with self.assertRaises(requests.ConnectionError):
                    client.get_trending("movie")

        self.assertIn("trakt_limit=unknown ", client.rate_limit_summary())

    def test_a_header_only_on_a_429_is_not_missing(self):
        routes = {"/movies/trending": [(429, {}, {"Retry-After": "1", "X-Ratelimit": "not json"}), (200, [])]}
        with _paced(routes) as client:
            _calls(client, 1)
        self.assertIn("trakt_limit=unknown ", client.rate_limit_summary())


if __name__ == "__main__":
    unittest.main()
