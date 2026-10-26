"""
Unit tests for the two 429 leftovers of B5.2 (RUN B5.3 · 0a):

  1. A 429 on the LAST attempt used to raise without sleeping its
     Retry-After, so the run's next request left inside Trakt's penalty.
     Now it arms the same pause the X-Ratelimit path arms, and the next
     request of the run waits it out first. The raise itself is unchanged:
     that title fails, the drain goes on.
  2. A Retry-After shaped as an HTTP date used to escape _request as a
     ValueError. Now it is read as seconds or as a date (against the
     injected clock), bounded 1–300 s; anything else waits the 2.0 s
     default and says so in one line.

Fakes only: tests/test_trakt_budget.py's paced client (routed requests, a
fake monotonic clock, an injected sleeper that records every wait), plus a
fixed wall clock for the dates. No network, no real clock, no real sleep.
Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import unittest
from datetime import datetime, timezone

import requests

from tests.test_trakt_budget import _calls, _paced

NOW = datetime(2026, 10, 21, 7, 27, 0, tzinfo=timezone.utc)
CLOCK = lambda: NOW  # noqa: E731 — the wall clock the pauses and the dates read


def _too_many(retry_after):
    return (429, {}, {"Retry-After": retry_after})


class LastAttemptPenaltyTest(unittest.TestCase):

    def test_a_last_attempt_429_makes_the_next_request_wait_its_retry_after(self):
        routes = {"/movies/trending": [_too_many("300"), _too_many("300"), _too_many("300"), (200, [])]}
        with _paced(routes, clock=CLOCK) as client:
            with self.assertRaises(requests.HTTPError):
                client.get_trending("movie")              # three 429s: the title fails, as before
            self.assertEqual(client.mono.waits, [300.0, 300.0])   # the third's penalty is not slept yet…
            client.get_trending("movie")                  # …but before the run's next request
            self.assertEqual(client.mono.waits, [300.0, 300.0, 300.0])
            sent_before = len(client.sent_times)
            client.get_trending("movie")                  # the pause is spent once
            self.assertEqual(client.mono.waits, [300.0, 300.0, 300.0])

        self.assertEqual(sent_before, 4)
        # Sent at 0, 301, 602 (the three 429s), then 903: after the 300 s penalty.
        self.assertEqual(client.sent_times[:4], [0.0, 301.0, 602.0, 903.0])
        self.assertIn("trakt_pauses=1 ", client.rate_limit_summary())
        self.assertEqual(client.out.getvalue().count("[TRAKT] Penalty from a 429"), 1)
        self.assertEqual(client.real_sleeps, [])

    def test_with_no_429_the_next_request_goes_at_once(self):
        with _paced({"/movies/trending": (200, [])}, clock=CLOCK) as client:
            _calls(client, 2)

        self.assertEqual(client.mono.waits, [])
        self.assertIn("trakt_pauses=0 ", client.rate_limit_summary())
        self.assertNotIn("Penalty from a 429", client.out.getvalue())

    def test_a_retried_429_still_waits_in_place_and_arms_nothing(self):
        # B5.1's behaviour for a 429 that is retried: Retry-After slept right
        # away, and no second wait before the next request.
        with _paced({"/movies/trending": [_too_many("300"), (200, []), (200, [])]}, clock=CLOCK) as client:
            _calls(client, 2)

        self.assertEqual(client.mono.waits, [300.0])
        self.assertIn("trakt_pauses=0 ", client.rate_limit_summary())


class RetryAfterShapesTest(unittest.TestCase):

    def _one_429(self, retry_after):
        with _paced({"/movies/trending": [_too_many(retry_after), (200, [])]}, clock=CLOCK) as client:
            _calls(client, 1)
        return client.mono.waits, client.out.getvalue()

    def test_an_http_date_waits_until_that_moment(self):
        waits, out = self._one_429("Wed, 21 Oct 2026 07:28:00 GMT")     # 60 s after NOW

        self.assertEqual(waits, [60.0])
        self.assertNotIn("not understood", out)

    def test_a_number_of_seconds_is_unchanged(self):
        waits, _ = self._one_429("7")
        self.assertEqual(waits, [7.0])

    def test_garbage_waits_the_default_and_says_so_once(self):
        waits, out = self._one_429("soon")

        self.assertEqual(waits, [2.0])
        self.assertEqual(out.count("[TRAKT] Retry-After 'soon' not understood — waiting 2.0 s"), 1)

    def test_the_wait_is_bounded_between_1_and_300_seconds(self):
        cases = (("Wed, 21 Oct 2026 08:27:00 GMT", 300.0),   # an hour away
                 ("Wed, 21 Oct 2026 07:00:00 GMT", 1.0),     # already past
                 ("3600", 300.0),
                 ("0", 1.0))
        for retry_after, expected in cases:
            with self.subTest(retry_after=retry_after):
                waits, _ = self._one_429(retry_after)
                self.assertEqual(waits, [expected])

    def test_a_date_on_the_last_attempt_arms_the_pause_for_its_seconds(self):
        date = "Wed, 21 Oct 2026 07:28:00 GMT"
        routes = {"/movies/trending": [_too_many(date), _too_many(date), _too_many(date), (200, [])]}
        with _paced(routes, clock=CLOCK) as client:
            with self.assertRaises(requests.HTTPError):
                client.get_trending("movie")
            client.get_trending("movie")

        self.assertEqual(client.mono.waits, [60.0, 60.0, 60.0])


if __name__ == "__main__":
    unittest.main()
