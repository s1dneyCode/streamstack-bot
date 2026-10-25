"""
Unit tests for measuring Trakt's wall on every 429 (RUN B5.2 · 0a).

Run #26 (25/10) hit nine 429s and never once received the X-Ratelimit header
the B5.1 pause was built on. So the client now says what it can see: on each
429, one line with the requests it made since the start and since the previous
429, the monotonic seconds between them, the Retry-After it got and the NAMES
of the response's headers. On the first response of a run it also prints that
response's header names once. Header VALUES are never printed, save Retry-After
and X-Ratelimit. wall_estimate() is the fewest requests seen between two 429s.

Fakes only: requests.get routed in memory (tests/test_trakt_related.py's
router), a fake monotonic clock that moves one second per request and that
every sleep moves too, an inert token bucket. No network, no real clock, no
real sleep. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import unittest
from unittest import mock

from bot import trakt as trakt_mod
from tests.test_trakt_related import BASE, _NoLimiter, _Router

SECRET_LOOKING = "Bearer not-a-real-token-but-never-print-it"


class _Mono:
    """A fake monotonic clock. Each request moves it one second; each sleep
    moves it by the sleep, and is recorded."""

    def __init__(self, t: float = 0.0):
        self.t = t
        self.waits = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.t += seconds


@contextlib.contextmanager
def _measured(routes: dict, step: float = 1.0):
    """A real TraktClient on a fake clock: HTTP routed, sleeps on the fake clock."""
    router, mono, out = _Router(routes), _Mono(), io.StringIO()

    def get(url, headers=None, params=None, timeout=None):
        response = router(url, headers=headers, params=params, timeout=timeout)
        mono.t += step
        return response

    with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), \
            mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
            mock.patch.object(trakt_mod.requests, "get", get), \
            mock.patch.object(trakt_mod.time, "sleep", mono.sleep), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        client = trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=mono)
        client.out, client.mono = out, mono
        yield client


def _too_many(retry_after="2"):
    return (429, {}, {"Retry-After": retry_after, "Authorization": SECRET_LOOKING, "X-Request-Id": "req-7f3a"})


class WallMeasurementTest(unittest.TestCase):

    def _two_429s(self):
        # Requests are sent at t = 0, 1, 2, 3(429), 6, 7, 8(429), 11 — each takes 1 s,
        # each Retry-After of 2 s moves the clock too.
        with _measured({"/movies/trending": [(200, []), (200, []), (200, []), _too_many(),
                                             (200, []), (200, []), _too_many(), (200, [])]}) as client:
            for _ in range(6):
                client.get_trending("movie")
        return client

    def test_each_429_says_how_many_requests_and_seconds_it_took(self):
        client = self._two_429s()
        out = client.out.getvalue()

        self.assertIn("[TRAKT] 429 #1 — requests since start=4, since previous 429=4 over 3.0 s (monotonic), "
                      "Retry-After=2, headers=['Authorization', 'Content-Type', 'Retry-After', 'X-Request-Id']", out)
        self.assertIn("[TRAKT] 429 #2 — requests since start=7, since previous 429=3 over 5.0 s (monotonic), "
                      "Retry-After=2, headers=", out)
        self.assertEqual(out.count("[TRAKT] 429 #"), 2)        # one line per 429
        self.assertIn("Rate limited (429, attempt 1/3)", out)  # B5.1's line stays

    def test_no_header_value_but_retry_after_is_ever_printed(self):
        client = self._two_429s()
        out = client.out.getvalue()

        self.assertNotIn(SECRET_LOOKING, out)
        self.assertNotIn("not-a-real-token", out)
        self.assertNotIn("req-7f3a", out)
        self.assertNotIn("application/json", out)              # Content-Type's value neither
        self.assertNotIn("fake-trakt-client-id", out)          # nor the client id the request carried
        # Positive control: the names are there, and Retry-After's value is.
        self.assertIn("'Authorization'", out)
        self.assertIn("Retry-After=2,", out)

    def test_the_wall_estimate_is_the_fewest_requests_between_two_429s(self):
        self.assertEqual(self._two_429s().wall_estimate(), 3)
        # Control: a run without a 429 has no estimate.
        with _measured({"/movies/trending": (200, [])}) as client:
            client.get_trending("movie")
        self.assertIsNone(client.wall_estimate())

    def test_a_429_without_retry_after_says_so(self):
        with _measured({"/movies/trending": [(429, {}), (200, [])]}) as client:
            client.get_trending("movie")

        self.assertIn("Retry-After=-, headers=['Content-Type']", client.out.getvalue())


class FirstResponseTest(unittest.TestCase):

    def test_the_first_response_names_its_headers_once(self):
        with _measured({"/movies/trending": (200, [], {"X-Request-Id": "req-7f3a", "Authorization": SECRET_LOOKING})}) \
                as client:
            for _ in range(3):
                client.get_trending("movie")
        out = client.out.getvalue()

        self.assertEqual(out.count("[TRAKT] First response headers:"), 1)
        self.assertIn("[TRAKT] First response headers: ['Authorization', 'Content-Type', 'X-Request-Id']", out)
        self.assertNotIn("req-7f3a", out)                      # names only
        self.assertNotIn(SECRET_LOOKING, out)

    def test_a_429_first_is_still_the_first_response(self):
        with _measured({"/movies/trending": [_too_many(), (200, [])]}) as client:
            client.get_trending("movie")
        out = client.out.getvalue()

        self.assertEqual(out.count("[TRAKT] First response headers:"), 1)
        self.assertLess(out.index("First response headers"), out.index("429 #1"))


if __name__ == "__main__":
    unittest.main()
