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
import traceback
import unittest
from unittest import mock

import requests

from bot import trakt as trakt_mod
from tests.test_trakt_related import BASE, _NoLimiter, _Router, _response

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


# ── B5.2 · 0a's review: what the first tests did not pin ─────────────────

class _Flaky:
    """requests.get that raises on its first call (after moving the clock),
    then answers through the routed fake."""

    def __init__(self, inner, mono, first_error, seconds=15.0):
        self.inner, self.mono, self.first_error, self.seconds, self.calls = inner, mono, first_error, seconds, 0

    def __call__(self, url, headers=None, params=None, timeout=None):
        self.calls += 1
        if self.calls == 1:
            self.mono.t += self.seconds
            raise self.first_error
        return self.inner(url, headers=headers, params=params, timeout=timeout)


class WallMeasurementEdgesTest(unittest.TestCase):

    def test_the_estimate_is_the_minimum_not_the_last(self):
        # 429s after 2 requests, then after 5: the minimum is the first.
        routes = {"/movies/trending": [(200, []), _too_many(), (200, []), (200, []), (200, []), (200, []),
                                       _too_many(), (200, [])]}
        with _measured(routes) as client:
            for _ in range(6):
                client.get_trending("movie")
        out = client.out.getvalue()

        self.assertIn("429 #1 — requests since start=2, since previous 429=2 over", out)
        self.assertIn("429 #2 — requests since start=7, since previous 429=5 over", out)
        self.assertEqual(client.wall_estimate(), 2)

    def test_a_429_on_the_very_first_response_is_measured_from_it(self):
        with _measured({"/movies/trending": [_too_many(), (200, [])]}) as client:
            client.get_trending("movie")

        self.assertIn("[TRAKT] 429 #1 — requests since start=1, since previous 429=1 over 0.0 s (monotonic), "
                      "Retry-After=2,", client.out.getvalue())
        self.assertEqual(client.wall_estimate(), 1)           # one 429 is an estimate too

    def test_every_429_prints_its_line_the_last_attempt_too(self):
        with _measured({"/movies/trending": [_too_many(), _too_many(), _too_many()]}) as client:
            with self.assertRaises(requests.HTTPError):
                client.get_trending("movie")
        out = client.out.getvalue()

        self.assertEqual(out.count("[TRAKT] 429 #"), 3)
        self.assertIn("[TRAKT] 429 #3 — requests since start=3, since previous 429=1 over", out)

    def test_s_counts_from_the_first_request_sent_not_from_the_client(self):
        with _measured({"/movies/trending": [(200, []), _too_many(), (200, [])]}) as client:
            client.mono.t = 100.0                             # the client was built at t = 0
            client.get_trending("movie")                      # sent at 100
            client.get_trending("movie")                      # the 429, sent at 101

        self.assertIn("429 #1 — requests since start=2, since previous 429=2 over 1.0 s (monotonic)",
                      client.out.getvalue())

    def test_the_429_line_is_printed_before_the_retry_after_sleep(self):
        seen = []
        with _measured({"/movies/trending": [_too_many("300"), (200, [])]}) as client:
            sleep = client.mono.sleep

            def snapshot_then_sleep(seconds):
                seen.append(client.out.getvalue())
                sleep(seconds)

            with mock.patch.object(trakt_mod.time, "sleep", snapshot_then_sleep):
                client.get_trending("movie")

        self.assertEqual(client.mono.waits, [300.0])
        self.assertIn("[TRAKT] 429 #1 —", seen[0])             # said before the 300 s, not after

    def test_an_attempt_that_got_no_response_is_counted_and_the_first_response_still_named(self):
        with _measured({"/movies/trending": [_too_many(), (200, [])]}) as client:
            flaky = _Flaky(trakt_mod.requests.get, client.mono, requests.ConnectionError("connection reset"))
            with mock.patch.object(trakt_mod.requests, "get", flaky):
                client.get_trending("movie")
        out = client.out.getvalue()

        # Sent at t = 0 (no answer; the clock moved 15 s; 2 s retry sleep), then 17 (the 429).
        self.assertIn("[TRAKT] 429 #1 — requests since start=2, since previous 429=2 over 17.0 s (monotonic)", out)
        self.assertEqual(out.count("[TRAKT] First response headers:"), 1)
        self.assertLess(out.index("Request failed (attempt 1/3): connection reset"),
                        out.index("[TRAKT] First response headers:"))

    def test_a_5xx_retry_is_an_attempt_too(self):
        with _measured({"/movies/trending": [(500, {}), _too_many(), (200, [])]}) as client:
            client.get_trending("movie")

        self.assertIn("429 #1 — requests since start=2, since previous 429=2 over", client.out.getvalue())

    def test_header_names_sort_without_regard_to_case_and_no_url_is_printed(self):
        with _measured({"/movies/trending": [(429, {}, {"cf-ray": "8d1e-EZE", "Retry-After": "2"}), (200, [])]}) \
                as client:
            client.get_trending("movie")
        out = client.out.getvalue()

        self.assertIn("headers=['cf-ray', 'Content-Type', 'Retry-After']", out)
        self.assertNotIn("8d1e-EZE", out)
        line = next(l for l in out.splitlines() if l.startswith("[TRAKT] 429 #1"))
        self.assertNotIn("http", line)
        self.assertNotIn("trending", line)                    # no path either
        self.assertIn("Retry-After=2,", line)                 # positive control


class NoValueLeaksTest(unittest.TestCase):

    def test_a_malformed_client_id_never_reaches_the_log_or_the_error(self):
        # requests refuses a header value with surrounding whitespace or CR/LF,
        # and its InvalidHeader message quotes the VALUE — here, the client id.
        def prepare_then_answer(url, headers=None, params=None, timeout=None):
            requests.Request("GET", url, headers=headers, params=params).prepare()   # requests' own header check
            return _Router({})(url, headers=headers, params=params, timeout=timeout)

        for client_id in (" fake-secret-client-id", "fake-secret-client-id\r\nline-2"):
            with self.subTest(client_id=repr(client_id)):
                mono, out = _Mono(), io.StringIO()
                with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), \
                        mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
                        mock.patch.object(trakt_mod.requests, "get", prepare_then_answer), \
                        mock.patch.object(trakt_mod.time, "sleep", mono.sleep), contextlib.redirect_stdout(out):
                    client = trakt_mod.TraktClient(client_id=client_id, monotonic=mono)
                    try:
                        client.get_trending("movie")
                    except requests.RequestException:
                        out.write(traceback.format_exc())
                text = out.getvalue()
                self.assertNotIn("fake-secret-client-id", text)
                self.assertIn("Request failed (attempt 3/3): a request header is malformed (value withheld)", text)
        # Control: a well-formed id goes through to a response.
        mono, out = _Mono(), io.StringIO()
        with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
                mock.patch.object(trakt_mod.requests, "get",
                                  lambda url, **kw: (requests.Request("GET", url, headers=kw.get("headers")).prepare(),
                                                     _Router({"/movies/trending": (200, [])})(url, **kw))[1]), \
                mock.patch.object(trakt_mod.time, "sleep", mono.sleep), contextlib.redirect_stdout(out):
            trakt_mod.TraktClient(client_id="fake-secret-client-id", monotonic=mono).get_trending("movie")
        self.assertIn("[TRAKT] First response headers:", out.getvalue())
        self.assertNotIn("fake-secret-client-id", out.getvalue())

    def test_no_query_string_reaches_a_failure_line(self):
        # requests' HTTPError says "… for url: <the full URL>", parameters included.
        def five_hundred(url, headers=None, params=None, timeout=None):
            full = requests.Request("GET", url, params=params).prepare().url
            response = _response(500, {"error": "boom"}, full)
            response.reason = "Internal Server Error"
            return response

        mono, out = _Mono(), io.StringIO()
        with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
                mock.patch.object(trakt_mod.requests, "get", five_hundred), \
                mock.patch.object(trakt_mod.time, "sleep", mono.sleep), contextlib.redirect_stdout(out):
            client = trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=mono)
            with self.assertRaises(requests.HTTPError):
                client.get_most_watched("movie", "weekly", 1, 100)
        text = out.getvalue()

        self.assertNotIn("extended=full", text)
        self.assertNotIn("limit=100", text)
        # Positive control: the failure, its status and its path are still said.
        self.assertIn("Request failed (attempt 3/3): 500 Server Error: Internal Server Error for url: "
                      f"{BASE}/movies/watched/weekly", text)

    def test_no_query_string_reaches_the_raised_error_either(self):
        # Phase A's list fetches raise out of main() by design: Python prints
        # the error's message in the traceback, so it must carry no query.
        for status, reason in ((500, "Internal Server Error"), (401, "Unauthorized")):
            def answer(url, headers=None, params=None, timeout=None, status=status, reason=reason):
                full = requests.Request("GET", url, params=params).prepare().url
                response = _response(status, {"error": "no"}, full)
                response.reason = reason
                return response

            with self.subTest(status=status):
                mono, out = _Mono(), io.StringIO()
                with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), \
                        mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
                        mock.patch.object(trakt_mod.requests, "get", answer), \
                        mock.patch.object(trakt_mod.time, "sleep", mono.sleep), contextlib.redirect_stdout(out):
                    client = trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=mono)
                    try:
                        client.get_most_watched("movie", "weekly", 1, 100)
                    except requests.HTTPError as exc:
                        raised, shown = exc, traceback.format_exc()
                self.assertNotIn("extended=full", shown)
                self.assertNotIn("limit=100", str(raised))
                # Positive controls: the status and the path are still there, and
                # the response still rides on the error (resolve_id reads its status).
                self.assertIn(f"{status} Client Error" if status < 500 else f"{status} Server Error", shown)
                self.assertIn(f"for url: {BASE}/movies/watched/weekly", shown)
                self.assertEqual(raised.response.status_code, status)

    def test_no_query_string_reaches_the_drains_failure_line(self):
        from bot import sync_trakt
        from tests.test_trakt_write_and_decay import _WriteDb

        class _Refusing:
            def resolve_id(self, media_type, tmdb_id):
                raise requests.HTTPError(f"403 Client Error: Forbidden for url: {BASE}/search/tmdb/278?type=movie&x=leak")

            def get_related(self, media_type, trakt_id, limit=20):
                return []

        out = io.StringIO()
        db = _WriteDb([{"id": "m-1", "tmdb_id": 278, "media_type": "movie", "title": "T", "popularity": 1.0}])
        with contextlib.redirect_stdout(out):
            stats = sync_trakt._run_phase_b(_Refusing(), db, "2026-10-26T02:17:00+00:00", 1)

        self.assertEqual(stats["errors"], 1)
        self.assertNotIn("x=leak", out.getvalue())
        self.assertIn(f"related drain failed — 403 Client Error: Forbidden for url: {BASE}/search/tmdb/278",
                      out.getvalue())

    def test_a_malformed_page_count_is_said_without_its_value(self):
        with _measured({"/movies/trending": (200, [], {"X-Pagination-Page-Count": "VALUE-7f3a"})}) as client:
            client.get_trending("movie")
        out = client.out.getvalue()

        self.assertNotIn("VALUE-7f3a", out)
        self.assertIn("[TRAKT] Malformed X-Pagination-Page-Count header for /movies/trending", out)

    def test_the_names_of_the_values_held_back_are_printed(self):
        # Positive controls for test_no_header_value_but_retry_after_is_ever_printed's negatives.
        with _measured({"/movies/trending": [_too_many(), (200, [])]}) as client:
            client.get_trending("movie")
        out = client.out.getvalue()

        self.assertIn("'X-Request-Id'", out)
        self.assertIn("'Content-Type'", out)
        self.assertEqual(client._headers["trakt-api-key"], "fake-trakt-client-id")   # the id WAS sent
        self.assertNotIn("fake-trakt-client-id", out)

if __name__ == "__main__":
    unittest.main()
