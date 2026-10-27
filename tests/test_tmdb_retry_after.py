"""
Unit tests for TmdbClient._get's 429 wait (RUN B5.5 · 0c(3)): Retry-After read
as seconds or as an HTTP date, the way bot/trakt.py reads it since B5.3, and
bounded to 1–300 s; absent, 2 s; not understood, 2 s and one line. Until B5.5
it was float(Retry-After): an HTTP date raised a ValueError out of _get, so
the 429's retry was lost and the caller logged a conversion error instead.

Fakes only: a real TmdbClient whose requests.get answers in memory, a clock
the test hands in, and a recording sleep. No network. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import json
import types
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest import mock

import requests

from bot import tmdb as tmdb_mod

NOW = datetime(2026, 10, 27, 6, 0, tzinfo=timezone.utc)


def _response(status, body, url, headers=None):
    r = requests.Response()
    r.status_code, r._content, r.url = status, json.dumps(body).encode("utf-8"), url
    r.reason = {200: "OK", 429: "Too Many Requests"}.get(status, "")
    r.headers = requests.structures.CaseInsensitiveDict(headers or {})
    return r


def _get_after_a_429(retry_after):
    """One 429 carrying *retry_after* (None: no header), then a 200. Returns
    (what _get returned or raised, the sleeps, the output)."""
    answers = [(429, {}, {} if retry_after is None else {"Retry-After": retry_after}), (200, {"ok": True}, {})]
    sleeps, out = [], io.StringIO()

    def get(url, params=None, timeout=None):
        status, body, headers = answers.pop(0)
        return _response(status, body, url, headers)

    with mock.patch.object(tmdb_mod.requests, "get", get), \
            mock.patch.object(tmdb_mod.time, "sleep", sleeps.append), contextlib.redirect_stdout(out):
        client = tmdb_mod.TmdbClient(api_key="fake-tmdb-key", clock=lambda: NOW)
        client._limiter = types.SimpleNamespace(acquire=lambda: None)
        try:
            result = client._get("/movie/550")
        except Exception as exc:  # the defect raised here; the assertion says which
            result = exc
    return result, sleeps, out.getvalue()


class RetryAfterTest(unittest.TestCase):

    def test_seconds_are_waited_and_the_request_retried(self):
        result, sleeps, _ = _get_after_a_429("7")
        self.assertEqual((result, sleeps), ({"ok": True}, [7.0]))

    def test_an_http_date_is_read_against_the_clock(self):
        result, sleeps, _ = _get_after_a_429(format_datetime(NOW + timedelta(seconds=30), usegmt=True))
        self.assertEqual((result, sleeps), ({"ok": True}, [30.0]))

    def test_a_date_already_past_waits_the_floor(self):
        result, sleeps, _ = _get_after_a_429(format_datetime(NOW - timedelta(minutes=5), usegmt=True))
        self.assertEqual((result, sleeps), ({"ok": True}, [1.0]))

    def test_a_long_wait_is_bounded_to_five_minutes(self):
        result, sleeps, _ = _get_after_a_429("3600")
        self.assertEqual((result, sleeps), ({"ok": True}, [300.0]))

    def test_a_value_not_understood_waits_two_seconds_and_says_so(self):
        result, sleeps, out = _get_after_a_429("soon")
        self.assertEqual((result, sleeps), ({"ok": True}, [2.0]))
        self.assertIn("[TMDB] Retry-After 'soon' not understood — waiting 2.0 s", out)

    def test_control_no_header_waits_two_seconds_as_before(self):
        result, sleeps, out = _get_after_a_429(None)
        self.assertEqual((result, sleeps), ({"ok": True}, [2.0]))
        self.assertNotIn("not understood", out)


if __name__ == "__main__":
    unittest.main()
