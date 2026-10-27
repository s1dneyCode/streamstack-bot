"""
Unit tests for the 401 fail-fast (RUN B5.5 · 0c(4)): a 401 from TMDb or from
Trakt ends the run — one line naming the refused credential, exit 1 — instead
of failing that one title and going on. A bad key fails every request alike,
so until B5.5 a run spent its whole budget (80 minutes for Trakt Sync and the
IMDb backfill) logging the same error, title after title.

Every other 4xx still raises as before (controls: 403, 404).

Fakes only: real TmdbClient / TraktClient whose requests.get answers in
memory, with inert limiters and sleeps; dummy keys. No network. Run from the
repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import json
import types
import unittest
from unittest import mock

import requests

from bot import tmdb as tmdb_mod
from bot import trakt as trakt_mod

BASE = "https://api.trakt.invalid"


def _response(status, body, url):
    r = requests.Response()
    r.status_code, r._content, r.url = status, json.dumps(body).encode("utf-8"), url
    r.reason = {200: "OK", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found"}.get(status, "")
    r.headers["Content-Type"] = "application/json"
    return r


class _Http:
    """Stands in for requests.get: one answer per call (the last repeats)."""

    def __init__(self, *answers):
        self.answers, self.calls = list(answers), []

    def __call__(self, url, headers=None, params=None, timeout=None):
        self.calls.append(url)
        status, body = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        full = requests.Request("GET", url, params=params).prepare().url
        return _response(status, body, full)


def _outcome(call):
    """What *call* returned or raised — BaseException too, so a run that
    does not stop is an assertion's failure, not the suite's error."""
    try:
        return call()
    except BaseException as exc:  # SystemExit included, on purpose
        return exc


@contextlib.contextmanager
def _tmdb(http):
    out = io.StringIO()
    with mock.patch.object(tmdb_mod.requests, "get", http), mock.patch.object(tmdb_mod.time, "sleep", lambda s: None), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        client = tmdb_mod.TmdbClient(api_key="fake-tmdb-key")
        client._limiter = types.SimpleNamespace(acquire=lambda: None)
        client.out = out
        yield client


@contextlib.contextmanager
def _trakt(http):
    out = io.StringIO()
    with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), \
            mock.patch.object(trakt_mod.requests, "get", http), \
            mock.patch.object(trakt_mod.time, "sleep", lambda s: None), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        client = trakt_mod.TraktClient(client_id="fake-trakt-client-id", monotonic=lambda: 0.0,
                                       sleeper=lambda s: None)
        client._limiter = types.SimpleNamespace(acquire=lambda: None)
        client.out = out
        yield client


class TmdbFailFastTest(unittest.TestCase):

    def test_a_401_ends_the_run_with_one_line_and_exit_1(self):
        http = _Http((401, {"status_code": 7}), (200, {"ok": True}))
        with _tmdb(http) as client:
            raised = _outcome(lambda: client._get("/movie/550"))

        self.assertIsInstance(raised, SystemExit)
        self.assertEqual(raised.code, 1)
        self.assertEqual(len(http.calls), 1)
        self.assertIn("[TMDB] ERROR: 401 Unauthorized for https://api.themoviedb.org/3/movie/550 — "
                      "TMDb refused TMDB_API_KEY; stopping the run (exit 1).", client.out.getvalue())
        self.assertNotIn("fake-tmdb-key", client.out.getvalue())

    def test_one_401_in_the_imdb_backfill_asks_no_second_title(self):
        # Through a caller that catches every Exception per title
        # (lookup_imdb_id): the exit must not be folded into an ERROR.
        http = _Http((401, {"status_code": 7}), (200, {"imdb_id": "tt0000002"}))
        with _tmdb(http) as client:
            raised = _outcome(lambda: [client.lookup_imdb_id(1, "movie"), client.lookup_imdb_id(2, "movie")])

        self.assertIsInstance(raised, SystemExit)
        self.assertEqual(len(http.calls), 1)

    def test_control_a_quiet_404_and_a_403_still_raise_as_before(self):
        for status in (403, 404):
            with self.subTest(status=status):
                http = _Http((status, {}))
                with _tmdb(http) as client:
                    raised = _outcome(lambda: client._get("/movie/550", quiet_statuses=frozenset({404})))
                self.assertIsInstance(raised, requests.HTTPError)
                self.assertEqual(len(http.calls), 1)


class TraktFailFastTest(unittest.TestCase):

    def test_a_401_ends_the_run_with_one_line_and_exit_1(self):
        http = _Http((401, {"error": "unauthorized"}), (200, []))
        with _trakt(http) as client:
            raised = _outcome(lambda: [client.get_related("movie", "tt0137523", limit=20),
                                       client.get_related("movie", "tt0068646", limit=20)])

        self.assertIsInstance(raised, SystemExit)
        self.assertEqual(raised.code, 1)
        self.assertEqual(len(http.calls), 1)
        self.assertIn(f"[TRAKT] ERROR: 401 Unauthorized for {BASE}/movies/tt0137523/related — "
                      "Trakt refused TRAKT_CLIENT_ID; stopping the run (exit 1).", client.out.getvalue())
        self.assertNotIn("fake-trakt-client-id", client.out.getvalue())

    def test_a_401_on_resolve_id_is_not_read_as_an_unresolved_title(self):
        http = _Http((401, {"error": "unauthorized"}))
        with _trakt(http) as client:
            raised = _outcome(lambda: client.resolve_id("movie", 550))

        self.assertIsInstance(raised, SystemExit)
        self.assertEqual(len(http.calls), 1)

    def test_control_a_403_still_raises_as_before(self):
        http = _Http((403, {"error": "forbidden"}))
        with _trakt(http) as client:
            raised = _outcome(lambda: client.get_related("movie", "tt0137523", limit=20))

        self.assertIsInstance(raised, requests.HTTPError)
        self.assertEqual(len(http.calls), 1)


if __name__ == "__main__":
    unittest.main()
