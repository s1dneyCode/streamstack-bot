"""
Unit tests for the 401 fail-fast (RUN B5.5 · 0c(4)): a 401 from TMDb or from
Trakt ends the run — one line naming the refused credential, exit 1 — instead
of failing that one title and going on. A bad key fails every request alike,
so until B5.5 the IMDb backfill spent its whole 80-minute budget logging the
same error, title after title.

Since B5.6 a 403 from Trakt ends the run too: on the public endpoints this
bot calls, Trakt answers an invalid client id or an unapproved app with 403
(its 401 is «OAuth must be provided»), so the 401 exit never fired on a bad
key. A bad key at the start already stopped Trakt Sync, at Phase A's first
list fetch, with a traceback; a 403 that came in Phase B, once Phase A had
passed, was counted title by title until the budget ran out. TMDb's 403 is
not one: TMDb refuses a bad key with 401.

Every other 4xx still raises as before (controls: TMDb's 403 and 404,
Trakt's 400 and 404).

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

from bot import sync_trakt
from bot import tmdb as tmdb_mod
from bot import trakt as trakt_mod
from tests.test_trakt_related import RUN, _FakeDb, _Router, _client, _hit, _row as _queue_row

BASE = "https://api.trakt.invalid"


def _response(status, body, url):
    r = requests.Response()
    r.status_code, r._content, r.url = status, json.dumps(body).encode("utf-8"), url
    r.reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
                404: "Not Found"}.get(status, "")
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


class _RecordingDb:
    """Phase A's db: records every call; none must come before the exit."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append(name)
            return {}
        return call


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

    def test_control_a_400_and_a_404_still_raise_as_before(self):
        # A 403 until B5.6, which now ends the run (TraktForbiddenFailFastTest).
        for status in (400, 404):
            with self.subTest(status=status):
                http = _Http((status, {"error": "no"}))
                with _trakt(http) as client:
                    raised = _outcome(lambda: client.get_related("movie", "tt0137523", limit=20))

                self.assertIsInstance(raised, requests.HTTPError)
                self.assertEqual(len(http.calls), 1)


class TraktForbiddenFailFastTest(unittest.TestCase):
    """B5.6: on the public endpoints this bot calls, Trakt answers a refused
    client id — or an unapproved app — with 403, so a 403 from Trakt ends
    the run too, with its own line."""

    def test_a_403_ends_the_run_with_one_line_and_exit_1(self):
        http = _Http((403, {"error": "forbidden"}), (200, []))
        with _trakt(http) as client:
            raised = _outcome(lambda: [client.get_related("movie", "tt0137523", limit=20),
                                       client.get_related("movie", "tt0068646", limit=20)])

        self.assertIsInstance(raised, SystemExit)
        self.assertEqual(raised.code, 1)
        self.assertEqual(len(http.calls), 1)
        self.assertIn(f"[TRAKT] ERROR: 403 Forbidden for {BASE}/movies/tt0137523/related — "
                      "Trakt refused TRAKT_CLIENT_ID or the app; stopping the run (exit 1).", client.out.getvalue())
        self.assertNotIn("limit=20", client.out.getvalue())   # the URL without its query
        self.assertNotIn("fake-trakt-client-id", client.out.getvalue())
        self.assertNotIn("not retrying (client error)", client.out.getvalue())   # the one line, not a 4xx's too

    def test_a_403_in_phase_a_ends_the_run_before_any_write(self):
        router = _Router({"/movies/watched/weekly": (403, {"error": "forbidden"})})
        db = _RecordingDb()
        with _client(router) as client:
            raised = _outcome(lambda: sync_trakt._run_phase_a(client, db, RUN, 300))

        self.assertIsInstance(raised, SystemExit)
        self.assertEqual(raised.code, 1)
        self.assertEqual(router.paths(), ["/movies/watched/weekly"])
        self.assertEqual(db.calls, [])
        self.assertIn(f"[TRAKT] ERROR: 403 Forbidden for {BASE}/movies/watched/weekly — ", client.out.getvalue())

    def test_a_403_in_phase_b_gets_past_the_per_title_catch(self):
        # _drain_one catches every Exception per title; SystemExit is not one,
        # so the second title is never asked and nothing is written.
        for path, first in (("/movies/tt0000001/related", dict(_queue_row("m-1", 1), imdb_id="tt0000001")),
                            ("/search/tmdb/1", _queue_row("m-1", 1))):
            with self.subTest(path=path):
                router = _Router({path: (403, {"error": "forbidden"})})
                db = _FakeDb([first, _queue_row("m-2", 2)])
                with _client(router) as client:
                    raised = _outcome(lambda: sync_trakt._run_phase_b(client, db, RUN, 2))

                self.assertIsInstance(raised, SystemExit)
                self.assertEqual(raised.code, 1)
                self.assertEqual(router.paths(), [path])
                self.assertEqual(db.written, [])
                self.assertIn(f"[TRAKT] ERROR: 403 Forbidden for {BASE}{path} — ", client.out.getvalue())

    def test_control_a_404_or_a_400_fails_its_title_and_the_drain_goes_on(self):
        for status in (404, 400):
            with self.subTest(status=status):
                router = _Router({
                    "/search/tmdb/1": (200, [_hit("movie", 11, 1)]),
                    "/movies/11/related": (status, {"error": "no"}),
                    "/search/tmdb/2": (200, [_hit("movie", 22, 2)]),
                    "/movies/22/related": (200, [{"title": "R", "ids": {"trakt": 5, "tmdb": 500}}]),
                })
                db = _FakeDb([_queue_row("m-1", 1), _queue_row("m-2", 2)])
                with _client(router) as client:
                    stats = _outcome(lambda: sync_trakt._run_phase_b(client, db, RUN, 2))

                self.assertIsInstance(stats, dict)
                self.assertEqual(stats["errors"], 1)
                self.assertEqual([m for m, _ in db.stamped], ["m-2"])
                self.assertIn(f"tmdb_id=1: related drain failed — {status} Client Error", client.out.getvalue())
                self.assertNotIn("[TRAKT] ERROR:", client.out.getvalue())


if __name__ == "__main__":
    unittest.main()
