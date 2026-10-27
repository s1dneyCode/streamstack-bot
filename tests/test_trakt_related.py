"""
Unit tests for Trakt's related titles fetched by the right id (RUN B5 · 0a):
TraktClient.resolve_id() turns our tmdb_id into Trakt's own id through
/search/tmdb/{id}?type=movie|show, and Phase B of sync_trakt asks /related with
that Trakt id — never with our tmdb_id, which Trakt reads as ITS id and answers
with another title's relateds (Grey's Anatomy → The Diplomat…).

The real TraktClient runs, with requests.get swapped for a router that records
every URL and answers real requests.Response objects, so _request()'s retry
policy (a non-429 4xx raised at once, a 5xx retried) is exercised as it is. The
rate limiter is a no-op and the retry sleep is patched; the host is .invalid.
No network, no database. Run from the repo root:
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
from bot import trakt as trakt_mod

BASE = "https://api.trakt.invalid"
RUN = "2026-10-25T03:00:00+00:00"


def _response(status: int, body, url: str, headers: dict | None = None) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode("utf-8")
    r.url = url
    r.headers["Content-Type"] = "application/json"
    r.headers.update(headers or {})
    return r


def _hit(kind: str, trakt_id: int, tmdb_id: int, title: str = "x") -> dict:
    """One /search/tmdb result."""
    return {"type": kind, "score": 1000, kind: {"title": title, "ids": {"trakt": trakt_id, "tmdb": tmdb_id}}}


class _Router:
    """Stands in for requests.get: answers by path, records (path, params)."""

    def __init__(self, routes: dict):
        # path -> (status, body) or (status, body, response headers), or a
        # list of them (one per call; the last one repeats)
        self.routes = routes
        self.calls = []

    def __call__(self, url, headers=None, params=None, timeout=None):
        path = url[len(BASE):]
        self.calls.append((path, dict(params or {})))
        answer = self.routes.get(path, (404, {"error": "not found"}))
        if isinstance(answer, list):
            answer = answer.pop(0) if len(answer) > 1 else answer[0]
        status, body, *rest = answer
        return _response(status, body, url, rest[0] if rest else None)

    def paths(self):
        return [p for p, _ in self.calls]


class _NoLimiter:
    """The token bucket, inert: building it reads no clock, acquiring never waits."""

    def __init__(self, *args, **kwargs):
        pass

    def acquire(self):
        pass


@contextlib.contextmanager
def _client(router: _Router, clock=None):
    """A real TraktClient whose HTTP, limiter, sleeps and clock are fakes;
    client.sleeps records every sleep it asked for."""
    out, sleeps = io.StringIO(), []
    with mock.patch.object(trakt_mod, "TRAKT_BASE", BASE), \
            mock.patch.object(trakt_mod, "RateLimiter", _NoLimiter), \
            mock.patch.object(trakt_mod.requests, "get", router), \
            mock.patch.object(trakt_mod.time, "sleep", sleeps.append), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        # monotonic: a fixed fake (B5.2), so no test reads the real clock.
        client = trakt_mod.TraktClient(client_id="fake-trakt-client-id", clock=clock, monotonic=lambda: 0.0)
        client._limiter = types.SimpleNamespace(acquire=lambda: None)
        client.out = out
        client.sleeps = sleeps
        yield client


class _FakeDb:
    """sync_trakt's db: the queue, and a record of every write_related — the
    one RPC that replaces a title's rows AND stamps it (B5.1). `replaced` and
    `stamped` are two views of those same calls, as B5's pins read them when
    they were two round trips."""

    def __init__(self, rows):
        self.rows = rows
        self.written = []  # (media_id, rows, synced_at)

    @property
    def replaced(self):
        return list(self.written)

    @property
    def stamped(self):
        return [(m, s) for m, _, s in self.written]

    def get_titles_for_related_drain(self, limit, *args, **kwargs):
        done = {m for m, _ in self.stamped}
        return [r for r in self.rows if r["id"] not in done][:limit]

    def write_related(self, media_id, rows, synced_at):
        self.written.append((media_id, rows, synced_at))
        return len(rows)


def _row(media_id="m-1", tmdb_id=693134, media_type="movie", title="Dune: Part Two"):
    return {"id": media_id, "tmdb_id": tmdb_id, "media_type": media_type, "title": title, "popularity": 50.0}


def _phase_b(client, db, batch=100):
    return sync_trakt._run_phase_b(client, db, RUN, batch)


# ── TraktClient.resolve_id ────────────────────────────────────────────────

class ResolveIdTest(unittest.TestCase):

    def test_a_tmdb_id_naming_a_movie_and_a_show_resolves_by_type(self):
        # 1416 is a movie AND Grey's Anatomy: the type is the filter, not a tiebreak,
        # whichever comes first in Trakt's answer.
        for order in ("movie first", "show first"):
            hits = [_hit("movie", 111, 1416), _hit("show", 1407, 1416, "Grey's Anatomy")]
            if order == "show first":
                hits.reverse()
            with self.subTest(order=order):
                router = _Router({"/search/tmdb/1416": (200, hits)})
                with _client(router) as client:
                    movie = client.resolve_id("movie", 1416)
                    show = client.resolve_id("tv", 1416)

                self.assertEqual(movie, 111)
                self.assertEqual(show, 1407)
                self.assertEqual(router.calls, [("/search/tmdb/1416", {"type": "movie"}),
                                                ("/search/tmdb/1416", {"type": "show"})])

    def test_a_different_tmdb_id_does_not_resolve(self):
        router = _Router({"/search/tmdb/1416": (200, [_hit("show", 9999, 5555)])})
        with _client(router) as client:
            self.assertIsNone(client.resolve_id("tv", 1416))
        # Control: the matching one resolves.
        router = _Router({"/search/tmdb/1416": (200, [_hit("show", 1407, 1416)])})
        with _client(router) as client:
            self.assertEqual(client.resolve_id("tv", 1416), 1407)

    def test_an_empty_answer_does_not_resolve(self):
        router = _Router({"/search/tmdb/1416": (200, [])})
        with _client(router) as client:
            self.assertIsNone(client.resolve_id("movie", 1416))
        self.assertEqual(len(router.calls), 1)  # it did ask
        # Control: the same lookup with a hit resolves.
        router = _Router({"/search/tmdb/1416": (200, [_hit("movie", 111, 1416)])})
        with _client(router) as client:
            self.assertEqual(client.resolve_id("movie", 1416), 111)

    def test_a_404_does_not_resolve_and_is_not_retried(self):
        router = _Router({"/search/tmdb/1416": (404, {"error": "not found"})})
        with _client(router) as client:
            self.assertIsNone(client.resolve_id("movie", 1416))
        self.assertEqual(len(router.calls), 1)

    def test_only_a_404_is_unresolved_any_other_4xx_propagates(self):
        # A bad key or a refused request must never read as "no such title":
        # Phase B would write zero rows and stamp the whole batch. (A 401
        # ends the run since B5.5 — test_fail_fast_401 — so it is not here.)
        for status in (400, 403):
            with self.subTest(status=status):
                router = _Router({"/search/tmdb/1416": (status, {"error": "no"})})
                with _client(router) as client, self.assertRaises(requests.HTTPError):
                    client.resolve_id("movie", 1416)
                self.assertEqual(len(router.calls), 1)  # a 4xx is not retried
        # Control: the 404 itself is unresolved.
        router = _Router({"/search/tmdb/1416": (404, {})})
        with _client(router) as client:
            self.assertIsNone(client.resolve_id("movie", 1416))

    def test_an_exhausted_429_propagates(self):
        router = _Router({"/search/tmdb/1416": (429, {})})
        with _client(router) as client, self.assertRaises(requests.HTTPError):
            client.resolve_id("movie", 1416)
        self.assertEqual(len(router.calls), 3)

    def test_the_first_match_wins(self):
        router = _Router({"/search/tmdb/1416": (200, [_hit("movie", 111, 1416), _hit("movie", 222, 1416)])})
        with _client(router) as client:
            self.assertEqual(client.resolve_id("movie", 1416), 111)

    def test_a_500_propagates_after_the_retries(self):
        router = _Router({"/search/tmdb/1416": (500, {"error": "boom"})})
        with _client(router) as client, self.assertRaises(requests.HTTPError):
            client.resolve_id("movie", 1416)
        self.assertEqual(len(router.calls), 3)  # _request's retry policy, unchanged
        # Control: a healthy answer after one 500 resolves.
        router = _Router({"/search/tmdb/1416": [(500, {}), (200, [_hit("movie", 111, 1416)])]})
        with _client(router) as client:
            self.assertEqual(client.resolve_id("movie", 1416), 111)


class GetRelatedTest(unittest.TestCase):

    def test_get_related_builds_its_path_from_the_trakt_id(self):
        router = _Router({"/movies/287071/related": (200, [])})
        with _client(router) as client:
            client.get_related("movie", 287071, limit=20)

        self.assertEqual(router.calls, [("/movies/287071/related", {"limit": 20})])


# ── sync_trakt Phase B ────────────────────────────────────────────────────

class PhaseBTest(unittest.TestCase):

    def test_the_related_call_carries_the_trakt_id_never_the_tmdb_id(self):
        router = _Router({
            "/search/tmdb/693134": (200, [_hit("movie", 287071, 693134, "Dune: Part Two")]),
            "/movies/287071/related": (200, [{"title": "Dune", "ids": {"trakt": 1, "tmdb": 438631}}]),
        })
        db = _FakeDb([_row()])
        with _client(router) as client:
            _phase_b(client, db)

        related = [p for p in router.paths() if p.endswith("/related")]
        self.assertEqual(related, ["/movies/287071/related"])            # the Trakt id is there
        self.assertFalse([p for p in related if "693134" in p])          # and the tmdb id never is
        self.assertIn("/search/tmdb/693134", router.paths())             # it is only searched by
        self.assertIn(("/movies/287071/related", {"limit": 20}), router.calls)

    def test_a_refused_lookup_writes_nothing_and_stamps_nothing(self):
        # Any 4xx but 404, or an exhausted 429, is an error — never "unresolved".
        for status in (403, 429):
            with self.subTest(status=status):
                router = _Router({"/search/tmdb/693134": (status, {})})
                db = _FakeDb([_row()])
                with _client(router) as client:
                    stats = _phase_b(client, db)

                self.assertEqual((db.replaced, db.stamped), ([], []))
                self.assertEqual((stats["errors"], stats["unresolved"]), (1, 0))

    def test_an_unresolved_title_whose_write_fails_is_not_stamped(self):
        # Zero rows are written BEFORE the stamp: a failed write must leave
        # the title in the queue, never stamped over a stale row.
        class _FailingReplace(_FakeDb):
            def write_related(self, media_id, rows, synced_at):
                raise RuntimeError("delete failed")  # the one transaction rolls back: no rows, no stamp

        router = _Router({"/search/tmdb/693134": (200, [])})
        db = _FailingReplace([_row()])
        with _client(router) as client:
            stats = _phase_b(client, db)

        self.assertEqual(db.stamped, [])
        self.assertEqual(stats["errors"], 1)

    def test_an_unresolved_title_is_stamped(self):
        router = _Router({"/search/tmdb/693134": (200, [])})
        db = _FakeDb([_row()])
        with _client(router) as client:
            stats = _phase_b(client, db)

        self.assertEqual(db.stamped, [("m-1", RUN)])
        self.assertEqual(stats["unresolved"], 1)
        self.assertIn("[TRAKT] unresolved tmdb=693134 type=movie «Dune: Part Two»", client.out.getvalue())
        # Control: no /related was asked for it.
        self.assertFalse([p for p in router.paths() if p.endswith("/related")])

    def test_an_unresolved_title_is_written_with_zero_rows(self):
        # So a stale row from the old, wrong path can never survive.
        router = _Router({"/search/tmdb/1416": (200, [_hit("movie", 111, 1416)])})
        db = _FakeDb([_row(media_id="m-2", tmdb_id=1416, media_type="tv", title="Grey's Anatomy")])
        with _client(router) as client:
            _phase_b(client, db)

        self.assertEqual(db.replaced, [("m-2", [], RUN)])
        self.assertIn("type=show", client.out.getvalue())

    def test_a_resolved_titles_rows_are_built_as_before(self):
        router = _Router({
            "/search/tmdb/1416": (200, [_hit("show", 1407, 1416, "Grey's Anatomy")]),
            "/shows/1407/related": (200, [
                {"title": "Private Practice", "ids": {"trakt": 1, "tmdb": 2734}},
                {"title": "No tmdb id", "ids": {"trakt": 2}},
                {"title": "Station 19", "ids": {"trakt": 3, "tmdb": 78191}},
            ]),
        })
        db = _FakeDb([_row(media_id="m-3", tmdb_id=1416, media_type="tv", title="Grey's Anatomy")])
        with _client(router) as client:
            stats = _phase_b(client, db)

        self.assertEqual(db.replaced, [("m-3", [
            {"related_tmdb_id": 2734, "related_media_type": "tv", "rank": 1},
            {"related_tmdb_id": 78191, "related_media_type": "tv", "rank": 2},
        ], RUN)])
        self.assertEqual(db.stamped, [("m-3", RUN)])
        self.assertEqual((stats["resolved"], stats["related_rows"]), (1, 2))

    def test_a_failure_is_counted_and_not_stamped_as_before(self):
        router = _Router({"/search/tmdb/693134": (500, {"error": "boom"})})
        db = _FakeDb([_row()])
        with _client(router) as client:
            stats = _phase_b(client, db)

        self.assertEqual(stats["errors"], 1)
        self.assertEqual(db.stamped, [])
        # Control: the same title, healthy and resolved, is stamped.
        router = _Router({"/search/tmdb/693134": (200, [_hit("movie", 287071, 693134)]),
                          "/movies/287071/related": (200, [])})
        db = _FakeDb([_row()])
        with _client(router) as client:
            _phase_b(client, db)
        self.assertEqual(db.stamped, [("m-1", RUN)])

    def test_the_phase_counts_scanned_resolved_unresolved_rows_errors(self):
        router = _Router({
            "/search/tmdb/1": (200, [_hit("movie", 10, 1)]),
            "/movies/10/related": (200, [{"ids": {"tmdb": 7}}, {"ids": {"tmdb": 8}}]),
            "/search/tmdb/2": (200, []),
            "/search/tmdb/3": (500, {}),
        })
        db = _FakeDb([_row("a", 1), _row("b", 2), _row("c", 3)])
        with _client(router) as client:
            stats = _phase_b(client, db)

        self.assertEqual({k: stats.get(k) for k in ("scanned", "resolved", "unresolved", "related_rows", "errors")},
                         {"scanned": 3, "resolved": 1, "unresolved": 1, "related_rows": 2, "errors": 1})


if __name__ == "__main__":
    unittest.main()
