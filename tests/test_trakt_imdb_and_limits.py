"""
Unit tests for the Trakt client and the drain made to fit their window
(RUN B5.1 · 0b):

  - a title whose queue row carries a well-formed imdb_id asks
    /movies|shows/{imdb_id}/related directly — one Trakt call, no
    /search/tmdb; a 404 for it falls back to B5's resolve path, once;
  - the client reads Trakt's own X-Ratelimit header after every response
    and, at TRAKT_RATELIMIT_FLOOR or below, pauses the next request until
    the header's `until` (1–300 s, on an injected clock) instead of running
    into a 300 s 429;
  - a time budget (TRAKT_TIME_BUDGET_MIN, default 80, inside the job's 100)
    stops the drain before the next title or slice, green, with its SUMMARY.

Fakes only: requests.get routed in memory (tests/test_trakt_related.py's
router, which can attach response headers), sleeps recorded, every clock
handed in by the test. No network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from bot import sync_trakt
from bot import trakt as trakt_mod
from tests.test_trakt_related import _Router, _client, _hit
from tests.test_trakt_related_queue import COUNTERS, _main
from tests.test_trakt_write_and_decay import _WriteDb

RUN = "2026-10-25T03:00:00+00:00"
START = datetime(2026, 10, 25, 3, 0, tzinfo=timezone.utc)


def _row(imdb_id="<absent>", media_id="m-1", tmdb_id=278, media_type="movie", title="The Shawshank Redemption"):
    row = {"id": media_id, "tmdb_id": tmdb_id, "media_type": media_type, "title": title, "popularity": 50.0}
    if imdb_id != "<absent>":
        row["imdb_id"] = imdb_id
    return row


def _drain(router, rows, batch=100, **kwargs):
    db = _WriteDb(rows)
    with _client(router) as client:
        stats = sync_trakt._run_phase_b(client, db, RUN, batch, **kwargs)
    return stats, db, router.paths(), client.out.getvalue()


# ── Phase B: /related by IMDB id when the title has one ──────────────────

class ImdbPathTest(unittest.TestCase):

    def test_a_title_with_an_imdb_id_asks_related_by_it_directly(self):
        router = _Router({"/movies/tt0111161/related": (200, [{"ids": {"tmdb": 680}}])})

        stats, db, paths, _ = _drain(router, [_row("tt0111161")])

        self.assertEqual(paths, ["/movies/tt0111161/related"])           # one call
        self.assertFalse([p for p in paths if p.startswith("/search/")])  # never the lookup
        self.assertIn(("/movies/tt0111161/related", {"limit": 20}), router.calls)
        self.assertEqual(db.written, [("m-1", [{"related_tmdb_id": 680, "related_media_type": "movie", "rank": 1}], RUN)])
        self.assertEqual((stats.get("related_by_imdb"), stats.get("related_by_tmdb"), stats["resolved"]), (1, 0, 0))
        # Control: the same title WITHOUT an imdb_id still resolves, as in B5.
        router = _Router({"/search/tmdb/278": (200, [_hit("movie", 234, 278)]),
                          "/movies/234/related": (200, [{"ids": {"tmdb": 680}}])})
        stats, db, paths, _ = _drain(router, [_row()])
        self.assertEqual(paths, ["/search/tmdb/278", "/movies/234/related"])
        self.assertEqual((stats.get("related_by_imdb"), stats.get("related_by_tmdb"), stats["resolved"]), (0, 1, 1))
        self.assertEqual(len(db.written), 1)

    def test_a_show_asks_shows_by_its_imdb_id(self):
        router = _Router({"/shows/tt0903747/related": (200, [{"ids": {"tmdb": 60059}}])})

        stats, db, paths, _ = _drain(router, [_row("tt0903747", tmdb_id=1396, media_type="tv", title="Breaking Bad")])

        self.assertEqual(paths, ["/shows/tt0903747/related"])
        self.assertEqual(db.written[0][1], [{"related_tmdb_id": 60059, "related_media_type": "tv", "rank": 1}])

    def test_an_odd_or_missing_imdb_id_takes_the_resolve_path(self):
        for imdb_id in (None, "", "nm0000123", "tt", "tt12a", "0111161", " tt0111161", "TT0111161", 111161):
            with self.subTest(imdb_id=imdb_id):
                router = _Router({"/search/tmdb/278": (200, [_hit("movie", 234, 278)]),
                                  "/movies/234/related": (200, [])})
                stats, _, paths, _ = _drain(router, [_row(imdb_id)])
                self.assertEqual(paths, ["/search/tmdb/278", "/movies/234/related"])
                self.assertEqual((stats.get("related_by_imdb"), stats.get("related_by_tmdb"), stats["errors"]), (0, 1, 0))
        # Control: the well-formed one goes straight to /related.
        router = _Router({"/movies/tt0111161/related": (200, [])})
        _, _, paths, _ = _drain(router, [_row("tt0111161")])
        self.assertEqual(paths, ["/movies/tt0111161/related"])

    def test_a_404_on_the_imdb_id_falls_back_once(self):
        router = _Router({
            "/movies/tt9999999/related": (404, {"error": "not found"}),
            "/search/tmdb/278": (200, [_hit("movie", 234, 278)]),
            "/movies/234/related": (200, [{"ids": {"tmdb": 680}}]),
        })

        stats, db, paths, _ = _drain(router, [_row("tt9999999")])

        self.assertEqual(paths, ["/movies/tt9999999/related", "/search/tmdb/278", "/movies/234/related"])
        self.assertEqual({k: stats.get(k) for k in ("scanned", "related_by_imdb", "related_by_tmdb", "resolved", "errors")},
                         {"scanned": 1, "related_by_imdb": 0, "related_by_tmdb": 1, "resolved": 1, "errors": 0})
        self.assertEqual(db.written, [("m-1", [{"related_tmdb_id": 680, "related_media_type": "movie", "rank": 1}], RUN)])

    def test_another_error_on_the_imdb_id_is_an_error_not_a_fallback(self):
        router = _Router({"/movies/tt0111161/related": (500, {"error": "boom"}),
                          "/search/tmdb/278": (200, [_hit("movie", 234, 278)])})

        stats, db, paths, _ = _drain(router, [_row("tt0111161")])

        self.assertEqual(stats["errors"], 1)
        self.assertNotIn("/search/tmdb/278", paths)
        self.assertEqual(db.written, [])                      # unstamped: the queue serves it again
        # Control: a 404 does fall back to the lookup.
        router = _Router({"/movies/tt0111161/related": (404, {}),
                          "/search/tmdb/278": (200, [_hit("movie", 234, 278)]), "/movies/234/related": (200, [])})
        _, _, paths, _ = _drain(router, [_row("tt0111161")])
        self.assertIn("/search/tmdb/278", paths)

    def test_get_related_takes_a_trakt_id_or_an_imdb_id(self):
        router = _Router({"/movies/tt0111161/related": (200, []), "/movies/234/related": (200, [])})
        with _client(router) as client:
            client.get_related("movie", "tt0111161")
            client.get_related("movie", 234)

        self.assertEqual(router.calls, [("/movies/tt0111161/related", {"limit": 20}),
                                        ("/movies/234/related", {"limit": 20})])

    def test_the_summary_says_how_each_title_was_asked(self):
        _, _, out = _main(START, phase_b_stats={"related_by_imdb": 7, "related_by_tmdb": 5})

        summary = next(line for line in out.splitlines() if "SUMMARY" in line)
        self.assertIn("related_by_imdb=7 related_by_tmdb=5", summary)
        # B5's counters still read as one run of text.
        self.assertIn("related_scanned=5 related_resolved=3 related_unresolved=2 related_rows=40 related_errors=0",
                      summary)


# ── TraktClient: the X-Ratelimit header ──────────────────────────────────

def _limit(remaining, until="2026-10-25T03:05:00Z", limit=1000, period=300, name="UNAUTHED_API_GET_LIMIT"):
    return {"X-Ratelimit": json.dumps({"name": name, "period": period, "limit": limit,
                                       "remaining": remaining, "until": until})}


def _at(minute, second=0):
    return lambda: datetime(2026, 10, 25, 3, minute, second, tzinfo=timezone.utc)


def _without_names_line(out):
    """The log without B5.2's first-response line, which names every header."""
    return "\n".join(line for line in out.splitlines() if not line.startswith("[TRAKT] First response headers:"))


def _three_requests(answers, clock=_at(4)):
    """Three GETs against one path whose responses are *answers* in order."""
    router = _Router({"/movies/trending": list(answers)})
    with _client(router, clock=clock) as client:
        for _ in range(3):
            client.get_trending("movie")
    return client, client.sleeps, client.out.getvalue()


class RateLimitHeaderTest(unittest.TestCase):

    def test_the_floor_is_25(self):
        self.assertEqual(trakt_mod.TRAKT_RATELIMIT_FLOOR, 25)

    def test_remaining_at_the_floor_pauses_the_next_request_until_until(self):
        client, sleeps, out = _three_requests([(200, [], _limit(25)), (200, [], _limit(999))])

        self.assertEqual(sleeps, [60.0])          # once, before the second request: 03:04 → 03:05
        self.assertEqual(out.count("Near the limit"), 1)
        self.assertIn("[TRAKT] Near the limit (remaining=25 of 1000 per 300 s) — pausing 60s until 2026-10-25T03:05:00Z",
                      out)
        self.assertIn("trakt_pauses=1", client.rate_limit_summary())
        # Below the floor pauses too.
        _, sleeps, _ = _three_requests([(200, [], _limit(3)), (200, [], _limit(999))])
        self.assertEqual(sleeps, [60.0])
        # Control: one above the floor never pauses.
        client, sleeps, out = _three_requests([(200, [], _limit(26))])
        self.assertEqual(sleeps, [])
        self.assertNotIn("Near the limit", out)
        self.assertIn("trakt_pauses=0", client.rate_limit_summary())

    def test_the_pause_is_bounded_between_1_and_300_seconds(self):
        _, sleeps, _ = _three_requests([(200, [], _limit(0, until="2026-10-25T04:00:00Z")), (200, [], _limit(999))])
        self.assertEqual(sleeps, [300.0])                     # an hour away: at most 300 s
        _, sleeps, _ = _three_requests([(200, [], _limit(0, until="2026-10-25T03:00:00Z")), (200, [], _limit(999))])
        self.assertEqual(sleeps, [1.0])                       # already past: at least 1 s
        _, sleeps, _ = _three_requests([(200, [], _limit(0, until="2026-10-25T03:04:30.000Z")), (200, [], _limit(999))])
        self.assertEqual(sleeps, [30.0])                      # milliseconds in `until` parse too

    def test_a_missing_or_malformed_header_is_logged_once_and_changes_nothing(self):
        # B5.2: a run whose responses carried no header at all says
        # "header-missing"; "unknown" stays for one that only came malformed.
        for label, answers, limit in (("missing", [(200, [])], "header-missing"),
                                      ("not json", [(200, [], {"X-Ratelimit": "not json"})], "unknown"),
                                      ("no remaining", [(200, [], {"X-Ratelimit": json.dumps({"limit": 1000})})],
                                       "unknown")):
            with self.subTest(header=label):
                client, sleeps, out = _three_requests(answers)
                # Three responses, one line about the header. (Since B5.2 the
                # first response's header NAMES are printed too; that line is
                # set aside, and everything else is counted as before.)
                self.assertIn("[TRAKT] First response headers:", out)
                self.assertEqual(_without_names_line(out).count("X-Ratelimit"), 1)
                self.assertEqual(sleeps, [])
                self.assertIn(f"trakt_limit={limit} ", client.rate_limit_summary())
        # Control: a well-formed header is read, and nothing is logged about it
        # beyond its name in the first response's list.
        client, _, out = _three_requests([(200, [], _limit(999))])
        self.assertIn("'X-Ratelimit'", out)
        self.assertNotIn("X-Ratelimit", _without_names_line(out))
        self.assertIn("trakt_limit=1000/300s", client.rate_limit_summary())

    def test_the_summary_names_the_first_limit_and_counts_pauses_and_429s(self):
        router = _Router({"/movies/trending": [
            (200, [], _limit(999, limit=1000, period=300)),
            (429, {}, {"Retry-After": "2", **_limit(0, limit=2000, period=600)}),
            (200, [], _limit(20)),
            (200, [], _limit(999)),
        ]})
        with _client(router, clock=_at(4)) as client:
            client.get_trending("movie")     # 1000/300 is the first header
            client.get_trending("movie")     # a 429 (Retry-After 2 s), then remaining=20
            client.get_trending("movie")     # paused first

        # B5.1's three tokens still lead the string; B5.2 appends the budget's
        # three (pinned in tests/test_trakt_budget.py).
        self.assertTrue(client.rate_limit_summary().startswith("trakt_limit=1000/300s trakt_pauses=1 trakt_429=1 "))
        self.assertEqual(client.sleeps, [2.0, 60.0])          # the 429's Retry-After, then the pause

    def test_a_429_still_honours_retry_after(self):
        router = _Router({"/movies/trending": [(429, {}, {"Retry-After": "300"}), (200, [])]})
        with _client(router, clock=_at(4)) as client:
            client.get_trending("movie")

        self.assertEqual(client.sleeps, [300.0])
        self.assertIn("trakt_429=1", client.rate_limit_summary())
        self.assertEqual(len(router.calls), 2)


# ── Phase B: the time budget ─────────────────────────────────────────────

class _Clock:
    """Minutes past START = titles written so far: the budget is spent by work, not by calls."""

    def __init__(self, db):
        self.db = db

    def __call__(self):
        return START + timedelta(minutes=len(self.db.written))


class _Quiet:
    def resolve_id(self, media_type, tmdb_id):
        return None

    def get_related(self, media_type, trakt_id, limit=20):
        return []


def _budgeted(db, minutes, clock=None):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        stats = sync_trakt._run_phase_b(_Quiet(), db, RUN, 100, deadline=START + timedelta(minutes=minutes),
                                        clock=clock or _Clock(db), budget_min=minutes)
    return stats, out.getvalue()


def _queue(n):
    return [_row(media_id=f"m-{i}", tmdb_id=100 + i, title=f"T{i}") for i in range(n)]


class _CountingDb(_WriteDb):
    def __init__(self, rows):
        super().__init__(rows)
        self.asks = 0

    def get_titles_for_related_drain(self, limit, before=None):
        self.asks += 1
        return super().get_titles_for_related_drain(limit, before)


class TimeBudgetTest(unittest.TestCase):

    def test_the_budget_stops_the_drain_before_the_next_title(self):
        db = _CountingDb(_queue(10))

        stats, out = _budgeted(db, minutes=5)

        self.assertEqual(len(db.written), 5)                  # the sixth title never started
        self.assertEqual(stats["scanned"], 5)
        self.assertTrue(stats.get("budget_reached"))
        self.assertIn("[TRAKT SYNC] time budget of 5 min reached after 5 title(s) — stopping this run's drain; "
                      "the rest waits for the next run", out)
        self.assertIsNone(stats["stuck_at"])
        # Control: a budget the queue never reaches drains it all, and says nothing.
        db = _CountingDb(_queue(10))
        stats, out = _budgeted(db, minutes=80)
        self.assertEqual(len(db.written), 10)
        self.assertFalse(stats.get("budget_reached"))
        self.assertNotIn("time budget", out)

    def test_a_spent_budget_asks_for_no_slice(self):
        db = _CountingDb(_queue(10))

        stats, out = _budgeted(db, minutes=0)

        self.assertEqual((db.asks, stats["scanned"]), (0, 0))
        self.assertIn("time budget of 0 min reached after 0 title(s)", out)
        # Control: with time left, it asks.
        db = _CountingDb(_queue(10))
        _budgeted(db, minutes=80)
        self.assertEqual(db.asks, 1)

    def test_main_turns_the_budget_into_a_deadline_from_the_runs_start(self):
        for env, minutes in (({"TRAKT_TIME_BUDGET_MIN": None}, 80), ({"TRAKT_TIME_BUDGET_MIN": "30"}, 30)):
            with self.subTest(env=env):
                seen = {}
                _main(START, env=env, seen=seen)
                self.assertEqual(seen["phase_b"].get("deadline"), START + timedelta(minutes=minutes))
                self.assertEqual(seen["phase_b"].get("budget_min"), minutes)
                self.assertTrue(callable(seen["phase_b"].get("clock")))
                self.assertEqual(seen["phase_b"]["clock"](), START)   # the injected clock, not the real one
                self.assertTrue(callable(seen["trakt"].get("clock")))
                self.assertEqual(seen["trakt"]["clock"](), START)     # the client's pause reads it too

    def test_a_budget_stop_is_green_with_its_summary(self):
        code, _, out = _main(START, phase_b_stats={"budget_reached": True})

        self.assertIsNone(code)
        self.assertIn("SUMMARY", out)
        self.assertIn("trakt_limit=1000/300s trakt_pauses=1 trakt_429=0", out)
        # Control: the same run with a failed slice is red.
        code, _, _ = _main(START, phase_b_stats={"budget_reached": True, "queue_failed": "APIError: 57014"})
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
