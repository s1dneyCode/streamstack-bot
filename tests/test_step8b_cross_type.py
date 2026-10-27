"""
Unit tests for the nightly's cross-type collisions (RUN B5.5 · 0a): Steps 7, 8
and 8b of bot/main.py, and the SUMMARY that counts them.

public.media holds ONE row per TMDb number (UNIQUE tmdb_id), whatever the
type, and TMDb numbers movies and shows in separate sequences: a show and a
movie can share a number, and the catalog can hold only one of them. Step 7
dedups the night's candidates by number alone, Step 8 skips a candidate whose
number is stored (type-blind), and until B5.5 Step 8b refreshed the poster of
any stored row whose number matched a candidate's — so a show seen tonight
wrote its poster onto the movie stored under its number. Now Step 8b skips a
candidate of the other type, and the three numbers are printed every night:
  dedup_cross_type      — numbers whose other-type candidate Step 7 dropped;
  skipped_other_type    — candidates Step 8 skipped whose stored row is the
                          other type (a title the catalog cannot hold today);
  cross_type_collisions — of those, the ones whose poster Step 8b would have
                          written onto that row (a fresh poster, another basename).

Fakes only: main.main() runs end to end on a TMDb client with canned lists, a
Supabase client whose `media` table applies the filters the code sends (read
through SupabaseClient's own get_media_poster_state_by_tmdb_ids), and a
recording refresh_poster. No network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import re
import types
import unittest
from unittest import mock

from bot import main as bot_main
from bot.supabase_client import SupabaseClient

FAKE_ENV = {"TMDB_API_KEY": "fake-tmdb-key", "OMDB_API_KEY": "fake-omdb-key",
            "SUPABASE_URL": "https://project.supabase.invalid", "SUPABASE_KEY": "fake-supabase-key"}

# Each TMDb list main() fetches, by the name of the method that fetches it.
LISTS = ("get_now_playing_movies", "get_trending_movies", "get_on_air_tv", "get_trending_tv",
         "get_upcoming_movies", "get_popular_movies", "get_popular_tv", "get_top_rated_movies",
         "get_discover_movies_by_revenue", "get_discover_movies_by_vote_count", "get_top_rated_tv",
         "get_discover_tv_by_vote_count")

SUMMARY_RE = re.compile(
    r"^\[BOT\] SUMMARY discovered=(\d+) new_inserted=(\d+) reverified=(\d+) enriched=(\d+) errors=(\d+) "
    r"dedup_cross_type=(\d+) skipped_other_type=(\d+) cross_type_collisions=(\d+)$",
    re.MULTILINE,
)


class _Query:
    """One PostgREST chain on the fake table. Applies what it is told."""

    def __init__(self, db):
        self.db, self.ops = db, []

    def _op(self, name, *args, **kwargs):
        self.ops.append((name, args, kwargs))
        return self

    def select(self, *a, **kw): return self._op("select", *a, **kw)
    def eq(self, *a, **kw): return self._op("eq", *a, **kw)
    def in_(self, *a, **kw): return self._op("in_", *a, **kw)
    def order(self, *a, **kw): return self._op("order", *a, **kw)
    def limit(self, *a, **kw): return self._op("limit", *a, **kw)

    def named(self, name):
        return [o for o in self.ops if o[0] == name]

    def execute(self):
        self.db.executed.append(self)
        rows = self.db.rows
        for _, (col, value), _ in self.named("eq"):
            rows = [r for r in rows if r.get(col) == value]
        for _, (col, values), _ in self.named("in_"):
            rows = [r for r in rows if r.get(col) in values]
        cols = [c.strip() for c in self.named("select")[0][1][0].split(",")]
        return types.SimpleNamespace(data=[{c: r.get(c) for c in cols} for r in rows])


class _Db:
    def __init__(self, rows):
        self.rows, self.executed = rows, []
        self.client = self

    def table(self, name):
        assert name == "media", name
        return _Query(self)

    def get_existing_tmdb_ids(self):
        return {r["tmdb_id"] for r in self.rows}  # type-blind, as the real one

    def get_media_poster_state_by_tmdb_ids(self, tmdb_ids):
        # SupabaseClient's own read on this fake table: what Step 8b compares
        # is what the real query selects, not a stand-in's.
        return SupabaseClient.get_media_poster_state_by_tmdb_ids(self, tmdb_ids)

    def get_titles_to_reverify(self, today, limit=None):
        return []

    def get_movies_missing_rt_score(self, today):
        return []

    def get_returning_series(self, limit=None):
        return []


class _Tmdb:
    """Canned lists by method name; every other list is empty."""

    def __init__(self, lists):
        self.lists = lists

    def get_genre_map(self, media_type):
        return {}

    def __getattr__(self, name):
        if name in LISTS:
            return lambda **kw: list(self.lists.get(name, []))
        raise AttributeError(name)


class _Omdb:
    quota_exhausted = False


def _stored(i, tmdb_id, media_type, poster):
    return {"id": f"m-{i:05d}", "tmdb_id": tmdb_id, "media_type": media_type, "title": f"Stored {i}",
            "poster_path": f"https://image.tmdb.org/t/p/w500{poster}"}


def _candidate(tmdb_id, media_type, poster):
    return {"tmdb_id": tmdb_id, "media_type": media_type, "title": f"Candidate {media_type} {tmdb_id}",
            "poster_path": f"https://image.tmdb.org/t/p/w500{poster}", "original_language": "en"}


def _main(db, lists):
    refreshed = []

    def refresh_poster(db_, tmdb_id, media_id, fresh):
        refreshed.append((tmdb_id, media_id, fresh))
        return "https://project.supabase.invalid/storage/v1/object/public/media-images/posters/x/y.jpg"

    out = io.StringIO()
    with mock.patch.dict(os.environ, FAKE_ENV, clear=True), \
            mock.patch.object(bot_main.sys, "argv", ["main"]), \
            mock.patch.object(bot_main, "SupabaseClient", lambda **kw: db), \
            mock.patch.object(bot_main, "TmdbClient", lambda **kw: _Tmdb(lists)), \
            mock.patch.object(bot_main, "OmdbClient", lambda **kw: _Omdb()), \
            mock.patch.object(bot_main, "refresh_poster", refresh_poster), \
            mock.patch.object(bot_main, "refresh_materialized_caches", lambda: None), \
            contextlib.redirect_stdout(out):
        bot_main.main()
    return out.getvalue(), refreshed


def _summary(out):
    m = SUMMARY_RE.search(out)
    if m is None:
        return None
    keys = ("discovered", "new_inserted", "reverified", "enriched", "errors",
            "dedup_cross_type", "skipped_other_type", "cross_type_collisions")
    return dict(zip(keys, map(int, m.groups())))


class Step8bNeverCrossesTypesTest(unittest.TestCase):

    def test_a_show_on_a_stored_movies_number_writes_no_poster_and_is_counted(self):
        db = _Db([_stored(1, 1416, "movie", "/movie-poster.jpg")])
        out, refreshed = _main(db, {"get_on_air_tv": [_candidate(1416, "tv", "/show-poster.jpg")]})

        self.assertEqual(refreshed, [])
        self.assertIn("[BOT] Step 8b: refreshed 0 poster(s) of 1 existing titles seen tonight "
                      "(1 cross-type collisions skipped, 0 errors).", out)
        summary = _summary(out)
        self.assertIsNotNone(summary, out)
        self.assertEqual((summary["skipped_other_type"], summary["cross_type_collisions"]), (1, 1))

    def test_control_the_same_candidate_on_a_stored_row_of_its_type_is_refreshed_once(self):
        db = _Db([_stored(1, 1416, "tv", "/old-poster.jpg")])
        out, refreshed = _main(db, {"get_on_air_tv": [_candidate(1416, "tv", "/show-poster.jpg")]})

        self.assertEqual(refreshed, [(1416, "m-00001", "https://image.tmdb.org/t/p/w500/show-poster.jpg")])
        self.assertIn("[BOT] Step 8b: refreshed 1 poster(s) of 1 existing titles seen tonight "
                      "(0 cross-type collisions skipped, 0 errors).", out)
        summary = _summary(out)
        self.assertIsNotNone(summary, out)
        self.assertEqual((summary["dedup_cross_type"], summary["skipped_other_type"],
                          summary["cross_type_collisions"]), (0, 0, 0))

    def test_a_movie_on_a_stored_shows_number_writes_no_poster_either(self):
        db = _Db([_stored(1, 1399, "tv", "/show-poster.jpg")])
        out, refreshed = _main(db, {"get_popular_movies": [_candidate(1399, "movie", "/movie-poster.jpg")]})

        self.assertEqual(refreshed, [])
        summary = _summary(out)
        self.assertIsNotNone(summary, out)
        self.assertEqual(summary["cross_type_collisions"], 1)


class Step8SkippedOtherTypeTest(unittest.TestCase):

    def test_a_skipped_candidate_whose_stored_row_is_the_other_type_is_counted(self):
        # The stored movie already carries this show's poster (an earlier
        # night's crossing): Step 8b would not write, so it is not a
        # collision, but the catalog still cannot hold the show.
        db = _Db([_stored(1, 1416, "movie", "/show-poster.jpg"),
                  _stored(2, 550, "movie", "/fight-club.jpg")])
        out, refreshed = _main(db, {"get_on_air_tv": [_candidate(1416, "tv", "/show-poster.jpg")],
                                    "get_top_rated_movies": [_candidate(550, "movie", "/fight-club.jpg")]})

        self.assertEqual(refreshed, [])
        summary = _summary(out)
        self.assertIsNotNone(summary, out)
        self.assertEqual((summary["skipped_other_type"], summary["cross_type_collisions"]), (1, 0))

    def test_control_skipped_candidates_of_their_own_type_count_zero(self):
        db = _Db([_stored(1, 550, "movie", "/fight-club.jpg"), _stored(2, 1399, "tv", "/got.jpg")])
        out, _ = _main(db, {"get_top_rated_movies": [_candidate(550, "movie", "/fight-club.jpg")],
                            "get_top_rated_tv": [_candidate(1399, "tv", "/got.jpg")]})

        summary = _summary(out)
        self.assertIsNotNone(summary, out)
        self.assertEqual(summary["skipped_other_type"], 0)


class Step7DedupCrossTypeTest(unittest.TestCase):

    def test_a_number_seen_as_a_movie_and_as_a_show_is_counted_once(self):
        # The movie comes first (Step 1), so the show (Steps 2 and 2b) is the
        # one dropped — twice, one number.
        db = _Db([_stored(1, 1416, "movie", "/movie-poster.jpg")])
        out, _ = _main(db, {"get_now_playing_movies": [_candidate(1416, "movie", "/movie-poster.jpg")],
                            "get_on_air_tv": [_candidate(1416, "tv", "/show-poster.jpg")],
                            "get_trending_tv": [_candidate(1416, "tv", "/show-poster.jpg")]})

        summary = _summary(out)
        self.assertIsNotNone(summary, out)
        self.assertEqual((summary["discovered"], summary["dedup_cross_type"]), (1, 1))
        self.assertEqual(summary["skipped_other_type"], 0)              # the kept movie is the stored type

    def test_control_the_same_show_in_two_lists_counts_zero(self):
        db = _Db([_stored(1, 1399, "tv", "/got.jpg")])
        out, _ = _main(db, {"get_on_air_tv": [_candidate(1399, "tv", "/got.jpg")],
                            "get_trending_tv": [_candidate(1399, "tv", "/got.jpg")]})

        summary = _summary(out)
        self.assertIsNotNone(summary, out)
        self.assertEqual((summary["discovered"], summary["dedup_cross_type"]), (1, 0))


class PosterStateReadTest(unittest.TestCase):

    def test_the_poster_state_read_selects_the_type_of_each_stored_row(self):
        db = _Db([_stored(1, 1416, "movie", "/movie-poster.jpg"), _stored(2, 1399, "tv", "/got.jpg")])
        state = SupabaseClient.get_media_poster_state_by_tmdb_ids(db, [1416, 1399, 7])

        self.assertEqual(state, {
            1416: {"id": "m-00001", "poster_path": "https://image.tmdb.org/t/p/w500/movie-poster.jpg",
                   "media_type": "movie"},
            1399: {"id": "m-00002", "poster_path": "https://image.tmdb.org/t/p/w500/got.jpg", "media_type": "tv"},
        })
        self.assertEqual(len(db.executed), 1)                           # one read, as before: a column more


class SummaryShapeTest(unittest.TestCase):

    def test_the_summary_keeps_its_five_keys_first_and_adds_the_three_counters(self):
        out, _ = _main(_Db([]), {})

        self.assertEqual(_summary(out), {"discovered": 0, "new_inserted": 0, "reverified": 0, "enriched": 0,
                                         "errors": 0, "dedup_cross_type": 0, "skipped_other_type": 0,
                                         "cross_type_collisions": 0})


if __name__ == "__main__":
    unittest.main()
