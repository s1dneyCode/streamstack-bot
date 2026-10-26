"""
Unit tests for backfill_rt_scores.py's phase 1, the imdb_id fill (RUN B5.4 · 0c).

Until B5.4 phase 1 wrote imdb_id with .eq("tmdb_id", …) — the May defect of
backfill_imdb_ids.py in another file: TMDb's movie and tv ids are separate
spaces, so a show with the movie's number got the movie's IMDb id too. It
also asked TMDb through get_imdb_id, which folds a 404 and a timeout into the
same None. Now it asks through lookup_imdb_id and writes like
bot/backfill_imdb_ids.py: by the row's id, the id and the stamp
(imdb_checked_at) for FOUND, the stamp alone for NONE and MISSING, nothing
for ERROR. Phase 2 (the RT scores) is not this run's: it is only checked to
read what phase 1 wrote.

Fakes only: a Supabase client whose `media` table applies the filters and
writes the script sends (so a write by tmdb_id would visibly touch the show),
TMDb and OMDb clients with canned answers, and a clock the test hands in.
No network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import types
import unittest
from datetime import datetime, timezone
from unittest import mock

from bot import backfill_rt_scores as mod
from bot.tmdb import IMDB_ERROR, IMDB_FOUND, IMDB_MISSING, IMDB_NONE, ImdbLookup

START = datetime(2026, 10, 26, 18, 0, tzinfo=timezone.utc)
FAKE_ENV = {"TMDB_API_KEY": "fake-tmdb-key", "OMDB_API_KEY": "fake-omdb-key",
            "SUPABASE_URL": "https://project.supabase.invalid", "SUPABASE_KEY": "fake-supabase-key"}


class _Query:
    """One PostgREST chain on the fake table. Applies what it is told."""

    def __init__(self, table):
        self.t, self.ops = table, []

    def _op(self, name, *args, **kwargs):
        self.ops.append((name, args, kwargs))
        return self

    def select(self, *a, **kw): return self._op("select", *a, **kw)
    def eq(self, *a, **kw): return self._op("eq", *a, **kw)
    def is_(self, *a, **kw): return self._op("is_", *a, **kw)
    def range(self, *a, **kw): return self._op("range", *a, **kw)
    def update(self, *a, **kw): return self._op("update", *a, **kw)

    def named(self, name):
        return [o for o in self.ops if o[0] == name]

    def execute(self):
        self.t.executed.append(self)
        rows = [r for r in self.t.rows if all(r.get(col) == value for _, (col, value), _ in self.named("eq"))]
        update = self.named("update")
        if update:
            for r in rows:
                r.update(update[0][1][0])
            return types.SimpleNamespace(data=rows)
        for _, (col, _null), _ in self.named("is_"):
            rows = [r for r in rows if r.get(col) is None]
        for _, (first, last), _ in self.named("range"):
            rows = rows[first:last + 1]
        cols = [c.strip() for c in self.named("select")[0][1][0].split(",")]
        return types.SimpleNamespace(data=[{c: r.get(c) for c in cols} for r in rows])


class _Db:
    def __init__(self, rows):
        self.rows, self.executed = rows, []
        self.client = self

    def table(self, name):
        assert name == "media", name
        return _Query(self)

    def get_movies_without_imdb_id(self):
        # SupabaseClient's read, which B5.4 leaves as it is: movies with no imdb_id.
        return [{"id": r["id"], "tmdb_id": r["tmdb_id"], "title": r["title"]}
                for r in self.rows if r["media_type"] == "movie" and r["imdb_id"] is None]

    @property
    def imdb_writes(self):
        """Phase 1's writes: (fields, the eq filter)."""
        return [(q.named("update")[0][1][0], q.named("eq")[0][1]) for q in self.executed
                if q.named("update") and "rt_score" not in q.named("update")[0][1][0]]


def _row(i, tmdb_id, media_type="movie", **fields):
    return {"id": f"m-{i:05d}", "tmdb_id": tmdb_id, "media_type": media_type, "title": f"Title {i}",
            "release_date": "2020-05-01", "imdb_id": None, "imdb_checked_at": None, "tmdb_missing_at": None,
            "rt_score": None, **fields}


class _Tmdb:
    """Canned answers by tmdb_id. get_imdb_id is the old door, folded to None
    the way TmdbClient folds it; lookup_imdb_id tells the outcomes apart."""

    def __init__(self, answers):
        self.answers, self.asked = dict(answers), []

    def lookup_imdb_id(self, tmdb_id, media_type):
        self.asked.append(("lookup_imdb_id", tmdb_id, media_type))
        return self.answers.get(tmdb_id, ImdbLookup(IMDB_NONE))

    def get_imdb_id(self, tmdb_id, media_type):
        self.asked.append(("get_imdb_id", tmdb_id, media_type))
        result = self.answers.get(tmdb_id, ImdbLookup(IMDB_NONE))
        return result.imdb_id if result.outcome == IMDB_FOUND else None


class _Omdb:
    def __init__(self, score=None):
        self.score, self.asked = score, []

    def get_rt_score(self, title, year, imdb_id):
        self.asked.append({"title": title, "year": year, "imdb_id": imdb_id})
        return self.score


def _main(db, tmdb, omdb=None):
    out = io.StringIO()
    with mock.patch.dict(os.environ, FAKE_ENV, clear=True), \
            mock.patch.object(mod.sys, "argv", ["backfill_rt_scores"]), \
            mock.patch.object(mod, "SupabaseClient", lambda **kw: db), \
            mock.patch.object(mod, "TmdbClient", lambda **kw: tmdb), \
            mock.patch.object(mod, "OmdbClient", lambda **kw: omdb or _Omdb()), \
            mock.patch.object(mod.time, "sleep", lambda s: None), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        mod.main(clock=lambda: START)
    return out.getvalue()


class Phase1WritesByIdTest(unittest.TestCase):

    def test_found_writes_the_id_and_the_stamp_for_that_row_only(self):
        # Same tmdb_id, other type: TMDb's movie and tv ids are separate spaces.
        movie, show = _row(1, 1416), _row(2, 1416, media_type="tv")
        db = _Db([movie, show])
        _main(db, _Tmdb({1416: ImdbLookup(IMDB_FOUND, "tt0413573")}))

        self.assertEqual(db.imdb_writes, [({"imdb_id": "tt0413573", "imdb_checked_at": START.isoformat()},
                                           ("id", "m-00001"))])
        self.assertEqual((movie["imdb_id"], movie["imdb_checked_at"]), ("tt0413573", START.isoformat()))
        # Positive control: the show with the same tmdb_id is untouched.
        self.assertEqual((show["imdb_id"], show["imdb_checked_at"]), (None, None))

    def test_none_and_missing_stamp_only_by_id(self):
        db = _Db([_row(1, 101), _row(2, 102)])
        _main(db, _Tmdb({101: ImdbLookup(IMDB_NONE), 102: ImdbLookup(IMDB_MISSING)}))

        self.assertEqual(db.imdb_writes, [({"imdb_checked_at": START.isoformat()}, ("id", "m-00001")),
                                          ({"imdb_checked_at": START.isoformat()}, ("id", "m-00002"))])
        self.assertEqual([r["imdb_id"] for r in db.rows], [None, None])
        self.assertEqual([r["tmdb_missing_at"] for r in db.rows], [None, None])  # its own writers set that

    def test_an_error_writes_nothing_and_phase_1_goes_on(self):
        db = _Db([_row(1, 101), _row(2, 102)])
        out = _main(db, _Tmdb({101: ImdbLookup(IMDB_ERROR, reason="HTTPError 500"),
                               102: ImdbLookup(IMDB_FOUND, "tt0000102")}))

        self.assertEqual(db.imdb_writes, [({"imdb_id": "tt0000102", "imdb_checked_at": START.isoformat()},
                                           ("id", "m-00002"))])
        self.assertIsNone(db.rows[0]["imdb_checked_at"])                  # asked again on the next run
        self.assertIn("HTTPError 500", out)

    def test_phase_1_asks_tmdb_through_lookup_imdb_id_as_a_movie(self):
        tmdb = _Tmdb({})
        _main(_Db([_row(1, 1416)]), tmdb)

        self.assertEqual(tmdb.asked, [("lookup_imdb_id", 1416, "movie")])

    def test_phase_2_reads_the_id_phase_1_wrote(self):
        # Control: phase 2 is not this run's, and still reads what phase 1 wrote.
        db = _Db([_row(1, 1416)])
        omdb = _Omdb(score=91)
        out = _main(db, _Tmdb({1416: ImdbLookup(IMDB_FOUND, "tt0413573")}), omdb)

        self.assertEqual(omdb.asked, [{"title": "Title 1", "year": "2020", "imdb_id": "tt0413573"}])
        self.assertEqual(db.rows[0]["rt_score"], 91)
        self.assertIn("[RT_BACKFILL] Done. 1 updated, 0 not found", out)


if __name__ == "__main__":
    unittest.main()
