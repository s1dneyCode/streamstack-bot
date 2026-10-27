"""
Unit tests for daily_verify.py's 404s (RUN B5.5 · 0c(7)): a 404 from TMDb in
check 2 (/movie/{id}) or check 3 (/{media_type}/{id}) stamps the row with
SupabaseClient.set_tmdb_missing, and both checks leave stamped rows out of
their targets — so a title gone from TMDb is asked once, not every day.
Measured on 26/10: ~45 titles answered 404 every day and were asked again
the next. Check 1 asks TMDb nothing per title (it reads the now-playing list),
so it is not touched.

Fakes only: a Supabase client whose `media` table applies the filters the
checks send (the stamp goes through SupabaseClient's own set_tmdb_missing),
and a TMDb client with canned answers by path. No network, no database. Run
from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import types
import unittest
from datetime import date, timedelta
from unittest import mock

import requests

from bot import daily_verify as mod
from bot.supabase_client import SupabaseClient

SOON = (date.today() + timedelta(days=10)).isoformat()
PAST = (date.today() - timedelta(days=10)).isoformat()


class _Query:
    """One PostgREST chain on the fake table. Applies what it is told."""

    def __init__(self, db):
        self.db, self.ops = db, []

    def _op(self, name, *args, **kwargs):
        self.ops.append((name, args, kwargs))
        return self

    def select(self, *a, **kw): return self._op("select", *a, **kw)
    def eq(self, *a, **kw): return self._op("eq", *a, **kw)
    def gte(self, *a, **kw): return self._op("gte", *a, **kw)
    def lte(self, *a, **kw): return self._op("lte", *a, **kw)
    def in_(self, *a, **kw): return self._op("in_", *a, **kw)
    def is_(self, *a, **kw): return self._op("is_", *a, **kw)
    def range(self, *a, **kw): return self._op("range", *a, **kw)
    def update(self, *a, **kw): return self._op("update", *a, **kw)

    def named(self, name):
        return [o for o in self.ops if o[0] == name]

    def execute(self):
        rows = self.db.rows
        for _, (col, value), _ in self.named("eq"):
            rows = [r for r in rows if r.get(col) == value]
        for _, (col, value), _ in self.named("gte"):
            rows = [r for r in rows if r.get(col) is not None and r[col] >= value]
        for _, (col, value), _ in self.named("lte"):
            rows = [r for r in rows if r.get(col) is not None and r[col] <= value]
        for _, (col, values), _ in self.named("in_"):
            rows = [r for r in rows if r.get(col) in values]
        for _, (col, _null), _ in self.named("is_"):
            rows = [r for r in rows if r.get(col) is None]
        update = self.named("update")
        if update:
            for r in rows:
                r.update(update[0][1][0])
            self.db.updates.append((update[0][1][0], [r["id"] for r in rows]))
            return types.SimpleNamespace(data=rows)
        for _, (first, last), _ in self.named("range"):
            rows = rows[first:last + 1]
        cols = [c.strip() for c in self.named("select")[0][1][0].split(",")]
        return types.SimpleNamespace(data=[{c: r.get(c) for c in cols} for r in rows])


class _Db:
    def __init__(self, rows):
        self.rows, self.updates = rows, []
        self.client = self

    def table(self, name):
        assert name == "media", name
        return _Query(self)

    def set_tmdb_missing(self, media_id):
        return SupabaseClient.set_tmdb_missing(self, media_id)  # the real stamp, on this fake table

    @property
    def stamps(self):
        return [ids for fields, ids in self.updates if "tmdb_missing_at" in fields]


def _http_error(status):
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"{status} Client Error", response=response)


class _Tmdb:
    """Canned answers by path: a dict (200) or a status code (raised)."""

    def __init__(self, answers):
        self.answers, self.asked = answers, []

    def _get(self, path, params=None, **kw):
        self.asked.append(path)
        answer = self.answers[path]
        if isinstance(answer, int):
            raise _http_error(answer)
        return answer


def _row(i, tmdb_id, media_type="movie", **fields):
    return {"id": f"m-{i:05d}", "tmdb_id": tmdb_id, "media_type": media_type, "title": f"Title {i}",
            "tmdb_missing_at": None, **fields}


def _run(check, db, tmdb):
    out = io.StringIO()
    with mock.patch.object(mod.time, "sleep", lambda s: None), contextlib.redirect_stdout(out):
        check(db, tmdb)
    return out.getvalue()


class Check2UpcomingDatesTest(unittest.TestCase):

    def test_a_404_is_stamped_once_and_not_asked_the_next_day(self):
        gone, alive = _row(1, 101, release_date=SOON, status="Post Production"), \
            _row(2, 102, release_date=SOON, status="Post Production")
        db = _Db([gone, alive])
        tmdb = _Tmdb({"/movie/101": 404, "/movie/102": {"release_date": SOON, "status": "Post Production"}})

        _run(mod.verify_upcoming_dates, db, tmdb)
        _run(mod.verify_upcoming_dates, db, tmdb)                         # the next day

        self.assertEqual(db.stamps, [["m-00001"]])                        # one stamp, on that row
        self.assertIsNotNone(gone["tmdb_missing_at"])
        self.assertEqual(tmdb.asked.count("/movie/101"), 1)               # no second ask
        # Positive control: the 200 is asked both days and never stamped.
        self.assertEqual(tmdb.asked.count("/movie/102"), 2)
        self.assertIsNone(alive["tmdb_missing_at"])

    def test_control_another_failure_is_not_a_stamp(self):
        db = _Db([_row(1, 101, release_date=SOON, status="Planned")])
        tmdb = _Tmdb({"/movie/101": 500})
        _run(mod.verify_upcoming_dates, db, tmdb)

        self.assertEqual(db.stamps, [])


class Check3StaleStatusTest(unittest.TestCase):

    def test_a_404_is_stamped_once_and_not_asked_the_next_day(self):
        gone, alive = _row(1, 1416, media_type="tv", release_date=PAST, status="In Production"), \
            _row(2, 1417, release_date=PAST, status="Planned")
        db = _Db([gone, alive])
        tmdb = _Tmdb({"/tv/1416": 404, "/movie/1417": {"status": "Planned"}})

        _run(mod.verify_stale_status, db, tmdb)
        _run(mod.verify_stale_status, db, tmdb)

        self.assertEqual(db.stamps, [["m-00001"]])
        self.assertEqual(tmdb.asked.count("/tv/1416"), 1)
        self.assertEqual(tmdb.asked.count("/movie/1417"), 2)               # positive control
        self.assertIsNone(alive["tmdb_missing_at"])

    def test_a_row_stamped_earlier_is_not_asked(self):
        db = _Db([_row(1, 1416, media_type="tv", release_date=PAST, status="In Production",
                       tmdb_missing_at="2026-10-20T08:00:00+00:00")])
        tmdb = _Tmdb({})
        _run(mod.verify_stale_status, db, tmdb)

        self.assertEqual(tmdb.asked, [])


if __name__ == "__main__":
    unittest.main()
