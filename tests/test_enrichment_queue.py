"""
Unit tests for the enrichment queue (RUN B3 · 0a): bot.enrich_new_titles takes
its rows from the server function public.enrichment_queue(p_limit) — migration
20261023170000 — instead of a table select ordered by created_at.

Fakes only: a Supabase client whose rpc(name, params) records both and answers
canned rows (or raises a PostgREST-shaped error), a table() that records the
per-title steps' chains, and a TMDb client that records what it is asked. No
network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import sys
import types
import unittest
from unittest import mock

from postgrest.exceptions import APIError

from bot import enrich_new_titles

MIGRATION = "20261023170000"
FAKE_ENV = {
    "TMDB_API_KEY": "fake-tmdb-key",
    "SUPABASE_URL": "https://project.supabase.invalid",
    "SUPABASE_KEY": "fake-supabase-key",
}
# The selection the script used to make, recognisable by its projection.
THIRTEEN = ("id, tmdb_id, title, media_type, poster_path, poster_url, release_date, "
            "is_in_theatres, is_streamable_now, title_logo_url, certification, "
            "genres, runtime")


# postgrest's own exception, built from the JSON PostgREST answers when the
# function is not in its schema cache (404, PGRST202).
MISSING_FUNCTION = APIError({
    "code": "PGRST202",
    "message": "Could not find the function public.enrichment_queue(p_limit) in the schema cache",
    "hint": None,
    "details": "Searched for the function public.enrichment_queue with parameter p_limit, "
               "but no matches were found in the schema cache.",
})
DENIED = APIError({"code": "42501", "message": "permission denied for function enrichment_queue",
                   "hint": None, "details": None})


class _FakeQuery:
    """A table chain (the per-title steps): records every call, answers on execute()."""

    METHODS = {"select", "eq", "is_", "limit", "order", "update", "range"}

    def __init__(self, db: "_FakeSupabase", table: str):
        self._db = db
        self.table = table
        self.ops = []

    def __getattr__(self, name):
        if name not in self.METHODS:
            raise AttributeError(name)

        def record(*args, **kwargs):
            self.ops.append((name, args, kwargs))
            return self

        return record

    def op(self, name):
        return next((o for o in self.ops if o[0] == name), None)

    def execute(self):
        self._db.executed.append(self)
        return types.SimpleNamespace(data=list(self._db.tables.get(self.table, [])), count=0)


class _FakeClient:
    """The raw supabase client (db.client): rpc() and table(), recorded."""

    def __init__(self, queue=None, tables=None, rpc_error=None):
        self.queue = queue
        self.tables = tables or {}
        self.rpc_error = rpc_error
        self.rpc_calls = []     # (name, params), one per rpc()
        self.rpc_executes = 0   # requests actually sent
        self.executed = []      # every executed table chain

    def rpc(self, name, params):
        self.rpc_calls.append((name, params))

        def execute():
            self.rpc_executes += 1
            if self.rpc_error is not None:
                raise self.rpc_error
            return types.SimpleNamespace(data=self.queue)

        return types.SimpleNamespace(execute=execute)

    def table(self, name):
        return _FakeQuery(self, name)


def _FakeSupabase(**kwargs) -> types.SimpleNamespace:
    """Stands in for SupabaseClient: like the real wrapper, it has no rpc() or
    table() of its own — only `.client`, the raw client. So a `db.rpc(...)`
    slip fails here as it would in production."""
    return types.SimpleNamespace(client=_FakeClient(**kwargs))


class _FakeTmdb:
    def __init__(self):
        self.paths = []  # every _get path, in order

    def _get(self, path, params=None):
        self.paths.append(path)
        return {}

    def get_title_logo(self, **kwargs):
        return None


def _row(**overrides) -> dict:
    """One queue row with the thirteen keys; it skips every step that would call
    TMDb beyond Step 0, so the loop runs on fakes alone."""
    row = {
        "id": "row-1", "tmdb_id": 101, "title": "Street Fighter", "media_type": "movie",
        "poster_path": "/p.jpg", "poster_url": "https://cdn.invalid/p.jpg",
        "release_date": "2026-10-16", "is_in_theatres": True, "is_streamable_now": False,
        "title_logo_url": "https://image.tmdb.invalid/t/p/w500/l.png",
        "certification": "PG-13", "genres": ["Action"], "runtime": 110,
    }
    row.update(overrides)
    return row


def _main(db, tmdb, argv=("enrich_new_titles",), env=None):
    """Run main() on fakes; return (exit_code_or_None, everything printed)."""
    out = io.StringIO()
    code = None
    environ = {**FAKE_ENV, **(env or {})}
    with mock.patch.dict(os.environ, environ), \
            mock.patch.object(sys, "argv", list(argv)), \
            mock.patch.object(enrich_new_titles, "SupabaseClient", lambda **kw: db), \
            mock.patch.object(enrich_new_titles, "TmdbClient", lambda **kw: tmdb), \
            mock.patch.object(enrich_new_titles.time, "sleep", lambda s: None), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        if env is None or "ENRICH_LIMIT" not in env:
            os.environ.pop("ENRICH_LIMIT", None)
        try:
            enrich_new_titles.main()
        except SystemExit as exc:
            code = exc.code
    return code, out.getvalue()


def _steps_tables() -> dict:
    return {"media_trailers": [{"media_id": 0}], "media_credits": [{"media_id": 0}],
            "streaming_availability": []}


class FetchQueueTest(unittest.TestCase):

    def setUp(self):
        # B3.1: fetch_queue may sleep between retries. These tests must never
        # wait for real, not even when a regression sends them into a retry.
        patcher = mock.patch.object(enrich_new_titles.time, "sleep", lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_calls_the_rpc_once_with_its_limit_and_returns_the_rows(self):
        rows = [_row(id="a"), _row(id="b")]
        db = _FakeSupabase(queue=rows)

        result = enrich_new_titles.fetch_queue(db, 250)

        self.assertEqual(db.client.rpc_calls, [("enrichment_queue", {"p_limit": 250})])
        self.assertEqual(db.client.rpc_executes, 1)  # one request, not one builder executed twice
        self.assertEqual(result, rows)
        self.assertEqual(db.client.executed, [])  # no table read at all

    def test_no_data_is_an_empty_list(self):
        self.assertEqual(enrich_new_titles.fetch_queue(_FakeSupabase(queue=None), 250), [])
        # Control: data is returned as is.
        self.assertEqual(enrich_new_titles.fetch_queue(_FakeSupabase(queue=[_row()]), 250), [_row()])

    def test_a_missing_function_fails_fast_naming_the_migration(self):
        db = _FakeSupabase(rpc_error=MISSING_FUNCTION)
        out = io.StringIO()

        with contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as cm:
            enrich_new_titles.fetch_queue(db, 250)

        self.assertEqual(cm.exception.code, 1)
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 1, out.getvalue())
        self.assertTrue(lines[0].startswith("[ENRICH] ERROR: "), lines[0])
        self.assertIn(MIGRATION, lines[0])
        # Control: a healthy server does not exit.
        self.assertEqual(enrich_new_titles.fetch_queue(_FakeSupabase(queue=[]), 250), [])

    def test_other_errors_are_not_taken_for_a_missing_function(self):
        # A permission error is a different failure: it propagates, loud,
        # instead of being reported as an undeployed migration.
        with self.assertRaises(APIError) as cm:
            enrich_new_titles.fetch_queue(_FakeSupabase(rpc_error=DENIED), 250)
        self.assertEqual(cm.exception.code, "42501")


class MainSelectionTest(unittest.TestCase):

    def test_main_selects_through_the_rpc(self):
        db = _FakeSupabase(queue=[_row()], tables=_steps_tables())

        code, _ = _main(db, _FakeTmdb())

        self.assertIsNone(code)
        self.assertEqual(db.client.rpc_calls, [("enrichment_queue", {"p_limit": 500})])

    def test_main_never_selects_from_the_table(self):
        # With rows, and with an empty queue too: an empty server answer must
        # not fall back to a table read either.
        for queue in ([_row()], []):
            with self.subTest(rows=len(queue)):
                db = _FakeSupabase(queue=queue, tables=_steps_tables())

                _main(db, _FakeTmdb())

                # How the pin tells the selection from the per-title steps: the
                # old selection is the only read with the thirteen-column
                # projection, and the only chain ordered by created_at. The
                # steps read other tables (media_trailers, media_credits,
                # streaming_availability) by media_id, stamp media with an
                # update, and count media with select("id", count="exact").
                selects = [q.op("select")[1][0] for q in db.client.executed if q.op("select")]
                self.assertNotIn(THIRTEEN, selects)
                self.assertFalse([q for q in db.client.executed
                                  if q.op("order") and q.op("order")[1][:1] == ("created_at",)])
                # The ONLY read of media is the summary's count.
                self.assertEqual(
                    [q.op("select") for q in db.client.executed if q.table == "media" and q.op("select")],
                    [("select", ("id",), {"count": "exact"})],
                )
        # Control: table() IS used — by the steps, not for the selection.
        db = _FakeSupabase(queue=[_row()], tables=_steps_tables())
        _main(db, _FakeTmdb())
        self.assertIn("media_trailers", [q.table for q in db.client.executed])

    def test_the_loop_keeps_the_servers_order(self):
        # Order is the whole point: the server's order must reach the loop
        # untouched — not reversed, not re-sorted newest-first inside the batch.
        queue = [_row(id="c", tmdb_id=303, release_date="2019-01-01"),
                 _row(id="a", tmdb_id=101, release_date="2026-10-20"),
                 _row(id="b", tmdb_id=202, release_date="2024-05-05")]
        db, tmdb = _FakeSupabase(queue=queue, tables=_steps_tables()), _FakeTmdb()

        _, out = _main(db, tmdb, argv=("enrich_new_titles", "--limit=37"))

        self.assertEqual(tmdb.paths, ["/movie/303", "/movie/101", "/movie/202"])
        stamped = [q.op("eq")[1] for q in db.client.executed
                   if q.op("update") and "enriched_at" in q.op("update")[1][0]]
        self.assertEqual(stamped, [("id", "c"), ("id", "a"), ("id", "b")])
        self.assertIn("[ENRICH] 3 title(s) to process this run.\n", out)

    def test_the_canned_rows_reach_the_per_title_loop(self):
        db = _FakeSupabase(queue=[_row(tmdb_id=4242, media_type="movie")], tables=_steps_tables())
        tmdb = _FakeTmdb()

        _main(db, tmdb)

        self.assertEqual(tmdb.paths[:1], ["/movie/4242"])  # Step 0's base detail
        stamped = [q.op("eq")[1] for q in db.client.executed
                   if q.op("update") and "enriched_at" in q.op("update")[1][0]]
        self.assertEqual(stamped, [("id", "row-1")])

    def test_limit_flag_and_env_still_set_p_limit(self):
        cases = [
            ("--limit=37", ("enrich_new_titles", "--limit=37"), None, 37),
            ("--limit=2000, the dispatch default", ("enrich_new_titles", "--limit=2000"), None, 2000),
            ("ENRICH_LIMIT=42", ("enrich_new_titles",), {"ENRICH_LIMIT": "42"}, 42),
            ("neither: the default", ("enrich_new_titles",), None, 500),
        ]
        for label, argv, env, expected in cases:
            with self.subTest(label):
                db = _FakeSupabase(queue=[], tables=_steps_tables())

                _main(db, _FakeTmdb(), argv=argv, env=env)

                self.assertEqual(db.client.rpc_calls, [("enrichment_queue", {"p_limit": expected})])

    def test_the_log_line_says_the_server_order(self):
        _, out = _main(_FakeSupabase(queue=[], tables=_steps_tables()), _FakeTmdb(),
                       argv=("enrich_new_titles", "--limit=37"))

        self.assertIn(
            "[ENRICH] Scanning up to 37 title(s) where enriched_at IS NULL "
            "(server order: in theatres → home rails → popular → newest)...\n",
            out,
        )

    def test_main_fails_fast_when_the_function_is_missing(self):
        tmdb = _FakeTmdb()

        code, out = _main(_FakeSupabase(rpc_error=MISSING_FUNCTION, tables=_steps_tables()), tmdb)

        self.assertEqual(code, 1)
        self.assertIn(MIGRATION, out)
        self.assertEqual(tmdb.paths, [])  # no title was touched
        # Control: a healthy server runs the loop and does not exit.
        code, _ = _main(_FakeSupabase(queue=[_row()], tables=_steps_tables()), _FakeTmdb())
        self.assertIsNone(code)


if __name__ == "__main__":
    unittest.main()
