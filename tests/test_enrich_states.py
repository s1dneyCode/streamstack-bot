"""
Unit tests for the enrichment's «States» step (RUN B3.1 · 0a): it never writes
is_in_theatres — that column belongs to daily_verify.py's now-playing sync —
and it keeps writing is_streamable_now exactly as before, counted in
«States updated».

main() is driven with fakes: a Supabase client whose rpc() serves the queue
and whose table() chains record every update payload, and a TMDb client that
answers {}. Each row skips every step that would call TMDb beyond Step 0, so
«States» is the step under test. No network, no database. Run from the repo
root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import sys
import types
import unittest
from unittest import mock

from bot import enrich_new_titles

FAKE_ENV = {
    "TMDB_API_KEY": "fake-tmdb-key",
    "SUPABASE_URL": "https://project.supabase.invalid",
    "SUPABASE_KEY": "fake-supabase-key",
}


class _FakeQuery:
    METHODS = {"select", "eq", "is_", "limit", "order", "update", "range"}

    def __init__(self, client: "_FakeClient", table: str):
        self._client = client
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
        self._client.executed.append(self)
        return types.SimpleNamespace(data=list(self._client.tables.get(self.table, [])), count=0)


class _FakeClient:
    def __init__(self, queue, tables):
        self.queue = queue
        self.tables = tables
        self.executed = []

    def rpc(self, name, params):
        return types.SimpleNamespace(execute=lambda: types.SimpleNamespace(data=list(self.queue)))

    def table(self, name):
        return _FakeQuery(self, name)

    @property
    def media_payloads(self) -> list[dict]:
        """Every update payload sent to media, in order."""
        return [q.op("update")[1][0] for q in self.executed if q.table == "media" and q.op("update")]


class _FakeTmdb:
    def _get(self, path, params=None):
        return {}

    def get_title_logo(self, **kwargs):
        return None


def _row(**overrides) -> dict:
    row = {
        "id": "row-1", "tmdb_id": 101, "title": "Street Fighter", "media_type": "movie",
        "poster_path": "/p.jpg", "poster_url": "https://cdn.invalid/p.jpg",
        "release_date": "2026-10-16", "is_in_theatres": True, "is_streamable_now": False,
        "title_logo_url": "https://image.tmdb.invalid/t/p/w500/l.png",
        "certification": "PG-13", "genres": ["Action"], "runtime": 110,
    }
    row.update(overrides)
    return row


def _run(row: dict, streaming: bool) -> tuple[_FakeClient, str]:
    """main() over one row; `streaming` says whether the title has a provider row."""
    client = _FakeClient(
        queue=[row],
        tables={
            "media_trailers": [{"media_id": 0}],
            "media_credits": [{"media_id": 0}],
            "streaming_availability": [{"media_id": row["id"]}] if streaming else [],
        },
    )
    out = io.StringIO()
    with mock.patch.dict(os.environ, FAKE_ENV), \
            mock.patch.object(sys, "argv", ["enrich_new_titles"]), \
            mock.patch.object(enrich_new_titles, "SupabaseClient", lambda **kw: types.SimpleNamespace(client=client)), \
            mock.patch.object(enrich_new_titles, "TmdbClient", lambda **kw: _FakeTmdb()), \
            mock.patch.object(enrich_new_titles.time, "sleep", lambda s: None), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        enrich_new_titles.main()
    return client, out.getvalue()


def _states_updated(out: str) -> int:
    line = next(l for l in out.splitlines() if l.startswith("[ENRICH] States updated"))
    return int(line.split(":")[1])


class StatesOwnershipTest(unittest.TestCase):

    def test_states_never_writes_is_in_theatres(self):
        # Ownership, not a value: whatever the title arrives with, and with or
        # without a provider row, neither States' payload nor the stamp may
        # carry the key at all. (The fixture skips steps 1–8, which write
        # nothing here; B1's exact-payload pins cover Step 5.)
        for in_theatres in (True, False):
            for streaming in (False, True):
                with self.subTest(in_theatres=in_theatres, streaming=streaming):
                    client, _ = _run(_row(is_in_theatres=in_theatres, is_streamable_now=False), streaming)

                    self.assertEqual([p for p in client.media_payloads if "is_in_theatres" in p], [])
                    # Control: the run did reach the end of the row (it was stamped).
                    self.assertTrue([p for p in client.media_payloads if "enriched_at" in p])


class StatesStreamableTest(unittest.TestCase):

    def test_a_provider_row_flips_streamable_to_true_and_is_counted(self):
        client, out = _run(_row(is_streamable_now=False), streaming=True)

        self.assertIn({"is_streamable_now": True}, client.media_payloads)
        self.assertEqual(_states_updated(out), 1)
        self.assertIn("[ENRICH]   state: streamable=True\n", out)
        # The write targets this row, and the probe asks about this row in
        # any region and of any kind (no region or monetization filter).
        write = next(q for q in client.executed
                     if q.table == "media" and q.op("update") and "is_streamable_now" in q.op("update")[1][0])
        self.assertEqual(write.op("eq"), ("eq", ("id", "row-1"), {}))
        probe = next(q for q in client.executed if q.table == "streaming_availability")
        self.assertEqual(probe.ops, [("select", ("media_id",), {}), ("eq", ("media_id", "row-1"), {}),
                                     ("limit", (1,), {})])

    def test_the_streamable_rule_is_unchanged(self):
        # A control that holds before and after B3.1: a title not in theatres
        # whose provider row turns it streamable is written true and counted.
        client, out = _run(_row(is_in_theatres=False, is_streamable_now=False), streaming=True)

        self.assertTrue(any(p.get("is_streamable_now") is True for p in client.media_payloads))
        self.assertEqual(_states_updated(out), 1)

    def test_no_provider_row_flips_streamable_to_false_and_is_counted(self):
        client, out = _run(_row(is_streamable_now=True), streaming=False)

        self.assertIn({"is_streamable_now": False}, client.media_payloads)
        self.assertEqual(_states_updated(out), 1)

    def test_a_matching_streamable_is_not_written_nor_counted(self):
        # Control for the counter: an in-theatres title whose streamable flag
        # already matches leaves States with nothing to write — both ways.
        for streamable in (True, False):
            with self.subTest(streamable=streamable):
                client, out = _run(_row(is_in_theatres=True, is_streamable_now=streamable),
                                   streaming=streamable)

                self.assertEqual([p for p in client.media_payloads if "is_streamable_now" in p], [])
                self.assertEqual(_states_updated(out), 0)


if __name__ == "__main__":
    unittest.main()
