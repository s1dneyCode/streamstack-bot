"""
Unit tests for the queue RPC's retry (RUN B3.1 · 0b): fetch_queue tries up to
three times, sleeping 2 s then 4 s, on a transient failure only — a 5xx from
PostgREST or Cloudflare, or a dropped or timed-out connection. The function is
STABLE and has no side effects, so a retry is safe. PGRST202 still fails fast
at the first attempt, and a 4xx still propagates at once.

The errors are the ones measured: postgrest's APIError (code = the HTTP status
as an int when the body is not JSON, e.g. Cloudflare's 503/520; PostgREST's
own PGRST00x for a connection or pool failure) and httpx's transport errors.
The sleep is the module's time.sleep, patched with a recorder: no test waits.
No network, no database. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import types
import unittest
from unittest import mock

import httpx
from postgrest.exceptions import APIError

from bot import enrich_new_titles

ROWS = [{"id": "a"}, {"id": "b"}]


def _cloudflare(status: int) -> APIError:
    """What postgrest raises when a 5xx arrives with an HTML body."""
    return APIError({"message": "JSON could not be generated", "code": status,
                     "hint": "Refer to full message for details",
                     "details": f"b'<html><title>{status}</title></html>'"})


def _postgrest(code: str, message: str) -> APIError:
    return APIError({"code": code, "message": message, "hint": None, "details": None})


MISSING_FUNCTION = _postgrest(
    "PGRST202", "Could not find the function public.enrichment_queue(p_limit) in the schema cache")
DENIED = _postgrest("42501", "permission denied for function enrichment_queue")


class _ScriptedClient:
    """db.client whose rpc().execute() answers one scripted outcome per attempt."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.rpc_calls = []

    def rpc(self, name, params):
        self.rpc_calls.append((name, params))

        def execute():
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return types.SimpleNamespace(data=outcome)

        return types.SimpleNamespace(execute=execute)


def _fetch(outcomes):
    """fetch_queue over a scripted client; returns (result_or_exception, client, sleeps, output)."""
    client = _ScriptedClient(outcomes)
    sleeps, out = [], io.StringIO()
    with mock.patch.object(enrich_new_titles.time, "sleep", sleeps.append), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            result = enrich_new_titles.fetch_queue(types.SimpleNamespace(client=client), 250)
        except BaseException as exc:  # SystemExit included: the fail-fast is an exit
            result = exc
    return result, client, sleeps, out.getvalue()


class QueueRetryTest(unittest.TestCase):

    def test_two_transient_failures_then_rows(self):
        result, client, sleeps, out = _fetch([
            _cloudflare(503),
            httpx.RemoteProtocolError("Server disconnected without sending a response."),
            ROWS,
        ])

        self.assertEqual(result, ROWS)
        self.assertEqual(client.rpc_calls, [("enrichment_queue", {"p_limit": 250})] * 3)
        self.assertEqual(sleeps, [2, 4])
        self.assertEqual(out.splitlines(), [
            "[ENRICH] queue RPC failed (attempt 1 of 3): APIError: JSON could not be generated "
            "(code 503) — retrying in 2 s",
            "[ENRICH] queue RPC failed (attempt 2 of 3): RemoteProtocolError: Server disconnected "
            "without sending a response. — retrying in 4 s",
        ])

    def test_each_transient_kind_is_retried(self):
        kinds = [_cloudflare(s) for s in (503, 520, 521, 522, 523, 524)] + [
            _cloudflare("503"),  # the same status, should a client hand it over as text
            _postgrest("PGRST000", "Could not connect with the database"),
            _postgrest("PGRST001", "Could not connect with the database due to an internal error"),
            _postgrest("PGRST002", "Could not query the database for the schema cache. Retrying."),
            _postgrest("PGRST003", "Timed out acquiring connection from connection pool."),
            httpx.ReadTimeout("The read operation timed out"),
            httpx.ConnectError("[Errno 104] Connection reset by peer"),
            httpx.RemoteProtocolError("Server disconnected without sending a response."),
        ]
        for error in kinds:
            with self.subTest(error=f"{type(error).__name__} {getattr(error, 'code', '')}"):
                result, client, sleeps, _ = _fetch([error, ROWS])

                self.assertEqual(result, ROWS)
                self.assertEqual(len(client.rpc_calls), 2)
                self.assertEqual(sleeps, [2])

    def test_three_transient_failures_propagate_the_third(self):
        third = httpx.ReadTimeout("The read operation timed out")

        result, client, sleeps, out = _fetch([_cloudflare(503), _cloudflare(520), third])

        self.assertIs(result, third)
        self.assertEqual(len(client.rpc_calls), 3)
        self.assertEqual(sleeps, [2, 4])  # no sleep after the last attempt
        self.assertEqual(len(out.splitlines()), 2)


STATEMENT_TIMEOUT = "canceling statement due to statement timeout"


class QueueStatementTimeoutTest(unittest.TestCase):
    """B5.5 · 0c(6): a 57014 (PostgREST's 8 s statement_timeout, the server
    under load) is transient here too, as in the related queue's retry since
    B5.1 (_TRANSIENT_RPC_CODES): the same three attempts, 2 s then 4 s."""

    def test_one_statement_timeout_is_retried_once_then_rows(self):
        result, client, sleeps, out = _fetch([_postgrest("57014", STATEMENT_TIMEOUT), ROWS])

        self.assertEqual(result, ROWS)
        self.assertEqual(len(client.rpc_calls), 2)
        self.assertEqual(sleeps, [2])
        self.assertEqual(out.splitlines(), [
            f"[ENRICH] queue RPC failed (attempt 1 of 3): APIError: {STATEMENT_TIMEOUT} (code 57014) — retrying in 2 s",
        ])

    def test_two_statement_timeouts_are_retried_as_the_related_queue_does(self):
        result, client, sleeps, _ = _fetch([_postgrest("57014", STATEMENT_TIMEOUT)] * 2 + [ROWS])

        self.assertEqual(result, ROWS)
        self.assertEqual(len(client.rpc_calls), 3)
        self.assertEqual(sleeps, [2, 4])

    def test_three_statement_timeouts_fail_as_before(self):
        third = _postgrest("57014", STATEMENT_TIMEOUT)
        result, client, sleeps, _ = _fetch([_postgrest("57014", STATEMENT_TIMEOUT)] * 2 + [third])

        self.assertIs(result, third)
        self.assertEqual(len(client.rpc_calls), 3)
        self.assertEqual(sleeps, [2, 4])


class QueueRetryControlsTest(unittest.TestCase):

    def test_a_missing_function_fails_fast_at_the_first_attempt(self):
        # B3's fail-fast stays first and alone: no retry, no sleep.
        result, client, sleeps, out = _fetch([MISSING_FUNCTION, ROWS, ROWS])

        self.assertIsInstance(result, SystemExit)
        self.assertEqual(result.code, 1)
        self.assertEqual(len(client.rpc_calls), 1)
        self.assertEqual(sleeps, [])
        self.assertEqual(len(out.splitlines()), 1)
        self.assertIn("20261023170000", out)

    def test_a_4xx_propagates_at_once(self):
        for error in (DENIED, _cloudflare(404), _cloudflare(400)):
            with self.subTest(error=f"{getattr(error, 'code', '')}"):
                result, client, sleeps, out = _fetch([error, ROWS, ROWS])

                self.assertIs(result, error)
                self.assertEqual(len(client.rpc_calls), 1)
                self.assertEqual(sleeps, [])
                self.assertEqual(out, "")

    def test_a_healthy_queue_is_one_call_and_no_sleep(self):
        result, client, sleeps, out = _fetch([ROWS])

        self.assertEqual(result, ROWS)
        self.assertEqual(len(client.rpc_calls), 1)
        self.assertEqual(sleeps, [])
        self.assertEqual(out, "")


if __name__ == "__main__":
    unittest.main()
