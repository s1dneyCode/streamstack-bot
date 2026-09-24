"""
Unit tests for bot.main.refresh_materialized_caches().

No network and no database: psycopg is swapped in sys.modules for a fake that
records every statement the bot would have sent. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import os
import re
import sys
import types
import unittest
from unittest import mock

from bot import main as bot_main

# Written out literally rather than imported from bot.main, so a change to the
# module's list cannot silently change what these tests expect.
EXPLORE = "select public.refresh_media_explore_universe()"
HOME_RAILS = "select public.refresh_home_rail_universes()"

# A stand-in that can never resolve (.invalid), so no test can reach a database.
FAKE_PASSWORD = "fake-password"
FAKE_DSN = f"postgresql://fake-user:{FAKE_PASSWORD}@db.invalid:5432/postgres"


class OperationalError(Exception):
    """Named like psycopg's, since the class name is what the log shows."""


class ProgrammingError(Exception):
    """What psycopg raises for a malformed DSN — quoting the bad part of it."""


class _FakePsycopg:
    """Just enough of psycopg's surface for refresh_materialized_caches()."""

    def __init__(self, fail_on=(), connect_error=None):
        self.fail_on = set(fail_on)
        self.connect_error = connect_error
        self.connect_calls = []  # (dsn, kwargs), one per connect()
        self.executed = []  # every statement, in the order it was sent

    def module(self) -> types.ModuleType:
        mod = types.ModuleType("psycopg")
        mod.connect = self.connect
        return mod

    def connect(self, dsn, **kwargs):
        self.connect_calls.append((dsn, kwargs))
        if self.connect_error is not None:
            raise self.connect_error
        return _FakeConnection(self)


class _FakeConnection:
    def __init__(self, psycopg: _FakePsycopg):
        self._psycopg = psycopg

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def cursor(self):
        return _FakeCursor(self._psycopg)


class _FakeCursor:
    def __init__(self, psycopg: _FakePsycopg):
        self._psycopg = psycopg

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql):
        # Deliberately no params: the steps are fixed statements, so anything
        # extra is a TypeError and shows up as a FAILED line.
        self._psycopg.executed.append(sql)
        if sql in self._psycopg.fail_on:
            raise RuntimeError("simulated failure")


def _chained_like_psycopg(message: str) -> ProgrammingError:
    """How psycopg 3.2.13 reports a malformed DSN (measured): a
    ProgrammingError raised `from None` over an OperationalError that carries
    the same message — so the secret is in __context__ as well."""
    try:
        try:
            raise OperationalError(message)
        except OperationalError:
            raise ProgrammingError(message) from None
    except ProgrammingError as exc:
        return exc


def _run(fake: _FakePsycopg, dsn: str | None) -> str:
    """Call the refresh with `fake` as psycopg and return what it printed.

    stdout and stderr land in one buffer, as they do in the Actions log, so a
    leak through a traceback or a stray stderr write is caught too.
    """
    out = io.StringIO()
    with mock.patch.dict(sys.modules, {"psycopg": fake.module()}), \
            mock.patch.dict(os.environ), \
            contextlib.redirect_stdout(out), \
            contextlib.redirect_stderr(out):
        if dsn is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = dsn
        bot_main.refresh_materialized_caches()
    return out.getvalue()


def _normalised(out: str) -> str:
    """Timings vary run to run; every other character of the log is pinned."""
    return re.sub(r"\d+\.\ds", "Ns", out)


class RefreshMaterializedCachesTest(unittest.TestCase):

    def test_calls_the_two_db_functions_in_order_and_nothing_else(self):
        fake = _FakePsycopg()

        out = _run(fake, FAKE_DSN)

        self.assertEqual(fake.executed, [EXPLORE, HOME_RAILS])
        self.assertEqual(fake.connect_calls, [(FAKE_DSN, {"autocommit": True})])
        self.assertEqual(
            _normalised(out),
            "[BOT] Explore + Popular refreshed in Ns.\n"
            "[BOT] Home rails refreshed in Ns.\n",
        )

    def test_failing_first_step_still_runs_the_second(self):
        fake = _FakePsycopg(fail_on={EXPLORE})

        out = _run(fake, FAKE_DSN)  # must not raise: a failure never fails the run

        self.assertEqual(fake.executed, [EXPLORE, HOME_RAILS])
        self.assertEqual(
            _normalised(out),
            "[BOT] Explore + Popular refresh FAILED after Ns — simulated "
            "failure. Previous data kept.\n"
            "[BOT] Home rails refreshed in Ns.\n",
        )

    def test_failing_last_step_is_logged_and_does_not_raise(self):
        fake = _FakePsycopg(fail_on={HOME_RAILS})

        out = _run(fake, FAKE_DSN)

        self.assertEqual(fake.executed, [EXPLORE, HOME_RAILS])
        self.assertEqual(
            _normalised(out),
            "[BOT] Explore + Popular refreshed in Ns.\n"
            "[BOT] Home rails refresh FAILED after Ns — simulated failure. "
            "Previous data kept.\n",
        )

    def test_missing_database_url_skips_both_with_the_existing_message(self):
        # Unset, or empty — the latter is what GitHub Actions passes when the
        # secret does not exist.
        for dsn in (None, ""):
            with self.subTest(dsn=dsn):
                fake = _FakePsycopg()

                out = _run(fake, dsn)

                self.assertEqual(fake.connect_calls, [])
                self.assertEqual(fake.executed, [])
                self.assertEqual(
                    out,
                    "[BOT] Materialized caches: DATABASE_URL not set — "
                    "refresh skipped.\n",
                )

    def test_connect_failure_skips_both_without_raising(self):
        fake = _FakePsycopg(connect_error=OperationalError("simulated connect failure"))

        out = _run(fake, FAKE_DSN)

        self.assertEqual(len(fake.connect_calls), 1)
        self.assertEqual(fake.executed, [])
        self.assertEqual(
            out,
            "[BOT] Materialized caches: connection FAILED — OperationalError. "
            "Refresh skipped.\n",
        )

    def test_connect_failure_never_echoes_the_dsn(self):
        # Shaped like what psycopg 3.2.13 raises for a malformed DSN (measured
        # with fake DSNs): the offending token is quoted back, password and all.
        # This repo's Actions logs are public, and GitHub masks only a secret's
        # exact value, never a fragment of it.
        leaks = (
            _chained_like_psycopg(
                f'invalid percent-encoded token: "{FAKE_PASSWORD}%zz"'
            ),
            ProgrammingError(f'unexpected spaces found in "{FAKE_DSN}"'),
            OperationalError(f"failed to resolve host '{FAKE_PASSWORD}@db.invalid'"),
        )
        for i, exc in enumerate(leaks):
            with self.subTest(case=i, exc=type(exc).__name__):
                fake = _FakePsycopg(connect_error=exc)

                out = _run(fake, FAKE_DSN)

                self.assertNotIn(FAKE_PASSWORD, out)
                self.assertNotIn("db.invalid", out)
                self.assertEqual(
                    out,
                    f"[BOT] Materialized caches: connection FAILED — "
                    f"{type(exc).__name__}. Refresh skipped.\n",
                )


if __name__ == "__main__":
    unittest.main()
