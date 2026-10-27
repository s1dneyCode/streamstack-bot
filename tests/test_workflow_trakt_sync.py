"""
Unit tests for Trakt Sync's schedule (RUN B5 · 0c, moved by B5.1 · 0c): daily
while the backlog is re-drained — decision 1.439 — at 02:17 UTC, off the top of
the hour that GitHub delays (run #24 of the 03:00 schedule started five hours
late, on top of the other daily jobs), with 5,000 related titles a run (3,000
until B5.4) and a time budget of 80 minutes inside the job's 100 (90 until
B5.6).

What moved in B5.1: the cron (0 3 * * * → 17 2 * * *), the header's numbers
(56,411 / ≈ 19 days → 131,456 pending / ≈ 44 days), and two new pins — the
budget the workflow passes, and the code's defaults matching what the workflow
passes.

What moved in B5.6: timeout-minutes 90 → 100, and its pin asserts the margin
— the budget, one window and two capped Retry-After from the code's own
constants, plus a setup allowance — instead of the number.

The workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import inspect
import pathlib
import re
import unittest

from bot import sync_trakt, trakt

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "trakt_sync.yml"
# Checkout, setup-python and pip install before the budget's clock starts:
# 1–2 minutes measured, 3 allowed (B5.6).
SETUP_ALLOWANCE_MIN = 3


def active_crons(text: str) -> list[str]:
    """Every cron expression on a line that is not a comment."""
    crons = []
    for line in text.splitlines():
        code = line.split("#", 1)[0]
        match = re.search(r"-\s*cron:\s*'([^']+)'", code)
        if match:
            crons.append(match.group(1))
    return crons


def is_weekly(cron: str) -> bool:
    """A cron whose day-of-week field names a day (e.g. '0 8 * * 1')."""
    return cron.split()[4] != "*"


def on_the_hour(cron: str) -> bool:
    """A cron at minute 0 — the slot GitHub documents as the one delayed."""
    return cron.split()[0] == "0"


def header(text: str) -> str:
    """The comment block above `name:`."""
    return text.split("name:", 1)[0]


def job_timeout_minutes(text: str) -> int:
    """The one job's timeout-minutes."""
    found = re.findall(r"^\s*timeout-minutes:\s*(\d+)\s*$", text, re.M)
    if len(found) != 1:
        raise AssertionError(f"expected one timeout-minutes, found {found}")
    return int(found[0])


def worst_case_minutes() -> float:
    """The job's margin, from the code's own numbers and the setup allowance
    (B5.6): the time budget, then a request in flight that waits one full
    budget window and a capped Retry-After on each of its retries, plus the
    setup — checkout, setup-python and pip install run before main() starts
    the budget's clock. One request's worst case, not a title's: a title makes
    up to three requests."""
    retries = inspect.signature(trakt.TraktClient._request).parameters["retries"].default
    return (sync_trakt.DEFAULT_TRAKT_TIME_BUDGET_MIN
            + trakt.DEFAULT_WINDOW_SECONDS / 60
            + (retries - 1) * trakt._PAUSE_MAX_SECONDS / 60
            + SETUP_ALLOWANCE_MIN)


class TraktSyncCadenceTest(unittest.TestCase):

    def setUp(self):
        self.text = WORKFLOW.read_text(encoding="utf-8")

    def test_the_schedule_is_daily_at_0217_utc(self):
        self.assertEqual(active_crons(self.text), ["17 2 * * *"])
        self.assertFalse([c for c in active_crons(self.text) if is_weekly(c)])
        self.assertFalse([c for c in active_crons(self.text) if on_the_hour(c)])

    def test_the_scan_bites_on_a_monday_fixture(self):
        # Positive control: the scan sees an active Monday cron, and ignores a commented one.
        monday = "on:\n  schedule:\n    - cron: '0 8 * * 1'  # Mondays\n"
        commented = "on:\n  schedule:\n    - cron: '17 2 * * *'\n    # - cron: '0 8 * * 1'\n"
        self.assertEqual([c for c in active_crons(monday) if is_weekly(c)], ["0 8 * * 1"])
        self.assertEqual([c for c in active_crons(monday) if on_the_hour(c)], ["0 8 * * 1"])
        self.assertEqual(active_crons(commented), ["17 2 * * *"])

    def test_the_related_batch_is_5000_for_the_schedule_and_the_dispatch(self):
        self.assertRegex(self.text, r"related_batch:\n(?:.*\n){0,3}?\s+default: '5000'")
        self.assertIn("TRAKT_RELATED_BATCH: ${{ github.event.inputs.related_batch || '5000' }}", self.text)
        self.assertNotIn("'4000'", self.text)

    def test_the_time_budget_is_80_minutes_for_the_schedule_and_the_dispatch(self):
        self.assertRegex(self.text, r"time_budget_min:\n(?:.*\n){0,3}?\s+default: '80'")
        self.assertIn("TRAKT_TIME_BUDGET_MIN: ${{ github.event.inputs.time_budget_min || '80' }}", self.text)

    def test_the_budget_ends_inside_the_jobs_timeout(self):
        # B5.6: the pin is the margin, not a number (it asserted 90 until
        # then): 80 + 5 + 10 + 3 = 98 minutes inside the job's timeout. The
        # budget is the code's default, which is what the workflow passes.
        budget = re.search(r"time_budget_min \|\| '(\d+)'", self.text)
        self.assertIsNotNone(budget, "the workflow passes no TRAKT_TIME_BUDGET_MIN")
        self.assertEqual(int(budget.group(1)), sync_trakt.DEFAULT_TRAKT_TIME_BUDGET_MIN)
        self.assertLessEqual(worst_case_minutes(), job_timeout_minutes(self.text))

    def test_the_margin_pin_bites_on_the_old_90(self):
        # Positive control: the same arithmetic against B5.5's 90 fails.
        old = re.sub(r"timeout-minutes:\s*\d+", "timeout-minutes: 90", self.text)
        self.assertEqual(job_timeout_minutes(old), 90)
        self.assertGreater(worst_case_minutes(), job_timeout_minutes(old))

    def test_the_codes_defaults_are_what_the_workflow_passes(self):
        # A run started without the env (by hand, outside Actions) drains the same.
        self.assertEqual(sync_trakt.DEFAULT_TRAKT_RELATED_BATCH, 5000)
        self.assertEqual(sync_trakt.DEFAULT_TRAKT_TIME_BUDGET_MIN, 80)
        self.assertEqual(trakt.DEFAULT_WINDOW_CALLS, 420)

    def test_the_budget_window_is_420_calls_for_the_schedule_and_the_dispatch(self):
        # B5.2: the client's own sliding-window budget; TRAKT_WINDOW_SECONDS
        # is left to its env default (300), not mapped here.
        self.assertRegex(self.text, r"window_calls:\n(?:.*\n){0,3}?\s+default: '420'")
        self.assertIn("TRAKT_WINDOW_CALLS: ${{ github.event.inputs.window_calls || '420' }}", self.text)
        self.assertNotIn("TRAKT_WINDOW_SECONDS", self.text)

    def test_the_header_no_longer_counts_on_trakts_header(self):
        # Run #26: Trakt sent no X-Ratelimit; the client paces itself (B5.2).
        self.assertNotIn("pauses before Trakt's own limit", header(self.text))
        self.assertIn("420 requests per 300 s", header(self.text))

    def test_the_file_says_why_until_when_and_how_much(self):
        head = self.text.splitlines()[0]
        self.assertTrue(head.startswith("#"), head)
        for words in ("decision 1.439", "back to Mondays", "131,456", "≈ 44 days", "80"):
            with self.subTest(words=words):
                self.assertIn(words, header(self.text))
        self.assertNotIn("56,411", header(self.text))   # B5's count, before the server measured the queue


if __name__ == "__main__":
    unittest.main()
