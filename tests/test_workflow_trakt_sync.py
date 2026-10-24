"""
Unit tests for Trakt Sync's cadence (RUN B5 · 0c): daily at 03:00 UTC with
3,000 related titles a run while the backlog stamped before 2026-10-24 is
re-drained — decision 1.439 — and a dispatch input that keeps 3,000 too.

The workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import unittest

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "trakt_sync.yml"


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


class TraktSyncCadenceTest(unittest.TestCase):

    def setUp(self):
        self.text = WORKFLOW.read_text(encoding="utf-8")

    def test_the_schedule_is_daily_at_0300_utc(self):
        self.assertEqual(active_crons(self.text), ["0 3 * * *"])
        self.assertFalse([c for c in active_crons(self.text) if is_weekly(c)])

    def test_the_scan_bites_on_a_monday_fixture(self):
        # Positive control: the scan sees an active Monday cron, and ignores a commented one.
        monday = "on:\n  schedule:\n    - cron: '0 8 * * 1'  # Mondays\n"
        commented = "on:\n  schedule:\n    - cron: '0 3 * * *'\n    # - cron: '0 8 * * 1'\n"
        self.assertEqual([c for c in active_crons(monday) if is_weekly(c)], ["0 8 * * 1"])
        self.assertEqual(active_crons(commented), ["0 3 * * *"])

    def test_the_related_batch_is_3000_for_the_schedule_and_the_dispatch(self):
        self.assertRegex(self.text, r"related_batch:\n(?:.*\n){0,3}?\s+default: '3000'")
        self.assertIn("TRAKT_RELATED_BATCH: ${{ github.event.inputs.related_batch || '3000' }}", self.text)
        self.assertNotIn("'4000'", self.text)

    def test_the_job_has_room_for_two_requests_a_title(self):
        self.assertIn("timeout-minutes: 90", self.text)

    def test_the_file_says_why_and_until_when(self):
        head = self.text.splitlines()[0]
        self.assertTrue(head.startswith("#"), head)
        self.assertIn("decision 1.439", self.text.split("name:", 1)[0])
        self.assertIn("back to Mondays", self.text.split("name:", 1)[0])


if __name__ == "__main__":
    unittest.main()
