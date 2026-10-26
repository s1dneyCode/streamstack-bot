"""
Unit tests for the «Backfill IMDb ids» workflow (RUN B5.3 · 0b): dispatch
only — no schedule —, its two inputs (batch, time_budget_min) mapped to the
script's env with the script's own defaults, a 90-minute job around the
80-minute budget, Python 3.11, and the secrets by reference only.

The workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import unittest

from bot import backfill_imdb_ids

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "backfill_imdb_ids.yml"


def _text() -> str:
    """The workflow's text, or '' while it does not exist (so a missing file
    fails these pins as assertions, not as an error)."""
    return WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.exists() else ""


class BackfillImdbIdsWorkflowTest(unittest.TestCase):

    def setUp(self):
        self.text = _text()

    def test_it_runs_on_dispatch_only(self):
        self.assertIn("workflow_dispatch:", self.text)
        self.assertNotIn("schedule:", self.text)
        self.assertNotRegex(self.text, r"(?m)^\s*-\s*cron:")

    def test_the_two_inputs_reach_the_scripts_env_with_its_defaults(self):
        self.assertRegex(self.text, r"batch:\n(?:.*\n){0,3}?\s+default: '20000'")
        self.assertRegex(self.text, r"time_budget_min:\n(?:.*\n){0,3}?\s+default: '80'")
        self.assertIn("IMDB_BACKFILL_BATCH: ${{ github.event.inputs.batch || '20000' }}", self.text)
        self.assertIn("IMDB_TIME_BUDGET_MIN: ${{ github.event.inputs.time_budget_min || '80' }}", self.text)
        # …and those defaults are the script's, for a run started by hand.
        self.assertEqual(backfill_imdb_ids.DEFAULT_IMDB_BACKFILL_BATCH, 20000)
        self.assertEqual(backfill_imdb_ids.DEFAULT_IMDB_TIME_BUDGET_MIN, 80)

    def test_the_job_gives_the_budget_room_and_runs_the_script(self):
        timeout = re.search(r"timeout-minutes:\s*(\d+)", self.text)
        self.assertIsNotNone(timeout)
        self.assertEqual(int(timeout.group(1)), 90)
        self.assertLess(backfill_imdb_ids.DEFAULT_IMDB_TIME_BUDGET_MIN, int(timeout.group(1)))
        self.assertIn("run: python -m bot.backfill_imdb_ids", self.text)
        self.assertIn("python-version: '3.11'", self.text)
        self.assertIn("run: pip install -r requirements.txt", self.text)

    def test_the_secrets_go_by_reference_only(self):
        for key in ("TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"):
            with self.subTest(key=key):
                self.assertIn(f"{key}: ${{{{ secrets.{key} }}}}", self.text)
        values = re.findall(r"(?m)^\s+(?:TMDB_API_KEY|SUPABASE_URL|SUPABASE_KEY):\s*(.+)$", self.text)
        self.assertTrue(values)
        self.assertTrue(all(v.startswith("${{ secrets.") for v in values), values)

    def test_two_dispatches_never_drain_the_queue_at_once(self):
        self.assertIn("group: ${{ github.workflow }}", self.text)
        self.assertIn("cancel-in-progress: false", self.text)

    def test_the_file_says_what_it_is_for(self):
        head = self.text.split("name:", 1)[0]
        self.assertTrue(head.startswith("#"), head[:40])
        self.assertIn("20261026120000", head)                  # the migration it needs first
        self.assertIn("name: Backfill IMDb ids", self.text)


if __name__ == "__main__":
    unittest.main()
