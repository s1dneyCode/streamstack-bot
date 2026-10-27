"""
Unit tests for the «TMDb Mirror» workflow (RUN B5.8 · 0c): hourly at :11 —
off the top of the hour that GitHub delays and away from Posters HD's :41 —
plus dispatch with its inputs (time_budget_min, max_files, workers) mapped to
the script's env with the script's own defaults; one run at a time; a
90-minute job around a budget a dispatch may raise to 80; Python 3.11,
unbuffered; the two Supabase secrets by reference, on the step that runs the
script and nowhere else, and NOT TMDB_API_KEY (the image CDN needs no key).

The pins are exact, not substrings — the shape of
tests/test_workflow_posters_hd.py, whose helpers this file reuses. The
workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import unittest

from bot import posters_hd, tmdb_mirror
from tests.test_workflow_posters_hd import active_crons, secrets_named, step_env, steps

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "tmdb_mirror.yml"
RUN_LINE = "python -m bot.tmdb_mirror"


def _text() -> str:
    """The workflow's text, or '' while it does not exist (so a missing file
    fails these pins as assertions, not as an error)."""
    return WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.exists() else ""


def _worst_case_minutes() -> float:
    """The nominal margin of a dispatch at the largest budget: the budget, the
    last chunk a worker holds (CHUNK_PER_WORKER files, each a fetch timeout
    and an upload timeout, one after another), and 3 minutes of setup before
    the budget's clock starts. Not an upper bound: requests' and httpx's
    timeouts limit each phase (connect, each read gap), not a whole request,
    and the page's ledger write after that chunk is not counted. A run past
    the job's 90 minutes is cancelled with no SUMMARY, and its page's files
    are uploaded again next run, harmlessly. UPLOAD_TIMEOUT_SECONDS restates
    storage3's default; the breaker stops a run whose files keep failing."""
    per_file = posters_hd.FETCH_TIMEOUT_SECONDS + tmdb_mirror.UPLOAD_TIMEOUT_SECONDS
    return tmdb_mirror.MAX_TMDB_MIRROR_TIME_BUDGET_MIN + tmdb_mirror.CHUNK_PER_WORKER * per_file / 60 + 3


class TmdbMirrorWorkflowTest(unittest.TestCase):

    def setUp(self):
        self.text = _text()
        self.run_step = next((s for s in steps(self.text) if f"run: {RUN_LINE}" in s), "")

    def test_it_runs_hourly_at_11_and_on_dispatch(self):
        self.assertEqual(active_crons(self.text), ["11 * * * *"])
        self.assertIn("\n  workflow_dispatch:\n", self.text)
        # Away from Posters HD's :41 (positive control: that one is still at :41).
        posters = (WORKFLOW.parent / "posters_hd.yml").read_text(encoding="utf-8")
        self.assertEqual(active_crons(posters), ["41 * * * *"])

    def test_the_three_inputs_reach_the_scripts_env_with_its_defaults(self):
        dispatch = self.text.split("\n  workflow_dispatch:\n    inputs:\n", 1)[-1].split("\nconcurrency:", 1)[0]
        for name, default in (("time_budget_min", "25"), ("max_files", "20000"), ("workers", "6")):
            with self.subTest(input=name):
                self.assertRegex(dispatch, rf"(?m)^      {name}:\n        description: .+\n"
                                           rf"        required: false\n        default: '{default}'$")
        self.assertIn("TMDB_MIRROR_TIME_BUDGET_MIN: ${{ github.event.inputs.time_budget_min || '25' }}", self.run_step)
        self.assertIn("TMDB_MIRROR_MAX_FILES: ${{ github.event.inputs.max_files || '20000' }}", self.run_step)
        self.assertIn("TMDB_MIRROR_WORKERS: ${{ github.event.inputs.workers || '6' }}", self.run_step)
        # …and those defaults are the script's, for a run started by hand.
        self.assertEqual((tmdb_mirror.DEFAULT_TMDB_MIRROR_TIME_BUDGET_MIN, tmdb_mirror.DEFAULT_TMDB_MIRROR_MAX_FILES,
                          tmdb_mirror.DEFAULT_TMDB_MIRROR_WORKERS), (25, 20000, 6))

    def test_one_run_at_a_time(self):
        block = r"(?m)^concurrency:\n  group: \$\{\{ github\.workflow \}\}\n  cancel-in-progress: false$"
        self.assertRegex(self.text, block)

    def test_the_job_holds_the_largest_budget_a_dispatch_may_ask(self):
        timeouts = re.findall(r"(?m)^(\s*)timeout-minutes:\s*(\d+)\s*$", self.text)
        self.assertEqual(timeouts, [("    ", "90")])
        # B5.6's margin, from the code's own numbers: the largest budget, then the last chunk in
        # flight — every file a fetch timeout and an upload timeout — and 3 minutes of setup.
        self.assertEqual(tmdb_mirror.MAX_TMDB_MIRROR_TIME_BUDGET_MIN, 80)
        self.assertLessEqual(_worst_case_minutes(), int(timeouts[0][1]))
        self.assertGreater(_worst_case_minutes(), 85)          # positive control: an 85-minute job would not hold it
        self.assertEqual(re.findall(r"(?m)^\s+run: (.+?)\s*$", self.text), ["pip install -r requirements.txt", RUN_LINE])
        self.assertIn("python-version: '3.11'", self.text)
        self.assertIn("PYTHONUNBUFFERED: '1'", self.run_step)

    def test_the_two_supabase_secrets_on_the_scripts_step_only_and_no_tmdb_key(self):
        self.assertEqual(secrets_named(self.text), ["SUPABASE_KEY", "SUPABASE_URL"])
        self.assertNotIn("TMDB_API_KEY", self.text)
        self.assertEqual(secrets_named(self.run_step), ["SUPABASE_KEY", "SUPABASE_URL"])
        self.assertEqual(self.text.count("secrets."), self.run_step.count("secrets."))
        self.assertEqual(sorted(step_env(self.run_step)),
                         ["PYTHONUNBUFFERED", "SUPABASE_KEY", "SUPABASE_URL", "TMDB_MIRROR_MAX_FILES",
                          "TMDB_MIRROR_TIME_BUDGET_MIN", "TMDB_MIRROR_WORKERS"])
        for key in ("SUPABASE_URL", "SUPABASE_KEY"):
            with self.subTest(key=key):
                self.assertIn(f"{key}: ${{{{ secrets.{key} }}}}", self.run_step)   # positive control
        self.assertEqual(len(re.findall(r"(?m)^\s*env:", self.text)), 1)

    def test_the_file_says_what_it_is_for(self):
        head = self.text.split("name:", 1)[0]
        self.assertTrue(head.startswith("#"), head[:40])
        for words in ("tmdb_image_mirror", "w1280", "w500", "w185", ":11", "copied=0", "one-year cache"):
            with self.subTest(words=words):
                self.assertIn(words, head)
        self.assertIn("\nname: TMDb Mirror\n", self.text)


if __name__ == "__main__":
    unittest.main()
