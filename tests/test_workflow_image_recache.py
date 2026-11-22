"""
Unit tests for the «Image Recache» workflow (RUN B5.10 · 0c): hourly at :53 —
off the top of the hour, away from Posters HD's :41 and TMDb Mirror's 12:23 —
plus dispatch with its inputs (prefixes, time_budget_min, max_objects,
workers, restart) mapped to the script's env with the script's own defaults;
one run at a time; a 90-minute job around a budget a dispatch may raise to
80; Python 3.11, unbuffered; the two Supabase secrets by reference, on the
step that runs the script and nowhere else, and NOT TMDB_API_KEY (the sweep
reads our own public URLs). It is not an image writer for TMDb Mirror: it
re-uploads existing keys and writes no field the mirror copies, so it is not
in the mirror's workflow_run list.

The pins are exact, not substrings — the shape of
tests/test_workflow_posters_hd.py, whose helpers this file reuses. The
workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import unittest

from bot import image_recache
from tests.test_workflow_posters_hd import active_crons, secrets_named, step_env, steps
from tests.test_workflow_tmdb_mirror import IMAGE_WRITERS, _listed_workflows, _triggers, _workflow_names

WORKFLOWS = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "image_recache.yml"
RUN_LINE = "python -m bot.image_recache"
INPUTS = (("prefixes", "posters/,seasons/,carousels/", "IMAGE_RECACHE_PREFIXES"),
          ("time_budget_min", "25", "IMAGE_RECACHE_TIME_BUDGET_MIN"),
          ("max_objects", "40000", "IMAGE_RECACHE_MAX_OBJECTS"),
          ("workers", "6", "IMAGE_RECACHE_WORKERS"),
          ("restart", "false", "IMAGE_RECACHE_RESTART"))


def _text() -> str:
    """The workflow's text, or '' while it does not exist (so a missing file
    fails these pins as assertions, not as an error)."""
    return WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.exists() else ""


def _minute(cron: str) -> int | None:
    match = re.fullmatch(r"(\d{1,2}) .+", cron.strip())
    return int(match.group(1)) if match else None


def _worst_case_minutes() -> float:
    """The nominal margin of a dispatch at the largest budget: the budget (the
    gate's 90 s sit inside it: the clock starts before), the last chunk a
    worker holds (CHUNK_PER_WORKER objects, each a fetch timeout and an upload
    timeout, one after another), and 3 minutes of setup before the budget's
    clock starts. Not an upper bound: requests' and httpx's timeouts limit
    each phase, not a whole request. The retry round starts only while the
    budget holds."""
    per_object = image_recache.FETCH_TIMEOUT_SECONDS + image_recache.UPLOAD_TIMEOUT_SECONDS
    return image_recache.MAX_IMAGE_RECACHE_TIME_BUDGET_MIN + image_recache.CHUNK_PER_WORKER * per_object / 60 + 3


class ImageRecacheWorkflowTest(unittest.TestCase):

    def setUp(self):
        self.text = _text()
        self.run_step = next((s for s in steps(self.text) if f"run: {RUN_LINE}" in s), "")

    def test_it_runs_hourly_at_53_and_on_dispatch_only(self):
        self.assertEqual(active_crons(self.text), ["53 * * * *"])
        self.assertEqual(sorted(_triggers(self.text)), ["schedule", "workflow_dispatch"])
        # Off the top of the hour, and off the other image jobs' minutes.
        others = [_minute(c) for name in ("posters_hd.yml", "tmdb_mirror.yml")
                  for c in active_crons((WORKFLOWS / name).read_text(encoding="utf-8"))]
        self.assertEqual(others, [41, 23])                                     # positive control: what they run at
        self.assertNotIn(_minute(active_crons(self.text)[0]) if active_crons(self.text) else None, others + [0, None])

    def test_the_five_inputs_reach_the_scripts_env_with_its_defaults(self):
        dispatch = self.text.split("\n  workflow_dispatch:\n    inputs:\n", 1)[-1].split("\nconcurrency:", 1)[0]
        for name, default, env in INPUTS:
            with self.subTest(input=name):
                self.assertRegex(dispatch, rf"(?m)^      {name}:\n        description: .+\n"
                                           rf"        required: false\n        default: '{re.escape(default)}'$")
                self.assertIn(f"{env}: ${{{{ github.event.inputs.{name} || '{default}' }}}}", self.run_step)
                self.assertEqual(self.text.count(f"{name}:"), 1)
        # …and those defaults are the script's, for a run started by hand.
        self.assertEqual((image_recache.DEFAULT_IMAGE_RECACHE_PREFIXES,
                          image_recache.DEFAULT_IMAGE_RECACHE_TIME_BUDGET_MIN,
                          image_recache.DEFAULT_IMAGE_RECACHE_MAX_OBJECTS,
                          image_recache.DEFAULT_IMAGE_RECACHE_WORKERS),
                         ("posters/,seasons/,carousels/", 25, 40000, 6))

    def test_one_run_at_a_time(self):
        block = r"(?m)^concurrency:\n  group: \$\{\{ github\.workflow \}\}\n  cancel-in-progress: false$"
        self.assertRegex(self.text, block)
        # Positive control: per ref does not count.
        self.assertNotRegex("concurrency:\n  group: ${{ github.workflow }}-${{ github.ref }}\n  cancel-in-progress: false",
                            block)

    def test_the_job_holds_the_largest_budget_a_dispatch_may_ask(self):
        timeouts = re.findall(r"(?m)^(\s*)timeout-minutes:\s*(\d+)\s*$", self.text)
        self.assertEqual(timeouts, [("    ", "90")])
        self.assertEqual(image_recache.MAX_IMAGE_RECACHE_TIME_BUDGET_MIN, 80)
        self.assertLessEqual(_worst_case_minutes(), int(timeouts[0][1]) if timeouts else 0)
        self.assertGreater(_worst_case_minutes(), 85)                          # positive control: 85 would not hold it
        self.assertEqual(re.findall(r"(?m)^\s+run: (.+?)\s*$", self.text), ["pip install -r requirements.txt", RUN_LINE])
        self.assertIn("python-version: '3.11'", self.text)
        self.assertIn("PYTHONUNBUFFERED: '1'", self.run_step)

    def test_the_two_supabase_secrets_on_the_scripts_step_only_and_no_tmdb_key(self):
        self.assertEqual(secrets_named(self.text), ["SUPABASE_KEY", "SUPABASE_URL"])
        self.assertNotIn("TMDB_API_KEY", self.text)
        self.assertEqual(secrets_named(self.run_step), ["SUPABASE_KEY", "SUPABASE_URL"])
        self.assertEqual(self.text.count("secrets."), self.run_step.count("secrets."))
        self.assertEqual(sorted(step_env(self.run_step)),
                         sorted(["PYTHONUNBUFFERED", "SUPABASE_URL", "SUPABASE_KEY"] + [env for _, _, env in INPUTS]))
        for key in ("SUPABASE_URL", "SUPABASE_KEY"):
            with self.subTest(key=key):
                self.assertIn(f"{key}: ${{{{ secrets.{key} }}}}", self.run_step)   # positive control
        self.assertEqual(len(re.findall(r"(?m)^\s*env:", self.text)), 1)

    def test_it_is_not_an_image_writer_for_the_mirror(self):
        # RUN B5.10 · (c): the mirror's IMAGE_WRITERS is a fixed list, and the sweep is not on it.
        self.assertNotIn("image_recache", IMAGE_WRITERS)
        mirror = (WORKFLOWS / "tmdb_mirror.yml").read_text(encoding="utf-8")
        self.assertNotIn("Image Recache", _listed_workflows(mirror))
        # Positive controls: the name is this workflow's, once, and the mirror's list is read.
        self.assertEqual(_workflow_names().get("Image Recache"), ["image_recache.yml"])
        self.assertIn("Nightly Enrichment", _listed_workflows(mirror))

    def test_the_file_says_what_it_is_for(self):
        head = self.text.split("name:", 1)[0]
        self.assertTrue(head.startswith("#"), head[:40])
        for words in ("no-cache", "7 days", "a year", "625,395", "48.5 GB", "33.2 GB", "10.6 GB", "4.7 GB",
                      "`prefixes`", "#1109", "every 5 s for up to 90 s", "image_recache_cursor", "≈ 20 runs",
                      "drain_done=1", ":53", "image_cache_page", "22/11"):
            with self.subTest(words=words):
                self.assertIn(words, head)
        self.assertIn("\nname: Image Recache\n", self.text)


if __name__ == "__main__":
    unittest.main()
