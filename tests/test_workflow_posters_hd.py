"""
Unit tests for the «Posters HD» workflow (RUN B5.7 · 0b): hourly at :41 —
off the top of the hour that GitHub delays, the reason Trakt Sync runs at
02:17 — plus dispatch with its inputs (batch, time_budget_min, and since
B5.8 refresh_existing) mapped to the script's env with the script's own
defaults; one run at a time; a
30-minute job around the 20-minute budget; Python 3.11, unbuffered; the two
Supabase secrets by reference, on the step that runs the script and nowhere
else, and NOT TMDB_API_KEY (TMDb's image CDN needs no key: least privilege).

The pins are exact, not substrings: the workflow is the whole of what the job
is handed, so an extra secret, an extra step, a `|| true` or a commented-out
block must fail them.

The workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import unittest

from bot import posters_hd

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "posters_hd.yml"
RUN_LINE = "python -m bot.posters_hd"


def _text() -> str:
    """The workflow's text, or '' while it does not exist (so a missing file
    fails these pins as assertions, not as an error)."""
    return WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.exists() else ""


def active_crons(text: str) -> list[str]:
    """Every cron expression on a line that is not a comment, quoted or not."""
    crons = []
    for line in text.splitlines():
        match = re.search(r"-\s*cron:\s*(['\"]?)(.+?)\1\s*$", line.split("#", 1)[0])
        if match:
            crons.append(match.group(2).strip())
    return crons


def steps(text: str) -> list[str]:
    """The job's steps, each as its own text."""
    body = text.split("\n    steps:\n", 1)[1] if "\n    steps:\n" in text else ""
    return [chunk for chunk in re.split(r"(?m)^      - ", body) if chunk.strip()]


def step_env(step: str) -> list[str]:
    """The names under a step's `env:` — comments and blank lines skipped,
    so neither ends the block."""
    names, indent = [], None
    for line in step.splitlines():
        code = line.split("#", 1)[0].rstrip()
        if not code.strip():
            continue
        lead = len(code) - len(code.lstrip())
        if re.fullmatch(r"\s*env:", code):
            indent = lead
            continue
        if indent is not None:
            if lead <= indent:
                indent = None
            else:
                names.append(code.strip().split(":", 1)[0])
    return names


def secrets_named(text: str) -> list[str]:
    """Every secret the file references, anywhere."""
    return sorted(set(re.findall(r"secrets\.(\w+)", text)))


class PostersHdWorkflowTest(unittest.TestCase):

    def setUp(self):
        self.text = _text()
        self.run_step = next((s for s in steps(self.text) if f"run: {RUN_LINE}" in s), "")

    def test_it_runs_hourly_at_41_and_on_dispatch(self):
        self.assertEqual(active_crons(self.text), ["41 * * * *"])
        self.assertIn("\n  workflow_dispatch:\n", self.text)
        # Positive control for the scan: any quoting is read, a commented cron is not.
        self.assertEqual(active_crons("    - cron: \"0 * * * *\"\n    - cron: 5 * * * *\n    # - cron: '1 * * * *'\n"),
                         ["0 * * * *", "5 * * * *"])

    def test_the_two_inputs_reach_the_scripts_env_with_its_defaults(self):
        self.assertRegex(self.text, r"batch:\n(?:.*\n){0,3}?\s+default: '300'")
        self.assertRegex(self.text, r"time_budget_min:\n(?:.*\n){0,3}?\s+default: '20'")
        self.assertIn("POSTERS_HD_BATCH: ${{ github.event.inputs.batch || '300' }}", self.run_step)
        self.assertIn("POSTERS_HD_TIME_BUDGET_MIN: ${{ github.event.inputs.time_budget_min || '20' }}", self.run_step)
        # …and those defaults are the script's, for a run started by hand.
        self.assertEqual(posters_hd.DEFAULT_POSTERS_HD_BATCH, 300)
        self.assertEqual(posters_hd.DEFAULT_POSTERS_HD_TIME_BUDGET_MIN, 20)

    def test_refresh_existing_is_a_dispatch_input_off_by_default(self):
        # B5.8: a dispatch with it on makes every made title pending once (the long cache reaches
        # the 28 posters made before it); the schedule never sets it.
        self.assertRegex(self.text, r"refresh_existing:\n(?:.*\n){0,3}?\s+default: 'false'")
        self.assertIn("POSTERS_HD_REFRESH_EXISTING: ${{ github.event.inputs.refresh_existing || 'false' }}",
                      self.run_step)
        # It is one of workflow_dispatch's inputs, at their indent (six spaces), not anywhere else.
        dispatch = self.text.split("\n  workflow_dispatch:\n    inputs:\n", 1)[-1].split("\nconcurrency:", 1)[0]
        self.assertRegex(dispatch, r"(?m)^      refresh_existing:\n        description: .+\n"
                                   r"        required: false\n        default: 'false'$")
        self.assertEqual(self.text.count("refresh_existing:"), 1)

    def test_one_run_at_a_time(self):
        block = r"(?m)^concurrency:\n  group: \$\{\{ github\.workflow \}\}\n  cancel-in-progress: false$"
        self.assertRegex(self.text, block)
        # Positive control: the same block commented out, or per ref, does not count.
        self.assertNotRegex("# concurrency:\n#   group: ${{ github.workflow }}\n#   cancel-in-progress: false", block)
        self.assertNotRegex("concurrency:\n  group: ${{ github.workflow }}-${{ github.ref }}\n  cancel-in-progress: false",
                            block)

    def test_the_job_gives_the_budget_room_and_runs_the_script_alone(self):
        # One timeout, on the job (four spaces in): a step's would leave the job at GitHub's 360.
        self.assertEqual(re.findall(r"(?m)^(\s*)timeout-minutes:\s*(\d+)\s*$", self.text), [("    ", "30")])
        self.assertLess(posters_hd.DEFAULT_POSTERS_HD_TIME_BUDGET_MIN, 30)
        # Exactly two commands: the install and the script, with nothing after it.
        self.assertEqual(re.findall(r"(?m)^\s+run: (.+?)\s*$", self.text), ["pip install -r requirements.txt", RUN_LINE])
        self.assertIn("python-version: '3.11'", self.text)
        self.assertIn("PYTHONUNBUFFERED: '1'", self.run_step)

    def test_the_two_supabase_secrets_on_the_scripts_step_only_and_no_tmdb_key(self):
        self.assertEqual(secrets_named(self.text), ["SUPABASE_KEY", "SUPABASE_URL"])
        self.assertNotIn("TMDB_API_KEY", self.text)
        # The step that runs the script holds every secret reference, and this env exactly.
        self.assertEqual(secrets_named(self.run_step), ["SUPABASE_KEY", "SUPABASE_URL"])
        self.assertEqual(self.text.count("secrets."), self.run_step.count("secrets."))
        # B5.8 added POSTERS_HD_REFRESH_EXISTING to this exact set.
        self.assertEqual(sorted(step_env(self.run_step)),
                         ["POSTERS_HD_BATCH", "POSTERS_HD_REFRESH_EXISTING", "POSTERS_HD_TIME_BUDGET_MIN",
                          "PYTHONUNBUFFERED", "SUPABASE_KEY", "SUPABASE_URL"])
        for key in ("SUPABASE_URL", "SUPABASE_KEY"):
            with self.subTest(key=key):
                self.assertIn(f"{key}: ${{{{ secrets.{key} }}}}", self.run_step)   # positive control
        # One env block in the whole file: no job-level or flow-style env beside it.
        self.assertEqual(len(re.findall(r"(?m)^\s*env:", self.text)), 1)
        # Positive controls for the scans: a key after a comment is read, any secret is seen.
        self.assertEqual(step_env("name: x\n  env:\n# note\n    A: 1\n    B: 2\n  run: y\n"), ["A", "B"])
        self.assertEqual(secrets_named("run: X=${{ secrets.TMDB_API_KEY }} y"), ["TMDB_API_KEY"])

    def test_the_file_says_what_it_is_for(self):
        head = self.text.split("name:", 1)[0]
        self.assertTrue(head.startswith("#"), head[:40])
        for words in ("20261027200000", "poster_hd_url", ":41", "pending=0"):
            with self.subTest(words=words):
                self.assertIn(words, head)
        self.assertIn("\nname: Posters HD\n", self.text)


if __name__ == "__main__":
    unittest.main()
