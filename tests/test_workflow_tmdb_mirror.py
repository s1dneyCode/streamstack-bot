"""
Unit tests for the «TMDb Mirror» workflow (RUN B5.8 · 0c; its schedule moved
by B5.9 · 0a). Since B5.9 it runs after every workflow that writes an image
field the mirror copies (workflow_run, types completed, by the writers'
`name:`), once a day as a fallback off the top of the hour and after the
nightly chain, and on dispatch with its inputs (time_budget_min, max_files,
workers) mapped to the script's env with the script's own defaults; one run at
a time; a 90-minute job around a budget a dispatch may raise to 80; Python
3.11, unbuffered; the two Supabase secrets by reference, on the step that runs
the script and nowhere else, and NOT TMDB_API_KEY (the image CDN needs no key).
Until B5.9 it ran hourly at :11.

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
WORKFLOWS = WORKFLOW.parent
RUN_LINE = "python -m bot.tmdb_mirror"
# The bot scripts that write an image field the mirror copies — media.backdrop_path,
# media.title_logo_url (a value, not the null a bare upsert_media writes) or
# media_credits.profile_path — as RUN B5.9 measured them (file:line in its report).
IMAGE_WRITERS = frozenset({"main", "enrich_new_titles", "backfill_backdrops", "backfill_title_logos",
                           "backfill_credits", "backfill_cast_photos", "backfill_cast_characters"})
# The house's daily crons the fallback must follow: every scheduled workflow but the two
# image jobs (Posters HD hourly, and this one).
NOT_THE_NIGHTLY_CHAIN = {"posters_hd.yml", "tmdb_mirror.yml"}


def _top_name(text: str) -> str | None:
    """A workflow file's top-level `name:`."""
    match = re.search(r"(?m)^name: (.+?)\s*$", text)
    return match.group(1) if match else None


def _workflow_names() -> dict[str, list[str]]:
    """Every workflow file in the repo by its top-level `name:`."""
    names = {}
    for path in sorted(WORKFLOWS.glob("*.yml")):
        names.setdefault(_top_name(path.read_text(encoding="utf-8")), []).append(path.name)
    return names


def _on_block(text: str) -> str:
    """The text of the `on:` block, up to the concurrency block."""
    return text.split("\non:\n", 1)[1].split("\nconcurrency:", 1)[0] if "\non:\n" in text else ""


def _triggers(text: str) -> list[str]:
    return re.findall(r"(?m)^  ([a-z_]+):", _on_block(text))


def _workflow_run(text: str) -> str:
    """The workflow_run trigger's own lines."""
    block = _on_block(text)
    if "\n  workflow_run:\n" not in "\n" + block:
        return ""
    after = ("\n" + block).split("\n  workflow_run:\n", 1)[1]
    return re.split(r"(?m)^  [a-z_]+:", after, maxsplit=1)[0]


def _listed_workflows(text: str) -> list[str]:
    """The names under workflow_run's `workflows:`, quotes stripped."""
    run = _workflow_run(text)
    listed = run.split("    workflows:\n", 1)[1] if "    workflows:\n" in run else ""
    return [m.strip().strip("'\"") for m in re.findall(r"(?m)^      - (.+?)\s*$", listed)]


def _daily(cron: str) -> tuple[int, int] | None:
    """(hour, minute) of a once-a-day cron (`M H * * *`), or None."""
    match = re.fullmatch(r"(\d{1,2}) (\d{1,2}) \* \* \*", cron.strip())
    return (int(match.group(2)), int(match.group(1))) if match else None


def _writer_workflows() -> set[str]:
    """The `name:` of every workflow in the repo that runs an image writer."""
    found = set()
    for path in WORKFLOWS.glob("*.yml"):
        text = path.read_text(encoding="utf-8")
        if set(re.findall(r"python -m bot\.([a-z_]+)", text)) & IMAGE_WRITERS:
            found.add(_top_name(text))
    return found


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

    # ── when it runs (B5.9) ───────────────────────────────────────────────

    def test_the_triggers_are_the_writers_one_daily_fallback_and_dispatch(self):
        self.assertEqual(sorted(_triggers(self.text)), ["schedule", "workflow_dispatch", "workflow_run"])
        self.assertEqual(len(active_crons(self.text)), 1)
        self.assertIsNotNone(_daily(active_crons(self.text)[0]), active_crons(self.text))
        # Positive control: B5.8's hourly cron is not a daily one.
        self.assertIsNone(_daily("11 * * * *"))

    def test_the_listed_writers_are_every_workflow_that_runs_an_image_writer(self):
        expected = _writer_workflows()
        self.assertEqual(sorted(_listed_workflows(self.text)), sorted(expected))
        # Positive control: what (a) found, and every writer module is a bot script.
        self.assertEqual(expected, {"Nightly Bot", "Nightly Enrichment", "Backfills", "Full Backfill — New Data",
                                    "Full Backfill — After Bulk Import"})
        for module in IMAGE_WRITERS:
            with self.subTest(module=module):
                self.assertTrue((WORKFLOWS.parent.parent / "bot" / f"{module}.py").exists())

    def test_each_listed_name_is_the_name_of_one_workflow_in_the_repo(self):
        names = _workflow_names()
        listed = _listed_workflows(self.text)
        self.assertTrue(listed)
        for name in listed:
            with self.subTest(name=name):
                self.assertEqual(len(names.get(name, [])), 1, names.get(name))
        # Positive control: a misspelt name names no workflow.
        self.assertNotIn("Nightly Enrichement", names)

    def test_workflow_run_fires_on_completed_whatever_the_conclusion(self):
        run = _workflow_run(self.text)
        self.assertRegex(run, r"(?m)^    types: \[completed\]$")
        self.assertEqual(re.findall(r"(?m)^    types: (.+)$", run), ["[completed]"])
        # Positive control: a branch filter or a conclusion filter would show up here.
        self.assertEqual(re.findall(r"(?m)^    ([a-z_-]+):", run), ["workflows", "types"])

    def test_the_fallback_runs_once_a_day_off_the_hour_after_the_nightly_chain(self):
        fallback = _daily(active_crons(self.text)[0]) if active_crons(self.text) else None
        self.assertIsNotNone(fallback, active_crons(self.text))
        hour, minute = fallback
        self.assertNotEqual(minute, 0)
        chain = []
        for path in WORKFLOWS.glob("*.yml"):
            if path.name not in NOT_THE_NIGHTLY_CHAIN:
                chain.extend(d for d in map(_daily, active_crons(path.read_text(encoding="utf-8"))) if d)
        self.assertIn((10, 0), chain)                          # Coverage Sweep, the chain's last
        self.assertGreater((hour, minute), max(chain))
        # Positive controls: the top of the hour and a time inside the chain fail.
        self.assertEqual(_daily("0 12 * * *")[1], 0)
        self.assertLess(_daily("23 9 * * *"), max(chain))

    def test_an_upstream_run_by_a_push_or_a_pull_request_does_not_sweep(self):
        # Nightly Bot also runs on push and pull_request, and then only its tests job runs.
        job_if = re.findall(r"(?m)^    if: (.+)$", self.text)
        self.assertEqual(job_if, ["github.event_name != 'workflow_run' || github.event.workflow_run.event == 'schedule' "
                                  "|| github.event.workflow_run.event == 'workflow_dispatch'"])
        self.assertNotIn("push", job_if[0])                    # positive control: only the two events that write

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
        # B5.9 rewrote this list: ":11" left with the hourly cron; the new schedule's why came in.
        for words in ("tmdb_image_mirror", "w1280", "w500", "w185", "copied=0", "one-year cache",
                      "first copy is complete", "3,300", "50 MB", "workflow_run", "once a day"):
            with self.subTest(words=words):
                self.assertIn(words, head)
        self.assertIn("\nname: TMDb Mirror\n", self.text)


if __name__ == "__main__":
    unittest.main()
