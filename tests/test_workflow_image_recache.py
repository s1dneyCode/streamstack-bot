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

RUN B5.10.1 · 0b: the drain relaunches itself. The workflow may read its code
and dispatch itself (permissions contents: read, actions: write, nothing
else); a sixth input, chain_left, defaults to '0' (a dispatch is one run);
the script's step is `recache`, and a last step relaunches the workflow when
that step was green and its budget ended with the drain not done — with the
run's own knobs, restart 'false' always and chain_left one less (48 for the
hourly runs) — unless one of four reasons, in order, says not to. It uses the
workflow's own token, never a secret of the repo.

The pins are exact, not substrings — the shape of
tests/test_workflow_posters_hd.py, whose helpers this file reuses. The
workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import shlex
import unittest

from bot import image_recache
from tests.test_workflow_posters_hd import active_crons, secrets_named, step_env, steps
from tests.test_workflow_tmdb_mirror import IMAGE_WRITERS, _listed_workflows, _on_block, _triggers, _workflow_names

WORKFLOWS = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "image_recache.yml"
RUN_LINE = "python -m bot.image_recache"
INPUTS = (("prefixes", "posters/,seasons/,carousels/", "IMAGE_RECACHE_PREFIXES"),
          ("time_budget_min", "25", "IMAGE_RECACHE_TIME_BUDGET_MIN"),
          ("max_objects", "40000", "IMAGE_RECACHE_MAX_OBJECTS"),
          ("workers", "6", "IMAGE_RECACHE_WORKERS"),
          ("restart", "false", "IMAGE_RECACHE_RESTART"))
# B5.10.1 · 0b: the relaunch.
SCRIPT_STEP = "Re-cache images"
RELAUNCH_STEP = "Relaunch while the drain is not done"
RELAUNCH_IF = "success() && steps.recache.outputs.budget_hit == '1' && steps.recache.outputs.drain_done == '0'"
RELAUNCH_ENV = ("GH_TOKEN", "GH_REPO", "CHAIN_LEFT", "PREFIXES", "BUDGET_MIN", "MAX_OBJECTS", "WORKERS", "RETAGGED",
                "REFUSED")
KNOBS = (("PREFIXES", "prefixes"), ("BUDGET_MIN", "time_budget_min"), ("MAX_OBJECTS", "max_objects"),
         ("WORKERS", "workers"))
HOP_INPUTS = {"prefixes": "$PREFIXES", "time_budget_min": "$BUDGET_MIN", "max_objects": "$MAX_OBJECTS",
              "workers": "$WORKERS", "restart": "false", "chain_left": "$((CHAIN_LEFT - 1))"}
NO_RELAUNCH = ("is not a whole number", "chain exhausted; the hourly run takes it from here",
               "this hop refused as much as it retagged; the hourly run tries again",
               "a run is already waiting; it takes the drain from here")
WAITING_QUERY = ("gh run list --workflow image_recache.yml --limit 20 --json status --jq '[.[] | select("
                 '.status == "queued" or .status == "pending" or .status == "waiting" or .status == "requested"'
                 ")] | length'")
# The four guards, each the condition of one reason above, in the same order.
GUARDS = ('if ! [[ "$CHAIN_LEFT" =~ ^(0|[1-9][0-9]*)$ ]]; then', "if (( CHAIN_LEFT == 0 )); then",
          "if (( RETAGGED <= REFUSED )); then", 'if [ "$waiting" != "0" ]; then')
RELAUNCH_COMMAND = ('gh workflow run image_recache.yml --ref "$GITHUB_REF_NAME" -f "prefixes=$PREFIXES" '
                    '-f "time_budget_min=$BUDGET_MIN" -f "max_objects=$MAX_OBJECTS" -f "workers=$WORKERS" '
                    '-f restart=false -f "chain_left=$((CHAIN_LEFT - 1))"')
# The shell without its echo lines: every command and condition, exactly, in order.
SKELETON = ["set -euo pipefail",
            GUARDS[0], "  exit 0", "fi",
            GUARDS[1], "  exit 0", "fi",
            GUARDS[2], "  exit 0", "fi",
            f"waiting=$({WAITING_QUERY})",
            GUARDS[3], "  exit 0", "fi",
            RELAUNCH_COMMAND]


def _skeleton(shell: list[str]) -> list[str]:
    """A shell block's lines without its echo lines and blank lines."""
    return [line for line in shell if line.strip() and not line.strip().startswith("echo ")]


def _echoes(shell: list[str]) -> list[str]:
    return [line.strip() for line in shell if line.strip().startswith("echo ")]


def _step_named(text: str, name: str) -> str:
    """The step whose `name:` is *name*, as its own text, or ''."""
    return next((s for s in steps(text) if s.startswith(f"name: {name}\n")), "")


def _step_names(text: str) -> list[str]:
    return [s.split("\n", 1)[0].removeprefix("name: ") for s in steps(text)]


def _step_if(step: str) -> str | None:
    """A step's one `if:` condition (eight spaces in), or None."""
    found = re.findall(r"(?m)^        if: (.+?)\s*$", step)
    return found[0] if len(found) == 1 else None


def _guards_the_chain(condition: str | None) -> bool:
    """True when an `if:` runs a step only after a green script step whose
    budget ended with the drain not done: all three clauses, joined by &&,
    and no || that could open it."""
    clauses = {c.strip() for c in (condition or "").split("&&")}
    wanted = {"success()", "steps.recache.outputs.budget_hit == '1'", "steps.recache.outputs.drain_done == '0'"}
    return wanted <= clauses and "||" not in (condition or "")


def _shell(step: str) -> list[str]:
    """The lines of a step's `run: |` block, without its ten-space indent."""
    if "\n        run: |\n" not in step:
        return []
    lines = []
    for line in step.split("\n        run: |\n", 1)[1].splitlines():
        if line.strip() and not line.startswith("          "):
            break
        lines.append(line[10:])
    return lines


def _dispatch(command: str | None):
    """(workflow, ref, {input: value}) of a `gh workflow run` line, or None
    when it is not one, passes a flag other than --ref and -f, or one input
    twice."""
    try:
        words = shlex.split(command or "")
    except ValueError:
        return None
    if words[:3] != ["gh", "workflow", "run"] or len(words) < 4:
        return None
    workflow, ref, inputs, rest = words[3], None, {}, words[4:]
    while rest:
        if len(rest) < 2 or rest[0] not in ("--ref", "-f"):
            return None
        flag, value, rest = rest[0], rest[1], rest[2:]
        if flag == "--ref":
            ref = value
        elif "=" not in value or value.split("=", 1)[0] in inputs:
            return None
        else:
            inputs[value.split("=", 1)[0]] = value.split("=", 1)[1]
    return workflow, ref, inputs


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
        self.relaunch = _step_named(self.text, RELAUNCH_STEP)
        self.shell = _shell(self.relaunch)

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
        # B5.10.1 · 0b: a third command, the relaunch step's block — B5.10 pinned exactly the first two.
        self.assertEqual(re.findall(r"(?m)^\s+run: (.+?)\s*$", self.text),
                         ["pip install -r requirements.txt", RUN_LINE, "|"])
        self.assertIn("\n        run: |\n", self.relaunch)                     # the block is the relaunch's
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
        # B5.10.1 · 0b: two env blocks, the script's and the relaunch's — B5.10 pinned one.
        self.assertEqual(len(re.findall(r"(?m)^\s*env:", self.text)), 2)
        self.assertEqual([len(re.findall(r"(?m)^        env:$", s)) for s in (self.run_step, self.relaunch)], [1, 1])

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

    # ── B5.10.1 · 0b: the drain relaunches itself ───────────────────────

    def test_the_workflow_may_read_its_code_and_dispatch_itself_and_nothing_else(self):
        grants = r"(?m)^permissions:\n((?:  .+\n)*)"
        self.assertEqual(re.findall(grants, self.text), ["  contents: read\n  actions: write\n"])
        self.assertEqual(self.text.count("permissions:"), 1)                  # no job-level one beside it
        # At the top level, after one-run-at-a-time and before the jobs: not inside `on:`, whose keys are triggers.
        self.assertLess(self.text.find("\nconcurrency:\n"), self.text.find("\npermissions:\n"))
        self.assertLess(self.text.find("\npermissions:\n"), self.text.find("\njobs:\n"))
        self.assertNotIn("permissions", _on_block(self.text))
        # Positive control: a third grant, or a blanket one, reads differently.
        self.assertEqual(re.findall(grants, "permissions:\n  contents: read\n  actions: write\n  id-token: write\n"),
                         ["  contents: read\n  actions: write\n  id-token: write\n"])
        self.assertEqual(re.findall(grants, "permissions: write-all\n"), [])

    def test_chain_left_is_a_sixth_input_that_keeps_a_dispatch_one_run(self):
        dispatch = self.text.split("\n  workflow_dispatch:\n    inputs:\n", 1)[-1].split("\nconcurrency:", 1)[0]
        self.assertRegex(dispatch, r"(?m)^      chain_left:\n"
                                   r"        description: \"Hops this run may add after itself while its budget ends "
                                   r"with the drain not done \('0' = this run only; the hourly runs use 48\)\"\n"
                                   r"        required: false\n        default: '0'$")
        self.assertEqual(self.text.count("chain_left:"), 1)
        self.assertEqual(re.findall(r"(?m)^      ([a-z_]+):$", dispatch),
                         [name for name, _, _ in INPUTS] + ["chain_left"])
        # The hourly runs, which have no inputs, chain with 48; the script never reads it.
        self.assertIn("CHAIN_LEFT: ${{ github.event.inputs.chain_left || '48' }}", self.relaunch)
        self.assertNotIn("chain_left", self.run_step)
        self.assertTrue(self.relaunch)                                         # positive control: the step is there

    def test_the_scripts_step_is_recache_and_the_relaunch_comes_after_it_last(self):
        self.assertRegex(self.run_step, r"(?m)^        id: recache$")
        self.assertEqual(re.findall(r"(?m)^\s+id: (.+?)\s*$", self.text), ["recache"])
        self.assertEqual(_step_names(self.text)[-2:], [SCRIPT_STEP, RELAUNCH_STEP])
        self.assertTrue(self.run_step.startswith(f"name: {SCRIPT_STEP}\n"))     # positive control: the script's step

    def test_the_relaunch_runs_only_after_a_green_run_whose_budget_ended_before_the_drain(self):
        self.assertEqual(_step_if(self.relaunch), RELAUNCH_IF)
        self.assertTrue(_guards_the_chain(_step_if(self.relaunch)))
        self.assertEqual(len(re.findall(r"(?m)^\s+if: ", self.text)), 1)          # no other if: anywhere
        # Negatives beside it: without the drain_done clause, without success(), or opened by ||.
        self.assertFalse(_guards_the_chain("success() && steps.recache.outputs.budget_hit == '1'"))
        self.assertFalse(_guards_the_chain("steps.recache.outputs.budget_hit == '1' && "
                                           "steps.recache.outputs.drain_done == '0'"))
        self.assertFalse(_guards_the_chain(RELAUNCH_IF + " || always()"))
        self.assertFalse(_guards_the_chain(None))

    def test_the_relaunch_carries_the_runs_knobs_and_the_workflows_own_token(self):
        self.assertEqual(sorted(step_env(self.relaunch)), sorted(RELAUNCH_ENV))
        for line in ("GH_TOKEN: ${{ github.token }}", "GH_REPO: ${{ github.repository }}",
                     "RETAGGED: ${{ steps.recache.outputs.retagged }}", "REFUSED: ${{ steps.recache.outputs.refused }}"):
            with self.subTest(line=line):
                self.assertIn(line, self.relaunch)
        script_env = {name: env for name, _, env in INPUTS}
        defaults = {name: default for name, default, _ in INPUTS}
        for env, name in KNOBS:
            with self.subTest(knob=name):                                      # a hop carries its parent's knobs
                fallback = f"${{{{ github.event.inputs.{name} || '{defaults[name]}' }}}}"
                self.assertIn(f"{env}: {fallback}", self.relaunch)
                self.assertIn(f"{script_env[name]}: {fallback}", self.run_step)   # the same as the script's
        # Never a secret of the repo, and restart never carried.
        self.assertNotIn("secrets.", self.relaunch)
        self.assertEqual(secrets_named(self.relaunch), [])
        self.assertNotIn("inputs.restart", self.relaunch)
        self.assertIn("secrets.", self.run_step)                               # positive control: the scan sees one

    def test_the_relaunch_dispatches_this_workflow_with_restart_false_and_one_hop_less(self):
        commands = [line.strip() for line in self.shell if line.strip().startswith("gh workflow run ")]
        self.assertEqual(len(commands), 1, commands)
        self.assertEqual(_dispatch(commands[0] if commands else None),
                         ("image_recache.yml", "$GITHUB_REF_NAME", HOP_INPUTS))
        # …and the line itself, quoting included (shlex reads quoted and bare words alike).
        self.assertEqual(commands, [RELAUNCH_COMMAND])
        unquoted = RELAUNCH_COMMAND.replace('"prefixes=$PREFIXES"', "prefixes=$PREFIXES")
        self.assertNotEqual(unquoted, RELAUNCH_COMMAND)
        self.assertEqual(_dispatch(unquoted), _dispatch(RELAUNCH_COMMAND))   # why both: shlex drops the quotes
        # Every input a hop passes is one the workflow declares (gh refuses any other).
        dispatch = self.text.split("\n  workflow_dispatch:\n    inputs:\n", 1)[-1].split("\nconcurrency:", 1)[0]
        self.assertEqual(sorted(HOP_INPUTS), sorted(re.findall(r"(?m)^      ([a-z_]+):$", dispatch)))
        # Negatives beside it: the probes' shapes read differently.
        good = commands[0] if commands else ""
        for bad in (good.replace("restart=false", "restart=$RESTART"),
                    good.replace('"chain_left=$((CHAIN_LEFT - 1))"', '"chain_left=$CHAIN_LEFT"'),
                    good.replace(' -f "workers=$WORKERS"', ""),
                    good.replace("image_recache.yml", "tmdb_mirror.yml")):
            with self.subTest(bad=bad):
                self.assertNotEqual(_dispatch(bad), ("image_recache.yml", "$GITHUB_REF_NAME", HOP_INPUTS))

    def test_the_four_reasons_not_to_relaunch_come_in_order_each_a_plain_exit_0(self):
        script = "\n".join(self.shell)
        self.assertTrue(script.startswith("set -euo pipefail\n"), script[:40])
        at = [script.find(reason) for reason in NO_RELAUNCH]
        self.assertTrue(all(position >= 0 for position in at), at)
        self.assertEqual(at, sorted(at))
        relaunch_at = script.find("gh workflow run ")
        self.assertGreater(relaunch_at, at[-1])
        # Each reason ends the step with a plain exit 0 before the next is asked; no other exit.
        exits = [m.start() for m in re.finditer(r"(?m)^\s*exit 0$", script)]
        self.assertEqual(len(exits), 4, exits)
        bounds = at + [relaunch_at]
        for i in range(4):
            with self.subTest(reason=NO_RELAUNCH[i]):
                self.assertTrue(bounds[i] < exits[i] < bounds[i + 1], (bounds, exits))
        self.assertEqual(len(re.findall(r"\bexit\b", script)), 4)
        # The waiting check asks this workflow's queue for the four waiting states.
        self.assertIn(WAITING_QUERY, script)
        # And the line a relaunch prints says how many hops are left.
        self.assertRegex(script, r'(?m)^echo ".*relaunched.*\$\(\(CHAIN_LEFT - 1\)\) left"$')

    def test_the_shell_is_exactly_its_four_guards_then_the_relaunch(self):
        # Every command and condition, in order: the guard of each reason is pinned, not only its line.
        self.assertEqual(_skeleton(self.shell), SKELETON)
        echoes = _echoes(self.shell)
        self.assertEqual(len(echoes), 5, echoes)
        for i, reason in enumerate(NO_RELAUNCH):
            with self.subTest(reason=reason):                                  # each guard says its own reason
                at = self.shell.index(GUARDS[i]) if GUARDS[i] in self.shell else -1
                self.assertGreaterEqual(at, 0)
                self.assertIn(reason, self.shell[at + 1] if at >= 0 else "")
                self.assertEqual(self.shell[at + 2:at + 4] if at >= 0 else [], ["  exit 0", "fi"])
        self.assertIn("relaunched", echoes[-1] if echoes else "")
        # Negatives beside it: the guards a review could get wrong read differently.
        for bad in (("(( RETAGGED <= REFUSED ))", "(( REFUSED <= RETAGGED ))"),
                    ("(( RETAGGED <= REFUSED ))", "(( RETAGGED < REFUSED ))"),
                    ("(( CHAIN_LEFT == 0 ))", "(( CHAIN_LEFT <= 1 ))"),
                    ('[ "$waiting" != "0" ]', '[ "$waiting" = "x" ]'),
                    ("^(0|[1-9][0-9]*)$", "^.*$")):
            with self.subTest(bad=bad[1]):
                mutated = [line.replace(*bad) for line in self.shell]
                self.assertNotEqual(mutated, self.shell)                       # the mutation applied
                self.assertNotEqual(_skeleton(mutated), SKELETON)

    def test_the_shell_reads_every_value_from_its_env_never_an_inline_expression(self):
        # The run's outputs and the dispatch inputs reach bash through env (GitHub's hardening advice).
        self.assertTrue(self.shell)
        self.assertNotIn("${{", "\n".join(self.shell))
        self.assertIn("${{ steps.recache.outputs.retagged }}", self.relaunch)  # positive control: in the env
        self.assertNotIn("${{", "\n".join(_shell("name: x\n        run: |\n          echo $X\n")))
        self.assertIn("${{", "\n".join(_shell("name: x\n        run: |\n          echo ${{ inputs.x }}\n")))

    def test_the_header_says_how_the_drain_chains_and_what_it_costs(self):
        head = self.text.split("name:", 1)[0]
        before, _, paragraph = head.partition("# B5.10.1")
        self.assertTrue(paragraph)
        for words in ("ten scheduled runs", "3.7 days", "twenty-four", "≈ 27 days", "hop", "budget_hit=1",
                      "drain_done=0", "not a whole number", "exhausted", "refused as much as it retagged",
                      "48 hops", "already waiting", "its own knobs", "A red hop never relaunches and stays visible",
                      "backstop", "restarter", "chains with 48", "reads as 48", "drain_done=1",
                      "ends chains for good", "ends only its own", "≈ 27 Actions minutes", "≈ 47 hops", "≈ 1,300",
                      "12 workers", "private repository"):
            with self.subTest(words=words):
                self.assertIn(words, paragraph)                                # in B5.10.1's own paragraph
        # Positive control: the B5.10 paragraph before it says none of the chain's numbers.
        for words in ("48 hops", "chains with 48", "≈ 1,300"):
            with self.subTest(before=words):
                self.assertNotIn(words, before)


if __name__ == "__main__":
    unittest.main()
