"""
Unit tests for the enrichment workflow's cap (RUN B3 · 0b): the scheduled run
serves 1,000 titles a night, said in so many words; the dispatch keeps its own
input; no run can hand the script an empty --limit=.

The workflow is read as TEXT: the CI tests job installs only requirements.txt,
which carries no YAML library. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import unittest

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "nightly_enrichment.yml"

SCHEDULED = "github.event_name == 'schedule'"
DISPATCHED = "github.event_name == 'workflow_dispatch'"


def _steps(text: str) -> list[dict]:
    """The job's steps as {"name", "if", "run", "lines"}, read line by line."""
    steps, current = [], None
    for line in text.splitlines():
        head = re.match(r"^      - (.*)$", line)
        if head:
            current = {"lines": [head.group(1).strip()]}
            steps.append(current)
        elif current is not None and (line.startswith("        ") or not line.strip()):
            current["lines"].append(line.strip())
        else:
            current = None
    for step in steps:
        for line in step["lines"]:
            for key in ("name", "if", "run"):
                if line.startswith(f"{key}:"):
                    step[key] = line.split(":", 1)[1].strip()
    return steps


def unsafe_limits(text: str) -> list[str]:
    """Every --limit= a run could receive empty: bare, or an expression
    without a literal fallback (`|| 'N'`). A schedule has no inputs, so
    `${{ github.event.inputs.limit }}` alone reaches the shell as nothing,
    and the script's int('') would crash."""
    bad = []
    for line in text.splitlines():
        for match in re.finditer(r"--limit=(\$\{\{.*?\}\}|\S*)", line):
            value = match.group(1)
            if re.fullmatch(r"\d+", value):
                continue
            if value.startswith("${{") and re.search(r"\|\|\s*'\d+'\s*\}\}$", value):
                continue
            bad.append(line.strip())
    return bad


class EnrichmentWorkflowLimitTest(unittest.TestCase):

    def setUp(self):
        self.text = WORKFLOW.read_text(encoding="utf-8")
        self.steps = _steps(self.text)
        self.enrich_steps = [s for s in self.steps if "bot.enrich_new_titles" in s.get("run", "")]

    def test_the_scheduled_run_passes_1000_explicitly(self):
        scheduled = [s for s in self.enrich_steps if s.get("if") == SCHEDULED]

        self.assertEqual(len(scheduled), 1, self.enrich_steps)
        self.assertEqual(scheduled[0]["run"], "python -m bot.enrich_new_titles --limit=1000")

    def test_the_dispatch_path_keeps_its_input(self):
        dispatched = [s for s in self.enrich_steps if s.get("if") == DISPATCHED]

        self.assertEqual(len(dispatched), 1, self.enrich_steps)
        self.assertEqual(
            dispatched[0]["run"],
            "python -m bot.enrich_new_titles --limit=${{ github.event.inputs.limit || '2000' }}",
        )
        # The input itself, with its own default, is where it was.
        self.assertRegex(self.text, r"inputs:\n\s+limit:\n(?:.*\n){0,3}?\s+default: '2000'")

    def test_every_enrichment_step_runs_on_one_event_only(self):
        # An ungated step would run on both events: twice on a dispatch, and
        # with an expression a schedule cannot fill.
        self.assertEqual(sorted(s.get("if", "") for s in self.enrich_steps), sorted([SCHEDULED, DISPATCHED]))
        for step in self.enrich_steps:
            for secret in ("TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"):
                self.assertIn(f"{secret}: ${{{{ secrets.{secret} }}}}", step["lines"])

    def test_no_run_can_reach_the_script_with_an_empty_limit(self):
        self.assertEqual(unsafe_limits(self.text), [])

    def test_the_scan_bites_on_its_fixture(self):
        # Positive control: the scan sees what it is meant to see.
        fixture = "\n".join([
            "        run: python -m bot.enrich_new_titles --limit=",
            "        run: python -m bot.enrich_new_titles --limit=${{ inputs.limit }}",
            "        run: python -m bot.enrich_new_titles --limit=${{ github.event.inputs.limit }} --x",
            "        run: python -m bot.enrich_new_titles --limit=1000",
            "        run: python -m bot.enrich_new_titles --limit=${{ github.event.inputs.limit || '2000' }}",
        ])
        self.assertEqual(len(unsafe_limits(fixture)), 3)

    def test_the_schedule_itself_is_unchanged(self):
        self.assertIn("- cron: '0 9 * * *'", self.text)


if __name__ == "__main__":
    unittest.main()
