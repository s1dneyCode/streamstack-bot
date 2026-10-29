"""
Pins B3.2 (RUN B4 · 0a): every workflow line in the repo that runs
bot.backfill_states carries --skip-theatres.

Without the flag the backfill writes is_in_theatres = False on every title it
updates, and a title in theatres always differs from that — so either full
sequence, dispatched, emptied the New in Theatres rail until daily_verify.py,
which owns the flag, ran the next morning. Until B4 only backfills.yml passed
it; full_backfill_new.yml (step 11) and full_backfill_after_bulkimport.yml
(step 4) did not.

The scan reads every workflow file as text (the CI tests job installs no YAML
library), so a new sequence that runs the backfill without the flag fails here
too. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import pathlib
import re
import unittest

WORKFLOWS = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows"


def state_lines() -> list[tuple[str, int, str]]:
    """(file, line number, code) of every line, comments left out, that runs
    bot.backfill_states — in any workflow of the repo."""
    found = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            code = line.split("#", 1)[0].rstrip()
            if re.search(r"python -m bot\.backfill_states\b", code):
                found.append((path.name, number, code.strip()))
    return found


def skips_the_theatres(code: str) -> bool:
    """The command passes --skip-theatres as one of its arguments."""
    command = code.split("python -m bot.backfill_states", 1)[-1]
    return "--skip-theatres" in command.split()


class BackfillStatesFlagTest(unittest.TestCase):

    def test_every_line_that_runs_the_backfill_skips_the_theatres(self):
        lines = state_lines()
        self.assertTrue(lines)
        for name, number, code in lines:
            with self.subTest(at=f"{name}:{number}"):
                self.assertTrue(skips_the_theatres(code), code)

    def test_the_scan_finds_the_three_known_lines(self):
        # Positive control for the scan: the three workflows B3.2 named are all read.
        self.assertTrue({"backfills.yml", "full_backfill_new.yml", "full_backfill_after_bulkimport.yml"}
                        <= {name for name, _, _ in state_lines()})

    def test_the_rule_fails_todays_two_lines(self):
        # Positive control for the rule: the line the two full sequences had until B4 fails it,
        # the one backfills.yml has always had passes it, and so does a later argument order.
        self.assertFalse(skips_the_theatres("run: python -m bot.backfill_states"))
        self.assertFalse(skips_the_theatres("run: python -m bot.backfill_states --skip-theatres-not"))
        self.assertTrue(skips_the_theatres("run: python -m bot.backfill_states --skip-theatres"))
        self.assertTrue(skips_the_theatres("run: python -m bot.backfill_states --verbose --skip-theatres"))

    def test_a_commented_line_is_not_a_line_that_runs(self):
        self.assertFalse(re.search(r"python -m bot\.backfill_states\b",
                                   "        # run: python -m bot.backfill_states".split("#", 1)[0]))


if __name__ == "__main__":
    unittest.main()
