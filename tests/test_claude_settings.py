"""
Pins the project's .claude/settings.json (RUN B5.5, housekeeping): Claude Code
is denied reading the local secrets files — .env*, and .claude/settings.local.json,
which is Fran's and git-ignored globally — through the Read tool and through
`cat` in Bash. The file is committed and holds no secret; the test reads it as
JSON. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import json
import pathlib
import unittest

SETTINGS = pathlib.Path(__file__).resolve().parent.parent / ".claude" / "settings.json"

DENIED = [
    "Read(./.env.json)",
    "Read(./.claude/settings.local.json)",
    "Read(./.env*)",
    "Bash(cat .env*)",
    "Bash(cat .claude/settings.local.json)",
]


class ClaudeSettingsTest(unittest.TestCase):

    def test_the_secrets_files_are_denied_to_read_and_to_cat(self):
        settings = json.loads(SETTINGS.read_text(encoding="utf-8"))
        deny = settings["permissions"]["deny"]
        for rule in DENIED:
            with self.subTest(rule=rule):
                self.assertIn(rule, deny)

    def test_the_file_allows_nothing(self):
        # A committed file reaches every clone: it only takes rights away.
        permissions = json.loads(SETTINGS.read_text(encoding="utf-8"))["permissions"]
        self.assertEqual(sorted(permissions), ["deny"])


if __name__ == "__main__":
    unittest.main()
