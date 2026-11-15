"""Il CHANGELOG e le versioni dichiarate non si separano.

Workspace Cargo, crate del binding e `pyproject.toml` portano la stessa
versione, e il CHANGELOG ne ha la sezione in cima: una release senza note, o
note di una versione che nessun manifest dichiara, la scopre questa prova
invece del lettore.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]


def toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def release_versions(text: str) -> list[str]:
    """Ogni sezione ha una versione: nessun titolo viene saltato dal gate."""
    versions = []
    for index, heading in enumerate(re.findall(r"^## (.+)$", text, flags=re.MULTILINE)):
        match = re.fullmatch(r"(\d+\.\d+\.\d+) — (Non rilasciata|\d{4}-\d{2}-\d{2})", heading)
        if match is None:
            raise ValueError("sezione CHANGELOG senza versione e stato canonici")
        version, state = match.groups()
        if version in versions or (state == "Non rilasciata" and index != 0):
            raise ValueError("versione ripetuta o sezione non rilasciata fuori dalla cima")
        versions.append(version)
    if not versions:
        raise ValueError("CHANGELOG senza sezioni di versione")
    return versions


class ChangelogTests(unittest.TestCase):
    def test_declared_versions_agree(self) -> None:
        workspace = toml(ROOT / "Cargo.toml")["workspace"]["package"]["version"]
        binding = toml(ROOT / "crates/plenora-database-py/Cargo.toml")["package"]["version"]
        wheel = toml(ROOT / "crates/plenora-database-py/pyproject.toml")["project"]["version"]
        self.assertEqual({workspace, binding, wheel}, {workspace})

    def test_the_first_section_is_the_declared_version(self) -> None:
        workspace = toml(ROOT / "Cargo.toml")["workspace"]["package"]["version"]
        text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        sections = release_versions(text)
        self.assertEqual(sections[0], workspace)
        self.assertEqual(len(sections), len(set(sections)), "sezione ripetuta")

    def test_release_state_does_not_hide_the_current_version(self) -> None:
        for state in ("Non rilasciata", "2026-11-15"):
            self.assertEqual(release_versions(f"## 12.0.0 — {state}\n"), ["12.0.0"])

    def test_no_section_can_bypass_version_uniqueness_and_order(self) -> None:
        for text in (
            "# Changelog\n",
            "## Non rilasciata — 12.0.0\n## 11.0.0 — 2026-11-14\n",
            "## 12.0.0 — Non rilasciata\n## 12.0.0 — 2026-11-15\n",
            "## 12.0.0 — 2026-11-15\n## 11.0.0 — Non rilasciata\n",
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                release_versions(text)


if __name__ == "__main__":
    unittest.main()
