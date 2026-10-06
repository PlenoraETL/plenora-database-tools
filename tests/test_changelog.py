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

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]


def toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


class ChangelogTests(unittest.TestCase):
    def test_declared_versions_agree(self) -> None:
        workspace = toml(ROOT / "Cargo.toml")["workspace"]["package"]["version"]
        binding = toml(ROOT / "crates/plenora-database-py/Cargo.toml")["package"]["version"]
        wheel = toml(ROOT / "crates/plenora-database-py/pyproject.toml")["project"]["version"]
        self.assertEqual({workspace, binding, wheel}, {workspace})

    def test_the_first_section_is_the_declared_version(self) -> None:
        workspace = toml(ROOT / "Cargo.toml")["workspace"]["package"]["version"]
        text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        sections = re.findall(r"^## (\d+\.\d+\.\d+) ", text, flags=re.MULTILINE)
        self.assertTrue(sections, "CHANGELOG senza sezioni di versione")
        self.assertEqual(sections[0], workspace)
        self.assertEqual(len(sections), len(set(sections)), "sezione ripetuta")


if __name__ == "__main__":
    unittest.main()
