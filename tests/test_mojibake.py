"""Prove del gate sul testo UTF-8 codificato due volte."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts import check_mojibake

#: Testo italiano corretto con i caratteri che la doppia codifica rovina.
CORRECT = "perch" + chr(0xE9) + " gi" + chr(0xE0) + " " + chr(0xE8) + " cos" + chr(0xEC) + \
    " " + chr(0x2014) + " " + chr(0xAB) + "citazione" + chr(0xBB) + " pi" + chr(0xF9) + \
    " " + chr(0xA7) + "6 " + chr(0x20AC)


def double_encoded(text: str) -> str:
    """`text` riletto come Windows-1252 dopo la codifica UTF-8."""
    return text.encode("utf-8").decode("cp1252", errors="replace")


class MojibakeTests(unittest.TestCase):
    def check(self, files: dict[str, str]) -> tuple[int, list[str]]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, text in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")
            return check_mojibake.check_repository(root)

    def test_il_testo_corretto_passa(self) -> None:
        checked, problems = self.check({"a.rs": f"// {CORRECT}\n", "b.md": CORRECT})
        self.assertEqual(checked, 2)
        self.assertEqual(problems, [])

    def test_ogni_carattere_codificato_due_volte_si_rifiuta(self) -> None:
        for character in CORRECT:
            if ord(character) < 0x80:
                continue
            with self.subTest(code=hex(ord(character))):
                _, problems = self.check({"a.rs": f'let m = "{double_encoded(character)}";\n'})
                self.assertEqual(len(problems), 1, problems)

    def test_la_a_accentata_con_lo_spazio_riscritto(self) -> None:
        broken = double_encoded(chr(0xE0)).replace(chr(0xA0), " ")
        _, problems = self.check({"a.py": f"# gi{broken}\n"})
        self.assertEqual(len(problems), 1)
        self.assertIn("a.py:1", problems[0])

    def test_vendor_e_contracts_restano_fuori(self) -> None:
        broken = double_encoded(CORRECT)
        checked, problems = self.check(
            {"vendor/x.rs": broken, "contracts/y.md": broken, "z.txt": "ok"}
        )
        self.assertEqual(checked, 1)
        self.assertEqual(problems, [])

    def test_il_repository_e_pulito(self) -> None:
        _, problems = check_mojibake.check_repository()
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
