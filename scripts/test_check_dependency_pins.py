#!/usr/bin/env python3
"""Self-test di `check_dependency_pins.py` su manifest sintetici."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_dependency_pins", ROOT / "scripts" / "check_dependency_pins.py"
)
assert SPEC is not None and SPEC.loader is not None
pins = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = pins
SPEC.loader.exec_module(pins)

WORKSPACE = """[workspace.package]
rust-version = "{msrv}"

[workspace.dependencies]
{dependencies}
local = {{ path = "crates/local" }}
"""


class DependencyPinTests(unittest.TestCase):
    def scan(self, dependencies: str, msrv: str = "1.98.1", crate: str = "") -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text(
                WORKSPACE.format(msrv=msrv, dependencies=dependencies), encoding="utf-8"
            )
            (root / "rust-toolchain.toml").write_text(
                '[toolchain]\nchannel = "1.98.1"\n', encoding="utf-8"
            )
            (root / "crates" / "a").mkdir(parents=True)
            (root / "crates" / "a" / "Cargo.toml").write_text(
                "[package]\nname = \"a\"\n\n[dependencies]\nserde.workspace = true\n"
                "tokio = { workspace = true, features = [\"rt\"] }\n" + crate
                + "\n[[bin]]\nname = \"a\"\npath = \"src/main.rs\"\n",
                encoding="utf-8",
            )
            (root / "fuzz").mkdir()
            (root / "fuzz" / "Cargo.toml").write_text("[package]\nname = \"f\"\n", encoding="utf-8")
            return [violation.message for violation in pins.scan(root)]

    def test_a_motivated_exact_pin_passes(self) -> None:
        self.assertEqual(
            self.scan('# Serve a X.\nserde = "=1.0.0"\ntokio = { version = "=1.2.3" } # Y.\n'),
            [],
        )

    def test_a_comment_block_motivates_only_the_next_line(self) -> None:
        self.assertEqual(
            self.scan('# Serve a X.\nserde = "=1.0.0"\nbytes = "=1.0.0"\n'),
            ["bytes: pin senza commento di motivazione"],
        )

    def test_a_range_is_rejected(self) -> None:
        for version in ('"1.0"', '"^1.0.0"', '"=1.0"', '{ version = "~1.2.3" }'):
            with self.subTest(version=version):
                self.assertIn(
                    "serde: versione non esatta (serve =x.y.z)",
                    self.scan(f"# Serve a X.\nserde = {version}\n"),
                )

    def test_a_crate_manifest_pin_is_checked_too(self) -> None:
        self.assertEqual(
            self.scan("", crate='pyo3 = { version = "=0.1.0" }\n'),
            ["pyo3: pin senza commento di motivazione"],
        )

    def test_the_msrv_must_be_the_pinned_toolchain(self) -> None:
        self.assertEqual(
            self.scan("", msrv="1.98"),
            ["rust-version 1.98 diverso dalla toolchain fissata 1.98.1"],
        )

    def test_the_repository_is_clean(self) -> None:
        self.assertEqual([str(violation) for violation in pins.scan()], [])


if __name__ == "__main__":
    unittest.main()
