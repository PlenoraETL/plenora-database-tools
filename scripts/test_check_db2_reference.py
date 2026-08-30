#!/usr/bin/env python3
"""Self-test del gate live Db2."""

from __future__ import annotations

import unittest
from pathlib import Path

import check_db2_reference as gate


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "db2-assurance.yml"


def qualified(names: set[str]) -> set[str]:
    return {
        f"{'spatial_live_tests' if name.startswith('live_spatial_') else 'live_tests'}::{name}"
        for name in names
    }


class Db2ReferenceGateTests(unittest.TestCase):
    def test_python_live_inventory_includes_the_orm_gate(self) -> None:
        self.assertEqual(gate.PYTHON_LIVE_EXPECTED, 6)
        self.assertEqual(
            gate.PYTHON_LIVE_TARGETS,
            (
                "db2_sdk_gate_tests/test_db2_session.py",
                "db2_sdk_gate_tests/test_orm.py::test_live_db2_generated_defaults_and_ddl",
            ),
        )

    def test_inventory_has_all_twelve_named_live_tests(self) -> None:
        inventory = gate.live_test_inventory()
        self.assertEqual(len(inventory), 12)
        self.assertLessEqual(gate.REQUIRED_LIVE_TESTS, inventory)

    def test_complete_named_run_passes(self) -> None:
        names = gate.live_test_inventory()
        listing = "\n".join(f"{name}: test" for name in sorted(qualified(names)))
        output = "\n".join(f"test {name} ... ok" for name in sorted(qualified(names)))
        self.assertEqual(gate.validate_live_run(listing, output), sorted(qualified(names)))

    def test_substitution_at_equal_count_fails(self) -> None:
        names = gate.live_test_inventory()
        compiled = qualified(names)
        executed = set(compiled)
        executed.remove(next(iter(executed)))
        executed.add("live_tests::live_inventato")
        listing = "\n".join(f"{name}: test" for name in sorted(compiled))
        output = "\n".join(f"test {name} ... ok" for name in sorted(executed))
        with self.assertRaisesRegex(RuntimeError, "incompleta"):
            gate.validate_live_run(listing, output)

    def test_every_live_test_carries_an_explicit_reason(self) -> None:
        reasons = {}
        for source in (gate.LIVE_SOURCE, gate.SPATIAL_LIVE_SOURCE):
            reasons.update(
                gate.live_inventory.ignore_reasons(source.read_text(encoding="utf-8"))
            )
        self.assertEqual(set(reasons), gate.live_test_inventory())
        self.assertTrue(all(reason.startswith("richiede Db2") for reason in reasons.values()))

    def test_build_contract_is_pinned_and_platform_explicit(self) -> None:
        bases = gate.validate_build_contract()
        self.assertEqual(set(bases), {"rust", "db2"})

    def test_cli_probe_requires_matching_connection_and_capability_provider(self) -> None:
        valid = {
            "connection": {"provider": "db2"},
            "capabilities": {"provider": "db2"},
        }
        gate.validate_cli_probe(valid)
        for surface in ("connection", "capabilities"):
            invalid = {key: dict(value) for key, value in valid.items()}
            invalid[surface]["provider"] = "postgres"
            with self.assertRaisesRegex(RuntimeError, surface):
                gate.validate_cli_probe(invalid)

    def test_workflow_executes_self_test_gate_and_cleanup(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        for expected in (
            "python3 scripts/test_check_db2_reference.py",
            "python3 scripts/check_db2_reference.py",
            "docker-compose.db2.yml",
            "db2-windows-build:",
            "--features db2",
            "PLENORA_EXPECT_DB2_RUNTIME",
            '".github/scripts/verify_wheel.py"',
            '"rust-toolchain.toml"',
            '"requirements-self-tests.txt"',
            "down --volumes",
        ):
            self.assertIn(expected, workflow)


if __name__ == "__main__":
    unittest.main()
