#!/usr/bin/env python3
"""Unit test del gate live SQL Server.

Il gate esiste per impedire che un test live dichiarato smetta silenziosamente
di essere eseguito. Questi test fissano quel contratto: il conteggio della
matrice live e il nome dei test che non possono mancare.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

import check_sqlserver_reference as gate
from scripts import compose_network as compose_network_module


LIVE_ROW_DIAGNOSTICS = "live_provider_row_diagnostics_matches_confirmed_rollback_oracle"
WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "sqlserver-assurance.yml"


def live_output(names: list[str], passed: int) -> str:
    lines = [f"test live_tests::{name} ... ok" for name in names]
    lines.append(
        f"test result: ok. {passed} passed; 0 failed; 0 ignored; 0 measured; "
        "3 filtered out"
    )
    return "\n".join(lines)


class RequiredLiveTests(unittest.TestCase):
    def test_workflow_watches_the_public_cli_surface(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('      - "crates/plenora-database-cli/**"', workflow)

    def test_row_diagnostics_oracle_is_pinned(self) -> None:
        self.assertIn(LIVE_ROW_DIAGNOSTICS, gate.REQUIRED_LIVE_TESTS)

    def test_expected_count_covers_the_row_diagnostics_oracle(self) -> None:
        self.assertEqual(gate.EXPECTED_LIVE_TESTS, 45)

    def test_a_complete_matrix_passes(self) -> None:
        gate.validate_live_result(
            live_output([LIVE_ROW_DIAGNOSTICS], gate.EXPECTED_LIVE_TESTS)
        )

    def test_a_missing_row_diagnostics_test_fails_the_gate(self) -> None:
        """Il caso che il gate deve impedire: matrice piena, oracolo assente."""
        with self.assertRaises(RuntimeError) as raised:
            gate.validate_live_result(
                live_output(["live_reference_probe_and_catalog"], gate.EXPECTED_LIVE_TESTS)
            )
        self.assertIn(LIVE_ROW_DIAGNOSTICS, str(raised.exception))

    def test_a_shrunken_matrix_fails_the_gate(self) -> None:
        with self.assertRaises(RuntimeError):
            gate.validate_live_result(
                live_output([LIVE_ROW_DIAGNOSTICS], gate.EXPECTED_LIVE_TESTS - 1)
            )

    def test_a_skipped_matrix_fails_the_gate(self) -> None:
        with self.assertRaises(RuntimeError):
            gate.validate_live_result(
                "test result: ok. 0 passed; 0 failed; 45 ignored; 0 measured; 0 filtered out"
            )


    def test_gate_runs_the_public_cli_probe_against_the_private_ca_fixture(self) -> None:
        output = (
            "test live_database_probe_sqlserver_private_ca ... ok\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out"
        )
        with patch.object(gate, "run_cargo", return_value=output) as run_cargo:
            gate.run_live_cli_probe()
        arguments = run_cargo.call_args.args[0]
        self.assertEqual(
            arguments[:5],
            ["test", "-p", "plenora-database-cli", "--test", "live_probe"],
        )
        self.assertIn("live_database_probe_sqlserver_private_ca", arguments)
        self.assertIn("--exact", arguments)
        self.assertIn("--ignored", arguments)


class ComposeNetworkDiscovery(unittest.TestCase):
    """La rete Compose va osservata dal container, non presunta dal nome.

    Un nome cablato vale solo per il checkout il cui progetto Compose si chiama
    `plenora-database-tools`: in un worktree il progetto cambia e il gate non è
    più eseguibile. La scoperta è fail-closed, come per MySQL: se i metadati non
    provano la rete, il gate fallisce invece di ripiegare su un nome inventato.
    """

    def test_the_observed_compose_network_of_the_running_fixture_is_used(self) -> None:
        labels = '{"com.docker.compose.project":"plenora-database-tools-row-diagnostics"}'
        networks = (
            '{"plenora-database-tools-row-diagnostics_default":'
            '{"Aliases":["dataflow-sqlserver","sqlserver-hostname-mismatch"]}}'
        )
        with patch.object(
            compose_network_module, "_inspect", side_effect=[labels, networks]
        ):
            self.assertEqual(
                gate.sqlserver_network(),
                "plenora-database-tools-row-diagnostics_default",
            )

    def test_a_container_outside_compose_fails_closed(self) -> None:
        with patch.object(compose_network_module, "_inspect", return_value="null"):
            with self.assertRaisesRegex(
                RuntimeError, "senza label di progetto Compose"
            ):
                gate.sqlserver_network()

    def test_missing_network_metadata_fails_closed(self) -> None:
        labels = '{"com.docker.compose.project":"plenora-database-tools"}'
        with patch.object(
            compose_network_module, "_inspect", side_effect=[labels, "null"]
        ):
            with self.assertRaisesRegex(RuntimeError, "non e sulla rete"):
                gate.sqlserver_network()

    def test_a_network_without_the_container_alias_fails_closed(self) -> None:
        labels = '{"com.docker.compose.project":"plenora-database-tools"}'
        networks = '{"plenora-database-tools_default":{"Aliases":["altro"]}}'
        with patch.object(
            compose_network_module, "_inspect", side_effect=[labels, networks]
        ):
            with self.assertRaisesRegex(
                RuntimeError, "alias dataflow-sqlserver assente"
            ):
                gate.sqlserver_network()

    def test_no_hardcoded_network_constant_remains(self) -> None:
        self.assertFalse(
            hasattr(gate, "NETWORK"),
            "una costante cablata reintrodurrebbe il fallback silenzioso",
        )

    def test_cargo_runs_on_the_discovered_network(self) -> None:
        with patch.object(
            gate, "sqlserver_network", return_value="observed_default"
        ) as discovery:
            command, _environment = gate.cargo(["test"])
        discovery.assert_called_once_with()
        self.assertIn("--network", command)
        self.assertEqual(command[command.index("--network") + 1], "observed_default")


if __name__ == "__main__":
    unittest.main()
