#!/usr/bin/env python3
"""Unit test del gate live PostgreSQL.

I tre test row diagnostics PostgreSQL saltano senza assert quando
`PLENORA_TEST_POSTGRES_DSN` non è impostata. Il gate li dichiara eseguiti:
questi test fissano che non possa dichiararlo senza averli visti passare.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import check_postgres_reference as gate


ROW_DIAGNOSTICS = (
    "live_provider_row_diagnostics_matches_confirmed_rollback_oracle",
    "live_provider_row_diagnostics_lost_rollback_ack_is_quarantined",
    "live_provider_row_diagnostics_commit_ambiguity_partitions_all_rows_unknown",
)


def output(names: tuple[str, ...]) -> str:
    return "\n".join(f"test test_suite::tests::{name} ... ok" for name in names)


class RequiredLiveTests(unittest.TestCase):
    def test_the_three_row_diagnostics_tests_are_pinned(self) -> None:
        self.assertEqual(set(gate.REQUIRED_LIVE_TESTS), set(ROW_DIAGNOSTICS))

    def test_a_complete_run_passes(self) -> None:
        gate.validate_live_row_diagnostics(output(ROW_DIAGNOSTICS))

    def test_a_silently_skipped_test_fails_the_gate(self) -> None:
        """Il caso che il gate deve impedire: DSN assente, early-return."""
        with self.assertRaises(RuntimeError) as raised:
            gate.validate_live_row_diagnostics("")
        for name in ROW_DIAGNOSTICS:
            self.assertIn(name, str(raised.exception))

    def test_a_partial_run_names_the_missing_tests(self) -> None:
        with self.assertRaises(RuntimeError) as raised:
            gate.validate_live_row_diagnostics(output(ROW_DIAGNOSTICS[:2]))
        message = str(raised.exception)
        self.assertIn(ROW_DIAGNOSTICS[2], message)
        self.assertNotIn(ROW_DIAGNOSTICS[0], message)

    def test_a_failed_test_is_not_counted_as_executed(self) -> None:
        failed = "\n".join(
            [
                f"test test_suite::tests::{ROW_DIAGNOSTICS[0]} ... ok",
                f"test test_suite::tests::{ROW_DIAGNOSTICS[1]} ... ok",
                f"test test_suite::tests::{ROW_DIAGNOSTICS[2]} ... FAILED",
            ]
        )
        with self.assertRaises(RuntimeError):
            gate.validate_live_row_diagnostics(failed)


class ComposeNetworkDiscovery(unittest.TestCase):
    """La rete Compose va osservata dal container, non presunta dal nome.

    Un nome cablato vale solo per il checkout il cui progetto Compose si chiama
    `plenora-database-tools`: in un worktree il container è su un'altra rete e
    i cargo con DSN non lo raggiungono, producendo errori `Protocol`/`Connect`
    che sembrano difetti del provider. La scoperta è fail-closed: se i metadati
    non provano la rete, il gate fallisce invece di ripiegare su un nome
    inventato.
    """

    LABELS = '{"com.docker.compose.project":"plenora-database-tools-row-diagnostics"}'
    NETWORKS = (
        '{"plenora-database-tools-row-diagnostics_default":'
        '{"Aliases":["dataflow-postgres","postgres"]}}'
    )

    def test_the_observed_compose_network_of_the_running_fixture_is_used(self) -> None:
        with patch.object(gate, "run", side_effect=[self.LABELS, self.NETWORKS]):
            self.assertEqual(
                gate.postgres_network(),
                "plenora-database-tools-row-diagnostics_default",
            )

    def test_a_container_outside_compose_fails_closed(self) -> None:
        with patch.object(gate, "run", return_value="null"):
            with self.assertRaisesRegex(
                RuntimeError, "progetto Compose del riferimento PostgreSQL assente"
            ):
                gate.postgres_network()

    def test_missing_network_metadata_fails_closed(self) -> None:
        with patch.object(gate, "run", side_effect=[self.LABELS, "null"]):
            with self.assertRaisesRegex(
                RuntimeError, "rete Compose del riferimento PostgreSQL assente"
            ):
                gate.postgres_network()

    def test_a_network_without_the_container_alias_fails_closed(self) -> None:
        networks = (
            '{"plenora-database-tools-row-diagnostics_default":{"Aliases":["altro"]}}'
        )
        with patch.object(gate, "run", side_effect=[self.LABELS, networks]):
            with self.assertRaisesRegex(
                RuntimeError, "rete Compose del riferimento PostgreSQL assente"
            ):
                gate.postgres_network()

    def test_no_hardcoded_network_constant_remains(self) -> None:
        self.assertFalse(
            hasattr(gate, "NETWORK"),
            "una costante cablata reintrodurrebbe il fallback silenzioso",
        )

    def test_cargo_with_dsn_runs_on_the_discovered_network(self) -> None:
        with patch.object(
            gate, "postgres_network", return_value="observed_default"
        ) as discovery:
            command = gate.cargo(["test"], "host=dataflow-postgres")
        discovery.assert_called_once_with()
        self.assertIn("--network", command)
        self.assertEqual(command[command.index("--network") + 1], "observed_default")

    def test_cargo_without_dsn_does_not_query_docker(self) -> None:
        """Senza DSN non c'è nulla da raggiungere: nessuna rete, nessuna query."""

        with patch.object(gate, "postgres_network") as discovery:
            command = gate.cargo(["clippy"])
        discovery.assert_not_called()
        self.assertNotIn("--network", command)


if __name__ == "__main__":
    unittest.main()
