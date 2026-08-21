#!/usr/bin/env python3
"""Unit test del gate live PostgreSQL.

I tre test row diagnostics PostgreSQL saltano senza assert quando
`PLENORA_TEST_POSTGRES_DSN` non è impostata. Il gate li dichiara eseguiti:
questi test fissano che non possa dichiararlo senza averli visti passare.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

import check_postgres_reference as gate
from scripts import compose_network as compose_network_module


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
        with patch.object(
            compose_network_module, "_inspect", side_effect=[self.LABELS, self.NETWORKS]
        ):
            self.assertEqual(
                gate.postgres_network(),
                "plenora-database-tools-row-diagnostics_default",
            )

    def test_a_container_outside_compose_fails_closed(self) -> None:
        with patch.object(compose_network_module, "_inspect", return_value="null"):
            with self.assertRaisesRegex(
                RuntimeError, "senza label di progetto Compose"
            ):
                gate.postgres_network()

    def test_missing_network_metadata_fails_closed(self) -> None:
        with patch.object(
            compose_network_module, "_inspect", side_effect=[self.LABELS, "null"]
        ):
            with self.assertRaisesRegex(RuntimeError, "non e sulla rete"):
                gate.postgres_network()

    def test_a_network_without_the_container_alias_fails_closed(self) -> None:
        networks = (
            '{"plenora-database-tools-row-diagnostics_default":{"Aliases":["altro"]}}'
        )
        with patch.object(
            compose_network_module, "_inspect", side_effect=[self.LABELS, networks]
        ):
            with self.assertRaisesRegex(
                RuntimeError, "alias dataflow-postgres assente"
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
class CliLiveFixtures(unittest.TestCase):
    """Le suite live del CLI devono restare nel gate, e restare eseguibili.

    Sono marcate `#[ignore]`: senza `--include-ignored` `cargo test` le salta
    e il gate resta verde avendo eseguito zero fixture. E la forma di falso
    verde che le ha lasciate rotte per una campagna intera, con tre di esse
    che costruivano un riferimento a un oggetto con un campo che il contratto
    non ha piu.
    """

    SUITES = ("live_f5", "contract_snapshot")

    def source(self) -> str:
        return (
            Path(gate.__file__).resolve().parent / "check_postgres_reference.py"
        ).read_text(encoding="utf-8")

    def test_both_cli_suites_are_named_by_the_gate(self) -> None:
        source = self.source()
        for suite in self.SUITES:
            self.assertIn(
                f'"{suite}"', source, f"il gate non nomina la suite {suite}"
            )

    def test_the_ignored_fixtures_are_included(self) -> None:
        self.assertIn(
            '"--include-ignored"',
            self.source(),
            "senza --include-ignored le fixture live vengono saltate",
        )

    def test_the_cli_suites_run_against_the_reference(self) -> None:
        """Con il DSN, quindi sulla rete del fixture.

        Una suite live lanciata senza DSN non fallisce: si limita a non
        misurare niente, ed e di nuovo un verde che non significa nulla.
        """
        source = self.source()
        start = source.index('for suite in ("live_f5"')
        block = source[start : source.index("ipc_materialization =", start)]
        self.assertIn("dsn,", block, "le suite CLI non ricevono il DSN")
        self.assertIn(
            "insecure_local=True",
            block,
            "il riferimento di questo gate e plaintext: senza l'interruttore "
            "il CLI rifiuta la connessione",
        )

    def test_the_declared_check_names_the_cli_fixtures(self) -> None:
        self.assertIn(
            '"cli_live_fixtures_and_contract_snapshots"',
            self.source(),
            "il gate esegue le fixture ma non lo dichiara fra i check",
        )


if __name__ == "__main__":
    unittest.main()
