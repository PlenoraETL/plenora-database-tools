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


def qualified(names) -> list[str]:
    return [f"live_tests::{name}" for name in names]


def listing(names) -> str:
    return "\n".join(f"{name}: test" for name in qualified(names))


def live_output(names, passed: int | None = None) -> str:
    names = list(names)
    lines = [f"test {name} ... ok" for name in qualified(names)]
    lines.append(
        f"test result: ok. {passed if passed is not None else len(names)} passed; "
        "0 failed; 0 ignored; 0 measured; 3 filtered out"
    )
    return "\n".join(lines)


def inventory() -> list[str]:
    return sorted(gate.live_test_inventory())


def executable() -> list[str]:
    skipped = set(gate.SKIPPED_LIVE_TESTS)
    return [name for name in inventory() if name not in skipped]


class RequiredLiveTests(unittest.TestCase):
    def test_workflow_watches_the_public_cli_surface(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('      - "crates/plenora-database-cli/**"', workflow)

    def test_row_diagnostics_oracle_is_pinned(self) -> None:
        self.assertIn(LIVE_ROW_DIAGNOSTICS, gate.REQUIRED_LIVE_TESTS)

    def test_the_inventory_comes_from_the_sources_and_holds_the_oracle(self) -> None:
        """Non un conteggio: i nomi.

        Il gate verificava soltanto che quarantacinque test fossero passati, e
        un totale non distingue un test da un altro: sostituirne uno lasciava
        la matrice piena e il gate verde.
        """

        names = inventory()
        self.assertIn(LIVE_ROW_DIAGNOSTICS, names)
        self.assertGreater(len(names), len(gate.SKIPPED_LIVE_TESTS))
        self.assertNotIn("live_config", names, "un helper non e un test")

    def test_the_skipped_tests_carry_their_reason_from_the_code(self) -> None:
        reasons = gate.skipped_with_reasons()
        self.assertEqual(set(reasons), set(gate.SKIPPED_LIVE_TESTS))
        for name, reason in reasons.items():
            self.assertTrue(reason.startswith("richiede"), f"{name}: {reason}")

    def test_a_complete_matrix_passes(self) -> None:
        names = executable()
        self.assertEqual(
            gate.validate_live_result(live_output(names), listing(inventory())),
            sorted(qualified(names)),
        )

    def test_a_missing_row_diagnostics_test_fails_the_gate(self) -> None:
        """Il caso che il gate deve impedire: matrice piena, oracolo assente."""
        names = [name for name in executable() if name != LIVE_ROW_DIAGNOSTICS]
        with self.assertRaises(RuntimeError) as raised:
            # Il totale resta quello di prima: solo il nome lo smaschera.
            gate.validate_live_result(
                live_output(names, len(executable())), listing(inventory())
            )
        self.assertIn(LIVE_ROW_DIAGNOSTICS, str(raised.exception))

    def test_a_substituted_test_fails_the_gate(self) -> None:
        """Il confronto per nome rileva sostituzioni a conteggio invariato."""

        names = executable()[:-1] + ["live_test_inventato_che_non_esiste"]
        with self.assertRaises(RuntimeError) as raised:
            gate.validate_live_result(live_output(names), listing(inventory()))
        self.assertIn(executable()[-1], str(raised.exception))

    def test_a_test_missing_from_the_compiled_suite_fails_the_gate(self) -> None:
        names = executable()
        with self.assertRaisesRegex(RuntimeError, "assenti dalla suite"):
            gate.validate_live_result(
                live_output(names), listing(inventory()[1:])
            )

    def test_a_shrunken_matrix_fails_the_gate(self) -> None:
        with self.assertRaises(RuntimeError):
            gate.validate_live_result(
                live_output(executable()[:-1]), listing(inventory())
            )

    def test_a_skipped_matrix_fails_the_gate(self) -> None:
        with self.assertRaises(RuntimeError):
            gate.validate_live_result(
                "test result: ok. 0 passed; 0 failed; 45 ignored; 0 measured; 0 filtered out",
                listing(inventory()),
            )

    def test_the_cargo_skips_come_from_the_declaration(self) -> None:
        """Due elenchi separati sarebbero andati alla deriva."""

        source = (
            Path(gate.__file__).resolve().parent / "check_sqlserver_reference.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            'for name in SKIPPED_LIVE_TESTS for argument in ("--skip", name)', source
        )
        for name in gate.SKIPPED_LIVE_TESTS:
            self.assertEqual(
                source.count(f'"{name}"'), 1, f"{name} nominato piu di una volta"
            )


    def test_gate_runs_the_public_cli_probe_against_the_private_ca_fixture(self) -> None:
        output = (
            "test live_database_probe_sqlserver_private_ca ... ok\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out"
        )
        with patch.object(gate, "run_cargo", return_value=output) as run_cargo:
            gate.run_live_cli_probe()
        arguments = run_cargo.call_args.args[0]
        # `--features sqlserver` fa parte dell'invocazione, non e un dettaglio:
        # senza, il binario risponde `unsupported` e il probe proverebbe
        # soltanto che il provider non e stato compilato — un verde che non
        # dice nulla sul server.
        self.assertEqual(
            arguments[:7],
            [
                "test",
                "-p",
                "plenora-database-cli",
                "--features",
                "sqlserver",
                "--test",
                "live_probe",
            ],
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
        """Il ramo container: la rete si chiede, non si scrive.

        `PLENORA_SQLSERVER_GATE_HOST_CARGO` va fissata, non ereditata, perché il
        test deve descrivere il ramo scelto e non l'ambiente dell'esecutore.
        """

        with (
            patch.dict(
                gate.os.environ,
                {"PLENORA_SQLSERVER_GATE_HOST_CARGO": "0"},
                clear=False,
            ),
            patch.object(
                gate, "sqlserver_network", return_value="observed_default"
            ) as discovery,
        ):
            command, environment = gate.cargo(["test"])

        discovery.assert_called_once_with()
        self.assertIn("--network", command)
        self.assertEqual(command[command.index("--network") + 1], "observed_default")
        self.assertIsNone(environment, "il ramo container non passa un ambiente")

    def test_host_cargo_never_asks_for_a_network(self) -> None:
        """Il ramo host: cargo locale, ambiente esplicito, nessuna rete.

        E il ramo che il workflow esegue. Chiedere la rete Compose qui
        fallirebbe — il container di riferimento e raggiunto su `127.0.0.1`,
        non da dentro una rete Docker — quindi la scoperta non deve proprio
        essere invocata.
        """

        with (
            patch.dict(
                gate.os.environ,
                {"PLENORA_SQLSERVER_GATE_HOST_CARGO": "1"},
                clear=False,
            ),
            patch.object(
                gate,
                "sqlserver_network",
                side_effect=AssertionError("cargo sull'host non ha una rete Compose"),
            ) as discovery,
        ):
            command, environment = gate.cargo(["test"])

        discovery.assert_not_called()
        self.assertEqual(command, ["cargo", "test"])
        self.assertNotIn("--network", command)
        self.assertIsNotNone(environment)
        # L'ambiente e quello che i test live leggono: host, credenziali e
        # CA privata. La CA non ha `setdefault` nel gate — la posizione la
        # decide il gate, non chi lo invoca — e qui si verifica che sia
        # proprio quella.
        self.assertEqual(environment["PLENORA_SQLSERVER_HOST"], "127.0.0.1")
        self.assertEqual(environment["PLENORA_SQLSERVER_DATABASE"], "dataflow_test")
        self.assertEqual(environment["PLENORA_SQLSERVER_USER"], "dataflow")
        self.assertEqual(
            environment["PLENORA_SQLSERVER_PASSWORD"], gate.DEFAULT_PASSWORD
        )
        self.assertEqual(
            environment["PLENORA_SQLSERVER_PRIVATE_CA"], str(gate.PRIVATE_CA)
        )
        self.assertEqual(
            environment["PLENORA_SQLSERVER_MISMATCH_HOST"], "127.0.0.2"
        )

    def test_the_mismatch_address_is_published_by_the_fixture(self) -> None:
        """L'indirizzo del mismatch deve avere qualcuno in ascolto.

        La prova che quel test fa e sul **certificato**: connettersi a un
        indirizzo che il certificato non copre e vedersi rifiutare la verifica
        dell'hostname. Se li non risponde nessuno, la connessione muore prima,
        al TCP, e il test riceve `Io` invece di `Authentication` — fallisce
        dicendo la cosa giusta per la ragione sbagliata.

        Le due dichiarazioni vivono in file diversi e devono concordare: qui
        l'una interroga l'altra.
        """

        compose = (gate.ROOT / "docker-compose.sqlserver.yml").read_text(encoding="utf-8")
        with patch.dict(
            gate.os.environ,
            {"PLENORA_SQLSERVER_GATE_HOST_CARGO": "1"},
            clear=False,
        ):
            _, environment = gate.cargo(["test"])
        assert environment is not None
        mismatch = environment["PLENORA_SQLSERVER_MISMATCH_HOST"]
        self.assertIn(
            f'"{mismatch}:1433:1433"',
            compose,
            f"il fixture non pubblica {mismatch}: la prova sul certificato "
            "fallirebbe al TCP, prima di arrivare al TLS",
        )

    def test_the_mismatch_address_is_absent_from_the_certificate(self) -> None:
        """E deve restare **fuori** dal certificato, altrimenti non e un mismatch.

        L'altra meta della stessa condizione. L'indirizzo verificato ci sta
        dentro, quello del mismatch no: se qualcuno aggiungesse il secondo ai
        SAN per far passare un test, la prova continuerebbe a girare e non
        proverebbe piu niente.
        """

        extensions = (gate.ROOT / "docker" / "sqlserver" / "tls" / "server.ext").read_text(
            encoding="utf-8"
        )
        with patch.dict(
            gate.os.environ,
            {"PLENORA_SQLSERVER_GATE_HOST_CARGO": "1"},
            clear=False,
        ):
            _, environment = gate.cargo(["test"])
        assert environment is not None
        self.assertIn(
            f"IP:{environment['PLENORA_SQLSERVER_HOST']}",
            extensions,
            "l'indirizzo verificato non e fra i SAN: la prima meta del test "
            "fallirebbe sulla verifica invece di riuscire",
        )
        self.assertNotIn(
            f"IP:{environment['PLENORA_SQLSERVER_MISMATCH_HOST']}",
            extensions,
            "l'indirizzo del mismatch e fra i SAN: non e piu un mismatch",
        )


if __name__ == "__main__":
    unittest.main()
