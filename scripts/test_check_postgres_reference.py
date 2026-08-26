#!/usr/bin/env python3
"""Unit test del gate live PostgreSQL.

I tre test row diagnostics PostgreSQL saltano senza assert quando
`PLENORA_TEST_POSTGRES_DSN` non è impostata. Il gate li dichiara eseguiti:
questi test fissano che non possa dichiararlo senza averli visti passare.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path
from unittest.mock import patch

import check_postgres_reference as gate
from scripts import compose_network as compose_network_module
from scripts import live_inventory


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

    def test_the_declared_step_is_recorded_after_the_fixture_runs(self) -> None:
        """Il passo si registra **dopo** l'esecuzione, non prima.

        Una dichiarazione scritta prima del comando resta vera anche quando il
        comando sparisce: e la forma esatta della lista tematica che questo
        gate pubblicava.
        """

        source = self.source()
        start = source.index('for suite in ("live_f5"')
        block = source[start : source.index("ipc_materialization =", start)]
        self.assertIn('steps.append(f"cli_live_fixture_{suite}")', block)
        self.assertLess(
            block.index("run("),
            block.index("steps.append"),
            "il passo e dichiarato prima di essere eseguito",
        )

    def test_the_report_publishes_the_steps_it_ran(self) -> None:
        """Nessuna lista tematica scritta a mano nel verdetto."""

        source = self.source()
        self.assertIn('"steps": steps,', source)
        self.assertNotIn('"checks": [', source)

    def test_the_provider_suite_includes_the_ignored_tests(self) -> None:
        """I `#[ignore]` del provider devono entrare nella corsa.

        Senza `--include-ignored` restavano fuori, e il report li dichiarava
        eseguiti lo stesso.
        """
        source = self.source()
        start = source.index('"-p",\n                    "plenora-db-postgres",')
        block = source[start : source.index("validate_live_row_diagnostics", start)]
        self.assertIn("--include-ignored", block)


class LiveInventory(unittest.TestCase):
    """L'inventario dei test live si deriva dai sorgenti, non si scrive."""

    def test_the_inventory_is_read_from_the_sources_and_is_not_empty(self) -> None:
        inventory = gate.live_test_inventory()
        self.assertGreater(
            len(inventory),
            len(gate.REQUIRED_LIVE_TESTS),
            "l'inventario derivato deve coprire piu dei tre nomi pinnati",
        )
        for name in gate.REQUIRED_LIVE_TESTS:
            self.assertIn(name, inventory)
        for name in inventory:
            self.assertTrue(name.startswith("live_"), name)

    def test_a_helper_named_like_a_live_test_stays_out(self) -> None:
        """Il prefisso non basta: serve l'attributo di test.

        `fn live_dsn()` esiste in due moduli del provider ed e un helper, non
        un test: nessuna corsa puo riportarlo `ok`. Raccoglierlo rendeva il
        gate impossibile da superare — dopo l'intera suite live falliva sempre
        nominando `live_dsn`, e i test di questo file restavano verdi perche
        verificavano solo il prefisso.
        """

        self.assertNotIn("live_dsn", gate.live_test_inventory())

    def test_only_annotated_functions_enter_the_inventory(self) -> None:
        source = (
            "    fn live_dsn() -> String {\n"
            "        String::new()\n"
            "    }\n"
            "\n"
            "    // Un helper con un commento sopra resta un helper.\n"
            "    fn live_helper_commentato() -> String {\n"
            "        String::new()\n"
            "    }\n"
            "\n"
            "    #[tokio::test]\n"
            "    #[ignore]\n"
            "    async fn live_vero_test() {}\n"
            "\n"
            "    #[tokio::test]\n"
            "    #[allow(clippy::too_many_lines)]\n"
            "    // Un commento fra gli attributi e la firma non toglie il test.\n"
            "    async fn live_test_commentato() {}\n"
        )
        self.assertEqual(
            set(live_inventory.annotated_tests(source)),
            {"live_vero_test", "live_test_commentato"},
        )

    def test_a_test_with_comments_between_attributes_and_signature_is_kept(self) -> None:
        """La forma reale che una prima versione della regex aveva escluso."""

        self.assertIn(
            "live_postgis_read_when_dsn_is_available", gate.live_test_inventory()
        )

    @staticmethod
    def qualified(names: list[str]) -> list[str]:
        return [f"qualche::modulo::tests::{name}" for name in names]

    def listing(self, names: list[str]) -> str:
        return "\n".join(f"{name}: test" for name in self.qualified(names))

    def run_output(self, names: list[str]) -> str:
        return "\n".join(f"test {name} ... ok" for name in self.qualified(names))

    def test_a_complete_run_returns_the_observed_names(self) -> None:
        inventory = sorted(gate.live_test_inventory())
        self.assertEqual(
            gate.validate_live_inventory(
                self.run_output(inventory), self.listing(inventory)
            ),
            self.qualified(inventory),
        )

    def test_a_missing_live_test_fails_the_gate(self) -> None:
        """Il caso che il report non sapeva vedere: un test mai eseguito."""
        inventory = sorted(gate.live_test_inventory())
        omitted = inventory[0]
        with self.assertRaises(RuntimeError) as raised:
            gate.validate_live_inventory(
                self.run_output(inventory[1:]), self.listing(inventory)
            )
        self.assertIn(omitted, str(raised.exception))

    def test_a_test_missing_from_the_compiled_suite_fails_the_gate(self) -> None:
        """Definito nei sorgenti ma non nel binario: sparito in silenzio.

        E il caso che il confronto fra cargo e cargo non vedrebbe mai — un
        modulo non incluso, un `cfg` che lo esclude — e la ragione per cui
        l'inventario dai sorgenti resta.
        """

        inventory = sorted(gate.live_test_inventory())
        absent = inventory[0]
        with self.assertRaises(RuntimeError) as raised:
            gate.validate_live_inventory(
                self.run_output(inventory[1:]), self.listing(inventory[1:])
            )
        self.assertIn(absent, str(raised.exception))
        self.assertIn("assenti dalla suite", str(raised.exception))

    def test_a_failed_live_test_does_not_count_as_executed(self) -> None:
        inventory = sorted(gate.live_test_inventory())
        observed = self.run_output(inventory[1:])
        observed += f"\ntest qualche::modulo::tests::{inventory[0]} ... FAILED"
        with self.assertRaises(RuntimeError):
            gate.validate_live_inventory(observed, self.listing(inventory))

    def test_homonymous_tests_do_not_satisfy_each_other(self) -> None:
        """Due test con lo stesso nome foglia sono due prove, non una.

        Il confronto avviene sui nomi completi: eseguirne uno non puo bastare
        per entrambi. Con un `set` di nomi foglia bastava.
        """

        listing = "primo::tests::live_omonimo: test\nsecondo::tests::live_omonimo: test"
        listed = gate.listed_live_tests(listing)
        self.assertEqual(len(listed), 2)
        executed = set(live_inventory.EXECUTED.findall("test primo::tests::live_omonimo ... ok"))
        self.assertEqual(sorted(listed - executed), ["secondo::tests::live_omonimo"])

    def test_the_listing_ignores_what_is_not_a_live_test(self) -> None:
        listing = (
            "arrow::tests::decimal_parser_is_exact_and_checked: test\n"
            "test_suite::tests::live_vero: test\n"
        )
        self.assertEqual(gate.listed_live_tests(listing), {"test_suite::tests::live_vero"})

    def test_an_empty_listing_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "non ha elencato"):
            gate.listed_live_tests("arrow::tests::qualcosa: test")

    def test_the_tests_the_reference_cannot_qualify_are_declared(self) -> None:
        """Girano e riportano `ok`, ma non provano niente: vanno dichiarati.

        PF.2 e PF.3 leggono `POSTGRES_URL_BARE` — un PostgreSQL senza PostGIS
        — e senza quella variabile stampano una riga di skip e ritornano.
        `cargo test` li conta comunque fra i passati, quindi il report li
        avrebbe presentati come prove del ramo negativo della probe.

        La lista sta nel gate e finisce nel verdetto: toglierla di li
        significa aver aggiunto il fixture bare, non aver risolto il problema.
        """

        self.assertEqual(
            set(gate.NON_QUALIFYING_LIVE_TESTS),
            {
                "preflight_pf2_capability_negative_reports_no_postgis",
                "preflight_pf3_spatial_query_without_postgis_fails_cleanly",
                "live_private_ca_mtls_and_cancellation_when_configured",
            },
        )
        # Ogni voce deve dire **quale** fixture manca: una dichiarazione senza
        # motivo e una scusa.
        for reason in gate.NON_QUALIFYING_LIVE_TESTS.values():
            self.assertTrue(
                "POSTGRES_URL_BARE" in reason or "TLS" in reason,
                reason,
            )

        source = (
            Path(gate.__file__).resolve().parent / "check_postgres_reference.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            '"declared_not_qualifying"',
            source,
            "il verdetto non dichiara i test che non qualificano",
        )

    def test_the_declared_names_exist_in_the_sources(self) -> None:
        """Una dichiarazione su un test che non esiste piu non dichiara nulla."""

        # I nomi possono stare sia negli integration test sia nei moduli di
        # `src/`: il test TLS vive li.
        paths = list((gate.ROOT / "crates" / "plenora-db-postgres" / "tests").rglob("*.rs"))
        paths += list((gate.ROOT / "crates" / "plenora-db-postgres" / "src").rglob("*.rs"))
        source = "\n".join(path.read_text(encoding="utf-8") for path in paths)
        for name in gate.NON_QUALIFYING_LIVE_TESTS:
            self.assertIn(f"fn {name}(", source, name)

    def test_the_observed_name_is_matched_outside_test_suite_too(self) -> None:
        """La forma precedente ancorava a `test_suite::tests::`.

        I test live degli altri moduli non erano nemmeno osservabili: qualunque
        inventario costruito su quel pattern li avrebbe dichiarati mancanti.
        """
        name = sorted(gate.live_test_inventory())[0]
        self.assertIn(
            f"transaction::tests::{name}",
            live_inventory.EXECUTED.findall(f"test transaction::tests::{name} ... ok"),
        )


class LiveTestsMustMeasure(unittest.TestCase):
    """Una prova live che salta in silenzio si dichiara passata.

    Quindici prove di `test_suite.rs` cominciavano cosi:

        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };

    Senza DSN si concludevano subito, e `cargo test` le contava fra quelle
    passate. Una prova che non ha toccato nessun server e indistinguibile, nel
    resoconto, da una che lo ha attraversato — e il runner non puo vedere la
    differenza, perche una prova che rientra subito stampa `... ok` come le
    altre.

    Il difetto non era attivo: i tre gate la DSN la impostano, quindi quelle
    prove giravano. Era silenzioso, che e peggio. Il giorno in cui il nome
    della variabile cambiasse, o la risoluzione del nome del container
    saltasse, la matrice delle versioni direbbe «cinque major su cinque, tutto
    passato» avendo misurato soltanto i test unitari, che non aprono una
    connessione.

    Queste due guardie tengono ferme le due meta del rimedio: che nessuna prova
    torni alla forma silenziosa, e che i gate continuino a dichiarare di
    pretendere la misura.
    """

    ROOT = Path(__file__).resolve().parents[1]
    SUITE = ROOT / "crates" / "plenora-db-postgres" / "src" / "test_suite.rs"
    GATES = (
        "scripts/check_postgres_reference.py",
        "scripts/check_postgres_hardening.py",
        "scripts/check_postgres_matrix.py",
    )

    #: Il gate che ha la fixture a CA privata, e che percio puo pretendere la
    #: prova mTLS. Gli altri due la dichiarano «non qualificante», ed e giusto:
    #: i loro compose sono plaintext.
    TLS_GATE = "scripts/check_postgres_hardening.py"

    def test_no_live_test_reads_the_dsn_without_the_loud_helper(self) -> None:
        source = self.SUITE.read_text(encoding="utf-8")
        direct = re.findall(
            r'let Ok\(\w+\) = std::env::var\("PLENORA_TEST_POSTGRES_DSN"\)',
            source,
        )
        self.assertEqual(
            direct,
            [],
            "una prova legge la DSN senza passare da live_dsn_or_skip: "
            "tornerebbe a saltare in silenzio",
        )
        self.assertIn(
            "fn live_dsn_or_skip",
            source,
            "l'helper che rende rumoroso il salto e sparito",
        )
        self.assertIn("PLENORA_REQUIRE_LIVE_POSTGRES", source)

        # Il materiale TLS aveva la stessa forma, su variabili diverse, ed
        # era sfuggito alla prima stesura di questa guardia proprio per
        # quello: si cercava un nome, non una forma.
        diretto_tls = re.findall(
            r'std::env::var\(\"PLENORA_TEST_POSTGRES_TLS_\w+\"\)\s*,?\s*\n\s*\)\s*else',
            source,
        )
        self.assertEqual(
            diretto_tls,
            [],
            "una prova legge il materiale TLS senza passare da "
            "live_tls_material_or_skip: tornerebbe a saltare in silenzio",
        )
        self.assertIn("fn live_tls_material_or_skip", source)

    def test_the_tls_gate_demands_the_tls_measure(self) -> None:
        source = (self.ROOT / self.TLS_GATE).read_text(encoding="utf-8")
        self.assertIn(
            "PLENORA_REQUIRE_LIVE_POSTGRES_TLS=1",
            source,
            "il gate con la fixture TLS non pretende la prova mTLS",
        )

    def test_every_gate_demands_the_measure(self) -> None:
        for name in self.GATES:
            source = (self.ROOT / name).read_text(encoding="utf-8")
            self.assertIn(
                "PLENORA_REQUIRE_LIVE_POSTGRES=1",
                source,
                f"{name}: non pretende che le prove live misurino",
            )


if __name__ == "__main__":
    unittest.main()
