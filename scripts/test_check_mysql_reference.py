#!/usr/bin/env python3
"""Regressioni fail-closed della fixture e del runner MySQL reference."""

from __future__ import annotations

import re
import subprocess
import tempfile
import unittest
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

GIT_ATTRIBUTES = ROOT / ".gitattributes"
COMPOSE = ROOT / "docker-compose.mysql.yml"
SERVER_EXT = ROOT / "docker" / "mysql" / "tls" / "server.ext"
GENERATOR = ROOT / "docker" / "mysql" / "tls" / "generate.sh"
GENERATOR_TEST = ROOT / "docker" / "mysql" / "tls" / "test_generate.sh"
REFERENCES = ROOT / "docker" / "mysql" / "references.json"
STATIC_WORKFLOW = ROOT / ".github" / "workflows" / "mysql-static-gate.yml"
ASSURANCE_WORKFLOW = ROOT / ".github" / "workflows" / "mysql-assurance.yml"
LIVE_TESTS = ROOT / "crates" / "plenora-db-mysql" / "src" / "live_tests.rs"
GATE = ROOT / "scripts" / "check_mysql_reference.py"
SPEC = importlib.util.spec_from_file_location("mysql_gate", GATE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("gate MySQL non importabile")
gate = importlib.util.module_from_spec(SPEC)
# Registrato prima di eseguirlo: `dataclasses` risolve le annotazioni
# cercando il modulo in `sys.modules`, e un modulo caricato a mano non ci
# finisce da solo — il decoratore fallirebbe con un `AttributeError` su
# `None` che non nomina nessuna delle due cose.
sys.modules[SPEC.name] = gate
SPEC.loader.exec_module(gate)

SDK_RUNNER = ROOT / "scripts" / "check_sdk_tests.py"
SDK_SPEC = importlib.util.spec_from_file_location("sdk_gate", SDK_RUNNER)
if SDK_SPEC is None or SDK_SPEC.loader is None:
    raise RuntimeError("runner della suite SDK non importabile")
sdk = importlib.util.module_from_spec(SDK_SPEC)
sys.modules[SDK_SPEC.name] = sdk
SDK_SPEC.loader.exec_module(sdk)

from scripts import compose_network as compose_network_module  # noqa: E402
from scripts import sdk_wheel_probe as probe  # noqa: E402
from scripts import render_state  # noqa: E402
from scripts.mysql_inventory import collect  # noqa: E402
from scripts.mysql_references import (  # noqa: E402
    BASELINE,
    COMPATIBILITY,
    REFERENCES as MATRIX,
    validate_compose_pins_the_baseline,
)


def unit_output(names: set[str]) -> str:
    return "\n".join(f"test {name} ... ok" for name in sorted(names))


def live_output(names: set[str]) -> str:
    return "\n".join(f"test {name} ... ok" for name in sorted(names))


def gate_run_cargo(
    *,
    unit: set[str] | None = None,
    live_default: set[str] | None = None,
    live_reference: set[str] | None = None,
):
    """Doppio di `run_cargo` che distingue i tre runner dagli argomenti."""

    unit = gate.EXPECTED_UNIT_TESTS if unit is None else unit
    live_default = (
        gate.EXPECTED_LIVE_DEFAULT_TESTS if live_default is None else live_default
    )
    live_reference = (
        gate.EXPECTED_LIVE_REFERENCE_TESTS
        if live_reference is None
        else live_reference
    )
    calls: list[list[str]] = []

    cli_output = (
        "test live_database_probe_mysql_private_ca ... ok\n"
        "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out"
    )

    def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
        calls.append(arguments)
        if "live_database_probe_mysql_private_ca" in arguments:
            return cli_output
        if arguments[:3] != ["test", "-p", "plenora-db-mysql"]:
            return ""
        if "--skip" in arguments:
            return unit_output(unit)
        if "--ignored" in arguments:
            return live_output(live_reference)
        return live_output(live_default)

    return calls, run_cargo



# I runner citati nelle tabelle, e la famiglia di inventario che contano.
# `RUNNER_FAMILIES` usa i nomi con cui l'inventario espone i totali nelle
# guardie; `RUNNER_FAMILIES_INTERNAL` quelli di `collect()`.
RUNNER_FAMILIES = (
    ("`cargo test -- --skip live_`", "unit"),
    ("`cargo test live_ -- --ignored`", "live reference"),
    ("`cargo test live_`", "live default"),
)
RUNNER_FAMILIES_INTERNAL = (
    ("`cargo test -- --skip live_`", "unit"),
    ("`cargo test live_`", "live_default"),
    ("`cargo test live_ -- --ignored`", "live_reference"),
)


# Un comando **invocato** in un documento porta davanti il nome del
# binario. Senza quel vincolo qualunque parola con un trattino — una
# dipendenza, un container — finirebbe per essere cercata nel dispatch.
INVOKED_SUBCOMMAND = r'plenora-database ([a-z][a-z0-9-]+)'


def current_surfaces() -> list[Path]:
    """I documenti che affermano qualcosa sullo **stato corrente**.

    Le guardie devono guardare tutto cio che un lettore prende per vero
    oggi, non il solo documento che sembra il piu ovvio: una superficie
    lasciata fuori e esattamente il posto dove la deriva sopravvive.

    Resta fuori una categoria sola: i `CHANGELOG.md`, che sono per
    costruzione un elenco di stati passati — riscriverli per allinearli al
    presente li distruggerebbe.
    """

    documents = [ROOT / "README.md"]
    documents += sorted((ROOT / "docs").rglob("*.md"))
    documents += sorted((ROOT / "crates").rglob("README.md"))
    python = ROOT / "crates" / "plenora-database-py" / "python"
    documents += sorted(python.rglob("*.py"))
    documents += sorted(python.rglob("*.pyi"))
    # Le doc Rust sono documentazione a tutti gli effetti: `cargo doc` le
    # pubblica e chi legge il crate le trova per prime, quindi appartengono
    # alla stessa guardia dei documenti Markdown.
    documents += sorted((ROOT / "crates").rglob("src/**/*.rs"))
    return [
        document
        for document in documents
        if document.name != "CHANGELOG.md"
    ]


class MysqlReferenceFixtureTests(unittest.TestCase):
    def test_repository_enforces_lf_for_portable_source_and_fixture_files(self) -> None:
        attributes = GIT_ATTRIBUTES.read_text(encoding="utf-8").splitlines()

        for suffix in (
            "*.conf",
            "*.ext",
            "*.json",
            "*.jsonl",
            "*.md",
            "*.py",
            "*.rs",
            "*.sh",
            "*.sql",
            "*.toml",
            "*.txt",
            "*.yaml",
            "*.yml",
        ):
            self.assertIn(f"{suffix} text eol=lf", attributes)

    # --- fonte di verita unica di versione e digest -----------------------

    def test_the_gate_declares_no_version_of_its_own(self) -> None:
        """Versione e digest arrivano solo da `docker/mysql/references.json`.

        Una versione ricopiata nel gate e la forma piu comune di matrice
        stale: il compose avvia una cosa, il gate ne afferma un'altra.
        """

        source = GATE.read_text(encoding="utf-8")
        for entry in MATRIX:
            self.assertNotIn(entry.exact_version, source)
            self.assertNotIn(entry.digest, source)
        self.assertEqual(gate.EXPECTED_REFERENCE, BASELINE.image)
        self.assertEqual(gate.EXPECTED_VERSION, BASELINE.exact_version)
        self.assertEqual(gate.EXPECTED_VERSION_PREFIX, BASELINE.version_prefix)

    def test_compose_pins_the_baseline_and_only_the_baseline(self) -> None:
        validate_compose_pins_the_baseline()
        compose = COMPOSE.read_text(encoding="utf-8")
        self.assertEqual(compose.count(BASELINE.image), 2)
        for entry in COMPATIBILITY:
            self.assertNotIn(entry.digest, compose)

    def test_live_tests_read_the_expected_version_from_the_same_source(self) -> None:
        """Nessun prefisso di versione scritto a mano nei test live."""

        source = LIVE_TESTS.read_text(encoding="utf-8")
        self.assertIn("include_str!", source)
        self.assertIn("references.json", source)
        for entry in MATRIX:
            self.assertNotIn(f'"{entry.version_prefix}"', source)

    def test_the_gate_propagates_the_expected_version_to_cargo(self) -> None:
        with (
            patch.dict(
                gate.os.environ,
                {"PLENORA_MYSQL_GATE_HOST_CARGO": "0"},
                clear=False,
            ),
            patch.object(gate, "mysql_tls_volume", return_value="mysql_tls"),
            patch.object(gate, "mysql_network", return_value="mysql-qualified_default"),
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
        ):
            command, _ = gate.cargo(["test"])

        self.assertIn(
            f"PLENORA_MYSQL_EXPECTED_VERSION={BASELINE.version_prefix}", command
        )

    def test_reference_identity_is_rejected_on_any_version_drift(self) -> None:
        with (
            patch.object(gate, "docker_value", side_effect=[BASELINE.image, "sha256:x"]),
            patch.object(gate, "mysql_value", return_value=BASELINE.exact_version),
        ):
            identity = gate.validate_reference()
        self.assertEqual(identity["version"], BASELINE.exact_version)

        with (
            patch.object(gate, "docker_value", side_effect=[BASELINE.image, "sha256:x"]),
            patch.object(gate, "mysql_value", return_value="8.0.46"),
        ):
            with self.assertRaisesRegex(RuntimeError, "versione MySQL inattesa"):
                gate.validate_reference()

        with (
            patch.object(
                gate, "docker_value", side_effect=["mysql@sha256:other", "sha256:x"]
            ),
            patch.object(gate, "mysql_value", return_value=BASELINE.exact_version),
        ):
            with self.assertRaisesRegex(RuntimeError, "digest di riferimento"):
                gate.validate_reference()

    # --- inventario dei test ----------------------------------------------

    def test_the_three_families_cover_the_source_and_stay_disjoint(self) -> None:
        observed = collect()
        self.assertEqual(gate.EXPECTED_UNIT_TESTS, set(observed["unit"]))
        self.assertEqual(
            gate.EXPECTED_LIVE_DEFAULT_TESTS, set(observed["live_default"])
        )
        self.assertEqual(
            gate.EXPECTED_LIVE_REFERENCE_TESTS, set(observed["live_reference"])
        )
        self.assertFalse(
            gate.EXPECTED_LIVE_DEFAULT_TESTS & gate.EXPECTED_LIVE_REFERENCE_TESTS
        )
        self.assertFalse(
            {name for name in gate.EXPECTED_UNIT_TESTS if name.startswith("live_")}
        )
        gate.validate_inventory()

    def test_a_stale_inventory_fails_the_gate_before_anything_is_started(self) -> None:
        """Un test aggiunto o rimosso deve rompere il gate, non passare zitto."""

        for family, attribute in (
            ("unit", "EXPECTED_UNIT_TESTS"),
            ("live_default", "EXPECTED_LIVE_DEFAULT_TESTS"),
            ("live_reference", "EXPECTED_LIVE_REFERENCE_TESTS"),
        ):
            declared = set(getattr(gate, attribute))
            removed = set(declared)
            removed.pop()
            with patch.object(gate, attribute, removed):
                with self.assertRaisesRegex(RuntimeError, f"inventario {family}"):
                    gate.validate_inventory()
            added = set(declared)
            added.add(f"{family}::tests::never_written")
            with patch.object(gate, attribute, added):
                with self.assertRaisesRegex(RuntimeError, f"inventario {family}"):
                    gate.validate_inventory()

    def test_the_inventory_check_runs_before_the_reference_is_started(self) -> None:
        calls: list[str] = []

        with (
            patch.object(
                gate,
                "validate_inventory",
                side_effect=lambda: calls.append("inventory"),
            ),
            patch.object(gate, "validate_fixture", side_effect=lambda: calls.append("fixture")),
            patch.object(
                gate,
                "ensure_reference_running",
                side_effect=lambda: calls.append("ensure_running"),
            ),
            patch.object(
                gate,
                "validate_reference",
                side_effect=RuntimeError("stop after reference probe"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after reference probe"):
                gate.main()

        self.assertEqual(calls, ["inventory", "fixture", "ensure_running"])

    def test_gate_pins_provider_read_hostname_verification_by_name(self) -> None:
        self.assertIn(
            "live_tests::live_provider_read_rejects_a_hostname_mismatch",
            gate.EXPECTED_LIVE_REFERENCE_TESTS,
        )
        self.assertIn(
            "live_tests::live_verified_tls_rejects_a_hostname_mismatch",
            gate.EXPECTED_LIVE_REFERENCE_TESTS,
        )
        # Il numero e un fermo, non una misura: aggiungere un test live
        # dev'essere un atto deliberato, e passare da qui e cio che lo rende
        # tale. Sei righe sono le prove di `query_stream`; le due ultime sono
        # lo stress concorrente, che questo provider non aveva — PostgreSQL
        # ce l'ha da tempo, e la lacuna non era di contratto: nessuno aveva
        # mai chiesto a MySQL di servire dodici lettori insieme.
        self.assertEqual(len(gate.EXPECTED_LIVE_REFERENCE_TESTS), 33)

    def test_gate_pins_the_query_operation_live_test_by_name(self) -> None:
        for name in (
            "live_tests::live_scalar_single_source_query_uses_prepare_metadata_as_schema",
            "live_tests::live_query_operation_executes_once_holds_lease_and_stays_demand_bounded",
            "live_tests::live_query_operation_cancellation_and_timeout_quarantine_the_session",
        ):
            self.assertIn(name, gate.EXPECTED_LIVE_REFERENCE_TESTS)

    def test_gate_pins_the_append_write_live_tests_by_name(self) -> None:
        self.assertIn(
            "live_tests::live_provider_row_diagnostics_matches_confirmed_rollback_oracle",
            gate.EXPECTED_LIVE_REFERENCE_TESTS,
        )
        self.assertEqual(
            {
                name
                for name in gate.EXPECTED_LIVE_REFERENCE_TESTS
                if "append" in name
            },
            {
                "live_tests::live_append_batch_failure_rolls_back_without_partial_rows",
                "live_tests::live_append_commits_a_single_transaction_and_reads_back_exactly",
                "live_tests::live_append_spatial_xy_preserves_srid_and_coordinates",
                "live_tests::live_append_timeout_quarantines_and_replaces_the_pooled_session",
            },
        )

    def test_gate_pins_the_v12_and_policy_live_default_tests(self) -> None:
        """I test live non `#[ignore]` hanno un runner e un inventario propri.

        Devono coincidere con la raccolta reale della `cargo test` non filtrata.
        """

        self.assertEqual(
            len(gate.EXPECTED_LIVE_DEFAULT_TESTS), len(collect()["live_default"])
        )
        self.assertIn(
            "live_tests::live_v12_write_upsert_rejects_conflicting_unique_index",
            gate.EXPECTED_LIVE_DEFAULT_TESTS,
        )
        self.assertIn(
            "live_tests::live_native_query_policy_deny_rejects_ddl",
            gate.EXPECTED_LIVE_DEFAULT_TESTS,
        )

    def test_gate_pins_the_replace_and_truncate_contract_tests(self) -> None:
        """Il contratto Replace/TruncateInsert e fissato per nome.

        Cinque test, una promessa ciascuna: identita e metadata conservati,
        target assente rifiutato, rollback su errore, rollback su
        cancellazione, e `TruncateInsert` che resta fail-closed senza toccare
        il server.
        """

        for name in (
            "live_tests::live_v12_write_replace_preserves_table_identity_and_metadata",
            "live_tests::live_v12_write_replace_on_a_missing_target_is_not_found",
            "live_tests::live_v12_write_replace_restores_the_previous_rows_when_the_stream_fails",
            "live_tests::live_v12_write_replace_restores_the_previous_rows_on_cancellation",
            "live_tests::live_v12_write_truncate_insert_rejected_without_remote_effects",
            "live_tests::live_v12_write_create_failure_leaves_the_table_and_reports_partial",
        ):
            self.assertIn(name, gate.EXPECTED_LIVE_DEFAULT_TESTS)
        # Le prove fail-closed generiche non sostituiscono i contratti più
        # specifici elencati sopra.
        for gone in (
            "live_tests::live_v12_write_replace_rejected_fail_closed",
            "live_tests::live_v12_write_truncate_insert_rejected_fail_closed",
        ):
            self.assertNotIn(gone, gate.EXPECTED_LIVE_DEFAULT_TESTS)

    def test_the_versioned_fixture_declares_the_replace_contract_target(self) -> None:
        """Il target Replace nasce nell'init versionato, non nei test.

        Il trigger richiede privilegi che l'utente della fixture non ha con il
        binlog attivo: se un test provasse a crearlo fallirebbe con 1419, e la
        tentazione sarebbe togliere il trigger dalla prova.
        """

        fixture = (ROOT / "docker" / "mysql" / "init" / "001-reference.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE TABLE replace_target", fixture)
        self.assertIn("CREATE TRIGGER replace_target_audit", fixture)
        self.assertIn("AUTO_INCREMENT", fixture)
        self.assertIn("replace_target_label_uk", fixture)
        self.assertIn("replace_target_fk", fixture)
        self.assertIn("replace_target_ck", fixture)

    def test_gate_pins_the_offline_write_plan_inventory(self) -> None:
        write_tests = {
            name
            for name in gate.EXPECTED_UNIT_TESTS
            if name.startswith("write::tests::")
        }
        self.assertEqual(
            len(write_tests),
            len([
                name
                for name in collect()["unit"]
                if name.startswith("write::tests::")
            ]),
        )
        self.assertIn(
            "write::tests::compile_and_preflight_qualify_only_xy_wkb_with_matching_srid",
            write_tests,
        )
        self.assertIn(
            "write::tests::spatial_batch_rejects_ewkb_srid_and_z_before_binding",
            write_tests,
        )
        self.assertIn(
            "write::tests::upsert_preflight_rejects_a_conflicting_extra_unique_index",
            write_tests,
        )
        self.assertIn(
            "write::tests::a_created_table_survives_the_rollback_and_every_outcome_says_so",
            write_tests,
        )
        self.assertIn(
            "provider::tests::prepare_write_rejects_unqualified_operations_before_the_network",
            gate.EXPECTED_UNIT_TESTS,
        )
        self.assertIn(
            "provider::tests::write_rejects_a_stream_schema_different_from_prepare",
            gate.EXPECTED_UNIT_TESTS,
        )
        self.assertEqual(len(gate.EXPECTED_UNIT_TESTS), len(collect()["unit"]))

    def test_gate_pins_the_read_batching_and_diagnostics_inventory(self) -> None:
        """I test offline di batching e diagnostica sono fissati per nome.

        Senza pin nominale il gate fallisce chiuso su un delta legittimo, e la
        correzione più comoda sarebbe alzare un conteggio: da lì un test
        sostituito passerebbe inosservato.
        """

        for name in (
            "read::tests::a_read_conversion_defect_publishes_the_absolute_source_index",
            "read::tests::unattributable_read_failures_never_invent_provenance",
            "read::tests::a_row_that_does_not_fit_opens_the_next_batch",
            "read::tests::cancellation_with_a_pending_row_returns_its_memory_lease",
            "read::tests::default_limits_batch_many_rows_over_four_columns",
        ):
            self.assertIn(name, gate.EXPECTED_UNIT_TESTS)

    def test_gate_pins_the_aggregate_and_distinct_live_test_by_name(self) -> None:
        self.assertIn(
            "live_tests::live_grouped_aggregate_having_bind_and_distinct_over_verified_tls",
            gate.EXPECTED_LIVE_REFERENCE_TESTS,
        )

    def test_gate_pins_the_physical_join_live_test_by_name(self) -> None:
        self.assertIn(
            "live_tests::live_physical_joins_bind_on_clauses_and_publish_outer_nullability",
            gate.EXPECTED_LIVE_REFERENCE_TESTS,
        )

    def test_gate_pins_the_scalar_window_live_test_by_name(self) -> None:
        self.assertIn(
            "live_tests::live_scalar_window_functions_publish_peer_stable_ranking_and_range_aggregates",
            gate.EXPECTED_LIVE_REFERENCE_TESTS,
        )

    # --- runner -------------------------------------------------------------

    def test_gate_runs_three_serialized_suites_and_the_cli_probe(self) -> None:
        calls, run_cargo = gate_run_cargo()

        with (
            patch.object(gate, "validate_inventory"),
            patch.object(gate, "validate_fixture"),
            patch.object(gate, "ensure_reference_running"),
            patch.object(gate, "validate_reference", return_value={}),
            patch.object(gate, "run_cargo", side_effect=run_cargo),
        ):
            self.assertEqual(gate.main(), 0)

        suites = [
            arguments
            for arguments in calls
            if arguments[:3] == ["test", "-p", "plenora-db-mysql"]
        ]
        self.assertEqual(len(suites), 3)
        unit, live_default, live_reference = suites
        self.assertIn("--skip", unit)
        self.assertNotIn("--ignored", live_default)
        self.assertIn("--test-threads=1", live_default)
        self.assertIn("--ignored", live_reference)
        self.assertIn("--test-threads=1", live_reference)
        cli_call = next(
            arguments
            for arguments in calls
            if "live_database_probe_mysql_private_ca" in arguments
        )
        self.assertEqual(cli_call[:3], ["test", "-p", "plenora-database-cli"])
        self.assertIn("live_probe", cli_call)
        self.assertIn("--exact", cli_call)
        self.assertIn("--ignored", cli_call)
        # Senza la feature l'adapter MySQL non entra nel binario e il probe
        # verifica solo che il provider non sia stato compilato.
        self.assertEqual(
            cli_call[cli_call.index("--features") + 1], "mysql"
        )

    def test_gate_rejects_unit_test_count_drift(self) -> None:
        calls, run_cargo = gate_run_cargo(
            unit={f"offline::{index}" for index in range(105)}
        )

        with (
            patch.object(gate, "validate_inventory"),
            patch.object(gate, "validate_fixture"),
            patch.object(gate, "ensure_reference_running"),
            patch.object(gate, "validate_reference", return_value={}),
            patch.object(gate, "run_cargo", side_effect=run_cargo),
        ):
            expected = len(collect()["unit"])
            with self.assertRaisesRegex(
                RuntimeError, f"eseguiti 105, attesi {expected}"
            ):
                gate.main()

        self.assertFalse(
            any("--ignored" in arguments for arguments in calls),
            "il gate deve fermarsi prima dei test live",
        )

    def test_gate_rejects_same_count_unit_test_name_substitution(self) -> None:
        substituted = set(gate.EXPECTED_UNIT_TESTS)
        substituted.remove("read::tests::invalid_batch_size_is_rejected_before_io")
        substituted.add("read::tests::same_count_replacement")
        _calls, run_cargo = gate_run_cargo(unit=substituted)

        with (
            patch.object(gate, "validate_inventory"),
            patch.object(gate, "validate_fixture"),
            patch.object(gate, "ensure_reference_running"),
            patch.object(gate, "validate_reference", return_value={}),
            patch.object(gate, "run_cargo", side_effect=run_cargo),
        ):
            with self.assertRaisesRegex(RuntimeError, "inventario test unit"):
                gate.main()

    def test_gate_rejects_same_count_live_test_name_substitution(self) -> None:
        for family, names, message in (
            (
                "live_default",
                set(gate.EXPECTED_LIVE_DEFAULT_TESTS),
                "inventario test live default",
            ),
            (
                "live_reference",
                set(gate.EXPECTED_LIVE_REFERENCE_TESTS),
                "inventario test live reference",
            ),
        ):
            names.pop()
            names.add("live_tests::live_same_count_replacement")
            _calls, run_cargo = gate_run_cargo(**{family: names})

            with (
                patch.object(gate, "validate_inventory"),
                patch.object(gate, "validate_fixture"),
                patch.object(gate, "ensure_reference_running"),
                patch.object(gate, "validate_reference", return_value={}),
                patch.object(gate, "run_cargo", side_effect=run_cargo),
            ):
                with self.assertRaisesRegex(RuntimeError, message):
                    gate.main()

    # --- TLS: CA privata e hostname ---------------------------------------

    def test_host_cargo_default_is_covered_by_the_server_certificate(self) -> None:
        source = GATE.read_text(encoding="utf-8")
        server_ext = SERVER_EXT.read_text(encoding="utf-8")

        self.assertIn('setdefault("PLENORA_MYSQL_HOST", "127.0.0.1")', source)
        self.assertIn("IP.1 = 127.0.0.1", server_ext)
        self.assertIn("DNS.1 = dataflow-mysql", server_ext)

    def test_host_cargo_refuses_to_run_without_the_private_ca(self) -> None:
        """Senza CA il gate non degrada a verifica indebolita: si ferma."""

        with (
            patch.dict(
                gate.os.environ,
                {"PLENORA_MYSQL_GATE_HOST_CARGO": "1"},
                clear=True,
            ),
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
        ):
            with self.assertRaisesRegex(RuntimeError, "PLENORA_MYSQL_CA obbligatoria"):
                gate.cargo(["test"])

    def test_live_tests_never_fall_back_to_trusting_the_server_certificate(
        self,
    ) -> None:
        """La fixture live esige la CA privata: nessun opt-out silenzioso."""

        source = LIVE_TESTS.read_text(encoding="utf-8")
        self.assertNotIn("MysqlCertificatePolicy::TrustServerCertificate", source)
        self.assertIn("PLENORA_MYSQL_CA", source)

    # La scoperta della rete e ora `scripts/compose_network`, condivisa con gli
    # altri due gate: i self-test intercettano l'ispezione li, dove avviene.
    # Resta verificato qui cio che e specifico di MySQL — il secondo alias.

    def test_network_discovery_requires_the_hostname_mismatch_alias(self) -> None:
        labels = '{"com.docker.compose.project":"mysql-qualified"}'
        without_alias = (
            '{"mysql-qualified_default":{"Aliases":["dataflow-mysql","mysql"]}}'
        )
        with patch.object(
            compose_network_module, "_inspect", side_effect=[labels, without_alias]
        ):
            with self.assertRaisesRegex(RuntimeError, "mysql-hostname-mismatch"):
                gate.mysql_network()

    def test_the_missing_alias_error_says_what_it_would_cost(self) -> None:
        """Il motivo viaggia con l'errore, non resta nel gate.

        Un alias mancante non e un dettaglio: senza di esso la prova TLS
        negativa fallirebbe per DNS e passerebbe per un successo.
        """

        labels = '{"com.docker.compose.project":"mysql-qualified"}'
        without_alias = (
            '{"mysql-qualified_default":{"Aliases":["dataflow-mysql","mysql"]}}'
        )
        with patch.object(
            compose_network_module, "_inspect", side_effect=[labels, without_alias]
        ):
            with self.assertRaisesRegex(RuntimeError, "errore DNS"):
                gate.mysql_network()

    def test_cargo_uses_the_observed_compose_network_of_the_running_fixture(self) -> None:
        labels = '{"com.docker.compose.project":"mysql-qualified"}'
        networks = (
            '{"mysql-qualified_default":'
            '{"Aliases":["dataflow-mysql","mysql","mysql-hostname-mismatch"]}}'
        )
        with patch.object(
            compose_network_module, "_inspect", side_effect=[labels, networks]
        ):
            self.assertEqual(gate.mysql_network(), "mysql-qualified_default")

    def test_network_discovery_rejects_a_container_without_compose_labels(self) -> None:
        with patch.object(compose_network_module, "_inspect", return_value="null"):
            with self.assertRaisesRegex(
                RuntimeError, "senza label di progetto Compose"
            ):
                gate.mysql_network()

    def test_network_discovery_rejects_missing_network_metadata(self) -> None:
        labels = '{"com.docker.compose.project":"mysql-qualified"}'
        with patch.object(
            compose_network_module, "_inspect", side_effect=[labels, "null"]
        ):
            with self.assertRaisesRegex(
                RuntimeError, "non e sulla rete mysql-qualified_default"
            ):
                gate.mysql_network()

    def test_the_tls_volume_comes_from_the_shared_discovery(self) -> None:
        """Anche il volume della CA si chiede a Docker, non si scrive."""

        mounts = (
            '[{"Destination":"/etc/mysql/tls","Name":"plenora-mysql_mysql_tls"}]'
        )
        with patch.object(compose_network_module, "_inspect", return_value=mounts):
            self.assertEqual(gate.mysql_tls_volume(), "plenora-mysql_mysql_tls")

        with patch.object(compose_network_module, "_inspect", return_value="[]"):
            with self.assertRaisesRegex(RuntimeError, "/etc/mysql/tls"):
                gate.mysql_tls_volume()

    def test_version_probe_never_exposes_a_root_password_in_docker_arguments(self) -> None:
        completed = SimpleNamespace(
            returncode=0, stdout=f"{BASELINE.exact_version}\n", stderr=""
        )
        with (
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
            patch.object(gate.subprocess, "run", return_value=completed) as run,
        ):
            self.assertEqual(gate.mysql_value("SELECT VERSION()"), BASELINE.exact_version)

        arguments = run.call_args.args[0]
        self.assertNotIn("root", arguments)
        self.assertNotIn("fixture-secret", repr(arguments))
        self.assertNotIn("input", run.call_args.kwargs)
        self.assertIn('MYSQL_PWD="$MYSQL_PASSWORD"', repr(arguments))

    def test_ca_signing_key_is_not_mounted_into_the_mysql_service(self) -> None:
        compose = COMPOSE.read_text(encoding="utf-8")
        generator = GENERATOR.read_text(encoding="utf-8")
        certgen = compose.split("  mysql-certgen:\n", 1)[1].split("\n  mysql:\n", 1)[0]
        mysql = compose.split("\n  mysql:\n", 1)[1].split("\nvolumes:\n", 1)[0]

        self.assertIn("mysql_ca_private:/ca", certgen)
        self.assertNotIn("mysql_ca_private", mysql)
        self.assertNotIn("-keyout /tls/ca.key", compose)
        self.assertNotIn("-CAkey /tls/ca.key", compose)
        self.assertIn('rm -f "$TLS_DIR/ca.key"', generator)

    def test_partial_regeneration_invokes_non_executable_generator_via_bash(self) -> None:
        generator_test = GENERATOR_TEST.read_text(encoding="utf-8")

        self.assertEqual(generator_test.count("bash /fixture/generate.sh"), 3)

    def test_the_fixture_contract_test_proves_the_mismatch_alias_is_unusable(
        self,
    ) -> None:
        """Il certificato non deve coprire l'alias della prova TLS negativa.

        Se un giorno il SAN includesse `mysql-hostname-mismatch`, i due test
        di rifiuto passerebbero da prova a tautologia inversa: fallirebbero.
        Il contratto della fixture lo verifica alla generazione.
        """

        generator_test = GENERATOR_TEST.read_text(encoding="utf-8")
        self.assertIn("-checkhost mysql-hostname-mismatch", generator_test)
        self.assertIn("-checkhost dataflow-mysql", generator_test)

    def test_root_credential_is_random_and_unused_by_the_healthcheck(self) -> None:
        compose = COMPOSE.read_text(encoding="utf-8")

        self.assertNotIn("MYSQL_ROOT_PASSWORD", compose)
        self.assertIn('MYSQL_RANDOM_ROOT_PASSWORD: "yes"', compose)
        self.assertIn("mysqladmin ping -h 127.0.0.1 --protocol=TCP -u dataflow", compose)

    def test_cargo_container_password_is_not_embedded_in_docker_arguments(self) -> None:
        with (
            patch.dict(
                gate.os.environ,
                {"PLENORA_MYSQL_GATE_HOST_CARGO": "0"},
                clear=False,
            ),
            patch.object(gate, "mysql_tls_volume", return_value="mysql_tls"),
            patch.object(gate, "mysql_network", return_value="mysql-qualified_default"),
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
        ):
            command, environment = gate.cargo(["test"])

        self.assertNotIn("fixture-secret", repr(command))
        self.assertIn("PLENORA_MYSQL_PASSWORD", command)
        self.assertIsNotNone(environment)
        self.assertEqual(environment["PLENORA_MYSQL_PASSWORD"], "fixture-secret")

    def test_host_cargo_password_is_only_passed_in_the_process_environment(self) -> None:
        with (
            patch.dict(
                gate.os.environ,
                {
                    "PLENORA_MYSQL_GATE_HOST_CARGO": "1",
                    "PLENORA_MYSQL_CA": "/tmp/mysql-ca.pem",
                },
                clear=True,
            ),
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
        ):
            command, environment = gate.cargo(["test"])

        self.assertEqual(command, ["cargo", "test"])
        self.assertNotIn("fixture-secret", repr(command))
        self.assertIsNotNone(environment)
        self.assertEqual(environment["PLENORA_MYSQL_PASSWORD"], "fixture-secret")
        self.assertEqual(environment["PLENORA_MYSQL_CA"], "/tmp/mysql-ca.pem")

    def test_host_cargo_honours_an_explicit_executable(self) -> None:
        executable = "C:/toolchains/rust-1.98/cargo.exe"
        with (
            patch.dict(
                gate.os.environ,
                {
                    "PLENORA_MYSQL_GATE_HOST_CARGO": "1",
                    "PLENORA_MYSQL_CA": r"C:\tmp\ca.pem",
                    "CARGO": executable,
                },
                clear=True,
            ),
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
        ):
            command, _ = gate.cargo(["fmt", "--version"])

        self.assertEqual(command, [executable, "fmt", "--version"])

    def test_every_caller_of_the_tls_generator_passes_all_its_arguments(self) -> None:
        """Un gate che non riesce ad avviarsi non e un gate.

        `docker/mysql/tls/generate.sh` dichiara i propri parametri obbligatori
        con `${N:?...}`, e ne ha quattro da quando serve piu di una fixture:
        MySQL e MariaDB hanno host diversi, e un default silenzioso emetterebbe
        per un riferimento un certificato valido per l'altro.

        La guardia verifica staticamente ogni invocazione, così un argomento
        mancante fallisce prima di avviare le fixture.
        """

        generator = (ROOT / "docker" / "mysql" / "tls" / "generate.sh").read_text(
            encoding="utf-8"
        )
        required = len(re.findall(r"\$\{(\d+):\?", generator))
        self.assertGreaterEqual(required, 4, "il generatore non dichiara i suoi parametri")

        # I chiamanti sono dichiarati, non cercati: uno nuovo che questa lista
        # non conoscesse verrebbe saltato in silenzio, che e esattamente il modo
        # in cui il difetto e passato.
        callers = {
            "scripts/check_mysql_matrix.py": r'"/fixture/generate\.sh",\n((?:\s*(?:#[^\n]*|//[^\n]*|"[^"]*",)\n)+)\s*\]',
            "docker-compose.mysql.yml": r'entrypoint: \["/bin/bash", "/fixture/generate\.sh"\]\n\s*command: \[([^\]]*)\]',
            "docker-compose.mariadb.yml": r'entrypoint: \["/bin/bash", "/fixture/generate\.sh"\]\n\s*command: \[([^\]]*)\]',
        }
        for name, pattern in callers.items():
            source = (ROOT / name).read_text(encoding="utf-8")
            matches = re.findall(pattern, source)
            self.assertTrue(matches, f"{name}: nessuna invocazione riconosciuta")
            for arguments in matches:
                passed = len(re.findall(r'"[^"]*"', arguments))
                self.assertEqual(
                    passed,
                    required,
                    f"{name}: passa {passed} argomenti a un generatore che ne "
                    f"pretende {required}",
                )

    def test_gate_source_does_not_duplicate_the_fixture_password(self) -> None:
        self.assertNotIn("DataFlow_Test_2026!", GATE.read_text(encoding="utf-8"))

    def test_fixture_gate_runs_contract_and_partial_regeneration_tests(self) -> None:
        with patch.object(gate, "run") as run:
            gate.validate_fixture()

        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(
            commands[0],
            [sys.executable, str(ROOT / "scripts" / "test_check_mysql_reference.py")],
        )
        self.assertIn(gate.EXPECTED_REFERENCE, commands[1])
        self.assertIn("/fixture/test_generate.sh", commands[1])

    # --- gate veloce su PR --------------------------------------------------

    def test_the_static_entry_point_runs_only_serverless_checks(self) -> None:
        """`--static` non deve toccare docker, cargo o la rete."""

        calls: list[list[str]] = []
        with (
            patch.object(gate, "run", side_effect=lambda command, **_: calls.append(command) or ""),
            patch.object(gate, "run_cargo", side_effect=AssertionError("cargo nel gate statico")),
            patch.object(gate, "docker_value", side_effect=AssertionError("docker nel gate statico")),
        ):
            self.assertEqual(gate.run_static_checks(), 0)

        executed = [command[-1] for command in calls]
        self.assertEqual(len(calls), 2)
        self.assertTrue(executed[0].endswith("test_check_mysql_reference.py"))
        self.assertEqual(executed[1], "tests.test_mysql_matrix")

    def test_an_unknown_flag_never_degrades_into_another_entry_point(self) -> None:
        self.assertIs(gate.selected_entry_point([]), gate.main)
        self.assertIs(gate.selected_entry_point(["--static"]), gate.run_static_checks)
        for arguments in (["--statik"], ["--static", "--full"], ["--full"]):
            with self.assertRaisesRegex(RuntimeError, "argomenti non riconosciuti"):
                gate.selected_entry_point(arguments)

    def test_the_pr_workflow_only_invokes_the_static_entry_point(self) -> None:
        """Nessuna logica del gate duplicata nello YAML del gate veloce."""

        workflow = STATIC_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("python3 scripts/check_mysql_reference.py --static", workflow)
        self.assertIn("cancel-in-progress: true", workflow)
        self.assertIn("contents: read", workflow)
        for forbidden in ("cargo ", "docker ", "openssl", "mysql -"):
            self.assertNotIn(forbidden, workflow)

    # --- campagna schedulata --------------------------------------------------

    def test_the_assurance_workflow_only_invokes_the_two_gates(self) -> None:
        """La campagna e due invocazioni: nessuna logica ricopiata nello YAML.

        Una matrice riscritta in YAML e la forma piu comune di divergenza: il
        workflow avvia una versione, lo script ne dichiara un'altra, e nessuno
        dei due sa di essere in disaccordo.
        """

        workflow = ASSURANCE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("python3 scripts/check_mysql_reference.py |", workflow)
        self.assertIn("python3 scripts/check_mysql_matrix.py |", workflow)
        for forbidden in (
            "cargo ",
            "docker run",
            "docker compose",
            "openssl",
            "--ignored",
            "mysql@sha256:",
        ):
            self.assertNotIn(forbidden, workflow)

    def test_the_assurance_workflow_is_scheduled_and_manual_only(self) -> None:
        workflow = ASSURANCE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("schedule:", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertIn("cancel-in-progress: true", workflow)
        self.assertIn("contents: read", workflow)
        self.assertNotIn("permissions: write-all", workflow)

    def test_no_tls_material_is_versioned_for_the_runner_to_reuse(self) -> None:
        """La CA e l'identita del server nascono sul runner, a ogni run.

        Un certificato versionato sarebbe una chiave privata pubblicata e una
        prova TLS che smette di provare l'emissione: il repository contiene
        solo lo script generatore e le estensioni.
        """

        tracked = subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.split()
        for entry in tracked:
            self.assertFalse(
                entry.endswith((".pem", ".key", ".crt", ".p12", ".pfx", ".jks")),
                f"materiale TLS versionato: {entry}",
            )
        self.assertEqual(
            sorted(
                entry
                for entry in tracked
                if entry.startswith("docker/mysql/tls/")
            ),
            [
                "docker/mysql/tls/generate.sh",
                "docker/mysql/tls/server.ext",
                "docker/mysql/tls/test_generate.sh",
            ],
        )

    # I conteggi non compaiono come letterali: il confronto per nome con la
    # sorgente e piu forte e non richiede costanti duplicate. Dove serve un
    # numero lo si chiede all'inventario; i documenti lo generano da li.

    def test_no_surface_names_a_subcommand_the_cli_does_not_have(self) -> None:
        """Un comando citato deve esistere nel dispatch.

        `docs/STATO.md` genera dal codice l'elenco dei comandi esistenti, ma
        non puo impedire alle altre superfici di citarne uno inesistente. In
        quel caso il lettore arriverebbe soltanto a `usage()`.

        L'autorita e `COMMAND_CATALOGUE`, non una frase e nemmeno i rami del
        match: fra quelli ci sono anche gli arm che traducono il nome di un
        provider, e con quelli `plenora-database sqlite` sarebbe passato per
        un comando legittimo.
        """

        implemented = {name for name, _ in render_state.cli_subcommands()}
        self.assertGreaterEqual(len(implemented), 40, "catalogo CLI non trovato")

        for document in current_surfaces():
            text = document.read_text(encoding="utf-8")
            # Conta solo cio che il documento presenta **come comando**, cioe
            # preceduto dal nome del binario: `mysql-async` e una dipendenza e
            # `dataflow-mysql` un container, e nessuno dei due va cercato nel
            # dispatch.
            for name in sorted(set(re.findall(INVOKED_SUBCOMMAND, text))):
                self.assertIn(
                    name,
                    implemented,
                    f"{document.relative_to(ROOT).as_posix()} cita "
                    f"'{name}', che il CLI non implementa",
                )

    def test_every_documented_docker_volume_exists_in_a_compose(self) -> None:
        """Un volume citato nei documenti deve essere quello vero.

        I volumi sono prefissati dal progetto Compose: dopo una rinomina, ogni
        nome documentato deve continuare a risolversi in un file Compose.
        """

        legitimate = set()
        for compose in sorted(ROOT.glob("docker-compose.*.yml")):
            text = compose.read_text(encoding="utf-8")
            project = next(
                line.removeprefix("name:").strip()
                for line in text.splitlines()
                if line.startswith("name:")
            )
            in_volumes = False
            for line in text.splitlines():
                if line.startswith("volumes:"):
                    in_volumes = True
                    continue
                if in_volumes:
                    if line.startswith((" ", "\t")) and line.strip():
                        legitimate.add(f"{project}_{line.strip().rstrip(':')}")
                    elif line.strip():
                        in_volumes = False
        self.assertGreaterEqual(len(legitimate), 6, "volumi Compose non trovati")

        # La nota di migrazione deve poter nominare il volume del **vecchio**
        # progetto: e il punto della nota, e non e una deriva.
        legacy = {"database-tools_mysql_data"}

        # Il suffisso deve essere un segmento intero, delimitato da `_`:
        # senza quel vincolo `tokio_postgres_rustls` passava per un volume,
        # perche finisce per "tls".
        pattern = re.compile(
            r"\b[a-z][a-z0-9-]*_(?:mysql|postgres|sqlserver)"
            r"[a-z0-9_]*_(?:data|tls|certs|private)\b"
        )
        for document in current_surfaces():
            text = document.read_text(encoding="utf-8")
            for name in sorted(set(pattern.findall(text))):
                self.assertIn(
                    name,
                    legitimate | legacy,
                    f"{document.relative_to(ROOT).as_posix()} cita il volume "
                    f"'{name}', che nessun Compose dichiara",
                )

    # ------------------------------------------------------------------
    # Capability del SDK MySQL: il binding esiste, e un test lo esercita.
    #
    # Ognuna di queste e stata dichiarata presente mentre nessun test la
    # esercitava. Il terzo lato di prima — "un documento la nomina" —
    # presidiava la prosa, e non c'e piu: le capability le elenca
    # `docs/STATO.md`, generato dalle dichiarazioni.
    MYSQL_SDK_CAPABILITIES = (
        (
            "SessionContext",
            ("context: Option<crate::session_context_py::PySessionContext>",),
            "test_begin_carries_a_session_context",
        ),
        (
            "OLTP con begin",
            ("fn begin",),
            "test_begin_commits_and_rolls_back",
        ),
        (
            "read Arrow",
            ("fn read", "fn aread"),
            "test_read_streams_arrow_ipc",
        ),
        (
            "copy_from bulk",
            ("fn copy_from", "fn acopy_from"),
            "test_copy_from_appends_rows",
        ),
        (
            "builder AST",
            ("fn execute_portable_rows", "fn execute_portable_count"),
            "test_ast_builders_select_insert_update_delete",
        ),
    )

    def test_the_mysql_sdk_capabilities_exist_and_are_tested(self) -> None:
        """Per ogni capability: il binding esiste, e un test la esercita.

        `begin`, `copy_from`, `read` e i builder AST sono stati elencati fra i
        "non inclusi" mentre erano gia implementati, e `begin(context=...)` e
        stato esposto pur essendo impossibile — il core impone un punto nella
        chiave, MySQL lo vietava. Una capability senza copertura live e una
        promessa che nessuno ha mai visto mantenere.
        """

        sync = (
            ROOT / "crates" / "plenora-database-py" / "src" / "session_family.rs"
        ).read_text(encoding="utf-8")
        asynchronous = (
            ROOT / "crates" / "plenora-database-py" / "src" / "async_session_family.rs"
        ).read_text(encoding="utf-8")
        binding = sync + asynchronous
        tests = (
            ROOT
            / "crates"
            / "plenora-database-py"
            / "python"
            / "tests"
            / "test_mysql_capabilities.py"
        ).read_text(encoding="utf-8")

        for name, symbols, test in self.MYSQL_SDK_CAPABILITIES:
            for symbol in symbols:
                self.assertIn(
                    symbol,
                    binding,
                    f"capability '{name}': '{symbol}' assente dal binding MySQL",
                )
            # Con la parentesi: `def x` combacia anche con `def x_altro`,
            # quindi rinominare un test lo farebbe sparire senza rumore.
            self.assertIn(
                f"def {test}(",
                tests,
                f"capability '{name}' senza copertura live ({test})",
            )

    def test_the_mysql_sdk_is_exposed_both_sync_and_async(self) -> None:
        """Le due varianti esistono, e i test le esercitano entrambe.

        Un binding async che resta indietro rispetto al sync e invisibile a
        chi legge la documentazione, che parla di "stessa superficie".
        """

        native = (
            ROOT
            / "crates"
            / "plenora-database-py"
            / "python"
            / "plenora_database"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("def connect_mysql(", native)
        self.assertIn("async def aconnect_mysql(", native)

        tests = (
            ROOT
            / "crates"
            / "plenora-database-py"
            / "python"
            / "tests"
            / "test_mysql_capabilities.py"
        ).read_text(encoding="utf-8")
        for expected in (
            "def test_async_begin_with_context_and_policy(",
            "def test_aread_streams_arrow_ipc(",
            "def test_acopy_from_appends_rows(",
            "def test_async_ast_builders(",
        ):
            self.assertIn(expected, tests, f"variante async scoperta: {expected}")

    def test_the_mysql_session_context_accepts_the_keys_the_core_produces(
        self,
    ) -> None:
        """Le due validazioni della chiave di context non possono divergere.

        Il core impone `namespace.name`; una regola locale che vietasse il
        punto rifiuterebbe **ogni** chiave valida, e la capability
        risulterebbe pubblicata e inutilizzabile. Il provider deve delegare.
        """

        transaction = (
            ROOT / "crates" / "plenora-db-mysql" / "src" / "transaction.rs"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "session_context::validate_context_key",
            transaction,
            "MySQL non delega al core la validazione della chiave di context",
        )
        # Il quoting resta, ma non e cio che ha sbloccato il caso: il
        # server accetta `@plenora_ctx_app.tenant` anche senza backtick. A
        # rifiutare le chiavi era la regola locale del provider, e la
        # delega al core sopra e la riga che conta. I backtick rendono la
        # resa indipendente da come la regola del core evolvera.
        self.assertIn("SET @`{CONTEXT_VARIABLE_PREFIX}{name}`", transaction)
        # E deve starci: 64 caratteri meno il prefisso. Il core ne ammette
        # 63, quindi la fascia 53..63 e valida per il core e impossibile per
        # il server — va chiusa prima di aprire la transazione.
        self.assertIn("const MAX_USER_VARIABLE_NAME: usize = 64;", transaction)
        self.assertIn(
            "MAX_USER_VARIABLE_NAME - CONTEXT_VARIABLE_PREFIX.len()", transaction
        )
        self.assertIn(
            "validate_context_keys(options)?;",
            transaction,
            "il context va validato prima di qualunque statement",
        )
        # Prima di `SET TRANSACTION`, non dopo `START TRANSACTION`.
        self.assertLess(
            transaction.index("validate_context_keys(options)?;"),
            transaction.index("SET SESSION TRANSACTION ISOLATION LEVEL"),
            "la validazione del context segue il primo statement",
        )

    def test_the_published_description_and_the_bindings_agree(self) -> None:
        """La descrizione del pacchetto elenca esattamente i motori raggiungibili.

        E' il testo che finisce sull'indice del pacchetto, dove nessuno legge
        le note: se promette un motore che non c'e, qualcuno installa e scopre
        il vuoto; se ne tace uno che c'e, qualcuno non prova nemmeno.

        La corrispondenza è bidirezionale: nessun motore promesso senza factory
        e nessuna factory pubblica taciuta dalla descrizione.
        """

        native = (
            ROOT
            / "crates"
            / "plenora-database-py"
            / "python"
            / "plenora_database"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        pyproject = (ROOT / "crates" / "plenora-database-py" / "pyproject.toml").read_text(
            encoding="utf-8"
        )
        description = next(
            line for line in pyproject.splitlines() if line.startswith("description =")
        )

        #: motore -> (come si chiama la factory, come si scrive nella descrizione)
        engines = {
            "postgres": ("def connect(", "Postgres"),
            "mysql": ("def connect_mysql(", "MySQL"),
            "mariadb": ("def connect_mariadb(", "MariaDB"),
            "sqlserver": ("def connect_sqlserver(", "SQL Server"),
        }

        for engine, (factory, advertised) in engines.items():
            has_binding = factory in native
            is_advertised = advertised in description
            self.assertEqual(
                has_binding,
                is_advertised,
                f"{engine}: binding={has_binding} descrizione={is_advertised} — "
                f"la descrizione del pacchetto e le factory non coincidono. "
                f"{description}",
            )

        # Almeno una factory deve esserci, o il confronto sarebbe vero a vuoto
        # su un pacchetto senza binding e senza descrizione.
        self.assertTrue(
            any(factory in native for factory, _ in engines.values()),
            "nessuna factory riconosciuta: la guardia non sta guardando nulla",
        )

    def test_the_mysql_tls_default_is_documented_as_verifying(self) -> None:
        """Nessuna doc puo dire che il default MySQL non verifica.

        Firma e documentazione devono entrambe descrivere il default `require`;
        nessuna nota può suggerire `TrustServerCertificate` in sua assenza.
        """

        source = (
            ROOT / "crates" / "plenora-database-py" / "src" / "session_family.rs"
        ).read_text(encoding="utf-8")
        # Il default resta `require`, nella firma pyo3.
        self.assertIn('tls_mode="require"', source)
        signature = source.index("pub fn connect_mysql(")
        doc = source[max(0, signature - 1400) : signature]
        self.assertNotIn(
            "Se `None`, usa `TrustServerCertificate`",
            doc,
            "la doc di connect_mysql descrive un default che non verifica",
        )

    def test_every_compose_declares_its_own_project(self) -> None:
        """Un progetto Compose per file, altrimenti si cancellano a vicenda.

        Senza `name:` Compose deriva il progetto dalla directory: i quattro
        compose finiscono nello stesso, e `down --remove-orphans` su uno
        rimuove i container degli altri provider. Il fallimento successivo e
        muto — i test live dell'altro provider falliscono su `Connect`, senza
        alcun indizio che l'host sia semplicemente sparito.
        """

        declared = {}
        for compose in sorted(ROOT.glob("docker-compose.*.yml")):
            lines = compose.read_text(encoding="utf-8").splitlines()
            names = [
                line.removeprefix("name:").strip()
                for line in lines
                if line.startswith("name:")
            ]
            self.assertEqual(
                len(names), 1, f"{compose.name} non dichiara un progetto Compose"
            )
            declared[compose.name] = names[0]

        self.assertGreaterEqual(len(declared), 4)
        self.assertEqual(
            len(set(declared.values())),
            len(declared),
            f"progetti Compose non distinti: {declared}",
        )

    def test_the_migration_note_lists_every_container_the_composes_declare(
        self,
    ) -> None:
        """La procedura di migrazione non puo dimenticare un container.

        A collidere sono i `container_name`, che sono fissi: se la procedura ne
        omette uno — `dataflow-sqlserver-certgen` e `dataflow-sqlserver-init`
        mancavano — chi la segue trova un conflitto al primo `up` del provider
        dimenticato, molto dopo aver creduto la migrazione conclusa.

        L'elenco si confronta con i Compose, non con la memoria di chi scrive.
        """

        declared = set()
        for compose in sorted(ROOT.glob("docker-compose.*.yml")):
            declared |= set(
                re.findall(
                    r"^\s*container_name:\s*(\S+)\s*$",
                    compose.read_text(encoding="utf-8"),
                    re.MULTILINE,
                )
            )
        self.assertTrue(declared, "nessun container_name dichiarato nei Compose")

        note = (ROOT / "docs" / "operativo.md").read_text(encoding="utf-8")
        body = note.split("**Migrazione (una tantum).**", 1)[1].split("\n## ", 1)[0]
        listed = {
            token
            for token in re.findall(r"dataflow-[a-z0-9-]+", body)
            if not token.endswith("-")
        }
        self.assertEqual(
            listed,
            declared,
            f"procedura disallineata: mancanti={sorted(declared - listed)}, "
            f"inattesi={sorted(listed - declared)}",
        )

    def test_the_migration_note_never_tells_anyone_to_delete_volumes(self) -> None:
        """La migrazione dei progetti Compose non tocca i volumi.

        A collidere sono i `container_name`, che sono fissi; i volumi sono
        prefissati dal progetto e convivono. Una procedura che include
        `docker volume rm` distrugge dati per un problema che non esiste, e
        non e reversibile.
        """

        note = (ROOT / "docs" / "operativo.md").read_text(encoding="utf-8")
        migration = note.split("**Migrazione (una tantum).**", 1)
        self.assertEqual(len(migration), 2, "nota di migrazione assente")
        body = migration[1].split("\n## ", 1)[0]
        for line in body.splitlines():
            if line.strip().startswith("docker volume rm"):
                self.fail(f"la migrazione cancella volumi: {line.strip()}")
        self.assertIn("Non** cancellare i volumi", body)

    def test_the_hardening_gate_reaches_both_postgres_references(self) -> None:
        """Il gate hardening interroga due riferimenti in progetti distinti.

        Il gate usa l'helper condiviso per collegarsi a entrambe le reti.
        """

        source = (ROOT / "scripts" / "check_postgres_hardening.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("compose_network_arguments", source)
        self.assertNotIn("appartengono a progetti Compose diversi", source)
        self.assertIn('"dataflow-postgres", "dataflow-postgres-tls"', source)

    def test_no_runner_hardcodes_a_compose_network(self) -> None:
        """La rete Compose si scopre, non si scrive.

        I compose dichiarano progetti distinti, quindi `<progetto>_default`
        cambia con il progetto. Un runner che lo scrive a mano si rompe in
        silenzio: il container c'e, la rete c'e, ma il gate finisce altrove e
        fallisce con un errore di trasporto che non nomina la causa.

        Le reti private create dal gate stesso — le matrici — sono esentate:
        non appartengono a nessun progetto Compose, e il loro nome non finisce
        in `_default`.
        """

        # Solo i runner: i `test_*.py` contengono nomi di rete come dati di
        # prova, ed e proprio la scoperta che verificano.
        for source in sorted((ROOT / "scripts").glob("*.py")):
            if source.name.startswith("test_"):
                continue
            for line in source.read_text(encoding="utf-8").splitlines():
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                # Il nome di una rete Compose e `<progetto>_default` come
                # stringa intera; un identificatore che finisce per
                # `_default` dentro un nome di test non lo e.
                for token in re.findall(r'"([^"]*)"', stripped):
                    # Un nome di rete Compose e `<progetto>_default` con un
                    # progetto davanti: `live_default` e una famiglia di test,
                    # non una rete.
                    if not token.endswith("-tools_default") and not re.fullmatch(
                        r"[a-z0-9][a-z0-9._-]*-[a-z0-9._-]*_default", token
                    ):
                        continue
                    # `f"{project}_default"` **e** la scoperta: costruisce il
                    # nome dal progetto letto dalle label. E il letterale
                    # senza interpolazione a essere un nome scritto a mano.
                    self.assertIn(
                        "{",
                        token,
                        f"{source.name} scrive a mano una rete Compose: {token}",
                    )

    def test_no_runner_removes_orphan_containers(self) -> None:
        """Nessun gate passa `--remove-orphans`.

        I nomi progetto distinti tolgono la causa, questo toglie l'arma: anche
        se un compose perdesse il suo `name:`, nessun runner del repository
        potrebbe cancellare i container di un altro provider.
        """

        sources = sorted((ROOT / "scripts").glob("*.py")) + sorted(
            (ROOT / ".github" / "workflows").glob("*.yml")
        )
        for source in sources:
            # Questo file nomina il flag per vietarlo: e l'unica occorrenza
            # legittima in codice eseguibile.
            if source.resolve() == Path(__file__).resolve():
                continue
            # Solo le righe eseguibili. Un commento che spiega **perche** il
            # flag non c'e e la documentazione della regola, non una sua
            # violazione: vietare anche quello obbligherebbe a togliere la
            # spiegazione insieme al flag e renderebbe invisibile il motivo
            # della regola.
            executable = "\n".join(
                line
                for line in source.read_text(encoding="utf-8").splitlines()
                if not line.lstrip().startswith("#")
            )
            self.assertNotIn(
                "--remove-orphans",
                executable,
                f"{source.name} puo cancellare container di altri provider",
            )

    def test_the_reference_matrix_document_is_the_only_place_with_digests(
        self,
    ) -> None:
        document = REFERENCES.read_text(encoding="utf-8")
        for entry in MATRIX:
            self.assertIn(entry.digest, document)
            self.assertIn(entry.exact_version, document)


# Le credenziali che i compose dichiarano per i quattro riferimenti che la suite
# SDK interroga. Sono esattamente quelle che il runner deve **chiedere ai
# container**, e quelle che non deve contenere.
SDK_FIXTURE_VARIABLES = {
    "dataflow-postgres": ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"),
    "dataflow-mysql": ("MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"),
    "dataflow-mariadb": ("MARIADB_USER", "MARIADB_PASSWORD", "MARIADB_DATABASE"),
    "dataflow-sqlserver": (
        "PLENORA_TEST_USER",
        "MSSQL_SA_PASSWORD",
        "PLENORA_TEST_DATABASE",
    ),
}


def compose_declarations(pattern: str) -> list[str]:
    """I valori dichiarati dai compose per le chiavi che `pattern` nomina."""

    values: list[str] = []
    for compose in sorted(ROOT.glob("docker-compose.*.yml")):
        for line in compose.read_text(encoding="utf-8").splitlines():
            match = re.match(rf"^\s*({pattern}):\s*(\S.*)$", line)
            if match:
                values.append(match.group(2).strip().strip('"').strip("'"))
    return values


# Il wheel esce dalla build in una directory temporanea e vi resta: il
# comando dei test lo nomina, quindi i doppi lo nominano allo stesso modo.
#
# La versione non e ripetuta qui: e maturin a comporre il nome del wheel da
# `pyproject.toml`, quindi i doppi la leggono dalla stessa fonte. Scritta a
# mano sarebbe rimasta indietro al primo bump, e un doppio che nomina una
# versione che non esiste piu somiglia troppo a uno che nomina quella giusta.
SDK_VERSION = sdk.toml_version(
    sdk.PYPROJECT.read_text(encoding="utf-8"), "project"
)
SDK_WHEEL_NAME = f"plenora_database-{SDK_VERSION}-cp310-abi3-linux_x86_64.whl"
SDK_ARTIFACT = {
    "wheel": SDK_WHEEL_NAME,
    "wheel_sha256": "a" * 64,
    "native_sha256": "b" * 64,
    "cli": {
        "binary": "plenora-database",
        "sha256": "c" * 64,
        "features": ["postgres"],
        "build_command": (
            "cargo build --release --locked -p plenora-database-cli "
            "--no-default-features --features postgres"
        ),
    },
    "package_path": "/usr/local/lib/python3.13/site-packages/plenora_database",
    "native_path": (
        "/usr/local/lib/python3.13/site-packages/plenora_database/_native.abi3.so"
    ),
    "maturin": "1.14.1",
    "rustc": "1.98.0",
}
SDK_VERSIONS = {
    "pandas": "3.0.5",
    "pyarrow": "25.0.1",
    "pytest": "9.1.1",
    "pytest-asyncio": "1.4.0",
    "python": "3.13.9",
}
SDK_COUNTS = {"passed": 231, "skipped": 4, "deselected": 0}
SDK_IMAGES = {
    "build": {
        "reference": "rust:1.98",
        "id": "sha256:f58923369ba2",
        "digests": ["rust@sha256:f58923369ba2"],
    },
    "test": {
        "reference": "python:3.13-slim",
        "id": "sha256:ffb752e139c0",
        "digests": ["python@sha256:ffb752e139c0"],
    },
}


# Le quattro fonti della versione, come le legge il runner. I test le
# perturbano una alla volta: e il caso reale — un bump dimenticato tocca una
# dichiarazione sola.
SDK_VERSION_SOURCES = {
    "pyproject": sdk.PYPROJECT.read_text(encoding="utf-8"),
    "cargo_toml": sdk.CARGO_MANIFEST.read_text(encoding="utf-8"),
    "cargo_lock": sdk.CARGO_LOCK.read_text(encoding="utf-8"),
    "changelog": sdk.CHANGELOG.read_text(encoding="utf-8"),
}
SDK_STALE_VERSION = "99.99.99"


def sdk_bumped_source(source: str, document: str) -> str:
    """Il documento con la **sola** versione del SDK cambiata.

    Ogni fonte la dichiara in una forma sua, e la sostituzione deve colpire
    quella e nient'altro: `Cargo.lock` porta una `version` per ogni package
    del workspace, e una replace ingenua le riscriverebbe tutte — un test che
    passa perche ha rotto tutto non prova niente.
    """

    if source == "cargo_lock":
        target = f'name = "{sdk.CARGO_PACKAGE}"\nversion = "{SDK_VERSION}"'
        replacement = f'name = "{sdk.CARGO_PACKAGE}"\nversion = "{SDK_STALE_VERSION}"'
    elif source == "changelog":
        target = f"## [{SDK_VERSION}] —"
        replacement = f"## [{SDK_STALE_VERSION}] —"
    else:
        target = f'version = "{SDK_VERSION}"'
        replacement = f'version = "{SDK_STALE_VERSION}"'
    if document.count(target) != 1:
        raise AssertionError(
            f"{source}: la versione non compare una volta sola come '{target}'"
        )
    return document.replace(target, replacement)


def sdk_pytest_command(scope: str) -> list[str]:
    """Il comando della suite, con la directory temporanea gia risolta."""

    return sdk.pytest_command(
        scope=scope, artifacts=Path("/tmp/plenora-artifacts"), wheel=SDK_WHEEL_NAME
    )


def sdk_suite_output(
    scope: str,
    *,
    passed: int | None = None,
    deselected: int | None = None,
    skips: dict[str, int] | None = None,
) -> tuple[str, str]:
    """Output e riepilogo di una corsa, per default a contratto rispettato.

    Ogni parametro sovrascrive una voce del contratto: e il modo di scrivere
    "questa corsa e come quella attesa, tranne che...".
    """

    contract = sdk.SCOPE_CONTRACTS[scope]
    passed = contract.passed if passed is None else passed
    deselected = contract.deselected if deselected is None else deselected
    skips = dict(contract.skips) if skips is None else skips

    lines = [
        f"SKIPPED [{count}] tests/test_{index}.py:{10 + index}: {reason}"
        for index, (reason, count) in enumerate(sorted(skips.items()))
    ]
    parts = [f"{passed} passed"]
    if sum(skips.values()):
        parts.append(f"{sum(skips.values())} skipped")
    if deselected:
        parts.append(f"{deselected} deselected")
    summary = f"{', '.join(parts)} in 1.23s"
    return "\n".join([*lines, summary, ""]), summary


class PythonSdkRunnerTests(unittest.TestCase):
    """`scripts/check_sdk_tests.py`: cosa costruisce, con cosa, e cosa dichiara.

    Il runner e l'unico modo supportato di eseguire la suite SDK, quindi
    quello che promette deve essere verificabile: l'albero pulito prima di
    partire, l'ambiente tracciato, l'artefatto identificato — e importato da
    dove e stato installato — e nessuna credenziale ricopiata dentro.
    """

    def test_the_runner_copies_no_fixture_credential(self) -> None:
        """Nessun literal della fixture nel sorgente del runner.

        Una password ricopiata e una seconda fonte per un dato che ne ha una
        sola: al primo cambio del compose il gate fallisce in autenticazione,
        cioe con l'errore di un server configurato male invece che di un
        runner stale. Vale anche per utente e database, che identificano la
        fixture tanto quanto la password.
        """

        source = SDK_RUNNER.read_text(encoding="utf-8")
        # I nomi dei container **sono** nel runner, per costruzione: sono
        # l'indirizzo a cui chiedere il resto. Vanno tolti prima di cercare
        # i valori, perche li contengono come sottostringa.
        for name in compose_declarations("container_name"):
            source = source.replace(name, "<container>")
        values = compose_declarations(
            "POSTGRES_USER|POSTGRES_PASSWORD|POSTGRES_DB"
            "|MYSQL_USER|MYSQL_PASSWORD|MYSQL_DATABASE"
            "|MARIADB_USER|MARIADB_PASSWORD|MARIADB_DATABASE"
            "|PLENORA_TEST_USER|MSSQL_SA_PASSWORD|PLENORA_TEST_DATABASE"
        )
        # Senza questa riga la guardia passerebbe anche a mani vuote: un
        # compose rinominato, e il ciclo sotto non gira nemmeno una volta.
        self.assertGreaterEqual(
            len(values), 12, "i compose non dichiarano piu le credenziali attese"
        )
        for value in values:
            self.assertNotIn(
                value,
                source,
                "il runner ricopia una credenziale della fixture invece di "
                "chiederla al container",
            )

    def test_the_runner_reads_both_references_from_their_containers(self) -> None:
        asked: list[tuple[str, str]] = []

        def observed(container: str, variable: str) -> str:
            asked.append((container, variable))
            return f"<{variable}>"

        with patch.object(sdk, "container_variable", side_effect=observed):
            environment = sdk.live_environment(cli="/artifacts/plenora-database")

        expected = {
            (container, variable)
            for container, variables in SDK_FIXTURE_VARIABLES.items()
            for variable in variables
        }
        self.assertEqual(set(asked), expected)
        joined = " ".join(environment)
        self.assertIn("password=<POSTGRES_PASSWORD>", joined)
        self.assertIn("dbname=<POSTGRES_DB>", joined)
        self.assertIn("PLENORA_TEST_MYSQL_PASSWORD=<MYSQL_PASSWORD>", joined)
        self.assertIn("PLENORA_TEST_MYSQL_DATABASE=<MYSQL_DATABASE>", joined)
        self.assertIn("PLENORA_TEST_MARIADB_PASSWORD=<MARIADB_PASSWORD>", joined)
        self.assertIn("PLENORA_TEST_SQLSERVER_USER=<PLENORA_TEST_USER>", joined)
        self.assertIn(
            f"PLENORA_TEST_SQLSERVER_HOST={sdk.SQLSERVER_TLS_HOST}", joined
        )
        self.assertNotEqual(sdk.SQLSERVER_TLS_HOST, sdk.SQLSERVER_CONTAINER)

    def test_the_offline_scope_asks_the_references_nothing(self) -> None:
        """Senza server non si chiedono ne reti ne credenziali."""

        def refuse(*_args: object, **_kwargs: object) -> str:
            raise AssertionError("scope offline: nessuna domanda ai riferimenti")

        with (
            patch.object(sdk, "container_variable", side_effect=refuse),
            patch.object(sdk, "compose_network_arguments", side_effect=refuse),
            patch.object(sdk, "compose_volume", side_effect=refuse),
        ):
            command = sdk_pytest_command("offline")

        self.assertNotIn("--network", command)
        self.assertNotIn("-e", command)

    def test_the_build_is_locked_and_pins_a_maturin_the_package_declares(
        self,
    ) -> None:
        source = SDK_RUNNER.read_text(encoding="utf-8")
        self.assertIn("maturin build --release --locked", source)

        pyproject = (
            ROOT / "crates" / "plenora-database-py" / "pyproject.toml"
        ).read_text(encoding="utf-8")
        requirements = (ROOT / "requirements-sdk-build.txt").read_text(
            encoding="utf-8"
        )
        pinned = sdk.validate_maturin_pin(pyproject, requirements)
        self.assertEqual(sdk.pinned_versions(requirements)["maturin"], pinned)

        # E il vincolo del pacchetto a comandare, in entrambe le direzioni.
        for outside in ("maturin==1.6.0", "maturin==2.0.0"):
            with self.assertRaises(RuntimeError):
                sdk.validate_maturin_pin(pyproject, outside)

    def test_the_suite_environment_is_pinned_and_checked_against_the_container(
        self,
    ) -> None:
        """I pin dichiarati devono essere quelli installati, tutti e soli.

        Un pin che nessuno installa e una dichiarazione, non un vincolo: il
        file resterebbe fermo mentre il container risolve tutt'altro, e il
        verdetto registrerebbe versioni che nessuno ha imposto.
        """

        requirements = ROOT / "requirements-sdk-tests.txt"
        self.assertIn(
            f"pip install -q -r /repo/{requirements.name}",
            sdk_pytest_command("offline")[-1],
            "il container di test non installa dai pin versionati",
        )

        pins = sdk.pinned_versions(requirements.read_text(encoding="utf-8"))
        for package in ("pytest", "pytest-asyncio", "pyarrow", "pandas"):
            self.assertIn(package, pins)
        sdk.validate_installed_pins(dict(pins))

        with self.assertRaises(RuntimeError):
            sdk.validate_installed_pins({**pins, "pyarrow": "1.0.0"})
        with self.assertRaises(RuntimeError):
            sdk.validate_installed_pins(
                {name: version for name, version in pins.items() if name != "pandas"}
            )
        with self.assertRaises(RuntimeError):
            sdk.validate_installed_pins({**pins, "requests": "2.32.0"})
        with self.assertRaises(RuntimeError):
            sdk.pinned_versions("pytest>=8.0")

    def test_the_verdict_identifies_the_artifact_and_the_environment(self) -> None:
        """Il nome del wheel non identifica nulla: e lo stesso a ogni build."""

        recorded = sdk.verdict(
            scope="live",
            commit="c" * 40,
            dirty=[],
            artifact=SDK_ARTIFACT,
            images=SDK_IMAGES,
            versions=SDK_VERSIONS,
            counts=SDK_COUNTS,
            summary=" 231 passed, 4 skipped ",
        )

        self.assertEqual(recorded["git_commit"], "c" * 40)
        self.assertEqual(recorded["artifact"]["wheel_sha256"], "a" * 64)
        self.assertEqual(recorded["artifact"]["native_sha256"], "b" * 64)
        self.assertEqual(recorded["pytest"], "231 passed, 4 skipped")
        # La riga di riepilogo e per chi legge; i conteggi sono cio che il
        # contratto ha verificato, e si confrontano senza interpretarla.
        self.assertEqual(recorded["counts"], SDK_COUNTS)
        self.assertEqual(
            set(recorded["versions"]),
            {
                "maturin",
                "pandas",
                "pyarrow",
                "pytest",
                "pytest_asyncio",
                "python",
                "rustc",
            },
        )
        self.assertEqual(recorded["versions"]["maturin"], "1.14.1")
        self.assertEqual(recorded["versions"]["rustc"], "1.98.0")

        # Il bench confronta due artefatti, quindi il verdetto ne identifica
        # due: del CLI servono anche le feature — decidono quali provider
        # sono dentro il binario — e il comando che le ha chieste.
        cli = recorded["artifact"]["cli"]
        self.assertEqual(cli["binary"], "plenora-database")
        self.assertEqual(cli["sha256"], "c" * 64)
        self.assertEqual(cli["features"], ["postgres"])
        self.assertIn("--no-default-features", cli["build_command"])
        self.assertIn("--locked", cli["build_command"])

        # Un tag e mutabile: senza id e digest il verdetto direbbe "rust:1.98"
        # e non quale rust:1.98.
        self.assertEqual(recorded["images"]["build"]["reference"], "rust:1.98")
        self.assertTrue(recorded["images"]["build"]["id"].startswith("sha256:"))
        self.assertTrue(recorded["images"]["test"]["digests"])

    def test_the_verdict_of_a_clean_tree_is_the_only_authoritative_one(self) -> None:
        """`authoritative` segue l'albero, non la buona volonta di chi esegue.

        Associare il risultato a HEAD non basta: se l'albero non coincide con
        HEAD, quel nome descrive altro codice. Il verdetto deve dirlo nel
        campo che si legge per primo.
        """

        def recorded(dirty: list[str]) -> dict[str, object]:
            return sdk.verdict(
                scope="live",
                commit="c" * 40,
                dirty=dirty,
                artifact=SDK_ARTIFACT,
                images=SDK_IMAGES,
                versions=SDK_VERSIONS,
                counts=SDK_COUNTS,
                summary="231 passed",
            )

        clean = recorded([])
        self.assertIs(clean["authoritative"], True)
        self.assertIs(clean["worktree_dirty"], False)
        self.assertEqual(clean["worktree_changes"], [])

        dirty = recorded([" M crates/plenora-database-py/src/lib.rs", "?? nuovo.rs"])
        self.assertIs(dirty["authoritative"], False)
        self.assertIs(dirty["worktree_dirty"], True)
        self.assertEqual(
            dirty["worktree_changes"],
            [" M crates/plenora-database-py/src/lib.rs", "?? nuovo.rs"],
        )
        # Il commit resta scritto: e vero che quello era HEAD. Non e vero che
        # descrive cio che ha girato, ed e per questo che i due campi
        # convivono.
        self.assertEqual(dirty["git_commit"], "c" * 40)

    def test_a_clean_worktree_is_the_precondition_of_the_run(self) -> None:
        """Albero pulito: nessuna riga di `git status --porcelain -uall`."""

        sdk.assert_clean_worktree([])

        with patch.object(sdk, "git", return_value="") as observed:
            self.assertEqual(sdk.porcelain_entries(), [])
        self.assertEqual(observed.call_args.args[0], ["status", "--porcelain", "-uall"])

    def test_staged_unstaged_and_untracked_changes_each_refuse_the_run(self) -> None:
        """Le tre forme di divergenza sono rifiutate, e nominate.

        Nessuna delle tre e piu innocua delle altre: il wheel si costruisce
        dai file su disco, e un sorgente mai tracciato e esattamente cio che
        nessun commit puo descrivere.
        """

        for entry, kind in (
            ("M  crates/plenora-database-py/src/lib.rs", "staged"),
            (" M crates/plenora-database-py/src/lib.rs", "non staged"),
            ("?? crates/plenora-database-py/src/nuovo.rs", "untracked"),
        ):
            with patch.object(sdk, "git", return_value=f"{entry}\n"):
                entries = sdk.porcelain_entries()
            self.assertEqual(entries, [entry], kind)
            with self.assertRaises(RuntimeError) as raised:
                sdk.assert_clean_worktree(entries)
            self.assertIn(entry.split()[-1], str(raised.exception), kind)
            self.assertIn("--allow-dirty", str(raised.exception), kind)

    def test_only_allow_dirty_lets_a_dirty_tree_through(self) -> None:
        """Il default rifiuta; l'opzione esiste e non e implicita."""

        source = SDK_RUNNER.read_text(encoding="utf-8")
        self.assertIn('"--allow-dirty"', source)
        self.assertIn("if not arguments.allow_dirty:", source)
        self.assertIn("assert_clean_worktree(dirty)", source)

    def test_a_file_touched_during_the_run_fails_the_gate(self) -> None:
        """Build e test non riscrivono il repository che stanno verificando."""

        before = {"crates/plenora-database-py/src/lib.rs": "M "}
        with patch.object(sdk, "worktree_state", return_value=dict(before)):
            sdk.assert_worktree_unchanged(before, "la build del wheel")

        # Un file tracciato riscritto dalla build.
        with patch.object(
            sdk, "worktree_state", return_value={**before, "Cargo.lock": "M "}
        ):
            with self.assertRaises(RuntimeError) as raised:
                sdk.assert_worktree_unchanged(before, "la build del wheel")
        self.assertIn("Cargo.lock", str(raised.exception))
        self.assertIn("la build del wheel", str(raised.exception))

        # E un file **nuovo** lasciato dalla suite: senza `-uall` non
        # comparirebbe, e una fixture dimenticata sul disco passerebbe per
        # albero fermo.
        with patch.object(
            sdk, "worktree_state", return_value={**before, "fixture.csv": "??"}
        ):
            with self.assertRaises(RuntimeError) as raised:
                sdk.assert_worktree_unchanged(before, "l'esecuzione della suite")
        self.assertIn("fixture.csv", str(raised.exception))
        self.assertIn("l'esecuzione della suite", str(raised.exception))

        # Entrambe le fasi vanno confrontate: una sola lascerebbe scoperta
        # l'altra, ed e la corsa dei test quella che scrive file.
        source = SDK_RUNNER.read_text(encoding="utf-8")
        self.assertEqual(source.count("assert_worktree_unchanged(before,"), 2)

    def test_the_worktree_state_reads_untracked_files_too(self) -> None:
        """Lo stato confrontato durante la corsa include gli untracked."""

        def observed(arguments: list[str]) -> str:
            if arguments[0] == "status":
                self.assertEqual(arguments, ["status", "--porcelain", "-uall"])
                return "?? nuovo.rs\n M lib.rs\n"
            return "3\t1\tlib.rs\n"

        with patch.object(sdk, "git", side_effect=observed):
            state = sdk.worktree_state()

        self.assertIn("nuovo.rs", state)
        self.assertEqual(state["nuovo.rs"], "??")
        self.assertEqual(state["lib.rs"], " M+3-1")

    def test_the_suite_imports_the_package_only_from_the_installed_wheel(
        self,
    ) -> None:
        """Il wheel costruito deve essere quello che risponde ai test.

        Tre strade portano alla copia sorgente e nessuna e visibile nel
        risultato — i test sono gli stessi e passano uguale: un `PYTHONPATH`
        verso `python/`, una `cwd` dentro il source tree, e l'inserimento di
        `sys.path` che pytest fa risalire fino al padre di `tests/`, che e un
        package. Il comando le chiude tutte e tre.
        """

        with (
            patch.object(sdk, "container_variable", return_value="x"),
            patch.object(sdk, "compose_network_arguments", return_value=[]),
            patch.object(sdk, "compose_volume", return_value="tls"),
        ):
            command = sdk_pytest_command("live")

        script = command[-1]
        self.assertIn(f"pip install -q --no-deps /artifacts/{SDK_WHEEL_NAME}", script)
        self.assertNotIn("PYTHONPATH", script)
        # La suite gira in una directory che non contiene il package.
        self.assertIn("-w", command)
        self.assertEqual(command[command.index("-w") + 1], "/suite")
        self.assertIn(
            "cp -r /repo/crates/plenora-database-py/python/tests /suite/tests",
            script,
        )
        self.assertIn("python -m pytest tests -q -rs", script)
        # Il repository entra in sola lettura: nemmeno un test distratto puo
        # reinstallare il `.so` accanto ai sorgenti.
        self.assertIn(f"{ROOT}:/repo:ro", command)
        self.assertIn("/artifacts:ro", " ".join(command))
        # E prima di pytest si verifica da dove verrebbe l'import.
        self.assertIn("python /repo/scripts/sdk_wheel_probe.py", script)
        self.assertLess(
            script.index("sdk_wheel_probe.py"),
            script.index("python -m pytest"),
            "il probe deve girare prima della suite, non dopo",
        )

    def test_the_runner_installs_nothing_into_the_source_tree(self) -> None:
        """Il modulo nativo non viene copiato accanto ai sorgenti.

        La build esporta il wheel in una directory temporanea: una corsa fuori
        dal runner non può importare per errore un artefatto residuo.
        """

        source = SDK_RUNNER.read_text(encoding="utf-8")
        self.assertIn("NATIVE.unlink()", source)
        self.assertNotIn("cp /tmp/extracted", source)
        self.assertIn("tempfile.TemporaryDirectory", source)
        self.assertIn("maturin build --release --locked --out", source)
        self.assertEqual(sdk.ARTIFACT_MOUNT, "/artifacts")

    def test_the_probe_refuses_an_origin_outside_site_packages(self) -> None:
        """La guardia sta dentro l'interprete che eseguira i test.

        E l'unico punto in cui la domanda "da dove verrebbe l'import" ha una
        risposta certa: `sysconfig` la da per quell'interprete, mentre un
        controllo sul comando poteva solo escludere le strade note.
        """

        site = Path("/usr/local/lib/python3.13/site-packages")
        probe.assert_installed(
            "plenora_database", site / "plenora_database" / "__init__.py", [site]
        )

        source_tree = Path("/repo/crates/plenora-database-py/python")
        with self.assertRaises(RuntimeError) as raised:
            probe.assert_installed(
                "plenora_database",
                source_tree / "plenora_database" / "__init__.py",
                [site],
            )
        # Il messaggio deve dire **quale** copia avrebbe vinto: senza il
        # percorso non si distingue un source tree in sys.path da un wheel
        # mai installato.
        self.assertIn(str(source_tree), str(raised.exception))

        with self.assertRaises(RuntimeError):
            probe.module_origin("plenora_database_che_non_esiste")

        # Le directory di installazione si chiedono a sysconfig, non si
        # scrivono: sono due, e in un venv non sono quelle di sistema.
        self.assertTrue(probe.site_directories())

    def test_the_runner_rejects_a_verdict_built_on_a_source_import(self) -> None:
        """Il runner ricontrolla l'origine invece di fidarsi del probe.

        Chi rilegge il verdetto non ha visto girare il probe: i percorsi
        stanno nel JSON perche siano verificabili, e il runner li rifiuta
        alla stessa regola.
        """

        installed = (
            "/usr/local/lib/python3.13/site-packages/plenora_database "
            "/usr/local/lib/python3.13/site-packages/plenora_database/"
            "_native.abi3.so "
            f"{'b' * 64}"
        )
        origin = sdk.installed_origin(f"{sdk.ORIGIN_MARKER}{installed}\n")
        self.assertEqual(origin["native_sha256"], "b" * 64)
        self.assertIn("site-packages", origin["package_path"])

        source = (
            "/repo/crates/plenora-database-py/python/plenora_database "
            "/repo/crates/plenora-database-py/python/plenora_database/"
            "_native.abi3.so "
            f"{'b' * 64}"
        )
        with self.assertRaisesRegex(RuntimeError, "site-packages"):
            sdk.installed_origin(f"{sdk.ORIGIN_MARKER}{source}\n")

        with self.assertRaisesRegex(RuntimeError, "non interpretabile"):
            sdk.installed_origin(f"{sdk.ORIGIN_MARKER}solo-un-campo\n")
        with self.assertRaisesRegex(RuntimeError, "output senza la riga"):
            sdk.installed_origin("nessun marcatore qui\n")

    def test_the_environment_must_contain_the_wheel_under_test(self) -> None:
        """Il pacchetto sotto esame deve comparire nel `pip freeze`.

        Compare in due forme — `nome==versione` e `nome @ file://…` — e
        leggerne una sola lo renderebbe invisibile nell'altra: un
        `pip install` fallito in modo non fatale lascerebbe un ambiente
        utilizzabile, e la suite girerebbe su una copia che nessuno ha
        costruito qui.
        """

        pins = sdk.pinned_versions(
            (ROOT / "requirements-sdk-tests.txt").read_text(encoding="utf-8")
        )
        declared = " ".join(f"{name}=={version}" for name, version in pins.items())
        python = f"{sdk.PYTHON_MARKER}3.13.9"

        for artifact in (
            f"plenora-database=={SDK_VERSION}",
            f"plenora_database @ file:///artifacts/"
            f"plenora_database-{SDK_VERSION}.whl",
        ):
            output = f"{sdk.PACKAGES_MARKER}{declared} {artifact}\n{python}\n"
            versions = sdk.installed_versions(output)
            self.assertEqual(versions["pyarrow"], pins["pyarrow"])
            self.assertEqual(versions["python"], "3.13.9")
            self.assertNotIn("plenora-database", versions)

        without = f"{sdk.PACKAGES_MARKER}{declared}\n{python}\n"
        with self.assertRaisesRegex(RuntimeError, "non risulta installato"):
            sdk.installed_versions(without)

    def test_the_images_are_tracked_because_a_tag_is_not_a_pin(self) -> None:
        """La promessa e "tracciato", e il verdetto porta di cosa.

        `rust:1.98` e un tag mutabile e l'`apt-get` della build non e fissato:
        chiamare l'ambiente riproducibile prometterebbe che una seconda corsa
        lo ricostruisce identico, che nessuna misura del runner garantisce.
        """

        source = SDK_RUNNER.read_text(encoding="utf-8")
        self.assertIn("Tracciato, non riproducibile", source)

        with patch.object(sdk, "run", return_value="sha256:abc []\n") as observed:
            identity = sdk.image_identity("rust:1.98")
        self.assertEqual(observed.call_args.args[0][:3], ["docker", "image", "inspect"])
        self.assertEqual(identity["reference"], "rust:1.98")
        self.assertEqual(identity["id"], "sha256:abc")

        with patch.object(
            sdk, "run", return_value='sha256:abc ["rust@sha256:def"]\n'
        ):
            self.assertEqual(
                sdk.image_identity("rust:1.98")["digests"], ["rust@sha256:def"]
            )

    def test_the_benchmarks_have_an_option_instead_of_an_impossible_filter(
        self,
    ) -> None:
        """Il runner non inoltra argomenti: il filtro deve essere suo.

        La documentazione diceva di "usare il runner e filtrare con
        `-k benchmark`", che non era eseguibile: il runner accetta le sue
        opzioni e nient'altro.
        """

        with (
            patch.object(sdk, "container_variable", return_value="x"),
            patch.object(sdk, "compose_network_arguments", return_value=["--network", "n"]),
            patch.object(sdk, "compose_volume", return_value="tls"),
        ):
            command = sdk_pytest_command("benchmark")

        self.assertIn("pytest tests -k benchmark", command[-1])
        self.assertIn("--network", command)
        self.assertIn("-e", command)
        self.assertTrue(
            any("PLENORA_BENCH_PARITY=1" in argument for argument in command),
            "i bench di parita sono opt-in: senza la variabile si saltano",
        )

        readme = (ROOT / "crates" / "plenora-database-py" / "README.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("--benchmark-only", readme)
        self.assertNotIn(
            "-k benchmark",
            readme,
            "il README indica un filtro che il runner non accetta",
        )

    def test_the_parity_bench_runs_the_cli_this_build_produced(self) -> None:
        """Il binario del bench nasce nella corsa, e il runner ne dice il path.

        Il confronto SDK / CLI e un rapporto fra due tempi: finche il binario
        arrivava da `target/release` del repository, quel rapporto metteva
        insieme un wheel appena costruito e un eseguibile di provenienza
        ignota — nel caso osservato di tre giorni prima, di un commit che
        nessuno sapeva dire. E il percorso, scritto dentro il test, era il
        punto di mount di allora: cambiato il mount, il bench non ha piu
        trovato niente e si e saltato da solo.
        """

        bench = (
            ROOT
            / "crates"
            / "plenora-database-py"
            / "python"
            / "tests"
            / "test_benchmark_parity.py"
        ).read_text(encoding="utf-8")
        self.assertIn('CLI_BIN_ENV = "PLENORA_CLI_BIN"', bench)

        # Nessuna delle due superfici puo tornare a nominare l'albero.
        source = SDK_RUNNER.read_text(encoding="utf-8")
        for surface, text in (("bench", bench), ("runner", source)):
            self.assertNotIn(
                "target/release/plenora-database",
                text,
                f"il {surface} torna a prendere il CLI dal repository",
            )

        with (
            patch.object(sdk, "container_variable", return_value="x"),
            patch.object(sdk, "compose_network_arguments", return_value=[]),
            patch.object(sdk, "compose_volume", return_value="tls"),
        ):
            command = sdk_pytest_command("benchmark")
        self.assertIn("PLENORA_CLI_BIN=/artifacts/plenora-database", command)
        self.assertIn("/artifacts:ro", " ".join(command))

    def test_both_sides_of_the_parity_bench_speak_the_same_transport(self) -> None:
        """Il CLI del bench si collega come ci si collega il SDK.

        Il riferimento di sviluppo e plaintext per costruzione: il lato SDK
        passa da `_harness` con `insecure_local`, e il CLI ha il proprio
        interruttore. Due lati che parlano trasporti diversi non costituiscono
        un confronto valido.
        """

        tests = ROOT / "crates" / "plenora-database-py" / "python" / "tests"
        bench = (tests / "test_benchmark_parity.py").read_text(encoding="utf-8")
        harness = (tests / "_harness.py").read_text(encoding="utf-8")

        self.assertIn('CLI_INSECURE_TLS_ENV = "PLENORA_TLS_INSECURE_LOCAL"', bench)
        self.assertIn("CLI_INSECURE_TLS_ENV: \"1\"", bench)
        self.assertIn('LOCAL_TLS_MODE = "insecure_local"', harness)

        # Il nome della variabile e quello che il CLI legge davvero.
        pfm = (
            ROOT / "crates" / "plenora-database-cli" / "src" / "pfm.rs"
        ).read_text(encoding="utf-8")
        self.assertIn(
            'POSTGRES_INSECURE_LOCAL_ENV: &str = "PLENORA_TLS_INSECURE_LOCAL"', pfm
        )

    def test_the_cli_is_built_beside_the_wheel_with_declared_features(self) -> None:
        """Stesso container, stessa toolchain, stesso `Cargo.lock`.

        E feature esplicite in entrambe le direzioni: `--no-default-features`
        fa si che il verdetto dichiari cosa c'e dentro il binario invece di
        ereditare un default che puo cambiare senza che nessuno se ne accorga.
        """

        self.assertEqual(sdk.CLI_FEATURES, ("postgres", "mysql", "sqlserver"))
        self.assertEqual(
            sdk.CLI_BUILD_COMMAND,
            "cargo build --release --locked -p plenora-database-cli "
            "--no-default-features --features postgres,mysql,sqlserver",
        )

        source = SDK_RUNNER.read_text(encoding="utf-8")
        # Il comando del verdetto e quello eseguito: una seconda stringa
        # sarebbe una dichiarazione, non un fatto.
        self.assertIn('f"{CLI_BUILD_COMMAND}; "', source)
        self.assertIn('"build_command": CLI_BUILD_COMMAND,', source)
        # Il default del pacchetto deve restare quello che il gate chiede:
        # se cambia, il confronto misura un binario diverso da quello atteso.
        manifest = (
            ROOT / "crates" / "plenora-database-cli" / "Cargo.toml"
        ).read_text(encoding="utf-8")
        self.assertIn('default = ["postgres"]', manifest)

    def test_the_cli_path_may_never_fall_back_into_the_repository(self) -> None:
        """La guardia sul percorso, in entrambi i lati.

        Dentro il container: un percorso sotto il mount del repository e un
        artefatto che il gate non ha costruito, e non ha un digest nel
        verdetto. Sull'host: `TMPDIR` e una variabile d'ambiente, e se
        puntasse nell'albero il gate scriverebbe gli artefatti nel repository
        che sta verificando.
        """

        sdk.assert_cli_outside_repository("/artifacts/plenora-database")
        self.assertEqual(sdk.cli_binary_path(), "/artifacts/plenora-database")

        for refused in (
            "/repo/target/release/plenora-database",
            "/repo",
            "/usr/local/bin/plenora-database",
        ):
            with self.assertRaises(RuntimeError):
                sdk.assert_cli_outside_repository(refused)

        # E il percorso composto dalle costanti deve restare fuori: se
        # qualcuno spostasse la directory degli artefatti dentro il mount del
        # repository, il gate deve fermarsi invece di misurare.
        with patch.object(sdk, "ARTIFACT_MOUNT", "/repo/artifacts"):
            with self.assertRaisesRegex(RuntimeError, "dentro il repository"):
                sdk.cli_binary_path()

        sdk.assert_artifacts_outside_repository(Path(tempfile.gettempdir()))
        with self.assertRaisesRegex(RuntimeError, "TMPDIR"):
            sdk.assert_artifacts_outside_repository(ROOT / "target" / "staging")

    def test_the_release_version_is_the_same_in_every_source(self) -> None:
        """Quattro fonti, una versione — e ognuna decide una cosa diversa.

        `pyproject.toml` compone il nome del wheel, il `Cargo.toml` del crate
        decide cosa risponde `p.version()`, `Cargo.lock` e cio che le build
        `--locked` pretendono di ritrovare, e il CHANGELOG e quello che legge
        chi aggiorna. Due che divergono producono un artefatto che mente su se
        stesso: per esempio un nome wheel diverso dalla versione restituita dal
        modulo.
        """

        self.assertEqual(sdk.declared_version(), SDK_VERSION)
        sdk.validate_declared_versions(**SDK_VERSION_SOURCES)

    def test_a_single_stale_source_fails_the_version_check(self) -> None:
        """Ogni fonte, modificata da sola, deve far fallire.

        Da sola e la parte che conta: un bump dimenticato tocca **una**
        dichiarazione, non tutte, ed e esattamente il caso che una guardia
        scritta male lascia passare.
        """

        for source, document in SDK_VERSION_SOURCES.items():
            perturbed = dict(SDK_VERSION_SOURCES)
            perturbed[source] = sdk_bumped_source(source, document)
            self.assertNotEqual(
                perturbed[source], document, f"{source}: perturbazione inefficace"
            )
            with self.assertRaises(RuntimeError) as raised:
                sdk.validate_declared_versions(**perturbed)
            message = str(raised.exception)
            self.assertIn("divergono", message)
            # Il messaggio deve dire quale fonte dice cosa: sapere che
            # divergono senza sapere quale sia indietro non basta a
            # correggerle.
            self.assertIn(SDK_STALE_VERSION, message)
            self.assertIn(SDK_VERSION, message)

    def test_each_version_source_is_read_where_it_is_declared(self) -> None:
        """I tre parser, sulle forme che le fonti possono davvero avere."""

        self.assertEqual(
            sdk.toml_version('[project]\nversion = "1.2.3"\n', "project"), "1.2.3"
        )
        # `version.workspace = true` non e una versione: il crate del binding
        # ha per contratto un ciclo di release proprio, separato dal
        # workspace Rust, e ereditarla lo romperebbe in silenzio.
        with self.assertRaises(RuntimeError):
            sdk.toml_version("[package]\nversion.workspace = true\n", "package")
        with self.assertRaises(RuntimeError):
            sdk.toml_version("[package]\nname = 'x'\n", "package")

        lock = '[[package]]\nname = "altro"\nversion = "9.9.9"\n'
        with self.assertRaisesRegex(RuntimeError, "non compare"):
            sdk.locked_version(lock)
        self.assertEqual(
            sdk.locked_version(
                f'{lock}\n[[package]]\nname = "{sdk.CARGO_PACKAGE}"\n'
                'version = "1.2.3"\n'
            ),
            "1.2.3",
        )

        # Una sezione `[Unreleased]` non e una release e viene saltata: e il
        # modo normale di lavorare fra un rilascio e l'altro, e farla fallire
        # costringerebbe a rilasciare per poter eseguire il gate.
        self.assertEqual(
            sdk.changelog_version(
                "## [Unreleased] — 1.3.0\n\ntesto\n\n## [1.2.3] — 2026-08-17\n"
            ),
            "1.2.3",
        )
        with self.assertRaisesRegex(RuntimeError, "nessuna release"):
            sdk.changelog_version("## [Unreleased] — 1.3.0\n")

    def test_the_version_is_checked_before_anything_is_built(self) -> None:
        """Un bump incoerente si scopre prima della build, non dopo.

        Costruire wheel e CLI per poi accorgersi che il wheel dichiara una
        versione e il modulo nativo un'altra significa buttare la corsa —
        e, se nessuno guarda, pubblicarli.
        """

        source = SDK_RUNNER.read_text(encoding="utf-8")
        # Le due verifiche stanno in `preconditions`, che chi costruisce
        # chiama prima. Il test cercava le due righe adiacenti dentro `main`,
        # dove stavano finche non e esistita una campagna: la forma e
        # cambiata, la proprieta no, e a presidiarla sono ora due
        # affermazioni invece di una posizione.
        head = source.index("def preconditions()")
        tail = source.index("\ndef ", head + 1)
        for check in ("declared_version()", "validate_maturin_pin("):
            self.assertIn(
                check,
                source[head:tail],
                f"{check} non e piu dentro preconditions",
            )
        # E chi costruisce le invoca: la posizione da sola non basterebbe,
        # perche `preconditions` potrebbe esistere senza che nessuno la chiami.
        for caller in ("def preflight()", "def main()"):
            start = source.index(caller)
            # `main` e l'ultima funzione del file: senza il ripiego la ricerca
            # del prossimo `def` non trova niente, e il test fallirebbe per la
            # forma del file invece che per cio che sorveglia.
            end = source.find("\ndef ", start + 1)
            if end == -1:
                end = len(source)
            self.assertIn(
                "preconditions()",
                source[start:end],
                f"{caller} costruisce senza aver verificato le versioni",
            )

    def test_every_scope_declares_what_a_correct_run_looks_like(self) -> None:
        """I tre contratti stanno in una struttura sola, e sono coerenti.

        Un conteggio di soli `passed` non descrive una corsa: gli stessi 24
        escono da una suite che ne salta 195 e da una che ne deseleziona 195.
        """

        self.assertEqual(
            set(sdk.SCOPE_CONTRACTS), {"live", "offline", "benchmark"}
        )
        live = sdk.SCOPE_CONTRACTS["live"]
        offline = sdk.SCOPE_CONTRACTS["offline"]
        benchmark = sdk.SCOPE_CONTRACTS["benchmark"]

        self.assertEqual((live.passed, live.skipped, live.deselected), (252, 5, 0))
        self.assertEqual(
            (offline.passed, offline.skipped, offline.deselected), (37, 220, 0)
        )
        self.assertEqual(
            (benchmark.passed, benchmark.skipped, benchmark.deselected),
            (2, 0, 255),
        )
        # I due scope che girano l'intera suite ne vedono lo stesso totale:
        # il wheel standard salta Db2 anche live, e nessuno deseleziona.
        self.assertEqual(
            live.passed + live.skipped,
            offline.passed + offline.skipped,
        )
        # Il bench e un sottoinsieme della stessa suite.
        self.assertEqual(
            live.passed + live.skipped,
            benchmark.passed + benchmark.deselected,
        )

        for scope, contract in sdk.SCOPE_CONTRACTS.items():
            output, summary = sdk_suite_output(scope)
            self.assertEqual(
                sdk.assert_scope_contract(
                    scope=scope, output=output, summary=summary
                ),
                {
                    "passed": contract.passed,
                    "skipped": contract.skipped,
                    "deselected": contract.deselected,
                },
            )

    def test_an_unexpected_skip_or_deselection_fails_every_scope(self) -> None:
        """Ogni scope rifiuta uno skip in piu, e una deselezione in piu.

        Uno skip e un test che non ha risposto: di cio che doveva verificare
        non si sa niente, e salta per motivi che somigliano a un errore di
        configurazione — un binario spostato, una variabile che nessuno passa
        piu — cioe resta verde proprio quando il gate ha smesso di misurare.
        Una deselezione fa lo stesso da un'altra porta: un `-k` che
        non seleziona piu niente non e un errore per pytest.
        """

        intruso = "bench: CLI binary non trovato in /artifacts/plenora-database"

        for scope, contract in sdk.SCOPE_CONTRACTS.items():
            # Uno skip in piu, con un motivo che nessun contratto prevede.
            output, summary = sdk_suite_output(
                scope,
                passed=contract.passed - 1,
                skips={**contract.skips, intruso: 1},
            )
            with self.assertRaises(RuntimeError) as raised:
                sdk.assert_scope_contract(
                    scope=scope, output=output, summary=summary
                )
            message = str(raised.exception)
            self.assertIn(scope, message)
            # Riepilogo e motivi devono essere nel messaggio: senza, chi
            # legge deve rieseguire la suite per sapere cosa e cambiato.
            self.assertIn(summary, message)
            self.assertIn(intruso, message)
            self.assertIn(f"passed: {contract.passed - 1}", message)

            # Una deselezione in piu.
            output, summary = sdk_suite_output(
                scope, passed=contract.passed - 1, deselected=contract.deselected + 1
            )
            with self.assertRaises(RuntimeError) as raised:
                sdk.assert_scope_contract(
                    scope=scope, output=output, summary=summary
                )
            self.assertIn(
                f"deselected: {contract.deselected + 1}", str(raised.exception)
            )

            # E un test in piu che passa: la suite e cambiata, e il contratto
            # e il posto dove dirlo.
            output, summary = sdk_suite_output(scope, passed=contract.passed + 1)
            with self.assertRaisesRegex(RuntimeError, "passed"):
                sdk.assert_scope_contract(
                    scope=scope, output=output, summary=summary
                )

    def test_an_offline_skip_may_not_be_silently_substituted(self) -> None:
        """Gli skip previsti sono fissati per motivo, non solo per totale.

        Il totale e proprio cio che rende invisibile una sostituzione: uno
        skip nuovo al posto di uno atteso lascia il numero fermo. Le famiglie
        sono quelle live — Postgres, MySQL, bench opt-in — e sono il
        funzionamento di `offline`, non un difetto.
        """

        contract = sdk.SCOPE_CONTRACTS["offline"]
        self.assertEqual(
            set(contract.skips),
            {
                sdk.POSTGRES_SKIP,
                sdk.MYSQL_SKIP,
                sdk.MARIADB_SKIP,
                sdk.SQLSERVER_SKIP,
                sdk.DB2_SKIP,
                sdk.BENCH_SKIP,
            },
        )

        substituted = dict(contract.skips)
        substituted[sdk.POSTGRES_SKIP] -= 1
        substituted["non implementato su questa piattaforma"] = 1
        self.assertEqual(sum(substituted.values()), contract.skipped)

        output, summary = sdk_suite_output("offline", skips=substituted)
        with self.assertRaises(RuntimeError) as raised:
            sdk.assert_scope_contract(
                scope="offline", output=output, summary=summary
            )
        message = str(raised.exception)
        self.assertNotIn("skipped:", message, "il totale coincide: non e quello")
        self.assertIn("skip inatteso 'non implementato", message)
        self.assertIn(f"skip previsto '{sdk.POSTGRES_SKIP}'", message)

        # Una famiglia che sparisce del tutto e la stessa deriva.
        without = {
            reason: count
            for reason, count in contract.skips.items()
            if reason != sdk.MYSQL_SKIP
        }
        output, summary = sdk_suite_output(
            "offline", passed=contract.passed + contract.skips[sdk.MYSQL_SKIP],
            skips=without,
        )
        with self.assertRaisesRegex(RuntimeError, "skip previsto"):
            sdk.assert_scope_contract(
                scope="offline", output=output, summary=summary
            )

    def test_the_counts_are_read_from_the_summary_and_the_skip_lines(self) -> None:
        """I due parser che il contratto usa, sulle forme che pytest produce."""

        self.assertEqual(
            sdk.pytest_counts("24 passed, 195 skipped in 0.83s"),
            {"passed": 24, "skipped": 195},
        )
        self.assertEqual(
            sdk.pytest_counts("2 passed, 217 deselected in 1.65s"),
            {"passed": 2, "deselected": 217},
        )
        # La durata non e un conteggio.
        self.assertEqual(sdk.pytest_counts("219 passed in 7.25s"), {"passed": 219})

        # `-rs` raggruppa i parametrizzati che saltano nello stesso punto:
        # contare le righe invece di sommare gli `[N]` darebbe 2 invece di 12.
        output = (
            "SKIPPED [11] tests/test_v030_p1.py:42: live test: manca env X\n"
            "SKIPPED [1] tests/test_query.py:9: live test: manca env X\n"
            "non una riga di skip\n"
        )
        self.assertEqual(sdk.skip_reasons(output), {"live test: manca env X": 12})

        with self.assertRaisesRegex(RuntimeError, "riga di riepilogo"):
            sdk.pytest_summary("nessun riepilogo qui\n")


if __name__ == "__main__":
    unittest.main()
