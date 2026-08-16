#!/usr/bin/env python3
"""Regressioni fail-closed della fixture e del runner MySQL reference."""

from __future__ import annotations

import subprocess
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
SPEC.loader.exec_module(gate)

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
        self.assertEqual(len(gate.EXPECTED_LIVE_REFERENCE_TESTS), 25)

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

        Prima erano invisibili: nessun runner li nominava e la loro comparsa
        nella `cargo test` nuda veniva letta come rumore.
        """

        self.assertEqual(len(gate.EXPECTED_LIVE_DEFAULT_TESTS), 29)
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
        # I nomi del vecchio contratto — Replace rifiutata, TruncateInsert
        # rifiutata senza prova di assenza di effetti — non devono
        # sopravvivere all'inventario.
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
        self.assertEqual(len(write_tests), 35)
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
            "write::tests::a_created_table_survives_the_rollback_and_the_error_says_so",
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
        self.assertEqual(len(gate.EXPECTED_UNIT_TESTS), 122)

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
            with self.assertRaisesRegex(RuntimeError, "eseguiti 105, attesi 122"):
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

    def test_network_discovery_requires_the_hostname_mismatch_alias(self) -> None:
        labels = '{"com.docker.compose.project":"mysql-qualified"}'
        without_alias = (
            '{"mysql-qualified_default":{"Aliases":["dataflow-mysql","mysql"]}}'
        )
        with patch.object(gate, "docker_value", side_effect=[labels, without_alias]):
            with self.assertRaisesRegex(RuntimeError, "mysql-hostname-mismatch"):
                gate.mysql_network()

    def test_cargo_uses_the_observed_compose_network_of_the_running_fixture(self) -> None:
        labels = '{"com.docker.compose.project":"mysql-qualified"}'
        networks = (
            '{"mysql-qualified_default":'
            '{"Aliases":["dataflow-mysql","mysql","mysql-hostname-mismatch"]}}'
        )
        with patch.object(gate, "docker_value", side_effect=[labels, networks]):
            self.assertEqual(gate.mysql_network(), "mysql-qualified_default")

    def test_network_discovery_rejects_a_container_without_compose_labels(self) -> None:
        with patch.object(gate, "docker_value", return_value="null"):
            with self.assertRaisesRegex(
                RuntimeError, "progetto Compose del riferimento MySQL assente"
            ):
                gate.mysql_network()

    def test_network_discovery_rejects_missing_network_metadata(self) -> None:
        labels = '{"com.docker.compose.project":"mysql-qualified"}'
        with patch.object(gate, "docker_value", side_effect=[labels, "null"]):
            with self.assertRaisesRegex(
                RuntimeError, "rete Compose del riferimento MySQL assente"
            ):
                gate.mysql_network()

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
        executable = "C:/toolchains/rust-1.92/cargo.exe"
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

    def test_the_reference_matrix_document_is_the_only_place_with_digests(
        self,
    ) -> None:
        document = REFERENCES.read_text(encoding="utf-8")
        for entry in MATRIX:
            self.assertIn(entry.digest, document)
            self.assertIn(entry.exact_version, document)


if __name__ == "__main__":
    unittest.main()
