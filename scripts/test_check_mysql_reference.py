#!/usr/bin/env python3
"""Regressioni fail-closed della fixture e del runner MySQL reference."""

from __future__ import annotations

import re
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

from scripts import compose_network as compose_network_module  # noqa: E402
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



def current_surfaces() -> list[Path]:
    """I documenti che affermano qualcosa sullo **stato corrente**.

    Le guardie devono guardare tutto cio che un lettore prende per vero
    oggi, non la sola tabella di `docs/mysql/README.md`: una superficie
    lasciata fuori e esattamente il posto dove la deriva sopravvive.

    Restano fuori due categorie, e solo quelle:

    * `docs/history/`, che registra cio che era vero allora;
    * i `CHANGELOG.md`, che sono per costruzione un elenco di stati
      passati — riscriverli per allinearli al presente li distruggerebbe.
    """

    documents = [ROOT / "README.md"]
    documents += sorted((ROOT / "docs").rglob("*.md"))
    documents += sorted((ROOT / "crates").rglob("README.md"))
    python = ROOT / "crates" / "plenora-database-py" / "python"
    documents += sorted(python.rglob("*.py"))
    documents += sorted(python.rglob("*.pyi"))
    return [
        document
        for document in documents
        if "/docs/history/" not in document.as_posix()
        and document.name != "CHANGELOG.md"
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

    def test_the_replace_contract_is_not_generalised_to_sql_server(self) -> None:
        """Il contratto Replace vale per PostgreSQL e MySQL, non per SQL Server.

        SQL Server usa ancora staging + publish con tabella di backup: il
        target viene ricreato. Una frase che assegna la stessa semantica ai
        tre provider farebbe pianificare su una garanzia che uno dei tre non
        offre.
        """

        interfaces = (ROOT / "docs" / "interfaces.md").read_text(encoding="utf-8")
        self.assertIn("non ha la stessa semantica su tutti i provider", interfaces)
        self.assertIn("staging + `RENAME`/publish", interfaces)
        # La riga della tabella deve differenziare la colonna SQL Server.
        row = next(
            line for line in interfaces.splitlines() if line.startswith("| `Replace` |")
        )
        self.assertIn("staging + publish", row)

    # I conteggi non compaiono piu come letterali in questo file: ogni test
    # aggiunto ne avrebbe richiesti quattro aggiornamenti, e il valore che
    # portavano era gia coperto dal confronto per nome con la sorgente. Dove
    # serve un numero, lo si chiede all'inventario. L'unico posto dove i
    # conteggi restano scritti a mano e la tabella di `docs/mysql/README.md`,
    # perche un documento deve mostrarli — ed e presidiata dal test qui sotto.

    def test_the_documented_test_counts_match_the_inventory(self) -> None:
        """I conteggi in `docs/mysql/README.md` seguono la sorgente.

        Una tabella di numeri scritti a mano invecchia al primo test aggiunto,
        e il lettore non ha modo di accorgersene: e la stessa classe di deriva
        che il gate previene sull'inventario, applicata alla documentazione.
        """

        observed = collect()
        readme = (ROOT / "docs" / "mysql" / "README.md").read_text(encoding="utf-8")
        for runner, family in (
            ("`cargo test -- --skip live_`", "unit"),
            ("`cargo test live_`", "live_default"),
            ("`cargo test live_ -- --ignored`", "live_reference"),
        ):
            row = next(
                (line for line in readme.splitlines() if runner in line),
                None,
            )
            self.assertIsNotNone(row, f"riga assente per {runner}")
            documented = row.rsplit("|", 2)[1].strip()
            self.assertEqual(
                documented,
                str(len(observed[family])),
                f"conteggio {family} in docs/mysql/README.md non allineato",
            )

    def test_no_document_still_claims_mysql_supports_truncate_insert(self) -> None:
        """`TruncateInsert` non deve comparire fra le mode MySQL disponibili.

        L'elenco delle mode viene ricopiato in piu documenti: basta che uno
        resti indietro perche un consumer pianifichi su una modalita che il
        provider rifiuta in prepare.
        """

        # Solo elenchi di mode: sono affermazioni di supporto e non possono
        # comparire dentro una negazione. Frasi come "non esistono piu staging
        # persistenti" descrivono legittimamente cio che e stato rimosso, e un
        # match sul solo sostantivo le colpirebbe.
        stale = (
            "Append/Create/TruncateInsert/Upsert/DeleteByKeys/Update",
            "Append + Create + TruncateInsert",
            "tutti 7 WriteMode",
            "Tutti 7 WriteMode",
            "tutti 7 modi qualificati",
        )
        for document in current_surfaces():
            text = document.read_text(encoding="utf-8")
            for claim in stale:
                self.assertNotIn(
                    claim,
                    text,
                    f"{document.relative_to(ROOT).as_posix()} ripete '{claim}'",
                )

    def test_no_surface_states_a_test_count_the_inventory_contradicts(
        self,
    ) -> None:
        """Nessun documento corrente puo dichiarare conteggi diversi.

        La tabella di `docs/mysql/README.md` era presidiata, ma gli stessi
        numeri comparivano anche altrove — `PROVIDER-MATURITY-MATRIX.md`
        affermava "129 test live" e "123 unit" — e nessuno se ne accorgeva.
        Un conteggio scritto in un documento non presidiato invecchia al
        primo test aggiunto, e il lettore non ha modo di distinguerlo da uno
        aggiornato.
        """

        observed = collect()
        totals = {
            "unit": len(observed["unit"]),
            "live default": len(observed["live_default"]),
            "live reference": len(observed["live_reference"]),
        }
        live_total = totals["live default"] + totals["live reference"]

        for document in current_surfaces():
            where = document.relative_to(ROOT).as_posix()
            text = document.read_text(encoding="utf-8")

            for match in re.finditer(
                r"\*\*(\d+) (unit|live default|live reference)\*\*", text
            ):
                self.assertEqual(
                    int(match.group(1)),
                    totals[match.group(2)],
                    f"{where} dichiara '{match.group(0)}', inventario "
                    f"{totals[match.group(2)]}",
                )

            # "N test live" e ambiguo fra i provider: vale solo dove il
            # contesto parla di MySQL.
            for match in re.finditer(r"(\d+) test live", text):
                window = text[max(0, match.start() - 400) : match.end() + 400]
                if "ysql" not in window:
                    continue
                self.assertEqual(
                    int(match.group(1)),
                    live_total,
                    f"{where} dichiara '{match.group(0)}', inventario "
                    f"{live_total} (default + reference)",
                )

    def test_every_mysql_cli_command_is_documented_and_every_documented_one_exists(
        self,
    ) -> None:
        """I sub-comandi MySQL del CLI e quelli citati nei documenti coincidono.

        La deriva va in due direzioni, e nessuna delle due e visibile a chi
        legge: un comando aggiunto al CLI e mai documentato resta
        introvabile — `mysql-conditional-update` lo e stato — e un comando
        documentato ma inesistente manda il lettore contro un `usage()`.
        """

        main = (
            ROOT / "crates" / "plenora-database-cli" / "src" / "main.rs"
        ).read_text(encoding="utf-8")
        implemented = set(re.findall(r'"(mysql-[a-z-]+)" =>', main))
        self.assertGreaterEqual(len(implemented), 9, "dispatch CLI non trovato")

        readme = (ROOT / "docs" / "mysql" / "README.md").read_text(encoding="utf-8")
        documented = set(re.findall(r"\bmysql-[a-z][a-z-]*\b", readme))
        self.assertEqual(
            implemented - documented,
            set(),
            "sub-comandi MySQL assenti da docs/mysql/README.md",
        )

        # Il numero dichiarato accanto all'elenco deve essere quello vero.
        declared = re.search(r"\*\*CLI\*\* \((\d+) sub-comandi", readme)
        self.assertIsNotNone(declared, "conteggio sub-comandi CLI assente")
        self.assertEqual(
            int(declared.group(1)),
            len(implemented),
            "conteggio sub-comandi CLI non allineato al dispatch",
        )

        # Nessuna superficie corrente puo citare un comando inesistente.
        for document in current_surfaces():
            text = document.read_text(encoding="utf-8")
            for name in set(re.findall(r"\bmysql-[a-z][a-z-]*\b", text)):
                if not name.startswith(
                    (
                        "mysql-probe",
                        "mysql-describe",
                        "mysql-inspect",
                        "mysql-execute",
                        "mysql-transaction",
                        "mysql-conditional",
                    )
                ):
                    continue
                self.assertIn(
                    name,
                    implemented,
                    f"{document.relative_to(ROOT).as_posix()} cita "
                    f"'{name}', che il CLI non implementa",
                )

    def test_no_surface_still_says_mysql_is_absent_from_the_python_sdk(self) -> None:
        """Il SDK Python espone MySQL: nessun documento puo negarlo.

        `connect_mysql` / `aconnect_mysql` esistono, con `begin`, `read`,
        `copy_from` e i builder AST. Chi leggeva "non ancora esposti al SDK
        Python" concludeva di dover passare dal CLI o dal driver Rust, e la
        conclusione era sbagliata.
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

        stale = (
            "MySQL / SQL Server: driver Rust presenti",
            "non ancora esposti al SDK Python",
            "v0.4-alpha, scaffold",
            "SDK Python MySQL v0.4-alpha scaffold",
            "Solo Postgres",
        )
        for document in current_surfaces():
            text = document.read_text(encoding="utf-8")
            for claim in stale:
                self.assertNotIn(
                    claim,
                    text,
                    f"{document.relative_to(ROOT).as_posix()} ripete '{claim}'",
                )

    def test_every_documented_docker_volume_exists_in_a_compose(self) -> None:
        """Un volume citato nei documenti deve essere quello vero.

        I volumi sono prefissati dal progetto Compose: rinominato il
        progetto, il vecchio nome resta scritto nei documenti e chi lo cerca
        non lo trova — `plenora-database-tools_postgres_tls_certs` era
        sopravvissuto cosi in `docs/postgres/HARDENING.md`.
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

        pattern = re.compile(
            r"\b[a-z][a-z0-9-]*_(?:mysql|postgres|sqlserver)"
            r"[a-z0-9_]*(?:data|tls|certs|private)\b"
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

        readme = (ROOT / "docs" / "mysql" / "README.md").read_text(encoding="utf-8")
        body = readme.split("**Migrazione (una tantum).**", 1)[1].split("## ", 1)[0]
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

        readme = (ROOT / "docs" / "mysql" / "README.md").read_text(encoding="utf-8")
        migration = readme.split("**Migrazione (una tantum).**", 1)
        self.assertEqual(len(migration), 2, "nota di migrazione assente")
        body = migration[1].split("## ", 1)[0]
        for line in body.splitlines():
            if line.strip().startswith("docker volume rm"):
                self.fail(f"la migrazione cancella volumi: {line.strip()}")
        self.assertIn("Non** cancellare i volumi", body)

    def test_the_hardening_gate_reaches_both_postgres_references(self) -> None:
        """Il gate hardening interroga due riferimenti in progetti distinti.

        Prima pretendeva che condividessero il progetto Compose, e la
        separazione dei progetti lo ha rotto: ora si attacca a entrambe le
        reti tramite l'helper condiviso.
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
            # legittima nel repository.
            if source.resolve() == Path(__file__).resolve():
                continue
            self.assertNotIn(
                "--remove-orphans",
                source.read_text(encoding="utf-8"),
                f"{source.name} puo cancellare container di altri provider",
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
