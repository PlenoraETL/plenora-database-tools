#!/usr/bin/env python3
"""Gate riproducibile del riferimento MySQL 8.4 iniziale."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "dataflow-mysql"
NETWORK = "plenora-database-tools_default"
RUST_IMAGE = "rust:1.92"
EXPECTED_DIGEST = "sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb"
EXPECTED_REFERENCE = f"mysql@{EXPECTED_DIGEST}"
EXPECTED_OFFLINE_TESTS = {
    "arrow::tests::decimal_parser_is_exact_and_checked",
    "arrow::tests::zero_dates_fail_closed_without_panicking",
    "catalog::tests::schema_token_is_stable_and_sensitive",
    "config::tests::debug_redacts_credentials",
    "config::tests::driver_opts_require_tls_even_for_explicit_trust_opt_out",
    "config::tests::invalid_configuration_fails_before_io",
    "config::tests::pooled_driver_opts_reapply_bootstrap_after_connection_reset",
    "config::tests::tls_verification_is_the_default",
    "error::tests::deadline_and_requested_cancellation_have_distinct_envelopes",
    "error::tests::in_flight_timeout_still_reports_quarantine",
    "error::tests::incidental_certificate_text_in_io_stays_io",
    "error::tests::pre_session_timeout_and_cancellation_do_not_claim_quarantine",
    "error::tests::read_cancellation_is_non_retryable_and_effect_free",
    "error::tests::server_code_mappings_win_over_tls_identity_text",
    "error::tests::tls_hostname_rejection_is_distinct_from_generic_io",
    "error::tests::write_timeout_never_claims_rollback",
    "parameter::tests::binding_is_positional_and_rejects_extra_parameters",
    "parameter::tests::wkb_is_rejected_until_srid_preflight_exists",
    "pool::tests::checkout_preserves_the_independent_acquire_budget",
    "pool::tests::zero_capacity_is_rejected_without_network",
    "provider::tests::provider_surface_is_typed_and_fail_closed",
    "provider::tests::published_spatial_capabilities_match_generic_geometry_contract",
    "provider::tests::query_demands_the_having_bind_before_reaching_the_network",
    "provider::tests::query_demands_the_join_on_bind_before_reaching_the_network",
    "provider::tests::query_honours_cancellation_before_reaching_the_network",
    "provider::tests::query_keeps_unqualified_ast_fail_closed_before_the_network",
    "provider::tests::query_renders_and_binds_before_reaching_the_network",
    "query::tests::aggregate_windows_render_without_an_order_but_a_frame_requires_one",
    "query::tests::boolean_width_matches_the_catalog_read_path",
    "query::tests::count_star_is_the_only_wildcard_accepted_inside_an_aggregate",
    "query::tests::cross_database_and_oversized_identifiers_are_rejected_before_rendering",
    "query::tests::decimal_precision_is_reconstructed_and_bounded",
    "query::tests::distinct_on_is_reported_as_absent_from_mysql",
    "query::tests::distinct_ordering_must_belong_to_the_projection",
    "query::tests::distinct_projection_renders_and_orders_inside_the_projection",
    "query::tests::distinct_stays_deterministic_across_joins",
    "query::tests::geometry_and_empty_result_sets_fail_closed",
    "query::tests::group_determinism_reads_the_relation_qualifier_as_part_of_the_key",
    "query::tests::grouped_aggregates_render_with_binds_ordered_by_clause",
    "query::tests::grouped_shapes_without_a_deterministic_group_fail_closed",
    "query::tests::join_on_cannot_reference_a_relation_introduced_later",
    "query::tests::join_shapes_outside_the_qualified_subset_fail_closed",
    "query::tests::lateral_is_reported_as_not_yet_qualified_instead_of_absent",
    "query::tests::peer_stable_ranking_renders_while_total_order_windows_stay_closed",
    "query::tests::physical_joins_render_with_relation_qualified_columns_and_ordered_binds",
    "query::tests::renderer_is_the_shared_mysql_dialect",
    "query::tests::right_join_renders_while_full_join_is_reported_as_absent_from_mysql",
    "query::tests::scalar_single_source_renders_with_backticks_and_positional_binds",
    "query::tests::scalar_windows_render_over_the_final_relation_set_with_projection_binds_first",
    "query::tests::statement_metadata_maps_to_the_arrow_contract",
    "query::tests::the_on_clause_boundary_is_covered_by_the_core_and_by_the_provider",
    "query::tests::the_window_function_boundary_is_covered_by_the_core_and_by_the_provider",
    "query::tests::unqualified_ast_subsets_stay_unsupported",
    "query::tests::window_frames_are_limited_to_the_units_mysql_can_represent",
    "query::tests::window_only_functions_and_argument_counts_fail_before_the_network",
    "query::tests::window_operands_stay_row_only_scalar_and_relation_valid",
    "query::tests::windows_combined_with_grouping_or_distinct_stay_closed",
    "query::tests::windows_outside_the_projection_stay_closed",
    "read::tests::builder_capacity_is_bounded_by_the_byte_budget_before_allocation",
    "read::tests::expired_resource_deadline_maps_to_timeout",
    "read::tests::invalid_batch_size_is_rejected_before_io",
    "read::tests::reservation_fails_before_allocation_when_rows_are_exhausted",
    "read::tests::terminal_stream_error_is_sticky_instead_of_becoming_eof",
    "read::tests::valid_row_bound_reserves_cell_payload_and_buffer_growth",
    "session::tests::bootstrap_is_explicit_and_deterministic",
    "types::tests::concrete_spatial_type_produces_an_exact_valid_contract",
    "types::tests::limit_without_order_is_rejected_fail_closed",
    "types::tests::mapping_preserves_signedness_and_rejects_wide_decimal",
    "types::tests::mysql_geomcollection_alias_produces_the_canonical_exact_type",
    "types::tests::spatial_projection_is_wkb_xy_with_declared_srid",
}
EXPECTED_LIVE_TESTS = {
    "live_deadline_reports_timeout_and_quarantines_the_session",
    "live_early_stream_drop_cancels_worker_and_keeps_provider_usable",
    "live_grouped_aggregate_having_bind_and_distinct_over_verified_tls",
    "live_inflight_cancellation_quarantines_the_session",
    "live_operation_timeout_quarantines_the_session",
    "live_physical_joins_bind_on_clauses_and_publish_outer_nullability",
    "live_pool_acquire_timeout_is_independent_from_connect_timeout",
    "live_pool_reset_reapplies_deterministic_session_bootstrap",
    "live_provider_connection_capabilities_and_inspect",
    "live_query_operation_cancellation_and_timeout_quarantine_the_session",
    "live_query_operation_executes_once_holds_lease_and_stays_demand_bounded",
    "live_read_projection_filter_order_and_default_schema",
    "live_reference_probe_catalog_and_spatial_metadata",
    "live_scalar_single_source_query_uses_prepare_metadata_as_schema",
    "live_scalar_window_functions_publish_peer_stable_ranking_and_range_aggregates",
    "live_streaming_read_maps_scalar_and_xy_geometry_exactly",
    "live_variable_rows_are_not_consumed_past_the_current_batch_budget",
    "live_verified_tls_rejects_a_hostname_mismatch",
}


def run(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
    capture: bool = False,
    timeout: int = 15 * 60,
) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=False,
        text=True,
        capture_output=capture,
        timeout=timeout,
    )
    if capture:
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
    if completed.returncode:
        raise RuntimeError(f"comando fallito ({completed.returncode}): {command[0]}")
    return f"{completed.stdout}{completed.stderr}" if capture else ""


def docker_value(arguments: list[str]) -> str:
    completed = subprocess.run(
        ["docker", *arguments],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        sys.stderr.write(completed.stderr)
        raise RuntimeError("interrogazione Docker MySQL fallita")
    return completed.stdout.strip()


def fixture_password() -> str:
    environment = json.loads(
        docker_value(["inspect", "--format", "{{json .Config.Env}}", CONTAINER])
    )
    prefix = "MYSQL_PASSWORD="
    for entry in environment:
        if entry.startswith(prefix):
            password = entry.removeprefix(prefix)
            if password:
                return password
    raise RuntimeError("password utente fixture MySQL assente")


def mysql_value(statement: str) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER,
            "/bin/sh",
            "-c",
            'exec env MYSQL_PWD="$MYSQL_PASSWORD" mysql -Nse "$1" '
            "-u dataflow --ssl-mode=REQUIRED",
            "mysql-reference-probe",
            statement,
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        sys.stderr.write(completed.stderr)
        raise RuntimeError("probe SQL MySQL fallita")
    return completed.stdout.strip()


def mysql_tls_volume() -> str:
    mounts = json.loads(docker_value(["inspect", "--format", "{{json .Mounts}}", CONTAINER]))
    for mount in mounts:
        if mount.get("Destination") == "/etc/mysql/tls" and mount.get("Name"):
            return str(mount["Name"])
    raise RuntimeError("volume CA MySQL non montato nel container di riferimento")


def cargo(arguments: list[str]) -> tuple[list[str], dict[str, str] | None]:
    if os.environ.get("PLENORA_MYSQL_GATE_HOST_CARGO") == "1":
        environment = os.environ.copy()
        environment.setdefault("PLENORA_MYSQL_HOST", "127.0.0.1")
        environment.setdefault("PLENORA_MYSQL_DATABASE", "dataflow_test")
        environment.setdefault("PLENORA_MYSQL_USER", "dataflow")
        environment.setdefault("PLENORA_MYSQL_PASSWORD", fixture_password())
        if "PLENORA_MYSQL_CA" not in environment:
            raise RuntimeError("PLENORA_MYSQL_CA obbligatoria con cargo host")
        return [os.environ.get("CARGO", "cargo"), *arguments], environment

    environment = os.environ.copy()
    environment["PLENORA_MYSQL_PASSWORD"] = fixture_password()
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        NETWORK,
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        "plenora-cargo-registry:/usr/local/cargo/registry",
        "-v",
        "plenora-cargo-git:/usr/local/cargo/git",
        "-v",
        f"{mysql_tls_volume()}:/mysql-tls:ro",
        "-w",
        "/workspace",
        "-e",
        f"PLENORA_MYSQL_HOST={CONTAINER}",
        "-e",
        "PLENORA_MYSQL_DATABASE=dataflow_test",
        "-e",
        "PLENORA_MYSQL_USER=dataflow",
        "-e",
        "PLENORA_MYSQL_PASSWORD",
        "-e",
        "PLENORA_MYSQL_CA=/mysql-tls/ca.pem",
        RUST_IMAGE,
        "/usr/local/cargo/bin/cargo",
        *arguments,
    ]
    return command, environment


def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
    command, environment = cargo(arguments)
    return run(command, environment=environment, capture=capture)


def validate_fixture() -> None:
    run([sys.executable, str(ROOT / "scripts" / "test_check_mysql_reference.py")])
    run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{ROOT / 'docker' / 'mysql' / 'tls'}:/fixture:ro",
            EXPECTED_REFERENCE,
            "/bin/bash",
            "/fixture/test_generate.sh",
        ]
    )


def ensure_reference_running() -> None:
    compose = str(ROOT / "docker-compose.mysql.yml")
    run(["docker", "compose", "-f", compose, "config", "--quiet"])
    run(
        ["docker", "compose", "-f", compose, "up", "-d", "--wait", "mysql"],
        timeout=5 * 60,
    )


def validate_reference() -> dict[str, str]:
    compose = (ROOT / "docker-compose.mysql.yml").read_text(encoding="utf-8")
    if compose.count(EXPECTED_REFERENCE) != 2:
        raise RuntimeError("digest MySQL 8.4 non fissato sui due servizi")
    configured = docker_value(["inspect", "--format", "{{.Config.Image}}", CONTAINER])
    image_id = docker_value(["inspect", "--format", "{{.Image}}", CONTAINER])
    if configured != EXPECTED_REFERENCE:
        raise RuntimeError("container MySQL diverso dal digest di riferimento")
    version = mysql_value("SELECT VERSION()")
    if not version.startswith("8.4."):
        raise RuntimeError(f"versione MySQL inattesa: {version}")
    return {"configured_reference": configured, "image_id": image_id, "version": version}


def main() -> int:
    validate_fixture()
    ensure_reference_running()
    identity = validate_reference()
    run_cargo(["fmt", "--all", "--", "--check"])
    run_cargo(
        [
            "clippy",
            "-p",
            "plenora-db-mysql",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ]
    )
    offline_output = run_cargo(
        ["test", "-p", "plenora-db-mysql", "--locked"],
        capture=True,
    )
    executed_offline_tests = set(
        re.findall(r"^test ([^ ]+) \.\.\. ok$", offline_output, re.MULTILINE)
    )
    if executed_offline_tests != EXPECTED_OFFLINE_TESTS:
        missing = sorted(EXPECTED_OFFLINE_TESTS - executed_offline_tests)
        unexpected = sorted(executed_offline_tests - EXPECTED_OFFLINE_TESTS)
        raise RuntimeError(
            "inventario test offline MySQL inatteso: "
            f"eseguiti {len(executed_offline_tests)}, "
            f"attesi {len(EXPECTED_OFFLINE_TESTS)}, "
            f"mancanti={missing}, inattesi={unexpected}"
        )
    live_output = run_cargo(
        [
            "test",
            "-p",
            "plenora-db-mysql",
            "live_",
            "--locked",
            "--",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ],
        capture=True,
    )
    executed_live_tests = set(
        re.findall(r"^test live_tests::([^ ]+) \.\.\. ok$", live_output, re.MULTILINE)
    )
    if executed_live_tests != EXPECTED_LIVE_TESTS:
        raise RuntimeError(
            f"set test live MySQL inatteso: {sorted(executed_live_tests)}"
        )
    print(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed",
                "provider": "mysql",
                "reference": "MySQL 8.4 LTS",
                "offline_tests": len(EXPECTED_OFFLINE_TESTS),
                "live_tests": len(EXPECTED_LIVE_TESTS),
                "image": identity,
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"MySQL reference gate FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
