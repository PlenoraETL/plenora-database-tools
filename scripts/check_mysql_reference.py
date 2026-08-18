#!/usr/bin/env python3
"""Gate riproducibile del riferimento MySQL baseline.

La versione e il digest del riferimento non sono scritti qui: arrivano da
`docker/mysql/references.json`, unica fonte di verita della matrice. Il gate
avvia la baseline dichiarata, ne verifica l'identita e poi esegue le tre
famiglie di test del provider, ciascuna con il proprio runner e il proprio
inventario fissato per nome.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Il gate viene invocato sia come modulo del pacchetto sia come script: la
# radice del repository deve restare importabile in entrambi i casi.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import (  # noqa: E402
    compose_network,
    compose_volume,
    container_variable,
)
from scripts.mysql_inventory import (  # noqa: E402
    collect as collect_inventory,
    difference as inventory_difference,
)
from scripts.mysql_references import (  # noqa: E402
    BASELINE,
    validate_compose_pins_the_baseline,
)


ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "dataflow-mysql"
RUST_IMAGE = "rust:1.92"
EXPECTED_DIGEST = BASELINE.digest
EXPECTED_REFERENCE = BASELINE.image
EXPECTED_VERSION = BASELINE.exact_version
EXPECTED_VERSION_PREFIX = BASELINE.version_prefix

# --- inventario dei test --------------------------------------------------
#
# Tre famiglie, tre runner. `unit` gira senza server; `live_default` sono i
# test live NON marcati `#[ignore]`, che una `cargo test` nuda esegue e che
# quindi richiedono comunque il riferimento acceso; `live_reference` sono i
# test live `#[ignore]`, raggiungibili solo con `--ignored`.
#
# Le liste sono fissate per nome perche un conteggio non distingue un test
# sostituito da un test aggiunto. `validate_inventory()` le confronta con la
# sorgente Rust prima di qualsiasi esecuzione: se divergono, il gate fallisce
# chiuso invece di qualificare una superficie che non e piu quella dichiarata.

EXPECTED_UNIT_TESTS = {
    "arrow::tests::decimal_parser_is_exact_and_checked",
    "arrow::tests::zero_dates_fail_closed_without_panicking",
    "catalog::tests::schema_token_is_stable_and_sensitive",
    "config::tests::debug_redacts_credentials",
    "config::tests::driver_opts_require_tls_even_for_explicit_trust_opt_out",
    "config::tests::in_memory_private_ca_reaches_the_driver_without_a_path",
    "config::tests::invalid_configuration_fails_before_io",
    "config::tests::non_secret_validation_does_not_require_a_password",
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
    "profile::tests::no_other_module_writes_the_timeout_statement",
    "profile::tests::the_catalog_is_queried_only_through_the_profile",
    "profile::tests::the_functional_index_flag_matches_the_query_that_supports_it",
    "profile::tests::the_profile_accepts_mysql_and_rejects_mariadb_from_either_string",
    "profile::tests::the_profile_names_the_product_it_serves",
    "profile::tests::the_statement_timeout_keeps_the_contract_unit",
    "provider::tests::invalid_row_diagnostics_policy_is_rejected_before_transaction_setup",
    "provider::tests::prepare_write_honours_cancellation_before_the_network",
    "provider::tests::prepare_write_rejects_unqualified_operations_before_the_network",
    "provider::tests::provider_surface_is_typed_and_fail_closed",
    "provider::tests::published_spatial_capabilities_match_generic_geometry_contract",
    "provider::tests::query_demands_the_having_bind_before_reaching_the_network",
    "provider::tests::query_demands_the_join_on_bind_before_reaching_the_network",
    "provider::tests::query_honours_cancellation_before_reaching_the_network",
    "provider::tests::query_keeps_unqualified_ast_fail_closed_before_the_network",
    "provider::tests::query_renders_and_binds_before_reaching_the_network",
    "provider::tests::the_provider_is_always_built_through_a_profile",
    "provider::tests::write_rejects_a_budget_that_did_not_prepare_it",
    "provider::tests::write_rejects_a_stream_schema_different_from_prepare",
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
    "read::tests::a_read_conversion_defect_publishes_the_absolute_source_index",
    "read::tests::a_row_that_does_not_fit_opens_the_next_batch",
    "read::tests::a_single_row_over_the_batch_budget_fails_with_resource_limit",
    "read::tests::builder_capacity_is_bounded_by_the_byte_budget_before_allocation",
    "read::tests::cancellation_with_a_pending_row_returns_its_memory_lease",
    "read::tests::default_limits_batch_many_rows_over_four_columns",
    "read::tests::expired_resource_deadline_maps_to_timeout",
    "read::tests::invalid_batch_size_is_rejected_before_io",
    "read::tests::reservation_fails_before_allocation_when_rows_are_exhausted",
    "read::tests::rows_keep_their_order_and_count_across_batch_boundaries",
    "read::tests::terminal_stream_error_is_sticky_instead_of_becoming_eof",
    "read::tests::unattributable_read_failures_never_invent_provenance",
    "row_diagnostics::tests::a_current_batch_with_rows_beyond_the_declared_total_is_rejected",
    "row_diagnostics::tests::row_rejection_causes_come_from_server_codes_only",
    "row_diagnostics::tests::source_offsets_advance_absolutely_across_batch_boundaries",
    "session::tests::bootstrap_is_explicit_and_deterministic",
    "session::tests::exactly_one_affected_row_is_required_for_row_scoped_success",
    "types::tests::concrete_spatial_type_produces_an_exact_valid_contract",
    "types::tests::limit_without_order_is_rejected_fail_closed",
    "types::tests::mapping_preserves_signedness_and_rejects_wide_decimal",
    "types::tests::mysql_geomcollection_alias_produces_the_canonical_exact_type",
    "types::tests::spatial_projection_is_wkb_xy_with_declared_srid",
    "write::tests::a_created_table_survives_the_rollback_and_every_outcome_says_so",
    "write::tests::a_declared_deadlock_stays_rolled_back_instead_of_unknown",
    "write::tests::an_already_quarantined_error_stays_non_retryable_when_rollback_is_unobservable",
    "write::tests::an_error_after_a_successful_commit_declares_the_rows_committed",
    "write::tests::batch_schema_drift_is_rejected_before_binding",
    "write::tests::chunk_binding_is_positional_and_never_interpolates_values",
    "write::tests::chunk_bounds_are_checked_against_the_batch",
    "write::tests::chunk_size_is_deterministic_and_fits_the_placeholder_ceiling",
    "write::tests::commit_interruption_produces_an_unknown_outcome_without_automatic_retry",
    "write::tests::committed_outcome_row_counts_are_contract_valid",
    "write::tests::compile_accepts_supported_arrow_types_in_schema_order",
    "write::tests::compile_and_preflight_qualify_only_xy_wkb_with_matching_srid",
    "write::tests::compile_rejects_a_foreign_contract_version",
    "write::tests::compile_rejects_cross_database_and_layer_targets",
    "write::tests::compile_rejects_dimensions_the_mysql_server_cannot_represent",
    "write::tests::compile_rejects_empty_or_unqualified_arrow_schemas",
    "write::tests::compile_rejects_unqualified_operation_shapes_before_the_network",
    "write::tests::create_accepts_keys_and_renders_them_as_a_primary_key",
    "write::tests::insert_renders_qualified_quoted_columns_in_schema_order",
    "write::tests::insert_requires_at_least_one_row",
    "write::tests::insert_row_count_overflow_is_checked",
    "write::tests::insert_stops_at_the_placeholder_ceiling_before_the_network",
    "write::tests::modes_without_key_semantics_reject_keys_and_update_columns",
    "write::tests::null_cells_in_non_nullable_columns_fail_before_the_network",
    "write::tests::primary_key_types_and_limits_are_refused_before_the_server",
    "transaction::tests::a_key_of_fifty_three_characters_is_refused_before_any_statement",
    "transaction::tests::a_key_of_fifty_two_characters_is_accepted",
    "transaction::tests::an_empty_context_is_valid",
    "transaction::tests::the_longest_writable_key_is_fifty_two_characters",
    "write::tests::pre_commit_errors_claim_rollback_only_when_it_is_confirmed",
    "write::tests::server_preflight_keeps_unqualified_write_targets_closed",
    "write::tests::server_preflight_rejects_targets_that_strict_cannot_write",
    "write::tests::server_preflight_reports_no_losses_only_for_a_compatible_table",
    "write::tests::server_preflight_requires_microsecond_temporal_precision",
    "write::tests::spatial_batch_enforces_exact_type_and_cumulative_component_budget",
    "write::tests::spatial_batch_rejects_ewkb_srid_and_z_before_binding",
    "write::tests::upsert_keys_only_renders_noop_on_duplicate_clause",
    "write::tests::upsert_preflight_accepts_a_redundant_unique_index_on_the_same_keys",
    "write::tests::upsert_preflight_accepts_keys_matching_a_unique_index",
    "write::tests::upsert_preflight_rejects_a_conflicting_extra_unique_index",
    "write::tests::upsert_preflight_rejects_a_functional_unique_index",
    "write::tests::upsert_preflight_rejects_keys_without_a_backing_unique_index",
    "write::tests::upsert_renders_on_duplicate_update_for_non_key_columns",
}
EXPECTED_LIVE_DEFAULT_TESTS = {
    "live_tests::live_native_query_policy_allow_permits_ddl",
    "live_tests::live_native_query_policy_deny_rejects_conditional_update_ddl",
    "live_tests::live_native_query_policy_deny_rejects_ddl",
    "live_tests::live_native_query_policy_deny_rejects_ddl_via_query",
    "live_tests::live_native_query_policy_deny_rejects_session_control",
    "live_tests::live_v12_capabilities_publish_verified_spatial_functions",
    "live_tests::live_v12_conditional_update_rolls_back_on_mismatch",
    "live_tests::live_v12_execute_ddl_accepts_statements_the_prepared_protocol_refuses",
    "live_tests::live_v12_execute_ddl_in_flight_interruption_reports_an_unknown_remote_effect",
    "live_tests::live_v12_execute_ddl_pre_cancellation_reports_no_remote_effect",
    "live_tests::live_v12_provider_execute_ddl_creates_and_drops_table",
    "live_tests::live_v12_query_spatial_functions_render_and_execute",
    "live_tests::live_v12_query_spatial_predicate_intersects_in_filter",
    "live_tests::live_v12_transaction_execute_and_commit",
    "live_tests::live_v12_transaction_query_returns_typed_rows",
    "live_tests::live_v12_transaction_rollback_drops_all_writes",
    "live_tests::live_v12_transaction_session_context_reaches_the_server",
    "live_tests::live_v12_session_context_is_cleared_when_a_connection_is_reused",
    "live_tests::live_v12_session_context_does_not_reach_the_next_transaction",
    "live_tests::live_v12_transaction_savepoint_rollback_to_partial",
    "live_tests::live_v12_write_create_failure_leaves_the_table_and_reports_partial",
    "live_tests::live_v12_write_create_mode_builds_table_and_inserts",
    "live_tests::live_v12_write_create_mode_conflict_if_exists",
    "live_tests::live_v12_write_create_with_keys_declares_the_primary_key",
    "live_tests::live_v12_write_create_primary_key_on_text_is_refused_before_the_network",
    "live_tests::live_v12_write_delete_by_keys_removes_matching_rows",
    "live_tests::live_v12_write_delete_by_keys_without_keys_rejected",
    "live_tests::live_v12_write_replace_on_a_missing_target_is_not_found",
    "live_tests::live_v12_write_replace_preserves_table_identity_and_metadata",
    "live_tests::live_v12_write_replace_restores_the_previous_rows_on_cancellation",
    "live_tests::live_v12_write_replace_restores_the_previous_rows_when_the_stream_fails",
    "live_tests::live_v12_write_truncate_insert_rejected_without_remote_effects",
    "live_tests::live_v12_write_update_via_staging_updates_matching_rows",
    "live_tests::live_v12_write_update_without_keys_rejected",
    "live_tests::live_v12_write_upsert_rejects_conflicting_unique_index",
    "live_tests::live_v12_write_upsert_updates_existing_and_inserts_new",
    "live_tests::live_v12_write_upsert_without_keys_rejected",
}
EXPECTED_LIVE_REFERENCE_TESTS = {
    "live_tests::live_a_row_over_the_batch_budget_carries_over_to_the_next_batch",
    "live_tests::live_append_batch_failure_rolls_back_without_partial_rows",
    "live_tests::live_append_commits_a_single_transaction_and_reads_back_exactly",
    "live_tests::live_append_spatial_xy_preserves_srid_and_coordinates",
    "live_tests::live_append_timeout_quarantines_and_replaces_the_pooled_session",
    "live_tests::live_deadline_reports_timeout_and_quarantines_the_session",
    "live_tests::live_default_limits_batch_many_rows_over_four_columns",
    "live_tests::live_early_stream_drop_cancels_worker_and_keeps_provider_usable",
    "live_tests::live_grouped_aggregate_having_bind_and_distinct_over_verified_tls",
    "live_tests::live_inflight_cancellation_quarantines_the_session",
    "live_tests::live_operation_timeout_quarantines_the_session",
    "live_tests::live_physical_joins_bind_on_clauses_and_publish_outer_nullability",
    "live_tests::live_pool_acquire_timeout_is_independent_from_connect_timeout",
    "live_tests::live_pool_reset_reapplies_deterministic_session_bootstrap",
    "live_tests::live_provider_connection_capabilities_and_inspect",
    "live_tests::live_provider_read_rejects_a_hostname_mismatch",
    "live_tests::live_provider_row_diagnostics_matches_confirmed_rollback_oracle",
    "live_tests::live_query_operation_cancellation_and_timeout_quarantine_the_session",
    "live_tests::live_query_operation_executes_once_holds_lease_and_stays_demand_bounded",
    "live_tests::live_read_projection_filter_order_and_default_schema",
    "live_tests::live_reference_probe_catalog_and_spatial_metadata",
    "live_tests::live_scalar_single_source_query_uses_prepare_metadata_as_schema",
    "live_tests::live_scalar_window_functions_publish_peer_stable_ranking_and_range_aggregates",
    "live_tests::live_streaming_read_maps_scalar_and_xy_geometry_exactly",
    "live_tests::live_verified_tls_rejects_a_hostname_mismatch",
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
    """La password della fixture, letta dal container che la porta.

    Una copia nel sorgente sarebbe una seconda fonte per un dato che ne ha
    una sola — il compose — e le due divergerebbero in silenzio.
    """

    return container_variable(CONTAINER, "MYSQL_PASSWORD")


def mysql_value(statement: str) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER,
            "/bin/sh",
            "-c",
            # TCP, non socket: durante il bootstrap l'entrypoint MySQL
            # avvia un server temporaneo raggiungibile solo dal socket. Una
            # probe sul socket puo quindi rispondere prima che il server
            # definitivo esista, e la verifica successiva trova il vuoto.
            'exec env MYSQL_PWD="$MYSQL_PASSWORD" mysql -Nse "$1" '
            "-u dataflow -h 127.0.0.1 --protocol=TCP --ssl-mode=REQUIRED",
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
    """Volume con la CA privata, chiesto a Docker come la rete."""

    return compose_volume(CONTAINER, "/etc/mysql/tls")


def mysql_network() -> str:
    """Rete Compose del riferimento, con l'alias della prova TLS negativa.

    La scoperta e condivisa con gli altri due gate; resta specifico di MySQL
    il secondo alias, che nessun altro riferimento usa.
    """

    return compose_network(
        CONTAINER,
        required_alias=CONTAINER,
        required_aliases={
            "mysql-hostname-mismatch": (
                "la prova TLS negativa diventerebbe un errore DNS invece di "
                "un rifiuto di identita"
            )
        },
    )


def host_ca_path() -> str:
    """CA privata esportata sull'host per l'esecuzione con cargo locale.

    # Raises

    `RuntimeError` quando la CA non e disponibile: senza di essa i test live
    girerebbero con verifica indebolita, cioe proprio la condizione che il
    gate esiste per escludere.
    """

    ca_path = os.environ.get("PLENORA_MYSQL_CA")
    if not ca_path:
        raise RuntimeError(
            "PLENORA_MYSQL_CA obbligatoria con cargo host: esportare la CA da "
            f"{CONTAINER}:/etc/mysql/tls/ca.pem prima di eseguire il gate"
        )
    return ca_path


def cargo(arguments: list[str]) -> tuple[list[str], dict[str, str] | None]:
    if os.environ.get("PLENORA_MYSQL_GATE_HOST_CARGO") == "1":
        environment = os.environ.copy()
        environment.setdefault("PLENORA_MYSQL_HOST", "127.0.0.1")
        environment.setdefault("PLENORA_MYSQL_DATABASE", "dataflow_test")
        environment.setdefault("PLENORA_MYSQL_USER", "dataflow")
        environment.setdefault("PLENORA_MYSQL_PASSWORD", fixture_password())
        environment["PLENORA_MYSQL_CA"] = host_ca_path()
        environment["PLENORA_MYSQL_EXPECTED_VERSION"] = EXPECTED_VERSION_PREFIX
        return [os.environ.get("CARGO", "cargo"), *arguments], environment

    environment = os.environ.copy()
    environment["PLENORA_MYSQL_PASSWORD"] = fixture_password()
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        mysql_network(),
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
        "-e",
        f"PLENORA_MYSQL_EXPECTED_VERSION={EXPECTED_VERSION_PREFIX}",
        RUST_IMAGE,
        "/usr/local/cargo/bin/cargo",
        *arguments,
    ]
    return command, environment


def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
    command, environment = cargo(arguments)
    return run(command, environment=environment, capture=capture)


def validate_inventory() -> None:
    """L'inventario dichiarato deve coincidere con la sorgente Rust.

    # Raises

    `RuntimeError` appena una famiglia diverge: un test aggiunto e mai
    eseguito, o rimosso e mai notato, resterebbe altrimenti invisibile.
    """

    observed = collect_inventory()
    declared = {
        "unit": frozenset(EXPECTED_UNIT_TESTS),
        "live_default": frozenset(EXPECTED_LIVE_DEFAULT_TESTS),
        "live_reference": frozenset(EXPECTED_LIVE_REFERENCE_TESTS),
    }
    for family, names in declared.items():
        if names != observed[family]:
            raise RuntimeError(
                f"inventario {family} MySQL stale rispetto alla sorgente: "
                f"{inventory_difference(names, observed[family])}"
            )


def validate_fixture() -> None:
    validate_compose_pins_the_baseline()
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
    configured = docker_value(["inspect", "--format", "{{.Config.Image}}", CONTAINER])
    image_id = docker_value(["inspect", "--format", "{{.Image}}", CONTAINER])
    if configured != EXPECTED_REFERENCE:
        raise RuntimeError("container MySQL diverso dal digest di riferimento")
    version = mysql_value("SELECT VERSION()")
    if version != EXPECTED_VERSION:
        raise RuntimeError(
            f"versione MySQL inattesa: {version}, attesa {EXPECTED_VERSION}"
        )
    return {"configured_reference": configured, "image_id": image_id, "version": version}


def executed_tests(output: str, pattern: str) -> set[str]:
    return set(re.findall(pattern, output, re.MULTILINE))


def verify_suite(family: str, declared: set[str], observed: set[str]) -> None:
    if declared != observed:
        raise RuntimeError(
            f"inventario test {family} MySQL inatteso: "
            f"eseguiti {len(observed)}, attesi {len(declared)}, "
            f"{inventory_difference(frozenset(declared), frozenset(observed))}"
        )


def run_unit_suite() -> None:
    """I test senza server: il filtro esclude ogni test live per nome."""

    output = run_cargo(
        ["test", "-p", "plenora-db-mysql", "--locked", "--", "--skip", "live_"],
        capture=True,
    )
    verify_suite(
        "unit",
        EXPECTED_UNIT_TESTS,
        executed_tests(output, r"^test ([^ ]+) \.\.\. ok$"),
    )


def run_live_default_suite() -> None:
    """I test live che il runner di default esegue senza `--ignored`."""

    output = run_cargo(
        [
            "test",
            "-p",
            "plenora-db-mysql",
            "live_",
            "--locked",
            "--",
            "--nocapture",
            "--test-threads=1",
        ],
        capture=True,
    )
    verify_suite(
        "live default",
        EXPECTED_LIVE_DEFAULT_TESTS,
        executed_tests(output, r"^test (live_tests::[^ ]+) \.\.\. ok$"),
    )


def run_live_reference_suite() -> None:
    """I test live `#[ignore]`: l'inventario di qualifica del riferimento."""

    output = run_cargo(
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
    verify_suite(
        "live reference",
        EXPECTED_LIVE_REFERENCE_TESTS,
        executed_tests(output, r"^test (live_tests::[^ ]+) \.\.\. ok$"),
    )


def run_live_cli_probe() -> None:
    test_name = "live_database_probe_mysql_private_ca"
    output = run_cargo(
        [
            "test",
            "-p",
            "plenora-database-cli",
            # L'adapter MySQL della CLI e opt-in: senza la feature il binario
            # risponde `unsupported` e il probe proverebbe soltanto che il
            # provider non e stato compilato.
            "--features",
            "mysql",
            "--test",
            "live_probe",
            test_name,
            "--locked",
            "--",
            "--ignored",
            "--exact",
            "--nocapture",
        ],
        capture=True,
    )
    if not re.search(rf"^test {re.escape(test_name)} \.\.\. ok$", output, re.MULTILINE):
        raise RuntimeError("probe CLI live MySQL non eseguito")
    if not re.search(
        r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output
    ):
        raise RuntimeError("risultato probe CLI live MySQL inatteso")


def run_static_checks() -> int:
    """I controlli del gate che non richiedono ne container ne toolchain Rust.

    Sono tre: la matrice dichiarata deve coincidere con cio che il compose
    avvia, l'inventario dichiarato con la sorgente Rust, e le due suite Python
    che presidiano gate e matrice devono passare. Girano in secondi su ogni PR
    perche intercettano subito la classe di errore che altrimenti si scopre
    mezz'ora dopo, in mezzo a una campagna live.
    """

    validate_compose_pins_the_baseline()
    validate_inventory()
    run([sys.executable, str(ROOT / "scripts" / "test_check_mysql_reference.py")])
    run([sys.executable, "-m", "unittest", "tests.test_mysql_matrix"])
    print(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed",
                "provider": "mysql",
                "scope": "static",
                "reference": BASELINE.label,
                "reference_version": EXPECTED_VERSION,
                "unit_tests": len(EXPECTED_UNIT_TESTS),
                "live_default_tests": len(EXPECTED_LIVE_DEFAULT_TESTS),
                "live_reference_tests": len(EXPECTED_LIVE_REFERENCE_TESTS),
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    validate_inventory()
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
    run_unit_suite()
    run_live_default_suite()
    run_live_reference_suite()
    run_live_cli_probe()
    print(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed",
                "provider": "mysql",
                "reference": BASELINE.label,
                "reference_version": EXPECTED_VERSION,
                "unit_tests": len(EXPECTED_UNIT_TESTS),
                "live_default_tests": len(EXPECTED_LIVE_DEFAULT_TESTS),
                "live_reference_tests": len(EXPECTED_LIVE_REFERENCE_TESTS),
                "image": identity,
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def selected_entry_point(arguments: list[str]):
    """`--static` sceglie i soli controlli senza server; nient'altro e valido.

    # Raises

    `RuntimeError` per qualunque altro argomento: un flag scritto male non
    deve degradare in silenzio al gate completo, ne viceversa.
    """

    if not arguments:
        return main
    if arguments == ["--static"]:
        return run_static_checks
    raise RuntimeError(f"argomenti non riconosciuti: {arguments}")


if __name__ == "__main__":
    try:
        raise SystemExit(selected_entry_point(sys.argv[1:])())
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"MySQL reference gate FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
