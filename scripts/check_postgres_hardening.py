#!/usr/bin/env python3
"""Gate di hardening live del provider PostgreSQL/PostGIS."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"
NETWORK = "plenora-database-tools_default"
DEFAULT_DSN = (
    "host=dataflow-postgres port=5432 user=dataflow "
    "password=dataflow_test_2026 dbname=dataflow_test"
)
DEFAULT_TLS_DSN = (
    "host=dataflow-postgres-tls port=5432 user=dataflow_tls "
    "password=dataflow_tls_test_2026 dbname=dataflow_tls_test"
)
TLS_VOLUME = "plenora-database-tools_postgres_tls_certs"


def run(command: list[str], *, capture: bool = False) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=capture,
    )
    if completed.returncode:
        if capture:
            sys.stderr.write(completed.stdout)
            sys.stderr.write(completed.stderr)
        raise RuntimeError(f"check fallito: {command[0]}")
    return completed.stdout if capture else ""


def cargo(
    arguments: list[str],
    dsn: str | None = None,
    tls_dsn: str | None = None,
) -> list[str]:
    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-w",
        "/workspace",
    ]
    if dsn is not None:
        command += [
            "--network",
            NETWORK,
            "-e",
            f"PLENORA_TEST_POSTGRES_DSN={dsn}",
        ]
    if tls_dsn is not None:
        command += [
            "-v",
            f"{TLS_VOLUME}:/tls:ro",
            "-e",
            f"PLENORA_TEST_POSTGRES_TLS_DSN={tls_dsn}",
            "-e",
            "PLENORA_TEST_POSTGRES_TLS_CA=/tls/ca.crt",
            "-e",
            "PLENORA_TEST_POSTGRES_TLS_CLIENT_CERT=/tls/client.crt",
            "-e",
            "PLENORA_TEST_POSTGRES_TLS_CLIENT_KEY=/tls/client.key",
        ]
    return [*command, IMAGE, "cargo", *arguments]


def main() -> int:
    dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN", DEFAULT_DSN)
    tls_dsn = os.environ.get("PLENORA_TEST_POSTGRES_TLS_DSN", DEFAULT_TLS_DSN)
    checks = [
        "container_health",
        "rustfmt",
        "clippy_deny_warnings",
        "provider_common_conformance",
        "public_api_v0_1_compile_freeze",
        "deterministic_numeric_codec_boundaries",
        "strict_numeric_text_parser",
        "strict_parameter_uuid_decimal_parser",
        "range_composite_escaping",
        "bounded_metrics_without_dynamic_labels",
        "startup_session_defaults",
        "single_strict_reset_on_reuse",
        "parameterless_one_shot_read_fast_path",
        "parameterized_typed_one_shot_fast_path",
        "custom_type_prepared_fallback",
        "query_operation_typed_one_shot",
        "query_operation_empty_result_describe",
        "bounded_iterative_query_operation_validation",
        "advanced_query_semantic_validation",
        "spatial_function_arity_and_boolean_context_validation",
        "ewkb_header_contract_validation",
        "spatial_catalog_renderer_lockstep",
        "bounded_validated_schema_cache",
        "external_ddl_schema_token_invalidation",
        "write_target_schema_cache_invalidation",
        "concurrent_pool_stress",
        "concurrent_server_side_cancellation",
        "invalid_session_exclusion",
        "pool_recovery_after_cancellation",
        "temporal_extremes_are_mapping_errors",
        "poisoned_internal_mutex_recovery",
        "commit_fault_recovery_semantics",
        "four_axis_error_envelope",
        "verified_rollback_on_write_cancellation",
        "concrete_race_free_cancellation_token",
        "canonical_arrow_postgis_metadata",
        "legacy_metadata_dual_read_with_divergence_rejection",
        "end_to_end_atomic_resource_budget",
        "write_budget_identity_enforcement",
        "verified_rollback_on_resource_exhaustion",
        "iterative_bounded_ewkb_scanner",
        "live_geometry_component_budget",
        "spatial_ref_sys_authority_resolution",
        "resolved_crs_preflight_verification",
        "srs_in_structural_schema_token",
        "monotonic_resource_deadline",
        "read_deadline_server_side_cancellation",
        "write_deadline_verified_rollback",
        "commit_deadline_requires_recovery",
        "explicit_rollback_for_all_precommit_errors",
        "trigger_failure_verified_rollback",
        "write_error_execution_id",
        "tls_mode_cannot_be_weakened_by_dsn",
        "private_ca_and_mtls_live",
        "tls_hostname_and_chain_verification",
        "server_side_cancellation_over_mtls",
        "pool_recovery_over_mtls",
    ]
    try:
        run(
            [
                "docker",
                "compose",
                "-f",
                "docker-compose.postgres-tls.yml",
                "up",
                "-d",
                "--wait",
            ]
        )
        state = run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.Health.Status}}",
                "dataflow-postgres",
            ],
            capture=True,
        ).strip()
        if state != "running|healthy":
            raise RuntimeError("container PostgreSQL non healthy")
        tls_state = run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.Health.Status}}",
                "dataflow-postgres-tls",
            ],
            capture=True,
        ).strip()
        if tls_state != "running|healthy":
            raise RuntimeError("container PostgreSQL mTLS non healthy")
        run(cargo(["fmt", "--all", "--", "--check"]))
        run(
            cargo(
                [
                    "clippy",
                    "-p",
                    "plenora-db-postgres",
                    "--all-targets",
                    "--",
                    "-D",
                    "warnings",
                ]
            )
        )
        run(
            cargo(
                [
                    "test",
                    "-p",
                    "plenora-database-core",
                    "-p",
                    "plenora-database-sql",
                ]
            )
        )
        run(
            cargo(
                [
                    "test",
                    "-p",
                    "plenora-db-postgres",
                    "--lib",
                    "--",
                    "--nocapture",
                ],
                dsn,
                tls_dsn,
            )
        )
    except RuntimeError as error:
        print(f"postgres hardening gate: {error}", file=sys.stderr)
        return 1

    report = {
        "schema_version": 1,
        "gate": "postgres-postgis-hardening-v1",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": True,
        "secrets_persisted": False,
        "reference_target": "PostgreSQL 16 / PostGIS 3.4",
        "checks": checks,
        "remaining_external_matrix": [
            "public_ca_tls",
            "linux_arm64",
            "managed_postgres_services",
        ],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
