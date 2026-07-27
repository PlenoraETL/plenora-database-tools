#!/usr/bin/env python3
"""Gate live del driver PostgreSQL/PostGIS di riferimento."""

from __future__ import annotations

import json
import os
import re
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
            sys.stderr.write(completed.stderr)
        raise RuntimeError(f"check fallito: {command[0]}")
    return completed.stdout if capture else ""


def cargo(arguments: list[str], dsn: str | None = None) -> list[str]:
    command = [
        "docker", "run", "--rm",
        "-v", f"{ROOT}:/workspace",
        "-v", f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-w", "/workspace",
    ]
    if dsn is not None:
        command += [
            "--network", NETWORK,
            "-e", f"PLENORA_TEST_POSTGRES_DSN={dsn}",
        ]
    return [*command, IMAGE, "cargo", *arguments]


def main() -> int:
    dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN", DEFAULT_DSN)
    try:
        state = run(
            ["docker", "inspect", "--format",
             "{{.State.Status}}|{{.State.Health.Status}}", "dataflow-postgres"],
            capture=True,
        ).strip()
        if state != "running|healthy":
            raise RuntimeError("container PostgreSQL non healthy")
        run(cargo(["clippy", "--workspace", "--all-targets", "--", "-D", "warnings"]))
        run(cargo([
            "test",
            "-p", "plenora-database-core",
            "-p", "plenora-database-sql",
        ]))
        run(cargo(["test", "-p", "plenora-db-postgres", "--", "--nocapture"], dsn))
        output = run(
            cargo([
                "test", "-p", "plenora-db-postgres",
                "live_copy_vs_prepared_benchmark", "--",
                "--ignored", "--nocapture",
            ], dsn),
            capture=True,
        )
        matches = re.findall(
            r'\{"rows":1000,"copy_text_micros":\d+,'
            r'"copy_binary_micros":\d+,'
            r'"prepared_micros":\d+,"differences":0\}',
            output,
        )
        if not matches:
            raise RuntimeError("risultato benchmark non trovato")
        benchmark = json.loads(matches[-1])
    except RuntimeError as error:
        print(f"postgres reference gate: {error}", file=sys.stderr)
        return 1
    report = {
        "schema_version": 1,
        "gate": "postgres-postgis-reference-freeze",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": True,
        "secrets_persisted": False,
        "checks": [
            "container_health",
            "clippy_deny_warnings",
            "read_write_spatial_live",
            "query_ast_cte_join_group_having",
            "bounded_shared_pool_and_session_reset",
            "startup_defaults_without_configuration_query",
            "parameterless_one_shot_read_fast_path",
            "parameterized_typed_one_shot_fast_path",
            "custom_type_prepared_fallback",
            "query_operation_typed_one_shot",
            "query_operation_empty_result_describe",
            "bounded_iterative_query_operation_validation",
            "advanced_query_cte_derived_lateral_window_set_locking",
            "dialect_correct_query_limit_rendering",
            "spatial_wkb_constructor_rendering",
            "spatial_catalog_72_renderer_lockstep",
            "spatial_bbox_knn_gist_plan",
            "spatial_xy_xyz_xym_xyzm_roundtrip",
            "spatial_curve_surface_collection_introspection",
            "ewkb_header_contract_validation",
            "bounded_validated_schema_cache",
            "external_ddl_schema_token_invalidation",
            "server_side_read_write_cancellation",
            "tcp_keepalive_and_connect_timeouts",
            "advanced_catalog_introspection",
            "partition_rls_policy_acl_materialized_view_introspection",
            "advanced_type_read_write_roundtrip",
            "binary_array_uuid_range_composite_interval_roundtrip",
            "portable_interval_text_encoding",
            "negative_scale_numeric_and_typed_parameters",
            "temporal_extremes_are_mapping_errors",
            "poisoned_internal_mutex_recovery",
            "transactional_additive_schema_evolution",
            "write_preflight",
            "byte_limits",
            "fault_before_commit_rollback",
            "fault_after_commit_unknown",
            "copy_text_binary_prepared_differential",
        ],
        "benchmark": benchmark,
        "freeze_scope": "postgres-postgis-data-path-v3",
        "advanced_scope": "postgres-postgis-advanced-profile-v1",
        "open_non_blocking": [],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
