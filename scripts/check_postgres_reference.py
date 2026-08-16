#!/usr/bin/env python3
"""Gate live del driver PostgreSQL/PostGIS di riferimento."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Il gate viene invocato sia come modulo del pacchetto sia come script: la
# radice del repository deve restare importabile in entrambi i casi.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import compose_network  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"
CONTAINER = "dataflow-postgres"
DEFAULT_DSN = (
    "host=dataflow-postgres port=5432 user=dataflow "
    "password=dataflow_test_2026 dbname=dataflow_test"
)
# Test live che il gate non può dichiarare senza averli visti passare. Saltano
# con un early-return quando la DSN non è impostata, quindi un `test result: ok`
# non prova che siano stati eseguiti: solo il nome lo prova.
REQUIRED_LIVE_TESTS = frozenset(
    {
        "live_provider_row_diagnostics_matches_confirmed_rollback_oracle",
        "live_provider_row_diagnostics_lost_rollback_ack_is_quarantined",
        "live_provider_row_diagnostics_commit_ambiguity_partitions_all_rows_unknown",
    }
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


def postgres_network() -> str:
    """Rete Compose osservata sul container di riferimento.

    Il nome dipende dal progetto Compose, cioè dalla directory del checkout: un
    valore cablato rende il gate ineseguibile in un worktree, dove i cargo con
    DSN finiscono su una rete che non contiene il container e falliscono con
    errori `Protocol`/`Connect` indistinguibili da un difetto del provider. La
    scoperta è fail-closed — senza label di progetto, senza la rete attesa o
    senza l'alias del container il gate fallisce invece di indovinare.
    """

    return compose_network(CONTAINER, required_alias=CONTAINER)


def cargo(
    arguments: list[str],
    dsn: str | None = None,
    *,
    insecure_local: bool = False,
) -> list[str]:
    """Comando `docker run` per un cargo dentro l'immagine pinnata.

    `insecure_local` esporta `PLENORA_TLS_INSECURE_LOCAL` al CLI. Serve
    perche il riferimento di questo gate e **plaintext** per costruzione — il
    riferimento TLS e un compose separato, `dataflow-postgres-tls`, ed e
    `check_postgres_hardening.py` a provarne la verifica. Dopo ADR-011 il CLI
    pretende TLS per default, quindi senza l'interruttore il passo IPC
    falliva in `connect` con un errore di protocollo: non una debolezza
    scoperta, ma un riferimento che non parla quel protocollo.

    L'interruttore vale solo per i passi che lo chiedono. Estenderlo a tutta
    la suite renderebbe invisibile una regressione sul default sicuro.
    """

    command = [
        "docker", "run", "--rm",
        "-v", f"{ROOT}:/workspace",
        "-v", f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-v", "plenora-conformance-current-target:/workspace/target",
        "-w", "/workspace",
    ]
    if insecure_local:
        command += ["-e", "PLENORA_TLS_INSECURE_LOCAL=1"]
    if dsn is not None:
        command += [
            "--network", postgres_network(),
            "-e", f"PLENORA_TEST_POSTGRES_DSN={dsn}",
        ]
    return [*command, IMAGE, "cargo", *arguments]


def check_ipc_materialization(dsn: str) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix=".postgres-ipc-gate-", dir=ROOT) as directory:
        output = Path(directory) / "schema-cache.arrow"
        container_output = "/workspace/" + output.relative_to(ROOT).as_posix()
        materialized = json.loads(run(
            cargo([
                "run", "--quiet", "-p", "plenora-database-cli", "--",
                "postgres-read-ipc", "PLENORA_TEST_POSTGRES_DSN",
                "plenora_fixture", "schema_cache_probe", container_output,
                "--max-rows", "100", "--max-output-bytes", "10485760",
                "--timeout-ms", "120000", "--order-by", "id",
            ], dsn, insecure_local=True),
            capture=True,
        ))
        inspected = json.loads(run(
            cargo([
                "run", "--quiet", "-p", "plenora-database-cli", "--",
                "inspect-dataset", container_output,
            ]),
            capture=True,
        ))
        geometry = next(field for field in inspected["fields"] if field["name"] == "geom")
        if materialized["status"] != "materialized" or materialized["rows"] != 1:
            raise RuntimeError("materializzazione IPC PostgreSQL incompleta")
        if materialized["row_order"] != "deterministic":
            raise RuntimeError("ordine IPC PostgreSQL non deterministico")
        if materialized["durability"] not in {"confirmed", "unconfirmed"}:
            raise RuntimeError("durability IPC PostgreSQL non dichiarata")
        if inspected["rows"] != 1 or inspected["contract_version"] != "1":
            raise RuntimeError("rilettura IPC PostgreSQL incoerente")
        if geometry["metadata"].get("plenora.geometry.encoding") != "ewkb":
            raise RuntimeError("encoding geometria IPC PostgreSQL non preservato")
        if geometry["metadata"].get("plenora.geometry.srid") != "4326":
            raise RuntimeError("SRID geometria IPC PostgreSQL non preservato")
        if list(Path(directory).glob(".*.partial-*")):
            raise RuntimeError("staging IPC PostgreSQL residuo")
        return {
            "rows": materialized["rows"],
            "batches": materialized["batches"],
            "row_order": materialized["row_order"],
            "durability": materialized["durability"],
            "geometry_encoding": geometry["metadata"]["plenora.geometry.encoding"],
            "geometry_srid": geometry["metadata"]["plenora.geometry.srid"],
        }


def validate_live_row_diagnostics(output: str) -> None:
    """Verifica che i test live row diagnostics siano nella matrice eseguita.

    Questi test escono con un early-return quando la DSN non è impostata, e in
    quel caso riportano comunque `ok`: il solo esito non prova che le
    asserzioni siano state eseguite. Il gate copre la seconda metà della prova
    imponendo la DSN a ogni invocazione (`cargo(..., dsn)`); questo controllo
    copre la prima, cioè che i tre test siano ancora compilati e inclusi nella
    corsa. Un test rinominato, cancellato o filtrato via smette di essere una
    prova e qui fallisce invece di sparire in silenzio.
    """

    executed = set(
        re.findall(r"^test test_suite::tests::([^ ]+) \.\.\. ok$", output, re.MULTILINE)
    )
    missing = sorted(REQUIRED_LIVE_TESTS - executed)
    if missing:
        raise RuntimeError(
            f"test live PostgreSQL dichiarati ma non eseguiti: {missing}"
        )


def main() -> int:
    dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN", DEFAULT_DSN)
    try:
        state = run(
            ["docker", "inspect", "--format",
             "{{.State.Status}}|{{.State.Health.Status}}", CONTAINER],
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
        provider_output = run(
            cargo(["test", "-p", "plenora-db-postgres", "--", "--nocapture"], dsn),
            capture=True,
        )
        validate_live_row_diagnostics(provider_output)
        ipc_materialization = check_ipc_materialization(dsn)
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
            "public_api_v0_1_compile_freeze",
            "provider_common_conformance",
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
            "write_preflight_crs_authority_and_loss_report",
            "byte_limits",
            "fault_before_commit_rollback",
            "fault_after_commit_unknown",
            "copy_text_binary_prepared_differential",
            "postgres_read_ipc_materialization_and_readback",
        ],
        "benchmark": benchmark,
        "ipc_materialization": ipc_materialization,
        "freeze_scope": "postgres-postgis-data-path-v3",
        "advanced_scope": "postgres-postgis-advanced-profile-v1",
        "release_reference": "postgres-provider-v0.1",
        "open_non_blocking": [],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
