#!/usr/bin/env python3
"""Gate live riproducibile del provider SQL Server 2022 di riferimento."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUST_IMAGE = "rust:1.92"
NETWORK = "plenora-database-tools_default"
CONTAINER = "dataflow-sqlserver"
EXPECTED_IMAGE = (
    "sha256:e07b9699a2b749969f19d86563ceeea22bd3a69f7f1db85a8d1ac4bdaf0c6f56"
)
EXPECTED_LIVE_TESTS = 30
DEFAULT_PASSWORD = "DataFlow_Test_2026!"
DOCKER_TIMEOUT_SECONDS = 30
CARGO_TIMEOUT_SECONDS = 15 * 60
ASSURANCE_RESULTS = ROOT / "assurance-results"
PRIVATE_CA = ASSURANCE_RESULTS / "sqlserver-private-ca.pem"


def run(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
    capture: bool = False,
    timeout_seconds: int,
) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=capture,
            env=environment,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"timeout dopo {timeout_seconds}s durante {command[0]}"
        ) from error
    if capture:
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
    if completed.returncode:
        raise RuntimeError(f"check fallito: {command[0]}")
    return f"{completed.stdout}{completed.stderr}" if capture else ""


def docker_value(arguments: list[str]) -> str:
    try:
        completed = subprocess.run(
            ["docker", *arguments],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=DOCKER_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("timeout interrogazione Docker SQL Server") from error
    if completed.returncode:
        sys.stderr.write(completed.stderr)
        raise RuntimeError("interrogazione Docker SQL Server fallita")
    return completed.stdout.strip()


def cargo(arguments: list[str]) -> tuple[list[str], dict[str, str] | None]:
    if os.environ.get("PLENORA_SQLSERVER_GATE_HOST_CARGO") == "1":
        environment = os.environ.copy()
        environment.setdefault("PLENORA_SQLSERVER_HOST", "127.0.0.1")
        environment.setdefault("PLENORA_SQLSERVER_DATABASE", "dataflow_test")
        environment.setdefault("PLENORA_SQLSERVER_USER", "dataflow")
        environment.setdefault("PLENORA_SQLSERVER_PASSWORD", DEFAULT_PASSWORD)
        environment["PLENORA_SQLSERVER_PRIVATE_CA"] = str(PRIVATE_CA)
        environment.setdefault("PLENORA_SQLSERVER_MISMATCH_HOST", "127.0.0.2")
        return ["cargo", *arguments], environment

    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        NETWORK,
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-v",
        f"{ROOT.parent / 'plenora-rustup-cache'}:/usr/local/rustup",
        "-w",
        "/workspace",
        "-e",
        "PLENORA_SQLSERVER_HOST=sqlserver",
        "-e",
        "PLENORA_SQLSERVER_DATABASE=dataflow_test",
        "-e",
        "PLENORA_SQLSERVER_USER=dataflow",
        "-e",
        f"PLENORA_SQLSERVER_PASSWORD={DEFAULT_PASSWORD}",
        "-e",
        "PLENORA_SQLSERVER_PRIVATE_CA=/workspace/assurance-results/sqlserver-private-ca.pem",
        "-e",
        "PLENORA_SQLSERVER_MISMATCH_HOST=sqlserver-hostname-mismatch",
        RUST_IMAGE,
        "cargo",
        *arguments,
    ]
    return command, None


def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
    command, environment = cargo(arguments)
    return run(
        command,
        environment=environment,
        capture=capture,
        timeout_seconds=CARGO_TIMEOUT_SECONDS,
    )


def validate_image_pin() -> dict[str, str]:
    compose = (ROOT / "docker-compose.sqlserver.yml").read_text(encoding="utf-8")
    reference = f"mcr.microsoft.com/mssql/server@{EXPECTED_IMAGE}"
    if compose.count(reference) != 3:
        raise RuntimeError("digest SQL Server non fissato per tutti i servizi")
    configured_image = docker_value(
        ["inspect", "--format", "{{.Config.Image}}", CONTAINER]
    )
    image_id = docker_value(["inspect", "--format", "{{.Image}}", CONTAINER])
    if configured_image != reference and image_id != EXPECTED_IMAGE:
        raise RuntimeError("container SQL Server diverso dal digest di riferimento")
    return {
        "configured_reference": configured_image,
        "runtime_image_id": image_id,
        "expected_digest": EXPECTED_IMAGE,
    }


def materialize_private_ca() -> None:
    ASSURANCE_RESULTS.mkdir(parents=True, exist_ok=True)
    if PRIVATE_CA.exists():
        PRIVATE_CA.chmod(stat.S_IREAD | stat.S_IWRITE)
        PRIVATE_CA.unlink()
    run(
        [
            "docker",
            "cp",
            f"{CONTAINER}:/var/opt/mssql/tls/ca.pem",
            str(PRIVATE_CA),
        ],
        timeout_seconds=DOCKER_TIMEOUT_SECONDS,
    )
    if not PRIVATE_CA.is_file() or PRIVATE_CA.stat().st_size == 0:
        raise RuntimeError("CA privata SQL Server non materializzata")


def certificate_identity(path: str) -> str:
    return docker_value(
        [
            "exec",
            CONTAINER,
            "openssl",
            "x509",
            "-in",
            path,
            "-noout",
            "-fingerprint",
            "-sha256",
            "-serial",
        ]
    )


def rotate_server_certificate() -> dict[str, str]:
    server_before = certificate_identity("/var/opt/mssql/tls/server.pem")
    ca_before = certificate_identity("/var/opt/mssql/tls/ca.pem")
    compose = str(ROOT / "docker-compose.sqlserver.yml")
    run(
        [
            "docker",
            "compose",
            "-f",
            compose,
            "run",
            "--rm",
            "-e",
            "PLENORA_TLS_ROTATE=1",
            "sqlserver-certgen",
        ],
        timeout_seconds=2 * 60,
    )
    run(
        [
            "docker",
            "compose",
            "-f",
            compose,
            "up",
            "-d",
            "--force-recreate",
            "--wait",
            "sqlserver",
        ],
        timeout_seconds=3 * 60,
    )
    server_after = certificate_identity("/var/opt/mssql/tls/server.pem")
    ca_after = certificate_identity("/var/opt/mssql/tls/ca.pem")
    if server_before == server_after:
        raise RuntimeError("rotazione TLS non ha cambiato il certificato server")
    if ca_before != ca_after:
        raise RuntimeError("rotazione TLS ha cambiato inaspettatamente la CA privata")
    materialize_private_ca()
    run_cargo(
        [
            "test",
            "-p",
            "plenora-db-sqlserver",
            "live_tests::live_private_ca_tls_validates_chain_and_hostname",
            "--locked",
            "--",
            "--ignored",
            "--exact",
        ],
        capture=True,
    )
    return {
        "ca": ca_after,
        "server_before": server_before,
        "server_after": server_after,
    }


def server_identity() -> dict[str, str]:
    output = docker_value(
        [
            "exec",
            CONTAINER,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-P",
            DEFAULT_PASSWORD,
            "-d",
            "dataflow_test",
            "-C",
            "-b",
            "-h",
            "-1",
            "-W",
            "-s",
            "|",
            "-Q",
            (
                "SET NOCOUNT ON; "
                "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')), "
                "CONVERT(nvarchar(128), SERVERPROPERTY('Edition')), "
                "CONVERT(nvarchar(10), compatibility_level) "
                "FROM sys.databases WHERE name = DB_NAME();"
            ),
        ]
    )
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) != 1:
        raise RuntimeError("identita SQL Server non deterministica")
    parts = [part.strip() for part in lines[0].split("|")]
    if len(parts) != 3 or not all(parts):
        raise RuntimeError("identita SQL Server incompleta")
    return {
        "product_version": parts[0],
        "edition": parts[1],
        "compatibility_level": parts[2],
    }


def validate_live_result(output: str) -> None:
    expected = re.compile(
        rf"test result: ok\. {EXPECTED_LIVE_TESTS} passed; "
        r"0 failed; 0 ignored; \d+ measured; \d+ filtered out"
    )
    if not expected.search(output):
        raise RuntimeError(
            f"matrice live SQL Server diversa da {EXPECTED_LIVE_TESTS}/"
            f"{EXPECTED_LIVE_TESTS}"
        )


def main() -> int:
    try:
        state = docker_value(
            [
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.Health.Status}}",
                CONTAINER,
            ]
        )
        if state != "running|healthy":
            raise RuntimeError("container SQL Server non healthy")
        image_identity = validate_image_pin()
        identity = server_identity()
        materialize_private_ca()
        run_cargo(
            [
                "clippy",
                "-p",
                "plenora-db-sqlserver",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ]
        )
        run_cargo(
            [
                "test",
                "-p",
                "plenora-db-sqlserver",
                "--lib",
                "--locked",
            ]
        )
        live_output = run_cargo(
            [
                "test",
                "-p",
                "plenora-db-sqlserver",
                "live_",
                "--locked",
                "--",
                "--ignored",
                "--test-threads=1",
            ],
            capture=True,
        )
        validate_live_result(live_output)
        tls_rotation = rotate_server_certificate()
    except RuntimeError as error:
        print(f"sqlserver reference gate: {error}", file=sys.stderr)
        return 1

    report = {
        "schema_version": 1,
        "gate": "sqlserver-reference-v1",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": True,
        "secrets_persisted": False,
        "container_image": image_identity,
        "server": identity,
        "tls_rotation": tls_rotation,
        "live_tests": {
            "expected": EXPECTED_LIVE_TESTS,
            "passed": EXPECTED_LIVE_TESTS,
            "failed": 0,
        },
        "checks": [
            "container_health",
            "immutable_image_digest",
            "server_identity",
            "clippy_deny_warnings",
            "offline_unit_tests",
            "provider_common_conformance",
            "bounded_arrow_read",
            "geometry_geography_xy_roundtrip",
            "prepared_write_reference_types",
            "keyed_update_upsert_delete",
            "tds_bulk_differential",
            "rich_query_cte_join_aggregate_window_set_offset",
            "empty_result_schema_description",
            "schema_drift_guard",
            "submicrosecond_temporal_fail_closed",
            "tls_verify_default",
            "private_ca_chain_hostname_and_rotation",
            "transaction_rollback",
            "physical_tds_cut",
            "physical_tds_blackhole",
            "unknown_commit_outcome",
            "atomic_create",
            "staged_replace_publish",
            "staged_replace_rollback_cleanup_dependencies_and_visibility",
            "create_replace_reference_type_profile",
        ],
        "open_non_blocking": [
            "sqlserver_2019_2025_azure_matrix",
            "spatial_z_m_fullglobe_unexercised_boundaries",
            "extended_temporal_graph_partition_catalog",
        ],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
