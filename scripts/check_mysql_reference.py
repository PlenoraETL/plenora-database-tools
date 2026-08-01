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
EXPECTED_OFFLINE_TESTS = 34
EXPECTED_LIVE_TESTS = {
    "live_deadline_reports_timeout_and_quarantines_the_session",
    "live_early_stream_drop_cancels_worker_and_keeps_provider_usable",
    "live_inflight_cancellation_quarantines_the_session",
    "live_operation_timeout_quarantines_the_session",
    "live_pool_acquire_timeout_is_independent_from_connect_timeout",
    "live_pool_reset_reapplies_deterministic_session_bootstrap",
    "live_provider_connection_capabilities_and_inspect",
    "live_read_projection_filter_order_and_default_schema",
    "live_reference_probe_catalog_and_spatial_metadata",
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
        return ["cargo", *arguments], environment

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
    executed_offline_tests = re.findall(r"^test [^ ]+ \.\.\. ok$", offline_output, re.MULTILINE)
    if len(executed_offline_tests) != EXPECTED_OFFLINE_TESTS:
        raise RuntimeError(
            "numero test offline MySQL inatteso: "
            f"{len(executed_offline_tests)}, attesi {EXPECTED_OFFLINE_TESTS}"
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
                "offline_tests": EXPECTED_OFFLINE_TESTS,
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
