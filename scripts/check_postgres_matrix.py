#!/usr/bin/env python3
"""Matrice live sulle major PostgreSQL supportate e immagini PostGIS stabili."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"
NETWORK = "plenora-database-tools_default"
CACHE = ROOT.parent / "plenora-cargo-cache"
PASSWORD = "plenora_matrix_test_2026"
TARGETS = [
    ("14", "3.5", "postgis/postgis:14-3.5"),
    ("15", "3.5", "postgis/postgis:15-3.5"),
    ("16", "3.5", "postgis/postgis:16-3.5"),
    ("17", "3.5", "postgis/postgis:17-3.5"),
    ("18", "3.6", "postgis/postgis:18-3.6"),
]


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


def ensure_network() -> None:
    inspected = subprocess.run(
        ["docker", "network", "inspect", NETWORK],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    if inspected.returncode:
        run(["docker", "network", "create", NETWORK])


def wait_until_ready(container: str) -> None:
    deadline = time.monotonic() + 120
    stable_identity: str | None = None
    stable_checks = 0
    while time.monotonic() < deadline:
        ready = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "psql",
                "-U",
                "plenora_matrix",
                "-d",
                "plenora_matrix",
                "-At",
                "-F",
                "|",
                "-c",
                (
                    "SELECT pg_postmaster_start_time()::text, "
                    "to_regclass('plenora_fixture.advanced_types') IS NOT NULL"
                ),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        identity = ready.stdout.strip()
        if ready.returncode == 0 and identity.endswith("|t"):
            if identity == stable_identity:
                stable_checks += 1
            else:
                stable_identity = identity
                stable_checks = 1
            if stable_checks >= 2:
                return
        else:
            stable_identity = None
            stable_checks = 0
        state = run(
            ["docker", "inspect", "--format", "{{.State.Status}}", container],
            capture=True,
        ).strip()
        if state == "exited":
            raise RuntimeError(f"container {container} terminato durante l'avvio")
        time.sleep(1)
    raise RuntimeError(f"timeout healthcheck {container}")


def cargo_test(container: str) -> None:
    dsn = (
        f"host={container} port=5432 user=plenora_matrix "
        f"password={PASSWORD} dbname=plenora_matrix"
    )
    run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            NETWORK,
            "-e",
            f"PLENORA_TEST_POSTGRES_DSN={dsn}",
            "-v",
            f"{ROOT}:/workspace",
            "-v",
            f"{CACHE}:/usr/local/cargo/registry",
            "-w",
            "/workspace",
            IMAGE,
            "cargo",
            "test",
            "-p",
            "plenora-db-postgres",
            "--lib",
            "--",
            "--nocapture",
        ]
    )


def inspect_versions(container: str) -> dict[str, str]:
    output = run(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-U",
            "plenora_matrix",
            "-d",
            "plenora_matrix",
            "-At",
            "-F",
            "|",
            "-c",
            "SELECT current_setting('server_version'), postgis_lib_version()",
        ],
        capture=True,
    ).strip()
    postgres, postgis = output.split("|", maxsplit=1)
    return {"postgres": postgres, "postgis": postgis}


def test_target(postgres: str, postgis: str, image: str) -> dict[str, str]:
    container = f"plenora-matrix-pg{postgres}"
    run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            container,
            "--network",
            NETWORK,
            "-e",
            "POSTGRES_USER=plenora_matrix",
            "-e",
            f"POSTGRES_PASSWORD={PASSWORD}",
            "-e",
            "POSTGRES_DB=plenora_matrix",
            "-v",
            f"{ROOT / 'docker' / 'postgres' / 'init'}:/docker-entrypoint-initdb.d:ro",
            image,
        ]
    )
    try:
        wait_until_ready(container)
        cargo_test(container)
        versions = inspect_versions(container)
        if versions["postgres"].split(".", maxsplit=1)[0] != postgres:
            raise RuntimeError(
                f"major PostgreSQL inattesa per {image}: {versions['postgres']}"
            )
        if not versions["postgis"].startswith(f"{postgis}."):
            raise RuntimeError(
                f"versione PostGIS inattesa per {image}: {versions['postgis']}"
            )
        return {
            "declared_postgres": postgres,
            "declared_postgis": postgis,
            "image": image,
            "actual_postgres": versions["postgres"],
            "actual_postgis": versions["postgis"],
            "status": "passed",
        }
    except RuntimeError:
        logs = subprocess.run(
            ["docker", "logs", "--tail", "120", container],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        sys.stderr.write(logs.stdout)
        sys.stderr.write(logs.stderr)
        raise
    finally:
        subprocess.run(
            ["docker", "stop", "--time", "10", container],
            cwd=ROOT,
            check=False,
            capture_output=True,
        )


def main() -> int:
    CACHE.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, str]] = []
    try:
        ensure_network()
        for postgres, postgis, image in TARGETS:
            results.append(test_target(postgres, postgis, image))
    except (RuntimeError, ValueError) as error:
        print(f"postgres matrix gate: {error}", file=sys.stderr)
        return 1
    report = {
        "schema_version": 1,
        "gate": "postgres-postgis-supported-major-matrix",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": True,
        "secrets_persisted": False,
        "targets": results,
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
