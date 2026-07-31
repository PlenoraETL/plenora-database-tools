#!/usr/bin/env python3
"""Gate riproducibile del riferimento MySQL 8.4 iniziale."""

from __future__ import annotations

import json
import os
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
DEFAULT_PASSWORD = "DataFlow_Test_2026!"


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


def cargo(arguments: list[str]) -> tuple[list[str], dict[str, str] | None]:
    if os.environ.get("PLENORA_MYSQL_GATE_HOST_CARGO") == "1":
        environment = os.environ.copy()
        environment.setdefault("PLENORA_MYSQL_HOST", "127.0.0.1")
        environment.setdefault("PLENORA_MYSQL_DATABASE", "dataflow_test")
        environment.setdefault("PLENORA_MYSQL_USER", "dataflow")
        environment.setdefault("PLENORA_MYSQL_PASSWORD", DEFAULT_PASSWORD)
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
        "plenora-cargo-registry:/usr/local/cargo/registry",
        "-v",
        "plenora-cargo-git:/usr/local/cargo/git",
        "-w",
        "/workspace",
        "-e",
        f"PLENORA_MYSQL_HOST={CONTAINER}",
        "-e",
        "PLENORA_MYSQL_DATABASE=dataflow_test",
        "-e",
        "PLENORA_MYSQL_USER=dataflow",
        "-e",
        f"PLENORA_MYSQL_PASSWORD={DEFAULT_PASSWORD}",
        RUST_IMAGE,
        "/usr/local/cargo/bin/cargo",
        *arguments,
    ]
    return command, None


def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
    command, environment = cargo(arguments)
    return run(command, environment=environment, capture=capture)


def validate_reference() -> dict[str, str]:
    compose = (ROOT / "docker-compose.mysql.yml").read_text(encoding="utf-8")
    if compose.count(EXPECTED_REFERENCE) != 1:
        raise RuntimeError("digest MySQL 8.4 non fissato in modo univoco")
    configured = docker_value(["inspect", "--format", "{{.Config.Image}}", CONTAINER])
    image_id = docker_value(["inspect", "--format", "{{.Image}}", CONTAINER])
    if configured != EXPECTED_REFERENCE and image_id != EXPECTED_DIGEST:
        raise RuntimeError("container MySQL diverso dal digest di riferimento")
    version = docker_value(
        [
            "exec",
            CONTAINER,
            "mysql",
            "-Nse",
            "SELECT VERSION()",
            "-u",
            "root",
            f"-pDataFlow_Root_2026!",
            "--ssl-mode=REQUIRED",
        ]
    )
    if not version.startswith("8.4."):
        raise RuntimeError(f"versione MySQL inattesa: {version}")
    return {"configured_reference": configured, "image_id": image_id, "version": version}


def main() -> int:
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
    run_cargo(["test", "-p", "plenora-db-mysql", "--locked"])
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
    if "test result: ok. 4 passed; 0 failed" not in live_output:
        raise RuntimeError("conteggio test live MySQL inatteso")
    print(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed",
                "provider": "mysql",
                "reference": "MySQL 8.4 LTS",
                "live_tests": 4,
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
