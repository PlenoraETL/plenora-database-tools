#!/usr/bin/env python3
"""Gate live Apache AGE 1.7.0 / PostgreSQL 18."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.compose_network import compose_network, container_variable


CONTAINER = "plenora-age"
IMAGE = "rust:1.98"
LIVE_TEST = "age::tests::live::live_age_1_7_pg18_parameters_types_and_transactions"


def age_dsn() -> str:
    return (
        f"host={CONTAINER} port=5432 "
        f"user={container_variable(CONTAINER, 'POSTGRES_USER')} "
        f"password={container_variable(CONTAINER, 'POSTGRES_PASSWORD')} "
        f"dbname={container_variable(CONTAINER, 'POSTGRES_DB')}"
    )


def cargo_command() -> list[str]:
    network = compose_network(CONTAINER, required_alias="age")
    return [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-v",
        "plenora-age-current-target:/workspace/target",
        "-w",
        "/workspace",
        "--network",
        network,
        "-e",
        f"PLENORA_TEST_AGE_DSN={age_dsn()}",
        "-e",
        "PLENORA_REQUIRE_LIVE_AGE=1",
        IMAGE,
        "cargo",
        "test",
        "-p",
        "plenora-db-postgres",
        LIVE_TEST,
        "--",
        "--exact",
        "--nocapture",
    ]


def main() -> int:
    completed = subprocess.run(
        cargo_command(), cwd=ROOT, check=False, text=True, capture_output=True
    )
    print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="")
    if completed.returncode:
        raise RuntimeError("gate live AGE fallito")
    if LIVE_TEST not in completed.stdout or "1 passed" not in completed.stdout:
        raise RuntimeError("il test live AGE richiesto non risulta eseguito")
    print("AGE_REFERENCE_OK version=1.7.0 postgres_major=18")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
