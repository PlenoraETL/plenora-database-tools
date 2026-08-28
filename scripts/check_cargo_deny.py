#!/usr/bin/env python3
"""Esegue la policy cargo-deny in un container riproducibile."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "docker" / "cargo-deny" / "Dockerfile"
BUILD_CONTEXT = DOCKERFILE.parent
IMAGE = "plenora-cargo-deny:0.20.2"
ADVISORY_VOLUME = "plenora-cargo-deny-advisory"


def commands(root: Path = ROOT) -> tuple[list[str], list[str], list[str]]:
    """Comandi di build e dei due check, esposti per il self-test statico."""

    build = [
        "docker",
        "build",
        "--file",
        str(DOCKERFILE),
        "--tag",
        IMAGE,
        str(BUILD_CONTEXT),
    ]
    check = [
        "docker",
        "run",
        "--rm",
        "--volume",
        f"{root}:/workspace:ro",
        "--volume",
        f"{ADVISORY_VOLUME}:/root/.cargo/advisory-db",
        IMAGE,
        "check",
        "--hide-inclusion-graph",
    ]
    fuzz_check = [
        "docker",
        "run",
        "--rm",
        "--volume",
        f"{root}:/workspace:ro",
        "--volume",
        f"{ADVISORY_VOLUME}:/root/.cargo/advisory-db",
        IMAGE,
        "--manifest-path",
        "fuzz/Cargo.toml",
        "check",
        "--hide-inclusion-graph",
    ]
    return build, check, fuzz_check


def run(command: list[str]) -> None:
    completed = subprocess.run(command, cwd=ROOT, check=False)
    if completed.returncode:
        raise RuntimeError(f"cargo-deny Docker fallito durante {command[1]}")


def main() -> int:
    try:
        for command in commands():
            run(command)
    except RuntimeError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
