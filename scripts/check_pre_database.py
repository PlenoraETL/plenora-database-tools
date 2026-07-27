#!/usr/bin/env python3
"""Gate completo prima dei provider reali; non apre connessioni database."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
RUST_IMAGE = "rust:1.92"


def execute(command: Sequence[str]) -> None:
    completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"check fallito: {command[0]}")


def cargo_command(arguments: Sequence[str]) -> list[str]:
    cargo = shutil.which("cargo")
    if cargo:
        return [cargo, *arguments]
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError("cargo e docker non disponibili")
    cache = REPO_ROOT.parent / "plenora-cargo-cache"
    cache.mkdir(parents=True, exist_ok=True)
    return [
        docker,
        "run",
        "--rm",
        "-v",
        f"{REPO_ROOT}:/workspace",
        "-v",
        f"{cache}:/usr/local/cargo/registry",
        "-w",
        "/workspace",
        RUST_IMAGE,
        "cargo",
        *arguments,
    ]


def write_json_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    checks = [
        ("phase0-offline", [sys.executable, "scripts/check_phase0.py"]),
        ("rustfmt", cargo_command(["fmt", "--all", "--", "--check"])),
        (
            "clippy",
            cargo_command(
                [
                    "clippy",
                    "--workspace",
                    "--all-targets",
                    "--",
                    "-D",
                    "warnings",
                ]
            ),
        ),
        (
            "rust-tests",
            cargo_command(["test", "--workspace", "--all-targets"]),
        ),
        (
            "cli-contract",
            cargo_command(
                [
                    "run",
                    "-q",
                    "-p",
                    "plenora-database-cli",
                    "--",
                    "validate-plan",
                    "contracts/v1/examples/plan-postgres-read.json",
                ]
            ),
        ),
    ]
    passed = []
    try:
        for check_id, command in checks:
            execute(command)
            passed.append({"id": check_id, "status": "passed"})
    except RuntimeError as exc:
        print(f"pre-database gate: {exc}", file=sys.stderr)
        return 1
    report = {
        "schema_version": 1,
        "gate": "pre-database-complete",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": 0,
        "rust_toolchain": "1.92.0",
        "checks": passed,
    }
    if args.output:
        write_json_atomic(args.output.resolve(), report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
