#!/usr/bin/env python3
"""Qualifica SQL Server 2019 e 2025 su immagini fissate per digest."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PASSWORD = "DataFlow_Test_2026!"
NETWORK = "plenora-sqlserver-matrix"
RUST_IMAGE = "rust:1.92"
EXPECTED_TESTS = 42


@dataclass(frozen=True)
class MatrixEntry:
    label: str
    major: int
    compatibility: int
    digest: str

    @property
    def image(self) -> str:
        return f"mcr.microsoft.com/mssql/server@sha256:{self.digest}"

    @property
    def container(self) -> str:
        return f"plenora-matrix-sqlserver-{self.major}"


MATRIX = (
    MatrixEntry(
        "SQL Server 2019",
        15,
        150,
        "46f719fd3457d4e7e8e5845fe00c35c20e7bae7ff1e8b9fe595f2a81029f5ba8",
    ),
    MatrixEntry(
        "SQL Server 2025",
        17,
        170,
        "86cc6144ef39bb0fbed2329e1ad79b13ee82e7b2e4739213a0db0800e668a74a",
    ),
)


def run(command: list[str], *, timeout: int = 900, capture: bool = False) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=capture,
        timeout=timeout,
    )
    if capture:
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
    if completed.returncode:
        raise RuntimeError(f"comando fallito: {command[0]}")
    return completed.stdout if capture else ""


def ensure_network() -> None:
    inspected = subprocess.run(
        ["docker", "network", "inspect", NETWORK],
        cwd=ROOT,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if inspected.returncode:
        run(["docker", "network", "create", NETWORK], timeout=30)


def sqlcmd_path(container: str) -> str:
    for candidate in ["/opt/mssql-tools18/bin/sqlcmd", "/opt/mssql-tools/bin/sqlcmd"]:
        found = subprocess.run(
            ["docker", "exec", container, "test", "-x", candidate],
            cwd=ROOT,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if not found.returncode:
            return candidate
    raise RuntimeError("sqlcmd assente dall'immagine SQL Server")


def wait_ready(entry: MatrixEntry, sqlcmd: str) -> None:
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        ready = subprocess.run(
            [
                "docker", "exec", entry.container, sqlcmd,
                "-S", "localhost", "-U", "sa", "-P", PASSWORD,
                "-C", "-b", "-Q", "SELECT 1",
            ],
            cwd=ROOT,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if not ready.returncode:
            return
        time.sleep(2)
    raise RuntimeError(f"timeout avvio {entry.label}")


def initialize(entry: MatrixEntry, sqlcmd: str) -> None:
    run(["docker", "cp", str(ROOT / "docker" / "sqlserver" / "init"), f"{entry.container}:/plenora-init"], timeout=30)
    for script in sorted((ROOT / "docker" / "sqlserver" / "init").glob("*.sql")):
        run([
            "docker", "exec", entry.container, sqlcmd,
            "-S", "localhost", "-U", "sa", "-P", PASSWORD,
            "-d", "master", "-C", "-b", "-i", f"/plenora-init/{script.name}",
        ], timeout=180)


def run_suite(entry: MatrixEntry) -> None:
    command = [
        "docker", "run", "--rm", "--network", NETWORK,
        "-v", f"{ROOT}:/workspace",
        "-v", f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-v", f"{ROOT.parent / 'plenora-rustup-cache'}:/usr/local/rustup",
        "-w", "/workspace",
        "-e", f"PLENORA_SQLSERVER_HOST={entry.container}",
        "-e", "PLENORA_SQLSERVER_DATABASE=dataflow_test",
        "-e", "PLENORA_SQLSERVER_USER=dataflow",
        "-e", f"PLENORA_SQLSERVER_PASSWORD={PASSWORD}",
        "-e", f"PLENORA_SQLSERVER_EXPECTED_MAJOR={entry.major}",
        "-e", f"PLENORA_SQLSERVER_EXPECTED_COMPATIBILITY={entry.compatibility}",
        RUST_IMAGE, "cargo", "test", "-p", "plenora-db-sqlserver", "live_",
        "--locked", "--", "--ignored", "--test-threads=1",
        "--skip", "live_private_ca_tls_validates_chain_and_hostname",
        "--skip", "polybase_external_catalog_is_structural_and_not_implicit",
        "--skip", "azure_sql_probe_uses_verified_tls_and_native_spatial_types",
    ]
    output = run(command, timeout=1200, capture=True)
    expected = re.compile(
        rf"test result: ok\. {EXPECTED_TESTS} passed; 0 failed; 0 ignored;"
    )
    if not expected.search(output):
        raise RuntimeError(f"matrice live inattesa per {entry.label}")


def qualify(entry: MatrixEntry) -> dict[str, object]:
    run([
        "docker", "run", "-d", "--name", entry.container,
        "--network", NETWORK,
        "-e", "ACCEPT_EULA=Y", "-e", f"MSSQL_SA_PASSWORD={PASSWORD}",
        "-e", "MSSQL_PID=Developer", entry.image,
    ], timeout=120)
    try:
        sqlcmd = sqlcmd_path(entry.container)
        wait_ready(entry, sqlcmd)
        initialize(entry, sqlcmd)
        run_suite(entry)
        identity = run([
            "docker", "exec", entry.container, sqlcmd,
            "-S", "localhost", "-U", "sa", "-P", PASSWORD,
            "-d", "dataflow_test", "-C", "-h", "-1", "-W", "-Q",
            "SET NOCOUNT ON; SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion'));",
        ], timeout=30, capture=True).strip()
        return {
            "label": entry.label,
            "major": entry.major,
            "compatibility_level": entry.compatibility,
            "image": entry.image,
            "product_version": identity,
            "live_tests": {"expected": EXPECTED_TESTS, "passed": EXPECTED_TESTS},
        }
    finally:
        subprocess.run(
            ["docker", "rm", "--force", entry.container],
            cwd=ROOT,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def main() -> int:
    ensure_network()
    results = []
    try:
        for entry in MATRIX:
            results.append(qualify(entry))
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"sqlserver matrix gate: {error}", file=sys.stderr)
        return 1
    report = {
        "schema_version": 1,
        "gate": "sqlserver-version-matrix-v1",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "results": results,
        "azure_sql": "requires_external_opt_in_gate_check_sqlserver_azure.py",
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
