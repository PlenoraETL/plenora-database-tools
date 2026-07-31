#!/usr/bin/env python3
"""Gate opt-in read-only per Azure SQL Database con TLS verificato."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    "PLENORA_SQLSERVER_HOST",
    "PLENORA_SQLSERVER_DATABASE",
    "PLENORA_SQLSERVER_USER",
    "PLENORA_SQLSERVER_PASSWORD",
)


def main() -> int:
    missing = [name for name in REQUIRED if not os.environ.get(name)]
    if missing:
        print(f"azure SQL gate: variabili mancanti: {', '.join(missing)}", file=sys.stderr)
        return 1
    completed = subprocess.run(
        [
            "cargo", "test", "-p", "plenora-db-sqlserver",
            "azure_sql_probe_uses_verified_tls_and_native_spatial_types",
            "--locked", "--", "--ignored", "--nocapture",
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=300,
    )
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    if completed.returncode:
        return 1
    if not re.search(r"test result: ok\. 1 passed; 0 failed;", completed.stdout):
        print("azure SQL gate: esito inatteso", file=sys.stderr)
        return 1
    print("azure SQL gate: passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
