#!/usr/bin/env python3
"""Gate ripetibile per il percorso PostGIS bbox + KNN indicizzato."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import compose_network  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"
# La rete Compose si scopre dalle label del container: i compose
# dichiarano progetti distinti, quindi un nome scritto a mano si rompe
# in silenzio al primo rename.
REFERENCE_CONTAINER = "dataflow-postgres"
DEFAULT_DSN = (
    "host=dataflow-postgres port=5432 user=dataflow "
    "password=dataflow_test_2026 dbname=dataflow_test"
)
MAX_MEDIAN_MICROS = 50_000
MAX_P95_MICROS = 100_000


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


def main() -> int:
    dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN", DEFAULT_DSN)
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        compose_network(REFERENCE_CONTAINER),
        "-e",
        f"PLENORA_TEST_POSTGRES_DSN={dsn}",
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-w",
        "/workspace",
        IMAGE,
        "cargo",
        "test",
        "-p",
        "plenora-db-postgres",
        "live_spatial_index_benchmark",
        "--",
        "--ignored",
        "--nocapture",
    ]
    try:
        output = run(command, capture=True)
        matches = re.findall(
            r'\{"index_used":(?:true|false),"median_micros":\d+,'
            r'"p95_micros":\d+,"rows":100,"samples":50\}',
            output,
        )
        if not matches:
            raise RuntimeError("risultato benchmark spatial non trovato")
        benchmark = json.loads(matches[-1])
        if not benchmark["index_used"]:
            raise RuntimeError("piano PostGIS senza indice GiST atteso")
        if benchmark["median_micros"] > MAX_MEDIAN_MICROS:
            raise RuntimeError("mediana spatial oltre budget")
        if benchmark["p95_micros"] > MAX_P95_MICROS:
            raise RuntimeError("p95 spatial oltre budget")
    except RuntimeError as error:
        print(f"postgres spatial performance gate: {error}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "schema_version": 1,
                "gate": "postgres-postgis-spatial-index-performance-v1",
                "status": "passed",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "database_connections_opened": True,
                "secrets_persisted": False,
                "budget": {
                    "max_median_micros": MAX_MEDIAN_MICROS,
                    "max_p95_micros": MAX_P95_MICROS,
                },
                "benchmark": benchmark,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
