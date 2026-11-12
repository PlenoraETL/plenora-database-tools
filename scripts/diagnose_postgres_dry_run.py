#!/usr/bin/env python3
"""Ripete gli scenari del gate sul fixture e stampa solo esiti, mai payload."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.vettori_arrow_data import Postgres, SCENARI, Scenari


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--postgres-env", default="PLENORA_TEST_POSTGRES_DSN")
    args = parser.parse_args()
    if not os.environ.get(args.postgres_env):
        parser.error("variabile DSN del fixture assente o vuota")
    # La stessa deroga del gate per il fixture Compose plaintext.
    os.environ.setdefault("PLENORA_TLS_INSECURE_LOCAL", "1")
    root = Path(__file__).resolve().parents[1] / "target"
    root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dryrun-diagnostic-", dir=root) as directory:
        rows = Scenari(Postgres(args.cli.resolve(), args.postgres_env, Path(directory))).esegui()
    definitions = {s[0]: s for s in SCENARI}
    for row in rows:
        scenario = definitions[row["scenario"]]
        result = {"scenario": row["scenario"], "mode": scenario[2],
                  "policy": scenario[5], "input_fields": scenario[3],
                  "input_rows": 2, "role": scenario[6],
                  "profile": "single_transaction", "accept_loss": False,
                  "verdetto": row["verdetto"]}
        for surface in ("dry_run", "scrittura"):
            result[surface] = {axis: row[surface].get(axis) for axis in
                               ("exit", "category", "phase", "retry", "remote_effect")}
        print(json.dumps(result, ensure_ascii=True))
    return int(any(row["verdetto"] != "conforme" for row in rows))


if __name__ == "__main__":
    raise SystemExit(main())
