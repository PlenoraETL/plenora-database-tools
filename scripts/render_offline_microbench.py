#!/usr/bin/env python3
"""Genera le tabelle dei microbenchmark Rust dal JSONL versionato."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "benchmarks" / "raw" / "offline-rust-microbench.jsonl"
TARGET = ROOT / "benchmarks" / "offline-rust-microbench.md"


def number(value: float) -> str:
    return f"{value:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")


def render() -> str:
    records = [
        json.loads(line)
        for line in SOURCE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        grouped[str(record["bench"])].append(record)

    lines = [
        "# Microbenchmark Rust offline",
        "",
        "**Documento generato** da `raw/offline-rust-microbench.jsonl`; non va",
        "modificato a mano. Si aggiorna con:",
        "",
        "```powershell",
        "python scripts\\render_offline_microbench.py",
        "```",
        "",
        "## Scopo",
        "",
        "Le misure versionate coprono parsing e validazione dei piani, rendering",
        "SQL, ispezione EWKB, contratto Arrow e compilazione dei read plan. Il",
        "workflow misura inoltre compilazione statement, cache metadata, lifecycle",
        "di sessione/transazione e consumo a batch del Core v3. Nessuno scenario",
        "apre connessioni: pool, cursori e latenza reali restano campagne provider.",
        "",
        "Non esiste un budget per queste superfici. Il workflow",
        "`.github/workflows/rust-microbench.yml` misura e pubblica, ma non blocca",
        "la PR. Il JSONL versionato non registra hardware, sistema operativo o",
        "commit: questi numeri sono uno snapshot, non una baseline confrontabile.",
        "",
        "## Esecuzione",
        "",
        "```bash",
        "cargo build --release --locked --examples \\",
        "  --package plenora-database-core \\",
        "  --package plenora-database-sql \\",
        "  --package plenora-database-engine \\",
        "  --package plenora-db-mysql \\",
        "  --package plenora-db-sqlserver",
        "```",
        "",
        "Ogni esempio riceve iterazioni e ripetizioni e scrive JSONL su stdout.",
        "Per una futura baseline autorevole il verdetto dovra includere almeno",
        "commit, toolchain, profilo, sistema operativo e identita della macchina.",
        "",
        "## Misure versionate",
        "",
    ]
    for bench, entries in grouped.items():
        lines += [
            f"### `{bench}`",
            "",
            "| scenario | iterazioni | ripetizioni | ns/op | op/s | RSS KiB |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for entry in entries:
            lines.append(
                "| `{}` | {} | {} | {} | {} | {} |".format(
                    entry["scenario"],
                    entry["iterations"],
                    entry["repetitions"],
                    number(float(entry["nanoseconds_per_operation"])),
                    number(float(entry["operations_per_second"])),
                    entry["peak_rss_kib"],
                )
            )
        lines.append("")
    lines += [
        "## Decisioni ancora aperte",
        "",
        "- quali scenari meritano un budget;",
        "- quale margine usare sul runner che eseguira il gate;",
        "- se una regressione debba bloccare o soltanto produrre un avviso.",
        "",
        "Finche queste decisioni non sono esplicite, il workflow resta una misura",
        "e non viene presentato come gate.",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args(argv if argv is not None else sys.argv[1:])
    rendered = render()
    if arguments.check:
        current = TARGET.read_text(encoding="utf-8") if TARGET.is_file() else ""
        if current != rendered:
            print("offline microbench: documento non aggiornato", file=sys.stderr)
            return 1
        return 0
    TARGET.write_text(rendered, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
