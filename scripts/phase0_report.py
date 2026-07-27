#!/usr/bin/env python3
"""Aggrega i JSONL della Fase 0 in statistiche riproducibili."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from scripts.phase0_harness import SCHEMA_VERSION, stable_json_digest
except ModuleNotFoundError:  # esecuzione diretta: python scripts\...
    from phase0_harness import SCHEMA_VERSION, stable_json_digest


class ReportError(RuntimeError):
    pass


def nearest_rank(values: Sequence[int], percentile: float) -> int:
    if not values:
        raise ReportError("percentile su insieme vuoto")
    if not 0 < percentile <= 1:
        raise ReportError("percentile fuori intervallo")
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def read_records(paths: Iterable[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ReportError(
                        f"JSONL non valido: {path.name}:{line_number}"
                    ) from exc
                if item.get("case_id"):
                    records.append(item)
    if not records:
        raise ReportError("nessun record case trovato")
    return records


def aggregate(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[str(record["case_id"])].append(record)

    cases: list[dict[str, Any]] = []
    for case_id in sorted(grouped):
        samples = grouped[case_id]
        passed = [sample for sample in samples if sample.get("status") == "passed"]
        failed = [sample for sample in samples if sample.get("status") != "passed"]
        wall = [
            int(sample["metrics"]["wall_ns"])
            for sample in passed
            if sample.get("metrics", {}).get("wall_ns") is not None
        ]
        rss_deltas = []
        for sample in passed:
            before = sample.get("metrics", {}).get("rss_before_bytes")
            after = sample.get("metrics", {}).get("rss_after_bytes")
            if before is not None and after is not None:
                rss_deltas.append(int(after) - int(before))
        summary_digests = sorted(
            {
                stable_json_digest(sample.get("summary"))
                for sample in passed
            }
        )
        first = samples[0]
        case: dict[str, Any] = {
            "case_id": case_id,
            "provider": first.get("provider"),
            "samples": len(samples),
            "passed": len(passed),
            "failed": len(failed),
            "stable_summary": len(summary_digests) <= 1,
            "summary_digests": summary_digests,
        }
        if wall:
            case["wall_ns"] = {
                "min": min(wall),
                "median": int(statistics.median(wall)),
                "p95_nearest_rank": nearest_rank(wall, 0.95),
                "max": max(wall),
            }
        if rss_deltas:
            case["rss_delta_bytes"] = {
                "min": min(rss_deltas),
                "median": int(statistics.median(rss_deltas)),
                "max": max(rss_deltas),
            }
        cases.append(case)

    return {
        "schema_version": SCHEMA_VERSION,
        "report": "phase0-aggregate",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cases": cases,
        "totals": {
            "cases": len(cases),
            "samples": sum(item["samples"] for item in cases),
            "passed": sum(item["passed"] for item in cases),
            "failed": sum(item["failed"] for item in cases),
            "unstable_summaries": sum(
                1 for item in cases if not item["stable_summary"]
            ),
        },
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Report aggregato Fase 0",
        "",
        f"Generato: {report['generated_at']}",
        "",
        "| Caso | Provider | Campioni | Pass/Fail | Mediana ms | p95 ms | Summary stabile |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for case in report["cases"]:
        timing = case.get("wall_ns") or {}
        median_ms = (
            f"{timing['median'] / 1_000_000:.3f}"
            if "median" in timing
            else "n/d"
        )
        p95_ms = (
            f"{timing['p95_nearest_rank'] / 1_000_000:.3f}"
            if "p95_nearest_rank" in timing
            else "n/d"
        )
        lines.append(
            "| {case_id} | {provider} | {samples} | {passed}/{failed} | "
            "{median} | {p95} | {stable} |".format(
                case_id=case["case_id"],
                provider=case.get("provider") or "",
                samples=case["samples"],
                passed=case["passed"],
                failed=case["failed"],
                median=median_ms,
                p95=p95_ms,
                stable="sì" if case["stable_summary"] else "NO",
            )
        )
    totals = report["totals"]
    lines.extend(
        [
            "",
            "## Totali",
            "",
            f"- casi: {totals['cases']};",
            f"- campioni: {totals['samples']};",
            f"- passati: {totals['passed']};",
            f"- falliti: {totals['failed']};",
            f"- summary instabili: {totals['unstable_summaries']}.",
            "",
            "Il p95 usa il metodo nearest-rank.",
            "",
        ]
    )
    return "\n".join(lines)


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
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
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--json", type=Path)
    parser.add_argument("--markdown", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    import sys

    args = parse_args(argv or sys.argv[1:])
    try:
        report = aggregate(read_records(path.resolve() for path in args.inputs))
        if args.json:
            write_text_atomic(
                args.json.resolve(),
                json.dumps(
                    report,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
                + "\n",
            )
        if args.markdown:
            write_text_atomic(args.markdown.resolve(), render_markdown(report))
        if not args.json and not args.markdown:
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    except ReportError as exc:
        print(f"phase0 report: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
