#!/usr/bin/env python3
"""Verifica fail-closed dei budget di coverage Rust e Python.

Il report e il budget sono input non fidati: una chiave assente, un numero non
finito o una percentuale incoerente non devono trasformarsi in un verde. Il
gate misura separatamente prodotto Rust, binding nativo e SDK Python, per
evitare che una superficie grande nasconda la regressione dell'altra.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUDGET = ROOT / "scripts" / "coverage_budget.json"
LLVM_METRICS = ("functions", "lines", "regions")
PYTHON_METRICS = ("lines", "branches")
REPORT_METRICS = {
    "llvm": LLVM_METRICS,
    "coverage.py": PYTHON_METRICS,
}
# Alias mantenuto per i consumatori del checker esistente.
METRICS = LLVM_METRICS


class CoverageError(ValueError):
    """Il report o il budget non possono sostenere un verdetto."""


def _object(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CoverageError(f"{where}: atteso un oggetto JSON")
    return value


def _number(value: Any, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CoverageError(f"{where}: atteso un numero")
    number = float(value)
    if not math.isfinite(number):
        raise CoverageError(f"{where}: il numero deve essere finito")
    return number


def load_json(path: Path, kind: str) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            return _object(json.load(handle), kind)
    except (OSError, json.JSONDecodeError) as error:
        raise CoverageError(f"{kind}: impossibile leggere {path}: {error}") from error


def read_totals(report: dict[str, Any]) -> dict[str, tuple[int, int, float]]:
    if report.get("type") != "llvm.coverage.json.export":
        raise CoverageError("report: tipo llvm-cov assente o sconosciuto")
    data = report.get("data")
    if not isinstance(data, list) or len(data) != 1:
        raise CoverageError("report: atteso esattamente un blocco data")
    totals = _object(_object(data[0], "report.data[0]").get("totals"), "report.totals")

    result: dict[str, tuple[int, int, float]] = {}
    for metric in LLVM_METRICS:
        item = _object(totals.get(metric), f"report.totals.{metric}")
        count_number = _number(item.get("count"), f"report.{metric}.count")
        covered_number = _number(item.get("covered"), f"report.{metric}.covered")
        percent = _number(item.get("percent"), f"report.{metric}.percent")
        if not count_number.is_integer() or count_number <= 0:
            raise CoverageError(f"report.{metric}.count: atteso un intero positivo")
        if not covered_number.is_integer() or not 0 <= covered_number <= count_number:
            raise CoverageError(f"report.{metric}.covered: conteggio non valido")
        if not 0 <= percent <= 100:
            raise CoverageError(f"report.{metric}.percent: fuori dall'intervallo 0..100")
        expected = 100.0 * covered_number / count_number
        if not math.isclose(percent, expected, rel_tol=0.0, abs_tol=1e-6):
            raise CoverageError(f"report.{metric}.percent: incoerente con i conteggi")
        result[metric] = (int(covered_number), int(count_number), percent)
    return result


def read_python_totals(report: dict[str, Any]) -> dict[str, tuple[int, int, float]]:
    meta = _object(report.get("meta"), "report.meta")
    if meta.get("format") != 3:
        raise CoverageError("report: formato coverage.py assente o sconosciuto")
    if meta.get("branch_coverage") is not True:
        raise CoverageError("report: branch coverage Python non abilitata")
    files = report.get("files")
    if not isinstance(files, dict) or not files:
        raise CoverageError("report: nessun file Python misurato")
    totals = _object(report.get("totals"), "report.totals")
    fields = {
        "lines": ("covered_lines", "num_statements"),
        "branches": ("covered_branches", "num_branches"),
    }
    result: dict[str, tuple[int, int, float]] = {}
    for metric, (covered_key, count_key) in fields.items():
        count_number = _number(totals.get(count_key), f"report.{metric}.{count_key}")
        covered_number = _number(
            totals.get(covered_key), f"report.{metric}.{covered_key}"
        )
        if not count_number.is_integer() or count_number <= 0:
            raise CoverageError(f"report.{metric}.{count_key}: atteso un intero positivo")
        if not covered_number.is_integer() or not 0 <= covered_number <= count_number:
            raise CoverageError(f"report.{metric}.{covered_key}: conteggio non valido")
        result[metric] = (
            int(covered_number),
            int(count_number),
            100.0 * covered_number / count_number,
        )
    return result


def read_budget(
    budget: dict[str, Any], surface: str
) -> tuple[str, dict[str, float]]:
    if budget.get("schema_version") != 1:
        raise CoverageError("budget: schema_version deve essere 1")
    surfaces = _object(budget.get("surfaces"), "budget.surfaces")
    selected = _object(surfaces.get(surface), f"budget.surfaces.{surface}")
    report_format = selected.get("report_format")
    if not isinstance(report_format, str) or report_format not in REPORT_METRICS:
        raise CoverageError(f"budget.{surface}: report_format assente o sconosciuto")
    metrics = REPORT_METRICS[report_format]
    minimum = _object(selected.get("minimum_percent"), f"budget.{surface}.minimum_percent")
    if set(minimum) != set(metrics):
        raise CoverageError(
            f"budget.{surface}: servono esattamente {', '.join(metrics)}"
        )
    result: dict[str, float] = {}
    for metric in metrics:
        value = _number(minimum[metric], f"budget.{surface}.{metric}")
        if not 0 <= value <= 100:
            raise CoverageError(f"budget.{surface}.{metric}: fuori da 0..100")
        result[metric] = value
    return report_format, result


def check(summary: Path, budget_path: Path, surface: str) -> bool:
    report_format, minimum = read_budget(load_json(budget_path, "budget"), surface)
    report = load_json(summary, "report")
    totals = (
        read_totals(report)
        if report_format == "llvm"
        else read_python_totals(report)
    )
    failures: list[str] = []
    print(f"coverage: {surface}")
    for metric in REPORT_METRICS[report_format]:
        covered, count, actual = totals[metric]
        threshold = minimum[metric]
        status = "OK" if actual >= threshold else "FAIL"
        print(
            f"  {status:4} {metric:9} {actual:6.2f}% "
            f"({covered}/{count}), minimo {threshold:.2f}%"
        )
        if actual < threshold:
            failures.append(metric)
    if failures:
        print(f"coverage sotto budget: {', '.join(failures)}", file=sys.stderr)
        return False
    return True


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", required=True, type=Path)
    parser.add_argument("--surface", required=True)
    parser.add_argument("--budget", type=Path, default=DEFAULT_BUDGET)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return 0 if check(args.summary, args.budget, args.surface) else 1
    except CoverageError as error:
        print(f"coverage non verificabile: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
