#!/usr/bin/env python3
"""Verifica fail-closed dei budget di coverage prodotti da llvm-cov.

Il report e il budget sono input non fidati: una chiave assente, un numero non
finito o una percentuale incoerente non devono trasformarsi in un verde. Il
gate misura separatamente prodotto Rust e binding Python, per evitare che una
superficie grande nasconda la regressione dell'altra.
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
METRICS = ("functions", "lines", "regions")


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
    for metric in METRICS:
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


def read_budget(budget: dict[str, Any], surface: str) -> dict[str, float]:
    if budget.get("schema_version") != 1:
        raise CoverageError("budget: schema_version deve essere 1")
    surfaces = _object(budget.get("surfaces"), "budget.surfaces")
    selected = _object(surfaces.get(surface), f"budget.surfaces.{surface}")
    minimum = _object(selected.get("minimum_percent"), f"budget.{surface}.minimum_percent")
    if set(minimum) != set(METRICS):
        raise CoverageError(
            f"budget.{surface}: servono esattamente {', '.join(METRICS)}"
        )
    result: dict[str, float] = {}
    for metric in METRICS:
        value = _number(minimum[metric], f"budget.{surface}.{metric}")
        if not 0 <= value <= 100:
            raise CoverageError(f"budget.{surface}.{metric}: fuori da 0..100")
        result[metric] = value
    return result


def check(summary: Path, budget_path: Path, surface: str) -> bool:
    totals = read_totals(load_json(summary, "report"))
    minimum = read_budget(load_json(budget_path, "budget"), surface)
    failures: list[str] = []
    print(f"coverage: {surface}")
    for metric in METRICS:
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
