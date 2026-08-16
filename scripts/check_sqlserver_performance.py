#!/usr/bin/env python3
"""Gate prestazionale riproducibile del provider SQL Server."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import compose_network  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"
# La rete Compose si scopre dalle label del container: i compose
# dichiarano progetti distinti, quindi un nome scritto a mano si rompe
# in silenzio al primo rename.
REFERENCE_CONTAINER = "dataflow-sqlserver"
RESULT_PREFIX = "PLENORA_SQLSERVER_PERF_RESULT="
DEFAULT_MANIFEST = ROOT / "benchmarks/manifests/sqlserver-performance-reference.json"
DEFAULT_BUDGET = ROOT / "benchmarks/baseline/sqlserver-performance-budget.json"
EXPECTED_IMAGE = (
    "sha256:e07b9699a2b749969f19d86563ceeea22bd3a69f7f1db85a8d1ac4bdaf0c6f56"
)
EXPECTED_REFERENCE = f"mcr.microsoft.com/mssql/server@{EXPECTED_IMAGE}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--budget", type=Path, default=DEFAULT_BUDGET)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--freeze", type=Path)
    return parser.parse_args()


def load_object(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} non contiene un oggetto JSON")
    return value


def validate_manifest(value: dict[str, Any]) -> None:
    if value.get("schema_version") != 1:
        raise ValueError("schema_version manifest non supportata")
    for key, minimum, maximum in (
        ("rows", 1, 1_000_000),
        ("batch_rows", 1, 65_536),
        ("warmup", 0, 100),
        ("repeat", 1, 100),
    ):
        candidate = value.get(key)
        if not isinstance(candidate, int) or not minimum <= candidate <= maximum:
            raise ValueError(f"{key} fuori intervallo")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def stats(values: list[int | float]) -> dict[str, float]:
    numeric = [float(value) for value in values]
    if not numeric:
        raise ValueError("campioni prestazionali assenti")
    return {
        "median": statistics.median(numeric),
        "p95": percentile(numeric, 0.95),
        "min": min(numeric),
        "max": max(numeric),
    }


def docker_state() -> None:
    completed = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}|{{.State.Health.Status}}",
            "dataflow-sqlserver",
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode or completed.stdout.strip() != "running|healthy":
        raise RuntimeError("container SQL Server non healthy")


def environment_identity(manifest: dict[str, Any]) -> dict[str, Any]:
    completed = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.Config.Image}}|{{.Image}}",
            "dataflow-sqlserver",
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    identity = completed.stdout.strip().split("|")
    if (
        completed.returncode
        or len(identity) != 2
        or (identity[0] != EXPECTED_REFERENCE and identity[1] != EXPECTED_IMAGE)
    ):
        raise RuntimeError("immagine SQL Server prestazionale non conforme")
    return {
        "platform": platform.system().lower(),
        "machine": platform.machine().lower(),
        "cpu_count": os.cpu_count(),
        "sqlserver_reference": identity[0],
        "sqlserver_runtime_image": identity[1],
        "rust_image": IMAGE,
        "campaign": manifest["campaign"],
    }


def execute(manifest: dict[str, Any]) -> dict[str, Any]:
    environment = {
        "PLENORA_SQLSERVER_HOST": "sqlserver",
        "PLENORA_SQLSERVER_DATABASE": "dataflow_test",
        "PLENORA_SQLSERVER_USER": "dataflow",
        "PLENORA_SQLSERVER_PASSWORD": "DataFlow_Test_2026!",
        "PLENORA_SQLSERVER_PERF_ROWS": str(manifest["rows"]),
        "PLENORA_SQLSERVER_PERF_BATCH_ROWS": str(manifest["batch_rows"]),
        "PLENORA_SQLSERVER_PERF_WARMUP": str(manifest["warmup"]),
        "PLENORA_SQLSERVER_PERF_REPEAT": str(manifest["repeat"]),
    }
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        compose_network(REFERENCE_CONTAINER),
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-v",
        f"{ROOT.parent / 'plenora-rustup-cache'}:/usr/local/rustup",
        "-w",
        "/workspace",
    ]
    for key, value in environment.items():
        command.extend(["-e", f"{key}={value}"])
    command.extend(
        [
            IMAGE,
            "cargo",
            "run",
            "--release",
            "-q",
            "-p",
            "plenora-db-sqlserver",
            "--example",
            "sqlserver_performance",
            "--locked",
        ]
    )
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=20 * 60,
    )
    if completed.returncode:
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise RuntimeError("esecuzione campagna prestazionale SQL Server fallita")
    matches = [
        line.removeprefix(RESULT_PREFIX)
        for line in completed.stdout.splitlines()
        if line.startswith(RESULT_PREFIX)
    ]
    if len(matches) != 1:
        raise RuntimeError("risultato prestazionale SQL Server non univoco")
    return json.loads(matches[0])


def aggregate(raw: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    repeat = manifest["repeat"]
    rows = manifest["rows"]
    reads = raw.get("reads")
    writes = raw.get("writes")
    lifecycle = raw.get("lifecycle")
    if not isinstance(reads, list) or len(reads) != repeat:
        raise RuntimeError("campioni read SQL Server incompleti")
    if not isinstance(writes, list) or len(writes) != repeat * 2:
        raise RuntimeError("campioni write SQL Server incompleti")
    if not isinstance(lifecycle, list) or len(lifecycle) != repeat * 2:
        raise RuntimeError("campioni lifecycle SQL Server incompleti")
    if any(sample.get("rows") != rows for sample in [*reads, *writes, *lifecycle]):
        raise RuntimeError("conteggio righe prestazionale non coerente")
    if any(sample.get("differences") != 0 for sample in writes):
        raise RuntimeError("differenziale write SQL Server non nullo")

    def mode_samples(samples: list[dict[str, Any]], mode: str) -> list[dict[str, Any]]:
        selected = [sample for sample in samples if sample.get("mode") == mode]
        if len(selected) != repeat:
            raise RuntimeError(f"campioni {mode} incompleti")
        return selected

    summary: dict[str, Any] = {
        "read_total_micros": stats([sample["total_micros"] for sample in reads]),
        "read_rows_per_second": stats(
            [sample["rows_per_second"] for sample in reads]
        ),
    }
    for mode in ("prepared", "tds_bulk"):
        selected = mode_samples(writes, mode)
        summary[f"{mode}_total_micros"] = stats(
            [sample["total_micros"] for sample in selected]
        )
        summary[f"{mode}_rows_per_second"] = stats(
            [sample["rows_per_second"] for sample in selected]
        )
    for mode in ("create", "replace"):
        selected = mode_samples(lifecycle, mode)
        summary[f"{mode}_total_micros"] = stats(
            [sample["total_micros"] for sample in selected]
        )
        summary[f"{mode}_rows_per_second"] = stats(
            [sample["rows_per_second"] for sample in selected]
        )
    return summary


def enforce_budget(
    summary: dict[str, Any], raw: dict[str, Any], budget: dict[str, Any]
) -> None:
    if budget.get("schema_version") != 1:
        raise ValueError("schema_version budget non supportata")
    limits = budget.get("limits")
    if not isinstance(limits, dict):
        raise ValueError("limits budget assente")
    for mode in ("read", "prepared", "tds_bulk", "create", "replace"):
        latency = summary[f"{mode}_total_micros"]["p95"]
        maximum = limits[f"{mode}_p95_micros_max"]
        if latency > maximum:
            raise RuntimeError(f"{mode} p95 {latency} oltre budget {maximum}")
        throughput = summary[f"{mode}_rows_per_second"]["median"]
        minimum = limits[f"{mode}_median_rows_per_second_min"]
        if throughput < minimum:
            raise RuntimeError(
                f"{mode} throughput mediano {throughput} sotto budget {minimum}"
            )
    peak = raw.get("peak_rss_bytes")
    if isinstance(peak, int) and peak > limits["peak_rss_bytes_max"]:
        raise RuntimeError(
            f"peak RSS {peak} oltre budget {limits['peak_rss_bytes_max']}"
        )


def compare_baseline(
    summary: dict[str, Any],
    environment: dict[str, Any],
    baseline: dict[str, Any],
    budget: dict[str, Any],
) -> dict[str, Any]:
    baseline_environment = baseline.get("environment")
    if not isinstance(baseline_environment, dict):
        return {
            "status": "not_comparable",
            "environment_mismatches": ["baseline_environment_missing"],
        }
    keys = (
        "platform",
        "machine",
        "cpu_count",
        "sqlserver_reference",
        "sqlserver_runtime_image",
        "rust_image",
        "campaign",
    )
    mismatches = [
        key
        for key in keys
        if environment.get(key) != baseline_environment.get(key)
    ]
    if mismatches:
        return {
            "status": "not_comparable",
            "environment_mismatches": mismatches,
        }
    multiplier = budget.get("regression", {}).get("p95_latency_multiplier_max")
    throughput_floor = budget.get("regression", {}).get(
        "median_throughput_ratio_min"
    )
    if not isinstance(multiplier, (int, float)) or not isinstance(
        throughput_floor, (int, float)
    ):
        raise ValueError("budget regressione incompleto")
    baseline_summary = baseline.get("summary")
    if not isinstance(baseline_summary, dict):
        raise ValueError("summary baseline assente")
    for mode in ("read", "prepared", "tds_bulk", "create", "replace"):
        current_latency = summary[f"{mode}_total_micros"]["p95"]
        previous_latency = baseline_summary[f"{mode}_total_micros"]["p95"]
        if current_latency > previous_latency * multiplier:
            raise RuntimeError(f"regressione latenza {mode}")
        current_rate = summary[f"{mode}_rows_per_second"]["median"]
        previous_rate = baseline_summary[f"{mode}_rows_per_second"]["median"]
        if current_rate < previous_rate * throughput_floor:
            raise RuntimeError(f"regressione throughput {mode}")
    return {"status": "passed", "environment_mismatches": []}


def main() -> int:
    args = parse_args()
    try:
        manifest = load_object(args.manifest)
        budget = load_object(args.budget)
        validate_manifest(manifest)
        docker_state()
        environment = environment_identity(manifest)
        raw = execute(manifest)
        summary = aggregate(raw, manifest)
        enforce_budget(summary, raw, budget)
        baseline_comparison = {"status": "not_requested"}
        if args.baseline:
            baseline_comparison = compare_baseline(
                summary,
                environment,
                load_object(args.baseline),
                budget,
            )
        report = {
            "schema_version": 1,
            "gate": "sqlserver-performance-v1",
            "status": "passed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "manifest": manifest,
            "environment": environment,
            "baseline_comparison": baseline_comparison,
            "summary": summary,
            "raw": raw,
        }
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(f"{rendered}\n", encoding="utf-8")
        if args.freeze:
            args.freeze.parent.mkdir(parents=True, exist_ok=True)
            args.freeze.write_text(f"{rendered}\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"sqlserver performance gate: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
