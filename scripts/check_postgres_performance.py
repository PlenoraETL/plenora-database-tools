#!/usr/bin/env python3
"""Campagna prestazionale riproducibile per PostgreSQL/PostGIS."""

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
REFERENCE_CONTAINER = "dataflow-postgres"
DEFAULT_DSN = (
    "host=dataflow-postgres port=5432 user=dataflow "
    "password=dataflow_test_2026 dbname=dataflow_test"
)
RESULT_PREFIX = "PLENORA_PERF_RESULT="
WRITE_MODES = ("copy_text", "copy_binary", "prepared")
DEFAULT_MANIFEST = ROOT / "benchmarks/manifests/postgres-performance-smoke.json"
DEFAULT_BUDGET = ROOT / "benchmarks/baseline/postgres-performance-budget.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--baseline",
        type=Path,
        help="report numerico precedente da confrontare",
    )
    parser.add_argument("--budget", type=Path, default=DEFAULT_BUDGET)
    parser.add_argument(
        "--freeze",
        type=Path,
        help="scrive anche una baseline solo con almeno i campioni richiesti",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} non contiene un oggetto JSON")
    return value


def validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != 1:
        raise ValueError("schema_version manifest non supportata")
    if not isinstance(manifest.get("campaign"), str):
        raise ValueError("campaign mancante")
    for name in ("warmup", "repeat"):
        value = manifest.get(name)
        if not isinstance(value, int) or value < (0 if name == "warmup" else 1):
            raise ValueError(f"{name} non valido")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("scenarios deve essere una lista non vuota")
    seen: set[tuple[str, int, int, int | None, int, bool, bool]] = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise ValueError("scenario non valido")
        profile = scenario.get("profile")
        rows = scenario.get("rows")
        batch_rows = scenario.get("batch_rows")
        if profile not in {"narrow", "wide", "spatial"}:
            raise ValueError(f"profilo non valido: {profile}")
        if not isinstance(rows, int) or not 0 < rows <= 100_000_000:
            raise ValueError("rows fuori intervallo")
        if not isinstance(batch_rows, int) or batch_rows <= 0:
            raise ValueError("batch_rows non valido")
        modes = scenario.get("modes", list(WRITE_MODES))
        if (
            not isinstance(modes, list)
            or any(mode not in WRITE_MODES for mode in modes)
            or len(modes) != len(set(modes))
        ):
            raise ValueError("modes non valido")
        target_batch_bytes = scenario.get("target_batch_bytes")
        if target_batch_bytes is not None and (
            not isinstance(target_batch_bytes, int) or target_batch_bytes <= 0
        ):
            raise ValueError("target_batch_bytes non valido")
        schema_cache_entries = scenario.get("schema_cache_entries", 256)
        if (
            not isinstance(schema_cache_entries, int)
            or not 0 <= schema_cache_entries <= 100_000
        ):
            raise ValueError("schema_cache_entries non valido")
        parameterized_read = scenario.get("parameterized_read", False)
        parameterized_fast_path = scenario.get("parameterized_fast_path", True)
        query_ast = scenario.get("query_ast", False)
        if not isinstance(parameterized_read, bool):
            raise ValueError("parameterized_read non valido")
        if not isinstance(parameterized_fast_path, bool):
            raise ValueError("parameterized_fast_path non valido")
        if not isinstance(query_ast, bool):
            raise ValueError("query_ast non valido")
        key = (
            profile,
            rows,
            batch_rows,
            target_batch_bytes,
            schema_cache_entries,
            parameterized_read,
            parameterized_fast_path,
            query_ast,
        )
        if key in seen:
            raise ValueError(f"scenario duplicato: {key}")
        seen.add(key)


def docker_state() -> None:
    completed = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}|{{.State.Health.Status}}",
            "dataflow-postgres",
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode or completed.stdout.strip() != "running|healthy":
        raise RuntimeError("container dataflow-postgres non healthy")


def cargo_command(env: dict[str, str]) -> list[str]:
    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-w",
        "/workspace",
        "--network",
        compose_network(REFERENCE_CONTAINER),
    ]
    for key, value in env.items():
        command.extend(["-e", f"{key}={value}"])
    return [
        *command,
        IMAGE,
        "cargo",
        "run",
        "--release",
        "-q",
        "-p",
        "plenora-db-postgres",
        "--example",
        "postgres_performance",
    ]


def execute_scenario(
    scenario: dict[str, Any],
    *,
    warmup: int,
    repeat: int,
    dsn: str,
) -> dict[str, Any]:
    public_name = (
        f"{scenario['profile']}/{scenario['rows']}/"
        f"batch-{scenario['batch_rows']}"
    )
    print(f"scenario {public_name}", flush=True)
    environment = {
        "PLENORA_TEST_POSTGRES_DSN": dsn,
        "PLENORA_PERF_PROFILE": str(scenario["profile"]),
        "PLENORA_PERF_ROWS": str(scenario["rows"]),
        "PLENORA_PERF_BATCH_ROWS": str(scenario["batch_rows"]),
        "PLENORA_PERF_WARMUP": str(warmup),
        "PLENORA_PERF_REPEAT": str(repeat),
        "PLENORA_PERF_MODES": ",".join(scenario.get("modes", WRITE_MODES)),
    }
    if "target_batch_bytes" in scenario:
        environment["PLENORA_PERF_TARGET_BATCH_BYTES"] = str(
            scenario["target_batch_bytes"]
        )
    if "schema_cache_entries" in scenario:
        environment["PLENORA_PERF_SCHEMA_CACHE_ENTRIES"] = str(
            scenario["schema_cache_entries"]
        )
    if "parameterized_read" in scenario:
        environment["PLENORA_PERF_PARAMETERIZED_READ"] = str(
            scenario["parameterized_read"]
        ).lower()
    if "parameterized_fast_path" in scenario:
        environment["PLENORA_PERF_PARAMETERIZED_FAST_PATH"] = str(
            scenario["parameterized_fast_path"]
        ).lower()
    if "query_ast" in scenario:
        environment["PLENORA_PERF_QUERY_AST"] = str(scenario["query_ast"]).lower()
    completed = subprocess.run(
        cargo_command(environment),
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise RuntimeError(f"scenario fallito: {public_name}")
    matches = [
        line.removeprefix(RESULT_PREFIX)
        for line in completed.stdout.splitlines()
        if line.startswith(RESULT_PREFIX)
    ]
    if len(matches) != 1:
        raise RuntimeError(f"risultato univoco non trovato: {public_name}")
    result = json.loads(matches[0])
    if len(result.get("reads", [])) != repeat:
        raise RuntimeError(f"campioni read incompleti: {public_name}")
    expected_modes = scenario.get("modes", list(WRITE_MODES))
    if result.get("modes") != expected_modes:
        raise RuntimeError(f"strategie write non coerenti: {public_name}")
    if len(result.get("writes", [])) != repeat * len(expected_modes):
        raise RuntimeError(f"campioni write incompleti: {public_name}")
    if any(sample.get("differences") != 0 for sample in result["writes"]):
        raise RuntimeError(f"differenza dati rilevata: {public_name}")
    if any(sample.get("rows") != scenario["rows"] for sample in result["reads"]):
        raise RuntimeError(f"conteggio read non coerente: {public_name}")
    if any(sample.get("rows") != scenario["rows"] for sample in result["writes"]):
        raise RuntimeError(f"conteggio write non coerente: {public_name}")
    return result


def percentile_nearest_rank(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def stats(values: list[int | float]) -> dict[str, float]:
    numeric = [float(value) for value in values]
    return {
        "median": statistics.median(numeric),
        "p95": percentile_nearest_rank(numeric, 0.95),
        "min": min(numeric),
        "max": max(numeric),
    }


def summarize_scenario(result: dict[str, Any]) -> dict[str, Any]:
    reads = result["reads"]
    read_summary = {
        metric: stats([sample[metric] for sample in reads])
        for metric in (
            "acquire_micros",
            "first_batch_micros",
            "remaining_micros",
            "total_micros",
            "rows_per_second",
            "materialized_bytes",
            "min_batch_rows",
            "max_batch_rows",
            "max_batch_bytes",
        )
    }
    write_summary: dict[str, Any] = {}
    for mode in result["modes"]:
        samples = [sample for sample in result["writes"] if sample["mode"] == mode]
        write_summary[mode] = {
            metric: stats([sample[metric] for sample in samples])
            for metric in (
                "prepare_micros",
                "execute_micros",
                "total_micros",
                "rows_per_second",
                "wal_bytes",
            )
        }
    return {
        "profile": result["profile"],
        "rows": result["rows"],
        "batch_rows": result["batch_rows"],
        "target_batch_bytes": result["configured_target_batch_bytes"],
        "schema_cache_entries": result.get("configured_schema_cache_entries", 256),
        "parameterized_read": result.get("configured_parameterized_read", False),
        "parameterized_fast_path": result.get(
            "configured_parameterized_fast_path", True
        ),
        "query_ast": result.get("configured_query_ast", False),
        "peak_rss_bytes": result["peak_rss_bytes"],
        "read": read_summary,
        "write": write_summary,
    }


def environment(results: list[dict[str, Any]]) -> dict[str, Any]:
    postgres = {result["postgres_version"] for result in results}
    postgis = {result["postgis_version"] for result in results}
    if len(postgres) != 1 or len(postgis) != 1:
        raise RuntimeError("versioni database incoerenti fra scenari")
    postgres_version = postgres.pop()
    postgis_version = postgis.pop()
    return {
        "postgres_version": postgres_version,
        "postgres_major": postgres_version.split(".", maxsplit=1)[0],
        "postgis_version": postgis_version,
        "postgis_major_minor": ".".join(postgis_version.split(".")[:2]),
        "platform": f"{platform.system()}-{platform.machine()}",
        "cpu_count": os.cpu_count(),
        "rust_image": IMAGE,
        "container": "dataflow-postgres",
    }


def scenario_key(
    summary: dict[str, Any],
) -> tuple[str, int, int, int | None, int, bool, bool, bool]:
    return (
        summary["profile"],
        summary["rows"],
        summary["batch_rows"],
        summary.get("target_batch_bytes"),
        summary.get("schema_cache_entries", 256),
        summary.get("parameterized_read", False),
        summary.get("parameterized_fast_path", True),
        summary.get("query_ast", False),
    )


def regression_percent(current: float, baseline: float) -> float:
    if baseline == 0:
        return 0.0 if current == 0 else math.inf
    return (current - baseline) * 100.0 / baseline


def compare_reports(
    current: dict[str, Any],
    baseline: dict[str, Any],
    budget: dict[str, Any],
) -> dict[str, Any]:
    mismatches = [
        field
        for field in budget["environment_must_match"]
        if current["environment"].get(field) != baseline["environment"].get(field)
    ]
    if mismatches:
        return {
            "status": "not_comparable",
            "environment_mismatches": mismatches,
            "missing_baseline_scenarios": [],
            "regressions": [],
        }
    old_by_key = {
        scenario_key(summary): summary for summary in baseline["summaries"]
    }
    missing_scenarios = [
        list(scenario_key(summary))
        for summary in current["summaries"]
        if scenario_key(summary) not in old_by_key
    ]
    if missing_scenarios:
        return {
            "status": "not_comparable",
            "environment_mismatches": [],
            "missing_baseline_scenarios": missing_scenarios,
            "regressions": [],
        }
    limits = budget["maximum_regression_percent"]
    regressions: list[dict[str, Any]] = []
    checks: list[tuple[str, str, str, str, str]] = [
        ("read_total_median", "read", "total_micros", "median", "lower"),
        ("read_total_p95", "read", "total_micros", "p95", "lower"),
        (
            "time_to_first_batch_median",
            "read",
            "first_batch_micros",
            "median",
            "lower",
        ),
    ]
    for summary in current["summaries"]:
        key = scenario_key(summary)
        old = old_by_key.get(key)
        if old is None:
            continue
        for budget_name, section, metric, statistic, _direction in checks:
            change = regression_percent(
                summary[section][metric][statistic],
                old[section][metric][statistic],
            )
            if change > limits[budget_name]:
                regressions.append(
                    {
                        "scenario": list(key),
                        "metric": budget_name,
                        "change_percent": change,
                        "limit_percent": limits[budget_name],
                    }
                )
        for mode in summary["write"]:
            if mode not in old["write"]:
                continue
            for statistic, budget_name in (
                ("median", "write_execute_median"),
                ("p95", "write_execute_p95"),
            ):
                change = regression_percent(
                    summary["write"][mode]["execute_micros"][statistic],
                    old["write"][mode]["execute_micros"][statistic],
                )
                if change > limits[budget_name]:
                    regressions.append(
                        {
                            "scenario": list(key),
                            "mode": mode,
                            "metric": budget_name,
                            "change_percent": change,
                            "limit_percent": limits[budget_name],
                        }
                    )
            wal_change = regression_percent(
                summary["write"][mode]["wal_bytes"]["median"],
                old["write"][mode]["wal_bytes"]["median"],
            )
            if wal_change > limits["wal_bytes"]:
                regressions.append(
                    {
                        "scenario": list(key),
                        "mode": mode,
                        "metric": "wal_bytes",
                        "change_percent": wal_change,
                        "limit_percent": limits["wal_bytes"],
                    }
                )
        current_rss = summary.get("peak_rss_bytes")
        baseline_rss = old.get("peak_rss_bytes")
        if current_rss is not None and baseline_rss is not None:
            rss_change = regression_percent(current_rss, baseline_rss)
            if rss_change > limits["peak_rss_bytes"]:
                regressions.append(
                    {
                        "scenario": list(key),
                        "metric": "peak_rss_bytes",
                        "change_percent": rss_change,
                        "limit_percent": limits["peak_rss_bytes"],
                    }
                )
    return {
        "status": "failed" if regressions else "passed",
        "environment_mismatches": [],
        "missing_baseline_scenarios": [],
        "regressions": regressions,
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as target:
        json.dump(report, target, ensure_ascii=False, indent=2, sort_keys=True)
        target.write("\n")


def main() -> int:
    args = parse_args()
    try:
        manifest_path = args.manifest.resolve()
        budget_path = args.budget.resolve()
        manifest = load_json(manifest_path)
        budget = load_json(budget_path)
        validate_manifest(manifest)
        docker_state()
        dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN", DEFAULT_DSN)
        results = [
            execute_scenario(
                scenario,
                warmup=manifest["warmup"],
                repeat=manifest["repeat"],
                dsn=dsn,
            )
            for scenario in manifest["scenarios"]
        ]
        report: dict[str, Any] = {
            "schema_version": 1,
            "campaign": manifest["campaign"],
            "status": "passed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "secrets_persisted": False,
            "manifest": str(manifest_path.relative_to(ROOT)).replace("\\", "/"),
            "environment": environment(results),
            "sample_count": manifest["repeat"],
            "warmup_count": manifest["warmup"],
            "summaries": [summarize_scenario(result) for result in results],
            "raw_results": results,
        }
        if args.baseline:
            comparison = compare_reports(
                report,
                load_json(args.baseline.resolve()),
                budget,
            )
            report["comparison"] = comparison
            if comparison["status"] == "failed":
                report["status"] = "failed"
        output = args.output
        if output is None:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output = ROOT / f"benchmarks/results/postgres-performance-{stamp}.json"
        output = output.resolve()
        write_report(output, report)
        if args.freeze:
            minimum = int(budget["minimum_samples"])
            if manifest["repeat"] < minimum:
                raise RuntimeError(
                    f"baseline non congelata: servono almeno {minimum} campioni"
                )
            write_report(args.freeze.resolve(), report)
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "output": str(output),
                    "campaign": report["campaign"],
                    "scenarios": len(results),
                    "samples_per_scenario": manifest["repeat"],
                },
                ensure_ascii=False,
            )
        )
        return 1 if report["status"] == "failed" else 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"postgres performance gate: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
