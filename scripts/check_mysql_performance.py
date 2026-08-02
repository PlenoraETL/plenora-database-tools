#!/usr/bin/env python3
"""Gate prestazionale riproducibile del provider MySQL.

La campagna copre soltanto le forme gia qualificate live: lettura Arrow
streaming e scrittura `Append` dentro il profilo `SingleTransaction`. Il gate
resta fail-closed: manifest, budget e campioni vengono validati prima di
qualunque verdetto e il confronto con una baseline avviene solo quando
l'ambiente osservato coincide con quello registrato. In assenza di baseline
misurata il confronto viene dichiarato `not_requested`, mai simulato.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"
CONTAINER = "dataflow-mysql"
RESULT_PREFIX = "PLENORA_MYSQL_PERF_RESULT="
APPEND_MODE = "append_single_transaction"
MODES = ("read", "append")
DEFAULT_MANIFEST = ROOT / "benchmarks/manifests/mysql-performance-reference.json"
DEFAULT_BUDGET = ROOT / "benchmarks/baseline/mysql-performance-budget.json"
EXPECTED_DIGEST = (
    "sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb"
)
EXPECTED_REFERENCE = f"mysql@{EXPECTED_DIGEST}"
EXPECTED_VERSION_PREFIX = "8.4."
MANIFEST_BOUNDS = (
    ("rows", 1, 1_000_000),
    ("batch_rows", 1, 65_536),
    ("warmup", 0, 100),
    ("repeat", 1, 100),
)
BUDGET_LIMIT_KEYS = (
    *(f"{mode}_p95_micros_max" for mode in MODES),
    *(f"{mode}_median_rows_per_second_min" for mode in MODES),
    "peak_rss_bytes_max",
)
REGRESSION_KEYS = ("p95_latency_multiplier_max", "median_throughput_ratio_min")
ENVIRONMENT_KEYS = (
    "platform",
    "machine",
    "cpu_count",
    "mysql_reference",
    "mysql_runtime_image",
    "mysql_version",
    "rust_image",
    "campaign",
)


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


def bounded_integer(value: Any, minimum: int, maximum: int) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and minimum <= value <= maximum
    )


def validate_manifest(value: dict[str, Any]) -> None:
    if value.get("schema_version") != 1:
        raise ValueError("schema_version manifest non supportata")
    campaign = value.get("campaign")
    if not isinstance(campaign, str) or not campaign:
        raise ValueError("campaign manifest assente")
    for key, minimum, maximum in MANIFEST_BOUNDS:
        if not bounded_integer(value.get(key), minimum, maximum):
            raise ValueError(f"{key} fuori intervallo [{minimum}, {maximum}]")


def validate_budget(value: dict[str, Any]) -> None:
    if value.get("schema_version") != 1:
        raise ValueError("schema_version budget non supportata")
    limits = value.get("limits")
    if not isinstance(limits, dict):
        raise ValueError("limits budget assente")
    for key in BUDGET_LIMIT_KEYS:
        candidate = limits.get(key)
        if not isinstance(candidate, (int, float)) or isinstance(candidate, bool):
            raise ValueError(f"limite budget {key} assente o non numerico")
        if candidate <= 0:
            raise ValueError(f"limite budget {key} non positivo")
    regression = value.get("regression")
    if not isinstance(regression, dict):
        raise ValueError("sezione regression budget assente")
    for key in REGRESSION_KEYS:
        candidate = regression.get(key)
        if not isinstance(candidate, (int, float)) or isinstance(candidate, bool):
            raise ValueError(f"regression budget {key} assente o non numerico")
        if candidate <= 0:
            raise ValueError(f"regression budget {key} non positivo")


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
            CONTAINER,
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode or completed.stdout.strip() != "running|healthy":
        raise RuntimeError("container MySQL di riferimento non healthy")


def mysql_network() -> str:
    override = os.environ.get("PLENORA_MYSQL_PERF_NETWORK")
    if override:
        return override
    completed = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{json .NetworkSettings.Networks}}",
            CONTAINER,
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError("interrogazione network MySQL fallita")
    networks = json.loads(completed.stdout)
    if not isinstance(networks, dict) or len(networks) != 1:
        raise RuntimeError(
            "network MySQL assente o ambiguo; impostare PLENORA_MYSQL_PERF_NETWORK"
        )
    return next(iter(networks))


def fixture_password() -> str:
    completed = subprocess.run(
        ["docker", "inspect", "--format", "{{json .Config.Env}}", CONTAINER],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError("interrogazione ambiente container MySQL fallita")
    prefix = "MYSQL_PASSWORD="
    for entry in json.loads(completed.stdout):
        if entry.startswith(prefix) and entry.removeprefix(prefix):
            return entry.removeprefix(prefix)
    raise RuntimeError("password utente fixture MySQL assente")


def mysql_tls_volume() -> str:
    completed = subprocess.run(
        ["docker", "inspect", "--format", "{{json .Mounts}}", CONTAINER],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError("interrogazione mount container MySQL fallita")
    for mount in json.loads(completed.stdout):
        if mount.get("Destination") == "/etc/mysql/tls" and mount.get("Name"):
            return str(mount["Name"])
    raise RuntimeError("volume CA MySQL non montato nel container di riferimento")


def mysql_version() -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER,
            "/bin/sh",
            "-c",
            'exec env MYSQL_PWD="$MYSQL_PASSWORD" mysql -Nse "$1" '
            "-u dataflow --ssl-mode=REQUIRED",
            "mysql-performance-probe",
            "SELECT VERSION()",
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError("probe versione MySQL prestazionale fallita")
    version = completed.stdout.strip()
    if not version.startswith(EXPECTED_VERSION_PREFIX):
        raise RuntimeError(f"versione MySQL inattesa: {version}")
    return version


def environment_identity(manifest: dict[str, Any]) -> dict[str, Any]:
    completed = subprocess.run(
        ["docker", "inspect", "--format", "{{.Config.Image}}|{{.Image}}", CONTAINER],
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
        or identity[0] != EXPECTED_REFERENCE
        or re.fullmatch(r"sha256:[0-9a-f]{64}", identity[1]) is None
    ):
        raise RuntimeError("immagine MySQL prestazionale non conforme al digest")
    loaded = subprocess.run(
        [
            "docker",
            "image",
            "inspect",
            identity[1],
            "--format",
            "{{json .RepoDigests}}",
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    try:
        repo_digests = json.loads(loaded.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            "immagine MySQL prestazionale non conforme al digest"
        ) from error
    if (
        loaded.returncode
        or not isinstance(repo_digests, list)
        or EXPECTED_REFERENCE not in repo_digests
    ):
        raise RuntimeError("immagine MySQL prestazionale non conforme al digest")
    return {
        "platform": platform.system().lower(),
        "machine": platform.machine().lower(),
        "cpu_count": os.cpu_count(),
        "mysql_reference": identity[0],
        "mysql_runtime_image": identity[1],
        "mysql_version": mysql_version(),
        "rust_image": IMAGE,
        "campaign": manifest["campaign"],
    }


def execute(manifest: dict[str, Any]) -> dict[str, Any]:
    environment = {
        "PLENORA_MYSQL_HOST": CONTAINER,
        "PLENORA_MYSQL_DATABASE": "dataflow_test",
        "PLENORA_MYSQL_USER": "dataflow",
        "PLENORA_MYSQL_CA": "/mysql-tls/ca.pem",
        "PLENORA_MYSQL_PERF_ROWS": str(manifest["rows"]),
        "PLENORA_MYSQL_PERF_BATCH_ROWS": str(manifest["batch_rows"]),
        "PLENORA_MYSQL_PERF_WARMUP": str(manifest["warmup"]),
        "PLENORA_MYSQL_PERF_REPEAT": str(manifest["repeat"]),
    }
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        mysql_network(),
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        "plenora-cargo-registry:/usr/local/cargo/registry",
        "-v",
        "plenora-cargo-git:/usr/local/cargo/git",
        "-v",
        f"{mysql_tls_volume()}:/mysql-tls:ro",
        "-w",
        "/workspace",
    ]
    for key, value in environment.items():
        command.extend(["-e", f"{key}={value}"])
    # La password resta fuori dagli argomenti: viaggia solo nell'ambiente del
    # processo docker, come nel gate di riferimento.
    command.extend(["-e", "PLENORA_MYSQL_PASSWORD"])
    command.extend(
        [
            IMAGE,
            "cargo",
            "run",
            "--release",
            "-q",
            "-p",
            "plenora-db-mysql",
            "--example",
            "mysql_performance",
            "--locked",
        ]
    )
    process_environment = os.environ.copy()
    process_environment["PLENORA_MYSQL_PASSWORD"] = fixture_password()
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=process_environment,
        check=False,
        text=True,
        capture_output=True,
        timeout=40 * 60,
    )
    if completed.returncode:
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise RuntimeError("esecuzione campagna prestazionale MySQL fallita")
    matches = [
        line.removeprefix(RESULT_PREFIX)
        for line in completed.stdout.splitlines()
        if line.startswith(RESULT_PREFIX)
    ]
    if len(matches) != 1:
        raise RuntimeError("risultato prestazionale MySQL non univoco")
    return json.loads(matches[0])


def samples(raw: dict[str, Any], key: str, expected: int) -> list[dict[str, Any]]:
    value = raw.get(key)
    if not isinstance(value, list) or len(value) != expected:
        raise RuntimeError(f"campioni {key} MySQL incompleti o in eccesso")
    for sample in value:
        if not isinstance(sample, dict):
            raise RuntimeError(f"campione {key} MySQL non strutturato")
    return value


def metric(sample: dict[str, Any], key: str) -> int:
    value = sample.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RuntimeError(f"metrica MySQL {key} assente o non numerica")
    return value


def aggregate(raw: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    repeat = manifest["repeat"]
    rows = manifest["rows"]
    expected_header = {
        "schema_version": 1,
        "profile": "append-single-transaction",
        "rows": rows,
        "batch_rows": manifest["batch_rows"],
        "warmup": manifest["warmup"],
        "repeat": repeat,
    }
    if any(raw.get(key) != value for key, value in expected_header.items()):
        raise RuntimeError("intestazione risultato prestazionale MySQL incoerente")
    reads = samples(raw, "reads", repeat)
    writes = samples(raw, "writes", repeat)
    for sample in reads:
        for key in ("rows", "total_micros", "rows_per_second"):
            metric(sample, key)
    for sample in writes:
        for key in ("rows", "rows_per_second", "differences", "transactions"):
            metric(sample, key)
    if any(metric(sample, "rows") != rows for sample in [*reads, *writes]):
        raise RuntimeError("conteggio righe prestazionale MySQL non coerente")
    if any(sample.get("mode") != APPEND_MODE for sample in writes):
        raise RuntimeError(
            f"modo write MySQL fuori dall'append qualificato ({APPEND_MODE})"
        )
    if any(metric(sample, "differences") != 0 for sample in writes):
        raise RuntimeError("differenziale write MySQL non nullo")
    if any(metric(sample, "transactions") != 1 for sample in writes):
        raise RuntimeError("append MySQL non osservato in una singola transazione")
    for sample in writes:
        prepare = metric(sample, "prepare_micros")
        execute_micros = metric(sample, "execute_micros")
        total = metric(sample, "total_micros")
        if prepare + execute_micros != total:
            raise RuntimeError(
                "decomposizione prepare/execute MySQL incoerente con il totale"
            )
    return {
        "read_total_micros": stats([sample["total_micros"] for sample in reads]),
        "read_rows_per_second": stats([sample["rows_per_second"] for sample in reads]),
        "append_total_micros": stats([sample["total_micros"] for sample in writes]),
        "append_prepare_micros": stats([sample["prepare_micros"] for sample in writes]),
        "append_execute_micros": stats([sample["execute_micros"] for sample in writes]),
        "append_rows_per_second": stats(
            [sample["rows_per_second"] for sample in writes]
        ),
    }


def enforce_budget(
    summary: dict[str, Any], raw: dict[str, Any], budget: dict[str, Any]
) -> None:
    validate_budget(budget)
    limits = budget["limits"]
    for mode in MODES:
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
    if not isinstance(peak, int) or isinstance(peak, bool) or peak <= 0:
        raise RuntimeError("evidenza peak RSS MySQL assente o non valida")
    if peak > limits["peak_rss_bytes_max"]:
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
    mismatches = [
        key
        for key in ENVIRONMENT_KEYS
        if environment.get(key) != baseline_environment.get(key)
    ]
    if mismatches:
        return {"status": "not_comparable", "environment_mismatches": mismatches}
    validate_budget(budget)
    multiplier = budget["regression"]["p95_latency_multiplier_max"]
    throughput_floor = budget["regression"]["median_throughput_ratio_min"]
    baseline_summary = baseline.get("summary")
    if not isinstance(baseline_summary, dict):
        raise ValueError("summary baseline assente")
    for mode in MODES:
        current_latency = summary[f"{mode}_total_micros"]["p95"]
        previous_latency = baseline_summary[f"{mode}_total_micros"]["p95"]
        if current_latency > previous_latency * multiplier:
            raise RuntimeError(f"regressione latenza {mode}")
        current_rate = summary[f"{mode}_rows_per_second"]["median"]
        previous_rate = baseline_summary[f"{mode}_rows_per_second"]["median"]
        if current_rate < previous_rate * throughput_floor:
            raise RuntimeError(f"regressione throughput {mode}")
    return {"status": "passed", "environment_mismatches": []}


def baseline_comparison(
    path: Path | None,
    summary: dict[str, Any],
    environment: dict[str, Any],
    budget: dict[str, Any],
) -> dict[str, Any]:
    """Nessuna baseline misurata viene inventata: assente e `not_requested`,
    indicata ma inesistente e `not_comparable` con la ragione esplicita."""
    if path is None:
        return {"status": "not_requested"}
    if not path.is_file():
        return {
            "status": "not_comparable",
            "environment_mismatches": ["baseline_file_absent"],
        }
    return compare_baseline(summary, environment, load_object(path), budget)


def main() -> int:
    args = parse_args()
    try:
        manifest = load_object(args.manifest)
        budget = load_object(args.budget)
        validate_manifest(manifest)
        validate_budget(budget)
        docker_state()
        environment = environment_identity(manifest)
        raw = execute(manifest)
        summary = aggregate(raw, manifest)
        enforce_budget(summary, raw, budget)
        comparison = baseline_comparison(args.baseline, summary, environment, budget)
        report = {
            "schema_version": 1,
            "gate": "mysql-performance-v1",
            "status": "passed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "manifest": manifest,
            "environment": environment,
            "baseline_comparison": comparison,
            "summary": summary,
            "raw": raw,
        }
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        for destination in (args.output, args.freeze):
            if destination:
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(f"{rendered}\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"mysql performance gate: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
