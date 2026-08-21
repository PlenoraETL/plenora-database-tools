#!/usr/bin/env python3
"""Harness riproducibile per la Fase 0 di plenora-database-tools.

Il programma non stampa DSN, token, SQL con valori o payload completi.
Le credenziali entrano esclusivamente da variabili d'ambiente.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import platform
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
DEFAULT_MANIFEST = (
    Path(__file__).resolve().parents[1]
    / "benchmarks"
    / "manifests"
    / "phase0-smoke.json"
)
INVENTORY_ROOTS = (
    "app/core/connections",
    "app/core/estrai",
    "app/core/query_execution",
    "app/core/carica",
    "app/core/features",
    "app/shared/connections",
)


class HarnessError(RuntimeError):
    """Errore controllato, già privo di segreti."""


@dataclass(frozen=True)
class CaseSpec:
    case_id: str
    provider: str
    runner: str
    mutates: bool
    required: bool


# I runner che questo harness sa eseguire. Il gate offline importa questo
# elenco invece di riscriverlo: due copie della stessa lista divergono, e
# quella del gate ammetteva ancora un runner che qui non esiste piu.
RUNNERS = ("postgres",)


class Manifest:
    def __init__(self, path: Path) -> None:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("schema_version") != SCHEMA_VERSION:
            raise HarnessError("schema_version manifest non supportata")
        cases: dict[str, CaseSpec] = {}
        for item in raw.get("cases", []):
            case_id = str(item["id"])
            if case_id in cases:
                raise HarnessError(f"case id duplicato: {case_id}")
            cases[case_id] = CaseSpec(
                case_id=case_id,
                provider=str(item["provider"]),
                runner=str(item["runner"]),
                mutates=bool(item.get("mutates", False)),
                required=bool(item.get("required", True)),
            )
        if not cases:
            raise HarnessError("manifest senza casi")
        self.path = path
        self.cases = cases

    def require(self, case_id: str, runner: str) -> CaseSpec:
        try:
            case = self.cases[case_id]
        except KeyError as exc:
            raise HarnessError(f"caso non registrato: {case_id}") from exc
        if case.runner != runner:
            raise HarnessError(
                f"runner errato per {case_id}: {runner}, atteso {case.runner}"
            )
        return case


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def stable_json_digest(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256_bytes(payload)


def environment_metadata() -> dict[str, Any]:
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
    }


def result_envelope(
    *, repeat: int = 1, warmup: int = 0
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "suite": "phase0-smoke",
        "generated_at": utc_now(),
        "environment": environment_metadata(),
        "run_config": {"repeat": repeat, "warmup": warmup},
    }


def current_rss_bytes() -> int | None:
    try:
        import psutil

        return int(psutil.Process().memory_info().rss)
    except Exception:
        return None


class Recorder:
    def __init__(
        self, manifest: Manifest, *, repeat: int = 1, warmup: int = 0
    ) -> None:
        if repeat < 1:
            raise HarnessError("repeat deve essere >= 1")
        if warmup < 0:
            raise HarnessError("warmup deve essere >= 0")
        self.manifest = manifest
        self.repeat = repeat
        self.warmup = warmup
        self.records: list[dict[str, Any]] = []

    def run(
        self,
        case_id: str,
        runner: str,
        operation: Callable[[], Mapping[str, Any]],
        *,
        repeat: int | None = None,
        warmup: int | None = None,
    ) -> Mapping[str, Any]:
        spec = self.manifest.require(case_id, runner)
        sample_count = self.repeat if repeat is None else repeat
        warmup_count = self.warmup if warmup is None else warmup
        if sample_count < 1 or warmup_count < 0:
            raise HarnessError("configurazione campioni non valida")

        for warmup_index in range(1, warmup_count + 1):
            try:
                operation()
            except Exception as exc:
                raise HarnessError(
                    f"warmup fallito: {case_id} #{warmup_index}"
                ) from exc

        last_summary: Mapping[str, Any] = {}
        for sample_index in range(1, sample_count + 1):
            last_summary = self._run_sample(
                spec, runner, operation, sample_index, sample_count
            )
        return last_summary

    def _run_sample(
        self,
        spec: CaseSpec,
        runner: str,
        operation: Callable[[], Mapping[str, Any]],
        sample_index: int,
        sample_count: int,
    ) -> Mapping[str, Any]:
        case_id = spec.case_id
        rss_before = current_rss_bytes()
        started = time.perf_counter_ns()
        timestamp = utc_now()
        try:
            summary = dict(operation())
        except Exception as exc:
            elapsed = time.perf_counter_ns() - started
            record = {
                "schema_version": SCHEMA_VERSION,
                "suite": "phase0-smoke",
                "case_id": case_id,
                "provider": spec.provider,
                "runner": runner,
                "mutates": spec.mutates,
                "sample_index": sample_index,
                "sample_count": sample_count,
                "status": "failed",
                "timestamp": timestamp,
                "metrics": {
                    "wall_ns": elapsed,
                    "rss_before_bytes": rss_before,
                    "rss_after_bytes": current_rss_bytes(),
                },
                "error": {
                    "category": type(exc).__name__,
                    "message": "case execution failed",
                },
            }
            self.records.append(record)
            raise HarnessError(f"caso fallito: {case_id}") from exc
        elapsed = time.perf_counter_ns() - started
        record = {
            "schema_version": SCHEMA_VERSION,
            "suite": "phase0-smoke",
            "case_id": case_id,
            "provider": spec.provider,
            "runner": runner,
            "mutates": spec.mutates,
            "sample_index": sample_index,
            "sample_count": sample_count,
            "status": "passed",
            "timestamp": timestamp,
            "metrics": {
                "wall_ns": elapsed,
                "rss_before_bytes": rss_before,
                "rss_after_bytes": current_rss_bytes(),
            },
            "summary": summary,
        }
        self.records.append(record)
        return summary


def write_jsonl_atomic(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def _source_symbols(path: Path) -> dict[str, Any]:
    payload = path.read_bytes()
    tree = ast.parse(payload, filename=str(path))
    functions: list[str] = []
    classes: dict[str, list[str]] = {}
    constants: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(node.name)
        elif isinstance(node, ast.ClassDef):
            methods = [
                item.name
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]
            classes[node.name] = methods
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets: Sequence[ast.expr]
            if isinstance(node, ast.Assign):
                targets = node.targets
            else:
                targets = (node.target,)
            for target in targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    constants.append(target.id)
    return {
        "sha256": sha256_bytes(payload),
        "bytes": len(payload),
        "functions": sorted(functions),
        "classes": {name: sorted(methods) for name, methods in sorted(classes.items())},
        "constants": sorted(constants),
    }


def _postgres_connect() -> Any:
    dsn = os.environ.get("PLENORA_PHASE0_PG_DSN")
    if not dsn:
        raise HarnessError("PLENORA_PHASE0_PG_DSN non configurata")
    try:
        import psycopg
    except ImportError as exc:
        raise HarnessError("psycopg non disponibile") from exc
    try:
        # Il caso streaming usa un server-side named cursor, che PostgreSQL
        # consente solo dentro una transazione. L'intera campagna è read-only
        # e la connessione viene chiusa nel finally.
        return psycopg.connect(dsn, connect_timeout=5, autocommit=False)
    except Exception as exc:
        raise HarnessError("connessione PostgreSQL fallita") from exc


def _pg_scalar(connection: Any, query: str) -> Any:
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
    if row is None:
        raise HarnessError("query PostgreSQL senza risultato")
    return row[0]


def run_postgres(recorder: Recorder) -> None:
    connection = _postgres_connect()
    try:
        recorder.run(
            "postgres.connection.version",
            "postgres",
            lambda: {
                "server_version": str(_pg_scalar(connection, "SHOW server_version")),
                "postgis_version": str(
                    _pg_scalar(connection, "SELECT postgis_lib_version()")
                ),
            },
        )

        def introspection() -> Mapping[str, Any]:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT table_name, column_name, data_type, udt_name,
                           is_nullable, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                    """
                )
                rows = cursor.fetchall()
            normalized = [tuple(str(value) for value in row) for row in rows]
            return {
                "columns": len(rows),
                "tables": len({row[0] for row in rows}),
                "digest": stable_json_digest(normalized),
            }

        recorder.run(
            "postgres.introspection.columns", "postgres", introspection
        )

        def fixture_preflight() -> Mapping[str, Any]:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT to_regclass('public.events_log'),
                           to_regclass('public.cities')
                    """
                )
                relations = cursor.fetchone()
                if relations is None or any(value is None for value in relations):
                    raise HarnessError("fixture PostgreSQL incompleta")
                cursor.execute("SELECT COUNT(*) FROM events_log")
                events = int(cursor.fetchone()[0])
                cursor.execute(
                    "SELECT COUNT(*) FROM cities WHERE geom IS NOT NULL"
                )
                geometries = int(cursor.fetchone()[0])
            if events != 10_000 or geometries != 18:
                raise HarnessError("conteggi fixture PostgreSQL inattesi")
            return {
                "events": events,
                "city_geometries": geometries,
                "ready": True,
            }

        recorder.run(
            "postgres.fixture.preflight", "postgres", fixture_preflight
        )

        def stream_events() -> Mapping[str, Any]:
            digest = hashlib.sha256()
            rows = 0
            fetches = 0
            with connection.cursor(name="plenora_phase0_events") as cursor:
                cursor.itersize = 1000
                cursor.execute(
                    """
                    SELECT id, customer_id, event_type, occurred_at, value
                    FROM events_log
                    ORDER BY id
                    """
                )
                while True:
                    chunk = cursor.fetchmany(1000)
                    if not chunk:
                        break
                    fetches += 1
                    rows += len(chunk)
                    for row in chunk:
                        digest.update(
                            json.dumps(
                                row,
                                default=str,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ).encode("utf-8")
                        )
                        digest.update(b"\n")
            return {
                "rows": rows,
                "fetches": fetches,
                "fetch_rows": 1000,
                "digest": digest.hexdigest(),
            }

        recorder.run(
            "postgres.read.events_stream", "postgres", stream_events
        )

        def spatial_read() -> Mapping[str, Any]:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, ST_SRID(geom), ST_AsEWKB(geom)
                    FROM cities
                    WHERE geom IS NOT NULL
                    ORDER BY id
                    """
                )
                rows = cursor.fetchall()
            payloads = [
                (int(row[0]), int(row[1]), bytes(row[2]).hex()) for row in rows
            ]
            return {
                "geometries": len(payloads),
                "srids": sorted({item[1] for item in payloads}),
                "ewkb_bytes": sum(len(bytes.fromhex(item[2])) for item in payloads),
                "digest": stable_json_digest(payloads),
            }

        recorder.run(
            "postgres.spatial.ewkb_read", "postgres", spatial_read
        )
    finally:
        connection.close()


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=(*RUNNERS, "all"),
    )
    parser.add_argument(
        "--manifest", type=Path, default=DEFAULT_MANIFEST
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="campioni misurati per caso (default: 1)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=0,
        help="esecuzioni non registrate prima dei campioni (default: 0)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    recorder: Recorder | None = None
    try:
        manifest = Manifest(args.manifest.resolve())
        recorder = Recorder(
            manifest, repeat=args.repeat, warmup=args.warmup
        )
        if args.command in ("postgres", "all"):
            run_postgres(recorder)
    except HarnessError as exc:
        print(f"phase0 harness: {exc}", file=sys.stderr)
        if recorder is not None and args.output and recorder.records:
            write_jsonl_atomic(
                args.output.resolve(),
                [
                    result_envelope(
                        repeat=args.repeat, warmup=args.warmup
                    ),
                    *recorder.records,
                ],
            )
        return 1

    output_records = [
        result_envelope(repeat=args.repeat, warmup=args.warmup),
        *recorder.records,
    ]
    if args.output:
        write_jsonl_atomic(args.output.resolve(), output_records)
    else:
        for record in output_records:
            print(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
