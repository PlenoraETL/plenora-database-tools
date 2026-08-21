#!/usr/bin/env python3
"""Gate offline della Fase 0.

Non apre connessioni e non legge variabili contenenti credenziali. Valida
contratti, esempi, golden manifest, manifest benchmark, documenti e sorgenti
Python del testkit.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import platform
import tempfile
from importlib.metadata import version
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource
except ImportError as exc:  # pragma: no cover - dipendenza del tooling
    raise SystemExit(
        "phase0 validate: richiede il pacchetto Python 'jsonschema'"
    ) from exc


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = REPO_ROOT / "contracts" / "v1"
# Le major di contratto presenti. La `v1` resta immutabile; la `v2` contiene
# solo il messaggio che ha cambiato major — le capability — e referenzia per
# `$id` le definizioni comuni della v1 invece di duplicarle. Il registry le
# tiene tutte, altrimenti un `$ref` fra versioni non risolverebbe.
CONTRACT_ROOTS = tuple(
    sorted(
        (path for path in (REPO_ROOT / "contracts").iterdir() if path.is_dir()),
        key=lambda path: path.name,
    )
)
GOLDEN_PATH = REPO_ROOT / "golden" / "v1" / "cases.json"
BENCHMARK_MANIFEST = (
    REPO_ROOT / "benchmarks" / "manifests" / "phase0-smoke.json"
)
SPATIAL_CATALOG = REPO_ROOT / "catalog" / "spatial-functions.v1.json"
CAPABILITIES_SCHEMA = CONTRACT_ROOT / "capabilities.schema.json"


class ValidationError(RuntimeError):
    pass


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"JSON non valido: {path}") from exc


def discover_schemas(root: Path) -> dict[Path, Mapping[str, Any]]:
    schemas: dict[Path, Mapping[str, Any]] = {}
    ids: set[str] = set()
    # Ricorsivo: gli schemi vivono una cartella per major, e cercarli solo al
    # primo livello ne trovava zero appena la radice e diventata `contracts/`.
    for path in sorted(root.rglob("*.schema.json")):
        raw = load_json(path)
        if not isinstance(raw, dict):
            raise ValidationError(f"schema non object: {path}")
        Draft202012Validator.check_schema(raw)
        schema_id = raw.get("$id")
        if not isinstance(schema_id, str) or not schema_id:
            raise ValidationError(f"$id assente: {path}")
        if schema_id in ids:
            raise ValidationError(f"$id duplicato: {schema_id}")
        ids.add(schema_id)
        schemas[path.resolve()] = raw
    if not schemas:
        raise ValidationError("nessuno schema trovato")
    return schemas


def build_registry(
    schemas: Iterable[Mapping[str, Any]],
) -> Registry:
    registry = Registry()
    for schema in schemas:
        registry = registry.with_resource(
            str(schema["$id"]), Resource.from_contents(schema)
        )
    return registry


def validate_instance(
    instance: Any,
    schema: Mapping[str, Any],
    registry: Registry,
    label: str,
) -> None:
    validator = Draft202012Validator(schema, registry=registry)
    errors = sorted(
        validator.iter_errors(instance),
        key=lambda error: [str(item) for item in error.absolute_path],
    )
    if errors:
        error = errors[0]
        location = "/".join(str(item) for item in error.absolute_path)
        suffix = f" a /{location}" if location else ""
        raise ValidationError(f"{label}{suffix}: {error.message}")


def validate_examples(
    schemas: Mapping[Path, Mapping[str, Any]],
    registry: Registry,
) -> int:
    # Ogni major ha il proprio indice, e i suoi esempi non possono uscire dalla
    # propria cartella: un esempio della v2 validato contro lo schema della v1
    # direbbe che le due versioni sono intercambiabili, che e cio che una nuova
    # major nega.
    validated = 0
    seen: set[Path] = set()
    for root in CONTRACT_ROOTS:
        index_path = root / "examples" / "index.json"
        if not index_path.is_file():
            # Saltarlo era un falso verde: una major senza indice non veniva
            # validata, e il gate passava avendo controllato niente. Se una
            # cartella di contratti esiste, i suoi esempi si validano.
            raise ValidationError(f"major senza indice degli esempi: {root.name}")
        index = load_json(index_path)
        entries = index.get("examples", [])
        for entry in entries:
            example_path = (index_path.parent / entry["file"]).resolve()
            schema_path = (index_path.parent / entry["schema"]).resolve()
            if not example_path.is_relative_to(root.resolve()):
                raise ValidationError(f"example path fuori da {root.name}")
            if not schema_path.is_relative_to(root.resolve()):
                raise ValidationError(f"schema path fuori da {root.name}")
            if example_path in seen:
                raise ValidationError(f"example duplicato: {entry['file']}")
            seen.add(example_path)
            try:
                schema = schemas[schema_path]
            except KeyError as exc:
                raise ValidationError(
                    f"schema non registrato: {entry['schema']}"
                ) from exc
            validate_instance(
                load_json(example_path),
                schema,
                registry,
                f"example {root.name}/{entry['file']}",
            )
        if not entries:
            raise ValidationError(f"examples index vuoto: {root.name}")
        validated += len(entries)
    return validated


def validate_golden(
    schemas: Mapping[Path, Mapping[str, Any]],
    registry: Registry,
) -> int:
    schema_path = (CONTRACT_ROOT / "golden-manifest.schema.json").resolve()
    golden = load_json(GOLDEN_PATH)
    validate_instance(golden, schemas[schema_path], registry, "golden")
    cases = golden["cases"]
    ids = [case["id"] for case in cases]
    if len(ids) != len(set(ids)):
        raise ValidationError("golden case id duplicato")
    required_categories = {
        "scalar",
        "temporal",
        "binary",
        "schema",
        "geometry",
        "write",
        "outcome",
        "arcgis",
        "security",
    }
    actual_categories = {case["category"] for case in cases}
    missing = sorted(required_categories - actual_categories)
    if missing:
        raise ValidationError(
            f"categorie golden mancanti: {', '.join(missing)}"
        )
    return len(cases)


def validate_benchmark_manifest() -> int:
    manifest = load_json(BENCHMARK_MANIFEST)
    if manifest.get("schema_version") != 1:
        raise ValidationError("benchmark manifest schema_version non valida")
    cases = manifest.get("cases", [])
    ids = [case.get("id") for case in cases]
    if not cases or len(ids) != len(set(ids)):
        raise ValidationError("benchmark manifest vuoto o con id duplicati")
    runners = {"inventory", "postgres", "arcgis"}
    invalid = sorted(
        {
            str(case.get("runner"))
            for case in cases
            if case.get("runner") not in runners
        }
    )
    if invalid:
        raise ValidationError(
            f"runner benchmark non validi: {', '.join(invalid)}"
        )
    return len(cases)


def validate_spatial_catalog() -> int:
    catalog = load_json(SPATIAL_CATALOG)
    if catalog.get("schema_version") != 1:
        raise ValidationError("catalogo spatial con schema_version non valida")
    functions = catalog.get("functions", [])
    ids = [function.get("id") for function in functions]
    if not functions or len(ids) != len(set(ids)):
        raise ValidationError("catalogo spatial vuoto o con id duplicati")
    for function in functions:
        required = {
            "id", "category", "arguments", "returns", "portable", "postgres"
        }
        if set(function) != required:
            raise ValidationError(
                f"campi catalogo spatial non validi: {function.get('id')}"
            )
        if not function["arguments"] or not function["postgres"].startswith("ST_"):
            raise ValidationError(
                f"firma catalogo spatial non valida: {function['id']}"
            )
    capability_schema = load_json(CAPABILITIES_SCHEMA)
    try:
        capability_ids = capability_schema["properties"]["spatial"][
            "properties"
        ]["functions"]["items"]["enum"]
    except (KeyError, TypeError) as exc:
        raise ValidationError(
            "schema capability senza catalogo delle funzioni spatial"
        ) from exc
    if set(capability_ids) != set(ids) or len(capability_ids) != len(ids):
        raise ValidationError(
            "schema capability e catalogo spatial non sono in lockstep"
        )
    return len(functions)


def validate_python_sources() -> int:
    paths = sorted((REPO_ROOT / "scripts").glob("*.py"))
    paths += sorted((REPO_ROOT / "tests").rglob("*.py"))
    for path in paths:
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError) as exc:
            raise ValidationError(f"Python non valido: {path}") from exc
    return len(paths)


def validate_documents() -> int:
    required = [
        REPO_ROOT / "Architetture.md",
        REPO_ROOT / "Prestazioni.md",
        REPO_ROOT / "docs" / "history" / "phase-0" / "README.md",
        REPO_ROOT / "docs" / "history" / "phase-0" / "pre-database-gate.md",
        REPO_ROOT / "docs" / "history" / "phase-0" / "open-decisions.md",
        REPO_ROOT / "docs" / "history" / "phase-1" / "README.md",
        REPO_ROOT / "docs" / "postgres" / "README.md",
        REPO_ROOT / "docs" / "postgres" / "HARDENING.md",
        REPO_ROOT / "docs" / "postgres" / "SAFETY-CASE.md",
        REPO_ROOT / "docs" / "postgres" / "COMPATIBILITY.md",
        REPO_ROOT / "docs" / "postgres" / "PERFORMANCE.md",
    ]
    required += sorted((REPO_ROOT / "docs" / "adr").glob("*.md"))
    for path in required:
        if not path.is_file():
            raise ValidationError(f"documento assente: {path}")
        text = path.read_text(encoding="utf-8")
        if text.count("```") % 2:
            raise ValidationError(f"code fence non bilanciato: {path}")
    return len(required)


def run_gate() -> dict[str, Any]:
    schemas = discover_schemas(REPO_ROOT / "contracts")
    registry = build_registry(schemas.values())
    checks = [
        {"id": "json-schemas", "status": "passed", "count": len(schemas)},
        {
            "id": "contract-examples",
            "status": "passed",
            "count": validate_examples(schemas, registry),
        },
        {
            "id": "golden-cases",
            "status": "passed",
            "count": validate_golden(schemas, registry),
        },
        {
            "id": "benchmark-manifest",
            "status": "passed",
            "count": validate_benchmark_manifest(),
        },
        {
            "id": "spatial-function-catalog",
            "status": "passed",
            "count": validate_spatial_catalog(),
        },
        {
            "id": "python-syntax",
            "status": "passed",
            "count": validate_python_sources(),
        },
        {
            "id": "required-documents",
            "status": "passed",
            "count": validate_documents(),
        },
    ]
    return {
        "schema_version": 1,
        "gate": "phase0-pre-database",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool_versions": {
            "python": platform.python_version(),
            "jsonschema": version("jsonschema"),
            "referencing": version("referencing"),
        },
        "database_connections_opened": 0,
        "checks": checks,
    }


def write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
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


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    import sys

    args = parse_args(argv or sys.argv[1:])
    try:
        report = run_gate()
        if args.output:
            write_json_atomic(args.output.resolve(), report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    except ValidationError as exc:
        print(f"phase0 validate: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
