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
import re
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
# Una sola major di contratto vive nel worktree, ed e quella che il codice
# emette. Le versioni precedenti stanno in Git, che e dove sta la storia: un
# archivio dentro l'albero di lavoro e una seconda copia che si legge come se
# fosse ancora valida.
ACTIVE_MAJOR = "v2"
ACTIVE_CONTRACT_ROOT = REPO_ROOT / "contracts" / ACTIVE_MAJOR
CONTRACT_ROOTS = tuple(
    sorted(
        (path for path in (REPO_ROOT / "contracts").iterdir() if path.is_dir()),
        key=lambda path: path.name,
    )
)
GOLDEN_ROOT = REPO_ROOT / "golden"
BENCHMARK_MANIFEST = (
    REPO_ROOT / "benchmarks" / "manifests" / "phase0-smoke.json"
)
SPATIAL_CATALOG = REPO_ROOT / "catalog" / "spatial-functions.v1.json"
CAPABILITIES_SCHEMA = ACTIVE_CONTRACT_ROOT / "capabilities.schema.json"
# Il dominio di questo repository sono i database. Un termine che appartiene a
# un altro dominio e una superficie che rientra dalla finestra: il controllo e
# strutturale — cerca la stringa nei file — invece di fidarsi di una frase in
# un documento che dice che non c'e piu.
FOREIGN_DOMAIN_TERMS = (
    "arcgis",
    "feature_service",
    "apply_edits",
    "global_id",
    "object_id_windows",
    "layer_outcomes",
)
# Dove si cerca. Non solo i contratti, per due ragioni imparate una alla volta:
# un artefatto di misura committato qui dentro porta con se cio che ha
# misurato — un inventario di un'altra base di codice ha tenuto ArcGIS nel
# repository per una campagna intera — e il codice e il posto dove un nome
# sopravvive piu a lungo, perche nessuno rilegge i commenti. `profile.rs`
# elencava `object_id_windows` fra le capability chiuse tre commit dopo che
# quella capability non esisteva piu.
DOMAIN_SCOPES = (
    "contracts",
    "golden",
    "benchmarks",
    "catalog",
    "docs",
    "crates",
    "scripts",
    "tests",
)
DOMAIN_SUFFIXES = frozenset(
    {".json", ".jsonl", ".md", ".yml", ".yaml", ".rs", ".py", ".pyi", ".toml"}
)
# Il gate nomina i termini per poterli cercare, e i test provano che la ricerca
# funziona: sono gli unici due posti dove comparire e legittimo.
DOMAIN_EXEMPT = ("scripts/phase0_validate.py", "tests/phase0/")


def harness_runners() -> tuple[str, ...]:
    """I runner ammessi sono quelli che l'harness sa eseguire.

    Importato invece che riscritto: l'elenco che stava qui ammetteva ancora un
    runner che l'harness non ha piu, e nessuno se ne sarebbe accorto finche un
    caso non fosse rimasto nel manifest senza nessuno che lo eseguisse.
    """
    try:
        from scripts.phase0_harness import RUNNERS
    except ModuleNotFoundError:  # esecuzione diretta: python scripts\...
        from phase0_harness import RUNNERS
    return RUNNERS


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


def validate_active_domain() -> int:
    """Il repository parla di database, e ha una sola major di contratto.

    Due controlli, entrambi sul testo dei file e non su cio che un documento
    dichiara di aver rimosso:

    1. nessun termine di un dominio estraneo compare in contratti, suite
       golden, benchmark, cataloghi, documenti, codice o script;
    2. nessun riferimento punta a una major diversa da quella attiva.

    Il secondo e il piu importante: un contratto puo essere ripulito e
    continuare a referenziare, per una definizione comune, il file da cui
    quella superficie e stata tolta. In quel caso la superficie e ancora li,
    raggiungibile, e chi valida non se ne accorge.

    Il primo copre i benchmark e il codice, non solo i contratti, perche
    l'ha imparato due volte: un raw di inventario di un'altra base di codice ha
    tenuto ArcGIS nel repository per una campagna intera, e un commento in
    `profile.rs` ha continuato a elencare una capability tre commit dopo che
    era stata tolta dal contratto. La guardia di allora non guardava ne l'uno
    ne l'altro.
    """
    if not any(ACTIVE_CONTRACT_ROOT.rglob("*.json")):
        raise ValidationError(f"major attiva assente: {ACTIVE_MAJOR}")
    if not any((GOLDEN_ROOT / ACTIVE_MAJOR).glob("*.json")):
        # Senza questa riga il gate resterebbe verde con la suite attiva
        # cancellata: gli schemi ci sarebbero ancora, e `validate_golden` non
        # avrebbe niente da confrontare.
        raise ValidationError(f"suite golden attiva assente: {ACTIVE_MAJOR}")
    others = tuple(
        root.name for root in CONTRACT_ROOTS if root.name != ACTIVE_MAJOR
    )
    if others:
        raise ValidationError(
            f"major oltre a quella attiva nel worktree: {', '.join(others)}"
        )
    namespace = "https://plenora.local/database-tools/"
    inspected = 0
    for scope in DOMAIN_SCOPES:
        for path in sorted((REPO_ROOT / scope).rglob("*")):
            if not path.is_file() or path.suffix not in DOMAIN_SUFFIXES:
                continue
            where = path.relative_to(REPO_ROOT).as_posix()
            if where.startswith(DOMAIN_EXEMPT):
                continue
            if "target" in path.relative_to(REPO_ROOT).parts:
                continue
            if "__pycache__" in path.relative_to(REPO_ROOT).parts:
                continue
            text = path.read_text(encoding="utf-8")
            lowered = text.lower()
            for term in FOREIGN_DOMAIN_TERMS:
                if term in lowered:
                    raise ValidationError(
                        f"dominio estraneo: '{term}' in {where}"
                    )
            start = 0
            while True:
                found = text.find(namespace, start)
                if found < 0:
                    break
                major = text[found + len(namespace) :].split("/", 1)[0]
                if major != ACTIVE_MAJOR:
                    raise ValidationError(
                        f"riferimento a una major non attiva: {major} in {where}"
                    )
                start = found + len(namespace)
            inspected += 1
    return inspected


def validate_golden(
    schemas: Mapping[Path, Mapping[str, Any]],
    registry: Registry,
) -> int:
    # Una suite per major, validata contro il manifest della **propria**
    # major. Le categorie richieste non sono piu un elenco scritto qui: sono
    # quelle che lo schema dichiara. Cosi togliere una categoria dal dominio la
    # toglie da entrambi i lati in un colpo solo, e nessuna delle due liste puo
    # restare indietro rispetto all'altra.
    total = 0
    suites = sorted(GOLDEN_ROOT.glob("*/cases.json"))
    if not suites:
        raise ValidationError("nessuna suite golden trovata")
    for suite_path in suites:
        major = suite_path.parent.name
        schema_path = (
            REPO_ROOT / "contracts" / major / "golden-manifest.schema.json"
        ).resolve()
        try:
            schema = schemas[schema_path]
        except KeyError as exc:
            raise ValidationError(
                f"suite golden senza manifest della propria major: {major}"
            ) from exc
        golden = load_json(suite_path)
        validate_instance(golden, schema, registry, f"golden {major}")
        cases = golden["cases"]
        ids = [case["id"] for case in cases]
        if len(ids) != len(set(ids)):
            raise ValidationError(f"golden case id duplicato: {major}")
        declared = set(
            schema["$defs"]["case"]["properties"]["category"]["enum"]
        )
        missing = sorted(declared - {case["category"] for case in cases})
        if missing:
            raise ValidationError(
                f"categorie golden mancanti in {major}: {', '.join(missing)}"
            )
        total += len(cases)
    return total


def validate_benchmark_manifest() -> int:
    manifest = load_json(BENCHMARK_MANIFEST)
    if manifest.get("schema_version") != 1:
        raise ValidationError("benchmark manifest schema_version non valida")
    cases = manifest.get("cases", [])
    ids = [case.get("id") for case in cases]
    if not cases or len(ids) != len(set(ids)):
        raise ValidationError("benchmark manifest vuoto o con id duplicati")
    runners = set(harness_runners())
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


def markdown_documents() -> list[Path]:
    """Tutti i Markdown versionati, senza artefatti di build."""
    skip = {"target", "node_modules", ".git", "__pycache__"}
    found = [
        path
        for path in sorted(REPO_ROOT.rglob("*.md"))
        if not skip & set(path.relative_to(REPO_ROOT).parts)
    ]
    if not found:
        raise ValidationError("nessun documento trovato")
    return found


def validate_documents() -> int:
    """Ogni documento e leggibile e ha i fence bilanciati.

    Qui c'era un elenco di undici percorsi scritti a mano, e il controllo era
    che esistessero. Presidiava documenti scelti una volta sola: due di quei
    percorsi non esistevano piu, e degli altri — la maggioranza — non diceva
    niente. Un elenco che va aggiornato a mano non e una guardia, e un secondo
    posto dove la verita puo restare indietro.
    """
    for path in markdown_documents():
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ValidationError(f"documento illeggibile: {path}") from exc
        if text.count("```") % 2:
            raise ValidationError(f"code fence non bilanciato: {path}")
    return len(markdown_documents())


def validate_document_links() -> int:
    """Un link interno deve puntare a qualcosa che esiste.

    E il modo in cui una riorganizzazione si accorge di aver rotto qualcosa:
    spostare un file lascia dietro di se i riferimenti, e chi li segue trova
    un 404 molto dopo che chi ha spostato ha finito.

    Nessuna esenzione: non ci sono documenti congelati nel worktree.
    """
    pattern = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
    checked = 0
    for path in markdown_documents():
        relative = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        for target in pattern.findall(text):
            if target.startswith(("http://", "https://", "#", "mailto:")):
                continue
            destination = target.split("#", 1)[0].strip()
            if not destination:
                continue
            checked += 1
            if not (path.parent / destination).resolve().exists():
                raise ValidationError(
                    f"link morto in {relative}: {target}"
                )
    return checked


def validate_generated_documents() -> int:
    """I documenti generati sono allineati alla loro sorgente.

    Un documento generato che nessuno rigenera e peggio di uno scritto a mano:
    ha l'aria di essere sempre vero.
    """
    try:
        from scripts.render_state import TARGET, render
    except ModuleNotFoundError:  # esecuzione diretta
        from render_state import TARGET, render

    generated = ((TARGET, render),)
    for path, renderer in generated:
        if not path.is_file():
            raise ValidationError(
                f"documento generato assente: {path.relative_to(REPO_ROOT)}"
            )
        if path.read_text(encoding="utf-8") != renderer():
            raise ValidationError(
                f"documento generato disallineato: "
                f"{path.relative_to(REPO_ROOT).as_posix()}; rigeneralo con "
                f"python scripts/render_state.py"
            )
    return len(generated)


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
            "id": "active-contract-domain",
            "status": "passed",
            "count": validate_active_domain(),
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
            "id": "markdown-documents",
            "status": "passed",
            "count": validate_documents(),
        },
        {
            "id": "document-links",
            "status": "passed",
            "count": validate_document_links(),
        },
        {
            "id": "generated-documents",
            "status": "passed",
            "count": validate_generated_documents(),
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
