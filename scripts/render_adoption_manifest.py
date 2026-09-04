"""Produce il manifest v4 dagli artefatti esatti di una release.

Il file committato contiene solo il pin e la selezione dei contratti. Versione
e digest arrivano dagli artefatti gia costruiti: inventarli prima della build
renderebbe il manifest formalmente valido e materialmente falso.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "contracts" / "adoption-source.json"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return f"sha256:{value.hexdigest()}"


def artifact(value: str, version: str, verification: list[str]) -> dict[str, Any]:
    parts = value.split("|", 3)
    if len(parts) < 3:
        raise ValueError("artifact deve essere NAME|SURFACE|PATH[|MODES]")
    name, surface, raw_path = parts[:3]
    path = Path(raw_path).resolve()
    if surface not in {"rust", "cli", "python_sdk", "runtime"}:
        raise ValueError("surface artifact non valida")
    if not path.is_file():
        raise ValueError("artifact non trovato")
    result: dict[str, Any] = {
        "name": name,
        "surface": surface,
        "version": version,
        "digest": digest(path),
        "verification": verification,
    }
    if surface == "python_sdk":
        if len(parts) != 4:
            raise ValueError("il wheel richiede api_modes sync,async")
        modes = parts[3].split(",")
        if not modes or any(mode not in {"sync", "async"} for mode in modes):
            raise ValueError("api_modes non valide")
        result["api_modes"] = modes
    elif len(parts) == 4:
        raise ValueError("api_modes sono ammesse soltanto per python_sdk")
    return result


def manifest(
    version: str,
    artifacts: list[str],
    verification: list[str],
) -> dict[str, Any]:
    source = json.loads(SOURCE.read_text(encoding="utf-8"))
    return {
        "schema_version": 4,
        "component": source["component"],
        "contracts_source": source["contracts_source"],
        "profile": source["profile"],
        "artifacts": [
            artifact(value, version, verification) for value in artifacts
        ],
        "contracts": [
            {"id": contract, "status": "conforming", "verification": verification}
            for contract in source["contracts"]
        ]
        + [
            {"id": contract, "status": "not_applicable"}
            for contract in source["not_applicable"]
        ],
        "deviations": [],
    }


def validate_manifest(document: dict[str, Any], schema_path: Path) -> None:
    """Rifiuta un manifest che non rispetta lo schema v4 fissato."""

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    errors = sorted(
        Draft202012Validator(schema).iter_errors(document),
        key=lambda error: [str(item) for item in error.absolute_path],
    )
    if errors:
        location = "/".join(str(item) for item in errors[0].absolute_path)
        raise ValueError(f"manifest non valido a /{location}: {errors[0].message}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--artifact", action="append", required=True)
    parser.add_argument("--verification", action="append", required=True)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    document = manifest(
        arguments.version,
        arguments.artifact,
        arguments.verification,
    )
    validate_manifest(document, arguments.schema)
    arguments.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
