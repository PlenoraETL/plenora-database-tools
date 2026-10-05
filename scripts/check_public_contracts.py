"""Black-box del profilo pubblico contro il pin di plenora-contracts."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

try:
    from scripts.public_contract_semantics import load_semantics
except ModuleNotFoundError:  # esecuzione diretta da scripts/
    from public_contract_semantics import load_semantics

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "contracts" / "v2" / "public-operation-contracts.schema.json"
UPSTREAM = ROOT / "contracts" / "upstream"
# Le directory di vettori del pin che questo componente esegue per intero:
# un vettore nuovo del pin che qui non ha una copia e un vettore che nessun
# test esegue, e il gate lo rifiuta invece di ignorarlo.
EXECUTED_VECTOR_SETS = {"arrow-v1": "vectors/arrow-v1"}


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("documento JSON non object")
    return value


def run_cli(cli: Path, *arguments: str, success: bool) -> dict[str, Any]:
    completed = subprocess.run(
        [str(cli), *arguments], capture_output=True, check=False
    )
    if completed.stderr:
        raise RuntimeError("il CLI JSON ha scritto su stderr")
    if (completed.returncode == 0) != success:
        raise RuntimeError("exit code CLI incoerente con lo status atteso")
    if completed.stdout.count(b"\n") != 1:
        raise RuntimeError("il CLI deve emettere un documento e una newline")
    value = json.loads(completed.stdout)
    if not isinstance(value, dict):
        raise RuntimeError("envelope CLI non object")
    return value


def validate(
    instance: Any,
    schema: dict[str, Any],
    label: str,
    registry: Registry,
) -> None:
    errors = sorted(
        Draft202012Validator(schema, registry=registry).iter_errors(instance),
        key=lambda error: [str(item) for item in error.absolute_path],
    )
    if errors:
        raise RuntimeError(f"{label}: documento non conforme allo schema")


def check_upstream_copies(contracts: Path, registry: Registry) -> int:
    """Le copie in `contracts/upstream` sono quelle del pin, e complete.

    I test Rust le leggono dal repository, con il loro SHA-256: senza questo
    confronto una copia potrebbe venire da un'altra revisione o mancare del
    tutto, e il test passerebbe su un insieme diverso da quello adottato.
    """

    source = load(UPSTREAM / "source.json")
    pin = load(ROOT / "contracts" / "adoption-source.json")
    if source["revision"] != pin["contracts_source"]["revision"]:
        raise RuntimeError("copie upstream da una revisione diversa dal pin")
    copied = {}
    for name, entry in source["files"].items():
        local = (UPSTREAM / name).read_bytes()
        pinned = (contracts / entry["source"]).read_bytes()
        if local != pinned:
            raise RuntimeError(f"{name}: copia diversa dal checkout fissato")
        if hashlib.sha256(local).hexdigest() != entry["sha256"]:
            raise RuntimeError(f"{name}: SHA-256 dichiarato errato")
        copied[entry["source"]] = name
    for directory in EXECUTED_VECTOR_SETS.values():
        for path in sorted((contracts / directory).glob("*.json")):
            relative = path.relative_to(contracts).as_posix()
            if relative not in copied:
                raise RuntimeError(f"{relative}: vettore del pin senza copia eseguita")
    vector_schema = load(contracts / "schemas" / "arrow-metadata-vector-v1.schema.json")
    for relative, name in copied.items():
        if relative.startswith("vectors/arrow-v1/"):
            validate(load(UPSTREAM / name), vector_schema, name, registry)
    return len(copied)


def check(contracts: Path, cli: Path) -> dict[str, int]:
    semantics = load_semantics(contracts)

    cli_schema = load(contracts / "schemas" / "cli-envelope-v2.schema.json")
    capability_schema = load(contracts / "schemas" / "capabilities-v2.schema.json")
    error_schema = load(contracts / "schemas" / "error-v1.schema.json")
    catalog = load(contracts / "catalogs" / "database-tools-v1.json")
    shared_schemas = [load(path) for path in (contracts / "schemas").glob("*.json")]
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema))
        for schema in shared_schemas
        if "$id" in schema
    )

    version = run_cli(cli, "--version", "--format", "json", success=True)
    validate(version, cli_schema, "version envelope", registry)
    if version["result"]["component_version"] != version["component_version"]:
        raise RuntimeError("versione CLI divergente nell'envelope")

    capabilities = run_cli(cli, "capabilities", "--format", "json", success=True)
    validate(capabilities, cli_schema, "capability envelope", registry)
    validate(capabilities["result"], capability_schema, "capability document", registry)
    if semantics.capability_errors(capabilities["result"]):
        raise RuntimeError("capability document: invarianti semantiche non rispettate")

    invalid = run_cli(cli, "read", "--format", "json", success=False)
    validate(invalid, cli_schema, "error envelope", registry)
    validate(invalid["error"], error_schema, "typed error", registry)
    if invalid["error"]["category"] != "invalid_plan":
        raise RuntimeError("invocazione invalida senza categoria invalid_plan")

    advertised = {item["id"]: item for item in capabilities["result"]["operations"]}
    selected = {
        item["id"]: item
        for item in catalog["operations"]
        if "cli" in item["surfaces"]
    }
    if set(advertised) != set(selected):
        raise RuntimeError("catalogo CLI e capability pubblicate divergenti")
    for operation, expected in selected.items():
        actual = advertised[operation]
        for field in ("version", "side_effect", "controls"):
            if actual[field] != expected[field]:
                raise RuntimeError(f"{operation}: campo {field} divergente")
        # Il catalogo conserva anche gli interchange contract per la
        # composizione; `plenora-capabilities-v2` espone solo questi due campi.
        for field in ("input", "output"):
            descriptor = {
                "contract": expected[field]["contract"],
                "content_types": expected[field]["content_types"],
            }
            if actual[field] != descriptor:
                raise RuntimeError(f"{operation}: campo {field} divergente")
        if actual["surfaces"] != ["cli"]:
            raise RuntimeError(f"{operation}: superficie artifact non esatta")

    bundle = load(BUNDLE)
    schemas = {
        value["x-plenora-contract"]
        for value in bundle["$defs"].values()
        if isinstance(value, dict) and "x-plenora-contract" in value
    }
    referenced = {
        descriptor["contract"]
        for operation in catalog["operations"]
        for descriptor in (operation["input"], operation["output"])
    }
    referenced.update(
        operation["attributes"]["contract"]
        for operation in catalog["operations"]
        if "attributes" in operation
    )
    if schemas != referenced:
        raise RuntimeError("schemi component-owned incompleti o orfani")

    upstream_copies = check_upstream_copies(contracts, registry)

    return {
        "upstream_copies": upstream_copies,
        "cli_envelopes": 3,
        "operations": len(advertised),
        "component_schemas": len(schemas),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    arguments = parser.parse_args()
    print(json.dumps(check(arguments.contracts.resolve(), arguments.cli.resolve())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
