"""Valida i risultati del binding runtime con gli schemi del pin.

`crates/plenora-database-engine/tests/runtime_vectors.rs`, con
`PLENORA_RUNTIME_EVIDENCE=<directory>`, scrive cio che il binding ha prodotto
eseguendo i vettori `database-*`: il documento di discovery e un risultato per
operazione, nella forma di un vettore `plenora-runtime-vector-v1`. Il test Rust
prova la semantica; qui si prova la forma, con gli schemi che il test Rust non
puo caricare senza una dipendenza in piu:

- ogni risultato contro `runtime-vector-v1`;
- ogni errore contro `error-v1` (assi comuni, ERR-006);
- ogni risultato JSON contro lo schema del componente nominato da
  `plenora.output.contract`;
- il documento di discovery contro `capabilities-v2` e le invarianti
  semantiche del pin, con le operazioni che il catalogo lega al runtime.

Un'evidenza attesa che manca e un errore: una directory vuota non prova niente.
"""

from __future__ import annotations

import argparse
import json
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
ARROW_STREAM = "application/vnd.apache.arrow.stream"
ERROR_CONTENT_TYPE = "application/vnd.plenora.error+json"

# Le evidenze che il test Rust scrive: una per operazione legata al runtime,
# piu la discovery. L'elenco e chiuso nei due versi.
EXPECTED = {
    "capabilities.json",
    "database-read-success.json",
    "database-query-success.json",
    "database-write-error.json",
    "database.test_connection.json",
    "database.list_catalogs.json",
    "database.list_schemas.json",
    "database.list_objects.json",
    "database.describe_object.json",
    "database.write.json",
}


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def errors(instance: Any, schema: dict[str, Any], registry: Registry) -> list[str]:
    return [
        error.message
        for error in Draft202012Validator(schema, registry=registry).iter_errors(instance)
    ]


def require(instance: Any, schema: dict[str, Any], registry: Registry, label: str) -> None:
    if errors(instance, schema, registry):
        raise RuntimeError(f"{label}: documento non conforme allo schema")


def check(contracts: Path, evidence: Path) -> dict[str, int]:
    semantics = load_semantics(contracts)
    shared = [load(path) for path in (contracts / "schemas").glob("*.json")]
    bundle = load(BUNDLE)
    # Il bundle rimanda agli altri schemi della major (esiti, tipi comuni).
    component = [load(path) for path in sorted(BUNDLE.parent.glob("*.schema.json"))]
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema))
        for schema in [*shared, *component]
        if "$id" in schema
    )
    by_contract = {
        definition["x-plenora-contract"]: definition
        for definition in bundle["$defs"].values()
        if isinstance(definition, dict) and "x-plenora-contract" in definition
    }
    vector_schema = load(contracts / "schemas" / "runtime-vector-v1.schema.json")
    error_schema = load(contracts / "schemas" / "error-v1.schema.json")
    capability_schema = load(contracts / "schemas" / "capabilities-v2.schema.json")
    catalog = load(contracts / "catalogs" / "database-tools-v1.json")

    found = {path.name for path in evidence.glob("*.json")}
    if found != EXPECTED:
        raise RuntimeError("evidenze runtime mancanti o inattese")

    capabilities = load(evidence / "capabilities.json")
    require(capabilities, capability_schema, registry, "discovery runtime")
    if semantics.capability_errors(capabilities):
        raise RuntimeError("discovery runtime: invarianti semantiche non rispettate")
    bound = [item["id"] for item in catalog["operations"] if "runtime" in item["surfaces"]]
    if [item["id"] for item in capabilities["operations"]] != bound:
        raise RuntimeError("discovery runtime diversa dal catalogo del pin")

    checked = 0
    for name in sorted(EXPECTED - {"capabilities.json"}):
        document = load(evidence / name)
        require(document, vector_schema, registry, name)
        metadata = document["metadata"]
        contract = metadata["plenora.output.contract"]
        if document["kind"] == "error":
            if document["content_type"] != ERROR_CONTENT_TYPE or contract != "plenora-error-v1":
                raise RuntimeError(f"{name}: envelope d'errore senza contratto d'errore")
            require(document["payload"], error_schema, registry, name)
        elif document["content_type"] == ARROW_STREAM:
            # I byte Arrow vanno al sink dell'host: il test Rust li rilegge.
            if contract != "plenora-database-read-result-v1":
                raise RuntimeError(f"{name}: stream Arrow con un altro contratto")
        else:
            if contract not in by_contract:
                raise RuntimeError(f"{name}: contratto d'uscita sconosciuto")
            require(document["payload"], by_contract[contract], registry, name)
        checked += 1
    return {"runtime_results": checked, "runtime_operations": len(bound)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    arguments = parser.parse_args()
    print(json.dumps(check(arguments.contracts.resolve(), arguments.evidence.resolve())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
