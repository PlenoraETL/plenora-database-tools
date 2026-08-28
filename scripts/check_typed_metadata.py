#!/usr/bin/env python3
"""Protegge la reflection tipizzata e la sua semantica fail-closed."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_API = Path("crates/plenora-database-engine/src/metadata/mod.rs")
WIRE_ADAPTER = Path("crates/plenora-database-engine/src/metadata/wire.rs")
ENGINE = Path("crates/plenora-database-engine/src/engine.rs")


def violations(root: Path) -> list[str]:
    """Restituisce tutte le regressioni architetturali osservate."""

    paths = [PUBLIC_API, WIRE_ADAPTER, ENGINE]
    missing = [path for path in paths if not (root / path).is_file()]
    if missing:
        return [f"file obbligatorio assente: {path}" for path in missing]

    public = (root / PUBLIC_API).read_text(encoding="utf-8")
    wire = (root / WIRE_ADAPTER).read_text(encoding="utf-8")
    engine = (root / ENGINE).read_text(encoding="utf-8")
    problems: list[str] = []

    for declaration in (
        "MetaData",
        "Table",
        "Column",
        "Index",
        "ForeignKey",
        "Constraint",
    ):
        matches = re.findall(rf"(?m)^pub struct {declaration}\b", public)
        if len(matches) != 1:
            problems.append(f"{declaration}: attesa una definizione pubblica tipizzata")

    if not {"NotMeasured", "Observed"} <= set(
        re.findall(r"\b(?:NotMeasured|Observed)\b", public)
    ):
        problems.append("Observation non distingue misurato da non misurato")

    forbidden = ("serde_json::Value", "HashMap<", "BTreeMap<")
    for token in forbidden:
        if token in public:
            problems.append(f"API metadata pubblica contiene il contenitore non tipizzato {token}")

    providers = {"Postgres", "Mysql", "Mariadb", "Sqlserver", "Db2"}
    mapped = set(re.findall(r"ProviderKind::([A-Za-z0-9_]+)\s*=>", wire))
    if not providers <= mapped:
        problems.append(
            "adapter metadata mancanti: " + ", ".join(sorted(providers - mapped))
        )

    if re.search(r"(?m)^pub (?:struct|enum) ", wire):
        problems.append("il wire adapter espone tipi JSON invece del catalogo comune")

    for marker in (
        "pub async fn reflect_table",
        "metadata_cache_ttl",
        "pub fn invalidate_metadata",
    ):
        if marker not in engine:
            problems.append(f"Engine privo del marker metadata: {marker}")
    if engine.count("metadata.clear()") < 2:
        problems.append("cache metadata non invalidata su dispose e rotazione secret")
    return problems


def main() -> int:
    problems = violations(ROOT)
    if problems:
        for problem in problems:
            print(f"ERRORE: {problem}", file=sys.stderr)
        return 1
    print("metadata: API tipizzata, 5 adapter e lifecycle cache verificati")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
