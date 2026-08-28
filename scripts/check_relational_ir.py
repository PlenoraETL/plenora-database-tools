#!/usr/bin/env python3
"""Impedisce che il modello query torni a dividersi fra core e renderer SQL."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CANONICAL = Path("crates/plenora-database-core/src/relational.rs")
COMPATIBILITY = Path("crates/plenora-database-core/src/query.rs")
SQL_RENDERER = Path("crates/plenora-database-sql/src/lib.rs")


def rust_product_sources(root: Path) -> list[Path]:
    """Restituisce i sorgenti Rust di prodotto, esclusi target generati."""

    return sorted((root / "crates").glob("*/src/**/*.rs"))


def definitions(root: Path, declaration: str) -> list[Path]:
    """Trova le definizioni pubbliche esatte di un tipo canonico."""

    pattern = re.compile(rf"(?m)^pub\s+(?:enum|struct)\s+{re.escape(declaration)}\b")
    return [
        path.relative_to(root)
        for path in rust_product_sources(root)
        if pattern.search(path.read_text(encoding="utf-8"))
    ]


def function_body(source: str, name: str) -> str | None:
    """Estrae il corpo di una funzione Rust contando le parentesi graffe."""

    match = re.search(rf"\bfn\s+{re.escape(name)}\b[^{{]*{{", source)
    if match is None:
        return None
    start = match.end() - 1
    depth = 0
    for offset, character in enumerate(source[start:], start=start):
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return source[start + 1 : offset]
    return None


def violations(root: Path) -> list[str]:
    """Restituisce tutte le violazioni, senza fermarsi alla prima."""

    problems: list[str] = []
    canonical = root / CANONICAL
    compatibility = root / COMPATIBILITY
    renderer = root / SQL_RENDERER
    for path in (canonical, compatibility, renderer):
        if not path.is_file():
            problems.append(f"file obbligatorio assente: {path.relative_to(root)}")
    if problems:
        return problems

    for declaration in ("QueryExpression", "QueryOperation"):
        found = definitions(root, declaration)
        if found != [CANONICAL]:
            rendered = ", ".join(map(str, found)) or "nessuna"
            problems.append(
                f"{declaration}: attesa una sola definizione in {CANONICAL}; trovate {rendered}"
            )

    facade = compatibility.read_text(encoding="utf-8")
    if "pub use crate::relational::*;" not in facade:
        problems.append("core::query non riesporta integralmente core::relational")
    if re.search(r"(?m)^pub\s+(?:enum|struct)\s+", facade):
        problems.append("core::query contiene tipi propri invece di essere una facciata")

    sql = renderer.read_text(encoding="utf-8")
    if "plenora_database_core::relational" not in sql:
        problems.append("database-sql non importa l'IR relazionale canonico")
    select_body = function_body(sql, "render_select")
    if select_body is None:
        problems.append("database-sql non espone render_select")
    elif not {
        "simple_select_to_relational",
        "render_query",
    } <= set(re.findall(r"\b[a-z_][a-z0-9_]*\b", select_body)):
        problems.append("render_select non abbassa Select verso l'IR canonico")
    filter_body = function_body(sql, "render_filter")
    if filter_body is None:
        problems.append("database-sql non espone render_filter")
    elif not {
        "simple_expression_to_relational",
        "render_query_expression",
    } <= set(re.findall(r"\b[a-z_][a-z0-9_]*\b", filter_body)):
        problems.append("render_filter non abbassa Expression verso l'IR canonico")
    return problems


def main() -> int:
    problems = violations(ROOT)
    if problems:
        for problem in problems:
            print(f"ERRORE: {problem}", file=sys.stderr)
        return 1
    sources = rust_product_sources(ROOT)
    source_lines = sum(
        len(path.read_text(encoding="utf-8").splitlines()) for path in sources
    )
    print(
        "IR relazionale: 1 definizione canonica, 2 adapter verificati, "
        f"{len(sources)} sorgenti/{source_lines} righe osservate"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
