"""Helper interni per serializzare valori Python nell'AST portable JSON.

L'AST è consumato lato Rust via serde. I nomi dei tag e i formati devono
combaciare con `plenora_database_core::provider::ParameterValue` e con
`plenora_database_core::portable::{Expression, Predicate, TableRef, ...}`.

Consumer: `plenora_database.query.*` builders.
"""
from __future__ import annotations

from typing import Any


def literal_value(value: Any) -> dict:
    """Converte un valore Python nella forma `ParameterValue` (tagged JSON).

    Il formato è `{"type": "<tag>", "value": <payload>}` in snake_case.
    Le nested collections (dict/list) sono serializzate come JSON.
    """
    # Ordine: bool prima di int (bool eredita da int in Python).
    if value is None:
        return {"type": "null", "value": {"type_name": "unknown"}}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        # Downgrade a i32 quando entra: il binding Postgres è strict sui
        # tipi (INT vs BIGINT) e sbagliare la larghezza fa fallire il bind.
        if -(2**31) <= value < 2**31:
            return {"type": "i32", "value": value}
        return {"type": "i64", "value": value}
    if isinstance(value, float):
        return {"type": "f64", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, (bytes, bytearray)):
        # Serde JSON serializza Vec<u8> come array di interi.
        return {"type": "bytes", "value": list(value)}
    if isinstance(value, (dict, list)):
        return {"type": "json", "value": value}
    raise TypeError(
        f"tipo Python non supportato come letterale AST portable: {type(value).__name__}"
    )


def literal_expr(value: Any) -> dict:
    """Espressione `Literal`: `{"kind": "literal", "value": ParameterValue}`."""
    return {"kind": "literal", "value": literal_value(value)}


def column_expr(name: str) -> dict:
    """Espressione `Column`: `{"kind": "column", "value": "<name>"}`."""
    return {"kind": "column", "value": name}


def table_ref(name: str, schema: str | None = None) -> dict:
    """`TableRef`: `{"schema": <opt>, "name": ...}`. Se schema è None, viene
    omesso (Rust ha `skip_serializing_if = Option::is_none`)."""
    out: dict[str, Any] = {"name": name}
    if schema is not None:
        out["schema"] = schema
    return out


def and_predicates(current: dict | None, new: dict) -> dict:
    """Combina predicati in AND. Se `current` è già un And, appende;
    se è un altro predicato, wrappa i due in And. Se None, ritorna `new`."""
    if current is None:
        return new
    if current.get("op") == "and":
        current["predicates"].append(new)
        return current
    return {"op": "and", "predicates": [current, new]}
