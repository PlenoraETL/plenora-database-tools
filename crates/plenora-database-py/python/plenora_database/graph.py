"""Tipi pubblici per i risultati Apache AGE."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypeAlias


@dataclass(frozen=True, slots=True)
class Vertex:
    id: int
    label: str
    properties: dict[str, GraphValue]


@dataclass(frozen=True, slots=True)
class Edge:
    id: int
    label: str
    start_id: int
    end_id: int
    properties: dict[str, GraphValue]


@dataclass(frozen=True, slots=True)
class Path:
    elements: tuple[GraphValue, ...]


GraphValue: TypeAlias = (
    None | bool | int | float | Decimal | str | list[Any] | dict[str, Any] | Vertex | Edge | Path
)


def _decode_value(encoded: dict[str, Any]) -> GraphValue:
    kind = encoded["type"]
    if kind == "null":
        return None
    value = encoded.get("value")
    if kind in {"bool", "integer", "float", "string"}:
        return value
    if kind == "numeric":
        return Decimal(value)
    if kind == "list":
        return [_decode_value(item) for item in value]
    if kind == "map":
        return {key: _decode_value(item) for key, item in value.items()}
    if kind == "vertex":
        return Vertex(
            id=value["id"],
            label=value["label"],
            properties={
                key: _decode_value(item) for key, item in value["properties"].items()
            },
        )
    if kind == "edge":
        return Edge(
            id=value["id"],
            label=value["label"],
            start_id=value["start_id"],
            end_id=value["end_id"],
            properties={
                key: _decode_value(item) for key, item in value["properties"].items()
            },
        )
    if kind == "path":
        return Path(tuple(_decode_value(item) for item in value))
    raise ValueError("tipo agtype non riconosciuto")


def _decode_rows(rows: list[dict[str, dict[str, Any]]]) -> list[dict[str, GraphValue]]:
    return [
        {column: _decode_value(value) for column, value in row.items()}
        for row in rows
    ]


__all__ = ["Edge", "GraphValue", "Path", "Vertex"]
