from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypeAlias

@dataclass(frozen=True)
class Vertex:
    id: int
    label: str
    properties: dict[str, GraphValue]

@dataclass(frozen=True)
class Edge:
    id: int
    label: str
    start_id: int
    end_id: int
    properties: dict[str, GraphValue]

@dataclass(frozen=True)
class Path:
    elements: tuple[GraphValue, ...]

GraphValue: TypeAlias = None | bool | int | float | Decimal | str | list[Any] | dict[str, Any] | Vertex | Edge | Path
