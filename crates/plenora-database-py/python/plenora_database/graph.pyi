from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypeAlias, TypeVar

_Model = TypeVar("_Model")

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

@dataclass(frozen=True)
class GraphNode:
    alias: str
    label: str | None = None
    def __post_init__(self) -> None: ...
    def property(self, name: str) -> GraphProperty: ...

@dataclass(frozen=True)
class GraphProperty:
    alias: str
    name: str
    def equals(self, parameter: str) -> GraphCondition: ...

@dataclass(frozen=True)
class GraphCondition:
    property: GraphProperty
    operator: str
    parameter: str

@dataclass(frozen=True)
class CypherQuery:
    graph: str
    def __post_init__(self) -> None: ...
    def match(self, *nodes: GraphNode) -> CypherQuery: ...
    def where(self, *conditions: GraphCondition) -> CypherQuery: ...
    def returning(self, *values: GraphNode | GraphProperty, names: Iterable[str] | None = None) -> CypherQuery: ...
    def limit(self, value: int) -> CypherQuery: ...
    def compile(self) -> tuple[str, list[str]]: ...
    def execute(self, executor: Any, parameters: Mapping[str, GraphValue] | None = None, *, max_rows: int = 10_000) -> list[dict[str, GraphValue]]: ...
    async def execute_async(self, executor: Any, parameters: Mapping[str, GraphValue] | None = None, *, max_rows: int = 10_000) -> list[dict[str, GraphValue]]: ...

def graph_query(graph: str) -> CypherQuery: ...

def vertex_model(label: str, *, id_field: str | None = None) -> Callable[[type[_Model]], type[_Model]]: ...
def edge_model(
    label: str,
    *,
    id_field: str | None = None,
    start_id_field: str = "start_id",
    end_id_field: str = "end_id",
) -> Callable[[type[_Model]], type[_Model]]: ...
def graph_entity_to_model(entity: Vertex | Edge, model: type[_Model]) -> _Model: ...
def graph_model_properties(instance: Any) -> dict[str, GraphValue]: ...
def bulk_vertices(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    merge_key: str | None = None,
    batch_size: int = 100,
) -> int: ...
def bulk_edges(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    start_label: str,
    start_key: str,
    end_label: str,
    end_key: str,
    start_value_field: str = "start",
    end_value_field: str = "end",
    batch_size: int = 100,
) -> int: ...
def graph_property_index_sql(
    graph: str,
    label: str,
    property_name: str,
    index_name: str,
    *,
    unique: bool = False,
) -> str: ...
def graph_unique_constraint_sql(
    graph: str,
    label: str,
    property_name: str,
    constraint_name: str,
) -> str: ...
async def abulk_vertices(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    merge_key: str | None = None,
    batch_size: int = 100,
) -> int: ...
async def abulk_edges(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    start_label: str,
    start_key: str,
    end_label: str,
    end_key: str,
    start_value_field: str = "start",
    end_value_field: str = "end",
    batch_size: int = 100,
) -> int: ...
