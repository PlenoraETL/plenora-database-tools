"""Lifecycle dichiarativo e diff fail-closed per schemi Apache AGE."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from inspect import isawaitable
from typing import Any

from .graph import _identifier, graph_property_index_sql


class GraphSchemaRisk(str, Enum):
    SAFE = "safe"
    LOSSY = "lossy"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True)
class GraphIndex:
    label: str
    property: str
    name: str
    unique: bool = False

    def __post_init__(self) -> None:
        _identifier(self.label, "label graph")
        _identifier(self.property, "proprieta graph")
        _identifier(self.name, "indice graph")


@dataclass(frozen=True, slots=True)
class GraphEdgeType:
    label: str
    start_label: str
    end_label: str

    def __post_init__(self) -> None:
        _identifier(self.label, "label edge")
        _identifier(self.start_label, "label vertex iniziale")
        _identifier(self.end_label, "label vertex finale")


@dataclass(frozen=True, slots=True)
class GraphSchema:
    name: str
    vertex_labels: tuple[str, ...] = ()
    edge_types: tuple[GraphEdgeType, ...] = ()
    indexes: tuple[GraphIndex, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.name, "nome graph")
        for label in self.vertex_labels:
            _identifier(label, "label vertex")
        if len(set(self.vertex_labels)) != len(self.vertex_labels):
            raise ValueError("label vertex duplicate")
        if len({item.label for item in self.edge_types}) != len(self.edge_types):
            raise ValueError("label edge duplicate")
        if len({item.name for item in self.indexes}) != len(self.indexes):
            raise ValueError("indici graph duplicati")
        labels = set(self.vertex_labels)
        if any(
            item.start_label not in labels or item.end_label not in labels
            for item in self.edge_types
        ):
            raise ValueError("edge graph riferisce una label vertex assente")
        if any(item.label not in labels for item in self.indexes):
            raise ValueError("indice graph riferisce una label vertex assente")


@dataclass(frozen=True, slots=True)
class GraphSchemaOperation:
    kind: str
    risk: GraphSchemaRisk
    name: str
    statement: str | None = None
    reverse_statement: str | None = None


@dataclass(frozen=True, slots=True)
class GraphSchemaMigration:
    revision: str
    diff: GraphSchemaDiff

    def __post_init__(self) -> None:
        if not isinstance(self.revision, str) or not self.revision:
            raise ValueError("revision graph non valida")

    def upgrade(self, session: Any) -> tuple[str, ...]:
        return self.diff.apply(session)

    def downgrade(self, session: Any) -> tuple[str, ...]:
        completed: list[str] = []
        for operation in reversed(self.diff.operations):
            if operation.kind == "create-graph":
                session.drop_graph(self.diff.desired.name, cascade=True)
            elif operation.reverse_statement is not None:
                session.execute_ddl(operation.reverse_statement)
            else:
                raise RuntimeError("migrazione graph non reversibile")
            completed.append(operation.kind)
        return tuple(completed)


@dataclass(frozen=True, slots=True)
class GraphSchemaDiff:
    desired: GraphSchema
    operations: tuple[GraphSchemaOperation, ...]

    @property
    def is_empty(self) -> bool:
        return not self.operations

    def apply(
        self,
        session: Any,
        *,
        allow: Iterable[GraphSchemaRisk | str] = (GraphSchemaRisk.SAFE,),
    ) -> tuple[str, ...]:
        allowed = {
            item if isinstance(item, GraphSchemaRisk) else GraphSchemaRisk(item)
            for item in allow
        }
        for operation in self.operations:
            if operation.risk is GraphSchemaRisk.UNSUPPORTED:
                raise RuntimeError("piano graph contiene un'operazione unsupported")
            if operation.risk not in allowed:
                raise RuntimeError("rischio graph non autorizzato")
        completed: list[str] = []
        for operation in self.operations:
            if operation.kind == "create-graph":
                capabilities = getattr(session, "age_admin_capabilities", {})
                if callable(capabilities):
                    capabilities = capabilities()
                if not isinstance(capabilities, dict) or not capabilities.get(
                    "create_graph", False
                ):
                    raise RuntimeError("create_graph AGE non qualificato")
                session.create_graph(self.desired.name)
            elif operation.kind == "drop-graph":
                session.drop_graph(operation.name, cascade=True)
            elif operation.statement is not None:
                session.execute_ddl(operation.statement)
            else:
                raise RuntimeError("operazione graph priva di esecutore")
            completed.append(operation.kind)
        return tuple(completed)

    async def apply_async(
        self,
        session: Any,
        *,
        allow: Iterable[GraphSchemaRisk | str] = (GraphSchemaRisk.SAFE,),
    ) -> tuple[str, ...]:
        allowed = {
            item if isinstance(item, GraphSchemaRisk) else GraphSchemaRisk(item)
            for item in allow
        }
        if any(
            item.risk is GraphSchemaRisk.UNSUPPORTED or item.risk not in allowed
            for item in self.operations
        ):
            raise RuntimeError("piano graph non autorizzato o unsupported")
        completed: list[str] = []
        for operation in self.operations:
            if operation.kind == "create-graph":
                capabilities = getattr(session, "age_admin_capabilities", None)
                capabilities = capabilities() if callable(capabilities) else capabilities
                if isawaitable(capabilities):
                    capabilities = await capabilities
                if not isinstance(capabilities, dict) or not capabilities.get(
                    "create_graph", False
                ):
                    raise RuntimeError("create_graph AGE non qualificato")
                outcome = session.create_graph(self.desired.name)
            elif operation.kind == "drop-graph":
                outcome = session.drop_graph(operation.name, cascade=True)
            elif operation.statement is not None:
                outcome = session.execute_ddl(operation.statement)
            else:
                raise RuntimeError("operazione graph priva di esecutore")
            if isawaitable(outcome):
                await outcome
            completed.append(operation.kind)
        return tuple(completed)

    def migration(self, revision: str) -> GraphSchemaMigration:
        return GraphSchemaMigration(revision, self)


def compare_graph_schema(
    desired: GraphSchema,
    *,
    observed_graphs: Iterable[str],
    observed_indexes: Iterable[str] = (),
    drop_extra_graphs: bool = False,
) -> GraphSchemaDiff:
    """Confronta sole osservazioni esplicite; label non misurate non sono inferite."""

    if not isinstance(desired, GraphSchema):
        raise TypeError("compare_graph_schema richiede GraphSchema")
    graphs = set(observed_graphs)
    indexes = set(observed_indexes)
    operations: list[GraphSchemaOperation] = []
    if desired.name not in graphs:
        operations.append(
            GraphSchemaOperation("create-graph", GraphSchemaRisk.SAFE, desired.name)
        )
    for index in desired.indexes:
        if index.name not in indexes:
            operations.append(
                GraphSchemaOperation(
                    "create-index",
                    GraphSchemaRisk.SAFE,
                    index.name,
                    graph_property_index_sql(
                        desired.name,
                        index.label,
                        index.property,
                        index.name,
                        unique=index.unique,
                    ),
                    f'DROP INDEX "{desired.name}"."{index.name}"',
                )
            )
    if drop_extra_graphs:
        for graph in sorted(graphs - {desired.name}):
            _identifier(graph, "nome graph osservato")
            operations.append(
                GraphSchemaOperation("drop-graph", GraphSchemaRisk.LOSSY, graph)
            )
    return GraphSchemaDiff(desired, tuple(operations))


__all__ = [
    "GraphEdgeType",
    "GraphIndex",
    "GraphSchema",
    "GraphSchemaDiff",
    "GraphSchemaMigration",
    "GraphSchemaOperation",
    "GraphSchemaRisk",
    "compare_graph_schema",
]
