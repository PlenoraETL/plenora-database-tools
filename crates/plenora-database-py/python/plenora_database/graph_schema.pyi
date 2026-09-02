from collections.abc import Iterable
from enum import Enum
from typing import Any

class GraphSchemaRisk(str, Enum):
    SAFE: GraphSchemaRisk
    LOSSY: GraphSchemaRisk
    UNSUPPORTED: GraphSchemaRisk

class GraphIndex:
    label: str
    property: str
    name: str
    unique: bool
    def __post_init__(self) -> None: ...

class GraphEdgeType:
    label: str
    start_label: str
    end_label: str
    def __post_init__(self) -> None: ...

class GraphSchema:
    name: str
    vertex_labels: tuple[str, ...]
    edge_types: tuple[GraphEdgeType, ...]
    indexes: tuple[GraphIndex, ...]
    def __post_init__(self) -> None: ...

class GraphSchemaOperation:
    kind: str
    risk: GraphSchemaRisk
    name: str
    statement: str | None
    reverse_statement: str | None

class GraphSchemaMigration:
    revision: str
    diff: GraphSchemaDiff
    def __post_init__(self) -> None: ...
    def upgrade(self, session: Any) -> tuple[str, ...]: ...
    def downgrade(self, session: Any) -> tuple[str, ...]: ...

class GraphSchemaDiff:
    desired: GraphSchema
    operations: tuple[GraphSchemaOperation, ...]
    @property
    def is_empty(self) -> bool: ...
    def apply(self, session: Any, *, allow: Iterable[GraphSchemaRisk | str] = ...) -> tuple[str, ...]: ...
    async def apply_async(self, session: Any, *, allow: Iterable[GraphSchemaRisk | str] = ...) -> tuple[str, ...]: ...
    def migration(self, revision: str) -> GraphSchemaMigration: ...

def compare_graph_schema(desired: GraphSchema, *, observed_graphs: Iterable[str], observed_indexes: Iterable[str] = (), drop_extra_graphs: bool = False) -> GraphSchemaDiff: ...
