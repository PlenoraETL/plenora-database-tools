from typing import Any, Generic, Mapping, TypeVar
from .expression import Table

T = TypeVar("T")
FrozenValue = str | int | float | bool | None | tuple[Any, ...]

class NativeAttributes:
    kind: str
    values: tuple[tuple[str, FrozenValue], ...]
    @classmethod
    def from_raw(cls, raw: Any) -> NativeAttributes: ...
    def get(self, name: str, default: FrozenValue = ...) -> FrozenValue: ...
    def items(self) -> tuple[tuple[str, FrozenValue], ...]: ...

class Observation(Generic[T]):
    measured: bool
    values: tuple[T, ...]

class SchemaToken:
    provider: str
    fingerprint: str
    native: NativeAttributes
    @classmethod
    def from_raw(cls, raw: Any) -> SchemaToken: ...

class SpatialColumnMetadata:
    srid: int | None
    dimensions: str | None
    geometry_type: str | None
    crs_id: str | None

class ColumnMetadata:
    ordinal: int | None
    native_type: str
    native_declaration: str | None
    nullable: bool | None
    default_expression: str | None
    identity: bool | None
    generated: bool | None
    numeric_precision: int | None
    numeric_scale: int | None
    spatial: SpatialColumnMetadata | None
    native: NativeAttributes

class IndexElement:
    expression: str
    included: bool | None
    descending: bool | None
    native: NativeAttributes

class Index:
    name: str | None
    unique: bool | None
    primary: bool | None
    elements: Observation[IndexElement]
    predicate: str | None
    spatial: bool | None
    native: NativeAttributes

class Constraint:
    name: str
    kind: str
    definition: str | None
    columns: Observation[str]
    native: NativeAttributes

class ForeignKey:
    name: str
    columns: Observation[str]
    referenced_schema: str | None
    referenced_object: str
    referenced_columns: Observation[str]
    on_update: str | None
    on_delete: str | None
    match_kind: str | None

class TableMetadata:
    kind: str
    schema_token: SchemaToken
    indexes: Observation[Index]
    constraints: Observation[Constraint]
    foreign_keys: Observation[ForeignKey]
    native: NativeAttributes

class MetaData:
    provider: str
    tables: tuple[Table, ...]
    def one_table(self) -> Table: ...
    def table(self, name: str, *, schema: str | None = ...) -> Table: ...
    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> MetaData: ...
