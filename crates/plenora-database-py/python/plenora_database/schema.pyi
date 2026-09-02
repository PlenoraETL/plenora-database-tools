from collections.abc import Iterable
from enum import Enum
from typing import Any

from .metadata import MetaData
from .orm import Migration, OrmMetadata

class SchemaRisk(str, Enum):
    SAFE: SchemaRisk
    REQUIRES_LOCK: SchemaRisk
    LOSSY: SchemaRisk
    UNSUPPORTED: SchemaRisk

class SchemaOperation:
    kind: str
    table: str
    risk: SchemaRisk
    statement: str | None
    reverse_statement: str | None
    column: str | None
    reason: str | None
    def __post_init__(self) -> None: ...

class SchemaDiff:
    provider: str
    operations: tuple[SchemaOperation, ...]
    fingerprint: str
    @property
    def is_empty(self) -> bool: ...
    @property
    def risks(self) -> frozenset[SchemaRisk]: ...
    def apply(
        self,
        session: Any,
        *,
        allow: Iterable[SchemaRisk | str] = ...,
    ) -> tuple[str, ...]: ...
    def migration(
        self,
        revision: str,
        down_revision: str | tuple[str, ...] | None,
        *,
        allow: Iterable[SchemaRisk | str] = ...,
    ) -> Migration: ...

def compare_schema(desired: OrmMetadata, observed: MetaData) -> SchemaDiff: ...
