from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator, Mapping
from datetime import date, datetime, time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Generic, TypeVar

from .errors import PlenoraDataMappingError
from .orm import DeclarativeBase, Geometry

T = TypeVar("T", bound=DeclarativeBase)

class JsonInputError(PlenoraDataMappingError):
    code: str
    record_index: int | None
    field: str | None
    def __init__(
        self,
        code: str,
        message: str,
        *,
        record_index: int | None = ...,
        field: str | None = ...,
    ) -> None: ...

@dataclass(frozen=True, slots=True)
class JsonGeometry:
    srid: int
    dimensions: str = ...
    semantics: str = ...
    geometry_type: str | None = ...
    encoding: str = ...
    def __post_init__(self) -> None: ...
    def orm_type(self) -> Geometry: ...

@dataclass(frozen=True, slots=True)
class JsonField:
    name: str
    type_: type[Any] | JsonGeometry | Geometry
    nullable: bool = ...
    def __post_init__(self) -> None: ...

class JsonSchema:
    fields: tuple[JsonField, ...]
    def __init__(self, fields: Iterable[JsonField]) -> None: ...
    @classmethod
    def from_model(cls, model: type[DeclarativeBase]) -> JsonSchema: ...

class JsonInput(Generic[T]):
    schema: JsonSchema
    def __init__(self, schema: JsonSchema) -> None: ...
    @classmethod
    def for_model(cls, model: type[T]) -> JsonInput[T]: ...
    def records(
        self,
        source: str | bytes | bytearray | Mapping[str, Any] | Iterable[Any],
    ) -> Iterator[dict[str, Any]]: ...
    def arecords(
        self, source: AsyncIterable[Any]
    ) -> AsyncIterator[dict[str, Any]]: ...
    def batches(
        self,
        source: str | bytes | bytearray | Mapping[str, Any] | Iterable[Any],
        *,
        batch_size: int = ...,
    ) -> Iterator[Any]: ...
    def abatches(
        self, source: AsyncIterable[Any], *, batch_size: int = ...
    ) -> AsyncIterator[Any]: ...
    def objects(
        self,
        source: str | bytes | bytearray | Mapping[str, Any] | Iterable[Any],
        model: type[T],
    ) -> Iterator[T]: ...
    def aobjects(
        self, source: AsyncIterable[Any], model: type[T]
    ) -> AsyncIterator[T]: ...
    def arrow_schema(self) -> Any: ...

__all__: list[str]
