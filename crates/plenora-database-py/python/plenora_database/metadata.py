"""Reflection tipizzata e immutabile del catalogo Core v3."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Generic, Mapping, TypeVar

from .expression import Table

T = TypeVar("T")
FrozenValue = str | int | float | bool | None | tuple[Any, ...]


def _freeze(value: Any) -> FrozenValue:
    if isinstance(value, Mapping):
        return tuple((str(key), _freeze(item)) for key, item in sorted(value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError("metadata nativo contiene un tipo non supportato")


@dataclass(frozen=True, slots=True)
class NativeAttributes:
    """Attributi provider-specifici senza esporre un dizionario mutabile."""

    kind: str
    values: tuple[tuple[str, FrozenValue], ...]

    @classmethod
    def from_raw(cls, raw: Any) -> NativeAttributes:
        if isinstance(raw, str):
            return cls(raw.lower(), ())
        if not isinstance(raw, Mapping) or len(raw) != 1:
            raise ValueError("metadata nativo non riconosciuto")
        kind, payload = next(iter(raw.items()))
        if payload is None:
            return cls(str(kind).lower(), ())
        if isinstance(payload, Mapping):
            values = tuple(
                (str(key), _freeze(value)) for key, value in sorted(payload.items())
            )
        else:
            values = (("value", _freeze(payload)),)
        return cls(str(kind).lower(), values)

    def get(self, name: str, default: FrozenValue = None) -> FrozenValue:
        return dict(self.values).get(name, default)

    def items(self) -> tuple[tuple[str, FrozenValue], ...]:
        return self.values


@dataclass(frozen=True, slots=True)
class Observation(Generic[T]):
    measured: bool
    values: tuple[T, ...]


def _observation(raw: Any, parser: Callable[[Any], T]) -> Observation[T]:
    if raw == "NotMeasured":
        return Observation(False, ())
    if isinstance(raw, Mapping) and set(raw) == {"Observed"}:
        values = raw["Observed"]
        if not isinstance(values, list):
            raise ValueError("observation misurata senza elenco")
        return Observation(True, tuple(parser(value) for value in values))
    raise ValueError("observation metadata non riconosciuta")


@dataclass(frozen=True, slots=True)
class SchemaToken:
    provider: str
    fingerprint: str
    native: NativeAttributes

    @classmethod
    def from_raw(cls, raw: Any) -> SchemaToken:
        native = NativeAttributes.from_raw(raw)
        payload = dict(native.values)
        fingerprint = payload.get("structural_fingerprint", payload.get("value"))
        if not isinstance(fingerprint, str):
            raise ValueError("schema token senza fingerprint")
        return cls(native.kind, fingerprint, native)


@dataclass(frozen=True, slots=True)
class SpatialColumnMetadata:
    srid: int | None
    dimensions: str | None
    geometry_type: str | None
    crs_id: str | None


@dataclass(frozen=True, slots=True)
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


@dataclass(frozen=True, slots=True)
class IndexElement:
    expression: str
    included: bool | None
    descending: bool | None
    native: NativeAttributes


@dataclass(frozen=True, slots=True)
class Index:
    name: str | None
    unique: bool | None
    primary: bool | None
    elements: Observation[IndexElement]
    predicate: str | None
    spatial: bool | None
    native: NativeAttributes


@dataclass(frozen=True, slots=True)
class Constraint:
    name: str
    kind: str
    definition: str | None
    columns: Observation[str]
    native: NativeAttributes


@dataclass(frozen=True, slots=True)
class ForeignKey:
    name: str
    columns: Observation[str]
    referenced_schema: str | None
    referenced_object: str
    referenced_columns: Observation[str]
    on_update: str | None
    on_delete: str | None
    match_kind: str | None


@dataclass(frozen=True, slots=True)
class TableMetadata:
    kind: str
    schema_token: SchemaToken
    indexes: Observation[Index]
    constraints: Observation[Constraint]
    foreign_keys: Observation[ForeignKey]
    native: NativeAttributes


@dataclass(frozen=True, slots=True)
class MetaData:
    provider: str
    tables: tuple[Table, ...]

    def one_table(self) -> Table:
        if len(self.tables) != 1:
            raise ValueError("reflection senza un'unica tabella")
        return self.tables[0]

    def table(self, name: str, *, schema: str | None = None) -> Table:
        matches = tuple(
            table
            for table in self.tables
            if table.name == name and (schema is None or table.schema == schema)
        )
        if len(matches) != 1:
            raise KeyError("tabella riflessa assente o ambigua")
        return matches[0]

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> MetaData:
        provider = document.get("provider")
        tables = document.get("tables")
        if not isinstance(provider, str) or not isinstance(tables, list):
            raise ValueError("documento metadata non riconosciuto")
        return cls(provider, tuple(_table(value) for value in tables))


def _spatial(raw: Any) -> SpatialColumnMetadata | None:
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ValueError("metadata spatial non riconosciuti")
    return SpatialColumnMetadata(
        raw.get("srid"),
        raw.get("dimensions"),
        raw.get("geometry_type"),
        raw.get("crs_id"),
    )


def _column(raw: Any) -> tuple[str, ColumnMetadata]:
    if not isinstance(raw, Mapping) or not isinstance(raw.get("name"), str):
        raise ValueError("colonna riflessa non riconosciuta")
    metadata = ColumnMetadata(
        raw.get("ordinal"),
        str(raw.get("native_type", "")),
        raw.get("native_declaration"),
        raw.get("nullable"),
        raw.get("default_expression"),
        raw.get("identity"),
        raw.get("generated"),
        raw.get("numeric_precision"),
        raw.get("numeric_scale"),
        _spatial(raw.get("spatial")),
        NativeAttributes.from_raw(raw.get("native")),
    )
    return raw["name"], metadata


def _index_element(raw: Any) -> IndexElement:
    return IndexElement(
        str(raw["expression"]),
        raw.get("included"),
        raw.get("descending"),
        NativeAttributes.from_raw(raw.get("native")),
    )


def _index(raw: Any) -> Index:
    return Index(
        raw.get("name"),
        raw.get("unique"),
        raw.get("primary"),
        _observation(raw.get("elements"), _index_element),
        raw.get("predicate"),
        raw.get("spatial"),
        NativeAttributes.from_raw(raw.get("native")),
    )


def _constraint(raw: Any) -> Constraint:
    return Constraint(
        str(raw["name"]),
        str(raw["kind"]),
        raw.get("definition"),
        _observation(raw.get("columns"), str),
        NativeAttributes.from_raw(raw.get("native")),
    )


def _foreign_key(raw: Any) -> ForeignKey:
    return ForeignKey(
        str(raw["name"]),
        _observation(raw.get("columns"), str),
        raw.get("referenced_schema"),
        str(raw["referenced_object"]),
        _observation(raw.get("referenced_columns"), str),
        raw.get("on_update"),
        raw.get("on_delete"),
        raw.get("match_kind"),
    )


def _table(raw: Any) -> Table:
    if not isinstance(raw, Mapping):
        raise ValueError("tabella riflessa non riconosciuta")
    reflected_columns = tuple(_column(value) for value in raw.get("columns", ()))
    table = Table(
        str(raw["name"]),
        (name for name, _ in reflected_columns),
        schema=raw.get("schema"),
        catalog=raw.get("catalog"),
    )
    for column, (_, metadata) in zip(table.columns, reflected_columns, strict=True):
        object.__setattr__(column, "metadata", metadata)
    object.__setattr__(
        table,
        "metadata",
        TableMetadata(
            str(raw["kind"]),
            SchemaToken.from_raw(raw.get("schema_token")),
            _observation(raw.get("indexes"), _index),
            _observation(raw.get("constraints"), _constraint),
            _observation(raw.get("foreign_keys"), _foreign_key),
            NativeAttributes.from_raw(raw.get("native")),
        ),
    )
    return table
