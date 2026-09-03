"""ORM dichiarativo sync/async sopra il lifecycle e l'IR Core v3.

Il modulo concentra mapping, relazioni esplicite senza lazy I/O, identity map,
unit of work, query di entita, DDL e migrazioni lineari. Le capability non
qualificate per un provider falliscono prima di inviare lo statement.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, MutableSequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from math import isfinite
import re
from uuid import UUID as UUIDValue
from inspect import isawaitable
from types import TracebackType, UnionType
from typing import Any, Generic, TypeVar, Union, get_args, get_origin, overload

from .expression import (
    BindType,
    Column,
    Expression,
    Ordering,
    Predicate,
    SelectStatement,
    Table,
    _spatial_function,
    _spatial_output,
    _spatial_predicate,
    _spatial_value,
    and_,
    bind,
    delete,
    func,
    insert,
    or_,
    select,
    update,
    upsert,
)
from .result import MultipleResultsFound, MutationResult, NoResultFound, Result
from .errors import PlenoraError
from .spatial import SpatialReference, _require_geographic_srids
from .types import int64 as typed_int64
from .types import date as typed_date
from .types import decimal as typed_decimal
from .types import null as typed_null
from .types import timestamp as typed_timestamp
from .types import timestamptz as typed_timestamptz
from .types import uuid as typed_uuid

T = TypeVar("T")

_GEOMETRY_TYPES = frozenset(
    {
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "circularstring",
        "compoundcurve",
        "curvepolygon",
        "multicurve",
        "multisurface",
        "polyhedralsurface",
        "tin",
        "triangle",
    }
)

_MYSQL_GEOMETRY_TYPES = frozenset(
    {
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
    }
)
_MYSQL_ORM_PROVIDERS = frozenset({"mysql", "mariadb"})
_SQLSERVER_ORM_PROVIDERS = frozenset({"sqlserver"})
_DB2_ORM_PROVIDERS = frozenset({"db2"})
_ORACLE_ORM_PROVIDERS = frozenset({"oracle"})
_WKB_ORM_PROVIDERS = frozenset(
    {
        *_MYSQL_ORM_PROVIDERS,
        *_SQLSERVER_ORM_PROVIDERS,
        *_DB2_ORM_PROVIDERS,
        *_ORACLE_ORM_PROVIDERS,
    }
)
_SPATIAL_NULL_WRAPPER_PROVIDERS = (
    _SQLSERVER_ORM_PROVIDERS | _DB2_ORM_PROVIDERS | _ORACLE_ORM_PROVIDERS
)
_FRAMED_ORM_PROVIDERS = _WKB_ORM_PROVIDERS
_GEOMETRY_ONLY_ORM_PROVIDERS = (
    _MYSQL_ORM_PROVIDERS | _DB2_ORM_PROVIDERS | _ORACLE_ORM_PROVIDERS
)
_XY_XYZ_ORM_PROVIDERS = (
    _SQLSERVER_ORM_PROVIDERS | _DB2_ORM_PROVIDERS | _ORACLE_ORM_PROVIDERS
)
_QUALIFIED_ORM_GEOMETRY_TYPES = frozenset({"point", "linestring", "polygon"})
_GEOMETRY_ORM_PROVIDERS = frozenset({"postgres", *_WKB_ORM_PROVIDERS})


class OrmError(PlenoraError):
    """Errore pubblico dello strato ORM; non include valori applicativi."""


class OrmMappingError(OrmError):
    """Il mapping dichiarativo viola un'invariante dell'ORM."""


class OrmStateError(OrmError):
    """L'operazione non e valida nello stato corrente dell'istanza/sessione."""


class StaleObjectError(OrmError):
    """Una mutazione ottimistica non ha interessato esattamente una riga."""


class OrmUnsupportedError(OrmError):
    """Capability ORM non ancora aperta da una prova riproducibile."""


class ObjectState(str, Enum):
    TRANSIENT = "transient"
    PENDING = "pending"
    PERSISTENT = "persistent"
    DELETED = "deleted"
    DETACHED = "detached"


@dataclass(frozen=True, slots=True)
class Geometry:
    """Tipo ORM spatial con CRS e dimensionalita dichiarati.

    Il valore Python associato e ``SpatialReference``. Ogni assegnazione viene
    riverificata dal validatore EWKB autorevole del modulo nativo.
    """

    srid: int
    dimensions: str = "xy"
    semantics: str = "geometry"
    geometry_type: str | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.srid, int)
            or isinstance(self.srid, bool)
            or self.srid <= 0
        ):
            raise ValueError("Geometry.srid deve essere un intero positivo")
        if self.dimensions not in {"xy", "xyz", "xym", "xyzm"}:
            raise ValueError("Geometry.dimensions non valida")
        if self.semantics not in {"geometry", "geography"}:
            raise ValueError("Geometry.semantics non valida")
        if self.geometry_type is not None:
            if not isinstance(self.geometry_type, str):
                raise ValueError("Geometry.geometry_type non valido")
            normalized = self.geometry_type.strip().lower()
            if normalized not in _GEOMETRY_TYPES:
                raise ValueError("Geometry.geometry_type non valido")
            object.__setattr__(self, "geometry_type", normalized)

    def validate(self, value: bytes | bytearray | SpatialReference) -> SpatialReference:
        if isinstance(value, SpatialReference):
            ewkb = value.ewkb
            if value.srid != self.srid or value.semantics != self.semantics:
                raise ValueError(
                    "valore geometry incompatibile con il mapping dichiarato"
                )
            dimensions = value.dimensions
        elif isinstance(value, (bytes, bytearray)):
            ewkb = bytes(value)
            dimensions = self.dimensions
        else:
            raise TypeError("Geometry accetta bytes, bytearray o SpatialReference")
        if dimensions != self.dimensions:
            raise ValueError(
                "dimensioni geometry incompatibili con il mapping dichiarato"
            )
        validated = SpatialReference.validated(
            ewkb, self.srid, self.dimensions, self.semantics
        )
        if self.geometry_type is not None:
            try:
                from . import _native
            except ImportError as error:
                raise RuntimeError(
                    "modulo nativo non disponibile per validare geometry_type"
                ) from error
            inspector = getattr(_native, "inspect_ewkb_geometry_type", None)
            if inspector is None:
                raise RuntimeError(
                    "estensione nativa incompatibile con geometry_type ORM"
                )
            actual = inspector(ewkb, self.srid, self.dimensions)
            if not isinstance(actual, str) or actual.lower() != self.geometry_type:
                raise ValueError("tipo EWKB incompatibile con il mapping dichiarato")
        return validated

    def bind(self, name: str) -> Expression:
        """Bind EWKB tipizzato con frame spatial del mapping."""

        return _spatial_value(bind(name, BindType.BINARY), self.srid, self.semantics)

    def predicate(
        self,
        function: str,
        column: Column,
        reference: Expression,
        distance: Expression | None = None,
    ) -> Predicate:
        supported = {
            "intersects",
            "contains",
            "within",
            "covers",
            "covered_by",
            "touches",
            "crosses",
            "overlaps",
            "disjoint",
            "equals",
            "d_within",
        }
        if function not in supported:
            raise OrmUnsupportedError("predicato spatial ORM non supportato")
        if not isinstance(column, Column) or not isinstance(reference, Expression):
            raise TypeError("predicato spatial ORM richiede colonna e riferimento")
        if function == "d_within":
            if not isinstance(distance, Expression):
                raise TypeError("d_within richiede una distanza bindata")
            return _spatial_predicate(function, column, reference, distance)
        if distance is not None:
            raise TypeError("la distanza e valida soltanto per d_within")
        return _spatial_predicate(function, column, reference)

    def function(
        self, function: str, column: Column, *arguments: Expression
    ) -> Expression:
        supported = {
            "geometry_type",
            "srid",
            "dimensions",
            "n_points",
            "is_empty",
            "is_valid",
            "area",
            "length",
            "distance",
            "centroid",
            "envelope",
        }
        if function not in supported:
            raise OrmUnsupportedError("funzione spatial ORM non supportata")
        if not isinstance(column, Column) or not all(
            isinstance(item, Expression) for item in arguments
        ):
            raise TypeError("funzione spatial ORM richiede espressioni relazionali")
        return _spatial_function(function, column, *arguments)


@dataclass(frozen=True, slots=True)
class BigInteger:
    """Tipo ORM portabile per un intero SQL signed a 64 bit."""


BIGINT = BigInteger()


@dataclass(frozen=True, slots=True)
class String:
    """Testo portabile con lunghezza massima DDL opzionale."""

    length: int | None = None

    def __post_init__(self) -> None:
        if self.length is not None and (
            not isinstance(self.length, int)
            or isinstance(self.length, bool)
            or self.length <= 0
            or self.length > 32_672
        ):
            raise ValueError("String.length non valida")


@dataclass(frozen=True, slots=True)
class Numeric:
    """Decimal portabile nel dominio comune Decimal128 dei provider."""

    precision: int = 38
    scale: int = 10

    def __post_init__(self) -> None:
        if (
            not isinstance(self.precision, int)
            or isinstance(self.precision, bool)
            or not 1 <= self.precision <= 38
            or not isinstance(self.scale, int)
            or isinstance(self.scale, bool)
            or not 0 <= self.scale <= self.precision
        ):
            raise ValueError("Numeric precision/scale non validi")


@dataclass(frozen=True, slots=True)
class Uuid:
    """UUID portabile; il valore Python pubblico resta una stringa canonica."""


@dataclass(frozen=True, slots=True)
class Json:
    """Documento JSON portabile rappresentato da dict o list Python."""


@dataclass(frozen=True, slots=True)
class DateTime:
    """Timestamp ORM con semantica timezone esplicita."""

    timezone: bool = False


UUID = Uuid()
JSON = Json()


class _InstanceState:
    __slots__ = (
        "dirty",
        "expired",
        "original",
        "relationship_original",
        "rollback_relationships",
        "rollback_snapshot",
        "rollback_state",
        "session",
        "status",
    )

    def __init__(self) -> None:
        self.status = ObjectState.TRANSIENT
        self.original: dict[str, Any] = {}
        self.rollback_snapshot: dict[str, Any] = {}
        self.rollback_state = ObjectState.TRANSIENT
        self.dirty: set[str] = set()
        self.expired: set[str] = set()
        self.relationship_original: dict[str, tuple[tuple[Any, ...], ...]] = {}
        self.rollback_relationships: dict[str, Any] = {}
        self.session: OrmSession | None = None


@dataclass(frozen=True, slots=True)
class InstanceInspection:
    state: ObjectState
    identity: tuple[Any, ...] | None
    dirty: tuple[str, ...]


@dataclass(slots=True)
class _SavepointInstance:
    values: dict[str, Any]
    relationships: dict[str, Any]
    status: ObjectState
    original: dict[str, Any]
    dirty: set[str]
    expired: set[str]
    relationship_original: dict[str, tuple[tuple[Any, ...], ...]]


@dataclass(slots=True)
class _SavepointSnapshot:
    identity_map: dict[Any, DeclarativeBase]
    pending: list[DeclarativeBase]
    deleted: list[DeclarativeBase]
    flushed_deleted: list[DeclarativeBase]
    instances: dict[DeclarativeBase, _SavepointInstance]


class MappedColumn(Generic[T]):
    """Descrittore dichiarativo; sulla classe espone la ``Column`` canonica."""

    def __init__(
        self,
        type_: Any | None = None,
        *,
        primary_key: bool = False,
        version: bool = False,
        nullable: bool = True,
        generated: bool = False,
        server_default: bool | ServerDefault = False,
        unique: bool = False,
    ) -> None:
        if primary_key and version:
            raise OrmMappingError(
                "una colonna non puo essere insieme chiave e versione"
            )
        if generated and not primary_key:
            raise OrmMappingError(
                "generated e supportato soltanto sulla chiave primaria"
            )
        if version and server_default:
            raise OrmMappingError("la versione ORM non puo usare un server default")
        self.type_ = type_
        self.primary_key = bool(primary_key)
        self.version = bool(version)
        self.nullable = False if primary_key or version else bool(nullable)
        self.generated = bool(generated)
        self.server_default = bool(server_default)
        self.server_default_spec = (
            server_default if isinstance(server_default, ServerDefault) else None
        )
        self.unique = bool(unique)
        self.name: str | None = None
        self.column: Column | None = None

    def _bind(self, name: str, column: Column) -> None:
        self.name = name
        self.column = column

    def _clone(self) -> MappedColumn[Any]:
        return MappedColumn(
            self.type_,
            primary_key=self.primary_key,
            version=self.version,
            nullable=self.nullable,
            generated=self.generated,
            server_default=self.server_default_spec or self.server_default,
            unique=self.unique,
        )

    def __get__(self, instance: Any, owner: type | None = None) -> T | Column | None:
        if instance is None:
            if self.column is None:
                raise OrmMappingError("attributo mappato non associato a una tabella")
            return self.column
        if self.name is None:
            raise OrmMappingError("attributo mappato senza nome")
        if self.name in _state(instance).expired:
            raise OrmStateError("attributo scaduto: usare OrmSession.refresh")
        return instance.__dict__.get(self.name)

    def __set__(self, instance: Any, value: T | None) -> None:
        if self.name is None:
            raise OrmMappingError("attributo mappato senza nome")
        value = self._coerce(value)
        state = _state(instance)
        state.expired.discard(self.name)
        previous = instance.__dict__.get(self.name)
        if state.status is ObjectState.PERSISTENT and previous != value:
            if self.primary_key or self.version:
                raise OrmStateError("chiave primaria e versione sono immutabili")
            if state.original.get(self.name) == value:
                state.dirty.discard(self.name)
            else:
                state.dirty.add(self.name)
        instance.__dict__[self.name] = value

    def _coerce(self, value: Any) -> Any:
        if value is None:
            if not self.nullable:
                raise ValueError("una colonna non nullable non accetta None")
            return None
        if isinstance(self.type_, Geometry):
            return self.type_.validate(value)
        if isinstance(self.type_, BigInteger):
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError("la colonna BIGINT richiede un int Python")
            if not -(2**63) <= value < 2**63:
                raise ValueError("la colonna BIGINT richiede un intero signed 64-bit")
            return value
        if isinstance(self.type_, String):
            if not isinstance(value, str):
                raise TypeError("la colonna String richiede str")
            if self.type_.length is not None and len(value) > self.type_.length:
                raise ValueError("la colonna String supera la lunghezza dichiarata")
            return value
        if isinstance(self.type_, Numeric) or self.type_ is Decimal:
            if isinstance(value, str):
                try:
                    value = Decimal(value)
                except Exception as error:
                    raise ValueError("valore Decimal non valido") from error
            if not isinstance(value, Decimal):
                raise TypeError("la colonna Numeric richiede Decimal")
            if not value.is_finite():
                raise ValueError("la colonna Numeric richiede un Decimal finito")
            if isinstance(self.type_, Numeric):
                sign, digits, exponent = value.as_tuple()
                del sign
                fractional = max(0, -exponent)
                integer = max(0, len(digits) + exponent)
                if fractional > self.type_.scale or integer + fractional > self.type_.precision:
                    raise ValueError("Decimal fuori da precision/scale dichiarati")
            return value
        if isinstance(self.type_, Uuid):
            try:
                return str(UUIDValue(str(value)))
            except (AttributeError, TypeError, ValueError) as error:
                raise ValueError("valore UUID non valido") from error
        if isinstance(self.type_, Json):
            if not isinstance(value, (dict, list)):
                raise TypeError("la colonna Json richiede dict o list")
            return value
        if isinstance(self.type_, DateTime):
            if isinstance(value, str):
                try:
                    value = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError as error:
                    raise ValueError("timestamp ORM non valido") from error
            if not isinstance(value, datetime):
                raise TypeError("la colonna DateTime richiede datetime")
            aware = value.tzinfo is not None and value.utcoffset() is not None
            if aware != self.type_.timezone:
                raise ValueError("timezone datetime incompatibile con il mapping")
            return value
        if self.type_ is datetime and isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError("timestamp ORM non valido") from error
        if self.type_ is datetime and isinstance(value, datetime):
            if value.tzinfo is not None and value.utcoffset() is not None:
                raise ValueError(
                    "datetime con timezone richiede DateTime(timezone=True)"
                )
        if self.type_ is date and isinstance(value, str):
            try:
                value = date.fromisoformat(value)
            except ValueError as error:
                raise ValueError("date ORM non valida") from error
        if self.type_ is Decimal and isinstance(value, str):
            try:
                value = Decimal(value)
            except Exception as error:
                raise ValueError("decimal ORM non valido") from error
        if isinstance(self.type_, type):
            if self.type_ is int and isinstance(value, bool):
                raise TypeError("la colonna int non accetta bool")
            if not isinstance(value, self.type_):
                raise TypeError("valore incompatibile con il tipo Python mappato")
            if self.type_ is int and not -(2**31) <= value < 2**31:
                raise ValueError("la colonna int richiede un intero SQL INTEGER")
        return value


Mapped = MappedColumn


def mapped_column(
    type_: Any | None = None,
    *,
    primary_key: bool = False,
    version: bool = False,
    nullable: bool = True,
    generated: bool = False,
    server_default: bool | ServerDefault = False,
    unique: bool = False,
) -> MappedColumn[Any]:
    return MappedColumn(
        type_,
        primary_key=primary_key,
        version=version,
        nullable=nullable,
        generated=generated,
        server_default=server_default,
        unique=unique,
    )


@dataclass(frozen=True, slots=True)
class ServerDefault:
    kind: str
    value: Any = None

    @classmethod
    def literal(cls, value: str | int | float | bool) -> ServerDefault:
        if not isinstance(value, (str, int, float, bool)):
            raise TypeError("server default literal non supportato")
        if isinstance(value, float) and not isfinite(value):
            raise ValueError("server default numerico non finito")
        return cls("literal", value)

    @classmethod
    def current_timestamp(cls) -> ServerDefault:
        return cls("current_timestamp")

    def __post_init__(self) -> None:
        if self.kind not in {"literal", "current_timestamp"}:
            raise ValueError("server default non valido")


@dataclass(frozen=True, slots=True)
class UniqueConstraint:
    columns: tuple[str, ...]
    name: str | None = None

    def __init__(self, *columns: str, name: str | None = None) -> None:
        if not columns or any(
            not isinstance(item, str) or not item for item in columns
        ):
            raise OrmMappingError("UniqueConstraint richiede colonne valide")
        object.__setattr__(self, "columns", tuple(columns))
        object.__setattr__(self, "name", name)


@dataclass(frozen=True, slots=True)
class CheckConstraint:
    """CHECK strutturale a una colonna, senza accettare frammenti SQL raw."""

    column: str
    operator: str
    value: str | int | float | bool
    name: str

    def __post_init__(self) -> None:
        if not isinstance(self.column, str) or not self.column:
            raise OrmMappingError("CheckConstraint richiede una colonna valida")
        if self.operator not in {"=", "!=", "<>", "<", "<=", ">", ">="}:
            raise OrmMappingError("CheckConstraint operator non valido")
        if not isinstance(self.value, (str, int, float, bool)):
            raise OrmMappingError("CheckConstraint valore non supportato")
        if isinstance(self.value, float) and not isfinite(self.value):
            raise OrmMappingError("CheckConstraint valore numerico non finito")
        if not isinstance(self.name, str) or not self.name:
            raise OrmMappingError("CheckConstraint richiede un nome stabile")

    @property
    def columns(self) -> tuple[str, ...]:
        return (self.column,)


@dataclass(frozen=True, slots=True)
class OrmIndex:
    """Indice ORM nominato; il nome rende reflection e diff deterministici."""

    columns: tuple[str, ...]
    name: str
    unique: bool = False

    def __init__(self, *columns: str, name: str, unique: bool = False) -> None:
        if not columns or any(
            not isinstance(column, str) or not column for column in columns
        ):
            raise OrmMappingError("OrmIndex richiede colonne valide")
        if not isinstance(name, str) or not name:
            raise OrmMappingError("OrmIndex richiede un nome stabile")
        if not isinstance(unique, bool):
            raise OrmMappingError("OrmIndex unique deve essere booleano")
        object.__setattr__(self, "columns", tuple(columns))
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)


@dataclass(frozen=True, slots=True)
class ForeignKeyConstraint:
    columns: tuple[str, ...]
    target: type[DeclarativeBase] | str
    target_columns: tuple[str, ...]
    name: str | None = None
    on_delete: str | None = None
    on_update: str | None = None

    def __init__(
        self,
        columns: Iterable[str],
        target: type[DeclarativeBase] | str,
        target_columns: Iterable[str],
        *,
        name: str | None = None,
        on_delete: str | None = None,
        on_update: str | None = None,
    ) -> None:
        local = tuple(columns)
        remote = tuple(target_columns)
        if not local or len(local) != len(remote):
            raise OrmMappingError(
                "ForeignKeyConstraint richiede colonne corrispondenti"
            )
        if any(not isinstance(item, str) or not item for item in (*local, *remote)):
            raise OrmMappingError(
                "ForeignKeyConstraint contiene una colonna non valida"
            )
        if not isinstance(target, (type, str)):
            raise OrmMappingError("ForeignKeyConstraint target non valido")
        normalized_delete = None if on_delete is None else on_delete.upper()
        if normalized_delete not in {None, "CASCADE", "RESTRICT", "SET NULL"}:
            raise OrmMappingError("ForeignKeyConstraint on_delete non valido")
        normalized_update = None if on_update is None else on_update.upper()
        if normalized_update not in {None, "CASCADE", "RESTRICT", "SET NULL"}:
            raise OrmMappingError("ForeignKeyConstraint on_update non valido")
        object.__setattr__(self, "columns", local)
        object.__setattr__(self, "target", target)
        object.__setattr__(self, "target_columns", remote)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "on_delete", normalized_delete)
        object.__setattr__(self, "on_update", normalized_update)


_CASCADE_VALUES = frozenset({"save-update", "delete", "delete-orphan"})
_INSERT_RETURNING_PROVIDERS = frozenset({"postgres", "mariadb", "sqlserver"})
_ORM_EVENTS = frozenset(
    {
        "before_flush",
        "after_flush",
        "before_insert",
        "after_insert",
        "before_update",
        "after_update",
        "before_delete",
        "after_delete",
        "after_commit",
        "after_rollback",
        "load",
        "refresh",
    }
)


class _RelationshipCollection(MutableSequence[T]):
    """Lista strumentata che mantiene entrambi i lati senza fare I/O."""

    def __init__(self, owner: DeclarativeBase, relation: Relationship[T]) -> None:
        self._owner = owner
        self._relation = relation
        self._items: list[T] = []

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int | slice) -> T | list[T]:
        return self._items[index]

    def __delitem__(self, index: int | slice) -> None:
        values = self._items[index]
        del self._items[index]
        for value in values if isinstance(index, slice) else (values,):
            self._relation._collection_removed(self._owner, value)

    def __setitem__(self, index: int | slice, value: T | Iterable[T]) -> None:
        if isinstance(index, slice):
            replacement = list(value)  # type: ignore[arg-type]
            previous = self._items[index]
            for item in replacement:
                self._relation._validate_value(item)
            self._items[index] = replacement
            for item in previous:
                if item not in self._items:
                    self._relation._collection_removed(self._owner, item)
            for item in replacement:
                self._relation._collection_added(self._owner, item)
            return
        self._relation._validate_value(value)
        previous = self._items[index]
        self._items[index] = value  # type: ignore[assignment]
        if previous is not value:
            self._relation._collection_removed(self._owner, previous)
            self._relation._collection_added(self._owner, value)  # type: ignore[arg-type]

    def insert(self, index: int, value: T) -> None:
        self._relation._validate_value(value)
        if any(item is value for item in self._items):
            return
        self._items.insert(index, value)
        self._relation._collection_added(self._owner, value)

    def _append_from_backref(self, value: T) -> None:
        if not any(item is value for item in self._items):
            self._items.append(value)

    def _remove_from_backref(self, value: T) -> None:
        for index, item in enumerate(self._items):
            if item is value:
                self._items.pop(index)
                break


class Relationship(Generic[T]):
    """Relazione esplicita scalare o a collezione, sempre priva di lazy I/O."""

    def __init__(
        self,
        target: type[T] | str,
        *,
        foreign_key: str | tuple[str, ...] | None = None,
        uselist: bool = False,
        back_populates: str | None = None,
        cascade: str | Iterable[str] = (),
        secondary: Table | None = None,
        secondary_local_key: str | tuple[str, ...] | None = None,
        secondary_remote_key: str | tuple[str, ...] | None = None,
        passive_deletes: bool = False,
    ) -> None:
        if (
            not isinstance(target, (type, str))
            or isinstance(target, str)
            and not target
        ):
            raise TypeError("relationship target richiede una classe o il suo nome")
        if foreign_key is not None:
            foreign_keys = (
                (foreign_key,) if isinstance(foreign_key, str) else foreign_key
            )
            if (
                not isinstance(foreign_keys, tuple)
                or not foreign_keys
                or any(not isinstance(item, str) or not item for item in foreign_keys)
                or len(set(foreign_keys)) != len(foreign_keys)
            ):
                raise OrmMappingError("relationship foreign_key non valida")
        if back_populates is not None and (
            not isinstance(back_populates, str) or not back_populates
        ):
            raise OrmMappingError("relationship back_populates non valido")
        if isinstance(cascade, str):
            cascades = frozenset(
                item.strip() for item in cascade.split(",") if item.strip()
            )
        else:
            cascades = frozenset(cascade)
        if not cascades <= _CASCADE_VALUES:
            raise OrmMappingError(
                "relationship cascade contiene un valore non supportato"
            )
        if "delete-orphan" in cascades and (not uselist or "delete" not in cascades):
            raise OrmMappingError("delete-orphan richiede uselist e cascade delete")
        if not isinstance(passive_deletes, bool):
            raise TypeError("passive_deletes deve essere booleano")
        if passive_deletes and "delete" not in cascades:
            raise OrmMappingError("passive_deletes richiede cascade delete")
        if secondary is not None:
            if not isinstance(secondary, Table) or not uselist:
                raise OrmMappingError("secondary richiede una relazione uselist")
            if foreign_key is not None:
                raise OrmMappingError("secondary non accetta foreign_key")
            if "delete-orphan" in cascades:
                raise OrmMappingError("delete-orphan non e valido su many-to-many")
            secondary_local_keys = _relationship_keys(secondary_local_key)
            secondary_remote_keys = _relationship_keys(secondary_remote_key)
            for keys in (secondary_local_keys, secondary_remote_keys):
                if not keys:
                    raise OrmMappingError("secondary richiede entrambe le chiavi")
                for key in keys:
                    try:
                        secondary.c[key]
                    except KeyError as error:
                        raise OrmMappingError(
                            "chiave secondary non presente"
                        ) from error
        elif foreign_key is None:
            raise OrmMappingError("relationship richiede foreign_key")
        self._target_ref = target
        self.foreign_key = foreign_key
        self.uselist = bool(uselist)
        self.back_populates = back_populates
        self.cascade = cascades
        self.secondary = secondary
        self.secondary_local_key = secondary_local_key
        self.secondary_remote_key = secondary_remote_key
        self.passive_deletes = passive_deletes
        self.name: str | None = None
        self.owner: type[DeclarativeBase] | None = None

    @property
    def target(self) -> type[T]:
        if isinstance(self._target_ref, type):
            return self._target_ref
        if self.owner is None:
            raise OrmMappingError("relationship non associata a un mapper")
        return self.owner.__registry__._resolve(self._target_ref)  # type: ignore[return-value]

    @property
    def direction(self) -> str:
        if self.secondary is not None:
            return "many-to-many"
        if self.uselist:
            return "one-to-many"
        if self.owner is None or self.foreign_key is None:
            raise OrmMappingError("relationship non configurata")
        owner_mapper = _mapper(self.owner)
        foreign_keys = self.foreign_keys
        if all(
            any(item.name == key for item in owner_mapper.attributes)
            for key in foreign_keys
        ):
            return "many-to-one"
        target_mapper = _mapper(self.target)  # type: ignore[arg-type]
        if all(
            any(item.name == key for item in target_mapper.attributes)
            for key in foreign_keys
        ):
            return "one-to-one"
        raise OrmMappingError("relationship riferisce una foreign key non mappata")

    @property
    def foreign_keys(self) -> tuple[str, ...]:
        if self.foreign_key is None:
            return ()
        return (
            (self.foreign_key,)
            if isinstance(self.foreign_key, str)
            else self.foreign_key
        )

    @property
    def secondary_local_keys(self) -> tuple[str, ...]:
        return _relationship_keys(self.secondary_local_key)

    @property
    def secondary_remote_keys(self) -> tuple[str, ...]:
        return _relationship_keys(self.secondary_remote_key)

    def _bind(self, name: str, owner: type[DeclarativeBase]) -> None:
        self.name = name
        self.owner = owner

    def _clone(self) -> Relationship[T]:
        return Relationship(
            self._target_ref,
            foreign_key=self.foreign_key,
            uselist=self.uselist,
            back_populates=self.back_populates,
            cascade=self.cascade,
            secondary=self.secondary,
            secondary_local_key=self.secondary_local_key,
            secondary_remote_key=self.secondary_remote_key,
            passive_deletes=self.passive_deletes,
        )

    def _validate_configuration(self) -> None:
        target_mapper = _mapper(self.target)  # type: ignore[arg-type]
        if self.owner is None:
            raise OrmMappingError("relationship non associata a un mapper")
        direction = self.direction
        owner_mapper = _mapper(self.owner)
        if direction == "many-to-one":
            primary_keys = target_mapper.primary_keys
            foreign_mapper = owner_mapper
        else:
            primary_keys = owner_mapper.primary_keys
            foreign_mapper = target_mapper
        if self.secondary is None:
            if len(self.foreign_keys) != len(primary_keys):
                raise OrmMappingError(
                    "relationship richiede una foreign key per ogni colonna primaria"
                )
            for foreign_key in self.foreign_keys:
                foreign_mapper.attribute(foreign_key)
        elif len(self.secondary_local_keys) != len(owner_mapper.primary_keys) or len(
            self.secondary_remote_keys
        ) != len(target_mapper.primary_keys):
            raise OrmMappingError(
                "secondary richiede una chiave per ogni colonna primaria"
            )
        if self.back_populates is not None:
            inverse = target_mapper.relationship(self.back_populates)
            if inverse.target is not self.owner or inverse.back_populates != self.name:
                raise OrmMappingError("back_populates non reciproco")
            if self.secondary is not inverse.secondary:
                raise OrmMappingError("back_populates usa una secondary diversa")

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Relationship[T]: ...

    @overload
    def __get__(
        self, instance: DeclarativeBase, owner: type | None = None
    ) -> T | None | MutableSequence[T]: ...

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        if self.name is None:
            raise OrmMappingError("relationship senza nome")
        if self.name not in instance.__dict__:
            raise OrmStateError("relazione non caricata: usare OrmSession.load")
        return instance.__dict__[self.name]

    def __set__(self, instance: Any, value: Any) -> None:
        if self.name is None:
            raise OrmMappingError("relationship senza nome")
        if self.uselist:
            if value is None or isinstance(value, (str, bytes, bytearray)):
                raise TypeError("relationship uselist richiede un iterabile di modelli")
            collection = _RelationshipCollection(instance, self)
            for item in value:
                collection.append(item)
            previous = instance.__dict__.get(self.name)
            if isinstance(previous, _RelationshipCollection):
                for item in tuple(previous):
                    if not any(candidate is item for candidate in collection):
                        self._collection_removed(instance, item)
            instance.__dict__[self.name] = collection
            return
        self._validate_value(value, nullable=True)
        previous = instance.__dict__.get(self.name)
        if previous is value and self.name in instance.__dict__:
            return
        instance.__dict__[self.name] = value
        self._synchronize_scalar_foreign_key(instance, value)
        if value is not None:
            self._synchronize_child_foreign_key(instance, value)
            owner_state = _state(instance)
            if "save-update" in self.cascade and owner_state.session is not None:
                owner_state.session.add(value)
        self._synchronize_inverse(instance, previous, value)

    def _validate_value(self, value: Any, *, nullable: bool = False) -> None:
        if value is None and nullable:
            return
        if not isinstance(value, self.target):
            raise TypeError("valore relationship incompatibile con il target")

    def _collection_added(self, owner: DeclarativeBase, value: T) -> None:
        self._synchronize_child_foreign_key(owner, value)
        owner_state = _state(owner)
        if "save-update" in self.cascade and owner_state.session is not None:
            owner_state.session.add(value)
        if self.back_populates is not None:
            inverse = _mapper(self.target).relationship(self.back_populates)  # type: ignore[arg-type]
            inverse._assign_from_backref(value, owner)

    def _collection_removed(self, owner: DeclarativeBase, value: T) -> None:
        if self.back_populates is not None:
            inverse = _mapper(self.target).relationship(self.back_populates)  # type: ignore[arg-type]
            if "delete-orphan" in self.cascade and inverse.name is not None:
                value.__dict__[inverse.name] = None
            else:
                inverse._remove_from_backref(value, owner)
        if "delete-orphan" in self.cascade:
            state = _state(value)
            if state.session is not None and state.status in {
                ObjectState.PENDING,
                ObjectState.PERSISTENT,
            }:
                state.session.delete(value)

    def _assign_from_backref(self, instance: T, value: DeclarativeBase) -> None:
        if self.name is None:
            raise OrmMappingError("relationship senza nome")
        if self.uselist:
            collection = instance.__dict__.get(self.name)
            if not isinstance(collection, _RelationshipCollection):
                collection = _RelationshipCollection(instance, self)
                instance.__dict__[self.name] = collection
            collection._append_from_backref(value)
        else:
            previous = instance.__dict__.get(self.name)
            instance.__dict__[self.name] = value
            self._synchronize_scalar_foreign_key(instance, value)
            if previous is not None and previous is not value:
                self._remove_inverse_only(instance, previous)

    def _remove_from_backref(self, instance: T, value: DeclarativeBase) -> None:
        if self.name is None:
            raise OrmMappingError("relationship senza nome")
        if self.uselist:
            collection = instance.__dict__.get(self.name)
            if isinstance(collection, _RelationshipCollection):
                collection._remove_from_backref(value)
        elif instance.__dict__.get(self.name) is value:
            instance.__dict__[self.name] = None
            self._synchronize_scalar_foreign_key(instance, None)

    def _synchronize_scalar_foreign_key(
        self, instance: DeclarativeBase, value: T | None
    ) -> None:
        if self.direction != "many-to-one" or self.foreign_key is None:
            return
        primary_keys = _mapper(self.target).primary_keys  # type: ignore[arg-type]
        primary = tuple(
            None
            if value is None or item.name is None
            else value.__dict__.get(item.name)
            for item in primary_keys
        )
        if value is None or all(item is not None for item in primary):
            for foreign_key, item in zip(self.foreign_keys, primary, strict=True):
                setattr(instance, foreign_key, item)

    def _synchronize_child_foreign_key(self, owner: DeclarativeBase, value: T) -> None:
        if (
            self.direction not in {"one-to-many", "one-to-one"}
            or self.foreign_key is None
        ):
            return
        primary = tuple(
            None if item.name is None else owner.__dict__.get(item.name)
            for item in _mapper(type(owner)).primary_keys
        )
        if all(item is not None for item in primary):
            for foreign_key, item in zip(self.foreign_keys, primary, strict=True):
                setattr(value, foreign_key, item)

    def _synchronize_inverse(
        self, instance: DeclarativeBase, previous: T | None, value: T | None
    ) -> None:
        if self.back_populates is None:
            return
        inverse = _mapper(self.target).relationship(self.back_populates)  # type: ignore[arg-type]
        if previous is not None:
            inverse._remove_from_backref(previous, instance)
        if value is not None:
            inverse._assign_from_backref(value, instance)

    def _remove_inverse_only(self, instance: T, previous: DeclarativeBase) -> None:
        if self.back_populates is None:
            return
        inverse = _mapper(type(previous)).relationship(self.back_populates)
        inverse._remove_from_backref(previous, instance)


def relationship(
    target: type[T] | str,
    *,
    foreign_key: str | tuple[str, ...] | None = None,
    uselist: bool = False,
    back_populates: str | None = None,
    cascade: str | Iterable[str] = (),
    secondary: Table | None = None,
    secondary_local_key: str | tuple[str, ...] | None = None,
    secondary_remote_key: str | tuple[str, ...] | None = None,
    passive_deletes: bool = False,
) -> Relationship[T]:
    return Relationship(
        target,
        foreign_key=foreign_key,
        uselist=uselist,
        back_populates=back_populates,
        cascade=cascade,
        secondary=secondary,
        secondary_local_key=secondary_local_key,
        secondary_remote_key=secondary_remote_key,
        passive_deletes=passive_deletes,
    )


@dataclass(frozen=True, slots=True)
class Mapper:
    model: type[DeclarativeBase]
    table: Table
    attributes: tuple[MappedColumn[Any], ...]
    primary_key: MappedColumn[Any]
    primary_keys: tuple[MappedColumn[Any], ...]
    version: MappedColumn[Any] | None
    relationships: tuple[Relationship[Any], ...]
    constraints: tuple[
        UniqueConstraint | CheckConstraint | OrmIndex | ForeignKeyConstraint, ...
    ]
    inherits: Mapper | None
    inheritance: str = "none"
    local_attributes: tuple[MappedColumn[Any], ...] = ()
    polymorphic_identity: Any = None
    polymorphic_on: str | None = None

    def attribute(self, name: str) -> MappedColumn[Any]:
        for attribute in self.attributes:
            if attribute.name == name:
                return attribute
        raise KeyError("attributo non presente nel mapper")

    def relationship(self, name: str) -> Relationship[Any]:
        for relation in self.relationships:
            if relation.name == name:
                return relation
        raise KeyError("relationship non presente nel mapper")


class Registry:
    """Registro immutabile dall'esterno dei mapper dichiarativi."""

    def __init__(self) -> None:
        self._by_model: dict[type[DeclarativeBase], Mapper] = {}
        self._by_name: dict[str, type[DeclarativeBase]] = {}

    def _register(self, mapper: Mapper) -> None:
        if mapper.model in self._by_model:
            raise OrmMappingError("classe gia presente nel registry")
        if mapper.model.__name__ in self._by_name:
            raise OrmMappingError("nome modello gia presente nel registry")
        self._by_model[mapper.model] = mapper
        self._by_name[mapper.model.__name__] = mapper.model

    def _resolve(self, name: str) -> type[DeclarativeBase]:
        try:
            return self._by_name[name]
        except KeyError as error:
            raise OrmMappingError(
                "target relationship non presente nel registry"
            ) from error

    def mapper_for(self, model: type[DeclarativeBase]) -> Mapper:
        try:
            return self._by_model[model]
        except KeyError as error:
            raise OrmMappingError("classe non mappata") from error

    def mappers(self) -> tuple[Mapper, ...]:
        return tuple(self._by_model.values())

    def polymorphic_mapper(self, mapper: Mapper, identity: Any) -> Mapper:
        root = _inheritance_root(mapper)
        matches = tuple(
            candidate
            for candidate in self._by_model.values()
            if _inheritance_root(candidate) is root
            and candidate is not root
            and candidate.inheritance == "single"
            and candidate.polymorphic_identity == identity
        )
        if len(matches) > 1:
            raise OrmMappingError("identita polimorfica duplicata nel registry")
        return matches[0] if matches else mapper


mapper_registry = Registry()


class OrmMetadata:
    """Compilatore DDL ristretto ai mapper dichiarativi del registry."""

    def __init__(
        self,
        registry: Registry = mapper_registry,
        *,
        models: Iterable[type[DeclarativeBase]] | None = None,
    ) -> None:
        if not isinstance(registry, Registry):
            raise TypeError("OrmMetadata richiede Registry")
        self.registry = registry
        self._models = None if models is None else tuple(models)
        if self._models is not None:
            for model in self._models:
                registry.mapper_for(model)

    @property
    def mappers(self) -> tuple[Mapper, ...]:
        if self._models is None:
            return self.registry.mappers()
        return tuple(self.registry.mapper_for(model) for model in self._models)

    def ddl(self, provider: str, *, checkfirst: bool = False) -> tuple[str, ...]:
        ordered = _ddl_mapper_order(self.mappers, self.registry)
        unique: list[Mapper] = []
        seen: set[int] = set()
        for mapper in ordered:
            if id(mapper.table) not in seen:
                seen.add(id(mapper.table))
                unique.append(mapper)
        statements: list[str] = []
        for mapper in unique:
            statements.append(
                _create_table_ddl(
                    mapper, provider, self.registry, checkfirst=checkfirst
                )
            )
            if provider == "oracle":
                statements.extend(_oracle_spatial_metadata_ddl(mapper))
            statements.extend(
                _create_index_ddl(mapper, index, provider, checkfirst=checkfirst)
                for index in mapper.constraints
                if isinstance(index, OrmIndex)
            )
        return tuple(statements)

    def create_all(self, session: Any, *, checkfirst: bool = False) -> None:
        provider = _session_provider(session)
        for statement in self.ddl(provider, checkfirst=checkfirst):
            _execute_ddl(session, statement)

    async def create_all_async(self, session: Any, *, checkfirst: bool = False) -> None:
        provider = _session_provider(session)
        for statement in self.ddl(provider, checkfirst=checkfirst):
            await _execute_ddl_async(session, statement)

    def drop_all(self, session: Any, *, checkfirst: bool = True) -> None:
        provider = _session_provider(session)
        if checkfirst and provider in {"oracle", "db2"}:
            raise OrmUnsupportedError(
                "il provider non qualifica DROP TABLE IF EXISTS"
            )
        ordered = _unique_table_mappers(_ddl_mapper_order(self.mappers, self.registry))
        for mapper in reversed(ordered):
            target = _qualified_table(mapper.table, provider)
            clause = " IF EXISTS" if checkfirst and provider != "sqlserver" else ""
            if checkfirst and provider == "sqlserver":
                object_name = _object_name(mapper.table).replace("'", "''")
                _execute_ddl(
                    session,
                    f"IF OBJECT_ID(N'{object_name}', N'U') IS NOT NULL DROP TABLE {target}",
                )
            else:
                _execute_ddl(session, f"DROP TABLE{clause} {target}")

    async def drop_all_async(self, session: Any, *, checkfirst: bool = True) -> None:
        provider = _session_provider(session)
        if checkfirst and provider in {"oracle", "db2"}:
            raise OrmUnsupportedError(
                "il provider non qualifica DROP TABLE IF EXISTS"
            )
        ordered = _unique_table_mappers(_ddl_mapper_order(self.mappers, self.registry))
        for mapper in reversed(ordered):
            target = _qualified_table(mapper.table, provider)
            clause = " IF EXISTS" if checkfirst and provider != "sqlserver" else ""
            if checkfirst and provider == "sqlserver":
                object_name = _object_name(mapper.table).replace("'", "''")
                await _execute_ddl_async(
                    session,
                    f"IF OBJECT_ID(N'{object_name}', N'U') IS NOT NULL DROP TABLE {target}",
                )
            else:
                await _execute_ddl_async(session, f"DROP TABLE{clause} {target}")


@dataclass(frozen=True, slots=True)
class Migration:
    revision: str
    down_revision: str | tuple[str, ...] | None
    upgrade: Any
    downgrade: Any | None = None
    checksum: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.revision, str) or not self.revision:
            raise ValueError("migration revision non valida")
        if self.revision.startswith("__plenora_"):
            raise ValueError("migration revision usa un namespace riservato")
        parents = _migration_parents(self.down_revision)
        if len(set(parents)) != len(parents) or self.revision in parents:
            raise ValueError("migration down_revision duplicata o autoriferita")
        if not callable(self.upgrade) or (
            self.downgrade is not None and not callable(self.downgrade)
        ):
            raise TypeError("migration richiede callback valide")
        if not re.fullmatch(r"[0-9a-f]{64}", self.checksum):
            raise ValueError("migration checksum deve essere SHA-256 esadecimale")


class MigrationRunner:
    """Runner DAG e transazionale di migrazioni esplicite."""

    def __init__(self, migrations: Iterable[Migration]) -> None:
        self.migrations = _order_migrations(tuple(migrations))

    def apply(self, session: Any) -> tuple[str, ...]:
        provider = _session_provider(session)
        _ensure_migration_table(session, provider)
        _migration_history(
            self.migrations, session.query_sql(_migration_history_select(provider))
        )
        completed: list[str] = []
        for migration in self.migrations:
            transaction = session.begin()
            try:
                transaction.query_sql(_migration_lock_sql(provider))
                history = _migration_history(
                    self.migrations,
                    transaction.query_sql(_migration_history_select(provider)),
                )
                if migration.revision in history:
                    transaction.commit()
                    continue
                statement, parameters = _migration_insert_statement(
                    migration, "running"
                )
                transaction.execute(statement, parameters)
                migration.upgrade(transaction)
                statement, parameters = _migration_state_statement(
                    migration, "applied"
                )
                transaction.execute(statement, parameters)
                transaction.commit()
            except BaseException:
                transaction.rollback()
                _record_migration_failure(session, provider, migration)
                raise
            completed.append(migration.revision)
        return tuple(completed)

    def rollback(self, session: Any, *, steps: int = 1) -> tuple[str, ...]:
        if not isinstance(steps, int) or isinstance(steps, bool) or steps < 1:
            raise ValueError("migration rollback steps non valido")
        provider = _session_provider(session)
        _ensure_migration_table(session, provider)
        rows = session.query_sql(_migration_history_select(provider))
        by_revision = {item.revision: item for item in self.migrations}
        applied = set(_migration_history(self.migrations, rows))
        completed: list[str] = []
        candidates = [
            item.revision
            for item in reversed(self.migrations)
            if item.revision in applied
        ]
        for revision in candidates[:steps]:
            if any(
                revision in _migration_parents(item.down_revision)
                and item.revision in applied
                for item in self.migrations
            ):
                raise OrmStateError("rollback migrazione non foglia del grafo")
            migration = by_revision.get(revision)
            if migration is None or migration.downgrade is None:
                raise OrmStateError("migration applicata priva di downgrade registrato")
            transaction = session.begin()
            try:
                transaction.query_sql(_migration_lock_sql(provider))
                current = set(
                    _migration_history(
                        self.migrations,
                        transaction.query_sql(_migration_history_select(provider)),
                    )
                )
                if revision not in current:
                    transaction.commit()
                    continue
                migration.downgrade(transaction)
                statement, parameters = _migration_delete_statement(migration)
                transaction.execute(statement, parameters)
                transaction.commit()
            except BaseException:
                transaction.rollback()
                raise
            applied.remove(migration.revision)
            completed.append(migration.revision)
        return tuple(completed)

    def recover(
        self,
        session: Any,
        revision: str,
        *,
        assume_applied: bool = False,
    ) -> None:
        migration = next(
            (item for item in self.migrations if item.revision == revision), None
        )
        if migration is None:
            raise OrmStateError("recover richiede una revisione registrata")
        provider = _session_provider(session)
        _ensure_migration_table(session, provider)
        transaction = session.begin()
        try:
            transaction.query_sql(_migration_lock_sql(provider))
            rows = transaction.query_sql(_migration_history_select(provider))
            target = next((row for row in rows if row["revision"] == revision), None)
            if target is None:
                raise OrmStateError("recover richiede una migrazione incompleta")
            if target["checksum"] != migration.checksum:
                raise OrmStateError("drift checksum nella storia migrazioni")
            if target["state"] not in {"running", "failed"}:
                raise OrmStateError("recover rifiuta una migrazione gia applicata")
            if assume_applied:
                statement, parameters = _migration_state_statement(
                    migration, "applied"
                )
            else:
                statement, parameters = _migration_delete_statement(migration)
            transaction.execute(statement, parameters)
            transaction.commit()
        except BaseException:
            transaction.rollback()
            raise


class AsyncMigrationRunner(MigrationRunner):
    async def apply(self, session: Any) -> tuple[str, ...]:  # type: ignore[override]
        provider = _session_provider(session)
        await _ensure_migration_table_async(session, provider)
        _migration_history(
            self.migrations,
            await session.query_sql(_migration_history_select(provider)),
        )
        completed: list[str] = []
        for migration in self.migrations:
            transaction = await session.begin()
            try:
                await transaction.query_sql(_migration_lock_sql(provider))
                history = _migration_history(
                    self.migrations,
                    await transaction.query_sql(_migration_history_select(provider)),
                )
                if migration.revision in history:
                    await transaction.commit()
                    continue
                statement, parameters = _migration_insert_statement(
                    migration, "running"
                )
                await transaction.execute(statement, parameters)
                outcome = migration.upgrade(transaction)
                if isawaitable(outcome):
                    await outcome
                statement, parameters = _migration_state_statement(
                    migration, "applied"
                )
                await transaction.execute(statement, parameters)
                await transaction.commit()
            except BaseException:
                await transaction.rollback()
                await _record_migration_failure_async(session, provider, migration)
                raise
            completed.append(migration.revision)
        return tuple(completed)

    async def rollback(  # type: ignore[override]
        self, session: Any, *, steps: int = 1
    ) -> tuple[str, ...]:
        if not isinstance(steps, int) or isinstance(steps, bool) or steps < 1:
            raise ValueError("migration rollback steps non valido")
        provider = _session_provider(session)
        await _ensure_migration_table_async(session, provider)
        rows = await session.query_sql(_migration_history_select(provider))
        by_revision = {item.revision: item for item in self.migrations}
        applied = set(_migration_history(self.migrations, rows))
        completed: list[str] = []
        candidates = [
            item.revision
            for item in reversed(self.migrations)
            if item.revision in applied
        ]
        for revision in candidates[:steps]:
            if any(
                revision in _migration_parents(item.down_revision)
                and item.revision in applied
                for item in self.migrations
            ):
                raise OrmStateError("rollback migrazione non foglia del grafo")
            migration = by_revision.get(revision)
            if migration is None or migration.downgrade is None:
                raise OrmStateError("migration applicata priva di downgrade registrato")
            transaction = await session.begin()
            try:
                await transaction.query_sql(_migration_lock_sql(provider))
                current = set(
                    _migration_history(
                        self.migrations,
                        await transaction.query_sql(
                            _migration_history_select(provider)
                        ),
                    )
                )
                if revision not in current:
                    await transaction.commit()
                    continue
                outcome = migration.downgrade(transaction)
                if isawaitable(outcome):
                    await outcome
                statement, parameters = _migration_delete_statement(migration)
                await transaction.execute(statement, parameters)
                await transaction.commit()
            except BaseException:
                await transaction.rollback()
                raise
            applied.remove(migration.revision)
            completed.append(migration.revision)
        return tuple(completed)

    async def recover(  # type: ignore[override]
        self,
        session: Any,
        revision: str,
        *,
        assume_applied: bool = False,
    ) -> None:
        migration = next(
            (item for item in self.migrations if item.revision == revision), None
        )
        if migration is None:
            raise OrmStateError("recover richiede una revisione registrata")
        provider = _session_provider(session)
        await _ensure_migration_table_async(session, provider)
        transaction = await session.begin()
        try:
            await transaction.query_sql(_migration_lock_sql(provider))
            rows = await transaction.query_sql(_migration_history_select(provider))
            target = next((row for row in rows if row["revision"] == revision), None)
            if target is None:
                raise OrmStateError("recover richiede una migrazione incompleta")
            if target["checksum"] != migration.checksum:
                raise OrmStateError("drift checksum nella storia migrazioni")
            if target["state"] not in {"running", "failed"}:
                raise OrmStateError("recover rifiuta una migrazione gia applicata")
            if assume_applied:
                statement, parameters = _migration_state_statement(
                    migration, "applied"
                )
            else:
                statement, parameters = _migration_delete_statement(migration)
            await transaction.execute(statement, parameters)
            await transaction.commit()
        except BaseException:
            await transaction.rollback()
            raise


_UNRESOLVED_MAPPED_TYPE = object()
_MAPPED_ANNOTATION_TYPES = {
    "bool": bool,
    "bytes": bytes,
    "date": date,
    "datetime": datetime,
    "Decimal": Decimal,
    "float": float,
    "int": int,
    "str": str,
    "time": time,
}


def _mapped_annotation_type(annotation: Any) -> Any:
    """Estrae il tipo di ``Mapped[T]`` senza risolvere forward ref arbitrarie."""

    if isinstance(annotation, str):
        match = re.fullmatch(r"(?:[A-Za-z_]\w*\.)?Mapped\[(.+)\]", annotation.strip())
        if match is None:
            return _UNRESOLVED_MAPPED_TYPE
        candidates = {
            item.strip().removeprefix("builtins.")
            for item in match.group(1).split("|")
            if item.strip() not in {"None", "NoneType"}
        }
        if len(candidates) != 1:
            return _UNRESOLVED_MAPPED_TYPE
        return _MAPPED_ANNOTATION_TYPES.get(
            candidates.pop(), _UNRESOLVED_MAPPED_TYPE
        )

    if get_origin(annotation) is not MappedColumn:
        return _UNRESOLVED_MAPPED_TYPE
    arguments = get_args(annotation)
    if len(arguments) != 1:
        return _UNRESOLVED_MAPPED_TYPE
    candidate = arguments[0]
    if get_origin(candidate) in {Union, UnionType}:
        values = tuple(item for item in get_args(candidate) if item is not type(None))
        if len(values) != 1:
            return _UNRESOLVED_MAPPED_TYPE
        candidate = values[0]
    return (
        candidate
        if candidate in set(_MAPPED_ANNOTATION_TYPES.values())
        else _UNRESOLVED_MAPPED_TYPE
    )


def _apply_annotation_types(namespace: Mapping[str, Any]) -> None:
    annotations = namespace.get("__annotations__", {})
    if not isinstance(annotations, Mapping):
        return
    for name, annotation in annotations.items():
        attribute = namespace.get(name)
        if not isinstance(attribute, MappedColumn) or attribute.type_ is not None:
            continue
        inferred = _mapped_annotation_type(annotation)
        if inferred is not _UNRESOLVED_MAPPED_TYPE:
            attribute.type_ = inferred


class _DeclarativeMeta(type):
    def __new__(mcls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        _apply_annotation_types(namespace)
        table_name = namespace.get("__tablename__")
        inherited_mappers = tuple(
            mapper
            for base in bases
            if isinstance((mapper := getattr(base, "__mapper__", None)), Mapper)
        )
        mapper_args = namespace.get("__mapper_args__", {})
        if not isinstance(mapper_args, Mapping):
            raise OrmMappingError("__mapper_args__ richiede un mapping")
        if len(inherited_mappers) > 1:
            raise OrmUnsupportedError("ereditarieta multipla mappata non supportata")
        inherited = inherited_mappers[0] if inherited_mappers else None
        strategy = "none"
        if inherited is not None:
            if mapper_args.get("concrete"):
                strategy = "concrete"
            elif table_name is None and "polymorphic_identity" in mapper_args:
                strategy = "single"
            elif table_name is not None and (
                mapper_args.get("joined") or mapper_args.get("inheritance") == "joined"
            ):
                strategy = "joined"
            else:
                raise OrmUnsupportedError(
                    "ereditarieta richiede concrete, joined o polymorphic_identity single-table"
                )
        if strategy == "single":
            if inherited is None or inherited.polymorphic_on is None:
                raise OrmMappingError(
                    "single-table richiede polymorphic_on sul mapper base"
                )
            declared_attributes = tuple(
                value for value in namespace.values() if isinstance(value, MappedColumn)
            )
            declared_relationships = tuple(
                value for value in namespace.values() if isinstance(value, Relationship)
            )
            if any(
                attribute.primary_key or attribute.version
                for attribute in declared_attributes
            ):
                raise OrmMappingError(
                    "single-table eredita chiave primaria e versione dalla base"
                )
            if any(
                not attribute.nullable and not attribute.server_default
                for attribute in declared_attributes
            ):
                raise OrmMappingError(
                    "una colonna single-table di sottotipo deve essere nullable o avere server default"
                )
            if namespace.get("__table_args__"):
                raise OrmUnsupportedError("vincoli locali single-table non qualificati")
            cls = super().__new__(mcls, name, bases, namespace)
            identity = mapper_args.get("polymorphic_identity")
            if identity is None:
                raise OrmMappingError("single-table richiede polymorphic_identity")
            registry = inherited.model.__registry__
            root = _inheritance_root(inherited)
            if any(
                candidate.polymorphic_identity == identity
                for candidate in registry.mappers()
                if candidate is not root
                and candidate.inheritance == "single"
                and _inheritance_root(candidate) is root
            ):
                raise OrmMappingError("identita polimorfica duplicata nel registry")
            previous_table = inherited.table
            names = tuple(column.name for column in previous_table.columns)
            declared_names = tuple(
                key for key, value in namespace.items() if value in declared_attributes
            )
            if set(names) & set(declared_names):
                raise OrmMappingError("colonna single-table duplicata nella gerarchia")
            target = Table(
                previous_table.name,
                (*names, *declared_names),
                schema=previous_table.schema,
                catalog=previous_table.catalog,
            )
            for candidate in registry.mappers():
                if candidate.table is previous_table:
                    object.__setattr__(candidate, "table", target)
                    candidate.model.__table__ = target
                    for attribute in candidate.attributes:
                        if attribute.name is not None:
                            attribute._bind(attribute.name, target.c[attribute.name])
            for attribute, column_name in zip(
                declared_attributes, declared_names, strict=True
            ):
                attribute._bind(column_name, target.c[column_name])
            mapper = Mapper(
                cls,
                target,
                (*inherited.attributes, *declared_attributes),
                inherited.primary_key,
                inherited.primary_keys,
                inherited.version,
                (*inherited.relationships, *declared_relationships),
                inherited.constraints,
                inherited,
                "single",
                declared_attributes,
                identity,
                inherited.polymorphic_on,
            )
            cls.__table__ = target
            cls.__mapper__ = mapper
            cls.__registry__ = registry
            registry._register(mapper)
            for relation_name, relation in (
                (key, value)
                for key, value in namespace.items()
                if isinstance(value, Relationship)
            ):
                relation._bind(relation_name, cls)
            return cls
        if table_name is not None:
            mixin_bases = tuple(
                base
                for base in bases
                if getattr(base, "__mapper__", None) is None
                and getattr(base, "__abstract__", False)
                and base.__name__ != "DeclarativeBase"
            )
            if mixin_bases:
                namespace = dict(namespace)
                for base in reversed(mixin_bases):
                    for attribute_name, value in vars(base).items():
                        if attribute_name in namespace:
                            continue
                        if isinstance(value, (MappedColumn, Relationship)):
                            namespace[attribute_name] = value._clone()
        if strategy == "concrete" and inherited is not None:
            namespace = dict(namespace)
            for attribute in inherited.attributes:
                if attribute.name is not None and attribute.name not in namespace:
                    namespace[attribute.name] = attribute._clone()
            for relation in inherited.relationships:
                if relation.name is not None and relation.name not in namespace:
                    namespace[relation.name] = relation._clone()
        cls = super().__new__(mcls, name, bases, namespace)
        if table_name is None:
            if inherited_mappers and not namespace.get("__abstract__", False):
                raise OrmUnsupportedError(
                    "una sottoclasse mappata richiede __tablename__ o __abstract__"
                )
            if inherited_mappers:
                cls.__mapper__ = None
            return cls
        if not isinstance(table_name, str) or not table_name:
            raise OrmMappingError("__tablename__ deve essere una stringa non vuota")
        declared_attributes = tuple(
            value for value in namespace.values() if isinstance(value, MappedColumn)
        )
        declared_relationships = tuple(
            value for value in namespace.values() if isinstance(value, Relationship)
        )
        if strategy == "joined" and inherited is not None:
            if any(attribute.primary_key for attribute in declared_attributes):
                raise OrmMappingError(
                    "joined-table eredita la chiave primaria dalla base"
                )
            attributes = (*inherited.attributes, *declared_attributes)
            relationships = (*inherited.relationships, *declared_relationships)
        else:
            attributes = declared_attributes
            relationships = declared_relationships
        declared_table_args = namespace.get("__table_args__", ())
        if not isinstance(declared_table_args, tuple) or not all(
            isinstance(
                item,
                (UniqueConstraint, CheckConstraint, OrmIndex, ForeignKeyConstraint),
            )
            for item in declared_table_args
        ):
            raise OrmMappingError("__table_args__ richiede una tupla di vincoli ORM")
        table_args = (
            *(
                inherited.constraints
                if inherited is not None and strategy == "concrete"
                else ()
            ),
            *declared_table_args,
        )
        if not attributes:
            raise OrmMappingError("un modello richiede almeno una colonna")
        primary_keys = (
            inherited.primary_keys
            if inherited is not None and strategy == "joined"
            else tuple(item for item in attributes if item.primary_key)
        )
        versions = tuple(item for item in attributes if item.version)
        if not primary_keys:
            raise OrmMappingError("un modello richiede almeno una chiave primaria")
        if sum(item.generated for item in primary_keys) > 1:
            raise OrmMappingError(
                "una chiave composta accetta un solo componente generated"
            )
        if len(versions) > 1:
            raise OrmMappingError("un modello accetta una sola colonna versione")
        local_names = tuple(
            key for key, value in namespace.items() if value in declared_attributes
        )
        names = (
            (*(item.name or "" for item in primary_keys), *local_names)
            if strategy == "joined"
            else local_names
        )
        target = Table(
            table_name,
            names,
            schema=namespace.get("__schema__"),
            catalog=namespace.get("__catalog__"),
        )
        bind_columns = (
            target.columns[len(primary_keys) :]
            if strategy == "joined"
            else target.columns
        )
        for attribute, column in zip(declared_attributes, bind_columns, strict=True):
            attribute._bind(column.name, column)
        constraint_attributes = (
            declared_attributes if strategy == "joined" else attributes
        )
        column_names = {
            *(attribute.name for attribute in constraint_attributes),
            *(attribute.name for attribute in primary_keys if strategy == "joined"),
        }
        constraints: tuple[
            UniqueConstraint | CheckConstraint | OrmIndex | ForeignKeyConstraint, ...
        ] = (
            *table_args,
            *(
                UniqueConstraint(attribute.name or "")
                for attribute in constraint_attributes
                if attribute.unique
                and not any(
                    isinstance(item, UniqueConstraint)
                    and item.columns == (attribute.name,)
                    for item in table_args
                )
            ),
        )
        for constraint in constraints:
            if not set(constraint.columns) <= column_names:
                raise OrmMappingError(
                    "vincolo riferisce una colonna locale non mappata"
                )
        registry = namespace.get("__registry__")
        if registry is None:
            registry = next(
                (base.__registry__ for base in bases if hasattr(base, "__registry__")),
                mapper_registry,
            )
        if not isinstance(registry, Registry):
            raise OrmMappingError("__registry__ richiede Registry")
        mapper = Mapper(
            cls,
            target,
            attributes,
            primary_keys[0],
            primary_keys,
            versions[0] if versions else None,
            relationships,
            constraints,
            inherited,
            strategy,
            declared_attributes,
            mapper_args.get("polymorphic_identity"),
            (
                inherited.polymorphic_on
                if strategy in {"single", "joined"} and inherited is not None
                else mapper_args.get("polymorphic_on")
            ),
        )
        if mapper.polymorphic_on is not None:
            try:
                mapper.attribute(mapper.polymorphic_on)
            except KeyError as error:
                raise OrmMappingError(
                    "polymorphic_on riferisce una colonna non mappata"
                ) from error
        if mapper.polymorphic_on is not None and mapper.polymorphic_identity is None:
            raise OrmMappingError("mapper polimorfico richiede polymorphic_identity")
        cls.__table__ = target
        cls.__mapper__ = mapper
        cls.__registry__ = registry
        registry._register(mapper)
        for relation_name, relation in (
            (key, value)
            for key, value in namespace.items()
            if isinstance(value, Relationship)
        ):
            relation._bind(relation_name, cls)
        return cls


class DeclarativeBase(metaclass=_DeclarativeMeta):
    __registry__ = mapper_registry
    __mapper__: Mapper | None = None
    __table__: Table

    def __init__(self, **values: Any) -> None:
        mapper = _mapper(type(self))
        declared = {
            *(attribute.name for attribute in mapper.attributes),
            *(relation.name for relation in mapper.relationships),
        }
        if set(values) - declared:
            raise TypeError("il costruttore contiene attributi non mappati")
        self.__dict__["_plenora_orm_state"] = _InstanceState()
        for name, value in values.items():
            setattr(self, name, value)


@dataclass(frozen=True, slots=True)
class LoaderOption:
    relationship: Relationship[Any] | tuple[Relationship[Any], ...]
    strategy: str

    def __post_init__(self) -> None:
        if not (
            isinstance(self.relationship, Relationship)
            or (
                isinstance(self.relationship, tuple)
                and self.relationship
                and all(isinstance(item, Relationship) for item in self.relationship)
            )
        ):
            raise TypeError("loader option richiede una relationship")
        if self.strategy not in {"selectin", "joined"}:
            raise ValueError("strategia eager non valida")


def selectinload(
    relation: Relationship[Any], *path: Relationship[Any]
) -> LoaderOption:
    return LoaderOption((relation, *path), "selectin")


def joinedload(relation: Relationship[Any], *path: Relationship[Any]) -> LoaderOption:
    return LoaderOption((relation, *path), "joined")


@dataclass(frozen=True, slots=True)
class OrmQuery(Generic[T]):
    """Query di entita immutabile e legata a una ``OrmSession``."""

    _session: OrmSession
    _mapper: Mapper
    _statement: SelectStatement
    _loaders: tuple[LoaderOption, ...] = ()
    _fixed_parameters: Mapping[str, Any] | None = None

    def where(self, predicate: Predicate) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.limit(value))

    def offset(self, value: int) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.offset(value))

    def distinct(self) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.distinct())

    def distinct_on(self, *expressions: Expression) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.distinct_on(*expressions))

    def with_for_update(
        self,
        *relations: str | Table,
        strength: str = "update",
        nowait: bool = False,
        skip_locked: bool = False,
    ) -> OrmQuery[T]:
        return replace(
            self,
            _statement=self._statement.with_for_update(
                *relations,
                strength=strength,
                nowait=nowait,
                skip_locked=skip_locked,
            ),
        )

    def group_by(self, *expressions: Expression) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.group_by(*expressions))

    def having(self, predicate: Predicate) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.having(predicate))

    def join(
        self,
        relation: str | Relationship[Any] | type[DeclarativeBase],
        on: Predicate | None = None,
        *,
        kind: str = "inner",
    ) -> OrmQuery[T]:
        statement = _join_statement(self._mapper, self._statement, relation, on, kind)
        return replace(self, _statement=statement)

    def where_related(
        self,
        relation: str | Relationship[Any],
        predicate: Predicate,
        *,
        kind: str = "inner",
    ) -> OrmQuery[T]:
        return self.join(relation, kind=kind).where(predicate)

    def options(self, *loaders: LoaderOption) -> OrmQuery[T]:
        statement = self._statement
        additions: list[LoaderOption] = []
        for loader in loaders:
            if not isinstance(loader, LoaderOption):
                raise TypeError("options accetta soltanto loader ORM")
            relation = _query_loader_path(self._mapper, loader.relationship)[0]
            relation._validate_configuration()
            if loader.strategy == "joined":
                statement = _joinedload_statement(
                    self._mapper, statement, relation, self._session._provider
                )
            additions.append(loader)
        return replace(
            self,
            _statement=statement,
            _loaders=(*self._loaders, *additions),
        )

    def project(self, *expressions: Expression) -> OrmRowsQuery:
        if not expressions or not all(
            isinstance(item, Expression) for item in expressions
        ):
            raise TypeError("project richiede espressioni relazionali")
        return OrmRowsQuery(
            self._session, replace(self._statement, projections=expressions)
        )

    def with_entities(self, *models: type[DeclarativeBase]) -> OrmEntityTupleQuery:
        mappers = tuple(_mapper(model) for model in models)
        if not mappers:
            raise TypeError("with_entities richiede almeno un modello")
        projections = tuple(
            projection
            for index, mapper in enumerate(mappers)
            for projection in _orm_projections(
                mapper, f"orm_entity_{index}_", self._session._provider
            )
        )
        return OrmEntityTupleQuery(
            self._session,
            mappers,
            replace(self._statement, projections=projections),
        )

    def all(self, parameters: Mapping[str, Any] | None = None) -> list[T]:
        return self._session._execute_entities(
            self._mapper,
            self._statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
            self._loaders,
        )

    def partitions(
        self,
        batch_size: int,
        parameters: Mapping[str, Any] | None = None,
        *,
        detach: bool = False,
    ) -> Iterator[list[T]]:
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
            raise ValueError("batch_size ORM deve essere positivo")
        if not self._statement.orderings:
            raise OrmStateError("lettura ORM a partizioni richiede order_by")
        _require_stable_partition_order(self._mapper, self._statement.orderings)
        offset = self._statement.row_offset or 0
        remaining = self._statement.row_limit
        while remaining is None or remaining > 0:
            size = batch_size if remaining is None else min(batch_size, remaining)
            statement = replace(self._statement, row_limit=size, row_offset=offset)
            rows = replace(self, _statement=statement).all(parameters)
            if not rows:
                return
            if detach:
                for instance in rows:
                    _expunge_loaded_graph(self._session, instance, set())
            yield rows
            count = len(rows)
            offset += count
            if remaining is not None:
                remaining -= count
            if count < size:
                return

    def stream(
        self,
        batch_size: int,
        parameters: Mapping[str, Any] | None = None,
        *,
        detach: bool = False,
    ) -> Iterator[T]:
        for partition in self.partitions(batch_size, parameters, detach=detach):
            yield from partition

    def first(self, parameters: Mapping[str, Any] | None = None) -> T | None:
        rows = self.limit(1).all(parameters)
        return None if not rows else rows[0]

    def one(self, parameters: Mapping[str, Any] | None = None) -> T:
        rows = self.limit(2).all(parameters)
        if not rows:
            raise NoResultFound("la query ORM non ha restituito righe")
        if len(rows) != 1:
            raise MultipleResultsFound("la query ORM ha restituito piu di una riga")
        return rows[0]

    def one_or_none(self, parameters: Mapping[str, Any] | None = None) -> T | None:
        rows = self.limit(2).all(parameters)
        if len(rows) > 1:
            raise MultipleResultsFound("la query ORM ha restituito piu di una riga")
        return None if not rows else rows[0]

    def count(self, parameters: Mapping[str, Any] | None = None) -> int:
        statement = _count_statement(self._statement)
        value = self._session._execute_scalar_query(
            statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )
        if not isinstance(value, int) or isinstance(value, bool):
            raise OrmStateError("COUNT ORM non ha restituito un intero")
        return value

    def exists(self, parameters: Mapping[str, Any] | None = None) -> bool:
        primary = self._mapper.primary_key.column
        if primary is None:
            raise OrmMappingError("mapper privo di chiave primaria")
        statement = replace(
            self._statement,
            projections=(primary,),
            orderings=(),
            row_limit=1,
            row_offset=None,
        )
        return self._session._execute_exists_query(
            statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )

    def update(
        self,
        values: Mapping[str, Any],
        parameters: Mapping[str, Any] | None = None,
    ) -> int:
        return self._session._execute_bulk_update(
            self._mapper,
            self._statement,
            values,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )

    def delete(self, parameters: Mapping[str, Any] | None = None) -> int:
        return self._session._execute_bulk_delete(
            self._mapper,
            self._statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )


@dataclass(frozen=True, slots=True)
class AsyncOrmQuery(Generic[T]):
    """Versione async della query di entita, sullo stesso IR e mapper."""

    _session: AsyncOrmSession
    _mapper: Mapper
    _statement: SelectStatement
    _loaders: tuple[LoaderOption, ...] = ()
    _fixed_parameters: Mapping[str, Any] | None = None

    def where(self, predicate: Predicate) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.limit(value))

    def offset(self, value: int) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.offset(value))

    def distinct(self) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.distinct())

    def distinct_on(self, *expressions: Expression) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.distinct_on(*expressions))

    def with_for_update(
        self,
        *relations: str | Table,
        strength: str = "update",
        nowait: bool = False,
        skip_locked: bool = False,
    ) -> AsyncOrmQuery[T]:
        return replace(
            self,
            _statement=self._statement.with_for_update(
                *relations,
                strength=strength,
                nowait=nowait,
                skip_locked=skip_locked,
            ),
        )

    def group_by(self, *expressions: Expression) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.group_by(*expressions))

    def having(self, predicate: Predicate) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.having(predicate))

    def join(
        self,
        relation: str | Relationship[Any] | type[DeclarativeBase],
        on: Predicate | None = None,
        *,
        kind: str = "inner",
    ) -> AsyncOrmQuery[T]:
        statement = _join_statement(self._mapper, self._statement, relation, on, kind)
        return replace(self, _statement=statement)

    def where_related(
        self,
        relation: str | Relationship[Any],
        predicate: Predicate,
        *,
        kind: str = "inner",
    ) -> AsyncOrmQuery[T]:
        return self.join(relation, kind=kind).where(predicate)

    def options(self, *loaders: LoaderOption) -> AsyncOrmQuery[T]:
        statement = self._statement
        additions: list[LoaderOption] = []
        for loader in loaders:
            if not isinstance(loader, LoaderOption):
                raise TypeError("options accetta soltanto loader ORM")
            relation = _query_loader_path(self._mapper, loader.relationship)[0]
            relation._validate_configuration()
            if loader.strategy == "joined":
                statement = _joinedload_statement(
                    self._mapper, statement, relation, self._session._provider
                )
            additions.append(loader)
        return replace(
            self,
            _statement=statement,
            _loaders=(*self._loaders, *additions),
        )

    def project(self, *expressions: Expression) -> AsyncOrmRowsQuery:
        if not expressions or not all(
            isinstance(item, Expression) for item in expressions
        ):
            raise TypeError("project richiede espressioni relazionali")
        return AsyncOrmRowsQuery(
            self._session, replace(self._statement, projections=expressions)
        )

    def with_entities(self, *models: type[DeclarativeBase]) -> AsyncOrmEntityTupleQuery:
        mappers = tuple(_mapper(model) for model in models)
        if not mappers:
            raise TypeError("with_entities richiede almeno un modello")
        projections = tuple(
            projection
            for index, mapper in enumerate(mappers)
            for projection in _orm_projections(
                mapper, f"orm_entity_{index}_", self._session._provider
            )
        )
        return AsyncOrmEntityTupleQuery(
            self._session,
            mappers,
            replace(self._statement, projections=projections),
        )

    async def all(self, parameters: Mapping[str, Any] | None = None) -> list[T]:
        return await self._session._execute_entities_async(
            self._mapper,
            self._statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
            self._loaders,
        )

    async def partitions(
        self,
        batch_size: int,
        parameters: Mapping[str, Any] | None = None,
        *,
        detach: bool = False,
    ) -> AsyncIterator[list[T]]:
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
            raise ValueError("batch_size ORM deve essere positivo")
        if not self._statement.orderings:
            raise OrmStateError("lettura ORM a partizioni richiede order_by")
        _require_stable_partition_order(self._mapper, self._statement.orderings)
        offset = self._statement.row_offset or 0
        remaining = self._statement.row_limit
        while remaining is None or remaining > 0:
            size = batch_size if remaining is None else min(batch_size, remaining)
            statement = replace(self._statement, row_limit=size, row_offset=offset)
            rows = await replace(self, _statement=statement).all(parameters)
            if not rows:
                return
            if detach:
                for instance in rows:
                    _expunge_loaded_graph(self._session, instance, set())
            yield rows
            count = len(rows)
            offset += count
            if remaining is not None:
                remaining -= count
            if count < size:
                return

    async def stream(
        self,
        batch_size: int,
        parameters: Mapping[str, Any] | None = None,
        *,
        detach: bool = False,
    ) -> AsyncIterator[T]:
        async for partition in self.partitions(batch_size, parameters, detach=detach):
            for instance in partition:
                yield instance

    async def first(self, parameters: Mapping[str, Any] | None = None) -> T | None:
        rows = await self.limit(1).all(parameters)
        return None if not rows else rows[0]

    async def one(self, parameters: Mapping[str, Any] | None = None) -> T:
        rows = await self.limit(2).all(parameters)
        if not rows:
            raise NoResultFound("la query ORM non ha restituito righe")
        if len(rows) != 1:
            raise MultipleResultsFound("la query ORM ha restituito piu di una riga")
        return rows[0]

    async def one_or_none(
        self, parameters: Mapping[str, Any] | None = None
    ) -> T | None:
        rows = await self.limit(2).all(parameters)
        if len(rows) > 1:
            raise MultipleResultsFound("la query ORM ha restituito piu di una riga")
        return None if not rows else rows[0]

    async def count(self, parameters: Mapping[str, Any] | None = None) -> int:
        statement = _count_statement(self._statement)
        value = await self._session._execute_scalar_query_async(
            statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )
        if not isinstance(value, int) or isinstance(value, bool):
            raise OrmStateError("COUNT ORM non ha restituito un intero")
        return value

    async def exists(self, parameters: Mapping[str, Any] | None = None) -> bool:
        primary = self._mapper.primary_key.column
        if primary is None:
            raise OrmMappingError("mapper privo di chiave primaria")
        statement = replace(
            self._statement,
            projections=(primary,),
            orderings=(),
            row_limit=1,
            row_offset=None,
        )
        return await self._session._execute_exists_query_async(
            statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )

    async def update(
        self,
        values: Mapping[str, Any],
        parameters: Mapping[str, Any] | None = None,
    ) -> int:
        return await self._session._execute_bulk_update_async(
            self._mapper,
            self._statement,
            values,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )

    async def delete(self, parameters: Mapping[str, Any] | None = None) -> int:
        return await self._session._execute_bulk_delete_async(
            self._mapper,
            self._statement,
            _merge_query_parameters(self._fixed_parameters, parameters),
        )


@dataclass(frozen=True, slots=True)
class OrmRowsQuery:
    _session: OrmSession
    _statement: SelectStatement

    def where(self, predicate: Predicate) -> OrmRowsQuery:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> OrmRowsQuery:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> OrmRowsQuery:
        return replace(self, _statement=self._statement.limit(value))

    def all(
        self, parameters: Mapping[str, Any] | None = None
    ) -> list[Mapping[str, Any]]:
        self._session._require_active()
        self._session._autoflush_now()
        _validate_spatial_statement(self._statement, self._session._spatial_functions)
        parameters = _geometry_query_parameters(
            self._statement, parameters, self._session._provider
        )
        result = self._session._transaction.execute(self._statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("proiezione ORM senza risultato relazionale")
        return result.all()


@dataclass(frozen=True, slots=True)
class OrmEntityTupleQuery:
    _session: OrmSession
    _mappers: tuple[Mapper, ...]
    _statement: SelectStatement

    def where(self, predicate: Predicate) -> OrmEntityTupleQuery:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> OrmEntityTupleQuery:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> OrmEntityTupleQuery:
        return replace(self, _statement=self._statement.limit(value))

    def all(self, parameters: Mapping[str, Any] | None = None) -> list[tuple[Any, ...]]:
        self._session._require_active()
        self._session._autoflush_now()
        _validate_spatial_statement(self._statement, self._session._spatial_functions)
        parameters = _geometry_query_parameters(
            self._statement, parameters, self._session._provider
        )
        result = self._session._transaction.execute(self._statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("query ORM multi-entita senza risultato relazionale")
        output: list[tuple[Any, ...]] = []
        for row in result.all():
            entities: list[Any] = []
            for index, mapper in enumerate(self._mappers):
                values = _entity_projection_values(
                    mapper, row, index, self._session._provider
                )
                entities.append(
                    None
                    if _projected_entity_is_null(mapper, values)
                    else self._session._hydrate(mapper, values)
                )
            output.append(tuple(entities))
        return output


@dataclass(frozen=True, slots=True)
class AsyncOrmRowsQuery:
    _session: AsyncOrmSession
    _statement: SelectStatement

    def where(self, predicate: Predicate) -> AsyncOrmRowsQuery:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> AsyncOrmRowsQuery:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> AsyncOrmRowsQuery:
        return replace(self, _statement=self._statement.limit(value))

    async def all(
        self, parameters: Mapping[str, Any] | None = None
    ) -> list[Mapping[str, Any]]:
        await self._session._autoflush_async()
        transaction = await self._session._ensure_started()
        _validate_spatial_statement(self._statement, self._session._spatial_functions)
        parameters = _geometry_query_parameters(
            self._statement, parameters, self._session._provider
        )
        result = await transaction.execute(self._statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("proiezione ORM senza risultato relazionale")
        return result.all()


@dataclass(frozen=True, slots=True)
class AsyncOrmEntityTupleQuery:
    _session: AsyncOrmSession
    _mappers: tuple[Mapper, ...]
    _statement: SelectStatement

    def where(self, predicate: Predicate) -> AsyncOrmEntityTupleQuery:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> AsyncOrmEntityTupleQuery:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> AsyncOrmEntityTupleQuery:
        return replace(self, _statement=self._statement.limit(value))

    async def all(
        self, parameters: Mapping[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        await self._session._autoflush_async()
        transaction = await self._session._ensure_started()
        _validate_spatial_statement(self._statement, self._session._spatial_functions)
        parameters = _geometry_query_parameters(
            self._statement, parameters, self._session._provider
        )
        result = await transaction.execute(self._statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("query ORM multi-entita senza risultato relazionale")
        output: list[tuple[Any, ...]] = []
        for row in result.all():
            entities: list[Any] = []
            for index, mapper in enumerate(self._mappers):
                values = _entity_projection_values(
                    mapper, row, index, self._session._provider
                )
                entities.append(
                    None
                    if _projected_entity_is_null(mapper, values)
                    else await self._session._hydrate_row_async(mapper, values)
                )
            output.append(tuple(entities))
        return output


def _session_spatial_functions(capabilities: Any) -> frozenset[str] | None:
    spatial = capabilities.get("spatial") if isinstance(capabilities, Mapping) else None
    functions = spatial.get("functions") if isinstance(spatial, Mapping) else None
    if not isinstance(functions, list) or not all(
        isinstance(item, str) for item in functions
    ):
        return None
    return frozenset(functions)


def _validate_spatial_statement(
    statement: SelectStatement, functions: frozenset[str] | None
) -> None:
    # Le fake session dei test unitari non pubblicano il documento completo;
    # le sessioni reali si. Quando il catalogo e presente ogni funzione del
    # piano deve appartenere all'intersezione misurata dal provider.
    if functions is None:
        return

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            if value.get("kind") == "spatial":
                function = value.get("function")
                if not isinstance(function, str) or function not in functions:
                    raise OrmUnsupportedError(
                        "funzione spatial ORM non qualificata dal provider"
                    )
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(statement.to_ast())


def _geometry_query_parameters(
    statement: SelectStatement,
    parameters: Mapping[str, Any] | None,
    provider: str,
) -> Mapping[str, Any] | None:
    """Normalizza i bind Geometry canonici nel formato del provider.

    L'IR conserva soltanto nome, SRID e semantica: il payload resta nella
    mappa separata e viene validato qui, immediatamente prima dell'I/O.
    """

    if provider not in _WKB_ORM_PROVIDERS or parameters is None:
        return parameters
    spatial_binds: dict[str, tuple[int, str]] = {}

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            if value.get("kind") == "spatial_value":
                expression = value.get("expression")
                srid = value.get("srid")
                semantics = value.get("semantics")
                if (
                    not isinstance(expression, Mapping)
                    or expression.get("kind") not in {"parameter", "typed_parameter"}
                    or not isinstance(expression.get("name"), str)
                    or not isinstance(srid, int)
                    or isinstance(srid, bool)
                    or not isinstance(semantics, str)
                ):
                    raise OrmMappingError("bind Geometry ORM non valido")
                name = expression["name"]
                frame = (srid, semantics)
                previous = spatial_binds.setdefault(name, frame)
                if previous != frame:
                    raise OrmMappingError(
                        "bind Geometry ORM riusato con frame incompatibili"
                    )
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(statement.to_ast())
    if not spatial_binds:
        return parameters
    normalized = dict(parameters)
    for name, (srid, semantics) in spatial_binds.items():
        if name not in normalized:
            continue
        if provider in _GEOMETRY_ONLY_ORM_PROVIDERS and semantics != "geometry":
            raise OrmUnsupportedError(
                "il provider non qualifica la semantica geography ORM"
            )
        raw = normalized[name]
        if isinstance(raw, SpatialReference):
            if raw.semantics != semantics:
                raise ValueError(
                    "valore geometry incompatibile con il mapping dichiarato"
                )
            ewkb = raw.ewkb
        elif isinstance(raw, (bytes, bytearray)):
            ewkb = bytes(raw)
        else:
            raise TypeError(
                "un bind Geometry ORM accetta bytes, bytearray o SpatialReference"
            )
        dimensions = "xy" if provider in _MYSQL_ORM_PROVIDERS else "unknown"
        reference = SpatialReference.validated(ewkb, srid, dimensions, semantics)
        normalized[name] = _geometry_bind_value(reference, provider)
    return normalized


def _require_geometry_mapping(type_: Geometry, provider: str) -> None:
    if provider not in _GEOMETRY_ORM_PROVIDERS:
        raise OrmUnsupportedError("Geometry ORM non e qualificata per il provider")
    if provider == "postgres":
        return
    if provider in _GEOMETRY_ONLY_ORM_PROVIDERS and type_.semantics != "geometry":
        raise OrmUnsupportedError(
            "il provider non qualifica la semantica geography ORM"
        )
    if provider in _MYSQL_ORM_PROVIDERS and type_.dimensions != "xy":
        raise OrmUnsupportedError(
            "Geometry ORM MySQL/MariaDB qualifica soltanto coordinate XY"
        )
    if provider in _XY_XYZ_ORM_PROVIDERS and type_.dimensions not in {"xy", "xyz"}:
        raise OrmUnsupportedError(
            "Geometry ORM del provider qualifica soltanto coordinate XY e XYZ"
        )
    if (
        type_.geometry_type is not None
        and type_.geometry_type not in _MYSQL_GEOMETRY_TYPES
    ):
        raise OrmUnsupportedError("tipo Geometry ORM non qualificato per MySQL/MariaDB")
    if provider in _XY_XYZ_ORM_PROVIDERS and (
        type_.geometry_type not in _QUALIFIED_ORM_GEOMETRY_TYPES
    ):
        raise OrmUnsupportedError("tipo Geometry ORM non qualificato per il provider")


def _require_geometry_mapper(mapper: Mapper, provider: str) -> None:
    for attribute in mapper.attributes:
        if isinstance(attribute.type_, Geometry):
            _require_geometry_mapping(attribute.type_, provider)


def _geometry_bind_value(value: SpatialReference, provider: str) -> bytes:
    if provider not in _WKB_ORM_PROVIDERS:
        return value.ewkb
    try:
        from . import _native
    except ImportError as error:
        raise RuntimeError(
            "modulo nativo non disponibile per preparare Geometry ORM"
        ) from error
    converter_name = (
        "geometry_wkb_xy" if provider in _MYSQL_ORM_PROVIDERS else "geometry_wkb"
    )
    converter = getattr(_native, converter_name, None)
    if converter is None:
        raise RuntimeError("estensione nativa incompatibile con Geometry ORM")
    converted = converter(value.ewkb)
    if not isinstance(converted, bytes):
        raise TypeError("conversione Geometry ORM non ha restituito bytes")
    return converted


def _geometry_parameter_value(value: SpatialReference | None, provider: str) -> Any:
    if value is None:
        if provider in _SQLSERVER_ORM_PROVIDERS:
            return typed_null("varbinary")
        return None
    return _geometry_bind_value(value, provider)


def _mapper(model: type[DeclarativeBase]) -> Mapper:
    mapper = getattr(model, "__mapper__", None)
    if not isinstance(mapper, Mapper):
        raise OrmMappingError("classe non mappata")
    return mapper


def _state(instance: Any) -> _InstanceState:
    current = instance.__dict__.get("_plenora_orm_state")
    if current is None:
        current = _InstanceState()
        instance.__dict__["_plenora_orm_state"] = current
    if not isinstance(current, _InstanceState):
        raise OrmStateError("stato interno dell'istanza non valido")
    return current


def _identity(mapper: Mapper, instance: DeclarativeBase) -> tuple[Any, ...]:
    values: list[Any] = []
    for attribute in mapper.primary_keys:
        name = attribute.name
        if name is None:
            raise OrmMappingError("chiave primaria senza nome")
        value = instance.__dict__.get(name)
        if value is None:
            raise OrmStateError(
                "la chiave primaria deve essere assegnata dal chiamante"
            )
        values.append(value)
    identity = tuple(values)
    try:
        hash(identity)
    except TypeError as error:
        raise OrmStateError("la chiave primaria deve essere hashable") from error
    return identity


def _identity_argument(mapper: Mapper, identity: Any) -> tuple[Any, ...]:
    values = identity if isinstance(identity, tuple) else (identity,)
    if len(values) != len(mapper.primary_keys) or any(
        value is None for value in values
    ):
        raise OrmStateError("l'identita richiesta non coincide con la chiave mappata")
    try:
        hash(values)
    except TypeError as error:
        raise OrmStateError("l'identita richiesta deve essere hashable") from error
    return values


def _row_identity(mapper: Mapper, row: Mapping[str, Any]) -> tuple[Any, ...]:
    primary_names = tuple(attribute.name for attribute in mapper.primary_keys)
    if any(name is None or name not in row for name in primary_names):
        raise OrmMappingError("risultato privo della chiave primaria mappata")
    identity = tuple(row[name] for name in primary_names if name is not None)
    if any(value is None for value in identity):
        raise OrmMappingError("risultato con chiave primaria nulla")
    try:
        hash(identity)
    except TypeError as error:
        raise OrmMappingError("risultato con identita non hashable") from error
    return identity


def _single_primary(mapper: Mapper) -> MappedColumn[Any]:
    if len(mapper.primary_keys) != 1:
        raise OrmUnsupportedError(
            "questa relationship richiede una chiave primaria semplice"
        )
    return mapper.primary_key


def inspect_instance(instance: DeclarativeBase) -> InstanceInspection:
    mapper = _mapper(type(instance))
    state = _state(instance)
    identity = None
    try:
        identity = _identity(mapper, instance)
    except OrmStateError:
        pass
    return InstanceInspection(state.status, identity, tuple(sorted(state.dirty)))


class OrmSession:
    """Unit of work sincrona che possiede una transazione Core."""

    def __init__(
        self,
        session: Any,
        *,
        autoflush: bool = True,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: Any | None = None,
        native_query_policy: str | None = None,
        insert_batch_size: int = 100,
        close_session: bool = False,
    ) -> None:
        begin = getattr(session, "begin", None)
        if not callable(begin):
            raise TypeError("OrmSession richiede una Session Core sincrona")
        capabilities = getattr(session, "capabilities", None)
        provider = (
            capabilities.get("provider") if isinstance(capabilities, Mapping) else None
        )
        if not isinstance(provider, str):
            raise TypeError("Session Core senza provider dichiarato")
        self._provider = provider
        if not isinstance(close_session, bool):
            raise TypeError("close_session deve essere booleano")
        self._session = session
        self._close_session_on_end = close_session
        self._core_session_closed = False
        self._spatial_functions = _session_spatial_functions(capabilities)
        transaction_options = {
            name: value
            for name, value in (
                ("isolation", isolation),
                ("read_only", read_only),
                ("statement_timeout_ms", statement_timeout_ms),
                ("context", context),
                ("native_query_policy", native_query_policy),
            )
            if value is not None
        }
        self._transaction = begin(**transaction_options) if transaction_options else begin()
        self._identity_map: dict[
            tuple[type[DeclarativeBase], tuple[Any, ...]], DeclarativeBase
        ] = {}
        self._pending: list[DeclarativeBase] = []
        self._deleted: list[DeclarativeBase] = []
        self._flushed_deleted: list[DeclarativeBase] = []
        self._deferred_foreign_keys: list[
            tuple[DeclarativeBase, DeclarativeBase, tuple[str, ...]]
        ] = []
        if (
            not isinstance(insert_batch_size, int)
            or isinstance(insert_batch_size, bool)
            or insert_batch_size < 1
        ):
            raise ValueError("insert_batch_size deve essere un intero positivo")
        self._insert_batch_size = insert_batch_size
        self._autoflush_enabled = bool(autoflush)
        self._in_flush = False
        self._listeners: dict[str, list[Any]] = {}
        self._savepoints: dict[str, _SavepointSnapshot] = {}
        self._active = True

    @property
    def is_active(self) -> bool:
        return self._active

    def __enter__(self) -> OrmSession:  # noqa: PYI034
        self._require_active()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if not self._active:
            return False
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False

    def _require_active(self) -> None:
        if not self._active:
            raise OrmStateError("OrmSession non attiva")

    def listen(self, event: str, callback: Any) -> None:
        if event not in _ORM_EVENTS or not callable(callback):
            raise ValueError("hook ORM non valido")
        self._listeners.setdefault(event, []).append(callback)

    def no_autoflush(self) -> _NoAutoflushContext:
        """Disabilita temporaneamente l'autoflush, anche in contesti annidati."""

        return _NoAutoflushContext(self)

    def _emit(self, event: str, instance: DeclarativeBase | None = None) -> None:
        for callback in self._listeners.get(event, ()):
            outcome = (
                callback(self, instance) if instance is not None else callback(self)
            )
            if isawaitable(outcome):
                raise OrmStateError("un hook async richiede AsyncOrmSession")

    def add(self, instance: DeclarativeBase) -> None:
        self._add(instance, set())

    def add_all(self, instances: Iterable[DeclarativeBase]) -> None:
        self._require_active()
        for instance in instances:
            self.add(instance)

    def _add(self, instance: DeclarativeBase, seen: set[int]) -> None:
        self._require_active()
        mapper = _mapper(type(instance))
        if id(instance) in seen:
            return
        seen.add(id(instance))
        state = _state(instance)
        if state.session is not None and state.session is not self:
            raise OrmStateError("istanza gia associata a un'altra OrmSession")
        if state.status is ObjectState.TRANSIENT:
            mapper = _mapper(type(instance))
            state.rollback_snapshot = _snapshot(mapper, instance)
            state.rollback_relationships = _relationship_snapshot(mapper, instance)
            state.rollback_state = ObjectState.TRANSIENT
            state.status = ObjectState.PENDING
            state.session = self
            self._pending.append(instance)
        elif state.status not in {ObjectState.PENDING, ObjectState.PERSISTENT}:
            raise OrmStateError("lo stato dell'istanza non consente add")
        for relation in mapper.relationships:
            relation._validate_configuration()
            if "save-update" not in relation.cascade:
                continue
            for related in _loaded_relationship_values(instance, relation):
                self._add(related, seen)

    def query(self, model: type[T]) -> OrmQuery[T]:
        self._require_active()
        mapper = _mapper(model)  # type: ignore[arg-type]
        self._require_relational_load(mapper)
        statement, parameters = _entity_select(mapper, self._provider)
        return OrmQuery(self, mapper, statement, _fixed_parameters=parameters)

    def bulk_insert(
        self,
        model: type[DeclarativeBase],
        rows: Iterable[Mapping[str, Any]],
    ) -> int:
        self._require_active()
        self._autoflush_now()
        mapper = _mapper(model)
        statement, parameters, count = _bulk_mapping_statement(
            mapper, rows, self._provider
        )
        affected = _affected_rows(self._transaction.execute(statement, parameters))
        if affected != count:
            raise StaleObjectError(
                "INSERT ORM batch non ha interessato il numero atteso di righe"
            )
        return affected

    def bulk_upsert(
        self,
        model: type[DeclarativeBase],
        rows: Iterable[Mapping[str, Any]],
        *,
        conflict_columns: Iterable[str],
        update_values: Mapping[str, Any] | None = None,
    ) -> int:
        self._require_active()
        self._autoflush_now()
        mapper = _mapper(model)
        statement, parameters, _ = _bulk_mapping_statement(
            mapper,
            rows,
            self._provider,
            conflict_columns=conflict_columns,
            update_values=update_values,
        )
        return _affected_rows(self._transaction.execute(statement, parameters))

    def get(self, model: type[T], identity: Any) -> T | None:
        self._require_active()
        self._autoflush_now()
        mapper = _mapper(model)  # type: ignore[arg-type]
        identity_values = _identity_argument(mapper, identity)
        key = (model, identity_values)
        if key in self._identity_map:
            return self._identity_map[key]  # type: ignore[return-value]
        self._require_relational_load(mapper)
        predicate, parameters = _identity_values_predicate(mapper, identity_values)
        statement, fixed_parameters = _entity_select(mapper, self._provider)
        statement = statement.where(predicate).limit(2)
        parameters = _merge_query_parameters(fixed_parameters, parameters)
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM senza risultato relazionale")
        row = result.one_or_none()
        if row is None:
            return None
        instance = self._hydrate(mapper, row)
        if _identity(mapper, instance) != identity_values:
            raise OrmMappingError(
                "risultato con identita diversa dalla chiave richiesta"
            )
        return instance  # type: ignore[return-value]

    def refresh(self, instance: DeclarativeBase) -> None:
        self._require_active()
        self._autoflush_now(exclude=instance)
        mapper = _mapper(type(instance))
        self._require_relational_load(mapper)
        state = _state(instance)
        if state.session is not self or state.status is not ObjectState.PERSISTENT:
            raise OrmStateError("refresh richiede un'istanza persistent della sessione")
        identity = _identity(mapper, instance)
        predicate, parameters = _identity_values_predicate(mapper, identity)
        statement, fixed_parameters = _entity_select(mapper, self._provider)
        statement = statement.where(predicate).limit(2)
        parameters = _merge_query_parameters(fixed_parameters, parameters)
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM senza risultato relazionale")
        row = result.one_or_none()
        if row is None:
            raise NoResultFound("refresh ORM senza riga")
        _validate_geometry_row(mapper, row, self._provider)
        for attribute in mapper.attributes:
            name = attribute.name
            if name is None or name not in row:
                raise OrmMappingError("risultato privo di una colonna mappata")
            instance.__dict__[name] = attribute._coerce(row[name])
        if _identity(mapper, instance) != identity:
            raise OrmMappingError("refresh con identita incoerente")
        state.original = _snapshot(mapper, instance)
        state.dirty.clear()
        state.expired.clear()
        self._emit("refresh", instance)

    def load(
        self,
        instance: DeclarativeBase,
        relation: str | Relationship[T],
    ) -> T | None | MutableSequence[T]:
        self._require_active()
        self._autoflush_now()
        mapper = _mapper(type(instance))
        state = _state(instance)
        if state.session is not self or state.status is not ObjectState.PERSISTENT:
            raise OrmStateError("load richiede un'istanza persistent della sessione")
        descriptor = (
            mapper.relationship(relation) if isinstance(relation, str) else relation
        )
        if descriptor not in mapper.relationships:
            raise OrmMappingError("relationship appartenente a un altro mapper")
        descriptor._validate_configuration()
        related = self._load_relationship(instance, descriptor)
        if descriptor.name is None:
            raise OrmMappingError("relationship senza nome")
        if descriptor.uselist:
            collection = _RelationshipCollection(instance, descriptor)
            for item in related:
                collection._append_from_backref(item)
                if descriptor.back_populates is not None:
                    inverse = _mapper(descriptor.target).relationship(
                        descriptor.back_populates
                    )
                    inverse._assign_from_backref(item, instance)
            instance.__dict__[descriptor.name] = collection
            _remember_relationship(instance, descriptor)
            _capture_loaded_relationship(instance, descriptor)
            return collection
        instance.__dict__[descriptor.name] = related
        if related is not None and descriptor.back_populates is not None:
            inverse = _mapper(descriptor.target).relationship(descriptor.back_populates)
            inverse._assign_from_backref(related, instance)
        _capture_loaded_relationship(instance, descriptor)
        return related

    def merge(self, instance: T, *, load: bool = True) -> T:
        return self._merge(instance, load=load, seen={})

    def _merge(self, instance: T, *, load: bool, seen: dict[int, Any]) -> T:
        self._require_active()
        if id(instance) in seen:
            return seen[id(instance)]
        mapper = _mapper(type(instance))  # type: ignore[arg-type]
        source_state = _state(instance)
        try:
            identity = _identity(mapper, instance)  # type: ignore[arg-type]
        except OrmStateError:
            identity = None
        target = None
        if identity is not None and load:
            target = self.get(
                type(instance), identity[0] if len(identity) == 1 else identity
            )
        if target is None:
            target = mapper.model.__new__(mapper.model)
            target.__dict__["_plenora_orm_state"] = _InstanceState()
            for attribute in mapper.attributes:
                if attribute.name in instance.__dict__:
                    setattr(target, attribute.name, instance.__dict__[attribute.name])
            self.add(target)
        else:
            for attribute in mapper.attributes:
                name = attribute.name
                if (
                    name is not None
                    and name in instance.__dict__
                    and not attribute.primary_key
                    and not attribute.version
                ):
                    setattr(target, name, instance.__dict__[name])
        seen[id(instance)] = target
        for relation in mapper.relationships:
            if relation.name not in instance.__dict__:
                continue
            if "save-update" not in relation.cascade:
                raise OrmStateError(
                    "merge di relationship richiede cascade save-update"
                )
            if relation.uselist:
                setattr(
                    target,
                    relation.name,
                    [
                        self._merge(item, load=load, seen=seen)
                        for item in instance.__dict__[relation.name]
                    ],
                )
            else:
                related = instance.__dict__[relation.name]
                setattr(
                    target,
                    relation.name,
                    None
                    if related is None
                    else self._merge(related, load=load, seen=seen),
                )
        if (
            source_state.session is self
            and source_state.status is ObjectState.PERSISTENT
        ):
            return instance
        return target  # type: ignore[return-value]

    def expunge(self, instance: DeclarativeBase) -> None:
        self._require_active()
        mapper = _mapper(type(instance))
        state = _state(instance)
        if state.session is not self:
            raise OrmStateError("istanza non associata a questa OrmSession")
        if instance in self._pending:
            self._pending.remove(instance)
        if instance in self._deleted:
            self._deleted.remove(instance)
        try:
            self._identity_map.pop((type(instance), _identity(mapper, instance)), None)
        except OrmStateError:
            pass
        state.status = ObjectState.DETACHED
        state.session = None
        state.dirty.clear()
        state.expired.clear()

    def expire(self, instance: DeclarativeBase, *attributes: str) -> None:
        self._require_active()
        mapper = _mapper(type(instance))
        state = _state(instance)
        if state.session is not self or state.status is not ObjectState.PERSISTENT:
            raise OrmStateError("expire richiede un'istanza persistent della sessione")
        names = (
            set(attributes)
            if attributes
            else {item.name for item in mapper.attributes if not item.primary_key}
        )
        mapped = {item.name for item in mapper.attributes}
        if not names <= mapped:
            raise OrmMappingError("expire riferisce un attributo non mappato")
        for name in names:
            if name in state.original:
                instance.__dict__[name] = state.original[name]
            state.dirty.discard(name)
        state.expired.update(name for name in names if name is not None)

    def expire_all(self) -> None:
        self._require_active()
        for instance in tuple(self._identity_map.values()):
            self.expire(instance)

    def _load_relationship(
        self, instance: DeclarativeBase, descriptor: Relationship[Any]
    ) -> Any:
        target_mapper = _mapper(descriptor.target)
        direction = descriptor.direction
        if direction == "many-to-one":
            foreign_value = tuple(
                instance.__dict__.get(name) for name in descriptor.foreign_keys
            )
            return (
                None
                if any(value is None for value in foreign_value)
                else self.get(
                    descriptor.target,
                    foreign_value[0] if len(foreign_value) == 1 else foreign_value,
                )
            )
        owner_mapper = _mapper(type(instance))
        owner_identity = _identity(owner_mapper, instance)
        if direction in {"one-to-many", "one-to-one"}:
            foreign = tuple(
                target_mapper.attribute(name).column for name in descriptor.foreign_keys
            )
            if any(column is None for column in foreign):
                raise OrmMappingError("foreign key relationship senza colonna")
            predicate, parameters = _identity_disjunction(
                tuple(column for column in foreign if column is not None),
                (owner_identity,),
                "orm_relationship",
            )
            rows = self.query(descriptor.target).where(predicate).all(parameters)
            if direction == "one-to-one":
                if len(rows) > 1:
                    raise MultipleResultsFound("relationship one-to-one non univoca")
                return None if not rows else rows[0]
            return rows
        secondary = descriptor.secondary
        if secondary is None:
            raise OrmMappingError("relationship many-to-many senza secondary")
        local_columns = _secondary_columns(descriptor, remote=False)
        remote_columns = _secondary_columns(descriptor, remote=True)
        target_primary = _primary_columns(target_mapper)
        predicate, parameters = _identity_disjunction(
            local_columns, (owner_identity,), "orm_relationship"
        )
        statement = (
            select(*_orm_projections(target_mapper, provider=self._provider))
            .select_from(target_mapper.table)
            .join(
                secondary,
                _column_equality(remote_columns, target_primary),
            )
            .where(predicate)
        )
        return self._execute_entities(target_mapper, statement, parameters)

    def delete(self, instance: DeclarativeBase) -> None:
        self._delete_graph(instance, set())

    def _delete_graph(self, instance: DeclarativeBase, seen: set[int]) -> None:
        self._require_active()
        mapper = _mapper(type(instance))
        if id(instance) in seen:
            return
        seen.add(id(instance))
        state = _state(instance)
        if state.session is not self:
            raise OrmStateError("istanza non associata a questa OrmSession")
        for relation in mapper.relationships:
            if "delete" not in relation.cascade:
                continue
            if relation.passive_deletes:
                _require_database_delete_cascade(mapper, relation)
                continue
            if relation.name not in instance.__dict__:
                raise OrmStateError("cascade delete richiede una relazione caricata")
            for related in _loaded_relationship_values(instance, relation):
                self._delete_graph(related, seen)
        if state.status is ObjectState.PENDING:
            self._pending.remove(instance)
            state.status = ObjectState.TRANSIENT
            state.session = None
            return
        if state.status is not ObjectState.PERSISTENT:
            raise OrmStateError("lo stato dell'istanza non consente delete")
        state.status = ObjectState.DELETED
        self._deleted.append(instance)

    def flush(self) -> None:
        self._require_active()
        if self._in_flush:
            return
        self._in_flush = True
        try:
            self._emit("before_flush")
            dirty = self._dirty_instances()
            pending = self._pending_insert_order()
            self._preflight((*self._pending, *dirty, *self._deleted))
            position = 0
            while position < len(pending):
                instance = pending[position]
                self._synchronize_relationships(instance)
                self._preflight((instance,))
                signature = self._pending_batch_signature(instance)
                batch = [instance]
                batch_limit = _insert_batch_limit(
                    signature, self._provider, self._insert_batch_size
                )
                if signature is not None:
                    while (
                        position + len(batch) < len(pending)
                        and len(batch) < batch_limit
                    ):
                        candidate = pending[position + len(batch)]
                        self._synchronize_relationships(candidate)
                        self._preflight((candidate,))
                        if self._pending_batch_signature(candidate) != signature:
                            break
                        batch.append(candidate)
                if len(batch) > 1:
                    self._insert_batch(batch)
                else:
                    self._insert(instance)
                position += len(batch)
            for instance, related, foreign_keys in self._deferred_foreign_keys:
                identity = _identity(_mapper(type(related)), related)
                for foreign_key, value in zip(foreign_keys, identity, strict=True):
                    instance.__dict__[foreign_key] = value
                    _state(instance).dirty.add(foreign_key)
                self._update(instance)
            for instance in dirty:
                if _state(instance).status is ObjectState.PERSISTENT:
                    self._update(instance)
            self._flush_many_to_many()
            self._remove_deleted_associations()
            for instance in tuple(self._deleted):
                self._delete(instance)
            self._emit("after_flush")
        except BaseException:
            try:
                if getattr(self._transaction, "is_active", True):
                    self._transaction.rollback()
            finally:
                self._detach_all(restore=True)
                self._active = False
                self._emit("after_rollback")
            raise
        finally:
            self._in_flush = False

    def commit(self) -> None:
        self._require_active()
        try:
            self.flush()
            self._transaction.commit()
        except BaseException:
            if getattr(self._transaction, "is_active", True):
                self._transaction.rollback()
            self._detach_all(restore=True)
            self._active = False
            try:
                self._close_owned_session()
            except BaseException:  # la chiusura non maschera l'errore originale
                pass
            raise
        self._detach_all(restore=False)
        self._active = False
        try:
            self._emit("after_commit")
        finally:
            self._close_owned_session()

    def savepoint(self, name: str) -> None:
        self._require_active()
        _validate_savepoint_name(name)
        if name in self._savepoints:
            raise OrmStateError("savepoint ORM gia attivo")
        self.flush()
        operation = getattr(self._transaction, "savepoint", None)
        if not callable(operation):
            raise OrmUnsupportedError("la transazione Core non espone savepoint")
        operation(name)
        self._savepoints[name] = _capture_savepoint(self)

    def rollback_to_savepoint(self, name: str) -> None:
        self._require_active()
        snapshot = self._savepoints.get(name)
        if snapshot is None:
            raise OrmStateError("savepoint ORM non attivo")
        operation = getattr(self._transaction, "rollback_to_savepoint", None)
        if not callable(operation):
            raise OrmUnsupportedError("la transazione Core non espone savepoint")
        operation(name)
        _restore_savepoint(self, snapshot)
        names = tuple(self._savepoints)
        position = names.index(name)
        for nested in names[position + 1 :]:
            self._savepoints.pop(nested, None)

    def release_savepoint(self, name: str) -> None:
        self._require_active()
        if name not in self._savepoints:
            raise OrmStateError("savepoint ORM non attivo")
        operation = getattr(self._transaction, "release_savepoint", None)
        if not callable(operation):
            raise OrmUnsupportedError("la transazione Core non espone savepoint")
        operation(name)
        self._savepoints.pop(name)

    def begin_nested(self, name: str) -> _OrmNestedTransaction:
        self._require_active()
        return _OrmNestedTransaction(self, name)

    def rollback(self) -> None:
        self._require_active()
        try:
            self._transaction.rollback()
        finally:
            self._detach_all(restore=True)
            self._active = False
            try:
                self._emit("after_rollback")
            finally:
                self._close_owned_session()

    def close(self) -> None:
        if self._active:
            self.rollback()
        else:
            self._close_owned_session()

    def _close_owned_session(self) -> None:
        if not self._close_session_on_end or self._core_session_closed:
            return
        close = getattr(self._session, "close", None)
        if not callable(close):
            raise TypeError("sessione Core priva di close")
        close()
        self._core_session_closed = True

    def _autoflush_now(self, *, exclude: DeclarativeBase | None = None) -> None:
        if not self._autoflush_enabled or self._in_flush:
            return
        has_work = bool(self._pending or self._deleted or self._dirty_instances())
        if not has_work:
            return
        excluded_dirty: set[str] | None = None
        if exclude is not None:
            excluded_dirty = set(_state(exclude).dirty)
            _state(exclude).dirty.clear()
        try:
            if self._pending or self._deleted or self._dirty_instances():
                self.flush()
        finally:
            if excluded_dirty is not None and self._active:
                _state(exclude).dirty.update(excluded_dirty)

    def _require_relational_load(self, mapper: Mapper) -> None:
        _require_geometry_mapper(mapper, self._provider)

    def _execute_entities(
        self,
        mapper: Mapper,
        statement: SelectStatement,
        parameters: Mapping[str, Any] | None,
        loaders: tuple[LoaderOption, ...] = (),
    ) -> list[Any]:
        self._require_active()
        self._autoflush_now()
        _validate_spatial_statement(statement, self._spatial_functions)
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM senza risultato relazionale")
        rows = result.all()
        row_instances = [self._hydrate(mapper, row) for row in rows]
        joined_collection = any(
            loader.strategy == "joined"
            and _query_loader_path(mapper, loader.relationship)[0].uselist
            for loader in loaders
        )
        if joined_collection:
            instances = list(dict.fromkeys(row_instances))
        else:
            instances = row_instances
        for loader in loaders:
            relation = _query_loader_path(mapper, loader.relationship)[0]
            if loader.strategy == "joined":
                for instance, row in zip(row_instances, rows, strict=True):
                    self._hydrate_joined(instance, relation, row)
            else:
                self._selectin_load(instances, relation)
        for loader in loaders:
            path = _query_loader_path(mapper, loader.relationship)
            self._load_nested_path(instances, path)
        return instances

    def _execute_scalar_query(
        self,
        statement: SelectStatement,
        parameters: Mapping[str, Any] | None,
    ) -> Any:
        self._require_active()
        self._autoflush_now()
        _validate_spatial_statement(statement, self._spatial_functions)
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("query scalare ORM senza risultato relazionale")
        return result.scalar_one()

    def _execute_exists_query(
        self,
        statement: SelectStatement,
        parameters: Mapping[str, Any] | None,
    ) -> bool:
        self._require_active()
        self._autoflush_now()
        _validate_spatial_statement(statement, self._spatial_functions)
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("EXISTS ORM senza risultato relazionale")
        return result.first() is not None

    def _execute_bulk_update(
        self,
        mapper: Mapper,
        query: SelectStatement,
        values: Mapping[str, Any],
        parameters: Mapping[str, Any] | None,
    ) -> int:
        self._require_active()
        self._autoflush_now()
        statement, mutation_parameters = _bulk_update_statement(
            mapper, query, values, self._provider
        )
        parameters = _geometry_query_parameters(query, parameters, self._provider)
        merged = _merge_query_parameters(parameters, mutation_parameters)
        _validate_spatial_statement(statement, self._spatial_functions)  # type: ignore[arg-type]
        affected = _affected_rows(self._transaction.execute(statement, merged))
        self._expire_mapper_instances(mapper, detach=False)
        return affected

    def _execute_bulk_delete(
        self,
        mapper: Mapper,
        query: SelectStatement,
        parameters: Mapping[str, Any] | None,
    ) -> int:
        self._require_active()
        self._autoflush_now()
        statement = _bulk_delete_statement(mapper, query)
        _validate_spatial_statement(statement, self._spatial_functions)  # type: ignore[arg-type]
        parameters = _geometry_query_parameters(query, parameters, self._provider)
        affected = _affected_rows(self._transaction.execute(statement, parameters))
        self._expire_mapper_instances(mapper, detach=True)
        return affected

    def _expire_mapper_instances(self, mapper: Mapper, *, detach: bool) -> None:
        root = _inheritance_root(mapper)
        for instance in tuple(dict.fromkeys(self._identity_map.values())):
            instance_mapper = _mapper(type(instance))
            if _inheritance_root(instance_mapper) is not root:
                continue
            if detach:
                self.expunge(instance)
            else:
                self.expire(instance)

    def _load_nested_path(
        self,
        instances: list[DeclarativeBase],
        path: tuple[Relationship[Any], ...],
    ) -> None:
        if len(path) < 2:
            return
        related = _loaded_relationship_instances(instances, path[0])
        for relation in path[1:]:
            self._selectin_load(related, relation)
            related = _loaded_relationship_instances(related, relation)

    def _hydrate_joined(
        self,
        instance: DeclarativeBase,
        relation: Relationship[Any],
        row: Mapping[str, Any],
    ) -> None:
        target_mapper = _mapper(relation.target)
        prefix = _loader_prefix(relation)
        primary_names = tuple(item.name for item in target_mapper.primary_keys)
        if any(name is None for name in primary_names):
            raise OrmMappingError("target eager privo di chiave")
        identity_values = tuple(
            row.get(f"{prefix}{name}") for name in primary_names if name is not None
        )
        related = None
        if all(value is not None for value in identity_values):
            values = _mapped_row_values(target_mapper, row, prefix, self._provider)
            related = self._hydrate(target_mapper, values)
        if relation.uselist:
            _append_joined_relationship(instance, relation, related)
        else:
            _assign_loaded_relationship(instance, relation, related)

    def _selectin_load(
        self,
        instances: list[DeclarativeBase],
        relation: Relationship[Any],
    ) -> None:
        if not instances:
            return
        target_mapper = _mapper(relation.target)
        direction = relation.direction
        if direction == "many-to-one":
            values = tuple(
                dict.fromkeys(
                    tuple(instance.__dict__.get(name) for name in relation.foreign_keys)
                    for instance in instances
                    if all(
                        instance.__dict__.get(name) is not None
                        for name in relation.foreign_keys
                    )
                )
            )
            if not values:
                for instance in instances:
                    _assign_loaded_relationship(instance, relation, None)
                return
            columns = tuple(item.column for item in target_mapper.primary_keys)
            if any(column is None for column in columns):
                raise OrmMappingError("selectinload privo di chiave primaria")
            predicate, parameters = _identity_disjunction(
                tuple(column for column in columns if column is not None),
                values,
                "orm_eager",
            )
            related = self._execute_entities(
                target_mapper,
                select(*_orm_projections(target_mapper, provider=self._provider)).where(
                    predicate
                ),
                parameters,
            )
            by_identity = {_identity(target_mapper, item): item for item in related}
            for instance in instances:
                identity = tuple(
                    instance.__dict__.get(name) for name in relation.foreign_keys
                )
                _assign_loaded_relationship(
                    instance,
                    relation,
                    by_identity.get(identity),
                )
            return
        owner_mapper = _mapper(type(instances[0]))
        owner_identities = tuple(_identity(owner_mapper, item) for item in instances)
        if direction in {"one-to-many", "one-to-one"}:
            foreign = tuple(
                target_mapper.attribute(name).column for name in relation.foreign_keys
            )
            if any(column is None for column in foreign):
                raise OrmMappingError("selectinload privo di foreign key")
            predicate, parameters = _identity_disjunction(
                tuple(column for column in foreign if column is not None),
                owner_identities,
                "orm_eager",
            )
            related = self._execute_entities(
                target_mapper,
                select(*_orm_projections(target_mapper, provider=self._provider)).where(
                    predicate
                ),
                parameters,
            )
            grouped: dict[Any, list[DeclarativeBase]] = {
                key: [] for key in owner_identities
            }
            for item in related:
                key = tuple(item.__dict__.get(name) for name in relation.foreign_keys)
                grouped.setdefault(key, []).append(item)
            for instance, identity in zip(instances, owner_identities, strict=True):
                values = grouped.get(identity, [])
                if direction == "one-to-one" and len(values) > 1:
                    raise MultipleResultsFound("relationship one-to-one non univoca")
                _assign_loaded_relationship(
                    instance,
                    relation,
                    (None if not values else values[0])
                    if direction == "one-to-one"
                    else values,
                )
            return
        self._selectin_many_to_many(instances, relation, owner_identities)

    def _selectin_entities(
        self,
        mapper: Mapper,
        column: Column | None,
        values: tuple[Any, ...],
    ) -> list[DeclarativeBase]:
        if not values:
            return []
        if column is None:
            raise OrmMappingError("selectinload privo di colonna")
        parameters = {f"orm_eager_{index}": value for index, value in enumerate(values)}
        predicate = column.in_(
            *(bind(name, _bind_type_for_value(value)) for name, value in parameters.items())
        )
        statement = select(*_orm_projections(mapper, provider=self._provider)).where(
            predicate
        )
        return self._execute_entities(mapper, statement, parameters)

    def _selectin_many_to_many(
        self,
        instances: list[DeclarativeBase],
        relation: Relationship[Any],
        owner_ids: tuple[tuple[Any, ...], ...],
    ) -> None:
        secondary = relation.secondary
        target_mapper = _mapper(relation.target)
        if secondary is None:
            raise OrmMappingError("many-to-many non configurata")
        local_columns = _secondary_columns(relation, remote=False)
        remote_columns = _secondary_columns(relation, remote=True)
        predicate, parameters = _identity_disjunction(
            local_columns, owner_ids, "orm_eager"
        )
        owner_projections = tuple(
            column.label(f"orm_eager_owner_{index}")
            for index, column in enumerate(local_columns)
        )
        statement = (
            select(
                *_orm_projections(target_mapper, provider=self._provider),
                *owner_projections,
            )
            .select_from(target_mapper.table)
            .join(
                secondary,
                _column_equality(remote_columns, _primary_columns(target_mapper)),
            )
            .where(predicate)
        )
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM eager senza risultato relazionale")
        grouped: dict[Any, list[DeclarativeBase]] = {key: [] for key in owner_ids}
        for row in result.all():
            owner_identity = tuple(
                row[f"orm_eager_owner_{index}"] for index in range(len(local_columns))
            )
            grouped.setdefault(owner_identity, []).append(
                self._hydrate(target_mapper, row)
            )
        for instance, identity in zip(instances, owner_ids, strict=True):
            _assign_loaded_relationship(instance, relation, grouped.get(identity, []))

    def _hydrate(
        self, mapper: Mapper, row: Mapping[str, Any], *, emit: bool = True
    ) -> DeclarativeBase:
        row = _normalize_geometry_row(mapper, row, self._provider)
        _validate_geometry_row(mapper, row, self._provider)
        if mapper.inherits is None and mapper.polymorphic_on is not None:
            discriminator = row.get(mapper.polymorphic_on)
            mapper = mapper.model.__registry__.polymorphic_mapper(mapper, discriminator)
        identity = _row_identity(mapper, row)
        key = (mapper.model, identity)
        existing = self._identity_map.get(key)
        if existing is not None:
            return existing
        instance = mapper.model.__new__(mapper.model)
        instance.__dict__["_plenora_orm_state"] = _InstanceState()
        for attribute in mapper.attributes:
            name = attribute.name
            if name is None or name not in row:
                raise OrmMappingError("risultato privo di una colonna mappata")
            setattr(instance, name, row[name])
        state = _state(instance)
        state.status = ObjectState.PERSISTENT
        state.session = self
        state.original = _snapshot(mapper, instance)
        state.rollback_snapshot = dict(state.original)
        state.rollback_relationships = {}
        state.rollback_state = ObjectState.PERSISTENT
        self._identity_map[key] = instance
        if emit:
            self._emit("load", instance)
        return instance

    def _dirty_instances(self) -> tuple[DeclarativeBase, ...]:
        seen: set[int] = set()
        values: list[DeclarativeBase] = []
        for instance in self._identity_map.values():
            state = _state(instance)
            if state.dirty and id(instance) not in seen:
                seen.add(id(instance))
                values.append(instance)
        return tuple(values)

    def _pending_insert_order(self) -> tuple[DeclarativeBase, ...]:
        pending = tuple(self._pending)
        self._deferred_foreign_keys.clear()
        by_id = {id(instance): instance for instance in pending}
        dependencies: dict[int, set[int]] = {key: set() for key in by_id}
        edges: dict[
            tuple[int, int],
            tuple[DeclarativeBase, DeclarativeBase, tuple[str, ...]],
        ] = {}
        for instance in pending:
            mapper = _mapper(type(instance))
            for relation in mapper.relationships:
                relation._validate_configuration()
                if relation.name is None or relation.name not in instance.__dict__:
                    continue
                for related in _loaded_relationship_values(instance, relation):
                    state = _state(related)
                    if state.session is not self:
                        raise OrmStateError(
                            "relationship punta a un oggetto non aggiunto alla OrmSession"
                        )
                    if state.status is ObjectState.PENDING:
                        if relation.direction == "many-to-one":
                            dependencies[id(instance)].add(id(related))
                            edges[(id(instance), id(related))] = (
                                instance,
                                related,
                                relation.foreign_keys,
                            )
                        elif relation.direction in {"one-to-many", "one-to-one"}:
                            dependencies[id(related)].add(id(instance))
                            edges[(id(related), id(instance))] = (
                                related,
                                instance,
                                relation.foreign_keys,
                            )
                    elif state.status is not ObjectState.PERSISTENT:
                        raise OrmStateError(
                            "relationship punta a un oggetto in stato non persistibile"
                        )
        ordered: list[DeclarativeBase] = []
        remaining = dict(dependencies)
        while remaining:
            ready = tuple(key for key, values in remaining.items() if not values)
            if not ready:
                deferred = None
                for child_id, parents in remaining.items():
                    for parent_id in parents:
                        child, parent, foreign_key = edges[(child_id, parent_id)]
                        if all(
                            _mapper(type(child)).attribute(name).nullable
                            for name in foreign_key
                        ):
                            deferred = (child_id, parent_id, child, parent, foreign_key)
                            break
                    if deferred is not None:
                        break
                if deferred is None:
                    raise OrmUnsupportedError(
                        "ciclo FK non nullable non inseribile senza vincoli differibili"
                    )
                child_id, parent_id, child, parent, foreign_key = deferred
                remaining[child_id].remove(parent_id)
                self._deferred_foreign_keys.append((child, parent, foreign_key))
                continue
            for key in ready:
                ordered.append(by_id[key])
                remaining.pop(key)
            for values in remaining.values():
                values.difference_update(ready)
        return tuple(ordered)

    def _synchronize_relationships(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        for relation in mapper.relationships:
            if relation.name is None or relation.name not in instance.__dict__:
                continue
            for related in _loaded_relationship_values(instance, relation):
                if relation.direction == "many-to-one":
                    identity = _identity(_mapper(relation.target), related)
                    for foreign_key, value in zip(
                        relation.foreign_keys, identity, strict=True
                    ):
                        setattr(instance, foreign_key, value)
                elif relation.direction in {"one-to-many", "one-to-one"}:
                    identity = _identity(mapper, instance)
                    for foreign_key, value in zip(
                        relation.foreign_keys, identity, strict=True
                    ):
                        setattr(related, foreign_key, value)

    def _flush_many_to_many(self) -> None:
        seen: set[tuple[Any, ...]] = set()
        for instance in tuple(self._identity_map.values()):
            mapper = _mapper(type(instance))
            state = _state(instance)
            for relation in mapper.relationships:
                if relation.secondary is None or relation.name not in instance.__dict__:
                    continue
                local_value = _identity(mapper, instance)
                current = {
                    _identity(_mapper(relation.target), related)
                    for related in _loaded_relationship_values(instance, relation)
                }
                original = set(state.relationship_original.get(relation.name or "", ()))
                for remote_identity in current - original:
                    remote_value = remote_identity
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    assignments, parameters = _association_values(
                        relation, local_value, remote_value
                    )
                    self._transaction.execute(
                        insert(relation.secondary).values(**assignments), parameters
                    )
                for remote_identity in original - current:
                    remote_value = remote_identity
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    predicate, parameters = _association_predicate(
                        relation, local_value, remote_value
                    )
                    self._transaction.execute(
                        delete(relation.secondary).where(predicate),
                        parameters,
                    )
                _remember_relationship(instance, relation)

    def _remove_deleted_associations(self) -> None:
        seen: set[tuple[Any, ...]] = set()
        for instance in self._deleted:
            mapper = _mapper(type(instance))
            local_value = _identity(mapper, instance)
            for relation in mapper.relationships:
                if relation.secondary is None:
                    continue
                signature = (
                    id(relation.secondary),
                    *sorted(
                        zip(
                            relation.secondary_local_keys,
                            local_value,
                            strict=True,
                        ),
                        key=lambda item: item[0],
                    ),
                )
                if signature in seen:
                    continue
                seen.add(signature)
                predicate, parameters = _association_owner_predicate(
                    relation, local_value
                )
                self._transaction.execute(
                    delete(relation.secondary).where(predicate), parameters
                )

    def _preflight(self, instances: tuple[DeclarativeBase, ...]) -> None:
        for instance in instances:
            mapper = _mapper(type(instance))
            state = _state(instance)
            if (
                state.status is ObjectState.PENDING
                and mapper.polymorphic_on is not None
            ):
                current = instance.__dict__.get(mapper.polymorphic_on)
                if current is None:
                    instance.__dict__[mapper.polymorphic_on] = (
                        mapper.polymorphic_identity
                    )
                elif current != mapper.polymorphic_identity:
                    raise OrmStateError("discriminatore polimorfico incoerente")
            missing_primary = tuple(
                attribute
                for attribute in mapper.primary_keys
                if attribute.name is None
                or instance.__dict__.get(attribute.name) is None
            )
            if not (
                state.status is ObjectState.PENDING
                and missing_primary
                and all(attribute.generated for attribute in missing_primary)
            ):
                _identity(mapper, instance)
            server_fed = tuple(
                attribute
                for attribute in mapper.attributes
                if attribute.name not in instance.__dict__
                and (attribute.generated or attribute.server_default)
            )
            if (
                state.status is ObjectState.PENDING
                and server_fed
                and self._provider
                not in {"postgres", "mariadb", "sqlserver", "mysql", "db2"}
            ):
                raise OrmUnsupportedError(
                    "generated key e server default ORM non qualificati per il provider"
                )
            for attribute in mapper.attributes:
                name = attribute.name
                if name is None:
                    raise OrmMappingError("attributo mappato senza nome")
                value = instance.__dict__.get(name)
                if (
                    value is None
                    and not attribute.nullable
                    and not (
                        state.status is ObjectState.PENDING
                        and (
                            attribute.version
                            or attribute.generated
                            or attribute.server_default
                            or any(
                                name in relation.foreign_keys
                                and relation.name in instance.__dict__
                                and instance.__dict__[relation.name] is not None
                                for relation in mapper.relationships
                            )
                        )
                    )
                ):
                    raise OrmStateError("manca una colonna non nullable")
                if (
                    attribute.version
                    and value is not None
                    and (
                        not isinstance(value, int)
                        or isinstance(value, bool)
                        or value < 1
                    )
                ):
                    raise OrmStateError("versione ottimistica non valida")
                if (
                    isinstance(attribute.type_, Geometry)
                    and value is not None
                    and (state.status is ObjectState.PENDING or name in state.dirty)
                ):
                    _require_geometry_mapping(attribute.type_, self._provider)

    def _insert(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        if mapper.inheritance == "joined":
            self._insert_joined(instance, mapper)
            return
        self._emit("before_insert", instance)
        version = mapper.version
        if version is not None and version.name not in instance.__dict__:
            instance.__dict__[version.name] = 1
        assignments: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        returning: list[Column] = []
        for index, attribute in enumerate(mapper.attributes):
            name = attribute.name
            if any(
                deferred is instance and name in foreign_keys
                for deferred, _, foreign_keys in self._deferred_foreign_keys
            ):
                continue
            if name is not None and name in instance.__dict__:
                bind_name = f"orm_insert_{index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and (
                    value is not None
                    or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
                ):
                    assignments[name] = _spatial_value(
                        bind(bind_name, BindType.BINARY),
                        attribute.type_.srid,
                        attribute.type_.semantics,
                    )
                    parameters[bind_name] = _geometry_parameter_value(
                        value, self._provider
                    )
                else:
                    assignments[name] = _attribute_bind(attribute, bind_name)
                    parameters[bind_name] = _attribute_parameter(attribute, value)
            elif (
                attribute.generated or attribute.server_default
            ) and attribute.column is not None:
                returning.append(attribute.column)
        statement = insert(mapper.table).values(**assignments)
        uses_returning = (
            bool(returning) and self._provider in _INSERT_RETURNING_PROVIDERS
        )
        if uses_returning:
            statement = statement.returning(*returning)
        outcome = self._transaction.execute(statement, parameters)
        if uses_returning:
            if not isinstance(outcome, Result):
                raise OrmStateError(
                    "INSERT ORM con default senza risultato relazionale"
                )
            row = outcome.one()
            for attribute in mapper.attributes:
                if any(attribute.column is column for column in returning):
                    name = attribute.name
                    if name is None or name not in row:
                        raise OrmMappingError(
                            "INSERT non ha restituito una colonna richiesta"
                        )
                    instance.__dict__[name] = attribute._coerce(row[name])
        else:
            _require_one_row(outcome)
            if returning:
                self._hydrate_post_insert_defaults(mapper, instance, returning)
        self._mark_inserted(instance, mapper)
        self._emit("after_insert", instance)

    def _pending_batch_signature(
        self, instance: DeclarativeBase
    ) -> tuple[type[DeclarativeBase], tuple[str, ...]] | None:
        mapper = _mapper(type(instance))
        if (
            mapper.inheritance == "joined"
            or mapper.relationships
            or self._listeners.get("before_insert")
        ):
            return None
        version = mapper.version
        if version is not None and version.name not in instance.__dict__:
            instance.__dict__[version.name] = 1
        deferred = {
            name
            for owner, _, names in self._deferred_foreign_keys
            if owner is instance
            for name in names
        }
        names = tuple(
            attribute.name
            for attribute in mapper.attributes
            if attribute.name is not None and attribute.name in instance.__dict__
        )
        if deferred.intersection(names) or any(
            attribute.name not in instance.__dict__
            for attribute in mapper.attributes
            if attribute.generated or attribute.server_default
        ):
            return None
        return mapper.model, names

    def _insert_batch(self, instances: list[DeclarativeBase]) -> None:
        mapper = _mapper(type(instances[0]))
        statement = insert(mapper.table)
        parameters: dict[str, Any] = {}
        for instance in instances:
            self._emit("before_insert", instance)
        self._preflight(tuple(instances))
        for row_index, instance in enumerate(instances):
            assignments: dict[str, Any] = {}
            for column_index, attribute in enumerate(mapper.attributes):
                name = attribute.name
                if name is None or name not in instance.__dict__:
                    continue
                bind_name = f"orm_insert_batch_{row_index}_{column_index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and (
                    value is not None
                    or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
                ):
                    assignments[name] = _spatial_value(
                        bind(bind_name, BindType.BINARY),
                        attribute.type_.srid,
                        attribute.type_.semantics,
                    )
                    parameters[bind_name] = _geometry_parameter_value(
                        value, self._provider
                    )
                else:
                    assignments[name] = _attribute_bind(attribute, bind_name)
                    parameters[bind_name] = _attribute_parameter(attribute, value)
            statement = statement.values(**assignments)
        outcome = self._transaction.execute(statement, parameters)
        _require_row_count(outcome, len(instances))
        for instance in instances:
            self._mark_inserted(instance, mapper)
            self._emit("after_insert", instance)

    def _mark_inserted(self, instance: DeclarativeBase, mapper: Mapper) -> None:
        state = _state(instance)
        state.status = ObjectState.PERSISTENT
        state.original = _snapshot(mapper, instance)
        state.dirty.clear()
        key = (type(instance), _identity(mapper, instance))
        existing = self._identity_map.get(key)
        if existing is not None and existing is not instance:
            raise OrmStateError("identity map contiene gia la chiave")
        self._identity_map[key] = instance
        self._pending.remove(instance)
        self._propagate_primary_key(instance)

    def _insert_joined(self, instance: DeclarativeBase, mapper: Mapper) -> None:
        lineage = _inheritance_lineage(mapper)
        if len(lineage) < 2:
            raise OrmMappingError("mapper joined privo di base")
        self._emit("before_insert", instance)
        if mapper.version is not None and mapper.version.name not in instance.__dict__:
            instance.__dict__[mapper.version.name] = 1
        for index, fragment in enumerate(lineage):
            attributes = (
                fragment.attributes
                if index == 0
                else (*mapper.primary_keys, *fragment.local_attributes)
            )
            self._insert_fragment(
                mapper, fragment.table, attributes, instance, f"level_{index}"
            )
        self._mark_inserted(instance, mapper)
        self._emit("after_insert", instance)

    def _insert_fragment(
        self,
        mapper: Mapper,
        table: Table,
        attributes: tuple[MappedColumn[Any], ...],
        instance: DeclarativeBase,
        role: str,
    ) -> None:
        assignments: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        returning: list[MappedColumn[Any]] = []
        for index, attribute in enumerate(attributes):
            name = attribute.name
            if name is None:
                raise OrmMappingError("attributo mappato senza nome")
            if name in instance.__dict__:
                bind_name = f"orm_insert_{role}_{index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and (
                    value is not None
                    or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
                ):
                    assignments[name] = _spatial_value(
                        bind(bind_name, BindType.BINARY),
                        attribute.type_.srid,
                        attribute.type_.semantics,
                    )
                    parameters[bind_name] = _geometry_parameter_value(
                        value, self._provider
                    )
                else:
                    assignments[name] = _attribute_bind(attribute, bind_name)
                    parameters[bind_name] = _attribute_parameter(attribute, value)
            elif attribute.generated or attribute.server_default:
                returning.append(attribute)
        statement = insert(table).values(**assignments)
        uses_returning = (
            bool(returning) and self._provider in _INSERT_RETURNING_PROVIDERS
        )
        if uses_returning:
            columns = tuple(table.c[attribute.name or ""] for attribute in returning)
            statement = statement.returning(*columns)
        outcome = self._transaction.execute(statement, parameters)
        if uses_returning:
            if not isinstance(outcome, Result):
                raise OrmStateError(
                    "INSERT ORM con default senza risultato relazionale"
                )
            row = outcome.one()
            for attribute in returning:
                name = attribute.name
                if name is None or name not in row:
                    raise OrmMappingError(
                        "INSERT non ha restituito una colonna richiesta"
                    )
                instance.__dict__[name] = attribute._coerce(row[name])
        else:
            _require_one_row(outcome)
            if returning:
                self._hydrate_fragment_defaults(mapper, table, instance, returning)

    def _hydrate_fragment_defaults(
        self,
        mapper: Mapper,
        table: Table,
        instance: DeclarativeBase,
        attributes: list[MappedColumn[Any]],
    ) -> None:
        generated = tuple(
            attribute
            for attribute in mapper.primary_keys
            if attribute in attributes
            and attribute.generated
            and attribute.name is not None
            and instance.__dict__.get(attribute.name) is None
        )
        if generated:
            scalar = getattr(self._transaction, "execute_scalar", None)
            if not callable(scalar):
                raise OrmUnsupportedError(
                    "il provider richiede execute_scalar per leggere l'identita generated"
                )
            if self._provider == "mysql":
                value = scalar("SELECT LAST_INSERT_ID()")
            elif self._provider == "db2":
                value = scalar("VALUES CAST(IDENTITY_VAL_LOCAL() AS INTEGER)")
            else:
                raise OrmUnsupportedError("identita generated non qualificata")
            generated[0].__set__(instance, value)
        identity = _identity(mapper, instance)
        predicate, parameters = _table_identity_predicate(mapper, table, identity)
        columns = tuple(table.c[attribute.name or ""] for attribute in attributes)
        result = self._transaction.execute(
            select(*columns).select_from(table).where(predicate).limit(2), parameters
        )
        if not isinstance(result, Result):
            raise OrmStateError("lettura default ORM senza risultato relazionale")
        row = result.one()
        for attribute in attributes:
            name = attribute.name
            if name is None or name not in row:
                raise OrmMappingError("lettura default priva di una colonna richiesta")
            instance.__dict__[name] = attribute._coerce(row[name])

    def _hydrate_post_insert_defaults(
        self,
        mapper: Mapper,
        instance: DeclarativeBase,
        attributes: list[Column],
    ) -> None:
        generated = tuple(
            attribute
            for attribute in mapper.primary_keys
            if attribute.generated
            and attribute.name is not None
            and instance.__dict__.get(attribute.name) is None
        )
        if generated:
            scalar = getattr(self._transaction, "execute_scalar", None)
            if not callable(scalar):
                raise OrmUnsupportedError(
                    "il provider richiede execute_scalar per leggere l'identita generated"
                )
            if self._provider == "mysql":
                value = scalar("SELECT LAST_INSERT_ID()")
            elif self._provider == "db2":
                # Db2 espone IDENTITY_VAL_LOCAL come DECIMAL e il bordo ODBC
                # conserva i DECIMAL come testo. La colonna ORM `int` genera
                # INTEGER, quindi il cast mantiene il tipo senza rendere
                # `_coerce` permissivo verso stringhe numeriche applicative.
                value = scalar("VALUES CAST(IDENTITY_VAL_LOCAL() AS INTEGER)")
            else:
                raise OrmUnsupportedError("identita generated non qualificata")
            generated[0].__set__(instance, value)
        identity = _identity(mapper, instance)
        predicate, parameters = _identity_values_predicate(mapper, identity)
        statement = select(*attributes).where(predicate).limit(2)
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("lettura default ORM senza risultato relazionale")
        row = result.one()
        for attribute in mapper.attributes:
            if any(attribute.column is column for column in attributes):
                name = attribute.name
                if name is None or name not in row:
                    raise OrmMappingError(
                        "lettura default priva di una colonna richiesta"
                    )
                instance.__dict__[name] = attribute._coerce(row[name])

    def _propagate_primary_key(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        identity = _identity(mapper, instance)
        for relation in mapper.relationships:
            if relation.direction not in {"one-to-many", "one-to-one"}:
                continue
            for related in _loaded_relationship_values(instance, relation):
                for foreign_key, value in zip(
                    relation.foreign_keys, identity, strict=True
                ):
                    setattr(related, foreign_key, value)

    def _update(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        if mapper.inheritance == "joined":
            self._update_joined(instance, mapper)
            return
        state = _state(instance)
        names = tuple(sorted(state.dirty))
        if not names:
            return
        self._emit("before_update", instance)
        assignments: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        for index, name in enumerate(names):
            bind_name = f"orm_update_{index}"
            attribute = mapper.attribute(name)
            value = instance.__dict__[name]
            if isinstance(attribute.type_, Geometry) and (
                value is not None or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
            ):
                assignments[name] = _spatial_value(
                    bind(bind_name, BindType.BINARY),
                    attribute.type_.srid,
                    attribute.type_.semantics,
                )
                parameters[bind_name] = _geometry_parameter_value(value, self._provider)
            else:
                assignments[name] = _attribute_bind(attribute, bind_name)
                parameters[bind_name] = _attribute_parameter(attribute, value)
        predicate, identity_parameters = _identity_predicate(mapper, instance)
        parameters.update(identity_parameters)
        if mapper.version is not None:
            version_name = mapper.version.name
            if version_name is None:
                raise OrmMappingError("colonna versione senza nome")
            current = state.original.get(version_name)
            if not isinstance(current, int) or isinstance(current, bool) or current < 1:
                raise OrmStateError("versione ottimistica non valida")
            assignments[version_name] = bind("orm_version_next", BindType.BIG_INTEGER)
            parameters["orm_version_next"] = current + 1
            version_column = mapper.version.column
            if version_column is None:
                raise OrmMappingError("colonna versione non associata")
            predicate = predicate & (
                version_column == bind("orm_version_current", BindType.BIG_INTEGER)
            )
            parameters["orm_version_current"] = current
        affected = self._transaction.execute(
            update(mapper.table).values(**assignments).where(predicate), parameters
        )
        _require_one_row(affected)
        if mapper.version is not None and mapper.version.name is not None:
            instance.__dict__[mapper.version.name] = parameters["orm_version_next"]
        state.original = _snapshot(mapper, instance)
        state.dirty.clear()
        self._emit("after_update", instance)

    def _update_joined(self, instance: DeclarativeBase, mapper: Mapper) -> None:
        lineage = _inheritance_lineage(mapper)
        if len(lineage) < 2:
            raise OrmMappingError("mapper joined privo di base")
        state = _state(instance)
        dirty = set(state.dirty)
        if not dirty:
            return
        self._emit("before_update", instance)
        root = lineage[0]
        identity = _identity(mapper, instance)
        for index, fragment in enumerate(lineage):
            local = fragment.attributes if index == 0 else fragment.local_attributes
            names = tuple(
                sorted(
                    name
                    for name in dirty
                    if any(attribute.name == name for attribute in local)
                    and (root.version is None or name != root.version.name)
                )
            )
            if not names and not (index == 0 and root.version is not None):
                continue
            assignments, parameters = self._update_assignments(
                mapper, names, f"level_{index}", instance
            )
            predicate, identity_parameters = _table_identity_predicate(
                mapper, fragment.table, identity
            )
            parameters.update(identity_parameters)
            if index == 0 and root.version is not None:
                version_name = root.version.name
                current = (
                    None if version_name is None else state.original.get(version_name)
                )
                if (
                    not isinstance(current, int)
                    or isinstance(current, bool)
                    or current < 1
                ):
                    raise OrmStateError("versione ottimistica non valida")
                if version_name is None:
                    raise OrmMappingError("colonna versione senza nome")
                assignments[version_name] = bind(
                    "orm_version_next", BindType.BIG_INTEGER
                )
                parameters["orm_version_next"] = current + 1
                predicate = predicate & (
                    root.table.c[version_name]
                    == bind("orm_version_current", BindType.BIG_INTEGER)
                )
                parameters["orm_version_current"] = current
            _require_one_row(
                self._transaction.execute(
                    update(fragment.table).values(**assignments).where(predicate),
                    parameters,
                )
            )
            if (
                index == 0
                and root.version is not None
                and root.version.name is not None
            ):
                instance.__dict__[root.version.name] = parameters["orm_version_next"]
        state.original = _snapshot(mapper, instance)
        state.dirty.clear()
        self._emit("after_update", instance)

    def _update_assignments(
        self,
        mapper: Mapper,
        names: tuple[str, ...],
        role: str,
        instance: DeclarativeBase,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        assignments: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        for index, name in enumerate(names):
            attribute = mapper.attribute(name)
            bind_name = f"orm_update_{role}_{index}"
            value = instance.__dict__[name]
            if isinstance(attribute.type_, Geometry) and (
                value is not None or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
            ):
                assignments[name] = _spatial_value(
                    bind(bind_name, BindType.BINARY),
                    attribute.type_.srid,
                    attribute.type_.semantics,
                )
                parameters[bind_name] = _geometry_parameter_value(value, self._provider)
            else:
                assignments[name] = _attribute_bind(attribute, bind_name)
                parameters[bind_name] = _attribute_parameter(attribute, value)
        return assignments, parameters

    def _delete(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        if mapper.inheritance == "joined":
            self._delete_joined(instance, mapper)
            return
        self._emit("before_delete", instance)
        state = _state(instance)
        predicate, parameters = _identity_predicate(mapper, instance)
        if mapper.version is not None:
            name = mapper.version.name
            column = mapper.version.column
            current = None if name is None else state.original.get(name)
            if (
                column is None
                or not isinstance(current, int)
                or isinstance(current, bool)
            ):
                raise OrmStateError("versione ottimistica non valida")
            predicate = predicate & (
                column == bind("orm_version_current", BindType.BIG_INTEGER)
            )
            parameters["orm_version_current"] = current
        affected = self._transaction.execute(
            delete(mapper.table).where(predicate), parameters
        )
        _require_one_row(affected)
        key = (type(instance), _identity(mapper, instance))
        self._identity_map.pop(key, None)
        self._deleted.remove(instance)
        self._flushed_deleted.append(instance)
        state.dirty.clear()
        self._emit("after_delete", instance)

    def _delete_joined(self, instance: DeclarativeBase, mapper: Mapper) -> None:
        lineage = _inheritance_lineage(mapper)
        if len(lineage) < 2:
            raise OrmMappingError("mapper joined privo di base")
        self._emit("before_delete", instance)
        identity = _identity(mapper, instance)
        state = _state(instance)
        root = lineage[0]
        for fragment in reversed(lineage):
            predicate, parameters = _table_identity_predicate(
                mapper, fragment.table, identity
            )
            if fragment is root and root.version is not None:
                name = root.version.name
                current = None if name is None else state.original.get(name)
                if (
                    not isinstance(current, int)
                    or isinstance(current, bool)
                    or name is None
                ):
                    raise OrmStateError("versione ottimistica non valida")
                predicate = predicate & (
                    root.table.c[name]
                    == bind("orm_version_current", BindType.BIG_INTEGER)
                )
                parameters["orm_version_current"] = current
            _require_one_row(
                self._transaction.execute(
                    delete(fragment.table).where(predicate), parameters
                )
            )
        self._identity_map.pop((type(instance), identity), None)
        self._deleted.remove(instance)
        self._flushed_deleted.append(instance)
        state.dirty.clear()
        self._emit("after_delete", instance)

    def _detach_all(self, *, restore: bool) -> None:
        instances = (
            list(self._identity_map.values())
            + self._pending
            + self._deleted
            + self._flushed_deleted
        )
        seen: set[int] = set()
        for instance in instances:
            if id(instance) in seen:
                continue
            seen.add(id(instance))
            state = _state(instance)
            if restore:
                mapper = _mapper(type(instance))
                mapped_names = {
                    attribute.name
                    for attribute in mapper.attributes
                    if attribute.name is not None
                }
                for name in mapped_names - set(state.rollback_snapshot):
                    instance.__dict__.pop(name, None)
                instance.__dict__.update(state.rollback_snapshot)
                for relation in mapper.relationships:
                    if relation.name is None:
                        continue
                    if relation.name not in state.rollback_relationships:
                        instance.__dict__.pop(relation.name, None)
                        continue
                    saved = state.rollback_relationships[relation.name]
                    if relation.uselist:
                        collection = _RelationshipCollection(instance, relation)
                        for item in saved:
                            collection._append_from_backref(item)
                        instance.__dict__[relation.name] = collection
                    else:
                        instance.__dict__[relation.name] = saved
            state.status = (
                ObjectState.TRANSIENT
                if restore and state.rollback_state is ObjectState.TRANSIENT
                else ObjectState.DETACHED
            )
            state.session = None
            state.dirty.clear()
            state.expired.clear()
        self._identity_map.clear()
        self._pending.clear()
        self._deleted.clear()
        self._flushed_deleted.clear()
        self._deferred_foreign_keys.clear()
        self._savepoints.clear()


class AsyncOrmSession(OrmSession):
    """Unit of work async con gli stessi mapper e invarianti della sync."""

    def __init__(
        self,
        session: Any,
        *,
        autoflush: bool = True,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: Any | None = None,
        native_query_policy: str | None = None,
        insert_batch_size: int = 100,
        close_session: bool = False,
    ) -> None:
        begin = getattr(session, "begin", None)
        if not callable(begin):
            raise TypeError("AsyncOrmSession richiede una AsyncSession Core")
        capabilities = getattr(session, "capabilities", None)
        provider = (
            capabilities.get("provider") if isinstance(capabilities, Mapping) else None
        )
        if not isinstance(provider, str):
            raise TypeError("AsyncSession Core senza provider dichiarato")
        self._provider = provider
        if not isinstance(close_session, bool):
            raise TypeError("close_session deve essere booleano")
        self._spatial_functions = _session_spatial_functions(capabilities)
        self._session = session
        self._close_session_on_end = close_session
        self._core_session_closed = False
        self._transaction_options = {
            name: value
            for name, value in (
                ("isolation", isolation),
                ("read_only", read_only),
                ("statement_timeout_ms", statement_timeout_ms),
                ("context", context),
                ("native_query_policy", native_query_policy),
            )
            if value is not None
        }
        self._transaction: Any | None = None
        self._identity_map = {}
        self._pending = []
        self._deleted = []
        self._flushed_deleted = []
        self._deferred_foreign_keys = []
        if (
            not isinstance(insert_batch_size, int)
            or isinstance(insert_batch_size, bool)
            or insert_batch_size < 1
        ):
            raise ValueError("insert_batch_size deve essere un intero positivo")
        self._insert_batch_size = insert_batch_size
        self._autoflush_enabled = bool(autoflush)
        self._in_flush = False
        self._listeners = {}
        self._savepoints = {}
        self._active = True

    async def _emit_async(
        self, event: str, instance: DeclarativeBase | None = None
    ) -> None:
        for callback in self._listeners.get(event, ()):
            outcome = (
                callback(self, instance) if instance is not None else callback(self)
            )
            if isawaitable(outcome):
                await outcome

    def __enter__(self) -> None:
        raise TypeError("AsyncOrmSession richiede 'async with'")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        raise TypeError("AsyncOrmSession richiede 'async with'")

    async def __aenter__(self) -> AsyncOrmSession:  # noqa: PYI034
        self._require_active()
        await self._ensure_started()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if not self._active:
            return False
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()
        return False

    async def _ensure_started(self) -> Any:
        self._require_active()
        if self._transaction is None:
            self._transaction = await self._session.begin(**self._transaction_options)
        return self._transaction

    def query(self, model: type[T]) -> AsyncOrmQuery[T]:
        self._require_active()
        mapper = _mapper(model)  # type: ignore[arg-type]
        self._require_relational_load(mapper)
        statement, parameters = _entity_select(mapper, self._provider)
        return AsyncOrmQuery(self, mapper, statement, _fixed_parameters=parameters)

    async def bulk_insert(
        self,
        model: type[DeclarativeBase],
        rows: Iterable[Mapping[str, Any]],
    ) -> int:
        self._require_active()
        await self._autoflush_async()
        mapper = _mapper(model)
        statement, parameters, count = _bulk_mapping_statement(
            mapper, rows, self._provider
        )
        transaction = await self._ensure_started()
        affected = _affected_rows(await transaction.execute(statement, parameters))
        if affected != count:
            raise StaleObjectError(
                "INSERT ORM batch non ha interessato il numero atteso di righe"
            )
        return affected

    async def bulk_upsert(
        self,
        model: type[DeclarativeBase],
        rows: Iterable[Mapping[str, Any]],
        *,
        conflict_columns: Iterable[str],
        update_values: Mapping[str, Any] | None = None,
    ) -> int:
        self._require_active()
        await self._autoflush_async()
        mapper = _mapper(model)
        statement, parameters, _ = _bulk_mapping_statement(
            mapper,
            rows,
            self._provider,
            conflict_columns=conflict_columns,
            update_values=update_values,
        )
        transaction = await self._ensure_started()
        return _affected_rows(await transaction.execute(statement, parameters))

    async def get(self, model: type[T], identity: Any) -> T | None:
        self._require_active()
        await self._autoflush_async()
        mapper = _mapper(model)  # type: ignore[arg-type]
        identity_values = _identity_argument(mapper, identity)
        key = (model, identity_values)
        if key in self._identity_map:
            return self._identity_map[key]  # type: ignore[return-value]
        self._require_relational_load(mapper)
        predicate, parameters = _identity_values_predicate(mapper, identity_values)
        statement, fixed_parameters = _entity_select(mapper, self._provider)
        statement = statement.where(predicate).limit(2)
        parameters = _merge_query_parameters(fixed_parameters, parameters)
        transaction = await self._ensure_started()
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = await transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM senza risultato relazionale")
        row = result.one_or_none()
        if row is None:
            return None
        instance = await self._hydrate_row_async(mapper, row)
        if _identity(mapper, instance) != identity_values:
            raise OrmMappingError(
                "risultato con identita diversa dalla chiave richiesta"
            )
        return instance  # type: ignore[return-value]

    async def refresh(self, instance: DeclarativeBase) -> None:
        self._require_active()
        await self._autoflush_async(exclude=instance)
        mapper = _mapper(type(instance))
        self._require_relational_load(mapper)
        state = _state(instance)
        if state.session is not self or state.status is not ObjectState.PERSISTENT:
            raise OrmStateError("refresh richiede un'istanza persistent della sessione")
        identity = _identity(mapper, instance)
        predicate, parameters = _identity_values_predicate(mapper, identity)
        statement, fixed_parameters = _entity_select(mapper, self._provider)
        statement = statement.where(predicate).limit(2)
        parameters = _merge_query_parameters(fixed_parameters, parameters)
        transaction = await self._ensure_started()
        result = await transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM senza risultato relazionale")
        row = result.one_or_none()
        if row is None:
            raise NoResultFound("refresh ORM senza riga")
        _validate_geometry_row(mapper, row, self._provider)
        for attribute in mapper.attributes:
            name = attribute.name
            if name is None or name not in row:
                raise OrmMappingError("risultato privo di una colonna mappata")
            instance.__dict__[name] = attribute._coerce(row[name])
        if _identity(mapper, instance) != identity:
            raise OrmMappingError("refresh con identita incoerente")
        state.original = _snapshot(mapper, instance)
        state.dirty.clear()
        state.expired.clear()
        await self._emit_async("refresh", instance)

    async def load(
        self,
        instance: DeclarativeBase,
        relation: str | Relationship[T],
    ) -> T | None | MutableSequence[T]:
        self._require_active()
        await self._autoflush_async()
        mapper = _mapper(type(instance))
        state = _state(instance)
        if state.session is not self or state.status is not ObjectState.PERSISTENT:
            raise OrmStateError("load richiede un'istanza persistent della sessione")
        descriptor = (
            mapper.relationship(relation) if isinstance(relation, str) else relation
        )
        if descriptor not in mapper.relationships:
            raise OrmMappingError("relationship appartenente a un altro mapper")
        descriptor._validate_configuration()
        related = await self._load_relationship_async(instance, descriptor)
        if descriptor.name is None:
            raise OrmMappingError("relationship senza nome")
        if descriptor.uselist:
            collection = _RelationshipCollection(instance, descriptor)
            for item in related:
                collection._append_from_backref(item)
                if descriptor.back_populates is not None:
                    inverse = _mapper(descriptor.target).relationship(
                        descriptor.back_populates
                    )
                    inverse._assign_from_backref(item, instance)
            instance.__dict__[descriptor.name] = collection
            _remember_relationship(instance, descriptor)
            _capture_loaded_relationship(instance, descriptor)
            return collection
        instance.__dict__[descriptor.name] = related
        if related is not None and descriptor.back_populates is not None:
            inverse = _mapper(descriptor.target).relationship(descriptor.back_populates)
            inverse._assign_from_backref(related, instance)
        _capture_loaded_relationship(instance, descriptor)
        return related

    async def merge(self, instance: T, *, load: bool = True) -> T:  # type: ignore[override]
        return await self._merge_async(instance, load=load, seen={})

    async def _merge_async(self, instance: T, *, load: bool, seen: dict[int, Any]) -> T:
        self._require_active()
        if id(instance) in seen:
            return seen[id(instance)]
        mapper = _mapper(type(instance))  # type: ignore[arg-type]
        source_state = _state(instance)
        try:
            identity = _identity(mapper, instance)  # type: ignore[arg-type]
        except OrmStateError:
            identity = None
        target = None
        if identity is not None and load:
            target = await self.get(
                type(instance), identity[0] if len(identity) == 1 else identity
            )
        if target is None:
            target = mapper.model.__new__(mapper.model)
            target.__dict__["_plenora_orm_state"] = _InstanceState()
            for attribute in mapper.attributes:
                if attribute.name in instance.__dict__:
                    setattr(target, attribute.name, instance.__dict__[attribute.name])
            self.add(target)
        else:
            for attribute in mapper.attributes:
                name = attribute.name
                if (
                    name is not None
                    and name in instance.__dict__
                    and not attribute.primary_key
                    and not attribute.version
                ):
                    setattr(target, name, instance.__dict__[name])
        seen[id(instance)] = target
        for relation in mapper.relationships:
            if relation.name not in instance.__dict__:
                continue
            if "save-update" not in relation.cascade:
                raise OrmStateError(
                    "merge di relationship richiede cascade save-update"
                )
            if relation.uselist:
                values = [
                    await self._merge_async(item, load=load, seen=seen)
                    for item in instance.__dict__[relation.name]
                ]
                setattr(target, relation.name, values)
            else:
                related = instance.__dict__[relation.name]
                setattr(
                    target,
                    relation.name,
                    None
                    if related is None
                    else await self._merge_async(related, load=load, seen=seen),
                )
        if (
            source_state.session is self
            and source_state.status is ObjectState.PERSISTENT
        ):
            return instance
        return target  # type: ignore[return-value]

    async def _autoflush_async(self, *, exclude: DeclarativeBase | None = None) -> None:
        if not self._autoflush_enabled or self._in_flush:
            return
        if not (self._pending or self._deleted or self._dirty_instances()):
            return
        excluded_dirty: set[str] | None = None
        if exclude is not None:
            excluded_dirty = set(_state(exclude).dirty)
            _state(exclude).dirty.clear()
        try:
            if self._pending or self._deleted or self._dirty_instances():
                await self.flush()
        finally:
            if excluded_dirty is not None and self._active:
                _state(exclude).dirty.update(excluded_dirty)

    async def _load_relationship_async(
        self, instance: DeclarativeBase, descriptor: Relationship[Any]
    ) -> Any:
        target_mapper = _mapper(descriptor.target)
        direction = descriptor.direction
        if direction == "many-to-one":
            foreign_value = tuple(
                instance.__dict__.get(name) for name in descriptor.foreign_keys
            )
            return (
                None
                if any(value is None for value in foreign_value)
                else await self.get(
                    descriptor.target,
                    foreign_value[0] if len(foreign_value) == 1 else foreign_value,
                )
            )
        owner_mapper = _mapper(type(instance))
        owner_identity = _identity(owner_mapper, instance)
        if direction in {"one-to-many", "one-to-one"}:
            foreign = tuple(
                target_mapper.attribute(name).column for name in descriptor.foreign_keys
            )
            if any(column is None for column in foreign):
                raise OrmMappingError("foreign key relationship senza colonna")
            predicate, parameters = _identity_disjunction(
                tuple(column for column in foreign if column is not None),
                (owner_identity,),
                "orm_relationship",
            )
            rows = await self.query(descriptor.target).where(predicate).all(parameters)
            if direction == "one-to-one":
                if len(rows) > 1:
                    raise MultipleResultsFound("relationship one-to-one non univoca")
                return None if not rows else rows[0]
            return rows
        secondary = descriptor.secondary
        if secondary is None:
            raise OrmMappingError("relationship many-to-many senza secondary")
        local_columns = _secondary_columns(descriptor, remote=False)
        remote_columns = _secondary_columns(descriptor, remote=True)
        target_primary = _primary_columns(target_mapper)
        predicate, parameters = _identity_disjunction(
            local_columns, (owner_identity,), "orm_relationship"
        )
        statement = (
            select(*_orm_projections(target_mapper, provider=self._provider))
            .select_from(target_mapper.table)
            .join(
                secondary,
                _column_equality(remote_columns, target_primary),
            )
            .where(predicate)
        )
        return await self._execute_entities_async(target_mapper, statement, parameters)

    async def _execute_entities_async(
        self,
        mapper: Mapper,
        statement: SelectStatement,
        parameters: Mapping[str, Any] | None,
        loaders: tuple[LoaderOption, ...] = (),
    ) -> list[Any]:
        await self._autoflush_async()
        transaction = await self._ensure_started()
        _validate_spatial_statement(statement, self._spatial_functions)
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = await transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM senza risultato relazionale")
        rows = result.all()
        row_instances = [await self._hydrate_row_async(mapper, row) for row in rows]
        joined_collection = any(
            loader.strategy == "joined"
            and _query_loader_path(mapper, loader.relationship)[0].uselist
            for loader in loaders
        )
        if joined_collection:
            instances = list(dict.fromkeys(row_instances))
        else:
            instances = row_instances
        for loader in loaders:
            relation = _query_loader_path(mapper, loader.relationship)[0]
            if loader.strategy == "joined":
                for instance, row in zip(row_instances, rows, strict=True):
                    await self._hydrate_joined_async(instance, relation, row)
            else:
                await self._selectin_load_async(instances, relation)
        for loader in loaders:
            path = _query_loader_path(mapper, loader.relationship)
            await self._load_nested_path_async(instances, path)
        return instances

    async def _execute_scalar_query_async(
        self,
        statement: SelectStatement,
        parameters: Mapping[str, Any] | None,
    ) -> Any:
        await self._autoflush_async()
        transaction = await self._ensure_started()
        _validate_spatial_statement(statement, self._spatial_functions)
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = await transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("query scalare ORM senza risultato relazionale")
        return result.scalar_one()

    async def _execute_exists_query_async(
        self,
        statement: SelectStatement,
        parameters: Mapping[str, Any] | None,
    ) -> bool:
        await self._autoflush_async()
        transaction = await self._ensure_started()
        _validate_spatial_statement(statement, self._spatial_functions)
        parameters = _geometry_query_parameters(statement, parameters, self._provider)
        result = await transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("EXISTS ORM senza risultato relazionale")
        return result.first() is not None

    async def _execute_bulk_update_async(
        self,
        mapper: Mapper,
        query: SelectStatement,
        values: Mapping[str, Any],
        parameters: Mapping[str, Any] | None,
    ) -> int:
        await self._autoflush_async()
        transaction = await self._ensure_started()
        statement, mutation_parameters = _bulk_update_statement(
            mapper, query, values, self._provider
        )
        parameters = _geometry_query_parameters(query, parameters, self._provider)
        merged = _merge_query_parameters(parameters, mutation_parameters)
        _validate_spatial_statement(statement, self._spatial_functions)  # type: ignore[arg-type]
        affected = _affected_rows(await transaction.execute(statement, merged))
        self._expire_mapper_instances(mapper, detach=False)
        return affected

    async def _execute_bulk_delete_async(
        self,
        mapper: Mapper,
        query: SelectStatement,
        parameters: Mapping[str, Any] | None,
    ) -> int:
        await self._autoflush_async()
        transaction = await self._ensure_started()
        statement = _bulk_delete_statement(mapper, query)
        _validate_spatial_statement(statement, self._spatial_functions)  # type: ignore[arg-type]
        parameters = _geometry_query_parameters(query, parameters, self._provider)
        affected = _affected_rows(await transaction.execute(statement, parameters))
        self._expire_mapper_instances(mapper, detach=True)
        return affected

    async def _load_nested_path_async(
        self,
        instances: list[DeclarativeBase],
        path: tuple[Relationship[Any], ...],
    ) -> None:
        if len(path) < 2:
            return
        related = _loaded_relationship_instances(instances, path[0])
        for relation in path[1:]:
            await self._selectin_load_async(related, relation)
            related = _loaded_relationship_instances(related, relation)

    async def _hydrate_joined_async(
        self,
        instance: DeclarativeBase,
        relation: Relationship[Any],
        row: Mapping[str, Any],
    ) -> None:
        target_mapper = _mapper(relation.target)
        prefix = _loader_prefix(relation)
        primary_names = tuple(item.name for item in target_mapper.primary_keys)
        if any(name is None for name in primary_names):
            raise OrmMappingError("target eager privo di chiave")
        identity_values = tuple(
            row.get(f"{prefix}{name}") for name in primary_names if name is not None
        )
        related = None
        if all(value is not None for value in identity_values):
            values = _mapped_row_values(target_mapper, row, prefix, self._provider)
            related = await self._hydrate_row_async(target_mapper, values)
        if relation.uselist:
            _append_joined_relationship(instance, relation, related)
        else:
            _assign_loaded_relationship(instance, relation, related)

    async def _selectin_load_async(
        self,
        instances: list[DeclarativeBase],
        relation: Relationship[Any],
    ) -> None:
        if not instances:
            return
        target_mapper = _mapper(relation.target)
        direction = relation.direction
        if direction == "many-to-one":
            values = tuple(
                dict.fromkeys(
                    tuple(instance.__dict__.get(name) for name in relation.foreign_keys)
                    for instance in instances
                    if all(
                        instance.__dict__.get(name) is not None
                        for name in relation.foreign_keys
                    )
                )
            )
            if not values:
                for instance in instances:
                    _assign_loaded_relationship(instance, relation, None)
                return
            columns = tuple(item.column for item in target_mapper.primary_keys)
            if any(column is None for column in columns):
                raise OrmMappingError("selectinload privo di chiave primaria")
            predicate, parameters = _identity_disjunction(
                tuple(column for column in columns if column is not None),
                values,
                "orm_eager",
            )
            related = await self._execute_entities_async(
                target_mapper,
                select(*_orm_projections(target_mapper, provider=self._provider)).where(
                    predicate
                ),
                parameters,
            )
            by_identity = {_identity(target_mapper, item): item for item in related}
            for instance in instances:
                identity = tuple(
                    instance.__dict__.get(name) for name in relation.foreign_keys
                )
                _assign_loaded_relationship(
                    instance,
                    relation,
                    by_identity.get(identity),
                )
            return
        owner_mapper = _mapper(type(instances[0]))
        owner_identities = tuple(_identity(owner_mapper, item) for item in instances)
        if direction in {"one-to-many", "one-to-one"}:
            foreign = tuple(
                target_mapper.attribute(name).column for name in relation.foreign_keys
            )
            if any(column is None for column in foreign):
                raise OrmMappingError("selectinload privo di foreign key")
            predicate, parameters = _identity_disjunction(
                tuple(column for column in foreign if column is not None),
                owner_identities,
                "orm_eager",
            )
            related = await self._execute_entities_async(
                target_mapper,
                select(*_orm_projections(target_mapper, provider=self._provider)).where(
                    predicate
                ),
                parameters,
            )
            grouped: dict[Any, list[DeclarativeBase]] = {
                key: [] for key in owner_identities
            }
            for item in related:
                key = tuple(item.__dict__.get(name) for name in relation.foreign_keys)
                grouped.setdefault(key, []).append(item)
            for instance, identity in zip(instances, owner_identities, strict=True):
                values = grouped.get(identity, [])
                if direction == "one-to-one" and len(values) > 1:
                    raise MultipleResultsFound("relationship one-to-one non univoca")
                _assign_loaded_relationship(
                    instance,
                    relation,
                    (None if not values else values[0])
                    if direction == "one-to-one"
                    else values,
                )
            return
        await self._selectin_many_to_many_async(instances, relation, owner_identities)

    async def _selectin_entities_async(
        self,
        mapper: Mapper,
        column: Column | None,
        values: tuple[Any, ...],
    ) -> list[DeclarativeBase]:
        if not values:
            return []
        if column is None:
            raise OrmMappingError("selectinload privo di colonna")
        parameters = {f"orm_eager_{index}": value for index, value in enumerate(values)}
        statement = select(*_orm_projections(mapper, provider=self._provider)).where(
            column.in_(
                *(
                    bind(name, _bind_type_for_value(value))
                    for name, value in parameters.items()
                )
            )
        )
        return await self._execute_entities_async(mapper, statement, parameters)

    async def _selectin_many_to_many_async(
        self,
        instances: list[DeclarativeBase],
        relation: Relationship[Any],
        owner_ids: tuple[tuple[Any, ...], ...],
    ) -> None:
        secondary = relation.secondary
        target_mapper = _mapper(relation.target)
        if secondary is None:
            raise OrmMappingError("many-to-many non configurata")
        local_columns = _secondary_columns(relation, remote=False)
        remote_columns = _secondary_columns(relation, remote=True)
        predicate, parameters = _identity_disjunction(
            local_columns, owner_ids, "orm_eager"
        )
        owner_projections = tuple(
            column.label(f"orm_eager_owner_{index}")
            for index, column in enumerate(local_columns)
        )
        statement = (
            select(
                *_orm_projections(target_mapper, provider=self._provider),
                *owner_projections,
            )
            .select_from(target_mapper.table)
            .join(
                secondary,
                _column_equality(remote_columns, _primary_columns(target_mapper)),
            )
            .where(predicate)
        )
        transaction = await self._ensure_started()
        result = await transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM eager senza risultato relazionale")
        grouped: dict[Any, list[DeclarativeBase]] = {key: [] for key in owner_ids}
        for row in result.all():
            owner_identity = tuple(
                row[f"orm_eager_owner_{index}"] for index in range(len(local_columns))
            )
            grouped.setdefault(owner_identity, []).append(
                await self._hydrate_row_async(target_mapper, row)
            )
        for instance, identity in zip(instances, owner_ids, strict=True):
            _assign_loaded_relationship(instance, relation, grouped.get(identity, []))

    async def _hydrate_row_async(
        self, mapper: Mapper, row: Mapping[str, Any]
    ) -> DeclarativeBase:
        identity = _row_identity(mapper, row)
        was_present = (mapper.model, identity) in self._identity_map
        instance = self._hydrate(mapper, row, emit=False)
        if not was_present:
            await self._emit_async("load", instance)
        return instance

    async def flush(self) -> None:
        self._require_active()
        if self._in_flush:
            return
        self._in_flush = True
        await self._ensure_started()
        try:
            await self._emit_async("before_flush")
            dirty = self._dirty_instances()
            pending = self._pending_insert_order()
            self._preflight((*self._pending, *dirty, *self._deleted))
            position = 0
            while position < len(pending):
                instance = pending[position]
                self._synchronize_relationships(instance)
                self._preflight((instance,))
                signature = self._pending_batch_signature(instance)
                batch = [instance]
                batch_limit = _insert_batch_limit(
                    signature, self._provider, self._insert_batch_size
                )
                if signature is not None:
                    while (
                        position + len(batch) < len(pending)
                        and len(batch) < batch_limit
                    ):
                        candidate = pending[position + len(batch)]
                        self._synchronize_relationships(candidate)
                        self._preflight((candidate,))
                        if self._pending_batch_signature(candidate) != signature:
                            break
                        batch.append(candidate)
                if len(batch) > 1:
                    await self._insert_batch_async(batch)
                else:
                    await self._insert_async(instance)
                position += len(batch)
            for instance, related, foreign_keys in self._deferred_foreign_keys:
                identity = _identity(_mapper(type(related)), related)
                for foreign_key, value in zip(foreign_keys, identity, strict=True):
                    instance.__dict__[foreign_key] = value
                    _state(instance).dirty.add(foreign_key)
                await self._update_async(instance)
            for instance in dirty:
                if _state(instance).status is ObjectState.PERSISTENT:
                    await self._update_async(instance)
            await self._flush_many_to_many_async()
            await self._remove_deleted_associations_async()
            for instance in tuple(self._deleted):
                await self._delete_async(instance)
            await self._emit_async("after_flush")
        except BaseException:
            try:
                if self._transaction is not None:
                    await self._transaction.rollback()
            finally:
                self._detach_all(restore=True)
                self._active = False
                await self._emit_async("after_rollback")
            raise
        finally:
            self._in_flush = False

    async def commit(self) -> None:
        self._require_active()
        try:
            await self.flush()
            await self._transaction.commit()
        except BaseException:
            if self._active and self._transaction is not None:
                try:
                    await self._transaction.rollback()
                # Il rollback e best-effort: non deve mascherare l'errore di commit.
                except BaseException:  # noqa: BLE001, S110
                    pass
            self._detach_all(restore=True)
            self._active = False
            try:
                self._close_owned_session()
            except BaseException:  # la chiusura non maschera l'errore originale
                pass
            raise
        self._detach_all(restore=False)
        self._active = False
        try:
            await self._emit_async("after_commit")
        finally:
            self._close_owned_session()

    async def savepoint(self, name: str) -> None:
        self._require_active()
        _validate_savepoint_name(name)
        if name in self._savepoints:
            raise OrmStateError("savepoint ORM gia attivo")
        await self.flush()
        transaction = await self._ensure_started()
        operation = getattr(transaction, "savepoint", None)
        if not callable(operation):
            raise OrmUnsupportedError("la transazione Core non espone savepoint")
        await operation(name)
        self._savepoints[name] = _capture_savepoint(self)

    async def rollback_to_savepoint(self, name: str) -> None:
        self._require_active()
        snapshot = self._savepoints.get(name)
        if snapshot is None:
            raise OrmStateError("savepoint ORM non attivo")
        transaction = await self._ensure_started()
        operation = getattr(transaction, "rollback_to_savepoint", None)
        if not callable(operation):
            raise OrmUnsupportedError("la transazione Core non espone savepoint")
        await operation(name)
        _restore_savepoint(self, snapshot)
        names = tuple(self._savepoints)
        position = names.index(name)
        for nested in names[position + 1 :]:
            self._savepoints.pop(nested, None)

    async def release_savepoint(self, name: str) -> None:
        self._require_active()
        if name not in self._savepoints:
            raise OrmStateError("savepoint ORM non attivo")
        transaction = await self._ensure_started()
        operation = getattr(transaction, "release_savepoint", None)
        if not callable(operation):
            raise OrmUnsupportedError("la transazione Core non espone savepoint")
        await operation(name)
        self._savepoints.pop(name)

    def begin_nested(self, name: str) -> _AsyncOrmNestedTransaction:
        self._require_active()
        return _AsyncOrmNestedTransaction(self, name)

    async def rollback(self) -> None:
        self._require_active()
        try:
            if self._transaction is not None:
                await self._transaction.rollback()
        finally:
            self._detach_all(restore=True)
            self._active = False
            try:
                await self._emit_async("after_rollback")
            finally:
                self._close_owned_session()

    async def close(self) -> None:
        if self._active:
            await self.rollback()
        else:
            self._close_owned_session()

    async def _insert_async(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        if mapper.inheritance == "joined":
            await self._insert_joined_async(instance, mapper)
            return
        await self._emit_async("before_insert", instance)
        version = mapper.version
        if version is not None and version.name not in instance.__dict__:
            instance.__dict__[version.name] = 1
        assignments: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        returning: list[Column] = []
        for index, attribute in enumerate(mapper.attributes):
            name = attribute.name
            if any(
                deferred is instance and name in foreign_keys
                for deferred, _, foreign_keys in self._deferred_foreign_keys
            ):
                continue
            if name is not None and name in instance.__dict__:
                bind_name = f"orm_insert_{index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and (
                    value is not None
                    or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
                ):
                    assignments[name] = _spatial_value(
                        bind(bind_name, BindType.BINARY),
                        attribute.type_.srid,
                        attribute.type_.semantics,
                    )
                    parameters[bind_name] = _geometry_parameter_value(
                        value, self._provider
                    )
                else:
                    assignments[name] = _attribute_bind(attribute, bind_name)
                    parameters[bind_name] = _attribute_parameter(attribute, value)
            elif (
                attribute.generated or attribute.server_default
            ) and attribute.column is not None:
                returning.append(attribute.column)
        statement = insert(mapper.table).values(**assignments)
        uses_returning = (
            bool(returning) and self._provider in _INSERT_RETURNING_PROVIDERS
        )
        if uses_returning:
            statement = statement.returning(*returning)
        outcome = await self._transaction.execute(statement, parameters)
        if uses_returning:
            if not isinstance(outcome, Result):
                raise OrmStateError(
                    "INSERT ORM con default senza risultato relazionale"
                )
            row = outcome.one()
            for attribute in mapper.attributes:
                if any(attribute.column is column for column in returning):
                    name = attribute.name
                    if name is None or name not in row:
                        raise OrmMappingError(
                            "INSERT non ha restituito una colonna richiesta"
                        )
                    instance.__dict__[name] = attribute._coerce(row[name])
        else:
            _require_one_row(outcome)
            if returning:
                await self._hydrate_post_insert_defaults_async(
                    mapper, instance, returning
                )
        self._mark_inserted(instance, mapper)
        await self._emit_async("after_insert", instance)

    async def _insert_batch_async(
        self, instances: list[DeclarativeBase]
    ) -> None:
        mapper = _mapper(type(instances[0]))
        statement = insert(mapper.table)
        parameters: dict[str, Any] = {}
        for instance in instances:
            await self._emit_async("before_insert", instance)
        self._preflight(tuple(instances))
        for row_index, instance in enumerate(instances):
            assignments: dict[str, Any] = {}
            for column_index, attribute in enumerate(mapper.attributes):
                name = attribute.name
                if name is None or name not in instance.__dict__:
                    continue
                bind_name = f"orm_insert_batch_{row_index}_{column_index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and (
                    value is not None
                    or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
                ):
                    assignments[name] = _spatial_value(
                        bind(bind_name, BindType.BINARY),
                        attribute.type_.srid,
                        attribute.type_.semantics,
                    )
                    parameters[bind_name] = _geometry_parameter_value(
                        value, self._provider
                    )
                else:
                    assignments[name] = _attribute_bind(attribute, bind_name)
                    parameters[bind_name] = _attribute_parameter(attribute, value)
            statement = statement.values(**assignments)
        transaction = await self._ensure_started()
        outcome = await transaction.execute(statement, parameters)
        _require_row_count(outcome, len(instances))
        for instance in instances:
            self._mark_inserted(instance, mapper)
            await self._emit_async("after_insert", instance)

    async def _insert_joined_async(
        self, instance: DeclarativeBase, mapper: Mapper
    ) -> None:
        lineage = _inheritance_lineage(mapper)
        if len(lineage) < 2:
            raise OrmMappingError("mapper joined privo di base")
        await self._emit_async("before_insert", instance)
        if mapper.version is not None and mapper.version.name not in instance.__dict__:
            instance.__dict__[mapper.version.name] = 1
        for index, fragment in enumerate(lineage):
            attributes = (
                fragment.attributes
                if index == 0
                else (*mapper.primary_keys, *fragment.local_attributes)
            )
            await self._insert_fragment_async(
                mapper, fragment.table, attributes, instance, f"level_{index}"
            )
        self._mark_inserted(instance, mapper)
        await self._emit_async("after_insert", instance)

    async def _insert_fragment_async(
        self,
        mapper: Mapper,
        table: Table,
        attributes: tuple[MappedColumn[Any], ...],
        instance: DeclarativeBase,
        role: str,
    ) -> None:
        assignments: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        returning: list[MappedColumn[Any]] = []
        for index, attribute in enumerate(attributes):
            name = attribute.name
            if name is None:
                raise OrmMappingError("attributo mappato senza nome")
            if name in instance.__dict__:
                bind_name = f"orm_insert_{role}_{index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and (
                    value is not None
                    or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
                ):
                    assignments[name] = _spatial_value(
                        bind(bind_name, BindType.BINARY),
                        attribute.type_.srid,
                        attribute.type_.semantics,
                    )
                    parameters[bind_name] = _geometry_parameter_value(
                        value, self._provider
                    )
                else:
                    assignments[name] = _attribute_bind(attribute, bind_name)
                    parameters[bind_name] = _attribute_parameter(attribute, value)
            elif attribute.generated or attribute.server_default:
                returning.append(attribute)
        statement = insert(table).values(**assignments)
        uses_returning = (
            bool(returning) and self._provider in _INSERT_RETURNING_PROVIDERS
        )
        if uses_returning:
            statement = statement.returning(
                *(table.c[attribute.name or ""] for attribute in returning)
            )
        outcome = await self._transaction.execute(statement, parameters)
        if uses_returning:
            if not isinstance(outcome, Result):
                raise OrmStateError(
                    "INSERT ORM con default senza risultato relazionale"
                )
            row = outcome.one()
            for attribute in returning:
                name = attribute.name
                if name is None or name not in row:
                    raise OrmMappingError(
                        "INSERT non ha restituito una colonna richiesta"
                    )
                instance.__dict__[name] = attribute._coerce(row[name])
        else:
            _require_one_row(outcome)
            if returning:
                await self._hydrate_fragment_defaults_async(
                    mapper, table, instance, returning
                )

    async def _hydrate_fragment_defaults_async(
        self,
        mapper: Mapper,
        table: Table,
        instance: DeclarativeBase,
        attributes: list[MappedColumn[Any]],
    ) -> None:
        generated = tuple(
            attribute
            for attribute in mapper.primary_keys
            if attribute in attributes
            and attribute.generated
            and attribute.name is not None
            and instance.__dict__.get(attribute.name) is None
        )
        if generated:
            scalar = getattr(self._transaction, "execute_scalar", None)
            if not callable(scalar):
                raise OrmUnsupportedError(
                    "il provider richiede execute_scalar per leggere l'identita generated"
                )
            if self._provider == "mysql":
                value = await scalar("SELECT LAST_INSERT_ID()")
            elif self._provider == "db2":
                value = await scalar("VALUES CAST(IDENTITY_VAL_LOCAL() AS INTEGER)")
            else:
                raise OrmUnsupportedError("identita generated non qualificata")
            generated[0].__set__(instance, value)
        identity = _identity(mapper, instance)
        predicate, parameters = _table_identity_predicate(mapper, table, identity)
        columns = tuple(table.c[attribute.name or ""] for attribute in attributes)
        result = await self._transaction.execute(
            select(*columns).select_from(table).where(predicate).limit(2), parameters
        )
        if not isinstance(result, Result):
            raise OrmStateError("lettura default ORM senza risultato relazionale")
        row = result.one()
        for attribute in attributes:
            name = attribute.name
            if name is None or name not in row:
                raise OrmMappingError("lettura default priva di una colonna richiesta")
            instance.__dict__[name] = attribute._coerce(row[name])

    async def _hydrate_post_insert_defaults_async(
        self,
        mapper: Mapper,
        instance: DeclarativeBase,
        attributes: list[Column],
    ) -> None:
        generated = tuple(
            attribute
            for attribute in mapper.primary_keys
            if attribute.generated
            and attribute.name is not None
            and instance.__dict__.get(attribute.name) is None
        )
        if generated:
            scalar = getattr(self._transaction, "execute_scalar", None)
            if not callable(scalar):
                raise OrmUnsupportedError(
                    "il provider richiede execute_scalar per leggere l'identita generated"
                )
            if self._provider == "mysql":
                value = await scalar("SELECT LAST_INSERT_ID()")
            elif self._provider == "db2":
                value = await scalar("VALUES CAST(IDENTITY_VAL_LOCAL() AS INTEGER)")
            else:
                raise OrmUnsupportedError("identita generated non qualificata")
            generated[0].__set__(instance, value)
        identity = _identity(mapper, instance)
        predicate, parameters = _identity_values_predicate(mapper, identity)
        statement = select(*attributes).where(predicate).limit(2)
        result = await self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("lettura default ORM senza risultato relazionale")
        row = result.one()
        for attribute in mapper.attributes:
            if any(attribute.column is column for column in attributes):
                name = attribute.name
                if name is None or name not in row:
                    raise OrmMappingError(
                        "lettura default priva di una colonna richiesta"
                    )
                instance.__dict__[name] = attribute._coerce(row[name])

    async def _flush_many_to_many_async(self) -> None:
        seen: set[tuple[Any, ...]] = set()
        for instance in tuple(self._identity_map.values()):
            mapper = _mapper(type(instance))
            state = _state(instance)
            for relation in mapper.relationships:
                if relation.secondary is None or relation.name not in instance.__dict__:
                    continue
                local_value = _identity(mapper, instance)
                current = {
                    _identity(_mapper(relation.target), related)
                    for related in _loaded_relationship_values(instance, relation)
                }
                original = set(state.relationship_original.get(relation.name or "", ()))
                for remote_identity in current - original:
                    remote_value = remote_identity
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    assignments, parameters = _association_values(
                        relation, local_value, remote_value
                    )
                    await self._transaction.execute(
                        insert(relation.secondary).values(**assignments), parameters
                    )
                for remote_identity in original - current:
                    remote_value = remote_identity
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    predicate, parameters = _association_predicate(
                        relation, local_value, remote_value
                    )
                    await self._transaction.execute(
                        delete(relation.secondary).where(predicate),
                        parameters,
                    )
                _remember_relationship(instance, relation)

    async def _remove_deleted_associations_async(self) -> None:
        seen: set[tuple[Any, ...]] = set()
        for instance in self._deleted:
            mapper = _mapper(type(instance))
            local_value = _identity(mapper, instance)
            for relation in mapper.relationships:
                if relation.secondary is None:
                    continue
                signature = (
                    id(relation.secondary),
                    *sorted(
                        zip(
                            relation.secondary_local_keys,
                            local_value,
                            strict=True,
                        ),
                        key=lambda item: item[0],
                    ),
                )
                if signature in seen:
                    continue
                seen.add(signature)
                predicate, parameters = _association_owner_predicate(
                    relation, local_value
                )
                await self._transaction.execute(
                    delete(relation.secondary).where(predicate), parameters
                )

    async def _update_async(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        if mapper.inheritance == "joined":
            await self._update_joined_async(instance, mapper)
            return
        state = _state(instance)
        names = tuple(sorted(state.dirty))
        if not names:
            return
        await self._emit_async("before_update", instance)
        assignments: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        for index, name in enumerate(names):
            bind_name = f"orm_update_{index}"
            attribute = mapper.attribute(name)
            value = instance.__dict__[name]
            if isinstance(attribute.type_, Geometry) and (
                value is not None or self._provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
            ):
                assignments[name] = _spatial_value(
                    bind(bind_name, BindType.BINARY),
                    attribute.type_.srid,
                    attribute.type_.semantics,
                )
                parameters[bind_name] = _geometry_parameter_value(value, self._provider)
            else:
                assignments[name] = _attribute_bind(attribute, bind_name)
                parameters[bind_name] = _attribute_parameter(attribute, value)
        predicate, identity_parameters = _identity_predicate(mapper, instance)
        parameters.update(identity_parameters)
        if mapper.version is not None:
            version_name = mapper.version.name
            if version_name is None:
                raise OrmMappingError("colonna versione senza nome")
            current = state.original.get(version_name)
            if not isinstance(current, int) or isinstance(current, bool) or current < 1:
                raise OrmStateError("versione ottimistica non valida")
            assignments[version_name] = bind("orm_version_next", BindType.BIG_INTEGER)
            parameters["orm_version_next"] = current + 1
            version_column = mapper.version.column
            if version_column is None:
                raise OrmMappingError("colonna versione non associata")
            predicate = predicate & (
                version_column == bind("orm_version_current", BindType.BIG_INTEGER)
            )
            parameters["orm_version_current"] = current
        affected = await self._transaction.execute(
            update(mapper.table).values(**assignments).where(predicate), parameters
        )
        _require_one_row(affected)
        if mapper.version is not None and mapper.version.name is not None:
            instance.__dict__[mapper.version.name] = parameters["orm_version_next"]
        state.original = _snapshot(mapper, instance)
        state.dirty.clear()
        await self._emit_async("after_update", instance)

    async def _update_joined_async(
        self, instance: DeclarativeBase, mapper: Mapper
    ) -> None:
        lineage = _inheritance_lineage(mapper)
        if len(lineage) < 2:
            raise OrmMappingError("mapper joined privo di base")
        state = _state(instance)
        dirty = set(state.dirty)
        if not dirty:
            return
        await self._emit_async("before_update", instance)
        root = lineage[0]
        identity = _identity(mapper, instance)
        for index, fragment in enumerate(lineage):
            local = fragment.attributes if index == 0 else fragment.local_attributes
            names = tuple(
                sorted(
                    name
                    for name in dirty
                    if any(attribute.name == name for attribute in local)
                    and (root.version is None or name != root.version.name)
                )
            )
            if not names and not (index == 0 and root.version is not None):
                continue
            assignments, parameters = self._update_assignments(
                mapper, names, f"level_{index}", instance
            )
            predicate, identity_parameters = _table_identity_predicate(
                mapper, fragment.table, identity
            )
            parameters.update(identity_parameters)
            if index == 0 and root.version is not None:
                name = root.version.name
                current = None if name is None else state.original.get(name)
                if (
                    not isinstance(current, int)
                    or isinstance(current, bool)
                    or name is None
                ):
                    raise OrmStateError("versione ottimistica non valida")
                assignments[name] = bind("orm_version_next", BindType.BIG_INTEGER)
                parameters["orm_version_next"] = current + 1
                predicate = predicate & (
                    root.table.c[name]
                    == bind("orm_version_current", BindType.BIG_INTEGER)
                )
                parameters["orm_version_current"] = current
            _require_one_row(
                await self._transaction.execute(
                    update(fragment.table).values(**assignments).where(predicate),
                    parameters,
                )
            )
            if (
                index == 0
                and root.version is not None
                and root.version.name is not None
            ):
                instance.__dict__[root.version.name] = parameters["orm_version_next"]
        state.original = _snapshot(mapper, instance)
        state.dirty.clear()
        await self._emit_async("after_update", instance)

    async def _delete_async(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
        if mapper.inheritance == "joined":
            await self._delete_joined_async(instance, mapper)
            return
        await self._emit_async("before_delete", instance)
        state = _state(instance)
        predicate, parameters = _identity_predicate(mapper, instance)
        if mapper.version is not None:
            name = mapper.version.name
            column = mapper.version.column
            current = None if name is None else state.original.get(name)
            if (
                column is None
                or not isinstance(current, int)
                or isinstance(current, bool)
            ):
                raise OrmStateError("versione ottimistica non valida")
            predicate = predicate & (
                column == bind("orm_version_current", BindType.BIG_INTEGER)
            )
            parameters["orm_version_current"] = current
        affected = await self._transaction.execute(
            delete(mapper.table).where(predicate), parameters
        )
        _require_one_row(affected)
        key = (type(instance), _identity(mapper, instance))
        self._identity_map.pop(key, None)
        self._deleted.remove(instance)
        self._flushed_deleted.append(instance)
        state.dirty.clear()
        await self._emit_async("after_delete", instance)

    async def _delete_joined_async(
        self, instance: DeclarativeBase, mapper: Mapper
    ) -> None:
        lineage = _inheritance_lineage(mapper)
        if len(lineage) < 2:
            raise OrmMappingError("mapper joined privo di base")
        await self._emit_async("before_delete", instance)
        identity = _identity(mapper, instance)
        state = _state(instance)
        root = lineage[0]
        for fragment in reversed(lineage):
            predicate, parameters = _table_identity_predicate(
                mapper, fragment.table, identity
            )
            if fragment is root and root.version is not None:
                name = root.version.name
                current = None if name is None else state.original.get(name)
                if (
                    not isinstance(current, int)
                    or isinstance(current, bool)
                    or name is None
                ):
                    raise OrmStateError("versione ottimistica non valida")
                predicate = predicate & (
                    root.table.c[name]
                    == bind("orm_version_current", BindType.BIG_INTEGER)
                )
                parameters["orm_version_current"] = current
            _require_one_row(
                await self._transaction.execute(
                    delete(fragment.table).where(predicate), parameters
                )
            )
        self._identity_map.pop((type(instance), identity), None)
        self._deleted.remove(instance)
        self._flushed_deleted.append(instance)
        state.dirty.clear()
        await self._emit_async("after_delete", instance)


class _NoAutoflushContext:
    def __init__(self, session: OrmSession) -> None:
        self._session = session
        self._previous: bool | None = None

    def __enter__(self) -> _NoAutoflushContext:  # noqa: PYI034
        self._session._require_active()
        self._previous = self._session._autoflush_enabled
        self._session._autoflush_enabled = False
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> bool:
        if self._previous is not None:
            self._session._autoflush_enabled = self._previous
        return False


class _OrmNestedTransaction:
    def __init__(self, session: OrmSession, name: str) -> None:
        _validate_savepoint_name(name)
        self._session = session
        self._name = name

    def __enter__(self) -> _OrmNestedTransaction:  # noqa: PYI034
        self._session.savepoint(self._name)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc_type is None:
            self._session.release_savepoint(self._name)
        else:
            self._session.rollback_to_savepoint(self._name)
            self._session.release_savepoint(self._name)
        return False


class _AsyncOrmNestedTransaction:
    def __init__(self, session: AsyncOrmSession, name: str) -> None:
        _validate_savepoint_name(name)
        self._session = session
        self._name = name

    async def __aenter__(self) -> _AsyncOrmNestedTransaction:  # noqa: PYI034
        await self._session.savepoint(self._name)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc_type is None:
            await self._session.release_savepoint(self._name)
        else:
            await self._session.rollback_to_savepoint(self._name)
            await self._session.release_savepoint(self._name)
        return False


def _geometry_srid_alias(projected_name: str) -> str:
    return f"orm_geometry_srid_{projected_name}"


def _orm_projections(
    mapper: Mapper, prefix: str = "", provider: str = "postgres"
) -> tuple[Expression, ...]:
    projections: list[Expression] = []
    attributes = (
        _single_table_attributes(mapper)
        if mapper.inherits is None
        else mapper.attributes
    )
    for attribute in attributes:
        name = attribute.name
        column = attribute.column
        if name is None or column is None:
            raise OrmMappingError("attributo mappato privo di colonna")
        if isinstance(attribute.type_, Geometry):
            _require_geometry_mapping(attribute.type_, provider)
            projected_name = f"{prefix}{name}"
            projection = _spatial_output(column, attribute.type_.semantics).label(
                projected_name
            )
            projections.append(projection)
            if provider in _FRAMED_ORM_PROVIDERS:
                projections.append(
                    _spatial_function("srid", column).label(
                        _geometry_srid_alias(projected_name)
                    )
                )
            continue
        else:
            projection = column.label(f"{prefix}{name}") if prefix else column
        projections.append(projection)
    return tuple(projections)


def _merge_query_parameters(
    fixed: Mapping[str, Any] | None,
    supplied: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not fixed:
        return None if supplied is None else dict(supplied)
    merged = dict(fixed)
    for name, value in (supplied or {}).items():
        if name in merged and merged[name] != value:
            raise OrmStateError("parametro ORM riservato ridefinito")
        merged[name] = value
    return merged


def _inheritance_predicate(mapper: Mapper) -> Predicate:
    inherited = mapper.inherits
    if inherited is None:
        raise OrmMappingError("mapper joined privo di base")
    predicates: list[Predicate] = []
    for attribute in mapper.primary_keys:
        name = attribute.name
        base_column = attribute.column
        if name is None or base_column is None:
            raise OrmMappingError("chiave joined priva di colonna")
        predicates.append(base_column == mapper.table.c[name])
    predicate = predicates[0]
    for item in predicates[1:]:
        predicate = predicate & item
    return predicate


def _inheritance_root(mapper: Mapper) -> Mapper:
    current = mapper
    while current.inherits is not None:
        current = current.inherits
    return current


def _inheritance_lineage(mapper: Mapper) -> tuple[Mapper, ...]:
    lineage: list[Mapper] = []
    current: Mapper | None = mapper
    while current is not None:
        lineage.append(current)
        current = current.inherits
    return tuple(reversed(lineage))


def _single_table_attributes(mapper: Mapper) -> tuple[MappedColumn[Any], ...]:
    root = _inheritance_root(mapper)
    if mapper is not root or root.polymorphic_on is None:
        return mapper.attributes
    attributes: list[MappedColumn[Any]] = list(root.attributes)
    seen = {attribute.name for attribute in attributes}
    for candidate in root.model.__registry__.mappers():
        if candidate.inheritance == "single" and _inheritance_root(candidate) is root:
            for attribute in candidate.local_attributes:
                if attribute.name not in seen:
                    attributes.append(attribute)
                    seen.add(attribute.name)
    return tuple(attributes)


def _entity_select(
    mapper: Mapper, provider: str
) -> tuple[SelectStatement, dict[str, Any] | None]:
    statement = select(*_orm_projections(mapper, provider=provider))
    parameters: dict[str, Any] | None = None
    if mapper.inheritance == "joined":
        lineage = _inheritance_lineage(mapper)
        statement = statement.select_from(lineage[0].table)
        for child in lineage[1:]:
            statement = statement.join(child.table, _inheritance_predicate(child))
    if mapper.inheritance == "single":
        if mapper.polymorphic_on is None:
            raise OrmMappingError("mapper single privo di discriminatore")
        column = mapper.attribute(mapper.polymorphic_on).column
        if column is None:
            raise OrmMappingError("discriminatore privo di colonna")
        parameter = "orm_polymorphic_identity"
        statement = statement.where(
            column == bind(parameter, _bind_type_for_value(mapper.polymorphic_identity))
        )
        parameters = {parameter: mapper.polymorphic_identity}
    return statement, parameters


def _validate_bulk_query(mapper: Mapper, query: SelectStatement) -> None:
    if mapper.inheritance == "joined":
        raise OrmUnsupportedError("bulk DML joined-table non qualificato atomicamente")
    if (
        query.joins
        or query.groupings
        or query.having_predicate is not None
        or query.orderings
        or query.set_operations
        or query.row_limit is not None
        or query.row_offset is not None
        or query.is_distinct
    ):
        raise OrmUnsupportedError(
            "bulk DML ORM non supporta join, grouping, ordering o paginazione"
        )


def _bulk_update_statement(
    mapper: Mapper,
    query: SelectStatement,
    values: Mapping[str, Any],
    provider: str,
) -> tuple[Any, dict[str, Any]]:
    _validate_bulk_query(mapper, query)
    if not isinstance(values, Mapping) or not values:
        raise TypeError("bulk update ORM richiede un mapping non vuoto")
    assignments: dict[str, Expression] = {}
    parameters: dict[str, Any] = {}
    for index, (name, value) in enumerate(values.items()):
        if not isinstance(name, str):
            raise TypeError("bulk update ORM richiede nomi attributo stringa")
        try:
            attribute = mapper.attribute(name)
        except KeyError as error:
            raise OrmMappingError(
                "bulk update riferisce un attributo non mappato"
            ) from error
        if attribute.primary_key or attribute.version or attribute.generated:
            raise OrmStateError(
                "bulk update non puo mutare chiave, versione o colonna generated"
            )
        coerced = attribute._coerce(value)
        bind_name = f"orm_bulk_update_{index}"
        if isinstance(attribute.type_, Geometry) and (
            coerced is not None or provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
        ):
            _require_geometry_mapping(attribute.type_, provider)
            assignments[name] = _spatial_value(
                bind(bind_name, BindType.BINARY),
                attribute.type_.srid,
                attribute.type_.semantics,
            )
            parameters[bind_name] = _geometry_parameter_value(coerced, provider)
        else:
            assignments[name] = _attribute_bind(attribute, bind_name)
            parameters[bind_name] = _attribute_parameter(attribute, coerced)
    statement = update(mapper.table).values(**assignments)
    if query.predicate is not None:
        statement = statement.where(query.predicate)
    return statement, parameters


def _bulk_delete_statement(mapper: Mapper, query: SelectStatement) -> Any:
    _validate_bulk_query(mapper, query)
    statement = delete(mapper.table)
    if query.predicate is not None:
        statement = statement.where(query.predicate)
    return statement


def _count_statement(query: SelectStatement) -> SelectStatement:
    source = query._resolved_source()
    if source is None:
        raise OrmMappingError("COUNT ORM privo di source relazionale")
    shaped = replace(
        query,
        source=source,
        orderings=query.orderings if query.distinct_expressions else (),
        row_limit=None,
        row_offset=None,
    )
    if (
        shaped.is_distinct
        or shaped.distinct_expressions
        or shaped.groupings
        or shaped.having_predicate is not None
        or shaped.set_operations
    ):
        derived = shaped.subquery("_plenora_orm_count")
        return select(func.count()).select_from(derived)
    return replace(shaped, projections=(func.count(),))


def _bulk_mapping_statement(
    mapper: Mapper,
    rows: Iterable[Mapping[str, Any]],
    provider: str,
    *,
    conflict_columns: Iterable[str] | None = None,
    update_values: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, Any], int]:
    if mapper.inheritance != "none":
        raise OrmUnsupportedError("bulk mapping non qualificato con inheritance")
    materialized = tuple(rows)
    if not materialized or any(not isinstance(row, Mapping) for row in materialized):
        raise OrmMappingError("bulk mapping richiede almeno una riga")
    prepared = [dict(row) for row in materialized]
    if mapper.version is not None and mapper.version.name is not None:
        for row in prepared:
            row.setdefault(mapper.version.name, 1)
    keys = tuple(prepared[0])
    if not keys or any(set(row) != set(keys) for row in prepared):
        raise OrmMappingError("le righe bulk richiedono le stesse colonne")
    attributes = {attribute.name: attribute for attribute in mapper.attributes}
    if set(keys) - attributes.keys():
        raise OrmMappingError("bulk mapping contiene una colonna non mappata")
    for attribute in mapper.attributes:
        if (
            attribute.name not in keys
            and not attribute.nullable
            and not attribute.generated
            and not attribute.server_default
        ):
            raise OrmStateError("bulk mapping omette una colonna non nullable")
    conflicts = None if conflict_columns is None else tuple(conflict_columns)
    if conflicts is not None:
        if (
            not conflicts
            or len(set(conflicts)) != len(conflicts)
            or not set(conflicts) <= set(keys)
        ):
            raise OrmMappingError("conflict_columns bulk non valide")
        if provider == "sqlserver" and len(prepared) > 1:
            raise OrmUnsupportedError(
                "SQL Server qualifica l'upsert portabile per una riga alla volta"
            )
        statement = upsert(mapper.table)
    else:
        if update_values is not None:
            raise OrmMappingError("update_values richiede bulk_upsert")
        statement = insert(mapper.table)
    parameters: dict[str, Any] = {}
    for row_index, row in enumerate(prepared):
        assignments: dict[str, Expression] = {}
        for column_index, name in enumerate(keys):
            attribute = attributes[name]
            value = attribute._coerce(row[name])
            bind_name = f"orm_bulk_row_{row_index}_{column_index}"
            if isinstance(attribute.type_, Geometry) and (
                value is not None or provider in _SPATIAL_NULL_WRAPPER_PROVIDERS
            ):
                _require_geometry_mapping(attribute.type_, provider)
                assignments[name] = _spatial_value(
                    bind(bind_name, BindType.BINARY),
                    attribute.type_.srid,
                    attribute.type_.semantics,
                )
                parameters[bind_name] = _geometry_parameter_value(value, provider)
            else:
                assignments[name] = _attribute_bind(attribute, bind_name)
                parameters[bind_name] = _attribute_parameter(attribute, value)
        statement = statement.values(**assignments)
    if conflicts is not None:
        statement = statement.on_conflict(
            *(mapper.table.c[name] for name in conflicts)
        )
        if update_values:
            updates: dict[str, Expression] = {}
            for index, (name, raw_value) in enumerate(update_values.items()):
                attribute = attributes.get(name)
                if attribute is None or attribute.primary_key:
                    raise OrmMappingError("update_values bulk contiene una colonna non valida")
                value = attribute._coerce(raw_value)
                bind_name = f"orm_bulk_update_{index}"
                updates[name] = _attribute_bind(attribute, bind_name)
                parameters[bind_name] = _attribute_parameter(attribute, value)
            statement = statement.set(**updates)
    if provider == "sqlserver" and len(parameters) > 2_100:
        raise OrmUnsupportedError(
            "bulk mapping SQL Server oltre il limite qualificato di bind"
        )
    return statement, parameters, len(prepared)


def _affected_rows(value: Any) -> int:
    if isinstance(value, MutationResult):
        value = value.affected_rows
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise OrmStateError("bulk DML ORM non ha restituito un row count valido")
    return value


def _query_relationship(
    mapper: Mapper, value: str | Relationship[Any]
) -> Relationship[Any]:
    relation = mapper.relationship(value) if isinstance(value, str) else value
    if relation not in mapper.relationships:
        raise OrmMappingError("relationship appartenente a un altro mapper")
    return relation


def _require_stable_partition_order(
    mapper: Mapper, orderings: tuple[Ordering, ...]
) -> None:
    ordered_columns = {
        ordering.expression.name
        for ordering in orderings
        if isinstance(ordering.expression, Column)
        and ordering.expression.table is mapper.table
    }
    primary_names = {attribute.name for attribute in mapper.primary_keys}
    if not primary_names <= ordered_columns:
        raise OrmStateError(
            "lettura ORM a partizioni richiede tutte le chiavi primarie in order_by"
        )


def _require_database_delete_cascade(
    owner_mapper: Mapper, relation: Relationship[Any]
) -> None:
    relation._validate_configuration()
    if relation.direction not in {"one-to-many", "one-to-one"}:
        raise OrmUnsupportedError(
            "passive_deletes richiede una foreign key del figlio con ON DELETE CASCADE"
        )
    target_mapper = _mapper(relation.target)
    owner_keys = tuple(attribute.name or "" for attribute in owner_mapper.primary_keys)
    for constraint in target_mapper.constraints:
        if not isinstance(constraint, ForeignKeyConstraint):
            continue
        target = _constraint_target(constraint, target_mapper.model.__registry__)
        if (
            target.model is owner_mapper.model
            and constraint.columns == relation.foreign_keys
            and constraint.target_columns == owner_keys
            and constraint.on_delete == "CASCADE"
        ):
            return
    raise OrmUnsupportedError(
        "passive_deletes privo di una foreign key dichiarata ON DELETE CASCADE"
    )


def _query_loader_path(
    mapper: Mapper,
    value: Relationship[Any] | tuple[Relationship[Any], ...],
) -> tuple[Relationship[Any], ...]:
    values = (value,) if isinstance(value, Relationship) else value
    if not isinstance(values, tuple) or not values:
        raise TypeError("loader ORM richiede un percorso di relationship")
    path: list[Relationship[Any]] = []
    current = mapper
    for relation in values:
        if not isinstance(relation, Relationship):
            raise TypeError("loader ORM richiede un percorso di relationship")
        relation = _query_relationship(current, relation)
        relation._validate_configuration()
        path.append(relation)
        current = _mapper(relation.target)
    return tuple(path)


def _loaded_relationship_instances(
    instances: list[DeclarativeBase], relation: Relationship[Any]
) -> list[DeclarativeBase]:
    name = relation.name
    if name is None:
        raise OrmMappingError("relationship senza nome")
    related: list[DeclarativeBase] = []
    seen: set[int] = set()
    for instance in instances:
        value = instance.__dict__.get(name)
        values = tuple(value) if relation.uselist and value is not None else (value,)
        for item in values:
            if item is not None and id(item) not in seen:
                seen.add(id(item))
                related.append(item)
    return related


def _mapped_row_values(
    mapper: Mapper,
    row: Mapping[str, Any],
    prefix: str,
    provider: str,
) -> dict[str, Any]:
    values = {
        attribute.name: row[f"{prefix}{attribute.name}"]
        for attribute in mapper.attributes
        if attribute.name is not None
    }
    if provider in _FRAMED_ORM_PROVIDERS:
        for attribute in mapper.attributes:
            if isinstance(attribute.type_, Geometry) and attribute.name is not None:
                values[_geometry_srid_alias(attribute.name)] = row[
                    _geometry_srid_alias(f"{prefix}{attribute.name}")
                ]
    return _normalize_geometry_row(mapper, values, provider)


def _normalize_geometry_row(
    mapper: Mapper, row: Mapping[str, Any], provider: str
) -> dict[str, Any]:
    values = dict(row)
    if provider != "db2":
        return values
    for attribute in mapper.attributes:
        name = attribute.name
        if not isinstance(attribute.type_, Geometry) or name is None:
            continue
        value = values.get(name)
        if isinstance(value, str):
            try:
                values[name] = bytes.fromhex(value)
            except ValueError as error:
                raise OrmMappingError("WKB Geometry ORM Db2 non valido") from error
    return values


def _entity_projection_values(
    mapper: Mapper, row: Mapping[str, Any], index: int, provider: str
) -> dict[str, Any]:
    return _mapped_row_values(mapper, row, f"orm_entity_{index}_", provider)


def _validate_geometry_row(
    mapper: Mapper, row: Mapping[str, Any], provider: str
) -> None:
    if provider not in _FRAMED_ORM_PROVIDERS:
        return
    for attribute in mapper.attributes:
        type_ = attribute.type_
        name = attribute.name
        if not isinstance(type_, Geometry) or name is None:
            continue
        srid_name = _geometry_srid_alias(name)
        if name not in row or srid_name not in row:
            raise OrmMappingError("risultato privo del frame Geometry ORM")
        value = row.get(name)
        observed = row.get(srid_name)
        if value is None:
            if observed is not None:
                raise OrmMappingError("SRID presente per una Geometry ORM NULL")
            continue
        if (
            not isinstance(observed, int)
            or isinstance(observed, bool)
            or observed != type_.srid
        ):
            raise OrmMappingError("SRID Geometry ORM diverso dal mapping dichiarato")


def _projected_entity_is_null(mapper: Mapper, values: Mapping[str, Any]) -> bool:
    return all(values.get(attribute.name) is None for attribute in mapper.primary_keys)


def _relationship_join_predicate(
    mapper: Mapper, relation: Relationship[Any]
) -> Predicate:
    target_mapper = _mapper(relation.target)
    direction = relation.direction
    if direction == "many-to-one":
        foreign = tuple(mapper.attribute(name).column for name in relation.foreign_keys)
        primary = tuple(item.column for item in target_mapper.primary_keys)
    else:
        foreign = tuple(
            target_mapper.attribute(name).column for name in relation.foreign_keys
        )
        primary = tuple(item.column for item in mapper.primary_keys)
    if any(item is None for item in (*foreign, *primary)) or len(foreign) != len(
        primary
    ):
        raise OrmMappingError("relationship priva delle colonne di join")
    predicates = tuple(
        left == right
        for left, right in zip(foreign, primary, strict=True)
        if left is not None and right is not None
    )
    return predicates[0] if len(predicates) == 1 else and_(*predicates)


def _relationship_keys(
    value: str | tuple[str, ...] | None,
) -> tuple[str, ...]:
    if value is None:
        return ()
    values = (value,) if isinstance(value, str) else value
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(item, str) or not item for item in values)
        or len(set(values)) != len(values)
    ):
        raise OrmMappingError("chiavi secondary non valide")
    return values


def _primary_columns(mapper: Mapper) -> tuple[Column, ...]:
    columns = tuple(item.column for item in mapper.primary_keys)
    if any(column is None for column in columns):
        raise OrmMappingError("mapper privo delle colonne primary key")
    return tuple(column for column in columns if column is not None)


def _secondary_columns(
    relation: Relationship[Any], *, remote: bool
) -> tuple[Column, ...]:
    secondary = relation.secondary
    if secondary is None:
        raise OrmMappingError("relationship many-to-many senza secondary")
    keys = relation.secondary_remote_keys if remote else relation.secondary_local_keys
    return tuple(secondary.c[key] for key in keys)


def _column_equality(left: tuple[Column, ...], right: tuple[Column, ...]) -> Predicate:
    if not left or len(left) != len(right):
        raise OrmMappingError("join many-to-many con arita incoerente")
    predicates = tuple(one == two for one, two in zip(left, right, strict=True))
    return predicates[0] if len(predicates) == 1 else and_(*predicates)


def _identity_disjunction(
    columns: tuple[Column, ...],
    identities: tuple[tuple[Any, ...], ...],
    prefix: str,
) -> tuple[Predicate, dict[str, Any]]:
    if (
        not columns
        or not identities
        or any(len(identity) != len(columns) for identity in identities)
    ):
        raise OrmMappingError("identita composita non coerente con le colonne")
    parameters: dict[str, Any] = {}
    alternatives: list[Predicate] = []
    for row_index, identity in enumerate(identities):
        terms: list[Predicate] = []
        for column_index, (column, value) in enumerate(
            zip(columns, identity, strict=True)
        ):
            name = f"{prefix}_{row_index}_{column_index}"
            parameters[name] = value
            terms.append(column == bind(name, _bind_type_for_value(value)))
        alternatives.append(terms[0] if len(terms) == 1 else and_(*terms))
    return (
        alternatives[0] if len(alternatives) == 1 else or_(*alternatives),
        parameters,
    )


def _join_statement(
    mapper: Mapper,
    statement: SelectStatement,
    value: str | Relationship[Any] | type[DeclarativeBase],
    on: Predicate | None,
    kind: str,
) -> SelectStatement:
    if isinstance(value, type):
        target_mapper = _mapper(value)
        if on is None:
            raise OrmMappingError("join verso un modello richiede on esplicito")
        return statement.select_from(mapper.table).join(
            target_mapper.table, on, kind=kind
        )
    relation = _query_relationship(mapper, value)
    relation._validate_configuration()
    target_mapper = _mapper(relation.target)
    if relation.secondary is not None:
        if on is not None:
            raise OrmMappingError("join many-to-many non accetta on esplicito")
        secondary = relation.secondary
        return (
            statement.select_from(mapper.table)
            .join(
                secondary,
                _column_equality(
                    _primary_columns(mapper),
                    _secondary_columns(relation, remote=False),
                ),
                kind=kind,
            )
            .join(
                target_mapper.table,
                _column_equality(
                    _secondary_columns(relation, remote=True),
                    _primary_columns(target_mapper),
                ),
                kind=kind,
            )
        )
    predicate = _relationship_join_predicate(mapper, relation) if on is None else on
    return statement.select_from(mapper.table).join(
        target_mapper.table, predicate, kind=kind
    )


def _loader_prefix(relation: Relationship[Any]) -> str:
    if relation.name is None:
        raise OrmMappingError("relationship senza nome")
    return f"orm_eager_{relation.name}_"


def _joinedload_statement(
    mapper: Mapper,
    statement: SelectStatement,
    relation: Relationship[Any],
    provider: str,
) -> SelectStatement:
    joined = _join_statement(mapper, statement, relation, None, "left")
    projections = (
        *joined.projections,
        *_orm_projections(_mapper(relation.target), _loader_prefix(relation), provider),
    )
    return replace(joined, projections=projections)


def _assign_loaded_relationship(
    instance: DeclarativeBase,
    relation: Relationship[Any],
    value: DeclarativeBase | list[DeclarativeBase] | None,
) -> None:
    if relation.name is None:
        raise OrmMappingError("relationship senza nome")
    if relation.uselist:
        collection = _RelationshipCollection(instance, relation)
        for item in value or []:  # type: ignore[union-attr]
            collection._append_from_backref(item)
            if relation.back_populates is not None:
                inverse = _mapper(relation.target).relationship(relation.back_populates)
                inverse._assign_from_backref(item, instance)
        instance.__dict__[relation.name] = collection
        _remember_relationship(instance, relation)
        return
    instance.__dict__[relation.name] = value
    if value is not None and relation.back_populates is not None:
        inverse = _mapper(relation.target).relationship(relation.back_populates)
        inverse._assign_from_backref(value, instance)


def _append_joined_relationship(
    instance: DeclarativeBase,
    relation: Relationship[Any],
    value: DeclarativeBase | None,
) -> None:
    if relation.name is None or not relation.uselist:
        raise OrmMappingError("accumulo joinedload richiede una collezione")
    collection = instance.__dict__.get(relation.name)
    if not isinstance(collection, _RelationshipCollection):
        collection = _RelationshipCollection(instance, relation)
        instance.__dict__[relation.name] = collection
    if value is not None:
        collection._append_from_backref(value)
        if relation.back_populates is not None:
            inverse = _mapper(relation.target).relationship(relation.back_populates)
            inverse._assign_from_backref(value, instance)
    _remember_relationship(instance, relation)


def _loaded_relationship_values(
    instance: DeclarativeBase, relation: Relationship[Any]
) -> tuple[DeclarativeBase, ...]:
    if relation.name is None or relation.name not in instance.__dict__:
        return ()
    value = instance.__dict__[relation.name]
    if value is None:
        return ()
    if relation.uselist:
        return tuple(value)
    return (value,)


def _remember_relationship(
    instance: DeclarativeBase, relation: Relationship[Any]
) -> None:
    if relation.name is None:
        raise OrmMappingError("relationship senza nome")
    _state(instance).relationship_original[relation.name] = tuple(
        _identity(_mapper(relation.target), related)
        for related in _loaded_relationship_values(instance, relation)
    )


def _association_signature(
    relation: Relationship[Any],
    local_value: tuple[Any, ...],
    remote_value: tuple[Any, ...],
) -> tuple[Any, ...]:
    pairs = tuple(zip(relation.secondary_local_keys, local_value, strict=True)) + tuple(
        zip(relation.secondary_remote_keys, remote_value, strict=True)
    )
    return (id(relation.secondary), *sorted(pairs, key=lambda item: item[0]))


def _association_values(
    relation: Relationship[Any],
    local_identity: tuple[Any, ...],
    remote_identity: tuple[Any, ...],
) -> tuple[dict[str, Any], dict[str, Any]]:
    assignments: dict[str, Any] = {}
    parameters: dict[str, Any] = {}
    for role, keys, identity in (
        ("local", relation.secondary_local_keys, local_identity),
        ("remote", relation.secondary_remote_keys, remote_identity),
    ):
        if len(keys) != len(identity):
            raise OrmMappingError("identita many-to-many con arita incoerente")
        for index, (key, value) in enumerate(zip(keys, identity, strict=True)):
            name = f"orm_link_{role}_{index}"
            assignments[key] = bind(name, _bind_type_for_value(value))
            parameters[name] = value
    return assignments, parameters


def _association_predicate(
    relation: Relationship[Any],
    local_identity: tuple[Any, ...],
    remote_identity: tuple[Any, ...],
) -> tuple[Predicate, dict[str, Any]]:
    assignments, parameters = _association_values(
        relation, local_identity, remote_identity
    )
    secondary = relation.secondary
    if secondary is None:
        raise OrmMappingError("relationship many-to-many senza secondary")
    terms = tuple(
        secondary.c[key] == expression for key, expression in assignments.items()
    )
    return (terms[0] if len(terms) == 1 else and_(*terms), parameters)


def _association_owner_predicate(
    relation: Relationship[Any], owner_identity: tuple[Any, ...]
) -> tuple[Predicate, dict[str, Any]]:
    secondary = relation.secondary
    keys = relation.secondary_local_keys
    if secondary is None or len(keys) != len(owner_identity):
        raise OrmMappingError("identita owner many-to-many con arita incoerente")
    parameters: dict[str, Any] = {}
    terms: list[Predicate] = []
    for index, (key, value) in enumerate(zip(keys, owner_identity, strict=True)):
        name = f"orm_link_owner_{index}"
        parameters[name] = value
        terms.append(
            secondary.c[key] == bind(name, _bind_type_for_value(value))
        )
    return (terms[0] if len(terms) == 1 else and_(*terms), parameters)


def _attribute_bind(attribute: MappedColumn[Any], name: str) -> Expression:
    return bind(name, _bind_type_for_mapping(attribute.type_))


def _bind_type_for_mapping(type_: Any) -> BindType:
    if isinstance(type_, BigInteger):
        return BindType.BIG_INTEGER
    if isinstance(type_, Geometry) or type_ is bytes:
        return BindType.BINARY
    if isinstance(type_, Numeric) or type_ is Decimal:
        return BindType.DECIMAL
    if isinstance(type_, Uuid):
        return BindType.UUID
    if isinstance(type_, Json):
        return BindType.JSON
    if isinstance(type_, DateTime):
        return BindType.TIMESTAMP_TZ if type_.timezone else BindType.TIMESTAMP
    if isinstance(type_, String):
        return BindType.STRING
    if type_ is bool:
        return BindType.BOOLEAN
    if type_ is int:
        return BindType.INTEGER
    if type_ is float:
        return BindType.FLOAT
    if type_ is date:
        return BindType.DATE
    if type_ is datetime:
        return BindType.TIMESTAMP
    return BindType.STRING


def _bind_type_for_value(value: Any) -> BindType:
    if isinstance(value, bool):
        return BindType.BOOLEAN
    if isinstance(value, int):
        return BindType.BIG_INTEGER if not -(2**31) <= value < 2**31 else BindType.INTEGER
    if isinstance(value, float):
        return BindType.FLOAT
    if isinstance(value, Decimal):
        return BindType.DECIMAL
    if isinstance(value, UUIDValue):
        return BindType.UUID
    if isinstance(value, (dict, list)):
        return BindType.JSON
    if isinstance(value, (bytes, bytearray, SpatialReference)):
        return BindType.BINARY
    if isinstance(value, datetime):
        return (
            BindType.TIMESTAMP_TZ
            if value.tzinfo is not None and value.utcoffset() is not None
            else BindType.TIMESTAMP
        )
    if isinstance(value, date):
        return BindType.DATE
    return BindType.STRING


def _attribute_parameter(attribute: MappedColumn[Any], value: Any) -> Any:
    if isinstance(attribute.type_, BigInteger) and value is not None:
        return typed_int64(value)
    if (isinstance(attribute.type_, Numeric) or attribute.type_ is Decimal) and value is not None:
        return typed_decimal(format(value, "f"))
    if isinstance(attribute.type_, Uuid) and value is not None:
        return typed_uuid(str(value))
    if isinstance(attribute.type_, DateTime) and value is not None:
        encoded = value.isoformat()
        return (
            typed_timestamptz(encoded)
            if attribute.type_.timezone
            else typed_timestamp(encoded)
        )
    if attribute.type_ is datetime and value is not None:
        return typed_timestamp(value.isoformat())
    if attribute.type_ is date and value is not None:
        return typed_date(value.isoformat())
    return value


def _snapshot(mapper: Mapper, instance: DeclarativeBase) -> dict[str, Any]:
    return {
        attribute.name: instance.__dict__.get(attribute.name)
        for attribute in mapper.attributes
        if attribute.name is not None
    }


def _relationship_snapshot(mapper: Mapper, instance: DeclarativeBase) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    for relation in mapper.relationships:
        if relation.name is None or relation.name not in instance.__dict__:
            continue
        value = instance.__dict__[relation.name]
        snapshot[relation.name] = tuple(value) if relation.uselist else value
    return snapshot


def _validate_savepoint_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("nome savepoint ORM non valido")


def _tracked_instances(session: OrmSession) -> tuple[DeclarativeBase, ...]:
    values = (
        *session._identity_map.values(),
        *session._pending,
        *session._deleted,
        *session._flushed_deleted,
    )
    return tuple(dict.fromkeys(values))


def _expunge_loaded_graph(
    session: OrmSession, instance: DeclarativeBase, seen: set[int]
) -> None:
    if id(instance) in seen:
        return
    seen.add(id(instance))
    mapper = _mapper(type(instance))
    for relation in mapper.relationships:
        if relation.name is None or relation.name not in instance.__dict__:
            continue
        for related in _loaded_relationship_values(instance, relation):
            _expunge_loaded_graph(session, related, seen)
    if _state(instance).session is session:
        session.expunge(instance)


def _capture_savepoint(session: OrmSession) -> _SavepointSnapshot:
    instances: dict[DeclarativeBase, _SavepointInstance] = {}
    for instance in _tracked_instances(session):
        mapper = _mapper(type(instance))
        state = _state(instance)
        instances[instance] = _SavepointInstance(
            values={
                attribute.name: instance.__dict__[attribute.name]
                for attribute in mapper.attributes
                if attribute.name is not None and attribute.name in instance.__dict__
            },
            relationships=_relationship_snapshot(mapper, instance),
            status=state.status,
            original=dict(state.original),
            dirty=set(state.dirty),
            expired=set(state.expired),
            relationship_original=dict(state.relationship_original),
        )
    return _SavepointSnapshot(
        identity_map=dict(session._identity_map),
        pending=list(session._pending),
        deleted=list(session._deleted),
        flushed_deleted=list(session._flushed_deleted),
        instances=instances,
    )


def _restore_savepoint(session: OrmSession, snapshot: _SavepointSnapshot) -> None:
    captured = set(snapshot.instances)
    for instance in _tracked_instances(session):
        if instance in captured:
            continue
        state = _state(instance)
        state.status = (
            ObjectState.TRANSIENT
            if state.status is ObjectState.PENDING
            else ObjectState.DETACHED
        )
        state.session = None
        state.dirty.clear()
        state.expired.clear()
    for instance, saved in snapshot.instances.items():
        mapper = _mapper(type(instance))
        mapped_names = {
            attribute.name
            for attribute in mapper.attributes
            if attribute.name is not None
        }
        relationship_names = {
            relation.name
            for relation in mapper.relationships
            if relation.name is not None
        }
        for name in (*mapped_names, *relationship_names):
            instance.__dict__.pop(name, None)
        instance.__dict__.update(saved.values)
        for relation in mapper.relationships:
            name = relation.name
            if name is None or name not in saved.relationships:
                continue
            value = saved.relationships[name]
            if relation.uselist:
                collection = _RelationshipCollection(instance, relation)
                for item in value:
                    collection._append_from_backref(item)
                instance.__dict__[name] = collection
            else:
                instance.__dict__[name] = value
        state = _state(instance)
        state.status = saved.status
        state.session = session
        state.original = dict(saved.original)
        state.dirty = set(saved.dirty)
        state.expired = set(saved.expired)
        state.relationship_original = dict(saved.relationship_original)
    session._identity_map = dict(snapshot.identity_map)
    session._pending = list(snapshot.pending)
    session._deleted = list(snapshot.deleted)
    session._flushed_deleted = list(snapshot.flushed_deleted)
    session._deferred_foreign_keys.clear()


def _capture_loaded_relationship(
    instance: DeclarativeBase, relation: Relationship[Any]
) -> None:
    if relation.name is None:
        raise OrmMappingError("relationship senza nome")
    state = _state(instance)
    if relation.name in state.rollback_relationships:
        return
    value = instance.__dict__.get(relation.name)
    state.rollback_relationships[relation.name] = (
        tuple(value) if relation.uselist else value
    )


def _identity_predicate(
    mapper: Mapper, instance: DeclarativeBase
) -> tuple[Any, dict[str, Any]]:
    return _identity_values_predicate(mapper, _identity(mapper, instance))


def _identity_values_predicate(
    mapper: Mapper, identity: tuple[Any, ...]
) -> tuple[Predicate, dict[str, Any]]:
    predicate: Predicate | None = None
    parameters: dict[str, Any] = {}
    for index, (attribute, value) in enumerate(
        zip(mapper.primary_keys, identity, strict=True)
    ):
        column = attribute.column
        if column is None:
            raise OrmMappingError("chiave primaria non associata")
        name = f"orm_primary_key_{index}"
        current = column == _attribute_bind(attribute, name)
        predicate = current if predicate is None else predicate & current
        parameters[name] = _attribute_parameter(attribute, value)
    if predicate is None:
        raise OrmMappingError("mapper privo di chiave primaria")
    return predicate, parameters


def _table_identity_predicate(
    mapper: Mapper, table: Table, identity: tuple[Any, ...]
) -> tuple[Predicate, dict[str, Any]]:
    predicate: Predicate | None = None
    parameters: dict[str, Any] = {}
    for index, (attribute, value) in enumerate(
        zip(mapper.primary_keys, identity, strict=True)
    ):
        if attribute.name is None:
            raise OrmMappingError("chiave primaria senza nome")
        name = f"orm_primary_key_{index}"
        current = table.c[attribute.name] == _attribute_bind(attribute, name)
        predicate = current if predicate is None else predicate & current
        parameters[name] = _attribute_parameter(attribute, value)
    if predicate is None:
        raise OrmMappingError("mapper privo di chiave primaria")
    return predicate, parameters


def _session_provider(session: Any) -> str:
    capabilities = getattr(session, "capabilities", None)
    provider = (
        capabilities.get("provider") if isinstance(capabilities, Mapping) else None
    )
    if provider not in {"postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2"}:
        raise OrmUnsupportedError("provider DDL ORM non qualificato")
    return provider


def _execute_ddl(session: Any, statement: str) -> None:
    execute = getattr(session, "execute_ddl", None)
    if not callable(execute):
        execute = getattr(session, "execute", None)
    if not callable(execute):
        raise TypeError("sessione priva di una superficie DDL")
    execute(statement)


async def _execute_ddl_async(session: Any, statement: str) -> None:
    execute = getattr(session, "execute_ddl", None)
    if not callable(execute):
        execute = getattr(session, "execute", None)
    if not callable(execute):
        raise TypeError("sessione priva di una superficie DDL")
    outcome = execute(statement)
    if isawaitable(outcome):
        await outcome


def _quote_identifier(value: str, provider: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise OrmMappingError("identificatore DDL non valido")
    if provider in {"mysql", "mariadb"}:
        return f"`{value.replace('`', '``')}`"
    if provider == "sqlserver":
        return f"[{value.replace(']', ']]')}]"
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _object_name(table: Table) -> str:
    return ".".join(
        item for item in (table.catalog, table.schema, table.name) if item is not None
    )


def _qualified_table(table: Table, provider: str) -> str:
    return ".".join(
        _quote_identifier(item, provider)
        for item in (table.catalog, table.schema, table.name)
        if item is not None
    )


def _ddl_type(attribute: MappedColumn[Any], provider: str) -> str:
    type_ = attribute.type_
    if isinstance(type_, BigInteger):
        return "NUMBER(19)" if provider == "oracle" else "BIGINT"
    if isinstance(type_, String):
        if type_.length is not None:
            prefix = (
                "NVARCHAR"
                if provider == "sqlserver"
                else "VARCHAR2"
                if provider == "oracle"
                else "VARCHAR"
            )
            return f"{prefix}({type_.length})"
        type_ = str
    if isinstance(type_, Numeric):
        numeric = "NUMBER" if provider == "oracle" else "DECIMAL"
        return f"{numeric}({type_.precision}, {type_.scale})"
    if isinstance(type_, Uuid):
        return (
            "UUID"
            if provider == "postgres"
            else "UNIQUEIDENTIFIER"
            if provider == "sqlserver"
            else "VARCHAR2(36)"
            if provider == "oracle"
            else "CHAR(36)"
        )
    if isinstance(type_, Json):
        return (
            "JSONB"
            if provider == "postgres"
            else "JSON"
            if provider in {"mysql", "mariadb"}
            else "NVARCHAR(MAX)"
            if provider == "sqlserver"
            else "JSON"
            if provider == "oracle"
            else "CLOB"
        )
    if isinstance(type_, DateTime):
        if not type_.timezone:
            return "TIMESTAMP" if provider != "sqlserver" else "DATETIME2"
        if provider == "postgres":
            return "TIMESTAMPTZ"
        if provider == "sqlserver":
            return "DATETIMEOFFSET"
        raise OrmUnsupportedError(
            "DateTime timezone ORM non qualificato per il provider"
        )
    if isinstance(type_, Geometry):
        _require_geometry_mapping(type_, provider)
        base = type_.semantics
        geometry_type = type_.geometry_type or "geometry"
        if provider == "mysql":
            return f"{geometry_type.upper()} SRID {type_.srid}"
        if provider == "mariadb":
            # MariaDB non ammette l'attributo SRID di colonna. Il frame viene
            # comunque scritto come secondo argomento e verificato per riga
            # con ST_SRID durante l'idratazione ORM.
            return geometry_type.upper()
        if provider == "sqlserver":
            return base
        if provider == "db2":
            return "ST_GEOMETRY"
        if provider == "oracle":
            return "MDSYS.SDO_GEOMETRY"
        return f"{base}({geometry_type}, {type_.srid})"
    mapping = {
        int: "NUMBER(10)" if provider == "oracle" else "INTEGER",
        str: (
            "VARCHAR(255)"
            if provider in {"mysql", "mariadb"}
            or attribute.primary_key
            or attribute.unique
            else "VARCHAR2(4000)"
            if provider == "oracle"
            else "TEXT"
            if provider == "postgres"
            else "VARCHAR(32672)"
            if provider == "db2"
            else "NVARCHAR(255)"
        ),
        bool: "BIT" if provider == "sqlserver" else "BOOLEAN",
        float: "BINARY_DOUBLE"
        if provider == "oracle"
        else "DOUBLE PRECISION"
        if provider == "postgres"
        else "DOUBLE"
        if provider != "sqlserver"
        else "FLOAT",
        bytes: "BYTEA"
        if provider == "postgres"
        else "VARBINARY(MAX)"
        if provider == "sqlserver"
        else "BLOB",
        datetime: "TIMESTAMP" if provider != "sqlserver" else "DATETIME2",
        date: "DATE",
        time: "TIME",
        Decimal: "NUMBER(38, 10)" if provider == "oracle" else "DECIMAL(38, 10)",
    }
    if type_ is time and provider == "oracle":
        raise OrmUnsupportedError("tipo TIME Oracle non qualificato")
    try:
        return mapping[type_]
    except KeyError as error:
        raise OrmUnsupportedError("tipo DDL ORM non qualificato") from error


def _render_server_default(default: ServerDefault, provider: str) -> str:
    if default.kind == "current_timestamp":
        return "CURRENT_TIMESTAMP"
    value = default.value
    if isinstance(value, bool):
        if provider == "sqlserver":
            return "1" if value else "0"
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39) * 2)}'"
    raise OrmUnsupportedError("server default DDL non qualificato")


def _constraint_target(constraint: ForeignKeyConstraint, registry: Registry) -> Mapper:
    model = (
        registry._resolve(constraint.target)
        if isinstance(constraint.target, str)
        else constraint.target
    )
    mapper = registry.mapper_for(model)
    names = {attribute.name for attribute in mapper.attributes}
    if not set(constraint.target_columns) <= names:
        raise OrmMappingError(
            "ForeignKeyConstraint riferisce una colonna remota assente"
        )
    return mapper


def _ddl_mapper_order(
    mappers: tuple[Mapper, ...], registry: Registry
) -> tuple[Mapper, ...]:
    selected = {mapper.model: mapper for mapper in mappers}
    dependencies: dict[type[DeclarativeBase], set[type[DeclarativeBase]]] = {
        model: set() for model in selected
    }
    for mapper in mappers:
        if (
            mapper.inheritance == "joined"
            and mapper.inherits is not None
            and mapper.inherits.model in selected
        ):
            dependencies[mapper.model].add(mapper.inherits.model)
        for constraint in mapper.constraints:
            if isinstance(constraint, ForeignKeyConstraint):
                target = _constraint_target(constraint, registry)
                if target.model in selected and target.model is not mapper.model:
                    dependencies[mapper.model].add(target.model)
    ordered: list[Mapper] = []
    while dependencies:
        ready = tuple(model for model, values in dependencies.items() if not values)
        if not ready:
            raise OrmUnsupportedError("ciclo DDL fra foreign key non supportato")
        for model in ready:
            ordered.append(selected[model])
            dependencies.pop(model)
        for values in dependencies.values():
            values.difference_update(ready)
    return tuple(ordered)


def _unique_table_mappers(mappers: tuple[Mapper, ...]) -> tuple[Mapper, ...]:
    unique: list[Mapper] = []
    seen: set[int] = set()
    for mapper in mappers:
        if id(mapper.table) not in seen:
            seen.add(id(mapper.table))
            unique.append(mapper)
    return tuple(unique)


def _create_table_ddl(
    mapper: Mapper,
    provider: str,
    registry: Registry,
    *,
    checkfirst: bool,
) -> str:
    columns: list[str] = []
    ddl_attributes = (
        (*mapper.primary_keys, *mapper.local_attributes)
        if mapper.inheritance == "joined"
        else _single_table_attributes(_inheritance_root(mapper))
    )
    for attribute in ddl_attributes:
        if attribute.name is None:
            raise OrmMappingError("colonna DDL senza nome")
        declaration = f"{_quote_identifier(attribute.name, provider)} {_ddl_type(attribute, provider)}"
        if attribute.generated and mapper.inheritance != "joined":
            if provider == "oracle":
                raise OrmUnsupportedError(
                    "identity generated Oracle non qualificata dalla superficie ORM"
                )
            declaration += {
                "postgres": " GENERATED BY DEFAULT AS IDENTITY",
                "mysql": " AUTO_INCREMENT",
                "mariadb": " AUTO_INCREMENT",
                "sqlserver": " IDENTITY(1,1)",
                "db2": " GENERATED BY DEFAULT AS IDENTITY",
            }[provider]
        if not attribute.nullable:
            declaration += " NOT NULL"
        if attribute.server_default:
            if attribute.server_default_spec is None:
                raise OrmUnsupportedError(
                    "DDL richiede ServerDefault esplicito, non il solo marker True"
                )
            declaration += f" DEFAULT {_render_server_default(attribute.server_default_spec, provider)}"
        columns.append(declaration)
    primary = ", ".join(
        _quote_identifier(attribute.name or "", provider)
        for attribute in mapper.primary_keys
    )
    columns.append(f"PRIMARY KEY ({primary})")
    if mapper.inheritance == "joined":
        if mapper.inherits is None:
            raise OrmMappingError("mapper joined privo di base")
        remote = ", ".join(
            _quote_identifier(attribute.name or "", provider)
            for attribute in mapper.inherits.primary_keys
        )
        columns.append(
            f"FOREIGN KEY ({primary}) REFERENCES "
            f"{_qualified_table(mapper.inherits.table, provider)} ({remote}) "
            "ON DELETE CASCADE"
        )
    for constraint in mapper.constraints:
        name = (
            ""
            if constraint.name is None
            else f"CONSTRAINT {_quote_identifier(constraint.name, provider)} "
        )
        local = ", ".join(
            _quote_identifier(item, provider) for item in constraint.columns
        )
        if isinstance(constraint, UniqueConstraint):
            columns.append(f"{name}UNIQUE ({local})")
            continue
        if isinstance(constraint, CheckConstraint):
            operator = "<>" if constraint.operator == "!=" else constraint.operator
            literal = _render_server_default(
                ServerDefault.literal(constraint.value), provider
            )
            columns.append(
                f"{name}CHECK ({_quote_identifier(constraint.column, provider)} "
                f"{operator} {literal})"
            )
            continue
        if isinstance(constraint, OrmIndex):
            continue
        target = _constraint_target(constraint, registry)
        remote = ", ".join(
            _quote_identifier(item, provider) for item in constraint.target_columns
        )
        suffix = (
            "" if constraint.on_delete is None else f" ON DELETE {constraint.on_delete}"
        )
        if constraint.on_update is not None:
            if provider == "oracle":
                raise OrmUnsupportedError(
                    "Oracle non supporta ON UPDATE nelle foreign key"
                )
            suffix += f" ON UPDATE {constraint.on_update}"
        columns.append(
            f"{name}FOREIGN KEY ({local}) REFERENCES "
            f"{_qualified_table(target.table, provider)} ({remote}){suffix}"
        )
    target = _qualified_table(mapper.table, provider)
    body = ", ".join(columns)
    if checkfirst and provider == "sqlserver":
        object_name = _object_name(mapper.table).replace("'", "''")
        return f"IF OBJECT_ID(N'{object_name}', N'U') IS NULL CREATE TABLE {target} ({body})"
    clause = " IF NOT EXISTS" if checkfirst else ""
    if checkfirst and provider in {"oracle", "db2"}:
        raise OrmUnsupportedError(
            "il provider non qualifica CREATE TABLE IF NOT EXISTS"
        )
    return f"CREATE TABLE{clause} {target} ({body})"


def _create_index_ddl(
    mapper: Mapper,
    index: OrmIndex,
    provider: str,
    *,
    checkfirst: bool,
) -> str:
    target = _qualified_table(mapper.table, provider)
    name = _quote_identifier(index.name, provider)
    columns = ", ".join(
        _quote_identifier(column, provider) for column in index.columns
    )
    unique = "UNIQUE " if index.unique else ""
    attributes = {
        attribute.name: attribute
        for attribute in _single_table_attributes(_inheritance_root(mapper))
        if attribute.name is not None
    }
    spatial = [
        attributes[column]
        for column in index.columns
        if column in attributes and isinstance(attributes[column].type_, Geometry)
    ]
    if spatial:
        if provider != "oracle":
            statement = f"CREATE {unique}INDEX {name} ON {target} ({columns})"
        else:
            if index.unique or len(index.columns) != 1 or len(spatial) != 1:
                raise OrmUnsupportedError(
                    "un indice Spatial Oracle deve essere non univoco e monocolonna"
                )
            statement = (
                f"CREATE INDEX {name} ON {target} ({columns}) "
                "INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2"
            )
    else:
        statement = f"CREATE {unique}INDEX {name} ON {target} ({columns})"
    if not checkfirst:
        return statement
    if provider == "postgres":
        return f"CREATE {unique}INDEX IF NOT EXISTS {name} ON {target} ({columns})"
    if provider == "sqlserver":
        object_name = _object_name(mapper.table).replace("'", "''")
        index_name = index.name.replace("'", "''")
        return (
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = "
            f"OBJECT_ID(N'{object_name}') AND name = N'{index_name}') {statement}"
        )
    raise OrmUnsupportedError(
        "il provider non qualifica CREATE INDEX idempotente"
    )


def _oracle_spatial_metadata_ddl(mapper: Mapper) -> tuple[str, ...]:
    """Registra nel catalogo Oracle il CRS dichiarato dal mapping ORM."""

    attributes = (
        (*mapper.primary_keys, *mapper.local_attributes)
        if mapper.inheritance == "joined"
        else _single_table_attributes(_inheritance_root(mapper))
    )
    spatial_attributes = tuple(
        attribute for attribute in attributes if isinstance(attribute.type_, Geometry)
    )
    if not spatial_attributes:
        return ()
    if mapper.table.catalog is not None:
        raise OrmUnsupportedError(
            "metadata Spatial Oracle cross-database non qualificati"
        )
    if mapper.table.name != mapper.table.name.upper() or (
        mapper.table.schema is not None
        and mapper.table.schema != mapper.table.schema.upper()
    ):
        raise OrmUnsupportedError(
            "Oracle Spatial richiede nomi tabella e schema canonici uppercase"
        )
    table_name = mapper.table.name.replace("'", "''")
    statements: list[str] = []
    for attribute in spatial_attributes:
        geometry = attribute.type_
        assert isinstance(geometry, Geometry)
        _require_geometry_mapping(geometry, "oracle")
        if attribute.name is None:
            raise OrmMappingError("colonna Geometry Oracle senza nome")
        if attribute.name != attribute.name.upper():
            raise OrmUnsupportedError(
                "Oracle Spatial richiede nomi colonna canonici uppercase"
            )
        column_name = attribute.name.replace("'", "''")
        geographic = geometry.srid in _require_geographic_srids()
        if geographic:
            xy = (
                "MDSYS.SDO_DIM_ELEMENT('LONGITUDE', -180, 180, 0.005), "
                "MDSYS.SDO_DIM_ELEMENT('LATITUDE', -90, 90, 0.005)"
            )
        else:
            xy = (
                "MDSYS.SDO_DIM_ELEMENT('X', -1000000000000000, "
                "1000000000000000, 0.005), "
                "MDSYS.SDO_DIM_ELEMENT('Y', -1000000000000000, "
                "1000000000000000, 0.005)"
            )
        dimensions = (
            f"MDSYS.SDO_DIM_ARRAY({xy}, MDSYS.SDO_DIM_ELEMENT('Z', "
            "-1000000000000000, 1000000000000000, 0.005))"
            if geometry.dimensions == "xyz"
            else f"MDSYS.SDO_DIM_ARRAY({xy})"
        )
        statements.append(
            "BEGIN "
            "DELETE FROM USER_SDO_GEOM_METADATA "
            f"WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'; "
            "INSERT INTO USER_SDO_GEOM_METADATA "
            "(TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES "
            f"('{table_name}', '{column_name}', {dimensions}, {geometry.srid}); "
            "COMMIT; END;"
        )
    return tuple(statements)


def _migration_parents(value: str | tuple[str, ...] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        if not value:
            raise ValueError("migration down_revision non valida")
        return (value,)
    if (
        not isinstance(value, tuple)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise ValueError("migration down_revision non valida")
    return value


def _order_migrations(migrations: tuple[Migration, ...]) -> tuple[Migration, ...]:
    by_revision: dict[str, Migration] = {}
    positions: dict[str, int] = {}
    for position, migration in enumerate(migrations):
        if migration.revision in by_revision:
            raise OrmMappingError("grafo migrazioni con revisione duplicata")
        by_revision[migration.revision] = migration
        positions[migration.revision] = position
    for migration in migrations:
        if any(
            parent not in by_revision
            for parent in _migration_parents(migration.down_revision)
        ):
            raise OrmMappingError("grafo migrazioni con revisione genitore assente")

    pending = dict(by_revision)
    ordered: list[Migration] = []
    emitted: set[str] = set()
    while pending:
        ready = [
            item
            for item in pending.values()
            if set(_migration_parents(item.down_revision)) <= emitted
        ]
        if not ready:
            raise OrmMappingError("grafo migrazioni ciclico")
        ready.sort(key=lambda item: (positions[item.revision], item.revision))
        for item in ready:
            ordered.append(item)
            emitted.add(item.revision)
            del pending[item.revision]
    return tuple(ordered)


def _validate_applied_migrations(
    migrations: tuple[Migration, ...], applied: set[str]
) -> None:
    registered = {item.revision for item in migrations}
    if not applied <= registered:
        raise OrmStateError("storia migrazioni contiene revisioni non registrate")
    for migration in migrations:
        if (
            migration.revision in applied
            and not set(_migration_parents(migration.down_revision)) <= applied
        ):
            raise OrmStateError("storia migrazioni non chiusa sugli antenati")


def _migration_history(
    migrations: tuple[Migration, ...], rows: Iterable[Any]
) -> dict[str, tuple[str, str]]:
    registered = {item.revision: item for item in migrations}
    history: dict[str, tuple[str, str]] = {}
    for row in rows:
        try:
            revision = row["revision"]
            checksum = row["checksum"]
            state = row["state"]
        except (KeyError, TypeError) as error:
            raise OrmStateError("storia migrazioni priva di campi 2.0") from error
        if not all(isinstance(value, str) for value in (revision, checksum, state)):
            raise OrmStateError("storia migrazioni con campi non validi")
        if revision in history:
            raise OrmStateError("storia migrazioni con revisione duplicata")
        migration = registered.get(revision)
        if migration is None:
            raise OrmStateError("storia migrazioni contiene revisioni non registrate")
        if checksum != migration.checksum:
            raise OrmStateError("drift checksum nella storia migrazioni")
        if state not in {"applied", "running", "failed"}:
            raise OrmStateError("storia migrazioni con stato non valido")
        if state != "applied":
            raise OrmStateError("migrazione incompleta: richiede recover esplicito")
        history[revision] = (checksum, state)
    _validate_applied_migrations(migrations, set(history))
    return history


def _migration_history_table() -> Table:
    return Table(
        "_plenora_orm_migrations",
        ("revision", "checksum", "state", "applied_at"),
    )


def _migration_insert_statement(migration: Migration, state: str) -> tuple[Any, dict[str, str]]:
    history = _migration_history_table()
    statement = insert(history).values(
        revision=bind("orm_revision", BindType.STRING),
        checksum=bind("orm_checksum", BindType.STRING),
        state=bind("orm_state", BindType.STRING),
    )
    return statement, {
        "orm_revision": migration.revision,
        "orm_checksum": migration.checksum,
        "orm_state": state,
    }


def _migration_state_statement(
    migration: Migration, state: str
) -> tuple[Any, dict[str, str]]:
    history = _migration_history_table()
    statement = update(history).values(
        state=bind("orm_state", BindType.STRING)
    ).where(history.c.revision == bind("orm_revision", BindType.STRING))
    return statement, {"orm_revision": migration.revision, "orm_state": state}


def _migration_delete_statement(migration: Migration) -> tuple[Any, dict[str, str]]:
    history = _migration_history_table()
    statement = delete(history).where(
        history.c.revision == bind("orm_revision", BindType.STRING)
    )
    return statement, {"orm_revision": migration.revision}


def _record_migration_failure(
    session: Any, provider: str, migration: Migration
) -> None:
    transaction = session.begin()
    try:
        transaction.query_sql(_migration_lock_sql(provider))
        rows = transaction.query_sql(_migration_history_select(provider))
        target = next(
            (row for row in rows if row["revision"] == migration.revision), None
        )
        if target is not None and target["checksum"] != migration.checksum:
            raise OrmStateError("drift checksum nella storia migrazioni")
        if target is None:
            statement, parameters = _migration_insert_statement(migration, "failed")
        else:
            statement, parameters = _migration_state_statement(migration, "failed")
        transaction.execute(statement, parameters)
        transaction.commit()
    except BaseException:
        transaction.rollback()
        raise


async def _record_migration_failure_async(
    session: Any, provider: str, migration: Migration
) -> None:
    transaction = await session.begin()
    try:
        await transaction.query_sql(_migration_lock_sql(provider))
        rows = await transaction.query_sql(_migration_history_select(provider))
        target = next(
            (row for row in rows if row["revision"] == migration.revision), None
        )
        if target is not None and target["checksum"] != migration.checksum:
            raise OrmStateError("drift checksum nella storia migrazioni")
        if target is None:
            statement, parameters = _migration_insert_statement(migration, "failed")
        else:
            statement, parameters = _migration_state_statement(migration, "failed")
        await transaction.execute(statement, parameters)
        await transaction.commit()
    except BaseException:
        await transaction.rollback()
        raise


def _migration_table_ddl(provider: str) -> str:
    if provider == "oracle":
        return (
            'CREATE TABLE "_plenora_orm_migrations" ('
            '"revision" VARCHAR2(255) NOT NULL PRIMARY KEY, '
            '"checksum" CHAR(64) NOT NULL, '
            '"state" VARCHAR2(16) NOT NULL, '
            '"applied_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)'
        )
    if provider == "db2":
        return (
            'CREATE TABLE "_plenora_orm_migrations" ('
            '"revision" VARCHAR(255) NOT NULL PRIMARY KEY, '
            '"checksum" CHAR(64) NOT NULL, '
            '"state" VARCHAR(16) NOT NULL, '
            '"applied_at" TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP)'
        )
    if provider == "sqlserver":
        return (
            "IF OBJECT_ID(N'_plenora_orm_migrations', N'U') IS NULL "
            "CREATE TABLE [_plenora_orm_migrations] ("
            "[revision] NVARCHAR(255) PRIMARY KEY, "
            "[checksum] CHAR(64) NOT NULL, "
            "[state] NVARCHAR(16) NOT NULL, "
            "[applied_at] DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
    quote = "`" if provider in {"mysql", "mariadb"} else '"'
    return (
        f"CREATE TABLE IF NOT EXISTS {quote}_plenora_orm_migrations{quote} ("
        f"{quote}revision{quote} VARCHAR(255) PRIMARY KEY, "
        f"{quote}checksum{quote} CHAR(64) NOT NULL, "
        f"{quote}state{quote} VARCHAR(16) NOT NULL, "
        f"{quote}applied_at{quote} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def _migration_history_select(provider: str) -> str:
    if provider in {"oracle", "db2"}:
        return (
            'SELECT "revision" AS "revision", "checksum" AS "checksum", '
            '"state" AS "state" FROM "_plenora_orm_migrations" '
            "WHERE \"revision\" <> '__plenora_lock__'"
        )
    return (
        "SELECT revision, checksum, state FROM _plenora_orm_migrations "
        "WHERE revision <> '__plenora_lock__'"
    )


def _migration_lock_seed_sql(provider: str) -> str:
    values = "'__plenora_lock__', '0000000000000000000000000000000000000000000000000000000000000000', 'lock'"
    if provider == "postgres":
        return (
            "INSERT INTO _plenora_orm_migrations (revision, checksum, state) "
            f"VALUES ({values}) ON CONFLICT (revision) DO NOTHING"
        )
    if provider in {"mysql", "mariadb"}:
        return (
            "INSERT IGNORE INTO _plenora_orm_migrations (revision, checksum, state) "
            f"VALUES ({values})"
        )
    if provider == "sqlserver":
        return (
            "IF NOT EXISTS (SELECT 1 FROM _plenora_orm_migrations WHERE revision = '__plenora_lock__') "
            "INSERT INTO _plenora_orm_migrations (revision, checksum, state) "
            f"VALUES ({values})"
        )
    if provider == "oracle":
        return (
            'MERGE INTO "_plenora_orm_migrations" target '
            "USING (SELECT '__plenora_lock__' AS \"revision\", "
            "'0000000000000000000000000000000000000000000000000000000000000000' "
            "AS \"checksum\", 'lock' AS \"state\" FROM DUAL) source "
            'ON (target."revision" = source."revision") '
            'WHEN NOT MATCHED THEN INSERT ("revision", "checksum", "state") '
            'VALUES (source."revision", source."checksum", source."state")'
        )
    return (
        'MERGE INTO "_plenora_orm_migrations" AS target '
        "USING (VALUES ('__plenora_lock__', "
        "'0000000000000000000000000000000000000000000000000000000000000000', 'lock')) "
        'AS source ("revision", "checksum", "state") '
        'ON target."revision" = source."revision" '
        'WHEN NOT MATCHED THEN INSERT ("revision", "checksum", "state") '
        'VALUES (source."revision", source."checksum", source."state")'
    )


def _migration_lock_sql(provider: str) -> str:
    if provider == "sqlserver":
        return (
            "SELECT revision FROM _plenora_orm_migrations WITH (UPDLOCK, HOLDLOCK) "
            "WHERE revision = '__plenora_lock__'"
        )
    suffix = " FOR UPDATE WITH RS" if provider == "db2" else " FOR UPDATE"
    quote = '"' if provider in {"postgres", "oracle", "db2"} else "`"
    return (
        f"SELECT {quote}revision{quote} FROM {quote}_plenora_orm_migrations{quote} "
        f"WHERE {quote}revision{quote} = '__plenora_lock__'{suffix}"
    )


def _migration_table_exists_sql(provider: str) -> str:
    if provider == "oracle":
        return (
            "SELECT COUNT(*) FROM USER_TABLES "
            "WHERE TABLE_NAME = '_plenora_orm_migrations'"
        )
    return (
        "SELECT COUNT_BIG(*) FROM SYSCAT.TABLES "
        "WHERE TABSCHEMA = CURRENT SCHEMA "
        "AND TABNAME = '_plenora_orm_migrations'"
    )


def _migration_table_exists(session: Any, provider: str) -> bool:
    scalar = getattr(session, "execute_scalar", None)
    if not callable(scalar):
        raise TypeError("sessione priva di execute_scalar per il catalogo migrazioni")
    value = scalar(_migration_table_exists_sql(provider))
    try:
        return int(value) > 0
    except (TypeError, ValueError) as error:
        raise OrmStateError(
            "catalogo migrazioni con conteggio non valido"
        ) from error


def _ensure_migration_table(session: Any, provider: str) -> None:
    if provider not in {"oracle", "db2"}:
        _execute_ddl(session, _migration_table_ddl(provider))
        _execute_migration_sql(session, _migration_lock_seed_sql(provider))
        return
    if _migration_table_exists(session, provider):
        _execute_migration_sql(session, _migration_lock_seed_sql(provider))
        return
    try:
        _execute_ddl(session, _migration_table_ddl(provider))
    except Exception:
        # Due runner possono osservare insieme l'assenza. Il perdente accetta
        # soltanto il caso in cui il catalogo dimostri che l'altro ha creato
        # esattamente la tabella attesa; ogni altro errore resta pubblico.
        if not _migration_table_exists(session, provider):
            raise
    _execute_migration_sql(session, _migration_lock_seed_sql(provider))


async def _ensure_migration_table_async(session: Any, provider: str) -> None:
    if provider not in {"oracle", "db2"}:
        await _execute_ddl_async(session, _migration_table_ddl(provider))
        await _execute_migration_sql_async(
            session, _migration_lock_seed_sql(provider)
        )
        return
    scalar = getattr(session, "execute_scalar", None)
    if not callable(scalar):
        raise TypeError("sessione priva di execute_scalar per il catalogo migrazioni")

    async def exists() -> bool:
        outcome = scalar(_migration_table_exists_sql(provider))
        value = await outcome if isawaitable(outcome) else outcome
        try:
            return int(value) > 0
        except (TypeError, ValueError) as error:
            raise OrmStateError(
                "catalogo migrazioni con conteggio non valido"
            ) from error

    if await exists():
        await _execute_migration_sql_async(
            session, _migration_lock_seed_sql(provider)
        )
        return
    try:
        await _execute_ddl_async(session, _migration_table_ddl(provider))
    except Exception:
        if not await exists():
            raise
    await _execute_migration_sql_async(session, _migration_lock_seed_sql(provider))


def _execute_migration_sql(session: Any, statement: str) -> None:
    execute = getattr(session, "execute_sql", None)
    if not callable(execute):
        raise TypeError("sessione priva di execute_sql per le migrazioni")
    execute(statement)


async def _execute_migration_sql_async(session: Any, statement: str) -> None:
    execute = getattr(session, "execute_sql", None)
    if not callable(execute):
        raise TypeError("sessione priva di execute_sql per le migrazioni")
    outcome = execute(statement)
    if isawaitable(outcome):
        await outcome


def _require_one_row(affected: Any) -> None:
    if isinstance(affected, MutationResult):
        affected = affected.affected_rows
    if not isinstance(affected, int) or isinstance(affected, bool) or affected != 1:
        raise StaleObjectError(
            "la mutazione ORM non ha interessato esattamente una riga"
        )


def _insert_batch_limit(
    signature: tuple[type[DeclarativeBase], tuple[str, ...]] | None,
    provider: str,
    configured: int,
) -> int:
    if signature is None or provider != "sqlserver":
        return configured
    return min(configured, max(1, 2_100 // len(signature[1])))


def _require_row_count(affected: Any, expected: int) -> None:
    if isinstance(affected, MutationResult):
        affected = affected.affected_rows
    if (
        not isinstance(affected, int)
        or isinstance(affected, bool)
        or affected != expected
    ):
        raise StaleObjectError(
            "la mutazione ORM batch non ha interessato il numero atteso di righe"
        )


__all__ = [
    "BIGINT",
    "JSON",
    "UUID",
    "AsyncMigrationRunner",
    "AsyncOrmEntityTupleQuery",
    "AsyncOrmQuery",
    "AsyncOrmRowsQuery",
    "AsyncOrmSession",
    "BigInteger",
    "CheckConstraint",
    "DateTime",
    "DeclarativeBase",
    "ForeignKeyConstraint",
    "Geometry",
    "Json",
    "InstanceInspection",
    "LoaderOption",
    "Mapped",
    "MappedColumn",
    "Mapper",
    "Migration",
    "MigrationRunner",
    "Numeric",
    "ObjectState",
    "OrmEntityTupleQuery",
    "OrmError",
    "OrmIndex",
    "OrmMappingError",
    "OrmMetadata",
    "OrmQuery",
    "OrmRowsQuery",
    "OrmSession",
    "OrmStateError",
    "OrmUnsupportedError",
    "Registry",
    "Relationship",
    "ServerDefault",
    "String",
    "StaleObjectError",
    "UniqueConstraint",
    "Uuid",
    "inspect_instance",
    "joinedload",
    "mapped_column",
    "mapper_registry",
    "relationship",
    "selectinload",
]
