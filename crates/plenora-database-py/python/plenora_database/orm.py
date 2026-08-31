"""ORM dichiarativo sync/async sopra il lifecycle e l'IR Core v3.

Il modulo concentra mapping, relazioni esplicite senza lazy I/O, identity map,
unit of work, query di entita, DDL e migrazioni lineari. Le capability non
qualificate per un provider falliscono prima di inviare lo statement.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableSequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from inspect import isawaitable
from types import TracebackType
from typing import Any, Generic, TypeVar, overload

from .expression import (
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
    bind,
    delete,
    insert,
    select,
    update,
)
from .result import MultipleResultsFound, NoResultFound, Result
from .spatial import SpatialReference

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
_GEOMETRY_ORM_PROVIDERS = frozenset({"postgres", *_MYSQL_ORM_PROVIDERS})


class OrmError(RuntimeError):
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

        return _spatial_value(bind(name), self.srid, self.semantics)

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
    def literal(cls, value: str | float | bool) -> ServerDefault:
        if not isinstance(value, (str, int, float, bool)):
            raise TypeError("server default literal non supportato")
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
class ForeignKeyConstraint:
    columns: tuple[str, ...]
    target: type[DeclarativeBase] | str
    target_columns: tuple[str, ...]
    name: str | None = None
    on_delete: str | None = None

    def __init__(
        self,
        columns: Iterable[str],
        target: type[DeclarativeBase] | str,
        target_columns: Iterable[str],
        *,
        name: str | None = None,
        on_delete: str | None = None,
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
        object.__setattr__(self, "columns", local)
        object.__setattr__(self, "target", target)
        object.__setattr__(self, "target_columns", remote)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "on_delete", normalized_delete)


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
        foreign_key: str | None = None,
        uselist: bool = False,
        back_populates: str | None = None,
        cascade: str | Iterable[str] = (),
        secondary: Table | None = None,
        secondary_local_key: str | None = None,
        secondary_remote_key: str | None = None,
    ) -> None:
        if (
            not isinstance(target, (type, str))
            or isinstance(target, str)
            and not target
        ):
            raise TypeError("relationship target richiede una classe o il suo nome")
        if foreign_key is not None and (
            not isinstance(foreign_key, str) or not foreign_key
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
        if secondary is not None:
            if not isinstance(secondary, Table) or not uselist:
                raise OrmMappingError("secondary richiede una relazione uselist")
            if foreign_key is not None:
                raise OrmMappingError("secondary non accetta foreign_key")
            if "delete-orphan" in cascades:
                raise OrmMappingError("delete-orphan non e valido su many-to-many")
            for key in (secondary_local_key, secondary_remote_key):
                if not isinstance(key, str) or not key:
                    raise OrmMappingError("secondary richiede entrambe le chiavi")
                try:
                    secondary.c[key]
                except KeyError as error:
                    raise OrmMappingError("chiave secondary non presente") from error
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
        if any(item.name == self.foreign_key for item in owner_mapper.attributes):
            return "many-to-one"
        target_mapper = _mapper(self.target)  # type: ignore[arg-type]
        if any(item.name == self.foreign_key for item in target_mapper.attributes):
            return "one-to-one"
        raise OrmMappingError("relationship riferisce una foreign key non mappata")

    def _bind(self, name: str, owner: type[DeclarativeBase]) -> None:
        self.name = name
        self.owner = owner

    def _validate_configuration(self) -> None:
        target_mapper = _mapper(self.target)  # type: ignore[arg-type]
        if self.owner is None:
            raise OrmMappingError("relationship non associata a un mapper")
        _single_primary(_mapper(self.owner))
        _single_primary(target_mapper)
        direction = self.direction
        if direction == "one-to-many" and self.foreign_key is not None:
            target_mapper.attribute(self.foreign_key)
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
        primary_name = _single_primary(_mapper(self.target)).name  # type: ignore[arg-type]
        primary = (
            None
            if value is None or primary_name is None
            else value.__dict__.get(primary_name)
        )
        if value is None or primary is not None:
            setattr(instance, self.foreign_key, primary)

    def _synchronize_child_foreign_key(self, owner: DeclarativeBase, value: T) -> None:
        if (
            self.direction not in {"one-to-many", "one-to-one"}
            or self.foreign_key is None
        ):
            return
        primary_name = _single_primary(_mapper(type(owner))).name
        primary = None if primary_name is None else owner.__dict__.get(primary_name)
        if primary is not None:
            setattr(value, self.foreign_key, primary)

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
    foreign_key: str | None = None,
    uselist: bool = False,
    back_populates: str | None = None,
    cascade: str | Iterable[str] = (),
    secondary: Table | None = None,
    secondary_local_key: str | None = None,
    secondary_remote_key: str | None = None,
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
    constraints: tuple[UniqueConstraint | ForeignKeyConstraint, ...]
    inherits: Mapper | None

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
        return tuple(
            _create_table_ddl(mapper, provider, self.registry, checkfirst=checkfirst)
            for mapper in ordered
        )

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
        if checkfirst and provider == "db2":
            raise OrmUnsupportedError("Db2 non qualifica DROP TABLE IF EXISTS")
        for mapper in reversed(_ddl_mapper_order(self.mappers, self.registry)):
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
        if checkfirst and provider == "db2":
            raise OrmUnsupportedError("Db2 non qualifica DROP TABLE IF EXISTS")
        for mapper in reversed(_ddl_mapper_order(self.mappers, self.registry)):
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
    down_revision: str | None
    upgrade: Any
    downgrade: Any | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.revision, str) or not self.revision:
            raise ValueError("migration revision non valida")
        if self.down_revision is not None and (
            not isinstance(self.down_revision, str) or not self.down_revision
        ):
            raise ValueError("migration down_revision non valida")
        if not callable(self.upgrade) or (
            self.downgrade is not None and not callable(self.downgrade)
        ):
            raise TypeError("migration richiede callback valide")


class MigrationRunner:
    """Runner lineare e transazionale di migrazioni esplicite."""

    def __init__(self, migrations: Iterable[Migration]) -> None:
        self.migrations = tuple(migrations)
        _validate_migration_chain(self.migrations)

    def apply(self, session: Any) -> tuple[str, ...]:
        provider = _session_provider(session)
        session.execute(_migration_table_ddl(provider))
        rows = session.execute_returning_rows(
            "SELECT revision FROM _plenora_orm_migrations"
        )
        applied = {row["revision"] for row in rows}
        expected = {item.revision for item in self.migrations[: len(applied)]}
        if applied != expected:
            raise OrmStateError(
                "storia migrazioni non coerente con la catena registrata"
            )
        completed: list[str] = []
        history = Table("_plenora_orm_migrations", ("revision", "applied_at"))
        for migration in self.migrations:
            if migration.revision in applied:
                continue
            transaction = session.begin()
            try:
                migration.upgrade(transaction)
                transaction.execute(
                    insert(history).values(revision=bind("orm_revision")),
                    {"orm_revision": migration.revision},
                )
                transaction.commit()
            except BaseException:
                transaction.rollback()
                raise
            completed.append(migration.revision)
        return tuple(completed)

    def rollback(self, session: Any, *, steps: int = 1) -> tuple[str, ...]:
        if not isinstance(steps, int) or isinstance(steps, bool) or steps < 1:
            raise ValueError("migration rollback steps non valido")
        rows = session.execute_returning_rows(
            "SELECT revision FROM _plenora_orm_migrations ORDER BY applied_at DESC"
        )
        by_revision = {item.revision: item for item in self.migrations}
        history = Table("_plenora_orm_migrations", ("revision", "applied_at"))
        completed: list[str] = []
        for row in rows[:steps]:
            migration = by_revision.get(row["revision"])
            if migration is None or migration.downgrade is None:
                raise OrmStateError("migration applicata priva di downgrade registrato")
            transaction = session.begin()
            try:
                migration.downgrade(transaction)
                transaction.execute(
                    delete(history).where(history.c.revision == bind("orm_revision")),
                    {"orm_revision": migration.revision},
                )
                transaction.commit()
            except BaseException:
                transaction.rollback()
                raise
            completed.append(migration.revision)
        return tuple(completed)


class AsyncMigrationRunner(MigrationRunner):
    async def apply(self, session: Any) -> tuple[str, ...]:  # type: ignore[override]
        provider = _session_provider(session)
        await session.execute(_migration_table_ddl(provider))
        rows = await session.execute_returning_rows(
            "SELECT revision FROM _plenora_orm_migrations"
        )
        applied = {row["revision"] for row in rows}
        expected = {item.revision for item in self.migrations[: len(applied)]}
        if applied != expected:
            raise OrmStateError(
                "storia migrazioni non coerente con la catena registrata"
            )
        completed: list[str] = []
        history = Table("_plenora_orm_migrations", ("revision", "applied_at"))
        for migration in self.migrations:
            if migration.revision in applied:
                continue
            transaction = await session.begin()
            try:
                outcome = migration.upgrade(transaction)
                if isawaitable(outcome):
                    await outcome
                await transaction.execute(
                    insert(history).values(revision=bind("orm_revision")),
                    {"orm_revision": migration.revision},
                )
                await transaction.commit()
            except BaseException:
                await transaction.rollback()
                raise
            completed.append(migration.revision)
        return tuple(completed)

    async def rollback(  # type: ignore[override]
        self, session: Any, *, steps: int = 1
    ) -> tuple[str, ...]:
        if not isinstance(steps, int) or isinstance(steps, bool) or steps < 1:
            raise ValueError("migration rollback steps non valido")
        rows = await session.execute_returning_rows(
            "SELECT revision FROM _plenora_orm_migrations ORDER BY applied_at DESC"
        )
        by_revision = {item.revision: item for item in self.migrations}
        history = Table("_plenora_orm_migrations", ("revision", "applied_at"))
        completed: list[str] = []
        for row in rows[:steps]:
            migration = by_revision.get(row["revision"])
            if migration is None or migration.downgrade is None:
                raise OrmStateError("migration applicata priva di downgrade registrato")
            transaction = await session.begin()
            try:
                outcome = migration.downgrade(transaction)
                if isawaitable(outcome):
                    await outcome
                await transaction.execute(
                    delete(history).where(history.c.revision == bind("orm_revision")),
                    {"orm_revision": migration.revision},
                )
                await transaction.commit()
            except BaseException:
                await transaction.rollback()
                raise
            completed.append(migration.revision)
        return tuple(completed)


class _DeclarativeMeta(type):
    def __new__(mcls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        table_name = namespace.get("__tablename__")
        inherited_mappers = tuple(
            mapper
            for base in bases
            if isinstance((mapper := getattr(base, "__mapper__", None)), Mapper)
        )
        if inherited_mappers and table_name is not None:
            mapper_args = namespace.get("__mapper_args__", {})
            if not isinstance(mapper_args, Mapping) or not mapper_args.get("concrete"):
                raise OrmUnsupportedError(
                    "ereditarieta mappata richiede __mapper_args__={'concrete': True}"
                )
            if len(inherited_mappers) != 1:
                raise OrmUnsupportedError(
                    "ereditarieta multipla mappata non supportata"
                )
            if inherited_mappers[0].relationships:
                raise OrmUnsupportedError(
                    "ereditarieta concreta di relationship richiede una rimappatura esplicita"
                )
            namespace = dict(namespace)
            for attribute in inherited_mappers[0].attributes:
                if attribute.name is not None and attribute.name not in namespace:
                    namespace[attribute.name] = attribute._clone()
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
        attributes = tuple(
            value for value in namespace.values() if isinstance(value, MappedColumn)
        )
        relationships = tuple(
            value for value in namespace.values() if isinstance(value, Relationship)
        )
        declared_table_args = namespace.get("__table_args__", ())
        if not isinstance(declared_table_args, tuple) or not all(
            isinstance(item, (UniqueConstraint, ForeignKeyConstraint))
            for item in declared_table_args
        ):
            raise OrmMappingError("__table_args__ richiede una tupla di vincoli ORM")
        table_args = (
            *(inherited_mappers[0].constraints if inherited_mappers else ()),
            *declared_table_args,
        )
        if not attributes:
            raise OrmMappingError("un modello richiede almeno una colonna")
        primary_keys = tuple(item for item in attributes if item.primary_key)
        versions = tuple(item for item in attributes if item.version)
        if not primary_keys:
            raise OrmMappingError("un modello richiede almeno una chiave primaria")
        if sum(item.generated for item in primary_keys) > 1:
            raise OrmMappingError(
                "una chiave composta accetta un solo componente generated"
            )
        if len(versions) > 1:
            raise OrmMappingError("un modello accetta una sola colonna versione")
        names = tuple(key for key, value in namespace.items() if value in attributes)
        target = Table(
            table_name,
            names,
            schema=namespace.get("__schema__"),
            catalog=namespace.get("__catalog__"),
        )
        for attribute, column in zip(attributes, target.columns, strict=True):
            attribute._bind(column.name, column)
        column_names = {attribute.name for attribute in attributes}
        constraints: tuple[UniqueConstraint | ForeignKeyConstraint, ...] = (
            *table_args,
            *(
                UniqueConstraint(attribute.name or "")
                for attribute in attributes
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
            inherited_mappers[0] if inherited_mappers else None,
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
    relationship: str | Relationship[Any]
    strategy: str

    def __post_init__(self) -> None:
        if not isinstance(self.relationship, (str, Relationship)):
            raise TypeError("loader option richiede una relationship")
        if self.strategy not in {"selectin", "joined"}:
            raise ValueError("strategia eager non valida")


def selectinload(relation: str | Relationship[Any]) -> LoaderOption:
    return LoaderOption(relation, "selectin")


def joinedload(relation: str | Relationship[Any]) -> LoaderOption:
    return LoaderOption(relation, "joined")


@dataclass(frozen=True, slots=True)
class OrmQuery(Generic[T]):
    """Query di entita immutabile e legata a una ``OrmSession``."""

    _session: OrmSession
    _mapper: Mapper
    _statement: SelectStatement
    _loaders: tuple[LoaderOption, ...] = ()

    def where(self, predicate: Predicate) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> OrmQuery[T]:
        return replace(self, _statement=self._statement.limit(value))

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
            relation = _query_relationship(self._mapper, loader.relationship)
            relation._validate_configuration()
            if loader.strategy == "joined":
                if relation.uselist:
                    raise OrmUnsupportedError(
                        "joinedload di collezioni richiede una strategia di deduplicazione"
                    )
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
            self._mapper, self._statement, parameters, self._loaders
        )

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


@dataclass(frozen=True, slots=True)
class AsyncOrmQuery(Generic[T]):
    """Versione async della query di entita, sullo stesso IR e mapper."""

    _session: AsyncOrmSession
    _mapper: Mapper
    _statement: SelectStatement
    _loaders: tuple[LoaderOption, ...] = ()

    def where(self, predicate: Predicate) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.where(predicate))

    def order_by(self, *values: Expression | Ordering) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.order_by(*values))

    def limit(self, value: int) -> AsyncOrmQuery[T]:
        return replace(self, _statement=self._statement.limit(value))

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
            relation = _query_relationship(self._mapper, loader.relationship)
            relation._validate_configuration()
            if loader.strategy == "joined":
                if relation.uselist:
                    raise OrmUnsupportedError(
                        "joinedload di collezioni richiede una strategia di deduplicazione"
                    )
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
            self._mapper, self._statement, parameters, self._loaders
        )

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
        _validate_spatial_statement(
            self._statement, self._session._spatial_functions
        )
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
        _validate_spatial_statement(
            self._statement, self._session._spatial_functions
        )
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
        _validate_spatial_statement(
            self._statement, self._session._spatial_functions
        )
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
        _validate_spatial_statement(
            self._statement, self._session._spatial_functions
        )
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

    if provider not in _MYSQL_ORM_PROVIDERS or parameters is None:
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
                    or expression.get("kind")
                    not in {"parameter", "typed_parameter"}
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
        if semantics != "geometry":
            raise OrmUnsupportedError(
                "MySQL/MariaDB non qualificano la semantica geography ORM"
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
        reference = SpatialReference.validated(ewkb, srid, "xy", semantics)
        normalized[name] = _geometry_bind_value(reference, provider)
    return normalized


def _require_geometry_mapping(type_: Geometry, provider: str) -> None:
    if provider not in _GEOMETRY_ORM_PROVIDERS:
        raise OrmUnsupportedError(
            "Geometry ORM non e qualificata per il provider"
        )
    if provider not in _MYSQL_ORM_PROVIDERS:
        return
    if type_.semantics != "geometry":
        raise OrmUnsupportedError(
            "MySQL/MariaDB non qualificano la semantica geography ORM"
        )
    if type_.dimensions != "xy":
        raise OrmUnsupportedError(
            "Geometry ORM MySQL/MariaDB qualifica soltanto coordinate XY"
        )
    if (
        type_.geometry_type is not None
        and type_.geometry_type not in _MYSQL_GEOMETRY_TYPES
    ):
        raise OrmUnsupportedError(
            "tipo Geometry ORM non qualificato per MySQL/MariaDB"
        )


def _require_geometry_mapper(mapper: Mapper, provider: str) -> None:
    for attribute in mapper.attributes:
        if isinstance(attribute.type_, Geometry):
            _require_geometry_mapping(attribute.type_, provider)


def _geometry_bind_value(value: SpatialReference, provider: str) -> bytes:
    if provider not in _MYSQL_ORM_PROVIDERS:
        return value.ewkb
    try:
        from . import _native
    except ImportError as error:
        raise RuntimeError(
            "modulo nativo non disponibile per preparare Geometry ORM"
        ) from error
    converter = getattr(_native, "geometry_wkb_xy", None)
    if converter is None:
        raise RuntimeError(
            "estensione nativa incompatibile con Geometry ORM MySQL/MariaDB"
        )
    converted = converter(value.ewkb)
    if not isinstance(converted, bytes):
        raise RuntimeError("conversione Geometry ORM non ha restituito bytes")
    return converted


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

    def __init__(self, session: Any, *, autoflush: bool = True) -> None:
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
        self._spatial_functions = _session_spatial_functions(capabilities)
        self._transaction = begin()
        self._identity_map: dict[
            tuple[type[DeclarativeBase], tuple[Any, ...]], DeclarativeBase
        ] = {}
        self._pending: list[DeclarativeBase] = []
        self._deleted: list[DeclarativeBase] = []
        self._flushed_deleted: list[DeclarativeBase] = []
        self._deferred_foreign_keys: list[
            tuple[DeclarativeBase, DeclarativeBase, str]
        ] = []
        self._autoflush_enabled = bool(autoflush)
        self._in_flush = False
        self._listeners: dict[str, list[Any]] = {}
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

    def _emit(self, event: str, instance: DeclarativeBase | None = None) -> None:
        for callback in self._listeners.get(event, ()):
            outcome = (
                callback(self, instance) if instance is not None else callback(self)
            )
            if isawaitable(outcome):
                raise OrmStateError("un hook async richiede AsyncOrmSession")

    def add(self, instance: DeclarativeBase) -> None:
        self._add(instance, set())

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
        return OrmQuery(
            self, mapper, select(*_orm_projections(mapper, provider=self._provider))
        )

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
        statement = (
            select(*_orm_projections(mapper, provider=self._provider))
            .where(predicate)
            .limit(2)
        )
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
        statement = (
            select(*_orm_projections(mapper, provider=self._provider))
            .where(predicate)
            .limit(2)
        )
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
            foreign_value = instance.__dict__.get(descriptor.foreign_key)
            return (
                None
                if foreign_value is None
                else self.get(descriptor.target, foreign_value)
            )
        owner_mapper = _mapper(type(instance))
        _single_primary(owner_mapper)
        _single_primary(target_mapper)
        owner_identity = _identity(owner_mapper, instance)[0]
        if direction in {"one-to-many", "one-to-one"}:
            foreign = target_mapper.attribute(descriptor.foreign_key or "").column
            if foreign is None:
                raise OrmMappingError("foreign key relationship senza colonna")
            rows = (
                self.query(descriptor.target)
                .where(foreign == bind("orm_relationship_identity"))
                .all({"orm_relationship_identity": owner_identity})
            )
            if direction == "one-to-one":
                if len(rows) > 1:
                    raise MultipleResultsFound("relationship one-to-one non univoca")
                return None if not rows else rows[0]
            return rows
        secondary = descriptor.secondary
        if secondary is None:
            raise OrmMappingError("relationship many-to-many senza secondary")
        target_primary = _single_primary(target_mapper).column
        if target_primary is None:
            raise OrmMappingError("target relationship senza chiave")
        statement = (
            select(*_orm_projections(target_mapper, provider=self._provider))
            .select_from(target_mapper.table)
            .join(
                secondary,
                secondary.c[descriptor.secondary_remote_key or ""] == target_primary,
            )
            .where(
                secondary.c[descriptor.secondary_local_key or ""]
                == bind("orm_relationship_identity")
            )
        )
        return self._execute_entities(
            target_mapper, statement, {"orm_relationship_identity": owner_identity}
        )

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
            for instance in pending:
                self._synchronize_relationships(instance)
                self._preflight((instance,))
                self._insert(instance)
            for instance, related, foreign_key in self._deferred_foreign_keys:
                instance.__dict__[foreign_key] = _identity(
                    _mapper(type(related)), related
                )[0]
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
            raise
        self._detach_all(restore=False)
        self._active = False
        self._emit("after_commit")

    def rollback(self) -> None:
        self._require_active()
        try:
            self._transaction.rollback()
        finally:
            self._detach_all(restore=True)
            self._active = False
            self._emit("after_rollback")

    def close(self) -> None:
        if self._active:
            self.rollback()

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
        instances = [self._hydrate(mapper, row) for row in rows]
        for loader in loaders:
            relation = _query_relationship(mapper, loader.relationship)
            if loader.strategy == "joined":
                for instance, row in zip(instances, rows, strict=True):
                    self._hydrate_joined(instance, relation, row)
            else:
                self._selectin_load(instances, relation)
        return instances

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
            values = _mapped_row_values(
                target_mapper, row, prefix, self._provider
            )
            related = self._hydrate(target_mapper, values)
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
                    instance.__dict__.get(relation.foreign_key)
                    for instance in instances
                    if instance.__dict__.get(relation.foreign_key) is not None
                )
            )
            related = self._selectin_entities(
                target_mapper, _single_primary(target_mapper).column, values
            )
            by_identity = {_identity(target_mapper, item)[0]: item for item in related}
            for instance in instances:
                _assign_loaded_relationship(
                    instance,
                    relation,
                    by_identity.get(instance.__dict__.get(relation.foreign_key)),
                )
            return
        owner_mapper = _mapper(type(instances[0]))
        _single_primary(owner_mapper)
        _single_primary(target_mapper)
        owner_ids = tuple(_identity(owner_mapper, item)[0] for item in instances)
        if direction in {"one-to-many", "one-to-one"}:
            foreign = target_mapper.attribute(relation.foreign_key or "").column
            related = self._selectin_entities(target_mapper, foreign, owner_ids)
            grouped: dict[Any, list[DeclarativeBase]] = {key: [] for key in owner_ids}
            for item in related:
                grouped.setdefault(item.__dict__.get(relation.foreign_key), []).append(
                    item
                )
            for instance, identity in zip(instances, owner_ids, strict=True):
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
        self._selectin_many_to_many(instances, relation, owner_ids)

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
        predicate = column.in_(*(bind(name) for name in parameters))
        statement = select(
            *_orm_projections(mapper, provider=self._provider)
        ).where(predicate)
        return self._execute_entities(mapper, statement, parameters)

    def _selectin_many_to_many(
        self,
        instances: list[DeclarativeBase],
        relation: Relationship[Any],
        owner_ids: tuple[Any, ...],
    ) -> None:
        secondary = relation.secondary
        target_mapper = _mapper(relation.target)
        target_primary = _single_primary(target_mapper).column
        if secondary is None or target_primary is None:
            raise OrmMappingError("many-to-many non configurata")
        parameters = {
            f"orm_eager_{index}": value for index, value in enumerate(owner_ids)
        }
        local_column = secondary.c[relation.secondary_local_key or ""]
        statement = (
            select(
                *_orm_projections(target_mapper, provider=self._provider),
                local_column.label("orm_eager_owner"),
            )
            .select_from(target_mapper.table)
            .join(
                secondary,
                secondary.c[relation.secondary_remote_key or ""] == target_primary,
            )
            .where(local_column.in_(*(bind(name) for name in parameters)))
        )
        result = self._transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM eager senza risultato relazionale")
        grouped: dict[Any, list[DeclarativeBase]] = {key: [] for key in owner_ids}
        for row in result.all():
            grouped.setdefault(row["orm_eager_owner"], []).append(
                self._hydrate(target_mapper, row)
            )
        for instance, identity in zip(instances, owner_ids, strict=True):
            _assign_loaded_relationship(instance, relation, grouped.get(identity, []))

    def _hydrate(
        self, mapper: Mapper, row: Mapping[str, Any], *, emit: bool = True
    ) -> DeclarativeBase:
        _validate_geometry_row(mapper, row, self._provider)
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
        edges: dict[tuple[int, int], tuple[DeclarativeBase, DeclarativeBase, str]] = {}
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
                                relation.foreign_key or "",
                            )
                        elif relation.direction in {"one-to-many", "one-to-one"}:
                            dependencies[id(related)].add(id(instance))
                            edges[(id(related), id(instance))] = (
                                related,
                                instance,
                                relation.foreign_key or "",
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
                        if _mapper(type(child)).attribute(foreign_key).nullable:
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
                    value = _identity(_mapper(relation.target), related)[0]
                    setattr(instance, relation.foreign_key or "", value)
                elif relation.direction in {"one-to-many", "one-to-one"}:
                    value = _identity(mapper, instance)[0]
                    setattr(related, relation.foreign_key or "", value)

    def _flush_many_to_many(self) -> None:
        seen: set[tuple[Any, ...]] = set()
        for instance in tuple(self._identity_map.values()):
            mapper = _mapper(type(instance))
            state = _state(instance)
            for relation in mapper.relationships:
                if relation.secondary is None or relation.name not in instance.__dict__:
                    continue
                local_value = _identity(mapper, instance)[0]
                current = {
                    _identity(_mapper(relation.target), related)
                    for related in _loaded_relationship_values(instance, relation)
                }
                original = set(state.relationship_original.get(relation.name or "", ()))
                for remote_identity in current - original:
                    remote_value = remote_identity[0]
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    self._transaction.execute(
                        insert(relation.secondary).values(
                            **{
                                relation.secondary_local_key or "": bind(
                                    "orm_link_local"
                                ),
                                relation.secondary_remote_key or "": bind(
                                    "orm_link_remote"
                                ),
                            }
                        ),
                        {
                            "orm_link_local": local_value,
                            "orm_link_remote": remote_value,
                        },
                    )
                for remote_identity in original - current:
                    remote_value = remote_identity[0]
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    predicate = (
                        relation.secondary.c[relation.secondary_local_key or ""]
                        == bind("orm_link_local")
                    ) & (
                        relation.secondary.c[relation.secondary_remote_key or ""]
                        == bind("orm_link_remote")
                    )
                    self._transaction.execute(
                        delete(relation.secondary).where(predicate),
                        {
                            "orm_link_local": local_value,
                            "orm_link_remote": remote_value,
                        },
                    )
                _remember_relationship(instance, relation)

    def _remove_deleted_associations(self) -> None:
        seen: set[tuple[int, str, Any]] = set()
        for instance in self._deleted:
            mapper = _mapper(type(instance))
            local_value = _identity(mapper, instance)[0]
            for relation in mapper.relationships:
                if relation.secondary is None:
                    continue
                signature = (
                    id(relation.secondary),
                    relation.secondary_local_key or "",
                    local_value,
                )
                if signature in seen:
                    continue
                seen.add(signature)
                self._transaction.execute(
                    delete(relation.secondary).where(
                        relation.secondary.c[relation.secondary_local_key or ""]
                        == bind("orm_link_owner")
                    ),
                    {"orm_link_owner": local_value},
                )

    def _preflight(self, instances: tuple[DeclarativeBase, ...]) -> None:
        for instance in instances:
            mapper = _mapper(type(instance))
            state = _state(instance)
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
                                relation.foreign_key == name
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
                deferred is instance and foreign_key == name
                for deferred, _, foreign_key in self._deferred_foreign_keys
            ):
                continue
            if name is not None and name in instance.__dict__:
                bind_name = f"orm_insert_{index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and value is not None:
                    assignments[name] = _spatial_value(
                        bind(bind_name),
                        attribute.type_.srid,
                        attribute.type_.semantics,
                    )
                    parameters[bind_name] = _geometry_bind_value(value, self._provider)
                else:
                    assignments[name] = bind(bind_name)
                    parameters[bind_name] = value
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
        self._emit("after_insert", instance)

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
        _single_primary(mapper)
        primary = _identity(mapper, instance)[0]
        for relation in mapper.relationships:
            if relation.direction not in {"one-to-many", "one-to-one"}:
                continue
            for related in _loaded_relationship_values(instance, relation):
                setattr(related, relation.foreign_key or "", primary)

    def _update(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
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
            if isinstance(attribute.type_, Geometry) and value is not None:
                assignments[name] = _spatial_value(
                    bind(bind_name),
                    attribute.type_.srid,
                    attribute.type_.semantics,
                )
                parameters[bind_name] = _geometry_bind_value(value, self._provider)
            else:
                assignments[name] = bind(bind_name)
                parameters[bind_name] = value
        predicate, identity_parameters = _identity_predicate(mapper, instance)
        parameters.update(identity_parameters)
        if mapper.version is not None:
            version_name = mapper.version.name
            if version_name is None:
                raise OrmMappingError("colonna versione senza nome")
            current = state.original.get(version_name)
            if not isinstance(current, int) or isinstance(current, bool) or current < 1:
                raise OrmStateError("versione ottimistica non valida")
            assignments[version_name] = bind("orm_version_next")
            parameters["orm_version_next"] = current + 1
            version_column = mapper.version.column
            if version_column is None:
                raise OrmMappingError("colonna versione non associata")
            predicate = predicate & (version_column == bind("orm_version_current"))
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

    def _delete(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
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
            predicate = predicate & (column == bind("orm_version_current"))
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


class AsyncOrmSession(OrmSession):
    """Unit of work async con gli stessi mapper e invarianti della sync."""

    def __init__(self, session: Any, *, autoflush: bool = True) -> None:
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
        self._spatial_functions = _session_spatial_functions(capabilities)
        self._session = session
        self._transaction: Any | None = None
        self._identity_map = {}
        self._pending = []
        self._deleted = []
        self._flushed_deleted = []
        self._deferred_foreign_keys = []
        self._autoflush_enabled = bool(autoflush)
        self._in_flush = False
        self._listeners = {}
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
            self._transaction = await self._session.begin()
        return self._transaction

    def query(self, model: type[T]) -> AsyncOrmQuery[T]:
        self._require_active()
        mapper = _mapper(model)  # type: ignore[arg-type]
        self._require_relational_load(mapper)
        return AsyncOrmQuery(
            self, mapper, select(*_orm_projections(mapper, provider=self._provider))
        )

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
        statement = (
            select(*_orm_projections(mapper, provider=self._provider))
            .where(predicate)
            .limit(2)
        )
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
        statement = (
            select(*_orm_projections(mapper, provider=self._provider))
            .where(predicate)
            .limit(2)
        )
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
            foreign_value = instance.__dict__.get(descriptor.foreign_key)
            return (
                None
                if foreign_value is None
                else await self.get(descriptor.target, foreign_value)
            )
        owner_mapper = _mapper(type(instance))
        _single_primary(owner_mapper)
        _single_primary(target_mapper)
        owner_identity = _identity(owner_mapper, instance)[0]
        if direction in {"one-to-many", "one-to-one"}:
            foreign = target_mapper.attribute(descriptor.foreign_key or "").column
            if foreign is None:
                raise OrmMappingError("foreign key relationship senza colonna")
            rows = (
                await self.query(descriptor.target)
                .where(foreign == bind("orm_relationship_identity"))
                .all({"orm_relationship_identity": owner_identity})
            )
            if direction == "one-to-one":
                if len(rows) > 1:
                    raise MultipleResultsFound("relationship one-to-one non univoca")
                return None if not rows else rows[0]
            return rows
        secondary = descriptor.secondary
        if secondary is None:
            raise OrmMappingError("relationship many-to-many senza secondary")
        target_primary = _single_primary(target_mapper).column
        if target_primary is None:
            raise OrmMappingError("target relationship senza chiave")
        statement = (
            select(*_orm_projections(target_mapper, provider=self._provider))
            .select_from(target_mapper.table)
            .join(
                secondary,
                secondary.c[descriptor.secondary_remote_key or ""] == target_primary,
            )
            .where(
                secondary.c[descriptor.secondary_local_key or ""]
                == bind("orm_relationship_identity")
            )
        )
        return await self._execute_entities_async(
            target_mapper, statement, {"orm_relationship_identity": owner_identity}
        )

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
        instances = [await self._hydrate_row_async(mapper, row) for row in rows]
        for loader in loaders:
            relation = _query_relationship(mapper, loader.relationship)
            if loader.strategy == "joined":
                for instance, row in zip(instances, rows, strict=True):
                    await self._hydrate_joined_async(instance, relation, row)
            else:
                await self._selectin_load_async(instances, relation)
        return instances

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
            values = _mapped_row_values(
                target_mapper, row, prefix, self._provider
            )
            related = await self._hydrate_row_async(target_mapper, values)
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
                    instance.__dict__.get(relation.foreign_key)
                    for instance in instances
                    if instance.__dict__.get(relation.foreign_key) is not None
                )
            )
            related = await self._selectin_entities_async(
                target_mapper, _single_primary(target_mapper).column, values
            )
            by_identity = {_identity(target_mapper, item)[0]: item for item in related}
            for instance in instances:
                _assign_loaded_relationship(
                    instance,
                    relation,
                    by_identity.get(instance.__dict__.get(relation.foreign_key)),
                )
            return
        owner_mapper = _mapper(type(instances[0]))
        _single_primary(owner_mapper)
        _single_primary(target_mapper)
        owner_ids = tuple(_identity(owner_mapper, item)[0] for item in instances)
        if direction in {"one-to-many", "one-to-one"}:
            foreign = target_mapper.attribute(relation.foreign_key or "").column
            related = await self._selectin_entities_async(
                target_mapper, foreign, owner_ids
            )
            grouped: dict[Any, list[DeclarativeBase]] = {key: [] for key in owner_ids}
            for item in related:
                grouped.setdefault(item.__dict__.get(relation.foreign_key), []).append(
                    item
                )
            for instance, identity in zip(instances, owner_ids, strict=True):
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
        await self._selectin_many_to_many_async(instances, relation, owner_ids)

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
        statement = select(
            *_orm_projections(mapper, provider=self._provider)
        ).where(column.in_(*(bind(name) for name in parameters)))
        return await self._execute_entities_async(mapper, statement, parameters)

    async def _selectin_many_to_many_async(
        self,
        instances: list[DeclarativeBase],
        relation: Relationship[Any],
        owner_ids: tuple[Any, ...],
    ) -> None:
        secondary = relation.secondary
        target_mapper = _mapper(relation.target)
        target_primary = _single_primary(target_mapper).column
        if secondary is None or target_primary is None:
            raise OrmMappingError("many-to-many non configurata")
        parameters = {
            f"orm_eager_{index}": value for index, value in enumerate(owner_ids)
        }
        local_column = secondary.c[relation.secondary_local_key or ""]
        statement = (
            select(
                *_orm_projections(target_mapper, provider=self._provider),
                local_column.label("orm_eager_owner"),
            )
            .select_from(target_mapper.table)
            .join(
                secondary,
                secondary.c[relation.secondary_remote_key or ""] == target_primary,
            )
            .where(local_column.in_(*(bind(name) for name in parameters)))
        )
        transaction = await self._ensure_started()
        result = await transaction.execute(statement, parameters)
        if not isinstance(result, Result):
            raise OrmStateError("SELECT ORM eager senza risultato relazionale")
        grouped: dict[Any, list[DeclarativeBase]] = {key: [] for key in owner_ids}
        for row in result.all():
            grouped.setdefault(row["orm_eager_owner"], []).append(
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
            for instance in pending:
                self._synchronize_relationships(instance)
                self._preflight((instance,))
                await self._insert_async(instance)
            for instance, related, foreign_key in self._deferred_foreign_keys:
                instance.__dict__[foreign_key] = _identity(
                    _mapper(type(related)), related
                )[0]
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
            raise
        self._detach_all(restore=False)
        self._active = False
        await self._emit_async("after_commit")

    async def rollback(self) -> None:
        self._require_active()
        try:
            if self._transaction is not None:
                await self._transaction.rollback()
        finally:
            self._detach_all(restore=True)
            self._active = False
            await self._emit_async("after_rollback")

    async def close(self) -> None:
        if self._active:
            await self.rollback()

    async def _insert_async(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
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
                deferred is instance and foreign_key == name
                for deferred, _, foreign_key in self._deferred_foreign_keys
            ):
                continue
            if name is not None and name in instance.__dict__:
                bind_name = f"orm_insert_{index}"
                value = instance.__dict__[name]
                if isinstance(attribute.type_, Geometry) and value is not None:
                    assignments[name] = _spatial_value(
                        bind(bind_name), attribute.type_.srid, attribute.type_.semantics
                    )
                    parameters[bind_name] = _geometry_bind_value(value, self._provider)
                else:
                    assignments[name] = bind(bind_name)
                    parameters[bind_name] = value
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
        await self._emit_async("after_insert", instance)

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
                local_value = _identity(mapper, instance)[0]
                current = {
                    _identity(_mapper(relation.target), related)
                    for related in _loaded_relationship_values(instance, relation)
                }
                original = set(state.relationship_original.get(relation.name or "", ()))
                for remote_identity in current - original:
                    remote_value = remote_identity[0]
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    await self._transaction.execute(
                        insert(relation.secondary).values(
                            **{
                                relation.secondary_local_key or "": bind(
                                    "orm_link_local"
                                ),
                                relation.secondary_remote_key or "": bind(
                                    "orm_link_remote"
                                ),
                            }
                        ),
                        {
                            "orm_link_local": local_value,
                            "orm_link_remote": remote_value,
                        },
                    )
                for remote_identity in original - current:
                    remote_value = remote_identity[0]
                    signature = _association_signature(
                        relation, local_value, remote_value
                    )
                    if signature in seen:
                        continue
                    seen.add(signature)
                    predicate = (
                        relation.secondary.c[relation.secondary_local_key or ""]
                        == bind("orm_link_local")
                    ) & (
                        relation.secondary.c[relation.secondary_remote_key or ""]
                        == bind("orm_link_remote")
                    )
                    await self._transaction.execute(
                        delete(relation.secondary).where(predicate),
                        {
                            "orm_link_local": local_value,
                            "orm_link_remote": remote_value,
                        },
                    )
                _remember_relationship(instance, relation)

    async def _remove_deleted_associations_async(self) -> None:
        seen: set[tuple[int, str, Any]] = set()
        for instance in self._deleted:
            mapper = _mapper(type(instance))
            local_value = _identity(mapper, instance)[0]
            for relation in mapper.relationships:
                if relation.secondary is None:
                    continue
                signature = (
                    id(relation.secondary),
                    relation.secondary_local_key or "",
                    local_value,
                )
                if signature in seen:
                    continue
                seen.add(signature)
                await self._transaction.execute(
                    delete(relation.secondary).where(
                        relation.secondary.c[relation.secondary_local_key or ""]
                        == bind("orm_link_owner")
                    ),
                    {"orm_link_owner": local_value},
                )

    async def _update_async(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
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
            if isinstance(attribute.type_, Geometry) and value is not None:
                assignments[name] = _spatial_value(
                    bind(bind_name), attribute.type_.srid, attribute.type_.semantics
                )
                parameters[bind_name] = _geometry_bind_value(value, self._provider)
            else:
                assignments[name] = bind(bind_name)
                parameters[bind_name] = value
        predicate, identity_parameters = _identity_predicate(mapper, instance)
        parameters.update(identity_parameters)
        if mapper.version is not None:
            version_name = mapper.version.name
            if version_name is None:
                raise OrmMappingError("colonna versione senza nome")
            current = state.original.get(version_name)
            if not isinstance(current, int) or isinstance(current, bool) or current < 1:
                raise OrmStateError("versione ottimistica non valida")
            assignments[version_name] = bind("orm_version_next")
            parameters["orm_version_next"] = current + 1
            version_column = mapper.version.column
            if version_column is None:
                raise OrmMappingError("colonna versione non associata")
            predicate = predicate & (version_column == bind("orm_version_current"))
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

    async def _delete_async(self, instance: DeclarativeBase) -> None:
        mapper = _mapper(type(instance))
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
            predicate = predicate & (column == bind("orm_version_current"))
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


def _geometry_srid_alias(projected_name: str) -> str:
    return f"orm_geometry_srid_{projected_name}"


def _orm_projections(
    mapper: Mapper, prefix: str = "", provider: str = "postgres"
) -> tuple[Expression, ...]:
    projections: list[Expression] = []
    for attribute in mapper.attributes:
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
            if provider in _MYSQL_ORM_PROVIDERS:
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


def _query_relationship(
    mapper: Mapper, value: str | Relationship[Any]
) -> Relationship[Any]:
    relation = mapper.relationship(value) if isinstance(value, str) else value
    if relation not in mapper.relationships:
        raise OrmMappingError("relationship appartenente a un altro mapper")
    return relation


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
    if provider in _MYSQL_ORM_PROVIDERS:
        for attribute in mapper.attributes:
            if isinstance(attribute.type_, Geometry) and attribute.name is not None:
                values[_geometry_srid_alias(attribute.name)] = row[
                    _geometry_srid_alias(f"{prefix}{attribute.name}")
                ]
    return values


def _entity_projection_values(
    mapper: Mapper, row: Mapping[str, Any], index: int, provider: str
) -> dict[str, Any]:
    return _mapped_row_values(mapper, row, f"orm_entity_{index}_", provider)


def _validate_geometry_row(
    mapper: Mapper, row: Mapping[str, Any], provider: str
) -> None:
    if provider not in _MYSQL_ORM_PROVIDERS:
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
            raise OrmMappingError(
                "SRID Geometry ORM diverso dal mapping dichiarato"
            )


def _projected_entity_is_null(mapper: Mapper, values: Mapping[str, Any]) -> bool:
    return all(values.get(attribute.name) is None for attribute in mapper.primary_keys)


def _relationship_join_predicate(
    mapper: Mapper, relation: Relationship[Any]
) -> Predicate:
    target_mapper = _mapper(relation.target)
    direction = relation.direction
    if direction == "many-to-one":
        foreign = mapper.attribute(relation.foreign_key or "").column
        primary = _single_primary(target_mapper).column
    else:
        foreign = target_mapper.attribute(relation.foreign_key or "").column
        primary = _single_primary(mapper).column
    if foreign is None or primary is None:
        raise OrmMappingError("relationship priva delle colonne di join")
    return foreign == primary


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
        local_primary = _single_primary(mapper).column
        target_primary = _single_primary(target_mapper).column
        if local_primary is None or target_primary is None:
            raise OrmMappingError("join relationship privo di chiave")
        secondary = relation.secondary
        return (
            statement.select_from(mapper.table)
            .join(
                secondary,
                local_primary == secondary.c[relation.secondary_local_key or ""],
                kind=kind,
            )
            .join(
                target_mapper.table,
                secondary.c[relation.secondary_remote_key or ""] == target_primary,
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
        *_orm_projections(
            _mapper(relation.target), _loader_prefix(relation), provider
        ),
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
    relation: Relationship[Any], local_value: Any, remote_value: Any
) -> tuple[Any, ...]:
    pairs = sorted(
        (
            (relation.secondary_local_key or "", local_value),
            (relation.secondary_remote_key or "", remote_value),
        ),
        key=lambda item: item[0],
    )
    return (id(relation.secondary), *pairs[0], *pairs[1])


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
        current = column == bind(name)
        predicate = current if predicate is None else predicate & current
        parameters[name] = value
    if predicate is None:
        raise OrmMappingError("mapper privo di chiave primaria")
    return predicate, parameters


def _session_provider(session: Any) -> str:
    capabilities = getattr(session, "capabilities", None)
    provider = (
        capabilities.get("provider") if isinstance(capabilities, Mapping) else None
    )
    if provider not in {"postgres", "mysql", "mariadb", "sqlserver", "db2"}:
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
        return f"{base}({geometry_type}, {type_.srid})"
    mapping = {
        int: "INTEGER",
        str: (
            "VARCHAR(255)"
            if provider in {"mysql", "mariadb"}
            or attribute.primary_key
            or attribute.unique
            else "TEXT"
            if provider == "postgres"
            else "VARCHAR(32672)"
            if provider == "db2"
            else "NVARCHAR(255)"
        ),
        bool: "BOOLEAN" if provider not in {"sqlserver"} else "BIT",
        float: "DOUBLE PRECISION"
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
        Decimal: "DECIMAL(38, 10)",
    }
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


def _create_table_ddl(
    mapper: Mapper,
    provider: str,
    registry: Registry,
    *,
    checkfirst: bool,
) -> str:
    columns: list[str] = []
    for attribute in mapper.attributes:
        if attribute.name is None:
            raise OrmMappingError("colonna DDL senza nome")
        declaration = f"{_quote_identifier(attribute.name, provider)} {_ddl_type(attribute, provider)}"
        if attribute.generated:
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
        target = _constraint_target(constraint, registry)
        remote = ", ".join(
            _quote_identifier(item, provider) for item in constraint.target_columns
        )
        suffix = (
            "" if constraint.on_delete is None else f" ON DELETE {constraint.on_delete}"
        )
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
    if checkfirst and provider == "db2":
        raise OrmUnsupportedError("Db2 non qualifica CREATE TABLE IF NOT EXISTS")
    return f"CREATE TABLE{clause} {target} ({body})"


def _validate_migration_chain(migrations: tuple[Migration, ...]) -> None:
    seen: set[str] = set()
    previous: str | None = None
    for migration in migrations:
        if migration.revision in seen or migration.down_revision != previous:
            raise OrmMappingError("le migrazioni devono formare una catena lineare")
        seen.add(migration.revision)
        previous = migration.revision


def _migration_table_ddl(provider: str) -> str:
    if provider == "db2":
        raise OrmUnsupportedError("runner migrazioni Db2 non qualificato")
    if provider == "sqlserver":
        return (
            "IF OBJECT_ID(N'_plenora_orm_migrations', N'U') IS NULL "
            "CREATE TABLE [_plenora_orm_migrations] ("
            "[revision] NVARCHAR(255) PRIMARY KEY, "
            "[applied_at] DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
    quote = "`" if provider in {"mysql", "mariadb"} else '"'
    return (
        f"CREATE TABLE IF NOT EXISTS {quote}_plenora_orm_migrations{quote} ("
        f"{quote}revision{quote} VARCHAR(255) PRIMARY KEY, "
        f"{quote}applied_at{quote} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def _require_one_row(affected: Any) -> None:
    if not isinstance(affected, int) or isinstance(affected, bool) or affected != 1:
        raise StaleObjectError(
            "la mutazione ORM non ha interessato esattamente una riga"
        )


__all__ = [
    "AsyncMigrationRunner",
    "AsyncOrmEntityTupleQuery",
    "AsyncOrmQuery",
    "AsyncOrmRowsQuery",
    "AsyncOrmSession",
    "DeclarativeBase",
    "ForeignKeyConstraint",
    "Geometry",
    "InstanceInspection",
    "LoaderOption",
    "Mapped",
    "MappedColumn",
    "Mapper",
    "Migration",
    "MigrationRunner",
    "ObjectState",
    "OrmEntityTupleQuery",
    "OrmError",
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
    "StaleObjectError",
    "UniqueConstraint",
    "inspect_instance",
    "joinedload",
    "mapped_column",
    "mapper_registry",
    "relationship",
    "selectinload",
]
