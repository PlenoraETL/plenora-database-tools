"""Builder Pythonic per il portable AST (provider-agnostic).

Ogni builder si accumula in un dict AST via chain-methods (`.where_eq(...)`,
`.columns(...)`, `.returning(...)`) e termina con `.all()` / `.one()` /
`.scalar()` / `.execute()`. La serializzazione JSON avviene solo nei
metodi terminali; il dict AST è ispezionabile via `.to_ast()`.

Le classi non conoscono il provider: parlano il linguaggio canonico del core
Rust (`PortableStatement`), che ogni adapter compila nel proprio dialetto.
"""
from __future__ import annotations

import json
from typing import Any, TYPE_CHECKING, TypeVar

from .result import MutationResult, Result, Row

from ._ast import and_predicates, column_expr, literal_expr, table_ref
from .spatial import (
    SpatialReference,
    _spatial_predicate_dict,
    _spatial_reference_dict,
    _validate_predicate_reference_combo,
)

if TYPE_CHECKING:
    from ._session import Session


class _WhereMixin:
    """Predicati WHERE condivisi tra Select/Update/Delete."""

    def __init__(self) -> None:
        self._filter: dict | None = None

    def where_eq(self, column: str, value: Any) -> "_WhereMixin":
        self._add({"op": "eq", "column": column, "value": literal_expr(value)})
        return self

    def where_ne(self, column: str, value: Any) -> "_WhereMixin":
        self._add({"op": "ne", "column": column, "value": literal_expr(value)})
        return self

    def where_lt(self, column: str, value: Any) -> "_WhereMixin":
        self._add({"op": "lt", "column": column, "value": literal_expr(value)})
        return self

    def where_lte(self, column: str, value: Any) -> "_WhereMixin":
        self._add({"op": "lte", "column": column, "value": literal_expr(value)})
        return self

    def where_gt(self, column: str, value: Any) -> "_WhereMixin":
        self._add({"op": "gt", "column": column, "value": literal_expr(value)})
        return self

    def where_gte(self, column: str, value: Any) -> "_WhereMixin":
        self._add({"op": "gte", "column": column, "value": literal_expr(value)})
        return self

    def where_in(self, column: str, values: list) -> "_WhereMixin":
        self._add({
            "op": "in",
            "column": column,
            "values": [literal_expr(v) for v in values],
        })
        return self

    def where_between(self, column: str, low: Any, high: Any) -> "_WhereMixin":
        self._add({
            "op": "between",
            "column": column,
            "low": literal_expr(low),
            "high": literal_expr(high),
        })
        return self

    def where_like(self, column: str, pattern: str) -> "_WhereMixin":
        self._add({
            "op": "like",
            "column": column,
            "pattern": literal_expr(pattern),
        })
        return self

    def where_is_null(self, column: str) -> "_WhereMixin":
        self._add({"op": "is_null", "column": column})
        return self

    def where_is_not_null(self, column: str) -> "_WhereMixin":
        self._add({"op": "is_not_null", "column": column})
        return self

    def where_spatial(
        self,
        column: str,
        predicate: str,
        reference: SpatialReference,
        distance_meters: float | None = None,
    ) -> "_WhereMixin":
        """Predicato spaziale portable.

        Args:
            column: nome colonna geometry/geography sul target.
            predicate: "intersects" / "contains" / "within" /
                       "bounding_box" / "d_within".
            reference: `SpatialReference` (crea con `spatial.geometry(...)`
                       o `spatial.geography(...)`).
            distance_meters: obbligatorio se predicate == "d_within".

        Il core Rust compila in `ST_<Predicate>(column, ST_GeomFromEWKB($n)::<cast>)`
        con `<cast>` = `geometry` o `geography` in base a
        `reference.semantics`.

        Raises:
            ValueError: se predicate/semantics/srid producono un silent
                wrong result (es. d_within + geometry + SRID 4326).
        """
        _validate_predicate_reference_combo(predicate, reference)
        self._add({
            "op": "spatial",
            "column": column,
            "predicate": _spatial_predicate_dict(predicate, distance_meters),
            "reference": _spatial_reference_dict(reference),
        })
        return self

    def _add(self, pred: dict) -> None:
        self._filter = and_predicates(self._filter, pred)


_ReturningBuilder = TypeVar("_ReturningBuilder", bound="_ReturningMutation")


def _statement_json(builder: Any) -> str:
    """Serializza una volta sola la forma canonica usata dai due bordi."""
    return json.dumps(builder.to_ast())


def _validate_returning(builder: Any, *, required: bool) -> None:
    """Regola comune dei terminali mutation sync e async."""
    name = type(builder).__name__
    if required and not builder._returning:
        raise RuntimeError(f"{name}.all() richiede prima .returning(...)")
    if not required and builder._returning:
        raise RuntimeError(
            f"{name}.execute() non usa RETURNING; usa {builder._execute_hint} "
            "se lo hai chiamato"
        )


def _one_or_none(result: Result, name: str) -> Row | None:
    """Applica la cardinalita 0..1 senza dipendere dal bordo di esecuzione."""
    if not result:
        return None
    if len(result) > 1:
        raise RuntimeError(f"{name}.one() atteso 0 o 1 riga, trovate {len(result)}")
    return result.first()


def _exactly_one(result: Result, name: str) -> Row:
    """Applica la cardinalita esatta condivisa dai terminali mutation."""
    if len(result) != 1:
        raise RuntimeError(f"{name}.one() atteso 1 riga, trovate {len(result)}")
    return result.one()


def _provider(session: Any) -> str:
    capabilities = session.capabilities
    return str(capabilities["provider"])


class _ReturningMutation:
    """Terminali comuni alle mutazioni con `RETURNING`."""

    _returning: list[str]
    _session: Any
    _execute_hint = ".all() / .one()"

    def returning(
        self: _ReturningBuilder, *cols: str
    ) -> _ReturningBuilder:
        self._returning = list(cols)
        return self

    def execute(self) -> MutationResult:
        _validate_returning(self, required=False)
        affected = self._session._execute_portable_count(_statement_json(self))
        return MutationResult(type(self).__name__.lower(), _provider(self._session), affected)

    def all(self) -> list[Row]:
        _validate_returning(self, required=True)
        return Result(self._session._execute_portable_rows(_statement_json(self))).all()

    def one(self) -> Row:
        return _exactly_one(Result([row.as_dict() for row in self.all()]), type(self).__name__)

    def to_ast(self) -> dict:
        raise NotImplementedError


def _append_values(builder: Any, values: dict[str, Any]) -> None:
    if not builder._columns:
        builder._columns = list(values)
    builder._values.append([literal_expr(values[column]) for column in builder._columns])


def _append_rows(builder: Any, rows: list[dict]) -> None:
    if not rows:
        return
    if not builder._columns:
        builder._columns = list(rows[0])
    expected = set(builder._columns)
    for index, row in enumerate(rows):
        actual = set(row)
        if actual != expected:
            details = []
            if missing := expected - actual:
                details.append(f"colonne mancanti: {sorted(missing)}")
            if extra := actual - expected:
                details.append(f"chiavi extra ignorate: {sorted(extra)}")
            name = type(builder).__name__
            raise ValueError(
                f"{name}.rows() riga {index} ha chiavi diverse dalla prima: "
                f"{'; '.join(details)}. Attesi esattamente {sorted(expected)}."
            )
        builder._values.append(
            [literal_expr(row[column]) for column in builder._columns]
        )


class Select(_WhereMixin):
    """SELECT builder. Terminali: `.all()`, `.one()`, `.scalar()`."""

    def __init__(self, session: "Session", table: str, schema: str | None = None) -> None:
        super().__init__()
        self._session = session
        self._table = table_ref(table, schema)
        self._projection: dict = {"kind": "all"}
        self._order_by: list[dict] = []
        self._limit: int | None = None

    def columns(self, *cols: str) -> "Select":
        self._projection = {"kind": "columns", "value": list(cols)}
        return self

    def order_by(self, column: str, direction: str = "asc", nulls: str | None = None) -> "Select":
        entry: dict = {"column": column, "direction": direction}
        if nulls is not None:
            entry["nulls"] = nulls
        self._order_by.append(entry)
        return self

    def limit(self, n: int) -> "Select":
        self._limit = n
        return self

    def to_ast(self) -> dict:
        ast: dict = {
            "type": "select",
            "table": self._table,
            "projection": self._projection,
        }
        if self._filter is not None:
            ast["filter"] = self._filter
        if self._order_by:
            ast["order_by"] = self._order_by
        if self._limit is not None:
            ast["limit"] = self._limit
        return ast

    def all(self) -> list[Row]:
        return Result(self._session._execute_portable_rows(_statement_json(self))).all()

    def one(self) -> Row | None:
        return _one_or_none(
            Result([row.as_dict() for row in self.limit(2).all()]),
            type(self).__name__,
        )

    def scalar(self) -> Any:
        row = self.one()
        if row is None:
            return None
        # Primo valore della prima riga; se projection è columns, prendo la
        # prima colonna; se è all, prendo il primo item in insertion order.
        return row[0]


class Insert(_ReturningMutation):
    """INSERT builder. Multi-row via `.rows([{...}, {...}])`, single-row
    via `.values(**kwargs)`. Terminali: `.execute()` (no returning),
    `.all()` / `.one()` (con returning)."""

    _execute_hint = ".all() o .one()"

    def __init__(self, session: "Session", table: str, schema: str | None = None) -> None:
        self._session = session
        self._table = table_ref(table, schema)
        self._columns: list[str] = []
        self._values: list[list[dict]] = []  # list of rows, each row is list[Expression]
        self._returning: list[str] = []

    def values(self, **kwargs: Any) -> "Insert":
        """Singola riga da kwargs. Se non hai ancora chiamato .columns(),
        i keys diventano le colonne."""
        _append_values(self, kwargs)
        return self

    def rows(self, rows: list[dict]) -> "Insert":
        """Multi-row: ogni dict deve avere ESATTAMENTE le stesse chiavi.

        Le chiavi devono coincidere: ignorare quelle extra causerebbe perdita
        silenziosa di dati, quindi il builder fallisce in modo esplicito.
        """
        _append_rows(self, rows)
        return self

    def to_ast(self) -> dict:
        ast: dict = {
            "type": "insert",
            "table": self._table,
            "columns": self._columns,
            "values": self._values,
        }
        if self._returning:
            ast["returning"] = self._returning
        return ast



class Update(_ReturningMutation, _WhereMixin):
    """UPDATE builder. `.set(**kwargs)` per gli assignments."""

    def __init__(self, session: "Session", table: str, schema: str | None = None) -> None:
        super().__init__()
        self._session = session
        self._table = table_ref(table, schema)
        self._assignments: list[list] = []
        self._returning: list[str] = []

    def set(self, **kwargs: Any) -> "Update":
        for col, val in kwargs.items():
            self._assignments.append([col, literal_expr(val)])
        return self

    def to_ast(self) -> dict:
        ast: dict = {
            "type": "update",
            "table": self._table,
            "assignments": self._assignments,
        }
        if self._filter is not None:
            ast["filter"] = self._filter
        if self._returning:
            ast["returning"] = self._returning
        return ast



class Delete(_ReturningMutation, _WhereMixin):
    """DELETE builder."""

    def __init__(self, session: "Session", table: str, schema: str | None = None) -> None:
        super().__init__()
        self._session = session
        self._table = table_ref(table, schema)
        self._returning: list[str] = []

    def to_ast(self) -> dict:
        ast: dict = {
            "type": "delete",
            "table": self._table,
        }
        if self._filter is not None:
            ast["filter"] = self._filter
        if self._returning:
            ast["returning"] = self._returning
        return ast



class Upsert(_ReturningMutation):
    """UPSERT (INSERT ... ON CONFLICT) builder."""

    def __init__(self, session: "Session", table: str, schema: str | None = None) -> None:
        self._session = session
        self._table = table_ref(table, schema)
        self._columns: list[str] = []
        self._values: list[list[dict]] = []
        self._conflict_target: list[str] = []
        self._update_on_conflict: list[list] = []
        self._returning: list[str] = []

    def values(self, **kwargs: Any) -> "Upsert":
        _append_values(self, kwargs)
        return self

    def rows(self, rows: list[dict]) -> "Upsert":
        """Multi-row: fail-closed su chiavi non uniformi."""
        _append_rows(self, rows)
        return self

    def conflict_target(self, *cols: str) -> "Upsert":
        self._conflict_target = list(cols)
        return self

    def update_on_conflict(self, **kwargs: Any) -> "Upsert":
        for col, val in kwargs.items():
            self._update_on_conflict.append([col, literal_expr(val)])
        return self

    def to_ast(self) -> dict:
        ast: dict = {
            "type": "upsert",
            "table": self._table,
            "columns": self._columns,
            "values": self._values,
            "conflict_target": self._conflict_target,
        }
        if self._update_on_conflict:
            ast["update_on_conflict"] = self._update_on_conflict
        if self._returning:
            ast["returning"] = self._returning
        return ast


class _BuilderFactory:
    """Factory sync riusate da sessioni e transazioni."""

    __slots__ = ()

    def select(self, table: str, schema: str | None = None) -> Select:
        return Select(self, table, schema)

    def insert(self, table: str, schema: str | None = None) -> Insert:
        return Insert(self, table, schema)

    def update(self, table: str, schema: str | None = None) -> Update:
        return Update(self, table, schema)

    def delete(self, table: str, schema: str | None = None) -> Delete:
        return Delete(self, table, schema)

    def upsert(self, table: str, schema: str | None = None) -> Upsert:
        return Upsert(self, table, schema)
