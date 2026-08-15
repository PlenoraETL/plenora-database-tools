"""Builder Pythonic per il portable AST (provider-agnostic).

Ogni builder si accumula in un dict AST via chain-methods (`.where_eq(...)`,
`.columns(...)`, `.returning(...)`) e termina con `.all()` / `.one()` /
`.scalar()` / `.execute()`. La serializzazione JSON avviene solo nei
metodi terminali; il dict AST è ispezionabile via `.to_ast()`.

Nota: le classi qui non sanno del provider — parlano il linguaggio
canonico del core Rust (`PortableStatement`). Il driver Postgres o
un futuro driver MySQL/SQL Server lo compilano nel dialetto opportuno.
"""
from __future__ import annotations

import json
from typing import Any, TYPE_CHECKING

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
        `reference.semantics` (fix driver v0.2, ora esteso review #4).

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

    def all(self) -> list[dict]:
        return self._session._execute_portable_rows(json.dumps(self.to_ast()))

    def one(self) -> dict | None:
        rows = self.limit(2).all()
        if not rows:
            return None
        if len(rows) > 1:
            raise RuntimeError("Select.one() atteso 0 o 1 riga, trovate {}".format(len(rows)))
        return rows[0]

    def scalar(self) -> Any:
        row = self.one()
        if row is None:
            return None
        # Primo valore della prima riga; se projection è columns, prendo la
        # prima colonna; se è all, prendo il primo item in insertion order.
        return next(iter(row.values()))


class Insert:
    """INSERT builder. Multi-row via `.rows([{...}, {...}])`, single-row
    via `.values(**kwargs)`. Terminali: `.execute()` (no returning),
    `.all()` / `.one()` (con returning)."""

    def __init__(self, session: "Session", table: str, schema: str | None = None) -> None:
        self._session = session
        self._table = table_ref(table, schema)
        self._columns: list[str] = []
        self._values: list[list[dict]] = []  # list of rows, each row is list[Expression]
        self._returning: list[str] = []

    def values(self, **kwargs: Any) -> "Insert":
        """Singola riga da kwargs. Se non hai ancora chiamato .columns(),
        i keys diventano le colonne."""
        if not self._columns:
            self._columns = list(kwargs.keys())
        row = [literal_expr(kwargs[col]) for col in self._columns]
        self._values.append(row)
        return self

    def rows(self, rows: list[dict]) -> "Insert":
        """Multi-row: ogni dict deve avere ESATTAMENTE le stesse chiavi.

        Fix review #13: prima le chiavi extra rispetto alla prima riga
        venivano silently ignorate (perdita di dati). Ora fail-closed
        se le chiavi non combaciano — meglio errore esplicito che
        INSERT con colonne dimenticate.
        """
        if not rows:
            return self
        if not self._columns:
            self._columns = list(rows[0].keys())
        expected = set(self._columns)
        for i, r in enumerate(rows):
            actual = set(r.keys())
            if actual != expected:
                missing = expected - actual
                extra = actual - expected
                parts = []
                if missing:
                    parts.append(f"colonne mancanti: {sorted(missing)}")
                if extra:
                    parts.append(f"chiavi extra ignorate: {sorted(extra)}")
                raise ValueError(
                    f"Insert.rows() riga {i} ha chiavi diverse dalla prima: "
                    f"{'; '.join(parts)}. Attesi esattamente {sorted(expected)}."
                )
            self._values.append([literal_expr(r[col]) for col in self._columns])
        return self

    def returning(self, *cols: str) -> "Insert":
        self._returning = list(cols)
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

    def execute(self) -> int:
        if self._returning:
            raise RuntimeError(
                "Insert.execute() non usa RETURNING; usa .all() o .one() se lo hai chiamato"
            )
        return self._session._execute_portable_count(json.dumps(self.to_ast()))

    def all(self) -> list[dict]:
        if not self._returning:
            raise RuntimeError(
                "Insert.all() richiede prima .returning(...)"
            )
        return self._session._execute_portable_rows(json.dumps(self.to_ast()))

    def one(self) -> dict:
        rows = self.all()
        if len(rows) != 1:
            raise RuntimeError(f"Insert.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]


class Update(_WhereMixin):
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

    def returning(self, *cols: str) -> "Update":
        self._returning = list(cols)
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

    def execute(self) -> int:
        if self._returning:
            raise RuntimeError(
                "Update.execute() non usa RETURNING; usa .all() / .one() se lo hai chiamato"
            )
        return self._session._execute_portable_count(json.dumps(self.to_ast()))

    def all(self) -> list[dict]:
        if not self._returning:
            raise RuntimeError("Update.all() richiede prima .returning(...)")
        return self._session._execute_portable_rows(json.dumps(self.to_ast()))

    def one(self) -> dict:
        rows = self.all()
        if len(rows) != 1:
            raise RuntimeError(f"Update.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]


class Delete(_WhereMixin):
    """DELETE builder."""

    def __init__(self, session: "Session", table: str, schema: str | None = None) -> None:
        super().__init__()
        self._session = session
        self._table = table_ref(table, schema)
        self._returning: list[str] = []

    def returning(self, *cols: str) -> "Delete":
        self._returning = list(cols)
        return self

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

    def execute(self) -> int:
        if self._returning:
            raise RuntimeError(
                "Delete.execute() non usa RETURNING; usa .all() / .one() se lo hai chiamato"
            )
        return self._session._execute_portable_count(json.dumps(self.to_ast()))

    def all(self) -> list[dict]:
        if not self._returning:
            raise RuntimeError("Delete.all() richiede prima .returning(...)")
        return self._session._execute_portable_rows(json.dumps(self.to_ast()))

    def one(self) -> dict:
        rows = self.all()
        if len(rows) != 1:
            raise RuntimeError(f"Delete.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]


class Upsert:
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
        if not self._columns:
            self._columns = list(kwargs.keys())
        row = [literal_expr(kwargs[col]) for col in self._columns]
        self._values.append(row)
        return self

    def rows(self, rows: list[dict]) -> "Upsert":
        """Multi-row: fail-closed su chiavi non uniformi (fix review #13)."""
        if not rows:
            return self
        if not self._columns:
            self._columns = list(rows[0].keys())
        expected = set(self._columns)
        for i, r in enumerate(rows):
            actual = set(r.keys())
            if actual != expected:
                missing = expected - actual
                extra = actual - expected
                parts = []
                if missing:
                    parts.append(f"colonne mancanti: {sorted(missing)}")
                if extra:
                    parts.append(f"chiavi extra ignorate: {sorted(extra)}")
                raise ValueError(
                    f"Upsert.rows() riga {i} ha chiavi diverse dalla prima: "
                    f"{'; '.join(parts)}. Attesi esattamente {sorted(expected)}."
                )
            self._values.append([literal_expr(r[col]) for col in self._columns])
        return self

    def conflict_target(self, *cols: str) -> "Upsert":
        self._conflict_target = list(cols)
        return self

    def update_on_conflict(self, **kwargs: Any) -> "Upsert":
        for col, val in kwargs.items():
            self._update_on_conflict.append([col, literal_expr(val)])
        return self

    def returning(self, *cols: str) -> "Upsert":
        self._returning = list(cols)
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

    def execute(self) -> int:
        if self._returning:
            raise RuntimeError(
                "Upsert.execute() non usa RETURNING; usa .all() / .one() se lo hai chiamato"
            )
        return self._session._execute_portable_count(json.dumps(self.to_ast()))

    def all(self) -> list[dict]:
        if not self._returning:
            raise RuntimeError("Upsert.all() richiede prima .returning(...)")
        return self._session._execute_portable_rows(json.dumps(self.to_ast()))

    def one(self) -> dict:
        rows = self.all()
        if len(rows) != 1:
            raise RuntimeError(f"Upsert.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]
