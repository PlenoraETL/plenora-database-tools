"""Builder asincroni per l'AST portabile.

Estende i builder sync `Select`/`Insert`/`Update`/`Delete`/`Upsert`
ereditando la chain di predicati + `.to_ast()` e overriddando SOLO i
metodi terminali (`.all()` / `.one()` / `.scalar()` / `.execute()`)
come coroutine asincrone.

I chain-method sync (where_eq, values, set, columns, order_by, limit,
returning, conflict_target, update_on_conflict, where_spatial, ...)
ritornano `self` di tipo dinamico → funzionano immutati sui subclass
async: la catena resta AsyncSelect/AsyncInsert/etc.
"""
from __future__ import annotations

from typing import Any

from .result import MutationResult, Result, Row

from .query import (
    Delete,
    Insert,
    Select,
    Update,
    Upsert,
    _exactly_one,
    _one_or_none,
    _provider,
    _statement_json,
    _validate_returning,
)


class AsyncSelect(Select):
    """SELECT builder async. Terminali coroutines."""

    async def all(self) -> list[Row]:  # type: ignore[override]
        rows = await self._session._execute_portable_rows(_statement_json(self))
        return Result(rows).all()

    async def one(self) -> Row | None:  # type: ignore[override]
        rows = await self.limit(2).all()
        return _one_or_none(
            Result([row.as_dict() for row in rows]), type(self).__name__
        )

    async def scalar(self) -> Any:  # type: ignore[override]
        row = await self.one()
        if row is None:
            return None
        return row[0]


class _AsyncReturningMutation:
    """Terminali coroutine comuni alle mutazioni con `RETURNING`."""

    _returning: list[str]
    _session: Any
    _execute_hint = ".all() / .one()"

    async def execute(self) -> MutationResult:  # type: ignore[override]
        _validate_returning(self, required=False)
        affected = await self._session._execute_portable_count(_statement_json(self))
        return MutationResult(
            type(self).__name__.lower(), _provider(self._session), affected
        )

    async def all(self) -> list[Row]:  # type: ignore[override]
        _validate_returning(self, required=True)
        rows = await self._session._execute_portable_rows(_statement_json(self))
        return Result(rows).all()

    async def one(self) -> Row:  # type: ignore[override]
        rows = await self.all()
        return _exactly_one(
            Result([row.as_dict() for row in rows]), type(self).__name__
        )


class AsyncInsert(_AsyncReturningMutation, Insert):
    """INSERT builder async."""

    _execute_hint = ".all() o .one()"


class AsyncUpdate(_AsyncReturningMutation, Update):
    """UPDATE builder async."""


class AsyncDelete(_AsyncReturningMutation, Delete):
    """DELETE builder async."""


class AsyncUpsert(_AsyncReturningMutation, Upsert):
    """UPSERT builder async."""


class _AsyncBuilderFactory:
    """Factory async riusate da sessioni e transazioni."""

    __slots__ = ()

    def select(self, table: str, schema: str | None = None) -> AsyncSelect:
        return AsyncSelect(self, table, schema)

    def insert(self, table: str, schema: str | None = None) -> AsyncInsert:
        return AsyncInsert(self, table, schema)

    def update(self, table: str, schema: str | None = None) -> AsyncUpdate:
        return AsyncUpdate(self, table, schema)

    def delete(self, table: str, schema: str | None = None) -> AsyncDelete:
        return AsyncDelete(self, table, schema)

    def upsert(self, table: str, schema: str | None = None) -> AsyncUpsert:
        return AsyncUpsert(self, table, schema)
