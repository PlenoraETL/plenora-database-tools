"""Builder async per il portable AST (F3-7).

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

from .query import (
    Delete,
    Insert,
    Select,
    Update,
    Upsert,
    _exactly_one,
    _one_or_none,
    _statement_json,
    _validate_returning,
)


class AsyncSelect(Select):
    """SELECT builder async. Terminali coroutines."""

    async def all(self) -> list[dict]:  # type: ignore[override]
        return await self._session._execute_portable_rows(_statement_json(self))

    async def one(self) -> dict | None:  # type: ignore[override]
        return _one_or_none(await self.limit(2).all(), type(self).__name__)

    async def scalar(self) -> Any:  # type: ignore[override]
        row = await self.one()
        if row is None:
            return None
        return next(iter(row.values()))


class _AsyncReturningMutation:
    """Terminali coroutine comuni alle mutazioni con `RETURNING`."""

    _returning: list[str]
    _session: Any
    _execute_hint = ".all() / .one()"

    async def execute(self) -> int:  # type: ignore[override]
        _validate_returning(self, required=False)
        return await self._session._execute_portable_count(_statement_json(self))

    async def all(self) -> list[dict]:  # type: ignore[override]
        _validate_returning(self, required=True)
        return await self._session._execute_portable_rows(_statement_json(self))

    async def one(self) -> dict:  # type: ignore[override]
        return _exactly_one(await self.all(), type(self).__name__)


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
