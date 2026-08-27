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

import json
from typing import Any

from .query import Delete, Insert, Select, Update, Upsert


class AsyncSelect(Select):
    """SELECT builder async. Terminali coroutines."""

    async def all(self) -> list[dict]:  # type: ignore[override]
        return await self._session._execute_portable_rows(json.dumps(self.to_ast()))

    async def one(self) -> dict | None:  # type: ignore[override]
        rows = await self.limit(2).all()
        if not rows:
            return None
        if len(rows) > 1:
            raise RuntimeError(
                f"AsyncSelect.one() atteso 0 o 1 riga, trovate {len(rows)}"
            )
        return rows[0]

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
        name = type(self).__name__
        if self._returning:
            raise RuntimeError(
                f"{name}.execute() non usa RETURNING; usa {self._execute_hint} "
                "se lo hai chiamato"
            )
        return await self._session._execute_portable_count(json.dumps(self.to_ast()))

    async def all(self) -> list[dict]:  # type: ignore[override]
        name = type(self).__name__
        if not self._returning:
            raise RuntimeError(f"{name}.all() richiede prima .returning(...)")
        return await self._session._execute_portable_rows(json.dumps(self.to_ast()))

    async def one(self) -> dict:  # type: ignore[override]
        rows = await self.all()
        if len(rows) != 1:
            raise RuntimeError(
                f"{type(self).__name__}.one() atteso 1 riga, trovate {len(rows)}"
            )
        return rows[0]


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
