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


class AsyncInsert(Insert):
    """INSERT builder async."""

    async def execute(self) -> int:  # type: ignore[override]
        if self._returning:
            raise RuntimeError(
                "AsyncInsert.execute() non usa RETURNING; usa .all() o .one() se lo hai chiamato"
            )
        return await self._session._execute_portable_count(json.dumps(self.to_ast()))

    async def all(self) -> list[dict]:  # type: ignore[override]
        if not self._returning:
            raise RuntimeError("AsyncInsert.all() richiede prima .returning(...)")
        return await self._session._execute_portable_rows(json.dumps(self.to_ast()))

    async def one(self) -> dict:  # type: ignore[override]
        rows = await self.all()
        if len(rows) != 1:
            raise RuntimeError(f"AsyncInsert.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]


class AsyncUpdate(Update):
    """UPDATE builder async."""

    async def execute(self) -> int:  # type: ignore[override]
        if self._returning:
            raise RuntimeError(
                "AsyncUpdate.execute() non usa RETURNING; usa .all() / .one() se lo hai chiamato"
            )
        return await self._session._execute_portable_count(json.dumps(self.to_ast()))

    async def all(self) -> list[dict]:  # type: ignore[override]
        if not self._returning:
            raise RuntimeError("AsyncUpdate.all() richiede prima .returning(...)")
        return await self._session._execute_portable_rows(json.dumps(self.to_ast()))

    async def one(self) -> dict:  # type: ignore[override]
        rows = await self.all()
        if len(rows) != 1:
            raise RuntimeError(f"AsyncUpdate.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]


class AsyncDelete(Delete):
    """DELETE builder async."""

    async def execute(self) -> int:  # type: ignore[override]
        if self._returning:
            raise RuntimeError(
                "AsyncDelete.execute() non usa RETURNING; usa .all() / .one() se lo hai chiamato"
            )
        return await self._session._execute_portable_count(json.dumps(self.to_ast()))

    async def all(self) -> list[dict]:  # type: ignore[override]
        if not self._returning:
            raise RuntimeError("AsyncDelete.all() richiede prima .returning(...)")
        return await self._session._execute_portable_rows(json.dumps(self.to_ast()))

    async def one(self) -> dict:  # type: ignore[override]
        rows = await self.all()
        if len(rows) != 1:
            raise RuntimeError(f"AsyncDelete.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]


class AsyncUpsert(Upsert):
    """UPSERT builder async."""

    async def execute(self) -> int:  # type: ignore[override]
        if self._returning:
            raise RuntimeError(
                "AsyncUpsert.execute() non usa RETURNING; usa .all() / .one() se lo hai chiamato"
            )
        return await self._session._execute_portable_count(json.dumps(self.to_ast()))

    async def all(self) -> list[dict]:  # type: ignore[override]
        if not self._returning:
            raise RuntimeError("AsyncUpsert.all() richiede prima .returning(...)")
        return await self._session._execute_portable_rows(json.dumps(self.to_ast()))

    async def one(self) -> dict:  # type: ignore[override]
        rows = await self.all()
        if len(rows) != 1:
            raise RuntimeError(f"AsyncUpsert.one() atteso 1 riga, trovate {len(rows)}")
        return rows[0]
