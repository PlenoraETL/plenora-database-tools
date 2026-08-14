"""Wrapper Python di AsyncSession (F3-7).

Aggiunge sopra al nativo `_native.AsyncSession`:
  - Factory methods `select` / `insert` / `update` / `delete` /
    `upsert` che ritornano subclass AsyncSelect/etc. con terminali
    (`.all()` / `.one()` / `.scalar()` / `.execute()`) come coroutine.
  - Delega trasparente delle properties e dei metodi async
    (`execute`, `execute_scalar`, `execute_returning_rows`).
  - Async context manager Python (`__aenter__` / `__aexit__`).

Uso:

    import plenora_database as p

    async def handler():
        async with await p.aconnect(dsn) as s:
            cnt = await s.execute_scalar("SELECT COUNT(*) FROM t")
            rows = await s.select("t").where_eq("id", 1).all()
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._native import AsyncSession as _NativeAsyncSession
    from .async_query import (
        AsyncDelete,
        AsyncInsert,
        AsyncSelect,
        AsyncUpdate,
        AsyncUpsert,
    )


class AsyncSession:
    """Handle asincrono alla sessione Postgres. Ottenuto da
    `await plenora_database.aconnect(dsn)`."""

    __slots__ = ("_native",)

    def __init__(self, native: "_NativeAsyncSession") -> None:
        self._native = native

    # ---------------------------- attributi ----------------------------

    @property
    def server_version(self) -> str:
        return self._native.server_version

    @property
    def postgis_version(self) -> str | None:
        return self._native.postgis_version

    @property
    def is_closed(self) -> bool:
        return self._native.is_closed

    # --------------------------- lifecycle ------------------------------

    def close(self) -> None:
        self._native.close()

    async def __aenter__(self) -> "AsyncSession":
        return self

    async def __aexit__(self, *_args: Any) -> bool:
        self.close()
        return False

    def __repr__(self) -> str:
        return repr(self._native)

    # ---------------------------- SQL raw -------------------------------

    async def execute(self, sql: str, params: list | None = None) -> int:
        return await self._native.execute(sql, params)

    async def execute_scalar(self, sql: str, params: list | None = None) -> Any:
        return await self._native.execute_scalar(sql, params)

    async def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]:
        return await self._native.execute_returning_rows(sql, params)

    # ------------------------ Arrow batch read -------------------------

    async def aread(
        self,
        schema: str,
        object: str,
        batch_rows: int | None = None,
    ):
        """Async equivalente di `Session.read()`.

        Ritorna un awaitable che si risolve in `AsyncBatchReader`
        (async iterator protocol: `async for chunk in reader`).
        """
        return await self._native.aread(schema, object, batch_rows)

    # -------------------- transactions ----------------------------------

    async def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
        statement_timeout_ms: int | None = None,
    ) -> "AsyncTransaction":
        """Apre una transazione async user-managed.

        Uso come async context manager (commit su exit ok, rollback
        su eccezione):

            async with await s.begin() as tx:
                await tx.execute(...)
        """
        from ._async_transaction import AsyncTransaction

        native_tx = await self._native.begin(
            isolation, read_only, deferrable, statement_timeout_ms
        )
        return AsyncTransaction(native_tx)

    # -------------------- portable AST builders -------------------------

    def select(self, table: str, schema: str | None = None) -> "AsyncSelect":
        from .async_query import AsyncSelect

        return AsyncSelect(self, table, schema)

    def insert(self, table: str, schema: str | None = None) -> "AsyncInsert":
        from .async_query import AsyncInsert

        return AsyncInsert(self, table, schema)

    def update(self, table: str, schema: str | None = None) -> "AsyncUpdate":
        from .async_query import AsyncUpdate

        return AsyncUpdate(self, table, schema)

    def delete(self, table: str, schema: str | None = None) -> "AsyncDelete":
        from .async_query import AsyncDelete

        return AsyncDelete(self, table, schema)

    def upsert(self, table: str, schema: str | None = None) -> "AsyncUpsert":
        from .async_query import AsyncUpsert

        return AsyncUpsert(self, table, schema)

    # ------- API interne consumate dai builder (via json AST) -----------

    async def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return await self._native.execute_portable_rows(ast_json)

    async def _execute_portable_count(self, ast_json: str) -> int:
        return await self._native.execute_portable_count(ast_json)
