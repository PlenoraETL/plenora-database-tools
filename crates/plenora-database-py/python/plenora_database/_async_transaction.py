"""Wrapper Python di AsyncTransaction (F3-7).

Come `_transaction.Transaction` sync ma metodi coroutine. Include
factory methods `select/insert/update/delete/upsert` che ritornano
i builder Async, e async context manager.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, overload

from .async_query import _AsyncBuilderFactory
from .expression import ExecutableStatement, _execute_statement_async
from .graph import GraphValue, _decode_rows
from .result import MutationResult, Result

if TYPE_CHECKING:
    from ._native import AsyncTransaction as _NativeAsyncTransaction


class AsyncTransaction(_AsyncBuilderFactory):
    """Transazione asincrona user-managed. Costruita da
    `await session.begin(...)`.

    Uso raccomandato con async context manager:

        async with await s.begin() as tx:
            await tx.execute("INSERT ...")
            row = await tx.select("t").where_eq("id", 1).one()
    """

    __slots__ = ("_native", "_provider")

    def __init__(
        self,
        native: "_NativeAsyncTransaction",
        provider: str = "postgres",
    ) -> None:
        self._native = native
        self._provider = provider

    # ---------------------------- attributi ----------------------------

    async def is_active(self) -> bool:
        # Nota: la proprietà nativa ritorna un awaitable (lock async).
        return await self._native.is_active

    # ------------------------- lifecycle --------------------------------

    async def commit(self) -> None:
        await self._native.commit()

    async def rollback(self) -> None:
        await self._native.rollback()

    async def savepoint(self, name: str) -> None:
        await self._native.savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        await self._native.rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        await self._native.release_savepoint(name)

    async def conditional_update(
        self,
        update_sql: str,
        update_params: list | None = None,
        expected_affected_rows: int = 1,
        key_probe_sql: str | None = None,
        key_probe_params: list | None = None,
    ) -> None:
        """Async equivalente di `Transaction.conditional_update`.
        Vedi la docstring sync per la semantica."""
        await self._native.conditional_update(
            update_sql,
            update_params,
            expected_affected_rows,
            key_probe_sql,
            key_probe_params,
        )

    async def __aenter__(self) -> "AsyncTransaction":
        return self

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        return await self._native.__aexit__(exc_type, exc_value, traceback)

    def __repr__(self) -> str:
        return repr(self._native)

    # --------------------------- SQL raw --------------------------------

    @overload
    async def execute(self, sql: str, params: list | None = None) -> int: ...

    @overload
    async def execute(
        self,
        sql: ExecutableStatement,
        params: Mapping[str, Any] | None = None,
    ) -> Result | int | MutationResult: ...

    async def execute(self, sql, params=None):
        if isinstance(sql, ExecutableStatement):
            return await _execute_statement_async(
                self._native,
                sql,
                params,
                self._provider,
            )
        return await self._native.execute(sql, params)

    async def execute_scalar(self, sql: str, params: list | None = None) -> Any:
        return await self._native.execute_scalar(sql, params)

    async def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]:
        return await self._native.execute_returning_rows(sql, params)

    async def cypher(
        self,
        graph: str,
        query: str,
        columns: list[str],
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, GraphValue]]:
        rows = await self._native.cypher(graph, query, columns, params)
        return _decode_rows(rows)

    # ------- API interne consumate dai builder (via json AST) -----------

    async def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return await self._native.execute_portable_rows(ast_json)

    async def _execute_portable_count(self, ast_json: str) -> int:
        return await self._native.execute_portable_count(ast_json)
