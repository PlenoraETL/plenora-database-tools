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

from typing import TYPE_CHECKING, Any, Mapping, overload

from .async_query import _AsyncBuilderFactory
from .expression import ExecutableStatement, _execute_statement_async
from .graph import GraphValue, _decode_rows
from .result import MutationResult, Result

if TYPE_CHECKING:
    from ._native import AsyncSession as _NativeAsyncSession


class AsyncSession(_AsyncBuilderFactory):
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
    def capabilities(self) -> dict:
        """Capability effettivamente sondate per questa connessione."""
        return self._native.capabilities

    @property
    def postgis_version(self) -> str | None:
        return self._native.postgis_version

    @property
    def age_version(self) -> str | None:
        return self._native.age_version

    async def age_capabilities(self) -> dict:
        return await self._native.age_capabilities()

    async def age_admin_capabilities(self) -> dict:
        return await self._native.age_admin_capabilities()

    async def list_graphs(self) -> list[str]:
        return await self._native.list_graphs()

    async def create_graph(self, graph: str) -> None:
        await self._native.create_graph(graph)

    async def drop_graph(self, graph: str, *, cascade: bool = False) -> None:
        await self._native.drop_graph(graph, cascade=cascade)

    @property
    def is_closed(self) -> bool:
        return self._native.is_closed

    # --------------------------- lifecycle ------------------------------

    def close(self) -> None:
        self._native.close()

    def metrics(self) -> dict:
        """Snapshot locale dei contatori, senza I/O e senza `await`."""
        return self._native.metrics()

    async def __aenter__(self) -> "AsyncSession":
        return self

    async def __aexit__(self, *_args: Any) -> bool:
        self.close()
        return False

    def __repr__(self) -> str:
        return repr(self._native)

    # ---------------------------- SQL raw -------------------------------

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
                self.capabilities["provider"],
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
        *,
        max_rows: int = 10_000,
    ) -> list[dict[str, GraphValue]]:
        rows = await self._native.cypher(
            graph, query, columns, params, max_rows=max_rows
        )
        return _decode_rows(rows)

    async def execute_ddl(self, sql: str) -> None:
        return await self._native.execute_ddl(sql)

    @property
    def inspect(self) -> "_AsyncInspector":
        return _AsyncInspector(self._native)

    # ------------------------ Arrow batch read -------------------------

    async def aread(
        self,
        schema: str,
        object: str,
        *,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        catalog: str | None = None,
        checkpoint=None,
    ):
        """Async equivalente di `Session.read()`. Accetta gli stessi
        parametri opzionali `projection` / `order_by` / `limit`.

        Ritorna un awaitable che si risolve in `AsyncBatchReader`
        (async iterator protocol: `async for chunk in reader`).
        """
        return await self._native.aread(
            schema,
            object,
            projection,
            order_by,
            limit,
            catalog=catalog,
            checkpoint=checkpoint,
        )

    # ------------------------ Arrow bulk write -------------------------

    async def acopy_from(
        self,
        schema: str,
        table: str,
        source: Any,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "compatible",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict:
        """Bulk write async — analogo di `Session.copy_from`. Vedi la
        docstring lì per l'input `source`, i mode, le mapping policy
        e i parametri `keys` / `update_columns`.
        """
        from ._arrow_io import _to_ipc_bytes
        ipc_bytes = _to_ipc_bytes(source)
        return await self._native.acopy_from(
            schema, table, ipc_bytes, mode, transaction_profile,
            mapping_policy, keys, update_columns,
        )

    # -------------------- transactions ----------------------------------

    async def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: "SessionContext | None" = None,  # noqa: F821
        native_query_policy: str | None = None,
    ) -> "AsyncTransaction":
        """Apre una transazione async user-managed.

        Uso come async context manager (commit su exit ok, rollback
        su eccezione):

            async with await s.begin() as tx:
                await tx.execute(...)

        Options aggiuntive (PFM):
        - `context` (CHG-002): `SessionContext` transaction-local.
        - `native_query_policy` (CHG-003): "allow" (default) o "deny".
        """
        from ._async_transaction import AsyncTransaction

        native_tx = await self._native.begin(
            isolation,
            read_only,
            deferrable,
            statement_timeout_ms,
            context,
            native_query_policy,
        )
        return AsyncTransaction(native_tx, self.capabilities["provider"])

    # ------- API interne consumate dai builder (via json AST) -----------

    async def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return await self._native.execute_portable_rows(ast_json)

    async def _execute_portable_count(self, ast_json: str) -> int:
        return await self._native.execute_portable_count(ast_json)


class _AsyncInspector:
    __slots__ = ("_native",)

    def __init__(self, native: "_NativeAsyncSession") -> None:
        self._native = native

    async def catalogs(self) -> list[str]:
        return await self._native.inspect_catalogs()

    async def schemas(self) -> list[str]:
        return await self._native.inspect_schemas()

    async def tables(self, schema: str) -> list[dict]:
        return await self._native.inspect_tables(schema)

    async def describe(self, schema: str, table: str) -> dict:
        return await self._native.inspect_describe(schema, table)
