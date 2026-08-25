"""Type stubs top-level per plenora_database."""
from typing import Any

from . import spatial as spatial
from ._async_session import AsyncSession
from ._async_transaction import AsyncTransaction
from ._native import (
    AsyncMysqlSession,
    MysqlSession,
    PlenoraAuthenticationError,
    PlenoraAuthorizationError,
    PlenoraCancelledError,
    PlenoraCommitOutcomeUnknownError,
    PlenoraConcurrentModificationError,
    PlenoraConflictError,
    PlenoraCrsError,
    PlenoraDataMappingError,
    PlenoraError,
    PlenoraExecutionError,
    PlenoraInternalError,
    PlenoraInvalidConfigurationError,
    PlenoraInvalidPlanError,
    PlenoraIoError,
    PlenoraNotFoundError,
    PlenoraProtocolError,
    PlenoraResourceLimitError,
    PlenoraSchemaError,
    PlenoraTimeoutError,
    PlenoraTransientError,
    PlenoraUnsupportedError,
    SessionContext,
)
from ._session import Session
from ._transaction import Transaction
from .async_query import (
    AsyncDelete,
    AsyncInsert,
    AsyncSelect,
    AsyncUpdate,
    AsyncUpsert,
)
from .query import Delete, Insert, Select, Update, Upsert
from .spatial import SpatialReference
from .types import (
    TypedValue,
    date,
    decimal,
    null,
    timestamp,
    timestamptz,
    uuid,
)



class _MysqlSessionWrapper:
    """Cio che `connect_mysql` restituisce davvero.

    Non e `MysqlSession`: e il wrapper Python che gli sta davanti, aggiunge la
    conversione automatica della sorgente in `copy_from` e delega tutto il
    resto. La delega passa da `__getattr__`, quindi la superficie nativa resta
    raggiungibile ma non tipizzata: cio che questo stub dichiara e cio che il
    wrapper definisce di suo.
    """

    def __init__(self, native: MysqlSession) -> None: ...
    def __getattr__(self, name: str) -> Any: ...
    @property
    def server_version(self) -> str: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def __enter__(self) -> _MysqlSessionWrapper: ...
    def __exit__(
        self, exc_type: Any, exc_value: Any, traceback: Any
    ) -> bool: ...
    def __repr__(self) -> str: ...
    def execute(self, sql: str, params: list | None = None) -> int: ...
    def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]: ...
    def execute_ddl(self, sql: str) -> None: ...
    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: SessionContext | None = None,
        native_query_policy: str | None = None,
    ) -> Any: ...
    def read(
        self,
        schema: str,
        object: str,
        *,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
    ) -> Any: ...
    def select(self, table: str, schema: str | None = None) -> Select: ...
    def insert(self, table: str, schema: str | None = None) -> Insert: ...
    def update(self, table: str, schema: str | None = None) -> Update: ...
    def delete(self, table: str, schema: str | None = None) -> Delete: ...
    def upsert(self, table: str, schema: str | None = None) -> Upsert: ...
    def copy_from(
        self,
        schema: str,
        table: str,
        source: Any,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "strict",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict: ...
    def _execute_portable_rows(self, ast_json: str) -> list[dict]: ...
    def _execute_portable_count(self, ast_json: str) -> int: ...


class _AsyncMysqlSessionWrapper:
    """Cio che `aconnect_mysql` restituisce davvero: vedi
    [`_MysqlSessionWrapper`], con `aread` e `acopy_from` al posto di `read` e
    `copy_from`."""

    def __init__(self, native: AsyncMysqlSession) -> None: ...
    def __getattr__(self, name: str) -> Any: ...
    @property
    def server_version(self) -> str: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    async def __aenter__(self) -> _AsyncMysqlSessionWrapper: ...
    async def __aexit__(
        self, exc_type: Any, exc_value: Any, traceback: Any
    ) -> bool: ...
    def __repr__(self) -> str: ...
    async def execute(self, sql: str, params: list | None = None) -> int: ...
    async def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
    async def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]: ...
    async def execute_ddl(self, sql: str) -> None: ...
    async def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: SessionContext | None = None,
        native_query_policy: str | None = None,
    ) -> Any: ...
    async def aread(
        self,
        schema: str,
        object: str,
        *,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
    ) -> Any: ...
    def select(self, table: str, schema: str | None = None) -> AsyncSelect: ...
    def insert(self, table: str, schema: str | None = None) -> AsyncInsert: ...
    def update(self, table: str, schema: str | None = None) -> AsyncUpdate: ...
    def delete(self, table: str, schema: str | None = None) -> AsyncDelete: ...
    def upsert(self, table: str, schema: str | None = None) -> AsyncUpsert: ...
    async def acopy_from(
        self,
        schema: str,
        table: str,
        source: Any,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "strict",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict: ...
    async def _execute_portable_rows(self, ast_json: str) -> list[dict]: ...
    async def _execute_portable_count(self, ast_json: str) -> int: ...


def version() -> str: ...
def connect(dsn: str, tls_mode: str = "require") -> Session: ...
async def aconnect(dsn: str, tls_mode: str = "require") -> AsyncSession: ...

# La famiglia MySQL: sei write mode su sette per entrambi i prodotti, con
# `truncate_insert` fail-closed — `TRUNCATE` e DDL con commit implicito, e
# nessun rollback riporta indietro le righe.
#
# Due factory e non un parametro: il prodotto lo dichiara il consumatore, e la
# probe verifica quella scelta invece di compierla (ADR 0014).
def connect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _MysqlSessionWrapper: ...

async def aconnect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncMysqlSessionWrapper: ...

def connect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _MysqlSessionWrapper: ...

async def aconnect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncMysqlSessionWrapper: ...


__all__: list[str]
