"""Python SDK per plenora-database-tools.

Milestone corrente: F3-4 (portable AST builder Pythonic).

Uso base:

    import plenora_database

    with plenora_database.connect(dsn="host=localhost user=me dbname=app") as s:
        # SQL raw
        cnt = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")

        # Portable AST (provider-agnostic)
        row = s.select("users").where_eq("id", 1).one()
        new = s.insert("users").values(name="Ada").returning("id").one()
        n = s.update("users").set(name="Alan").where_eq("id", 1).execute()

Le API di spatial / transaction / async arrivano in F3-5..F3-8.
"""

from ._native import version
from ._session import Session
from ._transaction import Transaction
from ._async_session import AsyncSession
from ._async_transaction import AsyncTransaction
from .async_query import (
    AsyncDelete,
    AsyncInsert,
    AsyncSelect,
    AsyncUpdate,
    AsyncUpsert,
)
from . import spatial
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
from .errors import (
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
)
from .async_query import AsyncDelete, AsyncInsert, AsyncSelect, AsyncUpdate, AsyncUpsert  # noqa: F401
from .query import Delete, Insert, Select, Update, Upsert
from ._native import SessionContext  # PFM CHG-002
from ._native import aconnect as _native_aconnect
from ._native import connect as _native_connect
from ._native import (
    AsyncMysqlSession,
    MysqlSession,
    aconnect_mysql as _native_aconnect_mysql,
    connect_mysql as _native_connect_mysql,
)


def connect(dsn: str, tls_mode: str = "require") -> Session:
    """Apre una nuova sessione Postgres (sync).

    La DSN è nel formato libpq (`host=... user=... password=... dbname=...`).
    Il probe iniziale verifica connessione + PostGIS. Fallisce con
    PlenoraError se la DSN è invalida, la rete non risponde o l'auth
    fallisce.

    TLS (ADR-011):
    - `"require"` (default): TLS obbligatorio + WebPKI trust store pubblico.
    - `"insecure_local"`: TLS disabilitato. **Solo per test/dev locali.**

    Per CA privata / mTLS costruire il provider Rust in-process (via
    Rust binding low-level); l'API pubblica `connect(dsn)` copre solo
    i due preset di produzione più comuni.
    """
    return Session(_native_connect(dsn, tls_mode))


def connect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> MysqlSession:
    """Apre una nuova sessione MySQL (sync).

    API disponibili in MysqlSession:
    - `execute(sql, params) → int`
    - `execute_scalar(sql, params) → Any`
    - `execute_returning_rows(sql, params) → list[dict]`
    - `execute_ddl(sql) → None`
    - `begin(isolation, read_only, statement_timeout_ms) → Transaction`
    - `copy_from(schema, table, source, mode, transaction_profile,
      mapping_policy, keys, update_columns) → dict`
    - `close()`, `__enter__/__exit__`, `is_closed`, `server_version`

    Placeholder MySQL: `?` (non `$1` come Postgres).

    TLS (parity con Postgres 0.9.0):
    - `tls_mode="require"` (default): TLS + verifica certificato server
      via WebPKI trust store pubblico. Se `tls_ca_pem` è passato viene
      usata come CA privata invece di WebPKI.
    - `tls_mode="insecure_trust_server"`: TLS attivo ma senza verifica
      del certificato. **Solo test/dev locali** (vulnerabile a MITM).

    ⚠️ WriteMode residui (post-review 2026-08-15):
    - `Replace` e `TruncateInsert` sono **fail-closed Unsupported** su
      MySQL. `Replace` (staging+RENAME) perde vincoli/indici/FK; MySQL
      `TRUNCATE` è DDL con commit implicito → non rollback-safe.
      Workaround: `Create` + `Append`, o `Update` con `DELETE FROM`.

    Parametri:
      - host, database, user, password: obbligatori
      - port: default 3306
      - tls_ca_pem: bytes del PEM di una CA privata (opzionale)
      - tls_mode: `"require"` (default) | `"insecure_trust_server"`
    """
    native = _native_connect_mysql(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _MysqlSessionWrapper(native)


class _MysqlSessionWrapper:
    """Wrapper Python-side che aggiunge `copy_from` con conversione
    automatica dell'input (pyarrow.Table / RecordBatch / list[dict] /
    pandas.DataFrame / bytes IPC) verso Arrow IPC bytes."""

    __slots__ = ("_native",)

    def __init__(self, native: MysqlSession) -> None:
        self._native = native

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    @property
    def server_version(self) -> str:
        return self._native.server_version

    @property
    def is_closed(self) -> bool:
        return self._native.is_closed

    def close(self) -> None:
        self._native.close()

    def __enter__(self):
        self._native.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        return self._native.__exit__(exc_type, exc_value, traceback)

    def __repr__(self) -> str:
        return repr(self._native)

    def execute(self, sql, params=None):
        return self._native.execute(sql, params)

    def execute_scalar(self, sql, params=None):
        return self._native.execute_scalar(sql, params)

    def execute_returning_rows(self, sql, params=None):
        return self._native.execute_returning_rows(sql, params)

    def execute_ddl(self, sql):
        return self._native.execute_ddl(sql)

    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
    ):
        return self._native.begin(isolation, read_only, statement_timeout_ms)

    def read(
        self,
        schema: str,
        object: str,
        *,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
    ):
        """Apre uno stream Arrow IPC su una tabella/vista MySQL.

        Ritorna un `BatchReader` che implementa il Python iterator
        protocol; ogni `next(reader)` produce `bytes` Arrow IPC stream
        (schema + 1 record batch + EOS marker).

        Uso tipico (richiede pyarrow installato):

            import io, pyarrow.ipc as ipc
            for chunk in s.read("mydb", "large_table", limit=10000):
                batch = ipc.open_stream(io.BytesIO(chunk)).read_all()

        Parametri opzionali:
          - `projection`: lista di colonne (default: tutte)
          - `order_by`: lista di `(colonna, "asc"|"desc")` per ORDER BY
          - `limit`: numero massimo di righe (default: nessun limite)

        Non carica l'intero dataset in memoria — legge batch-by-batch
        dal cursor `mysql_async`.
        """
        return self._native.read(schema, object, projection, order_by, limit)

    # -------------------- portable AST builders (v0.9+) -----------------

    def select(self, table: str, schema: str | None = None) -> "Select":
        return Select(self, table, schema)

    def insert(self, table: str, schema: str | None = None) -> "Insert":
        return Insert(self, table, schema)

    def update(self, table: str, schema: str | None = None) -> "Update":
        return Update(self, table, schema)

    def delete(self, table: str, schema: str | None = None) -> "Delete":
        return Delete(self, table, schema)

    def upsert(self, table: str, schema: str | None = None) -> "Upsert":
        return Upsert(self, table, schema)

    def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return self._native.execute_portable_rows(ast_json)

    def _execute_portable_count(self, ast_json: str) -> int:
        return self._native.execute_portable_count(ast_json)

    def copy_from(
        self,
        schema: str,
        table: str,
        source,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "strict",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict:
        """Bulk write MySQL via `prepare_write` + `write` del provider.

        **WriteMode supportati** (post py-v0.9.1):
        - `append` (default)
        - `create` (CREATE TABLE + INSERT)
        - `upsert` (INSERT ... ON DUPLICATE KEY UPDATE)
        - `update` (UPDATE JOIN staging)
        - `delete_by_keys` (DELETE WHERE keys IN staging)

        **Fail-closed** (`PlenoraUnsupportedError`):
        - `replace` — staging + RENAME perde vincoli/indici/FK/trigger.
        - `truncate_insert` — TRUNCATE è DDL con commit implicito
          (non rollback-safe). Vedi CHANGELOG 0.9.1 per workaround.

        `mapping_policy` **deve essere** `"strict"` su MySQL (default
        post py-v0.9.2; prima era `"compatible"` che il provider
        rifiutava con `PlenoraUnsupportedError` "richiede
        MappingPolicy::Strict"). Loss preflight non ancora
        qualificato per MySQL.

        `source` accetta:
          - `pyarrow.Table` / `RecordBatch` / list[RecordBatch]
          - `list[dict]` (converted via pa.Table.from_pylist)
          - `pandas.DataFrame` (converted via pa.Table.from_pandas)
          - `bytes` (Arrow IPC stream self-contained)

        Placeholder MySQL: `?` (non `$1` come Postgres). Ritorna dict
        con struttura `WriteOutcome` del core (status, rows.confirmed
        / .inserted / .updated / .deleted / .failed / .skipped, ecc.).

        Vedi Session.copy_from docstring per parametri completi.
        """
        from ._arrow_io import _to_ipc_bytes
        ipc_bytes = _to_ipc_bytes(source)
        return self._native.copy_from(
            schema, table, ipc_bytes, mode, transaction_profile,
            mapping_policy, keys, update_columns,
        )


async def aconnect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_AsyncMysqlSessionWrapper":
    """Apre una nuova sessione MySQL async.

    Awaitable analogo di `connect_mysql` — vedi docstring per TLS,
    WriteMode residui e API.

    Uso:

        async with await aconnect_mysql("localhost", "db", "u", "p") as s:
            n = await s.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
            v = await s.execute_scalar("SELECT COUNT(*) FROM t")
    """
    native = await _native_aconnect_mysql(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _AsyncMysqlSessionWrapper(native)


class _AsyncMysqlSessionWrapper:
    """Wrapper Python-side per AsyncMysqlSession: aggiunge ergonomia
    `acopy_from` con auto-conversion source + portable AST builders
    async (`await s.select(t).where_eq(...).all()`)."""

    __slots__ = ("_native",)

    def __init__(self, native: AsyncMysqlSession) -> None:
        self._native = native

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    @property
    def server_version(self) -> str:
        return self._native.server_version

    @property
    def is_closed(self) -> bool:
        return self._native.is_closed

    def close(self) -> None:
        self._native.close()

    async def __aenter__(self):
        await self._native.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool:
        return await self._native.__aexit__(exc_type, exc_value, traceback)

    def __repr__(self) -> str:
        return repr(self._native)

    # --- delegazione async coroutines ---
    async def execute(self, sql, params=None):
        return await self._native.execute(sql, params)

    async def execute_scalar(self, sql, params=None):
        return await self._native.execute_scalar(sql, params)

    async def execute_returning_rows(self, sql, params=None):
        return await self._native.execute_returning_rows(sql, params)

    async def execute_ddl(self, sql):
        return await self._native.execute_ddl(sql)

    async def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
    ):
        return await self._native.begin(isolation, read_only, statement_timeout_ms)

    async def aread(
        self,
        schema: str,
        object: str,
        *,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
    ):
        return await self._native.aread(schema, object, projection, order_by, limit)

    async def acopy_from(
        self,
        schema: str,
        table: str,
        source,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "strict",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict:
        """Bulk write async MySQL.

        Come `_MysqlSessionWrapper.copy_from` sync — vedi docstring per
        WriteMode disponibili (5, non 7) e `mapping_policy` obbligatorio
        `"strict"` su MySQL.

        `source` accetta pyarrow/pandas/list-of-dict/bytes.
        """
        from ._arrow_io import _to_ipc_bytes
        ipc_bytes = _to_ipc_bytes(source)
        return await self._native.acopy_from(
            schema, table, ipc_bytes, mode, transaction_profile,
            mapping_policy, keys, update_columns,
        )

    # -------------------- portable AST builders async (v0.9+) -----------

    def select(self, table: str, schema: str | None = None):
        from .async_query import AsyncSelect
        return AsyncSelect(self, table, schema)

    def insert(self, table: str, schema: str | None = None):
        from .async_query import AsyncInsert
        return AsyncInsert(self, table, schema)

    def update(self, table: str, schema: str | None = None):
        from .async_query import AsyncUpdate
        return AsyncUpdate(self, table, schema)

    def delete(self, table: str, schema: str | None = None):
        from .async_query import AsyncDelete
        return AsyncDelete(self, table, schema)

    def upsert(self, table: str, schema: str | None = None):
        from .async_query import AsyncUpsert
        return AsyncUpsert(self, table, schema)

    async def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return await self._native.execute_portable_rows(ast_json)

    async def _execute_portable_count(self, ast_json: str) -> int:
        return await self._native.execute_portable_count(ast_json)


async def aconnect(dsn: str, tls_mode: str = "require") -> AsyncSession:
    """Apre una nuova sessione Postgres asincrona.

    Coroutine: `s = await aconnect(dsn)` oppure
    `async with await aconnect(dsn) as s: ...`.

    Sotto il cofano il probe capabilities usa il runtime tokio condiviso
    con il resto del SDK (nessuna nuova thread pool viene creata).

    TLS: come `connect()` sync — default `"require"` + WebPKI.
    Per test/dev locali passare `tls_mode="insecure_local"`.
    """
    native = await _native_aconnect(dsn, tls_mode)
    return AsyncSession(native)


__all__ = [
    "connect",
    "aconnect",
    "connect_mysql",
    "aconnect_mysql",
    "MysqlSession",
    "AsyncMysqlSession",
    "version",
    "Session",
    "AsyncSession",
    "Transaction",
    "AsyncTransaction",
    "Select",
    "Insert",
    "Update",
    "Delete",
    "Upsert",
    "AsyncSelect",
    "AsyncInsert",
    "AsyncUpdate",
    "AsyncDelete",
    "AsyncUpsert",
    # Spatial
    "spatial",
    "SpatialReference",
    # Typed params helpers
    "TypedValue",
    "uuid",
    "date",
    "timestamp",
    "timestamptz",
    "decimal",
    "null",
    # Errors
    "PlenoraError",
    "PlenoraInvalidPlanError",
    "PlenoraInvalidConfigurationError",
    "PlenoraSchemaError",
    "PlenoraDataMappingError",
    "PlenoraCrsError",
    "PlenoraUnsupportedError",
    "PlenoraNotFoundError",
    "PlenoraConflictError",
    "PlenoraConcurrentModificationError",
    "PlenoraAuthenticationError",
    "PlenoraAuthorizationError",
    "PlenoraTimeoutError",
    "PlenoraCancelledError",
    "PlenoraResourceLimitError",
    "PlenoraIoError",
    "PlenoraProtocolError",
    "PlenoraTransientError",
    "PlenoraExecutionError",
    "PlenoraInternalError",
    "PlenoraCommitOutcomeUnknownError",
    # PFM CHG-002
    "SessionContext",
]
