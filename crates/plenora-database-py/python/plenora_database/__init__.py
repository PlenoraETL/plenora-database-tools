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
from .query import Delete, Insert, Select, Update, Upsert
from ._native import aconnect as _native_aconnect
from ._native import connect as _native_connect
from ._native import (
    AsyncMysqlSession,
    MysqlSession,
    aconnect_mysql as _native_aconnect_mysql,
    connect_mysql as _native_connect_mysql,
)


def connect(dsn: str) -> Session:
    """Apre una nuova sessione Postgres (sync).

    La DSN è nel formato libpq (`host=... user=... password=... dbname=...`).
    Il probe iniziale verifica connessione + PostGIS. Fallisce con
    PlenoraError se la DSN è invalida, la rete non risponde o l'auth
    fallisce.
    """
    return Session(_native_connect(dsn))


def connect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
) -> MysqlSession:
    """Apre una nuova sessione MySQL (sync).

    API disponibili in MysqlSession:
    - `execute(sql, params) → int`
    - `execute_scalar(sql, params) → Any`
    - `execute_returning_rows(sql, params) → list[dict]`
    - `execute_ddl(sql) → None`
    - `begin(isolation, read_only, statement_timeout_ms) → Transaction`
      (v0.5+, supporta savepoints + conditional_update via Transaction
      provider-agnostic)
    - `copy_from(schema, table, source, mode, transaction_profile,
      mapping_policy, keys, update_columns) → dict` (v0.6+, bulk write
      via Arrow IPC; supporta tutti 7 WriteMode)
    - `close()`, `__enter__/__exit__`, `is_closed`, `server_version`

    Non incluso (roadmap SDK MySQL post-0.6):
    - read (Arrow stream)
    - portable AST builders (select/insert/etc.)
    - AsyncMysqlSession
    - spatial predicates + SpatialReference
    - typed params helper (uuid/date/decimal)

    Placeholder MySQL: `?` (non `$1` come Postgres).

    Parametri:
      - host, database, user, password: obbligatori
      - port: default 3306
      - tls_ca_pem: bytes del PEM della CA privata. Se None, usa
        TrustServerCertificate (solo per sviluppo)
    """
    native = _native_connect_mysql(host, database, user, password, port, tls_ca_pem)
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

    def copy_from(
        self,
        schema: str,
        table: str,
        source,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "compatible",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict:
        """Bulk write MySQL via `prepare_write` + `write` del provider.

        Supporta tutti 7 WriteMode: append (default), create,
        truncate_insert, upsert, update, delete_by_keys, replace.

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
) -> AsyncMysqlSession:
    """Apre una nuova sessione MySQL async (v0.8+).

    Awaitable analogo di `connect_mysql`. `AsyncMysqlSession` espone
    execute/execute_scalar/execute_returning_rows/execute_ddl come
    coroutines + begin() → AsyncTransaction + aread() + acopy_from().

    Uso:

        async with await aconnect_mysql("localhost", "db", "u", "p") as s:
            n = await s.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
            v = await s.execute_scalar("SELECT COUNT(*) FROM t")
    """
    return await _native_aconnect_mysql(host, database, user, password, port, tls_ca_pem)


async def aconnect(dsn: str) -> AsyncSession:
    """Apre una nuova sessione Postgres asincrona.

    Coroutine: `s = await aconnect(dsn)` oppure
    `async with await aconnect(dsn) as s: ...`.

    Sotto il cofano il probe capabilities usa il runtime tokio condiviso
    con il resto del SDK (nessuna nuova thread pool viene creata).
    """
    native = await _native_aconnect(dsn)
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
]
