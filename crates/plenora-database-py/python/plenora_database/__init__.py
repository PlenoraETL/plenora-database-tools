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
from ._native import MysqlSession, connect_mysql as _native_connect_mysql


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

    Scaffold v0.4-alpha — API subset ridotta rispetto a Postgres:
    - execute(sql, params) → affected_rows
    - execute_scalar(sql, params) → value
    - execute_returning_rows(sql, params) → list[dict]
    - execute_ddl(sql) → None
    - close(), __enter__/__exit__, is_closed, server_version

    Non incluso (roadmap SDK MySQL): begin()/Transaction, copy_from,
    read (Arrow stream), portable AST builders (select/insert/etc.),
    async variant.

    Placeholder MySQL: `?` (non `$1` come Postgres).

    Parametri:
      - host, database, user, password: obbligatori
      - port: default 3306
      - tls_ca_pem: bytes del PEM della CA privata. Se None, usa
        TrustServerCertificate (solo per sviluppo)
    """
    return _native_connect_mysql(host, database, user, password, port, tls_ca_pem)


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
    "MysqlSession",
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
