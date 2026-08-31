"""Type stubs top-level per plenora_database."""
from typing import Any, Mapping, overload

from . import spatial as spatial
from ._async_session import AsyncSession
from ._async_transaction import AsyncTransaction
from ._engine import AsyncEngine, Engine
from ._native import (
    AsyncDatabaseSession,
    DatabaseSession,
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
from .json_input import JsonField, JsonGeometry, JsonInput, JsonInputError, JsonSchema
from .expression import (
    BindParameter,
    BindType,
    Column,
    CommonTable,
    DeleteStatement,
    DerivedTable,
    ExecutableStatement,
    Expression,
    FunctionExpression,
    InsertStatement,
    Ordering,
    Predicate,
    ScalarSubquery,
    SelectStatement,
    Table,
    UpdateStatement,
    UpsertStatement,
    WindowExpression,
    and_,
    bind,
    column,
    delete,
    func,
    insert,
    or_,
    select,
    table,
    update,
    upsert,
)
from .async_query import (
    AsyncDelete,
    AsyncInsert,
    AsyncSelect,
    AsyncUpdate,
    AsyncUpsert,
)
from .query import Delete, Insert, Select, Update, Upsert
from .result import MultipleResultsFound, MutationResult, NoResultFound, Result, Row
from .metadata import (
    ColumnMetadata,
    Constraint,
    ForeignKey,
    Index,
    IndexElement,
    MetaData,
    NativeAttributes,
    Observation,
    SchemaToken,
    SpatialColumnMetadata,
    TableMetadata,
)
from .spatial import SpatialReference
from .orm import (
    AsyncMigrationRunner,
    AsyncOrmEntityTupleQuery,
    AsyncOrmQuery,
    AsyncOrmRowsQuery,
    AsyncOrmSession,
    DeclarativeBase,
    ForeignKeyConstraint,
    Geometry,
    InstanceInspection,
    LoaderOption,
    Mapped,
    MappedColumn,
    Mapper,
    Migration,
    MigrationRunner,
    ObjectState,
    OrmEntityTupleQuery,
    OrmError,
    OrmMappingError,
    OrmMetadata,
    OrmQuery,
    OrmRowsQuery,
    OrmSession,
    OrmStateError,
    OrmUnsupportedError,
    Registry,
    Relationship,
    ServerDefault,
    StaleObjectError,
    UniqueConstraint,
    inspect_instance,
    joinedload,
    mapped_column,
    mapper_registry,
    relationship,
    selectinload,
)
from .types import (
    TypedValue,
    date,
    decimal,
    int32,
    int64,
    null,
    timestamp,
    timestamptz,
    uuid,
)



class _DatabaseSessionWrapper:
    """Cio che `connect_mysql` restituisce davvero.

    Non e `DatabaseSession`: e il wrapper Python che gli sta davanti, aggiunge la
    conversione automatica della sorgente in `copy_from` e delega tutto il
    resto. La delega passa da `__getattr__`, quindi la superficie nativa resta
    raggiungibile ma non tipizzata: cio che questo stub dichiara e cio che il
    wrapper definisce di suo.
    """

    def __init__(self, native: DatabaseSession) -> None: ...
    def __getattr__(self, name: str) -> Any: ...
    @property
    def server_version(self) -> str: ...
    @property
    def capabilities(self) -> dict: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def __enter__(self) -> _DatabaseSessionWrapper: ...
    def __exit__(
        self, exc_type: Any, exc_value: Any, traceback: Any
    ) -> bool: ...
    def __repr__(self) -> str: ...
    @overload
    def execute(self, sql: str, params: list | None = None) -> int: ...
    @overload
    def execute(
        self, sql: ExecutableStatement, params: Mapping[str, Any] | None = None
    ) -> Result | int | MutationResult: ...
    def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]: ...
    def execute_ddl(self, sql: str) -> None: ...
    @property
    def inspect(self) -> _DatabaseInspector: ...
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


class _AsyncDatabaseSessionWrapper:
    """Cio che `aconnect_mysql` restituisce davvero: vedi
    [`_DatabaseSessionWrapper`], con `aread` e `acopy_from` al posto di `read` e
    `copy_from`."""

    def __init__(self, native: AsyncDatabaseSession) -> None: ...
    def __getattr__(self, name: str) -> Any: ...
    @property
    def server_version(self) -> str: ...
    @property
    def capabilities(self) -> dict: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    async def __aenter__(self) -> _AsyncDatabaseSessionWrapper: ...
    async def __aexit__(
        self, exc_type: Any, exc_value: Any, traceback: Any
    ) -> bool: ...
    def __repr__(self) -> str: ...
    @overload
    async def execute(self, sql: str, params: list | None = None) -> int: ...
    @overload
    async def execute(
        self, sql: ExecutableStatement, params: Mapping[str, Any] | None = None
    ) -> Result | int | MutationResult: ...
    async def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
    async def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]: ...
    async def execute_ddl(self, sql: str) -> None: ...
    @property
    def inspect(self) -> _AsyncDatabaseInspector: ...
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


class _DatabaseInspector:
    def __init__(self, native: DatabaseSession) -> None: ...
    def catalogs(self) -> list[str]: ...
    def schemas(self) -> list[str]: ...
    def tables(self, schema: str) -> list[dict]: ...
    def describe(self, schema: str, table: str) -> dict: ...


class _AsyncDatabaseInspector:
    def __init__(self, native: AsyncDatabaseSession) -> None: ...
    async def catalogs(self) -> list[str]: ...
    async def schemas(self) -> list[str]: ...
    async def tables(self, schema: str) -> list[dict]: ...
    async def describe(self, schema: str, table: str) -> dict: ...


def version() -> str: ...
def create_engine(dsn: str, tls_mode: str = "require") -> Engine[Session]: ...
async def create_async_engine(
    dsn: str, tls_mode: str = "require"
) -> AsyncEngine[AsyncSession]: ...
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
) -> _DatabaseSessionWrapper: ...

async def aconnect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...

def connect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...

async def aconnect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...

def connect_sqlserver(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...

async def aconnect_sqlserver(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...

def connect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...

async def aconnect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...

def create_mysql_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> Engine[_DatabaseSessionWrapper]: ...
async def create_async_mysql_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...
def create_mariadb_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> Engine[_DatabaseSessionWrapper]: ...
async def create_async_mariadb_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...
def create_sqlserver_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> Engine[_DatabaseSessionWrapper]: ...
async def create_async_sqlserver_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...
def create_db2_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> Engine[_DatabaseSessionWrapper]: ...
async def create_async_db2_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = ..., tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...


__all__: list[str]
