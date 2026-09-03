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
    ReadCheckpoint,
    SessionContext,
)
from ._session import Session
from ._transaction import Transaction
from .asgi import DatabaseASGIMiddleware, session_dependency
from .config import (
    EngineConfig as EngineConfig,
    PoolConfig as PoolConfig,
    async_engine_from_url as async_engine_from_url,
    engine_from_url as engine_from_url,
)
from .diagnostics import (
    ExplainPlan,
    ProbeReport,
    ProbeResult,
    explain,
    explain_async,
    probe_engine,
)
from .json_input import JsonField, JsonGeometry, JsonInput, JsonInputError, JsonSchema
from .expression import (
    ArithmeticExpression,
    BindParameter,
    BindType as BindType,
    CaseExpression,
    CastExpression,
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
    SelectStatement as SelectStatement,
    Table,
    UpdateStatement,
    UpsertStatement,
    WindowExpression,
    and_,
    bind as bind,
    case,
    column,
    delete,
    func,
    insert,
    or_,
    select as select,
    table as table,
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
from .protocols import AsyncSessionProtocol, SessionProtocol
from .result import (
    MultipleResultsFound,
    MutationResult,
    NoResultFound,
    Result as Result,
    Row as Row,
)
from .graph import (
    CypherQuery,
    Edge,
    GraphCondition,
    GraphNode,
    GraphProperty,
    GraphValue,
    Path,
    Vertex,
    abulk_edges,
    abulk_vertices,
    bulk_edges,
    bulk_vertices,
    edge_model,
    graph_entity_to_model,
    graph_model_properties,
    graph_property_index_sql,
    graph_query,
    graph_unique_constraint_sql,
    vertex_model,
)
from .graph_schema import (
    GraphEdgeType,
    GraphIndex,
    GraphSchema,
    GraphSchemaDiff,
    GraphSchemaMigration,
    GraphSchemaOperation,
    GraphSchemaRisk,
    compare_graph_schema,
)
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
from .schema import SchemaDiff, SchemaOperation, SchemaRisk, compare_schema
from .telemetry import InstrumentedEngine, instrument_engine
from .orm import (
    BIGINT,
    JSON,
    UUID,
    AsyncMigrationRunner,
    AsyncOrmEntityTupleQuery,
    AsyncOrmQuery,
    AsyncOrmRowsQuery,
    AsyncOrmSession,
    BigInteger,
    CheckConstraint,
    DateTime,
    DeclarativeBase,
    ForeignKeyConstraint,
    Geometry,
    Json,
    InstanceInspection,
    LoaderOption,
    Mapped,
    MappedColumn,
    Mapper,
    Migration,
    MigrationRunner,
    Numeric,
    ObjectState,
    OrmEntityTupleQuery,
    OrmError,
    OrmIndex,
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
    String,
    StaleObjectError,
    UniqueConstraint,
    Uuid,
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
    """Wrapper interno usato dagli Engine della famiglia database.

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
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def __repr__(self) -> str: ...
    def execute(
        self, statement: ExecutableStatement, params: Mapping[str, Any] | None = None
    ) -> Result | MutationResult: ...
    def execute_sql(self, sql: str, params: list | None = None) -> MutationResult: ...
    def query_sql(self, sql: str, params: list | None = None) -> Result: ...
    def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
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
        catalog: str | None = None,
        checkpoint: ReadCheckpoint | None = None,
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
        mapping_policy: str,
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict: ...
    def _execute_portable_rows(self, ast_json: str) -> list[dict]: ...
    def _execute_portable_count(self, ast_json: str) -> int: ...

class _AsyncDatabaseSessionWrapper:
    """Wrapper async interno della famiglia database: vedi
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
    async def execute(
        self, statement: ExecutableStatement, params: Mapping[str, Any] | None = None
    ) -> Result | MutationResult: ...
    async def execute_sql(
        self, sql: str, params: list | None = None
    ) -> MutationResult: ...
    async def query_sql(self, sql: str, params: list | None = None) -> Result: ...
    async def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
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
        catalog: str | None = None,
        checkpoint: ReadCheckpoint | None = None,
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
        mapping_policy: str,
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
def _create_postgres_engine(
    dsn: str, tls_mode: str = "require", *, max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine[Session]: ...
async def _create_async_postgres_engine(
    dsn: str, tls_mode: str = "require", *, max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine[AsyncSession]: ...
def _connect_postgres(dsn: str, tls_mode: str = "require") -> Session: ...
async def _aconnect_postgres(dsn: str, tls_mode: str = "require") -> AsyncSession: ...

# La famiglia MySQL: sei write mode su sette per entrambi i prodotti, con
# `truncate_insert` fail-closed — `TRUNCATE` e DDL con commit implicito, e
# nessun rollback riporta indietro le righe.
#
# Due factory e non un parametro: il prodotto lo dichiara il consumatore, e la
# probe verifica quella scelta invece di compierla (ADR 0014).
def _connect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...
async def _aconnect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...
def _connect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...
async def _aconnect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...
def _connect_sqlserver(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...
async def _aconnect_sqlserver(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...
def _connect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...
async def _aconnect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...
def _connect_oracle(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> _DatabaseSessionWrapper: ...
async def _aconnect_oracle(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> _AsyncDatabaseSessionWrapper: ...
def _create_mysql_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine[_DatabaseSessionWrapper]: ...
async def _create_async_mysql_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...
def _create_mariadb_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine[_DatabaseSessionWrapper]: ...
async def _create_async_mariadb_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...
def _create_sqlserver_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine[_DatabaseSessionWrapper]: ...
async def _create_async_sqlserver_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_pem: bytes | None = ...,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...
def _create_db2_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> Engine[_DatabaseSessionWrapper]: ...
async def _create_async_db2_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...
def _create_oracle_engine(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> Engine[_DatabaseSessionWrapper]: ...
async def _create_async_oracle_engine(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = ...,
    tls_ca_path: str | None = ...,
    tls_mode: str = "require",
) -> AsyncEngine[_AsyncDatabaseSessionWrapper]: ...

__all__: list[str]
