"""Python SDK per plenora-database-tools.

Motori raggiungibili: **PostgreSQL**, **MySQL**, **MariaDB**, **SQL Server**,
**Oracle** e **IBM Db2 LUW** tramite `EngineConfig`, `engine_from_url` e
`async_engine_from_url`. Non c'e selezione automatica fra i prodotti: chi
dichiara un motore e finisce sull'altro viene rifiutato alla probe.

Uso base:

    import plenora_database

    config = plenora_database.EngineConfig.from_postgres_dsn(dsn)
    with plenora_database.engine_from_url(config) as engine:
        with engine.session() as s:
            cnt = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")
            row = s.select("users").where_eq("id", 1).one()
            new = s.insert("users").values(name="Ada").returning("id").one()

Le API spatial, transazionali e asincrone sono esposte da `spatial`,
`Transaction` / `AsyncTransaction` e dagli Engine sync/async.
"""

from . import spatial
from ._async_session import AsyncSession, _AsyncInspector
from ._async_transaction import AsyncTransaction
from ._engine import AsyncEngine, Engine
from ._native import (
    AsyncDatabaseSession,
    DatabaseSession,
    ReadCheckpoint,
    SessionContext,  # PFM CHG-002
    version,
)
from ._native import aconnect as _native_aconnect
from ._native import (
    aconnect_db2 as _native_aconnect_db2,
)
from ._native import (
    aconnect_mariadb as _native_aconnect_mariadb,
)
from ._native import (
    aconnect_mysql as _native_aconnect_mysql,
)
from ._native import aconnect_oracle as _native_aconnect_oracle
from ._native import (
    aconnect_sqlserver as _native_aconnect_sqlserver,
)
from ._native import connect as _native_connect
from ._native import (
    connect_db2 as _native_connect_db2,
)
from ._native import (
    connect_mariadb as _native_connect_mariadb,
)
from ._native import (
    connect_mysql as _native_connect_mysql,
)
from ._native import connect_oracle as _native_connect_oracle
from ._native import (
    connect_sqlserver as _native_connect_sqlserver,
)
from ._native import (
    create_async_db2_engine as _native_create_async_db2_engine,
)
from ._native import create_async_engine as _native_create_async_engine
from ._native import (
    create_async_mariadb_engine as _native_create_async_mariadb_engine,
)
from ._native import (
    create_async_mysql_engine as _native_create_async_mysql_engine,
)
from ._native import create_async_oracle_engine as _native_create_async_oracle_engine
from ._native import (
    create_async_sqlserver_engine as _native_create_async_sqlserver_engine,
)
from ._native import (
    create_db2_engine as _native_create_db2_engine,
)
from ._native import create_engine as _native_create_engine
from ._native import (
    create_mariadb_engine as _native_create_mariadb_engine,
)
from ._native import (
    create_mysql_engine as _native_create_mysql_engine,
)
from ._native import create_oracle_engine as _native_create_oracle_engine
from ._native import (
    create_sqlserver_engine as _native_create_sqlserver_engine,
)
from ._session import Session, _Inspector
from ._transaction import Transaction
from .asgi import DatabaseASGIMiddleware, session_dependency
from .async_query import (
    AsyncDelete,
    AsyncInsert,
    AsyncSelect,
    AsyncUpdate,
    AsyncUpsert,
    _AsyncBuilderFactory,
)
from .config import EngineConfig, PoolConfig, async_engine_from_url, engine_from_url
from .diagnostics import (
    ExplainPlan,
    ProbeReport,
    ProbeResult,
    explain,
    explain_async,
    probe_engine,
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
from .expression import (
    ArithmeticExpression,
    BindParameter,
    BindType,
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
    SelectStatement,
    Table,
    UpdateStatement,
    UpsertStatement,
    WindowExpression,
    and_,
    bind,
    case,
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
from .json_input import (
    JsonField,
    JsonGeometry,
    JsonInput,
    JsonInputError,
    JsonSchema,
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
from .query import Delete, Insert, Select, Update, Upsert, _BuilderFactory
from .protocols import AsyncSessionProtocol, SessionProtocol
from .result import MultipleResultsFound, MutationResult, NoResultFound, Result, Row
from .schema import SchemaDiff, SchemaOperation, SchemaRisk, compare_schema
from .spatial import SpatialReference
from .telemetry import InstrumentedEngine, instrument_engine
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

# Compatibilita con i nomi privati pubblicati dagli stub della famiglia.
_DatabaseInspector = _Inspector
_AsyncDatabaseInspector = _AsyncInspector


def _connect_postgres(dsn: str, tls_mode: str = "require") -> Session:
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


def _create_postgres_engine(
    dsn: str,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine:
    """Crea un Engine PostgreSQL sync condivisibile fra richieste."""
    return Engine(
        _native_create_engine(
            dsn, tls_mode, max_connections, acquire_timeout_ms
        )
    )


async def _create_async_postgres_engine(
    dsn: str,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine:
    """Crea un Engine PostgreSQL asyncio condivisibile fra richieste."""
    native = await _native_create_async_engine(
        dsn, tls_mode, max_connections, acquire_timeout_ms
    )
    return AsyncEngine(native)


def _connect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_DatabaseSessionWrapper":
    """Apre una nuova sessione MySQL (sync).

    La sessione usa la superficie comune provider-neutral. Le query native
    usano il segnaposto `?`; i builder portabili scelgono il rendering.

    TLS:
    - `tls_mode="require"` (default): TLS + verifica certificato server
      via WebPKI trust store pubblico. Se `tls_ca_pem` è passato viene
      usata come CA privata invece di WebPKI.
    - `tls_mode="insecure_trust_server"`: TLS attivo ma senza verifica
      del certificato. **Solo test/dev locali** (vulnerabile a MITM).

    `TruncateInsert` resta fail-closed: `TRUNCATE` causa un commit implicito
    e non puo rispettare il rollback promesso dal contratto. Le capability
    effettive sono disponibili su `session.capabilities`.

    Parametri:
      - host, database, user, password: obbligatori
      - port: default 3306
      - tls_ca_pem: bytes del PEM di una CA privata (opzionale)
      - tls_mode: `"require"` (default) | `"insecure_trust_server"`
    """
    native = _native_connect_mysql(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _DatabaseSessionWrapper(native)


def _connect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_DatabaseSessionWrapper":
    """Apre una nuova sessione MariaDB (sync).

    MariaDB condivide il protocollo e il segnaposto `?` con MySQL, ma usa un
    profilo distinto per catalogo, timeout, metadata ed error mapping. Il
    prodotto dichiarato viene verificato dalla probe: non c'e selezione
    automatica fra MySQL e MariaDB.

    MariaDB non vincola l'SRID nella DDL. Per operazioni spatial il chiamante
    dichiara quindi il CRS nel piano e il provider lo verifica sui valori; una
    divergenza fallisce senza accettare metadata ambigui. `geography` non e un
    tipo del prodotto. Le capability effettive sono disponibili su
    `session.capabilities`.

    Parametri e policy TLS coincidono con la factory MySQL.
    """
    native = _native_connect_mariadb(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _DatabaseSessionWrapper(native)


def _connect_sqlserver(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_DatabaseSessionWrapper":
    """Apre una nuova sessione SQL Server (sync).

    Usa il protocollo TDS e la porta predefinita 1433. Le query native usano
    segnaposto `@P1`, `@P2`, ...; i builder portabili scelgono il rendering.
    La probe rifiuta server di un prodotto diverso e pubblica le capability
    effettive su `session.capabilities`.

    Parametri e policy TLS seguono la factory MySQL, salvo la porta.
    """
    native = _native_connect_sqlserver(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _DatabaseSessionWrapper(native)


def _connect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> "_DatabaseSessionWrapper":
    """Apre una sessione IBM Db2 LUW sincrona.

    Usa la superficie provider-neutral e segnaposto SQL nativi ``?``. La
    probe pubblica le capability effettive su `session.capabilities`.

    TLS e fail-closed: ``require`` e il default; ``disable`` abilita
    plaintext soltanto quando richiesto esplicitamente. Una CA privata si
    passa come percorso persistente, perche il client IBM puo rileggerla
    quando apre nuove connessioni.
    """
    native = _native_connect_db2(
        host, database, user, password, port, tls_ca_path, tls_mode
    )
    return _DatabaseSessionWrapper(native)


def _connect_oracle(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> "_DatabaseSessionWrapper":
    """Apre una sessione Oracle thin sincrona."""
    native = _native_connect_oracle(
        host, service, user, password, port, tls_ca_path, tls_mode
    )
    return _DatabaseSessionWrapper(native)


class _DatabaseSessionWrapper(_BuilderFactory):
    """Wrapper Python-side che aggiunge `copy_from` con conversione
    automatica dell'input (pyarrow.Table / RecordBatch / list[dict] /
    pandas.DataFrame / bytes IPC) verso Arrow IPC bytes."""

    __slots__ = ("_native",)

    def __init__(self, native: DatabaseSession) -> None:
        self._native = native

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    @property
    def server_version(self) -> str:
        return self._native.server_version

    @property
    def capabilities(self) -> dict:
        return self._native.capabilities

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

    def execute(self, statement, params=None):
        if not isinstance(statement, ExecutableStatement):
            raise TypeError("execute richiede uno statement relazionale")
        from .expression import _execute_statement

        return _execute_statement(
            self._native, statement, params, self.capabilities["provider"]
        )

    def execute_sql(self, sql, params=None):
        affected = self._native.execute(sql, params)
        return MutationResult("sql", self.capabilities["provider"], affected)

    def query_sql(self, sql, params=None):
        return Result(self._native.execute_returning_rows(sql, params))

    def execute_scalar(self, sql, params=None):
        return self._native.execute_scalar(sql, params)

    def execute_ddl(self, sql):
        return self._native.execute_ddl(sql)

    @property
    def inspect(self) -> "_Inspector":
        return _Inspector(self._native)

    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: "SessionContext | None" = None,
        native_query_policy: str | None = None,
    ):
        """Apre una transazione gestita dal chiamante.

        `context` applica un `SessionContext` con la strategia del provider;
        `native_query_policy` puo limitare le query native agli statement
        ammessi dalla policy selezionata.
        """
        native = self._native.begin(
            isolation,
            read_only,
            statement_timeout_ms,
            context,
            native_query_policy,
        )
        return Transaction(native, self.capabilities["provider"])

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
    ):
        """Apre uno stream Arrow IPC su una tabella o vista.

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

        Non carica l'intero dataset in memoria: il provider legge dal proprio
        cursore in batch limitati.
        """
        return self._native.read(
            schema,
            object,
            projection,
            order_by,
            limit,
            catalog=catalog,
            checkpoint=checkpoint,
        )

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
        mapping_policy: str,
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict:
        """Esegue un bulk write tramite il provider della sessione.

        `mapping_policy` e obbligatoria. Mode e policy non qualificate dal
        provider falliscono in modo conservativo; la matrice effettiva e
        disponibile su `session.capabilities`.

        `source` accetta:
          - `pyarrow.Table` / `RecordBatch` / iterabile di RecordBatch
          - `list[dict]` (converted via pa.Table.from_pylist)
          - `pandas.DataFrame` (converted via pa.Table.from_pandas)
          - `bytes` (Arrow IPC stream self-contained)

        Ritorna la rappresentazione Python del `WriteOutcome` del core.
        """
        from ._arrow_io import _to_ipc_bytes

        ipc_bytes = _to_ipc_bytes(source)
        return self._native.copy_from(
            schema,
            table,
            ipc_bytes,
            mode,
            transaction_profile,
            mapping_policy,
            keys,
            update_columns,
        )


async def _aconnect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_AsyncDatabaseSessionWrapper":
    """Apre una nuova sessione MySQL async.

    Funzione interna usata da `async_engine_from_url`.
    """
    native = await _native_aconnect_mysql(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _AsyncDatabaseSessionWrapper(native)


async def _aconnect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_AsyncDatabaseSessionWrapper":
    """Apre una nuova sessione MariaDB async.

    Funzione interna usata da `async_engine_from_url`.
    """
    native = await _native_aconnect_mariadb(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _AsyncDatabaseSessionWrapper(native)


async def _aconnect_sqlserver(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_AsyncDatabaseSessionWrapper":
    """Apre una nuova sessione SQL Server async.

    Funzione interna usata da `async_engine_from_url`; la factory
    provider-neutral e l'ingresso pubblico.
    """
    native = await _native_aconnect_sqlserver(
        host, database, user, password, port, tls_ca_pem, tls_mode
    )
    return _AsyncDatabaseSessionWrapper(native)


async def _aconnect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> "_AsyncDatabaseSessionWrapper":
    """Apre una sessione IBM Db2 LUW asincrona.

    Default TLS, percorso CA e opt-out plaintext seguono il contratto della
    variante sincrona interna.
    """
    native = await _native_aconnect_db2(
        host, database, user, password, port, tls_ca_path, tls_mode
    )
    return _AsyncDatabaseSessionWrapper(native)


async def _aconnect_oracle(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> "_AsyncDatabaseSessionWrapper":
    """Apre una sessione Oracle thin asincrona."""
    native = await _native_aconnect_oracle(
        host, service, user, password, port, tls_ca_path, tls_mode
    )
    return _AsyncDatabaseSessionWrapper(native)


class _AsyncDatabaseSessionWrapper(_AsyncBuilderFactory):
    """Wrapper Python-side per AsyncDatabaseSession: aggiunge ergonomia
    `acopy_from` con auto-conversion source + portable AST builders
    async (`await s.select(t).where_eq(...).all()`)."""

    __slots__ = ("_native",)

    def __init__(self, native: AsyncDatabaseSession) -> None:
        self._native = native

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    @property
    def server_version(self) -> str:
        return self._native.server_version

    @property
    def capabilities(self) -> dict:
        return self._native.capabilities

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
    async def execute(self, statement, params=None):
        if not isinstance(statement, ExecutableStatement):
            raise TypeError("execute richiede uno statement relazionale")
        from .expression import _execute_statement_async

        return await _execute_statement_async(
            self._native, statement, params, self.capabilities["provider"]
        )

    async def execute_sql(self, sql, params=None):
        affected = await self._native.execute(sql, params)
        return MutationResult("sql", self.capabilities["provider"], affected)

    async def query_sql(self, sql, params=None):
        return Result(await self._native.execute_returning_rows(sql, params))

    async def execute_scalar(self, sql, params=None):
        return await self._native.execute_scalar(sql, params)

    async def execute_ddl(self, sql):
        return await self._native.execute_ddl(sql)

    @property
    def inspect(self) -> "_AsyncInspector":
        return _AsyncInspector(self._native)

    async def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: "SessionContext | None" = None,
        native_query_policy: str | None = None,
    ):
        """Come `_DatabaseSessionWrapper.begin` sync — vedi docstring per
        `context` / `native_query_policy`."""
        native = await self._native.begin(
            isolation,
            read_only,
            statement_timeout_ms,
            context,
            native_query_policy,
        )
        return AsyncTransaction(native, self.capabilities["provider"])

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
    ):
        return await self._native.aread(
            schema,
            object,
            projection,
            order_by,
            limit,
            catalog=catalog,
            checkpoint=checkpoint,
        )

    async def acopy_from(
        self,
        schema: str,
        table: str,
        source,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str,
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict:
        """Esegue un bulk write asincrono tramite il provider della sessione.

        `mapping_policy` e obbligatoria; mode e policy non qualificate dal
        provider falliscono in modo conservativo. Le capability effettive
        sono disponibili su `session.capabilities`.

        `source` accetta pyarrow/pandas/list-of-dict/bytes.
        """
        from ._arrow_io import _to_ipc_bytes

        ipc_bytes = _to_ipc_bytes(source)
        return await self._native.acopy_from(
            schema,
            table,
            ipc_bytes,
            mode,
            transaction_profile,
            mapping_policy,
            keys,
            update_columns,
        )

    async def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return await self._native.execute_portable_rows(ast_json)

    async def _execute_portable_count(self, ast_json: str) -> int:
        return await self._native.execute_portable_count(ast_json)


def _create_mysql_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine:
    """Crea un Engine MySQL con il lifecycle provider-neutral."""
    native = _native_create_mysql_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        max_connections,
        acquire_timeout_ms,
    )
    return Engine(native, _DatabaseSessionWrapper)


async def _create_async_mysql_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine:
    """Crea la variante asyncio dell'Engine MySQL."""
    native = await _native_create_async_mysql_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        max_connections,
        acquire_timeout_ms,
    )
    return AsyncEngine(native, _AsyncDatabaseSessionWrapper)


def _create_mariadb_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine:
    """Crea un Engine MariaDB esplicito e qualificato dalla probe."""
    native = _native_create_mariadb_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        max_connections,
        acquire_timeout_ms,
    )
    return Engine(native, _DatabaseSessionWrapper)


async def _create_async_mariadb_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine:
    """Crea la variante asyncio dell'Engine MariaDB."""
    native = await _native_create_async_mariadb_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        max_connections,
        acquire_timeout_ms,
    )
    return AsyncEngine(native, _AsyncDatabaseSessionWrapper)


def _create_sqlserver_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine:
    """Crea un Engine SQL Server con lifecycle Core v3."""
    native = _native_create_sqlserver_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        max_connections,
        acquire_timeout_ms,
    )
    return Engine(native, _DatabaseSessionWrapper)


async def _create_async_sqlserver_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    *,
    max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> AsyncEngine:
    """Crea la variante asyncio dell'Engine SQL Server."""
    native = await _native_create_async_sqlserver_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        max_connections,
        acquire_timeout_ms,
    )
    return AsyncEngine(native, _AsyncDatabaseSessionWrapper)


def _create_db2_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Engine:
    """Crea un Engine Db2; richiede il wheel costruito con feature Db2."""
    native = _native_create_db2_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_path,
        tls_mode,
    )
    return Engine(native, _DatabaseSessionWrapper)


async def _create_async_db2_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> AsyncEngine:
    """Crea la variante asyncio dell'Engine Db2."""
    native = await _native_create_async_db2_engine(
        host,
        database,
        user,
        password,
        port,
        tls_ca_path,
        tls_mode,
    )
    return AsyncEngine(native, _AsyncDatabaseSessionWrapper)


def _create_oracle_engine(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Engine:
    """Crea un Engine Oracle thin."""
    native = _native_create_oracle_engine(
        host, service, user, password, port, tls_ca_path, tls_mode
    )
    return Engine(native, _DatabaseSessionWrapper)


async def _create_async_oracle_engine(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> AsyncEngine:
    """Crea la variante asyncio dell'Engine Oracle thin."""
    native = await _native_create_async_oracle_engine(
        host, service, user, password, port, tls_ca_path, tls_mode
    )
    return AsyncEngine(native, _AsyncDatabaseSessionWrapper)


async def _aconnect_postgres(dsn: str, tls_mode: str = "require") -> AsyncSession:
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


#: Alias compatibili delle due sessioni di famiglia.
#:
#: La superficie e indipendente dal prodotto. Gli alias restano esportati
#: perche rimuoverli romperebbe `isinstance` e annotazioni di tipo esistenti.
__all__ = [  # noqa: RUF022 - grouped by public API surface
    "Engine",
    "AsyncEngine",
    "SessionProtocol",
    "AsyncSessionProtocol",
    "EngineConfig",
    "PoolConfig",
    "engine_from_url",
    "async_engine_from_url",
    "table",
    "column",
    "bind",
    "case",
    "func",
    "select",
    "insert",
    "update",
    "delete",
    "upsert",
    "and_",
    "or_",
    "Table",
    "Column",
    "CommonTable",
    "DerivedTable",
    "Expression",
    "ExecutableStatement",
    "FunctionExpression",
    "Predicate",
    "Ordering",
    "BindParameter",
    "ArithmeticExpression",
    "BindType",
    "CaseExpression",
    "CastExpression",
    "ScalarSubquery",
    "SelectStatement",
    "InsertStatement",
    "UpdateStatement",
    "DeleteStatement",
    "UpsertStatement",
    "WindowExpression",
    "Result",
    "Row",
    "NoResultFound",
    "MultipleResultsFound",
    "MutationResult",
    "MetaData",
    "TableMetadata",
    "ColumnMetadata",
    "SpatialColumnMetadata",
    "SchemaToken",
    "Observation",
    "NativeAttributes",
    "Index",
    "IndexElement",
    "Constraint",
    "ForeignKey",
    "SchemaDiff",
    "SchemaOperation",
    "SchemaRisk",
    "compare_schema",
    "ExplainPlan",
    "ProbeReport",
    "ProbeResult",
    "explain",
    "explain_async",
    "probe_engine",
    "DatabaseSession",
    "ReadCheckpoint",
    "AsyncDatabaseSession",
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
    "Vertex",
    "Edge",
    "Path",
    "GraphValue",
    "CypherQuery",
    "GraphCondition",
    "GraphNode",
    "GraphProperty",
    "abulk_edges",
    "abulk_vertices",
    "bulk_edges",
    "bulk_vertices",
    "edge_model",
    "graph_entity_to_model",
    "graph_model_properties",
    "graph_property_index_sql",
    "graph_query",
    "graph_unique_constraint_sql",
    "vertex_model",
    "GraphEdgeType",
    "GraphIndex",
    "GraphSchema",
    "GraphSchemaDiff",
    "GraphSchemaMigration",
    "GraphSchemaOperation",
    "GraphSchemaRisk",
    "compare_graph_schema",
    # Application integration
    "DatabaseASGIMiddleware",
    "session_dependency",
    "InstrumentedEngine",
    "instrument_engine",
    # JSON ingress: bordo Python verso ORM / Arrow
    "JsonField",
    "JsonGeometry",
    "JsonInput",
    "JsonInputError",
    "JsonSchema",
    # ORM sync verticale
    "BIGINT",
    "JSON",
    "UUID",
    "BigInteger",
    "CheckConstraint",
    "DateTime",
    "DeclarativeBase",
    "AsyncMigrationRunner",
    "AsyncOrmEntityTupleQuery",
    "Geometry",
    "Json",
    "AsyncOrmQuery",
    "AsyncOrmRowsQuery",
    "AsyncOrmSession",
    "ForeignKeyConstraint",
    "InstanceInspection",
    "LoaderOption",
    "Mapped",
    "MappedColumn",
    "Mapper",
    "Migration",
    "MigrationRunner",
    "Numeric",
    "ObjectState",
    "OrmEntityTupleQuery",
    "OrmError",
    "OrmIndex",
    "OrmMappingError",
    "OrmMetadata",
    "OrmQuery",
    "OrmRowsQuery",
    "OrmSession",
    "OrmStateError",
    "OrmUnsupportedError",
    "Registry",
    "Relationship",
    "ServerDefault",
    "String",
    "StaleObjectError",
    "UniqueConstraint",
    "Uuid",
    "inspect_instance",
    "joinedload",
    "mapped_column",
    "mapper_registry",
    "relationship",
    "selectinload",
    # Typed params helpers
    "TypedValue",
    "uuid",
    "int32",
    "int64",
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
