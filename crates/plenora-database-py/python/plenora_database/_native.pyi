"""Type stubs per il modulo nativo PyO3 (`plenora_database._native`).

Questi stubs descrivono l'interfaccia esposta dal modulo Rust compilato
via maturin. Non c'è auto-generazione con pyo3 0.23 con abi3, quindi
li manteniamo manualmente. Consumer usa i re-export dal top-level
`plenora_database` (vedi `__init__.pyi`).
"""
from typing import Any

# ============================ Functions ============================

def version() -> str: ...

def geographic_srids() -> list[int]: ...
def inspect_ewkb_geometry_type(ewkb: bytes, srid: int, dimensions: str) -> str: ...
def geometry_wkb_xy(ewkb: bytes) -> bytes: ...
def geometry_wkb(ewkb: bytes) -> bytes: ...

def validate_ewkb_reference(ewkb: bytes, srid: int, dimensions: str) -> None: ...

def compile_relational_query(ast_json: str, provider_name: str) -> tuple[str, list[str]]: ...
def compile_relational_mutation(
    ast_json: str, provider_name: str
) -> tuple[str, list[str], bool]: ...

def create_engine(
    dsn: str, tls_mode: str = "require", max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Engine: ...

def create_async_engine(
    dsn: str, tls_mode: str = "require", max_connections: int = 4,
    acquire_timeout_ms: int = 10_000,
) -> Any: ...

def create_mysql_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    max_connections: int = 4, acquire_timeout_ms: int = 10_000,
) -> Engine: ...
def create_async_mysql_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    max_connections: int = 4, acquire_timeout_ms: int = 10_000,
) -> Any: ...
def create_mariadb_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    max_connections: int = 4, acquire_timeout_ms: int = 10_000,
) -> Engine: ...
def create_async_mariadb_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    max_connections: int = 4, acquire_timeout_ms: int = 10_000,
) -> Any: ...
def create_sqlserver_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    max_connections: int = 4, acquire_timeout_ms: int = 10_000,
) -> Engine: ...
def create_async_sqlserver_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
    max_connections: int = 4, acquire_timeout_ms: int = 10_000,
) -> Any: ...
def create_db2_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Engine: ...
def create_async_db2_engine(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Any: ...
def create_oracle_engine(
    host: str, service: str, user: str, password: str,
    port: int | None = None, tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Engine: ...
def create_async_oracle_engine(
    host: str, service: str, user: str, password: str,
    port: int | None = None, tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Any: ...

# PFM CHG-002: SessionContext transaction-local.
class SessionContext:
    def __init__(self) -> None: ...
    def insert_public(self, name: str, value: str | int | bool) -> None: ...
    def insert_internal(self, name: str, value: str | int | bool) -> None: ...
    def insert_sensitive(self, name: str, value: str | int | bool) -> None: ...
    def get(self, name: str) -> str | int | bool | None: ...
    def classification(self, name: str) -> str | None: ...
    def is_empty(self) -> bool: ...
    def keys(self) -> list[str]: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

def connect(dsn: str, tls_mode: str = "require") -> Session: ...

def aconnect(dsn: str, tls_mode: str = "require") -> Any:  # awaitable → AsyncSession
    ...

class Engine:
    @property
    def metadata_cache_entries(self) -> int: ...
    @property
    def provider_kind(self) -> str: ...
    @property
    def is_disposed(self) -> bool: ...
    def session(self) -> Any: ...
    def statistics(self) -> dict: ...
    def reflect_table(
        self, schema: str | None, table: str, *,
        catalog: str | None = ..., refresh: bool = ...,
    ) -> dict: ...
    def invalidate_metadata(
        self, schema: str | None = ..., table: str | None = ...,
        *, catalog: str | None = ...,
    ) -> int: ...
    def dispose(self) -> None: ...
    def __enter__(self) -> Engine: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool: ...
    def __repr__(self) -> str: ...

class AsyncEngine:
    @property
    def metadata_cache_entries(self) -> int: ...
    @property
    def provider_kind(self) -> str: ...
    @property
    def is_disposed(self) -> bool: ...
    def session(self) -> Any: ...
    def statistics(self) -> dict: ...
    def reflect_table(
        self, schema: str | None, table: str, *,
        catalog: str | None = ..., refresh: bool = ...,
    ) -> Any: ...
    def invalidate_metadata(
        self, schema: str | None = ..., table: str | None = ...,
        *, catalog: str | None = ...,
    ) -> int: ...
    def dispose(self) -> None: ...
    def __aenter__(self) -> Any: ...
    def __aexit__(
        self, exc_type: object, exc_value: object, traceback: object
    ) -> Any: ...
    def __repr__(self) -> str: ...

def connect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> DatabaseSession: ...

def aconnect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> Any:  # awaitable → AsyncDatabaseSession
    ...

def connect_mariadb(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> DatabaseSession: ...
def aconnect_mariadb(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> Any: ...
def connect_sqlserver(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> DatabaseSession: ...
def aconnect_sqlserver(
    host: str, database: str, user: str, password: str,
    port: int | None = None, tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> Any: ...

def connect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> DatabaseSession: ...

def aconnect_db2(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Any: ...

def connect_oracle(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> DatabaseSession: ...

def aconnect_oracle(
    host: str,
    service: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_path: str | None = None,
    tls_mode: str = "require",
) -> Any: ...

# ============================ DatabaseSession ===================================

class DatabaseSession:
    @property
    def server_version(self) -> str: ...
    @property
    def capabilities(self) -> dict: ...
    @property
    def public_capabilities(self) -> dict: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def __enter__(self) -> DatabaseSession: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool: ...
    def __repr__(self) -> str: ...
    def execute(self, sql: str, params: list | None = None) -> int: ...
    def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]: ...
    def execute_ddl(self, sql: str) -> None: ...
    def inspect_catalogs(self) -> list[str]: ...
    def inspect_schemas(self) -> list[str]: ...
    def inspect_tables(self, schema: str) -> list[dict]: ...
    def inspect_describe(self, schema: str, table: str) -> dict: ...
    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: SessionContext | None = None,  # PFM CHG-002
        native_query_policy: str | None = None,  # PFM CHG-003: "allow"|"deny"
    ) -> Transaction: ...
    def execute_portable_rows(self, ast_json: str) -> list[dict]: ...
    def execute_portable_count(self, ast_json: str) -> int: ...
    def read(
        self,
        schema: str,
        object: str,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        *,
        catalog: str | None = None,
        checkpoint: ReadCheckpoint | None = None,
    ) -> BatchReader: ...
    def copy_from(
        self,
        schema: str,
        table: str,
        ipc_bytes: bytes,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "strict",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict: ...

# ============================ AsyncDatabaseSession ==============================

class AsyncDatabaseSession:
    @property
    def server_version(self) -> str: ...
    @property
    def capabilities(self) -> dict: ...
    @property
    def public_capabilities(self) -> dict: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def __aenter__(self) -> Any: ...  # awaitable → AsyncDatabaseSession
    def __aexit__(
        self, exc_type: object, exc_value: object, traceback: object
    ) -> Any: ...  # awaitable → bool
    def __repr__(self) -> str: ...
    def execute(self, sql: str, params: list | None = None) -> Any: ...  # awaitable → int
    def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...  # awaitable → Any
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> Any: ...  # awaitable → list[dict]
    def execute_ddl(self, sql: str) -> Any: ...  # awaitable → None
    def inspect_catalogs(self) -> Any: ...  # awaitable → list[str]
    def inspect_schemas(self) -> Any: ...  # awaitable → list[str]
    def inspect_tables(self, schema: str) -> Any: ...  # awaitable → list[dict]
    def inspect_describe(self, schema: str, table: str) -> Any: ...  # awaitable → dict
    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: SessionContext | None = None,  # PFM CHG-002
        native_query_policy: str | None = None,  # PFM CHG-003: "allow"|"deny"
    ) -> Any: ...  # awaitable → AsyncTransaction
    def aread(
        self,
        schema: str,
        object: str,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        *,
        catalog: str | None = None,
        checkpoint: ReadCheckpoint | None = None,
    ) -> Any: ...  # awaitable → AsyncBatchReader
    def execute_portable_rows(self, ast_json: str) -> Any: ...  # awaitable → list[dict]
    def execute_portable_count(self, ast_json: str) -> Any: ...  # awaitable → int
    def acopy_from(
        self,
        schema: str,
        table: str,
        ipc_bytes: bytes,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "strict",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> Any: ...  # awaitable → dict
# ============================ Session (sync) ============================

class Session:
    @property
    def server_version(self) -> str: ...
    @property
    def capabilities(self) -> dict: ...
    @property
    def public_capabilities(self) -> dict: ...
    @property
    def postgis_version(self) -> str | None: ...
    @property
    def age_version(self) -> str | None: ...
    @property
    def age_capabilities(self) -> dict: ...
    @property
    def age_admin_capabilities(self) -> dict: ...
    def list_graphs(self) -> list[str]: ...
    def create_graph(self, graph: str) -> None: ...
    def drop_graph(self, graph: str, *, cascade: bool = False) -> None: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def __enter__(self) -> Session: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool: ...
    def __repr__(self) -> str: ...
    def execute(self, sql: str, params: list | None = None) -> int: ...
    def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]: ...
    def execute_ddl(self, sql: str) -> None: ...
    def cypher(
        self,
        graph: str,
        cypher: str,
        columns: list[str],
        params: dict | None = None,
        *,
        max_rows: int = 10_000,
    ) -> list[dict]: ...
    def execute_portable_rows(self, ast_json: str) -> list[dict]: ...
    def execute_portable_count(self, ast_json: str) -> int: ...
    def metrics(self) -> dict: ...
    def inspect_catalogs(self) -> list[str]: ...
    def inspect_schemas(self) -> list[str]: ...
    def inspect_tables(self, schema: str) -> list[dict]: ...
    def inspect_describe(self, schema: str, table: str) -> dict: ...
    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: SessionContext | None = None,  # PFM CHG-002
        native_query_policy: str | None = None,  # PFM CHG-003: "allow"|"deny"
    ) -> Transaction: ...
    def read(
        self,
        schema: str,
        object: str,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        *,
        catalog: str | None = None,
        checkpoint: ReadCheckpoint | None = None,
    ) -> BatchReader: ...
    def copy_from(
        self,
        schema: str,
        table: str,
        ipc_bytes: bytes,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "compatible",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict: ...

# ============================ Transaction (sync) ============================

class Transaction:
    @property
    def is_active(self) -> bool: ...
    def execute(self, sql: str, params: list | None = None) -> int: ...
    def execute_scalar(self, sql: str, params: list | None = None) -> Any: ...
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> list[dict]: ...
    def execute_portable_rows(self, ast_json: str) -> list[dict]: ...
    def execute_portable_count(self, ast_json: str) -> int: ...
    def cypher(
        self,
        graph: str,
        cypher: str,
        columns: list[str],
        params: dict | None = None,
        *,
        max_rows: int = 10_000,
    ) -> list[dict]: ...
    def savepoint(self, name: str) -> None: ...
    def rollback_to_savepoint(self, name: str) -> None: ...
    def release_savepoint(self, name: str) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def conditional_update(
        self,
        update_sql: str,
        update_params: list | None = None,
        expected_affected_rows: int = 1,
        key_probe_sql: str | None = None,
        key_probe_params: list | None = None,
    ) -> None: ...
    def __enter__(self) -> Transaction: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool: ...
    def __repr__(self) -> str: ...

# ============================ AsyncSession ============================

class AsyncSession:
    @property
    def server_version(self) -> str: ...
    @property
    def capabilities(self) -> dict: ...
    @property
    def public_capabilities(self) -> dict: ...
    @property
    def postgis_version(self) -> str | None: ...
    @property
    def age_version(self) -> str | None: ...
    def age_capabilities(self) -> Any: ...
    def age_admin_capabilities(self) -> Any: ...
    def list_graphs(self) -> Any: ...
    def create_graph(self, graph: str) -> Any: ...
    def drop_graph(self, graph: str, *, cascade: bool = False) -> Any: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def metrics(self) -> dict: ...
    def cypher(
        self,
        graph: str,
        cypher: str,
        columns: list[str],
        params: dict | None = None,
        *,
        max_rows: int = 10_000,
    ) -> Any: ...
    def __aenter__(self) -> Any: ...  # awaitable → AsyncSession
    def __aexit__(
        self, exc_type: object, exc_value: object, traceback: object
    ) -> Any: ...  # awaitable → bool
    def __repr__(self) -> str: ...
    def execute(self, sql: str, params: list | None = None) -> Any: ...  # awaitable → int
    def execute_scalar(
        self, sql: str, params: list | None = None
    ) -> Any: ...  # awaitable → Any
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> Any: ...  # awaitable → list[dict]
    def execute_ddl(self, sql: str) -> Any: ...  # awaitable → None
    def inspect_catalogs(self) -> Any: ...  # awaitable → list[str]
    def inspect_schemas(self) -> Any: ...  # awaitable → list[str]
    def inspect_tables(self, schema: str) -> Any: ...  # awaitable → list[dict]
    def inspect_describe(self, schema: str, table: str) -> Any: ...  # awaitable → dict
    def execute_portable_rows(self, ast_json: str) -> Any: ...  # awaitable → list[dict]
    def execute_portable_count(self, ast_json: str) -> Any: ...  # awaitable → int
    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: SessionContext | None = None,  # PFM CHG-002
        native_query_policy: str | None = None,  # PFM CHG-003
    ) -> Any: ...  # awaitable → AsyncTransaction
    def aread(
        self,
        schema: str,
        object: str,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        *,
        catalog: str | None = None,
        checkpoint: ReadCheckpoint | None = None,
    ) -> Any: ...  # awaitable → AsyncBatchReader
    def acopy_from(
        self,
        schema: str,
        table: str,
        ipc_bytes: bytes,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "compatible",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> Any: ...  # awaitable → dict


class ReadCheckpoint:
    def __init__(
        self,
        provider: str,
        schema: str,
        object: str,
        order_by: list[tuple[str, str]],
        values: list,
        projection: list[str] | None = None,
        *,
        catalog: str | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(document: str) -> ReadCheckpoint: ...
    def to_json(self) -> str: ...
    @property
    def provider(self) -> str: ...
    @property
    def catalog(self) -> str | None: ...
    @property
    def schema(self) -> str | None: ...
    @property
    def object(self) -> str: ...
    @property
    def order_by(self) -> list[tuple[str, str]]: ...
    def __repr__(self) -> str: ...


class BatchReader:
    def __iter__(self) -> BatchReader: ...
    def __next__(self) -> bytes: ...
    def schema_bytes(self) -> bytes: ...
    def __repr__(self) -> str: ...


class AsyncBatchReader:
    def __aiter__(self) -> AsyncBatchReader: ...
    def __anext__(self) -> Any: ...  # awaitable → bytes
    def schema_bytes(self) -> Any: ...  # awaitable → bytes
    def __repr__(self) -> str: ...

# ============================ AsyncTransaction ============================

class AsyncTransaction:
    def cypher(
        self,
        graph: str,
        cypher: str,
        columns: list[str],
        params: dict | None = None,
        *,
        max_rows: int = 10_000,
    ) -> Any: ...
    @property
    def is_active(self) -> Any: ...  # awaitable → bool (nota: lock async)
    def execute(self, sql: str, params: list | None = None) -> Any: ...  # awaitable → int
    def execute_scalar(
        self, sql: str, params: list | None = None
    ) -> Any: ...  # awaitable → Any
    def execute_returning_rows(
        self, sql: str, params: list | None = None
    ) -> Any: ...  # awaitable → list[dict]
    def execute_portable_rows(self, ast_json: str) -> Any: ...  # awaitable → list[dict]
    def execute_portable_count(self, ast_json: str) -> Any: ...  # awaitable → int
    def savepoint(self, name: str) -> Any: ...  # awaitable → None
    def rollback_to_savepoint(self, name: str) -> Any: ...  # awaitable → None
    def release_savepoint(self, name: str) -> Any: ...  # awaitable → None
    def commit(self) -> Any: ...  # awaitable → None
    def rollback(self) -> Any: ...  # awaitable → None
    def conditional_update(
        self,
        update_sql: str,
        update_params: list | None = None,
        expected_affected_rows: int = 1,
        key_probe_sql: str | None = None,
        key_probe_params: list | None = None,
    ) -> Any: ...  # awaitable → None
    def __aenter__(self) -> Any: ...  # awaitable → AsyncTransaction
    def __aexit__(
        self, exc_type: object, exc_value: object, traceback: object
    ) -> Any: ...  # awaitable → bool
    def __repr__(self) -> str: ...

# ============================ Exception hierarchy ============================

class PlenoraError(RuntimeError):
    category: str
    phase: str
    retry: dict[str, object]
    remote_effect: str
    provider: str | None
    execution_id: str | None
    message: str
    details: dict | None
    diagnostics: dict | list | None
    parameter_index: int | None
    portable_type: str | None
    target_type: str | None

class PlenoraInvalidPlanError(PlenoraError): ...
class PlenoraInvalidConfigurationError(PlenoraError): ...
class PlenoraSchemaError(PlenoraError): ...
class PlenoraDataMappingError(PlenoraError): ...
class PlenoraCrsError(PlenoraError): ...
class PlenoraUnsupportedError(PlenoraError): ...
class PlenoraNotFoundError(PlenoraError): ...
class PlenoraConflictError(PlenoraError): ...
class PlenoraConcurrentModificationError(PlenoraError): ...
class PlenoraAuthenticationError(PlenoraError): ...
class PlenoraAuthorizationError(PlenoraError): ...
class PlenoraTimeoutError(PlenoraError): ...
class PlenoraCancelledError(PlenoraError): ...
class PlenoraResourceLimitError(PlenoraError): ...
class PlenoraIoError(PlenoraError): ...
class PlenoraProtocolError(PlenoraError): ...
class PlenoraTransientError(PlenoraError): ...
class PlenoraExecutionError(PlenoraError): ...
class PlenoraInternalError(PlenoraError): ...

# PFM CHG-004: commit con esito ignoto — sotto Internal ma con
# attributi extra per guidare recovery. Consumer che vuole gestire
# quarantine separatamente dalla generica "Internal" filtra qui.
class PlenoraCommitOutcomeUnknownError(PlenoraInternalError):
    automatic_retry_allowed: bool  # sempre False per outcome unknown
    recovery_action: str  # istruzione human-readable
