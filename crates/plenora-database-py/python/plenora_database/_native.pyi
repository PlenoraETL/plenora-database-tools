"""Type stubs per il modulo nativo PyO3 (`plenora_database._native`).

Questi stubs descrivono l'interfaccia esposta dal modulo Rust compilato
via maturin. Non c'è auto-generazione con pyo3 0.23 con abi3, quindi
li manteniamo manualmente. Consumer usa i re-export dal top-level
`plenora_database` (vedi `__init__.pyi`).
"""
from typing import Any

# ============================ Functions ============================

def version() -> str: ...

def connect(dsn: str) -> Session: ...

def aconnect(dsn: str) -> Any:  # awaitable → AsyncSession
    ...

def connect_mysql(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
) -> MysqlSession: ...

# ============================ MysqlSession (v0.4-alpha, scaffold) ============

class MysqlSession:
    @property
    def server_version(self) -> str: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def __enter__(self) -> MysqlSession: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool: ...
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
    ) -> Transaction: ...
    def read(
        self,
        schema: str,
        object: str,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
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

# ============================ Session (sync) ============================

class Session:
    @property
    def server_version(self) -> str: ...
    @property
    def postgis_version(self) -> str | None: ...
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
    ) -> Transaction: ...
    def read(
        self,
        schema: str,
        object: str,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
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
    def postgis_version(self) -> str | None: ...
    @property
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
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
    def execute_portable_rows(self, ast_json: str) -> Any: ...  # awaitable → list[dict]
    def execute_portable_count(self, ast_json: str) -> Any: ...  # awaitable → int
    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
        statement_timeout_ms: int | None = None,
    ) -> Any: ...  # awaitable → AsyncTransaction
    def aread(
        self,
        schema: str,
        object: str,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
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
    retry: str
    remote_effect: str
    provider: str | None
    execution_id: str | None
    diagnostics: dict | list | None

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
