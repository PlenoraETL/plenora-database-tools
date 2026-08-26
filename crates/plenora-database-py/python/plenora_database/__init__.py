"""Python SDK per plenora-database-tools.

Motori raggiungibili: **PostgreSQL** con `connect`, **MySQL** con
`connect_mysql`, **MariaDB** con `connect_mariadb`, e i rispettivi `aconnect_*`
per la forma asincrona. Non c'e selezione automatica fra i prodotti: chi
dichiara un motore e finisce sull'altro viene rifiutato alla probe.

Uso base:

    import plenora_database

    with plenora_database.connect(dsn="host=localhost user=me dbname=app") as s:
        # SQL raw
        cnt = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")

        # Portable AST (provider-agnostic)
        row = s.select("users").where_eq("id", 1).one()
        new = s.insert("users").values(name="Ada").returning("id").one()
        n = s.update("users").set(name="Alan").where_eq("id", 1).execute()

Le API di spatial, transaction e async **ci sono**: `spatial`, `Transaction` /
`AsyncTransaction`, e le factory `aconnect*`. Questa riga diceva che sarebbero
arrivate in una milestone futura, ed e rimasta invariata mentre arrivavano.
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
    aconnect_mariadb as _native_aconnect_mariadb,
    aconnect_mysql as _native_aconnect_mysql,
    connect_mariadb as _native_connect_mariadb,
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
) -> "_MysqlSessionWrapper":
    """Apre una nuova sessione MySQL (sync).

    API disponibili in MysqlSession:
    - `execute(sql, params) → int`
    - `execute_scalar(sql, params) → Any`
    - `execute_returning_rows(sql, params) → list[dict]`
    - `execute_ddl(sql) → None`
    - `begin(isolation, read_only, statement_timeout_ms, context,
      native_query_policy) → Transaction` — `context` accetta un
      `SessionContext`, `native_query_policy` vale `"allow"` o `"deny"`.
      Le chiavi del context sono `namespace.name` e su MySQL non possono
      superare **52 caratteri**: diventano variabili utente con prefisso
      `plenora_ctx_`, e il server ne ammette 64 in tutto. Oltre quella
      soglia il piano fallisce `InvalidPlan` prima di aprire la
      transazione
    - `read(schema, object, projection, order_by, limit) → BatchReader`
      — streaming Arrow IPC bounded
    - `copy_from(schema, table, source, mode, transaction_profile,
      mapping_policy, keys, update_columns) → dict`
    - builder AST portabili: `select/insert/update/delete/upsert`, con gli
      stessi terminali di Postgres
    - `close()`, `__enter__/__exit__`, `is_closed`, `server_version`

    L'equivalente async e `aconnect_mysql`: stessa superficie, con
    `aread` e `acopy_from` al posto di `read` e `copy_from`.

    Placeholder MySQL: `?` (non `$1` come Postgres).

    TLS (parity con Postgres 0.9.0):
    - `tls_mode="require"` (default): TLS + verifica certificato server
      via WebPKI trust store pubblico. Se `tls_ca_pem` è passato viene
      usata come CA privata invece di WebPKI.
    - `tls_mode="insecure_trust_server"`: TLS attivo ma senza verifica
      del certificato. **Solo test/dev locali** (vulnerabile a MITM).

    ⚠️ WriteMode: 6 disponibili su 7.
    - `TruncateInsert` è **fail-closed Unsupported** su MySQL: `TRUNCATE`
      è DDL con commit implicito, quindi non rollback-safe, e non viene
      emulato con `DELETE` perché avrebbe semantica diversa
      (`AUTO_INCREMENT` non azzerato, trigger e log riga per riga attivi).
      Usare `Replace`, che è `DELETE FROM` + insert nella stessa
      transazione.

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


def connect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_MysqlSessionWrapper":
    """Apre una nuova sessione MariaDB (sync).

    Stessa superficie di `connect_mysql` — stesso protocollo, stessi
    placeholder `?`, stesse opzioni TLS — ma un **provider diverso**, e la
    differenza non e cosmetica: il profilo di prodotto decide le query di
    catalogo, l'istruzione di timeout (`max_statement_time` in secondi, non
    `MAX_EXECUTION_TIME` in millisecondi), i metadata pubblicati nel
    namespace `plenora.mariadb.*` e la classificazione dei codici server.

    Non c'e selezione automatica, ed e una decisione (ADR 0014): questa
    factory puntata su un server MySQL viene **rifiutata** alla probe, e
    `connect_mysql` puntata su MariaDB pure. Chi dichiara un prodotto e
    finisce sull'altro ha un problema di configurazione, non una comodita
    da assecondare.

    WriteMode: le stesse 6 su 7 di MySQL, `TruncateInsert` esclusa per la
    stessa ragione permanente.

    **Spatial: aperto**, in lettura e in scrittura. Questa docstring diceva il
    contrario — «lo spatial resta chiuso, `information_schema.columns.SRS_ID`
    non esiste» — e la ragione era vera quando e stata scritta: senza quella
    colonna il catalogo non sa dire l'SRID, e una geometria senza CRS non e
    descrivibile.

    Cio che e cambiato non e il prodotto: nessuna DDL di MariaDB puo ancora
    vincolare una colonna a un SRID, e il registro OGC risponde sempre zero. E'
    cambiato il percorso. Il CRS lo **dichiara il chiamante**, nel piano, e il
    provider lo verifica valore per valore mentre le righe passano: una
    dichiarazione che i valori smentiscono fa fallire la lettura alla riga che
    la smentisce, invece di essere creduta sulla parola.

    Da li discendono le bandiere aperte: lettura e scrittura WKB, tipi
    geometrici misti nella stessa colonna, indice spaziale, e ventuno funzioni
    spatial verificate — le **sue**, non quelle di MySQL, perche l'insieme dei
    due prodotti non coincide.

    Resta chiusa `geography`: non esiste su questo prodotto, e non e una lacuna
    di misura. Le dimensioni sono XY soltanto.

    L'equivalente async e `aconnect_mariadb`.

    Parametri: identici a `connect_mysql`.
    """
    native = _native_connect_mariadb(
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
        context: "SessionContext | None" = None,  # noqa: F821
        native_query_policy: str | None = None,
    ):
        """Apre una tx MySQL user-managed.

        Options aggiuntive (PFM, parity con Postgres SDK 0.9.0):
        - `context` (CHG-002): `SessionContext` applicato via
          `SET @plenora_ctx_*` MySQL (session-scoped).
        - `native_query_policy` (CHG-003): "allow" (default) o "deny"
          — restringe agli statement CRUD OLTP.
        """
        return self._native.begin(
            isolation,
            read_only,
            statement_timeout_ms,
            context,
            native_query_policy,
        )

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

        **WriteMode supportati** (6 su 7):
        - `append` (default)
        - `create` (CREATE TABLE + INSERT). `keys` e opzionale e diventa la
          PRIMARY KEY della tabella creata: le colonne indicate devono
          esistere nello schema Arrow, essere **non-nullable** e non
          ripetersi, altrimenti il piano viene rifiutato prima di toccare il
          server
        - `replace` (DELETE FROM + INSERT nella stessa transazione:
          il target deve già esistere e non viene ricreato, quindi
          schema, indici, FK, trigger, check, default, grant e
          AUTO_INCREMENT restano quelli di prima)
        - `upsert` (INSERT ... ON DUPLICATE KEY UPDATE)
        - `update` (UPDATE JOIN staging)
        - `delete_by_keys` (DELETE WHERE keys IN staging)

        **Fail-closed** (`PlenoraUnsupportedError`):
        - `truncate_insert` — TRUNCATE è DDL con commit implicito, quindi
          non rollback-safe, e non viene emulato con DELETE perché avrebbe
          semantica diversa (AUTO_INCREMENT non azzerato, trigger e log
          riga per riga attivi). Usare `replace`.

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


async def aconnect_mariadb(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    tls_ca_pem: bytes | None = None,
    tls_mode: str = "require",
) -> "_AsyncMysqlSessionWrapper":
    """Apre una nuova sessione MariaDB async.

    Awaitable analogo di `connect_mariadb` — vedi la sua docstring per TLS,
    write mode residui e per la ragione della factory separata.

    Uso:

        async with await aconnect_mariadb("localhost", "db", "u", "p") as s:
            n = await s.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
    """
    native = await _native_aconnect_mariadb(
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
        context: "SessionContext | None" = None,  # noqa: F821
        native_query_policy: str | None = None,
    ):
        """Come `_MysqlSessionWrapper.begin` sync — vedi docstring per
        `context` / `native_query_policy`."""
        return await self._native.begin(
            isolation,
            read_only,
            statement_timeout_ms,
            context,
            native_query_policy,
        )

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

        Come `_MysqlSessionWrapper.copy_from` sync — vedi quella docstring
        per i 6 WriteMode disponibili su 7 (`truncate_insert` resta
        fail-closed) e per `mapping_policy` obbligatorio `"strict"` su
        MySQL.

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
    "connect_mariadb",
    "aconnect_mariadb",
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
