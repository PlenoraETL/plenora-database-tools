"""Wrapper Python di alto livello attorno alla Session nativa.

La Session nativa (`plenora_database._native.Session`) espone metodi
di basso livello. Questo wrapper:
  - Aggiunge factory methods `select` / `insert` / `update` / `delete`
    / `upsert` che ritornano i builder di `plenora_database.query`.
  - Fa da forward proxy trasparente per gli altri metodi/getters.
  - Implementa il context manager Python (`__enter__` / `__exit__`).
"""
from __future__ import annotations

from typing import Any

from ._arrow_io import _to_ipc_bytes
from ._native import Session as _NativeSession
from ._transaction import Transaction
from .query import Delete, Insert, Select, Update, Upsert


class Session:
    """Handle alla sessione Postgres. Restituita da
    `plenora_database.connect(dsn)`. Context-manager friendly."""

    __slots__ = ("_native",)

    def __init__(self, native: _NativeSession) -> None:
        self._native = native

    # ---------------------------- attributi ----------------------------

    @property
    def server_version(self) -> str:
        return self._native.server_version

    @property
    def postgis_version(self) -> str | None:
        return self._native.postgis_version

    @property
    def is_closed(self) -> bool:
        return self._native.is_closed

    # -------------------------- lifecycle ------------------------------

    def close(self) -> None:
        self._native.close()

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *_args: Any) -> bool:
        self.close()
        return False

    def __repr__(self) -> str:
        return repr(self._native)

    # --------------------------- SQL raw --------------------------------

    def execute(self, sql: str, params: list | None = None) -> int:
        return self._native.execute(sql, params)

    def execute_scalar(self, sql: str, params: list | None = None) -> Any:
        return self._native.execute_scalar(sql, params)

    def execute_returning_rows(self, sql: str, params: list | None = None) -> list[dict]:
        return self._native.execute_returning_rows(sql, params)

    # ------------------------ Arrow batch read -------------------------

    def read(
        self,
        schema: str,
        object: str,
        *,
        projection: list[str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
    ):
        """Apre uno stream Arrow IPC su una tabella/vista Postgres.

        Ritorna un `BatchReader` che implementa il Python iterator
        protocol; ogni `next(reader)` produce `bytes` Arrow IPC stream
        (schema + 1 record batch + EOS marker).

        Parametri opzionali:
          - `projection`: lista di colonne da leggere (default: tutte)
          - `order_by`: lista di `(colonna, "asc"|"desc")` per ORDER BY
          - `limit`: numero massimo di righe (default: nessun limite)

        Uso tipico (richiede pyarrow installato):

            import io, pyarrow.ipc as ipc
            for chunk in s.read(
                "public", "large_table",
                projection=["id", "amount"],
                order_by=[("id", "asc")],
                limit=10000,
            ):
                batch = ipc.open_stream(io.BytesIO(chunk)).read_all()

        Non carica l'intero dataset in memoria — legge batch-by-batch
        dal cursor server-side. La size dei batch è decisa dal provider
        (Postgres: bounded dal buffer del cursore server-side).

        Nota: per filter WHERE, usa il builder pythonic
        `s.select(table).where_eq(...).all()` (path OLTP portable AST).
        Lo streaming con filter arriverà come `Select.stream()` in v0.3+.
        """
        return self._native.read(schema, object, projection, order_by, limit)

    # ------------------------ Arrow bulk write -------------------------

    def copy_from(
        self,
        schema: str,
        table: str,
        source: Any,
        *,
        mode: str = "append",
        transaction_profile: str = "single_transaction",
        mapping_policy: str = "compatible",
        keys: list[str] | None = None,
        update_columns: list[str] | None = None,
    ) -> dict:
        """Bulk write via `prepare_write` + `write` del provider.

        Postgres usa COPY internamente per mode `append`; per gli altri
        mode combina COPY con DDL / SQL (CREATE / TRUNCATE / INSERT ...
        ON CONFLICT / UPDATE ... FROM / DELETE ...).

        Parametri:
          - `schema`, `table`: target
          - `source`: input dati. Accetta:
              * `pyarrow.Table` (schema derivato)
              * `pyarrow.RecordBatch`
              * lista di `pyarrow.RecordBatch` (tutti con stesso schema)
              * `list[dict]` (v0.3.0+, convertito via `pa.Table.from_pylist`)
              * `pandas.DataFrame` (v0.3.0+, convertito via
                `pa.Table.from_pandas`, richiede pandas installato)
              * `bytes` — buffer Arrow IPC stream self-contained (schema
                + N batches + EOS). Utile per zero-copy da altri produttori.
          - `mode`: "append" | "create" | "replace" | "truncate_insert"
            | "update" | "upsert" | "delete_by_keys" (default "append")
          - `transaction_profile`: "single_transaction" | "chunk_committed"
            | "staged_swap" | "best_effort_ddl" (default "single_transaction")
          - `mapping_policy`: "strict" | "compatible" | "lossy" | "native"
            (default "compatible"). "strict" boccia ogni loss anche minore
            (e.g. Arrow nullable verso PG NOT NULL); "compatible" tollera
            loss non-DataLoss; scelta consigliata per input pyarrow tipici.
          - `keys`: lista di colonne key (v0.3.0+). Obbligatorio per
            mode "upsert" / "update" / "delete_by_keys". Rifiutato con
            errore per gli altri mode.
          - `update_columns`: lista di colonne da aggiornare per mode
            "update" (v0.3.0+). Vuoto = tutte le non-key.

        Ritorna un dict con la struttura `WriteOutcome` del contratto v2,
        serializzata dalla stessa fonte del JSON:

            {
              "schema_version": 2,
              "status": "committed",
              "execution_id": "...",
              "provider": "postgres",
              "rows": {"received": N, "confirmed": N, "inserted": N, ...},
            }

        `recovery` compare **solo** negli esiti che lo prevedono —
        `partially_committed` e `outcome_unknown` — perche le altre varianti
        dello schema non lo dichiarano. Gli stati usano la forma del contratto
        (`partially_committed`, non `partiallycommitted`).

        Richiede pyarrow installato (a meno che `source` sia già bytes).
        """
        ipc_bytes = _to_ipc_bytes(source)
        return self._native.copy_from(
            schema, table, ipc_bytes, mode, transaction_profile,
            mapping_policy, keys, update_columns,
        )

    # ------------------------ transactions -----------------------------

    def begin(
        self,
        isolation: str | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
        statement_timeout_ms: int | None = None,
        context: "SessionContext | None" = None,  # noqa: F821 (forward-ref)
        native_query_policy: str | None = None,
    ) -> Transaction:
        """Apre una transazione user-managed.

        Usa come context manager per commit/rollback automatico:

            with s.begin() as tx:
                tx.execute("INSERT ...")

        Options:
        - `isolation`: "read_uncommitted" / "read_committed" /
          "repeatable_read" / "serializable"
        - `read_only`: True/False
        - `deferrable`: True/False (solo con Serializable + ReadOnly)
        - `statement_timeout_ms`: int
        - `context` (PFM CHG-002): SessionContext applicato via
          `SET LOCAL` (transaction-local, no leak fra riusi pool).
        - `native_query_policy` (PFM CHG-003): "allow" (default) o
          "deny" — restringe agli statement CRUD OLTP.
        """
        native_tx = self._native.begin(
            isolation,
            read_only,
            deferrable,
            statement_timeout_ms,
            context,
            native_query_policy,
        )
        return Transaction(native_tx)

    # --------------------- observability + inspect ----------------------

    def metrics(self) -> dict:
        """Snapshot dei contatori interni del provider (pool_checkouts,
        schema_cache_hits/misses, catalog_introspections, read_rows,
        writes_committed, ecc.).

        Ritorna un dict con ~25 chiavi u64. Uso tipico: espone al sistema
        di monitoring / oncall per diagnosticare cold cache, pool
        exhaustion, write ambiguità.
        """
        return self._native.metrics()

    @property
    def inspect(self) -> "_Inspector":
        """Namespace per catalog introspection:

            s.inspect.catalogs()             -> list[str]
            s.inspect.schemas()              -> list[str]
            s.inspect.tables(schema)         -> list[dict]
            s.inspect.describe(schema, name) -> dict
        """
        return _Inspector(self._native)

    # -------------------- portable AST builders -------------------------

    def select(self, table: str, schema: str | None = None) -> Select:
        return Select(self, table, schema)

    def insert(self, table: str, schema: str | None = None) -> Insert:
        return Insert(self, table, schema)

    def update(self, table: str, schema: str | None = None) -> Update:
        return Update(self, table, schema)

    def delete(self, table: str, schema: str | None = None) -> Delete:
        return Delete(self, table, schema)

    def upsert(self, table: str, schema: str | None = None) -> Upsert:
        return Upsert(self, table, schema)

    # ------- API interne consumate dai builder (via json AST) -----------

    def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return self._native.execute_portable_rows(ast_json)

    def _execute_portable_count(self, ast_json: str) -> int:
        return self._native.execute_portable_count(ast_json)


class _Inspector:
    """Namespace per operazioni di catalog introspection.

    Ottenuto via `session.inspect`. Non istanziato direttamente.
    """

    __slots__ = ("_native",)

    def __init__(self, native: _NativeSession) -> None:
        self._native = native

    def catalogs(self) -> list[str]:
        """Ritorna la lista dei catalog (database) accessibili all'utente."""
        return self._native.inspect_catalogs()

    def schemas(self) -> list[str]:
        """Ritorna la lista degli schemas utente (system schemas esclusi
        di default: pg_catalog, information_schema, pg_toast, ...)."""
        return self._native.inspect_schemas()

    def tables(self, schema: str) -> list[dict]:
        """Ritorna la lista degli oggetti (tabelle, viste, materialized,
        foreign tables, partition parents) nello schema indicato.
        Ogni entry è dict con `{name, kind, is_partition}`."""
        return self._native.inspect_tables(schema)

    def describe(self, schema: str, table: str) -> dict:
        """Descrive un oggetto: ritorna dict con `schema`, `columns`,
        `schema_token` (fingerprint strutturale per invalidazione cache)."""
        return self._native.inspect_describe(schema, table)
