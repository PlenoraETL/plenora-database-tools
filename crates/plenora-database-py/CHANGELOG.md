# Changelog — plenora-database Python SDK

Tutte le release seguono [semver](https://semver.org). Fino alla v1.0.0
l'API è considerata "prevista stabile ma non frozen" — breaking change
sono possibili in una minor (`0.x → 0.x+1`) e sempre documentati qui.

I wheel di ogni release sono allegati come asset alla release GitHub
corrispondente. Il tag ha il prefisso `py-` (es. `py-v0.1.3`) per non
confondersi con il ciclo di release del Rust workspace (che usa tag
`vX.Y.Z` senza prefisso).

---

## [0.6.0] — 2026-08-14

Completa il pattern bulk-write MySQL: `MysqlSession.copy_from` con tutti
7 WriteMode + accettazione flessibile dell'input (pyarrow / pandas /
list-of-dict / bytes IPC).

### Added

- **`MysqlSession.copy_from(schema, table, source, mode='append',
  transaction_profile='single_transaction', mapping_policy='compatible',
  keys=None, update_columns=None) → dict`** — bulk write MySQL:

  ```python
  import pyarrow as pa
  with p.connect_mysql("localhost", "db", "u", "p") as s:
      # Append (default)
      tbl = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
      outcome = s.copy_from("mydb", "events", tbl)

      # Create (CREATE TABLE dallo schema Arrow + INSERT bulk)
      outcome = s.copy_from("mydb", "events_new", tbl, mode="create")

      # Upsert (INSERT ... ON DUPLICATE KEY UPDATE)
      outcome = s.copy_from("mydb", "events", tbl, mode="upsert", keys=["id"])

      # Update (staging TEMPORARY table + UPDATE JOIN)
      outcome = s.copy_from("mydb", "events", tbl, mode="update", keys=["id"])

      # Replace (staging persistent + RENAME atomic swap)
      outcome = s.copy_from("mydb", "events", tbl, mode="replace")

      # DeleteByKeys (DELETE ... WHERE (keys) IN (...))
      outcome = s.copy_from("mydb", "events", keys_tbl, mode="delete_by_keys", keys=["id"])
  ```

- **`source` accetta**:
  - `pyarrow.Table` / `pyarrow.RecordBatch` / `list[pyarrow.RecordBatch]`
  - `list[dict]` (convertito via `pa.Table.from_pylist`)
  - `pandas.DataFrame` (convertito via `pa.Table.from_pandas`)
  - `bytes` (Arrow IPC stream self-contained per zero-copy)

- **Mode / profile / policy**: stessi valori di `Session.copy_from`
  Postgres. Il provider MySQL supporta tutti 7 WriteMode (v1.2 core
  Rust); ora esposti dal SDK Python.

- **Wrapper Python `_MysqlSessionWrapper`** in `__init__.py`: aggiunge
  ergonomia `copy_from` con auto-conversion `source → ipc_bytes` via
  helper `_to_ipc_bytes` (riusato dal path Postgres). L'API sottostante
  `MysqlSession._native.copy_from(schema, table, ipc_bytes, ...)`
  rimane accessibile per il consumer che preferisce bytes precompilati.

### Design

Zero duplication su Postgres:
- Helper generici in `write.rs` (parse_mode/profile/mapping_policy,
  decode_ipc_stream, make_operation, default_budget, VecBatchStream,
  outcome_into_py, wrap_outcome) sono ora `pub(crate)` e usati sia
  dal path Postgres sia MySQL.
- Nuovo modulo `mysql_write.rs` (~100 righe) contiene solo la
  differenza: `Arc<MysqlProvider>` invece di `Arc<PostgresProvider>`
  nella chiamata `prepare_write` + `write`.

### Compatibilità

- 100% backward-compat con v0.5.0 (nessun cambio API esistente).
- `MysqlSession` API additiva: `copy_from` nuovo, tutto il resto
  invariato.

### Wheel

- `plenora_database-0.6.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.6.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.6.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/generic-database-tools/releases/tag/py-v0.6.0>

---

## [0.5.0] — 2026-08-14

Completa il pattern OLTP MySQL: `MysqlSession.begin()` + savepoints
via il `Transaction` provider-agnostic esistente.

### Added

- **`MysqlSession.begin(isolation=None, read_only=None, statement_timeout_ms=None)`**
  → ritorna la classe `Transaction` (provider-agnostic, ereditata dal
  path Postgres). Sblocca tutti i pattern OLTP dal Python MySQL:

  ```python
  with s.begin(isolation="serializable") as tx:
      tx.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
      tx.savepoint("sp1")
      tx.execute("...")
      tx.rollback_to_savepoint("sp1")
      tx.release_savepoint("sp1")
      # commit auto su __exit__; rollback su eccezione
  ```

- Metodi ereditati da `Transaction` disponibili anche per MySQL:
  - `execute` / `execute_scalar` / `execute_returning_rows`
  - `savepoint` / `rollback_to_savepoint` / `release_savepoint`
  - `commit` / `rollback` / `conditional_update`
  - `is_active`, `__enter__/__exit__`, `__repr__`

- MySQL non ha `deferrable` (parametro Postgres-only) — non esposto.

### Design

Zero duplication: `Transaction` è già un wrapper sopra
`Box<dyn TransactionScope>` — non provider-specific. La modifica è
piccola (~40 righe) nel `mysql_session.rs`: parsing opzioni + call a
`provider.begin_transaction` + `Transaction::new(scope)`.

### Compatibilità

- 100% backward-compat con v0.4.0: nessun cambio all'API esistente.
- Aggiunta additiva su `MysqlSession`.

### Wheel

- `plenora_database-0.5.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.5.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.5.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.5.0>

---

## [0.4.0] — 2026-08-14

Prima esposizione MySQL nel SDK Python (scaffold, non feature parity
con Postgres).

### Added

- **`connect_mysql(host, database, user, password, port=None, tls_ca_pem=None)`** —
  factory per aprire una sessione MySQL. Non usa DSN libpq (che è
  Postgres-specifico); accetta componenti separati.
- **`MysqlSession`** (nuova classe Python + nativa Rust) con subset:
  - `execute(sql, params) → int` (affected_rows, DML in tx dedicata)
  - `execute_scalar(sql, params) → Any` (SELECT 1 riga × 1 colonna)
  - `execute_returning_rows(sql, params) → list[dict]` (SELECT con rows)
  - `execute_ddl(sql) → None` (DDL raw, autocommit MySQL)
  - `close()`, `__enter__/__exit__`, `is_closed`, `server_version`, `__repr__`
- Placeholder syntax: `?` (convenzione MySQL, non `$1` come Postgres).
- Type stubs `.pyi` (`_native.pyi` + `MysqlSession` export in `__init__.pyi`
  se presente).
- Test live `test_mysql_session.py` (6 test): connect + server_version,
  execute/scalar/rows roundtrip, NULL handling, context manager, DDL
  autocommit visibility.

### Not Included (roadmap SDK MySQL post-0.4)

- `begin()` + `Transaction` context manager (savepoints, conditional_update)
- `copy_from` bulk write (7 WriteMode via Arrow IPC)
- `read()` streaming Arrow
- Portable AST builders (`select/insert/update/delete/upsert`)
- Spatial predicates + `SpatialReference`
- Typed params (uuid/decimal/date/etc. — MySQL binding usa string
  passthrough per ora)
- `AsyncMysqlSession` async variant
- Metrics + inspect namespace (analogo a Session Postgres)

### Motivo scaffold

Il gap Consumer Surface era enorme: prima MySQL era raggiungibile solo
via API Rust diretta o CLI generic `database-probe`. Questa release
sblocca il pattern OLTP base (probe + execute + query + DDL) dal Python
consumer, che è sufficiente per validare il driver end-to-end e per
casi d'uso semplici (script batch, migrazione dati, integration tests).
Il resto delle capability arriverà quando serve un consumer PFM
concreto — evita over-engineering di feature non richieste.

### Compatibilità

100% backward-compatible con v0.3.0: nessun cambio all'API Postgres
(`Session`, `AsyncSession`, `Transaction`, `copy_from`, `read`, portable
builders, ecc.). L'aggiunta è additiva (`connect_mysql` + `MysqlSession`
sono nuove).

### Wheel

- `plenora_database-0.4.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.4.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.4.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.4.0>

---

## [0.3.0] — 2026-08-14

Completa la superficie Postgres del SDK. Tre gap P1 chiusi:
1. bulk write UPSERT/UPDATE/DELETE
2. read() con projection/order_by/limit
3. `copy_from` accetta pandas.DataFrame + list[dict]

### Added

- **`copy_from(mode="upsert", keys=[...])`** — INSERT ... ON CONFLICT
  DO UPDATE dallo schema Arrow, con conflict target dato dai `keys`.
  Sblocca ETL idempotenti (import periodici con chiave primaria).
- **`copy_from(mode="update", keys=[...], update_columns=[...])`** —
  UPDATE ... FROM dallo staging (implementato dal provider Rust).
- **`copy_from(mode="delete_by_keys", keys=[...])`** — DELETE ... USING
  dallo staging (basta la colonna key nel dataset).
- **`read(projection=[...], order_by=[...], limit=N)`** — SELECT
  projection + ORDER BY + LIMIT sul cursore server-side. Prima
  scaricava tutta la tabella. `order_by` è list di
  `("column", "asc"|"desc")`.
- **`copy_from(source=list[dict])`** — convertito via
  `pyarrow.Table.from_pylist`. Ergonomia per script Python senza
  pyarrow object model esposto.
- **`copy_from(source=pandas.DataFrame)`** — convertito via
  `pyarrow.Table.from_pandas`. Zero-boilerplate per data scientist
  che partono da pandas.
- Validation early: `keys` è obbligatorio per upsert/update/delete_by_keys;
  errore con messaggio chiaro se assente. Rifiutato per gli altri mode
  (Append/Create/etc.) per prevenire mismatch.
- 12 nuovi test in `test_v030_p1.py` (upsert happy path + error paths,
  read projection/limit/order_by, list[dict] + pandas + edge cases).

### Compatibilità

- **Backward-compat** con v0.2.0 per il pattern `copy_from(mode="append")`
  senza `keys` (funziona identico).
- Chi passava `keys=[...]` per mode diverso da upsert/update/delete_by_keys
  in v0.2.0 (impossibile — parametro non esisteva) ora otterrebbe errore
  early: nessun consumer impattato.
- API sync + async stubs `.pyi` aggiornati.

### Stato Postgres SDK

Dopo v0.3.0 la copertura Postgres è **complete** rispetto ai gap
P1 identificati per il primo consumer target (PFM). Restano gap P2
(async cancellation graceful, pool config esposto, altre 67 funzioni
PostGIS come builder) rimandati a rispettivi minor bump quando
prioritari.

Da v0.3.0 il SDK è pronto come **base pattern** per esporre MySQL e
SQL Server (che il core Rust supporta già). Il pattern binding
(session/tx/copy_from/read) è stabilizzato — le duplicazioni cross-provider
si applicano con la stessa struttura.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.3.0>

---

## [0.2.0] — 2026-08-14

Nuove capability bulk-write. Prima minor bump del SDK.

### Added

- **`copy_from(mode="create")`** ora funziona end-to-end: crea la
  tabella target dallo schema Arrow prima del COPY. Sblocca il pattern
  ETL scratch (`load parquet/csv into new_table`) senza DDL preventivo
  dal consumer.
- **`copy_from(mode="replace")`** e **`mode="truncate_insert"`** sono
  parimenti operativi (già supportati dal provider Postgres, ora
  raggiungibili dal SDK).
- Test `test_copy_from_mode_create_builds_table_from_arrow_schema`:
  verifica DDL applicato + righe landed dopo `copy_from(mode="create")`.
- Test `test_copy_from_mode_create_conflicts_if_target_exists`:
  verifica che `mode="create"` su target esistente restituisca
  `PlenoraConflictError` (Conflict del preflight).

### Fixed (core Rust)

- `plenora_db_postgres::write::execute` chiamava `row_diagnostics::validate_input`
  per **tutti** i mode, ma la funzione rifiutava esplicitamente ogni mode
  diverso da `Append + SingleTransaction` con messaggio
  `"la diagnostica PostgreSQL supporta solo Append con SingleTransaction"`.
  Risultato: `Create`, `Replace`, `TruncateInsert` (e altri) erano
  raggiungibili solo escludendo row diagnostics via
  `declared_input_rows() = None` sullo stream, cosa che il SDK non fa.
- Fix: `validate_input` viene ora invocato solo per `Append + SingleTransaction`
  (che è l'unico scenario dove ha senso — quarantina di righe individuali
  su target esistente). Gli altri mode saltano il gate diagnostico e
  vanno al path normale.

### Compatibilità

- **Backward-compat** con v0.1.3 al 100% per il pattern `mode="append"`
  (default).
- Nessun cambio API o firma. È un'aggiunta di capability precedentemente
  bloccata da un check troppo restrittivo.
- Il workspace Rust (`plenora-db-postgres`) tocca la funzione
  `execute`; unit test invariati (103/103 pass).

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.2.0>

---

## [0.1.3] — 2026-08-14

Bugfix di `copy_from` introdotto in v0.1.2.

### Fixed

- `copy_from` / `acopy_from` hardcodavano `MappingPolicy::Strict`, che
  boccia il pattern comune "Arrow nullable → PG NOT NULL" (severity
  `DataLoss`). Riprodotto live: 4/8 test bulk write fallivano su una
  tabella con vincoli `NOT NULL`.

### Changed

- **Default cambiato** (silent): `copy_from(mapping_policy=)` ora è
  `"compatible"` invece di `"strict"` hardcoded. È la scelta ragionevole
  per input pyarrow tipici (tutti i campi nullable per default). Chi
  vuole il vecchio comportamento passa esplicitamente `mapping_policy="strict"`.

### Added

- Parametro `mapping_policy: str` esposto a `Session.copy_from` /
  `AsyncSession.acopy_from` (default `"compatible"`).
- 2 nuovi test: `test_copy_from_strict_policy_rejects_nullable_to_not_null`
  e `test_copy_from_invalid_mapping_policy_raises_invalid_plan`.
- Suite live totale: **156 test** (154 in v0.1.2 → 156 in v0.1.3).

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.3>

---

## [0.1.2] — 2026-08-14

Nuova feature (`copy_from`) + fix API contract + documentazione observability.

### Added

- **`Session.copy_from(schema, table, source, ...)`** e
  **`AsyncSession.acopy_from(...)`**: bulk write via `prepare_write` +
  `write` del provider Postgres (COPY internamente per mode `append`).
  Accetta `pyarrow.Table` / `RecordBatch` / iterable / bytes IPC.
  Ritorna dict con struttura `WriteOutcome` del core Rust.
- 8 nuovi test in `test_copy_from.py`: happy path (sync/async) + error
  paths (mode/profile invalidi, tipo source non supportato, iterable
  vuoto).
- README: sezione "Bulk write (COPY)" con esempi sync + async.
- README: sezione "Observability" con esempio structured logging
  OpenTelemetry-compatibile a partire da `Session.metrics()` e dai
  campi diagnostici delle `PlenoraError`.

### Removed (BREAKING)

- Parametro `batch_rows: int | None = None` rimosso da `Session.read()`
  e `AsyncSession.aread()`. In v0.1.0 e v0.1.1 era **accettato ma
  silenziosamente ignorato** (il core `Provider::read()` non espone una
  batch size esplicita). Rimuoverlo è più onesto che mantenere un
  contract violato.
- **Migration**: chi passava `batch_rows=N` esplicitamente riceverà
  `TypeError`; rimuovere il kwarg dalla chiamata. La size dei batch
  emessi dallo stream è decisa dal provider (Postgres: bounded dal
  buffer del cursore server-side).

**Note**: v0.1.2 aveva un bug in `copy_from` (default `mapping_policy`
troppo rigido). Fixato in v0.1.3.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.2>

---

## [0.1.1] — 2026-08-14

Bugfix del version mismatch scoperto in v0.1.0.

### Fixed

- `p.version()` restituiva `"1.1.0"` (versione del Rust workspace,
  ereditata via `version.workspace = true`) invece di `"0.1.0"`
  (versione dichiarata in `pyproject.toml` e usata nel filename del
  wheel). Confondeva `pip check` e semver gate consumer-side.

### Changed

- `Cargo.toml` del crate `plenora-database-py` ora dichiara la sua
  version esplicitamente (`version = "0.1.1"`), separata dal Rust
  workspace. Coincide con `pyproject.toml`.

**Compatibilità**: 100% backward-compatible con v0.1.0 a livello API.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.1>

---

## [0.1.0] — 2026-08-14

Prima release stabile del binding Python. PyO3 abi3-py310 sopra al core
Rust di `plenora-database-tools`.

### Added

- **Sync + Async parity**: `Session` + `AsyncSession` con context
  manager, `Transaction` + `AsyncTransaction` con savepoint e
  `conditional_update` (optimistic-lock).
- **Portable AST builder Pythonic**: `Select` / `Insert` / `Update` /
  `Delete` / `Upsert` provider-agnostic.
- **PostGIS end-to-end**: `where_spatial` + `SpatialReference` per
  predicati geometrici cross-SRID (5 predicati: intersects, contains,
  within, bounding_box, d_within).
- **Streaming Arrow**: `read()` / `aread()` restituiscono
  `BatchReader` che emette Arrow IPC stream chunk-per-chunk.
- **Error hierarchy**: 19 classi tipizzate sotto `PlenoraError` mappate
  su `ErrorCategory` del core Rust.
- **Type stubs** (`*.pyi` + `py.typed`): PEP 561 completo.
- **Wheel multi-platform**: Linux (manylinux_2_34), macOS aarch64,
  Windows x86_64. Un unico wheel `abi3-py310` per piattaforma
  (compatibile Python 3.10 / 3.11 / 3.12 / 3.13).
- **Performance**: ~13× più veloce del subprocess CLI su happy path
  scalar (0.62 ms/call vs 8.40 ms/call, misurato in
  `test_benchmark_parity.py`).

### Scope

- **Coperto**: Postgres 15+ / PostGIS 3.x — OLTP, streaming read, spatial, tx.
- **Non coperto** (roadmap successiva): MySQL, SQL Server (driver Rust
  presenti nel workspace ma non ancora esposti al SDK).
- **DDL plane**: fuori scope v0.1.

### Driver Rust — fix inclusi (pre-Fase-3)

- **P0.7**: Decimal + UUID binding nel path OLTP (dual-representation
  text/binary).
- **P0.8**: NUMERIC decoder wire format nel path OLTP.
- **H7.1**: filtri catalog contro system schemas.
- **H7.2**: cancel-aware `BatchStream::next_batch(&CancellationToken)`.
- Portable spatial: `::geography` cast semanticamente corretto per
  `PortableStatement`.

**Note**: v0.1.0 aveva un bug in `p.version()` (mismatch con
`pyproject.toml`). Fixato in v0.1.1.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.0>
