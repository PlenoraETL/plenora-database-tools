# Changelog — plenora-database Python SDK

Tutte le release seguono [semver](https://semver.org). Fino alla v1.0.0
l'API è considerata "prevista stabile ma non frozen" — breaking change
sono possibili in una minor (`0.x → 0.x+1`) e sempre documentati qui.

I wheel di ogni release sono allegati come asset alla release GitHub
corrispondente. Il tag ha il prefisso `py-` (es. `py-v0.1.3`) per non
confondersi con il ciclo di release del Rust workspace (che usa tag
`vX.Y.Z` senza prefisso).

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
