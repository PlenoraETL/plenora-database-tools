# Plenora Database Tools — Riferimento completo delle interfacce

**Ambito**: tutte le API pubbliche esposte dai crate del workspace `plenora-database-tools`, aggiornate al 2026-08-11 (dopo Fase A + Fase B parziale, con B2 esplicitamente escluso).
**Principio guida**: black-box che uniforma read/write, robusta, veloce, contratti chiari.

---

## 1. Cosa fa la libreria

Un runtime foundation per accedere a database relazionali in modo dichiarativo, con **due piani**:

- **Data plane** — bulk streaming Arrow/GeoArrow: read/write di grandi dataset tabellari e geospaziali. Preesistente al lavoro di Fase A.
- **Application plane (OLTP)** — transazioni multi-statement con savepoint, optimistic concurrency, session context, facade ergonomica per query singole. Introdotto in Fase A.

Entrambi i piani condividono la stessa infrastruttura: pool, TLS, timeout, cancellation, error taxonomy, capabilities.

**Non fa**: ORM, migration engine, authz, workflow, message broker, audit ledger. La libreria fornisce le primitive; la logica di dominio vive altrove.

---

## 2. Struttura dei crate

| Crate | Ruolo |
|---|---|
| `plenora-database-core` | Contratti puri: tipi, trait, error taxonomy, protocolli. Nessun runtime, nessun driver. |
| `plenora-database-sql` | Costruzione AST SQL, quoting, prepared statement builder. |
| `plenora-database-engine` | Orchestrazione validate/prepare/execute. |
| `plenora-database-testkit` | Fixture condivise, oracle differenziale per test cross-provider. |
| `plenora-db-postgres` | Driver PostgreSQL 16 + PostGIS 3.4 (**provider di riferimento**). |
| `plenora-db-mysql` | Driver MySQL 8.0/8.4 (read + append only). |
| `plenora-db-sqlserver` | Driver SQL Server 2022 + Azure SQL (opt-in). |
| `plenora-database-cli` | Entrypoint CLI (`probe`, `inspect-dataset`, gestione secret DSN). |
| `plenora-database-py` | Bindings Python PyO3 (`abi3-py310`) — SDK sync + async per Postgres/PostGIS. Vedi [README dedicato](../crates/plenora-database-py/README.md). |

Confine dati pubblico: **Apache Arrow 59.1** con geometrie in **GeoArrow-WKB**.
Toolchain: **Rust 1.92**, `#![forbid(unsafe_code)]` ovunque, `overflow-checks = true` anche in release.

---

## 3. Data plane — API bulk Arrow

### 3.1 `Provider` trait — l'API principale

Ogni driver implementa questo trait. Definito in `plenora_database_core::provider`.

```rust
pub trait Provider: Send + Sync {
    fn kind(&self) -> ProviderKind;

    fn test_connection<'a>(&'a self, ..) -> ProviderFuture<'a, ConnectionInfo>;
    fn probe_capabilities<'a>(&'a self, ..) -> ProviderFuture<'a, ProviderCapabilities>;
    fn inspect<'a>(&'a self, .., op: &'a Operation, ..) -> ProviderFuture<'a, Inspection>;

    fn read<'a>(&'a self, .., op: &'a ReadOperation, ..) -> ProviderFuture<'a, Box<dyn BatchStream>>;
    fn query<'a>(&'a self, .., op: &'a QueryOperation, ..) -> ProviderFuture<'a, Box<dyn BatchStream>>;

    fn prepare_write<'a>(&'a self, .., op: &'a WriteOperation, schema: SchemaRef, ..)
        -> ProviderFuture<'a, PreparedWrite>;
    fn write<'a>(&'a self, .., prepared: PreparedWrite, input: Box<dyn BatchStream>, ..)
        -> ProviderFuture<'a, WriteOutcome>;

    // Aggiunto in A1:
    fn begin_transaction<'a>(&'a self, .., options: &'a TransactionOptions, ..)
        -> ProviderFuture<'a, Box<dyn TransactionScope>>;
}
```

Tutti i metodi accettano `secret: &SecretString` (DSN redatto in Debug), `budget: &ResourceBudget` (limiti risorse), `cancellation: &CancellationToken`.

### 3.2 Operazioni di lettura

- **`ReadOperation`** — SELECT strutturato: target `ObjectRef` (schema.table), projection (colonne), `FilterExpression` (WHERE bind-safe), `OrderBy`, `row_limit`. Ritorna `BatchStream<RecordBatch>`.
- **`QueryOperation`** — query più ricche: JOIN, GROUP BY, HAVING, CTE (ricorsive), window/frame, set operations (UNION/INTERSECT/EXCEPT), DISTINCT, derived tables, `QueryLock` (FOR UPDATE/SHARE con NOWAIT/SKIP LOCKED).
- **`FilterExpression`** — And/Or/Eq/Ne/Lt/Lte/Gt/Gte/Like/In/Between/Spatial. I predicati spatial hanno un catalogo dedicato (`SpatialFunction`).

### 3.3 Operazioni di scrittura

**`WriteOperation`** dichiara: target + mode + input_schema.

**7 write modes** (support dipende dal driver):

| Mode | Semantica | Postgres | MySQL | SQL Server |
|---|---|---|---|---|
| `Create` | CREATE TABLE se non esiste | ✅ | ✅ | ✅ |
| `Append` | INSERT o COPY bulk | ✅ COPY binario | ✅ Prepared | ✅ Prepared/TdsBulk |
| `Replace` | DELETE + INSERT atomico | ✅ | ❌ | ✅ |
| `TruncateInsert` | TRUNCATE + INSERT (non-atomico) | ✅ | ❌ | ✅ |
| `Update` | UPDATE con WHERE bind-safe | ✅ | ❌ | ✅ |
| `Upsert` | INSERT ... ON CONFLICT | ✅ | ❌ | ❌ (design) |
| `DeleteByKeys` | DELETE per lista chiavi | ✅ | ❌ | ⚠️ parziale |

**`PreparedWrite`** — output di `prepare_write`: contratto Arrow verificato, budget lease, loss report della normalizzazione tipi.

**`WriteOutcome`** — output di `write`: `WriteStatus` (Committed | RolledBack | PartiallyCommitted | **OutcomeUnknown**), `RowCounts` (received/confirmed/inserted/updated/deleted/failed/skipped), `LayerOutcome[]`, `Recovery` opzionale (per `OutcomeUnknown` con precondizioni di retry sicuro).

### 3.4 Streaming dati — `BatchStream`

```rust
pub trait BatchStream: Send {
    fn schema(&self) -> SchemaRef;
    fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>>;
    fn declared_input_rows(&self) -> Option<u64>;  // per write con diagnostica row-scoped
    fn row_diagnostics_policy(&self) -> RowDiagnosticsPolicy;
}
```

Batching bounded (`ResourceBudget`), cancellation-aware.

### 3.5 Parametri — `ParameterValue`

14 varianti tipizzate:

```rust
pub enum ParameterValue {
    Bool(bool), I32(i32), I64(i64), F64(f64),
    String(String), Bytes(Vec<u8>),
    Date(String), Timestamp(String), TimestampTz(String),
    Decimal(String), Uuid(String), Json(Value),
    Wkb { bytes: Vec<u8>, srid: Option<u32>, dimensions: Dimensions, semantics: SpatialSemantics },
    Null { type_name: String },
}
```

Bind sempre positional (`$1..$n`), mai stringifica interpolata. `ParameterBag` è la mappa nome→valore per i piani dichiarativi.

### 3.6 Capabilities

**`ProviderCapabilities`** (probe live): elenco strutturato di ciò che il provider supporta (write modes, isolation levels, spatial dimensions, DDL, ecc.). Consumer verifica le capability invece di interrogare il nome del provider.

---

## 4. Application plane — API OLTP (Fase A)

### 4.1 `TransactionScope` trait

Definito in `plenora_database_core::transaction`. Object-safe: viene consegnato come `Box<dyn TransactionScope>`.

```rust
pub trait TransactionScope: Send {
    fn execute<'a>(&'a mut self, stmt: &'a Statement, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, u64>;  // ritorna righe modificate

    fn query<'a>(&'a mut self, stmt: &'a Statement, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, Vec<Vec<ParameterValue>>>;

    fn query_stream<'a>(&'a mut self, stmt: &'a Statement, batch_size: u32,
                        cancel: &'a CancellationToken)
        -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>>;  // cursor server-side

    fn execute_conditional_update<'a>(&'a mut self, request: ConditionalUpdate<'a>,
                                       cancel: &'a CancellationToken)
        -> ProviderFuture<'a, ()>;

    fn savepoint<'a>(&'a mut self, name: &'a str, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, ()>;
    fn rollback_to_savepoint<'a>(&'a mut self, name: &'a str, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, ()>;
    fn release_savepoint<'a>(&'a mut self, name: &'a str, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, ()>;

    fn commit<'a>(self: Box<Self>, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, CommitOutcome>;
    fn rollback<'a>(self: Box<Self>, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, ()>;
}
```

`commit`/`rollback` consumano il box (transazione conclusa). Drop senza chiusura esplicita mette la connessione in quarantena.

### 4.2 `TransactionOptions`

Opzioni al `Provider::begin_transaction`.

```rust
pub struct TransactionOptions {
    pub isolation: Option<IsolationLevel>,   // ReadUncommitted | ReadCommitted | RepeatableRead | Serializable
    pub access_mode: Option<AccessMode>,     // ReadWrite | ReadOnly
    pub deferrable: Option<bool>,            // solo per Serializable READ ONLY
    pub statement_timeout_ms: Option<u64>,   // applicato via SET LOCAL
    pub context: SessionContext,             // key/value tipizzati transaction-local
    pub native_query_policy: NativeQueryPolicy,  // Allow | Deny
}
```

`Default::default()` = tutti i default del provider + policy `Allow`.

### 4.3 `Statement`

L'unità atomica di esecuzione dentro una tx.

```rust
pub struct Statement {
    pub sql: String,
    pub params: Vec<ParameterValue>,
}

impl Statement {
    pub fn new(sql: impl Into<String>) -> Self;
    pub fn with_params(self, params: Vec<ParameterValue>) -> Self;
}
```

Placeholder positional `$1..$n`. Il SQL è opaco al core (la validazione governance è di `NativeQueryPolicy`).

### 4.4 `CommitOutcome`

```rust
pub enum CommitOutcome {
    Committed,
    OutcomeUnknown { recovery: Recovery },
}
```

`OutcomeUnknown` è **la novità critica**: quando il canale si compromette in fase Commit, la libreria dichiara esplicitamente che non sa se il server ha applicato o meno. La `Recovery` include `automatic_retry_allowed = false` — mai auto-retry cieco.

### 4.5 `ConditionalUpdate` — optimistic concurrency

```rust
pub struct ConditionalUpdate<'a> {
    pub update: &'a Statement,           // UPDATE ... WHERE key = $1 AND version = $2
    pub key_probe: Option<&'a Statement>, // SELECT 1 FROM ... WHERE key = $1 LIMIT 1
    pub expected_affected_rows: u64,      // tipicamente 1
}
```

Contratto di ritorno:
- `Ok(())` se affected == expected
- `Err(NotFound)` se probe conferma chiave assente
- `Err(ConcurrentModification)` in tutti gli altri casi di mismatch (default conservativo se probe è `None`)
- errori tecnici propagati

`ConcurrentModification` è una `ErrorCategory` di prima classe: il consumer distingue tra `NotFound` (aggregate cancellato), `Conflict` (unique/FK/CHECK), `ConcurrentModification` (versione avanzata) e errori tecnici retryable.

### 4.6 `RowStream` — cursor server-side (B4)

```rust
pub trait RowStream: Send {
    fn next_batch<'a>(&'a mut self, cancel: &'a CancellationToken)
        -> ProviderFuture<'a, Option<Vec<Vec<ParameterValue>>>>;
}
```

Aperto via `TransactionScope::query_stream(sql, batch_size)`. Il cursor è transaction-scoped (chiuso automaticamente da commit/rollback). Consumer chiama `next_batch()` finché non riceve `None`.

**Uso**: read grandi che non devono materializzare in memoria l'intero result set (mappe GIS, export, analytics). Su Postgres implementato con `DECLARE ... CURSOR FOR` + `FETCH FORWARD N`.

---

## 5. Facade OLTP — `plenora_database_core::facade` (A5)

Helper standalone sopra `TransactionScope`. Nessun tipo driver esposto.

```rust
pub async fn query_one(tx, statement, cancel) -> Result<Vec<ParameterValue>>;
    // NotFound se 0, Conflict se >1

pub async fn query_optional(tx, statement, cancel) -> Result<Option<Vec<ParameterValue>>>;

pub async fn execute_scalar_bool(tx, statement, cancel) -> Result<bool>;
pub async fn execute_scalar_i32(tx, statement, cancel) -> Result<i32>;
pub async fn execute_scalar_i64(tx, statement, cancel) -> Result<i64>;
pub async fn execute_scalar_f64(tx, statement, cancel) -> Result<f64>;
pub async fn execute_scalar_string(tx, statement, cancel) -> Result<String>;
```

Verificano 1 riga × 1 colonna + tipo atteso, altrimenti `DataMapping`.

---

## 6. Session context — `plenora_database_core::session_context` (A4)

Contesto tipizzato transaction-local con classificazione e redazione.

```rust
pub enum SessionValue { Text(String), Integer(i64), Boolean(bool) }

pub enum SessionClassification { Public, Internal, Sensitive }

pub struct SessionEntry {
    pub value: SessionValue,
    pub classification: SessionClassification,
}

impl SessionEntry {
    pub fn public(v: SessionValue) -> Self;
    pub fn internal(v: SessionValue) -> Self;
    pub fn sensitive(v: SessionValue) -> Self;
}

pub struct SessionContext { ... }

impl SessionContext {
    pub fn insert(&mut self, name: impl Into<String>, entry: SessionEntry) -> Result<()>;
    pub fn get(&self, name: &str) -> Option<&SessionEntry>;
    pub fn iter(&self) -> impl Iterator<...>;
}
```

**Regole**:
- Chiave nel formato `namespace.name` obbligatorio (es. `app.tenant_id`, `sec.actor_id`).
- Solo caratteri `[a-z0-9_]` nei segmenti, max 63 caratteri (limite Postgres).
- Valori Text senza NUL/CR/LF, max 8192 byte.
- `Debug` redige i valori `Sensitive` e `Internal`. Solo `Public` è stampato in chiaro.
- Applicato transaction-local via `set_config(name, value, true)` — resettato dal commit/rollback.

**Uso PFM tipico**: `app.tenant_id`, `app.actor_id`, `app.correlation_id`, `app.policy_version` — accessibili dal DB via `current_setting(name, true)` per RLS policy.

---

## 7. Errori e retry

### 7.1 Struttura `DatabaseError`

```rust
pub struct DatabaseError {
    pub category: ErrorCategory,
    pub phase: ErrorPhase,
    pub remote_effect: RemoteEffect,
    pub retry: RetryDisposition,
    pub provider: Option<ProviderKind>,
    pub execution_id: Option<String>,
    pub message: String,  // redatto — mai DSN, token, SQL bindato
    pub diagnostics: Option<Box<RowDiagnostics>>,
}
```

### 7.2 `ErrorCategory` (19 varianti)

`InvalidPlan | InvalidConfiguration | Schema | DataMapping | Crs | Unsupported | NotFound | Conflict | ConcurrentModification | Authentication | Authorization | Timeout | Cancelled | ResourceLimit | Io | Protocol | Transient | Execution | Internal`

`ConcurrentModification` è la variante aggiunta in A2, richiesta dal PFM per il pattern optimistic locking.

### 7.3 `ErrorPhase`

`Validate | Connect | Probe | Prepare | Read | Write | Finalize | Commit | Rollback | Cleanup`

### 7.4 `RemoteEffect`

`None | RolledBack | Partial | Committed | Unknown`

**Regola**: `Unknown` significa impossibilità di verificare l'effetto lato server. Il chiamante DEVE trattarlo come "possibilmente applicato" e verificare fuori banda prima di ritentare.

### 7.5 `RetryDisposition`

`Never | Quarantine | Safe | RequiresIdempotencyKey | RequiresRecovery | After(u64)`

- `Quarantine`: la connessione va isolata, non riusata finché lo stato non è verificato.
- `Safe`: retry immediato consentito.
- `After(ms)`: retry consentito dopo il delay.
- `RequiresRecovery`: retry solo dopo procedura di recovery esplicita.

### 7.6 Mapping SQLSTATE Postgres (A2)

~35 SQLSTATE mappati esplicitamente dalla classe 08/0A/22/23/25/28/3F/40/42/53/55/57/58/XX. Mapping **phase-aware**: lo stesso SQLSTATE produce `RemoteEffect` diverso a seconda della `ErrorPhase` (es. errore transport in `Commit` → `Unknown`; in `Connect` → `None`).

Il SQLSTATE originale **non è esposto** nel messaggio pubblico (provider neutrality).

---

## 8. Governance

### 8.1 `NativeQueryPolicy` (B3)

```rust
pub enum NativeQueryPolicy {
    Allow,   // default: qualsiasi SQL well-formed non-transazionale
    Deny,    // profilo applicativo PFM: solo CRUD parametrizzato
}
```

`Deny` permette SOLO: `SELECT | WITH | INSERT | UPDATE | DELETE | VALUES | TABLE | MERGE`. Nega DDL (`CREATE`, `DROP`, `ALTER`, `TRUNCATE`, `GRANT`, `REVOKE`), comandi di sessione (`SET`, `RESET`, `SHOW`, `LOCK`), utility (`VACUUM`, `ANALYZE`, `EXPLAIN`, `CALL`, `DO`), pub/sub (`LISTEN`, `NOTIFY`), IO (`COPY`), multi-statement.

Comandi transazionali (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `DECLARE`, `FETCH`, `CLOSE`) sono **sempre** rifiutati (anche in Allow) perché gestiti dal `TransactionScope`.

### 8.2 Conformance profile (A6)

```rust
pub enum Capability {
    Transactions, Savepoints, OptimisticConcurrency, SessionContext,
    OltpFacadeScalar, OltpFacadeQueryOne, BoundParameters,
    Cancellation, StatementTimeout,
    IsolationReadCommitted, IsolationRepeatableRead, IsolationSerializable,
}

pub struct ConformanceProfile {
    pub name: &'static str,
    pub required: &'static [Capability],
}

pub const APPLICATION_OLTP_V1: ConformanceProfile;  // 11 capability richieste

pub struct CapabilityEvidence {
    pub capability: Capability,
    pub kind: EvidenceKind,      // Verified | Failed | Unverified
    pub notes: Option<String>,
}

pub struct ProfileReport {
    pub profile: String,
    pub status: ProfileStatus,   // Pass | Fail
    pub missing: Vec<Capability>,
    pub failed: Vec<Capability>,
    pub evidence: Vec<CapabilityEvidence>,
}

pub async fn probe_application_oltp_v1(
    provider: &dyn Provider,
    secret: &SecretString,
    cancel: &CancellationToken,
) -> Vec<CapabilityEvidence>;

pub fn check_profile(profile: &ConformanceProfile,
                     evidence: &[CapabilityEvidence]) -> ProfileReport;
```

**Uso PFM**: al bootstrap il backend chiama `probe_application_oltp_v1` + `check_profile(&APPLICATION_OLTP_V1, ...)`. Se `Pass` la libreria è utilizzabile per il profilo applicativo, senza mai interrogare il nome del provider.

---

## 9. Spatial — `plenora_database_core::spatial_predicate` (B1)

```rust
pub enum SpatialPredicate {
    Intersects,
    Contains,
    Within,
    DWithin { distance_meters: f64 },
    BoundingBox,   // operatore index-friendly (Postgres &&)
}

pub struct SpatialReference {
    pub ewkb: Vec<u8>,       // WKB con SRID prefix
    pub srid: u32,
    pub dimensions: Dimensions,      // Xy | Xyz | Xym | Xyzm
    pub semantics: SpatialSemantics, // Geometry | Geography
}

pub struct SpatialFilter {
    pub geometry_column: String,
    pub predicate: SpatialPredicate,
    pub reference: SpatialReference,
}
```

**Traduttore Postgres**: `plenora_db_postgres::build_spatial_select(schema, table, projection, filter, limit) -> Result<Statement>`. Genera il `SELECT ... WHERE ST_Intersects(...)` (o predicato corrispondente) e ritorna uno `Statement` con la geometria di riferimento come `ParameterValue::Bytes(ewkb)` legato a `ST_GeomFromEWKB($n)::geometry`.

**Il consumer NON scrive mai `ST_*` a mano.**

---

## 10. Cancellation — `plenora_database_core::cancellation`

```rust
pub struct CancellationToken { ... }

impl CancellationToken {
    pub fn new() -> Self;
    pub fn with_deadline(deadline: Instant) -> Self;
    pub fn child_token(&self) -> Self;
    pub fn cancel(&self);
    pub fn cancel_due_to_deadline(&self);
    pub fn is_cancelled(&self) -> bool;
    pub fn reason(&self) -> Option<CancellationReason>;  // Requested | Deadline | Parent
    pub fn cancelled(&self) -> Cancelled<'_>;             // Future awaitable
}
```

Gerarchico (child_token cancellato dal parent), deadline-aware, deregistrazione dei waker su drop. Passato a ogni chiamata async di lettura/scrittura.

---

## 11. Cosa la libreria **NON** fa

**Confermato out-of-scope**:

- **Migration engine**. B2 esplicitamente cancellato. Il consumer usa un tool esterno (refinery, sqlx-migrate). Vedi `docs/history/phase-1/migration-plane-design.md`.
- **ORM / mapping dominio**. Nessuna nozione di `Asset`, `WorkOrder`, `Tenant`, ecc.
- **Authorization engine**. La libreria NON valuta policy: passa il `SessionContext` al DB, che le applica via RLS/altro.
- **Audit ledger**. L'observability sta in sink dedicati, non in tabelle owned dalla libreria.
- **Workflow engine / message broker**. Fuori scope.
- **Query builder vendor-specifico esposto pubblicamente**. Il consumer parla via `Statement` (SQL opaco) o `ReadOperation`/`QueryOperation` (dichiarativo portable), mai via un builder che espone `ST_*` di Postgres.

**Tecnicamente non fatto (Postgres) — gap noti**:

- `LISTEN`/`NOTIFY` (pub/sub)
- Advisory locks (`pg_advisory_lock`)
- Tipi Postgres esotici: `XML`, `money`, `tsvector`, `cidr`, `inet`, `macaddr`, `hstore`, PostGIS `raster`
- Enum/domain/composite mappati a `Utf8` opaco (round-trip funziona ma perde type safety a runtime)
- Generated columns STORED sono **rifiutate** in insert (schema restrittivo)
- Prepared statement cache esplicita (fallback prepare implicito)
- Retry policy centralizzata (le classificazioni sono `RetryDisposition::Safe/After/...` ma la retry effettiva la fa il chiamante)

---

## 12. Provider coverage matrix

| Capability | Postgres | MySQL 8.4 | SQL Server |
|---|---|---|---|
| `test_connection` / `inspect` / `read` streaming | ✅ | ✅ | ✅ |
| Bulk write Append | ✅ COPY BIN | ✅ Prepared | ✅ Prep/TdsBulk |
| Bulk write Replace/TruncateInsert/Update | ✅ | ❌ | ✅ |
| Bulk write Upsert | ✅ ON CONFLICT | ❌ | ❌ (design) |
| Bulk write DeleteByKeys | ✅ | ❌ | ⚠️ parziale |
| Geometrie 4D (XY/XYZ/XYM/XYZM) | ✅ PostGIS | XY only (Z/M rifiutati) | ✅ geog / ⚠️ geom |
| `begin_transaction` + savepoint | ✅ | ❌ (non implementato) | ❌ (non implementato) |
| `execute_conditional_update` | ✅ | ❌ | ❌ |
| `query_stream` (cursor) | ✅ | ❌ | ❌ |
| SessionContext | ✅ | ❌ | ❌ |
| Facade OLTP (query_one, scalar) | ✅ | ❌ | ❌ |
| NativeQueryPolicy | ✅ | ❌ | ❌ |
| Spatial predicate builder | ✅ (PostGIS) | ❌ | ❌ |
| APPLICATION_OLTP_V1 profile | ✅ Pass | ❌ | ❌ |

**Stato attuale**: solo PostgreSQL è provider di riferimento per **entrambi** i piani. MySQL/SqlServer supportano il solo data plane bulk (con restrizioni note).

---

## 13. Come iniziare — snippet PFM

### 13.1 Aprire una transazione + write versionato

```rust
use plenora_database_core::*;
use plenora_database_core::transaction::*;

let mut ctx = SessionContext::new();
ctx.insert("app.tenant_id", SessionEntry::public(SessionValue::Text("acme".into())))?;
ctx.insert("app.actor_id",  SessionEntry::sensitive(SessionValue::Text(user_id.into())))?;

let opts = TransactionOptions {
    isolation: Some(IsolationLevel::ReadCommitted),
    statement_timeout_ms: Some(5_000),
    context: ctx,
    native_query_policy: NativeQueryPolicy::Deny,
    ..Default::default()
};

let mut tx = provider.begin_transaction(&secret, &opts, &budget, &cancel).await?;

// Optimistic update con expected_version
let update = Statement::new(
    "UPDATE work_order SET status=$1, version=version+1 WHERE id=$2 AND version=$3"
).with_params(vec![
    ParameterValue::String("completed".into()),
    ParameterValue::I64(order_id),
    ParameterValue::I64(expected_version),
]);
let probe = Statement::new("SELECT 1 FROM work_order WHERE id=$1 LIMIT 1")
    .with_params(vec![ParameterValue::I64(order_id)]);

tx.execute_conditional_update(
    ConditionalUpdate { update: &update, key_probe: Some(&probe), expected_affected_rows: 1 },
    &cancel,
).await?;  // Ok/NotFound/ConcurrentModification

// Insert outbox atomicamente nella stessa tx
tx.execute(&Statement::new(
    "INSERT INTO outbox_event (aggregate_id, payload) VALUES ($1, $2)"
).with_params(vec![
    ParameterValue::I64(order_id),
    ParameterValue::Json(event_payload),
]), &cancel).await?;

match tx.commit(&cancel).await? {
    CommitOutcome::Committed => Ok(()),
    CommitOutcome::OutcomeUnknown { recovery } => {
        // stato incerto: verifica fuori banda prima di ritentare
        Err(handle_ambiguous_commit(recovery))
    }
}
```

### 13.2 Query singola con facade

```rust
use plenora_database_core::facade::*;

let mut tx = provider.begin_transaction(&secret, &opts, &budget, &cancel).await?;

let version: i64 = execute_scalar_i64(
    tx.as_mut(),
    &Statement::new("SELECT version FROM work_order WHERE id=$1")
        .with_params(vec![ParameterValue::I64(order_id)]),
    &cancel,
).await?;

tx.commit(&cancel).await?;
```

### 13.3 Streaming di dataset grande

```rust
let mut tx = provider.begin_transaction(&secret, &opts, &budget, &cancel).await?;
let stmt = Statement::new("SELECT id, geom FROM buildings WHERE tenant_id=$1")
    .with_params(vec![ParameterValue::String(tenant.into())]);

let mut stream = tx.query_stream(&stmt, 1_000, &cancel).await?;
while let Some(batch) = stream.next_batch(&cancel).await? {
    // batch: Vec<Vec<ParameterValue>>, max 1000 righe per iterazione
    process(batch);
}
drop(stream);
tx.commit(&cancel).await?;
```

### 13.4 Query spatial senza scrivere `ST_*`

```rust
use plenora_db_postgres::build_spatial_select;
use plenora_database_core::{SpatialFilter, SpatialPredicate, SpatialReference};

let filter = SpatialFilter {
    geometry_column: "geom".into(),
    predicate: SpatialPredicate::Intersects,
    reference: SpatialReference {
        ewkb: viewport_wkb,
        srid: 4326,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    },
};
let stmt = build_spatial_select(
    Some("public"),
    "buildings",
    &["id", "geom"],
    &filter,
    Some(500),
)?;

let mut tx = provider.begin_transaction(&secret, &opts, &budget, &cancel).await?;
let rows = tx.query(&stmt, &cancel).await?;
tx.commit(&cancel).await?;
```

### 13.5 Bootstrap: verifica conformità

```rust
use plenora_database_core::{probe_application_oltp_v1, check_profile, APPLICATION_OLTP_V1};

let evidence = probe_application_oltp_v1(&provider, &secret, &cancel).await;
let report = check_profile(&APPLICATION_OLTP_V1, &evidence);
match report.status {
    ProfileStatus::Pass => info!("provider PFM-ready"),
    ProfileStatus::Fail => {
        error!(missing = ?report.missing, failed = ?report.failed, "provider non PFM-ready");
        return Err(BootstrapError::NonConformant);
    }
}
```

---

## 14. Sicurezza e postura

- `#![forbid(unsafe_code)]` in tutti i crate
- `overflow-checks = true` anche in release (fail-closed su invarianti aritmetiche)
- TLS via `rustls` + `webpki-roots` + supporto client cert (mTLS su Postgres)
- Secret redatti: `SecretString::Debug = "[REDACTED]"`, DSN mai in log
- Bind sempre positional, mai stringify interpolata
- Session context: valori Sensitive/Internal redatti in Debug
- Cancellation gerarchica cooperativa: nessun kill di query lato client se non tramite `pg_cancel_backend` (Postgres) o equivalente
- Fuzzing su decoder EWKB (`fuzz/` corpus)
- Test integrazione live su ogni commit (`ci.yml`, `postgres-postgis-assurance.yml`)

---

## 15. Versioning

- Contratti (`contracts/v1/`) versionati, con manifest di readiness in `release/`
- Modifiche **additive** all'API pubblica sono minor bump
- Modifiche **breaking** richiedono `contracts/v2/` (nessuna ancora emessa)
- Nuove `ErrorCategory`, nuove `Capability`, nuovi `SpatialPredicate` sono additivi (enum non-exhaustive de facto tramite match wildcard)
