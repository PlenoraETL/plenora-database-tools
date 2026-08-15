//! Driver pilota PostgreSQL/PostGIS.
//!
//! Il driver di riferimento copre connessione, capability, introspezione, read
//! batch-bounded verso Arrow/GeoArrow-WKB e tutte le modalità di scrittura
//! definite dal core.

mod arrow;
mod catalog;
mod connection;
mod control;
mod error;
mod field_contract;
mod metrics;
mod parameter_codec;
mod pool;
mod preflight;
mod query_execution;
mod query_plan;
mod read_stream;
mod schema_cache;
mod spatial;
mod transaction;
mod types;
mod write;

pub use spatial::{build_spatial_select, spatial_reference};

pub use connection::{PostgresNetworkOptions, PostgresTlsConfig, PostgresTlsMode};
pub use metrics::PostgresMetricsSnapshot;
pub use schema_cache::PostgresSchemaToken;

use arrow_schema::{Field, Schema, SchemaRef};
use catalog::{
    capability_document, describe_object_metadata, list_catalogs, list_objects, list_schemas,
    load_columns_and_token, schema_token,
};
use connection::connection_fingerprint;
#[cfg(test)]
use connection::{
    configure_session_startup, connect as open_connection,
    connect_with_tls as open_connection_with_tls, connection_config, connection_config_for_mode,
};
use error::{check_cancelled, classify_error, public_error};
use metrics::PostgresMetrics;
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    ObjectRef, Operation, ProviderKind, ReadOperation, WriteOperation,
};
use plenora_database_core::protocol;
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use pool::{PooledClient, PostgresPool, PostgresSessionOptions};
#[cfg(test)]
use read_stream::batch_memory_bytes;
use read_stream::BudgetCancellation;
use schema_cache::{PostgresSchemaCache, SchemaCacheKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
#[cfg(test)]
use std::time::Duration as StdDuration;
#[cfg(test)]
use tokio_postgres::config::SslMode;
#[cfg(test)]
use tokio_postgres::Client;
use types::ColumnSpec;

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn contract_schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

#[derive(Debug, Clone)]
pub struct PostgresProvider {
    batch_rows: usize,
    statement_timeout_ms: u64,
    lock_timeout_ms: u64,
    fault_point: Option<PostgresFaultPoint>,
    insert_mode: PostgresInsertMode,
    parameterized_read_fast_path: bool,
    target_batch_bytes: Option<u64>,
    max_batch_bytes: u64,
    max_wkb_cell_bytes: u64,
    tls_mode: PostgresTlsMode,
    tls_config: PostgresTlsConfig,
    network_options: PostgresNetworkOptions,
    schema_evolution: PostgresSchemaEvolution,
    pool: Arc<PostgresPool>,
    schema_cache: Arc<PostgresSchemaCache>,
    metrics: Arc<PostgresMetrics>,
    pool_acquire_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresFaultPoint {
    BeforeCommit,
    AfterCommitAcknowledgement,
    RollbackAcknowledgementLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresInsertMode {
    CopyText,
    CopyBinary,
    Prepared,
}

/// Configurazione prestazionale coerente e misurata del data path `PostgreSQL`.
///
/// `LowLatency` mantiene piccoli i batch per ridurre il time to first batch.
/// `BalancedBulk` privilegia throughput e WAL usando COPY binario e batch più
/// grandi, senza adottare il profilo da 32K che aumenta sensibilmente RSS e
/// latenza iniziale sui dati wide/spatial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPerformanceProfile {
    LowLatency,
    BalancedBulk,
}

impl PostgresPerformanceProfile {
    #[must_use]
    pub const fn batch_rows(self) -> usize {
        match self {
            Self::LowLatency => 1_024,
            Self::BalancedBulk => 8_192,
        }
    }

    #[must_use]
    pub const fn insert_mode(self) -> PostgresInsertMode {
        match self {
            Self::LowLatency => PostgresInsertMode::CopyText,
            Self::BalancedBulk => PostgresInsertMode::CopyBinary,
        }
    }

    #[must_use]
    pub const fn target_batch_bytes(self) -> u64 {
        match self {
            Self::LowLatency => 1024 * 1024,
            Self::BalancedBulk => 4 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSchemaEvolution {
    Disabled,
    AddNullableColumns,
}

impl Default for PostgresProvider {
    /// TLS `Require` + trust store `WebPKI` pubblico (secure-by-default).
    ///
    /// Cambio breaking in `1.2.0` — ADR-011: prima era `Disabled`.
    /// Per test/dev locali senza TLS usare esplicitamente
    /// [`PostgresProvider::insecure_local`].
    ///
    /// Per CA private / mTLS: `default().with_tls_config(custom)`.
    fn default() -> Self {
        Self::for_profile(PostgresPerformanceProfile::LowLatency)
            .with_tls_mode(PostgresTlsMode::Require)
            .with_tls_config(PostgresTlsConfig::webpki())
    }
}

impl PostgresProvider {
    fn build(batch_rows: usize) -> Self {
        let metrics = Arc::new(PostgresMetrics::default());
        Self {
            batch_rows,
            statement_timeout_ms: 30_000,
            lock_timeout_ms: 5_000,
            fault_point: None,
            insert_mode: PostgresInsertMode::CopyText,
            parameterized_read_fast_path: true,
            target_batch_bytes: None,
            max_batch_bytes: 16 * 1024 * 1024,
            max_wkb_cell_bytes: 64 * 1024 * 1024,
            tls_mode: PostgresTlsMode::Disabled,
            tls_config: PostgresTlsConfig::default(),
            network_options: PostgresNetworkOptions::default(),
            schema_evolution: PostgresSchemaEvolution::Disabled,
            pool: Arc::new(PostgresPool::new(8, Arc::clone(&metrics))),
            schema_cache: Arc::new(PostgresSchemaCache::new(256)),
            metrics,
            pool_acquire_timeout_ms: 30_000,
        }
    }

    #[must_use]
    pub fn new(batch_rows: usize) -> Self {
        Self::build(batch_rows)
    }

    /// Provider **senza TLS** (`PostgresTlsMode::Disabled`).
    ///
    /// **Solo per test/dev locali** contro `docker-compose` o setup
    /// dev-env noti. In produzione l'operatore ha responsabilità di
    /// non usare questo costruttore: il nome è esplicito per non
    /// nasconderlo dietro un default.
    ///
    /// ADR-011: introdotto in `1.2.0` insieme al flip di `default()`
    /// a `Require`. Sostituisce il pattern
    /// `PostgresProvider::default().with_tls_mode(Disabled)`.
    #[must_use]
    pub fn insecure_local() -> Self {
        Self::for_profile(PostgresPerformanceProfile::LowLatency)
            .with_tls_mode(PostgresTlsMode::Disabled)
    }

    /// Alias storico di [`Self::default`] (pre-`1.2.0` era necessario
    /// perché `default()` era Disabled). Mantenuto per non rompere
    /// consumer intermedio che aveva già migrato al secure factory.
    #[must_use]
    #[deprecated(since = "1.2.0", note = "usa Self::default() (ora secure-by-default) — ADR-011")]
    pub fn default_secure() -> Self {
        Self::default()
    }

    /// Costruisce il provider con un profilo prestazionale misurato.
    ///
    /// I setter successivi possono ancora sovrascrivere le singole scelte.
    #[must_use]
    pub fn for_profile(profile: PostgresPerformanceProfile) -> Self {
        Self::build(profile.batch_rows()).with_performance_profile(profile)
    }

    /// Applica un profilo a un provider già configurato, conservando pool,
    /// timeout, TLS, limiti in byte ed evoluzione schema.
    #[must_use]
    pub const fn with_performance_profile(mut self, profile: PostgresPerformanceProfile) -> Self {
        self.batch_rows = profile.batch_rows();
        self.insert_mode = profile.insert_mode();
        self.target_batch_bytes = Some(profile.target_batch_bytes());
        self
    }

    /// Imposta un target soft in byte per i batch di lettura.
    ///
    /// Il batch viene chiuso dopo la riga che raggiunge il target; una singola
    /// riga può quindi superarlo. `max_batch_bytes` resta il limite hard.
    #[must_use]
    pub const fn with_target_batch_bytes(mut self, target_batch_bytes: u64) -> Self {
        self.target_batch_bytes = Some(target_batch_bytes);
        self
    }

    /// Disabilita il target adattivo e usa soltanto il tetto in righe.
    #[must_use]
    pub const fn without_target_batch_bytes(mut self) -> Self {
        self.target_batch_bytes = None;
        self
    }

    #[must_use]
    pub const fn with_timeouts(mut self, statement_timeout_ms: u64, lock_timeout_ms: u64) -> Self {
        self.statement_timeout_ms = statement_timeout_ms;
        self.lock_timeout_ms = lock_timeout_ms;
        self
    }

    #[must_use]
    pub const fn with_fault_injection(mut self, fault_point: PostgresFaultPoint) -> Self {
        self.fault_point = Some(fault_point);
        self
    }

    #[must_use]
    pub const fn with_insert_mode(mut self, insert_mode: PostgresInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }

    /// Abilita o disabilita il protocollo one-shot per read con bind tipizzabili.
    #[must_use]
    pub const fn with_parameterized_read_fast_path(mut self, enabled: bool) -> Self {
        self.parameterized_read_fast_path = enabled;
        self
    }

    #[must_use]
    pub const fn with_byte_limits(mut self, max_batch_bytes: u64, max_wkb_cell_bytes: u64) -> Self {
        self.max_batch_bytes = max_batch_bytes;
        self.max_wkb_cell_bytes = max_wkb_cell_bytes;
        self
    }

    #[must_use]
    pub const fn with_tls_mode(mut self, tls_mode: PostgresTlsMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    #[must_use]
    pub fn with_tls_config(mut self, tls_config: PostgresTlsConfig) -> Self {
        self.tls_mode = PostgresTlsMode::Require;
        self.tls_config = tls_config;
        self
    }

    #[must_use]
    pub const fn with_network_options(mut self, network_options: PostgresNetworkOptions) -> Self {
        self.network_options = network_options;
        self
    }

    #[must_use]
    pub const fn with_schema_evolution(
        mut self,
        schema_evolution: PostgresSchemaEvolution,
    ) -> Self {
        self.schema_evolution = schema_evolution;
        self
    }

    #[must_use]
    pub fn with_pool_size(mut self, max_connections: usize, acquire_timeout_ms: u64) -> Self {
        self.pool = Arc::new(PostgresPool::new(
            max_connections,
            Arc::clone(&self.metrics),
        ));
        self.pool_acquire_timeout_ms = acquire_timeout_ms;
        self
    }

    /// Imposta il numero massimo di schemi `PostgreSQL` conservati nella cache.
    ///
    /// La cache resta strict: ogni hit viene validato contro un token di
    /// catalogo. Una capacità pari a zero la disabilita.
    #[must_use]
    pub fn with_schema_cache_capacity(mut self, max_entries: usize) -> Self {
        self.schema_cache = Arc::new(PostgresSchemaCache::new(max_entries));
        self
    }

    /// Restituisce contatori bounded e privi di label sensibili o cardinalità dinamica.
    #[must_use]
    pub fn metrics_snapshot(&self) -> PostgresMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn pool_idle_connections(&self) -> usize {
        self.pool.idle_connections()
    }

    #[must_use]
    pub fn schema_cache_entries(&self) -> usize {
        self.schema_cache.len()
    }

    #[cfg(test)]
    async fn connect(secret: &SecretString) -> Result<Client> {
        open_connection(secret).await
    }

    async fn connect_session(&self, secret: &SecretString) -> Result<PooledClient> {
        let mut client = self
            .pool
            .checkout(
                secret,
                self.tls_mode,
                &self.tls_config,
                self.network_options,
                PostgresSessionOptions::new(self.statement_timeout_ms, self.lock_timeout_ms),
                self.pool_acquire_timeout_ms,
            )
            .await?;
        if client.was_reused() {
            if let Err(error) = client.client()?.batch_execute("DISCARD ALL").await {
                client.invalidate();
                return Err(classify_error(ErrorPhase::Connect, &error));
            }
            self.metrics.session_reset();
        }
        Ok(client)
    }

    #[cfg(test)]
    async fn connect_with_tls(
        secret: &SecretString,
        tls_mode: PostgresTlsMode,
        tls_config: &PostgresTlsConfig,
        network_options: PostgresNetworkOptions,
        statement_timeout_ms: u64,
        lock_timeout_ms: u64,
    ) -> Result<Client> {
        open_connection_with_tls(
            secret,
            tls_mode,
            tls_config,
            network_options,
            statement_timeout_ms,
            lock_timeout_ms,
        )
        .await
    }

    fn schema_cache_key(client: &PooledClient, source: &ObjectRef) -> SchemaCacheKey {
        SchemaCacheKey::new(
            client.key(),
            source.schema.as_deref().unwrap_or("public").to_owned(),
            source.object.clone(),
        )
    }

    async fn cached_columns(
        &self,
        client: &PooledClient,
        source: &ObjectRef,
    ) -> Result<(Arc<Vec<ColumnSpec>>, PostgresSchemaToken)> {
        let key = Self::schema_cache_key(client, source);
        if let Some((candidate_token, candidate_columns)) = self.schema_cache.candidate(&key) {
            self.metrics.schema_token_check();
            let current = match schema_token(client.client()?, source).await {
                Ok(token) => token,
                Err(error) => {
                    if self.schema_cache.invalidate(&key) {
                        self.metrics.schema_cache_invalidation();
                    }
                    return Err(error);
                }
            };
            if candidate_token.structurally_equals(&current) {
                self.schema_cache.touch(&key);
                self.metrics.schema_cache_hit();
                return Ok((candidate_columns, current.public));
            }
            if self.schema_cache.invalidate(&key) {
                self.metrics.schema_cache_invalidation();
            }
        }
        self.metrics.schema_cache_miss();
        self.metrics.catalog_introspection();
        let (columns, token) = load_columns_and_token(client.client()?, source).await?;
        if self
            .schema_cache
            .insert(key, token.clone(), Arc::clone(&columns))
        {
            self.metrics.schema_cache_eviction();
        }
        Ok((columns, token.public))
    }

    fn invalidate_cached_schema(&self, secret: &SecretString, source: &ObjectRef) {
        let connection = connection_fingerprint(
            secret,
            self.tls_mode,
            &self.tls_config,
            self.network_options,
            self.statement_timeout_ms,
            self.lock_timeout_ms,
        );
        let key = SchemaCacheKey::new(
            connection,
            source.schema.as_deref().unwrap_or("public").to_owned(),
            source.object.clone(),
        );
        if self.schema_cache.invalidate(&key) {
            self.metrics.schema_cache_invalidation();
        }
    }

    async fn inspection(&self, client: &PooledClient, operation: &Operation) -> Result<Inspection> {
        match operation {
            Operation::DatabaseListCatalogs => Ok(Inspection {
                operation: "database.list_catalogs".to_owned(),
                document: json!({"catalogs": list_catalogs(client.client()?).await?}),
            }),
            Operation::DatabaseListSchemas { .. } => Ok(Inspection {
                operation: "database.list_schemas".to_owned(),
                document: json!({"schemas": list_schemas(client.client()?).await?}),
            }),
            Operation::DatabaseListObjects { source } => {
                let schema = source
                    .as_ref()
                    .and_then(|value| value.schema.as_deref())
                    .unwrap_or("public");
                let objects = list_objects(client.client()?, schema).await?;
                Ok(Inspection {
                    operation: "database.list_objects".to_owned(),
                    document: json!({"schema": schema, "objects": objects}),
                })
            }
            Operation::DatabaseDescribeObject { source } => {
                let (columns, schema_token) = self.cached_columns(client, source).await?;
                let metadata = describe_object_metadata(client.client()?, source).await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!({
                        "columns": columns.as_ref(),
                        "schema_token": schema_token,
                        "relation": metadata.relation,
                        "constraints": metadata.constraints,
                        "indexes": metadata.indexes,
                        "policies": metadata.policies,
                        "privileges": metadata.privileges
                    }),
                })
            }
            _ => Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                ErrorPhase::Probe,
                "operazione di introspezione PostgreSQL non supportata",
            )),
        }
    }
}

impl Provider for PostgresProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Connect)?;
            let client = self.connect_session(secret).await?;
            let row = client
                .client()?
                .query_one(
                    "SELECT current_setting('server_version'), current_database(), current_user",
                    &[],
                )
                .await
                .map_err(|error| classify_error(ErrorPhase::Connect, &error))?;
            let info = ConnectionInfo {
                provider: ProviderKind::Postgres,
                server_version: row.get(0),
                connection_identity: Some(format!(
                    "{}/{}",
                    row.get::<_, String>(1),
                    row.get::<_, String>(2)
                )),
            };
            drop(client);
            Ok(info)
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Probe)?;
            let client = self.connect_session(secret).await?;
            capability_document(client.client()?).await
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Probe)?;
            let client = self.connect_session(secret).await?;
            self.inspection(&client, operation).await
        })
    }

    fn read<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            let control = BudgetCancellation::new(cancellation, budget)?;
            query_execution::read_stream(
                self,
                secret,
                operation,
                parameters,
                budget,
                control.token(),
            )
            .await
        })
    }

    fn query<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a QueryOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            let control = BudgetCancellation::new(cancellation, budget)?;
            query_execution::query_stream(
                self,
                secret,
                operation,
                parameters,
                budget,
                control.token(),
            )
            .await
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            let control = BudgetCancellation::new(cancellation, budget)?;
            check_cancelled(control.token(), ErrorPhase::Prepare)?;
            write::validate_schema(&input_schema, operation)?;
            let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
            let column_count = u64::try_from(input_schema.fields().len())
                .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
            let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
            let loss_report = preflight::write(self, secret, operation, &input_schema).await?;
            Ok(PreparedWrite {
                operation: operation.clone(),
                input_schema,
                loss_report,
                budget: budget.clone(),
                operation_lease,
                columns_lease,
            })
        })
    }

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async move {
            if !prepared.budget.is_same_budget(budget) {
                return Err(DatabaseError::invalid_plan(
                    "il budget di write non coincide con quello usato in prepare_write",
                ));
            }
            let control = BudgetCancellation::new(cancellation, budget)?;
            let target = prepared.operation.target.clone();
            let result = write::execute(
                secret,
                prepared,
                input,
                budget,
                control.token(),
                write::WriteRuntime {
                    statement_timeout_ms: self.statement_timeout_ms,
                    lock_timeout_ms: self.lock_timeout_ms,
                    fault_point: self.fault_point,
                    insert_mode: self.insert_mode,
                    max_batch_bytes: self.max_batch_bytes,
                    max_wkb_cell_bytes: self.max_wkb_cell_bytes,
                    tls_mode: self.tls_mode,
                    tls_config: self.tls_config.clone(),
                    network_options: self.network_options,
                    schema_evolution: self.schema_evolution,
                    pool: Arc::clone(&self.pool),
                    metrics: Arc::clone(&self.metrics),
                    pool_acquire_timeout_ms: self.pool_acquire_timeout_ms,
                },
            )
            .await;
            if result.is_ok() {
                self.invalidate_cached_schema(secret, &target);
            }
            result
        })
    }

    fn begin_transaction<'a>(
        &'a self,
        secret: &'a SecretString,
        options: &'a plenora_database_core::transaction::TransactionOptions,
        _budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn plenora_database_core::transaction::TransactionScope>> {
        Box::pin(async move {
            let client = self.connect_session(secret).await?;
            let transaction =
                transaction::PostgresTransaction::begin(client, options, cancellation).await?;
            Ok(Box::new(transaction) as Box<dyn plenora_database_core::transaction::TransactionScope>)
        })
    }

    fn execute_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            error::check_cancelled(cancellation, ErrorPhase::Prepare)?;
            let mut client = self.connect_session(secret).await?;
            let result = client
                .client_mut()?
                .batch_execute(sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Write, &error));
            if result.is_err() {
                client.invalidate();
            }
            result
        })
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn parse_decimal128(value: &str, scale: i8) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches(['-', '+']);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(if integer.is_empty() { "0" } else { integer });
    digits.push_str(fraction);
    let mut parsed = digits.parse::<i128>().map_err(|_| {
        public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            false,
            "decimal oltre Decimal128",
        )
    })?;
    let exponent = i32::from(scale)
        - i32::try_from(fraction.len()).map_err(|_| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "scala decimal non valida",
            )
        })?;
    if exponent >= 0 {
        let factor = 10_i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    false,
                    "decimal oltre Decimal128",
                )
            })?;
        parsed = parsed.checked_mul(factor).ok_or_else(|| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "decimal oltre Decimal128",
            )
        })?;
    } else {
        let divisor = 10_i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    false,
                    "scala decimal non valida",
                )
            })?;
        if parsed % divisor != 0 {
            return Err(public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "decimal non rappresentabile alla scala Arrow dichiarata",
            ));
        }
        parsed /= divisor;
    }
    Ok(if negative { -parsed } else { parsed })
}

#[cfg(test)]
mod test_suite;
