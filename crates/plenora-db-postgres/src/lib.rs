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
mod query_execution;
mod query_plan;
mod read_stream;
mod schema_cache;
mod types;
mod write;

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
use plenora_database_core::loss::{LossCategory, LossReport, LossSeverity, MappingLoss};
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
    fn default() -> Self {
        Self::for_profile(PostgresPerformanceProfile::LowLatency)
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

    #[allow(clippy::too_many_lines)]
    async fn preflight_write(
        &self,
        secret: &SecretString,
        operation: &WriteOperation,
        input_schema: &SchemaRef,
    ) -> Result<LossReport> {
        let client = self.connect_session(secret).await?;
        validate_resolved_crs_against_postgis(&client, input_schema).await?;
        let schema_name = operation.target.schema.as_deref().unwrap_or("public");
        let exists: bool = client
            .client()?
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )",
                &[&schema_name, &operation.target.object],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
            .get(0);
        if operation.mode == plenora_database_core::plan::WriteMode::Create && exists {
            return Err(public_error(
                ErrorCategory::Conflict,
                ErrorPhase::Prepare,
                false,
                "target PostgreSQL già esistente",
            ));
        }
        if !matches!(
            operation.mode,
            plenora_database_core::plan::WriteMode::Create
                | plenora_database_core::plan::WriteMode::Replace
        ) && !exists
        {
            return Err(public_error(
                ErrorCategory::NotFound,
                ErrorPhase::Prepare,
                false,
                "target PostgreSQL non esistente",
            ));
        }
        let mut losses = Vec::new();
        if exists
            && !matches!(
                operation.mode,
                plenora_database_core::plan::WriteMode::Create
                    | plenora_database_core::plan::WriteMode::Replace
            )
        {
            let (target_columns, _schema_token) =
                self.cached_columns(&client, &operation.target).await?;
            for (field_id, source) in input_schema.fields().iter().enumerate() {
                let Some(target) = target_columns
                    .iter()
                    .find(|column| column.name == source.name().as_str())
                else {
                    if self.schema_evolution == PostgresSchemaEvolution::AddNullableColumns
                        && supports_additive_evolution(operation.mode)
                    {
                        losses.push(MappingLoss {
                            field_id: u32::try_from(field_id).map_err(|_| {
                                DatabaseError::invalid_plan(
                                    "numero colonne oltre il contratto LossReport",
                                )
                            })?,
                            category: LossCategory::NativeType,
                            severity: LossSeverity::Information,
                            reason: "colonna aggiunta al target come nullable".to_owned(),
                            source_type: Some(source.data_type().to_string()),
                            target_type: Some("nuova colonna nullable".to_owned()),
                        });
                    } else {
                        losses.push(mapping_loss(
                            field_id,
                            LossCategory::NativeType,
                            "colonna sorgente assente nel target",
                            Some(source.data_type().to_string()),
                            None,
                        )?);
                    }
                    continue;
                };
                let target_field = target.arrow_field();
                if source.data_type() != target_field.data_type() {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::NativeType,
                        "tipo Arrow non equivalente al target PostgreSQL",
                        Some(source.data_type().to_string()),
                        Some(target_field.data_type().to_string()),
                    )?);
                }
                if source.is_nullable() && !target.nullable {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::Nullability,
                        "sorgente nullable verso colonna NOT NULL",
                        None,
                        None,
                    )?);
                }
                let source_srid = source
                    .metadata()
                    .get(protocol::GEOMETRY_SRID)
                    .or_else(|| source.metadata().get("plenora.srid"));
                let target_srid = target_field
                    .metadata()
                    .get(protocol::GEOMETRY_SRID)
                    .or_else(|| target_field.metadata().get("plenora.srid"));
                if source_srid != target_srid && (source_srid.is_some() || target_srid.is_some()) {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::Srid,
                        "SRID sorgente e target differenti",
                        source_srid.cloned(),
                        target_srid.cloned(),
                    )?);
                }
                let source_crs = source.metadata().get(protocol::GEOMETRY_CRS_ID);
                let target_crs = target_field.metadata().get(protocol::GEOMETRY_CRS_ID);
                if source_crs != target_crs && (source_crs.is_some() || target_crs.is_some()) {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::Crs,
                        "identificatore CRS sorgente e target differente",
                        source_crs.cloned(),
                        target_crs.cloned(),
                    )?);
                }
            }
            for key in &operation.keys {
                if !target_columns.iter().any(|column| &column.name == key) {
                    return Err(DatabaseError::invalid_plan(
                        "chiave write assente nel target PostgreSQL",
                    ));
                }
            }
        }
        let report = LossReport {
            schema_version: 1,
            policy: operation.mapping_policy,
            losses,
        };
        if !report.permits_execution() {
            return Err(public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                false,
                "preflight PostgreSQL rileva conversioni non ammesse",
            ));
        }
        drop(client);
        Ok(report)
    }
}

async fn validate_resolved_crs_against_postgis(
    client: &PooledClient,
    input_schema: &SchemaRef,
) -> Result<()> {
    for field in input_schema.fields() {
        if field
            .metadata()
            .get(protocol::GEOMETRY_CRS_RESOLUTION)
            .is_none_or(|value| value != "resolved")
        {
            continue;
        }
        let srid = field
            .metadata()
            .get(protocol::GEOMETRY_SRID)
            .and_then(|value| value.parse::<i32>().ok())
            .ok_or_else(|| DatabaseError::invalid_plan("CRS resolved senza SRID valido"))?;
        let declared_id = field
            .metadata()
            .get(protocol::GEOMETRY_CRS_ID)
            .ok_or_else(|| DatabaseError::invalid_plan("CRS resolved senza identificatore"))?;
        let row = client
            .client()?
            .query_opt(
                "SELECT auth_name, auth_srid
                 FROM spatial_ref_sys
                 WHERE srid = $1",
                &[&srid],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Prepare,
                    false,
                    "SRID resolved assente da spatial_ref_sys",
                )
            })?;
        let authority: Option<String> = row.get(0);
        let authority_code: Option<i32> = row.get(1);
        let observed_id = authority
            .filter(|value| !value.is_empty())
            .zip(authority_code)
            .map(|(name, code)| format!("{name}:{code}"))
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Prepare,
                    false,
                    "spatial_ref_sys non risolve un authority ID",
                )
            })?;
        if !declared_id.eq_ignore_ascii_case(&observed_id) {
            return Err(public_error(
                ErrorCategory::Crs,
                ErrorPhase::Prepare,
                false,
                "identificatore CRS non coerente con spatial_ref_sys",
            ));
        }
    }
    Ok(())
}

fn mapping_loss(
    field_id: usize,
    category: LossCategory,
    reason: &str,
    source_type: Option<String>,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    Ok(MappingLoss {
        field_id: u32::try_from(field_id).map_err(|_| {
            DatabaseError::invalid_plan("numero colonne oltre il contratto LossReport")
        })?,
        category,
        severity: LossSeverity::DataLoss,
        reason: reason.to_owned(),
        source_type,
        target_type,
    })
}

const fn supports_additive_evolution(mode: plenora_database_core::plan::WriteMode) -> bool {
    matches!(
        mode,
        plenora_database_core::plan::WriteMode::Append
            | plenora_database_core::plan::WriteMode::TruncateInsert
            | plenora_database_core::plan::WriteMode::Update
            | plenora_database_core::plan::WriteMode::Upsert
    )
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
            let loss_report = self
                .preflight_write(secret, operation, &input_schema)
                .await?;
            Ok(PreparedWrite {
                operation: operation.clone(),
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
