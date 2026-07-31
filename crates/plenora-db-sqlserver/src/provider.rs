use crate::read::MAX_CONFIGURED_BATCH_ROWS;
use crate::read::{read_operation, read_query_operation};
use crate::write::{
    prepare_write_with_external_contract_leases, prepare_write_with_options as prepare_driver_write,
};
use crate::{
    describe_object, list_objects, list_schemas, probe_server, write_prepared, SqlServerConfig,
    SqlServerInsertMode, SqlServerPool, SqlServerSchemaEvolution,
};
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    ObjectRef, Operation, ProviderKind, ReadOperation, WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

struct CachedPool {
    secret_fingerprint: [u8; 32],
    pool: Arc<SqlServerPool>,
}

/// Adattatore SQL Server per il contratto comune Plenora.
///
/// L'endpoint, l'utente e le policy TLS provengono da [`SqlServerConfig`].
/// Il [`SecretString`] ricevuto dai metodi [`Provider`] sostituisce sempre la
/// password della configurazione: il piano conserva soltanto `connection_ref`
/// e il secret entra al confine runtime.
///
/// Le capability restano fail-closed. In questa baseline il trait comune
/// espone catalogo, read tabellare senza trasformazioni e le modalità write
/// già provate dal data path TDS. Query relazionali e opzioni read non ancora
/// collegate restituiscono `Unsupported`.
pub struct SqlServerProvider {
    config: SqlServerConfig,
    batch_rows: usize,
    max_connections: usize,
    insert_mode: SqlServerInsertMode,
    schema_evolution: SqlServerSchemaEvolution,
    cached_pool: Mutex<Option<CachedPool>>,
}

impl std::fmt::Debug for SqlServerProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlServerProvider")
            .field("config", &self.config)
            .field("batch_rows", &self.batch_rows)
            .field("max_connections", &self.max_connections)
            .field("insert_mode", &self.insert_mode)
            .field("schema_evolution", &self.schema_evolution)
            .field(
                "pool_initialized",
                &lock_recover(&self.cached_pool).is_some(),
            )
            .finish()
    }
}

impl SqlServerProvider {
    /// Costruisce un provider bounded senza aprire connessioni.
    ///
    /// # Errors
    ///
    /// Fallisce per configurazione invalida, batch nullo/eccessivo o pool a
    /// capacità zero.
    pub fn new(config: SqlServerConfig, batch_rows: usize, max_connections: usize) -> Result<Self> {
        config.validate()?;
        if batch_rows == 0 || batch_rows > MAX_CONFIGURED_BATCH_ROWS {
            return Err(provider_error(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Validate,
                "batch_rows SQL Server fuori dal profilo bounded",
            ));
        }
        if max_connections == 0 {
            return Err(provider_error(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Validate,
                "pool SQL Server con capacità zero",
            ));
        }
        Ok(Self {
            config,
            batch_rows,
            max_connections,
            insert_mode: SqlServerInsertMode::Prepared,
            schema_evolution: SqlServerSchemaEvolution::Disabled,
            cached_pool: Mutex::new(None),
        })
    }

    /// Seleziona esplicitamente il codec write. Il default resta
    /// [`SqlServerInsertMode::Prepared`].
    #[must_use]
    pub const fn with_insert_mode(mut self, insert_mode: SqlServerInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }

    #[must_use]
    pub const fn with_schema_evolution(
        mut self,
        schema_evolution: SqlServerSchemaEvolution,
    ) -> Self {
        self.schema_evolution = schema_evolution;
        self
    }

    fn pool_for(&self, secret: &SecretString) -> Result<Arc<SqlServerPool>> {
        let fingerprint: [u8; 32] = Sha256::digest(secret.expose().as_bytes()).into();
        let mut cached = lock_recover(&self.cached_pool);
        if let Some(candidate) = cached.as_ref() {
            if candidate.secret_fingerprint == fingerprint {
                return Ok(Arc::clone(&candidate.pool));
            }
        }
        let config = self.config.clone().with_password(secret.clone());
        let pool = SqlServerPool::new(config, self.max_connections)?;
        *cached = Some(CachedPool {
            secret_fingerprint: fingerprint,
            pool: Arc::clone(&pool),
        });
        drop(cached);
        Ok(pool)
    }

    fn validate_source(&self, source: &ObjectRef) -> Result<()> {
        if source.layer_id.is_some() {
            return Err(unsupported(
                ErrorPhase::Prepare,
                "layer_id non appartiene al provider SQL Server",
            ));
        }
        if source
            .catalog
            .as_deref()
            .is_some_and(|catalog| catalog != self.config.database())
        {
            return Err(unsupported(
                ErrorPhase::Prepare,
                "accesso cross-database SQL Server non supportato dal provider",
            ));
        }
        Ok(())
    }

    async fn inspect_operation(
        &self,
        pool: &Arc<SqlServerPool>,
        operation: &Operation,
        cancellation: &CancellationToken,
    ) -> Result<Inspection> {
        let mut pooled = pool.checkout(cancellation).await?;
        let inspection = match operation {
            Operation::DatabaseListCatalogs => {
                let probe = probe_server(pooled.session_mut()?, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_catalogs".to_owned(),
                    document: json!({"catalogs": [probe.database]}),
                })
            }
            Operation::DatabaseListSchemas { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schemas = list_schemas(pooled.session_mut()?, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_schemas".to_owned(),
                    document: json!({"schemas": schemas}),
                })
            }
            Operation::DatabaseListObjects { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schema = source.as_ref().and_then(|item| item.schema.as_deref());
                let objects = list_objects(pooled.session_mut()?, schema, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_objects".to_owned(),
                    document: json!({"schema": schema, "objects": objects}),
                })
            }
            Operation::DatabaseDescribeObject { source } => {
                self.validate_source(source)?;
                let schema = source.schema.as_deref().unwrap_or("dbo");
                let description =
                    describe_object(pooled.session_mut()?, schema, &source.object, cancellation)
                        .await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!({
                        "columns": description.columns,
                        "schema_token": description.token,
                        "relation": {
                            "catalog": description.catalog,
                            "schema": description.schema,
                            "name": description.name,
                            "kind": description.kind,
                            "temporal_type": description.temporal_type,
                            "memory_optimized": description.memory_optimized,
                            "durability": description.durability,
                        },
                        "constraints": description.constraints,
                        "indexes": description.indexes,
                    }),
                })
            }
            _ => Err(unsupported(
                ErrorPhase::Probe,
                "operazione di introspezione SQL Server non supportata",
            )),
        };
        drop(pooled);
        inspection
    }
}

impl Provider for SqlServerProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Sqlserver
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Connect)?;
            let pool = self.pool_for(secret)?;
            let mut pooled = pool.checkout(cancellation).await?;
            let probe = probe_server(pooled.session_mut()?, cancellation).await?;
            drop(pooled);
            Ok(ConnectionInfo {
                provider: ProviderKind::Sqlserver,
                server_version: probe.product_version,
                connection_identity: Some(probe.database),
            })
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Probe)?;
            let pool = self.pool_for(secret)?;
            let mut pooled = pool.checkout(cancellation).await?;
            let probe = probe_server(pooled.session_mut()?, cancellation).await?;
            drop(pooled);
            Ok(ProviderCapabilities {
                schema_version: 1,
                provider: ProviderKind::Sqlserver,
                provider_version: probe.product_version,
                extension_versions: BTreeMap::new(),
                reads: ReadCapabilities {
                    streaming: true,
                    server_cursor: false,
                    pagination: true,
                    object_id_windows: false,
                    projection: true,
                    filter: true,
                    ordering: true,
                    resumable: false,
                },
                writes: WriteCapabilities {
                    create: true,
                    append: true,
                    update: true,
                    upsert: true,
                    replace: true,
                    delete_by_keys: true,
                    bulk: true,
                    array_binding: false,
                    returning: false,
                    apply_edits: false,
                    rollback_on_failure: true,
                    use_global_ids: false,
                },
                transactions: TransactionCapabilities {
                    single_transaction: true,
                    savepoints: false,
                    transactional_ddl: true,
                    staged_swap: true,
                    scope: TransactionScope::Transaction,
                },
                spatial: SpatialCapabilities {
                    read_wkb: true,
                    write_wkb: true,
                    geometry: probe.geometry_type_id.is_some(),
                    geography: probe.geography_type_id.is_some(),
                    spatial_index: probe.geometry_type_id.is_some()
                        && probe.geography_type_id.is_some(),
                    mixed_geometry_types: false,
                    dimensions: vec![
                        Dimensions::Xy,
                        Dimensions::Xyz,
                        Dimensions::Xym,
                        Dimensions::Xyzm,
                    ],
                    functions: crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec(),
                },
                limits: ProviderLimits {
                    // Il core applica già il limite portabile più stretto.
                    max_identifier_bytes: Some(256),
                    max_bind_parameters: Some(crate::MAX_BIND_PARAMETERS as u64),
                    max_statement_bytes: None,
                    max_batch_rows: Some(MAX_CONFIGURED_BATCH_ROWS as u64),
                    max_payload_bytes: None,
                    max_record_count: None,
                },
            })
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Probe)?;
            let pool = self.pool_for(secret)?;
            self.inspect_operation(&pool, operation, cancellation).await
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
            ensure_not_cancelled(cancellation, ErrorPhase::Read)?;
            self.validate_source(&operation.source)?;
            let pool = self.pool_for(secret)?;
            read_operation(
                &pool,
                operation,
                parameters,
                self.batch_rows,
                budget,
                cancellation,
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
            ensure_not_cancelled(cancellation, ErrorPhase::Read)?;
            let pool = self.pool_for(secret)?;
            read_query_operation(
                &pool,
                self.config.database(),
                operation,
                parameters,
                self.batch_rows,
                budget,
                cancellation,
            )
            .await
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: plenora_database_core::arrow::SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Prepare)?;
            self.validate_source(&operation.target)?;
            let pool = self.pool_for(secret)?;
            let driver_prepared = prepare_driver_write(
                &pool,
                operation,
                Arc::clone(&input_schema),
                budget,
                cancellation,
                self.insert_mode,
                self.schema_evolution,
            )
            .await?;
            let loss_report = driver_prepared.loss_report().clone();
            drop(driver_prepared);

            let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
            let column_count = u64::try_from(input_schema.fields().len())
                .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
            let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
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
            ensure_not_cancelled(cancellation, ErrorPhase::Write)?;
            self.validate_source(&prepared.operation.target)?;
            let input_schema = input.schema();
            let pool = self.pool_for(secret)?;
            let driver_prepared = prepare_write_with_external_contract_leases(
                &pool,
                &prepared.operation,
                input_schema,
                budget,
                cancellation,
                self.insert_mode,
                self.schema_evolution,
            )
            .await?;
            let result = write_prepared(driver_prepared, input, cancellation).await;
            drop(prepared);
            result
        })
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn ensure_not_cancelled(cancellation: &CancellationToken, phase: ErrorPhase) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(DatabaseError::cancelled(
            Some(ProviderKind::Sqlserver),
            phase,
            "operazione SQL Server cancellata prima dell'I/O",
        ))
    } else {
        Ok(())
    }
}

fn unsupported(phase: ErrorPhase, message: &'static str) -> DatabaseError {
    DatabaseError::unsupported(ProviderKind::Sqlserver, phase, message)
}

fn provider_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: &'static str,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CertificatePolicy;

    fn provider() -> SqlServerProvider {
        let config = SqlServerConfig::new(
            "sql.example.test",
            "warehouse",
            "loader",
            SecretString::new("constructor-secret"),
        )
        .with_certificate_policy(CertificatePolicy::TrustServerCertificate);
        SqlServerProvider::new(config, 1_024, 4).expect("provider")
    }

    #[test]
    fn implements_common_provider_contract_type() {
        const fn assert_provider<T: Provider>() {}
        assert_provider::<SqlServerProvider>();
        assert_eq!(provider().kind(), ProviderKind::Sqlserver);
    }

    #[tokio::test]
    async fn pre_cancelled_connection_fails_without_network() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = provider()
            .test_connection(&SecretString::new("runtime-secret"), &cancellation)
            .await
            .expect_err("cancelled");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        assert_eq!(error.phase, ErrorPhase::Connect);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert_eq!(error.provider, Some(ProviderKind::Sqlserver));
    }

    #[test]
    fn provider_debug_redacts_constructor_and_runtime_state() {
        let rendered = format!("{:?}", provider());
        assert!(!rendered.contains("constructor-secret"));
        assert!(!rendered.contains("loader"));
        assert!(rendered.contains("[REDACTED]"));
    }
}
