use crate::catalog;
use crate::config::OracleConfig;
use crate::transaction::{execute_ddl, OracleTransaction};
use crate::OraclePool;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::relational::SpatialFunction;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::transaction::{TransactionOptions, TransactionScope as Transaction};
use plenora_database_core::{CancellationToken, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

struct CachedPool {
    secret_fingerprint: [u8; 32],
    pool: Arc<OraclePool>,
}

/// Adapter Oracle. Non incorpora credenziali e non apre connessioni nel costruttore.
pub struct OracleProvider {
    config: OracleConfig,
    max_connections: usize,
    cached_pool: Mutex<Option<CachedPool>>,
}

impl std::fmt::Debug for OracleProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OracleProvider")
            .field("config", &self.config)
            .field("max_connections", &self.max_connections)
            .field(
                "pool_initialized",
                &lock_recover(&self.cached_pool).is_some(),
            )
            .finish()
    }
}

impl OracleProvider {
    /// Costruisce il provider dopo la validazione locale completa.
    ///
    /// # Errors
    ///
    /// Propaga `InvalidConfiguration` senza contattare Oracle.
    pub fn new(config: OracleConfig) -> Result<Self> {
        Self::new_with_pool(config, 4)
    }

    /// Costruisce il provider con un pool lazy bounded.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidConfiguration` se configurazione o capacita non
    /// sono valide.
    pub fn new_with_pool(config: OracleConfig, max_connections: usize) -> Result<Self> {
        config.validate()?;
        if max_connections == 0 {
            return Err(plenora_database_core::DatabaseError::new(
                plenora_database_core::ErrorCategory::InvalidConfiguration,
                plenora_database_core::ErrorPhase::Validate,
                Some(ProviderKind::Oracle),
                "provider Oracle con pool a capacita zero",
            ));
        }
        Ok(Self {
            config,
            max_connections,
            cached_pool: Mutex::new(None),
        })
    }

    #[must_use]
    pub const fn config(&self) -> &OracleConfig {
        &self.config
    }

    fn pool_for(&self, secret: &SecretString) -> Result<Arc<OraclePool>> {
        let fingerprint: [u8; 32] = Sha256::digest(secret.expose().as_bytes()).into();
        let mut cached = lock_recover(&self.cached_pool);
        if let Some(candidate) = cached.as_ref() {
            if candidate.secret_fingerprint == fingerprint {
                return Ok(Arc::clone(&candidate.pool));
            }
        }
        let pool = OraclePool::new(self.config.clone(), secret.clone(), self.max_connections)?;
        *cached = Some(CachedPool {
            secret_fingerprint: fingerprint,
            pool: Arc::clone(&pool),
        });
        drop(cached);
        Ok(pool)
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Provider for OracleProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Oracle
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            let connection = pool.checkout(cancellation).await?;
            let info = connection.connection()?.server_info().await;
            drop(connection);
            Ok(ConnectionInfo {
                provider: ProviderKind::Oracle,
                server_version: if info.version.is_empty() {
                    "oracle-version-not-reported".to_owned()
                } else {
                    info.version
                },
                connection_identity: info.database_name.or(info.service_name),
            })
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            let info = self.test_connection(secret, cancellation).await?;
            let pool = self.pool_for(secret)?;
            let spatial = catalog::probe_spatial(&self.config, &pool, cancellation).await?;
            oracle_capabilities(info.server_version, spatial).published()
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            catalog::inspect(&self.config, &pool, operation, cancellation).await
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
            let pool = self.pool_for(secret)?;
            crate::read::read_operation(
                &self.config,
                &pool,
                operation,
                parameters,
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
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            crate::write::prepare_write(
                &self.config,
                &pool,
                operation,
                input_schema,
                budget,
                cancellation,
            )
            .await
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
            let pool = self.pool_for(secret)?;
            crate::write::execute_write(&self.config, &pool, prepared, input, budget, cancellation)
                .await
        })
    }

    fn begin_transaction<'a>(
        &'a self,
        secret: &'a SecretString,
        options: &'a TransactionOptions,
        _budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn Transaction>> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            let transaction =
                OracleTransaction::begin(&self.config, &pool, options, cancellation).await?;
            Ok(Box::new(transaction) as Box<dyn Transaction>)
        })
    }

    fn execute_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            execute_ddl(&self.config, &pool, sql, cancellation).await
        })
    }
}

/// Documento iniziale fail-closed. Le prove live apriranno i singoli campi.
#[must_use]
pub fn oracle_capabilities(
    provider_version: String,
    spatial_available: bool,
) -> ProviderCapabilities {
    let qualified_functions = vec![
        SpatialFunction::Srid,
        SpatialFunction::Dimensions,
        SpatialFunction::Intersects,
        SpatialFunction::Contains,
        SpatialFunction::Within,
        SpatialFunction::DWithin,
    ];
    let functions_by_semantics = if spatial_available {
        BTreeMap::from([
            (SpatialSemantics::Geometry, qualified_functions.clone()),
            (SpatialSemantics::Geography, qualified_functions),
        ])
    } else {
        BTreeMap::new()
    };
    let functions =
        plenora_database_core::capabilities::intersect_spatial_functions(&functions_by_semantics);
    ProviderCapabilities {
        schema_version: 2,
        provider: ProviderKind::Oracle,
        provider_version,
        extension_versions: BTreeMap::new(),
        reads: ReadCapabilities {
            streaming: true,
            server_cursor: true,
            pagination: true,
            projection: true,
            filter: true,
            ordering: true,
            resumable: false,
        },
        writes: WriteCapabilities {
            create: true,
            append: true,
            truncate_insert: false,
            update: true,
            upsert: true,
            replace: true,
            delete_by_keys: true,
            bulk: true,
            array_binding: false,
            returning: false,
            rollback_on_failure: true,
        },
        transactions: TransactionCapabilities {
            single_transaction: true,
            savepoints: true,
            transactional_ddl: false,
            staged_swap: false,
            scope: TransactionScope::Transaction,
        },
        spatial: SpatialCapabilities {
            read_wkb: spatial_available,
            write_wkb: spatial_available,
            geometry: spatial_available,
            geography: spatial_available,
            spatial_index: spatial_available,
            mixed_geometry_types: spatial_available,
            dimensions: if spatial_available {
                vec![Dimensions::Xy, Dimensions::Xyz]
            } else {
                Vec::new()
            },
            functions,
            functions_by_semantics,
            requires_declared_crs: false,
        },
        limits: ProviderLimits {
            max_identifier_bytes: None,
            max_bind_parameters: None,
            max_statement_bytes: None,
            max_batch_rows: None,
            max_payload_bytes: None,
        },
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
