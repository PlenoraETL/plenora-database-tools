use crate::catalog;
use crate::config::OracleConfig;
use crate::connection::connect;
use crate::transaction::{execute_ddl, OracleTransaction};
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
use std::collections::BTreeMap;

/// Adapter Oracle. Non incorpora credenziali e non apre connessioni nel costruttore.
#[derive(Debug)]
pub struct OracleProvider {
    config: OracleConfig,
}

impl OracleProvider {
    /// Costruisce il provider dopo la validazione locale completa.
    ///
    /// # Errors
    ///
    /// Propaga `InvalidConfiguration` senza contattare Oracle.
    pub fn new(config: OracleConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    #[must_use]
    pub const fn config(&self) -> &OracleConfig {
        &self.config
    }
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
            let connection = connect(&self.config, secret, cancellation).await?;
            let info = connection.server_info().await;
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
            let spatial = catalog::probe_spatial(&self.config, secret, cancellation).await?;
            oracle_capabilities(info.server_version, spatial).published()
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(
            async move { catalog::inspect(&self.config, secret, operation, cancellation).await },
        )
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
            crate::read::read_operation(
                &self.config,
                secret,
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
            crate::write::prepare_write(
                &self.config,
                secret,
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
            crate::write::execute_write(&self.config, secret, prepared, input, budget, cancellation)
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
            let transaction =
                OracleTransaction::begin(&self.config, secret, options, cancellation).await?;
            Ok(Box::new(transaction) as Box<dyn Transaction>)
        })
    }

    fn execute_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { execute_ddl(&self.config, secret, sql, cancellation).await })
    }
}

/// Documento iniziale fail-closed. Le prove live apriranno i singoli campi.
#[must_use]
pub fn oracle_capabilities(
    provider_version: String,
    spatial_available: bool,
) -> ProviderCapabilities {
    let functions_by_semantics = if spatial_available {
        BTreeMap::from([(
            SpatialSemantics::Geometry,
            vec![
                SpatialFunction::Srid,
                SpatialFunction::Dimensions,
                SpatialFunction::Intersects,
                SpatialFunction::Contains,
                SpatialFunction::Within,
                SpatialFunction::DWithin,
            ],
        )])
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
            geography: false,
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
