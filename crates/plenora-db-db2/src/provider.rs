use crate::catalog;
use crate::connection::probe;
use crate::read::read_operation;
use crate::Db2Config;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::capabilities::{
    intersect_spatial_functions, ProviderCapabilities, ProviderLimits, ReadCapabilities,
    SpatialCapabilities, TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::SpatialFunction;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::transaction::{
    TransactionOptions, TransactionScope as TransactionScopeContract,
};
use plenora_database_core::{CancellationToken, Result};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Db2Provider {
    config: Db2Config,
}

impl Db2Provider {
    /// Costruisce il provider dopo aver validato integralmente la configurazione.
    ///
    /// # Errors
    ///
    /// Propaga `InvalidConfiguration` senza contattare il driver Db2.
    pub fn new(config: Db2Config) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    #[must_use]
    pub const fn config(&self) -> &Db2Config {
        &self.config
    }
}

impl Provider for Db2Provider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Db2
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            let observed = probe(&self.config, secret, cancellation).await?;
            Ok(ConnectionInfo {
                provider: ProviderKind::Db2,
                server_version: observed.server_version,
                connection_identity: Some(observed.database),
            })
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            let observed = probe(&self.config, secret, cancellation).await?;
            db2_capabilities(observed.server_version, observed.spatial).published()
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
            read_operation(
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
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn TransactionScopeContract>> {
        Box::pin(async move {
            let transaction = crate::transaction::Db2Transaction::begin(
                &self.config,
                secret,
                options,
                budget,
                cancellation,
            )
            .await?;
            Ok(Box::new(transaction) as Box<dyn TransactionScopeContract>)
        })
    }
}

pub fn db2_capabilities(provider_version: String, spatial_available: bool) -> ProviderCapabilities {
    let functions_by_semantics = if spatial_available {
        BTreeMap::from([(
            SpatialSemantics::Geometry,
            vec![
                SpatialFunction::Srid,
                SpatialFunction::Dimensions,
                SpatialFunction::Intersects,
                SpatialFunction::Contains,
                SpatialFunction::Within,
            ],
        )])
    } else {
        BTreeMap::new()
    };
    let functions = intersect_spatial_functions(&functions_by_semantics);
    ProviderCapabilities {
        schema_version: 2,
        provider: ProviderKind::Db2,
        provider_version,
        extension_versions: BTreeMap::new(),
        reads: ReadCapabilities {
            streaming: true,
            server_cursor: false,
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
            bulk: false,
            array_binding: false,
            returning: false,
            rollback_on_failure: true,
        },
        transactions: TransactionCapabilities {
            single_transaction: true,
            savepoints: true,
            transactional_ddl: true,
            staged_swap: false,
            scope: TransactionScope::Transaction,
        },
        spatial: SpatialCapabilities {
            read_wkb: spatial_available,
            write_wkb: spatial_available,
            geometry: spatial_available,
            geography: false,
            spatial_index: false,
            mixed_geometry_types: spatial_available,
            dimensions: if spatial_available {
                vec![Dimensions::Xy, Dimensions::Xyz]
            } else {
                Vec::new()
            },
            functions,
            functions_by_semantics,
            requires_declared_crs: spatial_available,
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
