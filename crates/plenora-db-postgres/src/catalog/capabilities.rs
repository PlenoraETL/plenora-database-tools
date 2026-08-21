use crate::error::classify_error;
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::query::SpatialFunction;
use plenora_database_core::{ErrorPhase, Result};
use std::collections::BTreeMap;
use tokio_postgres::Client;

pub async fn capability_document(client: &Client) -> Result<ProviderCapabilities> {
    let row = client
        .query_one(
            r"
            SELECT
                current_setting('server_version'),
                COALESCE(
                  (SELECT extversion FROM pg_extension WHERE extname = 'postgis'),
                  ''
                )
            ",
            &[],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let server_version: String = row.get(0);
    let postgis_version: String = row.get(1);
    let spatial = !postgis_version.is_empty();
    let mut extensions = BTreeMap::new();
    if spatial {
        extensions.insert("postgis".to_owned(), postgis_version);
    }
    Ok(ProviderCapabilities {
        schema_version: 2,
        provider: ProviderKind::Postgres,
        provider_version: server_version,
        extension_versions: extensions,
        reads: ReadCapabilities {
            streaming: true,
            // Il data path usa RowStream con backpressure, ma non espone un
            // cursore server nominato o riprendibile.
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
            // Su PostgreSQL `TRUNCATE` e transazionale: le righe tornano
            // indietro insieme a tutto il resto se qualcosa fallisce dopo.
            truncate_insert: true,
            update: true,
            upsert: true,
            replace: true,
            delete_by_keys: true,
            bulk: true,
            array_binding: false,
            // WriteOutcome non trasporta ancora righe restituite.
            returning: false,
            rollback_on_failure: true,
        },
        transactions: TransactionCapabilities {
            single_transaction: true,
            // Il Provider esegue una transazione atomica, ma non espone
            // operazioni SAVEPOINT/ROLLBACK TO al chiamante.
            savepoints: false,
            transactional_ddl: true,
            staged_swap: true,
            scope: TransactionScope::Transaction,
        },
        spatial: SpatialCapabilities {
            read_wkb: spatial,
            write_wkb: spatial,
            geometry: spatial,
            geography: spatial,
            spatial_index: spatial,
            mixed_geometry_types: spatial,
            dimensions: if spatial {
                vec![
                    Dimensions::Xy,
                    Dimensions::Xyz,
                    Dimensions::Xym,
                    Dimensions::Xyzm,
                ]
            } else {
                Vec::new()
            },
            functions: if spatial {
                SpatialFunction::ALL.to_vec()
            } else {
                Vec::new()
            },
        },
        limits: ProviderLimits {
            max_identifier_bytes: Some(63),
            max_bind_parameters: Some(65_535),
            max_statement_bytes: None,
            max_batch_rows: None,
            max_payload_bytes: None,
        },
    })
}
