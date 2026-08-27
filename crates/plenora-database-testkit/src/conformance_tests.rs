use super::*;
use plenora_database_core::capabilities::{
    ProviderLimits, ReadCapabilities, SpatialCapabilities, TransactionCapabilities,
    TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::query::SpatialFunction;
use serde_json::json;
use std::collections::BTreeMap;

fn valid_capabilities() -> ProviderCapabilities {
    ProviderCapabilities {
        schema_version: 2,
        provider: ProviderKind::Postgres,
        provider_version: "16.9".to_owned(),
        extension_versions: BTreeMap::from([("postgis".to_owned(), "3.5.2".to_owned())]),
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
            truncate_insert: true,
            update: true,
            upsert: true,
            replace: true,
            delete_by_keys: true,
            bulk: true,
            array_binding: false,
            returning: true,
            rollback_on_failure: true,
        },
        transactions: TransactionCapabilities {
            single_transaction: true,
            savepoints: true,
            transactional_ddl: true,
            staged_swap: true,
            scope: TransactionScope::Transaction,
        },
        spatial: SpatialCapabilities {
            functions_by_semantics: std::collections::BTreeMap::new(),
            read_wkb: true,
            write_wkb: true,
            geometry: true,
            geography: true,
            spatial_index: true,
            mixed_geometry_types: true,
            dimensions: vec![Dimensions::Xy, Dimensions::Xyz],
            functions: vec![SpatialFunction::Intersects],
            requires_declared_crs: false,
        },
        limits: ProviderLimits {
            max_identifier_bytes: Some(63),
            max_bind_parameters: Some(65_535),
            max_statement_bytes: None,
            max_batch_rows: None,
            max_payload_bytes: None,
        },
    }
}

#[test]
fn coherent_capabilities_are_accepted() {
    validate_capabilities(ProviderKind::Postgres, &valid_capabilities())
        .expect("capability coerenti");
}

#[test]
fn spatial_claim_without_semantics_is_rejected() {
    let mut capabilities = valid_capabilities();
    capabilities.spatial.geometry = false;
    capabilities.spatial.geography = false;
    assert!(validate_capabilities(ProviderKind::Postgres, &capabilities).is_err());
}

#[test]
fn duplicate_spatial_entries_are_rejected() {
    let mut capabilities = valid_capabilities();
    capabilities.spatial.dimensions.push(Dimensions::Xy);
    assert!(validate_capabilities(ProviderKind::Postgres, &capabilities).is_err());
}

#[test]
fn transaction_claims_are_fail_closed() {
    let mut capabilities = valid_capabilities();
    capabilities.transactions.single_transaction = false;
    assert!(validate_capabilities(ProviderKind::Postgres, &capabilities).is_err());
}

#[test]
fn inspection_requires_canonical_operation_and_object() {
    let inspection = Inspection {
        operation: "database.list_schemas".to_owned(),
        document: json!({"schemas": []}),
    };
    validate_inspection(
        &Operation::DatabaseListSchemas { source: None },
        &inspection,
    )
    .expect("inspection coerente");

    let invalid = Inspection {
        operation: "database.list_catalogs".to_owned(),
        document: json!([]),
    };
    assert!(validate_inspection(&Operation::DatabaseListCatalogs, &invalid).is_err());
}
