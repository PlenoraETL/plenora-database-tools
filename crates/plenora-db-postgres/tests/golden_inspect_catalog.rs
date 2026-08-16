//! Golden test end-to-end su `Provider::inspect` (catalog.rs +
//! catalog/schema.rs), che sono coperti dalla CLI `postgres-describe` ma
//! privi di test strutturali.
//!
//! Verifica tutte e 4 le operations supportate:
//!   - DatabaseListCatalogs
//!   - DatabaseListSchemas
//!   - DatabaseListObjects { schema }
//!   - DatabaseDescribeObject { schema, object }
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]
#![allow(
    clippy::approx_constant,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::doc_markdown,
    clippy::unreadable_literal,
    clippy::single_match_else,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::uninlined_format_args,
    clippy::match_same_arms,
    clippy::manual_let_else,
    clippy::redundant_closure_for_method_calls
)]

use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn public_ref(object: &str) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("public".to_owned()),
        object: object.to_owned(),
        layer_id: None,
    }
}

// ============================================================================
//  H7c.1 — list_catalogs
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7c_list_catalogs_reports_current_database() {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let out = p
        .inspect(&secret(), &Operation::DatabaseListCatalogs, &cancel)
        .await
        .expect("inspect");
    assert_eq!(out.operation, "database.list_catalogs");
    let catalogs = out.document["catalogs"].as_array().expect("catalogs array");
    assert!(
        catalogs
            .iter()
            .any(|c| c["name"] == "dataflow_test" || c.as_str() == Some("dataflow_test")),
        "dataflow_test non trovato: {catalogs:?}"
    );
}

// ============================================================================
//  H7c.2 — list_schemas
// ============================================================================

/// v0.2 (fix H7.1): `Provider::inspect::DatabaseListSchemas` filtra i system
/// schema Postgres (pg_catalog / information_schema / pg_toast / pg_temp_* /
/// pg_toast_temp_*). Il consumer che ha bisogno anche dei system schema deve
/// interrogare pg_namespace direttamente.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7c_list_schemas_excludes_system_schemas() {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let out = p
        .inspect(
            &secret(),
            &Operation::DatabaseListSchemas { source: None },
            &cancel,
        )
        .await
        .expect("inspect");
    assert_eq!(out.operation, "database.list_schemas");
    let schemas = out.document["schemas"].as_array().expect("schemas array");
    // public deve esserci (schema utente default).
    assert!(
        schemas
            .iter()
            .any(|s| s["name"] == "public" || s.as_str() == Some("public")),
        "public assente: {schemas:?}"
    );
    // Nessun system schema.
    let has_system = schemas.iter().any(|s| {
        matches!(
            s.as_str(),
            Some("pg_catalog" | "information_schema" | "pg_toast")
        ) || matches!(
            s["name"].as_str(),
            Some("pg_catalog" | "information_schema" | "pg_toast")
        )
    });
    assert!(
        !has_system,
        "system schema non deve comparire dopo fix H7.1 v0.2: {schemas:?}"
    );
}

// ============================================================================
//  H7c.3 — list_objects per schema public (deve includere spatial_ref_sys
//  se PostGIS è installato)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7c_list_objects_returns_relations_of_the_schema() {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let out = p
        .inspect(
            &secret(),
            &Operation::DatabaseListObjects {
                source: Some(ObjectRef {
                    catalog: None,
                    schema: Some("public".to_owned()),
                    object: String::new(),
                    layer_id: None,
                }),
            },
            &cancel,
        )
        .await
        .expect("inspect");
    assert_eq!(out.operation, "database.list_objects");
    assert_eq!(out.document["schema"], "public");
    let objects = out.document["objects"].as_array().expect("objects array");
    assert!(!objects.is_empty(), "public deve avere almeno un oggetto");
}

// ============================================================================
//  H7c.4 — describe_object su spatial_ref_sys (tabella nota PostGIS)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7c_describe_object_returns_columns_and_schema_token() {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let out = p
        .inspect(
            &secret(),
            &Operation::DatabaseDescribeObject {
                source: public_ref("spatial_ref_sys"),
            },
            &cancel,
        )
        .await
        .expect("inspect");
    assert_eq!(out.operation, "database.describe_object");
    assert!(
        out.document["schema_token"].is_string() || out.document["schema_token"].is_object(),
        "schema_token mancante"
    );
    let columns = out.document["columns"].as_array().expect("columns array");
    // spatial_ref_sys ha almeno: srid, auth_name, auth_srid, srtext, proj4text
    let names: Vec<String> = columns
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect();
    assert!(names.contains(&"srid".to_owned()));
    assert!(names.contains(&"srtext".to_owned()));
}

// ============================================================================
//  H7c.5 — describe_object su tabella temp costruita al volo con vari tipi
//  (verifica catalog/schema.rs decoder tipi)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7c_describe_object_handles_multiple_column_types() {
    use plenora_database_core::provider::Provider as _;
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use plenora_database_core::transaction::{Statement, TransactionOptions};

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    // Crea tabella persistente (describe non funziona su TEMP: la connessione
    // di inspect è distinta da quella di setup).
    let mut tx_setup = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
        .await
        .expect("begin setup");
    tx_setup
        .execute(
            &Statement::new(
                "CREATE TABLE IF NOT EXISTS _h7c_types ( \
                 id BIGSERIAL PRIMARY KEY, \
                 label TEXT NOT NULL, \
                 amount NUMERIC(12,2), \
                 flag BOOL DEFAULT true, \
                 ts TIMESTAMPTZ NOT NULL DEFAULT now(), \
                 payload JSONB, \
                 tags TEXT[])",
            ),
            &cancel,
        )
        .await
        .expect("create");
    Box::new(tx_setup)
        .commit(&cancel)
        .await
        .expect("commit setup");

    let out = p
        .inspect(
            &secret(),
            &Operation::DatabaseDescribeObject {
                source: public_ref("_h7c_types"),
            },
            &cancel,
        )
        .await
        .expect("inspect");

    let columns = out.document["columns"].as_array().expect("cols");
    let names: Vec<String> = columns
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect();
    for expected in ["id", "label", "amount", "flag", "ts", "payload", "tags"] {
        assert!(
            names.contains(&expected.to_owned()),
            "colonna {expected} mancante: {names:?}"
        );
    }

    // Cleanup
    let mut tx_cleanup = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
        .await
        .expect("begin cleanup");
    let _ = tx_cleanup
        .execute(&Statement::new("DROP TABLE IF EXISTS _h7c_types"), &cancel)
        .await;
    let _ = Box::new(tx_cleanup).commit(&cancel).await;
}
