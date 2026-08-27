//! Contratti live per introspezione degli schemi e cancellazione degli stream.
//!
//! Sono `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]
#![allow(clippy::doc_markdown)]

use plenora_database_core::plan::{ObjectRef, Operation, ReadOperation};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;

const DSN: &str =
    "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

// `DatabaseListSchemas` espone soltanto schemi applicativi. Chi necessita dei
// namespace di sistema deve interrogare `pg_namespace` direttamente.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7_1_list_schemas_excludes_system_schemas_by_default() {
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let out = provider
        .inspect(
            &secret(),
            &Operation::DatabaseListSchemas { source: None },
            &cancel,
        )
        .await
        .expect("inspect");
    let schemas = out.document["schemas"].as_array().expect("array");
    let names: Vec<&str> = schemas.iter().filter_map(|s| s.as_str()).collect();

    // Nessun system schema deve comparire.
    for banned in ["pg_catalog", "information_schema", "pg_toast"] {
        assert!(
            !names.contains(&banned),
            "system schema '{banned}' non deve essere nella lista. Schemas: {names:?}"
        );
    }
    // Nessun pg_temp_* / pg_toast_temp_*.
    for name in &names {
        assert!(
            !name.starts_with("pg_temp_") && !name.starts_with("pg_toast_temp_"),
            "temp schema '{name}' non deve essere nella lista"
        );
    }
    // Almeno 'public' deve esserci (schema utente default).
    assert!(
        names.contains(&"public"),
        "'public' deve essere presente nella lista. Schemas: {names:?}"
    );
}

// Ogni `next_batch` deve osservare il token ricevuto, compresi i FETCH già in
// flight; la firma rende impossibile omettere accidentalmente la cancellazione.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7_2_batch_stream_honors_cancellation_after_start() {
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();

    let op = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("public".into()),
            object: "spatial_ref_sys".into(),
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let mut stream = provider
        .read(&secret(), &op, &ParameterBag::default(), &budget(), &cancel)
        .await
        .expect("read");

    // `spatial_ref_sys` garantisce un primo batch non vuoto nella fixture.
    let first = stream
        .next_batch(&cancel)
        .await
        .expect("first")
        .expect("some");
    assert!(first.num_rows() > 0);

    // La cancellazione avviene fra due richieste per provare il bordo stream.
    cancel.cancel();

    // Il batch successivo deve riflettere la cancellazione.
    let post = stream.next_batch(&cancel).await;
    match post {
        Ok(_) => panic!(
            "REGRESSION H7.2: BatchStream::next_batch NON è cancel-aware. \
             Un token cancellato dopo il primo batch deve produrre Err(Cancelled)."
        ),
        Err(e) => {
            assert!(
                matches!(e.category, plenora_database_core::ErrorCategory::Cancelled),
                "atteso categoria Cancelled, trovato {:?}: {}",
                e.category,
                e.message
            );
        }
    }
}
