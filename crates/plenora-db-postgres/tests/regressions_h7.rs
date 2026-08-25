//! Regression tests per i finding scoperti durante l'hardening H7
//! (2026-08-12). Documentano il **comportamento attuale** e falliranno se
//! quel comportamento cambia (voluto o meno) — così il consumer PFM viene
//! avvertito di un breaking change prima di aggiornare la libreria.
//!
//! Sono `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.
//!
//! **Piano di fix**: entrambi i finding sono targetati per v0.2 (breaking
//! change del trait). Quando si fixano, aggiornare l'asserzione qui e
//! documentare la migration path per il consumer.

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

// ============================================================================
//  H7.1 — Provider::inspect::DatabaseListSchemas filtra i system schema
// ============================================================================
//
// Contesto (v0.2, fix applicato):
//   Il metodo `Provider::inspect(DatabaseListSchemas)` filtra pg_catalog,
//   information_schema, pg_toast, pg_temp_* e pg_toast_temp_*. Il consumer
//   che ha bisogno anche dei system schemas deve interrogare pg_namespace
//   direttamente (o usare la CLI con un'opzione futura --include-system,
//   quando disponibile).
//
// Storia:
//   In v0.1 il metodo restituiva l'elenco grezzo. La CLI `inspect-schemas`
//   filtrava client-side. Con v0.2 il filtro è built-in: se un consumer
//   dipendeva dal comportamento precedente, deve aggiornarsi.
//
// Se questo test fallisce:
//   Il filtro è stato rimosso o modificato: valutare l'impatto downstream.

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

// ============================================================================
//  H7.2 — BatchStream::next_batch è cancel-aware
// ============================================================================
//
// Contesto (v0.2, fix applicato):
//   Il metodo `BatchStream::next_batch(&mut self, &CancellationToken)`
//   accetta ora obbligatoriamente un `CancellationToken`. Le implementazioni
//   (postgres/mysql/sqlserver) usano `tokio::select!` fra la fetch e
//   `cancellation.cancelled()`, quindi un consumer può interrompere read
//   in flight — Python SDK compreso.
//
// Storia:
//   In v0.1 il trait aveva `next_batch(&mut self)` senza token e un
//   `next_batch_with_cancellation(&CancellationToken)` come default impl.
//   La firma inconsistente permetteva ai consumer di dimenticare la
//   cancellazione. v0.2 unifica il metodo.
//
// Se questo test fallisce:
//   Il trait o l'impl postgres non è più cancel-aware. Investigare
//   PostgresBatchStream::next_batch: deve consumare il token via select!.

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
    };
    let mut stream = provider
        .read(&secret(), &op, &ParameterBag::default(), &budget(), &cancel)
        .await
        .expect("read");

    // Consuma il primo batch (esiste sempre — spatial_ref_sys ha >>0 righe).
    let first = stream
        .next_batch(&cancel)
        .await
        .expect("first")
        .expect("some");
    assert!(first.num_rows() > 0);

    // Cancella il token DOPO lo start del batch.
    cancel.cancel();

    // Il PROSSIMO next_batch DEVE riflettere la cancellazione con
    // Err(Cancelled) — il trait ora è cancel-aware.
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
