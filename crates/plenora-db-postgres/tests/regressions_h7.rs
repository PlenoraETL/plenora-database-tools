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
//  Finding H7.1 — Provider::inspect::DatabaseListSchemas non filtra i
//  system schema (pg_catalog / pg_temp_* / pg_toast_* / information_schema)
// ============================================================================
//
// Contesto:
//   Il metodo `Provider::inspect(DatabaseListSchemas)` restituisce l'elenco
//   grezzo di pg_namespace. Il consumer (es. la CLI `inspect-schemas`)
//   deve filtrare esplicitamente i system schema.
//
// Impatto:
//   Un consumer che assume filtri built-in vedrà rumore. Il PFM può cadere
//   in questa trappola nella prima integration.
//
// Piano di fix (v0.2):
//   Cambiare `Provider::inspect(DatabaseListSchemas { source, include_system })`
//   con `include_system: bool` default `false`. Breaking del trait: quando
//   si fixa, la firma cambia e questo test diventa un test del NUOVO
//   comportamento.
//
// Se questo test fallisce:
//   Verificare se il fix è stato applicato. Se sì, il test va aggiornato
//   con la nuova asserzione (system schema NON presenti by default).

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn regression_h7_1_list_schemas_returns_raw_including_system() {
    let provider = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let out = provider
        .inspect(&secret(), &Operation::DatabaseListSchemas { source: None }, &cancel)
        .await
        .expect("inspect");
    let schemas = out.document["schemas"].as_array().expect("array");

    // Almeno un system schema DEVE essere presente (contract attuale).
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
        has_system,
        "REGRESSION H7.1: Provider::inspect(DatabaseListSchemas) non restituisce \
         più i system schema. Se questo è intenzionale (fix v0.2), aggiornare il \
         test. Schemas ricevuti: {schemas:?}"
    );
}

// ============================================================================
//  Finding H7.2 — BatchStream::next_batch() NON è cancel-aware sul trait
//  pubblico
// ============================================================================
//
// Contesto:
//   Il metodo `BatchStream::next_batch(&mut self)` del trait pubblico non
//   accetta un `CancellationToken`. Esiste `PostgresBatchStream::
//   next_batch_with_cancellation` ma è `pub(crate)`, quindi il consumer
//   Arrow bulk non può cancellare uno stream in flight tramite il trait.
//
// Impatto:
//   Chi consuma Arrow read via il trait (Python SDK via pyarrow C stream)
//   non può interrompere read grossi. Il token passato a `Provider::read`
//   viene ignorato dopo la creazione dello stream.
//
// Piano di fix (v0.2):
//   Cambiare `BatchStream::next_batch(&CancellationToken)` come firma —
//   breaking del trait. Alternativa non breaking: aggiungere metodo
//   `next_batch_with_cancellation` al trait con default impl che ignora
//   il token (backwards compatible per impl esistenti).
//
// Se questo test fallisce:
//   Verificare se il fix è stato applicato. Il fix cambia la firma o
//   aggiunge un metodo; questo test va aggiornato o eliminato.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn regression_h7_2_batch_stream_ignores_cancellation_after_start() {
    let provider = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();

    let op = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("public".into()),
            object: "spatial_ref_sys".into(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        filter: None,
    };
    let mut stream = provider
        .read(&secret(), &op, &ParameterBag::default(), &budget(), &cancel)
        .await
        .expect("read");

    // Consuma il primo batch (esiste sempre — spatial_ref_sys ha >>0 righe).
    let first = stream.next_batch().await.expect("first").expect("some");
    assert!(first.num_rows() > 0);

    // Cancella il token DOPO lo start del batch.
    cancel.cancel();

    // Il PROSSIMO next_batch NON deve tornare Cancelled (contract attuale):
    // il trait non consulta il token. Deve tornare Ok(Some) o Ok(None)
    // normalmente.
    let post = stream.next_batch().await;
    match post {
        Ok(_) => {
            // Comportamento attuale desiderato: continua a produrre.
        }
        Err(e) => panic!(
            "REGRESSION H7.2: BatchStream::next_batch è diventato cancel-aware, \
             ha ritornato errore dopo cancel: {:?} — {}. Se questo è intenzionale \
             (fix v0.2), aggiornare test e documentare migration path per il consumer.",
            e.category, e.message
        ),
    }
}
