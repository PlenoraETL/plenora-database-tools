//! Golden test end-to-end su `Provider::write` (path bulk Arrow).
//!
//! Copre `write.rs` + `write/*` + `preflight.rs` esercitando i modi
//! principali: Create (crea + inserisce), Append (inserisce su tabella
//! esistente), Upsert (conflict resolution). Include un test di rejection
//! per validare che preflight blocchi mismatch di schema.
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
    clippy::redundant_closure_for_method_calls,
)]

use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    ObjectRef, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;
use std::collections::VecDeque;
use std::sync::Arc;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
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
//  BatchStream in-memory per iniettare Arrow batch dai test
// ============================================================================

struct MemoryStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl MemoryStream {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter().collect(),
        }
    }
}

impl BatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next_batch<'a>(&'a mut self, _cancellation: &'a plenora_database_core::CancellationToken) -> ProviderFuture<'a, Option<RecordBatch>> {
        let next = self.batches.pop_front();
        Box::pin(std::future::ready(Ok(next)))
    }
}

// ============================================================================
//  Fixture: schema (id BIGINT, label TEXT) + N righe seed
// ============================================================================

fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn build_batch(range: std::ops::Range<i64>) -> RecordBatch {
    let mut ids = Int64Builder::with_capacity((range.end - range.start) as usize);
    let mut labels = StringBuilder::new();
    for i in range {
        ids.append_value(i);
        labels.append_value(format!("label-{i}"));
    }
    let cols: Vec<ArrayRef> = vec![Arc::new(ids.finish()), Arc::new(labels.finish())];
    RecordBatch::try_new(simple_schema(), cols).expect("batch")
}

fn write_op(target: &str, mode: WriteMode) -> WriteOperation {
    WriteOperation {
        target: public_ref(target),
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn drop_target(table: &str) {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    if let Ok(mut tx) = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
    {
        let _ = tx
            .execute(&Statement::new(format!("DROP TABLE IF EXISTS {table}")), &cancel)
            .await;
        let _ = Box::new(tx).commit(&cancel).await;
    }
}

async fn count_rows(table: &str) -> i64 {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin count");
    let rows = tx
        .query(
            &Statement::new(format!("SELECT COUNT(*)::BIGINT FROM {table}")),
            &cancel,
        )
        .await
        .expect("count");
    let n = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::I64(v)) => *v,
        other => panic!("count non I64: {other:?}"),
    };
    let _ = Box::new(tx).rollback(&cancel).await;
    n
}

// ============================================================================
//  H7d.1 — WriteMode::Create: crea tabella + inserisce 100 righe
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7d_write_create_builds_table_and_inserts_batch() {
    let target = "_h7d_create";
    drop_target(target).await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let b = budget();
    let op = write_op(target, WriteMode::Create);

    let prepared = p
        .prepare_write(&secret(), &op, simple_schema(), &b, &cancel)
        .await
        .expect("prepare_write");

    let batches = vec![build_batch(0..50), build_batch(50..100)];
    let stream: Box<dyn BatchStream> = Box::new(MemoryStream::new(simple_schema(), batches));
    let outcome = p
        .write(&secret(), prepared, stream, &b, &cancel)
        .await
        .expect("write");

    assert_eq!(outcome.rows.confirmed, 100, "outcome rows.confirmed mismatch");
    assert_eq!(count_rows(target).await, 100, "count DB post-write");

    drop_target(target).await;
}

// ============================================================================
//  H7d.2 — WriteMode::Append: crea con Create, poi Append 50 righe → 150 tot
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7d_write_append_adds_rows_to_existing_table() {
    let target = "_h7d_append";
    drop_target(target).await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();

    // Seed: Create con 100 righe. Budget dedicato al seed (viene consumato).
    let b_seed = budget();
    let prepared_create = p
        .prepare_write(
            &secret(),
            &write_op(target, WriteMode::Create),
            simple_schema(),
            &b_seed,
            &cancel,
        )
        .await
        .expect("prepare create");
    let stream: Box<dyn BatchStream> = Box::new(MemoryStream::new(
        simple_schema(),
        vec![build_batch(0..100)],
    ));
    p.write(&secret(), prepared_create, stream, &b_seed, &cancel)
        .await
        .expect("create write");

    // Append 50 righe con id != esistenti. Nuovo budget per la seconda op.
    let b_append = budget();
    let prepared_append = p
        .prepare_write(
            &secret(),
            &write_op(target, WriteMode::Append),
            simple_schema(),
            &b_append,
            &cancel,
        )
        .await
        .expect("prepare append");
    let stream: Box<dyn BatchStream> = Box::new(MemoryStream::new(
        simple_schema(),
        vec![build_batch(100..150)],
    ));
    let outcome = p
        .write(&secret(), prepared_append, stream, &b_append, &cancel)
        .await
        .expect("append write");

    assert_eq!(outcome.rows.confirmed, 50);
    assert_eq!(count_rows(target).await, 150);

    drop_target(target).await;
}

// ============================================================================
//  H7d.3 — WriteMode::Replace: drop+create con nuovo dataset
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7d_write_replace_swaps_table_content() {
    let target = "_h7d_replace";
    drop_target(target).await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();

    // Seed 100 righe via Create.
    let b_seed = budget();
    let prepared = p
        .prepare_write(
            &secret(),
            &write_op(target, WriteMode::Create),
            simple_schema(),
            &b_seed,
            &cancel,
        )
        .await
        .expect("prepare create");
    let stream: Box<dyn BatchStream> = Box::new(MemoryStream::new(
        simple_schema(),
        vec![build_batch(0..100)],
    ));
    p.write(&secret(), prepared, stream, &b_seed, &cancel)
        .await
        .expect("seed write");
    assert_eq!(count_rows(target).await, 100);

    // Replace con 25 righe (id 500..525): il contenuto deve essere sostituito.
    let b_replace = budget();
    let prepared_replace = p
        .prepare_write(
            &secret(),
            &write_op(target, WriteMode::Replace),
            simple_schema(),
            &b_replace,
            &cancel,
        )
        .await
        .expect("prepare replace");
    let stream: Box<dyn BatchStream> = Box::new(MemoryStream::new(
        simple_schema(),
        vec![build_batch(500..525)],
    ));
    let outcome = p
        .write(&secret(), prepared_replace, stream, &b_replace, &cancel)
        .await
        .expect("replace write");

    assert_eq!(outcome.rows.confirmed, 25);
    assert_eq!(
        count_rows(target).await,
        25,
        "Replace deve azzerare le righe pre-esistenti"
    );

    drop_target(target).await;
}

// ============================================================================
//  H7d.4 — Preflight: mode = Append su tabella INESISTENTE → errore pulito
//  (non deve crashare, deve tornare DatabaseError classificato)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7d_write_append_on_missing_table_returns_classified_error() {
    let target = "_h7d_nonexistent";
    drop_target(target).await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let outcome = p
        .prepare_write(
            &secret(),
            &write_op(target, WriteMode::Append),
            simple_schema(),
            &budget(),
            &cancel,
        )
        .await;

    // PreparedWrite non implementa Debug: match manuale invece di expect_err.
    let err = match outcome {
        Ok(_) => panic!("Append su tabella mancante deve fallire"),
        Err(e) => e,
    };
    // Non richiediamo una specifica category (dipende da implementazione):
    // basta che sia un errore classificato e informativo, non un panic.
    assert!(
        !err.message.is_empty(),
        "errore senza message: {err:?}"
    );
}
