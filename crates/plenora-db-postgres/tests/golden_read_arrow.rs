//! Golden test end-to-end su `Provider::read` (Arrow BatchStream).
//!
//! Copre i moduli con coverage strutturale bassa ma vivi via CLI
//! (`postgres-read-summary`, `postgres-read-ipc`):
//!   - `arrow.rs` (schema Arrow + range fields)
//!   - `types.rs` (Postgres → Arrow type mapping)
//!   - `query_execution.rs` (pipeline read)
//!   - `query_plan.rs` (query builder da ReadOperation)
//!   - `read_stream.rs` (BatchStream + cancel + memory)
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

use plenora_database_core::plan::{ObjectRef, ProviderKind, ReadOperation};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};
use plenora_db_postgres::PostgresProvider;

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
    }
}

fn empty_read(source: ObjectRef) -> ReadOperation {
    ReadOperation {
        source,
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    }
}

async fn create_persistent_fixture(table: &str, create_body: &str, seed_sql: &str) {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin fixture");
    tx.execute(
        &Statement::new(format!("DROP TABLE IF EXISTS {table}")),
        &cancel,
    )
    .await
    .expect("drop");
    tx.execute(
        &Statement::new(format!("CREATE TABLE {table} ({create_body})")),
        &cancel,
    )
    .await
    .expect("create");
    if !seed_sql.is_empty() {
        tx.execute(&Statement::new(seed_sql.to_owned()), &cancel)
            .await
            .expect("seed");
    }
    Box::new(tx).commit(&cancel).await.expect("commit fixture");
}

async fn drop_fixture(table: &str) {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    if let Ok(mut tx) = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
    {
        let _ = tx
            .execute(
                &Statement::new(format!("DROP TABLE IF EXISTS {table}")),
                &cancel,
            )
            .await;
        let _ = Box::new(tx).commit(&cancel).await;
    }
}

// ============================================================================
//  H7b.1 — Read su spatial_ref_sys (nota tabella PostGIS): schema Arrow ok,
//  almeno un batch, rows > 0.
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7b_read_spatial_ref_sys_produces_valid_arrow_stream() {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let op = empty_read(public_ref("spatial_ref_sys"));
    let mut stream = p
        .read(&secret(), &op, &ParameterBag::default(), &budget(), &cancel)
        .await
        .expect("read");

    let schema = stream.schema();
    let fields: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    for expected in ["srid", "auth_name", "srtext"] {
        assert!(
            fields.contains(&expected.to_owned()),
            "colonna {expected} mancante: {fields:?}"
        );
    }

    let mut batches = 0_u64;
    let mut rows = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await.expect("next batch") {
        batches += 1;
        rows += batch.num_rows() as u64;
    }
    assert!(batches >= 1, "almeno un batch atteso");
    assert!(
        rows >= 100,
        "spatial_ref_sys ha >>100 righe; ottenute: {rows}"
    );
}

// ============================================================================
//  H7b.2 — Read con tipi eterogenei (int, text, numeric, bool, timestamp,
//  jsonb): schema Arrow contiene tutte le colonne, il primo batch è
//  materializzato senza errori di mapping.
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7b_read_heterogeneous_types_yields_expected_arrow_schema() {
    let table = "_h7b_hetero";
    create_persistent_fixture(
        table,
        "id BIGSERIAL PRIMARY KEY, \
         label TEXT NOT NULL, \
         amount NUMERIC(12,2), \
         flag BOOL DEFAULT true, \
         ts TIMESTAMPTZ NOT NULL DEFAULT now(), \
         payload JSONB",
        &format!(
            "INSERT INTO {table} (label, amount, payload) SELECT \
             'row-' || gs::TEXT, (gs * 1.25)::NUMERIC(12,2), \
             json_build_object('n', gs)::JSONB \
             FROM generate_series(1, 25) gs"
        ),
    )
    .await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut stream = p
        .read(
            &secret(),
            &empty_read(public_ref(table)),
            &ParameterBag::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("read");
    let schema = stream.schema();
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    for expected in ["id", "label", "amount", "flag", "ts", "payload"] {
        assert!(
            names.contains(&expected.to_owned()),
            "colonna {expected} assente in Arrow schema: {names:?}"
        );
    }

    let mut total_rows = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await.expect("next batch") {
        total_rows += batch.num_rows() as u64;
    }
    assert_eq!(total_rows, 25, "atteso 25 righe, ottenuto {total_rows}");

    drop_fixture(table).await;
}

// ============================================================================
//  H7b.3 — Projection: SELECT solo di 2 colonne su 6, schema Arrow ne
//  contiene solo 2, i batch idem.
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7b_read_projection_reduces_schema_columns() {
    let table = "_h7b_proj";
    create_persistent_fixture(
        table,
        "id INT PRIMARY KEY, name TEXT NOT NULL, extra TEXT",
        &format!("INSERT INTO {table} SELECT gs, 'n' || gs, 'ext' FROM generate_series(1, 5) gs"),
    )
    .await;

    let op = ReadOperation {
        source: public_ref(table),
        projection: vec!["id".into(), "name".into()],
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut stream = p
        .read(&secret(), &op, &ParameterBag::default(), &budget(), &cancel)
        .await
        .expect("read");
    let names: Vec<String> = stream
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["id".to_owned(), "name".to_owned()]);

    let mut total = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await.expect("batch") {
        assert_eq!(batch.num_columns(), 2, "batch deve avere 2 colonne");
        total += batch.num_rows() as u64;
    }
    assert_eq!(total, 5);

    drop_fixture(table).await;
}

// ============================================================================
//  H7b.4 — Row limit onorato dal query builder.
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7b_read_row_limit_is_honored_by_query_planner() {
    let table = "_h7b_limit";
    create_persistent_fixture(
        table,
        "id INT PRIMARY KEY",
        &format!("INSERT INTO {table} SELECT gs FROM generate_series(1, 1000) gs"),
    )
    .await;

    let op = ReadOperation {
        source: public_ref(table),
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: Some(42),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut stream = p
        .read(&secret(), &op, &ParameterBag::default(), &budget(), &cancel)
        .await
        .expect("read");
    let mut total = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await.expect("batch") {
        total += batch.num_rows() as u64;
    }
    assert_eq!(total, 42, "row_limit=42 deve troncare a 42 righe");

    drop_fixture(table).await;
}

// ============================================================================
//  H7b.5 — NULL misti in colonne nullable: nessun errore di mapping, i
//  batch Arrow devono avere il null bit set correttamente.
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7b_read_handles_null_values_across_columns() {
    let table = "_h7b_nulls";
    create_persistent_fixture(
        table,
        "id INT PRIMARY KEY, label TEXT, amount NUMERIC(10,2), flag BOOL",
        &format!(
            "INSERT INTO {table} VALUES \
             (1, 'a', 10.0, true), \
             (2, NULL, 20.0, NULL), \
             (3, 'c', NULL, false), \
             (4, NULL, NULL, NULL)"
        ),
    )
    .await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut stream = p
        .read(
            &secret(),
            &empty_read(public_ref(table)),
            &ParameterBag::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("read");

    let mut total_rows = 0_u64;
    let mut null_labels = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await.expect("batch") {
        total_rows += batch.num_rows() as u64;
        // Colonna 'label' → conta null.
        let label_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "label")
            .expect("label col");
        let arr = batch.column(label_idx);
        for i in 0..arr.len() {
            if arr.is_null(i) {
                null_labels += 1;
            }
        }
    }
    assert_eq!(total_rows, 4);
    assert_eq!(null_labels, 2, "attese 2 label null");

    drop_fixture(table).await;
}

// Dopo la cancellazione il batch successivo deve fallire con l'envelope
// operativo completo. `RemoteEffect::None` distingue una lettura interrotta
// da una scrittura il cui effetto remoto potrebbe essere ignoto.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn h7b_read_stream_is_cancel_aware() {
    let table = "_h7b_bigstream";
    create_persistent_fixture(
        table,
        "id INT PRIMARY KEY",
        &format!("INSERT INTO {table} SELECT gs FROM generate_series(1, 20000) gs"),
    )
    .await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut stream = p
        .read(
            &secret(),
            &empty_read(public_ref(table)),
            &ParameterBag::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("read");
    let first = stream.next_batch(&cancel).await.expect("batch1");
    assert!(
        first.is_some(),
        "il primo batch arriva prima della cancellazione"
    );
    cancel.cancel();

    let error = stream
        .next_batch(&cancel)
        .await
        .expect_err("dopo la cancellazione il batch successivo e un rifiuto");
    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert_eq!(error.phase, ErrorPhase::Read);
    // Una lettura non lascia residui: chi la interrompe non ha nulla da
    // ripulire, e dichiararlo diversamente manderebbe a cercare uno stato che
    // non esiste.
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(error.provider, Some(ProviderKind::Postgres));

    drop_fixture(table).await;
}
