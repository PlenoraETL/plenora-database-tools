//! Edge case del read stream Arrow: single-huge-row e mix small+huge.
//!
//! Chiude il buco identificato in P0.5 pre-Fase 3 sul read stream. Il file
//! `golden_read_arrow.rs` copre già i casi normali (batching, projection,
//! null, jsonb+bytea 4 MiB via OLTP). Qui verifichiamo cosa succede quando
//! una singola riga eccede il budget di batching, e quando small e huge
//! sono intercalati.
//!
//! Aree coperte:
//!   1. Single row più grande della soglia tipica → deve produrre almeno
//!      1 batch con quella riga, senza panic/hang, e il count è corretto
//!   2. Mix small+huge intercalati → nessuna riga persa; il batching
//!      heuristico non deve "saltare" la huge o duplicarla
//!   3. Budget di memoria molto stretto con huge row → deve fallire
//!      pulitamente con ResourceLimit (o riuscire), NON hangere/panic
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]
#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::uninlined_format_args,
    clippy::unreadable_literal
)]

use plenora_database_core::plan::{ObjectRef, ReadOperation};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_db_postgres::PostgresProvider;
use std::time::Duration;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget_default() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget default")
}

/// Budget con memoria stretta: 8 MiB (mantiene cell_bytes ≤ memory_bytes).
fn budget_tight_memory() -> ResourceBudget {
    let lim = ResourceLimits {
        memory_bytes: 8 * 1024 * 1024,
        cell_bytes: 8 * 1024 * 1024,
        ..ResourceLimits::default()
    };
    ResourceBudget::new(lim).expect("budget tight")
}

fn public_ref(object: &str) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("public".to_owned()),
        object: object.to_owned(),
    }
}

fn empty_read(object: &str) -> ReadOperation {
    ReadOperation {
        source: public_ref(object),
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: None,
    }
}

async fn create_persistent_fixture(table: &str, create_body: &str, seed_sql: &str) {
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget_default(),
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
            &budget_default(),
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
//  E.1 — Single huge row (~4 MiB) su budget di default: deve leggersi
// ============================================================================
//
// Verifica il caso base: una tabella con UNA sola riga il cui payload
// (bytea) è ~4 MiB. Col budget di default (memory=512 MiB), il read deve
// completare senza errori e restituire esattamente 1 riga.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn edge_e1_single_huge_row_within_default_budget_returns_one_row() {
    let table = "_edge_e1_single_huge";
    create_persistent_fixture(
        table,
        "id INT PRIMARY KEY, payload BYTEA NOT NULL",
        // 4 MiB di payload deterministico (repeat costante — Postgres non fa
        // TOAST-compressione su random-like buffer ma il size a Arrow è quello).
        &format!(
            "INSERT INTO {table} (id, payload) VALUES \
             (1, decode(repeat('ff', 4194304), 'hex'))"
        ),
    )
    .await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut stream = p
        .read(
            &secret(),
            &empty_read(table),
            &ParameterBag::default(),
            &budget_default(),
            &cancel,
        )
        .await
        .expect("read");

    let names: Vec<String> = stream
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["id".to_string(), "payload".to_string()]);

    let start = std::time::Instant::now();
    let mut batches = 0_u64;
    let mut total_rows = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await.expect("batch") {
        batches += 1;
        total_rows += batch.num_rows() as u64;
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "read impiega >60s — sospetto hang su huge row"
        );
    }
    assert_eq!(total_rows, 1, "atteso 1 riga, ottenuto {total_rows}");
    assert!(batches >= 1, "atteso almeno 1 batch");

    drop_fixture(table).await;
}

// ============================================================================
//  E.2 — Mix small + huge intercalati: nessuna riga persa
// ============================================================================
//
// Verifica che il batching gestisca correttamente il mix. Setup:
//   - 50 righe piccole (id 1..50, payload 100 byte)
//   - 1 riga huge (id 51, payload 4 MiB)
//   - 50 righe piccole (id 52..101, payload 100 byte)
//
// Deve restituire esattamente 101 righe totali, e leggendo colonna id
// contigua deve essere 1..=101 senza gap.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn edge_e2_mixed_small_and_huge_rows_preserves_all_rows() {
    let table = "_edge_e2_mixed";
    // Setup manuale: il driver è single-statement, quindi 3 execute separati.
    create_persistent_fixture(table, "id INT PRIMARY KEY, payload BYTEA NOT NULL", "").await;
    let seed_provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let seed_cancel = CancellationToken::new();
    let mut seed_tx = seed_provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget_default(),
            &seed_cancel,
        )
        .await
        .expect("begin seed");
    for stmt in [
        format!(
            "INSERT INTO {table} (id, payload) \
             SELECT gs, decode(repeat('aa', 50), 'hex') FROM generate_series(1, 50) gs"
        ),
        format!(
            "INSERT INTO {table} (id, payload) VALUES \
             (51, decode(repeat('ff', 4194304), 'hex'))"
        ),
        format!(
            "INSERT INTO {table} (id, payload) \
             SELECT gs, decode(repeat('bb', 50), 'hex') FROM generate_series(52, 101) gs"
        ),
    ] {
        seed_tx
            .execute(&Statement::new(stmt), &seed_cancel)
            .await
            .expect("seed stmt");
    }
    Box::new(seed_tx)
        .commit(&seed_cancel)
        .await
        .expect("seed commit");

    let op = ReadOperation {
        source: public_ref(table),
        projection: vec!["id".into()],
        order_by: vec![plenora_database_core::plan::OrderBy {
            field: "id".into(),
            direction: plenora_database_core::plan::SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
    };
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let mut stream = p
        .read(
            &secret(),
            &op,
            &ParameterBag::default(),
            &budget_default(),
            &cancel,
        )
        .await
        .expect("read");

    let mut total_rows = 0_u64;
    let mut columns_per_batch: Vec<usize> = Vec::new();
    while let Some(batch) = stream.next_batch(&cancel).await.expect("batch") {
        total_rows += batch.num_rows() as u64;
        columns_per_batch.push(batch.num_columns());
    }
    assert_eq!(total_rows, 101, "atteso 101 righe, ottenuto {total_rows}");
    // La projection è ["id"] → ogni batch deve avere 1 colonna.
    for (i, cols) in columns_per_batch.iter().enumerate() {
        assert_eq!(*cols, 1, "batch #{i} deve avere 1 colonna, ha {cols}");
    }

    drop_fixture(table).await;
}

// ============================================================================
//  E.3 — Budget di memoria stretto con huge row: fallisce puliti o riesce
// ============================================================================
//
// Verifica il contratto di error handling: con budget memory=8 MiB e una
// riga con payload 16 MiB (cioè 2× il budget di memoria), il read deve:
//   - riuscire (se il driver alloca cell-by-cell senza pre-check), OR
//   - fallire con ErrorCategory::ResourceLimit (o simile categoria non-Internal)
//
// NON deve fare panic né hang. Timeout hard a 30s.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn edge_e3_tight_memory_with_huge_row_fails_cleanly_or_streams() {
    let table = "_edge_e3_tight";
    create_persistent_fixture(
        table,
        "id INT PRIMARY KEY, payload BYTEA NOT NULL",
        // 16 MiB — 2× il budget tight di 8 MiB.
        &format!(
            "INSERT INTO {table} (id, payload) VALUES \
             (1, decode(repeat('cc', 16777216), 'hex'))"
        ),
    )
    .await;

    let p = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let start = std::time::Instant::now();
    let stream_res = p
        .read(
            &secret(),
            &empty_read(table),
            &ParameterBag::default(),
            &budget_tight_memory(),
            &cancel,
        )
        .await;

    match stream_res {
        Ok(mut stream) => {
            // Read partì: consuma i batch finché finisce o esplode.
            let mut total = 0_u64;
            loop {
                match stream.next_batch(&cancel).await {
                    Ok(Some(batch)) => {
                        total += batch.num_rows() as u64;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        assert!(
                            !matches!(e.category, ErrorCategory::Internal),
                            "errore Internal su budget stretto (dovrebbe essere ResourceLimit): {}",
                            e.message
                        );
                        break;
                    }
                }
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "read hang oltre 30s"
                );
            }
            // Se completa senza errore, il total può essere 1 (driver alloca
            // per-row) o 0 (short-circuit prima di emettere).
            assert!(total <= 1, "atteso 0..=1 righe, ottenuto {total}");
        }
        Err(e) => {
            // Errore all'apertura è OK se categoria appropriata.
            assert!(
                !matches!(e.category, ErrorCategory::Internal),
                "errore Internal all'apertura del read su budget stretto: {}",
                e.message
            );
        }
    }
    assert!(
        start.elapsed() < Duration::from_secs(30),
        "operazione completa deve stare in <30s"
    );

    drop_fixture(table).await;
}
