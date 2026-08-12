//! Golden test end-to-end su pool concorrenza, streaming stress e Drop
//! implicito della transazione (quarantena connessione).
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]

use plenora_database_core::provider::{ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;
use std::sync::Arc;
use std::time::Duration;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

// ============================================================================
//  H4.1 — Pool concorrenza: N tx parallele su pool size limitato
// ============================================================================

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn h4_pool_backpressure_supports_50_concurrent_tx_on_pool_8() {
    let provider = Arc::new(PostgresProvider::new(1_024).with_pool_size(8, 5_000));
    let cancel = CancellationToken::new();

    let n = 50_u32;
    let mut handles = Vec::with_capacity(n as usize);
    let start = std::time::Instant::now();
    for i in 0..n {
        let p = Arc::clone(&provider);
        let c = cancel.clone();
        handles.push(tokio::spawn(async move {
            let mut tx = p
                .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &c)
                .await
                .expect("begin");
            let rows = tx
                .query(&Statement::new(format!("SELECT {i}::INT + 100")), &c)
                .await
                .expect("query");
            Box::new(tx).commit(&c).await.expect("commit");
            match rows.first().and_then(|r| r.get_index(0)) {
                Some(ParameterValue::I32(v)) => *v,
                other => panic!("cell inattesa: {other:?}"),
            }
        }));
    }
    let mut sum: i64 = 0;
    for h in handles {
        sum += i64::from(h.await.expect("task"));
    }
    let elapsed = start.elapsed();
    // sum atteso = Σ(i+100) per i=0..50 = 100*50 + 49*50/2 = 5000 + 1225 = 6225
    assert_eq!(sum, 6225, "risultati task non consistenti");
    // Back-pressure sano: 50 tx su pool 8 devono completare in <5s.
    assert!(
        elapsed < Duration::from_secs(5),
        "pool 8 con 50 tx troppo lento: {elapsed:?}"
    );
}

// ============================================================================
//  H4.2 — Streaming stress: 100k righe in batch da 500, cancel a metà
// ============================================================================

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h4_streaming_100k_rows_reaches_completion_without_oom() {
    let provider = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    let stmt = Statement::new("SELECT gs::BIGINT FROM generate_series(1, 100000) gs");
    let mut stream = tx
        .query_stream(&stmt, 500, &cancel)
        .await
        .expect("query_stream");
    let mut total = 0_u64;
    let mut batches = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await.expect("next batch") {
        total += batch.len() as u64;
        batches += 1;
        // Sanity: nessun batch oltre il limite dichiarato.
        assert!(batch.len() <= 500, "batch oversize: {}", batch.len());
    }
    drop(stream);
    Box::new(tx).rollback(&cancel).await.expect("rollback");

    assert_eq!(total, 100_000, "conteggio righe totali sbagliato");
    // 100k / 500 = 200 batch (con eventuale batch parziale finale).
    assert!(
        (200..=201).contains(&batches),
        "batch count fuori range: {batches}"
    );
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h4_streaming_cancel_mid_flight_is_honored_promptly() {
    let provider = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    let stmt = Statement::new("SELECT gs::BIGINT FROM generate_series(1, 1000000) gs");
    let mut stream = tx
        .query_stream(&stmt, 1_000, &cancel)
        .await
        .expect("query_stream");

    // Consumo qualche batch, poi cancello.
    let _first = stream.next_batch(&cancel).await.expect("first batch");
    let _second = stream.next_batch(&cancel).await.expect("second batch");
    cancel.cancel();

    // Il prossimo next_batch deve restituire Cancelled o comunque
    // interrompersi rapidamente. Non deve né panic né hangare.
    let start = std::time::Instant::now();
    let outcome = stream.next_batch(&cancel).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "cancel non onorato in tempi ragionevoli: {elapsed:?}"
    );
    match outcome {
        Err(e) => assert_eq!(
            e.category,
            plenora_database_core::ErrorCategory::Cancelled,
            "categoria errore inattesa dopo cancel: {:?}",
            e.category
        ),
        Ok(None) => {
            // Alcuni driver chiudono lo stream con Ok(None) invece di errore;
            // è accettabile purché sia rapido.
        }
        Ok(Some(_)) => panic!("stream deve interrompersi dopo cancel"),
    }
    drop(stream);
    // Il rollback può fallire (la connessione può essere in stato broken):
    // best-effort, ci basta che non panichi.
    let _ = Box::new(tx).rollback(&CancellationToken::new()).await;
}

// ============================================================================
//  H4.3 — Drop implicito: transazione droppata senza commit/rollback deve
//  mettere la connessione in quarantena (non riusata dal pool come sana)
// ============================================================================

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h4_transaction_drop_without_commit_does_not_leak_state_to_next_tx() {
    // Pool size 1 → se la connessione non viene invalidata, la stessa
    // sessione tornerà a tx2, con eventuali SET LOCAL/temp-table residui.
    let provider = PostgresProvider::new(1_024).with_pool_size(1, 5_000);
    let cancel = CancellationToken::new();
    let s = secret();

    // Tx1: apre la tx e imposta una variabile di sessione tramite SET LOCAL
    // (visibile solo nella tx corrente). Poi la droppa senza commit/rollback.
    {
        let mut tx1 = provider
            .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin tx1");
        tx1.execute(
            &Statement::new("SET LOCAL application_name = 'plenora_drop_probe'"),
            &cancel,
        )
        .await
        .expect("set local");
        // Drop implicito senza commit/rollback:
        drop(tx1);
    }

    // Tx2: sulla stessa connessione riusata (pool size 1). Se la connessione
    // era stata correttamente invalidata, il pool ne crea una nuova e
    // application_name torna al default. Altrimenti vedremmo ancora
    // 'plenora_drop_probe'.
    let mut tx2 = provider
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin tx2");
    let rows = tx2
        .query(&Statement::new("SELECT current_setting('application_name')"), &cancel)
        .await
        .expect("query app_name");
    let app_name = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::String(s)) => s.clone(),
        other => panic!("cella inattesa: {other:?}"),
    };
    Box::new(tx2).rollback(&cancel).await.expect("rollback tx2");

    assert_ne!(
        app_name, "plenora_drop_probe",
        "state leak: application_name della tx droppata è filtrato in tx2"
    );
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h4_transaction_drop_does_not_leave_pool_permanently_broken() {
    // Dopo il drop, il pool deve poter servire N tx consecutive senza errori.
    let provider = PostgresProvider::new(1_024).with_pool_size(1, 5_000);
    let cancel = CancellationToken::new();
    let s = secret();

    // Drop implicito
    {
        let tx = provider
            .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin drop-me");
        drop(tx);
    }

    // 5 tx successive: tutte devono passare.
    for i in 0..5_u32 {
        let mut tx = provider
            .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .unwrap_or_else(|e| panic!("begin iter {i}: {e:?}"));
        let rows = tx
            .query(&Statement::new(format!("SELECT {i}::INT")), &cancel)
            .await
            .unwrap_or_else(|e| panic!("query iter {i}: {e:?}"));
        assert!(matches!(
            rows.first().and_then(|r| r.get_index(0)),
            Some(ParameterValue::I32(v)) if *v == i32::try_from(i).unwrap()
        ));
        Box::new(tx).commit(&cancel).await.unwrap_or_else(|e| panic!("commit iter {i}: {e:?}"));
    }
}
