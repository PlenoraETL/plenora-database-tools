//! Roundtrip di tipi meno esercitati ma richiesti dal PFM.
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]

use plenora_database_core::provider::{ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

// ============================================================================
//  H5.1 — INTERVAL: al momento binary wire → dovrebbe fallire in modo pulito
//  con Unsupported. Cast a text lo rende consumabile.
// ============================================================================

#[ignore]
#[tokio::test]
async fn h5_interval_direct_binary_returns_unsupported_or_maps_via_text() {
    let p = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    // Interval come TEXT (workaround documentato).
    let rows = tx
        .query(
            &Statement::new("SELECT (INTERVAL '2 days 3 hours')::TEXT"),
            &cancel,
        )
        .await
        .expect("interval text");
    let s = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::String(v)) => v.clone(),
        other => panic!("interval::text non decoded come string: {other:?}"),
    };
    // Postgres formatta come "2 days 03:00:00" (o simile a seconda della config).
    assert!(
        s.contains("day") && s.contains("03:00"),
        "interval::text inatteso: {s}"
    );

    let _ = Box::new(tx).rollback(&cancel).await;
}

// ============================================================================
//  H5.2 — JSONB grosso (~1 MiB): roundtrip senza truncation
// ============================================================================

#[ignore]
#[tokio::test]
async fn h5_large_jsonb_roundtrips_bytewise_intact() {
    let p = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    // Costruisce un JSONB con un array di 10k stringhe → circa 1 MiB.
    let big: serde_json::Value = serde_json::json!({
        "items": (0..10_000)
            .map(|i| format!("string-payload-{i:06}-padding-xxxxx"))
            .collect::<Vec<_>>()
    });
    let param = ParameterValue::Json(big.clone());
    let rows = tx
        .query(
            &Statement::new("SELECT $1::JSONB")
                .with_params(vec![param]),
            &cancel,
        )
        .await
        .expect("query jsonb");
    let out = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::Json(v)) => v.clone(),
        other => panic!("jsonb non decoded come Json: {other:?}"),
    };
    // Serializzati identicamente byte-per-byte (dopo canonicalizzazione JSON).
    let orig_bytes = serde_json::to_vec(&big).expect("ser orig");
    let out_bytes = serde_json::to_vec(&out).expect("ser out");
    assert_eq!(
        orig_bytes, out_bytes,
        "JSONB grosso non è integro dopo roundtrip"
    );

    let _ = Box::new(tx).rollback(&cancel).await;
}

// ============================================================================
//  H5.3 — BYTEA grosso (~4 MiB): roundtrip senza truncation
// ============================================================================

#[ignore]
#[tokio::test]
async fn h5_large_bytea_roundtrips_bytewise_intact() {
    let p = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    // 4 MiB deterministici (pattern PRNG-like via LCG semplice).
    let mut buf = Vec::with_capacity(4 * 1024 * 1024);
    let mut state = 0x1234_5678_u32;
    for _ in 0..(4 * 1024 * 1024) {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        buf.push((state >> 16) as u8);
    }
    let orig = buf.clone();

    let rows = tx
        .query(
            &Statement::new("SELECT $1::BYTEA")
                .with_params(vec![ParameterValue::Bytes(buf)]),
            &cancel,
        )
        .await
        .expect("query bytea");
    let out = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::Bytes(v)) => v.clone(),
        other => panic!("bytea non decoded come Bytes: {other:?}"),
    };
    assert_eq!(out.len(), orig.len(), "bytea size mismatch");
    assert_eq!(out, orig, "bytea grosso non identico dopo roundtrip");

    let _ = Box::new(tx).rollback(&cancel).await;
}

// ============================================================================
//  H5.4 — TIMETZ come TEXT: verifica documentata (path OLTP non ha decoder
//  binario per TIMETZ)
// ============================================================================

#[ignore]
#[tokio::test]
async fn h5_timetz_via_text_cast_is_readable() {
    let p = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    let rows = tx
        .query(
            &Statement::new("SELECT ('10:30:45+02'::TIMETZ)::TEXT"),
            &cancel,
        )
        .await
        .expect("query timetz");
    let s = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::String(v)) => v.clone(),
        other => panic!("timetz::text non String: {other:?}"),
    };
    // Postgres normalizza in "10:30:45+02" o "10:30:45+02:00".
    assert!(
        s.starts_with("10:30:45"),
        "timetz text format inatteso: {s}"
    );

    let _ = Box::new(tx).rollback(&cancel).await;
}

// ============================================================================
//  H5.5 — Null tipizzato: encode Null con type_name deve produrre NULL della
//  colonna target
// ============================================================================

#[ignore]
#[tokio::test]
async fn h5_typed_null_binds_correctly_for_each_hint() {
    let p = PostgresProvider::new(1_024);
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    for hint in ["bool", "int4", "int8", "text", "uuid", "jsonb"] {
        let rows = tx
            .query(
                &Statement::new(format!(
                    "SELECT $1::{hint} IS NULL, pg_typeof($1::{hint})::TEXT"
                ))
                .with_params(vec![ParameterValue::Null {
                    type_name: hint.into(),
                }]),
                &cancel,
            )
            .await
            .unwrap_or_else(|e| panic!("query null::{hint}: {e:?}"));
        let is_null = matches!(
            rows.first().and_then(|r| r.get_index(0)),
            Some(ParameterValue::Bool(true))
        );
        assert!(is_null, "null::{hint} deve essere NULL");
        let type_name = match rows.first().and_then(|r| r.get_index(1)) {
            Some(ParameterValue::String(s)) => s.clone(),
            other => panic!("pg_typeof non String: {other:?}"),
        };
        assert!(
            !type_name.is_empty(),
            "pg_typeof restituito vuoto per hint={hint}"
        );
    }

    let _ = Box::new(tx).rollback(&cancel).await;
}
