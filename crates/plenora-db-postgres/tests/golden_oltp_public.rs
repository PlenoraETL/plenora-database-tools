//! Golden test end-to-end sull'API pubblica OLTP contro Postgres reale.
//!
//! Coprono i percorsi che il consumer PFM userà davvero:
//! begin+exec+commit, savepoint annidati, optimistic conflict cross-tx,
//! session context leak-free tra tx sulla stessa connessione del pool,
//! stream + drop implicito (quarantena), execute_scalar_* di tutti i tipi
//! scalari, execute_portable + returning + returning_one.
//!
//! Bootstrap: richiedono un Postgres raggiungibile all'hostname
//! `dataflow-postgres` (compose network `plenora-postgres_default`); i test
//! sono `#[ignore]` per default, esegui con:
//!
//! ```text
//! docker run --rm --network plenora-postgres_default -v ... rust:1.98 \
//!   cargo test --test golden_oltp_public -- --ignored --nocapture
//! ```

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

use plenora_database_core::facade::{
    execute_portable, execute_portable_returning, execute_portable_returning_one,
    execute_scalar_bool, execute_scalar_bytes, execute_scalar_date, execute_scalar_f64,
    execute_scalar_i32, execute_scalar_i64, execute_scalar_json, execute_scalar_string,
    execute_scalar_timestamp, execute_scalar_timestamptz, execute_scalar_uuid, query_one,
    query_optional,
};
use plenora_database_core::portable::{
    eq as p_eq, select as p_select, DeleteStatement, Expression, InsertStatement,
    PortableStatement, TableRef, UpdateStatement,
};
use plenora_database_core::provider::{ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::session_context::{SessionContext, SessionEntry, SessionValue};
use plenora_database_core::transaction::{ConditionalUpdate, Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_db_postgres::PostgresProvider;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn provider() -> PostgresProvider {
    PostgresProvider::insecure_local_with_batch_rows(1_024)
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

// ============================================================================
//  Golden 1 — happy path: begin + exec + query + commit
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn golden_begin_exec_query_commit_roundtrip() {
    let p = provider();
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    let affected = tx
        .execute(&Statement::new("SELECT 1"), &cancel)
        .await
        .expect("exec");
    assert_eq!(affected, 1, "SELECT 1 riporta 1 riga come affected");

    let rows = tx
        .query(&Statement::new("SELECT 42::INT"), &cancel)
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].get_index(0),
        Some(ParameterValue::I32(42))
    ));

    let outcome = Box::new(tx).commit(&cancel).await.expect("commit");
    assert!(matches!(
        outcome,
        plenora_database_core::transaction::CommitOutcome::Committed
    ));
}

// ============================================================================
//  Golden 2 — savepoint annidati: create + rollback_to + release + commit
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn golden_savepoints_nested_rollback_release_commit() {
    let p = provider();
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    tx.execute(
        &Statement::new("CREATE TEMP TABLE _golden_sp (id INT PRIMARY KEY) ON COMMIT DROP"),
        &cancel,
    )
    .await
    .expect("temp");

    tx.savepoint("outer", &cancel).await.expect("sp outer");
    tx.execute(
        &Statement::new("INSERT INTO _golden_sp VALUES (1)"),
        &cancel,
    )
    .await
    .expect("insert 1");

    tx.savepoint("inner", &cancel).await.expect("sp inner");
    tx.execute(
        &Statement::new("INSERT INTO _golden_sp VALUES (2)"),
        &cancel,
    )
    .await
    .expect("insert 2");

    tx.rollback_to_savepoint("inner", &cancel)
        .await
        .expect("rollback to inner");

    // Solo id=1 deve essere sopravvissuto.
    let rows = tx
        .query(
            &Statement::new("SELECT id FROM _golden_sp ORDER BY id"),
            &cancel,
        )
        .await
        .expect("select");
    let ids: Vec<i32> = rows
        .iter()
        .filter_map(|r| match r.get_index(0) {
            Some(ParameterValue::I32(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        vec![1],
        "rollback_to_savepoint(inner) deve preservare id=1"
    );

    tx.release_savepoint("outer", &cancel)
        .await
        .expect("release outer");
    let outcome = Box::new(tx).commit(&cancel).await.expect("commit");
    assert!(matches!(
        outcome,
        plenora_database_core::transaction::CommitOutcome::Committed
    ));
}

// ============================================================================
//  Golden 3 — optimistic conflict cross-tx: winner commits, loser gets
//  ConcurrentModification
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn golden_optimistic_conflict_cross_transactions() {
    let p = provider();
    let cancel = CancellationToken::new();
    let s = secret();

    // Setup: crea tabella persistente con (id=1, v=1) e committa.
    let mut tx_setup = p
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin setup");
    tx_setup
        .execute(
            &Statement::new(
                "CREATE TABLE IF NOT EXISTS _golden_oc (id INT PRIMARY KEY, v INT NOT NULL)",
            ),
            &cancel,
        )
        .await
        .expect("create");
    tx_setup
        .execute(
            &Statement::new(
                "INSERT INTO _golden_oc VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = 1",
            ),
            &cancel,
        )
        .await
        .expect("upsert");
    Box::new(tx_setup)
        .commit(&cancel)
        .await
        .expect("commit setup");

    let update = Statement::new("UPDATE _golden_oc SET v = v + 1 WHERE id = $1 AND v = $2")
        .with_params(vec![ParameterValue::I32(1), ParameterValue::I32(1)]);
    let probe = Statement::new("SELECT 1 FROM _golden_oc WHERE id = $1")
        .with_params(vec![ParameterValue::I32(1)]);

    // Winner
    let mut tx_win = p
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin winner");
    tx_win
        .execute_conditional_update(
            ConditionalUpdate {
                update: &update,
                key_probe: Some(&probe),
                expected_affected_rows: 1,
            },
            &cancel,
        )
        .await
        .expect("winner apply");
    Box::new(tx_win)
        .commit(&cancel)
        .await
        .expect("winner commit");

    // Loser: stessa expected_version=1, ma la riga ora è v=2.
    let mut tx_lose = p
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin loser");
    let outcome = tx_lose
        .execute_conditional_update(
            ConditionalUpdate {
                update: &update,
                key_probe: Some(&probe),
                expected_affected_rows: 1,
            },
            &cancel,
        )
        .await;
    let _ = Box::new(tx_lose).rollback(&cancel).await;

    let err = outcome.expect_err("il loser deve fallire");
    assert_eq!(err.category, ErrorCategory::ConcurrentModification);

    // Cleanup
    let mut tx_cleanup = p
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin cleanup");
    let _ = tx_cleanup
        .execute(&Statement::new("DROP TABLE IF EXISTS _golden_oc"), &cancel)
        .await;
    let _ = Box::new(tx_cleanup).commit(&cancel).await;
}

// ============================================================================
//  Golden 4 — session context leak-free: tx1 con context, tx2 sulla stessa
//  connessione del pool non deve vederlo
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn golden_session_context_is_isolated_across_pool_reuse() {
    // Pool size 1 forza il riuso della stessa connessione tra tx1 e tx2 —
    // il pattern peggiore per il context leak.
    let p = PostgresProvider::insecure_local_with_batch_rows(1_024).with_pool_size(1, 5_000);
    let cancel = CancellationToken::new();
    let s = secret();

    // Tx1: setta un context custom.
    let mut ctx = SessionContext::new();
    ctx.insert(
        "app.leak_probe",
        SessionEntry::public(SessionValue::Text("tx1-marker".into())),
    )
    .expect("session insert");
    let opts_with = TransactionOptions {
        context: ctx,
        ..TransactionOptions::default()
    };

    let mut tx1 = p
        .begin_transaction(&s, &opts_with, &budget(), &cancel)
        .await
        .expect("begin tx1");
    let inside = execute_scalar_string(
        tx1.as_mut(),
        &Statement::new("SELECT current_setting('app.leak_probe', true)"),
        &cancel,
    )
    .await
    .expect("inside");
    assert_eq!(inside, "tx1-marker");
    Box::new(tx1).commit(&cancel).await.expect("commit tx1");

    // Tx2: nessun context. Sulla stessa connessione riusata dal pool → deve
    // vedere stringa vuota, senza traccia del context di tx1.
    let mut tx2 = p
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin tx2");
    let after = execute_scalar_string(
        tx2.as_mut(),
        &Statement::new("SELECT current_setting('app.leak_probe', true)"),
        &cancel,
    )
    .await
    .expect("after");
    Box::new(tx2).rollback(&cancel).await.expect("rollback tx2");

    assert!(
        after.is_empty(),
        "session context deve essere resettato tra tx sulla stessa connessione: leaked={after:?}"
    );
}

// ============================================================================
//  Golden 5 — facade scalar_*: tutti i tipi supportati roundtrippano
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn golden_scalar_facade_covers_every_supported_type() {
    let p = provider();
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    assert!(
        execute_scalar_bool(tx.as_mut(), &Statement::new("SELECT true"), &cancel)
            .await
            .expect("bool")
    );
    assert_eq!(
        execute_scalar_i32(tx.as_mut(), &Statement::new("SELECT 7::INT"), &cancel)
            .await
            .expect("i32"),
        7
    );
    assert_eq!(
        execute_scalar_i64(
            tx.as_mut(),
            &Statement::new("SELECT 9223372036854775807::BIGINT"),
            &cancel,
        )
        .await
        .expect("i64"),
        i64::MAX
    );
    // f64: due epsilon di margine per il roundtrip.
    let f = execute_scalar_f64(
        tx.as_mut(),
        &Statement::new("SELECT 3.14159265358979::DOUBLE PRECISION"),
        &cancel,
    )
    .await
    .expect("f64");
    assert!((f - 3.14159265358979_f64).abs() < 1e-12);

    assert_eq!(
        execute_scalar_string(
            tx.as_mut(),
            &Statement::new("SELECT 'hello'::TEXT"),
            &cancel
        )
        .await
        .expect("string"),
        "hello"
    );
    assert_eq!(
        execute_scalar_bytes(
            tx.as_mut(),
            &Statement::new(r"SELECT '\xdeadbeef'::BYTEA"),
            &cancel,
        )
        .await
        .expect("bytes"),
        vec![0xde, 0xad, 0xbe, 0xef]
    );

    let uuid_out = execute_scalar_uuid(
        tx.as_mut(),
        &Statement::new("SELECT '11111111-2222-3333-4444-555555555555'::UUID"),
        &cancel,
    )
    .await
    .expect("uuid");
    assert_eq!(uuid_out, "11111111-2222-3333-4444-555555555555");

    let js = execute_scalar_json(
        tx.as_mut(),
        &Statement::new(r#"SELECT '{"k":"v"}'::JSONB"#),
        &cancel,
    )
    .await
    .expect("json");
    assert_eq!(js["k"], "v");

    let d = execute_scalar_date(
        tx.as_mut(),
        &Statement::new("SELECT '2026-08-12'::DATE"),
        &cancel,
    )
    .await
    .expect("date");
    assert_eq!(d, "2026-08-12");
    let ts = execute_scalar_timestamp(
        tx.as_mut(),
        &Statement::new("SELECT '2026-08-12T10:00:00'::TIMESTAMP"),
        &cancel,
    )
    .await
    .expect("ts");
    assert!(ts.starts_with("2026-08-12"));
    let tstz = execute_scalar_timestamptz(
        tx.as_mut(),
        &Statement::new("SELECT '2026-08-12T10:00:00Z'::TIMESTAMPTZ"),
        &cancel,
    )
    .await
    .expect("tstz");
    assert!(tstz.starts_with("2026-08-12"));

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  Golden 6 — execute_portable + execute_portable_returning + returning_one
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn golden_portable_facade_full_dml_flow() {
    let p = provider();
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _golden_portable ( \
             id INT PRIMARY KEY, \
             label TEXT NOT NULL) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("temp");

    // INSERT senza RETURNING → execute_portable
    let ins = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("_golden_portable"),
        columns: vec!["id".into(), "label".into()],
        values: vec![
            vec![
                Expression::literal(ParameterValue::I32(1)),
                Expression::literal(ParameterValue::String("a".into())),
            ],
            vec![
                Expression::literal(ParameterValue::I32(2)),
                Expression::literal(ParameterValue::String("b".into())),
            ],
        ],
        returning: Vec::new(),
    });
    let affected = execute_portable(tx.as_mut(), &ins, &cancel)
        .await
        .expect("insert");
    assert_eq!(affected, 2);

    // execute_portable non deve accettare SELECT.
    let sel = p_select("_golden_portable", vec!["id"])
        .where_(p_eq("id", ParameterValue::I32(1)))
        .into_statement();
    let err = execute_portable(tx.as_mut(), &sel, &cancel)
        .await
        .expect_err("select via execute_portable deve fallire");
    assert_eq!(err.category, ErrorCategory::InvalidPlan);

    // execute_portable_returning → SELECT
    let rows = execute_portable_returning(tx.as_mut(), &sel, &cancel)
        .await
        .expect("select");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].get_index(0), Some(ParameterValue::I32(1))));

    // execute_portable_returning_one → UPDATE ... RETURNING
    let upd = PortableStatement::Update(UpdateStatement {
        table: TableRef::new("_golden_portable"),
        assignments: vec![(
            "label".into(),
            Expression::literal(ParameterValue::String("a2".into())),
        )],
        filter: Some(p_eq("id", ParameterValue::I32(1))),
        returning: vec!["label".into()],
    });
    let row = execute_portable_returning_one(tx.as_mut(), &upd, &cancel)
        .await
        .expect("update returning one");
    assert!(matches!(row.get_index(0), Some(ParameterValue::String(s)) if s == "a2"));

    // DELETE ... RETURNING con 2 righe: returning_one deve fallire con Conflict.
    let del = PortableStatement::Delete(DeleteStatement {
        table: TableRef::new("_golden_portable"),
        filter: None,
        returning: vec!["id".into()],
    });
    let err = execute_portable_returning_one(tx.as_mut(), &del, &cancel)
        .await
        .expect_err("delete di 2 righe via returning_one deve fallire");
    assert_eq!(err.category, ErrorCategory::Conflict);

    // execute_portable rifiuta uno statement con RETURNING.
    let err = execute_portable(tx.as_mut(), &del, &cancel)
        .await
        .expect_err("execute_portable con RETURNING deve fallire");
    assert_eq!(err.category, ErrorCategory::InvalidPlan);

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  Golden 7 — query_one / query_optional shape validation
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn golden_query_one_and_optional_shape_errors() {
    let p = provider();
    let cancel = CancellationToken::new();
    let mut tx = p
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    // query_one: 0 righe → NotFound
    let err = query_one(
        tx.as_mut(),
        &Statement::new("SELECT 1 WHERE false"),
        &cancel,
    )
    .await
    .expect_err("0 righe → NotFound");
    assert_eq!(err.category, ErrorCategory::NotFound);

    // query_one: 2 righe → Conflict
    let err = query_one(
        tx.as_mut(),
        &Statement::new("SELECT * FROM generate_series(1,2)"),
        &cancel,
    )
    .await
    .expect_err("2 righe → Conflict");
    assert_eq!(err.category, ErrorCategory::Conflict);

    // query_optional: 0 righe → Ok(None)
    let none = query_optional(
        tx.as_mut(),
        &Statement::new("SELECT 1 WHERE false"),
        &cancel,
    )
    .await
    .expect("0 righe → Ok(None)");
    assert!(none.is_none());

    // query_optional: 1 riga → Ok(Some)
    let some = query_optional(tx.as_mut(), &Statement::new("SELECT 42::INT"), &cancel)
        .await
        .expect("1 riga → Ok(Some)");
    assert!(some.is_some());

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}
