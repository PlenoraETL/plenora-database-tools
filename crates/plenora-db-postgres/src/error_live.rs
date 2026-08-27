use super::*;
use tokio_postgres::{Client, NoTls};

/// DSN del riferimento plaintext, usato quando il runner non ne impone
/// uno.
const REFERENCE_DSN: &str =
    "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test";

/// Il DSN su cui girano questi test live.
///
/// `PLENORA_TEST_POSTGRES_DSN` ha la precedenza: e cosi che la matrice
/// delle versioni indirizza la suite verso il `PostgreSQL` che sta
/// qualificando. Senza questa lettura i test puntavano sempre al
/// riferimento, quindi la matrice affermava di averli superati su 14, 15
/// e 17 mentre giravano su 16 — e funzionavano solo perche la rete
/// privata della matrice condivideva il nome con quella del compose.
fn live_dsn() -> String {
    std::env::var("PLENORA_TEST_POSTGRES_DSN").unwrap_or_else(|_| REFERENCE_DSN.to_owned())
}

async fn connect() -> Client {
    let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
        .await
        .expect("connessione al PostgreSQL live");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn err_from(client: &Client, sql: &str) -> tokio_postgres::Error {
    client
        .batch_execute(sql)
        .await
        .expect_err(&format!("mi aspetto errore da: {sql}"))
}

fn state(err: &tokio_postgres::Error) -> Option<&str> {
    err.code().map(tokio_postgres::error::SqlState::code)
}

#[tokio::test]
async fn live_unique_violation_23505_is_conflict() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS a2_unique;
             CREATE TABLE a2_unique (id INT PRIMARY KEY);
             INSERT INTO a2_unique VALUES (1);",
        )
        .await
        .expect("setup");

    let raw = err_from(&client, "INSERT INTO a2_unique VALUES (1);").await;
    assert_eq!(state(&raw), Some("23505"));

    let mapped = classify_error(ErrorPhase::Write, &raw);
    assert_eq!(mapped.category, ErrorCategory::Conflict);
    assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
    assert!(matches!(mapped.retry, RetryDisposition::Never));

    client
        .batch_execute("DROP TABLE a2_unique;")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_not_null_violation_23502_is_conflict() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS a2_notnull;
             CREATE TABLE a2_notnull (id INT NOT NULL);",
        )
        .await
        .expect("setup");

    let raw = err_from(&client, "INSERT INTO a2_notnull VALUES (NULL);").await;
    assert_eq!(state(&raw), Some("23502"));
    let mapped = classify_error(ErrorPhase::Write, &raw);
    assert_eq!(mapped.category, ErrorCategory::Conflict);

    client
        .batch_execute("DROP TABLE a2_notnull;")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_foreign_key_violation_23503_is_conflict() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS a2_fk_child;
             DROP TABLE IF EXISTS a2_fk_parent;
             CREATE TABLE a2_fk_parent (id INT PRIMARY KEY);
             CREATE TABLE a2_fk_child  (id INT, parent INT REFERENCES a2_fk_parent(id));",
        )
        .await
        .expect("setup");

    let raw = err_from(&client, "INSERT INTO a2_fk_child VALUES (1, 999);").await;
    assert_eq!(state(&raw), Some("23503"));
    let mapped = classify_error(ErrorPhase::Write, &raw);
    assert_eq!(mapped.category, ErrorCategory::Conflict);

    client
        .batch_execute("DROP TABLE a2_fk_child; DROP TABLE a2_fk_parent;")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_check_violation_23514_is_conflict() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS a2_check;
             CREATE TABLE a2_check (v INT CHECK (v > 0));",
        )
        .await
        .expect("setup");

    let raw = err_from(&client, "INSERT INTO a2_check VALUES (-1);").await;
    assert_eq!(state(&raw), Some("23514"));
    assert_eq!(
        classify_error(ErrorPhase::Write, &raw).category,
        ErrorCategory::Conflict
    );

    client
        .batch_execute("DROP TABLE a2_check;")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_undefined_table_42p01_is_not_found() {
    let client = connect().await;
    let raw = err_from(&client, "SELECT * FROM a2_absent_table_xyz;").await;
    assert_eq!(state(&raw), Some("42P01"));
    let mapped = classify_error(ErrorPhase::Prepare, &raw);
    assert_eq!(mapped.category, ErrorCategory::NotFound);
    assert_eq!(mapped.remote_effect, RemoteEffect::None);
}

#[tokio::test]
async fn live_undefined_column_42703_is_not_found() {
    let client = connect().await;
    let raw = err_from(&client, "SELECT a2_absent_col FROM (VALUES (1)) t(x);").await;
    assert_eq!(state(&raw), Some("42703"));
    assert_eq!(
        classify_error(ErrorPhase::Prepare, &raw).category,
        ErrorCategory::NotFound
    );
}

#[tokio::test]
async fn live_syntax_error_42601_is_invalid_plan() {
    let client = connect().await;
    let raw = err_from(&client, "SELEKT 1;").await;
    assert_eq!(state(&raw), Some("42601"));
    assert_eq!(
        classify_error(ErrorPhase::Prepare, &raw).category,
        ErrorCategory::InvalidPlan
    );
}

#[tokio::test]
async fn live_division_by_zero_22012_is_execution() {
    let client = connect().await;
    let raw = err_from(&client, "SELECT 1/0;").await;
    assert_eq!(state(&raw), Some("22012"));
    let mapped = classify_error(ErrorPhase::Read, &raw);
    assert_eq!(mapped.category, ErrorCategory::Execution);
    assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
}

#[tokio::test]
async fn live_invalid_text_representation_22p02_is_data_mapping() {
    let client = connect().await;
    let raw = err_from(&client, "SELECT 'not-a-number'::integer;").await;
    assert_eq!(state(&raw), Some("22P02"));
    assert_eq!(
        classify_error(ErrorPhase::Read, &raw).category,
        ErrorCategory::DataMapping
    );
}

#[tokio::test]
async fn live_numeric_out_of_range_22003_is_data_mapping() {
    let client = connect().await;
    let raw = err_from(
        &client,
        "SELECT (999999999::bigint * 999999999::bigint)::int;",
    )
    .await;
    assert_eq!(state(&raw), Some("22003"));
    assert_eq!(
        classify_error(ErrorPhase::Read, &raw).category,
        ErrorCategory::DataMapping
    );
}

#[tokio::test]
async fn live_query_canceled_57014_is_cancelled() {
    let client = connect().await;
    let raw = err_from(
        &client,
        "SET statement_timeout = '50ms'; SELECT pg_sleep(2);",
    )
    .await;
    assert_eq!(state(&raw), Some("57014"));
    let mapped = classify_error(ErrorPhase::Read, &raw);
    assert_eq!(mapped.category, ErrorCategory::Cancelled);
    assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
}

#[tokio::test]
async fn live_in_failed_sql_transaction_25p02_is_protocol() {
    let client = connect().await;
    client.batch_execute("BEGIN;").await.expect("begin");
    let _ = client
        .batch_execute("SELECT undefined_col FROM (VALUES (1)) t(x);")
        .await;
    let raw = err_from(&client, "SELECT 1;").await;
    assert_eq!(state(&raw), Some("25P02"));
    assert_eq!(
        classify_error(ErrorPhase::Write, &raw).category,
        ErrorCategory::Protocol
    );
    client.batch_execute("ROLLBACK;").await.ok();
}

#[tokio::test]
async fn live_read_only_transaction_25006_is_authorization() {
    let client = connect().await;
    client
        .batch_execute("BEGIN READ ONLY;")
        .await
        .expect("begin ro");
    let raw = err_from(&client, "CREATE TEMP TABLE a2_ro_test (x INT);").await;
    assert_eq!(state(&raw), Some("25006"));
    assert_eq!(
        classify_error(ErrorPhase::Write, &raw).category,
        ErrorCategory::Authorization
    );
    client.batch_execute("ROLLBACK;").await.ok();
}

#[tokio::test]
async fn live_invalid_schema_3f000_is_not_found() {
    let client = connect().await;
    let raw = err_from(&client, "DROP SCHEMA a2_absent_schema RESTRICT;").await;
    assert_eq!(state(&raw), Some("3F000"));
    assert_eq!(
        classify_error(ErrorPhase::Prepare, &raw).category,
        ErrorCategory::NotFound
    );
}

#[tokio::test]
async fn live_deadlock_40p01_is_transient_safe_retry() {
    let client_a = connect().await;
    let client_b = connect().await;

    client_a
        .batch_execute(
            "DROP TABLE IF EXISTS a2_deadlock;
             CREATE TABLE a2_deadlock (id INT PRIMARY KEY, v INT);
             INSERT INTO a2_deadlock VALUES (1, 10), (2, 20);",
        )
        .await
        .expect("setup");

    client_a.batch_execute("BEGIN;").await.expect("begin A");
    client_b.batch_execute("BEGIN;").await.expect("begin B");

    client_a
        .execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 1;", &[])
        .await
        .expect("A locks 1");
    client_b
        .execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 2;", &[])
        .await
        .expect("B locks 2");

    let a_fut = client_a.execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 2;", &[]);
    let b_fut = client_b.execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 1;", &[]);

    let (res_a, res_b) = tokio::join!(a_fut, b_fut);
    let deadlock_err = match (res_a, res_b) {
        (Err(e), _) | (_, Err(e)) if state(&e) == Some("40P01") => e,
        other => panic!("nessun deadlock 40P01 osservato: {other:?}"),
    };

    let mapped = classify_error(ErrorPhase::Write, &deadlock_err);
    assert_eq!(mapped.category, ErrorCategory::Transient);
    assert!(matches!(mapped.retry, RetryDisposition::Safe));
    assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);

    client_a.batch_execute("ROLLBACK;").await.ok();
    client_b.batch_execute("ROLLBACK;").await.ok();
    client_a
        .batch_execute("DROP TABLE a2_deadlock;")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_generated_column_write_428c9_is_conflict() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS a2_generated;
             CREATE TABLE a2_generated (
                 id INT PRIMARY KEY,
                 price NUMERIC,
                 tax_rate NUMERIC,
                 total NUMERIC GENERATED ALWAYS AS (price * (1 + tax_rate)) STORED
             );",
        )
        .await
        .expect("setup");

    // INSERT che tocca la colonna generata → 428C9
    let raw = err_from(
        &client,
        "INSERT INTO a2_generated (id, price, tax_rate, total) \
         VALUES (1, 10.0, 0.22, 100.0);",
    )
    .await;
    assert_eq!(state(&raw), Some("428C9"));

    let mapped = classify_error(ErrorPhase::Write, &raw);
    assert_eq!(mapped.category, ErrorCategory::Conflict);
    assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
    assert!(
        mapped.message.contains("generat"),
        "msg poco chiaro: {}",
        mapped.message
    );

    client
        .batch_execute("DROP TABLE a2_generated;")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_serialization_failure_40001_is_transient_safe_retry() {
    let client_a = connect().await;
    let client_b = connect().await;

    client_a
        .batch_execute(
            "DROP TABLE IF EXISTS a2_serial;
             CREATE TABLE a2_serial (id INT PRIMARY KEY, v INT);
             INSERT INTO a2_serial VALUES (1, 100), (2, 200);",
        )
        .await
        .expect("setup");

    client_a
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE;")
        .await
        .expect("begin A");
    client_b
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE;")
        .await
        .expect("begin B");

    let _ = client_a
        .query("SELECT SUM(v) FROM a2_serial;", &[])
        .await
        .expect("A read");
    let _ = client_b
        .query("SELECT SUM(v) FROM a2_serial;", &[])
        .await
        .expect("B read");
    client_a
        .execute("INSERT INTO a2_serial VALUES (3, 300);", &[])
        .await
        .expect("A insert");
    client_b
        .execute("INSERT INTO a2_serial VALUES (4, 400);", &[])
        .await
        .expect("B insert");
    client_a.batch_execute("COMMIT;").await.expect("A commit");
    let raw = client_b
        .batch_execute("COMMIT;")
        .await
        .expect_err("B deve fallire in serialization");

    assert_eq!(state(&raw), Some("40001"));
    let mapped = classify_error(ErrorPhase::Commit, &raw);
    assert_eq!(mapped.category, ErrorCategory::Transient);
    assert!(matches!(mapped.retry, RetryDisposition::Safe));

    client_a
        .batch_execute("DROP TABLE a2_serial;")
        .await
        .expect("cleanup");
}
