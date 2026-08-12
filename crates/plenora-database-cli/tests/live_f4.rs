//! Live test end-to-end dei nuovi sottocomandi Fase 4 invocati come binario.
//!
//! Richiedono un Postgres raggiungibile all'hostname `dataflow-postgres`
//! (compose network `database-tools_default`) e la variabile ambiente `PG_DSN`
//! popolata con il DSN completo.
//!
//! Sono `#[ignore]` per default; esegui con:
//!
//! ```text
//! docker run --rm --network database-tools_default -v ... rust:1.92 \
//!   cargo test --test live_f4 -- --ignored --nocapture
//! ```

#![cfg(test)]

use serde_json::Value;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_plenora-database");

fn run(args: &[&str]) -> Value {
    let output = Command::new(BIN)
        .args(args)
        .env(
            "PG_DSN",
            "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
             dbname=dataflow_test",
        )
        .output()
        .expect("spawn CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI ha fallito per args={args:?}\nstdout={stdout}\nstderr={stderr}"
    );
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("output non JSON: {e}\nstdout={stdout}");
    })
}

fn run_expect_error(args: &[&str]) -> Value {
    let output = Command::new(BIN)
        .args(args)
        .env(
            "PG_DSN",
            "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
             dbname=dataflow_test",
        )
        .output()
        .expect("spawn CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !output.status.success(),
        "CLI doveva fallire ma ha avuto successo: args={args:?}\nstdout={stdout}"
    );
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("output errore non JSON: {e}\nstdout={stdout}");
    })
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_inspect_database_returns_expected_metadata_shape() {
    let out = run(&["inspect-database", "PG_DSN"]);
    assert!(out["database"].as_str().is_some(), "database mancante: {out}");
    assert!(out["version"].as_str().is_some(), "version mancante: {out}");
    assert!(
        out["version_num"].as_str().is_some(),
        "version_num mancante: {out}"
    );
    assert!(out["encoding"].as_str().is_some());
    assert!(out["timezone"].as_str().is_some());
    assert!(out["size"].as_str().is_some());
    let exts = out["extensions"].as_array().expect("extensions array");
    // plpgsql è sempre presente su Postgres.
    assert!(
        exts.iter().any(|e| e["name"] == "plpgsql"),
        "plpgsql non trovato tra le extensions: {exts:?}"
    );
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_inspect_schemas_lists_user_schemas() {
    let out = run(&["inspect-schemas", "PG_DSN"]);
    let count = out["count"].as_u64().expect("count u64");
    let schemas = out["schemas"].as_array().expect("schemas array");
    assert_eq!(count, schemas.len() as u64, "count/schemas mismatch");
    // 'public' deve esserci sempre; pg_catalog non deve.
    assert!(
        schemas.iter().any(|s| s["name"] == "public"),
        "public schema mancante: {schemas:?}"
    );
    assert!(
        !schemas.iter().any(|s| s["name"] == "pg_catalog"),
        "pg_catalog non deve essere elencato: {schemas:?}"
    );
    for s in schemas {
        assert!(s["owner"].as_str().is_some(), "owner mancante: {s}");
    }
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_inspect_tables_lists_relations_with_size_and_row_estimate() {
    let out = run(&["inspect-tables", "PG_DSN", "public"]);
    assert_eq!(out["schema"], "public");
    let tables = out["tables"].as_array().expect("tables array");
    // spatial_ref_sys è di PostGIS ed è sempre in public quando l'estensione è installata.
    let srs = tables.iter().find(|t| t["name"] == "spatial_ref_sys");
    if let Some(srs) = srs {
        assert_eq!(srs["kind"], "table");
        assert!(srs["total_size"].as_str().is_some());
        assert!(srs["estimated_rows"].is_number());
    }
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_diagnose_reports_healthy_when_probes_pass() {
    let out = run(&["diagnose", "PG_DSN"]);
    // Su un Postgres standard con PostGIS installato, OLTP e PFM_CORE devono
    // passare; PFM_GIS può fallire se PostGIS è assente. Ma lo status generale
    // ha una definizione precisa: overall_pass = OLTP && PFM_CORE.
    let status = out["status"].as_str().expect("status str");
    assert!(
        status == "healthy" || status == "unhealthy",
        "status inatteso: {status}"
    );
    assert_eq!(out["connection"]["status"], "ok");
    assert!(
        out["connection"]["connect_ms"].as_u64().is_some(),
        "connect_ms mancante"
    );
    // server_config deve avere almeno max_connections.
    assert!(
        out["server_config"]["max_connections"]["value"]
            .as_str()
            .is_some(),
        "server_config.max_connections mancante"
    );
    // I 3 profili sono sempre inclusi anche quando falliscono.
    assert!(out["profiles"]["APPLICATION_OLTP_V1"]["status"].is_string());
    assert!(out["profiles"]["PFM_CORE_V1"]["status"].is_string());
    assert!(out["profiles"]["PFM_GIS_V1"]["status"].is_string());
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_benchmark_write_requires_explicit_gate() {
    // Senza --allow-write-tests → invalid_plan
    let err = run_expect_error(&["benchmark-write", "PG_DSN", "5", "2"]);
    assert_eq!(err["status"], "error");
    assert_eq!(err["error"]["category"], "invalid_plan");
    assert!(err["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("--allow-write-tests"));
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_benchmark_write_reports_throughput_and_percentiles() {
    let out = run(&[
        "--allow-write-tests",
        "benchmark-write",
        "PG_DSN",
        "20", // iterations
        "5",  // batch_size
    ]);
    assert_eq!(out["iterations"], 20);
    assert_eq!(out["batch_size"], 5);
    assert_eq!(out["total_rows"], 100);
    assert!(out["rows_per_sec"].as_f64().unwrap_or(0.0) > 0.0);
    assert!(out["latency_us"]["p50"].is_number());
    assert!(out["latency_us"]["p95"].is_number());
    assert!(out["latency_us"]["p99"].is_number());
    let p50 = out["latency_us"]["p50"].as_u64().unwrap_or(0);
    let p95 = out["latency_us"]["p95"].as_u64().unwrap_or(0);
    let p99 = out["latency_us"]["p99"].as_u64().unwrap_or(0);
    assert!(p50 <= p95 && p95 <= p99, "percentili non monotoni: p50={p50} p95={p95} p99={p99}");
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_test_concurrency_reports_winner_and_loser_correctly() {
    let out = run(&[
        "--allow-write-tests",
        "test-concurrency",
        "PG_DSN",
    ]);
    assert_eq!(out["status"], "ok");
    assert_eq!(out["winner_committed"], true);
    assert_eq!(out["loser_error_category"], "ConcurrentModification");
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_profile_check_returns_pass_for_application_oltp_v1() {
    let out = run(&["profile-check", "PG_DSN", "APPLICATION_OLTP_V1"]);
    assert_eq!(out["status"], "pass", "APPLICATION_OLTP_V1 deve passare: {out}");
    assert!(out["evidence"].as_array().is_some());
    assert_eq!(out["missing"].as_array().map(Vec::len), Some(0));
    assert_eq!(out["failed"].as_array().map(Vec::len), Some(0));
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_format_junit_wraps_ok_status_as_system_out() {
    // JUnit non è JSON: parse manuale minimale.
    let output = Command::new(BIN)
        .args(["--format", "junit", "profile-list"])
        .env(
            "PG_DSN",
            "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
             dbname=dataflow_test",
        )
        .output()
        .expect("spawn");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("<?xml version=\"1.0\""));
    assert!(stdout.contains("<testsuite name=\"plenora-database-cli\""));
    assert!(stdout.contains("failures=\"0\""));
    assert!(stdout.contains("<system-out>"));
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f4_format_markdown_renders_title_and_bullets() {
    let output = Command::new(BIN)
        .args(["--format", "markdown", "profile-list"])
        .env(
            "PG_DSN",
            "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
             dbname=dataflow_test",
        )
        .output()
        .expect("spawn");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("# profile-list"));
    assert!(stdout.contains("- **profiles**:"));
}
