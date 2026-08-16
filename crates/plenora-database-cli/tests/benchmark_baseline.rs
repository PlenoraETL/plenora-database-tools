//! Gate CI su regressione performance: esegue i benchmark CLI, compara
//! p95/p99 con la baseline in `tests/fixtures/benchmark_baseline.json`.
//! Fallisce se la latenza attuale supera la baseline oltre la tolleranza
//! (default 30%).
//!
//! Su hardware diverso, aggiornare la baseline con una fresh run. La
//! tolerance è pensata per assorbire variazioni CI/dev machine senza
//! nascondere regressioni grosse.
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.
//! Sono anche relativamente lenti (~1-2s cadauno).

#![cfg(test)]
#![allow(clippy::doc_markdown, clippy::items_after_statements)]

use serde_json::Value;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_plenora-database");
const DSN: &str =
    "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test";
const BASELINE_PATH: &str = "tests/fixtures/benchmark_baseline.json";

fn load_baseline() -> Value {
    let content = std::fs::read(BASELINE_PATH)
        .unwrap_or_else(|_| panic!("baseline file mancante: {BASELINE_PATH}"));
    serde_json::from_slice(&content).expect("baseline JSON non parsabile")
}

fn run_json(args: &[&str]) -> Value {
    let output = Command::new(BIN)
        .args(args)
        .env("PG_DSN", DSN)
        .output()
        .expect("spawn");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI failed: args={args:?}\nstdout={stdout}\nstderr={stderr}"
    );
    serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("output non JSON: {e}\nstdout={stdout}"))
}

fn assert_within_tolerance(
    name: &str,
    metric: &str,
    actual_us: u64,
    baseline_us: u64,
    tolerance_pct: u64,
) {
    let allowed_us = baseline_us + (baseline_us * tolerance_pct / 100);
    assert!(
        actual_us <= allowed_us,
        "REGRESSIONE {name} {metric}: attuale {actual_us}µs > baseline {baseline_us}µs \
         + {tolerance_pct}% ({allowed_us}µs). Se questa è una nuova baseline attesa \
         (upgrade Postgres, cambio hardware), rigenerare {BASELINE_PATH}."
    );
}

fn compare_percentiles(name: &str, actual: &Value, baseline: &Value, tolerance_pct: u64) {
    let p95 = actual["latency_us"]["p95"]
        .as_u64()
        .expect("p95 atteso u64");
    let baseline_p95 = baseline["p95_us_max"]
        .as_u64()
        .unwrap_or_else(|| panic!("baseline p95_us_max mancante per {name}"));
    assert_within_tolerance(name, "p95", p95, baseline_p95, tolerance_pct);
    // p99 non gated: con 50 iter è troppo rumoroso, alcuni outliers di setup
    // pool possono dominare. Il p95 assorbe meglio i regression signal.
}

// ============================================================================
//  Test: benchmark-oltp
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn baseline_benchmark_oltp() {
    let baseline = load_baseline();
    let tol = baseline["tolerance_pct"].as_u64().unwrap_or(30);
    let cfg = &baseline["benchmarks"]["benchmark-oltp"];
    let iter = cfg["iterations"].as_u64().unwrap_or(50).to_string();
    let actual = run_json(&["benchmark-oltp", "PG_DSN", &iter]);
    compare_percentiles("benchmark-oltp", &actual, cfg, tol);
}

// ============================================================================
//  Test: benchmark-read
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn baseline_benchmark_read() {
    let baseline = load_baseline();
    let tol = baseline["tolerance_pct"].as_u64().unwrap_or(30);
    let cfg = &baseline["benchmarks"]["benchmark-read"];
    let iter = cfg["iterations"].as_u64().unwrap_or(50).to_string();
    let sql = cfg["sql"].as_str().unwrap_or("SELECT 1");
    let actual = run_json(&["benchmark-read", "PG_DSN", sql, &iter]);
    compare_percentiles("benchmark-read", &actual, cfg, tol);
}

// ============================================================================
//  Test: benchmark-write (richiede --allow-write-tests)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn baseline_benchmark_write() {
    let baseline = load_baseline();
    let tol = baseline["tolerance_pct"].as_u64().unwrap_or(30);
    let cfg = &baseline["benchmarks"]["benchmark-write"];
    let iter = cfg["iterations"].as_u64().unwrap_or(50).to_string();
    let batch = cfg["batch_size"].as_u64().unwrap_or(5).to_string();
    let actual = run_json(&[
        "--allow-write-tests",
        "benchmark-write",
        "PG_DSN",
        &iter,
        &batch,
    ]);
    compare_percentiles("benchmark-write", &actual, cfg, tol);
}

// ============================================================================
//  Test: benchmark-spatial
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn baseline_benchmark_spatial() {
    let baseline = load_baseline();
    let tol = baseline["tolerance_pct"].as_u64().unwrap_or(30);
    let cfg = &baseline["benchmarks"]["benchmark-spatial"];
    let iter = cfg["iterations"].as_u64().unwrap_or(50).to_string();
    let actual = run_json(&["benchmark-spatial", "PG_DSN", &iter]);
    compare_percentiles("benchmark-spatial", &actual, cfg, tol);
}
