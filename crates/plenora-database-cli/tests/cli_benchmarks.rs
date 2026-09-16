//! Smoke live dei benchmark CLI: conteggi, risultati e percentili ordinati.
//! Il confronto p95 e opt-in tramite `PLENORA_CLI_BENCHMARK_BASELINE`.

#![cfg(test)]
#![allow(clippy::doc_markdown, clippy::items_after_statements)]

use serde_json::Value;
use std::path::PathBuf;

mod common;

fn load_baseline() -> Value {
    let path = std::env::var_os("PLENORA_CLI_BENCHMARK_BASELINE").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/benchmark_baseline.json"),
        PathBuf::from,
    );
    let content = std::fs::read(path).expect("configurazione benchmark assente");
    serde_json::from_slice(&content).expect("baseline JSON non parsabile")
}

fn run_json(args: &[&str]) -> Value {
    let output = common::postgres_cli(args).output().expect("spawn");
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
         (upgrade Postgres, cambio hardware), rigenerare la baseline nell'ambiente scelto."
    );
}

fn compare_percentiles(name: &str, actual: &Value, baseline: &Value, tolerance_pct: u64) {
    let p95 = actual["latency_us"]["p95"]
        .as_u64()
        .expect("p95 atteso u64");
    let percentiles: Vec<u64> = ["min", "p50", "p95", "p99", "max"]
        .iter()
        .map(|key| actual["latency_us"][key].as_u64().expect("percentile u64"))
        .collect();
    assert!(percentiles.windows(2).all(|pair| pair[0] <= pair[1]));
    assert_eq!(actual["iterations"], baseline["iterations"]);
    if std::env::var_os("PLENORA_CLI_BENCHMARK_BASELINE").is_none() {
        println!("{name}: p95={p95}us; baseline_comparison=not_requested");
        return;
    }
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
fn live_benchmark_oltp() {
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
fn live_benchmark_read() {
    let baseline = load_baseline();
    let tol = baseline["tolerance_pct"].as_u64().unwrap_or(30);
    let cfg = &baseline["benchmarks"]["benchmark-read"];
    let iter = cfg["iterations"].as_u64().unwrap_or(50).to_string();
    let sql = cfg["sql"].as_str().unwrap_or("SELECT 1");
    let actual = run_json(&["benchmark-read", "PG_DSN", sql, &iter]);
    assert_eq!(actual["total_rows"], cfg["iterations"]);
    compare_percentiles("benchmark-read", &actual, cfg, tol);
}

// ============================================================================
//  Test: benchmark-write (richiede --allow-write-tests)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_benchmark_write() {
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
    assert_eq!(actual["batch_size"], cfg["batch_size"]);
    assert_eq!(
        actual["total_rows"].as_u64(),
        Some(
            cfg["iterations"].as_u64().expect("iterations")
                * cfg["batch_size"].as_u64().expect("batch_size")
        )
    );
    compare_percentiles("benchmark-write", &actual, cfg, tol);
}

// ============================================================================
//  Test: benchmark-spatial
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_benchmark_spatial() {
    let baseline = load_baseline();
    let tol = baseline["tolerance_pct"].as_u64().unwrap_or(30);
    let cfg = &baseline["benchmarks"]["benchmark-spatial"];
    let iter = cfg["iterations"].as_u64().unwrap_or(50).to_string();
    let actual = run_json(&["benchmark-spatial", "PG_DSN", &iter]);
    compare_percentiles("benchmark-spatial", &actual, cfg, tol);
}
