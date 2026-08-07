//! Microbenchmark offline della pipeline `parse -> validate -> fingerprint`.
//!
//! Superficie misurata: `plenora_database_engine::parse_and_validate`, che e'
//! il primo costo di ogni invocazione della CLI e non tocca alcun database.
//! Lo scenario `parse_only` isola il costo serde da quello di validazione e
//! fingerprint SHA-256: la differenza fra i due scenari e' il costo proprio
//! del contratto.
//!
//! Uso: `bench_plan_pipeline [iterazioni] [ripetizioni]`.
//! Emette una riga JSON per scenario su stdout (JSONL).
//!
//! Nessuna soglia e' codificata qui: il binario misura e basta. I budget
//! prestazionali sono una decisione di prodotto e vanno fissati altrove.

use plenora_database_core::plan::Plan;
use plenora_database_engine::parse_and_validate;
use serde_json::{json, Value};
use std::hint::black_box;
use std::time::Instant;

/// Piano di riferimento del contratto v1: la forma che la CLI vede piu' spesso.
const CONTRACT_READ_PLAN: &[u8] =
    include_bytes!("../../../contracts/v1/examples/plan-postgres-read.json");

/// Costruisce un piano di lettura con `columns` proiezioni e `terms` termini
/// di filtro, per osservare come scala il costo con la larghezza del piano.
fn wide_read_plan(columns: usize, terms: usize) -> Vec<u8> {
    let projection: Vec<Value> = (0..columns)
        .map(|index| json!(format!("c{index}")))
        .collect();
    let args: Vec<Value> = (0..terms)
        .map(|index| {
            json!({
                "op": "gte",
                "field": format!("c{}", index % columns),
                "parameter": format!("p{index}"),
            })
        })
        .collect();
    let plan = json!({
        "schema_version": 1,
        "connection_ref": "env:PLENORA_DATABASE_DSN",
        "provider": "postgres",
        "operation": {
            "id": "database.read",
            "source": {"catalog": "plenora", "schema": "public", "object": "events"},
            "projection": projection,
            "filter": {"op": "and", "args": args},
            "order_by": [{"field": "c0", "direction": "asc"}],
        },
        "limits": {
            "max_rows": 1_000_000,
            "max_batch_bytes": 16_777_216,
            "max_memory_bytes": 268_435_456,
            "timeout_ms": 30_000,
        },
    });
    serde_json::to_vec(&plan).expect("piano sintetico serializzabile")
}

/// Peak RSS del processo; assente fuori da Linux.
fn peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

/// Esegue `iterations` chiamate per ripetizione e riporta la mediana.
///
/// La chiusura restituisce un `usize` che viene accumulato: senza il
/// checksum il compilatore puo' eliminare la chiamata.
fn run_scenario(name: &str, iterations: usize, repetitions: usize, execute: impl Fn() -> usize) {
    black_box(execute());
    let mut durations = Vec::with_capacity(repetitions);
    let mut checksum: usize = 0;
    for _ in 0..repetitions {
        let start = Instant::now();
        for _ in 0..iterations {
            checksum = checksum.wrapping_add(execute());
        }
        durations.push(start.elapsed().as_secs_f64());
    }
    black_box(checksum);
    durations.sort_by(f64::total_cmp);
    let median = durations[durations.len() / 2];
    // Conteggi di benchmark: ben sotto 2^53, la conversione e' esatta.
    #[allow(clippy::cast_precision_loss)]
    let iterations_f64 = iterations as f64;
    let nanoseconds = median / iterations_f64 * 1e9;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "bench": "plan_pipeline",
            "scenario": name,
            "iterations": iterations,
            "repetitions": repetitions,
            "median_seconds": median,
            "nanoseconds_per_operation": nanoseconds,
            "operations_per_second": iterations_f64 / median,
            "checksum": checksum,
            "peak_rss_kib": peak_rss_kib(),
        }))
        .expect("record JSON serializzabile")
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let iterations: usize = args
        .next()
        .as_deref()
        .unwrap_or("2000")
        .parse()
        .expect("iterazioni");
    let repetitions: usize = args
        .next()
        .as_deref()
        .unwrap_or("9")
        .parse()
        .expect("ripetizioni");

    let wide_16 = wide_read_plan(16, 8);
    let wide_256 = wide_read_plan(256, 64);

    run_scenario(
        "parse_and_validate_contract_read",
        iterations,
        repetitions,
        || {
            parse_and_validate(CONTRACT_READ_PLAN)
                .expect("piano di contratto valido")
                .fingerprint()
                .len()
        },
    );
    run_scenario("parse_only_contract_read", iterations, repetitions, || {
        serde_json::from_slice::<Plan>(CONTRACT_READ_PLAN)
            .expect("piano di contratto deserializzabile")
            .connection_ref
            .len()
    });
    run_scenario(
        "parse_and_validate_wide_16c_8f",
        iterations,
        repetitions,
        || {
            parse_and_validate(&wide_16)
                .expect("piano sintetico valido")
                .fingerprint()
                .len()
        },
    );
    run_scenario(
        "parse_and_validate_wide_256c_64f",
        iterations,
        repetitions,
        || {
            parse_and_validate(&wide_256)
                .expect("piano sintetico valido")
                .fingerprint()
                .len()
        },
    );
}
