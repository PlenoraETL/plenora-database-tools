//! Sottocomandi benchmark: benchmark-oltp, benchmark-read, benchmark-spatial.
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliResult};
use plenora_database_core::provider::Provider;
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use serde_json::json;

pub(crate) fn rate_per_sec(iterations: u32, total_ms: u128) -> f64 {
    if total_ms == 0 {
        return 0.0;
    }
    f64::from(iterations) * 1000.0 / (total_ms as f64)
}

pub(crate) fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let n = sorted.len();
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((n as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(n - 1)]
}

pub(crate) async fn benchmark_oltp(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let iterations: u32 = args
        .next()
        .map(|s| s.parse().map_err(|_| "iterations non valido".to_owned()))
        .transpose()?
        .unwrap_or(100);
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let mut samples: Vec<u128> = Vec::with_capacity(iterations as usize);
    let overall_start = std::time::Instant::now();
    for _ in 0..iterations {
        let start = std::time::Instant::now();
        let mut tx = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
            .await?;
        tx.execute(&Statement::new("SELECT 1"), &cancel).await?;
        tx.commit(&cancel).await?;
        samples.push(start.elapsed().as_micros());
    }
    let overall_ms = overall_start.elapsed().as_millis();
    samples.sort_unstable();

    print_json(&json!({
        "iterations": iterations,
        "total_ms": overall_ms,
        "tx_per_sec": rate_per_sec(iterations, overall_ms),
        "latency_us": {
            "min": samples.first().copied().unwrap_or(0),
            "p50": percentile(&samples, 0.50),
            "p95": percentile(&samples, 0.95),
            "p99": percentile(&samples, 0.99),
            "max": samples.last().copied().unwrap_or(0),
        }
    }))
}

pub(crate) async fn benchmark_read(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let sql = args.next().ok_or("manca il SQL da benchmarkare")?;
    let iterations: u32 = args
        .next()
        .map(|s| s.parse().map_err(|_| "iterations non valido".to_owned()))
        .transpose()?
        .unwrap_or(100);
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let mut samples: Vec<u128> = Vec::with_capacity(iterations as usize);
    let mut total_rows = 0_u64;
    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    let overall_start = std::time::Instant::now();
    for _ in 0..iterations {
        let start = std::time::Instant::now();
        let rows = tx.query(&Statement::new(sql.clone()), &cancel).await?;
        samples.push(start.elapsed().as_micros());
        total_rows = total_rows.saturating_add(rows.len() as u64);
    }
    let overall_ms = overall_start.elapsed().as_millis();
    let _ = tx.rollback(&cancel).await;
    samples.sort_unstable();

    print_json(&json!({
        "iterations": iterations,
        "total_ms": overall_ms,
        "total_rows": total_rows,
        "queries_per_sec": rate_per_sec(iterations, overall_ms),
        "latency_us": {
            "min": samples.first().copied().unwrap_or(0),
            "p50": percentile(&samples, 0.50),
            "p95": percentile(&samples, 0.95),
            "p99": percentile(&samples, 0.99),
            "max": samples.last().copied().unwrap_or(0),
        }
    }))
}

pub(crate) async fn benchmark_spatial(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let iterations: u32 = args
        .next()
        .map(|s| s.parse().map_err(|_| "iterations non valido".to_owned()))
        .transpose()?
        .unwrap_or(50);
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    // Setup temp table con 1000 punti casuali in un bbox europeo.
    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE bench_spatial (id INT, geom geometry(Point, 4326)) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await?;
    tx.execute(
        &Statement::new(
            "INSERT INTO bench_spatial \
             SELECT gs, ST_SetSRID(ST_MakePoint(random()*20, 40+random()*20), 4326) \
             FROM generate_series(1, 1000) gs",
        ),
        &cancel,
    )
    .await?;
    tx.execute(
        &Statement::new("CREATE INDEX ON bench_spatial USING GIST(geom)"),
        &cancel,
    )
    .await?;

    let mut samples: Vec<u128> = Vec::with_capacity(iterations as usize);
    for _ in 0..iterations {
        let start = std::time::Instant::now();
        let _ = tx
            .query(
                &Statement::new(
                    "SELECT COUNT(*)::BIGINT FROM bench_spatial \
                     WHERE ST_Intersects(geom, ST_SetSRID(ST_MakeEnvelope(5, 45, 15, 55), 4326))",
                ),
                &cancel,
            )
            .await?;
        samples.push(start.elapsed().as_micros());
    }
    let _ = tx.rollback(&cancel).await;
    samples.sort_unstable();

    print_json(&json!({
        "iterations": iterations,
        "operation": "ST_Intersects over 1000 points with GIST index",
        "latency_us": {
            "min": samples.first().copied().unwrap_or(0),
            "p50": percentile(&samples, 0.50),
            "p95": percentile(&samples, 0.95),
            "p99": percentile(&samples, 0.99),
            "max": samples.last().copied().unwrap_or(0),
        }
    }))
}

