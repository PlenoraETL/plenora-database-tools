#![allow(clippy::doc_markdown, clippy::items_after_statements, clippy::match_same_arms)]
//! Comandi CLI di ergonomia/debugging: `execute-scalar`, `conditional-update`,
//! `pool-status`, `explain`.

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliError, CliResult};
use plenora_database_core::facade::{
    execute_scalar_bool, execute_scalar_bytes, execute_scalar_date, execute_scalar_f64,
    execute_scalar_i32, execute_scalar_i64, execute_scalar_json, execute_scalar_string,
    execute_scalar_timestamp, execute_scalar_timestamptz, execute_scalar_uuid,
};
use plenora_database_core::provider::{ParameterValue, Provider};
use plenora_database_core::transaction::{ConditionalUpdate, Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use serde_json::{json, Value};

// ============================================================================
//  F5.9 — execute-scalar DSN_ENV SQL --type=TYPE [--param VALUE:TYPE ...]
// ============================================================================

pub(crate) async fn execute_scalar(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let collected: Vec<String> = args.by_ref().collect();
    let (rest, params) = crate::typed_params::strip_bind_params(collected)?;
    let mut iter = rest.into_iter();
    let dsn_env = iter.next().ok_or("manca variabile ambiente DSN")?;
    let sql = iter.next().ok_or("manca il SQL")?;
    let mut ty: Option<String> = None;
    for arg in iter {
        if let Some(v) = arg.strip_prefix("--type=") {
            ty = Some(v.to_owned());
        } else if arg == "--type" {
            return Err("--type richiede sintassi --type=TYPE (es. --type=i64)".into());
        } else {
            return Err(format!("argomento execute-scalar inatteso: {arg}").into());
        }
    }
    let ty = ty.ok_or("manca --type=TYPE (bool|i32|i64|f64|string|uuid|json|bytes|date|timestamp|timestamptz)")?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();
    let opts = TransactionOptions {
        context: crate::session_ctx::active(),
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(&secret, &opts, &budget, &cancel)
        .await?;
    let stmt = Statement::new(sql).with_params(params.into_inner());

    let value: Value = match ty.as_str() {
        "bool" => json!(execute_scalar_bool(tx.as_mut(), &stmt, &cancel).await?),
        "i32" | "int" => json!(execute_scalar_i32(tx.as_mut(), &stmt, &cancel).await?),
        "i64" | "bigint" => json!(execute_scalar_i64(tx.as_mut(), &stmt, &cancel).await?),
        "f64" | "float" | "double" => {
            json!(execute_scalar_f64(tx.as_mut(), &stmt, &cancel).await?)
        }
        "string" | "text" => json!(execute_scalar_string(tx.as_mut(), &stmt, &cancel).await?),
        "uuid" => json!(execute_scalar_uuid(tx.as_mut(), &stmt, &cancel).await?),
        "json" | "jsonb" => execute_scalar_json(tx.as_mut(), &stmt, &cancel).await?,
        "bytes" | "bytea" => {
            let b = execute_scalar_bytes(tx.as_mut(), &stmt, &cancel).await?;
            // base64 per non gonfiare l'output.
            use base64::Engine as _;
            json!({
                "encoding": "base64",
                "value": base64::engine::general_purpose::STANDARD.encode(b),
            })
        }
        "date" => json!(execute_scalar_date(tx.as_mut(), &stmt, &cancel).await?),
        "timestamp" => json!(execute_scalar_timestamp(tx.as_mut(), &stmt, &cancel).await?),
        "timestamptz" => json!(execute_scalar_timestamptz(tx.as_mut(), &stmt, &cancel).await?),
        other => {
            return Err(format!(
                "--type sconosciuto: {other} \
                 (bool|i32|i64|f64|string|uuid|json|bytes|date|timestamp|timestamptz)"
            )
            .into());
        }
    };

    let _ = tx.rollback(&cancel).await;
    print_json(&json!({ "status": "ok", "type": ty, "value": value }))
}

// ============================================================================
//  F5.10 — conditional-update DSN_ENV UPDATE_SQL PROBE_SQL EXPECTED [--param ...]
// ============================================================================

pub(crate) async fn conditional_update(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    // Sintassi: [--param X:TYPE ...] DSN_ENV UPDATE_SQL PROBE_SQL EXPECTED
    // I --param sono positional per l'UPDATE. Il PROBE riusa gli stessi
    // (compat con execute_conditional_update: key_probe è Statement, quindi
    // condivide i params). Per semplicità v1: stesso set di params.
    let collected: Vec<String> = args.by_ref().collect();
    let (rest, params) = crate::typed_params::strip_bind_params(collected)?;
    let mut iter = rest.into_iter();
    let dsn_env = iter.next().ok_or("manca variabile ambiente DSN")?;
    let update_sql = iter.next().ok_or("manca UPDATE_SQL")?;
    let probe_sql = iter.next().ok_or("manca PROBE_SQL")?;
    let expected_raw = iter.next().ok_or("manca EXPECTED_AFFECTED (u64)")?;
    ensure_end(&mut iter)?;
    let expected: u64 = expected_raw
        .parse()
        .map_err(|_| format!("EXPECTED_AFFECTED non valido: {expected_raw}"))?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();
    let opts = TransactionOptions {
        context: crate::session_ctx::active(),
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(&secret, &opts, &budget, &cancel)
        .await?;

    let params_vec: Vec<ParameterValue> = params.into_inner();
    let update = Statement::new(update_sql).with_params(params_vec.clone());
    let probe = Statement::new(probe_sql).with_params(params_vec);

    let outcome = tx
        .execute_conditional_update(
            ConditionalUpdate {
                update: &update,
                key_probe: Some(&probe),
                expected_affected_rows: expected,
            },
            &cancel,
        )
        .await;

    let (status, category, message) = match &outcome {
        Ok(()) => ("ok", String::new(), String::new()),
        Err(e) => (
            match e.category {
                plenora_database_core::ErrorCategory::ConcurrentModification => "concurrent",
                plenora_database_core::ErrorCategory::NotFound => "not_found",
                _ => "error",
            },
            format!("{:?}", e.category),
            e.message.clone(),
        ),
    };
    if outcome.is_ok() {
        tx.commit(&cancel).await?;
    } else {
        let _ = tx.rollback(&cancel).await;
    }
    print_json(&json!({
        "status": status,
        "error_category": category,
        "message": message,
        "expected_affected": expected,
    }))
}

// ============================================================================
//  F5.11 — pool-status DSN_ENV
// ============================================================================

pub(crate) async fn pool_status(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;
    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();

    // Il PostgresProvider espone probe_capabilities che include info pool.
    // Come surface additiva minima: forza una test_connection (verifica pool
    // acquire/release) e riporta se ha successo + timing.
    let cancel = CancellationToken::new();
    let start = std::time::Instant::now();
    let connection = provider.test_connection(&secret, &cancel).await;
    let elapsed_ms = start.elapsed().as_millis();

    // Nota: il pool interno non espone metriche pubbliche granulari
    // (deadpool-postgres::Status non è re-esportato dalla nostra API).
    // Aggiungeremo un getter con metrics_recorder in una futura milestone.
    let payload = match connection {
        Ok(info) => json!({
            "status": "ok",
            "acquire_ms": elapsed_ms,
            "provider": info.provider,
            "server_version": info.server_version,
            "connection_identity": info.connection_identity,
            "note": "metrics granulari pool (idle/busy/waiting) esposte in una futura milestone",
        }),
        Err(e) => json!({
            "status": "fail",
            "acquire_ms": elapsed_ms,
            "error": e,
        }),
    };
    print_json(&payload)
}

// ============================================================================
//  F5.12 — explain DSN_ENV SQL [--analyze] [--verbose] [--format=json|text]
// ============================================================================

pub(crate) async fn explain(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let collected: Vec<String> = args.by_ref().collect();
    let (rest, params) = crate::typed_params::strip_bind_params(collected)?;
    let mut iter = rest.into_iter();
    let dsn_env = iter.next().ok_or("manca variabile ambiente DSN")?;
    let sql = iter.next().ok_or("manca il SQL")?;

    let mut analyze = false;
    let mut verbose = false;
    let mut fmt = "text".to_owned();
    for arg in iter {
        match arg.as_str() {
            "--analyze" => analyze = true,
            "--verbose" => verbose = true,
            s if s.starts_with("--format=") => s.trim_start_matches("--format=").clone_into(&mut fmt),
            other => return Err(format!("opzione explain sconosciuta: {other}").into()),
        }
    }
    if !matches!(fmt.as_str(), "text" | "json" | "yaml" | "xml") {
        return Err(format!("--format inatteso: {fmt} (text|json|yaml|xml)").into());
    }

    // Assembla EXPLAIN [ANALYZE] [VERBOSE] (FORMAT ...) STATEMENT.
    let mut prefix = String::from("EXPLAIN (");
    if analyze {
        prefix.push_str("ANALYZE, ");
    }
    if verbose {
        prefix.push_str("VERBOSE, ");
    }
    prefix.push_str("FORMAT ");
    prefix.push_str(&fmt.to_uppercase());
    prefix.push_str(") ");
    let full_sql = prefix + &sql;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();
    let opts = TransactionOptions {
        context: crate::session_ctx::active(),
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(&secret, &opts, &budget, &cancel)
        .await?;
    let stmt = Statement::new(full_sql).with_params(params.into_inner());
    let rows = tx.query(&stmt, &cancel).await?;
    let _ = tx.rollback(&cancel).await;

    // Ogni riga è una linea di testo (o un JSON in caso di FORMAT JSON).
    let mut lines: Vec<Value> = Vec::with_capacity(rows.len());
    for r in &rows {
        let cell = r.get_index(0);
        match cell {
            Some(ParameterValue::String(s)) => {
                // Se format = json, prova a parsare il JSON.
                if fmt == "json" {
                    match serde_json::from_str::<Value>(s) {
                        Ok(v) => lines.push(v),
                        Err(_) => lines.push(Value::String(s.clone())),
                    }
                } else {
                    lines.push(Value::String(s.clone()));
                }
            }
            Some(other) => lines.push(
                serde_json::to_value(other).unwrap_or(Value::Null),
            ),
            None => lines.push(Value::Null),
        }
    }
    let output_key = if fmt == "json" && lines.len() == 1 {
        "plan_json"
    } else {
        "plan"
    };
    print_json(&json!({
        "status": "ok",
        "format": fmt,
        "analyze": analyze,
        "verbose": verbose,
        output_key: if lines.len() == 1 { lines.remove(0) } else { Value::Array(lines) },
    }))
}

// Placeholder for future ensure_end wiring
#[allow(dead_code)]
fn __ensure(_: CliError) {}
