#![allow(clippy::doc_markdown, clippy::items_after_statements)]
//! Comandi CLI per il path bulk write plan-based (Arrow input):
//! `bulk-write` (`WriteOperation` JSON + input Arrow IPC) e
//! `postgres-write-ipc` (wrapper high-level SCHEMA/OBJECT/MODE).

use crate::ipc_input::IpcFileBatchStream;
use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliError, CliResult};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::{BatchStream, Provider};
use plenora_database_core::CancellationToken;
use serde_json::json;
use std::fs;

// ============================================================================
//  bulk-write DSN_ENV WRITE_OP.json INPUT.arrow [--dry-run]
// ============================================================================

pub(crate) async fn bulk_write(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let write_op_path = args.next().ok_or("manca il percorso WRITE_OP.json")?;
    let input_path = args.next().ok_or("manca il percorso INPUT.arrow")?;
    let dry_run = parse_dry_run(args)?;

    let contents = fs::read(&write_op_path)
        .map_err(|_| format!("WRITE_OP.json non leggibile: {write_op_path}"))?;
    let operation: WriteOperation = serde_json::from_slice(&contents).map_err(|e| {
        format!(
            "WRITE_OP.json non parsabile a riga {}, colonna {}",
            e.line(),
            e.column()
        )
    })?;

    let stream = IpcFileBatchStream::open(&input_path)?;
    execute_bulk_write(&dsn_env, operation, Box::new(stream), dry_run).await
}

// ============================================================================
//  postgres-write-ipc DSN_ENV SCHEMA OBJECT INPUT.arrow --mode=create|append|... [--dry-run]
// ============================================================================

pub(crate) async fn postgres_write_ipc(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let schema = args.next().ok_or("manca lo schema")?;
    let object = args.next().ok_or("manca l'oggetto")?;
    let input_path = args.next().ok_or("manca il percorso INPUT.arrow")?;

    // Flag opzionali: --mode=..., --keys=..., --update-columns=..., --dry-run
    let mut mode = WriteMode::Append;
    let mut keys: Vec<String> = Vec::new();
    let mut update_columns: Vec<String> = Vec::new();
    let mut dry_run = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--mode" => {
                let value = args.next().ok_or("--mode richiede un valore")?;
                mode = parse_write_mode(&value)?;
            }
            "--keys" => {
                let value = args.next().ok_or("--keys richiede una lista")?;
                keys = value.split(',').map(|s| s.trim().to_owned()).collect();
            }
            "--update-columns" => {
                let value = args.next().ok_or("--update-columns richiede una lista")?;
                update_columns = value.split(',').map(|s| s.trim().to_owned()).collect();
            }
            "--dry-run" => dry_run = true,
            other => {
                // Il nome dell'opzione non riconosciuta puo essere un
                // qualunque argomento fuori posto: SQL, una DSN, un token.
                let _ = other;
                return Err("opzione postgres-write-ipc sconosciuta".into());
            }
        }
    }

    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(schema),
            object,
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys,
        update_columns,
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let stream = IpcFileBatchStream::open(&input_path)?;
    execute_bulk_write(&dsn_env, operation, Box::new(stream), dry_run).await
}

// ============================================================================
//  Common execution path
// ============================================================================

async fn execute_bulk_write(
    dsn_env: &str,
    operation: WriteOperation,
    stream: Box<dyn BatchStream>,
    dry_run: bool,
) -> CliResult<()> {
    if dry_run {
        // Emette il piano che sarebbe applicato, senza toccare il DB.
        let schema = stream.schema();
        let fields: Vec<serde_json::Value> = schema
            .fields()
            .iter()
            .map(|f| {
                json!({
                    "name": f.name(),
                    "data_type": f.data_type().to_string(),
                    "nullable": f.is_nullable(),
                })
            })
            .collect();
        print_json(&json!({
            "status": "dry_run",
            "operation": operation,
            "input_schema": { "fields": fields },
        }))?;
        return Ok(());
    }

    let secret = secret_from_env(dsn_env)?;
    let provider = postgres_provider_for_pfm()?;
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let input_schema = stream.schema();
    let prepared = provider
        .prepare_write(&secret, &operation, input_schema, &budget, &cancel)
        .await?;
    let outcome = provider
        .write(&secret, prepared, stream, &budget, &cancel)
        .await?;

    print_json(&serde_json::to_value(&outcome).map_err(|error| {
        CliError::from(format!(
            "outcome non serializzabile: {}",
            error.classify() as u8
        ))
    })?)
}

fn parse_write_mode(value: &str) -> CliResult<WriteMode> {
    match value {
        "create" => Ok(WriteMode::Create),
        "append" => Ok(WriteMode::Append),
        "replace" => Ok(WriteMode::Replace),
        "truncate-insert" | "truncate_insert" => Ok(WriteMode::TruncateInsert),
        "update" => Ok(WriteMode::Update),
        "upsert" => Ok(WriteMode::Upsert),
        "delete-by-keys" | "delete_by_keys" => Ok(WriteMode::DeleteByKeys),
        _ => Err(
            "--mode sconosciuto: ammessi create, append, replace, truncate-insert, \
             update, upsert, delete-by-keys"
                .into(),
        ),
    }
}

fn parse_dry_run(args: &mut impl Iterator<Item = String>) -> CliResult<bool> {
    let rest: Vec<String> = args.by_ref().collect();
    if rest.is_empty() {
        return Ok(false);
    }
    if rest.len() == 1 && rest[0] == "--dry-run" {
        return Ok(true);
    }
    // Cio che resta puo essere qualunque cosa il chiamante abbia scritto:
    // il conteggio e azionabile, l'elenco sarebbe payload.
    Err(format!(
        "argomenti trailing non riconosciuti: {} oltre quelli previsti",
        rest.len()
    )
    .into())
}

/// Ensure `ensure_end` compatibility: consume l'iteratore. Definita qui per
/// avere una firma equivalente all'attesa del dispatcher.
#[allow(dead_code)]
fn ensure_end_local(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    ensure_end(args)
}
