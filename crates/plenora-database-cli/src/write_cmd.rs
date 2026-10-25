#![allow(clippy::doc_markdown, clippy::items_after_statements)]
//! Comandi CLI per il path bulk write plan-based (Arrow input):
//! `bulk-write` (`WriteOperation` JSON + input Arrow IPC) e
//! `postgres-write-ipc` (wrapper high-level SCHEMA/OBJECT/MODE).

use crate::ipc_input::IpcFileBatchStream;
use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliError, CliResult};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::SecretString;
use plenora_database_core::provider::{BatchStream, Provider};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase};
use serde_json::json;
use std::fs;

// ============================================================================
//  bulk-write DSN_ENV WRITE_OP.json INPUT.arrow [--dry-run [--offline]]
// ============================================================================

pub(crate) async fn bulk_write(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let write_op_path = args.next().ok_or("manca il percorso WRITE_OP.json")?;
    let input_path = args.next().ok_or("manca il percorso INPUT.arrow")?;
    let dry_run = parse_dry_run(args)?;

    // Il percorso e un argomento posizionale e non entra nel messaggio: in
    // quello slot puo finire una DSN o un token (`DatabaseError::message`).
    let contents = fs::read(&write_op_path)
        .map_err(|error| format!("WRITE_OP.json non leggibile: {}", error.kind()))?;
    let operation: WriteOperation = plenora_database_core::strict_json::from_slice(&contents)
        .map_err(|e| {
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
//  postgres-write-ipc DSN_ENV SCHEMA OBJECT INPUT.arrow [--mode create|append|...] [--dry-run [--offline]]
// ============================================================================

pub(crate) async fn postgres_write_ipc(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let schema = args.next().ok_or("manca lo schema")?;
    let object = args.next().ok_or("manca l'oggetto")?;
    let input_path = args.next().ok_or("manca il percorso INPUT.arrow")?;

    // Flag opzionali, ciascuno con il valore come argomento successivo (non
    // `--mode=...`, che si rifiuta come opzione sconosciuta): --mode, --keys,
    // --update-columns; e --dry-run, --offline.
    let mut mode = WriteMode::Append;
    let mut keys: Vec<String> = Vec::new();
    let mut update_columns: Vec<String> = Vec::new();
    let mut dry_run = false;
    let mut offline = false;
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
            "--offline" => offline = true,
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
    let dry_run = DryRun::from_flags(dry_run, offline)?;
    let stream = IpcFileBatchStream::open(&input_path)?;
    execute_bulk_write(&dsn_env, operation, Box::new(stream), dry_run).await
}

/// Il dry run chiesto: nessuno, con il catalogo del target, o offline.
///
/// Offline e soltanto la scelta esplicita di `--offline`. Fino al 25/10 una
/// variabile DSN nominata ma assente faceva cadere il dry run in offline
/// senza errore, e chi lo usava per decidere se scrivere leggeva `dry_run`
/// anche su un target che la scrittura avrebbe rifiutato.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DryRun {
    No,
    Catalog,
    Offline,
}

impl DryRun {
    fn from_flags(dry_run: bool, offline: bool) -> CliResult<Self> {
        match (dry_run, offline) {
            (false, false) => Ok(Self::No),
            (true, false) => Ok(Self::Catalog),
            (true, true) => Ok(Self::Offline),
            (false, true) => Err("--offline vale soltanto con --dry-run".into()),
        }
    }
}

/// La DSN del dry run con il catalogo: presente e non vuota.
fn dry_run_secret(dsn_env: &str) -> CliResult<SecretString> {
    require_dsn(std::env::var_os(dsn_env).as_deref())?;
    secret_from_env(dsn_env)
}

fn require_dsn(value: Option<&std::ffi::OsStr>) -> CliResult<()> {
    if value.is_none_or(std::ffi::OsStr::is_empty) {
        return Err(CliError::from(DatabaseError::new(
            ErrorCategory::InvalidConfiguration,
            ErrorPhase::Validate,
            Some(ProviderKind::Postgres),
            "variabile DSN assente o vuota: il dry run legge il catalogo del target, \
             senza catalogo si chiede con --offline",
        )));
    }
    Ok(())
}

// ============================================================================
//  Common execution path
// ============================================================================

async fn execute_bulk_write(
    dsn_env: &str,
    operation: WriteOperation,
    stream: Box<dyn BatchStream>,
    dry_run: DryRun,
) -> CliResult<()> {
    if dry_run != DryRun::No {
        // Il dry run decide cio che la scrittura deciderebbe, senza effetti
        // (`PostgresProvider::dry_run_write`): compila il piano, legge il
        // catalogo del bersaglio in sola lettura se la variabile DSN e
        // impostata, e giudica ogni valore di ogni batch con le funzioni
        // della scrittura. Fino al 24/10 compilava soltanto il piano: 13
        // vettori arrow-data-v1 che la scrittura rifiuta (valori, catalogo)
        // passavano il dry run con exit 0.
        //
        // Con il catalogo serve la DSN (`dry_run_secret`); offline solo con
        // `--offline`, e lo dice lo stato (`dry_run_offline`) oltre a
        // `checks`: le classi che decide `spatial_ref_sys` o il target non
        // sono state decise.
        let schema = stream.schema();
        let secret = match dry_run {
            DryRun::Catalog => Some(dry_run_secret(dsn_env)?),
            DryRun::No | DryRun::Offline => None,
        };
        let provider = postgres_provider_for_pfm()?;
        let budget = pfm_budget()?;
        let cancel = CancellationToken::new();
        let decided = provider
            .dry_run_write(secret.as_ref(), &operation, stream, &budget, &cancel)
            .await?;
        let loss_report = decided
            .loss_report
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| {
                CliError::from(format!(
                    "loss report non serializzabile: {}",
                    error.classify() as u8
                ))
            })?;
        let (status, catalog) = if secret.is_some() {
            ("dry_run", "performed")
        } else {
            ("dry_run_offline", "not_performed")
        };
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
            "status": status,
            "operation": operation,
            "input_schema": { "fields": fields },
            "input": { "batches": decided.batches, "rows": decided.rows },
            // Che cosa il dry run ha deciso. `catalog` comprende l'elenco
            // chiuso di cio che la scrittura chiede al target (schema,
            // privilegi, row-level security, colonne generate e identita,
            // viste, NOT NULL senza default, indice unico dell'upsert;
            // `plenora_db_postgres::write::target`); `row_security` dice se
            // una policy RLS decide sui valori (`not_performed`) o no;
            // `server` resta della scrittura: trigger, CHECK ed esclusioni,
            // unicita e foreign key sui valori, vincoli differiti,
            // conversioni della colonna, timeout e lock.
            "checks": {
                "plan": "performed",
                "values": "performed",
                "catalog": catalog,
                "row_security": decided.row_security.as_str(),
                "server": "not_performed",
            },
            "loss_report": loss_report,
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
    // Solo un commit certo e completo e un successo (SURF-014).
    let outcome = provider
        .write(&secret, prepared, stream, &budget, &cancel)
        .await?
        .settle()?;

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

fn parse_dry_run(args: &mut impl Iterator<Item = String>) -> CliResult<DryRun> {
    let rest: Vec<String> = args.by_ref().collect();
    match rest
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => return Ok(DryRun::No),
        ["--dry-run"] => return Ok(DryRun::Catalog),
        ["--dry-run", "--offline"] | ["--offline", "--dry-run"] => return Ok(DryRun::Offline),
        ["--offline"] => return DryRun::from_flags(false, true),
        _ => {}
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

#[cfg(test)]
#[path = "write_cmd_tests.rs"]
mod tests;
