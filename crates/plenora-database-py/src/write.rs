//! Bulk write via `Provider::prepare_write` + `Provider::write` (P3 v0.1.2).
//!
//! Espone `Session.copy_from(schema, table, ipc_bytes, mode, transaction_profile)`
//! e la sua variante async. Internamente il consumer Python passa un
//! buffer Arrow IPC stream (schema + N record batches + EOS) e questo
//! modulo lo deserializza, costruisce un `BatchStream` in-memory e chiama
//! il pattern `prepare_write` → `write` del `PostgresProvider`.
//!
//! Il provider Postgres usa COPY internamente per il bulk (append) via
//! `plenora_db_postgres::write`. Non c'è quindi bisogno di gestire COPY
//! direttamente qui: passiamo il `BatchStream` e il provider fa il resto
//! rispettando WriteMode / TransactionProfile / budget.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::redundant_pub_crate,
    clippy::too_many_arguments
)]

use crate::errors::to_py_err;
use crate::runtime;
use arrow_ipc::reader::StreamReader;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::{BatchStream, Provider, ProviderFuture, SecretString};
// Fase E: ResourceBudget/ResourceLimits ora consumati solo via `budget` module
use plenora_database_core::{CancellationToken, DatabaseError};
use plenora_db_postgres::PostgresProvider;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;

// ------------------------------ BatchStream helper ---------------------

pub(crate) struct VecBatchStream {
    pub(crate) schema: SchemaRef,
    pub(crate) batches: VecDeque<RecordBatch>,
    pub(crate) declared_rows: u64,
}

impl BatchStream for VecBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.declared_rows)
    }
}

// ------------------------------ Mode / profile parse -------------------

pub(crate) fn parse_mode(s: &str) -> Result<WriteMode, DatabaseError> {
    match s {
        "create" => Ok(WriteMode::Create),
        "append" => Ok(WriteMode::Append),
        "replace" => Ok(WriteMode::Replace),
        "truncate_insert" => Ok(WriteMode::TruncateInsert),
        "update" => Ok(WriteMode::Update),
        "upsert" => Ok(WriteMode::Upsert),
        "delete_by_keys" => Ok(WriteMode::DeleteByKeys),
        other => Err(DatabaseError::invalid_plan(format!(
            "mode sconosciuto '{other}': attesi \
             create/append/replace/truncate_insert/update/upsert/delete_by_keys"
        ))),
    }
}

pub(crate) fn parse_profile(s: &str) -> Result<TransactionProfile, DatabaseError> {
    match s {
        "read_only" => Ok(TransactionProfile::ReadOnly),
        "single_transaction" => Ok(TransactionProfile::SingleTransaction),
        "staged_swap" => Ok(TransactionProfile::StagedSwap),
        "chunk_committed" => Ok(TransactionProfile::ChunkCommitted),
        "best_effort_ddl" => Ok(TransactionProfile::BestEffortDdl),
        other => Err(DatabaseError::invalid_plan(format!(
            "transaction_profile sconosciuto '{other}': attesi \
             read_only/single_transaction/staged_swap/chunk_committed/best_effort_ddl"
        ))),
    }
}

pub(crate) fn parse_mapping_policy(s: &str) -> Result<MappingPolicy, DatabaseError> {
    match s {
        "strict" => Ok(MappingPolicy::Strict),
        "compatible" => Ok(MappingPolicy::Compatible),
        "lossy" => Ok(MappingPolicy::Lossy),
        "native" => Ok(MappingPolicy::Native),
        other => Err(DatabaseError::invalid_plan(format!(
            "mapping_policy sconosciuta '{other}': attesi \
             strict/compatible/lossy/native"
        ))),
    }
}

// ------------------------------ IPC decode -----------------------------

pub(crate) fn decode_ipc_stream(
    ipc_bytes: &[u8],
) -> Result<(SchemaRef, VecDeque<RecordBatch>, u64), DatabaseError> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| DatabaseError::invalid_plan(format!("Arrow IPC stream non valido: {e}")))?;
    let schema = reader.schema();
    let mut batches: VecDeque<RecordBatch> = VecDeque::new();
    let mut total_rows: u64 = 0;
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            DatabaseError::invalid_plan(format!("record batch Arrow IPC non valido: {e}"))
        })?;
        total_rows = total_rows.saturating_add(batch.num_rows() as u64);
        batches.push_back(batch);
    }
    Ok((schema, batches, total_rows))
}

// ------------------------------ WriteOutcome → Python dict -------------

fn outcome_to_pydict<'py>(py: Python<'py>, outcome: &WriteOutcome) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("schema_version", outcome.schema_version)?;
    d.set_item("status", format!("{:?}", outcome.status).to_lowercase())?;
    d.set_item("execution_id", outcome.execution_id.clone())?;
    d.set_item("provider", format!("{:?}", outcome.provider).to_lowercase())?;

    let rows = PyDict::new(py);
    rows.set_item("received", outcome.rows.received)?;
    rows.set_item("confirmed", outcome.rows.confirmed)?;
    rows.set_item("inserted", outcome.rows.inserted)?;
    rows.set_item("updated", outcome.rows.updated)?;
    rows.set_item("deleted", outcome.rows.deleted)?;
    rows.set_item("failed", outcome.rows.failed)?;
    rows.set_item("skipped", outcome.rows.skipped)?;
    d.set_item("rows", rows)?;

    if let Some(recovery) = &outcome.recovery {
        let rd = PyDict::new(py);
        rd.set_item(
            "last_certain_phase",
            format!("{:?}", recovery.last_certain_phase).to_lowercase(),
        )?;
        rd.set_item("automatic_retry_allowed", recovery.automatic_retry_allowed)?;
        rd.set_item("idempotency_key", recovery.idempotency_key.clone())?;
        rd.set_item("staging_object", recovery.staging_object.clone())?;
        rd.set_item("verification_action", recovery.verification_action.clone())?;
        d.set_item("recovery", rd)?;
    } else {
        d.set_item("recovery", py.None())?;
    }
    Ok(d)
}

// ------------------------------ Core bulk write ------------------------

pub(crate) fn make_operation(
    schema: &str,
    table: &str,
    mode: WriteMode,
    profile: TransactionProfile,
    mapping_policy: MappingPolicy,
    keys: Vec<String>,
    update_columns: Vec<String>,
) -> Result<WriteOperation, DatabaseError> {
    // Validazione early: i mode che richiedono chiavi rifiutano input vacuo
    // (invece di produrre SQL malformato più a valle).
    match mode {
        WriteMode::Upsert | WriteMode::Update | WriteMode::DeleteByKeys => {
            if keys.is_empty() {
                return Err(DatabaseError::invalid_plan(format!(
                    "mode '{}' richiede almeno una key column via keys=[...]",
                    match mode {
                        WriteMode::Upsert => "upsert",
                        WriteMode::Update => "update",
                        WriteMode::DeleteByKeys => "delete_by_keys",
                        _ => unreachable!(),
                    }
                )));
            }
        }
        _ => {
            if !keys.is_empty() {
                return Err(DatabaseError::invalid_plan(format!(
                    "keys=[...] è supportato solo per mode upsert/update/delete_by_keys, \
                     non per '{mode:?}'"
                )));
            }
            if !update_columns.is_empty() {
                return Err(DatabaseError::invalid_plan(format!(
                    "update_columns=[...] è supportato solo per mode update, \
                     non per '{mode:?}'"
                )));
            }
        }
    }
    Ok(WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: table.to_owned(),
        },
        mode,
        mapping_policy,
        transaction_profile: profile,
        keys,
        update_columns,
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    })
}

// Fase E: consolidato in `crate::budget::write_bulk_budget`. Il preset
// resta identico (rows 10M, mem 128 MiB, cell 4 MiB) — la ridefinizione
// per-modulo era il rischio, non il valore.
pub(crate) use crate::budget::write_bulk_budget as default_budget;

async fn do_copy_from_async(
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    schema_name: String,
    table_name: String,
    mode: WriteMode,
    profile: TransactionProfile,
    mapping_policy: MappingPolicy,
    keys: Vec<String>,
    update_columns: Vec<String>,
    ipc_bytes: Vec<u8>,
) -> Result<WriteOutcome, DatabaseError> {
    let (input_schema, batches, declared_rows) = decode_ipc_stream(&ipc_bytes)?;
    let stream = VecBatchStream {
        schema: Arc::clone(&input_schema),
        batches,
        declared_rows,
    };
    let operation = make_operation(
        &schema_name,
        &table_name,
        mode,
        profile,
        mapping_policy,
        keys,
        update_columns,
    )?;
    let budget = default_budget();
    let cancel = CancellationToken::new();
    let prepared = provider
        .prepare_write(&secret, &operation, input_schema, &budget, &cancel)
        .await?;
    let outcome = provider
        .write(&secret, prepared, Box::new(stream), &budget, &cancel)
        .await?;
    Ok(outcome)
}

// ------------------------------ Sync entrypoint ------------------------

/// Bulk write via `prepare_write` + `write`. Chiamato da `Session.copy_from`.
///
/// # Errors
///
/// `DatabaseError` in caso di IPC malformato, mode/profile invalidi o
/// errore del provider durante prepare/write.
#[allow(clippy::too_many_arguments)]
pub(crate) fn copy_from_sync(
    provider: &Arc<PostgresProvider>,
    secret: &SecretString,
    schema: &str,
    table: &str,
    ipc_bytes: &[u8],
    mode: &str,
    transaction_profile: &str,
    mapping_policy: &str,
    keys: Vec<String>,
    update_columns: Vec<String>,
) -> Result<WriteOutcome, DatabaseError> {
    let mode_enum = parse_mode(mode)?;
    let profile_enum = parse_profile(transaction_profile)?;
    let policy_enum = parse_mapping_policy(mapping_policy)?;
    let provider_arc = Arc::clone(provider);
    let secret_owned = secret.clone();
    let schema_owned = schema.to_owned();
    let table_owned = table.to_owned();
    let ipc_owned = ipc_bytes.to_vec();
    runtime().block_on(async move {
        do_copy_from_async(
            provider_arc,
            secret_owned,
            schema_owned,
            table_owned,
            mode_enum,
            profile_enum,
            policy_enum,
            keys,
            update_columns,
            ipc_owned,
        )
        .await
    })
}

// ------------------------------ Async entrypoint -----------------------

/// Bulk write async. Chiamato da `AsyncSession.acopy_from`.
///
/// # Errors
///
/// Come `copy_from_sync`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn copy_from_async(
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    schema: String,
    table: String,
    ipc_bytes: Vec<u8>,
    mode: String,
    transaction_profile: String,
    mapping_policy: String,
    keys: Vec<String>,
    update_columns: Vec<String>,
) -> Result<WriteOutcome, DatabaseError> {
    let mode_enum = parse_mode(&mode)?;
    let profile_enum = parse_profile(&transaction_profile)?;
    let policy_enum = parse_mapping_policy(&mapping_policy)?;
    do_copy_from_async(
        provider,
        secret,
        schema,
        table,
        mode_enum,
        profile_enum,
        policy_enum,
        keys,
        update_columns,
        ipc_bytes,
    )
    .await
}

// ------------------------------ Python outcome converter --------------

/// Converte un `WriteOutcome` in un Python dict con la stessa struttura
/// del JSON contract del core.
pub(crate) fn outcome_into_py<'py>(
    py: Python<'py>,
    outcome: &WriteOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    outcome_to_pydict(py, outcome)
}

/// Wrapper `Result<WriteOutcome, DatabaseError>` → `PyResult<PyDict>` per
/// uso comodo dagli entrypoint Session/AsyncSession.
pub(crate) fn wrap_outcome(
    py: Python<'_>,
    result: Result<WriteOutcome, DatabaseError>,
) -> PyResult<Bound<'_, PyDict>> {
    let outcome = result.map_err(to_py_err)?;
    outcome_into_py(py, &outcome)
}
