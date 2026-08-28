//! Bulk write via `Provider::prepare_write` e `Provider::write`.
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
use plenora_database_core::{CancellationToken, DatabaseError};
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::PyRuntimeError;
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
        // Il valore non entra nel messaggio: un argomento fuori posto puo
        // essere SQL, una DSN o un token, e questo testo diventa un'eccezione
        // Python, un log e a volte telemetria.
        _ => Err(DatabaseError::invalid_plan(
            "mode sconosciuto: attesi create, append, replace, truncate_insert, \
             update, upsert, delete_by_keys",
        )),
    }
}

pub(crate) fn parse_profile(s: &str) -> Result<TransactionProfile, DatabaseError> {
    match s {
        "read_only" => Ok(TransactionProfile::ReadOnly),
        "single_transaction" => Ok(TransactionProfile::SingleTransaction),
        "staged_swap" => Ok(TransactionProfile::StagedSwap),
        "chunk_committed" => Ok(TransactionProfile::ChunkCommitted),
        "best_effort_ddl" => Ok(TransactionProfile::BestEffortDdl),
        _ => Err(DatabaseError::invalid_plan(
            "transaction_profile sconosciuto: attesi read_only, single_transaction, \
             staged_swap, chunk_committed, best_effort_ddl",
        )),
    }
}

pub(crate) fn parse_mapping_policy(s: &str) -> Result<MappingPolicy, DatabaseError> {
    match s {
        "strict" => Ok(MappingPolicy::Strict),
        "compatible" => Ok(MappingPolicy::Compatible),
        "lossy" => Ok(MappingPolicy::Lossy),
        "native" => Ok(MappingPolicy::Native),
        _ => Err(DatabaseError::invalid_plan(
            "mapping_policy sconosciuta: attesi strict, compatible, lossy, native",
        )),
    }
}

// ------------------------------ IPC decode -----------------------------

pub(crate) fn decode_ipc_stream(
    ipc_bytes: &[u8],
) -> Result<(SchemaRef, VecDeque<RecordBatch>, u64), DatabaseError> {
    let cursor = Cursor::new(ipc_bytes);
    // Stessa regola dei messaggi sull'AST: `DatabaseError::message` non porta
    // payload, e un errore Arrow nomina volentieri colonne e valori.
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|_| DatabaseError::invalid_plan("Arrow IPC stream non valido"))?;
    let schema = reader.schema();
    let mut batches: VecDeque<RecordBatch> = VecDeque::new();
    let mut total_rows: u64 = 0;
    for batch_result in reader {
        let batch = batch_result.map_err(|_| {
            DatabaseError::invalid_plan(format!(
                "record batch Arrow IPC non valido: il {}o",
                batches.len() + 1
            ))
        })?;
        total_rows = total_rows.saturating_add(batch.num_rows() as u64);
        batches.push_back(batch);
    }
    Ok((schema, batches, total_rows))
}

// ------------------------------ WriteOutcome → Python dict -------------

/// L'esito di una scrittura come dizionario Python, **serializzato da Serde**.
///
/// Serde mantiene il dizionario Python identico al JSON del contratto.
/// `rename_all =
/// "snake_case"` e `skip_serializing_if` sono dichiarati una volta sul tipo, e
/// da li valgono per tutte le superfici.
fn outcome_to_pydict<'py>(py: Python<'py>, outcome: &WriteOutcome) -> PyResult<Bound<'py, PyDict>> {
    let value = serde_json::to_value(outcome).map_err(|error| {
        // Il messaggio non porta il documento: un esito puo contenere
        // identificatori di esecuzione e nomi di oggetti.
        PyRuntimeError::new_err(format!(
            "esito di scrittura non serializzabile nel contratto: {}",
            error.classify() as u8
        ))
    })?;
    let converted = crate::py_convert::json_to_python(py, &value)?;
    converted
        .cast_into::<PyDict>()
        .map_err(|_| PyRuntimeError::new_err("esito di scrittura che non serializza in un oggetto"))
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
    cancellation: CancellationToken,
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
    let prepared = provider
        .prepare_write(&secret, &operation, input_schema, &budget, &cancellation)
        .await?;
    let outcome = provider
        .write(&secret, prepared, Box::new(stream), &budget, &cancellation)
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
    cancellation: CancellationToken,
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
            cancellation,
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
    cancellation: CancellationToken,
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
        cancellation,
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
