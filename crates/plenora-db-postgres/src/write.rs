#[cfg(test)]
use crate::field_contract::FieldContract;
use crate::{
    control::select_with_cancellation,
    error::{classify_error, public_error},
    metrics::PostgresMetrics,
    PostgresFaultPoint, PostgresInsertMode, PostgresNetworkOptions, PostgresPool,
    PostgresSchemaEvolution, PostgresSessionOptions, PostgresTlsConfig, PostgresTlsMode,
};
use arrow_array::{Array, BinaryArray, RecordBatch};
#[cfg(test)]
use arrow_array::{Date32Array, Int32Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::SchemaRef;
#[cfg(test)]
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::Bytes;
#[cfg(test)]
use bytes::BytesMut;
use futures_util::SinkExt;
use plenora_database_core::ewkb::{inspect_ewkb_detailed, EwkbGeometryMetadata};
use plenora_database_core::field_contract::validate_schema_contract;
#[cfg(test)]
use plenora_database_core::geometry::GEOARROW_WKB_EXTENSION_NAME;
use plenora_database_core::outcome::{RowCounts, WriteOutcome, WriteStatus};
use plenora_database_core::plan::{
    ObjectRef, ProviderKind, TransactionProfile, WriteMode, WriteOperation,
};
#[cfg(test)]
use plenora_database_core::protocol;
use plenora_database_core::provider::{BatchStream, PreparedWrite, SecretString};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
#[cfg(test)]
use plenora_database_core::{RemoteEffect, RetryDisposition};
use plenora_database_sql::Identifier;
#[cfg(test)]
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::ToSql;
#[cfg(test)]
use tokio_postgres::types::{IsNull, Type};
use tokio_postgres::Transaction;

pub mod binary_codec;
mod plan;
mod prepared_codec;
mod recovery;
mod resources;
mod row_diagnostics;
mod sql;
mod value_codec;

use binary_codec::binary_copy_value;
#[cfg(test)]
use binary_codec::{
    decimal_string, encode_numeric_binary, interval_text, parse_numeric_components,
    PostgresIntervalBinary,
};
use plan::WriteColumnPlan;
use prepared_codec::arrow_value;
use recovery::{
    cancel_backend, commit_interruption_error, interruption_category, interruption_message,
    unknown_write_outcome, PreCommitRecovery,
};
#[cfg(test)]
use recovery::{cancelled_write_error, resource_write_error};
use resources::reserve_write_batch;
#[cfg(test)]
use sql::placeholder_expression;
use sql::{quote_object, renderer, statement};
use value_codec::copy_buffer;
#[cfg(test)]
use value_codec::{append_quoted_postgres_value, encode_copy_value};

static EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub struct WriteRuntime {
    pub statement_timeout_ms: u64,
    pub lock_timeout_ms: u64,
    pub fault_point: Option<PostgresFaultPoint>,
    pub insert_mode: PostgresInsertMode,
    pub max_batch_bytes: u64,
    pub max_wkb_cell_bytes: u64,
    pub tls_mode: PostgresTlsMode,
    pub tls_config: PostgresTlsConfig,
    pub network_options: PostgresNetworkOptions,
    pub schema_evolution: PostgresSchemaEvolution,
    pub pool: Arc<PostgresPool>,
    pub metrics: Arc<PostgresMetrics>,
    pub pool_acquire_timeout_ms: u64,
}

pub struct PreparedPostgresWrite {
    column_plans: Vec<WriteColumnPlan>,
}

pub fn prepare_state(
    schema: &SchemaRef,
    operation: &WriteOperation,
) -> Result<PreparedPostgresWrite> {
    Ok(PreparedPostgresWrite {
        column_plans: compile_schema_plan(schema, operation)?,
    })
}

fn compile_schema_plan(
    schema: &SchemaRef,
    operation: &WriteOperation,
) -> Result<Vec<WriteColumnPlan>> {
    if schema.fields().is_empty() {
        return Err(DatabaseError::invalid_plan(
            "write PostgreSQL richiede almeno un campo",
        ));
    }
    validate_schema_contract(schema)?;
    for field in schema.fields() {
        Identifier::new(field.name().clone())?;
    }
    for name in operation.keys.iter().chain(&operation.update_columns) {
        if schema.field_with_name(name).is_err() {
            return Err(DatabaseError::invalid_plan(
                "chiave o colonna update assente nello schema Arrow",
            ));
        }
    }
    schema
        .fields()
        .iter()
        .map(|field| WriteColumnPlan::compile(field))
        .collect()
}

// The orchestration is intentionally kept in one place so transaction boundaries,
// target preparation and uncertain-commit reporting remain easy to audit.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    secret: &SecretString,
    mut prepared: PreparedWrite,
    mut input: Box<dyn BatchStream>,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    runtime: WriteRuntime,
) -> Result<WriteOutcome> {
    if cancellation.is_cancelled() {
        runtime.metrics.cancellation();
        return Err(public_error(
            interruption_category(cancellation),
            ErrorPhase::Write,
            false,
            interruption_message(cancellation),
        ));
    }
    let schema = input.schema();
    if schema.as_ref() != prepared.input_schema.as_ref() {
        return Err(DatabaseError::invalid_plan(
            "schema stream diverso dallo schema preparato",
        ));
    }
    let prepared_state = prepared
        .take_driver_state::<PreparedPostgresWrite>()
        .ok_or_else(|| {
            DatabaseError::invalid_plan("stato prepared write PostgreSQL assente o incompatibile")
        })?;
    let column_plans = prepared_state.column_plans;
    // Row-scoped diagnostics è supportata solo per Append + SingleTransaction
    // (l'unico scenario dove ha senso quarantinare righe: il target esiste
    // già e si stanno aggiungendo N righe di cui alcune possono fallire).
    // Per Create/Replace/TruncateInsert/Update/Upsert/DeleteByKeys si salta
    // il gate e si va al path normale.
    let diagnostic_input = if prepared.operation.mode == WriteMode::Append
        && prepared.operation.transaction_profile == TransactionProfile::SingleTransaction
    {
        input
            .declared_input_rows()
            .map(|input_total| {
                row_diagnostics::validate_input(
                    &prepared.input_schema,
                    &schema,
                    &prepared.operation,
                    input_total,
                    input.row_diagnostics_policy(),
                )
            })
            .transpose()?
    } else {
        None
    };
    let execution_id = format!(
        "pg-{}-{}",
        std::process::id(),
        EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let checkout = runtime.pool.checkout(
        secret,
        runtime.tls_mode,
        &runtime.tls_config,
        runtime.network_options,
        PostgresSessionOptions::new(runtime.statement_timeout_ms, runtime.lock_timeout_ms),
        runtime.pool_acquire_timeout_ms,
    );
    let mut client = if let Some(result) = select_with_cancellation(checkout, cancellation).await {
        result?
    } else {
        runtime.metrics.cancellation();
        return Err(public_error(
            interruption_category(cancellation),
            ErrorPhase::Connect,
            false,
            interruption_message(cancellation),
        ));
    };
    if client.was_reused() {
        match select_with_cancellation(client.client()?.batch_execute("DISCARD ALL"), cancellation)
            .await
        {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                client.invalidate();
                return Err(classify_error(ErrorPhase::Connect, &error));
            }
            None => {
                client.invalidate();
                runtime.metrics.cancellation();
                return Err(public_error(
                    interruption_category(cancellation),
                    ErrorPhase::Connect,
                    false,
                    interruption_message(cancellation),
                ));
            }
        }
        runtime.metrics.session_reset();
    }
    client.invalidate();
    let cancel_token = client.client()?.cancel_token();
    let transaction = if let Some(result) =
        select_with_cancellation(client.client_mut()?.transaction(), cancellation).await
    {
        result.map_err(|e| classify_error(ErrorPhase::Write, &e))?
    } else {
        runtime.metrics.cancellation();
        return Err(public_error(
            interruption_category(cancellation),
            ErrorPhase::Write,
            false,
            interruption_message(cancellation),
        ));
    };
    let recovery = PreCommitRecovery {
        cancellation,
        runtime: &runtime,
        cancel_token: &cancel_token,
        execution_id: &execution_id,
    };
    let operation = &prepared.operation;
    if let Some(diagnostic_input) = diagnostic_input {
        let result = row_diagnostics::execute(
            transaction,
            input.as_mut(),
            &schema,
            operation,
            &column_plans,
            budget,
            cancellation,
            &runtime,
            &cancel_token,
            &execution_id,
            diagnostic_input,
        )
        .await;
        if result
            .as_ref()
            .is_ok_and(|outcome| outcome.status == WriteStatus::Committed)
        {
            client.mark_reusable();
        }
        drop(client);
        return result;
    }
    if let Some(result) = select_with_cancellation(
        evolve_target_schema(
            &transaction,
            operation,
            &schema,
            &column_plans,
            runtime.schema_evolution,
        ),
        cancellation,
    )
    .await
    {
        if let Err(error) = result {
            return Err(recovery.rollback_error(transaction, error).await);
        }
    } else {
        return Err(recovery.rollback_cancellation(transaction).await);
    }
    if let Some(result) = select_with_cancellation(
        prepare_target(&transaction, operation, &schema, &column_plans),
        cancellation,
    )
    .await
    {
        if let Err(error) = result {
            return Err(recovery.rollback_error(transaction, error).await);
        }
    } else {
        return Err(recovery.rollback_cancellation(transaction).await);
    }
    let mut received = 0_u64;
    let mut confirmed = 0_u64;
    loop {
        let batch = if let Some(result) =
            select_with_cancellation(input.next_batch(cancellation), cancellation).await
        {
            match result {
                Ok(batch) => batch,
                Err(error) => {
                    return Err(recovery.rollback_error(transaction, error).await);
                }
            }
        } else {
            return Err(recovery.rollback_cancellation(transaction).await);
        };
        let Some(batch) = batch else {
            break;
        };
        if let Err(error) = validate_batch_schema(&batch, &schema) {
            return Err(recovery.rollback_error(transaction, error).await);
        }
        if cancellation.is_cancelled() {
            return Err(recovery.rollback_cancellation(transaction).await);
        }
        let resources = match reserve_write_batch(&batch, &column_plans, &runtime, budget) {
            Ok(resources) => resources,
            Err(error) => {
                return Err(recovery.rollback_resource_error(transaction, &error).await);
            }
        };
        let batch_rows = resources.rows;
        if batch_rows == 0 {
            continue;
        }
        let written = if let Some(result) = select_with_cancellation(
            write_batch(
                &transaction,
                operation,
                &operation.target,
                &batch,
                &column_plans,
                runtime.insert_mode,
            ),
            cancellation,
        )
        .await
        {
            match result {
                Ok(written) => written,
                Err(error) => {
                    return Err(recovery.rollback_error(transaction, error).await);
                }
            }
        } else {
            return Err(recovery.rollback_cancellation(transaction).await);
        };
        let accounting = resources.commit().and_then(|()| {
            received = received.checked_add(batch_rows).ok_or_else(|| {
                DatabaseError::resource_limit("overflow nel conteggio righe ricevute")
            })?;
            confirmed = confirmed.checked_add(written).ok_or_else(|| {
                DatabaseError::resource_limit("overflow nel conteggio righe confermate")
            })?;
            Ok(())
        });
        if let Err(error) = accounting {
            return Err(recovery.rollback_resource_error(transaction, &error).await);
        }
    }
    // Solo Create fabbrica la tabella, quindi solo Create puo aggiungerle un
    // indice: per ogni altra mode gli indici del target sono quelli che il
    // target aveva, e la scrittura non li tocca.
    if operation.create_spatial_index && operation.mode == WriteMode::Create {
        if let Some(result) = select_with_cancellation(
            create_spatial_indexes(&transaction, &operation.target, &schema, &column_plans),
            cancellation,
        )
        .await
        {
            if let Err(error) = result {
                return Err(recovery.rollback_error(transaction, error).await);
            }
        } else {
            return Err(recovery.rollback_cancellation(transaction).await);
        }
    }
    // Il documento si costruisce e si valida **prima** del commit, mentre il
    // rollback e ancora possibile. Un `Update` su chiavi non univoche puo
    // confermare piu righe di quante ne ha ricevute — una riga in ingresso ne
    // tocca molte nel target — e il contratto lo rifiuta: scoprirlo dopo il
    // commit lascerebbe il chiamante con un errore su dati gia scritti.
    let outcome = committed_outcome(operation.mode, &execution_id, received, confirmed);
    if let Err(error) = outcome.validate() {
        return Err(recovery
            .rollback_error(transaction, contract_violation(error, &execution_id))
            .await);
    }
    if runtime.fault_point == Some(PostgresFaultPoint::BeforeCommit) {
        let error = public_error(
            ErrorCategory::Transient,
            ErrorPhase::Commit,
            true,
            "fault injection prima del commit PostgreSQL",
        );
        return Err(recovery.rollback_error(transaction, error).await);
    }
    let commit_result = select_with_cancellation(transaction.commit(), cancellation).await;
    if commit_result.is_none() {
        runtime.metrics.write_outcome_unknown();
        cancel_backend(
            &cancel_token,
            runtime.tls_mode,
            runtime.tls_config.connector(),
            runtime.network_options.connect_timeout_ms,
        )
        .await;
        drop(client);
        return Err(commit_interruption_error(cancellation, &execution_id));
    }
    if commit_result.is_some_and(|result| result.is_err()) {
        runtime.metrics.write_outcome_unknown();
        drop(client);
        return Ok(unknown_write_outcome(
            execution_id,
            received,
            "verificare lo stato remoto prima di un retry",
        ));
    }
    if runtime.fault_point == Some(PostgresFaultPoint::AfterCommitAcknowledgement) {
        runtime.metrics.write_outcome_unknown();
        drop(client);
        return Ok(unknown_write_outcome(
            execution_id,
            received,
            "fault injection: verificare lo stato remoto già committed",
        ));
    }
    client.mark_reusable();
    drop(client);
    // Da qui in poi nulla puo fallire: il documento e gia costruito e
    // validato sopra, mentre il rollback era ancora possibile. Chi aggiunge
    // codice sotto questa riga deve mantenerlo infallibile — un `?` qui
    // produrrebbe un errore su dati gia scritti, e il chiamante non ha modo
    // di distinguerlo da una scrittura mai avvenuta.
    runtime.metrics.write_committed(confirmed);
    Ok(outcome)
}

/// Il documento di un write andato a buon fine, per mode.
fn committed_outcome(
    mode: WriteMode,
    execution_id: &str,
    received: u64,
    confirmed: u64,
) -> WriteOutcome {
    let (inserted, updated, deleted) = match mode {
        WriteMode::Create | WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert => {
            (Some(confirmed), Some(0), Some(0))
        }
        WriteMode::Update => (Some(0), Some(confirmed), Some(0)),
        // PostgreSQL does not report whether an ON CONFLICT row was inserted or
        // updated without adding observable side effects to the statement.
        WriteMode::Upsert => (None, None, Some(0)),
        WriteMode::DeleteByKeys => (Some(0), Some(0), Some(confirmed)),
    };
    WriteOutcome {
        schema_version: 2,
        status: WriteStatus::Committed,
        execution_id: execution_id.to_owned(),
        provider: ProviderKind::Postgres,
        rows: RowCounts {
            received,
            confirmed,
            inserted,
            updated,
            deleted,
            failed: 0,
            skipped: received.saturating_sub(confirmed),
        },
        recovery: None,
    }
}

/// Un documento che non rispetta il contratto, scoperto prima del commit.
///
/// La causa e nostra, non del server: la scrittura sarebbe riuscita, ma il
/// conteggio che pubblicheremmo e incoerente. Il rollback che segue lo rende
/// un fallimento pulito invece di dati scritti con un esito impresentabile.
fn contract_violation(mut error: DatabaseError, execution_id: &str) -> DatabaseError {
    error.category = ErrorCategory::Internal;
    error.phase = ErrorPhase::Write;
    error.provider = Some(ProviderKind::Postgres);
    error.execution_id = Some(execution_id.to_owned());
    error
}

fn validate_batch_schema(batch: &RecordBatch, declared: &SchemaRef) -> Result<()> {
    if Arc::ptr_eq(batch.schema_ref(), declared) || batch.schema_ref() == declared {
        return Ok(());
    }
    Err(DatabaseError::invalid_plan(
        "lo schema del batch write diverge dallo schema dichiarato dallo stream",
    ))
}

async fn evolve_target_schema(
    transaction: &Transaction<'_>,
    operation: &WriteOperation,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
    policy: PostgresSchemaEvolution,
) -> Result<()> {
    if policy != PostgresSchemaEvolution::AddNullableColumns
        || !matches!(
            operation.mode,
            WriteMode::Append
                | WriteMode::Replace
                | WriteMode::TruncateInsert
                | WriteMode::Update
                | WriteMode::Upsert
        )
    {
        return Ok(());
    }
    let renderer = renderer();
    for (field, plan) in schema.fields().iter().zip(plans) {
        let column = renderer.quote_identifier(&Identifier::new(field.name().clone())?)?;
        execute_sql(
            transaction,
            &format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {column} {}",
                quote_object(&operation.target)?,
                plan.postgres_type
            ),
            &[],
        )
        .await?;
    }
    Ok(())
}

fn enforce_input_limits(
    batch: &RecordBatch,
    plans: &[WriteColumnPlan],
    max_batch_bytes: u64,
    max_wkb_cell_bytes: u64,
    max_geometry_components: u64,
    max_geometry_depth: u64,
) -> Result<u64> {
    let bytes = batch
        .columns()
        .iter()
        .map(|array| u64::try_from(array.get_array_memory_size()).unwrap_or(u64::MAX))
        .sum::<u64>();
    if bytes > max_batch_bytes {
        return Err(public_error(
            ErrorCategory::ResourceLimit,
            ErrorPhase::Write,
            false,
            "RecordBatch write oltre max_batch_bytes",
        ));
    }
    let mut geometry_components = 0_u64;
    for (plan, array) in plans.iter().zip(batch.columns()) {
        if plan.is_spatial() {
            let binary = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(mapping_error)?;
            for row in 0..binary.len() {
                if !binary.is_null(row) {
                    let value = binary.value(row);
                    if u64::try_from(value.len()).unwrap_or(u64::MAX) > max_wkb_cell_bytes {
                        return Err(public_error(
                            ErrorCategory::ResourceLimit,
                            ErrorPhase::Write,
                            false,
                            "cella WKB write oltre max_wkb_cell_bytes",
                        ));
                    }
                    let remaining = max_geometry_components
                        .checked_sub(geometry_components)
                        .ok_or_else(|| {
                            DatabaseError::resource_limit("budget componenti geometriche esaurito")
                        })?;
                    if remaining == 0 {
                        return Err(DatabaseError::resource_limit(
                            "budget componenti geometriche esaurito",
                        ));
                    }
                    let inspection = inspect_ewkb_detailed(value, remaining, max_geometry_depth)?;
                    validate_ewkb_contract(inspection.root, plan)?;
                    geometry_components = geometry_components
                        .checked_add(inspection.stats.components)
                        .ok_or_else(|| {
                            DatabaseError::resource_limit("overflow componenti geometriche")
                        })?;
                }
            }
        }
    }
    Ok(geometry_components)
}

/// Porta il target nello stato che la mode richiede prima del bulk insert.
///
/// Ogni mode scrive nel target dichiarato dal piano: non esiste piu una
/// tabella di staging pubblicata al posto dell'originale. Replace svuota il
/// target con `DELETE FROM` dentro la stessa transazione del bulk insert,
/// quindi identita dell'oggetto, indici, foreign key, trigger, check, default,
/// grant e sequence restano quelli di prima — e un fallimento dopo il DELETE
/// riporta indietro anche le righe cancellate.
///
/// `TruncateInsert` conserva `TRUNCATE`: su `PostgreSQL` e transazionale, quindi
/// rollback-safe quanto il `DELETE` di Replace ma piu economico.
async fn prepare_target(
    transaction: &Transaction<'_>,
    operation: &WriteOperation,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
) -> Result<()> {
    match operation.mode {
        WriteMode::Create => {
            create_table(
                transaction,
                &operation.target,
                schema,
                plans,
                &operation.keys,
            )
            .await
        }
        WriteMode::Replace => execute_sql(
            transaction,
            &format!("DELETE FROM {}", quote_object(&operation.target)?),
            &[],
        )
        .await
        .map(|_| ()),
        WriteMode::TruncateInsert => execute_sql(
            transaction,
            &format!("TRUNCATE TABLE {}", quote_object(&operation.target)?),
            &[],
        )
        .await
        .map(|_| ()),
        WriteMode::Append | WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys => {
            Ok(())
        }
    }
}

async fn create_table(
    transaction: &Transaction<'_>,
    target: &ObjectRef,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
    keys: &[String],
) -> Result<()> {
    let renderer = renderer();
    let mut definitions = schema
        .fields()
        .iter()
        .zip(plans)
        .map(|(field, plan)| {
            let name = renderer.quote_identifier(&Identifier::new(field.name().clone())?)?;
            let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!("{name} {}{nullability}", plan.postgres_type))
        })
        .collect::<Result<Vec<_>>>()?;
    if !keys.is_empty() {
        let quoted: Result<Vec<String>> = keys
            .iter()
            .map(|key| {
                let ident = Identifier::new(key.clone())?;
                renderer.quote_identifier(&ident)
            })
            .collect();
        definitions.push(format!("PRIMARY KEY ({})", quoted?.join(", ")));
    }
    execute_sql(
        transaction,
        &format!(
            "CREATE TABLE {} ({})",
            quote_object(target)?,
            definitions.join(", ")
        ),
        &[],
    )
    .await?;
    Ok(())
}

async fn write_batch(
    transaction: &Transaction<'_>,
    operation: &WriteOperation,
    target: &ObjectRef,
    batch: &RecordBatch,
    plans: &[WriteColumnPlan],
    insert_mode: PostgresInsertMode,
) -> Result<u64> {
    if matches!(
        operation.mode,
        WriteMode::Create | WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert
    ) {
        match insert_mode {
            PostgresInsertMode::CopyText => {
                return copy_batch(transaction, target, batch, plans).await;
            }
            PostgresInsertMode::CopyBinary => {
                return copy_binary_batch(transaction, target, batch, plans).await;
            }
            PostgresInsertMode::Prepared => {}
        }
    }
    let (sql, indexes) = statement(operation, target, batch.schema_ref(), plans)?;
    // La classificazione conserva SQLSTATE e permette al chiamante di
    // distinguere, per esempio, una unique violation da un errore di
    // protocollo.
    let statement = transaction
        .prepare(&sql)
        .await
        .map_err(|e| classify_error(ErrorPhase::Prepare, &e))?;
    let mut affected = 0;
    for row in 0..batch.num_rows() {
        let values = indexes
            .iter()
            .map(|index| arrow_value(batch.column(*index).as_ref(), &plans[*index], row))
            .collect::<Result<Vec<_>>>()?;
        let refs = values
            .iter()
            .map(|value| value.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        affected += transaction
            .execute(&statement, &refs)
            .await
            .map_err(|e| classify_error(ErrorPhase::Write, &e))?;
    }
    Ok(affected)
}

async fn copy_binary_batch(
    transaction: &Transaction<'_>,
    target: &ObjectRef,
    batch: &RecordBatch,
    plans: &[WriteColumnPlan],
) -> Result<u64> {
    let renderer = renderer();
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let id = Identifier::new(field.name().clone())?;
            renderer.quote_identifier(&id)
        })
        .collect::<Result<Vec<_>>>()?;
    let type_probe = transaction
        .prepare(&format!(
            "SELECT {} FROM {} LIMIT 0",
            columns.join(", "),
            quote_object(target)?
        ))
        .await
        .map_err(|e| classify_error(ErrorPhase::Prepare, &e))?;
    let types = type_probe
        .columns()
        .iter()
        .map(|column| column.type_().clone())
        .collect::<Vec<_>>();
    let sink = transaction
        .copy_in(&format!(
            "COPY {} ({}) FROM STDIN BINARY",
            quote_object(target)?,
            columns.join(", ")
        ))
        .await
        .map_err(|e| classify_error(ErrorPhase::Prepare, &e))?;
    let writer = BinaryCopyInWriter::new(sink, &types);
    futures_util::pin_mut!(writer);
    for row in 0..batch.num_rows() {
        let values = batch
            .columns()
            .iter()
            .zip(plans)
            .zip(&types)
            .map(|((array, plan), target_type)| {
                binary_copy_value(array.as_ref(), plan, target_type, row)
            })
            .collect::<Result<Vec<_>>>()?;
        let refs = values
            .iter()
            .map(|value| value.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        writer
            .as_mut()
            .write(&refs)
            .await
            .map_err(|e| classify_error(ErrorPhase::Write, &e))?;
    }
    writer
        .as_mut()
        .finish()
        .await
        .map_err(|e| classify_error(ErrorPhase::Write, &e))
}

async fn copy_batch(
    transaction: &Transaction<'_>,
    target: &ObjectRef,
    batch: &RecordBatch,
    plans: &[WriteColumnPlan],
) -> Result<u64> {
    let renderer = renderer();
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let id = Identifier::new(field.name().clone())?;
            renderer.quote_identifier(&id)
        })
        .collect::<Result<Vec<_>>>()?;
    let sql = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
        quote_object(target)?,
        columns.join(", ")
    );
    let sink = transaction
        .copy_in(&sql)
        .await
        .map_err(|e| classify_error(ErrorPhase::Prepare, &e))?;
    futures_util::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(copy_buffer(batch, plans)?))
        .await
        .map_err(|e| classify_error(ErrorPhase::Write, &e))?;
    sink.as_mut()
        .finish()
        .await
        .map_err(|e| classify_error(ErrorPhase::Write, &e))
}

async fn create_spatial_indexes(
    transaction: &Transaction<'_>,
    target: &ObjectRef,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
) -> Result<()> {
    let renderer = renderer();
    for (field, plan) in schema.fields().iter().zip(plans) {
        if !plan.is_spatial() {
            continue;
        }
        let field_name = renderer.quote_identifier(&Identifier::new(field.name().clone())?)?;
        let index_raw = format!("{}_{}_gix", target.object, field.name());
        // NAMEDATALEN limita byte, non caratteri Unicode. Il troncamento deve
        // quindi rispettare i confini UTF-8 e aggiungere un hash del nome
        // completo per non far collidere prefissi uguali.
        let index_name_str = truncate_index_name_63_bytes(&index_raw);
        let index_name = renderer.quote_identifier(&Identifier::new(index_name_str)?)?;
        execute_sql(
            transaction,
            &format!(
                "CREATE INDEX {index_name} ON {} USING GIST ({field_name})",
                quote_object(target)?
            ),
            &[],
        )
        .await?;
    }
    Ok(())
}

/// Tronca un nome di indice a max 63 byte (limite Postgres NAMEDATALEN
/// default) preservando unicità tramite suffix hash-8 in base16 derivato
/// dal nome completo.
///
/// Contratto:
/// - Se input ≤ 63 byte → ritornato invariato.
/// - Se input > 63 byte → prefisso troncato al confine char + `_` + 8
///   caratteri hex del hash FNV-1a a 32 bit del nome originale intero.
///   Totale ≤ 63 byte.
///
/// **Hash stabile cross-version**: FNV-1a è algoritmo deterministico
/// specificato pubblicamente, non cambia mai. `std::collections::hash_map::
/// DefaultHasher` invece non è garantito stabile fra versioni Rust
/// (`SipHasher`/`SipHasher13`/altro). Un upgrade di toolchain avrebbe
/// generato nomi indice diversi per la stessa tabella, causando
/// duplicati/lost indices su re-run.
fn truncate_index_name_63_bytes(name: &str) -> String {
    const MAX_BYTES: usize = 63;
    // Riserva: 1 byte per '_' + 8 byte per hash hex = 9 byte suffix.
    const SUFFIX_LEN: usize = 9;
    const PREFIX_BUDGET: usize = MAX_BYTES - SUFFIX_LEN;
    if name.len() <= MAX_BYTES {
        return name.to_owned();
    }
    let mut prefix_end = 0;
    for (i, _) in name.char_indices() {
        if i > PREFIX_BUDGET {
            break;
        }
        prefix_end = i;
    }
    let suffix = format!("_{:08x}", fnv1a_32(name.as_bytes()));
    let mut out = String::with_capacity(MAX_BYTES);
    out.push_str(&name[..prefix_end]);
    out.push_str(&suffix);
    out
}

/// FNV-1a 32-bit deterministico e stabile cross-version.
///
/// Specifica: <http://www.isthe.com/chongo/tech/comp/fnv/>.
/// Offset basis `0x811c9dc5`, prime `0x0100_0193`.
const fn fnv1a_32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

#[cfg(test)]
#[path = "write_truncate_index_name_tests.rs"]
mod truncate_index_name_tests;

async fn execute_sql(
    transaction: &Transaction<'_>,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64> {
    // Anche DDL, swap di staging e indici spaziali conservano SQLSTATE per
    // distinguere i conflitti dagli errori di protocollo.
    transaction
        .execute(sql, params)
        .await
        .map_err(|e| classify_error(ErrorPhase::Write, &e))
}

fn validate_ewkb_contract(metadata: EwkbGeometryMetadata, plan: &WriteColumnPlan) -> Result<()> {
    if let Some(expected) = plan.dimensions.as_deref() {
        if expected != metadata.dimensions_label() {
            return Err(spatial_mapping_error(
                "dimensioni EWKB diverse dal contratto Arrow",
            ));
        }
    }
    let geometry_type = metadata
        .geometry_type_name()
        .ok_or_else(|| spatial_mapping_error("tipo geometry EWKB non supportato"))?;
    if let Some(expected) = plan.geometry_type.as_deref() {
        if expected != "Geometry" && !expected.eq_ignore_ascii_case(geometry_type) {
            return Err(spatial_mapping_error(
                "tipo EWKB diverso dal contratto Arrow",
            ));
        }
    }
    if let Some(expected) = plan.srid {
        if expected != 0 && metadata.srid != Some(expected) {
            return Err(spatial_mapping_error(
                "SRID EWKB diverso dal contratto Arrow",
            ));
        }
    }
    Ok(())
}

fn spatial_mapping_error(message: &str) -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        false,
        message,
    )
}

fn mapping_error() -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        false,
        "tipo Arrow non supportato dal writer PostgreSQL",
    )
}

fn temporal_range_error() -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        false,
        "valore temporale Arrow fuori dall'intervallo PostgreSQL/chrono supportato",
    )
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
