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
use plenora_database_core::plan::{ObjectRef, ProviderKind, WriteMode, WriteOperation};
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

pub fn validate_schema(schema: &SchemaRef, operation: &WriteOperation) -> Result<()> {
    compile_schema_plan(schema, operation).map(drop)
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
// replacement publication and uncertain-commit reporting remain easy to audit.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    secret: &SecretString,
    prepared: PreparedWrite,
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
    let column_plans = compile_schema_plan(&schema, &prepared.operation)?;
    let diagnostic_input = input
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
        .transpose()?;
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
        result.map_err(|_| {
            public_error(
                ErrorCategory::Protocol,
                ErrorPhase::Write,
                false,
                "avvio transazione PostgreSQL fallito",
            )
        })?
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
    let (write_target, replace_original) = if let Some(result) = select_with_cancellation(
        prepare_target(
            &transaction,
            operation,
            &schema,
            &column_plans,
            &execution_id,
        ),
        cancellation,
    )
    .await
    {
        match result {
            Ok(target) => target,
            Err(error) => {
                return Err(recovery.rollback_error(transaction, error).await);
            }
        }
    } else {
        return Err(recovery.rollback_cancellation(transaction).await);
    };
    let mut received = 0_u64;
    let mut confirmed = 0_u64;
    loop {
        let batch = if let Some(result) =
            select_with_cancellation(input.next_batch(), cancellation).await
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
                &write_target,
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
    if let Some(original) = replace_original {
        if let Some(result) = select_with_cancellation(
            publish_replacement(&transaction, &write_target, &original),
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
    if operation.create_spatial_index {
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
    let (inserted, updated, deleted) = match operation.mode {
        WriteMode::Create | WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert => {
            (Some(confirmed), Some(0), Some(0))
        }
        WriteMode::Update => (Some(0), Some(confirmed), Some(0)),
        // PostgreSQL does not report whether an ON CONFLICT row was inserted or
        // updated without adding observable side effects to the statement.
        WriteMode::Upsert => (None, None, Some(0)),
        WriteMode::DeleteByKeys => (Some(0), Some(0), Some(confirmed)),
    };
    let outcome = WriteOutcome {
        schema_version: 1,
        status: WriteStatus::Committed,
        execution_id,
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
        layer_outcomes: Vec::new(),
        recovery: None,
    };
    outcome.validate()?;
    runtime.metrics.write_committed(confirmed);
    Ok(outcome)
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
            WriteMode::Append | WriteMode::TruncateInsert | WriteMode::Update | WriteMode::Upsert
        )
    {
        return Ok(());
    }
    let renderer = renderer();
    for (field, plan) in schema.fields().iter().zip(plans) {
        let column = renderer.quote_identifier(&Identifier::new(field.name().clone())?);
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

async fn prepare_target(
    transaction: &Transaction<'_>,
    operation: &WriteOperation,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
    execution_id: &str,
) -> Result<(ObjectRef, Option<ObjectRef>)> {
    match operation.mode {
        WriteMode::Create => {
            create_table(
                transaction,
                &operation.target,
                schema,
                plans,
                &operation.keys,
            )
            .await?;
            Ok((operation.target.clone(), None))
        }
        WriteMode::Replace => {
            let mut staging = operation.target.clone();
            staging.object = format!(
                "__pln_{}_{}",
                operation.target.object.chars().take(36).collect::<String>(),
                execution_id.replace('-', "_")
            );
            create_table(transaction, &staging, schema, plans, &operation.keys).await?;
            Ok((staging, Some(operation.target.clone())))
        }
        WriteMode::TruncateInsert => {
            execute_sql(
                transaction,
                &format!("TRUNCATE TABLE {}", quote_object(&operation.target)?),
                &[],
            )
            .await?;
            Ok((operation.target.clone(), None))
        }
        WriteMode::Append | WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys => {
            Ok((operation.target.clone(), None))
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
            let name = renderer.quote_identifier(&Identifier::new(field.name().clone())?);
            let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!("{name} {}{nullability}", plan.postgres_type))
        })
        .collect::<Result<Vec<_>>>()?;
    if !keys.is_empty() {
        let quoted = keys
            .iter()
            .map(|key| {
                Identifier::new(key.clone())
                    .map(|identifier| renderer.quote_identifier(&identifier))
            })
            .collect::<Result<Vec<_>>>()?;
        definitions.push(format!("PRIMARY KEY ({})", quoted.join(", ")));
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
    let statement = transaction.prepare(&sql).await.map_err(|_| {
        public_error(
            ErrorCategory::Protocol,
            ErrorPhase::Prepare,
            false,
            "preparazione write PostgreSQL fallita",
        )
    })?;
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
        affected += transaction.execute(&statement, &refs).await.map_err(|_| {
            public_error(
                ErrorCategory::Protocol,
                ErrorPhase::Write,
                false,
                "esecuzione write PostgreSQL fallita",
            )
        })?;
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
        .map(|field| Identifier::new(field.name().clone()).map(|id| renderer.quote_identifier(&id)))
        .collect::<Result<Vec<_>>>()?;
    let type_probe = transaction
        .prepare(&format!(
            "SELECT {} FROM {} LIMIT 0",
            columns.join(", "),
            quote_object(target)?
        ))
        .await
        .map_err(|_| {
            public_error(
                ErrorCategory::Protocol,
                ErrorPhase::Prepare,
                false,
                "preparazione tipi COPY binario fallita",
            )
        })?;
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
        .map_err(|_| {
            public_error(
                ErrorCategory::Protocol,
                ErrorPhase::Prepare,
                false,
                "apertura COPY binario fallita",
            )
        })?;
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
        writer.as_mut().write(&refs).await.map_err(|_| {
            public_error(
                ErrorCategory::Protocol,
                ErrorPhase::Write,
                false,
                "riga COPY binario PostgreSQL fallita",
            )
        })?;
    }
    writer.as_mut().finish().await.map_err(|_| {
        public_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            false,
            "finalizzazione COPY binario fallita",
        )
    })
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
        .map(|field| Identifier::new(field.name().clone()).map(|id| renderer.quote_identifier(&id)))
        .collect::<Result<Vec<_>>>()?;
    let sql = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
        quote_object(target)?,
        columns.join(", ")
    );
    let sink = transaction.copy_in(&sql).await.map_err(|_| {
        public_error(
            ErrorCategory::Protocol,
            ErrorPhase::Prepare,
            false,
            "preparazione COPY PostgreSQL fallita",
        )
    })?;
    futures_util::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(copy_buffer(batch, plans)?))
        .await
        .map_err(|_| {
            public_error(
                ErrorCategory::Protocol,
                ErrorPhase::Write,
                false,
                "invio COPY PostgreSQL fallito",
            )
        })?;
    sink.as_mut().finish().await.map_err(|_| {
        public_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            false,
            "finalizzazione COPY PostgreSQL fallita",
        )
    })
}

async fn publish_replacement(
    transaction: &Transaction<'_>,
    staging: &ObjectRef,
    original: &ObjectRef,
) -> Result<()> {
    execute_sql(
        transaction,
        &format!("DROP TABLE IF EXISTS {}", quote_object(original)?),
        &[],
    )
    .await?;
    let renderer = renderer();
    let target_name = renderer.quote_identifier(&Identifier::new(original.object.clone())?);
    execute_sql(
        transaction,
        &format!(
            "ALTER TABLE {} RENAME TO {target_name}",
            quote_object(staging)?
        ),
        &[],
    )
    .await?;
    Ok(())
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
        let field_name = renderer.quote_identifier(&Identifier::new(field.name().clone())?);
        let index_raw = format!("{}_{}_gix", target.object, field.name());
        let index_name = renderer.quote_identifier(&Identifier::new(
            index_raw.chars().take(63).collect::<String>(),
        )?);
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

async fn execute_sql(
    transaction: &Transaction<'_>,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64> {
    transaction.execute(sql, params).await.map_err(|_| {
        public_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            false,
            "DDL/DML PostgreSQL fallita",
        )
    })
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
mod tests {
    use super::*;

    fn decode_numeric_binary(payload: &[u8]) -> (bool, u128, u16) {
        assert!(payload.len() >= 8);
        let digits = usize::from(u16::from_be_bytes([payload[0], payload[1]]));
        let weight = i16::from_be_bytes([payload[2], payload[3]]);
        let negative = u16::from_be_bytes([payload[4], payload[5]]) == 0x4000;
        let scale = u16::from_be_bytes([payload[6], payload[7]]);
        assert_eq!(payload.len(), 8 + digits * 2);
        let groups = payload[8..]
            .chunks_exact(2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
            .collect::<Vec<_>>();
        assert!(groups.iter().all(|group| *group < 10_000));
        if groups.is_empty() {
            return (false, 0, scale);
        }
        let group_at = |exponent: i16| -> u16 {
            let index = i32::from(weight) - i32::from(exponent);
            usize::try_from(index)
                .ok()
                .and_then(|index| groups.get(index))
                .copied()
                .unwrap_or(0)
        };
        let mut text = String::new();
        if weight < 0 {
            text.push('0');
        } else {
            for exponent in (0..=weight).rev() {
                let group = group_at(exponent);
                if exponent == weight {
                    write!(&mut text, "{group}").expect("format integer group");
                } else {
                    write!(&mut text, "{group:04}").expect("format integer group");
                }
            }
        }
        let fractional_groups = usize::from(scale).div_ceil(4);
        for index in 0..fractional_groups {
            let exponent = -i16::try_from(index).expect("fraction index") - 1;
            write!(&mut text, "{:04}", group_at(exponent)).expect("format fraction group");
        }
        text.truncate(text.len() - fractional_groups * 4 + usize::from(scale));
        (
            negative,
            text.parse::<u128>().expect("decoded numeric"),
            scale,
        )
    }

    #[test]
    fn batch_schema_drift_is_rejected_before_encoding() {
        let declared = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let matching = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )
        .expect("matching batch");
        validate_batch_schema(&matching, &declared).expect("stable schema");

        let equivalent = RecordBatch::try_new(
            Arc::new(declared.as_ref().clone()),
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )
        .expect("equivalent batch");
        validate_batch_schema(&equivalent, &declared).expect("structurally stable schema");

        let drifted = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "renamed",
            DataType::Int32,
            true,
        )]));
        let drifted_batch =
            RecordBatch::try_new(drifted, vec![Arc::new(Int32Array::from(vec![Some(1)]))])
                .expect("drifted batch");
        assert_eq!(
            validate_batch_schema(&drifted_batch, &declared)
                .expect_err("schema drift")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    #[test]
    fn arrow_temporal_extremes_return_mapping_errors_without_panicking() {
        let date_field = Field::new("date_value", DataType::Date32, true);
        for extreme in [i32::MIN, i32::MAX] {
            let date = Date32Array::from(vec![Some(extreme)]);
            let mut text = String::new();
            let plan = WriteColumnPlan::compile(&date_field).expect("date plan");
            let date_text_error =
                encode_copy_value(&mut text, &date, &plan, 0).expect_err("date text range");
            assert_eq!(date_text_error.category, ErrorCategory::DataMapping);
            let date_prepared_error =
                arrow_value(&date, &plan, 0).expect_err("date prepared range");
            assert_eq!(date_prepared_error.category, ErrorCategory::DataMapping);
        }

        for extreme in [i64::MIN, i64::MAX] {
            let timestamp = TimestampMicrosecondArray::from(vec![Some(extreme)]);
            for timezone in [None, Some("UTC".into())] {
                let field = Field::new(
                    "timestamp_value",
                    DataType::Timestamp(TimeUnit::Microsecond, timezone),
                    true,
                );
                let plan = WriteColumnPlan::compile(&field).expect("timestamp plan");
                let mut text = String::new();
                let text_error = encode_copy_value(&mut text, &timestamp, &plan, 0)
                    .expect_err("timestamp text range");
                assert_eq!(text_error.category, ErrorCategory::DataMapping);
                let error =
                    arrow_value(&timestamp, &plan, 0).expect_err("timestamp prepared range");
                assert_eq!(error.category, ErrorCategory::DataMapping);
            }
        }
    }

    #[test]
    fn ewkb_header_must_match_spatial_contract() {
        let field = Field::new("geom", DataType::Binary, false).with_metadata(
            std::collections::HashMap::from([
                (
                    "ARROW:extension:name".to_owned(),
                    GEOARROW_WKB_EXTENSION_NAME.to_owned(),
                ),
                ("plenora.geometry_type".to_owned(), "Point".to_owned()),
                ("plenora.dimensions".to_owned(), "xyz".to_owned()),
                ("plenora.srid".to_owned(), "4326".to_owned()),
            ]),
        );
        let mut point_z_4326 = vec![1_u8];
        point_z_4326.extend_from_slice(&0xa000_0001_u32.to_le_bytes());
        point_z_4326.extend_from_slice(&4326_u32.to_le_bytes());
        point_z_4326.extend_from_slice(&[0_u8; 24]);
        let plan = WriteColumnPlan::compile(&field).expect("spatial plan");
        let inspection = inspect_ewkb_detailed(&point_z_4326, 10, 1).expect("valid point Z EWKB");
        validate_ewkb_contract(inspection.root, &plan).expect("matching contract");

        let mut wrong_srid = point_z_4326.clone();
        wrong_srid[5..9].copy_from_slice(&3857_u32.to_le_bytes());
        let inspection = inspect_ewkb_detailed(&wrong_srid, 10, 1).expect("valid wrong-SRID EWKB");
        assert_eq!(
            validate_ewkb_contract(inspection.root, &plan)
                .expect_err("SRID mismatch")
                .category,
            ErrorCategory::DataMapping
        );

        let mut point_xy_4326 = vec![1_u8];
        point_xy_4326.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
        point_xy_4326.extend_from_slice(&4326_u32.to_le_bytes());
        point_xy_4326.extend_from_slice(&[0_u8; 16]);
        let inspection = inspect_ewkb_detailed(&point_xy_4326, 10, 1).expect("valid point XY EWKB");
        assert_eq!(
            validate_ewkb_contract(inspection.root, &plan)
                .expect_err("dimension mismatch")
                .category,
            ErrorCategory::DataMapping
        );
        assert!(inspect_ewkb_detailed(&[2, 0, 0, 0, 1], 10, 1).is_err());
    }

    #[test]
    fn decimal_format_is_exact() {
        assert_eq!(decimal_string(12_345, 2), "123.45");
        assert_eq!(decimal_string(-1, 2), "-0.01");
        assert_eq!(decimal_string(7, 0), "7");
        assert_eq!(decimal_string(123, -2), "12300");
    }

    #[test]
    fn numeric_binary_codec_is_deterministic_at_boundaries() {
        let cases = [
            (0, 0),
            (1, 0),
            (-1, 0),
            (12_345, 2),
            (-987_654_321, 6),
            (1, 18),
            (999_999_999_999_999_999, 0),
            (123, -2),
            (-123, -4),
            (i128::MIN, 0),
            (i128::MAX, 0),
        ];
        for (value, scale) in cases {
            let mut first = BytesMut::new();
            encode_numeric_binary(value, scale, &mut first).expect("numeric encoding");
            let mut second = BytesMut::new();
            encode_numeric_binary(value, scale, &mut second).expect("numeric encoding");
            assert_eq!(first, second);

            let (negative, decoded, decoded_scale) = decode_numeric_binary(&first);
            let expected_scale = u16::from(scale.max(0).unsigned_abs());
            let expected = if scale < 0 {
                value
                    .unsigned_abs()
                    .checked_mul(10_u128.pow(u32::from(scale.unsigned_abs())))
                    .expect("scaled expected value")
            } else {
                value.unsigned_abs()
            };
            assert_eq!(decoded_scale, expected_scale);
            assert_eq!(decoded, expected);
            assert_eq!(negative, value < 0);
        }
    }

    #[test]
    fn binary_copy_jsonb_accepts_canonical_native_type_metadata() {
        let field = Field::new("payload", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                protocol::POSTGRES_NATIVE_TYPE.to_owned(),
                "jsonb".to_owned(),
            )]),
        );
        let array = StringArray::from(vec![Some(r#"{"safe":true}"#)]);
        let plan = WriteColumnPlan::compile(&field).expect("JSONB plan");
        let value = binary_copy_value(&array, &plan, &Type::JSONB, 0).expect("JSONB binary value");
        let mut encoded = BytesMut::new();
        assert!(matches!(
            value
                .to_sql_checked(&Type::JSONB, &mut encoded)
                .expect("JSONB binary encoding"),
            IsNull::No
        ));
        assert_eq!(encoded.first(), Some(&1));
    }

    #[test]
    fn prepared_placeholders_accept_canonical_native_metadata() {
        let jsonb = Field::new("payload", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                protocol::POSTGRES_NATIVE_TYPE.to_owned(),
                "jsonb".to_owned(),
            )]),
        );
        let jsonb_plan = WriteColumnPlan::compile(&jsonb).expect("JSONB plan");
        assert_eq!(placeholder_expression(&jsonb_plan, 1), "$1::text::jsonb");

        let domain = Field::new("code", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([
                (
                    protocol::POSTGRES_NATIVE_DECLARATION.to_owned(),
                    "public.safe_code".to_owned(),
                ),
                (protocol::POSTGRES_TYPE_KIND.to_owned(), "d".to_owned()),
            ]),
        );
        let domain_plan = WriteColumnPlan::compile(&domain).expect("domain plan");
        assert_eq!(
            placeholder_expression(&domain_plan, 2),
            "$2::text::\"public\".\"safe_code\""
        );
    }

    #[test]
    fn numeric_text_parser_rejects_ambiguous_input() {
        assert_eq!(
            parse_numeric_components("+.5").expect("leading dot"),
            (5, 1)
        );
        assert_eq!(
            parse_numeric_components("1.").expect("trailing dot"),
            (1, 0)
        );
        for invalid in ["", "-", "+", "--1", "++1", "+-1", "1.2.3", " 1", "1e2"] {
            assert!(
                parse_numeric_components(invalid).is_err(),
                "accepted invalid numeric: {invalid}"
            );
        }
    }

    #[test]
    fn postgres_range_and_composite_escaping_is_exact() {
        let mut encoded = String::new();
        append_quoted_postgres_value(&mut encoded, "a,\"b\\c\n'tè");
        assert_eq!(encoded, "\"a,\\\"b\\\\c\n'tè\"");
    }

    #[test]
    fn interval_text_is_portable_across_postgres_versions() {
        assert_eq!(
            interval_text(&PostgresIntervalBinary {
                months: 0,
                days: 2,
                microseconds: 11_045_000_000,
            }),
            "0 mons 2 days 03:04:05.000000"
        );
        assert_eq!(
            interval_text(&PostgresIntervalBinary {
                months: -1,
                days: 0,
                microseconds: -3_723_000_004,
            }),
            "-1 mons 0 days -01:02:03.000004"
        );
    }
}
#[test]
fn cancellation_reports_verified_rollback_or_requires_recovery() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let rolled_back = cancelled_write_error(&cancellation, true);
    assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(rolled_back.retry, RetryDisposition::Never);

    let unknown = cancelled_write_error(&cancellation, false);
    assert_eq!(unknown.remote_effect, RemoteEffect::Unknown);
    assert_eq!(unknown.retry, RetryDisposition::RequiresRecovery);
}

#[test]
fn deadline_is_a_timeout_with_the_same_rollback_guarantees() {
    let deadline = CancellationToken::new();
    deadline.cancel_due_to_deadline();
    let rolled_back = cancelled_write_error(&deadline, true);
    assert_eq!(rolled_back.category, ErrorCategory::Timeout);
    assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(rolled_back.retry, RetryDisposition::Never);

    let commit = commit_interruption_error(&deadline, "pg-test-1");
    assert_eq!(commit.category, ErrorCategory::Timeout);
    assert_eq!(commit.phase, ErrorPhase::Commit);
    assert_eq!(commit.remote_effect, RemoteEffect::Unknown);
    assert_eq!(commit.retry, RetryDisposition::RequiresRecovery);
    assert_eq!(commit.execution_id.as_deref(), Some("pg-test-1"));
}

#[test]
fn resource_failure_reports_verified_rollback_or_requires_recovery() {
    let cause = DatabaseError::resource_limit("budget esaurito");
    let rolled_back = resource_write_error(&cause, true);
    assert_eq!(rolled_back.category, ErrorCategory::ResourceLimit);
    assert_eq!(rolled_back.phase, ErrorPhase::Write);
    assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(rolled_back.retry, RetryDisposition::Never);

    let unknown = resource_write_error(&cause, false);
    assert_eq!(unknown.remote_effect, RemoteEffect::Unknown);
    assert_eq!(unknown.retry, RetryDisposition::RequiresRecovery);
}

#[test]
fn unknown_write_outcome_is_valid_and_never_retryable() {
    let outcome = unknown_write_outcome(
        "pg-test-unknown".to_owned(),
        7,
        "verificare lo stato remoto",
    );
    outcome.validate().expect("valid unknown outcome");
    assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
    assert_eq!(outcome.rows.received, 7);
    assert_eq!(outcome.rows.confirmed, 0);
    assert!(
        !outcome
            .recovery
            .as_ref()
            .expect("recovery")
            .automatic_retry_allowed
    );
}

#[test]
fn divergent_canonical_and_legacy_metadata_is_rejected() {
    let field = Field::new("geom", DataType::Binary, false).with_metadata(
        [
            (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
            ("plenora.dimensions".to_owned(), "xyz".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let error = FieldContract::parse(&field).expect_err("metadata divergence");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}

#[test]
fn incoherent_crs_metadata_is_rejected_before_preflight() {
    let base = [
        (
            protocol::GEOARROW_EXTENSION_NAME.to_owned(),
            GEOARROW_WKB_EXTENSION_NAME.to_owned(),
        ),
        (
            protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
            "resolved".to_owned(),
        ),
        (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
        (protocol::GEOMETRY_CRS_ID.to_owned(), "EPSG:4326".to_owned()),
    ]
    .into_iter()
    .collect();
    let resolved_without_id = Field::new("geom", DataType::Binary, false).with_metadata(base);
    assert!(FieldContract::parse(&resolved_without_id).is_err());

    let missing_with_srid = Field::new("geom", DataType::Binary, false).with_metadata(
        [
            (
                protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                GEOARROW_WKB_EXTENSION_NAME.to_owned(),
            ),
            (
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "missing".to_owned(),
            ),
            (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    assert!(FieldContract::parse(&missing_with_srid).is_err());
}
