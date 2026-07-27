use crate::{
    metrics::PostgresMetrics, public_error, public_error_envelope, PostgresFaultPoint,
    PostgresInsertMode, PostgresNetworkOptions, PostgresPool, PostgresSchemaEvolution,
    PostgresSessionOptions, PostgresTlsConfig, PostgresTlsMode,
};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, IntervalUnit, SchemaRef, TimeUnit};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures_util::SinkExt;
use plenora_database_core::ewkb::inspect_ewkb;
use plenora_database_core::geometry::GEOARROW_WKB_EXTENSION_NAME;
use plenora_database_core::outcome::{
    CertainPhase, Recovery, RowCounts, WriteOutcome, WriteStatus,
};
use plenora_database_core::plan::{ObjectRef, ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::protocol;
use plenora_database_core::provider::{BatchStream, PreparedWrite, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{to_sql_checked, IsNull, Kind, ToSql, Type};
use tokio_postgres::{CancelToken, NoTls, Transaction};

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
    if schema.fields().is_empty() {
        return Err(DatabaseError::invalid_plan(
            "write PostgreSQL richiede almeno un campo",
        ));
    }
    for field in schema.fields() {
        validate_metadata_coherence(field)?;
        validate_crs_metadata(field)?;
        Identifier::new(field.name().clone())?;
        pg_type(field)?;
    }
    for name in operation.keys.iter().chain(&operation.update_columns) {
        if schema.field_with_name(name).is_err() {
            return Err(DatabaseError::invalid_plan(
                "chiave o colonna update assente nello schema Arrow",
            ));
        }
    }
    Ok(())
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
    validate_schema(&schema, &prepared.operation)?;
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
        PostgresSessionOptions {
            statement_timeout_ms: runtime.statement_timeout_ms,
            lock_timeout_ms: runtime.lock_timeout_ms,
        },
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
        match select_with_cancellation(client.batch_execute("DISCARD ALL"), cancellation).await {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                client.invalidate();
                return Err(super::classify_error(ErrorPhase::Connect, &error));
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
    let cancel_token = client.cancel_token();
    let transaction =
        if let Some(result) = select_with_cancellation(client.transaction(), cancellation).await {
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
    let operation = &prepared.operation;
    if let Some(result) = select_with_cancellation(
        evolve_target_schema(&transaction, operation, &schema, runtime.schema_evolution),
        cancellation,
    )
    .await
    {
        result?;
    } else {
        runtime.metrics.cancellation();
        let rollback_confirmed = transaction.rollback().await.is_ok();
        return Err(cancelled_write_error(cancellation, rollback_confirmed));
    }
    let (write_target, replace_original) = if let Some(result) = select_with_cancellation(
        prepare_target(&transaction, operation, &schema, &execution_id),
        cancellation,
    )
    .await
    {
        result?
    } else {
        runtime.metrics.cancellation();
        let rollback_confirmed = transaction.rollback().await.is_ok();
        return Err(cancelled_write_error(cancellation, rollback_confirmed));
    };
    let mut received = 0_u64;
    let mut confirmed = 0_u64;
    loop {
        let batch = if let Some(result) =
            select_with_cancellation(input.next_batch(), cancellation).await
        {
            result?
        } else {
            runtime.metrics.cancellation();
            cancel_backend(
                &cancel_token,
                runtime.tls_mode,
                &runtime.tls_config.connector,
                runtime.network_options.connect_timeout_ms,
            )
            .await;
            let rollback_confirmed = transaction.rollback().await.is_ok();
            return Err(cancelled_write_error(cancellation, rollback_confirmed));
        };
        let Some(batch) = batch else {
            break;
        };
        if cancellation.is_cancelled() {
            runtime.metrics.cancellation();
            let rollback_confirmed = transaction.rollback().await.is_ok();
            return Err(cancelled_write_error(cancellation, rollback_confirmed));
        }
        let resources = match reserve_write_batch(&batch, &runtime, budget) {
            Ok(resources) => resources,
            Err(error) => {
                let rollback_confirmed = transaction.rollback().await.is_ok();
                return Err(resource_write_error(&error, rollback_confirmed));
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
                runtime.insert_mode,
            ),
            cancellation,
        )
        .await
        {
            result?
        } else {
            runtime.metrics.cancellation();
            cancel_backend(
                &cancel_token,
                runtime.tls_mode,
                &runtime.tls_config.connector,
                runtime.network_options.connect_timeout_ms,
            )
            .await;
            let rollback_confirmed = transaction.rollback().await.is_ok();
            return Err(cancelled_write_error(cancellation, rollback_confirmed));
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
            let rollback_confirmed = transaction.rollback().await.is_ok();
            return Err(resource_write_error(&error, rollback_confirmed));
        }
    }
    if let Some(original) = replace_original {
        if let Some(result) = select_with_cancellation(
            publish_replacement(&transaction, &write_target, &original),
            cancellation,
        )
        .await
        {
            result?;
        } else {
            runtime.metrics.cancellation();
            let rollback_confirmed = transaction.rollback().await.is_ok();
            return Err(cancelled_write_error(cancellation, rollback_confirmed));
        }
    }
    if operation.create_spatial_index {
        if let Some(result) = select_with_cancellation(
            create_spatial_indexes(&transaction, &operation.target, &schema),
            cancellation,
        )
        .await
        {
            result?;
        } else {
            runtime.metrics.cancellation();
            let rollback_confirmed = transaction.rollback().await.is_ok();
            return Err(cancelled_write_error(cancellation, rollback_confirmed));
        }
    }
    if runtime.fault_point == Some(PostgresFaultPoint::BeforeCommit) {
        return Err(public_error(
            ErrorCategory::Transient,
            ErrorPhase::Commit,
            true,
            "fault injection prima del commit PostgreSQL",
        ));
    }
    let commit_result = select_with_cancellation(transaction.commit(), cancellation).await;
    if commit_result.is_none() {
        runtime.metrics.write_outcome_unknown();
        cancel_backend(
            &cancel_token,
            runtime.tls_mode,
            &runtime.tls_config.connector,
            runtime.network_options.connect_timeout_ms,
        )
        .await;
        drop(client);
        return Err(commit_interruption_error(cancellation));
    }
    if commit_result.is_some_and(|result| result.is_err()) {
        runtime.metrics.write_outcome_unknown();
        drop(client);
        return Ok(WriteOutcome {
            schema_version: 1,
            status: WriteStatus::OutcomeUnknown,
            execution_id,
            provider: ProviderKind::Postgres,
            rows: RowCounts {
                received,
                confirmed: 0,
                inserted: None,
                updated: None,
                deleted: None,
                failed: 0,
                skipped: 0,
            },
            layer_outcomes: Vec::new(),
            recovery: Some(Recovery {
                last_certain_phase: CertainPhase::CommitOrEditRequested,
                automatic_retry_allowed: false,
                idempotency_key: None,
                staging_object: None,
                verification_action: Some(
                    "verificare lo stato remoto prima di un retry".to_owned(),
                ),
            }),
        });
    }
    if runtime.fault_point == Some(PostgresFaultPoint::AfterCommitAcknowledgement) {
        runtime.metrics.write_outcome_unknown();
        drop(client);
        return Ok(WriteOutcome {
            schema_version: 1,
            status: WriteStatus::OutcomeUnknown,
            execution_id,
            provider: ProviderKind::Postgres,
            rows: RowCounts {
                received,
                confirmed: 0,
                inserted: None,
                updated: None,
                deleted: None,
                failed: 0,
                skipped: 0,
            },
            layer_outcomes: Vec::new(),
            recovery: Some(Recovery {
                last_certain_phase: CertainPhase::CommitOrEditRequested,
                automatic_retry_allowed: false,
                idempotency_key: None,
                staging_object: None,
                verification_action: Some(
                    "fault injection: verificare lo stato remoto già committed".to_owned(),
                ),
            }),
        });
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

async fn select_with_cancellation<T, F>(future: F, cancellation: &CancellationToken) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => Some(result),
        _reason = cancellation.cancelled() => None,
    }
}

async fn cancel_backend(
    token: &CancelToken,
    tls_mode: PostgresTlsMode,
    tls_connector: &tokio_postgres_rustls::MakeRustlsConnect,
    timeout_ms: u64,
) {
    let cancellation = async {
        match tls_mode {
            PostgresTlsMode::Disabled => token.cancel_query(NoTls).await,
            PostgresTlsMode::Require => token.cancel_query(tls_connector.clone()).await,
        }
    };
    let _cancel_result =
        tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), cancellation).await;
}

fn cancelled_write_error(
    cancellation: &CancellationToken,
    rollback_confirmed: bool,
) -> DatabaseError {
    public_error_envelope(
        interruption_category(cancellation),
        ErrorPhase::Write,
        if rollback_confirmed {
            RemoteEffect::RolledBack
        } else {
            RemoteEffect::Unknown
        },
        if rollback_confirmed {
            RetryDisposition::Never
        } else {
            RetryDisposition::RequiresRecovery
        },
        interruption_message(cancellation),
    )
}

fn interruption_category(cancellation: &CancellationToken) -> ErrorCategory {
    if cancellation.reason() == Some(plenora_database_core::CancellationReason::Deadline) {
        ErrorCategory::Timeout
    } else {
        ErrorCategory::Cancelled
    }
}

fn interruption_message(cancellation: &CancellationToken) -> &'static str {
    if cancellation.reason() == Some(plenora_database_core::CancellationReason::Deadline) {
        "durata massima write PostgreSQL esaurita"
    } else {
        "write PostgreSQL cancellata sul server"
    }
}

fn commit_interruption_error(cancellation: &CancellationToken) -> DatabaseError {
    public_error_envelope(
        interruption_category(cancellation),
        ErrorPhase::Commit,
        RemoteEffect::Unknown,
        RetryDisposition::RequiresRecovery,
        "deadline o cancellazione durante commit PostgreSQL: verificare lo stato remoto",
    )
}

struct WriteBatchResources {
    rows: u64,
    bytes: u64,
    rows_lease: Option<ResourceLease>,
    output_lease: Option<ResourceLease>,
    memory_lease: Option<ResourceLease>,
    geometry_components: u64,
    geometry_lease: Option<ResourceLease>,
}

impl WriteBatchResources {
    const fn empty() -> Self {
        Self {
            rows: 0,
            bytes: 0,
            rows_lease: None,
            output_lease: None,
            memory_lease: None,
            geometry_components: 0,
            geometry_lease: None,
        }
    }

    fn commit(self) -> Result<()> {
        let (Some(rows_lease), Some(output_lease), Some(memory_lease)) =
            (self.rows_lease, self.output_lease, self.memory_lease)
        else {
            return Ok(());
        };
        rows_lease.commit(self.rows)?;
        output_lease.commit(self.bytes)?;
        drop(memory_lease);
        if self.geometry_components > 0 {
            self.geometry_lease
                .ok_or_else(|| DatabaseError::resource_limit("budget geometrico esaurito"))?
                .commit(self.geometry_components)?;
        }
        Ok(())
    }
}

fn reserve_write_batch(
    batch: &RecordBatch,
    runtime: &WriteRuntime,
    budget: &ResourceBudget,
) -> Result<WriteBatchResources> {
    let geometry_components = enforce_input_limits(
        batch,
        runtime
            .max_batch_bytes
            .min(budget.limits().memory_bytes)
            .min(budget.limits().output_bytes),
        runtime.max_wkb_cell_bytes.min(budget.limits().cell_bytes),
        budget.remaining(ResourceKind::GeometryComponents),
        budget.limits().nesting_depth,
    )?;
    let rows = u64::try_from(batch.num_rows())
        .map_err(|_| DatabaseError::resource_limit("batch oltre il conteggio supportato"))?;
    if rows == 0 {
        return Ok(WriteBatchResources::empty());
    }
    let bytes = batch
        .columns()
        .iter()
        .try_fold(0_u64, |total, array| {
            total.checked_add(u64::try_from(array.get_array_memory_size()).unwrap_or(u64::MAX))
        })
        .ok_or_else(|| DatabaseError::resource_limit("overflow nel conteggio byte del batch"))?;
    Ok(WriteBatchResources {
        rows,
        bytes,
        rows_lease: Some(budget.try_lease(ResourceKind::Rows, rows)?),
        output_lease: Some(budget.try_lease(ResourceKind::OutputBytes, bytes)?),
        memory_lease: Some(budget.try_lease(ResourceKind::MemoryBytes, bytes)?),
        geometry_components,
        geometry_lease: (geometry_components > 0)
            .then(|| budget.try_lease(ResourceKind::GeometryComponents, geometry_components))
            .transpose()?,
    })
}

fn resource_write_error(error: &DatabaseError, rollback_confirmed: bool) -> DatabaseError {
    public_error_envelope(
        ErrorCategory::ResourceLimit,
        ErrorPhase::Write,
        if rollback_confirmed {
            RemoteEffect::RolledBack
        } else {
            RemoteEffect::Unknown
        },
        if rollback_confirmed {
            RetryDisposition::Never
        } else {
            RetryDisposition::RequiresRecovery
        },
        &error.message,
    )
}

async fn evolve_target_schema(
    transaction: &Transaction<'_>,
    operation: &WriteOperation,
    schema: &SchemaRef,
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
    for field in schema.fields() {
        let column =
            renderer.quote_identifier(&Identifier::new(field.name().clone()).expect("validated"));
        execute_sql(
            transaction,
            &format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {column} {}",
                quote_object(&operation.target)?,
                pg_type(field)?
            ),
            &[],
        )
        .await?;
    }
    Ok(())
}

fn enforce_input_limits(
    batch: &RecordBatch,
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
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        if is_spatial(field) {
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
                    validate_ewkb_contract(value, field)?;
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
                    let stats = inspect_ewkb(value, remaining, max_geometry_depth)?;
                    geometry_components = geometry_components
                        .checked_add(stats.components)
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
    execution_id: &str,
) -> Result<(ObjectRef, Option<ObjectRef>)> {
    match operation.mode {
        WriteMode::Create => {
            create_table(transaction, &operation.target, schema, &operation.keys).await?;
            Ok((operation.target.clone(), None))
        }
        WriteMode::Replace => {
            let mut staging = operation.target.clone();
            staging.object = format!(
                "__pln_{}_{}",
                operation.target.object.chars().take(36).collect::<String>(),
                execution_id.replace('-', "_")
            );
            create_table(transaction, &staging, schema, &operation.keys).await?;
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
    keys: &[String],
) -> Result<()> {
    let renderer = renderer();
    let mut definitions = schema
        .fields()
        .iter()
        .map(|field| {
            let name = renderer.quote_identifier(&Identifier::new(field.name().clone())?);
            let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!("{name} {}{nullability}", pg_type(field)?))
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
    insert_mode: PostgresInsertMode,
) -> Result<u64> {
    if matches!(
        operation.mode,
        WriteMode::Create | WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert
    ) {
        match insert_mode {
            PostgresInsertMode::CopyText => return copy_batch(transaction, target, batch).await,
            PostgresInsertMode::CopyBinary => {
                return copy_binary_batch(transaction, target, batch).await;
            }
            PostgresInsertMode::Prepared => {}
        }
    }
    let (sql, indexes) = statement(operation, target, batch.schema_ref())?;
    let statement = transaction.prepare(&sql).await.map_err(|_| {
        public_error(
            ErrorCategory::Protocol,
            ErrorPhase::Prepare,
            false,
            "preparazione write PostgreSQL fallita",
        )
    })?;
    let mut affected = 0;
    let batch_schema = batch.schema();
    for row in 0..batch.num_rows() {
        let values = indexes
            .iter()
            .map(|index| {
                arrow_value(
                    batch.column(*index).as_ref(),
                    batch_schema.field(*index),
                    row,
                )
            })
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
    let schema = batch.schema();
    for row in 0..batch.num_rows() {
        let values = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .zip(&types)
            .map(|((array, field), target_type)| {
                binary_copy_value(array.as_ref(), field, target_type, row)
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
        .send(Bytes::from(copy_buffer(batch)?))
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

fn copy_buffer(batch: &RecordBatch) -> Result<Vec<u8>> {
    let schema = batch.schema();
    let mut output = String::new();
    for row in 0..batch.num_rows() {
        for column in 0..batch.num_columns() {
            if column > 0 {
                output.push('\t');
            }
            let array = batch.column(column).as_ref();
            if array.is_null(row) {
                output.push_str("\\N");
            } else {
                encode_copy_value(&mut output, array, schema.field(column), row)?;
            }
        }
        output.push('\n');
    }
    Ok(output.into_bytes())
}

#[allow(clippy::too_many_lines)]
fn encode_copy_value(
    output: &mut String,
    array: &dyn Array,
    field: &Field,
    row: usize,
) -> Result<()> {
    macro_rules! typed {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(mapping_error)?
        };
    }
    match field.data_type() {
        DataType::Boolean => output.push_str(if typed!(BooleanArray).value(row) {
            "t"
        } else {
            "f"
        }),
        DataType::Int32 => {
            write!(output, "{}", typed!(Int32Array).value(row)).expect("String write");
        }
        DataType::Int64 => {
            write!(output, "{}", typed!(Int64Array).value(row)).expect("String write");
        }
        DataType::Float32 => encode_float(output, f64::from(typed!(Float32Array).value(row))),
        DataType::Float64 => encode_float(output, typed!(Float64Array).value(row)),
        DataType::Utf8 => escape_copy_text(output, typed!(StringArray).value(row)),
        DataType::Binary => {
            let bytes = typed!(BinaryArray).value(row);
            if !is_spatial(field) {
                // COPY consumes one escaping layer before the bytea parser sees
                // the canonical PostgreSQL `\x` hexadecimal representation.
                output.push_str("\\\\x");
            }
            encode_hex(output, bytes);
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let value = epoch
                .checked_add_signed(Duration::days(i64::from(typed!(Date32Array).value(row))))
                .ok_or_else(temporal_range_error)?;
            write!(output, "{value}").expect("String write");
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let value = time_from_microseconds(typed!(Time64MicrosecondArray).value(row))?;
            write!(output, "{value}").expect("String write");
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = postgres_interval(typed!(IntervalMonthDayNanoArray), row)?;
            output.push_str(&interval_text(&value));
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let instant = DateTime::<Utc>::from_timestamp_micros(
                typed!(TimestampMicrosecondArray).value(row),
            )
            .ok_or_else(temporal_range_error)?;
            if timezone.is_some() {
                write!(output, "{}", instant.to_rfc3339()).expect("String write");
            } else {
                write!(output, "{}", instant.naive_utc()).expect("String write");
            }
        }
        DataType::Decimal128(_, scale) => {
            output.push_str(&decimal_string(typed!(Decimal128Array).value(row), *scale));
        }
        DataType::List(_) => {
            let value = list_string(typed!(ListArray), row)?;
            escape_copy_text(output, &value);
        }
        DataType::Struct(_) if is_range_field(field) => {
            let value = range_string(typed!(StructArray), row)?;
            escape_copy_text(output, &value);
        }
        DataType::Struct(_) if is_composite_field(field) => {
            let value = composite_string(typed!(StructArray), row)?;
            escape_copy_text(output, &value);
        }
        _ => return Err(mapping_error()),
    }
    Ok(())
}

fn encode_float(output: &mut String, value: f64) {
    if value.is_nan() {
        output.push_str("NaN");
    } else if value == f64::INFINITY {
        output.push_str("Infinity");
    } else if value == f64::NEG_INFINITY {
        output.push_str("-Infinity");
    } else {
        write!(output, "{value}").expect("String write");
    }
}

fn escape_copy_text(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\t' => output.push_str("\\t"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            _ => output.push(character),
        }
    }
}

fn time_from_microseconds(value: i64) -> Result<NaiveTime> {
    const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
    const SECONDS_PER_DAY: i64 = 86_400;
    if !(0..SECONDS_PER_DAY * MICROSECONDS_PER_SECOND).contains(&value) {
        return Err(mapping_error());
    }
    let seconds = u32::try_from(value / MICROSECONDS_PER_SECOND).map_err(|_| mapping_error())?;
    let nanoseconds =
        u32::try_from(value % MICROSECONDS_PER_SECOND).map_err(|_| mapping_error())? * 1_000;
    NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds).ok_or_else(mapping_error)
}

fn list_string(array: &ListArray, row: usize) -> Result<String> {
    let values = array.value(row);
    let mut output = String::from("{");
    for index in 0..values.len() {
        if index > 0 {
            output.push(',');
        }
        if values.is_null(index) {
            output.push_str("NULL");
        } else {
            append_array_item(&mut output, values.as_ref(), index)?;
        }
    }
    output.push('}');
    Ok(output)
}

fn append_array_item(output: &mut String, values: &dyn Array, index: usize) -> Result<()> {
    macro_rules! value {
        ($array:ty) => {
            values
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(mapping_error)?
                .value(index)
        };
    }
    match values.data_type() {
        DataType::Boolean => output.push_str(if value!(BooleanArray) { "t" } else { "f" }),
        DataType::Int32 => write!(output, "{}", value!(Int32Array)).expect("String write"),
        DataType::Int64 => write!(output, "{}", value!(Int64Array)).expect("String write"),
        DataType::Float32 => encode_float(output, f64::from(value!(Float32Array))),
        DataType::Float64 => encode_float(output, value!(Float64Array)),
        DataType::Utf8 => append_quoted_array_string(output, value!(StringArray)),
        _ => return Err(mapping_error()),
    }
    Ok(())
}

fn append_quoted_array_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        if matches!(character, '\\' | '"') {
            output.push('\\');
        }
        output.push(character);
    }
    output.push('"');
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
struct RangeValue {
    lower: Option<String>,
    upper: Option<String>,
    lower_inclusive: bool,
    upper_inclusive: bool,
    lower_unbounded: bool,
    upper_unbounded: bool,
    empty: bool,
}

fn range_value(array: &StructArray, row: usize) -> Result<RangeValue> {
    let string_value = |name: &str| -> Result<Option<String>> {
        let values = array
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(mapping_error)?;
        Ok((!values.is_null(row)).then(|| values.value(row).to_owned()))
    };
    let bool_value = |name: &str| -> Result<bool> {
        let values = array
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(mapping_error)?;
        if values.is_null(row) {
            return Err(mapping_error());
        }
        Ok(values.value(row))
    };
    Ok(RangeValue {
        lower: string_value("lower")?,
        upper: string_value("upper")?,
        lower_inclusive: bool_value("lower_inclusive")?,
        upper_inclusive: bool_value("upper_inclusive")?,
        lower_unbounded: bool_value("lower_unbounded")?,
        upper_unbounded: bool_value("upper_unbounded")?,
        empty: bool_value("empty")?,
    })
}

fn range_string(array: &StructArray, row: usize) -> Result<String> {
    let value = range_value(array, row)?;
    if value.empty {
        return Ok("empty".to_owned());
    }
    if (!value.lower_unbounded && value.lower.is_none())
        || (!value.upper_unbounded && value.upper.is_none())
    {
        return Err(mapping_error());
    }
    let mut output = String::new();
    output.push(if value.lower_inclusive { '[' } else { '(' });
    if !value.lower_unbounded {
        append_quoted_range_bound(
            &mut output,
            value.lower.as_deref().ok_or_else(mapping_error)?,
        );
    }
    output.push(',');
    if !value.upper_unbounded {
        append_quoted_range_bound(
            &mut output,
            value.upper.as_deref().ok_or_else(mapping_error)?,
        );
    }
    output.push(if value.upper_inclusive { ']' } else { ')' });
    Ok(output)
}

fn append_quoted_range_bound(output: &mut String, value: &str) {
    append_quoted_postgres_value(output, value);
}

fn append_quoted_postgres_value(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        if matches!(character, '\\' | '"') {
            output.push('\\');
        }
        output.push(character);
    }
    output.push('"');
}

fn composite_value(array: &StructArray, row: usize) -> Result<Vec<Option<String>>> {
    array
        .columns()
        .iter()
        .map(|column| {
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mapping_error)?;
            Ok((!values.is_null(row)).then(|| values.value(row).to_owned()))
        })
        .collect()
}

fn composite_string(array: &StructArray, row: usize) -> Result<String> {
    let values = composite_value(array, row)?;
    let mut output = String::from("(");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        if let Some(value) = value {
            append_quoted_postgres_value(&mut output, value);
        }
    }
    output.push(')');
    Ok(output)
}

fn encode_hex(output: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
}

// Keeping all statement shapes together makes placeholder ordering and field
// indexing reviewable across the seven supported write modes.
#[allow(clippy::too_many_lines)]
fn statement(
    operation: &WriteOperation,
    target: &ObjectRef,
    schema: &SchemaRef,
) -> Result<(String, Vec<usize>)> {
    let renderer = renderer();
    let field_index = |name: &str| {
        schema
            .index_of(name)
            .map_err(|_| DatabaseError::invalid_plan("campo write assente nello schema Arrow"))
    };
    match operation.mode {
        WriteMode::Update => {
            let mut indexes = operation
                .update_columns
                .iter()
                .map(|name| field_index(name))
                .collect::<Result<Vec<_>>>()?;
            indexes.extend(
                operation
                    .keys
                    .iter()
                    .map(|name| field_index(name))
                    .collect::<Result<Vec<_>>>()?,
            );
            let sets = operation
                .update_columns
                .iter()
                .enumerate()
                .map(|(position, name)| {
                    let field = schema.field(field_index(name)?);
                    let value = placeholder_expression(field, position + 1);
                    let identifier = Identifier::new(name.clone())?;
                    Ok(format!(
                        "{} = {value}",
                        renderer.quote_identifier(&identifier)
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let predicates = key_predicates(
                &renderer,
                &operation.keys,
                operation.update_columns.len() + 1,
            )?;
            Ok((
                format!(
                    "UPDATE {} SET {} WHERE {}",
                    quote_object(target)?,
                    sets.join(", "),
                    predicates
                ),
                indexes,
            ))
        }
        WriteMode::DeleteByKeys => Ok((
            format!(
                "DELETE FROM {} WHERE {}",
                quote_object(target)?,
                key_predicates(&renderer, &operation.keys, 1)?
            ),
            operation
                .keys
                .iter()
                .map(|name| field_index(name))
                .collect::<Result<Vec<_>>>()?,
        )),
        _ => {
            let columns = schema
                .fields()
                .iter()
                .map(|field| {
                    Identifier::new(field.name().clone()).map(|id| renderer.quote_identifier(&id))
                })
                .collect::<Result<Vec<_>>>()?;
            let values = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| placeholder_expression(field, index + 1))
                .collect::<Vec<_>>();
            let conflict = if operation.mode == WriteMode::Upsert {
                let keys = operation
                    .keys
                    .iter()
                    .map(|key| {
                        Identifier::new(key.clone()).map(|id| renderer.quote_identifier(&id))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let updates = schema
                    .fields()
                    .iter()
                    .filter(|field| !operation.keys.contains(field.name()))
                    .map(|field| {
                        let name = renderer.quote_identifier(
                            &Identifier::new(field.name().clone()).expect("validated"),
                        );
                        format!("{name} = EXCLUDED.{name}")
                    })
                    .collect::<Vec<_>>();
                if updates.is_empty() {
                    format!(" ON CONFLICT ({}) DO NOTHING", keys.join(", "))
                } else {
                    format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        keys.join(", "),
                        updates.join(", ")
                    )
                }
            } else {
                String::new()
            };
            Ok((
                format!(
                    "INSERT INTO {} ({}) VALUES ({}){conflict}",
                    quote_object(target)?,
                    columns.join(", "),
                    values.join(", ")
                ),
                (0..schema.fields().len()).collect(),
            ))
        }
    }
}

fn placeholder_expression(field: &Field, ordinal: usize) -> String {
    if is_geometry(field) {
        format!("ST_GeomFromEWKB(${ordinal})")
    } else if is_geography(field) {
        format!("ST_GeomFromEWKB(${ordinal})::geography")
    } else if matches!(field.data_type(), DataType::Decimal128(_, _)) {
        format!("${ordinal}::text::numeric")
    } else if matches!(field.data_type(), DataType::Utf8)
        && matches!(
            field
                .metadata()
                .get("plenora.native_type")
                .map(String::as_str),
            Some("json" | "jsonb" | "uuid")
        )
    {
        let native = field
            .metadata()
            .get("plenora.native_type")
            .expect("matched metadata");
        format!("${ordinal}::text::{native}")
    } else if matches!(
        field.data_type(),
        DataType::Utf8 | DataType::List(_) | DataType::Struct(_)
    ) && native_declaration_sql(field).is_some_and(|declaration| declaration != "text")
    {
        format!(
            "${ordinal}::text::{}",
            native_declaration_sql(field).expect("checked declaration")
        )
    } else {
        format!("${ordinal}")
    }
}

fn key_predicates(renderer: &Renderer, keys: &[String], first: usize) -> Result<String> {
    keys.iter()
        .enumerate()
        .map(|(offset, key)| {
            Identifier::new(key.clone())
                .map(|id| format!("{} = ${}", renderer.quote_identifier(&id), first + offset))
        })
        .collect::<Result<Vec<_>>>()
        .map(|items| items.join(" AND "))
}

#[allow(clippy::too_many_lines)]
fn arrow_value(
    array: &dyn Array,
    field: &Field,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>> {
    macro_rules! scalar {
        ($array:ty, $value:expr) => {{
            let typed = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| $value(typed, row)))
                as Box<dyn ToSql + Sync + Send>
        }};
    }
    Ok(match field.data_type() {
        DataType::Boolean => scalar!(BooleanArray, |a: &BooleanArray, i| a.value(i)),
        DataType::Int32 => scalar!(Int32Array, |a: &Int32Array, i| a.value(i)),
        DataType::Int64 => scalar!(Int64Array, |a: &Int64Array, i| a.value(i)),
        DataType::Float32 => scalar!(Float32Array, |a: &Float32Array, i| a.value(i)),
        DataType::Float64 => scalar!(Float64Array, |a: &Float64Array, i| a.value(i)),
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| typed.value(row).to_owned()))
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| typed.value(row).to_vec()))
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(mapping_error)?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let value = (!typed.is_null(row))
                .then(|| {
                    epoch
                        .checked_add_signed(Duration::days(i64::from(typed.value(row))))
                        .ok_or_else(temporal_range_error)
                })
                .transpose()?;
            Box::new(value)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let typed = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| time_from_microseconds(typed.value(row)))
                .transpose()?;
            Box::new(value)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let typed = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| postgres_interval(typed, row))
                .transpose()?;
            Box::new(value)
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(mapping_error)?;
            if timezone.is_some() {
                let value = (!typed.is_null(row))
                    .then(|| {
                        DateTime::<Utc>::from_timestamp_micros(typed.value(row))
                            .ok_or_else(temporal_range_error)
                    })
                    .transpose()?;
                Box::new(value)
            } else {
                let value = (!typed.is_null(row))
                    .then(|| {
                        DateTime::<Utc>::from_timestamp_micros(typed.value(row))
                            .map(|instant| instant.naive_utc())
                            .ok_or_else(temporal_range_error)
                    })
                    .transpose()?;
                Box::new(value)
            }
        }
        DataType::Decimal128(_, scale) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| decimal_string(typed.value(row), *scale)))
        }
        DataType::List(_) => {
            let typed = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| list_string(typed, row))
                .transpose()?;
            Box::new(value)
        }
        DataType::Struct(_) if is_range_field(field) => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| range_string(typed, row))
                .transpose()?;
            Box::new(value)
        }
        DataType::Struct(_) if is_composite_field(field) => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| composite_string(typed, row))
                .transpose()?;
            Box::new(value)
        }
        _ => return Err(mapping_error()),
    })
}

#[derive(Debug)]
struct NumericBinary {
    value: i128,
    scale: i8,
}

impl ToSql for NumericBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non numeric".into());
        }
        encode_numeric_binary(self.value, self.scale, output)?;
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        target_type.name() == "numeric"
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct EwkbBinary(Vec<u8>);

impl ToSql for EwkbBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non spatial".into());
        }
        output.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        matches!(target_type.name(), "geometry" | "geography")
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct UuidBinary([u8; 16]);

impl ToSql for UuidBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non UUID".into());
        }
        output.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        *target_type == Type::UUID
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct PostgresIntervalBinary {
    microseconds: i64,
    days: i32,
    months: i32,
}

fn interval_text(value: &PostgresIntervalBinary) -> String {
    const MICROS_PER_SECOND: u64 = 1_000_000;
    const MICROS_PER_MINUTE: u64 = 60 * MICROS_PER_SECOND;
    const MICROS_PER_HOUR: u64 = 60 * MICROS_PER_MINUTE;

    let negative = value.microseconds < 0;
    let absolute = value.microseconds.unsigned_abs();
    let hours = absolute / MICROS_PER_HOUR;
    let minutes = (absolute % MICROS_PER_HOUR) / MICROS_PER_MINUTE;
    let seconds = (absolute % MICROS_PER_MINUTE) / MICROS_PER_SECOND;
    let microseconds = absolute % MICROS_PER_SECOND;
    format!(
        "{} mons {} days {}{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}",
        value.months,
        value.days,
        if negative { "-" } else { "" }
    )
}

#[derive(Debug)]
struct PostgresRangeBinary(RangeValue);

impl ToSql for PostgresRangeBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let Kind::Range(member) = target_type.kind() else {
            return Err("target non range".into());
        };
        if self.0.empty {
            output.put_u8(0x01);
            return Ok(IsNull::No);
        }
        let mut flags = 0_u8;
        if self.0.lower_inclusive {
            flags |= 0x02;
        }
        if self.0.upper_inclusive {
            flags |= 0x04;
        }
        if self.0.lower_unbounded {
            flags |= 0x08;
        }
        if self.0.upper_unbounded {
            flags |= 0x10;
        }
        output.put_u8(flags);
        if !self.0.lower_unbounded {
            encode_range_bound(
                self.0.lower.as_deref().ok_or("range lower mancante")?,
                member,
                output,
            )?;
        }
        if !self.0.upper_unbounded {
            encode_range_bound(
                self.0.upper.as_deref().ok_or("range upper mancante")?,
                member,
                output,
            )?;
        }
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        matches!(target_type.kind(), Kind::Range(_))
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct PostgresCompositeBinary(Vec<Option<String>>);

impl ToSql for PostgresCompositeBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let Kind::Composite(fields) = target_type.kind() else {
            return Err("target non composite".into());
        };
        if fields.len() != self.0.len() {
            return Err("numero campi composite non coerente".into());
        }
        output.put_i32(i32::try_from(fields.len())?);
        for (field, value) in fields.iter().zip(&self.0) {
            output.put_u32(field.type_().oid());
            if let Some(value) = value {
                let mut encoded = BytesMut::new();
                encode_composite_field(value, field.type_(), &mut encoded)?;
                output.put_i32(i32::try_from(encoded.len())?);
                output.extend_from_slice(&encoded);
            } else {
                output.put_i32(-1);
            }
        }
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        matches!(target_type.kind(), Kind::Composite(_))
    }

    to_sql_checked!();
}

#[allow(clippy::too_many_lines)]
fn encode_composite_field(
    value: &str,
    field_type: &Type,
    output: &mut BytesMut,
) -> std::result::Result<(), Box<dyn std::error::Error + Sync + Send>> {
    match field_type.kind() {
        Kind::Domain(inner) => return encode_composite_field(value, inner, output),
        Kind::Enum(_) => {
            output.extend_from_slice(value.as_bytes());
            return Ok(());
        }
        _ => {}
    }
    match *field_type {
        Type::BOOL => {
            value.parse::<bool>()?.to_sql(field_type, output)?;
        }
        Type::INT2 => {
            value.parse::<i16>()?.to_sql(field_type, output)?;
        }
        Type::INT4 => {
            value.parse::<i32>()?.to_sql(field_type, output)?;
        }
        Type::INT8 => {
            value.parse::<i64>()?.to_sql(field_type, output)?;
        }
        Type::FLOAT4 => {
            value.parse::<f32>()?.to_sql(field_type, output)?;
        }
        Type::FLOAT8 => {
            value.parse::<f64>()?.to_sql(field_type, output)?;
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            output.extend_from_slice(value.as_bytes());
        }
        Type::DATE => {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")?.to_sql(field_type, output)?;
        }
        Type::TIME => {
            NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?.to_sql(field_type, output)?;
        }
        Type::TIMESTAMP => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))?
                .to_sql(field_type, output)?;
        }
        Type::TIMESTAMPTZ => {
            DateTime::parse_from_rfc3339(value)?
                .with_timezone(&Utc)
                .to_sql(field_type, output)?;
        }
        Type::NUMERIC => {
            let (unscaled, scale) = parse_numeric_components(value)?;
            NumericBinary {
                value: unscaled,
                scale,
            }
            .to_sql(field_type, output)?;
        }
        Type::JSON | Type::JSONB => {
            serde_json::from_str::<serde_json::Value>(value)?.to_sql(field_type, output)?;
        }
        Type::UUID => output.extend_from_slice(&parse_uuid_bytes(value)?),
        _ => return Err("campo composite binario non supportato".into()),
    }
    Ok(())
}

fn parse_uuid_bytes(
    value: &str,
) -> std::result::Result<[u8; 16], Box<dyn std::error::Error + Sync + Send>> {
    let compact = value
        .chars()
        .filter(|character| *character != '-')
        .collect::<String>();
    if compact.len() != 32 {
        return Err("UUID non valido".into());
    }
    let mut output = [0_u8; 16];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16)?;
    }
    Ok(output)
}

fn encode_range_bound(
    value: &str,
    member: &Type,
    output: &mut BytesMut,
) -> std::result::Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let mut encoded = BytesMut::new();
    match *member {
        Type::INT4 => {
            value.parse::<i32>()?.to_sql(member, &mut encoded)?;
        }
        Type::INT8 => {
            value.parse::<i64>()?.to_sql(member, &mut encoded)?;
        }
        Type::DATE => {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")?.to_sql(member, &mut encoded)?;
        }
        Type::TIMESTAMP => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")?
                .to_sql(member, &mut encoded)?;
        }
        Type::TIMESTAMPTZ => {
            DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z")?
                .with_timezone(&Utc)
                .to_sql(member, &mut encoded)?;
        }
        Type::NUMERIC => {
            let (unscaled, scale) = parse_numeric_components(value)?;
            NumericBinary {
                value: unscaled,
                scale,
            }
            .to_sql(member, &mut encoded)?;
        }
        _ => return Err("range subtype binario non supportato".into()),
    }
    output.put_i32(i32::try_from(encoded.len())?);
    output.extend_from_slice(&encoded);
    Ok(())
}

fn parse_numeric_components(
    value: &str,
) -> std::result::Result<(i128, i8), Box<dyn std::error::Error + Sync + Send>> {
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if unsigned.is_empty() {
        return Err("numeric vuoto".into());
    }
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if fraction.contains('.')
        || (integer.is_empty() && fraction.is_empty())
        || !integer.chars().all(|character| character.is_ascii_digit())
        || !fraction.chars().all(|character| character.is_ascii_digit())
    {
        return Err("numeric non valido".into());
    }
    let scale = i8::try_from(fraction.len())?;
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(if integer.is_empty() { "0" } else { integer });
    digits.push_str(fraction);
    let value = digits.parse::<i128>()?;
    Ok((if negative { -value } else { value }, scale))
}

impl ToSql for PostgresIntervalBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non interval".into());
        }
        output.put_i64(self.microseconds);
        output.put_i32(self.days);
        output.put_i32(self.months);
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        *target_type == Type::INTERVAL
    }

    to_sql_checked!();
}

fn postgres_interval(
    array: &IntervalMonthDayNanoArray,
    row: usize,
) -> Result<PostgresIntervalBinary> {
    let value = array.value(row);
    if value.nanoseconds % 1_000 != 0 {
        return Err(public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Write,
            false,
            "interval Arrow richiede precisione massima al microsecondo per PostgreSQL",
        ));
    }
    Ok(PostgresIntervalBinary {
        microseconds: value.nanoseconds / 1_000,
        days: value.days,
        months: value.months,
    })
}

fn binary_copy_value(
    array: &dyn Array,
    field: &Field,
    target_type: &Type,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>> {
    if matches!(field.data_type(), DataType::Struct(_)) && is_composite_field(field) {
        let typed = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| composite_value(typed, row).map(PostgresCompositeBinary))
            .transpose()?;
        return Ok(Box::new(value));
    }
    if matches!(field.data_type(), DataType::Struct(_)) && is_range_field(field) {
        let typed = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| range_value(typed, row).map(PostgresRangeBinary))
            .transpose()?;
        return Ok(Box::new(value));
    }
    if matches!(field.data_type(), DataType::List(_)) {
        if !matches!(target_type.kind(), Kind::Array(_)) {
            return Err(mapping_error());
        }
        let typed = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(mapping_error)?;
        return binary_array_value(typed, row);
    }
    if matches!(field.data_type(), DataType::Decimal128(_, _)) {
        let typed = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(mapping_error)?;
        let scale = match field.data_type() {
            DataType::Decimal128(_, scale) => *scale,
            _ => unreachable!("matched decimal"),
        };
        return Ok(Box::new((!typed.is_null(row)).then(|| NumericBinary {
            value: typed.value(row),
            scale,
        })));
    }
    if is_spatial(field) {
        let typed = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(mapping_error)?;
        return Ok(Box::new(
            (!typed.is_null(row)).then(|| EwkbBinary(typed.value(row).to_vec())),
        ));
    }
    if matches!(field.data_type(), DataType::Utf8)
        && matches!(
            field
                .metadata()
                .get("plenora.native_type")
                .map(String::as_str),
            Some("json" | "jsonb")
        )
    {
        let typed = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| serde_json::from_str::<serde_json::Value>(typed.value(row)))
            .transpose()
            .map_err(|_| DatabaseError::invalid_plan("JSON Arrow non valido"))?;
        return Ok(Box::new(value));
    }
    if target_type.name() == "uuid" {
        let typed = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| {
                parse_uuid_bytes(typed.value(row))
                    .map(UuidBinary)
                    .map_err(|_| DatabaseError::invalid_plan("UUID Arrow non valido"))
            })
            .transpose()?;
        return Ok(Box::new(value));
    }
    arrow_value(array, field, row)
}

#[allow(clippy::too_many_lines)]
fn binary_array_value(array: &ListArray, row: usize) -> Result<Box<dyn ToSql + Sync + Send>> {
    macro_rules! primitive {
        ($array:ty, $value:ty) => {{
            if array.is_null(row) {
                Box::new(None::<Vec<Option<$value>>>) as Box<dyn ToSql + Sync + Send>
            } else {
                let values = array.value(row);
                let typed = values
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(mapping_error)?;
                let result = (0..typed.len())
                    .map(|index| (!typed.is_null(index)).then(|| typed.value(index)))
                    .collect::<Vec<_>>();
                Box::new(Some(result)) as Box<dyn ToSql + Sync + Send>
            }
        }};
    }
    Ok(match array.value_type() {
        DataType::Boolean => primitive!(BooleanArray, bool),
        DataType::Int32 => primitive!(Int32Array, i32),
        DataType::Int64 => primitive!(Int64Array, i64),
        DataType::Float32 => primitive!(Float32Array, f32),
        DataType::Float64 => primitive!(Float64Array, f64),
        DataType::Utf8 => {
            if array.is_null(row) {
                Box::new(None::<Vec<Option<String>>>)
            } else {
                let values = array.value(row);
                let typed = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(mapping_error)?;
                let result = (0..typed.len())
                    .map(|index| (!typed.is_null(index)).then(|| typed.value(index).to_owned()))
                    .collect::<Vec<_>>();
                Box::new(Some(result))
            }
        }
        _ => return Err(mapping_error()),
    })
}

pub fn encode_numeric_binary(
    value: i128,
    scale: i8,
    output: &mut BytesMut,
) -> std::result::Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let negative = value < 0;
    let mut digits = value.unsigned_abs().to_string();
    let scale = if scale < 0 {
        digits.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
        0
    } else {
        usize::try_from(scale)?
    };
    if digits.len() <= scale {
        digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
    }
    let integer_digits = digits.len() - scale;
    let left_padding = (4 - integer_digits % 4) % 4;
    let right_padding = (4 - scale % 4) % 4;
    let mut padded = String::with_capacity(left_padding + digits.len() + right_padding);
    padded.push_str(&"0".repeat(left_padding));
    padded.push_str(&digits);
    padded.push_str(&"0".repeat(right_padding));
    let integer_groups = (left_padding + integer_digits) / 4;
    let mut groups = padded
        .as_bytes()
        .chunks_exact(4)
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .expect("decimal ASCII")
                .parse::<i16>()
                .expect("base10000")
        })
        .collect::<Vec<_>>();
    let leading = groups.iter().take_while(|digit| **digit == 0).count();
    let trailing = groups.iter().rev().take_while(|digit| **digit == 0).count();
    let end = groups.len().saturating_sub(trailing).max(leading);
    groups = groups[leading..end].to_vec();
    let weight = i16::try_from(integer_groups)?
        .checked_sub(1)
        .and_then(|value| value.checked_sub(i16::try_from(leading).ok()?))
        .ok_or("numeric weight overflow")?;
    output.put_i16(i16::try_from(groups.len())?);
    output.put_i16(if groups.is_empty() { 0 } else { weight });
    output.put_u16(if negative { 0x4000 } else { 0x0000 });
    output.put_u16(u16::try_from(scale)?);
    for group in groups {
        output.put_i16(group);
    }
    Ok(())
}

fn decimal_string(value: i128, scale: i8) -> String {
    let negative = value < 0;
    let mut digits = value.unsigned_abs().to_string();
    if scale < 0 {
        digits.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
        if negative {
            digits.insert(0, '-');
        }
        return digits;
    }
    let scale = usize::try_from(scale).expect("non-negative scale");
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let mut result = if scale == 0 {
        padded
    } else {
        format!("{}.{}", &padded[..split], &padded[split..])
    };
    if negative {
        result.insert(0, '-');
    }
    result
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
) -> Result<()> {
    let renderer = renderer();
    for field in schema.fields().iter().filter(|field| is_spatial(field)) {
        let field_name =
            renderer.quote_identifier(&Identifier::new(field.name().clone()).expect("validated"));
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

fn metadata_value<'a>(field: &'a Field, canonical: &str, legacy: &str) -> Option<&'a str> {
    field
        .metadata()
        .get(canonical)
        .or_else(|| field.metadata().get(legacy))
        .map(String::as_str)
}

fn validate_metadata_coherence(field: &Field) -> Result<()> {
    for (canonical, legacy) in [
        (protocol::GEOMETRY_DIMENSIONS, "plenora.dimensions"),
        (protocol::GEOMETRY_SRID, "plenora.srid"),
        (
            protocol::GEOMETRY_SPATIAL_SEMANTICS,
            "plenora.spatial_semantics",
        ),
        (protocol::POSTGRES_NATIVE_TYPE, "plenora.native_type"),
        (
            protocol::POSTGRES_NATIVE_DECLARATION,
            "plenora.native_declaration",
        ),
        (protocol::POSTGRES_TYPE_KIND, "plenora.postgres_type_kind"),
    ] {
        if let (Some(current), Some(previous)) = (
            field.metadata().get(canonical),
            field.metadata().get(legacy),
        ) {
            if current != previous {
                return Err(DatabaseError::invalid_plan(
                    "metadata canonico e legacy divergenti",
                ));
            }
        }
    }
    if let (Some(current), Some(previous)) = (
        field.metadata().get(protocol::GEOMETRY_TYPES),
        field.metadata().get("plenora.geometry_type"),
    ) {
        if !current.eq_ignore_ascii_case(previous) {
            return Err(DatabaseError::invalid_plan(
                "tipo geometrico canonico e legacy divergenti",
            ));
        }
    }
    Ok(())
}

fn validate_crs_metadata(field: &Field) -> Result<()> {
    if !is_spatial(field) {
        return Ok(());
    }
    let metadata = field.metadata();
    let resolution = metadata
        .get(protocol::GEOMETRY_CRS_RESOLUTION)
        .map(String::as_str);
    let srid = metadata
        .get(protocol::GEOMETRY_SRID)
        .or_else(|| metadata.get("plenora.srid"));
    let crs_id = metadata.get(protocol::GEOMETRY_CRS_ID);
    let definition = metadata.get(protocol::GEOMETRY_CRS_DEFINITION);
    let definition_format = metadata.get(protocol::GEOMETRY_CRS_DEFINITION_FORMAT);
    let axis_order = metadata.get(protocol::GEOMETRY_AXIS_ORDER);

    if srid.is_some_and(|value| {
        value
            .parse::<u32>()
            .ok()
            .filter(|value| *value > 0)
            .is_none()
    }) {
        return Err(DatabaseError::invalid_plan(
            "SRID CRS deve essere un intero positivo",
        ));
    }
    if definition.is_some() != definition_format.is_some() {
        return Err(DatabaseError::invalid_plan(
            "definizione CRS e formato devono essere presenti insieme",
        ));
    }
    if definition.is_some() && axis_order.is_none_or(|axis| axis == "unknown") {
        return Err(DatabaseError::invalid_plan(
            "una definizione CRS richiede un ordine assi esplicito",
        ));
    }
    if axis_order.is_some_and(|axis| {
        !matches!(
            axis.as_str(),
            "lon_lat" | "lat_lon" | "easting_northing" | "northing_easting" | "other" | "unknown"
        )
    }) {
        return Err(DatabaseError::invalid_plan("ordine assi CRS non valido"));
    }
    if crs_id.is_some_and(|value| {
        value.is_empty() || value.len() > 1_024 || value.chars().any(char::is_control)
    }) {
        return Err(DatabaseError::invalid_plan("identificatore CRS non valido"));
    }
    match resolution {
        Some("resolved") if srid.is_none() || crs_id.is_none() => Err(DatabaseError::invalid_plan(
            "CRS resolved PostgreSQL richiede SRID e identificatore",
        )),
        Some("missing")
            if srid.is_some()
                || crs_id.is_some()
                || definition.is_some()
                || definition_format.is_some()
                || axis_order.is_some() =>
        {
            Err(DatabaseError::invalid_plan(
                "CRS missing non ammette metadati CRS dichiarati",
            ))
        }
        Some("resolved" | "declared_unresolved" | "missing") | None => Ok(()),
        Some(_) => Err(DatabaseError::invalid_plan(
            "stato di risoluzione CRS non valido",
        )),
    }
}

fn pg_type(field: &Field) -> Result<String> {
    if is_geometry(field) || is_geography(field) {
        let base = if is_geography(field) {
            "geography"
        } else {
            "geometry"
        };
        let geometry_type =
            metadata_value(field, protocol::GEOMETRY_TYPES, "plenora.geometry_type")
                .unwrap_or("Geometry");
        if !geometry_type
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
        {
            return Err(DatabaseError::invalid_plan("geometry type non valido"));
        }
        let dimensions =
            match metadata_value(field, protocol::GEOMETRY_DIMENSIONS, "plenora.dimensions") {
                None | Some("xy") => "",
                Some("xyz") => "Z",
                Some("xym") => "M",
                Some("xyzm") => "ZM",
                Some(_) => {
                    return Err(DatabaseError::invalid_plan(
                        "dimensioni geometry non valide o ambigue",
                    ));
                }
            };
        let srid = metadata_value(field, protocol::GEOMETRY_SRID, "plenora.srid")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        return Ok(format!("{base}({geometry_type}{dimensions},{srid})"));
    }
    Ok(match field.data_type() {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int32 => "integer".to_owned(),
        DataType::Int64 => "bigint".to_owned(),
        DataType::Float32 => "real".to_owned(),
        DataType::Float64 => "double precision".to_owned(),
        DataType::Utf8 => native_declaration_sql(field).unwrap_or_else(|| "text".to_owned()),
        DataType::Binary => "bytea".to_owned(),
        DataType::Date32 => "date".to_owned(),
        DataType::Time64(TimeUnit::Microsecond) => "time".to_owned(),
        DataType::Interval(IntervalUnit::MonthDayNano) => "interval".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            if timezone.is_some() {
                "timestamptz".to_owned()
            } else {
                "timestamp".to_owned()
            }
        }
        DataType::Decimal128(precision, scale) => {
            format!("numeric({precision},{scale})")
        }
        DataType::List(_) => native_declaration_sql(field).ok_or_else(mapping_error)?,
        DataType::Struct(_) if is_range_field(field) => {
            native_declaration_sql(field).ok_or_else(mapping_error)?
        }
        DataType::Struct(_) if is_composite_field(field) => {
            native_declaration_sql(field).ok_or_else(mapping_error)?
        }
        _ => return Err(mapping_error()),
    })
}

fn native_declaration_sql(field: &Field) -> Option<String> {
    let declaration = metadata_value(
        field,
        protocol::POSTGRES_NATIVE_DECLARATION,
        "plenora.native_declaration",
    )
    .or_else(|| metadata_value(field, protocol::POSTGRES_NATIVE_TYPE, "plenora.native_type"))?;
    let base = declaration.strip_suffix("[]").unwrap_or(declaration);
    let supported = matches!(
        base,
        "text"
            | "character varying"
            | "boolean"
            | "smallint"
            | "integer"
            | "bigint"
            | "real"
            | "double precision"
            | "date"
            | "time without time zone"
            | "time with time zone"
            | "timestamp without time zone"
            | "timestamp with time zone"
            | "interval"
            | "uuid"
            | "json"
            | "jsonb"
            | "inet"
            | "cidr"
            | "int4range"
            | "int8range"
            | "numrange"
            | "tsrange"
            | "tstzrange"
            | "daterange"
    ) || (base.starts_with("numeric(")
        && base.ends_with(')')
        && base[8..base.len() - 1]
            .chars()
            .all(|character| character.is_ascii_digit() || character == ','));
    if supported {
        return Some(declaration.to_owned());
    }
    if matches!(
        field
            .metadata()
            .get(protocol::POSTGRES_TYPE_KIND)
            .or_else(|| field.metadata().get("plenora.postgres_type_kind"))
            .map(String::as_str),
        Some("e" | "d" | "c")
    ) {
        let (base, array_suffix) = declaration
            .strip_suffix("[]")
            .map_or((declaration, ""), |base| (base, "[]"));
        if base.split('.').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        }) {
            let renderer = renderer();
            let quoted = base
                .split('.')
                .map(|part| {
                    Identifier::new(part.to_owned())
                        .map(|identifier| renderer.quote_identifier(&identifier))
                })
                .collect::<Result<Vec<_>>>()
                .ok()?;
            return Some(format!("{}{array_suffix}", quoted.join(".")));
        }
    }
    None
}

fn is_range_field(field: &Field) -> bool {
    matches!(
        field
            .metadata()
            .get(protocol::POSTGRES_NATIVE_TYPE)
            .or_else(|| field.metadata().get("plenora.native_type"))
            .map(String::as_str),
        Some("int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange")
    )
}

fn is_composite_field(field: &Field) -> bool {
    field
        .metadata()
        .get(protocol::POSTGRES_TYPE_KIND)
        .or_else(|| field.metadata().get("plenora.postgres_type_kind"))
        .is_some_and(|kind| kind == "c")
}

fn is_spatial(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|value| value == GEOARROW_WKB_EXTENSION_NAME)
}

fn is_geometry(field: &Field) -> bool {
    is_spatial(field)
        && field
            .metadata()
            .get(protocol::GEOMETRY_SPATIAL_SEMANTICS)
            .or_else(|| field.metadata().get("plenora.spatial_semantics"))
            .is_none_or(|value| value == "geometry")
}

fn is_geography(field: &Field) -> bool {
    is_spatial(field)
        && field
            .metadata()
            .get(protocol::GEOMETRY_SPATIAL_SEMANTICS)
            .or_else(|| field.metadata().get("plenora.spatial_semantics"))
            .is_some_and(|value| value == "geography")
}

fn validate_ewkb_contract(bytes: &[u8], field: &Field) -> Result<()> {
    if bytes.len() < 5 {
        return Err(spatial_mapping_error("EWKB troncato"));
    }
    let little_endian = match bytes[0] {
        0 => false,
        1 => true,
        _ => return Err(spatial_mapping_error("byte order EWKB non valido")),
    };
    let read_u32 = |offset: usize| -> Result<u32> {
        let raw: [u8; 4] = bytes
            .get(offset..offset.saturating_add(4))
            .and_then(|value| value.try_into().ok())
            .ok_or_else(|| spatial_mapping_error("header EWKB troncato"))?;
        Ok(if little_endian {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        })
    };
    let type_word = read_u32(1)?;
    let ewkb_z = type_word & 0x8000_0000 != 0;
    let ewkb_m = type_word & 0x4000_0000 != 0;
    let has_srid = type_word & 0x2000_0000 != 0;
    let mut base_type = type_word & 0x0fff_ffff;
    let (iso_z, iso_m) = if base_type >= 3_000 {
        base_type -= 3_000;
        (true, true)
    } else if base_type >= 2_000 {
        base_type -= 2_000;
        (false, true)
    } else if base_type >= 1_000 {
        base_type -= 1_000;
        (true, false)
    } else {
        (false, false)
    };
    let z = ewkb_z || iso_z;
    let m = ewkb_m || iso_m;
    let dimensions = match (z, m) {
        (false, false) => "xy",
        (true, false) => "xyz",
        (false, true) => "xym",
        (true, true) => "xyzm",
    };
    if let Some(expected) =
        metadata_value(field, protocol::GEOMETRY_DIMENSIONS, "plenora.dimensions")
    {
        if expected != dimensions {
            return Err(spatial_mapping_error(
                "dimensioni EWKB diverse dal contratto Arrow",
            ));
        }
    }
    let geometry_type = match base_type {
        1 => "Point",
        2 => "LineString",
        3 => "Polygon",
        4 => "MultiPoint",
        5 => "MultiLineString",
        6 => "MultiPolygon",
        7 => "GeometryCollection",
        8 => "CircularString",
        9 => "CompoundCurve",
        10 => "CurvePolygon",
        11 => "MultiCurve",
        12 => "MultiSurface",
        13 => "Curve",
        14 => "Surface",
        15 => "PolyhedralSurface",
        16 => "TIN",
        17 => "Triangle",
        _ => return Err(spatial_mapping_error("tipo geometry EWKB non supportato")),
    };
    if let Some(expected) = metadata_value(field, protocol::GEOMETRY_TYPES, "plenora.geometry_type")
    {
        if expected != "Geometry" && !expected.eq_ignore_ascii_case(geometry_type) {
            return Err(spatial_mapping_error(
                "tipo EWKB diverso dal contratto Arrow",
            ));
        }
    }
    let actual_srid = has_srid.then(|| read_u32(5)).transpose()?;
    if let Some(expected) = metadata_value(field, protocol::GEOMETRY_SRID, "plenora.srid")
        .and_then(|value| value.parse::<u32>().ok())
    {
        if expected != 0 && actual_srid != Some(expected) {
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

fn quote_object(target: &ObjectRef) -> Result<String> {
    Ok(renderer().quote_object(&ObjectName {
        catalog: None,
        schema: target
            .schema
            .as_ref()
            .map(|value| Identifier::new(value.clone()))
            .transpose()?,
        object: Identifier::new(target.object.clone())?,
    }))
}

const fn renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
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
    fn arrow_temporal_extremes_return_mapping_errors_without_panicking() {
        let date_field = Field::new("date_value", DataType::Date32, true);
        for extreme in [i32::MIN, i32::MAX] {
            let date = Date32Array::from(vec![Some(extreme)]);
            let mut text = String::new();
            let date_text_error =
                encode_copy_value(&mut text, &date, &date_field, 0).expect_err("date text range");
            assert_eq!(date_text_error.category, ErrorCategory::DataMapping);
            let date_prepared_error =
                arrow_value(&date, &date_field, 0).expect_err("date prepared range");
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
                let mut text = String::new();
                let text_error = encode_copy_value(&mut text, &timestamp, &field, 0)
                    .expect_err("timestamp text range");
                assert_eq!(text_error.category, ErrorCategory::DataMapping);
                let error =
                    arrow_value(&timestamp, &field, 0).expect_err("timestamp prepared range");
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
        validate_ewkb_contract(&point_z_4326, &field).expect("matching contract");

        let mut wrong_srid = point_z_4326.clone();
        wrong_srid[5..9].copy_from_slice(&3857_u32.to_le_bytes());
        assert_eq!(
            validate_ewkb_contract(&wrong_srid, &field)
                .expect_err("SRID mismatch")
                .category,
            ErrorCategory::DataMapping
        );

        let mut point_xy_4326 = vec![1_u8];
        point_xy_4326.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
        point_xy_4326.extend_from_slice(&4326_u32.to_le_bytes());
        point_xy_4326.extend_from_slice(&[0_u8; 16]);
        assert_eq!(
            validate_ewkb_contract(&point_xy_4326, &field)
                .expect_err("dimension mismatch")
                .category,
            ErrorCategory::DataMapping
        );
        assert!(validate_ewkb_contract(&[2, 0, 0, 0, 1], &field).is_err());
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

    let commit = commit_interruption_error(&deadline);
    assert_eq!(commit.category, ErrorCategory::Timeout);
    assert_eq!(commit.phase, ErrorPhase::Commit);
    assert_eq!(commit.remote_effect, RemoteEffect::Unknown);
    assert_eq!(commit.retry, RetryDisposition::RequiresRecovery);
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
fn divergent_canonical_and_legacy_metadata_is_rejected() {
    let field = Field::new("geom", DataType::Binary, false).with_metadata(
        [
            (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
            ("plenora.dimensions".to_owned(), "xyz".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let error = validate_metadata_coherence(&field).expect_err("metadata divergence");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
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
    ]
    .into_iter()
    .collect();
    let resolved_without_id = Field::new("geom", DataType::Binary, false).with_metadata(base);
    assert!(validate_crs_metadata(&resolved_without_id).is_err());

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
    assert!(validate_crs_metadata(&missing_with_srid).is_err());
}
