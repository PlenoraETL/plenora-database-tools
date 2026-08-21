mod codec;
mod lifecycle;
mod plan;
mod resources;
mod row_diagnostics;
mod sql;

use crate::{describe_object, PooledSqlServerSession, SqlServerPool, SqlServerSession};
use plan::{TargetLifecycle, WritePlan};
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::loss::LossReport;
use plenora_database_core::outcome::{
    CertainPhase, Recovery, RowCounts, WriteOutcome, WriteStatus,
};
use plenora_database_core::plan::{ProviderKind, WriteOperation};
use plenora_database_core::provider::BatchStream;
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, Renderer};
use resources::WriteBatchResources;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tiberius::{FromSql, Query, Row};

static EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static LIFECYCLE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MutationCounts {
    confirmed: u64,
    inserted: u64,
    updated: u64,
    deleted: u64,
}

impl MutationCounts {
    fn checked_add(&mut self, other: Self) -> Result<()> {
        self.confirmed = self
            .confirmed
            .checked_add(other.confirmed)
            .ok_or_else(|| DatabaseError::resource_limit("conteggio confermato write overflow"))?;
        self.inserted = self
            .inserted
            .checked_add(other.inserted)
            .ok_or_else(|| DatabaseError::resource_limit("conteggio insert write overflow"))?;
        self.updated = self
            .updated
            .checked_add(other.updated)
            .ok_or_else(|| DatabaseError::resource_limit("conteggio update write overflow"))?;
        self.deleted = self
            .deleted
            .checked_add(other.deleted)
            .ok_or_else(|| DatabaseError::resource_limit("conteggio delete write overflow"))?;
        Ok(())
    }
}

/// Codec usato dal percorso append/truncate-insert SQL Server.
///
/// `Prepared` resta il default compatibile e copre l'intero profilo tipi.
/// `TdsBulk` è opt-in e viene accettato soltanto quando il piano soddisfa il
/// sottoinsieme verificato dal codec bulk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SqlServerInsertMode {
    #[default]
    Prepared,
    TdsBulk,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SqlServerSchemaEvolution {
    #[default]
    Disabled,
    AddNullableColumns,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteFaultPoint {
    BeforeCommit,
    TransportLostAfterFirstInsert,
    CommitConfirmationLost,
    #[cfg(test)]
    DelayCommitResponse,
    #[cfg(test)]
    DelayRollbackResponse,
}

pub struct PreparedSqlServerWrite {
    operation: WriteOperation,
    plan: WritePlan,
    pool: Arc<SqlServerPool>,
    budget: ResourceBudget,
    _operation_lease: Option<ResourceLease>,
    _columns_lease: Option<ResourceLease>,
    loss_report: LossReport,
    insert_mode: SqlServerInsertMode,
}

impl std::fmt::Debug for PreparedSqlServerWrite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let target_schema_fingerprint = match &self.plan.lifecycle {
            TargetLifecycle::Existing {
                schema_fingerprint, ..
            }
            | TargetLifecycle::Replace {
                schema_fingerprint, ..
            } => Some(schema_fingerprint.as_str()),
            TargetLifecycle::Create { .. } => None,
        };
        formatter
            .debug_struct("PreparedSqlServerWrite")
            .field("operation", &self.operation)
            .field("target_schema_fingerprint", &target_schema_fingerprint)
            .field("columns", &self.plan.columns.len())
            .field("insert_mode", &self.insert_mode)
            .field("loss_report", &self.loss_report)
            .finish_non_exhaustive()
    }
}

impl PreparedSqlServerWrite {
    #[must_use]
    pub const fn loss_report(&self) -> &LossReport {
        &self.loss_report
    }
}

pub fn validate_diagnostic_request(
    prepared_schema: &SchemaRef,
    stream_schema: &SchemaRef,
    operation: &WriteOperation,
    insert_mode: SqlServerInsertMode,
    input_total: u64,
    policy: plenora_database_core::RowDiagnosticsPolicy,
) -> Result<()> {
    row_diagnostics::validate_input(
        prepared_schema,
        stream_schema,
        operation,
        insert_mode,
        input_total,
        policy,
    )
    .map(drop)
}

/// Prepara un write contro un target esistente e congela schema, mapping e SQL.
///
/// # Errors
///
/// Fallisce prima della mutazione per modalità non supportata, mapping
/// incompatibile, budget insufficiente, SRID misti o schema non scrivibile.
#[allow(clippy::significant_drop_tightening)]
pub async fn prepare_write(
    pool: &Arc<SqlServerPool>,
    operation: &WriteOperation,
    input_schema: SchemaRef,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<PreparedSqlServerWrite> {
    prepare_write_with_mode(
        pool,
        operation,
        input_schema,
        budget,
        cancellation,
        SqlServerInsertMode::Prepared,
    )
    .await
}

/// Prepara esplicitamente il codec di inserimento.
///
/// # Errors
///
/// `TdsBulk` fallisce durante `prepare` se l'input non contiene tutte le
/// colonne scrivibili nell'ordine del catalogo o usa un tipo fuori dal profilo
/// TDS bulk verificato.
pub async fn prepare_write_with_mode(
    pool: &Arc<SqlServerPool>,
    operation: &WriteOperation,
    input_schema: SchemaRef,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    insert_mode: SqlServerInsertMode,
) -> Result<PreparedSqlServerWrite> {
    prepare_write_inner(
        pool,
        operation,
        input_schema,
        budget,
        cancellation,
        true,
        insert_mode,
        SqlServerSchemaEvolution::Disabled,
    )
    .await
}

pub async fn prepare_write_with_options(
    pool: &Arc<SqlServerPool>,
    operation: &WriteOperation,
    input_schema: SchemaRef,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    insert_mode: SqlServerInsertMode,
    schema_evolution: SqlServerSchemaEvolution,
) -> Result<PreparedSqlServerWrite> {
    prepare_write_inner(
        pool,
        operation,
        input_schema,
        budget,
        cancellation,
        true,
        insert_mode,
        schema_evolution,
    )
    .await
}

/// Variante usata dall'adattatore del trait comune.
///
/// Il [`plenora_database_core::provider::PreparedWrite`] mantiene già i lease
/// di operazione e colonne tra prepare ed execute. Acquisirli una seconda
/// volta renderebbe il risultato dipendente da limiti maggiori del necessario.
pub async fn prepare_write_with_external_contract_leases(
    pool: &Arc<SqlServerPool>,
    operation: &WriteOperation,
    input_schema: SchemaRef,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    insert_mode: SqlServerInsertMode,
    schema_evolution: SqlServerSchemaEvolution,
) -> Result<PreparedSqlServerWrite> {
    prepare_write_inner(
        pool,
        operation,
        input_schema,
        budget,
        cancellation,
        false,
        insert_mode,
        schema_evolution,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_write_inner(
    pool: &Arc<SqlServerPool>,
    operation: &WriteOperation,
    input_schema: SchemaRef,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    acquire_contract_leases: bool,
    insert_mode: SqlServerInsertMode,
    schema_evolution: SqlServerSchemaEvolution,
) -> Result<PreparedSqlServerWrite> {
    budget.ensure_active()?;
    plan::validate_operation(operation)?;
    if input_schema.fields().is_empty() {
        return Err(write_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "write SQL Server richiede almeno una colonna",
        ));
    }
    for field in input_schema.fields() {
        sql_identifier(field.name())?;
    }
    sql_identifier(&operation.target.object)?;
    let operation_lease = acquire_contract_leases
        .then(|| budget.try_lease(ResourceKind::ConcurrentOperations, 1))
        .transpose()?;
    let columns = u64::try_from(input_schema.fields().len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
    let columns_lease = acquire_contract_leases
        .then(|| budget.try_lease(ResourceKind::Columns, columns))
        .transpose()?;
    let control = BudgetCancellation::new(cancellation, budget);
    let schema = operation.target.schema.as_deref().ok_or_else(|| {
        write_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "target SQL Server senza schema",
        )
    })?;
    sql_identifier(schema)?;
    let mut pooled = pool.checkout(control.token()).await?;
    let plan = compile_write_plan(
        &mut pooled,
        operation,
        schema,
        Arc::clone(&input_schema),
        control.token(),
        schema_evolution,
    )
    .await?;
    if insert_mode == SqlServerInsertMode::TdsBulk {
        plan::validate_bulk_profile(&plan)?;
    }
    drop(pooled);
    let loss_report = plan.loss_report(operation.mapping_policy)?;
    Ok(PreparedSqlServerWrite {
        operation: operation.clone(),
        plan,
        pool: Arc::clone(pool),
        budget: budget.clone(),
        _operation_lease: operation_lease,
        _columns_lease: columns_lease,
        loss_report,
        insert_mode,
    })
}

async fn compile_write_plan(
    pooled: &mut PooledSqlServerSession,
    operation: &WriteOperation,
    schema: &str,
    input_schema: SchemaRef,
    cancellation: &CancellationToken,
    schema_evolution: SqlServerSchemaEvolution,
) -> Result<WritePlan> {
    match operation.mode {
        plenora_database_core::plan::WriteMode::Create => {
            if lifecycle::object_exists(
                pooled.session_mut()?,
                schema,
                &operation.target.object,
                cancellation,
            )
            .await?
            {
                return Err(write_error(
                    ErrorCategory::Schema,
                    ErrorPhase::Prepare,
                    "target create SQL Server gia esistente",
                ));
            }
            WritePlan::compile_create(operation, input_schema)
        }
        plenora_database_core::plan::WriteMode::Replace => {
            let description = describe_object(
                pooled.session_mut()?,
                schema,
                &operation.target.object,
                cancellation,
            )
            .await?;
            lifecycle::validate_replace_external_state(
                pooled.session_mut()?,
                schema,
                &operation.target.object,
                ErrorPhase::Prepare,
                cancellation,
            )
            .await?;
            let nonce = LIFECYCLE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let staging = lifecycle::derived_object_name(&operation.target.object, "stage", nonce)?;
            let backup = lifecycle::derived_object_name(&operation.target.object, "backup", nonce)?;
            if lifecycle::object_exists(pooled.session_mut()?, schema, &staging, cancellation)
                .await?
                || lifecycle::object_exists(pooled.session_mut()?, schema, &backup, cancellation)
                    .await?
            {
                return Err(write_error(
                    ErrorCategory::Schema,
                    ErrorPhase::Prepare,
                    "collisione oggetto lifecycle SQL Server",
                ));
            }
            WritePlan::compile_replace(&description, operation, input_schema, &staging, &backup)
        }
        _ => {
            let description = describe_object(
                pooled.session_mut()?,
                schema,
                &operation.target.object,
                cancellation,
            )
            .await?;
            let observed_spatial =
                inspect_target_spatial_contracts(pooled.session_mut()?, &description, cancellation)
                    .await?;
            WritePlan::compile_existing(
                &description,
                operation,
                input_schema,
                &observed_spatial,
                schema_evolution,
            )
        }
    }
}

/// Esegue il piano preparato in una singola transazione SQL Server.
///
/// # Errors
///
/// Restituisce errori fail-closed prima del commit. Se la conferma del commit
/// viene persa, restituisce invece un `WriteOutcome::OutcomeUnknown` con
/// recovery obbligatoria.
#[allow(clippy::too_many_lines, clippy::significant_drop_tightening)]
pub async fn write_prepared(
    prepared: PreparedSqlServerWrite,
    input: Box<dyn BatchStream>,
    cancellation: &CancellationToken,
) -> Result<WriteOutcome> {
    write_prepared_inner(prepared, input, cancellation, None).await
}

#[cfg(test)]
pub async fn write_prepared_with_fault(
    prepared: PreparedSqlServerWrite,
    input: Box<dyn BatchStream>,
    cancellation: &CancellationToken,
    fault: WriteFaultPoint,
) -> Result<WriteOutcome> {
    write_prepared_inner(prepared, input, cancellation, Some(fault)).await
}

#[allow(clippy::too_many_lines, clippy::significant_drop_tightening)]
async fn write_prepared_inner(
    prepared: PreparedSqlServerWrite,
    mut input: Box<dyn BatchStream>,
    cancellation: &CancellationToken,
    fault: Option<WriteFaultPoint>,
) -> Result<WriteOutcome> {
    let stream_schema = input.schema();
    if stream_schema.as_ref() != prepared.plan.input_schema.as_ref() {
        return Err(write_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            "schema input diverso da prepare_write SQL Server",
        ));
    }
    let diagnostic_input = input
        .declared_input_rows()
        .map(|input_total| {
            row_diagnostics::validate_input(
                &prepared.plan.input_schema,
                &stream_schema,
                &prepared.operation,
                prepared.insert_mode,
                input_total,
                input.row_diagnostics_policy(),
            )
        })
        .transpose()?;
    let control = BudgetCancellation::new(cancellation, &prepared.budget);
    let execution_id = format!(
        "sqlserver-{}-{}",
        std::process::id(),
        EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut pooled = prepared.pool.checkout(control.token()).await?;
    pooled.disallow_reuse();
    pooled.session_mut()?.begin(control.token()).await?;
    if let Err(error) =
        prepare_transaction_target(&mut pooled, &prepared.plan, control.token()).await
    {
        return Err(rollback_after_error(&mut pooled, error).await);
    }

    let (received, mutations) = if let Some(diagnostic_input) = diagnostic_input {
        let constraint_column = diagnostic_input.policy.constraint_column.clone();
        let mut tracker = diagnostic_input.tracker;
        let mut writer = row_diagnostics::SqlServerRowWriter::new(
            &mut pooled,
            input.as_mut(),
            &prepared.plan,
            &prepared.budget,
            control.token(),
            constraint_column,
            fault,
            &execution_id,
        );
        let diagnosed =
            plenora_database_core::diagnose_row_scoped_write(&mut writer, &mut tracker).await;
        let mutations = writer.mutations();
        drop(writer);
        match diagnosed {
            Ok(Some(outcome)) => {
                return Err(outcome.into_error(Some(ProviderKind::Sqlserver), Some(execution_id))?);
            }
            Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            Ok(None) => (diagnostic_input.input_total, mutations),
        }
    } else {
        let mut received = 0_u64;
        let mut mutations = MutationCounts::default();
        loop {
            let batch = match input.next_batch(control.token()).await {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            };
            let reservation = match WriteBatchResources::reserve(
                &batch,
                &prepared.plan,
                &prepared.budget,
                prepared.insert_mode,
            ) {
                Ok(reservation) => reservation,
                Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            };
            received = if let Some(value) = received.checked_add(reservation.rows) {
                value
            } else {
                let error = DatabaseError::resource_limit("conteggio righe write overflow");
                return Err(rollback_after_error(&mut pooled, error).await);
            };
            let batch_result = match prepared.insert_mode {
                SqlServerInsertMode::Prepared => {
                    execute_prepared_batch(
                        &mut pooled,
                        &prepared.plan,
                        &batch,
                        control.token(),
                        fault,
                        &execution_id,
                    )
                    .await
                }
                SqlServerInsertMode::TdsBulk => {
                    execute_bulk_batch(
                        &mut pooled,
                        &prepared.plan,
                        &batch,
                        control.token(),
                        fault,
                        &execution_id,
                    )
                    .await
                }
            };
            let batch_mutations = match batch_result {
                Ok(mutations) => mutations,
                Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            };
            if let Err(error) = reservation.commit() {
                return Err(rollback_after_error(&mut pooled, error).await);
            }
            if let Err(error) = mutations.checked_add(batch_mutations) {
                return Err(rollback_after_error(&mut pooled, error).await);
            }
        }
        (received, mutations)
    };
    if let Err(error) =
        finalize_transaction_target(&mut pooled, &prepared.plan, control.token()).await
    {
        return Err(rollback_after_error(&mut pooled, error).await);
    }
    if fault == Some(WriteFaultPoint::BeforeCommit) {
        let mut error = write_error(
            ErrorCategory::Timeout,
            ErrorPhase::Write,
            "interruzione pre-commit SQL Server",
        );
        error.execution_id = Some(execution_id);
        return Err(rollback_after_error(&mut pooled, error).await);
    }
    #[cfg(test)]
    if fault == Some(WriteFaultPoint::DelayRollbackResponse) {
        let mut error = write_error(
            ErrorCategory::Execution,
            ErrorPhase::Write,
            "errore pre-commit SQL Server con risposta rollback non disponibile",
        );
        error.execution_id = Some(execution_id);
        return Err(rollback_after_error_with_delayed_response(&mut pooled, error).await);
    }
    let commit_result = commit_session(&mut pooled, control.token(), fault).await;
    if commit_result.is_ok() {
        if fault == Some(WriteFaultPoint::CommitConfirmationLost) {
            pooled.quarantine();
            return unknown_commit_outcome(&prepared, execution_id, received);
        }
        pooled.allow_reuse_after_drain()?;
        let (inserted, updated, deleted) = match prepared.plan.mode {
            plenora_database_core::plan::WriteMode::Append
            | plenora_database_core::plan::WriteMode::TruncateInsert
            | plenora_database_core::plan::WriteMode::Create
            | plenora_database_core::plan::WriteMode::Replace => {
                (Some(mutations.inserted), None, None)
            }
            plenora_database_core::plan::WriteMode::Update => {
                (Some(0), Some(mutations.updated), Some(0))
            }
            plenora_database_core::plan::WriteMode::Upsert => {
                (Some(mutations.inserted), Some(mutations.updated), Some(0))
            }
            plenora_database_core::plan::WriteMode::DeleteByKeys => {
                (Some(0), Some(0), Some(mutations.deleted))
            }
        };
        let outcome = WriteOutcome {
            schema_version: 2,
            status: WriteStatus::Committed,
            execution_id,
            provider: ProviderKind::Sqlserver,
            rows: RowCounts {
                received,
                confirmed: mutations.confirmed,
                inserted,
                updated,
                deleted,
                failed: 0,
                skipped: received.saturating_sub(mutations.confirmed),
            },
            recovery: None,
        };
        outcome.validate()?;
        Ok(outcome)
    } else {
        pooled.quarantine();
        unknown_commit_outcome(&prepared, execution_id, received)
    }
}

async fn execute_prepared_batch(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    batch: &plenora_database_core::arrow::RecordBatch,
    cancellation: &CancellationToken,
    fault: Option<WriteFaultPoint>,
    execution_id: &str,
) -> Result<MutationCounts> {
    let mut mutations = MutationCounts::default();
    for row in 0..batch.num_rows() {
        let query = codec::bind_row(plan, batch, row)?;
        let results = pooled
            .session_mut()?
            .execute_query(query, ErrorPhase::Write, cancellation)
            .await?;
        mutations.checked_add(row_mutation(&results, plan.mode)?)?;
        if fault == Some(WriteFaultPoint::TransportLostAfterFirstInsert) {
            pooled.quarantine();
            return Err(transport_loss_error(execution_id));
        }
    }
    Ok(mutations)
}

async fn execute_bulk_batch(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    batch: &plenora_database_core::arrow::RecordBatch,
    cancellation: &CancellationToken,
    fault: Option<WriteFaultPoint>,
    execution_id: &str,
) -> Result<MutationCounts> {
    let rows = codec::bulk_rows(plan, batch)?;
    let expected = u64::try_from(batch.num_rows())
        .map_err(|_| DatabaseError::resource_limit("righe bulk non rappresentabili"))?;
    if expected > 0 {
        let inserted = pooled
            .session_mut()?
            .execute_bulk_rows(&plan.bulk_table, rows, cancellation)
            .await?;
        if inserted != expected {
            return Err(write_error(
                ErrorCategory::Protocol,
                ErrorPhase::Write,
                "conteggio TDS bulk diverso dalle righe inviate",
            ));
        }
        if fault == Some(WriteFaultPoint::TransportLostAfterFirstInsert) {
            pooled.quarantine();
            return Err(transport_loss_error(execution_id));
        }
    }
    Ok(MutationCounts {
        confirmed: expected,
        inserted: expected,
        updated: 0,
        deleted: 0,
    })
}

#[cfg(test)]
async fn commit_session(
    pooled: &mut PooledSqlServerSession,
    cancellation: &CancellationToken,
    fault: Option<WriteFaultPoint>,
) -> Result<()> {
    if fault == Some(WriteFaultPoint::DelayCommitResponse) {
        return pooled
            .session_mut()?
            .commit_with_delayed_response(cancellation)
            .await;
    }
    pooled.session_mut()?.commit(cancellation).await
}

#[cfg(not(test))]
async fn commit_session(
    pooled: &mut PooledSqlServerSession,
    cancellation: &CancellationToken,
    _fault: Option<WriteFaultPoint>,
) -> Result<()> {
    pooled.session_mut()?.commit(cancellation).await
}

fn unknown_commit_outcome(
    prepared: &PreparedSqlServerWrite,
    execution_id: String,
    received: u64,
) -> Result<WriteOutcome> {
    let outcome = WriteOutcome {
        schema_version: 2,
        status: WriteStatus::OutcomeUnknown,
        execution_id,
        provider: ProviderKind::Sqlserver,
        rows: RowCounts {
            received,
            confirmed: 0,
            inserted: None,
            updated: None,
            deleted: None,
            failed: 0,
            skipped: 0,
        },
        recovery: Some(Recovery {
            last_certain_phase: CertainPhase::CommitRequested,
            automatic_retry_allowed: false,
            idempotency_key: None,
            // Il DDL e i rename sono nella stessa transazione: dopo una
            // conferma persa lo staging o è già il target, o non esiste.
            staging_object: None,
            verification_action: Some(format!(
                "verificare il target [{}].[{}] prima di ogni retry",
                prepared.plan.schema, prepared.plan.object
            )),
        }),
    };
    outcome.validate()?;
    Ok(outcome)
}

async fn prepare_transaction_target(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    cancellation: &CancellationToken,
) -> Result<()> {
    match &plan.lifecycle {
        TargetLifecycle::Existing {
            lock_sql,
            truncate_sql,
            add_columns_sql,
            schema_fingerprint,
        } => {
            lock_and_verify_schema(pooled, plan, lock_sql, schema_fingerprint, cancellation)
                .await?;
            for sql in add_columns_sql {
                pooled
                    .session_mut()?
                    .execute_write_query(Query::new(sql.clone()), cancellation)
                    .await?;
            }
            if let Some(sql) = truncate_sql {
                pooled
                    .session_mut()?
                    .execute_write_query(Query::new(sql.clone()), cancellation)
                    .await?;
            }
        }
        TargetLifecycle::Create { create_sql } | TargetLifecycle::Replace { create_sql, .. } => {
            pooled
                .session_mut()?
                .execute_write_query(Query::new(create_sql.clone()), cancellation)
                .await?;
        }
    }
    Ok(())
}

async fn finalize_transaction_target(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    cancellation: &CancellationToken,
) -> Result<()> {
    create_spatial_indexes(pooled, plan, cancellation).await?;
    if let TargetLifecycle::Replace {
        lock_sql,
        schema_fingerprint,
        staging_object,
        backup_object,
        ..
    } = &plan.lifecycle
    {
        lock_and_verify_schema(pooled, plan, lock_sql, schema_fingerprint, cancellation).await?;
        lifecycle::validate_replace_external_state(
            pooled.session_mut()?,
            &plan.schema,
            &plan.object,
            ErrorPhase::Write,
            cancellation,
        )
        .await?;
        lifecycle::publish_replacement(
            pooled.session_mut()?,
            &plan.schema,
            &plan.object,
            staging_object,
            backup_object,
            cancellation,
        )
        .await?;
    }
    Ok(())
}

async fn create_spatial_indexes(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    cancellation: &CancellationToken,
) -> Result<()> {
    for index in &plan.spatial_indexes {
        let sql = match index.kind {
            plan::SpatialIndexKind::Geometry => {
                let bounds =
                    geometry_index_bounds(pooled, plan, &index.quoted_column, cancellation).await?;
                format!(
                    "CREATE SPATIAL INDEX {} ON {} ({}) \
                     USING GEOMETRY_AUTO_GRID \
                     WITH (BOUNDING_BOX = ({:.17}, {:.17}, {:.17}, {:.17}));",
                    index.quoted_name,
                    plan.bulk_table,
                    index.quoted_column,
                    bounds[0],
                    bounds[1],
                    bounds[2],
                    bounds[3]
                )
            }
            plan::SpatialIndexKind::Geography => format!(
                "CREATE SPATIAL INDEX {} ON {} ({}) USING GEOGRAPHY_AUTO_GRID;",
                index.quoted_name, plan.bulk_table, index.quoted_column
            ),
        };
        pooled
            .session_mut()?
            .execute_write_query(Query::new(sql), cancellation)
            .await?;
    }
    Ok(())
}

async fn geometry_index_bounds(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    quoted_column: &str,
    cancellation: &CancellationToken,
) -> Result<[f64; 4]> {
    let sql = format!(
        "DECLARE @plenora_envelope geometry = \
         (SELECT geometry::EnvelopeAggregate({quoted_column}) \
            FROM {} WHERE {quoted_column} IS NOT NULL); \
         SELECT @plenora_envelope.STPointN(1).STX, \
                @plenora_envelope.STPointN(1).STY, \
                @plenora_envelope.STPointN(3).STX, \
                @plenora_envelope.STPointN(3).STY;",
        plan.bulk_table
    );
    let mut results = pooled
        .session_mut()?
        .execute_query(Query::new(sql), ErrorPhase::Write, cancellation)
        .await?;
    if results.len() != 1 {
        return Err(write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "extent geometry SQL Server con result set inattesi",
        ));
    }
    let rows = results.pop().ok_or_else(|| {
        write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "extent geometry SQL Server senza result set",
        )
    })?;
    let row = rows.first().ok_or_else(|| {
        write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "extent geometry SQL Server senza riga",
        )
    })?;
    let mut bounds = [0.0_f64; 4];
    for (index, value) in bounds.iter_mut().enumerate() {
        *value = row
            .try_get::<f64, _>(index)
            .map_err(|_| {
                write_error(
                    ErrorCategory::Protocol,
                    ErrorPhase::Write,
                    "extent geometry SQL Server con tipo inatteso",
                )
            })?
            .ok_or_else(|| {
                write_error(
                    ErrorCategory::InvalidPlan,
                    ErrorPhase::Write,
                    "indice geometry SQL Server richiede almeno una geometria non NULL",
                )
            })?;
    }
    if !bounds.iter().all(|value| value.is_finite())
        || bounds[0] >= bounds[2]
        || bounds[1] >= bounds[3]
    {
        return Err(write_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Write,
            "bounding box geometry SQL Server non finita o degenerata",
        ));
    }
    Ok(bounds)
}

async fn lock_and_verify_schema(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    lock_sql: &str,
    schema_fingerprint: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    let results = pooled
        .session_mut()?
        .execute_query(
            Query::new(lock_sql.to_owned()),
            ErrorPhase::Write,
            cancellation,
        )
        .await?;
    if results.len() != 1 {
        return Err(write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "lock schema SQL Server senza result set atteso",
        ));
    }
    let current = describe_object(
        pooled.session_mut()?,
        &plan.schema,
        &plan.object,
        cancellation,
    )
    .await?;
    if current.token.structural_fingerprint != schema_fingerprint {
        return Err(write_error(
            ErrorCategory::Schema,
            ErrorPhase::Write,
            "schema SQL Server cambiato dopo prepare_write",
        ));
    }
    Ok(())
}

async fn rollback_after_error(
    pooled: &mut PooledSqlServerSession,
    original: DatabaseError,
) -> DatabaseError {
    rollback_after_error_inner(pooled, original, RollbackMode::Normal).await
}

#[cfg(test)]
async fn rollback_after_error_with_delayed_response(
    pooled: &mut PooledSqlServerSession,
    original: DatabaseError,
) -> DatabaseError {
    rollback_after_error_inner(pooled, original, RollbackMode::DelayedResponse).await
}

#[derive(Clone, Copy)]
enum RollbackMode {
    Normal,
    #[cfg(test)]
    DelayedResponse,
}

async fn rollback_after_error_inner(
    pooled: &mut PooledSqlServerSession,
    mut original: DatabaseError,
    mode: RollbackMode,
) -> DatabaseError {
    if matches!(
        pooled.session().map(SqlServerSession::state),
        Ok(crate::SessionState::Transaction | crate::SessionState::Uncommittable)
    ) {
        let recovery = CancellationToken::new();
        let rollback = match pooled.session_mut() {
            Ok(session) => rollback_session(session, &recovery, mode).await,
            Err(error) => Err(error),
        };
        if rollback.is_err() {
            pooled.quarantine();
            original.remote_effect = RemoteEffect::Unknown;
            original.retry = RetryDisposition::RequiresRecovery;
            original.message = format!(
                "{}; rollback SQL Server non confermato: recovery obbligatoria",
                original.message
            );
            return original;
        }
        original = confirmed_rollback_axes(original);
        if pooled.allow_reuse_after_drain().is_err() {
            pooled.quarantine();
        }
    } else {
        pooled.quarantine();
    }
    original
}

/// Assi dell'errore dopo un annullamento confermato sul percorso a batch.
///
/// L'annullamento confermato è un fatto sull'**effetto remoto**, non sulla
/// ritentabilità: un deadlock che il server ha annullato resta `Safe`, un
/// timeout che lascia la sessione recuperabile resta `RequiresRecovery`, e una
/// sessione già in quarantena vi resta. Convertire tutto a `Never`
/// trasformerebbe errori transitori recuperabili in fallimenti definitivi.
///
/// Il percorso row-scoped ha invece semantica propria: là il rifiuto è del
/// dato, ritentare la stessa riga riprodurrebbe lo stesso vincolo violato, e
/// `RowRejectionOutcome::axes` dichiara `Never`.
const fn confirmed_rollback_axes(mut original: DatabaseError) -> DatabaseError {
    original.remote_effect = RemoteEffect::RolledBack;
    original
}

#[cfg(test)]
async fn rollback_session(
    session: &mut SqlServerSession,
    cancellation: &CancellationToken,
    mode: RollbackMode,
) -> Result<()> {
    if matches!(mode, RollbackMode::DelayedResponse) {
        return session.rollback_with_delayed_response(cancellation).await;
    }
    session.rollback(cancellation).await
}

#[cfg(not(test))]
async fn rollback_session(
    session: &mut SqlServerSession,
    cancellation: &CancellationToken,
    _mode: RollbackMode,
) -> Result<()> {
    session.rollback(cancellation).await
}

fn transport_loss_error(execution_id: &str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::Io,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: Some(execution_id.to_owned()),
        message: "trasporto TDS perso durante write SQL Server: stato remoto da verificare"
            .to_owned(),
        diagnostics: None,
    }
}

fn row_mutation(
    results: &[Vec<Row>],
    mode: plenora_database_core::plan::WriteMode,
) -> Result<MutationCounts> {
    if results.len() != 1 {
        return Err(write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "DML SQL Server con numero di result set inatteso",
        ));
    }
    let Some(row) = results[0].first() else {
        if matches!(
            mode,
            plenora_database_core::plan::WriteMode::Update
                | plenora_database_core::plan::WriteMode::Upsert
                | plenora_database_core::plan::WriteMode::DeleteByKeys
        ) {
            return Ok(MutationCounts::default());
        }
        return Err(write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "INSERT SQL Server senza conferma OUTPUT",
        ));
    };
    if results[0].len() != 1 {
        return Err(write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "DML SQL Server ha mutato piu righe per un record Arrow",
        ));
    }
    let value: Option<i32> = row.try_get(0).map_err(|_| {
        write_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Write,
            "tipo OUTPUT DML SQL Server incompatibile",
        )
    })?;
    match (mode, value) {
        (
            plenora_database_core::plan::WriteMode::Append
            | plenora_database_core::plan::WriteMode::TruncateInsert
            | plenora_database_core::plan::WriteMode::Create
            | plenora_database_core::plan::WriteMode::Replace
            | plenora_database_core::plan::WriteMode::Upsert,
            Some(1),
        ) => Ok(MutationCounts {
            confirmed: 1,
            inserted: 1,
            updated: 0,
            deleted: 0,
        }),
        (
            plenora_database_core::plan::WriteMode::Update
            | plenora_database_core::plan::WriteMode::Upsert,
            Some(2),
        ) => Ok(MutationCounts {
            confirmed: 1,
            inserted: 0,
            updated: 1,
            deleted: 0,
        }),
        (plenora_database_core::plan::WriteMode::DeleteByKeys, Some(3)) => Ok(MutationCounts {
            confirmed: 1,
            inserted: 0,
            updated: 0,
            deleted: 1,
        }),
        _ => Err(write_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "codice OUTPUT DML SQL Server incompatibile col piano",
        )),
    }
}

async fn inspect_target_spatial_contracts(
    session: &mut SqlServerSession,
    description: &crate::SqlServerObjectDescription,
    cancellation: &CancellationToken,
) -> Result<HashMap<String, plan::ObservedSpatialContract>> {
    let renderer = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    );
    let schema = renderer.quote_identifier(&sql_identifier(&description.schema)?)?;
    let object = renderer.quote_identifier(&sql_identifier(&description.name)?)?;
    let mut contracts = HashMap::new();
    for column in &description.columns {
        if !matches!(column.native_type.as_str(), "geometry" | "geography") {
            continue;
        }
        let name = renderer.quote_identifier(&sql_identifier(&column.name)?)?;
        let sql = format!(
            "SELECT COUNT_BIG(DISTINCT CASE WHEN {name} IS NULL THEN NULL ELSE {name}.STSrid END), \
             MIN(CASE WHEN {name} IS NULL THEN NULL ELSE {name}.STSrid END), \
             COUNT_BIG(DISTINCT CASE WHEN {name} IS NULL THEN NULL ELSE \
             CONVERT(int, {name}.HasZ) * 2 + CONVERT(int, {name}.HasM) END), \
             MIN(CASE WHEN {name} IS NULL THEN NULL ELSE \
             CONVERT(int, {name}.HasZ) * 2 + CONVERT(int, {name}.HasM) END), \
             COALESCE(SUM(CONVERT(bigint, CASE WHEN {name}.STGeometryType() = N'FullGlobe' \
             THEN 1 ELSE 0 END)), 0) FROM {schema}.{object};"
        );
        let mut results = session
            .execute_query(Query::new(sql), ErrorPhase::Prepare, cancellation)
            .await?;
        if results.len() != 1 {
            return Err(write_error(
                ErrorCategory::Protocol,
                ErrorPhase::Prepare,
                "preflight target spatial con result set inattesi",
            ));
        }
        let mut rows = results.pop().ok_or_else(|| {
            write_error(
                ErrorCategory::Protocol,
                ErrorPhase::Prepare,
                "preflight target spatial senza result set",
            )
        })?;
        if rows.len() != 1 {
            return Err(write_error(
                ErrorCategory::Protocol,
                ErrorPhase::Prepare,
                "preflight target spatial con cardinalità inattesa",
            ));
        }
        let row = rows.pop().ok_or_else(|| {
            write_error(
                ErrorCategory::Protocol,
                ErrorPhase::Prepare,
                "preflight target spatial senza riga",
            )
        })?;
        let distinct: i64 = required(&row, 0, "distinct SRID")?;
        let srid: Option<i32> = optional(&row, 1, "SRID")?;
        let dimension_count: i64 = required(&row, 2, "numero profili dimensionali")?;
        let dimension_code: Option<i32> = optional(&row, 3, "profilo dimensionale")?;
        let full_globe: i64 = required(&row, 4, "FullGlobe")?;
        if distinct > 1 {
            return Err(write_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                "target SQL Server contiene SRID misti",
            ));
        }
        if full_globe > 0 {
            return Err(write_error(
                ErrorCategory::Unsupported,
                ErrorPhase::Prepare,
                "target spatial SQL Server contiene FullGlobe",
            ));
        }
        let dimensions =
            crate::types::spatial_dimensions_from_profile(dimension_count, dimension_code)?;
        let srid = srid
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    write_error(
                        ErrorCategory::DataMapping,
                        ErrorPhase::Prepare,
                        "SRID target SQL Server negativo",
                    )
                })
            })
            .transpose()?;
        contracts.insert(
            column.name.clone(),
            plan::ObservedSpatialContract {
                srid,
                dimensions: (dimensions != plenora_database_core::geometry::Dimensions::Unknown)
                    .then_some(dimensions),
            },
        );
    }
    Ok(contracts)
}

fn required<'a, T>(row: &'a Row, index: usize, name: &str) -> Result<T>
where
    T: FromSql<'a>,
{
    optional(row, index, name)?.ok_or_else(|| {
        write_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            format!("campo preflight obbligatorio assente: {name}"),
        )
    })
}

fn optional<'a, T>(row: &'a Row, index: usize, name: &str) -> Result<Option<T>>
where
    T: FromSql<'a>,
{
    row.try_get(index).map_err(|_| {
        write_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            format!("tipo preflight incompatibile: {name}"),
        )
    })
}

fn sql_identifier(value: &str) -> Result<Identifier> {
    if value.chars().count() > crate::MAX_IDENTIFIER_CHARACTERS {
        return Err(write_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "identificatore SQL Server oltre 128 caratteri",
        ));
    }
    Identifier::new(value.to_owned())
}

struct BudgetCancellation {
    token: CancellationToken,
    deadline_task: tokio::task::JoinHandle<()>,
}

impl BudgetCancellation {
    fn new(parent: &CancellationToken, budget: &ResourceBudget) -> Self {
        let token = parent.child_token_with_deadline(Some(budget.deadline()));
        let deadline_token = token.clone();
        let deadline = tokio::time::Instant::from_std(budget.deadline());
        let deadline_task = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            deadline_token.cancel_due_to_deadline();
        });
        Self {
            token,
            deadline_task,
        }
    }

    const fn token(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for BudgetCancellation {
    fn drop(&mut self) {
        self.deadline_task.abort();
    }
}

fn write_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: impl Into<String>,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod confirmed_rollback_retry_tests {
    use super::*;

    /// Sul percorso a batch un annullamento confermato cambia l'effetto
    /// remoto, non la disposizione di retry.
    ///
    /// Un deadlock 1205 classificato `Safe` che il server ha annullato resta
    /// ritentabile: forzarlo a `Never` trasformerebbe un errore transitorio
    /// recuperabile in un fallimento definitivo. La conversione a `Never`
    /// appartiene al solo percorso row-scoped, dove il rifiuto è del dato e
    /// ritentare la stessa riga produrrebbe lo stesso rifiuto.
    #[test]
    fn a_confirmed_batch_rollback_preserves_every_retry_disposition() {
        for disposition in [
            RetryDisposition::Never,
            RetryDisposition::Quarantine,
            RetryDisposition::Safe,
            RetryDisposition::RequiresIdempotencyKey,
            RetryDisposition::RequiresRecovery,
            RetryDisposition::After(1),
        ] {
            let original = DatabaseError {
                category: ErrorCategory::Execution,
                phase: ErrorPhase::Write,
                remote_effect: RemoteEffect::Unknown,
                retry: disposition,
                provider: Some(ProviderKind::Sqlserver),
                execution_id: Some("sqlserver-batch-rollback".to_owned()),
                message: "errore pre-commit SQL Server".to_owned(),
                diagnostics: None,
            };
            let rolled_back = confirmed_rollback_axes(original);
            assert_eq!(
                rolled_back.retry, disposition,
                "la disposizione {disposition:?} non appartiene all'annullamento"
            );
            assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
        }
    }
}
