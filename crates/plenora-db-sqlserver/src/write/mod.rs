mod codec;
mod plan;
mod resources;

use crate::{describe_object, PooledSqlServerSession, SqlServerPool, SqlServerSession};
use plan::WritePlan;
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
    _operation_lease: ResourceLease,
    _columns_lease: ResourceLease,
    loss_report: LossReport,
}

impl std::fmt::Debug for PreparedSqlServerWrite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedSqlServerWrite")
            .field("operation", &self.operation)
            .field("target_schema_fingerprint", &self.plan.schema_fingerprint)
            .field("columns", &self.plan.columns.len())
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
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let columns = u64::try_from(input_schema.fields().len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, columns)?;
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
    let description = describe_object(
        pooled.session_mut()?,
        schema,
        &operation.target.object,
        control.token(),
    )
    .await?;
    let observed_srids =
        inspect_target_spatial_srids(pooled.session_mut()?, &description, control.token()).await?;
    let plan = WritePlan::compile(
        &description,
        operation,
        Arc::clone(&input_schema),
        &observed_srids,
    )?;
    drop(pooled);
    let loss_report = LossReport {
        schema_version: 1,
        policy: operation.mapping_policy,
        losses: Vec::new(),
    };
    Ok(PreparedSqlServerWrite {
        operation: operation.clone(),
        plan,
        pool: Arc::clone(pool),
        budget: budget.clone(),
        _operation_lease: operation_lease,
        _columns_lease: columns_lease,
        loss_report,
    })
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
    if input.schema().as_ref() != prepared.plan.input_schema.as_ref() {
        return Err(write_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            "schema input diverso da prepare_write SQL Server",
        ));
    }
    let control = BudgetCancellation::new(cancellation, &prepared.budget);
    let execution_id = format!(
        "sqlserver-{}-{}",
        std::process::id(),
        EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut pooled = prepared.pool.checkout(control.token()).await?;
    pooled.disallow_reuse();
    pooled.session_mut()?.begin(control.token()).await?;
    if let Err(error) = lock_and_verify_schema(&mut pooled, &prepared.plan, control.token()).await {
        return Err(rollback_after_error(&mut pooled, error).await);
    }
    if let Some(sql) = &prepared.plan.truncate_sql {
        let result = pooled
            .session_mut()?
            .execute_write_query(Query::new(sql.clone()), control.token())
            .await;
        if let Err(error) = result {
            return Err(rollback_after_error(&mut pooled, error).await);
        }
    }

    let mut received = 0_u64;
    loop {
        let batch = match input.next_batch_with_cancellation(control.token()).await {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
        };
        let reservation =
            match WriteBatchResources::reserve(&batch, &prepared.plan, &prepared.budget) {
                Ok(reservation) => reservation,
                Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            };
        received = if let Some(value) = received.checked_add(reservation.rows) {
            value
        } else {
            let error = DatabaseError::resource_limit("conteggio righe write overflow");
            return Err(rollback_after_error(&mut pooled, error).await);
        };
        for row in 0..batch.num_rows() {
            let query = match codec::bind_row(&prepared.plan, &batch, row) {
                Ok(query) => query,
                Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            };
            let results = pooled
                .session_mut()?
                .execute_query(query, ErrorPhase::Write, control.token())
                .await;
            let results = match results {
                Ok(results) => results,
                Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            };
            let confirmed = match one_insert_confirmed(&results) {
                Ok(confirmed) => confirmed,
                Err(error) => return Err(rollback_after_error(&mut pooled, error).await),
            };
            if !confirmed {
                let error = write_error(
                    ErrorCategory::Protocol,
                    ErrorPhase::Write,
                    "INSERT SQL Server senza conferma OUTPUT univoca",
                );
                return Err(rollback_after_error(&mut pooled, error).await);
            }
            if fault == Some(WriteFaultPoint::TransportLostAfterFirstInsert) {
                pooled.quarantine();
                return Err(transport_loss_error(&execution_id));
            }
        }
        if let Err(error) = reservation.commit() {
            return Err(rollback_after_error(&mut pooled, error).await);
        }
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
        let outcome = WriteOutcome {
            schema_version: 1,
            status: WriteStatus::Committed,
            execution_id,
            provider: ProviderKind::Sqlserver,
            rows: RowCounts {
                received,
                confirmed: received,
                inserted: Some(received),
                updated: None,
                deleted: None,
                failed: 0,
                skipped: 0,
            },
            layer_outcomes: Vec::new(),
            recovery: None,
        };
        outcome.validate()?;
        Ok(outcome)
    } else {
        pooled.quarantine();
        unknown_commit_outcome(&prepared, execution_id, received)
    }
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
        schema_version: 1,
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
        layer_outcomes: Vec::new(),
        recovery: Some(Recovery {
            last_certain_phase: CertainPhase::CommitOrEditRequested,
            automatic_retry_allowed: false,
            idempotency_key: None,
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

async fn lock_and_verify_schema(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    cancellation: &CancellationToken,
) -> Result<()> {
    let results = pooled
        .session_mut()?
        .execute_query(
            Query::new(plan.lock_sql.clone()),
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
    if current.token.structural_fingerprint != plan.schema_fingerprint {
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
        original.remote_effect = RemoteEffect::RolledBack;
        if pooled.allow_reuse_after_drain().is_err() {
            pooled.quarantine();
        }
    } else {
        pooled.quarantine();
    }
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
    }
}

fn one_insert_confirmed(results: &[Vec<Row>]) -> Result<bool> {
    if results.len() != 1 || results.first().is_none_or(|rows| rows.len() != 1) {
        return Ok(false);
    }
    let value: Option<i32> = results[0][0].try_get(0).map_err(|_| {
        write_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Write,
            "tipo OUTPUT INSERT SQL Server incompatibile",
        )
    })?;
    Ok(value == Some(1))
}

async fn inspect_target_spatial_srids(
    session: &mut SqlServerSession,
    description: &crate::SqlServerObjectDescription,
    cancellation: &CancellationToken,
) -> Result<HashMap<String, Option<u32>>> {
    let renderer = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    );
    let schema = renderer.quote_identifier(&sql_identifier(&description.schema)?);
    let object = renderer.quote_identifier(&sql_identifier(&description.name)?);
    let mut srids = HashMap::new();
    for column in &description.columns {
        if !matches!(column.native_type.as_str(), "geometry" | "geography") {
            continue;
        }
        let name = renderer.quote_identifier(&sql_identifier(&column.name)?);
        let sql = format!(
            "SELECT COUNT_BIG(DISTINCT CASE WHEN {name} IS NULL THEN NULL ELSE {name}.STSrid END), \
             MIN(CASE WHEN {name} IS NULL THEN NULL ELSE {name}.STSrid END), \
             COALESCE(MAX(CONVERT(int, {name}.HasZ)), 0), \
             COALESCE(MAX(CONVERT(int, {name}.HasM)), 0), \
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
        let has_z: i32 = required(&row, 2, "HasZ")?;
        let has_m: i32 = required(&row, 3, "HasM")?;
        let full_globe: i64 = required(&row, 4, "FullGlobe")?;
        if distinct > 1 {
            return Err(write_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                "target SQL Server contiene SRID misti",
            ));
        }
        if has_z > 0 || has_m > 0 || full_globe > 0 {
            return Err(write_error(
                ErrorCategory::Unsupported,
                ErrorPhase::Prepare,
                "target spatial SQL Server fuori dal profilo XY strict",
            ));
        }
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
        srids.insert(column.name.clone(), srid);
    }
    Ok(srids)
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
    }
}
