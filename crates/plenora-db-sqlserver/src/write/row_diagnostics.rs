//! Percorso SQL Server row-scoped qualificato per `Prepared` append atomico.

use super::plan::WritePlan;
use super::resources::reserve_write_row;
use super::{row_mutation, MutationCounts, SqlServerInsertMode, WriteFaultPoint};
use crate::connection::RowQueryResult;
use crate::PooledSqlServerSession;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::plan::{ProviderKind, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::BatchStream;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::row_diagnostics::{
    RowApplication, RowDiagnosticsPolicy, RowRejection, RowScopedWriter, RowWriteFuture,
    CAUSE_CONSTRAINT_VIOLATION,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition, RollbackEvidence, WriteDiagnosticsTracker,
};

pub(super) struct DiagnosticInput {
    pub(super) input_total: u64,
    pub(super) policy: RowDiagnosticsPolicy,
    pub(super) tracker: WriteDiagnosticsTracker,
}

pub(super) fn validate_input(
    prepared_schema: &SchemaRef,
    stream_schema: &SchemaRef,
    operation: &WriteOperation,
    insert_mode: SqlServerInsertMode,
    input_total: u64,
    policy: RowDiagnosticsPolicy,
) -> Result<DiagnosticInput> {
    if prepared_schema.as_ref() != stream_schema.as_ref() {
        return Err(DatabaseError::invalid_plan(
            "schema stream SQL Server diverso dallo schema preparato",
        ));
    }
    if operation.mode != WriteMode::Append
        || operation.transaction_profile != TransactionProfile::SingleTransaction
        || insert_mode != SqlServerInsertMode::Prepared
    {
        return Err(DatabaseError::invalid_plan(
            "la diagnostica SQL Server supporta solo Prepared Append con SingleTransaction",
        ));
    }
    let tracker = WriteDiagnosticsTracker::new(input_total, policy.clone())?;
    for field in [
        policy.key_field.as_deref(),
        policy.constraint_column.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if prepared_schema.field_with_name(field).is_err() {
            return Err(DatabaseError::invalid_plan(
                "policy row-scoped riferita a un campo assente dallo schema preparato",
            ));
        }
    }
    Ok(DiagnosticInput {
        input_total,
        policy,
        tracker,
    })
}

pub(super) struct SqlServerRowWriter<'a> {
    pooled: &'a mut PooledSqlServerSession,
    input: &'a mut dyn BatchStream,
    plan: &'a WritePlan,
    budget: &'a ResourceBudget,
    cancellation: &'a CancellationToken,
    constraint_column: Option<String>,
    batch: Option<RecordBatch>,
    batch_start: u64,
    applied: u64,
    mutations: MutationCounts,
    fault: Option<WriteFaultPoint>,
    execution_id: &'a str,
}

impl<'a> SqlServerRowWriter<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        pooled: &'a mut PooledSqlServerSession,
        input: &'a mut dyn BatchStream,
        plan: &'a WritePlan,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
        constraint_column: Option<String>,
        fault: Option<WriteFaultPoint>,
        execution_id: &'a str,
    ) -> Self {
        Self {
            pooled,
            input,
            plan,
            budget,
            cancellation,
            constraint_column,
            batch: None,
            batch_start: 0,
            applied: 0,
            mutations: MutationCounts {
                confirmed: 0,
                inserted: 0,
                updated: 0,
                deleted: 0,
            },
            fault,
            execution_id,
        }
    }

    pub(super) const fn mutations(&self) -> MutationCounts {
        self.mutations
    }

    async fn locate(&mut self, source_index: u64) -> Result<usize> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                let end = checked_batch_end(self.batch_start, batch_rows(batch)?)?;
                if source_index < end {
                    let offset = source_index.checked_sub(self.batch_start).ok_or_else(|| {
                        diagnostic_error(
                            ErrorCategory::InvalidPlan,
                            "indice sorgente SQL Server gia superato",
                        )
                    })?;
                    return usize::try_from(offset).map_err(|_| {
                        diagnostic_error(
                            ErrorCategory::InvalidPlan,
                            "offset di batch SQL Server non rappresentabile",
                        )
                    });
                }
                self.batch_start = end;
                self.batch = None;
            }
            let batch = self
                .input
                .next_batch(self.cancellation)
                .await
                .map_err(provider_write_error)?
                .ok_or_else(short_input_error)?;
            if batch.schema().as_ref() != self.plan.input_schema.as_ref() {
                return Err(diagnostic_error(
                    ErrorCategory::Schema,
                    "schema batch SQL Server diverso dallo schema preparato",
                ));
            }
            if batch.num_rows() > 0 {
                self.batch = Some(batch);
            }
        }
    }
}

impl RowScopedWriter for SqlServerRowWriter<'_> {
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
        Box::pin(async move {
            let offset = self.locate(source_index).await?;
            let (query, resources) = {
                let batch = self.batch.as_ref().ok_or_else(|| {
                    diagnostic_error(
                        ErrorCategory::Protocol,
                        "batch SQL Server assente dopo il posizionamento",
                    )
                })?;
                (
                    super::codec::bind_row(self.plan, batch, offset)?,
                    reserve_write_row(batch, offset, self.plan, self.budget)?,
                )
            };
            match self
                .pooled
                .session_mut()?
                .execute_row_query(query, self.cancellation)
                .await?
            {
                RowQueryResult::Applied(results) => {
                    if let Err(error) = validate_append_output_cardinality(
                        results.len(),
                        results.first().map_or(0, Vec::len),
                    ) {
                        self.pooled.quarantine();
                        return Err(error);
                    }
                    let mutation = row_mutation(&results, WriteMode::Append).map_err(|error| {
                        self.pooled.quarantine();
                        ambiguous_affected_rows(error)
                    })?;
                    resources.commit()?;
                    self.mutations.checked_add(mutation)?;
                    self.applied = self.applied.checked_add(1).ok_or_else(|| {
                        diagnostic_error(
                            ErrorCategory::ResourceLimit,
                            "overflow nelle righe applicate SQL Server",
                        )
                    })?;
                    if self.fault == Some(WriteFaultPoint::TransportLostAfterFirstInsert) {
                        self.pooled.quarantine();
                        return Err(super::transport_loss_error(self.execution_id));
                    }
                    Ok(RowApplication::Applied)
                }
                RowQueryResult::ServerRejected { code, error } => {
                    if let Some(cause) = row_rejection_cause_from_code(code) {
                        Ok(RowApplication::Rejected(RowRejection {
                            cause: cause.to_owned(),
                            column: self.constraint_column.clone(),
                        }))
                    } else {
                        Err(error)
                    }
                }
            }
        })
    }

    fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(batch) = self.batch.take() {
                let end = checked_batch_end(self.batch_start, batch_rows(&batch)?)?;
                validate_consumed_batch_end(end, self.applied)?;
                self.batch_start = end;
            }
            loop {
                match self.input.next_batch(self.cancellation).await {
                    Ok(Some(batch)) if batch.num_rows() == 0 => {}
                    Ok(Some(_)) => return Err(extra_input_error()),
                    Ok(None) => return Ok(()),
                    Err(error) => return Err(provider_write_error(error)),
                }
            }
        })
    }

    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
        Box::pin(async move {
            let cleanup = CancellationToken::new();
            let session = self.pooled.session_mut();
            let confirmed = match session {
                Ok(session) => {
                    #[cfg(test)]
                    if self.fault == Some(WriteFaultPoint::DelayRollbackResponse) {
                        session
                            .rollback_with_delayed_response(&cleanup)
                            .await
                            .is_ok()
                    } else {
                        session.rollback(&cleanup).await.is_ok()
                    }
                    #[cfg(not(test))]
                    {
                        session.rollback(&cleanup).await.is_ok()
                    }
                }
                Err(_) => false,
            };
            if confirmed {
                if self.pooled.allow_reuse_after_drain().is_err() {
                    self.pooled.quarantine();
                }
                RollbackEvidence::Confirmed
            } else {
                self.pooled.quarantine();
                RollbackEvidence::Lost
            }
        })
    }
}

pub(super) const fn row_rejection_cause_from_code(code: u32) -> Option<&'static str> {
    match code {
        515 | 547 | 2_601 | 2_627 => Some(CAUSE_CONSTRAINT_VIOLATION),
        _ => None,
    }
}

fn validate_append_output_cardinality(result_sets: usize, rows: usize) -> Result<()> {
    if result_sets == 1 && rows == 1 {
        Ok(())
    } else {
        Err(ambiguous_affected_rows(diagnostic_error(
            ErrorCategory::Protocol,
            "INSERT diagnostico SQL Server deve confermare esattamente una riga",
        )))
    }
}

fn batch_rows(batch: &RecordBatch) -> Result<u64> {
    u64::try_from(batch.num_rows()).map_err(|_| {
        diagnostic_error(
            ErrorCategory::ResourceLimit,
            "righe batch SQL Server non rappresentabili",
        )
    })
}

fn checked_batch_end(batch_start: u64, rows: u64) -> Result<u64> {
    plenora_database_core::checked_source_row_end(batch_start, rows).ok_or_else(|| {
        diagnostic_error(
            ErrorCategory::ResourceLimit,
            "overflow nell'offset sorgente SQL Server",
        )
    })
}

fn validate_consumed_batch_end(end: u64, applied: u64) -> Result<()> {
    if end == applied {
        Ok(())
    } else {
        Err(extra_input_error())
    }
}

fn short_input_error() -> DatabaseError {
    diagnostic_error(
        ErrorCategory::InvalidPlan,
        "input SQL Server esaurito prima delle righe dichiarate",
    )
}

fn extra_input_error() -> DatabaseError {
    diagnostic_error(
        ErrorCategory::InvalidPlan,
        "input SQL Server oltre il totale dichiarato",
    )
}

const fn ambiguous_affected_rows(mut error: DatabaseError) -> DatabaseError {
    error.remote_effect = RemoteEffect::Unknown;
    error.retry = RetryDisposition::Quarantine;
    error
}

const fn provider_write_error(mut error: DatabaseError) -> DatabaseError {
    error.phase = ErrorPhase::Write;
    error.provider = Some(ProviderKind::Sqlserver);
    error
}

fn diagnostic_error(category: ErrorCategory, message: &str) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

#[cfg(test)]
#[path = "row_diagnostics_tests.rs"]
mod tests;
