//! Percorso Db2 row-scoped: uno statement per riga sorgente.

use crate::transaction::Db2Transaction;
use crate::write::{batch_values, validate_batch_schema, Db2WritePlan};
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::provider::BatchStream;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::row_diagnostics::{
    RowApplication, RowRejection, RowScopedWriter, RowWriteFuture, CAUSE_CONSTRAINT_VIOLATION,
};
use plenora_database_core::transaction::TransactionScope;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition, RollbackEvidence,
};

pub struct Db2RowWriter<'a> {
    transaction: &'a mut Db2Transaction,
    plan: &'a Db2WritePlan,
    input: &'a mut dyn BatchStream,
    schema: &'a SchemaRef,
    budget: &'a ResourceBudget,
    cancellation: &'a CancellationToken,
    constraint_column: Option<String>,
    batch: Option<Vec<Vec<plenora_database_core::provider::ParameterValue>>>,
    batch_start: u64,
    observed: u64,
    applied: u64,
}

impl<'a> Db2RowWriter<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        transaction: &'a mut Db2Transaction,
        plan: &'a Db2WritePlan,
        input: &'a mut dyn BatchStream,
        schema: &'a SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
        constraint_column: Option<String>,
    ) -> Self {
        Self {
            transaction,
            plan,
            input,
            schema,
            budget,
            cancellation,
            constraint_column,
            batch: None,
            batch_start: 0,
            observed: 0,
            applied: 0,
        }
    }

    pub(crate) const fn applied(&self) -> u64 {
        self.applied
    }

    async fn locate(&mut self, source_index: u64) -> Result<usize> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                let rows = u64::try_from(batch.len())
                    .map_err(|_| row_error("righe batch Db2 non rappresentabili"))?;
                let end = self
                    .batch_start
                    .checked_add(rows)
                    .ok_or_else(|| row_error("overflow nell'offset sorgente Db2"))?;
                if source_index < end {
                    return usize::try_from(source_index - self.batch_start)
                        .map_err(|_| row_error("offset batch Db2 non rappresentabile"));
                }
                self.batch_start = end;
                self.batch = None;
            }

            let batch = self
                .input
                .next_batch(self.cancellation)
                .await?
                .ok_or_else(|| row_error("input Db2 esaurito prima del totale dichiarato"))?;
            validate_batch_schema(&batch, self.schema)?;
            if batch.num_rows() == 0 {
                continue;
            }
            let rows = u64::try_from(batch.num_rows())
                .map_err(|_| row_error("righe batch Db2 non rappresentabili"))?;
            let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
            self.plan.validate_spatial_batch(&batch, self.budget)?;
            self.budget
                .try_lease(ResourceKind::Rows, rows)?
                .commit(rows)?;
            self.budget
                .try_lease(ResourceKind::MemoryBytes, bytes)?
                .commit(bytes)?;
            self.batch = Some(batch_values(&batch, self.plan)?);
        }
    }
}

impl RowScopedWriter for Db2RowWriter<'_> {
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
        Box::pin(async move {
            let offset = self.locate(source_index).await?;
            if source_index != self.observed {
                return Err(row_error("indice sorgente Db2 non contiguo"));
            }
            let statement = {
                let row = self
                    .batch
                    .as_ref()
                    .and_then(|batch| batch.get(offset))
                    .ok_or_else(|| row_error("riga Db2 assente dopo il posizionamento"))?;
                self.plan.insert_statement(std::slice::from_ref(row))
            };
            let application = match self
                .transaction
                .execute(&statement, self.cancellation)
                .await
            {
                Ok(1) => {
                    self.applied = self
                        .applied
                        .checked_add(1)
                        .ok_or_else(|| row_error("overflow nelle righe Db2 applicate"))?;
                    RowApplication::Applied
                }
                Ok(_) => {
                    return Err(protocol_error(
                        "INSERT diagnostico Db2 deve confermare esattamente una riga",
                    ));
                }
                Err(error) if error.category == ErrorCategory::Conflict => {
                    RowApplication::Rejected(RowRejection {
                        cause: CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                        column: self.constraint_column.clone(),
                    })
                }
                Err(error) => return Err(error),
            };
            self.observed = self
                .observed
                .checked_add(1)
                .ok_or_else(|| row_error("overflow nelle righe Db2 osservate"))?;
            Ok(application)
        })
    }

    fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(batch) = self.batch.take() {
                let end = self
                    .batch_start
                    .checked_add(u64::try_from(batch.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| row_error("overflow nel totale batch Db2"))?;
                if end != self.observed {
                    return Err(row_error("input Db2 oltre il totale dichiarato"));
                }
                self.batch_start = end;
            }
            loop {
                match self.input.next_batch(self.cancellation).await {
                    Ok(Some(batch)) if batch.num_rows() == 0 => {}
                    Ok(Some(_)) => return Err(row_error("input Db2 oltre il totale dichiarato")),
                    Ok(None) => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
        })
    }

    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
        Box::pin(async move {
            let cleanup = CancellationToken::new();
            if self.transaction.rollback_in_place(&cleanup).await.is_ok() {
                RollbackEvidence::Confirmed
            } else {
                RollbackEvidence::Lost
            }
        })
    }
}

fn row_error(message: &'static str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidPlan,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        provider: Some(plenora_database_core::plan::ProviderKind::Db2),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

fn protocol_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Protocol,
        ErrorPhase::Write,
        Some(plenora_database_core::plan::ProviderKind::Db2),
        message,
    )
}
