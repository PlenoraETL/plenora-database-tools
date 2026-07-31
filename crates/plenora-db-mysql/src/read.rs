use crate::{
    bind_parameters, describe_object, MysqlColumnBuffer, MysqlColumnKind, MysqlPool, MysqlReadPlan,
};
use plenora_database_core::arrow::array::{Array, BinaryArray};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::plan::ReadOperation;
use plenora_database_core::provider::{BatchStream, ParameterBag, ProviderFuture};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use std::sync::Arc;
use tokio::sync::mpsc;

const ROW_CHANNEL_CAPACITY: usize = 1;
pub const DEFAULT_BATCH_ROWS: usize = 8_192;
pub const MAX_BATCH_ROWS: usize = 65_536;

/// Avvia una lettura `MySQL` a memoria limitata e con schema ricontrollato.
///
/// # Errors
///
/// Fallisce chiuso per schema instabile, mapping non supportato, parametri
/// incoerenti, budget esaurito, cancellazione o errore del protocollo.
#[allow(clippy::significant_drop_tightening)]
pub async fn read_operation(
    pool: &Arc<MysqlPool>,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    batch_rows: usize,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    validate_batch_rows(batch_rows)?;
    budget.ensure_active()?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget);
    let internal = budget_cancellation.token().clone();
    let mut session = pool.checkout(&internal).await?;
    let schema = operation.source.schema.as_deref().ok_or_else(|| {
        read_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "schema MySQL obbligatorio per la lettura",
        )
    })?;
    let description =
        describe_object(&mut session, schema, &operation.source.object, &internal).await?;
    let plan = MysqlReadPlan::compile(&description, operation)?;
    let column_count = u64::try_from(plan.columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne MySQL non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    let parameters = bind_parameters(&plan.bind_names, parameters)?;
    let confirmation =
        describe_object(&mut session, schema, &operation.source.object, &internal).await?;
    if description.token != confirmation.token {
        return Err(read_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            "schema MySQL cambiato durante la preparazione",
        ));
    }

    let (sender, receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
    let worker_cancellation = internal.clone();
    let sql = plan.sql.clone();
    tokio::spawn(async move {
        let error_sender = sender.clone();
        if let Err(error) = session
            .pump_exec_rows(sql, parameters, sender, &worker_cancellation)
            .await
        {
            let _ = error_sender.send(Err(error)).await;
        }
    });
    let deadline_task = budget_cancellation.take_task()?;
    Ok(Box::new(MysqlBatchStream {
        receiver,
        columns: plan.columns,
        schema: plan.schema,
        batch_rows,
        budget: budget.clone(),
        cancellation: internal,
        deadline_task,
        _operation_lease: operation_lease,
        _columns_lease: columns_lease,
        finished: false,
    }))
}

pub struct MysqlBatchStream {
    receiver: mpsc::Receiver<Result<mysql_async::Row>>,
    columns: Vec<crate::MysqlColumnSpec>,
    schema: SchemaRef,
    batch_rows: usize,
    budget: ResourceBudget,
    cancellation: CancellationToken,
    deadline_task: tokio::task::JoinHandle<()>,
    _operation_lease: ResourceLease,
    _columns_lease: ResourceLease,
    finished: bool,
}

impl BatchStream for MysqlBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
        Box::pin(async move { self.next_batch_inner().await })
    }

    fn next_batch_with_cancellation<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                self.cancellation.cancel();
                self.finished = true;
                return Err(cancelled_read_error());
            }
            let completed = {
                let next = self.next_batch_inner();
                tokio::pin!(next);
                tokio::select! {
                    result = &mut next => Some(result),
                    _ = cancellation.cancelled() => None,
                }
            };
            if let Some(result) = completed {
                result
            } else {
                self.cancellation.cancel();
                self.finished = true;
                Err(cancelled_read_error())
            }
        })
    }
}

impl MysqlBatchStream {
    async fn next_batch_inner(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        self.budget.ensure_active()?;
        if self.cancellation.is_cancelled() {
            self.finished = true;
            return Err(cancelled_read_error());
        }
        let reservation = BatchReservation::new(&self.budget, self.batch_rows, &self.columns)?;
        let capacity = reservation.row_limit.min(1_024);
        let mut buffers = self
            .columns
            .iter()
            .map(|column| MysqlColumnBuffer::new(column, capacity))
            .collect::<Vec<_>>();
        let mut row_count = 0_usize;
        while row_count < reservation.row_limit {
            let received = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.finished = true;
                    return Err(cancelled_read_error());
                }
                row = self.receiver.recv() => row,
            };
            match received {
                Some(Ok(row)) => {
                    for (index, buffer) in buffers.iter_mut().enumerate() {
                        if let Err(error) =
                            buffer.append(&row, index, self.budget.limits().cell_bytes)
                        {
                            self.cancellation.cancel();
                            self.finished = true;
                            return Err(error);
                        }
                    }
                    row_count = row_count.saturating_add(1);
                }
                Some(Err(error)) => {
                    self.finished = true;
                    return Err(error);
                }
                None => {
                    self.finished = true;
                    break;
                }
            }
        }
        if row_count == 0 {
            return Ok(None);
        }
        let arrays = buffers
            .iter_mut()
            .map(MysqlColumnBuffer::finish)
            .collect::<Vec<_>>();
        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(DatabaseError::from)?;
        let actual_bytes = batch.columns().iter().try_fold(0_u64, |total, array| {
            let bytes = u64::try_from(array.get_array_memory_size()).map_err(|_| {
                DatabaseError::resource_limit("dimensione batch MySQL non rappresentabile")
            })?;
            total
                .checked_add(bytes)
                .ok_or_else(|| DatabaseError::resource_limit("dimensione batch MySQL in overflow"))
        })?;
        let components = validate_spatial_batch(
            &batch,
            &self.columns,
            reservation.component_limit,
            self.budget.limits().cell_bytes,
            self.budget.limits().nesting_depth,
        )?;
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| DatabaseError::resource_limit("righe MySQL non rappresentabili"))?;
        if let Err(error) = reservation.commit(rows, actual_bytes, components) {
            self.cancellation.cancel();
            self.finished = true;
            return Err(error);
        }
        Ok(Some(batch))
    }
}

impl Drop for MysqlBatchStream {
    fn drop(&mut self) {
        self.deadline_task.abort();
        if !self.finished {
            self.cancellation.cancel();
        }
    }
}

#[derive(Debug)]
struct BatchReservation {
    rows_lease: ResourceLease,
    memory_lease: ResourceLease,
    output_lease: ResourceLease,
    geometry_lease: Option<ResourceLease>,
    row_limit: usize,
    byte_limit: u64,
    component_limit: u64,
}

impl BatchReservation {
    fn new(
        budget: &ResourceBudget,
        batch_rows: usize,
        columns: &[crate::MysqlColumnSpec],
    ) -> Result<Self> {
        let rows = budget
            .remaining(ResourceKind::Rows)
            .min(u64::try_from(batch_rows).unwrap_or(u64::MAX));
        let bytes = budget
            .remaining(ResourceKind::MemoryBytes)
            .min(budget.remaining(ResourceKind::OutputBytes));
        if rows == 0 || bytes == 0 {
            return Err(DatabaseError::resource_limit("budget MySQL read esaurito"));
        }
        let has_spatial = columns
            .iter()
            .any(|column| column.kind == MysqlColumnKind::Geometry);
        let component_limit = if has_spatial {
            budget.remaining(ResourceKind::GeometryComponents)
        } else {
            0
        };
        if has_spatial && component_limit == 0 {
            return Err(DatabaseError::resource_limit(
                "budget componenti geometriche MySQL esaurito",
            ));
        }
        Ok(Self {
            rows_lease: budget.try_lease(ResourceKind::Rows, rows)?,
            memory_lease: budget.try_lease(ResourceKind::MemoryBytes, bytes)?,
            output_lease: budget.try_lease(ResourceKind::OutputBytes, bytes)?,
            geometry_lease: has_spatial
                .then(|| budget.try_lease(ResourceKind::GeometryComponents, component_limit))
                .transpose()?,
            row_limit: usize::try_from(rows).unwrap_or(usize::MAX),
            byte_limit: bytes,
            component_limit,
        })
    }

    fn commit(self, rows: u64, bytes: u64, components: u64) -> Result<()> {
        if bytes == 0 || bytes > self.byte_limit {
            return Err(DatabaseError::resource_limit(
                "batch Arrow MySQL oltre il budget memoria/output",
            ));
        }
        self.rows_lease.commit(rows)?;
        self.memory_lease.commit(bytes)?;
        self.output_lease.commit(bytes)?;
        if components > 0 {
            self.geometry_lease
                .ok_or_else(|| DatabaseError::resource_limit("budget geometrico MySQL assente"))?
                .commit(components)?;
        }
        Ok(())
    }
}

fn validate_spatial_batch(
    batch: &RecordBatch,
    columns: &[crate::MysqlColumnSpec],
    component_limit: u64,
    cell_limit: u64,
    nesting_depth: u64,
) -> Result<u64> {
    let mut components = 0_u64;
    for (index, column) in columns.iter().enumerate() {
        if column.kind != MysqlColumnKind::Geometry {
            continue;
        }
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                read_error(
                    ErrorCategory::Internal,
                    ErrorPhase::Read,
                    "array spatial MySQL non binario",
                )
            })?;
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let value = array.value(row);
            let length = u64::try_from(value.len())
                .map_err(|_| DatabaseError::resource_limit("WKB MySQL non rappresentabile"))?;
            if length > cell_limit {
                return Err(DatabaseError::resource_limit(
                    "WKB MySQL oltre il limite cella",
                ));
            }
            let remaining = component_limit.checked_sub(components).ok_or_else(|| {
                DatabaseError::resource_limit("componenti geometriche MySQL esaurite")
            })?;
            if remaining == 0 {
                return Err(DatabaseError::resource_limit(
                    "componenti geometriche MySQL esaurite",
                ));
            }
            let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(
                value,
                remaining,
                nesting_depth,
            )?;
            if inspection.root.srid.is_some() || inspection.root.dimensions_label() != "xy" {
                return Err(read_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    "ST_AsBinary MySQL ha prodotto WKB non XY o con SRID embedded",
                ));
            }
            components = components
                .checked_add(inspection.stats.components)
                .ok_or_else(|| {
                    DatabaseError::resource_limit("componenti geometriche MySQL in overflow")
                })?;
        }
    }
    Ok(components)
}

fn validate_batch_rows(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 || batch_rows > MAX_BATCH_ROWS {
        return Err(read_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Validate,
            "batch_rows MySQL fuori intervallo 1..=65536",
        ));
    }
    Ok(())
}

struct BudgetCancellation {
    token: CancellationToken,
    deadline_task: Option<tokio::task::JoinHandle<()>>,
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
            deadline_task: Some(deadline_task),
        }
    }

    const fn token(&self) -> &CancellationToken {
        &self.token
    }

    fn take_task(&mut self) -> Result<tokio::task::JoinHandle<()>> {
        self.deadline_task.take().ok_or_else(|| {
            read_error(
                ErrorCategory::Internal,
                ErrorPhase::Prepare,
                "task deadline MySQL assente",
            )
        })
    }
}

impl Drop for BudgetCancellation {
    fn drop(&mut self) {
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
    }
}

fn cancelled_read_error() -> DatabaseError {
    read_error(
        ErrorCategory::Cancelled,
        ErrorPhase::Read,
        "lettura MySQL cancellata",
    )
}

fn read_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: impl Into<String>,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::resource::ResourceLimits;

    #[test]
    fn invalid_batch_size_is_rejected_before_io() {
        assert_eq!(
            validate_batch_rows(0).expect_err("zero batch").category,
            ErrorCategory::InvalidPlan
        );
        assert_eq!(
            validate_batch_rows(MAX_BATCH_ROWS + 1)
                .expect_err("oversized batch")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    #[test]
    fn reservation_fails_before_allocation_when_rows_are_exhausted() {
        let budget = ResourceBudget::new(ResourceLimits {
            rows: 1,
            ..ResourceLimits::default()
        })
        .expect("budget");
        let consumed = budget.try_lease(ResourceKind::Rows, 1).expect("row lease");
        consumed.commit(1).expect("commit row");
        assert_eq!(
            BatchReservation::new(&budget, 1, &[])
                .expect_err("exhausted")
                .category,
            ErrorCategory::ResourceLimit
        );
    }
}
