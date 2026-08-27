use super::{enforce_input_limits, plan::WriteColumnPlan, WriteRuntime};
use arrow_array::{Array, RecordBatch};
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{DatabaseError, Result};
use plenora_database_engine::WriteResourceReservation;

pub(super) type WriteBatchResources = WriteResourceReservation;

pub(super) fn reserve_write_batch(
    batch: &RecordBatch,
    plans: &[WriteColumnPlan],
    runtime: &WriteRuntime,
    budget: &ResourceBudget,
) -> Result<WriteBatchResources> {
    let geometry_components = enforce_input_limits(
        batch,
        plans,
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
    let bytes = batch
        .columns()
        .iter()
        .try_fold(0_u64, |total, array| {
            total.checked_add(u64::try_from(array.get_array_memory_size()).unwrap_or(u64::MAX))
        })
        .ok_or_else(|| DatabaseError::resource_limit("overflow nel conteggio byte del batch"))?;
    WriteBatchResources::acquire(budget, rows, bytes, bytes, geometry_components)
}
