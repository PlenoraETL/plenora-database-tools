use super::{enforce_input_limits, plan::WriteColumnPlan, WriteRuntime};
use arrow_array::{Array, RecordBatch};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{DatabaseError, Result};

pub(super) struct WriteBatchResources {
    pub(super) rows: u64,
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

    pub(super) fn commit(self) -> Result<()> {
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
