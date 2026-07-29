use super::codec::inspect_batch;
use super::plan::WritePlan;
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{DatabaseError, Result};

pub(super) struct WriteBatchResources {
    pub(super) rows: u64,
    rows_lease: Option<ResourceLease>,
    output_lease: Option<ResourceLease>,
    memory_lease: Option<ResourceLease>,
    bytes: u64,
    geometry_components: u64,
    geometry_lease: Option<ResourceLease>,
}

impl WriteBatchResources {
    pub(super) fn reserve(
        batch: &RecordBatch,
        plan: &WritePlan,
        budget: &ResourceBudget,
    ) -> Result<Self> {
        budget.ensure_active()?;
        let component_limit = budget.remaining(ResourceKind::GeometryComponents);
        let inspection = inspect_batch(
            batch,
            plan,
            budget.limits().cell_bytes,
            component_limit,
            budget.limits().nesting_depth,
        )?;
        if inspection.rows == 0 {
            return Ok(Self {
                rows: 0,
                rows_lease: None,
                output_lease: None,
                memory_lease: None,
                bytes: 0,
                geometry_components: 0,
                geometry_lease: None,
            });
        }
        if inspection.bytes > budget.remaining(ResourceKind::MemoryBytes)
            || inspection.bytes > budget.remaining(ResourceKind::OutputBytes)
        {
            return Err(DatabaseError::resource_limit(
                "batch Arrow oltre il budget write SQL Server",
            ));
        }
        Ok(Self {
            rows: inspection.rows,
            rows_lease: Some(budget.try_lease(ResourceKind::Rows, inspection.rows)?),
            output_lease: Some(budget.try_lease(ResourceKind::OutputBytes, inspection.bytes)?),
            memory_lease: Some(budget.try_lease(ResourceKind::MemoryBytes, inspection.bytes)?),
            bytes: inspection.bytes,
            geometry_components: inspection.geometry_components,
            geometry_lease: (inspection.geometry_components > 0)
                .then(|| {
                    budget.try_lease(
                        ResourceKind::GeometryComponents,
                        inspection.geometry_components,
                    )
                })
                .transpose()?,
        })
    }

    pub(super) fn commit(self) -> Result<()> {
        let (Some(rows), Some(output), Some(memory)) =
            (self.rows_lease, self.output_lease, self.memory_lease)
        else {
            return Ok(());
        };
        rows.commit(self.rows)?;
        output.commit(self.bytes)?;
        drop(memory);
        if self.geometry_components > 0 {
            self.geometry_lease
                .ok_or_else(|| DatabaseError::resource_limit("budget geometrico assente"))?
                .commit(self.geometry_components)?;
        }
        Ok(())
    }
}
