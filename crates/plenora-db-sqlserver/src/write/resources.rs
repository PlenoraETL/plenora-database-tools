use super::codec::inspect_batch;
use super::plan::WritePlan;
use super::SqlServerInsertMode;
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
        insert_mode: SqlServerInsertMode,
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
        let descriptor_bytes = if insert_mode == SqlServerInsertMode::TdsBulk {
            bulk_descriptor_bytes(inspection.rows, plan.columns.len())?
        } else {
            0
        };
        let memory_bytes = inspection
            .bytes
            .checked_add(descriptor_bytes)
            .ok_or_else(|| DatabaseError::resource_limit("memoria codec TDS bulk overflow"))?;
        if memory_bytes > budget.remaining(ResourceKind::MemoryBytes)
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
            memory_lease: Some(budget.try_lease(ResourceKind::MemoryBytes, memory_bytes)?),
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
        memory.release()?;
        if self.geometry_components > 0 {
            self.geometry_lease
                .ok_or_else(|| DatabaseError::resource_limit("budget geometrico assente"))?
                .commit(self.geometry_components)?;
        }
        Ok(())
    }
}

fn bulk_descriptor_bytes(rows: u64, columns: usize) -> Result<u64> {
    let columns = u64::try_from(columns)
        .map_err(|_| DatabaseError::resource_limit("colonne bulk non rappresentabili"))?;
    let row_bytes = u64::try_from(std::mem::size_of::<tiberius::TokenRow<'static>>())
        .map_err(|_| DatabaseError::resource_limit("descriptor riga bulk non rappresentabile"))?;
    let cell_bytes = u64::try_from(std::mem::size_of::<tiberius::ColumnData<'static>>())
        .map_err(|_| DatabaseError::resource_limit("descriptor cella bulk non rappresentabile"))?;
    let cells = rows
        .checked_mul(columns)
        .ok_or_else(|| DatabaseError::resource_limit("celle bulk overflow"))?;
    rows.checked_mul(row_bytes)
        .and_then(|rows_size| {
            cells
                .checked_mul(cell_bytes)
                .and_then(|cells_size| rows_size.checked_add(cells_size))
        })
        .ok_or_else(|| DatabaseError::resource_limit("descriptor TDS bulk overflow"))
}
