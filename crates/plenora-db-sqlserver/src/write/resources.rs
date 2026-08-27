use super::codec::{inspect_batch, inspect_row};
use super::plan::WritePlan;
use super::SqlServerInsertMode;
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{DatabaseError, Result};
use plenora_database_engine::WriteResourceReservation;

pub(super) type WriteRowResources = WriteResourceReservation;
pub(super) type WriteBatchResources = WriteResourceReservation;

pub(super) fn reserve_write_row(
    batch: &RecordBatch,
    row: usize,
    plan: &WritePlan,
    budget: &ResourceBudget,
) -> Result<WriteRowResources> {
    let inspection = inspect_row(
        batch,
        row,
        plan,
        budget.limits().cell_bytes,
        budget.remaining(ResourceKind::GeometryComponents),
        budget.limits().nesting_depth,
    )?;
    WriteRowResources::acquire(
        budget,
        1,
        inspection.bytes,
        inspection.bytes,
        inspection.geometry_components,
    )
}

pub(super) fn reserve_write_batch(
    batch: &RecordBatch,
    plan: &WritePlan,
    budget: &ResourceBudget,
    insert_mode: SqlServerInsertMode,
) -> Result<WriteBatchResources> {
    let inspection = inspect_batch(
        batch,
        plan,
        budget.limits().cell_bytes,
        budget.remaining(ResourceKind::GeometryComponents),
        budget.limits().nesting_depth,
    )?;
    let descriptor_bytes = if insert_mode == SqlServerInsertMode::TdsBulk {
        bulk_descriptor_bytes(inspection.rows, plan.columns.len())?
    } else {
        0
    };
    let memory_bytes = inspection
        .bytes
        .checked_add(descriptor_bytes)
        .ok_or_else(|| DatabaseError::resource_limit("memoria codec TDS bulk overflow"))?;
    WriteBatchResources::acquire(
        budget,
        inspection.rows,
        inspection.bytes,
        memory_bytes,
        inspection.geometry_components,
    )
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
