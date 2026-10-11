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
    let bytes = reserved_batch_bytes(batch, plans)?;
    WriteBatchResources::acquire(budget, rows, bytes, bytes, geometry_components)
}

/// I byte che il batch occupa mentre si scrive: le colonne, piu la copia EWKB
/// delle colonne `wkb` con SRID dichiarato, che convive con l'originale finche
/// il batch riscritto non lo sostituisce. La copia si prenota qui, prima di
/// allocarla: valori, quattro byte di SRID per riga e offset.
pub(super) fn reserved_batch_bytes(batch: &RecordBatch, plans: &[WriteColumnPlan]) -> Result<u64> {
    let overflow = || DatabaseError::resource_limit("overflow nel conteggio byte del batch");
    let mut total = 0_u64;
    for (plan, array) in plans.iter().zip(batch.columns()) {
        let size = u64::try_from(array.get_array_memory_size()).map_err(|_| overflow())?;
        total = total.checked_add(size).ok_or_else(overflow)?;
        if plan.needs_srid_stamp() {
            let rows = u64::try_from(array.len()).map_err(|_| overflow())?;
            let copy = rows
                .checked_mul(8)
                .and_then(|extra| extra.checked_add(size))
                .ok_or_else(overflow)?;
            total = total.checked_add(copy).ok_or_else(overflow)?;
        }
    }
    Ok(total)
}
