//! B-12: consumo completo degli ingressi senza dimensioni prima degli effetti.
use super::{plan::WriteColumnPlan, validate_batch_schema};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{CancellationToken, ErrorPhase, Result};
use std::collections::VecDeque;

struct Buffered {
    schema: SchemaRef,
    batches: VecDeque<(RecordBatch, ResourceLease)>,
    rows: Option<u64>,
    diagnostics: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
}

impl BatchStream for Buffered {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn declared_input_rows(&self) -> Option<u64> {
        self.rows
    }
    fn row_diagnostics_policy(
        &self,
    ) -> plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
        self.diagnostics.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(super::recovery::cancelled_write_error(cancellation, true));
            }
            Ok(self.batches.pop_front().map(|(batch, _lease)| batch))
        })
    }
}

pub(super) async fn input(
    mut source: Box<dyn BatchStream>,
    plans: &[WriteColumnPlan],
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    max_batch_bytes: u64,
    max_cell_bytes: u64,
) -> Result<Box<dyn BatchStream>> {
    if !plans.iter().any(|p| p.needs_complete_prevalidation()) {
        return Ok(source);
    }
    let schema = source.schema();
    let mut buffered = Buffered {
        schema: schema.clone(),
        batches: VecDeque::new(),
        rows: source.declared_input_rows(),
        diagnostics: source.row_diagnostics_policy(),
    };
    let mut total_rows = 0_u64;
    let mut components = 0_u64;
    loop {
        budget.ensure_active()?;
        let Some(batch) = plenora_database_core::arrow_validation::validated_batch(
            source.next_batch(cancellation),
            ErrorPhase::Validate,
        )
        .await?
        else {
            break;
        };
        validate_batch_schema(&batch, &schema)?;
        let batch_components = super::enforce_input_limits(
            &batch,
            plans,
            max_batch_bytes,
            max_cell_bytes,
            budget
                .limits()
                .geometry_components
                .saturating_sub(components),
            budget.limits().nesting_depth,
        )?;
        components = components.saturating_add(batch_components);
        total_rows = total_rows.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        if total_rows > budget.limits().rows {
            return Err(plenora_database_core::DatabaseError::resource_limit(
                "input preventivo oltre il budget righe",
            ));
        }
        let bytes = super::resources::reserved_batch_bytes(&batch, plans)?.saturating_add(256);
        let lease = budget.try_lease(ResourceKind::MemoryBytes, bytes)?;
        buffered.batches.push_back((batch, lease));
    }
    Ok(Box::new(buffered))
}
