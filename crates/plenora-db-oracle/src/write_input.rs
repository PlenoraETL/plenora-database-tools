//! Il rifiuto dei vuoti precede anche il DDL e i batch gia validi.
//! I LOB vuoti sono esclusi per policy, indipendentemente dalla misura del
//! locator: non si inventa una riscrittura nel fixture del server.
use plenora_database_core::arrow::array::{Array, BinaryArray, LargeBinaryArray, StringArray};
use plenora_database_core::arrow::schema::DataType;
use plenora_database_core::arrow::{schema::SchemaRef, RecordBatch};
use plenora_database_core::plan::{ProviderKind, WriteOperation};
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};

use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::VecDeque;

pub const EMPTY_KEY: &str = "chiave vuota Oracle non ammessa";
pub const EMPTY_VALUE: &str = "valore vuoto Oracle non ammesso";

struct ValidatedInput {
    schema: SchemaRef,
    rows: Option<u64>,
    batches: VecDeque<(RecordBatch, ResourceLease)>,
}

impl BatchStream for ValidatedInput {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn declared_input_rows(&self) -> Option<u64> {
        self.rows
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            check_cancelled(cancellation)?;
            Ok(self.batches.pop_front().map(|(batch, _lease)| batch))
        })
    }
}

/// Legge l'ingresso con budget prima di ogni effetto remoto. Un limite di
/// memoria rifiuta l'operazione; non autorizza a saltare la validazione.
pub async fn reject_empty_input(
    mut input: Box<dyn BatchStream>,
    operation: &WriteOperation,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    let schema = input.schema();
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            matches!(
                field.data_type(),
                DataType::Utf8 | DataType::Binary | DataType::LargeBinary
            )
        })
        .map(|(index, field)| (index, operation.keys.contains(field.name())))
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Ok(input);
    }
    let mut validated = ValidatedInput {
        schema: schema.clone(),
        rows: input.declared_input_rows(),
        batches: VecDeque::new(),
    };
    while let Some(batch) = input.next_batch(cancellation).await? {
        check_cancelled(cancellation)?;
        budget.ensure_active()?;
        if batch.schema() != schema {
            return Err(DatabaseError::new(
                ErrorCategory::InvalidPlan,
                ErrorPhase::Write,
                Some(ProviderKind::Oracle),
                "schema dello stream diverso dallo schema preparato",
            ));
        }
        for (index, key) in &columns {
            let array = batch.column(*index);
            if (0..batch.num_rows()).any(|row| !array.is_null(row) && is_empty(array.as_ref(), row))
            {
                return Err(DatabaseError::new(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Write,
                    Some(ProviderKind::Oracle),
                    if *key { EMPTY_KEY } else { EMPTY_VALUE },
                ));
            }
        }
        let bytes = u64::try_from(batch.get_array_memory_size())
            .unwrap_or(u64::MAX)
            .max(1);
        let lease = budget.try_lease(ResourceKind::MemoryBytes, bytes)?;
        validated.batches.push_back((batch, lease));
    }
    Ok(Box::new(validated))
}

fn is_empty(array: &dyn Array, row: usize) -> bool {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return values.value(row).is_empty();
    }
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        return values.value(row).is_empty();
    }
    array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .is_some_and(|values| values.value(row).is_empty())
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(crate::error::interruption_error(
            cancellation,
            ErrorPhase::Write,
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "write_input_tests.rs"]
mod tests;
