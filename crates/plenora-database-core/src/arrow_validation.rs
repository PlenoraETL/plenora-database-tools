//! Validazione integrale al confine, prima di leggere o esportare gli array.

use crate::arrow::{array::Array, RecordBatch};
use crate::provider::ProviderFuture;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};

fn invalid(phase: ErrorPhase) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        phase,
        None,
        "dati Arrow non validi",
    )
}

/// Valida ricorsivamente buffer, offset, figli e dizionari senza esporre dati.
///
/// # Errors
/// Restituisce `data_mapping` nella fase del consumer per un array malformato.
pub fn validate_array(array: &dyn Array, phase: ErrorPhase) -> Result<()> {
    array.to_data().validate_full().map_err(|_| invalid(phase))
}

/// Unico controllo dei batch esterni, compresi quelli decodificati da IPC.
///
/// # Errors
/// Restituisce `data_mapping` senza il messaggio del decoder Arrow.
pub fn validate_batch(batch: &RecordBatch, phase: ErrorPhase) -> Result<()> {
    for column in batch.columns() {
        validate_array(column.as_ref(), phase)?;
        if column.len() != batch.num_rows() {
            return Err(invalid(phase));
        }
    }
    Ok(())
}

/// Attende e valida il batch originale prima di consegnarlo al consumer.
///
/// # Errors
/// Conserva gli errori dello stream; dati malformati sono `data_mapping` nella fase richiesta.
pub async fn validated_batch(
    future: ProviderFuture<'_, Option<RecordBatch>>,
    phase: ErrorPhase,
) -> Result<Option<RecordBatch>> {
    let batch = future.await?;
    if let Some(batch) = &batch {
        validate_batch(batch, phase)?;
    }
    Ok(batch)
}

struct ValidatedInput(Box<dyn crate::provider::BatchStream>);

impl crate::provider::BatchStream for ValidatedInput {
    fn schema(&self) -> crate::arrow::SchemaRef {
        self.0.schema()
    }

    fn declared_input_rows(&self) -> Option<u64> {
        self.0.declared_input_rows()
    }

    fn row_diagnostics_policy(&self) -> crate::row_diagnostics::RowDiagnosticsPolicy {
        self.0.row_diagnostics_policy()
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a crate::CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(validated_batch(
            self.0.next_batch(cancellation),
            crate::ErrorPhase::Write,
        ))
    }
}

/// Protegge lo stream dell'host anche quando il provider e applicativo.
#[must_use]
pub fn validated_input(
    input: Box<dyn crate::provider::BatchStream>,
) -> Box<dyn crate::provider::BatchStream> {
    Box::new(ValidatedInput(input))
}
