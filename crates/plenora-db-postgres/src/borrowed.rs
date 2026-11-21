//! Arrow su connessioni del chiamante. Nessuna apertura o chiusura di transazioni.
//!
//! La scrittura non modifica percorso, ruolo o timeout della sessione.
//! La lettura non modifica impostazioni. Interrompere o abbandonare il suo
//! stream non invia un `CancelRequest` su una nuova connessione: il chiamante
//! conserva il controllo della cancellazione server e dei suoi timeout.

use arrow_array::RecordBatch;
use futures_util::{Stream, StreamExt};
use plenora_database_core::plan::ReadOperation;
use plenora_database_core::provider::{BatchStream, ParameterBag};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{CancellationToken, Result};

pub use crate::write::{scrivi_arrow, BorrowedWriteOutcome as WriteOutcome, WritePlan};

/// Piano di lettura, con gli stessi parametri e codec del provider.
pub struct ReadPlan {
    pub operation: ReadOperation,
    pub parameters: ParameterBag,
    pub batch_rows: usize,
    pub max_batch_bytes: u64,
    pub max_wkb_cell_bytes: u64,
}

impl ReadPlan {
    #[must_use]
    pub fn new(operation: ReadOperation, parameters: ParameterBag) -> Self {
        let provider = crate::PostgresProvider::default();
        Self {
            operation,
            parameters,
            batch_rows: provider.batch_rows,
            max_batch_bytes: provider.max_batch_bytes,
            max_wkb_cell_bytes: provider.max_wkb_cell_bytes,
        }
    }
}

/// Legge Arrow senza possedere la connessione. Il budget e il token sono
/// condivisi con lo stream, che termina al primo errore.
/// # Errors
/// Errori tipizzati di piano, catalogo, budget o cancellazione.
pub async fn leggi_arrow<'c>(
    client: &'c tokio_postgres::Client,
    lettura: &ReadPlan,
    budget: &ResourceBudget,
    annulla: &CancellationToken,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'c> {
    let provider = crate::PostgresProvider {
        batch_rows: lettura.batch_rows,
        max_batch_bytes: lettura.max_batch_bytes,
        max_wkb_cell_bytes: lettura.max_wkb_cell_bytes,
        ..crate::PostgresProvider::default()
    };
    let control = crate::read_stream::BudgetCancellation::new(annulla, budget)?;
    let opening = crate::query_execution::read_on(
        &provider,
        client,
        None,
        &lettura.operation,
        &lettura.parameters,
        budget,
        control.token(),
    );
    let stream = crate::control::select_with_cancellation(opening, control.token())
        .await
        .unwrap_or_else(|| Err(crate::read_stream::cancelled_read_error(control.token())))?;
    Ok(futures_util::stream::try_unfold(
        (stream, control, client),
        |(mut stream, control, client)| async move {
            Ok(stream
                .next_batch(control.token())
                .await?
                .map(|batch| (batch, (stream, control, client))))
        },
    )
    .fuse())
}
