//! Streaming Arrow per i provider della famiglia MySQL.
//!
//! Espone `DatabaseSession.read(schema, table, projection, order_by, limit)`
//! che ritorna un `BatchReader` (pyclass provider-agnostic, ereditato
//! dal path Postgres). Il consumer Python itera bytes Arrow IPC stream:
//!
//! ```python
//! import io, pyarrow.ipc as ipc
//! for chunk in s.read("mydb", "events", limit=100_000):
//!     batch = ipc.open_stream(io.BytesIO(chunk)).read_all()
//! ```
//!
//! Riusa gli helper generici di `crate::arrow_reader`:
//! - `BatchReader` pyclass (provider-agnostic)
//! - `make_read_operation(schema, object, projection, order_by, limit)`
//! - `default_budget()`
//!
//! Differisce dal path Postgres solo nel tipo del provider e nella
//! chiamata `provider.read(...)`.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::future_not_send,
    clippy::redundant_pub_crate
)]

use crate::arrow_reader::{default_budget, make_read_operation, BatchReader};
use crate::runtime;
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::{CancellationToken, DatabaseError};
use std::sync::Arc;

/// Apre un `BatchReader` su una tabella MySQL.
///
/// La size dei batch è decisa internamente dal provider MySQL (buffered
/// dal `mysql_async` cursor server-side).
///
/// # Errors
///
/// `DatabaseError` se `order_by` ha direzione invalida, o se il provider
/// fallisce ad aprire lo stream.
pub(crate) fn open_family_reader(
    provider: &Arc<dyn Provider>,
    secret: &SecretString,
    schema: &str,
    object: &str,
    projection: Vec<String>,
    order_by: Vec<(String, String)>,
    limit: Option<u64>,
) -> Result<BatchReader, DatabaseError> {
    let operation = make_read_operation(schema, object, projection, order_by, limit)?;
    let provider_arc = Arc::clone(provider);
    let secret_owned = secret.clone();
    let cancellation = CancellationToken::new();
    let stream_cancellation = cancellation.clone();
    let stream = runtime().block_on(async move {
        provider_arc
            .read(
                &secret_owned,
                &operation,
                &ParameterBag::default(),
                &default_budget(),
                &stream_cancellation,
            )
            .await
    })?;
    Ok(BatchReader::new(stream, cancellation))
}
