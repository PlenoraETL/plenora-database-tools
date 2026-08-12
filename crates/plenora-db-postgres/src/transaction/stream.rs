//! Cursor server-side wrappato in `RowStream`. Il cursor Postgres è
//! transaction-scoped: commit/rollback lo chiude automaticamente.

use super::decode::decode_rows;
use crate::error::{check_cancelled, classify_error};
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::row::Row;
use plenora_database_core::transaction::RowStream;
use plenora_database_core::{CancellationToken, ErrorPhase};

pub struct PostgresRowStream<'a> {
    pub(super) client: &'a tokio_postgres::Client,
    pub(super) cursor_name: String,
    pub(super) batch_size: u32,
    pub(super) exhausted: bool,
}

impl RowStream for PostgresRowStream<'_> {
    fn next_batch<'b>(
        &'b mut self,
        cancellation: &'b CancellationToken,
    ) -> ProviderFuture<'b, Option<Vec<Row>>> {
        Box::pin(async move {
            if self.exhausted {
                return Ok(None);
            }
            check_cancelled(cancellation, ErrorPhase::Read)?;
            let fetch_sql = format!(
                "FETCH FORWARD {} FROM {}",
                self.batch_size, self.cursor_name
            );
            let rows = self
                .client
                .query(fetch_sql.as_str(), &[])
                .await
                .map_err(|error| classify_error(ErrorPhase::Read, &error))?;
            let n = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            let out = decode_rows(&rows)?;
            if n < self.batch_size {
                self.exhausted = true;
            }
            if out.is_empty() {
                Ok(None)
            } else {
                Ok(Some(out))
            }
        })
    }
}
