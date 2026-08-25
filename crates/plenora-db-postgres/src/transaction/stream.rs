//! Cursor server-side wrappato in `RowStream`. Il cursor Postgres è
//! transaction-scoped: commit/rollback lo chiude automaticamente.

use super::decode::decode_rows;
use crate::control::select_with_cancellation;
use crate::error::{check_cancelled, classify_error, public_error};
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
            // Fix review post-79665ca: `next_batch` faceva `.await`
            // diretto. Un cancel durante il FETCH restava in coda al
            // termine della query server-side. Ora race in-flight con
            // `select_with_cancellation`. Sul cancel marcato exhausted
            // per bloccare successivi `next_batch` — il cursor viene
            // chiuso dal commit/rollback della tx.
            let Some(fetch_result) =
                select_with_cancellation(self.client.query(fetch_sql.as_str(), &[]), cancellation)
                    .await
            else {
                self.exhausted = true;
                return Err(public_error(
                    crate::error::interruption_category(cancellation),
                    ErrorPhase::Read,
                    false,
                    "FETCH FORWARD interrotto durante l'esecuzione",
                ));
            };
            let rows = fetch_result.map_err(|error| classify_error(ErrorPhase::Read, &error))?;
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
