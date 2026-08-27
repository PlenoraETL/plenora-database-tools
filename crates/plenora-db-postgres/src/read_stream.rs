use super::{
    arrow::{read_mapping_error, ColumnBuffer},
    error::{classify_error, public_error},
    metrics::PostgresMetrics,
    types::{ColumnKind, ColumnSpec},
    PooledClient, PostgresProvider, PostgresTlsMode,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use futures_util::{Stream, StreamExt};
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::resource::{ResourceBudget, ResourceLease};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, ReadDiagnosticsTracker, Result,
};
use plenora_database_engine::{inspect_spatial_arrays, DeadlineGuard, ReadBatchReservation};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio_postgres::{CancelToken, NoTls, Row};
use tokio_postgres_rustls::MakeRustlsConnect;

pub type BudgetCancellation = DeadlineGuard;

pub struct PostgresBatchStream {
    client: PooledClient,
    cancel_token: CancelToken,
    tls_mode: PostgresTlsMode,
    tls_connector: MakeRustlsConnect,
    cancel_timeout_ms: u64,
    rows: PostgresRows,
    columns: Vec<ColumnSpec>,
    schema: SchemaRef,
    batch_rows: usize,
    target_batch_bytes: Option<u64>,
    max_batch_bytes: u64,
    max_wkb_cell_bytes: u64,
    budget: ResourceBudget,
    _operation_lease: ResourceLease,
    _columns_lease: ResourceLease,
    metrics: Arc<PostgresMetrics>,
    byte_estimate_scale_permille: u64,
    track_byte_estimate: bool,
    batches_since_byte_estimate: u8,
    finished: bool,
    /// Cursore delle righe già consegnate, base dell'indice sorgente
    /// pubblicato dalla diagnostica row-scoped di lettura.
    read_diagnostics: ReadDiagnosticsTracker,
}

pub type PostgresRows =
    Pin<Box<dyn Stream<Item = std::result::Result<Row, tokio_postgres::Error>> + Send>>;

pub struct ReadStreamSource {
    client: PooledClient,
    cancel_token: CancelToken,
    rows: PostgresRows,
    columns: Vec<ColumnSpec>,
    schema: SchemaRef,
}

impl ReadStreamSource {
    pub fn new(
        client: PooledClient,
        cancel_token: CancelToken,
        rows: PostgresRows,
        columns: Vec<ColumnSpec>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            client,
            cancel_token,
            rows,
            columns,
            schema,
        }
    }
}

fn adaptive_builder_capacity(
    columns: &[ColumnSpec],
    batch_rows: usize,
    target_batch_bytes: Option<u64>,
) -> usize {
    let Some(target) = target_batch_bytes else {
        return batch_rows;
    };
    let row_bytes = columns.iter().fold(0_u64, |total, column| {
        total.saturating_add(column.initial_arrow_bytes_per_row())
    });
    let rows = target / row_bytes.max(1);
    batch_rows.min(usize::try_from(rows.max(1)).unwrap_or(usize::MAX))
}

impl PostgresBatchStream {
    pub fn new(
        provider: &PostgresProvider,
        source: ReadStreamSource,
        budget: &ResourceBudget,
        operation_lease: ResourceLease,
        columns_lease: ResourceLease,
    ) -> Self {
        let ReadStreamSource {
            client,
            cancel_token,
            rows,
            columns,
            schema,
        } = source;
        Self {
            client,
            cancel_token,
            tls_mode: provider.tls_mode,
            tls_connector: provider.tls_config.connector().clone(),
            cancel_timeout_ms: provider.network_options.connect_timeout_ms,
            rows,
            columns,
            schema,
            batch_rows: provider.batch_rows,
            target_batch_bytes: provider
                .target_batch_bytes
                .map(|target| target.min(provider.max_batch_bytes)),
            max_batch_bytes: provider.max_batch_bytes,
            max_wkb_cell_bytes: provider.max_wkb_cell_bytes,
            budget: budget.clone(),
            _operation_lease: operation_lease,
            _columns_lease: columns_lease,
            metrics: Arc::clone(&provider.metrics),
            byte_estimate_scale_permille: 1_500,
            track_byte_estimate: true,
            batches_since_byte_estimate: 0,
            finished: false,
            read_diagnostics: ReadDiagnosticsTracker::default(),
        }
    }
    async fn cancelled<T>(&mut self, cancellation: &CancellationToken) -> Result<T> {
        self.metrics.cancellation();
        self.client.invalidate();
        self.finished = true;
        let _cancel_result = tokio::time::timeout(
            StdDuration::from_millis(self.cancel_timeout_ms),
            cancel_query(&self.cancel_token, self.tls_mode, &self.tls_connector),
        )
        .await;
        Err(cancelled_read_error(cancellation))
    }

    async fn deadline_exceeded<T>(&mut self) -> Result<T> {
        self.client.invalidate();
        self.finished = true;
        let _cancel_result = tokio::time::timeout(
            StdDuration::from_millis(self.cancel_timeout_ms),
            cancel_query(&self.cancel_token, self.tls_mode, &self.tls_connector),
        )
        .await;
        Err(deadline_read_error())
    }

    async fn next_row_before_deadline(
        &mut self,
    ) -> Result<Option<std::result::Result<Row, tokio_postgres::Error>>> {
        let Some(remaining) = self.budget.remaining_duration() else {
            return self.deadline_exceeded().await;
        };
        let next = {
            let mut rows = self.rows.as_mut();
            tokio::select! {
                row = rows.next() => Some(row),
                () = tokio::time::sleep(remaining) => None,
            }
        };
        match next {
            Some(row) => Ok(row),
            None => self.deadline_exceeded().await,
        }
    }

    fn reserve_batch(&self) -> Result<ReadBatchReservation> {
        self.budget.ensure_active()?;
        let has_spatial = self
            .columns
            .iter()
            .any(|column| matches!(column.kind, ColumnKind::Geometry | ColumnKind::Geography));
        ReadBatchReservation::acquire(
            &self.budget,
            self.batch_rows,
            Some(self.max_batch_bytes),
            has_spatial,
        )
    }

    fn observe_batch_size(&mut self, actual_bytes: u64, estimated_bytes: u64) {
        let Some(target) = self.target_batch_bytes else {
            return;
        };
        if self.track_byte_estimate && estimated_bytes > 0 {
            self.byte_estimate_scale_permille = actual_bytes
                .saturating_mul(1_100)
                .checked_div(estimated_bytes)
                .unwrap_or(8_000)
                .clamp(1_000, 8_000);
            if actual_bytes.saturating_mul(4) < target {
                self.track_byte_estimate = false;
                self.batches_since_byte_estimate = 0;
            }
        } else if !self.track_byte_estimate {
            self.batches_since_byte_estimate = self.batches_since_byte_estimate.saturating_add(1);
            if self.batches_since_byte_estimate >= 8 {
                self.track_byte_estimate = true;
                self.batches_since_byte_estimate = 0;
            }
        }
    }
}

impl PostgresBatchStream {
    #[allow(clippy::too_many_lines)]
    fn next_batch_inner(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            if self.budget.remaining_duration().is_none() {
                return self.deadline_exceeded().await;
            }
            let reservation = self.reserve_batch()?;
            let target_batch_bytes = self
                .target_batch_bytes
                .map(|target| target.min(reservation.byte_limit));
            let builder_capacity =
                adaptive_builder_capacity(&self.columns, reservation.row_limit, target_batch_bytes);
            let mut buffers = self
                .columns
                .iter()
                .map(|column| ColumnBuffer::new(column, builder_capacity))
                .collect::<Vec<_>>();
            let mut row_count = 0;
            let mut estimated_bytes = 0_u64;
            let mut target_limited = false;
            while row_count < reservation.row_limit {
                match self.next_row_before_deadline().await? {
                    Some(Ok(row)) => {
                        for (index, buffer) in buffers.iter_mut().enumerate() {
                            match buffer.append(&row, index) {
                                Ok(bytes) if self.track_byte_estimate => {
                                    estimated_bytes = estimated_bytes.saturating_add(bytes);
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    self.client.invalidate();
                                    return Err(attribute_conversion_defect(
                                        &self.read_diagnostics,
                                        &self.columns,
                                        error,
                                        Some(row_count),
                                        Some(index),
                                    ));
                                }
                            }
                        }
                        row_count += 1;
                        if row_count < reservation.row_limit
                            && self.track_byte_estimate
                            && target_batch_bytes.is_some_and(|target| {
                                estimated_bytes.saturating_mul(self.byte_estimate_scale_permille)
                                    >= target.saturating_mul(1_000)
                            })
                        {
                            target_limited = true;
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        self.client.invalidate();
                        return Err(classify_error(ErrorPhase::Read, &error));
                    }
                    None => {
                        self.finished = true;
                        break;
                    }
                }
            }
            if row_count == 0 {
                return Ok(None);
            }
            let arrays = buffers
                .iter_mut()
                .map(ColumnBuffer::finish)
                .collect::<Vec<_>>();
            let batch = match RecordBatch::try_new(Arc::clone(&self.schema), arrays) {
                Ok(batch) => batch,
                Err(error) => {
                    self.client.invalidate();
                    return Err(DatabaseError::from(error));
                }
            };
            let geometry_components = match enforce_batch_limits(
                &batch,
                &self.columns,
                reservation.byte_limit,
                self.max_wkb_cell_bytes.min(self.budget.limits().cell_bytes),
                reservation.component_limit,
                self.budget.limits().nesting_depth,
                &self.read_diagnostics,
            ) {
                Ok(components) => components,
                Err(error) => {
                    self.client.invalidate();
                    return Err(error);
                }
            };
            let actual_bytes = batch_memory_bytes(&batch);
            let actual_rows = u64::try_from(batch.num_rows()).map_err(|_| {
                DatabaseError::resource_limit("numero righe batch non rappresentabile")
            })?;
            reservation.commit(actual_rows, actual_bytes, geometry_components)?;
            // Il cursore avanza solo su un batch che sta per essere
            // consegnato: un batch fallito non ha righe pubblicate da contare.
            self.read_diagnostics.publish_batch(actual_rows)?;
            self.observe_batch_size(actual_bytes, estimated_bytes);
            self.metrics.read_batch(
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                actual_bytes,
            );
            if target_limited {
                self.metrics.target_limited_batch();
            }
            Ok(Some(batch))
        })
    }
}

impl BatchStream for PostgresBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            // Stream già terminato: cancel su stream drenato non è un errore.
            if self.finished {
                return Ok(None);
            }
            if cancellation.is_cancelled() {
                return self.cancelled(cancellation).await;
            }
            let completed = {
                let next = self.next_batch_inner();
                tokio::pin!(next);
                tokio::select! {
                    result = &mut next => Some(result),
                    _reason = cancellation.cancelled() => None,
                }
            };
            if let Some(result) = completed {
                result
            } else {
                self.cancelled(cancellation).await
            }
        })
    }
}

impl Drop for PostgresBatchStream {
    fn drop(&mut self) {
        if !self.finished {
            self.client.invalidate();
        }
    }
}

pub async fn cancel_and_invalidate(
    client: &mut PooledClient,
    tls_mode: PostgresTlsMode,
    tls_connector: &MakeRustlsConnect,
    timeout_ms: u64,
) {
    client.record_cancellation();
    let Ok(active_client) = client.client() else {
        client.invalidate();
        return;
    };
    let token = active_client.cancel_token();
    client.invalidate();
    let _cancel_result = tokio::time::timeout(
        StdDuration::from_millis(timeout_ms),
        cancel_query(&token, tls_mode, tls_connector),
    )
    .await;
}

async fn cancel_query(
    token: &CancelToken,
    tls_mode: PostgresTlsMode,
    tls_connector: &MakeRustlsConnect,
) -> std::result::Result<(), tokio_postgres::Error> {
    match tls_mode {
        PostgresTlsMode::Disabled => token.cancel_query(NoTls).await,
        PostgresTlsMode::Require => token.cancel_query(tls_connector.clone()).await,
    }
}

pub fn cancelled_read_error(cancellation: &CancellationToken) -> DatabaseError {
    let category = crate::error::interruption_category(cancellation);
    public_error(
        category,
        ErrorPhase::Read,
        false,
        if category == ErrorCategory::Timeout {
            "durata massima query PostgreSQL esaurita"
        } else {
            "query PostgreSQL cancellata sul server"
        },
    )
}

fn deadline_read_error() -> DatabaseError {
    public_error(
        ErrorCategory::Timeout,
        ErrorPhase::Read,
        false,
        "durata massima query PostgreSQL esaurita",
    )
}

/// Attribuisce un difetto di conversione alla riga sorgente e alla colonna del
/// piano che il percorso ha potuto osservare.
///
/// L'indice del batch e l'indice di colonna sono posizioni provate dal
/// percorso, non deduzioni: quando una delle due manca, il documento dichiara
/// il limite invece di completarlo. Il nome della colonna arriva dal piano
/// compilato, mai dal messaggio del server.
fn attribute_conversion_defect(
    diagnostics: &ReadDiagnosticsTracker,
    columns: &[ColumnSpec],
    error: DatabaseError,
    row: Option<usize>,
    column: Option<usize>,
) -> DatabaseError {
    let column = column
        .and_then(|index| columns.get(index))
        .map(|column| column.name.as_str());
    diagnostics.reject_conversion_defect(error, row.and_then(|row| u64::try_from(row).ok()), column)
}

fn enforce_batch_limits(
    batch: &RecordBatch,
    columns: &[ColumnSpec],
    max_batch_bytes: u64,
    max_wkb_cell_bytes: u64,
    max_geometry_components: u64,
    max_geometry_depth: u64,
    diagnostics: &ReadDiagnosticsTracker,
) -> Result<u64> {
    let bytes = batch_memory_bytes(batch);
    if bytes > max_batch_bytes {
        return Err(public_error(
            ErrorCategory::ResourceLimit,
            ErrorPhase::Read,
            false,
            "RecordBatch PostgreSQL oltre max_batch_bytes",
        ));
    }
    let mut arrays = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        if matches!(column.kind, ColumnKind::Geometry | ColumnKind::Geography) {
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .ok_or_else(|| {
                    read_mapping_error("colonna spaziale PostgreSQL non codificata come Binary")
                })?;
            arrays.push((index, array));
        }
    }
    inspect_spatial_arrays(
        arrays,
        max_geometry_components,
        max_wkb_cell_bytes,
        max_geometry_depth,
        |error, row, column| {
            attribute_conversion_defect(diagnostics, columns, error, Some(row), Some(column))
        },
        |_, _, _| Ok(()),
    )
}

pub fn batch_memory_bytes(batch: &RecordBatch) -> u64 {
    batch.columns().iter().fold(0_u64, |total, array| {
        total.saturating_add(u64::try_from(array.get_array_memory_size()).unwrap_or(u64::MAX))
    })
}

#[cfg(test)]
#[path = "read_stream_tests.rs"]
mod tests;
