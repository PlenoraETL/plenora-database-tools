use crate::error::{interruption_error, timeout_error};
use crate::{
    bind_parameters, describe_object, MysqlColumnBuffer, MysqlColumnKind, MysqlPool, MysqlReadPlan,
};
use mysql_async::prelude::StatementLike;
use mysql_async::{Params, Row, Value};
use plenora_database_core::arrow::array::{Array, BinaryArray};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::plan::ReadOperation;
use plenora_database_core::provider::{BatchStream, ParameterBag, ProviderFuture};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, ReadDiagnosticsPolicy,
    ReadDiagnosticsTracker, RemoteEffect, Result, RetryDisposition,
};
use std::sync::Arc;
use tokio::sync::mpsc;

const ROW_CHANNEL_CAPACITY: usize = 1;
const CONSERVATIVE_CELL_BYTES: u64 = 64;
pub const DEFAULT_BATCH_ROWS: usize = 8_192;
pub const MAX_BATCH_ROWS: usize = 65_536;

/// Avvia una lettura `MySQL` a memoria limitata e con schema ricontrollato.
///
/// # Errors
///
/// Fallisce chiuso per schema instabile, mapping non supportato, parametri
/// incoerenti, budget esaurito, cancellazione o errore del protocollo.
#[allow(clippy::significant_drop_tightening)]
pub async fn read_operation(
    pool: &Arc<MysqlPool>,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    batch_rows: usize,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    validate_batch_rows(batch_rows)?;
    ensure_active_read_budget(budget)?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget);
    let internal = budget_cancellation.token().clone();
    let mut session = pool.checkout(&internal).await?;
    let schema = operation.source.schema.as_deref().ok_or_else(|| {
        read_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "schema MySQL obbligatorio per la lettura",
        )
    })?;
    let description =
        describe_object(&mut session, schema, &operation.source.object, &internal).await?;
    let plan = MysqlReadPlan::compile(&description, operation)?;
    let column_count = u64::try_from(plan.columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne MySQL non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    let parameters = bind_parameters(&plan.bind_names, parameters)?;
    let confirmation =
        describe_object(&mut session, schema, &operation.source.object, &internal).await?;
    if description.token != confirmation.token {
        return Err(read_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            "schema MySQL cambiato durante la preparazione",
        ));
    }

    let sql = plan.sql.clone();
    start_row_stream(
        session,
        sql,
        parameters,
        plan,
        batch_rows,
        budget,
        internal,
        &mut budget_cancellation,
        operation_lease,
        columns_lease,
    )
}

/// Esegue una `QueryOperation` scalare a sorgente singola.
///
/// Rendering, validazione dell'AST e binding dei parametri avvengono prima di
/// qualsiasi contatto con il server. Lo schema di output arriva dai metadati
/// di `COM_STMT_PREPARE`, l'unica descrizione autoritativa disponibile su
/// `MySQL`, e lo statement viene poi eseguito una sola volta sullo stream
/// bounded a domanda gia usato dal path di lettura.
///
/// # Errors
///
/// Fallisce chiuso per AST non qualificato, identificatori oltre il limite
/// `MySQL`, parametri mancanti o eccedenti, budget esaurito, cancellazione,
/// deadline o errore del protocollo.
#[allow(clippy::significant_drop_tightening)]
pub async fn query_operation(
    pool: &Arc<MysqlPool>,
    database: &str,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    batch_rows: usize,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    validate_batch_rows(batch_rows)?;
    if cancellation.is_cancelled() {
        return Err(interruption_error(
            cancellation,
            ErrorPhase::Prepare,
            RemoteEffect::None,
        ));
    }
    let rendered = crate::query::render_query(operation, database)?;
    let bind_names = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    if bind_names.len() > crate::MAX_BIND_PARAMETERS {
        return Err(DatabaseError::resource_limit(
            "query MySQL oltre il limite di placeholder del prepared statement",
        ));
    }
    let bound = bind_parameters(&bind_names, parameters)?;
    ensure_active_read_budget(budget)?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget);
    let internal = budget_cancellation.token().clone();
    let mut session = pool.checkout(&internal).await?;
    let statement = session.prepare_statement(&rendered.sql, &internal).await?;
    if usize::from(statement.num_params()) != bind_names.len() {
        return Err(read_error(
            ErrorCategory::Protocol,
            ErrorPhase::Prepare,
            "placeholder MySQL diversi dai bind renderizzati",
        ));
    }
    let metadata = statement.columns();
    let columns = crate::query::query_result_columns(&metadata)?;
    let plan = MysqlReadPlan::from_query_columns(rendered.sql, bind_names, columns)?;
    let column_count = u64::try_from(plan.columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne MySQL non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    start_row_stream(
        session,
        statement,
        bound,
        plan,
        batch_rows,
        budget,
        internal,
        &mut budget_cancellation,
        operation_lease,
        columns_lease,
    )
}

/// Avvia il worker bounded a domanda condiviso da lettura e query.
#[allow(clippy::too_many_arguments)]
fn start_row_stream<S>(
    mut session: crate::MysqlSession,
    statement: S,
    parameters: Params,
    plan: MysqlReadPlan,
    batch_rows: usize,
    budget: &ResourceBudget,
    internal: CancellationToken,
    budget_cancellation: &mut BudgetCancellation,
    operation_lease: ResourceLease,
    columns_lease: ResourceLease,
) -> Result<Box<dyn BatchStream>>
where
    S: StatementLike + 'static,
{
    let (sender, receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
    let (demand_sender, demand_receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
    let worker_cancellation = internal.clone();
    let worker_task = tokio::spawn(async move {
        let error_sender = sender.clone();
        if let Err(error) = session
            .pump_exec_rows(
                statement,
                parameters,
                sender,
                demand_receiver,
                &worker_cancellation,
            )
            .await
        {
            let _ = error_sender.send(Err(error)).await;
        }
    });
    let deadline_task = budget_cancellation.take_task()?;
    Ok(Box::new(MysqlBatchStream {
        receiver,
        demand_sender,
        columns: plan.columns,
        schema: plan.schema,
        batch_rows,
        budget: budget.clone(),
        cancellation: internal,
        deadline_task,
        worker_task: Some(worker_task),
        _operation_lease: operation_lease,
        _columns_lease: columns_lease,
        state: MysqlStreamState::Active,
        pending: None,
        read_diagnostics: ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default())?,
    }))
}

/// Riga gia letta dal worker e gia misurata, non ancora accodata ai buffer.
struct MeasuredRow {
    row: Row,
    bytes: u64,
}

/// Riga che non e entrata nel batch precedente e apre quello successivo.
///
/// Il carry-over vale al massimo una riga: il worker resta a domanda, quindi
/// nessun prefetch cresce oltre la riga gia letta. Finche la riga attende, la
/// sua stima conservativa resta prenotata sul budget memoria: non esiste
/// memoria trattenuta dallo stream e non contabilizzata.
struct PendingRow {
    row: MeasuredRow,
    lease: ResourceLease,
}

impl PendingRow {
    /// Restituisce la quota al budget prima che la riserva del batch
    /// successivo prenoti di nuovo l'intero residuo.
    fn release(self) -> Result<MeasuredRow> {
        self.lease.release()?;
        Ok(self.row)
    }
}

/// Esito del riempimento di un batch: buffer, righe accodate e la riga
/// eventualmente rinviata al batch successivo.
struct BatchFill {
    buffers: Vec<MysqlColumnBuffer>,
    rows: usize,
    deferred: Option<MeasuredRow>,
}

pub struct MysqlBatchStream {
    receiver: mpsc::Receiver<Result<Row>>,
    demand_sender: mpsc::Sender<()>,
    columns: Vec<crate::MysqlColumnSpec>,
    schema: SchemaRef,
    batch_rows: usize,
    budget: ResourceBudget,
    cancellation: CancellationToken,
    deadline_task: tokio::task::JoinHandle<()>,
    worker_task: Option<tokio::task::JoinHandle<()>>,
    _operation_lease: ResourceLease,
    _columns_lease: ResourceLease,
    state: MysqlStreamState,
    /// Riga letta dal worker e non entrata nel batch precedente: apre il
    /// prossimo batch nella posizione sorgente che le spetta.
    pending: Option<PendingRow>,
    /// Cursore delle righe già consegnate, base dell'indice sorgente
    /// pubblicato dalla diagnostica row-scoped di lettura.
    read_diagnostics: ReadDiagnosticsTracker,
}

enum MysqlStreamState {
    Active,
    Drained,
    Failed(DatabaseError),
}

impl MysqlStreamState {
    fn terminal_result(&self) -> Option<Result<Option<RecordBatch>>> {
        match self {
            Self::Active => None,
            Self::Drained => Some(Ok(None)),
            Self::Failed(error) => Some(Err(error.clone())),
        }
    }
}

impl BatchStream for MysqlBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            // Stream già terminato (Drained o Failed): risultato terminale
            // ha precedenza sulla cancellazione — cancel su stream chiuso
            // non è un errore, restituisce lo stato esistente.
            if let Some(result) = self.state.terminal_result() {
                return result;
            }
            if cancellation.is_cancelled() {
                let error = interruption_error(cancellation, ErrorPhase::Read, RemoteEffect::None);
                self.fail(error.clone());
                return Err(error);
            }
            let completed = {
                let next = self.next_batch_inner();
                tokio::pin!(next);
                tokio::select! {
                    result = &mut next => Some(result),
                    _ = cancellation.cancelled() => None,
                }
            };
            completed.unwrap_or_else(|| {
                let error = interruption_error(cancellation, ErrorPhase::Read, RemoteEffect::None);
                self.fail(error.clone());
                Err(error)
            })
        })
    }
}

impl MysqlBatchStream {
    async fn next_batch_inner(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(result) = self.state.terminal_result() {
            return result;
        }
        let result = self.next_active_batch().await;
        match &result {
            Ok(Some(_)) => {}
            Ok(None) => self.state = MysqlStreamState::Drained,
            Err(error) => self.fail(error.clone()),
        }
        result
    }

    /// Misura la riga con la stima conservativa del percorso.
    ///
    /// Ogni difetto di conversione osservato qui è ancora attribuibile: la
    /// riga non ha raggiunto il consumatore, quindi porta con sé l'indice
    /// sorgente assoluto.
    fn measure_row(&self, row: &Row, row_count: usize) -> Result<u64> {
        conservative_row_bytes(row, self.columns.len()).map_err(|error| {
            attribute_conversion_defect(
                &self.read_diagnostics,
                &self.columns,
                error,
                Some(row_count),
                None,
            )
        })
    }

    /// Accoda una riga già misurata ai buffer del batch in costruzione.
    fn append_row(
        &self,
        row: &Row,
        buffers: &mut [MysqlColumnBuffer],
        row_count: usize,
    ) -> Result<()> {
        for (index, buffer) in buffers.iter_mut().enumerate() {
            buffer
                .append(row, index, self.budget.limits().cell_bytes)
                .map_err(|error| {
                    attribute_conversion_defect(
                        &self.read_diagnostics,
                        &self.columns,
                        error,
                        Some(row_count),
                        Some(index),
                    )
                })?;
        }
        Ok(())
    }

    /// Riempie i buffer finché il residuo di righe o di byte lo consente.
    ///
    /// La decisione guarda la riga effettivamente letta, non un massimo
    /// teorico: una riga che non entra nel residuo corrente viene rinviata
    /// intera al batch successivo, mai scartata e mai spezzata. La prima riga
    /// del batch non ha un successivo dove essere rinviata, quindi oltre il
    /// budget è un errore dichiarato e non un batch vuoto.
    async fn fill_batch(
        &mut self,
        reservation: &BatchReservation,
        carried: Option<MeasuredRow>,
    ) -> Result<BatchFill> {
        let capacity = bounded_buffer_capacity(
            reservation.row_limit,
            reservation.byte_limit,
            self.columns.len(),
        )?;
        let mut buffers = self
            .columns
            .iter()
            .map(|column| MysqlColumnBuffer::new(column, capacity))
            .collect::<Vec<_>>();
        let mut carried = carried;
        let mut rows = 0_usize;
        let mut estimated_bytes = 0_u64;
        let mut deferred = None;
        while rows < reservation.row_limit {
            let measured = if let Some(measured) = carried.take() {
                measured
            } else {
                // Una sola domanda per riga: il worker non produce nulla che
                // il batch non abbia chiesto, quindi il prefetch resta nullo.
                let _ = self.demand_sender.send(()).await;
                let received = tokio::select! {
                    _ = self.cancellation.cancelled() => {
                        return Err(interruption_error(
                            &self.cancellation,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ));
                    }
                    row = self.receiver.recv() => row,
                };
                match received {
                    Some(Ok(row)) => MeasuredRow {
                        bytes: self.measure_row(&row, rows)?,
                        row,
                    },
                    Some(Err(error)) => return Err(error),
                    None => break,
                }
            };
            let next_estimate = estimated_bytes
                .checked_add(measured.bytes)
                .ok_or_else(|| DatabaseError::resource_limit("stima batch MySQL in overflow"))?;
            if next_estimate > reservation.byte_limit {
                if rows == 0 {
                    return Err(DatabaseError::resource_limit(
                        "riga MySQL oltre il budget memoria del batch",
                    ));
                }
                deferred = Some(measured);
                break;
            }
            self.append_row(&measured.row, &mut buffers, rows)?;
            estimated_bytes = next_estimate;
            rows = rows.saturating_add(1);
        }
        Ok(BatchFill {
            buffers,
            rows,
            deferred,
        })
    }

    /// Trattiene la riga rinviata prenotandone la stima sul budget memoria.
    ///
    /// La prenotazione avviene dopo il commit della riserva, che ha appena
    /// restituito il residuo non consumato: la riga in attesa resta quindi
    /// visibile al budget per tutto il tempo in cui occupa memoria.
    fn park(&mut self, deferred: Option<MeasuredRow>) -> Result<()> {
        let Some(row) = deferred else {
            return Ok(());
        };
        let lease = self
            .budget
            .try_lease(ResourceKind::MemoryBytes, row.bytes)?;
        self.pending = Some(PendingRow { row, lease });
        Ok(())
    }

    async fn next_active_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cancellation.is_cancelled() {
            return Err(interruption_error(
                &self.cancellation,
                ErrorPhase::Read,
                RemoteEffect::None,
            ));
        }
        ensure_active_read_budget(&self.budget)?;
        // Il carry-over restituisce la sua quota prima della riserva: la
        // riserva prenota tutto il residuo, quindi la stessa riga risulterebbe
        // altrimenti contabilizzata due volte.
        let carried = self.pending.take().map(PendingRow::release).transpose()?;
        let reservation = BatchReservation::new(&self.budget, self.batch_rows, &self.columns)?;
        let mut fill = self.fill_batch(&reservation, carried).await?;
        if fill.rows == 0 {
            return Ok(None);
        }
        let arrays = fill
            .buffers
            .iter_mut()
            .map(MysqlColumnBuffer::finish)
            .collect::<Vec<_>>();
        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(DatabaseError::from)?;
        let actual_bytes = batch.columns().iter().try_fold(0_u64, |total, array| {
            let bytes = u64::try_from(array.get_array_memory_size()).map_err(|_| {
                DatabaseError::resource_limit("dimensione batch MySQL non rappresentabile")
            })?;
            total
                .checked_add(bytes)
                .ok_or_else(|| DatabaseError::resource_limit("dimensione batch MySQL in overflow"))
        })?;
        let components = validate_spatial_batch(
            &batch,
            &self.columns,
            reservation.component_limit,
            self.budget.limits().cell_bytes,
            self.budget.limits().nesting_depth,
            &self.read_diagnostics,
        )?;
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| DatabaseError::resource_limit("righe MySQL non rappresentabili"))?;
        reservation.commit(rows, actual_bytes, components)?;
        self.park(fill.deferred)?;
        // Il cursore avanza solo su un batch che sta per essere consegnato:
        // un batch fallito non ha righe pubblicate da contare.
        self.read_diagnostics.publish_batch(rows)?;
        Ok(Some(batch))
    }

    fn fail(&mut self, error: DatabaseError) {
        self.cancellation.cancel();
        // Nessuna riga sopravvive a uno stream fallito: la quota del
        // carry-over torna al budget subito, non al drop dello stream.
        self.pending = None;
        self.state = MysqlStreamState::Failed(error);
    }
}

fn bounded_buffer_capacity(
    row_limit: usize,
    byte_limit: u64,
    column_count: usize,
) -> Result<usize> {
    if column_count == 0 {
        return Ok(row_limit.min(1_024));
    }
    let columns = u64::try_from(column_count)
        .map_err(|_| DatabaseError::resource_limit("numero colonne MySQL non rappresentabile"))?;
    let minimum_row_bytes = CONSERVATIVE_CELL_BYTES
        .checked_mul(columns)
        .ok_or_else(|| DatabaseError::resource_limit("stima riga MySQL in overflow"))?;
    let rows_by_bytes = byte_limit / minimum_row_bytes;
    if rows_by_bytes == 0 {
        return Err(DatabaseError::resource_limit(
            "budget memoria insufficiente per una riga MySQL",
        ));
    }
    let rows_by_bytes = usize::try_from(rows_by_bytes).unwrap_or(usize::MAX);
    Ok(row_limit.min(rows_by_bytes).min(1_024))
}

fn conservative_row_bytes(row: &Row, column_count: usize) -> Result<u64> {
    let mut total = 0_u64;
    for index in 0..column_count {
        let value = row.as_ref(index).ok_or_else(|| {
            read_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                "riga MySQL con meno colonne del piano",
            )
        })?;
        let payload_bytes = match value {
            Value::Bytes(bytes) => u64::try_from(bytes.len())
                .map_err(|_| DatabaseError::resource_limit("payload MySQL non rappresentabile"))?
                .checked_mul(2)
                .ok_or_else(|| DatabaseError::resource_limit("stima payload MySQL in overflow"))?,
            Value::Time(..) => 64,
            Value::Date(..)
            | Value::Int(_)
            | Value::UInt(_)
            | Value::Float(_)
            | Value::Double(_) => 32,
            Value::NULL => 0,
        };
        total = total
            .checked_add(CONSERVATIVE_CELL_BYTES)
            .and_then(|value| value.checked_add(payload_bytes))
            .ok_or_else(|| DatabaseError::resource_limit("stima riga MySQL in overflow"))?;
    }
    Ok(total)
}

impl Drop for MysqlBatchStream {
    fn drop(&mut self) {
        self.deadline_task.abort();
        if matches!(self.state, MysqlStreamState::Active) {
            self.cancellation.cancel();
        }
        let Some(mut worker_task) = self.worker_task.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            worker_task.abort();
            return;
        };
        runtime.spawn(async move {
            if tokio::time::timeout(std::time::Duration::from_secs(5), &mut worker_task)
                .await
                .is_err()
            {
                worker_task.abort();
                let _ = worker_task.await;
            }
        });
    }
}

#[derive(Debug)]
struct BatchReservation {
    rows_lease: ResourceLease,
    memory_lease: ResourceLease,
    output_lease: ResourceLease,
    geometry_lease: Option<ResourceLease>,
    row_limit: usize,
    byte_limit: u64,
    component_limit: u64,
}

impl BatchReservation {
    fn new(
        budget: &ResourceBudget,
        batch_rows: usize,
        columns: &[crate::MysqlColumnSpec],
    ) -> Result<Self> {
        let rows = budget
            .remaining(ResourceKind::Rows)
            .min(u64::try_from(batch_rows).unwrap_or(u64::MAX));
        let bytes = budget
            .remaining(ResourceKind::MemoryBytes)
            .min(budget.remaining(ResourceKind::OutputBytes));
        if rows == 0 || bytes == 0 {
            return Err(DatabaseError::resource_limit("budget MySQL read esaurito"));
        }
        let has_spatial = columns
            .iter()
            .any(|column| column.kind == MysqlColumnKind::Geometry);
        let component_limit = if has_spatial {
            budget.remaining(ResourceKind::GeometryComponents)
        } else {
            0
        };
        if has_spatial && component_limit == 0 {
            return Err(DatabaseError::resource_limit(
                "budget componenti geometriche MySQL esaurito",
            ));
        }
        Ok(Self {
            rows_lease: budget.try_lease(ResourceKind::Rows, rows)?,
            memory_lease: budget.try_lease(ResourceKind::MemoryBytes, bytes)?,
            output_lease: budget.try_lease(ResourceKind::OutputBytes, bytes)?,
            geometry_lease: has_spatial
                .then(|| budget.try_lease(ResourceKind::GeometryComponents, component_limit))
                .transpose()?,
            row_limit: usize::try_from(rows).unwrap_or(usize::MAX),
            byte_limit: bytes,
            component_limit,
        })
    }

    fn commit(self, rows: u64, bytes: u64, components: u64) -> Result<()> {
        if bytes == 0 || bytes > self.byte_limit {
            return Err(DatabaseError::resource_limit(
                "batch Arrow MySQL oltre il budget memoria/output",
            ));
        }
        self.rows_lease.commit(rows)?;
        self.memory_lease.commit(bytes)?;
        self.output_lease.commit(bytes)?;
        if components > 0 {
            self.geometry_lease
                .ok_or_else(|| DatabaseError::resource_limit("budget geometrico MySQL assente"))?
                .commit(components)?;
        }
        Ok(())
    }
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
    columns: &[crate::MysqlColumnSpec],
    error: DatabaseError,
    row: Option<usize>,
    column: Option<usize>,
) -> DatabaseError {
    let column = column
        .and_then(|index| columns.get(index))
        .map(|column| column.name.as_str());
    diagnostics.reject_conversion_defect(error, row.and_then(|row| u64::try_from(row).ok()), column)
}

fn validate_spatial_batch(
    batch: &RecordBatch,
    columns: &[crate::MysqlColumnSpec],
    component_limit: u64,
    cell_limit: u64,
    nesting_depth: u64,
    diagnostics: &ReadDiagnosticsTracker,
) -> Result<u64> {
    let mut components = 0_u64;
    for (index, column) in columns.iter().enumerate() {
        if column.kind != MysqlColumnKind::Geometry {
            continue;
        }
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                read_error(
                    ErrorCategory::Internal,
                    ErrorPhase::Read,
                    "array spatial MySQL non binario",
                )
            })?;
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let value = array.value(row);
            let length = u64::try_from(value.len())
                .map_err(|_| DatabaseError::resource_limit("WKB MySQL non rappresentabile"))?;
            if length > cell_limit {
                return Err(DatabaseError::resource_limit(
                    "WKB MySQL oltre il limite cella",
                ));
            }
            let remaining = component_limit.checked_sub(components).ok_or_else(|| {
                DatabaseError::resource_limit("componenti geometriche MySQL esaurite")
            })?;
            if remaining == 0 {
                return Err(DatabaseError::resource_limit(
                    "componenti geometriche MySQL esaurite",
                ));
            }
            let inspection =
                plenora_database_core::ewkb::inspect_ewkb_detailed(value, remaining, nesting_depth)
                    .map_err(|error| {
                        attribute_conversion_defect(
                            diagnostics,
                            columns,
                            error,
                            Some(row),
                            Some(index),
                        )
                    })?;
            if inspection.root.srid.is_some() || inspection.root.dimensions_label() != "xy" {
                return Err(attribute_conversion_defect(
                    diagnostics,
                    columns,
                    read_error(
                        ErrorCategory::DataMapping,
                        ErrorPhase::Read,
                        "ST_AsBinary MySQL ha prodotto WKB non XY o con SRID embedded",
                    ),
                    Some(row),
                    Some(index),
                ));
            }
            components = components
                .checked_add(inspection.stats.components)
                .ok_or_else(|| {
                    DatabaseError::resource_limit("componenti geometriche MySQL in overflow")
                })?;
        }
    }
    Ok(components)
}

fn validate_batch_rows(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 || batch_rows > MAX_BATCH_ROWS {
        return Err(read_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Validate,
            "batch_rows MySQL fuori intervallo 1..=65536",
        ));
    }
    Ok(())
}

fn ensure_active_read_budget(budget: &ResourceBudget) -> Result<()> {
    budget.ensure_active().map_err(|error| {
        if budget.remaining_duration().is_none() {
            timeout_error(ErrorPhase::Read, RemoteEffect::None)
        } else {
            error
        }
    })
}

/// Token figlio con la deadline del budget e il task che la fa scattare.
///
/// Il `Drop` annulla il task: nessun percorso lascia un timer vivo dopo la
/// fine dell'operazione che lo ha creato.
pub struct BudgetCancellation {
    token: CancellationToken,
    deadline_task: Option<tokio::task::JoinHandle<()>>,
}

impl BudgetCancellation {
    pub fn new(parent: &CancellationToken, budget: &ResourceBudget) -> Self {
        let token = parent.child_token_with_deadline(Some(budget.deadline()));
        let deadline_token = token.clone();
        let deadline = tokio::time::Instant::from_std(budget.deadline());
        let deadline_task = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            deadline_token.cancel_due_to_deadline();
        });
        Self {
            token,
            deadline_task: Some(deadline_task),
        }
    }

    pub const fn token(&self) -> &CancellationToken {
        &self.token
    }

    fn take_task(&mut self) -> Result<tokio::task::JoinHandle<()>> {
        self.deadline_task.take().ok_or_else(|| {
            read_error(
                ErrorCategory::Internal,
                ErrorPhase::Prepare,
                "task deadline MySQL assente",
            )
        })
    }
}

impl Drop for BudgetCancellation {
    fn drop(&mut self) {
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
    }
}

fn read_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: impl Into<String>,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::consts::ColumnType;
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::arrow::Schema;
    use plenora_database_core::resource::ResourceLimits;

    /// Stima conservativa di una riga di sole colonne intere: per ciascuna
    /// `CONSERVATIVE_CELL_BYTES` piu i 32 byte di payload numerico.
    const INTEGER_ROW_BYTES: u64 = 4 * (CONSERVATIVE_CELL_BYTES + 32);

    fn integer_column(name: &str) -> crate::MysqlColumnSpec {
        crate::MysqlColumnSpec {
            name: name.to_owned(),
            native_type: "bigint".to_owned(),
            native_declaration: "bigint".to_owned(),
            nullable: false,
            collation: None,
            kind: MysqlColumnKind::I64,
            spatial_srid: None,
        }
    }

    fn binary_column(name: &str) -> crate::MysqlColumnSpec {
        crate::MysqlColumnSpec {
            name: name.to_owned(),
            native_type: "varbinary".to_owned(),
            native_declaration: "varbinary(65535)".to_owned(),
            nullable: false,
            collation: None,
            kind: MysqlColumnKind::Binary,
            spatial_srid: None,
        }
    }

    fn wire_columns(columns: &[crate::MysqlColumnSpec]) -> Arc<[mysql_async::Column]> {
        columns
            .iter()
            .map(|column| {
                let column_type = if column.kind == MysqlColumnKind::Binary {
                    ColumnType::MYSQL_TYPE_BLOB
                } else {
                    ColumnType::MYSQL_TYPE_LONGLONG
                };
                mysql_async::Column::new(column_type).with_name(column.name.as_bytes())
            })
            .collect()
    }

    /// Righe di sole colonne intere, con l'identificatore replicato su ogni
    /// colonna: l'ordine sorgente resta leggibile in qualunque batch.
    fn integer_rows(columns: &[crate::MysqlColumnSpec], count: i64) -> Vec<Row> {
        let wire = wire_columns(columns);
        (1..=count)
            .map(|id| {
                let values = columns.iter().map(|_| Value::Int(id)).collect::<Vec<_>>();
                mysql_common::row::new_row(values, Arc::clone(&wire))
            })
            .collect()
    }

    fn test_schema(columns: &[crate::MysqlColumnSpec]) -> SchemaRef {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(crate::MysqlColumnSpec::arrow_field)
                .collect::<Vec<_>>(),
        ))
    }

    /// Costruisce lo stream sopra un worker sintetico che rispetta lo stesso
    /// protocollo a domanda del worker `MySQL`: una riga per ogni richiesta.
    fn spawn_stream(
        columns: Vec<crate::MysqlColumnSpec>,
        rows: Vec<Row>,
        budget: &ResourceBudget,
    ) -> MysqlBatchStream {
        let (sender, receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
        let (demand_sender, mut demand_receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
        let worker_task = tokio::spawn(async move {
            for row in rows {
                if demand_receiver.recv().await.is_none() {
                    return;
                }
                if sender.send(Ok(row)).await.is_err() {
                    return;
                }
            }
        });
        let schema = test_schema(&columns);
        let column_count = u64::try_from(columns.len()).expect("colonne rappresentabili");
        MysqlBatchStream {
            receiver,
            demand_sender,
            columns,
            schema,
            batch_rows: DEFAULT_BATCH_ROWS,
            budget: budget.clone(),
            cancellation: CancellationToken::new(),
            deadline_task: tokio::spawn(async {}),
            worker_task: Some(worker_task),
            _operation_lease: budget
                .try_lease(ResourceKind::ConcurrentOperations, 1)
                .expect("lease operazione"),
            _columns_lease: budget
                .try_lease(ResourceKind::Columns, column_count)
                .expect("lease colonne"),
            state: MysqlStreamState::Active,
            pending: None,
            read_diagnostics: ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default())
                .expect("tracker"),
        }
    }

    async fn collect_batches(
        stream: &mut MysqlBatchStream,
        cancellation: &CancellationToken,
    ) -> Vec<Vec<i64>> {
        let mut batches = Vec::new();
        while let Some(batch) = stream
            .next_batch(cancellation)
            .await
            .expect("batch consegnato")
        {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("prima colonna intera");
            batches.push((0..column.len()).map(|row| column.value(row)).collect());
        }
        batches
    }

    fn bounded_budget(memory_bytes: u64) -> ResourceBudget {
        ResourceBudget::new(ResourceLimits {
            memory_bytes,
            output_bytes: memory_bytes,
            cell_bytes: memory_bytes.min(65_536),
            ..ResourceLimits::default()
        })
        .expect("budget di prova")
    }

    /// Con i limiti di default e quattro colonne il batch raccoglie tutte le
    /// righe disponibili. Il residuo non viene piu confrontato con il massimo
    /// teorico `cell_bytes × colonne`, che da solo bastava a chiudere il batch
    /// dopo una riga sola.
    #[tokio::test]
    async fn default_limits_batch_many_rows_over_four_columns() {
        let columns = vec![
            integer_column("a"),
            integer_column("b"),
            integer_column("c"),
            integer_column("d"),
        ];
        let rows = integer_rows(&columns, 64);
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget default");
        let mut stream = spawn_stream(columns, rows, &budget);
        let cancellation = CancellationToken::new();
        assert_eq!(
            collect_batches(&mut stream, &cancellation).await,
            vec![(1..=64).collect::<Vec<i64>>()]
        );
    }

    /// La riga che non entra nel residuo corrente apre il batch successivo
    /// nella sua posizione sorgente, senza perdite ne duplicazioni.
    #[tokio::test]
    async fn a_row_that_does_not_fit_opens_the_next_batch() {
        let columns = vec![
            integer_column("a"),
            integer_column("b"),
            integer_column("c"),
            integer_column("d"),
        ];
        let rows = integer_rows(&columns, 270);
        // 100 000 / 384 = 260 righe intere nel primo batch; la 261esima
        // eccede il residuo ed e rinviata.
        let admitted = i64::try_from(100_000 / INTEGER_ROW_BYTES).expect("righe ammesse");
        let budget = bounded_budget(100_000);
        let mut stream = spawn_stream(columns, rows, &budget);
        let cancellation = CancellationToken::new();
        assert_eq!(
            collect_batches(&mut stream, &cancellation).await,
            vec![
                (1..=admitted).collect::<Vec<i64>>(),
                (admitted + 1..=270).collect::<Vec<i64>>(),
            ]
        );
    }

    /// Una riga sola oltre il budget del batch non ha un batch successivo dove
    /// essere rinviata: fallisce `ResourceLimit` invece di produrre un batch
    /// vuoto o un ciclo che la ripropone.
    #[tokio::test]
    async fn a_single_row_over_the_batch_budget_fails_with_resource_limit() {
        let columns = vec![
            integer_column("id"),
            binary_column("payload"),
            integer_column("a"),
            integer_column("b"),
        ];
        let wire = wire_columns(&columns);
        let row = mysql_common::row::new_row(
            vec![
                Value::Int(1),
                Value::Bytes(vec![0x41; 60_000]),
                Value::Int(1),
                Value::Int(1),
            ],
            wire,
        );
        let budget = bounded_budget(100_000);
        let mut stream = spawn_stream(columns, vec![row], &budget);
        let cancellation = CancellationToken::new();
        for _ in 0..2 {
            assert_eq!(
                stream
                    .next_batch(&cancellation)
                    .await
                    .expect_err("riga oltre il budget del batch")
                    .category,
                ErrorCategory::ResourceLimit
            );
        }
    }

    /// Il carry-over non altera ne l'ordine ne il conteggio delle righe
    /// consegnate, qualunque sia il punto in cui cade il confine del batch.
    #[tokio::test]
    async fn rows_keep_their_order_and_count_across_batch_boundaries() {
        let columns = vec![
            integer_column("a"),
            integer_column("b"),
            integer_column("c"),
            integer_column("d"),
        ];
        let rows = integer_rows(&columns, 250);
        let budget = bounded_budget(40_000);
        let mut stream = spawn_stream(columns, rows, &budget);
        let cancellation = CancellationToken::new();
        let batches = collect_batches(&mut stream, &cancellation).await;
        assert!(batches.len() > 1, "confine di batch non attraversato");
        assert!(batches.iter().all(|batch| !batch.is_empty()));
        assert_eq!(batches.concat(), (1..=250).collect::<Vec<i64>>());
    }

    /// La cancellazione con una riga trattenuta restituisce al budget memoria
    /// esattamente la quota che la riga aveva prenotato.
    #[tokio::test]
    async fn cancellation_with_a_pending_row_returns_its_memory_lease() {
        let columns = vec![
            integer_column("a"),
            integer_column("b"),
            integer_column("c"),
            integer_column("d"),
        ];
        let rows = integer_rows(&columns, 270);
        let budget = bounded_budget(100_000);
        let mut stream = spawn_stream(columns, rows, &budget);
        let cancellation = CancellationToken::new();
        let first = stream
            .next_batch(&cancellation)
            .await
            .expect("primo batch")
            .expect("batch non vuoto");
        assert_eq!(
            u64::try_from(first.num_rows()).expect("righe rappresentabili"),
            100_000 / INTEGER_ROW_BYTES
        );
        let parked = budget.remaining(ResourceKind::MemoryBytes);
        cancellation.cancel();
        assert_eq!(
            stream
                .next_batch(&cancellation)
                .await
                .expect_err("cancellazione con riga trattenuta")
                .category,
            ErrorCategory::Cancelled
        );
        assert_eq!(
            budget.remaining(ResourceKind::MemoryBytes),
            parked + INTEGER_ROW_BYTES
        );
    }

    #[test]
    fn invalid_batch_size_is_rejected_before_io() {
        assert_eq!(
            validate_batch_rows(0).expect_err("zero batch").category,
            ErrorCategory::InvalidPlan
        );
        assert_eq!(
            validate_batch_rows(MAX_BATCH_ROWS + 1)
                .expect_err("oversized batch")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    #[test]
    fn reservation_fails_before_allocation_when_rows_are_exhausted() {
        let budget = ResourceBudget::new(ResourceLimits {
            rows: 1,
            ..ResourceLimits::default()
        })
        .expect("budget");
        let consumed = budget.try_lease(ResourceKind::Rows, 1).expect("row lease");
        consumed.commit(1).expect("commit row");
        assert_eq!(
            BatchReservation::new(&budget, 1, &[])
                .expect_err("exhausted")
                .category,
            ErrorCategory::ResourceLimit
        );
    }

    #[test]
    fn terminal_stream_error_is_sticky_instead_of_becoming_eof() {
        let error = read_error(
            ErrorCategory::Protocol,
            ErrorPhase::Read,
            "errore terminale",
        );
        let state = MysqlStreamState::Failed(error.clone());
        for _ in 0..2 {
            assert_eq!(
                state
                    .terminal_result()
                    .expect("stato terminale")
                    .expect_err("errore sticky"),
                error
            );
        }
    }

    #[test]
    fn builder_capacity_is_bounded_by_the_byte_budget_before_allocation() {
        assert_eq!(
            bounded_buffer_capacity(8_192, 128, 1).expect("due righe conservative"),
            2
        );
        assert_eq!(
            bounded_buffer_capacity(1, 63, 1)
                .expect_err("budget inferiore a una riga")
                .category,
            ErrorCategory::ResourceLimit
        );
    }

    fn spatial_column(name: &str) -> crate::MysqlColumnSpec {
        crate::MysqlColumnSpec {
            name: name.to_owned(),
            native_type: "geometry".to_owned(),
            native_declaration: "geometry".to_owned(),
            nullable: true,
            collation: None,
            kind: MysqlColumnKind::Geometry,
            spatial_srid: None,
        }
    }

    /// Un difetto di conversione osservato mentre il batch è in costruzione
    /// pubblica l'indice sorgente assoluto e la colonna del piano.
    #[test]
    fn a_read_conversion_defect_publishes_the_absolute_source_index() {
        let mut tracker =
            ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default()).expect("tracker");
        tracker
            .publish_batch(8_192)
            .expect("primo batch pubblicato");
        let columns = [spatial_column("shape"), spatial_column("footprint")];

        let error = attribute_conversion_defect(
            &tracker,
            &columns,
            read_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                "valore MySQL non rappresentabile",
            ),
            Some(17),
            Some(1),
        );
        assert_eq!(error.phase, ErrorPhase::Read);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        let report = error.row_diagnostics().expect("diagnostica MySQL");
        report.validate().expect("documento valido");
        assert_eq!(
            serde_json::to_value(report).expect("documento serializzabile"),
            serde_json::json!({
                "contract": "plenora-row-diagnostics-v1",
                "scope": "read",
                "index_basis": "source_row_zero_based",
                "completeness": "partial",
                "knowledge_limits": [
                    "read.batches_already_published",
                    "read.scan_stopped_at_first_defect"
                ],
                "observed_total": 1,
                "counts": {"conversion.value_not_representable": 1},
                "examples_limit": 10,
                "examples_truncated": false,
                "examples": [{
                    "source_index": 8_209,
                    "cause": "conversion.value_not_representable",
                    "column": "footprint"
                }]
            })
        );
    }

    /// Una colonna fuori dal piano non produce un nome inventato e un errore
    /// che non è un difetto di conversione non riceve una riga sorgente.
    #[test]
    fn unattributable_read_failures_never_invent_provenance() {
        let tracker =
            ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default()).expect("tracker");
        let columns = [spatial_column("shape")];

        let error = attribute_conversion_defect(
            &tracker,
            &columns,
            read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
            Some(3),
            Some(9),
        );
        let report = error.row_diagnostics().expect("diagnostica MySQL");
        assert_eq!(report.examples[0].source_index, 3);
        assert!(report.examples[0].column.is_none());

        let unknown_row = attribute_conversion_defect(
            &tracker,
            &columns,
            read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
            None,
            Some(0),
        );
        let report = unknown_row.row_diagnostics().expect("diagnostica MySQL");
        assert_eq!(
            report.completeness,
            plenora_database_core::row_diagnostics::Completeness::Unknown,
        );
        assert!(report.examples.is_empty(), "nessun indice inventato");

        let budget = attribute_conversion_defect(
            &tracker,
            &columns,
            DatabaseError::resource_limit("budget MySQL esaurito"),
            Some(3),
            Some(0),
        );
        assert_eq!(budget.category, ErrorCategory::ResourceLimit);
        assert!(budget.row_diagnostics().is_none());
    }

    #[tokio::test]
    async fn expired_resource_deadline_maps_to_timeout() {
        let budget = ResourceBudget::new(ResourceLimits {
            duration_ms: 1,
            ..ResourceLimits::default()
        })
        .expect("budget breve");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert_eq!(
            ensure_active_read_budget(&budget)
                .expect_err("deadline scaduta")
                .category,
            ErrorCategory::Timeout
        );
    }
}
