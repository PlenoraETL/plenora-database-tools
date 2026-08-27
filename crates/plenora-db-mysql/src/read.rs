use crate::error::{interruption_error, timeout_error};
use crate::{bind_parameters, MysqlColumnBuffer, MysqlColumnKind, MysqlPool, MysqlReadPlan};
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
    ReadDiagnosticsTracker, RemoteEffect, Result,
};
use plenora_database_engine::{inspect_spatial_arrays, DeadlineGuard, ReadBatchReservation};
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
    read_operation_with_profile(
        pool,
        operation,
        parameters,
        batch_rows,
        &crate::profile::MYSQL_PROFILE,
        budget,
        cancellation,
    )
    .await
}

/// La lettura, con il profilo che decide catalogo e parte spatial.
#[allow(clippy::redundant_pub_crate, clippy::too_many_arguments)]
pub(crate) async fn read_operation_with_profile(
    pool: &Arc<MysqlPool>,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    batch_rows: usize,
    profile: &'static dyn crate::profile::ProductProfile,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    let product = profile.product();
    validate_batch_rows(batch_rows)?;
    ensure_active_read_budget(budget, profile)?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget)?;
    let internal = budget_cancellation.token().clone();
    let mut session = pool.checkout(&internal).await?;
    let schema = operation.source.schema.as_deref().ok_or_else(|| {
        read_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            format!("schema {product} obbligatorio per la lettura"),
        )
    })?;
    let description = crate::catalog::describe_object_with_profile(
        &mut session,
        schema,
        &operation.source.object,
        profile,
        &internal,
    )
    .await?;
    let plan = MysqlReadPlan::compile_with_profile(&description, operation, profile)?;
    let column_count = u64::try_from(plan.columns.len()).map_err(|_| {
        DatabaseError::resource_limit(format!("numero colonne {product} non rappresentabile"))
    })?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    let parameters = bind_parameters(&plan.bind_names, parameters)?;
    let confirmation = crate::catalog::describe_object_with_profile(
        &mut session,
        schema,
        &operation.source.object,
        profile,
        &internal,
    )
    .await?;
    if description.token != confirmation.token {
        return Err(read_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            format!("schema {product} cambiato durante la preparazione"),
        ));
    }

    let sql = plan.sql.clone();
    start_row_stream(
        session,
        sql,
        parameters,
        plan,
        batch_rows,
        profile,
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
    query_operation_with_profile(
        pool,
        database,
        operation,
        parameters,
        batch_rows,
        &crate::profile::MYSQL_PROFILE,
        budget,
        cancellation,
    )
    .await
}

/// La query, con il profilo che interpreta i metadati del prepare.
#[allow(clippy::redundant_pub_crate, clippy::too_many_arguments)]
pub(crate) async fn query_operation_with_profile(
    pool: &Arc<MysqlPool>,
    database: &str,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    batch_rows: usize,
    profile: &'static dyn crate::profile::ProductProfile,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    let product = profile.product();
    validate_batch_rows(batch_rows)?;
    if cancellation.is_cancelled() {
        return Err(interruption_error(
            profile,
            cancellation,
            ErrorPhase::Prepare,
            RemoteEffect::None,
        ));
    }
    let plan_shape = crate::query::render_query_plan(operation, database, profile)?;
    let rendered = &plan_shape.rendered;
    let bind_names = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    if bind_names.len() > crate::MAX_BIND_PARAMETERS {
        return Err(DatabaseError::resource_limit(format!(
            "query {product} oltre il limite di placeholder del prepared statement"
        )));
    }
    let bound = bind_parameters(&bind_names, parameters)?;
    ensure_active_read_budget(budget, profile)?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget)?;
    let internal = budget_cancellation.token().clone();
    let mut session = pool.checkout(&internal).await?;
    let statement = session.prepare_statement(&rendered.sql, &internal).await?;
    if usize::from(statement.num_params()) != bind_names.len() {
        return Err(read_error(
            ErrorCategory::Protocol,
            ErrorPhase::Prepare,
            format!("placeholder {product} diversi dai bind renderizzati"),
        ));
    }
    let metadata = statement.columns();
    let columns = crate::query::query_result_columns_with_profile(&metadata, profile)?;
    let plan = MysqlReadPlan::from_query_columns_with_geometry(
        rendered.sql.clone(),
        bind_names,
        columns,
        &plan_shape,
        profile,
    )?;
    let column_count = u64::try_from(plan.columns.len()).map_err(|_| {
        DatabaseError::resource_limit(format!("numero colonne {product} non rappresentabile"))
    })?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    start_row_stream(
        session,
        statement,
        bound,
        plan,
        batch_rows,
        profile,
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
    profile: &'static dyn crate::profile::ProductProfile,
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
    let deadline_task = budget_cancellation.take_deadline_task()?;
    Ok(Box::new(MysqlBatchStream {
        receiver,
        profile,
        demand_sender,
        columns: plan.columns,
        crs_checks: plan.crs_checks,
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
    /// Il profilo del provider che ha aperto lo stream: valida il WKB
    /// contro la proiezione che lo stesso profilo ha scelto.
    profile: &'static dyn crate::profile::ProductProfile,
    demand_sender: mpsc::Sender<()>,
    columns: Vec<crate::MysqlColumnSpec>,
    /// I CRS che il piano ha dichiarato, da confermare su ogni riga.
    ///
    /// Vuoto ovunque il catalogo l'SRID lo sappia, che e la maggior parte
    /// delle letture: il costo di questo campo, li, e un `is_empty` per riga.
    crs_checks: Vec<crate::types::MysqlCrsCheck>,
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
            // Bordo dello stream: come per i metodi del provider, cio che
            // esce porta l'attribuzione del profilo che lo ha aperto.
            let profile = self.profile;
            let outcome = self.next_batch_attributed(cancellation).await;
            crate::profile::attributed(profile, outcome)
        })
    }
}

impl MysqlBatchStream {
    async fn next_batch_attributed(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecordBatch>> {
        {
            // Stream già terminato (Drained o Failed): risultato terminale
            // ha precedenza sulla cancellazione — cancel su stream chiuso
            // non è un errore, restituisce lo stato esistente.
            if let Some(result) = self.state.terminal_result() {
                return result;
            }
            if cancellation.is_cancelled() {
                let error = interruption_error(
                    self.profile,
                    cancellation,
                    ErrorPhase::Read,
                    RemoteEffect::None,
                );
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
                let error = interruption_error(
                    self.profile,
                    cancellation,
                    ErrorPhase::Read,
                    RemoteEffect::None,
                );
                self.fail(error.clone());
                Err(error)
            })
        }
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
    /// Conferma i CRS dichiarati dal piano su **questa** riga.
    ///
    /// La dichiarazione del chiamante e un'ipotesi finche qualcuno non la
    /// verifica, e verificarla una volta sola non basterebbe: la colonna che
    /// la richiede e quella che nessuna DDL vincola, quindi due righe della
    /// stessa colonna possono portare SRID diversi. E' esattamente il caso che
    /// rende falso un CRS pubblicato, ed e per questo che il controllo sta qui
    /// — nel ciclo delle righe — e non nel prepare.
    ///
    /// Un valore `NULL` non ha un CRS da confermare e non ne smentisce
    /// nessuno: la riga passa, e la colonna resta nulla come sarebbe stata.
    ///
    /// Il messaggio porta i due SRID e la posizione della riga, e niente
    /// altro. Un SRID e un identificatore di registro, non una cella: la
    /// geometria che l'ha portato non compare, ed e la regola che vale per
    /// ogni messaggio pubblico di questo crate.
    fn confirm_declared_crs(&self, row: &Row, row_count: usize) -> Result<()> {
        for check in &self.crs_checks {
            let product = self.profile.product();
            let value = row.as_ref(check.result_index).ok_or_else(|| {
                read_error(
                    ErrorCategory::Schema,
                    ErrorPhase::Read,
                    "riga senza la colonna di controllo del CRS dichiarato",
                )
            })?;
            let observed = match *value {
                mysql_async::Value::NULL => continue,
                mysql_async::Value::UInt(srid) => u32::try_from(srid).ok(),
                mysql_async::Value::Int(srid) => u32::try_from(srid).ok(),
                _ => None,
            };
            let Some(observed) = observed else {
                return Err(read_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Read,
                    format!(
                        "SRID {product} non leggibile alla riga {row_count} della                          colonna dichiarata"
                    ),
                ));
            };
            if observed != check.expected {
                return Err(read_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Read,
                    format!(
                        "CRS dichiarato non confermato dai valori {product}: la colonna                          e dichiarata SRID {} e la riga {row_count} porta SRID {observed}",
                        check.expected
                    ),
                ));
            }
        }
        Ok(())
    }

    fn append_row(
        &self,
        row: &Row,
        buffers: &mut [MysqlColumnBuffer],
        row_count: usize,
    ) -> Result<()> {
        // Prima dell'append, non dopo: una riga che smentisce la
        // dichiarazione non deve entrare in un batch che qualcuno potrebbe
        // ricevere se l'errore arrivasse un istante piu tardi.
        self.confirm_declared_crs(row, row_count)?;
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
        reservation: &ReadBatchReservation,
        carried: Option<MeasuredRow>,
    ) -> Result<BatchFill> {
        let product = self.profile.product();
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
                        return Err(interruption_error(self.profile,
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
            let next_estimate = estimated_bytes.checked_add(measured.bytes).ok_or_else(|| {
                DatabaseError::resource_limit(format!("stima batch {product} in overflow"))
            })?;
            if next_estimate > reservation.byte_limit {
                if rows == 0 {
                    return Err(DatabaseError::resource_limit(format!(
                        "riga {product} oltre il budget memoria del batch"
                    )));
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
        let product = self.profile.product();
        if self.cancellation.is_cancelled() {
            return Err(interruption_error(
                self.profile,
                &self.cancellation,
                ErrorPhase::Read,
                RemoteEffect::None,
            ));
        }
        ensure_active_read_budget(&self.budget, self.profile)?;
        // Il carry-over restituisce la sua quota prima della riserva: la
        // riserva prenota tutto il residuo, quindi la stessa riga risulterebbe
        // altrimenti contabilizzata due volte.
        let carried = self.pending.take().map(PendingRow::release).transpose()?;
        let reservation = reserve_batch(&self.budget, self.batch_rows, &self.columns)?;
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
                DatabaseError::resource_limit(format!(
                    "dimensione batch {product} non rappresentabile"
                ))
            })?;
            total.checked_add(bytes).ok_or_else(|| {
                DatabaseError::resource_limit(format!("dimensione batch {product} in overflow"))
            })
        })?;
        let components = validate_spatial_batch(
            &batch,
            &self.columns,
            self.profile,
            reservation.component_limit,
            self.budget.limits().cell_bytes,
            self.budget.limits().nesting_depth,
            &self.read_diagnostics,
        )?;
        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            DatabaseError::resource_limit(format!("righe {product} non rappresentabili"))
        })?;
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
        .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
    let minimum_row_bytes = CONSERVATIVE_CELL_BYTES
        .checked_mul(columns)
        .ok_or_else(|| DatabaseError::resource_limit("stima riga in overflow"))?;
    let rows_by_bytes = byte_limit / minimum_row_bytes;
    if rows_by_bytes == 0 {
        return Err(DatabaseError::resource_limit(
            "budget memoria insufficiente per una riga",
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
                "riga con meno colonne del piano",
            )
        })?;
        let payload_bytes = match value {
            Value::Bytes(bytes) => u64::try_from(bytes.len())
                .map_err(|_| DatabaseError::resource_limit("payload non rappresentabile"))?
                .checked_mul(2)
                .ok_or_else(|| DatabaseError::resource_limit("stima payload in overflow"))?,
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
            .ok_or_else(|| DatabaseError::resource_limit("stima riga in overflow"))?;
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

fn reserve_batch(
    budget: &ResourceBudget,
    batch_rows: usize,
    columns: &[crate::MysqlColumnSpec],
) -> Result<ReadBatchReservation> {
    let has_spatial = columns
        .iter()
        .any(|column| column.kind == MysqlColumnKind::Geometry);
    ReadBatchReservation::acquire(budget, batch_rows, None, has_spatial)
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

#[allow(clippy::too_many_arguments)]
fn validate_spatial_batch(
    batch: &RecordBatch,
    columns: &[crate::MysqlColumnSpec],
    profile: &dyn crate::profile::ProductProfile,
    component_limit: u64,
    cell_limit: u64,
    nesting_depth: u64,
    diagnostics: &ReadDiagnosticsTracker,
) -> Result<u64> {
    let product = profile.product();
    let mut arrays = Vec::new();
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
                    format!("array spatial {product} non binario"),
                )
            })?;
        arrays.push((index, array));
    }
    inspect_spatial_arrays(
        arrays,
        component_limit,
        cell_limit,
        nesting_depth,
        |error, row, column| {
            attribute_conversion_defect(diagnostics, columns, error, Some(row), Some(column))
        },
        |inspection, row, column| {
            if profile.geometry_output_is_unexpected(
                inspection.root.srid,
                inspection.root.dimensions_label(),
            ) {
                return Err(attribute_conversion_defect(
                    diagnostics,
                    columns,
                    read_error(
                        ErrorCategory::DataMapping,
                        ErrorPhase::Read,
                        format!("ST_AsBinary {product} ha prodotto WKB non XY o con SRID embedded"),
                    ),
                    Some(row),
                    Some(column),
                ));
            }
            Ok(())
        },
    )
}

fn validate_batch_rows(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 || batch_rows > MAX_BATCH_ROWS {
        return Err(read_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Validate,
            "batch_rows fuori intervallo 1..=65536",
        ));
    }
    Ok(())
}

fn ensure_active_read_budget(
    budget: &ResourceBudget,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<()> {
    budget.ensure_active().map_err(|error| {
        if budget.remaining_duration().is_none() {
            timeout_error(profile, ErrorPhase::Read, RemoteEffect::None)
        } else {
            error
        }
    })
}

/// Token figlio con la deadline del budget e il task che la fa scattare.
///
/// Il `Drop` annulla il task: nessun percorso lascia un timer vivo dopo la
/// fine dell'operazione che lo ha creato.
pub type BudgetCancellation = DeadlineGuard;

fn read_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: impl Into<String>,
) -> DatabaseError {
    DatabaseError::new(
        category,
        phase,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;
