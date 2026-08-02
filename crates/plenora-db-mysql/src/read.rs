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
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
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
    }))
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

    fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
        Box::pin(async move { self.next_batch_inner().await })
    }

    fn next_batch_with_cancellation<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
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

    async fn next_active_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cancellation.is_cancelled() {
            return Err(interruption_error(
                &self.cancellation,
                ErrorPhase::Read,
                RemoteEffect::None,
            ));
        }
        ensure_active_read_budget(&self.budget)?;
        let reservation = BatchReservation::new(&self.budget, self.batch_rows, &self.columns)?;
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
        let mut row_count = 0_usize;
        let mut estimated_bytes = 0_u64;
        let maximum_valid_row_bytes =
            maximum_valid_row_bytes(self.columns.len(), self.budget.limits().cell_bytes)?;
        while row_count < reservation.row_limit {
            let residual = reservation.byte_limit.saturating_sub(estimated_bytes);
            if row_count > 0 && residual < maximum_valid_row_bytes {
                break;
            }
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
                Some(Ok(row)) => {
                    let row_bytes = conservative_row_bytes(&row, self.columns.len())?;
                    let next_estimate =
                        estimated_bytes.checked_add(row_bytes).ok_or_else(|| {
                            DatabaseError::resource_limit("stima batch MySQL in overflow")
                        })?;
                    if next_estimate > reservation.byte_limit {
                        if row_count == 0 {
                            return Err(DatabaseError::resource_limit(
                                "riga MySQL oltre il budget memoria del batch",
                            ));
                        }
                        return Err(DatabaseError::resource_limit(
                            "riga MySQL cresciuta oltre il residuo memoria del batch",
                        ));
                    }
                    for (index, buffer) in buffers.iter_mut().enumerate() {
                        buffer.append(&row, index, self.budget.limits().cell_bytes)?;
                    }
                    row_count = row_count.saturating_add(1);
                    estimated_bytes = next_estimate;
                }
                Some(Err(error)) => {
                    return Err(error);
                }
                None => {
                    break;
                }
            }
        }
        if row_count == 0 {
            return Ok(None);
        }
        let arrays = buffers
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
        )?;
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| DatabaseError::resource_limit("righe MySQL non rappresentabili"))?;
        reservation.commit(rows, actual_bytes, components)?;
        Ok(Some(batch))
    }

    fn fail(&mut self, error: DatabaseError) {
        self.cancellation.cancel();
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

fn maximum_valid_row_bytes(column_count: usize, cell_limit: u64) -> Result<u64> {
    let columns = u64::try_from(column_count)
        .map_err(|_| DatabaseError::resource_limit("numero colonne MySQL non rappresentabile"))?;
    let bytes_value = cell_limit
        .checked_mul(2)
        .and_then(|value| value.checked_add(CONSERVATIVE_CELL_BYTES))
        .ok_or_else(|| DatabaseError::resource_limit("bound riga MySQL in overflow"))?;
    let maximum_cell = bytes_value.max(128);
    maximum_cell
        .checked_mul(columns)
        .ok_or_else(|| DatabaseError::resource_limit("bound riga MySQL in overflow"))
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

fn validate_spatial_batch(
    batch: &RecordBatch,
    columns: &[crate::MysqlColumnSpec],
    component_limit: u64,
    cell_limit: u64,
    nesting_depth: u64,
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
            let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(
                value,
                remaining,
                nesting_depth,
            )?;
            if inspection.root.srid.is_some() || inspection.root.dimensions_label() != "xy" {
                return Err(read_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    "ST_AsBinary MySQL ha prodotto WKB non XY o con SRID embedded",
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

struct BudgetCancellation {
    token: CancellationToken,
    deadline_task: Option<tokio::task::JoinHandle<()>>,
}

impl BudgetCancellation {
    fn new(parent: &CancellationToken, budget: &ResourceBudget) -> Self {
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

    const fn token(&self) -> &CancellationToken {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::resource::ResourceLimits;

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

    #[test]
    fn valid_row_bound_reserves_cell_payload_and_buffer_growth() {
        assert_eq!(maximum_valid_row_bytes(2, 4_096).expect("bound"), 16_512);
        assert!(maximum_valid_row_bytes(usize::MAX, u64::MAX).is_err());
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
