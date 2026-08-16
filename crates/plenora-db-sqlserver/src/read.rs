use crate::arrow::SqlServerColumnBuffer;
use crate::catalog::describe_object;
use crate::parameter::bind_parameters;
use crate::query::{describe_query, render_query, validate_query_sources, validate_spatial_inputs};
use crate::types::{spatial_dimensions_from_profile, SqlServerReadPlan};
use crate::SqlServerPool;
use plenora_database_core::arrow::array::{Array, BinaryArray};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::ewkb::inspect_ewkb;
use plenora_database_core::plan::{ObjectRef, ReadOperation};
use plenora_database_core::provider::{BatchStream, ParameterBag, ProviderFuture};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, ReadDiagnosticsTracker,
    RemoteEffect, Result, RetryDisposition,
};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, Renderer};
use std::sync::Arc;
use tiberius::{Query, Row};
use tokio::sync::mpsc;

const ROW_CHANNEL_CAPACITY: usize = 1;
pub const MAX_CONFIGURED_BATCH_ROWS: usize = 65_536;

/// Apre uno stream Arrow bounded su una tabella o vista SQL Server.
///
/// Il piano viene compilato dal catalogo, i metadati spatial vengono
/// preflightati e il token strutturale viene ricontrollato prima di avviare la
/// query. Una cancellazione o un drop anticipato mette la connessione TDS in
/// quarantena.
///
/// # Errors
///
/// Fallisce chiuso per schema instabile, tipo non supportato, SRID o profili
/// dimensionali misti, `FullGlobe`, budget insufficiente o errore TDS.
#[allow(clippy::significant_drop_tightening)]
pub async fn read_object(
    pool: &Arc<SqlServerPool>,
    schema: &str,
    object: &str,
    batch_rows: usize,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: object.to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        filter: None,
    };
    read_operation(
        pool,
        &operation,
        &ParameterBag::default(),
        batch_rows,
        budget,
        cancellation,
    )
    .await
}

#[allow(clippy::significant_drop_tightening)]
// Il lease `pooled` viene trasferito al worker TDS e deve vivere fino al drain.
pub async fn read_operation(
    pool: &Arc<SqlServerPool>,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    batch_rows: usize,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    validate_batch_rows(batch_rows)?;
    budget.ensure_active()?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget);
    let internal = budget_cancellation.token().clone();
    let mut pooled = pool.checkout(&internal).await?;
    let schema = operation.source.schema.as_deref().unwrap_or("dbo");
    let description = describe_object(
        pooled.session_mut()?,
        schema,
        &operation.source.object,
        &internal,
    )
    .await?;
    let mut plan = SqlServerReadPlan::compile_operation(&description, operation)?;
    let column_count = u64::try_from(description.columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    preflight_spatial(
        pooled.session_mut()?,
        schema,
        &operation.source.object,
        &mut plan,
        &internal,
    )
    .await?;
    let confirmation = describe_object(
        pooled.session_mut()?,
        schema,
        &operation.source.object,
        &internal,
    )
    .await?;
    if description.token != confirmation.token {
        pooled.quarantine();
        return Err(read_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            "schema SQL Server cambiato durante la preparazione",
        ));
    }

    start_plan_stream(
        pooled,
        plan,
        parameters,
        batch_rows,
        budget,
        internal,
        &mut budget_cancellation,
        operation_lease,
        columns_lease,
    )
}

/// Esegue una `QueryOperation` T-SQL completa usando la descrizione output
/// autoritativa del server e il controllo successivo del token COLMETADATA.
#[allow(clippy::significant_drop_tightening)]
pub async fn read_query_operation(
    pool: &Arc<SqlServerPool>,
    database: &str,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    batch_rows: usize,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    validate_batch_rows(batch_rows)?;
    let rendered = render_query(operation, parameters, budget)?;
    validate_query_sources(operation, database)?;
    budget.ensure_active()?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget);
    let internal = budget_cancellation.token().clone();
    let mut pooled = pool.checkout(&internal).await?;
    let spatial_validation = validate_spatial_inputs(
        pooled.session_mut()?,
        operation,
        parameters,
        budget,
        &internal,
    )
    .await?;
    let mut plan = describe_query(pooled.session_mut()?, rendered, parameters, &internal).await?;
    for output in spatial_validation.outputs {
        plan.apply_query_spatial_contract(
            output.projection_index,
            output.semantics,
            output.srid,
            output.dimensions,
        )?;
    }
    for expected in spatial_validation.source_tokens {
        let schema = expected.object.schema.as_deref().unwrap_or("dbo");
        let confirmation = describe_object(
            pooled.session_mut()?,
            schema,
            &expected.object.object,
            &internal,
        )
        .await?;
        if confirmation.token != expected.token {
            return Err(read_error(
                ErrorCategory::Schema,
                ErrorPhase::Prepare,
                "schema SQL Server cambiato durante la preparazione spatial",
            ));
        }
    }
    let column_count = u64::try_from(plan.columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    start_plan_stream(
        pooled,
        plan,
        parameters,
        batch_rows,
        budget,
        internal,
        &mut budget_cancellation,
        operation_lease,
        columns_lease,
    )
}

#[allow(clippy::too_many_arguments)]
fn start_plan_stream(
    mut pooled: crate::PooledSqlServerSession,
    plan: SqlServerReadPlan,
    parameters: &ParameterBag,
    batch_rows: usize,
    budget: &ResourceBudget,
    internal: CancellationToken,
    budget_cancellation: &mut BudgetCancellation,
    operation_lease: ResourceLease,
    columns_lease: ResourceLease,
) -> Result<Box<dyn BatchStream>> {
    let (sender, receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
    let worker_cancellation = internal.clone();
    let mut query = Query::new(plan.sql.clone());
    bind_parameters(&mut query, &plan.bind_names, parameters)?;
    let expected_columns = plan.columns.clone();
    tokio::spawn(async move {
        // Se il task viene abortito o va in unwind, Drop distrugge il client
        // invece di rimettere nel pool una sessione dal drain non dimostrato.
        pooled.disallow_reuse();
        let error_sender = sender.clone();
        let result = match pooled.session_mut() {
            Ok(session) => {
                session
                    .pump_query_rows(query, sender, &expected_columns, &worker_cancellation)
                    .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => {
                if let Err(error) = pooled.allow_reuse_after_drain() {
                    pooled.quarantine();
                    let _ = error_sender.send(Err(error)).await;
                }
            }
            Err(error) => {
                pooled.quarantine();
                let _ = error_sender.send(Err(error)).await;
            }
        }
    });
    let deadline_task = budget_cancellation.take_task()?;
    Ok(Box::new(SqlServerBatchStream {
        receiver,
        columns: plan.columns,
        schema: plan.schema,
        batch_rows,
        budget: budget.clone(),
        cancellation: internal,
        deadline_task,
        _operation_lease: operation_lease,
        _columns_lease: columns_lease,
        finished: false,
        read_diagnostics: ReadDiagnosticsTracker::default(),
    }))
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
                "task deadline SQL Server assente",
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

pub struct SqlServerBatchStream {
    receiver: mpsc::Receiver<Result<Row>>,
    columns: Vec<crate::SqlServerColumnSpec>,
    schema: SchemaRef,
    batch_rows: usize,
    budget: ResourceBudget,
    cancellation: CancellationToken,
    deadline_task: tokio::task::JoinHandle<()>,
    _operation_lease: ResourceLease,
    _columns_lease: ResourceLease,
    finished: bool,
    /// Cursore delle righe gia consegnate, base dell'indice sorgente
    /// pubblicato dalla diagnostica row-scoped di lettura.
    read_diagnostics: ReadDiagnosticsTracker,
}

impl BatchStream for SqlServerBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            // Stream già terminato (drain o fallimento pregresso): ritorna
            // None senza consultare la cancellazione — cancel su stream
            // finito non è un errore, è un no-op.
            if self.finished {
                return Ok(None);
            }
            if cancellation.is_cancelled() {
                self.cancellation.cancel();
                self.finished = true;
                return Err(cancelled_read_error());
            }
            let completed = {
                let next = self.next_batch_inner();
                tokio::pin!(next);
                tokio::select! {
                    result = &mut next => Some(result),
                    _ = cancellation.cancelled() => None,
                }
            };
            if let Some(result) = completed {
                result
            } else {
                self.cancellation.cancel();
                self.finished = true;
                Err(cancelled_read_error())
            }
        })
    }
}

impl SqlServerBatchStream {
    /// Accoda una riga ai buffer del batch in costruzione.
    ///
    /// Ogni difetto di conversione osservato qui è ancora attribuibile: la
    /// riga non ha raggiunto il consumatore, quindi porta con sé l'indice
    /// sorgente assoluto e la colonna del piano.
    fn append_row(
        &self,
        row: &Row,
        buffers: &mut [SqlServerColumnBuffer],
        row_count: usize,
    ) -> Result<()> {
        if row.result_index() != 0 {
            return Err(read_error(
                ErrorCategory::Protocol,
                ErrorPhase::Read,
                "QueryOperation SQL Server ha prodotto più result set",
            ));
        }
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

    async fn next_batch_inner(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        self.budget.ensure_active()?;
        if self.cancellation.is_cancelled() {
            self.finished = true;
            return Err(cancelled_read_error());
        }
        let reservation = BatchReservation::new(&self.budget, self.batch_rows, &self.columns)?;
        let capacity = reservation.row_limit.min(1_024);
        let mut buffers = self
            .columns
            .iter()
            .map(|column| SqlServerColumnBuffer::new(column, capacity))
            .collect::<Vec<_>>();
        let mut row_count = 0_usize;
        while row_count < reservation.row_limit {
            let received = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.finished = true;
                    return Err(cancelled_read_error());
                }
                row = self.receiver.recv() => row,
            };
            match received {
                Some(Ok(row)) => {
                    if let Err(error) = self.append_row(&row, &mut buffers, row_count) {
                        self.cancellation.cancel();
                        self.finished = true;
                        return Err(error);
                    }
                    row_count = row_count.saturating_add(1);
                }
                Some(Err(error)) => {
                    self.finished = true;
                    return Err(error);
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
            .map(SqlServerColumnBuffer::finish)
            .collect::<Vec<_>>();
        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(DatabaseError::from)?;
        self.finish_batch(batch, reservation).map(Some)
    }

    /// Valida, contabilizza e consegna il batch appena costruito.
    fn finish_batch(
        &mut self,
        batch: RecordBatch,
        reservation: BatchReservation,
    ) -> Result<RecordBatch> {
        match self.validated_batch(&batch, reservation) {
            Ok(()) => Ok(batch),
            // Un batch che non arriva alla consegna chiude lo stream: senza
            // terminalizzazione una `next_batch` successiva ripartirebbe dalle
            // righe seguenti, saltando in silenzio quelle del batch fallito.
            Err(error) => Err(self.terminal(error)),
        }
    }

    /// Contabilizza il batch costruito e ne autorizza la consegna.
    fn validated_batch(
        &mut self,
        batch: &RecordBatch,
        reservation: BatchReservation,
    ) -> Result<()> {
        let actual_bytes = batch.columns().iter().try_fold(0_u64, |total, array| {
            let bytes = u64::try_from(array.get_array_memory_size()).map_err(|_| {
                DatabaseError::resource_limit("dimensione batch Arrow non rappresentabile")
            })?;
            total
                .checked_add(bytes)
                .ok_or_else(|| DatabaseError::resource_limit("dimensione batch Arrow overflow"))
        })?;
        let components = validate_spatial_batch(
            batch,
            &self.columns,
            reservation.component_limit,
            self.budget.limits().cell_bytes,
            self.budget.limits().nesting_depth,
            &self.read_diagnostics,
        )?;
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| DatabaseError::resource_limit("numero righe non rappresentabile"))?;
        reservation.commit(rows, actual_bytes, components)?;
        // Il cursore avanza solo su un batch che sta per essere consegnato: un
        // batch fallito non ha righe pubblicate da contare.
        self.read_diagnostics.publish_batch(rows)
    }

    /// Chiude lo stream su un errore terminale e restituisce l'errore.
    fn terminal(&mut self, error: DatabaseError) -> DatabaseError {
        self.cancellation.cancel();
        self.finished = true;
        error
    }
}

impl Drop for SqlServerBatchStream {
    fn drop(&mut self) {
        self.deadline_task.abort();
        if !self.finished {
            self.cancellation.cancel();
        }
    }
}

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
        columns: &[crate::SqlServerColumnSpec],
    ) -> Result<Self> {
        let rows = budget
            .remaining(ResourceKind::Rows)
            .min(u64::try_from(batch_rows).unwrap_or(u64::MAX));
        let bytes = budget
            .remaining(ResourceKind::MemoryBytes)
            .min(budget.remaining(ResourceKind::OutputBytes));
        if rows == 0 || bytes == 0 {
            return Err(DatabaseError::resource_limit(
                "budget SQL Server read esaurito",
            ));
        }
        let has_spatial = columns
            .iter()
            .any(|column| column.kind.spatial_semantics().is_some());
        let component_limit = if has_spatial {
            budget.remaining(ResourceKind::GeometryComponents)
        } else {
            0
        };
        if has_spatial && component_limit == 0 {
            return Err(DatabaseError::resource_limit(
                "budget componenti geometriche esaurito",
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
                "batch Arrow oltre il budget memoria/output",
            ));
        }
        self.rows_lease.commit(rows)?;
        self.memory_lease.commit(bytes)?;
        self.output_lease.commit(bytes)?;
        if components > 0 {
            self.geometry_lease
                .ok_or_else(|| DatabaseError::resource_limit("budget geometrico assente"))?
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
    columns: &[crate::SqlServerColumnSpec],
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
    columns: &[crate::SqlServerColumnSpec],
    component_limit: u64,
    cell_limit: u64,
    nesting_depth: u64,
    diagnostics: &ReadDiagnosticsTracker,
) -> Result<u64> {
    let mut components = 0_u64;
    for (index, column) in columns.iter().enumerate() {
        if column.kind.spatial_semantics().is_none() {
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
                    "array spatial SQL Server non binario",
                )
            })?;
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let value = array.value(row);
            let length = u64::try_from(value.len())
                .map_err(|_| DatabaseError::resource_limit("WKB non rappresentabile"))?;
            if length > cell_limit {
                return Err(DatabaseError::resource_limit(
                    "WKB SQL Server oltre il limite cella",
                ));
            }
            let remaining = component_limit.checked_sub(components).ok_or_else(|| {
                DatabaseError::resource_limit("budget componenti geometriche esaurito")
            })?;
            if remaining == 0 {
                return Err(DatabaseError::resource_limit(
                    "budget componenti geometriche esaurito",
                ));
            }
            let stats = inspect_ewkb(value, remaining, nesting_depth).map_err(|error| {
                attribute_conversion_defect(diagnostics, columns, error, Some(row), Some(index))
            })?;
            components = components
                .checked_add(stats.components)
                .ok_or_else(|| DatabaseError::resource_limit("componenti geometriche overflow"))?;
        }
    }
    Ok(components)
}

async fn preflight_spatial(
    session: &mut crate::SqlServerSession,
    schema: &str,
    object: &str,
    plan: &mut SqlServerReadPlan,
    cancellation: &CancellationToken,
) -> Result<()> {
    let renderer = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    );
    let quoted_schema = renderer.quote_identifier(&sql_server_identifier(schema)?)?;
    let quoted_object = renderer.quote_identifier(&sql_server_identifier(object)?)?;
    let spatial = plan
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.kind.spatial_semantics().is_some())
        .map(|(index, column)| (index, column.name.clone()))
        .collect::<Vec<_>>();
    for (index, name) in spatial {
        let quoted_column = renderer.quote_identifier(&sql_server_identifier(&name)?)?;
        let sql = format!(
            "SELECT COUNT_BIG(DISTINCT CASE WHEN {quoted_column} IS NULL THEN NULL ELSE \
             {quoted_column}.STSrid END), MIN(CASE WHEN {quoted_column} IS NULL THEN NULL ELSE \
             {quoted_column}.STSrid END), COALESCE(SUM(CONVERT(bigint, CASE WHEN \
             {quoted_column}.STGeometryType() = N'FullGlobe' THEN 1 ELSE 0 END)), 0), \
             COUNT_BIG(DISTINCT CASE WHEN {quoted_column} IS NULL THEN NULL ELSE \
             CONVERT(int, {quoted_column}.HasZ) * 2 + \
             CONVERT(int, {quoted_column}.HasM) END), \
             MIN(CASE WHEN {quoted_column} IS NULL THEN NULL ELSE \
             CONVERT(int, {quoted_column}.HasZ) * 2 + \
             CONVERT(int, {quoted_column}.HasM) END) \
             FROM {quoted_schema}.{quoted_object};"
        );
        let mut results = session
            .execute_query(Query::new(sql), ErrorPhase::Prepare, cancellation)
            .await?;
        if results.len() != 1 || results.first().is_none_or(|rows| rows.len() != 1) {
            return Err(read_error(
                ErrorCategory::Protocol,
                ErrorPhase::Prepare,
                "preflight spatial SQL Server con cardinalità inattesa",
            ));
        }
        let row = results
            .pop()
            .and_then(|mut rows| rows.pop())
            .ok_or_else(|| {
                read_error(
                    ErrorCategory::Protocol,
                    ErrorPhase::Prepare,
                    "preflight spatial SQL Server senza riga",
                )
            })?;
        let srid_count: i64 = required(&row, 0, "numero SRID")?;
        let srid: Option<i32> = optional(&row, 1, "SRID")?;
        let full_globe: i64 = required(&row, 2, "FullGlobe")?;
        let dimension_count: i64 = required(&row, 3, "numero profili dimensionali")?;
        let dimension_code: Option<i32> = optional(&row, 4, "profilo dimensionale")?;
        if srid_count > 1 {
            return Err(read_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                "colonna spatial SQL Server con SRID misti",
            ));
        }
        if full_globe > 0 {
            return Err(read_error(
                ErrorCategory::Unsupported,
                ErrorPhase::Prepare,
                "FullGlobe SQL Server non rappresentabile nel profilo WKB",
            ));
        }
        let dimensions = spatial_dimensions_from_profile(dimension_count, dimension_code)?;
        let srid = srid
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    read_error(
                        ErrorCategory::DataMapping,
                        ErrorPhase::Prepare,
                        "SRID SQL Server negativo",
                    )
                })
            })
            .transpose()?;
        plan.apply_spatial_contract(index, srid, dimensions)?;
    }
    Ok(())
}

fn required<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<T>
where
    T: tiberius::FromSql<'a>,
{
    optional(row, index, field)?.ok_or_else(|| {
        read_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            format!("campo preflight SQL Server obbligatorio assente: {field}"),
        )
    })
}

fn optional<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<Option<T>>
where
    T: tiberius::FromSql<'a>,
{
    row.try_get(index).map_err(|_| {
        read_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            format!("tipo preflight SQL Server incompatibile: {field}"),
        )
    })
}

fn sql_server_identifier(value: &str) -> Result<Identifier> {
    if value.chars().count() > crate::MAX_IDENTIFIER_CHARACTERS {
        return Err(read_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "identificatore oltre 128 caratteri SQL Server",
        ));
    }
    Identifier::new(value.to_owned())
}

fn validate_batch_rows(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 || batch_rows > MAX_CONFIGURED_BATCH_ROWS {
        return Err(read_error(
            ErrorCategory::InvalidConfiguration,
            ErrorPhase::Prepare,
            "batch_rows SQL Server deve essere compreso tra 1 e 65536",
        ));
    }
    Ok(())
}

fn cancelled_read_error() -> DatabaseError {
    read_error(
        ErrorCategory::Cancelled,
        ErrorPhase::Read,
        "lettura SQL Server cancellata; connessione quarantinata",
    )
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
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::geometry::Dimensions;
    use plenora_database_core::ResourceLimits;

    #[test]
    fn batch_row_configuration_is_bounded() {
        assert!(validate_batch_rows(0).is_err());
        assert!(validate_batch_rows(1).is_ok());
        assert!(validate_batch_rows(MAX_CONFIGURED_BATCH_ROWS).is_ok());
        assert!(validate_batch_rows(MAX_CONFIGURED_BATCH_ROWS + 1).is_err());
    }

    #[test]
    fn reservations_fail_before_zero_or_missing_geometry_budget() {
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        assert!(BatchReservation::new(&budget, 0, &[]).is_err());
    }

    fn column(name: &str) -> crate::SqlServerColumnSpec {
        crate::SqlServerColumnSpec {
            name: name.to_owned(),
            native_type: "geometry".to_owned(),
            native_declaration: "geometry".to_owned(),
            nullable: true,
            collation: None,
            kind: crate::SqlServerColumnKind::Geometry,
            spatial_srid: None,
            spatial_dimensions: None,
            wire_encoding: crate::SqlServerWireEncoding::Projected,
        }
    }

    /// Un difetto di conversione osservato prima della consegna del batch
    /// pubblica l'indice sorgente assoluto del result set.
    #[test]
    fn a_read_conversion_defect_publishes_the_absolute_source_index() {
        let mut tracker = ReadDiagnosticsTracker::default();
        tracker.publish_batch(1_024).expect("batch pubblicato");
        let columns = [column("parcel_id"), column("shape")];

        let error = attribute_conversion_defect(
            &tracker,
            &columns,
            read_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                "valore SQL Server non rappresentabile",
            ),
            Some(7),
            Some(1),
        );
        assert_eq!(error.phase, ErrorPhase::Read);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        let report = error.row_diagnostics().expect("diagnostica SQL Server");
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
                    "source_index": 1_031,
                    "cause": "conversion.value_not_representable",
                    "column": "shape"
                }]
            })
        );
    }

    /// Provenienza e completezza non vengono inventate: né su una riga
    /// sconosciuta né su un errore che non è un difetto di conversione.
    #[test]
    fn unattributable_read_failures_never_invent_provenance() {
        let tracker = ReadDiagnosticsTracker::default();
        let columns = [column("shape")];

        let unknown_row = attribute_conversion_defect(
            &tracker,
            &columns,
            read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
            None,
            Some(0),
        );
        let report = unknown_row
            .row_diagnostics()
            .expect("diagnostica SQL Server");
        report.validate().expect("documento valido");
        assert_eq!(
            report.completeness,
            plenora_database_core::row_diagnostics::Completeness::Unknown
        );
        assert!(report.examples.is_empty());

        let missing_column = attribute_conversion_defect(
            &tracker,
            &columns,
            read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
            Some(4),
            Some(9),
        );
        let report = missing_column
            .row_diagnostics()
            .expect("diagnostica SQL Server");
        assert_eq!(report.examples[0].source_index, 4);
        assert!(report.examples[0].column.is_none());

        let protocol = attribute_conversion_defect(
            &tracker,
            &columns,
            read_error(
                ErrorCategory::Protocol,
                ErrorPhase::Read,
                "result set inatteso",
            ),
            Some(4),
            Some(0),
        );
        assert_eq!(protocol.category, ErrorCategory::Protocol);
        assert!(protocol.row_diagnostics().is_none());
    }

    /// Un batch che non supera la validazione spaziale chiude lo stream.
    ///
    /// Senza terminalizzazione una `next_batch` successiva ripartirebbe dalle
    /// righe seguenti, saltando in silenzio quelle del batch fallito: è
    /// esattamente il drop silenzioso che il contratto vieta.
    #[tokio::test]
    async fn a_failed_spatial_batch_terminalizes_the_stream() {
        use plenora_database_core::arrow::array::Int64Array;
        use plenora_database_core::arrow::{DataType, Field, Schema};

        // La colonna è dichiarata spatial nel piano ma l'array non è Binary:
        // `validate_spatial_batch` fallisce sul downcast, senza bisogno di
        // righe TDS reali.
        let columns = vec![column("shape")];
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "shape",
            DataType::Int64,
            true,
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let (sender, receiver) = mpsc::channel(1);
        let cancellation = CancellationToken::new();
        let reservation = BatchReservation::new(&budget, 8, &columns).expect("reservation");
        let mut stream = SqlServerBatchStream {
            receiver,
            columns,
            schema: Arc::clone(&schema),
            batch_rows: 8,
            budget: budget.clone(),
            cancellation: cancellation.clone(),
            deadline_task: tokio::spawn(async {}),
            _operation_lease: budget
                .try_lease(ResourceKind::ConcurrentOperations, 1)
                .expect("operation lease"),
            _columns_lease: budget.try_lease(ResourceKind::Columns, 1).expect("columns"),
            finished: false,
            read_diagnostics: ReadDiagnosticsTracker::default(),
        };

        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
            .expect("batch non spaziale");
        let error = stream
            .finish_batch(batch, reservation)
            .expect_err("il batch non supera la validazione spaziale");
        assert_eq!(error.category, ErrorCategory::Internal);

        assert!(stream.finished, "lo stream deve restare chiuso");
        assert!(
            cancellation.is_cancelled(),
            "il batch fallito deve cancellare lo stream"
        );
        assert_eq!(
            stream.read_diagnostics.published_rows(),
            0,
            "un batch mai consegnato non ha righe pubblicate"
        );

        // Una riga resta in coda: se lo stream ripartisse, la consumerebbe
        // saltando il batch fallito.
        drop(sender);
        assert!(
            stream
                .next_batch(&cancellation)
                .await
                .expect("stream chiuso")
                .is_none(),
            "una next_batch successiva non deve riprendere dopo un batch fallito"
        );
    }

    #[test]
    fn spatial_dimension_profile_is_exact_and_fail_closed() {
        assert_eq!(
            spatial_dimensions_from_profile(0, None).expect("empty"),
            Dimensions::Unknown
        );
        assert_eq!(
            spatial_dimensions_from_profile(1, Some(0)).expect("xy"),
            Dimensions::Xy
        );
        assert_eq!(
            spatial_dimensions_from_profile(1, Some(1)).expect("xym"),
            Dimensions::Xym
        );
        assert_eq!(
            spatial_dimensions_from_profile(1, Some(2)).expect("xyz"),
            Dimensions::Xyz
        );
        assert_eq!(
            spatial_dimensions_from_profile(1, Some(3)).expect("xyzm"),
            Dimensions::Xyzm
        );
        assert_eq!(
            spatial_dimensions_from_profile(2, Some(0))
                .expect_err("mixed profiles")
                .category,
            ErrorCategory::DataMapping
        );
        for incoherent in [
            spatial_dimensions_from_profile(-1, None),
            spatial_dimensions_from_profile(0, Some(0)),
            spatial_dimensions_from_profile(1, None),
            spatial_dimensions_from_profile(1, Some(4)),
        ] {
            assert_eq!(
                incoherent.expect_err("incoherent profile").category,
                ErrorCategory::Protocol
            );
        }
    }
}
