use crate::arrow::SqlServerColumnBuffer;
use crate::catalog::describe_object;
use crate::parameter::bind_parameters;
use crate::query::{describe_query, render_query, validate_query_sources};
use crate::types::SqlServerReadPlan;
use crate::SqlServerPool;
use plenora_database_core::arrow::array::{Array, BinaryArray};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::ewkb::inspect_ewkb;
use plenora_database_core::plan::{ObjectRef, ReadOperation};
use plenora_database_core::provider::{BatchStream, ParameterBag, ProviderFuture};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
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
/// Fallisce chiuso per schema instabile, tipo non supportato, SRID misti,
/// geometrie Z/M/FullGlobe nel profilo iniziale, budget insufficiente o errore
/// TDS.
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
    let rendered = render_query(operation)?;
    validate_query_sources(operation, database)?;
    budget.ensure_active()?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut budget_cancellation = BudgetCancellation::new(cancellation, budget);
    let internal = budget_cancellation.token().clone();
    let mut pooled = pool.checkout(&internal).await?;
    let plan = describe_query(pooled.session_mut()?, rendered, parameters, &internal).await?;
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
}

impl BatchStream for SqlServerBatchStream {
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
                    if row.result_index() != 0 {
                        self.cancellation.cancel();
                        self.finished = true;
                        return Err(read_error(
                            ErrorCategory::Protocol,
                            ErrorPhase::Read,
                            "QueryOperation SQL Server ha prodotto piÃ¹ result set",
                        ));
                    }
                    for (index, buffer) in buffers.iter_mut().enumerate() {
                        if let Err(error) =
                            buffer.append(&row, index, self.budget.limits().cell_bytes)
                        {
                            self.cancellation.cancel();
                            self.finished = true;
                            return Err(error);
                        }
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
        let actual_bytes = batch.columns().iter().try_fold(0_u64, |total, array| {
            let bytes = u64::try_from(array.get_array_memory_size()).map_err(|_| {
                DatabaseError::resource_limit("dimensione batch Arrow non rappresentabile")
            })?;
            total
                .checked_add(bytes)
                .ok_or_else(|| DatabaseError::resource_limit("dimensione batch Arrow overflow"))
        })?;
        let components = validate_spatial_batch(
            &batch,
            &self.columns,
            reservation.component_limit,
            self.budget.limits().cell_bytes,
            self.budget.limits().nesting_depth,
        )?;
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| DatabaseError::resource_limit("numero righe non rappresentabile"))?;
        if let Err(error) = reservation.commit(rows, actual_bytes, components) {
            self.cancellation.cancel();
            self.finished = true;
            return Err(error);
        }
        Ok(Some(batch))
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

fn validate_spatial_batch(
    batch: &RecordBatch,
    columns: &[crate::SqlServerColumnSpec],
    component_limit: u64,
    cell_limit: u64,
    nesting_depth: u64,
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
            let stats = inspect_ewkb(value, remaining, nesting_depth)?;
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
    let quoted_schema = renderer.quote_identifier(&sql_server_identifier(schema)?);
    let quoted_object = renderer.quote_identifier(&sql_server_identifier(object)?);
    let spatial = plan
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.kind.spatial_semantics().is_some())
        .map(|(index, column)| (index, column.name.clone()))
        .collect::<Vec<_>>();
    for (index, name) in spatial {
        let quoted_column = renderer.quote_identifier(&sql_server_identifier(&name)?);
        let sql = format!(
            "SELECT COUNT_BIG(DISTINCT CASE WHEN {quoted_column} IS NULL THEN NULL ELSE \
             {quoted_column}.STSrid END), MIN(CASE WHEN {quoted_column} IS NULL THEN NULL ELSE \
             {quoted_column}.STSrid END), COALESCE(SUM(CONVERT(bigint, CASE WHEN \
             {quoted_column}.STGeometryType() = N'FullGlobe' THEN 1 ELSE 0 END)), 0), \
             COALESCE(MAX(CONVERT(tinyint, {quoted_column}.HasZ)), 0), \
             COALESCE(MAX(CONVERT(tinyint, {quoted_column}.HasM)), 0) \
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
        // COALESCE con il literal intero promuove il risultato MAX(tinyint)
        // a `int` secondo la precedence T-SQL.
        let has_z: i32 = required(&row, 3, "HasZ")?;
        let has_m: i32 = required(&row, 4, "HasM")?;
        if srid_count > 1 {
            return Err(read_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                "colonna spatial SQL Server con SRID misti",
            ));
        }
        if full_globe > 0 || has_z > 0 || has_m > 0 {
            return Err(read_error(
                ErrorCategory::Unsupported,
                ErrorPhase::Prepare,
                "profilo spatial SQL Server iniziale limitato a geometrie XY non FullGlobe",
            ));
        }
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
        plan.apply_spatial_srid(index, srid)?;
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
