use crate::{describe_object, list_objects, list_schemas, probe_server, MysqlConfig, MysqlPool};
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::resource::ResourceKind;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

static WRITE_EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct CachedPool {
    secret_fingerprint: [u8; 32],
    pool: Arc<MysqlPool>,
}

pub struct MysqlProvider {
    config: MysqlConfig,
    max_connections: usize,
    cached_pool: Mutex<Option<CachedPool>>,
}

impl std::fmt::Debug for MysqlProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlProvider")
            .field("config", &self.config)
            .field("max_connections", &self.max_connections)
            .field(
                "pool_initialized",
                &lock_recover(&self.cached_pool).is_some(),
            )
            .finish()
    }
}

impl MysqlProvider {
    /// Costruisce un provider con pool lazy e configurazione validata.
    ///
    /// # Errors
    ///
    /// Fallisce se configurazione o limiti del pool non sono validi.
    pub fn new(config: MysqlConfig, max_connections: usize) -> Result<Self> {
        config.validate()?;
        if max_connections == 0 {
            return Err(provider_error(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Validate,
                "provider MySQL con pool a capacita zero",
            ));
        }
        Ok(Self {
            config,
            max_connections,
            cached_pool: Mutex::new(None),
        })
    }

    fn pool_for(&self, secret: &SecretString) -> Result<Arc<MysqlPool>> {
        let fingerprint: [u8; 32] = Sha256::digest(secret.expose().as_bytes()).into();
        let mut cached = lock_recover(&self.cached_pool);
        if let Some(candidate) = cached.as_ref() {
            if candidate.secret_fingerprint == fingerprint {
                return Ok(Arc::clone(&candidate.pool));
            }
        }
        let config = self.config.clone().with_password(secret.clone());
        let pool = Arc::new(MysqlPool::new(&config, self.max_connections)?);
        *cached = Some(CachedPool {
            secret_fingerprint: fingerprint,
            pool: Arc::clone(&pool),
        });
        drop(cached);
        Ok(pool)
    }

    fn validate_source(&self, source: &plenora_database_core::plan::ObjectRef) -> Result<()> {
        if source.layer_id.is_some() {
            return Err(unsupported("layer_id non appartiene al provider MySQL"));
        }
        if source
            .catalog
            .as_deref()
            .is_some_and(|catalog| catalog != self.config.database())
        {
            return Err(unsupported(
                "accesso cross-database MySQL non supportato dal provider",
            ));
        }
        Ok(())
    }

    async fn inspect_operation(
        &self,
        pool: &MysqlPool,
        operation: &Operation,
        cancellation: &CancellationToken,
    ) -> Result<Inspection> {
        let mut session = pool.checkout(cancellation).await?;
        let result = match operation {
            Operation::DatabaseListCatalogs => Ok(Inspection {
                operation: "database.list_catalogs".to_owned(),
                document: json!({"catalogs": [self.config.database()]}),
            }),
            Operation::DatabaseListSchemas { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schemas = list_schemas(&mut session, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_schemas".to_owned(),
                    document: json!({"schemas": schemas}),
                })
            }
            Operation::DatabaseListObjects { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schema = source
                    .as_ref()
                    .and_then(|value| value.schema.as_deref())
                    .unwrap_or_else(|| self.config.database());
                let objects = list_objects(&mut session, schema, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_objects".to_owned(),
                    document: json!({"schema": schema, "objects": objects}),
                })
            }
            Operation::DatabaseDescribeObject { source } => {
                self.validate_source(source)?;
                let schema = source
                    .schema
                    .as_deref()
                    .unwrap_or_else(|| self.config.database());
                let description =
                    describe_object(&mut session, schema, &source.object, cancellation).await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!(description),
                })
            }
            _ => Err(unsupported("operazione inspect non supportata da MySQL")),
        };
        drop(session);
        result
    }
}

impl Provider for MysqlProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Mysql
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            let mut session = pool.checkout(cancellation).await?;
            let probe = probe_server(&mut session, cancellation).await?;
            drop(session);
            Ok(ConnectionInfo {
                provider: ProviderKind::Mysql,
                server_version: probe.product_version,
                connection_identity: Some(probe.database),
            })
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            let mut session = pool.checkout(cancellation).await?;
            let probe = probe_server(&mut session, cancellation).await?;
            drop(session);
            Ok(ProviderCapabilities {
                schema_version: 1,
                provider: ProviderKind::Mysql,
                provider_version: probe.product_version,
                extension_versions: BTreeMap::new(),
                reads: ReadCapabilities {
                    streaming: true,
                    server_cursor: false,
                    pagination: false,
                    object_id_windows: false,
                    projection: true,
                    filter: true,
                    ordering: true,
                    resumable: false,
                },
                writes: WriteCapabilities {
                    create: false,
                    append: true,
                    update: false,
                    upsert: false,
                    replace: false,
                    delete_by_keys: false,
                    bulk: false,
                    array_binding: false,
                    returning: false,
                    apply_edits: false,
                    rollback_on_failure: true,
                    use_global_ids: false,
                },
                transactions: TransactionCapabilities {
                    single_transaction: true,
                    savepoints: false,
                    transactional_ddl: false,
                    staged_swap: false,
                    scope: TransactionScope::Transaction,
                },
                spatial: mysql_spatial_capabilities(),
                limits: ProviderLimits {
                    max_identifier_bytes: Some(crate::MAX_IDENTIFIER_CHARACTERS as u64),
                    max_bind_parameters: Some(crate::MAX_BIND_PARAMETERS as u64),
                    max_statement_bytes: None,
                    max_batch_rows: Some(crate::MAX_BATCH_ROWS as u64),
                    max_payload_bytes: None,
                    max_record_count: None,
                },
            })
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            self.inspect_operation(&pool, operation, cancellation).await
        })
    }

    fn read<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            self.validate_source(&operation.source)?;
            let pool = self.pool_for(secret)?;
            let mut effective = operation.clone();
            effective
                .source
                .schema
                .get_or_insert_with(|| self.config.database().to_owned());
            crate::read_operation(
                &pool,
                &effective,
                parameters,
                crate::DEFAULT_BATCH_ROWS,
                budget,
                cancellation,
            )
            .await
        })
    }

    fn query<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a QueryOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            crate::query_operation(
                &pool,
                self.config.database(),
                operation,
                parameters,
                crate::DEFAULT_BATCH_ROWS,
                budget,
                cancellation,
            )
            .await
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            budget.ensure_active()?;
            let effective_cancellation = crate::read::BudgetCancellation::new(cancellation, budget);
            let token = effective_cancellation.token();
            let plan = crate::write::MysqlWritePlan::compile(
                &input_schema,
                operation,
                self.config.database(),
            )?;
            let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
            let column_count = u64::try_from(input_schema.fields().len()).map_err(|_| {
                provider_error(
                    ErrorCategory::ResourceLimit,
                    ErrorPhase::Prepare,
                    "numero colonne MySQL non rappresentabile",
                )
            })?;
            let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
            let pool = self.pool_for(secret)?;
            let mut session = pool.checkout(token).await?;
            let target_schema = operation
                .target
                .schema
                .as_deref()
                .unwrap_or_else(|| self.config.database());
            let target =
                describe_object(&mut session, target_schema, &operation.target.object, token)
                    .await?;
            let loss_report = plan.preflight(&target)?;
            drop(session);
            Ok(PreparedWrite {
                operation: operation.clone(),
                input_schema,
                loss_report,
                budget: budget.clone(),
                operation_lease,
                columns_lease,
            })
        })
    }

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(execute_mysql_write(
            self,
            secret,
            prepared,
            input,
            budget,
            cancellation,
        ))
    }
}

async fn execute_mysql_write(
    provider: &MysqlProvider,
    secret: &SecretString,
    prepared: PreparedWrite,
    mut input: Box<dyn BatchStream>,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<WriteOutcome> {
    if !prepared.budget.is_same_budget(budget) {
        return Err(provider_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Write,
            "budget MySQL sostituito fra prepare e write",
        ));
    }
    budget.ensure_active()?;
    let PreparedWrite {
        operation,
        input_schema: prepared_schema,
        loss_report: prepared_loss,
        budget: _prepared_budget,
        operation_lease: _operation_lease,
        columns_lease: _columns_lease,
    } = prepared;
    let schema = input.schema();
    if schema.as_ref() != prepared_schema.as_ref() {
        return Err(provider_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Write,
            "schema stream MySQL diverso dallo schema preparato",
        ));
    }
    let plan =
        crate::write::MysqlWritePlan::compile(&schema, &operation, provider.config.database())?;
    let effective_cancellation = crate::read::BudgetCancellation::new(cancellation, budget);
    let token = effective_cancellation.token();
    let pool = provider.pool_for(secret)?;
    let mut session = pool.checkout(token).await?;
    let target_schema = operation
        .target
        .schema
        .as_deref()
        .unwrap_or_else(|| provider.config.database());
    let target =
        describe_object(&mut session, target_schema, &operation.target.object, token).await?;
    if plan.preflight(&target)? != prepared_loss {
        return Err(provider_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            "preflight MySQL cambiato fra prepare e write",
        ));
    }
    // Quando la sorgente dichiara quante righe produrrà, la scrittura può
    // partizionare l'input fra righe rifiutate, annullate e mai tentate: solo
    // allora il percorso diagnostico riga per riga ha un input_total da
    // pubblicare, e il costo di uno statement per riga è giustificato.
    // La policy va rifiutata prima di aprire la transazione: un errore di
    // configurazione non può lasciare affidamento implicito al pool reset.
    let diagnostic_input = input
        .declared_input_rows()
        .map(|rows| validate_diagnostic_input(&schema, rows, input.row_diagnostics_policy()))
        .transpose()?;
    let execution_id = start_write_transaction(&mut session, token).await?;
    if let Some((declared_rows, policy)) = diagnostic_input {
        let result = diagnostic_mysql_write(
            &mut session,
            input.as_mut(),
            &schema,
            &plan,
            budget,
            token,
            policy,
            declared_rows,
            &execution_id,
        )
        .await;
        drop(session);
        return result;
    }
    let progress = match write_input_batches(
        &mut session,
        input.as_mut(),
        &schema,
        &plan,
        budget,
        token,
    )
    .await
    {
        Ok(progress) => progress,
        Err(error) => {
            return Err(rollback_after_failure(&mut session, error, &execution_id).await);
        }
    };
    let result = match session
        .exec_transaction(
            crate::session::MysqlTransactionCommand::Commit,
            ErrorPhase::Commit,
            token,
        )
        .await
    {
        Ok(()) => {
            crate::write::committed_outcome(execution_id, progress.received, progress.inserted)
        }
        Err(error) => {
            session.discard().await;
            crate::write::commit_failure(error, execution_id, progress.received)
        }
    };
    drop(session);
    result
}

fn validate_diagnostic_input(
    schema: &SchemaRef,
    declared_rows: u64,
    policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
) -> Result<(
    u64,
    plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
)> {
    plenora_database_core::row_diagnostics::WriteDiagnosticsTracker::new(
        declared_rows,
        policy.clone(),
    )?;
    for field in [
        policy.key_field.as_deref(),
        policy.constraint_column.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if schema.field_with_name(field).is_err() {
            return Err(provider_error(
                ErrorCategory::InvalidPlan,
                ErrorPhase::Prepare,
                "policy row-scoped riferita a un campo assente dallo schema preparato",
            ));
        }
    }
    Ok((declared_rows, policy))
}

async fn start_write_transaction(
    session: &mut crate::MysqlSession,
    cancellation: &CancellationToken,
) -> Result<String> {
    let execution_id = format!(
        "mysql-{}-{}",
        std::process::id(),
        WRITE_EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    session
        .exec_transaction(
            crate::session::MysqlTransactionCommand::Start,
            ErrorPhase::Write,
            cancellation,
        )
        .await?;
    Ok(execution_id)
}

/// Scrittura diagnostica: uno statement per riga sorgente.
///
/// Il rifiuto chiude la transazione e viaggia come errore che trasporta il
/// documento `plenora-row-diagnostics-v1`; un input applicato per intero
/// procede al commit come il percorso normale.
#[allow(clippy::too_many_arguments)]
async fn diagnostic_mysql_write(
    session: &mut crate::MysqlSession,
    input: &mut dyn BatchStream,
    schema: &SchemaRef,
    plan: &crate::write::MysqlWritePlan,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
    declared_rows: u64,
    execution_id: &str,
) -> Result<WriteOutcome> {
    // Il codice MySQL prova la classe; la colonna arriva esclusivamente dal
    // contratto dichiarato della sorgente, già verificato contro lo schema
    // preparato prima dell'apertura della transazione.
    let constraint_column = policy.constraint_column.clone();
    let mut tracker = plenora_database_core::row_diagnostics::WriteDiagnosticsTracker::new(
        declared_rows,
        policy,
    )?;
    let applied;
    let diagnosed = {
        let mut writer = crate::row_diagnostics::MysqlRowWriter::new(
            session,
            plan,
            input,
            schema,
            budget,
            cancellation,
            constraint_column,
        );
        let diagnosed = plenora_database_core::row_diagnostics::diagnose_row_scoped_write(
            &mut writer,
            &mut tracker,
        )
        .await;
        applied = writer.applied();
        diagnosed
    };
    match diagnosed {
        // Il seam ha già annullato la transazione: l'evidenza raccolta è
        // dentro il documento e non va rifatta qui.
        Ok(Some(outcome)) => {
            Err(outcome.into_error(Some(ProviderKind::Mysql), Some(execution_id.to_owned()))?)
        }
        Ok(None) => match session
            .exec_transaction(
                crate::session::MysqlTransactionCommand::Commit,
                ErrorPhase::Commit,
                cancellation,
            )
            .await
        {
            Ok(()) => {
                crate::write::committed_outcome(execution_id.to_owned(), declared_rows, applied)
            }
            Err(error) => {
                session.discard().await;
                crate::write::commit_failure(error, execution_id.to_owned(), declared_rows)
            }
        },
        Err(error) => Err(rollback_after_failure(session, error, execution_id).await),
    }
}

#[derive(Default)]
struct WriteProgress {
    received: u64,
    inserted: u64,
}

async fn write_input_batches(
    session: &mut crate::MysqlSession,
    input: &mut dyn BatchStream,
    schema: &SchemaRef,
    plan: &crate::write::MysqlWritePlan,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<WriteProgress> {
    let mut progress = WriteProgress::default();
    loop {
        let batch = match input.next_batch_with_cancellation(cancellation).await {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(mut error) => {
                error.phase = ErrorPhase::Write;
                error.provider = Some(ProviderKind::Mysql);
                return Err(error);
            }
        };
        crate::write::validate_batch_schema(&batch, schema)?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch_rows = u64::try_from(batch.num_rows()).map_err(|_| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                "righe batch MySQL non rappresentabili",
            )
        })?;
        plan.validate_spatial_batch(&batch, budget)?;
        let row_lease = budget.try_lease(ResourceKind::Rows, batch_rows)?;
        progress.received = progress.received.checked_add(batch_rows).ok_or_else(|| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                "overflow conteggio righe MySQL",
            )
        })?;
        let affected = write_batch_chunks(session, &batch, plan, cancellation).await?;
        progress.inserted = progress.inserted.checked_add(affected).ok_or_else(|| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                "overflow righe inserite MySQL",
            )
        })?;
        row_lease.commit(batch_rows)?;
    }
    Ok(progress)
}

async fn write_batch_chunks(
    session: &mut crate::MysqlSession,
    batch: &plenora_database_core::arrow::RecordBatch,
    plan: &crate::write::MysqlWritePlan,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let mut inserted = 0_u64;
    let mut start = 0_usize;
    while start < batch.num_rows() {
        let rows = plan.rows_per_statement().min(batch.num_rows() - start);
        let parameters = plan.bind_chunk(batch, start, rows)?;
        let sql = plan.render_insert(rows)?;
        let affected = session
            .exec_write(&sql, parameters, ErrorPhase::Write, cancellation)
            .await?;
        inserted = inserted.checked_add(affected).ok_or_else(|| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                "overflow righe inserite MySQL",
            )
        })?;
        start += rows;
    }
    Ok(inserted)
}

async fn rollback_after_failure(
    session: &mut crate::MysqlSession,
    error: DatabaseError,
    execution_id: &str,
) -> DatabaseError {
    let cleanup_cancellation = CancellationToken::new();
    let confirmed = session
        .exec_transaction(
            crate::session::MysqlTransactionCommand::Rollback,
            ErrorPhase::Rollback,
            &cleanup_cancellation,
        )
        .await
        .is_ok();
    if !confirmed {
        session.discard().await;
    }
    crate::write::rolled_back_error(error, confirmed, execution_id)
}

fn unsupported(message: impl Into<String>) -> DatabaseError {
    provider_error(ErrorCategory::Unsupported, ErrorPhase::Prepare, message)
}

fn mysql_spatial_capabilities() -> SpatialCapabilities {
    SpatialCapabilities {
        read_wkb: true,
        write_wkb: true,
        geometry: true,
        geography: false,
        spatial_index: false,
        mixed_geometry_types: true,
        dimensions: vec![plenora_database_core::geometry::Dimensions::Xy],
        functions: Vec::new(),
    }
}

fn provider_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: impl Into<String>,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::plan::{ComparisonOperator, ObjectRef};
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::query::{
        ColumnRef, JoinKind, QueryExpression, QueryJoin, QueryLock, QueryLockStrength,
        QueryLockWait, QueryProjection, QuerySource, ScalarFunction,
    };
    use plenora_database_core::resource::ResourceLimits;

    const fn assert_provider<T: Provider>() {}

    fn parameterized_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("warehouse".to_owned()),
                    object: "events".to_owned(),
                    layer_id: None,
                },
                alias: None,
            }),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                },
                alias: None,
            }],
            joins: Vec::new(),
            filter: Some(QueryExpression::Compare {
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                }),
                operator: ComparisonOperator::Eq,
                right: Box::new(QueryExpression::Parameter {
                    name: "wanted".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        }
    }

    #[tokio::test]
    async fn query_renders_and_binds_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let parameters = ParameterBag::new(BTreeMap::from([
            ("wanted".to_owned(), ParameterValue::I64(7)),
            ("unused".to_owned(), ParameterValue::I64(9)),
        ]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &parameterized_query(),
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("ParameterBag con parametro non usato accettato");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    #[tokio::test]
    async fn query_honours_cancellation_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &parameterized_query(),
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("token gia cancellato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Cancelled);
    }

    #[tokio::test]
    async fn query_keeps_unqualified_ast_fail_closed_before_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let mut operation = parameterized_query();
        operation.locking = Some(QueryLock {
            strength: QueryLockStrength::Update,
            relations: Vec::new(),
            wait: QueryLockWait::NoWait,
        });
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("locking esplicito non qualificato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il bind di HAVING deve essere estratto e richiesto come ogni altro:
    /// la mancanza del valore va vista prima di aprire la connessione.
    #[tokio::test]
    async fn query_demands_the_having_bind_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let mut operation = parameterized_query();
        let events = QueryExpression::Scalar {
            function: ScalarFunction::Count,
            arguments: vec![QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            }],
        };
        operation.projection = vec![
            QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "actor_id".to_owned(),
                    },
                },
                alias: None,
            },
            QueryProjection {
                expression: events.clone(),
                alias: Some("events".to_owned()),
            },
        ];
        operation.group_by = vec![QueryExpression::Column {
            column: ColumnRef {
                relation: None,
                field: "actor_id".to_owned(),
            },
        }];
        operation.having = Some(QueryExpression::Compare {
            left: Box::new(events),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("bind HAVING mancante accettato");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il bind della clausola ON e posizionale come ogni altro e precede
    /// quello di WHERE: la sua assenza deve essere vista prima di aprire la
    /// connessione, non al `COM_STMT_EXECUTE`.
    #[tokio::test]
    async fn query_demands_the_join_on_bind_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let qualified = |relation: &str, field: &str| QueryExpression::Column {
            column: ColumnRef {
                relation: Some(relation.to_owned()),
                field: field.to_owned(),
            },
        };
        let mut operation = parameterized_query();
        operation.source = Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            alias: Some("e".to_owned()),
        });
        operation.projection = vec![QueryProjection {
            expression: qualified("a", "name"),
            alias: Some("actor".to_owned()),
        }];
        operation.joins = vec![QueryJoin {
            kind: JoinKind::Inner,
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("warehouse".to_owned()),
                    object: "actors".to_owned(),
                    layer_id: None,
                },
                alias: Some("a".to_owned()),
            }),
            derived_source: None,
            lateral: false,
            on: Some(QueryExpression::And {
                arguments: vec![
                    QueryExpression::Compare {
                        left: Box::new(qualified("e", "actor_id")),
                        operator: ComparisonOperator::Eq,
                        right: Box::new(qualified("a", "actor_id")),
                    },
                    QueryExpression::Compare {
                        left: Box::new(qualified("a", "tier")),
                        operator: ComparisonOperator::Gte,
                        right: Box::new(QueryExpression::Parameter {
                            name: "tier".to_owned(),
                        }),
                    },
                ],
            }),
        }];
        operation.filter = Some(QueryExpression::Compare {
            left: Box::new(qualified("e", "event_id")),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "wanted".to_owned(),
            }),
        });
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("bind della clausola ON mancante accettato");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    fn append_write_operation() -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            mode: plenora_database_core::plan::WriteMode::Append,
            mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
            transaction_profile: plenora_database_core::plan::TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    fn append_input_schema() -> SchemaRef {
        Arc::new(plenora_database_core::arrow::Schema::new_with_metadata(
            vec![plenora_database_core::arrow::Field::new(
                "id",
                plenora_database_core::arrow::DataType::Int64,
                false,
            )],
            BTreeMap::from([(
                plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
                plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
            )])
            .into_iter()
            .collect(),
        ))
    }

    struct EmptyBatchStream(SchemaRef);

    impl BatchStream for EmptyBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.0)
        }

        fn next_batch(
            &mut self,
        ) -> plenora_database_core::provider::ProviderFuture<
            '_,
            Option<plenora_database_core::arrow::RecordBatch>,
        > {
            Box::pin(async { Ok(None) })
        }
    }

    #[test]
    fn invalid_row_diagnostics_policy_is_rejected_before_transaction_setup() {
        let schema = append_input_schema();
        let policy = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy::default();
        assert!(validate_diagnostic_input(&schema, 0, policy.clone()).is_err());

        let mut zero_examples = policy;
        zero_examples.examples_limit = 0;
        assert!(validate_diagnostic_input(&schema, 1, zero_examples).is_err());

        let missing_field = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
            key_field: Some("missing_key".to_owned()),
            constraint_column: Some("missing_constraint_column".to_owned()),
            examples_limit: 10,
        };
        assert!(validate_diagnostic_input(&schema, 1, missing_field).is_err());

        let declared = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
            key_field: Some("id".to_owned()),
            constraint_column: Some("id".to_owned()),
            examples_limit: 10,
        };
        let (_, validated) = validate_diagnostic_input(&schema, 1, declared)
            .expect("campi dichiarati presenti nello schema preparato");
        assert_eq!(validated.constraint_column.as_deref(), Some("id"));
    }

    fn prepared_write_for_test(budget: &ResourceBudget, input_schema: SchemaRef) -> PreparedWrite {
        PreparedWrite {
            operation: append_write_operation(),
            input_schema,
            loss_report: plenora_database_core::loss::LossReport {
                schema_version: 1,
                policy: plenora_database_core::loss::MappingPolicy::Strict,
                losses: Vec::new(),
            },
            budget: budget.clone(),
            operation_lease: budget
                .try_lease(
                    plenora_database_core::resource::ResourceKind::ConcurrentOperations,
                    1,
                )
                .expect("lease operazione"),
            columns_lease: budget
                .try_lease(plenora_database_core::resource::ResourceKind::Columns, 1)
                .expect("lease colonne"),
        }
    }

    /// Il piano di scrittura e compilato prima di aprire la connessione: una
    /// forma non qualificata non deve arrivare al server.
    #[tokio::test]
    async fn prepare_write_rejects_unqualified_operations_before_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let mut operation = append_write_operation();
        operation.mode = plenora_database_core::plan::WriteMode::Upsert;
        let outcome = provider
            .prepare_write(
                &SecretString::new("unique-secret"),
                &operation,
                append_input_schema(),
                &budget,
                &CancellationToken::new(),
            )
            .await;
        let Err(error) = outcome else {
            panic!("upsert MySQL non qualificato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Un token gia cancellato chiude prima del checkout: non esiste una
    /// connessione da quarantinare.
    #[tokio::test]
    async fn prepare_write_honours_cancellation_before_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let outcome = provider
            .prepare_write(
                &SecretString::new("unique-secret"),
                &append_write_operation(),
                append_input_schema(),
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("token gia cancellato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Cancelled);
    }

    /// Le lease di prepare non sono trasferibili: un budget diverso da quello
    /// che ha prodotto il piano non puo eseguire la scrittura.
    #[tokio::test]
    async fn write_rejects_a_budget_that_did_not_prepare_it() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let prepared_budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let prepared = prepared_write_for_test(&prepared_budget, append_input_schema());
        let foreign = ResourceBudget::new(ResourceLimits::default()).expect("budget estraneo");
        let error = provider
            .write(
                &SecretString::new("unique-secret"),
                prepared,
                Box::new(EmptyBatchStream(append_input_schema())),
                &foreign,
                &CancellationToken::new(),
            )
            .await
            .expect_err("budget estraneo");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
    }

    #[tokio::test]
    async fn write_rejects_a_stream_schema_different_from_prepare() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let prepared = prepared_write_for_test(&budget, append_input_schema());
        let renamed = Arc::new(plenora_database_core::arrow::Schema::new_with_metadata(
            vec![plenora_database_core::arrow::Field::new(
                "renamed",
                plenora_database_core::arrow::DataType::Int64,
                false,
            )],
            BTreeMap::from([(
                plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
                plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
            )])
            .into_iter()
            .collect(),
        ));
        let error = provider
            .write(
                &SecretString::new("unique-secret"),
                prepared,
                Box::new(EmptyBatchStream(renamed)),
                &budget,
                &CancellationToken::new(),
            )
            .await
            .expect_err("schema stream diverso");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Write);
    }

    #[test]
    fn provider_surface_is_typed_and_fail_closed() {
        assert_provider::<MysqlProvider>();
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 2).expect("provider");
        assert_eq!(provider.kind(), ProviderKind::Mysql);
        let rendered = format!("{provider:?}");
        assert!(!rendered.contains("unique-secret"));
    }

    #[test]
    fn published_spatial_capabilities_match_generic_geometry_contract() {
        let capabilities = mysql_spatial_capabilities();
        assert!(capabilities.read_wkb);
        assert!(capabilities.write_wkb);
        assert!(capabilities.geometry);
        assert!(capabilities.mixed_geometry_types);
        assert_eq!(
            capabilities.dimensions,
            vec![plenora_database_core::geometry::Dimensions::Xy]
        );
        assert!(!capabilities.spatial_index);
    }
}
