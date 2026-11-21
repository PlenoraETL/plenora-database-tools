use super::{
    borrowed_cancelled, classify_error, effective, prepare_state, prevalidate, quote_object,
    select_with_cancellation, write_prepared, BatchFailure, BatchStream, CancellationToken,
    DatabaseError, ErrorPhase, PostgresInsertMode, PostgresSchemaEvolution, RecordBatch,
    ResourceBudget, Result, SchemaRef, SessionPath, Transaction, WriteColumnPlan, WriteMode,
    WriteOperation, WriteRuntime,
};
use futures_util::{Stream, StreamExt};
use plenora_database_core::execution_loss::ExecutionLossReport;
use plenora_database_core::loss::LossConsent;
use plenora_database_core::provider::ProviderFuture;

/// Piano immutabile: preflight e lock sono eseguiti sulla transazione prestata.
pub struct WritePlan {
    operation: WriteOperation,
    schema: SchemaRef,
    pub insert_mode: PostgresInsertMode,
    pub max_batch_bytes: u64,
    pub max_wkb_cell_bytes: u64,
    pub consent: LossConsent,
}

impl WritePlan {
    /// Compila il contratto Arrow senza contattare il server.
    /// # Errors
    /// Schema o operazione non validi.
    pub fn new(operation: WriteOperation, schema: SchemaRef) -> Result<Self> {
        prepare_state(&schema, &operation)?;
        let provider = crate::PostgresProvider::default();
        Ok(Self {
            operation,
            schema,
            insert_mode: provider.insert_mode,
            max_batch_bytes: provider.max_batch_bytes,
            max_wkb_cell_bytes: provider.max_wkb_cell_bytes,
            consent: LossConsent::Deny,
        })
    }
}

/// Esito provvisorio: le righe sono applicate nella transazione, non committate.
/// Le perdite effettive confermate restano vuote fino alla decisione del chiamante.
#[derive(Debug)]
pub struct WriteOutcome {
    pub received: u64,
    pub applied: u64,
    pub loss_report: ExecutionLossReport,
}

struct Input<S> {
    schema: SchemaRef,
    stream: std::pin::Pin<Box<S>>,
}

impl<S: Stream<Item = RecordBatch> + Send> BatchStream for Input<S> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    /// Precondizione: il consumer valida il batch con `validated_batch` prima dei codec.
    fn next_batch<'a>(
        &'a mut self,
        _token: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async { Ok(self.stream.next().await) })
    }
}

/// Scrive senza commit, rollback, cambio di ruolo o timeout.
///
/// Il chiamante deve annullare la transazione dopo un errore; un successo
/// non garantisce che i vincoli differiti accetteranno il successivo commit.
/// # Errors
/// Gli stessi controlli di preflight, codec e budget del percorso posseduto.
#[allow(clippy::too_many_lines)]
pub async fn scrivi_arrow(
    tx: &Transaction<'_>,
    piano: &WritePlan,
    batch: impl Stream<Item = RecordBatch> + Send,
    budget: &ResourceBudget,
    annulla: &CancellationToken,
) -> Result<WriteOutcome> {
    let control = crate::read_stream::BudgetCancellation::new(annulla, budget)?;
    let cancellation = control.token();
    crate::error::check_cancelled(cancellation, ErrorPhase::Write)?;
    budget.ensure_active()?;
    let _operation_lease = budget.try_lease(
        plenora_database_core::resource::ResourceKind::ConcurrentOperations,
        1,
    )?;
    let _columns_lease = budget.try_lease(
        plenora_database_core::resource::ResourceKind::Columns,
        u64::try_from(piano.schema.fields().len())
            .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?,
    )?;
    let preparation = async {
        super::visibility::guard_search_path(tx, ErrorPhase::Prepare).await?;
        let operation = WriteOperation {
            target: crate::target_schema::resolve(
                tx,
                &piano.operation.target,
                if piano.operation.mode == WriteMode::Create {
                    crate::target_schema::Purpose::Create
                } else {
                    crate::target_schema::Purpose::Existing
                },
                ErrorPhase::Prepare,
            )
            .await?,
            ..piano.operation.clone()
        };
        let role: String = tx
            .query_one(
                "SELECT pg_catalog.current_setting('session_replication_role')",
                &[],
            )
            .await
            .map_err(|e| classify_error(ErrorPhase::Prepare, &e))?
            .get(0);
        if role != "origin" {
            return Err(DatabaseError::invalid_plan(
                "la transazione prestata richiede session_replication_role origin",
            ));
        }
        let mut state = prepare_state(&piano.schema, &operation)?;
        let (report, admitted, instances, snapshot) = crate::preflight::write_on(
            tx,
            None,
            PostgresSchemaEvolution::Disabled,
            &operation,
            &piano.schema,
            &state.metadata_plan,
        )
        .await?;
        plenora_database_core::prepared_execution::authorize(&report)?;
        state.apply_admission(&piano.schema, &operation, &admitted);
        let mut evaluation =
            effective::Evaluation::new(instances, &operation, piano.consent, budget)?;
        evaluation.profile(&operation)?;
        evaluation.metadata(&state.metadata_plan, &piano.schema, &operation)?;
        let input: Box<dyn BatchStream + '_> = Box::new(Input {
            schema: piano.schema.clone(),
            stream: Box::pin(batch),
        });
        let input = select_with_cancellation(
            prevalidate::input(
                input,
                &state.column_plans,
                budget,
                cancellation,
                piano.max_batch_bytes,
                piano.max_wkb_cell_bytes,
            ),
            cancellation,
        )
        .await
        .unwrap_or_else(|| Err(borrowed_cancelled(cancellation)))?;
        Ok((operation, state, evaluation, input, snapshot))
    };
    let (operation, mut state, mut evaluation, input, snapshot) =
        Box::pin(select_with_cancellation(preparation, cancellation))
            .await
            .unwrap_or_else(|| Err(borrowed_cancelled(cancellation)))?;
    let paths = SessionPath::borrowed();
    let provider = crate::PostgresProvider::default();
    let runtime = WriteRuntime {
        statement_timeout_ms: provider.statement_timeout_ms,
        lock_timeout_ms: provider.lock_timeout_ms,
        fault_point: None,
        insert_mode: piano.insert_mode,
        max_batch_bytes: piano.max_batch_bytes,
        max_wkb_cell_bytes: piano.max_wkb_cell_bytes,
        tls_mode: provider.tls_mode,
        tls_config: provider.tls_config,
        network_options: provider.network_options,
        schema_evolution: PostgresSchemaEvolution::Disabled,
        pool: provider.pool,
        metrics: provider.metrics,
        pool_acquire_timeout_ms: provider.pool_acquire_timeout_ms,
    };
    let work = async {
        if operation.mode != WriteMode::Create {
            tx.batch_execute(&format!(
                "LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE",
                quote_object(&operation.target)?
            ))
            .await
            .map_err(|e| classify_error(ErrorPhase::Write, &e))?;
            if let Some(snapshot) = snapshot {
                if crate::catalog::schema_token(tx, &operation.target).await? != snapshot {
                    return Err(DatabaseError::invalid_plan(
                        "contratto PostgreSQL cambiato dopo il preflight",
                    ));
                }
            }
        }
        if state.column_plans.iter().any(WriteColumnPlan::is_spatial) {
            let qualifier = crate::postgis_schema::qualifier(tx, ErrorPhase::Write).await?;
            for plan in &mut state.column_plans {
                plan.qualify_postgis(&qualifier);
            }
        }
        let registrations =
            crate::field_identity::prepare(tx, &operation, &piano.schema, &state.metadata_plan)
                .await?;
        write_prepared(
            tx,
            &operation,
            &piano.schema,
            &mut state.column_plans,
            &registrations,
            input,
            budget,
            cancellation,
            &runtime,
            &paths,
            &mut evaluation,
        )
        .await
        .map_err(|failure| match failure {
            BatchFailure::Other(error) | BatchFailure::ServerAborted(error) => error,
        })
    };
    let result = Box::pin(select_with_cancellation(work, cancellation))
        .await
        .unwrap_or_else(|| Err(borrowed_cancelled(cancellation)));
    let report = evaluation.ledger.report();
    let result = result.and_then(|(received, applied)| {
        if applied > received {
            return Err(DatabaseError::invalid_plan(
                "conteggio applicato oltre le righe ricevute",
            ));
        }
        Ok((received, applied))
    });
    let (received, applied) = result.map_err(|mut error| {
        error.loss_report = Some(Box::new(report.clone().into()));
        error.remote_effect = plenora_database_core::RemoteEffect::Unknown;
        error.retry = plenora_database_core::RetryDisposition::RequiresRecovery;
        error
    })?;
    Ok(WriteOutcome {
        received,
        applied,
        loss_report: report,
    })
}
