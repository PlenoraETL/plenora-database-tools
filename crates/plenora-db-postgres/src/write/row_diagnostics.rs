//! Percorso `PostgreSQL` row-scoped per input con cardinalita dichiarata.

use super::plan::WriteColumnPlan;
use super::prepared_codec::arrow_value;
use super::recovery::cancel_backend;
use super::resources::reserve_write_batch;
use super::sql::statement;
use super::{validate_batch_schema, WriteRuntime};
use crate::control::select_with_cancellation;
use crate::error::{classify_error, public_error_envelope};
use crate::{PostgresFaultPoint, PostgresInsertMode};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use plenora_database_core::outcome::{RowCounts, WriteOutcome, WriteStatus};
use plenora_database_core::plan::{ProviderKind, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::BatchStream;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::row_diagnostics::{
    diagnose_row_scoped_write, Completeness, DiagnosticScope, DiagnosticStateCounts,
    PartitionCount, RowApplication, RowDiagnostics, RowDiagnosticsPolicy, RowRejection,
    RowScopedWriter, RowWriteFuture, WriteDiagnosticsTracker, WriteOutcomePartition,
    CAUSE_CONSTRAINT_VIOLATION, DEFAULT_EXAMPLES_LIMIT,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition, RollbackEvidence,
};
use tokio_postgres::types::ToSql;
use tokio_postgres::{CancelToken, Statement, Transaction};

use std::collections::BTreeMap;

pub(super) struct DiagnosticInput {
    input_total: u64,
    policy: RowDiagnosticsPolicy,
}

/// Valida l'intera selezione del percorso diagnostico prima del checkout.
///
/// # Errors
///
/// Rifiuta cardinalita, policy, schema, modalita o profilo transazionale non
/// compatibili con la prima slice `PostgreSQL`.
pub(super) fn validate_input(
    prepared_schema: &SchemaRef,
    stream_schema: &SchemaRef,
    operation: &WriteOperation,
    input_total: u64,
    policy: RowDiagnosticsPolicy,
) -> Result<DiagnosticInput> {
    if prepared_schema.as_ref() != stream_schema.as_ref() {
        return Err(DatabaseError::invalid_plan(
            "schema stream PostgreSQL diverso dallo schema preparato",
        ));
    }
    if operation.mode != WriteMode::Append
        || operation.transaction_profile != TransactionProfile::SingleTransaction
    {
        return Err(DatabaseError::invalid_plan(
            "la diagnostica PostgreSQL supporta solo Append con SingleTransaction",
        ));
    }
    WriteDiagnosticsTracker::new(input_total, policy.clone())?;
    for field in [
        policy.key_field.as_deref(),
        policy.constraint_column.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if prepared_schema.field_with_name(field).is_err() {
            return Err(DatabaseError::invalid_plan(
                "policy row-scoped riferita a un campo assente dallo schema preparato",
            ));
        }
    }
    Ok(DiagnosticInput {
        input_total,
        policy,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute(
    transaction: Transaction<'_>,
    input: &mut dyn BatchStream,
    schema: &SchemaRef,
    operation: &WriteOperation,
    plans: &[WriteColumnPlan],
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    runtime: &WriteRuntime,
    cancel_token: &CancelToken,
    execution_id: &str,
    diagnostic_input: DiagnosticInput,
) -> Result<WriteOutcome> {
    let constraint_column = diagnostic_input.policy.constraint_column.clone();
    let mut tracker =
        WriteDiagnosticsTracker::new(diagnostic_input.input_total, diagnostic_input.policy)?;
    let writer = PostgresRowWriter::new(
        transaction,
        input,
        schema,
        operation,
        plans,
        budget,
        cancellation,
        runtime,
        constraint_column,
    )
    .await;
    let mut writer = match writer {
        Ok(writer) => writer,
        Err((transaction, error)) => {
            return Err(rollback_error(transaction, error, execution_id).await);
        }
    };
    let diagnosed = diagnose_row_scoped_write(&mut writer, &mut tracker).await;
    let applied = writer.applied;
    match diagnosed {
        Ok(Some(outcome)) => {
            Err(outcome.into_error(Some(ProviderKind::Postgres), Some(execution_id.to_owned()))?)
        }
        Err(error) => {
            let transaction = writer.take_transaction()?;
            Err(rollback_error(transaction, error, execution_id).await)
        }
        Ok(None) => {
            let transaction = writer.take_transaction()?;
            // Documento costruito e validato prima del commit, mentre il
            // rollback e ancora possibile: un conteggio incoerente scoperto
            // dopo lascerebbe il chiamante con un errore su dati gia scritti.
            let outcome = committed_outcome(
                execution_id.to_owned(),
                diagnostic_input.input_total,
                applied,
            );
            if let Err(error) = outcome.validate() {
                return Err(rollback_error(transaction, error, execution_id).await);
            }
            let commit_result = select_with_cancellation(transaction.commit(), cancellation).await;
            if commit_result.is_none() {
                runtime.metrics.write_outcome_unknown();
                cancel_backend(
                    cancel_token,
                    runtime.tls_mode,
                    runtime.tls_config.connector(),
                    runtime.network_options.connect_timeout_ms,
                )
                .await;
                return Err(commit_unknown_error(
                    crate::error::interruption_category(cancellation),
                    execution_id,
                    diagnostic_input.input_total,
                    "commit diagnostico PostgreSQL interrotto; stato remoto ignoto",
                )?);
            }
            if commit_result.is_some_and(|result| result.is_err()) {
                runtime.metrics.write_outcome_unknown();
                return Err(commit_unknown_error(
                    ErrorCategory::Protocol,
                    execution_id,
                    diagnostic_input.input_total,
                    "commit diagnostico PostgreSQL senza esito osservabile",
                )?);
            }
            if runtime.fault_point == Some(PostgresFaultPoint::AfterCommitAcknowledgement) {
                runtime.metrics.write_outcome_unknown();
                return Err(commit_unknown_error(
                    ErrorCategory::Protocol,
                    execution_id,
                    diagnostic_input.input_total,
                    "fault injection: acknowledgement commit PostgreSQL non osservabile",
                )?);
            }
            // Il documento e gia stato validato prima del commit: qui non
            // resta nulla che possa fallire.
            runtime.metrics.write_committed(applied);
            Ok(outcome)
        }
    }
}

fn commit_unknown_error(
    category: ErrorCategory,
    execution_id: &str,
    input_total: u64,
    message: &str,
) -> Result<DatabaseError> {
    let diagnostics = RowDiagnostics {
        contract: plenora_database_core::row_diagnostics::CONTRACT.to_owned(),
        scope: DiagnosticScope::Write,
        index_basis: plenora_database_core::row_diagnostics::INDEX_BASIS.to_owned(),
        completeness: Completeness::Unknown,
        knowledge_limits: vec!["remote.commit_ack_unobserved".to_owned()],
        observed_total: 0,
        total: None,
        input_total: Some(input_total),
        counts: BTreeMap::new(),
        examples_limit: DEFAULT_EXAMPLES_LIMIT,
        examples_truncated: false,
        examples: Vec::new(),
        diagnostic_state_counts: Some(DiagnosticStateCounts {
            certainly_rejected: 0,
            certainly_not_attempted: 0,
            certainly_rolled_back: 0,
            effect_unknown: 0,
        }),
        write_outcome: Some(WriteOutcomePartition {
            certainly_rejected: PartitionCount::Known { value: 0 },
            certainly_not_attempted: PartitionCount::Known { value: 0 },
            certainly_rolled_back: PartitionCount::Known { value: 0 },
            effect_unknown: PartitionCount::Known { value: input_total },
        }),
    };
    diagnostics.validate()?;
    Ok(DatabaseError {
        category,
        phase: ErrorPhase::Commit,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::Quarantine,
        provider: Some(ProviderKind::Postgres),
        execution_id: Some(execution_id.to_owned()),
        message: message.to_owned(),
        diagnostics: Some(Box::new(diagnostics)),
    })
}

const fn committed_outcome(execution_id: String, received: u64, inserted: u64) -> WriteOutcome {
    WriteOutcome {
        schema_version: 2,
        status: WriteStatus::Committed,
        execution_id,
        provider: ProviderKind::Postgres,
        rows: RowCounts {
            received,
            confirmed: inserted,
            inserted: Some(inserted),
            updated: Some(0),
            deleted: Some(0),
            failed: 0,
            skipped: received.saturating_sub(inserted),
        },
        recovery: None,
    }
}

async fn rollback_error(
    transaction: Transaction<'_>,
    mut error: DatabaseError,
    execution_id: &str,
) -> DatabaseError {
    let confirmed = transaction.rollback().await.is_ok();
    error.provider = Some(ProviderKind::Postgres);
    error.execution_id = Some(execution_id.to_owned());
    if confirmed {
        error.remote_effect = RemoteEffect::RolledBack;
        error.retry = RetryDisposition::Never;
    } else {
        error.phase = ErrorPhase::Rollback;
        error.remote_effect = RemoteEffect::Unknown;
        error.retry = RetryDisposition::Quarantine;
    }
    error
}

struct PostgresRowWriter<'transaction, 'input> {
    transaction: Option<Transaction<'transaction>>,
    statement: Statement,
    indexes: Vec<usize>,
    input: &'input mut dyn BatchStream,
    schema: &'input SchemaRef,
    plans: &'input [WriteColumnPlan],
    budget: &'input ResourceBudget,
    cancellation: &'input CancellationToken,
    runtime: &'input WriteRuntime,
    constraint_column: Option<String>,
    batch: Option<RecordBatch>,
    batch_start: u64,
    applied: u64,
}

impl<'transaction, 'input> PostgresRowWriter<'transaction, 'input> {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        transaction: Transaction<'transaction>,
        input: &'input mut dyn BatchStream,
        schema: &'input SchemaRef,
        operation: &WriteOperation,
        plans: &'input [WriteColumnPlan],
        budget: &'input ResourceBudget,
        cancellation: &'input CancellationToken,
        runtime: &'input WriteRuntime,
        constraint_column: Option<String>,
    ) -> std::result::Result<Self, (Transaction<'transaction>, DatabaseError)> {
        let (sql, indexes) =
            match diagnostic_statement(operation, schema, plans, runtime.insert_mode) {
                Ok(statement) => statement,
                Err(error) => return Err((transaction, error)),
            };
        let prepared = select_with_cancellation(transaction.prepare(&sql), cancellation).await;
        let statement = match prepared {
            Some(Ok(statement)) => statement,
            Some(Err(error)) => {
                return Err((transaction, classify_error(ErrorPhase::Prepare, &error)));
            }
            None => {
                return Err((
                    transaction,
                    diagnostic_error(
                        crate::error::interruption_category(cancellation),
                        "preparazione INSERT diagnostico PostgreSQL interrotta",
                    ),
                ));
            }
        };
        Ok(Self {
            transaction: Some(transaction),
            statement,
            indexes,
            input,
            schema,
            plans,
            budget,
            cancellation,
            runtime,
            constraint_column,
            batch: None,
            batch_start: 0,
            applied: 0,
        })
    }

    fn transaction(&self) -> Result<&Transaction<'transaction>> {
        self.transaction.as_ref().ok_or_else(|| {
            diagnostic_error(
                ErrorCategory::Protocol,
                "transazione diagnostica PostgreSQL non disponibile",
            )
        })
    }

    fn take_transaction(&mut self) -> Result<Transaction<'transaction>> {
        self.transaction.take().ok_or_else(|| {
            diagnostic_error(
                ErrorCategory::Protocol,
                "transazione diagnostica PostgreSQL non disponibile",
            )
        })
    }

    async fn locate(&mut self, source_index: u64) -> Result<usize> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                let rows = batch_rows(batch)?;
                let end = checked_batch_end(self.batch_start, rows)?;
                if source_index < end {
                    let offset = source_index.checked_sub(self.batch_start).ok_or_else(|| {
                        diagnostic_error(
                            ErrorCategory::InvalidPlan,
                            "indice sorgente PostgreSQL gia superato",
                        )
                    })?;
                    return usize::try_from(offset).map_err(|_| {
                        diagnostic_error(
                            ErrorCategory::InvalidPlan,
                            "offset di batch PostgreSQL non rappresentabile",
                        )
                    });
                }
                self.batch_start = end;
                self.batch = None;
            }
            let batch = self
                .input
                .next_batch(self.cancellation)
                .await
                .map_err(provider_write_error)?
                .ok_or_else(short_input_error)?;
            validate_batch_schema(&batch, self.schema)?;
            if batch.num_rows() == 0 {
                continue;
            }
            let resources = reserve_write_batch(&batch, self.plans, self.runtime, self.budget)?;
            resources.commit()?;
            self.batch = Some(batch);
        }
    }
}

fn diagnostic_statement(
    operation: &WriteOperation,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
    _configured_mode: PostgresInsertMode,
) -> Result<(String, Vec<usize>)> {
    // COPY appartiene esclusivamente al fast path. Questo seam costruisce
    // sempre un INSERT prepared con cardinalita uno.
    statement(operation, &operation.target, schema, plans)
}

impl RowScopedWriter for PostgresRowWriter<'_, '_> {
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
        Box::pin(async move {
            let offset = self.locate(source_index).await?;
            let values = {
                let batch = self.batch.as_ref().ok_or_else(|| {
                    diagnostic_error(
                        ErrorCategory::Protocol,
                        "batch PostgreSQL assente dopo il posizionamento",
                    )
                })?;
                self.indexes
                    .iter()
                    .map(|index| {
                        arrow_value(batch.column(*index).as_ref(), &self.plans[*index], offset)
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            let refs = values
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync))
                .collect::<Vec<_>>();
            let result = select_with_cancellation(
                self.transaction()?.execute(&self.statement, &refs),
                self.cancellation,
            )
            .await;
            match result {
                Some(Ok(1)) => {
                    self.applied = self.applied.checked_add(1).ok_or_else(|| {
                        diagnostic_error(
                            ErrorCategory::ResourceLimit,
                            "overflow nelle righe applicate PostgreSQL",
                        )
                    })?;
                    Ok(RowApplication::Applied)
                }
                Some(Ok(affected)) => Err(invalid_affected_rows(affected)),
                Some(Err(error)) => {
                    if let Some(cause) = row_rejection_cause(&error) {
                        Ok(RowApplication::Rejected(RowRejection {
                            cause: cause.to_owned(),
                            column: self.constraint_column.clone(),
                        }))
                    } else {
                        Err(classify_error(ErrorPhase::Write, &error))
                    }
                }
                None => Err(diagnostic_error(
                    crate::error::interruption_category(self.cancellation),
                    "INSERT diagnostico PostgreSQL interrotto",
                )),
            }
        })
    }

    fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(batch) = self.batch.take() {
                let end = checked_batch_end(self.batch_start, batch_rows(&batch)?)?;
                validate_consumed_batch_end(end, self.applied)?;
                self.batch_start = end;
            }
            loop {
                match self.input.next_batch(self.cancellation).await {
                    Ok(Some(batch)) if batch.num_rows() == 0 => {}
                    Ok(Some(_)) => {
                        return Err(diagnostic_error(
                            ErrorCategory::InvalidPlan,
                            "input PostgreSQL oltre il totale dichiarato",
                        ));
                    }
                    Ok(None) => return Ok(()),
                    Err(error) => return Err(provider_write_error(error)),
                }
            }
        })
    }

    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
        Box::pin(async move {
            let Some(transaction) = self.transaction.take() else {
                return RollbackEvidence::Lost;
            };
            if transaction.rollback().await.is_ok()
                && self.runtime.fault_point != Some(PostgresFaultPoint::RollbackAcknowledgementLost)
            {
                RollbackEvidence::Confirmed
            } else {
                RollbackEvidence::Lost
            }
        })
    }
}

fn row_rejection_cause(error: &tokio_postgres::Error) -> Option<&'static str> {
    error
        .as_db_error()
        .and_then(|db_error| row_rejection_cause_from_sqlstate(db_error.code().code()))
}

fn row_rejection_cause_from_sqlstate(code: &str) -> Option<&'static str> {
    match code {
        "23502" | "23503" | "23505" | "23514" | "23P01" => Some(CAUSE_CONSTRAINT_VIOLATION),
        _ => None,
    }
}

fn invalid_affected_rows(_affected: u64) -> DatabaseError {
    diagnostic_error(
        ErrorCategory::Protocol,
        "INSERT diagnostico PostgreSQL deve interessare esattamente una riga",
    )
}

fn short_input_error() -> DatabaseError {
    diagnostic_error(
        ErrorCategory::InvalidPlan,
        "input PostgreSQL esaurito prima delle righe dichiarate",
    )
}

fn validate_consumed_batch_end(end: u64, applied: u64) -> Result<()> {
    if end == applied {
        Ok(())
    } else {
        Err(diagnostic_error(
            ErrorCategory::InvalidPlan,
            "input PostgreSQL oltre il totale dichiarato",
        ))
    }
}

fn batch_rows(batch: &RecordBatch) -> Result<u64> {
    u64::try_from(batch.num_rows()).map_err(|_| {
        diagnostic_error(
            ErrorCategory::ResourceLimit,
            "righe batch PostgreSQL non rappresentabili",
        )
    })
}

fn checked_batch_end(batch_start: u64, rows: u64) -> Result<u64> {
    plenora_database_core::checked_source_row_end(batch_start, rows).ok_or_else(|| {
        diagnostic_error(
            ErrorCategory::ResourceLimit,
            "overflow nell'offset sorgente PostgreSQL",
        )
    })
}

const fn provider_write_error(mut error: DatabaseError) -> DatabaseError {
    error.phase = ErrorPhase::Write;
    error.provider = Some(ProviderKind::Postgres);
    error
}

fn diagnostic_error(category: ErrorCategory, message: &str) -> DatabaseError {
    public_error_envelope(
        category,
        ErrorPhase::Write,
        RemoteEffect::Unknown,
        RetryDisposition::RequiresRecovery,
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use plenora_database_testkit::{block_on, RowWriteScript, ScriptedRowWriter};
    use serde_json::json;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("parcel_id", DataType::Int64, false),
            Field::new("area_m2", DataType::Int64, false),
        ]))
    }

    fn operation(mode: WriteMode, transaction_profile: TransactionProfile) -> WriteOperation {
        WriteOperation {
            target: plenora_database_core::plan::ObjectRef {
                catalog: None,
                schema: Some("public".to_owned()),
                object: "parcels".to_owned(),
            },
            mode,
            mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
            transaction_profile,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    fn policy() -> RowDiagnosticsPolicy {
        RowDiagnosticsPolicy {
            key_field: Some("parcel_id".to_owned()),
            constraint_column: Some("area_m2".to_owned()),
            examples_limit: 10,
        }
    }

    #[test]
    fn classifier_uses_only_the_sqlstate_allowlist() {
        for code in ["23502", "23503", "23505", "23514", "23P01"] {
            assert_eq!(
                row_rejection_cause_from_sqlstate(code),
                Some(CAUSE_CONSTRAINT_VIOLATION)
            );
        }
        for code in ["", "22000", "40001", "40P01", "42501", "57014", "23515"] {
            assert_eq!(row_rejection_cause_from_sqlstate(code), None);
        }
    }

    #[test]
    fn commit_ambiguity_partitions_the_entire_declared_input_as_effect_unknown() {
        let error = commit_unknown_error(
            ErrorCategory::Protocol,
            "pg-commit-unknown",
            5_200,
            "commit acknowledgement unavailable",
        )
        .expect("valid commit-unknown diagnostics");

        assert_eq!(error.phase, ErrorPhase::Commit);
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.retry, RetryDisposition::Quarantine);
        let diagnostics = error.diagnostics.expect("diagnostics");
        diagnostics.validate().expect("valid rc17 document");
        assert_eq!(diagnostics.observed_total, 0);
        assert_eq!(diagnostics.input_total, Some(5_200));
        assert_eq!(diagnostics.completeness, Completeness::Unknown);
        assert_eq!(
            diagnostics.write_outcome,
            Some(WriteOutcomePartition {
                certainly_rejected: PartitionCount::Known { value: 0 },
                certainly_not_attempted: PartitionCount::Known { value: 0 },
                certainly_rolled_back: PartitionCount::Known { value: 0 },
                effect_unknown: PartitionCount::Known { value: 5_200 },
            })
        );
    }

    #[test]
    fn absolute_batch_offsets_are_checked() {
        assert_eq!(checked_batch_end(0, 4_096).expect("first batch"), 4_096);
        assert_eq!(
            checked_batch_end(4_096, 1_104).expect("second batch"),
            5_200
        );
        assert!(checked_batch_end(u64::MAX, 1).is_err());
    }

    #[test]
    fn affected_rows_must_be_exactly_one() {
        for affected in [0, 2, u64::MAX] {
            let error = invalid_affected_rows(affected);
            assert_eq!(error.category, ErrorCategory::Protocol);
            assert_eq!(error.phase, ErrorPhase::Write);
        }
    }

    #[test]
    fn short_and_extra_declared_input_are_fail_closed() {
        assert_eq!(short_input_error().category, ErrorCategory::InvalidPlan);
        assert!(validate_consumed_batch_end(5_200, 5_200).is_ok());
        assert!(validate_consumed_batch_end(5_201, 5_200).is_err());
    }

    #[test]
    fn configured_copy_modes_still_render_a_prepared_insert() {
        let schema = schema();
        let operation = operation(WriteMode::Append, TransactionProfile::SingleTransaction);
        let plans = super::super::compile_schema_plan(&schema, &operation).expect("plans");
        for configured in [PostgresInsertMode::CopyText, PostgresInsertMode::CopyBinary] {
            let (sql, indexes) =
                diagnostic_statement(&operation, &schema, &plans, configured).expect("statement");
            assert!(sql.starts_with("INSERT INTO "));
            assert_eq!(indexes, vec![0, 1]);
            assert!(!sql.contains("COPY"));
        }
    }

    #[test]
    fn diagnostic_policy_and_supported_mode_are_pre_io_validation() {
        let schema = schema();
        assert!(validate_input(
            &schema,
            &schema,
            &operation(WriteMode::Append, TransactionProfile::SingleTransaction),
            5_200,
            policy(),
        )
        .is_ok());

        let mut missing = policy();
        missing.constraint_column = Some("missing".to_owned());
        assert!(validate_input(
            &schema,
            &schema,
            &operation(WriteMode::Append, TransactionProfile::SingleTransaction),
            5_200,
            missing,
        )
        .is_err());
        let mut missing = policy();
        missing.key_field = Some("missing".to_owned());
        assert!(validate_input(
            &schema,
            &schema,
            &operation(WriteMode::Append, TransactionProfile::SingleTransaction),
            5_200,
            missing,
        )
        .is_err());
        for mode in [
            WriteMode::Create,
            WriteMode::Replace,
            WriteMode::TruncateInsert,
            WriteMode::Update,
            WriteMode::Upsert,
            WriteMode::DeleteByKeys,
        ] {
            assert!(validate_input(
                &schema,
                &schema,
                &operation(mode, TransactionProfile::SingleTransaction),
                5_200,
                policy(),
            )
            .is_err());
        }
        for profile in [
            TransactionProfile::ReadOnly,
            TransactionProfile::StagedSwap,
            TransactionProfile::ChunkCommitted,
            TransactionProfile::BestEffortDdl,
        ] {
            assert!(validate_input(
                &schema,
                &schema,
                &operation(WriteMode::Append, profile),
                5_200,
                policy(),
            )
            .is_err());
        }
    }

    #[test]
    fn postgres_envelope_matches_rc17_for_confirmed_and_lost_rollback() {
        for (rollback, phase, remote_effect, retry, rolled_back, unknown) in [
            (
                RollbackEvidence::Confirmed,
                ErrorPhase::Write,
                RemoteEffect::RolledBack,
                RetryDisposition::Never,
                json!({"state": "known", "value": 4999}),
                json!({"state": "known", "value": 0}),
            ),
            (
                RollbackEvidence::Lost,
                ErrorPhase::Rollback,
                RemoteEffect::Unknown,
                RetryDisposition::Quarantine,
                json!({"state": "unknown"}),
                json!({"state": "unknown"}),
            ),
        ] {
            let mut writer = ScriptedRowWriter::new(RowWriteScript::constraint_violation(
                4_999, "area_m2", rollback,
            ));
            let mut tracker = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
            let outcome = block_on(diagnose_row_scoped_write(&mut writer, &mut tracker))
                .expect("diagnostic seam")
                .expect("rejection");
            let error = outcome
                .into_error(
                    Some(ProviderKind::Postgres),
                    Some("pg-offline-rc17".to_owned()),
                )
                .expect("PostgreSQL envelope");

            writer
                .verify_one_statement_per_row(4_999)
                .expect("one prepared statement per source row");
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert_eq!(error.phase, phase);
            assert_eq!(error.remote_effect, remote_effect);
            assert_eq!(error.retry, retry);
            assert_eq!(error.provider, Some(ProviderKind::Postgres));
            assert_eq!(error.execution_id.as_deref(), Some("pg-offline-rc17"));
            assert_eq!(
                serde_json::to_value(error.row_diagnostics()).expect("diagnostics JSON"),
                json!({
                    "contract": "plenora-row-diagnostics-v1",
                    "scope": "write",
                    "index_basis": "source_row_zero_based",
                    "completeness": "complete",
                    "observed_total": 1,
                    "total": 1,
                    "input_total": 5200,
                    "counts": {"database.constraint_violation": 1},
                    "examples_limit": 10,
                    "examples_truncated": false,
                    "examples": [{
                        "source_index": 4999,
                        "cause": "database.constraint_violation",
                        "column": "area_m2",
                        "key": {"field": "parcel_id", "state": "redacted"},
                        "write_state": "certainly_rejected"
                    }],
                    "diagnostic_state_counts": {
                        "certainly_rejected": 1,
                        "certainly_not_attempted": 0,
                        "certainly_rolled_back": 0,
                        "effect_unknown": 0
                    },
                    "write_outcome": {
                        "certainly_rejected": {"state": "known", "value": 1},
                        "certainly_not_attempted": {"state": "known", "value": 200},
                        "certainly_rolled_back": rolled_back,
                        "effect_unknown": unknown
                    }
                })
            );
        }
    }
}
