//! Percorso SQL Server row-scoped qualificato per `Prepared` append atomico.

use super::plan::WritePlan;
use super::resources::WriteRowResources;
use super::{row_mutation, MutationCounts, SqlServerInsertMode, WriteFaultPoint};
use crate::connection::RowQueryResult;
use crate::PooledSqlServerSession;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::plan::{ProviderKind, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::BatchStream;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::row_diagnostics::{
    RowApplication, RowDiagnosticsPolicy, RowRejection, RowScopedWriter, RowWriteFuture,
    CAUSE_CONSTRAINT_VIOLATION,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition, RollbackEvidence, WriteDiagnosticsTracker,
};

pub(super) struct DiagnosticInput {
    pub(super) input_total: u64,
    pub(super) policy: RowDiagnosticsPolicy,
    pub(super) tracker: WriteDiagnosticsTracker,
}

pub(super) fn validate_input(
    prepared_schema: &SchemaRef,
    stream_schema: &SchemaRef,
    operation: &WriteOperation,
    insert_mode: SqlServerInsertMode,
    input_total: u64,
    policy: RowDiagnosticsPolicy,
) -> Result<DiagnosticInput> {
    if prepared_schema.as_ref() != stream_schema.as_ref() {
        return Err(DatabaseError::invalid_plan(
            "schema stream SQL Server diverso dallo schema preparato",
        ));
    }
    if operation.mode != WriteMode::Append
        || operation.transaction_profile != TransactionProfile::SingleTransaction
        || insert_mode != SqlServerInsertMode::Prepared
    {
        return Err(DatabaseError::invalid_plan(
            "la diagnostica SQL Server supporta solo Prepared Append con SingleTransaction",
        ));
    }
    let tracker = WriteDiagnosticsTracker::new(input_total, policy.clone())?;
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
        tracker,
    })
}

pub(super) struct SqlServerRowWriter<'a> {
    pooled: &'a mut PooledSqlServerSession,
    input: &'a mut dyn BatchStream,
    plan: &'a WritePlan,
    budget: &'a ResourceBudget,
    cancellation: &'a CancellationToken,
    constraint_column: Option<String>,
    batch: Option<RecordBatch>,
    batch_start: u64,
    applied: u64,
    mutations: MutationCounts,
    fault: Option<WriteFaultPoint>,
    execution_id: &'a str,
}

impl<'a> SqlServerRowWriter<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        pooled: &'a mut PooledSqlServerSession,
        input: &'a mut dyn BatchStream,
        plan: &'a WritePlan,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
        constraint_column: Option<String>,
        fault: Option<WriteFaultPoint>,
        execution_id: &'a str,
    ) -> Self {
        Self {
            pooled,
            input,
            plan,
            budget,
            cancellation,
            constraint_column,
            batch: None,
            batch_start: 0,
            applied: 0,
            mutations: MutationCounts {
                confirmed: 0,
                inserted: 0,
                updated: 0,
                deleted: 0,
            },
            fault,
            execution_id,
        }
    }

    pub(super) const fn mutations(&self) -> MutationCounts {
        self.mutations
    }

    async fn locate(&mut self, source_index: u64) -> Result<usize> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                let end = checked_batch_end(self.batch_start, batch_rows(batch)?)?;
                if source_index < end {
                    let offset = source_index.checked_sub(self.batch_start).ok_or_else(|| {
                        diagnostic_error(
                            ErrorCategory::InvalidPlan,
                            "indice sorgente SQL Server gia superato",
                        )
                    })?;
                    return usize::try_from(offset).map_err(|_| {
                        diagnostic_error(
                            ErrorCategory::InvalidPlan,
                            "offset di batch SQL Server non rappresentabile",
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
            if batch.schema().as_ref() != self.plan.input_schema.as_ref() {
                return Err(diagnostic_error(
                    ErrorCategory::Schema,
                    "schema batch SQL Server diverso dallo schema preparato",
                ));
            }
            if batch.num_rows() > 0 {
                self.batch = Some(batch);
            }
        }
    }
}

impl RowScopedWriter for SqlServerRowWriter<'_> {
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
        Box::pin(async move {
            let offset = self.locate(source_index).await?;
            let (query, resources) = {
                let batch = self.batch.as_ref().ok_or_else(|| {
                    diagnostic_error(
                        ErrorCategory::Protocol,
                        "batch SQL Server assente dopo il posizionamento",
                    )
                })?;
                (
                    super::codec::bind_row(self.plan, batch, offset)?,
                    WriteRowResources::reserve(batch, offset, self.plan, self.budget)?,
                )
            };
            match self
                .pooled
                .session_mut()?
                .execute_row_query(query, self.cancellation)
                .await?
            {
                RowQueryResult::Applied(results) => {
                    if let Err(error) = validate_append_output_cardinality(
                        results.len(),
                        results.first().map_or(0, Vec::len),
                    ) {
                        self.pooled.quarantine();
                        return Err(error);
                    }
                    let mutation = row_mutation(&results, WriteMode::Append).map_err(|error| {
                        self.pooled.quarantine();
                        ambiguous_affected_rows(error)
                    })?;
                    resources.commit()?;
                    self.mutations.checked_add(mutation)?;
                    self.applied = self.applied.checked_add(1).ok_or_else(|| {
                        diagnostic_error(
                            ErrorCategory::ResourceLimit,
                            "overflow nelle righe applicate SQL Server",
                        )
                    })?;
                    if self.fault == Some(WriteFaultPoint::TransportLostAfterFirstInsert) {
                        self.pooled.quarantine();
                        return Err(super::transport_loss_error(self.execution_id));
                    }
                    Ok(RowApplication::Applied)
                }
                RowQueryResult::ServerRejected { code, error } => {
                    if let Some(cause) = row_rejection_cause_from_code(code) {
                        Ok(RowApplication::Rejected(RowRejection {
                            cause: cause.to_owned(),
                            column: self.constraint_column.clone(),
                        }))
                    } else {
                        Err(error)
                    }
                }
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
                    Ok(Some(_)) => return Err(extra_input_error()),
                    Ok(None) => return Ok(()),
                    Err(error) => return Err(provider_write_error(error)),
                }
            }
        })
    }

    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
        Box::pin(async move {
            let cleanup = CancellationToken::new();
            let session = self.pooled.session_mut();
            let confirmed = match session {
                Ok(session) => {
                    #[cfg(test)]
                    if self.fault == Some(WriteFaultPoint::DelayRollbackResponse) {
                        session
                            .rollback_with_delayed_response(&cleanup)
                            .await
                            .is_ok()
                    } else {
                        session.rollback(&cleanup).await.is_ok()
                    }
                    #[cfg(not(test))]
                    {
                        session.rollback(&cleanup).await.is_ok()
                    }
                }
                Err(_) => false,
            };
            if confirmed {
                if self.pooled.allow_reuse_after_drain().is_err() {
                    self.pooled.quarantine();
                }
                RollbackEvidence::Confirmed
            } else {
                self.pooled.quarantine();
                RollbackEvidence::Lost
            }
        })
    }
}

pub(super) const fn row_rejection_cause_from_code(code: u32) -> Option<&'static str> {
    match code {
        515 | 547 | 2_601 | 2_627 => Some(CAUSE_CONSTRAINT_VIOLATION),
        _ => None,
    }
}

fn validate_append_output_cardinality(result_sets: usize, rows: usize) -> Result<()> {
    if result_sets == 1 && rows == 1 {
        Ok(())
    } else {
        Err(ambiguous_affected_rows(diagnostic_error(
            ErrorCategory::Protocol,
            "INSERT diagnostico SQL Server deve confermare esattamente una riga",
        )))
    }
}

fn batch_rows(batch: &RecordBatch) -> Result<u64> {
    u64::try_from(batch.num_rows()).map_err(|_| {
        diagnostic_error(
            ErrorCategory::ResourceLimit,
            "righe batch SQL Server non rappresentabili",
        )
    })
}

fn checked_batch_end(batch_start: u64, rows: u64) -> Result<u64> {
    batch_start.checked_add(rows).ok_or_else(|| {
        diagnostic_error(
            ErrorCategory::ResourceLimit,
            "overflow nell'offset sorgente SQL Server",
        )
    })
}

fn validate_consumed_batch_end(end: u64, applied: u64) -> Result<()> {
    if end == applied {
        Ok(())
    } else {
        Err(extra_input_error())
    }
}

fn short_input_error() -> DatabaseError {
    diagnostic_error(
        ErrorCategory::InvalidPlan,
        "input SQL Server esaurito prima delle righe dichiarate",
    )
}

fn extra_input_error() -> DatabaseError {
    diagnostic_error(
        ErrorCategory::InvalidPlan,
        "input SQL Server oltre il totale dichiarato",
    )
}

const fn ambiguous_affected_rows(mut error: DatabaseError) -> DatabaseError {
    error.remote_effect = RemoteEffect::Unknown;
    error.retry = RetryDisposition::Quarantine;
    error
}

const fn provider_write_error(mut error: DatabaseError) -> DatabaseError {
    error.phase = ErrorPhase::Write;
    error.provider = Some(ProviderKind::Sqlserver);
    error
}

fn diagnostic_error(category: ErrorCategory, message: &str) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::arrow::{DataType, Field, Schema};
    use plenora_database_testkit::{block_on, RowWriteScript, ScriptedRowWriter};
    use serde_json::json;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("parcel_id", DataType::Int64, false),
            Field::new("area_m2", DataType::Int64, false),
        ]))
    }

    fn operation(mode: WriteMode, profile: TransactionProfile) -> WriteOperation {
        WriteOperation {
            target: plenora_database_core::plan::ObjectRef {
                catalog: None,
                schema: Some("dbo".to_owned()),
                object: "parcels".to_owned(),
                layer_id: None,
            },
            mode,
            mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
            transaction_profile: profile,
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
    fn routing_and_policy_are_validated_before_io() {
        let schema = schema();
        assert!(validate_input(
            &schema,
            &schema,
            &operation(WriteMode::Append, TransactionProfile::SingleTransaction),
            SqlServerInsertMode::Prepared,
            5_200,
            policy(),
        )
        .is_ok());

        let mut missing = policy();
        missing.key_field = Some("missing".to_owned());
        assert!(validate_input(
            &schema,
            &schema,
            &operation(WriteMode::Append, TransactionProfile::SingleTransaction),
            SqlServerInsertMode::Prepared,
            5_200,
            missing,
        )
        .is_err());
        let mut missing = policy();
        missing.constraint_column = Some("missing".to_owned());
        assert!(validate_input(
            &schema,
            &schema,
            &operation(WriteMode::Append, TransactionProfile::SingleTransaction),
            SqlServerInsertMode::Prepared,
            5_200,
            missing,
        )
        .is_err());

        assert!(validate_input(
            &schema,
            &schema,
            &operation(WriteMode::Append, TransactionProfile::SingleTransaction),
            SqlServerInsertMode::TdsBulk,
            5_200,
            policy(),
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
                SqlServerInsertMode::Prepared,
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
            TransactionProfile::ArcgisApplyEdits,
        ] {
            assert!(validate_input(
                &schema,
                &schema,
                &operation(WriteMode::Append, profile),
                SqlServerInsertMode::Prepared,
                5_200,
                policy(),
            )
            .is_err());
        }
    }

    #[test]
    fn classifier_uses_only_structured_tds_codes() {
        for code in [515, 547, 2_601, 2_627] {
            assert_eq!(
                row_rejection_cause_from_code(code),
                Some(CAUSE_CONSTRAINT_VIOLATION)
            );
        }
        for code in [0, 207, 245, 1_205, 1_222, 8_152, u32::MAX] {
            assert_eq!(row_rejection_cause_from_code(code), None);
        }
    }

    #[test]
    fn batch_offsets_short_extra_and_affected_rows_are_fail_closed() {
        assert_eq!(checked_batch_end(0, 4_096).expect("first batch"), 4_096);
        assert_eq!(
            checked_batch_end(4_096, 1_104).expect("second batch"),
            5_200
        );
        assert!(checked_batch_end(u64::MAX, 1).is_err());
        assert_eq!(short_input_error().category, ErrorCategory::InvalidPlan);
        assert!(validate_consumed_batch_end(5_200, 5_200).is_ok());
        assert!(validate_consumed_batch_end(5_201, 5_200).is_err());
        assert!(validate_append_output_cardinality(1, 1).is_ok());

        for (sets, rows) in [(0, 0), (1, 0), (1, 2), (2, 1)] {
            let error = validate_append_output_cardinality(sets, rows)
                .expect_err("cardinality must be rejected");
            assert_eq!(error.remote_effect, RemoteEffect::Unknown);
            assert_eq!(error.retry, RetryDisposition::Quarantine);
        }
    }

    #[test]
    fn sqlserver_envelope_matches_rc17_for_confirmed_and_lost_rollback() {
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
            let outcome = block_on(plenora_database_core::diagnose_row_scoped_write(
                &mut writer,
                &mut tracker,
            ))
            .expect("diagnostic seam")
            .expect("rejection");
            let error = outcome
                .into_error(
                    Some(ProviderKind::Sqlserver),
                    Some("sqlserver-offline-rc17".to_owned()),
                )
                .expect("SQL Server envelope");

            writer
                .verify_one_statement_per_row(4_999)
                .expect("one statement per source row");
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert_eq!(error.phase, phase);
            assert_eq!(error.remote_effect, remote_effect);
            assert_eq!(error.retry, retry);
            assert_eq!(error.provider, Some(ProviderKind::Sqlserver));
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
