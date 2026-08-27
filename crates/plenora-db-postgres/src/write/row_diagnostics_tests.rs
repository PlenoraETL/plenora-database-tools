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
