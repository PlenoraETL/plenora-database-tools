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
