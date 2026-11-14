use super::*;
use plenora_database_core::arrow::array::{Int64Array, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::{DataType, Field, TimeUnit};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ObjectRef, TransactionProfile};
use plenora_database_core::store_rewrite::{ColumnRewrites, StoreRewrite};
use plenora_database_core::{RemoteEffect, ResourceLimits};
use std::sync::Arc;

#[test]
fn p6_common_surface_reports_match_the_evaluator() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/mysql_permissive/surfaces.json"
    ))
    .unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        let consent = LossConsent::from_accept_loss(case["accept_loss"].as_bool().unwrap());
        let (plan, mut evaluation, mut batch) =
            fixture_with(WriteMode::Append, consent, case["name"] == "closed");
        if case["name"] == "exact" {
            batch = RecordBatch::try_new(
                batch.schema(),
                vec![
                    batch.column(0).clone(),
                    Arc::new(TimestampMicrosecondArray::from(vec![1_000_000])),
                ],
            )
            .unwrap();
        }
        let mut published = None;
        if case["name"] != "planned" {
            evaluation.begin(&batch).unwrap();
            let bound = evaluation.bind(&plan, &batch, 0, 1);
            if matches!(case["name"].as_str(), Some("denied" | "closed")) {
                assert_eq!(bound.unwrap_err().category, ErrorCategory::DataMapping);
            } else {
                bound.unwrap();
                evaluation.sent(0, 1).unwrap();
                evaluation.applied(0, 1, true).unwrap();
                if case["name"] != "rollback" {
                    evaluation.context.prepare_commit(true).unwrap();
                }
            }
            if matches!(case["name"].as_str(), Some("exact" | "committed")) {
                published = evaluation
                    .finish(Ok(
                        crate::write::committed_outcome("p6".into(), 1, 1).unwrap()
                    ))
                    .unwrap()
                    .loss_report;
            } else {
                let mut error = invalid();
                error.remote_effect = if case["name"] == "uncertain" {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::RolledBack
                };
                published = evaluation
                    .finish(Err(error))
                    .unwrap_err()
                    .loss_report
                    .map(|report| *report);
            }
        }
        let actual = serde_json::to_value(
            published.unwrap_or_else(|| evaluation.context.ledger.report().into()),
        )
        .unwrap();
        assert_eq!(actual, case["outcome"]["loss_report"], "{}", case["name"]);
    }
}

fn fixture(mode: WriteMode, consent: LossConsent) -> (MysqlWritePlan, Evaluation, RecordBatch) {
    fixture_with(mode, consent, false)
}
fn fixture_with(
    mode: WriteMode,
    consent: LossConsent,
    closed: bool,
) -> (MysqlWritePlan, Evaluation, RecordBatch) {
    let schema = plenora_database_core::protocol::contract_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "v",
            if closed {
                DataType::Float32
            } else {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            },
            true,
        ),
    ])
    .unwrap();
    let mut operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("db".into()),
            object: "p4".into(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: if matches!(mode, WriteMode::Update | WriteMode::Upsert) {
            vec!["id".into()]
        } else {
            vec![]
        },
        update_columns: vec![],
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let mut plan = MysqlWritePlan::compile_with_profile(
        &schema,
        &operation,
        "db",
        &crate::profile::MYSQL_PROFILE,
    )
    .unwrap();
    plan.set_rewrites(vec![ColumnRewrites {
        field: 1,
        target_type: if closed { "float(7,3)" } else { "datetime(3)" }.into(),
        rewrites: vec![if closed {
            StoreRewrite::Every
        } else {
            StoreRewrite::FractionalSeconds { digits: 3 }
        }],
    }]);
    operation.mapping_policy = MappingPolicy::Compatible;
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let evaluation = Evaluation::new(&schema, &plan, &operation, consent, &budget).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            if closed {
                Arc::new(plenora_database_core::arrow::array::Float32Array::from(
                    vec![0.0],
                ))
            } else {
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_501]))
            },
        ],
    )
    .unwrap();
    (plan, evaluation, batch)
}
#[test]
fn p4_cache_second_batch_and_commit_attribution() {
    let (plan, mut e, batch) = fixture(WriteMode::Append, LossConsent::Accept);
    for _ in 0..2 {
        e.begin(&batch).unwrap();
        let first = e.bind(&plan, &batch, 0, 1).unwrap();
        assert_eq!(first, e.bind(&plan, &batch, 0, 1).unwrap());
        assert!(e.context.ledger.report().effective.is_empty());
        e.sent(0, 1).unwrap();
        e.applied(0, 1, true).unwrap();
    }
    e.context.prepare_commit(true).unwrap();
    let outcome = crate::write::committed_outcome("p4".into(), 2, 2).unwrap();
    let outcome = e.finish(Ok(outcome)).unwrap();
    outcome.validate().unwrap();
    let json = serde_json::to_value(outcome).unwrap();
    assert_eq!(json["schema_version"], 3);
    assert_eq!(e.context.confirmed.effective[0].committed, 2);
}
#[test]
fn p4_no_match_consent_and_stage_only_uncertain() {
    let (plan, mut e, batch) = fixture(WriteMode::Update, LossConsent::from_accept_loss(false));
    e.begin(&batch).unwrap();
    e.bind_keys(&plan, &batch, 0).unwrap();
    e.applied(0, 1, false).unwrap();
    assert!(e.context.ledger.report().evaluation.entries.is_empty());
    let (plan, mut e, batch) = fixture(WriteMode::Append, LossConsent::from_accept_loss(false));
    e.begin(&batch).unwrap();
    assert!(e.bind(&plan, &batch, 0, 1).is_err());
    assert_eq!(
        e.context.ledger.report().evaluation.entries[0].would_change,
        1
    );
    let (plan, mut e, batch) = fixture(WriteMode::Append, LossConsent::Accept);
    e.begin(&batch).unwrap();
    e.bind(&plan, &batch, 0, 1).unwrap();
    e.context.ledger.uncertain().unwrap();
    assert!(e.context.ledger.report().effective.is_empty());
}
#[test]
fn p4_rollback_and_lost_commit_preserve_evaluation() {
    for effect in [RemoteEffect::RolledBack, RemoteEffect::Unknown] {
        let (plan, mut e, batch) = fixture(WriteMode::Append, LossConsent::Accept);
        e.begin(&batch).unwrap();
        e.bind(&plan, &batch, 0, 1).unwrap();
        e.sent(0, 1).unwrap();
        e.applied(0, 1, true).unwrap();
        e.context.prepare_commit(true).unwrap();
        let mut error = invalid();
        error.remote_effect = effect;
        let error = e.finish(Err(error)).unwrap_err();
        let report = e.context.ledger.report();
        assert_eq!(report.evaluation.entries[0].would_change, 1);
        if effect == RemoteEffect::RolledBack {
            assert!(report.effective.is_empty());
        } else {
            assert_eq!(report.effective[0].uncertain, 1);
            assert_eq!(report.effective[0].committed, 0);
        }
        assert!(error.loss_report.is_some());
    }
}
#[test]
fn p4_retry_reuses_authorized_values_and_limit_precedes_effects() {
    let codec = RuleCodec::new(
        ProviderKind::Mysql,
        0,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        "datetime(3)",
        StoreRewrite::FractionalSeconds { digits: 3 },
    )
    .unwrap();
    assert!(!codec.matches_rewrites(&[
        StoreRewrite::FractionalSeconds { digits: 3 },
        StoreRewrite::FractionalSeconds { digits: 2 }
    ]));

    let (plan, mut e, batch) = fixture(WriteMode::Upsert, LossConsent::Accept);
    e.begin(&batch).unwrap();
    let values = e.bind(&plan, &batch, 0, 1).unwrap();
    e.sent(0, 1).unwrap();
    e.applied(0, 1, false).unwrap();
    for token in &e.rows[0].as_ref().unwrap().tokens {
        e.context.ledger.retry_token(*token).unwrap();
    }
    assert_eq!(values, e.bind(&plan, &batch, 0, 1).unwrap());
    e.sent(0, 1).unwrap();
    e.applied(0, 1, true).unwrap();
    assert_eq!(e.context.ledger.report().evaluation.entries[0].checked, 1);
    e.received = SERIAL_ROW_LIMIT;
    assert_eq!(
        e.begin(&batch).unwrap_err().category,
        ErrorCategory::ResourceLimit
    );
}

#[test]
fn p5_frozen_replay_preserves_values_tokens_and_counts() {
    let (plan, mut e, batch) = fixture(WriteMode::Create, LossConsent::Accept);
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let mut values = Vec::new();
    for _ in 0..2 {
        e.reserve_rows(&budget, batch.num_rows()).unwrap();
        e.begin(&batch).unwrap();
        values.push(e.bind(&plan, &batch, 0, 1).unwrap());
        e.freeze_batch();
    }
    e.context.prepare_commit(true).unwrap();
    let before = e.context.ledger.report().evaluation;
    e.replay();
    for expected in values {
        e.begin(&batch).unwrap();
        assert_eq!(expected, e.bind(&plan, &batch, 0, 1).unwrap());
        assert_eq!(before, e.context.ledger.report().evaluation);
        e.sent(0, 1).unwrap();
        e.applied(0, 1, true).unwrap();
        e.commit_rows(1).unwrap();
    }
    assert!(e.begin(&batch).is_err());
}
#[test]
fn p5_reserves_remaining_budget_and_rejects_loss_before_replay() {
    let (plan, mut e, batch) = fixture(WriteMode::Create, LossConsent::from_accept_loss(false));
    let limits = ResourceLimits {
        rows: 2,
        ..ResourceLimits::default()
    };
    let budget = ResourceBudget::new(limits).unwrap();
    let _occupied = budget
        .try_lease(plenora_database_core::resource::ResourceKind::Rows, 2)
        .unwrap();
    assert_eq!(
        e.reserve_rows(&budget, 1).unwrap_err().category,
        ErrorCategory::ResourceLimit
    );
    e.begin(&batch).unwrap();
    assert!(e.bind(&plan, &batch, 0, 1).is_err());
    assert!(e.context.ledger.report().effective.is_empty());
}
