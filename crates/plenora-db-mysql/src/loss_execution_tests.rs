use super::*;
use plenora_database_core::arrow::array::{Int64Array, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::{DataType, Field, TimeUnit};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ObjectRef, TransactionProfile};
use plenora_database_core::store_rewrite::{ColumnRewrites, StoreRewrite};
use plenora_database_core::{RemoteEffect, ResourceLimits};
use std::sync::Arc;

fn fixture(mode: WriteMode, consent: LossConsent) -> (MysqlWritePlan, Evaluation, RecordBatch) {
    let schema = plenora_database_core::protocol::contract_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Timestamp(TimeUnit::Microsecond, None), true),
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
        target_type: "datetime(3)".into(),
        rewrites: vec![StoreRewrite::FractionalSeconds { digits: 3 }],
    }]);
    operation.mapping_policy = MappingPolicy::Compatible;
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let evaluation = Evaluation::new(&schema, &plan, &operation, consent, &budget).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_501])),
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
