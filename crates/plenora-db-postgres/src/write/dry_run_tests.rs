use super::*;
use arrow_array::StringArray;
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::loss::{LossConsent, MappingPolicy};
use plenora_database_core::loss_registry::{RuleId, RuleInstance, RuleParameters};
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::resource::ResourceLimits;
use std::collections::VecDeque;
use std::sync::Arc;

struct Input {
    batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
    declared: Option<u64>,
}

impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn declared_input_rows(&self) -> Option<u64> {
        self.declared
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.batches.pop_front())))
    }
}

#[tokio::test]
async fn public_offline_dry_run_defaults_to_deny_and_honors_explicit_consent() {
    let provider = crate::PostgresProvider::default();
    let mut operation = crate::write_visibility_live_tests::operation("target", WriteMode::Create);
    operation.mapping_policy = MappingPolicy::Compatible;
    let field =
        Field::new("v", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            plenora_database_core::protocol::POSTGRES_NATIVE_TYPE.to_owned(),
            "jsonb".to_owned(),
        )]));
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![r#"{"a":1,"a":2}"#]))],
    )
    .unwrap();
    let input = || {
        Box::new(Input {
            schema: schema.clone(),
            batches: VecDeque::from([batch.clone()]),
            declared: None,
        }) as Box<dyn BatchStream>
    };
    let budget = || ResourceBudget::new(ResourceLimits::default()).unwrap();
    let cancel = CancellationToken::new();
    let denied = provider
        .dry_run_write(None, &operation, input(), &budget(), &cancel)
        .await
        .unwrap_err();
    assert_eq!(denied.category, ErrorCategory::DataMapping);
    assert_eq!(denied.phase, ErrorPhase::Write);
    assert_eq!(denied.remote_effect, RemoteEffect::None);
    provider
        .dry_run_write_with_consent(
            None,
            &operation,
            input(),
            &budget(),
            &cancel,
            LossConsent::Accept,
        )
        .await
        .unwrap();
    for profile in [
        TransactionProfile::ChunkCommitted,
        TransactionProfile::BestEffortDdl,
    ] {
        operation.transaction_profile = profile;
        let error = provider
            .dry_run_write(None, &operation, input(), &budget(), &cancel)
            .await
            .unwrap_err();
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.remote_effect, RemoteEffect::None);
    }
}

#[tokio::test]
async fn dry_run_reuses_writer_classification_for_every_policy_path_and_consent() {
    use super::super::canonical::{Leaf, LeafClass, SessionRule, SessionShape};
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for declared in [None, Some(2)] {
            for consent in [LossConsent::Deny, LossConsent::Accept] {
                for value in [Some("abc"), Some("a"), Some("abc "), None] {
                    let mut operation =
                        crate::write_visibility_live_tests::operation("target", WriteMode::Append);
                    operation.mapping_policy = policy;
                    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(StringArray::from(vec![value]))],
                    )
                    .unwrap();
                    let mut state = super::super::prepare_state(&schema, &operation).unwrap();
                    state.column_plans[0].session_rule = Some(SessionRule {
                        leaves: vec![Leaf {
                            class: LeafClass::Padded { chars: 3 },
                            shape: SessionShape::Scalar,
                        }],
                        strict: policy == MappingPolicy::Strict,
                        key: false,
                    });
                    if policy != MappingPolicy::Strict {
                        state.loss_instances = vec![RuleInstance {
                            field_id: 0,
                            rule_id: RuleId::PgPadText,
                            parameters: RuleParameters::default(),
                            source_type: None,
                            target_type: None,
                        }];
                    }
                    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
                    let mut writer = super::super::effective::Evaluation::new(
                        state.loss_instances.clone(),
                        &operation,
                        consent,
                        &budget,
                    )
                    .unwrap();
                    writer.bind(&state.column_plans).unwrap();
                    let expected = writer.batch(&batch, &state.column_plans, &operation);
                    let mut input = Input {
                        batches: VecDeque::from([batch.clone(), batch]),
                        schema,
                        declared,
                    };
                    let actual = verify_input_with_consent(
                        &state,
                        &operation,
                        &mut input,
                        &budget,
                        &CancellationToken::new(),
                        InputLimits {
                            insert_mode: PostgresInsertMode::CopyText,
                            max_batch_bytes: 1_048_576,
                            max_wkb_cell_bytes: 1_048_576,
                        },
                        StatementGate::default(),
                        consent,
                    )
                    .await;
                    match (expected, actual) {
                        (Ok(_), Ok(verified)) => assert_eq!(verified.rows, 2),
                        (Err(expected), Err(actual)) => {
                            assert_eq!(actual.category, expected.category);
                            assert_eq!(actual.phase, expected.phase);
                            assert_eq!(actual.retry, expected.retry);
                            assert_eq!(actual.remote_effect, RemoteEffect::None);
                            let Some(plenora_database_core::execution_loss::VersionedLossReport::Execution(report)) = actual.loss_report.as_deref() else {
                                panic!("rapporto esecutivo assente")
                            };
                            assert_eq!(*report, writer.ledger.report());
                            assert!(report.effective.is_empty());
                            report.validate().unwrap();
                        }
                        result => panic!(
                            "parita dry run: {policy:?}/{declared:?}/{consent:?}: {result:?}"
                        ),
                    }
                }
            }
        }
    }
}

// I rifiuti R5 devono restare DataMapping anche senza cardinalita dichiarata:
// il rischio esiste, ma nessuna perdita e effettiva prima dell'invio.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn r5_leaf_refusals_keep_execution_risk_without_effects() {
    use super::super::canonical::{Leaf, LeafClass, SessionRule, SessionShape};
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for profile in [PostgresInsertMode::Prepared, PostgresInsertMode::CopyText] {
            for declared in [None, Some(1)] {
                for (class, rule_id, value) in [
                    (
                        LeafClass::Text { max_chars: Some(3) },
                        RuleId::PgTextLimit,
                        "ab   ",
                    ),
                    (LeafClass::Jsonb, RuleId::PgJsonb, r#"{"a":1,"a":2}"#),
                ] {
                    let mut operation =
                        crate::write_visibility_live_tests::operation("target", WriteMode::Append);
                    operation.mapping_policy = policy;
                    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(StringArray::from(vec![value]))],
                    )
                    .unwrap();
                    let mut state = super::super::prepare_state(&schema, &operation).unwrap();
                    state.column_plans[0].session_rule = Some(SessionRule {
                        leaves: vec![Leaf {
                            class,
                            shape: SessionShape::Scalar,
                        }],
                        strict: false,
                        key: false,
                    });
                    state.loss_instances = vec![RuleInstance {
                        field_id: 0,
                        rule_id,
                        parameters: RuleParameters::default(),
                        source_type: None,
                        target_type: None,
                    }];
                    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
                    let mut writer = super::super::effective::Evaluation::new(
                        state.loss_instances.clone(),
                        &operation,
                        LossConsent::Deny,
                        &budget,
                    )
                    .unwrap();
                    writer.bind(&state.column_plans).unwrap();
                    let written = writer
                        .batch(&batch, &state.column_plans, &operation)
                        .unwrap_err();
                    let mut input = Input {
                        batches: VecDeque::from([batch]),
                        schema,
                        declared,
                    };
                    let dry = verify_input_with_consent(
                        &state,
                        &operation,
                        &mut input,
                        &budget,
                        &CancellationToken::new(),
                        InputLimits {
                            insert_mode: profile,
                            max_batch_bytes: 1_048_576,
                            max_wkb_cell_bytes: 1_048_576,
                        },
                        StatementGate::default(),
                        LossConsent::Deny,
                    )
                    .await
                    .unwrap_err();
                    for (path, error) in [("dry run", &dry), ("writer batch", &written)] {
                        let context = format!(
                            "{path}: {policy:?}/{profile:?}/{declared:?}/v/{rule_id:?}: {}",
                            error.message
                        );
                        assert_eq!(error.category, ErrorCategory::DataMapping, "{context}");
                        assert_eq!(error.phase, ErrorPhase::Write, "{context}");
                    }
                    assert_eq!(dry.remote_effect, RemoteEffect::None);
                    let Some(
                        plenora_database_core::execution_loss::VersionedLossReport::Execution(
                            report,
                        ),
                    ) = dry.loss_report.as_deref()
                    else {
                        panic!("{policy:?}/{profile:?}/{declared:?}/{rule_id:?}: rapporto assente")
                    };
                    assert!(!report.risk.is_empty());
                    assert!(report.effective.is_empty());
                    assert_eq!(*report, writer.ledger.report());
                    report.validate().unwrap();
                    if rule_id == RuleId::PgJsonb {
                        assert!(report
                            .evaluation
                            .entries
                            .iter()
                            .any(|entry| entry.would_change == 1));
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn unknown_dimensions_validate_the_last_batch_before_delivery_and_obey_budget() {
    use plenora_database_core::protocol as p;
    let field = Field::new("geom", DataType::Binary, true).with_metadata(
        std::collections::HashMap::from([
            (p::FIELD_ID.to_owned(), "42".to_owned()),
            (p::GEOMETRY_ENCODING.to_owned(), "ewkb".to_owned()),
            (p::GEOMETRY_DIMENSIONS.to_owned(), "unknown".to_owned()),
            (p::GEOMETRY_TYPES_DECLARATION.to_owned(), "exact".to_owned()),
            (p::GEOMETRY_TYPES.to_owned(), "point".to_owned()),
            (
                p::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                "geometry".to_owned(),
            ),
            (p::GEOMETRY_PRECISION.to_owned(), "float64".to_owned()),
            (p::GEOMETRY_CRS_RESOLUTION.to_owned(), "missing".to_owned()),
        ]),
    );
    let plan = super::super::plan::WriteColumnPlan::compile(&field).unwrap();
    let schema = p::contract_schema(vec![field]).unwrap();
    let mut point = vec![1, 1, 0, 0, 0];
    point.extend([0; 16]);
    let bad = plenora_database_core::ewkb::with_root_srid(&point, 4326, 32).unwrap();
    let batch = |bytes: &[u8]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::BinaryArray::from(vec![Some(bytes)]))],
        )
        .unwrap()
    };
    let make_input = |last: &[u8]| {
        Box::new(Input {
            schema: schema.clone(),
            batches: VecDeque::from([batch(&point), batch(last)]),
            declared: Some(2),
        }) as Box<dyn BatchStream>
    };
    let cancel = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let error = super::super::prevalidate::input(
        make_input(&bad),
        std::slice::from_ref(&plan),
        &budget,
        &cancel,
        u64::MAX,
        u64::MAX,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(
        (
            error.category,
            error.phase,
            error.remote_effect,
            error.retry
        ),
        (
            plenora_database_core::ErrorCategory::Crs,
            ErrorPhase::Validate,
            plenora_database_core::RemoteEffect::None,
            plenora_database_core::RetryDisposition::Never
        )
    );
    let mut buffered = super::super::prevalidate::input(
        make_input(&point),
        std::slice::from_ref(&plan),
        &budget,
        &cancel,
        u64::MAX,
        u64::MAX,
    )
    .await
    .unwrap();
    assert_eq!(buffered.declared_input_rows(), Some(2));
    assert!(buffered.next_batch(&cancel).await.unwrap().is_some());
    assert!(buffered.next_batch(&cancel).await.unwrap().is_some());
    assert!(buffered.next_batch(&cancel).await.unwrap().is_none());
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 1,
        ..ResourceLimits::default()
    })
    .unwrap();
    let error = super::super::prevalidate::input(
        make_input(&point),
        &[plan],
        &budget,
        &cancel,
        u64::MAX,
        u64::MAX,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
}
