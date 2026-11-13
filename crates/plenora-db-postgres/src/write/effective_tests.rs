use super::super::canonical::{Leaf, SessionRule};
use super::*;
use arrow_array::TimestampMicrosecondArray;
use arrow_schema::{DataType, Field};

#[path = "../../../../tests/support/p1_snapshot.rs"]
mod p1_snapshot;

#[test]
fn metadata_loss_is_evaluated_in_every_write_mode_before_any_rows() {
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    let base = plenora_database_core::protocol::contract_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("owner", DataType::Utf8, true),
    ])
    .unwrap();
    let mut metadata = base.metadata().clone();
    metadata.insert("opaque", "x".repeat(crate::column_metadata::COLUMN_LIMIT));
    let schema = std::sync::Arc::new(arrow_schema::Schema::new_with_metadata(
        base.fields().clone(),
        metadata,
    ));
    for mode in [
        WriteMode::Create,
        WriteMode::Append,
        WriteMode::Replace,
        WriteMode::TruncateInsert,
        WriteMode::Update,
        WriteMode::Upsert,
        WriteMode::DeleteByKeys,
    ] {
        for policy in [
            MappingPolicy::Strict,
            MappingPolicy::Compatible,
            MappingPolicy::Lossy,
            MappingPolicy::Native,
        ] {
            let mut operation = crate::write_visibility_live_tests::operation("target", mode);
            operation.mapping_policy = policy;
            let prepared = super::super::prepare_state(&schema, &operation).unwrap();
            for consent in [LossConsent::Deny, LossConsent::Accept] {
                let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
                let mut evaluation = Evaluation::new(
                    prepared.loss_instances.clone(),
                    &operation,
                    consent,
                    &budget,
                )
                .unwrap();
                assert!(evaluation.ledger.report().evaluation.entries.is_empty());
                let result = evaluation.metadata(&prepared.metadata_plan, &schema, &operation);
                let allowed = mode == WriteMode::DeleteByKeys
                    || (policy != MappingPolicy::Strict && consent.accepted());
                assert_eq!(result.is_ok(), allowed, "{mode:?}/{policy:?}/{consent:?}");
                assert!(evaluation.ledger.report().effective.is_empty());
                if allowed && mode != WriteMode::DeleteByKeys {
                    evaluation.sent(0).unwrap();
                    evaluation.applied(0, 1).unwrap();
                    evaluation.ledger.complete();
                    evaluation.prepare_commit().unwrap();
                    assert_eq!(evaluation.confirmed.effective.len(), 1);
                    assert_eq!(evaluation.confirmed.effective[0].committed, 1);
                }
                evaluation.ledger.rollback();
                assert!(evaluation.ledger.report().effective.is_empty());
            }
        }
    }
}

#[test]
fn p1_adapter_report_baseline() {
    use plenora_database_core::loss_registry::RuleParameters;
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use std::sync::Arc;
    let mut observed = Vec::new();
    for consent in [LossConsent::Deny, LossConsent::Accept] {
        for outcome in ["rollback", "commit", "uncertain", "no_match"] {
            let p = plan(LeafClass::Padded { chars: 4 }, SessionShape::Scalar);
            let mut op = crate::write_visibility_live_tests::operation("target", WriteMode::Append);
            op.mapping_policy = MappingPolicy::Compatible;
            let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
            let mut e = Evaluation::new(
                vec![RuleInstance {
                    subject: None,
                    field_id: 0,
                    rule_id: RuleId::PgPadText,
                    parameters: RuleParameters::default(),
                    source_type: None,
                    target_type: None,
                }],
                &op,
                consent,
                &budget,
            )
            .unwrap();
            let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "v",
                DataType::Utf8,
                true,
            )]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
            let result = e.batch(&batch, &[p], &op);
            let error = result.as_ref().err().map(|e| serde_json::json!({"category": e.category, "phase": e.phase, "message": e.message}));
            if result.is_ok() {
                e.sent(0).unwrap();
                e.applied(0, u64::from(outcome != "no_match")).unwrap();
                e.prepare_commit().unwrap();
            }
            let preview = e.confirmed.clone();
            if result.is_err() || outcome == "rollback" {
                e.ledger.rollback();
            } else if outcome == "uncertain" {
                e.ledger.uncertain().unwrap();
            } else {
                e.ledger.commit().unwrap();
            }
            observed.push(
                serde_json::json!({"consent": format!("{consent:?}"), "outcome": outcome,
                "error": error, "preview": preview, "report": e.ledger.report()}),
            );
        }
    }
    p1_snapshot::check(
        std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../..")),
        "postgres_baseline",
        &serde_json::json!(observed),
    );
}

#[test]
fn jsonb_numbers_preserve_decimal_precision_without_consent() {
    use plenora_database_core::store_rewrite::StoreRewrite;
    let mut p = plan(LeafClass::Jsonb, SessionShape::Scalar);
    p.store.rewrites = vec![StoreRewrite::JsonNumbers];
    for (text, changed) in [
        ("1.5", false),
        ("1.0", false),
        ("123456789012345678901234567890.123456789", false),
        (r#"{"a":[1.5,12345678901234567890],"b":"-0 1e2"}"#, false),
        ("-0", true),
        ("-0.000", true),
        ("1e2", true),
        ("1E-2", true),
        ("-0.001", false),
        ("1e400", true),
    ] {
        let a = StringArray::from(vec![text]);
        let result = classify(RuleId::StoreJsonNumbers, &p, &a, 0).unwrap();
        assert_eq!(result, verdict(changed, Basis::Exact), "{text}");
        assert_eq!(
            result
                .authorize(MappingPolicy::Compatible, LossConsent::Deny, false)
                .is_err(),
            changed
        );
    }
    assert!(json_number_changes("[1,]").is_err());
}

// Enumerare anche i rami conservativi impedisce che una nuova regola entri
// senza una decisione sul caso ordinario. I rami non raggiungibili dal
// profilo ammesso non diventano capability per somiglianza.
#[test]
fn enumerate_non_exact_registry_rules_against_ordinary_postgres_values() {
    use plenora_database_core::loss_registry::{Applicability, ClassificationMode};
    use plenora_database_core::store_rewrite::StoreRewrite;
    let mut audited = Vec::new();
    for &id in RuleId::ALL {
        if matches!(id.definition().applicability, Applicability::Sqlserver)
            || !matches!(
                id.definition().mode,
                ClassificationMode::Mixed | ClassificationMode::Conservative
            )
        {
            continue;
        }
        audited.push(id);
        let sample = match id {
            RuleId::PgGeometryRepresentation => {
                let mut bytes = vec![1_u8];
                bytes.extend_from_slice(&1_u32.to_le_bytes());
                bytes.extend_from_slice(&[0; 16]);
                let mut p = plan(LeafClass::Jsonb, SessionShape::Scalar);
                p.encoding = Some("wkb".to_owned()); p.target_srid = None;
                let array = arrow_array::BinaryArray::from(vec![Some(bytes.as_slice())]);
                assert_eq!(classify(id, &p, &array, 0).unwrap(), Classification::Exact(Basis::Exact));
                bytes[0] = 0; bytes[1..5].copy_from_slice(&1_u32.to_be_bytes());
                let array = arrow_array::BinaryArray::from(vec![Some(bytes.as_slice())]);
                assert_eq!(classify(id, &p, &array, 0).unwrap(), Classification::WouldChange(Basis::Conservative));
                None
            }
            RuleId::PgJsonb => Some((LeafClass::Jsonb, "{\"a\":1.5}")),
            RuleId::PgNumeric => Some((LeafClass::Numeric { precision: 5, scale: 2 }, "1.50")),
            RuleId::PgTemporal => Some((LeafClass::Session { leaf: SessionLeaf::Timestamp, fraction: 3, reduced: true }, "1970-01-01 00:00:00.001000")),
            RuleId::StoreJsonNumbers => Some((LeafClass::Jsonb, "1.50")),
            // Schema mismatch / absent columns / conflicting spatial identity
            // are not ordinary equivalent copies (selector tested separately).
            RuleId::PgArrowMismatch | RuleId::PgMissingColumn | RuleId::PgCrs | RuleId::PgSrid
            // No admitted PG pair emits these rewrites; retain closed fallback.
            | RuleId::StoreEvery | RuleId::StoreExcessSpaces | RuleId::StoreNonAscii
            // PostgreSQL canonicalizes this alias to PgPadText (exact).
            | RuleId::StorePadText => None,
            _ => panic!("regola non esaminata: {id:?}"),
        };
        if let Some((class, text)) = sample {
            for shape in [SessionShape::Scalar, SessionShape::Array] {
                if id == RuleId::StoreJsonNumbers && shape == SessionShape::Array {
                    continue;
                }
                let mut p = plan(class, shape);
                if id == RuleId::StoreJsonNumbers {
                    p.store.rewrites = vec![StoreRewrite::JsonNumbers];
                }
                let value = if shape == SessionShape::Array {
                    format!("{{\"{}\",NULL}}", text.replace('"', "\\\""))
                } else {
                    text.to_owned()
                };
                let a = StringArray::from(vec![value]);
                let result = classify(id, &p, &a, 0).unwrap();
                assert_eq!(result, verdict(false, Basis::Exact), "{id:?}/{shape:?}");
                result
                    .authorize(MappingPolicy::Compatible, LossConsent::Deny, false)
                    .unwrap();
            }
        }
    }
    assert_eq!(audited.len(), 13);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/ammissione_scrittura/postgres.json"
    ))
    .unwrap();
    for server in fixture["misure"].as_array().unwrap() {
        for pair in server["chiavi"].as_array().unwrap() {
            for rewrite in pair["riscritture"].as_array().unwrap() {
                assert_eq!(
                    rewrite["classe"], "json_numeri",
                    "aggiornare l'audit dei predicati"
                );
            }
        }
    }
}

fn plan(class: LeafClass, shape: SessionShape) -> WriteColumnPlan {
    let mut plan = WriteColumnPlan::compile(&Field::new("v", DataType::Utf8, true)).unwrap();
    plan.session_rule = Some(SessionRule {
        leaves: vec![Leaf { class, shape }],
        strict: false,
        key: false,
    });
    plan
}

#[test]
fn batches_without_loss_rules_still_validate_values_without_a_lease() {
    use arrow_schema::Schema;
    use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLimits};
    use std::sync::Arc;

    let p = plan(LeafClass::Jsonb, SessionShape::Scalar);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    let valid = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["{}", "null"]))],
    )
    .unwrap();
    let invalid =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["{"]))]).unwrap();
    let empty = RecordBatch::new_empty(schema);
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let available = budget.remaining(ResourceKind::MemoryBytes);
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        let mut op = crate::write_visibility_live_tests::operation("target", WriteMode::Append);
        op.mapping_policy = policy;
        let mut evaluation = Evaluation::new(vec![], &op, LossConsent::Deny, &budget).unwrap();
        for batch in [&empty, &valid, &empty] {
            assert!(!evaluation
                .batch(batch, std::slice::from_ref(&p), &op)
                .unwrap());
            assert!(evaluation.lease.is_none());
            assert_eq!(budget.remaining(ResourceKind::MemoryBytes), available);
            for row in 0..batch.num_rows() {
                evaluation.sent(row).unwrap();
                evaluation.applied(row, 1).unwrap();
            }
        }
        let error = evaluation
            .batch(&invalid, std::slice::from_ref(&p), &op)
            .unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::DataMapping
        );
    }
}

#[test]
fn empty_batch_with_rules_releases_previous_lease_and_can_resume() {
    use arrow_schema::Schema;
    use plenora_database_core::loss_registry::RuleParameters;
    use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLimits};
    use std::sync::Arc;

    let p = plan(LeafClass::Jsonb, SessionShape::Scalar);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["{}"]))],
    )
    .unwrap();
    let empty = RecordBatch::new_empty(schema);
    let op = crate::write_visibility_live_tests::operation("target", WriteMode::Append);
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let available = budget.remaining(ResourceKind::MemoryBytes);
    let mut evaluation = Evaluation::new(
        vec![RuleInstance {
            subject: None,
            field_id: 0,
            rule_id: RuleId::PgJsonb,
            parameters: RuleParameters::default(),
            source_type: None,
            target_type: None,
        }],
        &op,
        LossConsent::Deny,
        &budget,
    )
    .unwrap();
    for current in [&empty, &batch, &empty, &batch] {
        assert!(!evaluation
            .batch(current, std::slice::from_ref(&p), &op)
            .unwrap());
        if current.num_rows() == 0 {
            assert!(evaluation.lease.is_none());
            assert_eq!(budget.remaining(ResourceKind::MemoryBytes), available);
        } else {
            assert!(evaluation.lease.is_some());
            assert!(budget.remaining(ResourceKind::MemoryBytes) < available);
            evaluation.sent(0).unwrap();
            evaluation.applied(0, 1).unwrap();
        }
    }
    evaluation.ledger.complete();
    evaluation.prepare_commit().unwrap();
    assert_eq!(evaluation.confirmed.evaluation.entries[0].checked, 2);
    drop(evaluation);
    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), available);
}

#[test]
fn decimal_exact_rounding_and_precision_without_float() {
    for (text, p, s, changed) in [
        ("1.2300", 5, 2, false),
        ("1.2301", 5, 2, true),
        ("1.005", 5, 2, true),
        ("-1.005", 5, 2, true),
        ("1e2", 5, 2, false),
        ("-0", 5, 2, false),
        ("1200", 2, -2, false),
        ("1250", 2, -2, true),
        ("0.00001", 2, 2, true),
        ("9.99e-100000", 2, 2, true),
        ("0e999999", 2, 2, false),
        (".50", 2, 2, false),
    ] {
        assert_eq!(
            numeric(text, p, s).unwrap(),
            verdict(changed, Basis::Exact),
            "{text}"
        );
    }
    for text in [
        "999.995", "-999.995", "1e999999", "--1", "1e", ".", "1..0", "1e2e3",
    ] {
        assert!(numeric(text, 5, 2).is_err(), "{text}");
    }
    assert_eq!(
        numeric("NaN", 5, 2).unwrap(),
        verdict(true, Basis::Conservative)
    );
}

#[test]
fn unicode_text_never_truncates_and_padding_is_per_outer_cell() {
    let p = plan(LeafClass::Padded { chars: 2 }, SessionShape::Scalar);
    let a = StringArray::from(vec![Some("😀é"), Some("a"), Some(""), Some("ab "), None]);
    for (row, changed) in [(0, false), (1, true), (2, true), (4, false)] {
        assert_eq!(
            classify(RuleId::PgPadText, &p, &a, row).unwrap(),
            verdict(changed, Basis::Exact)
        );
    }
    for consent in [LossConsent::Deny, LossConsent::Accept] {
        assert!(classify(RuleId::PgTextLimit, &p, &a, 3).is_err());
        assert!(Classification::HardReject
            .authorize(MappingPolicy::Lossy, consent, false)
            .is_err());
    }
    let mut builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    builder.values().append_value("a");
    builder.values().append_value("b");
    builder.values().append_null();
    builder.append(true);
    let list = builder.finish();
    let p = plan(LeafClass::Padded { chars: 2 }, SessionShape::Array);
    assert_eq!(
        classify(RuleId::PgPadText, &p, &list, 0).unwrap(),
        verdict(true, Basis::Exact)
    );
}

#[test]
fn json_validates_entire_document_and_decoded_duplicate_keys() {
    let p = plan(LeafClass::Jsonb, SessionShape::Scalar);
    for (text, duplicate) in [
        (r#"{"a":1,"b":2}"#, false),
        (r#"{"a":1,"\u0061":1}"#, true),
        (r#"[{"x":{"a":1,"a":2}}]"#, true),
        (r#" { "b":2, "a":1 } "#, false),
    ] {
        let a = StringArray::from(vec![text]);
        assert_eq!(
            classify(RuleId::PgJsonb, &p, &a, 0).unwrap(),
            verdict(duplicate, Basis::Exact)
        );
    }
    for text in ["{", "true false", r#"{"a":1,"a":2,}"#, "[1,]"] {
        assert!(classify(RuleId::PgJsonb, &p, &StringArray::from(vec![text]), 0).is_err());
    }
    let p = plan(LeafClass::Jsonb, SessionShape::Nested);
    assert_eq!(
        classify(RuleId::PgJsonb, &p, &StringArray::from(vec!["opaque"]), 0).unwrap(),
        verdict(true, Basis::Conservative)
    );
}

#[test]
fn temporal_integer_precision_negative_epoch_and_trailing_zeroes() {
    let a = TimestampMicrosecondArray::from(vec![0, -1, -1000, 999_999, 1_000_000]);
    for (row, changed) in [(0, false), (1, true), (2, false), (3, true), (4, false)] {
        assert_eq!(temporal(&a, row, 3).unwrap(), Some(changed));
    }
    assert!(!fractional_text("2026-01-01 00:00:00.123000+00", 3));
    assert!(fractional_text("2026-01-01 00:00:00.123001+00", 3));
    assert!(!fractional_text("00:00:00.000000", 0));
}

#[test]
fn conservative_and_information_selectors_and_nulls() {
    let p = plan(LeafClass::Rounding, SessionShape::Scalar);
    let a = StringArray::from(vec![Some("1.5"), None]);
    for rule in [
        RuleId::PgArrowMismatch,
        RuleId::PgMissingColumn,
        RuleId::PgSrid,
        RuleId::PgCrs,
        RuleId::PgNumeric,
    ] {
        assert_eq!(
            classify(rule, &p, &a, 0).unwrap(),
            verdict(true, Basis::Conservative)
        );
        assert!(matches!(
            classify(rule, &p, &a, 1).unwrap(),
            Classification::Exact(_)
        ));
    }
    for rule in [RuleId::PgNullability, RuleId::PgAddNullable] {
        assert_eq!(
            classify(rule, &p, &a, 0).unwrap(),
            verdict(false, Basis::Exact)
        );
    }
}

#[test]
fn batch_consent_rollback_no_match_and_unused_columns() {
    use arrow_schema::Schema;
    use plenora_database_core::loss_registry::RuleParameters;
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use std::sync::Arc;
    let p = plan(LeafClass::Jsonb, SessionShape::Scalar);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    let exact = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![r#"{"a":1}"#]))],
    )
    .unwrap();
    let changed = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![r#"{"a":1,"a":2}"#]))],
    )
    .unwrap();
    let mut op = crate::write_visibility_live_tests::operation("target", WriteMode::Append);
    op.mapping_policy = MappingPolicy::Compatible;
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let rules = vec![RuleInstance {
        subject: None,
        field_id: 0,
        rule_id: RuleId::PgJsonb,
        parameters: RuleParameters::default(),
        source_type: None,
        target_type: None,
    }];
    let mut evaluation = Evaluation::new(rules.clone(), &op, LossConsent::Deny, &budget).unwrap();
    assert!(!evaluation
        .batch(&exact, std::slice::from_ref(&p), &op)
        .unwrap());
    evaluation.sent(0).unwrap();
    evaluation.applied(0, 1).unwrap();
    assert!(evaluation
        .batch(&changed, std::slice::from_ref(&p), &op)
        .is_err());
    evaluation.ledger.rollback();
    let report = evaluation.ledger.report();
    assert!(report.effective.is_empty());
    assert_eq!(report.evaluation.entries[0].checked, 2);
    assert_eq!(report.evaluation.entries[0].would_change, 1);
    assert!(!report.evaluation.complete);
    let mut evaluation = Evaluation::new(rules.clone(), &op, LossConsent::Accept, &budget).unwrap();
    assert!(evaluation
        .batch(&changed, std::slice::from_ref(&p), &op)
        .unwrap());
    evaluation.sent(0).unwrap();
    evaluation.applied(0, 0).unwrap();
    evaluation.ledger.complete();
    evaluation.prepare_commit().unwrap();
    assert!(evaluation.confirmed.effective.is_empty());
    let mut committed = Evaluation::new(rules.clone(), &op, LossConsent::Accept, &budget).unwrap();
    committed
        .batch(&changed, std::slice::from_ref(&p), &op)
        .unwrap();
    committed.sent(0).unwrap();
    committed.applied(0, 1).unwrap();
    committed.ledger.complete();
    committed.prepare_commit().unwrap();
    assert_eq!(committed.confirmed.effective[0].committed, 1);
    assert_eq!(committed.confirmed.effective[0].uncertain, 0);
    assert!(committed.ledger.report().effective.is_empty());
    committed.ledger.uncertain().unwrap();
    assert_eq!(committed.ledger.report().effective[0].uncertain, 1);
    op.mode = WriteMode::DeleteByKeys;
    op.keys.clear();
    let mut evaluation = Evaluation::new(rules, &op, LossConsent::Deny, &budget).unwrap();
    assert!(!evaluation.batch(&changed, &[p], &op).unwrap());
    assert!(evaluation.ledger.report().evaluation.entries.is_empty());
}

#[test]
fn postgres_alias_conflicts_and_shared_truncation_are_rejected() {
    use plenora_database_core::loss_registry::RuleParameters;
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use plenora_database_core::store_rewrite::StoreRewrite;
    let mut p = plan(LeafClass::Padded { chars: 3 }, SessionShape::Scalar);
    p.store.rewrites = vec![StoreRewrite::PadText { chars: 4 }];
    let op = crate::write_visibility_live_tests::operation("target", WriteMode::Append);
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let mut evaluation = Evaluation::new(
        vec![RuleInstance {
            subject: None,
            field_id: 0,
            rule_id: RuleId::PgPadText,
            parameters: RuleParameters::default(),
            source_type: None,
            target_type: None,
        }],
        &op,
        LossConsent::Accept,
        &budget,
    )
    .unwrap();
    assert!(evaluation.bind(std::slice::from_ref(&p)).is_err());
    p.store.rewrites = vec![StoreRewrite::PadText { chars: 3 }];
    evaluation.bind(std::slice::from_ref(&p)).unwrap();
    assert_eq!(evaluation.rules[0].parameters.length, Some(3));
    p.store.rewrites = vec![StoreRewrite::ExcessSpaces { bytes: 3 }];
    let values = StringArray::from(vec!["abc "]);
    assert!(classify(RuleId::StoreExcessSpaces, &p, &values, 0).is_err());
}

#[test]
fn non_atomic_profiles_cannot_require_late_consent() {
    let mut op = crate::write_visibility_live_tests::operation("target", WriteMode::Append);
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        op.mapping_policy = policy;
        for transaction in [
            TransactionProfile::ChunkCommitted,
            TransactionProfile::BestEffortDdl,
        ] {
            op.transaction_profile = transaction;
            let error = profile(&op, LossConsent::Deny).unwrap_err();
            assert_eq!(
                error.category,
                plenora_database_core::ErrorCategory::Unsupported
            );
            assert_eq!(error.phase, ErrorPhase::Prepare);
            assert_eq!(
                error.remote_effect,
                plenora_database_core::RemoteEffect::None
            );
        }
    }
}
