use super::*;
use plenora_database_core::arrow::{DataType, Field, Schema};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ObjectRef, TransactionProfile};
use plenora_database_core::resource::ResourceLimits;
use std::sync::Arc;

#[path = "../../../../tests/support/p1_snapshot.rs"]
mod p1_snapshot;

#[test]
fn p1_adapter_report_baseline() {
    let mut observed = Vec::new();
    for consent in [LossConsent::Deny, LossConsent::Accept] {
        for outcome in ["rollback", "commit", "uncertain", "no_match"] {
            let (mut e, p, b) = setup(consent);
            let result = e.batch(&b, &p);
            let error = result.as_ref().err().map(|e| serde_json::json!({"category": e.category, "phase": e.phase, "message": e.message}));
            if result.is_ok() {
                e.sent(0).unwrap();
                e.applied(
                    0,
                    if outcome == "no_match" {
                        MutationCounts::default()
                    } else {
                        MutationCounts {
                            confirmed: 1,
                            inserted: 1,
                            ..MutationCounts::default()
                        }
                    },
                    &p,
                )
                .unwrap();
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
        "sqlserver_baseline",
        &serde_json::json!(observed),
    );
}

fn column(native: &str, declaration: &str) -> WriteColumnPlan {
    WriteColumnPlan {
        input_index: 0,
        name: "v".into(),
        kind: crate::SqlServerColumnKind::Utf8,
        native_type: native.into(),
        native_declaration: declaration.into(),
        nullable: true,
        collation: Some("Latin1_General_100_CI_AS".into()),
        spatial_srid: None,
        rewrites: vec![],
    }
}
fn exact() -> Classification {
    Classification::Exact(Basis::Exact)
}
fn changed() -> Classification {
    Classification::WouldChange(Basis::Exact)
}
fn conservative() -> Classification {
    Classification::WouldChange(Basis::Conservative)
}
#[test]
fn text_units_code_pages_padding_and_null() {
    for native in ["nvarchar", "nchar", "varchar", "char"] {
        let mut c = column(native, &format!("{native}(4)"));
        for (value, expected) in [
            ("abc", exact()),
            ("abcd", exact()),
            ("abcde", changed()),
            ("abcd ", changed()),
            ("", exact()),
        ] {
            assert_eq!(text(&c, value, None).unwrap(), expected);
        }
        assert_eq!(text(&c, "abc", Some(4)).unwrap(), changed());
        assert_eq!(
            classify(
                RuleId::SsTextLimit,
                &c,
                &StringArray::from(vec![None::<&str>]),
                0
            )
            .unwrap(),
            exact()
        );
        if native.starts_with('n') {
            assert_eq!(text(&c, "ab\u{1f600}", None).unwrap(), exact());
            assert_eq!(text(&c, "abc\u{1f600}", None).unwrap(), changed());
        } else {
            assert_eq!(encoding(&c, "\u{20ac}\u{e9}").unwrap(), exact());
            assert_eq!(text(&c, "\u{20ac}\u{e9}", Some(4)).unwrap(), changed());
            assert_eq!(encoding(&c, "\u{4e2d}").unwrap(), changed());
            assert_eq!(encoding(&c, "\u{81}").unwrap(), changed());
            assert_eq!(text(&c, "\u{4e2d}", None).unwrap(), conservative());
            c.collation = Some("unknown".into());
            assert_eq!(encoding(&c, "\u{4e2d}").unwrap(), conservative());
            assert_eq!(text(&c, "\u{4e2d}", Some(4)).unwrap(), conservative());
            assert_eq!(encoding(&c, "abc\0").unwrap(), exact());
            c.collation = Some("Latin1_General_100_CI_AS_SC_UTF8".into());
            assert_eq!(text(&c, "\u{e9}\u{e9}", None).unwrap(), exact());
            assert_eq!(text(&c, "\u{e9}\u{e9}a", None).unwrap(), changed());
        }
    }
}
#[test]
fn binary_suffixes_are_not_exempt_and_wrong_types_fail() {
    for native in ["binary", "varbinary"] {
        let c = column(native, &format!("{native}(4)"));
        for value in [b"abcd\0".as_slice(), b"abcd ", b"abcde"] {
            assert_eq!(
                classify(
                    RuleId::SsBinaryLimit,
                    &c,
                    &BinaryArray::from(vec![value]),
                    0
                )
                .unwrap(),
                changed()
            );
        }
        assert_eq!(
            classify(
                RuleId::SsBinaryLimit,
                &c,
                &BinaryArray::from(vec![b"abcd".as_slice()]),
                0
            )
            .unwrap(),
            exact()
        );
        assert!(classify(
            RuleId::SsBinaryLimit,
            &c,
            &StringArray::from(vec!["abcde"]),
            0
        )
        .is_err());
        let mut c = c;
        c.rewrites = vec![StoreRewrite::PadBinary { bytes: 4 }];
        assert_eq!(
            classify(
                RuleId::StorePadBinary,
                &c,
                &BinaryArray::from(vec![b"abc".as_slice()]),
                0
            )
            .unwrap(),
            changed()
        );
    }
}
#[test]
fn temporal_integer_predicates_and_offset_parser_boundaries() {
    for (native, declaration, unit) in [
        ("datetime", "datetime", 10_000),
        ("smalldatetime", "smalldatetime", 60_000_000),
        ("datetime2", "datetime2(0)", 1_000_000),
        ("time", "time(3)", 1_000),
    ] {
        let c = column(native, declaration);
        for value in [-unit, 0, unit] {
            assert_eq!(temporal(&c, value), exact());
        }
        for value in [-unit + 1, -1, 1, unit - 1] {
            assert_eq!(temporal(&c, value), changed());
        }
    }
    assert_eq!(temporal(&column("datetime", "datetime"), 3_000), changed());
    for p in [0, 3, 6, 7] {
        let c = column("datetimeoffset", &format!("datetimeoffset({p})"));
        assert_eq!(offset(&c, 0), exact());
        assert_eq!(offset(&c, 1), changed());
        assert_eq!(offset(&c, 10_u32.pow(9 - p)), exact());
    }
    assert!(codec::datetimeoffset("2026-01-01T00:00:00.1234567890+00:00").is_err());
    let c = column("datetimeoffset", "datetimeoffset(3)");
    assert_eq!(
        offset(
            &c,
            codec::datetimeoffset("2026-01-01T00:00:00.120000000-02:00")
                .unwrap()
                .nanosecond()
        ),
        exact()
    );
}
#[test]
fn xml_warning_is_conservative_even_for_empty_text() {
    let c = column("xml", "xml");
    for value in ["", "<a/>"] {
        assert_eq!(
            classify(RuleId::SsXml, &c, &StringArray::from(vec![value]), 0).unwrap(),
            conservative()
        );
    }
    assert_eq!(
        classify(RuleId::SsXml, &c, &StringArray::from(vec![None::<&str>]), 0).unwrap(),
        Classification::Exact(Basis::Conservative)
    );
}

#[test]
fn enumerate_non_exact_sqlserver_rules_for_campaign_values() {
    use plenora_database_core::loss_registry::{Applicability, ClassificationMode};
    let mut audited = Vec::new();
    for &id in RuleId::ALL {
        if !matches!(id.definition().applicability, Applicability::Sqlserver)
            || !matches!(
                id.definition().mode,
                ClassificationMode::Mixed | ClassificationMode::Conservative
            )
        {
            continue;
        }
        audited.push(id);
        let (native, declaration, value, expected) = match id {
            RuleId::SsEncoding => ("varchar", "varchar(4)", "abcd", exact()),
            RuleId::SsTextLimit => ("nvarchar", "nvarchar(4)", "abcd", exact()),
            // La forma vuota XML si serializza diversamente; non e una copia
            // testuale ordinaria. La normalizzazione completa resta opaca.
            RuleId::SsXml => ("xml", "xml", "<a/>", conservative()),
            _ => panic!("regola non esaminata: {id:?}"),
        };
        let c = column(native, declaration);
        let result = classify(id, &c, &StringArray::from(vec![value]), 0).unwrap();
        assert_eq!(result, expected);
        assert_eq!(
            result
                .authorize(MappingPolicy::Compatible, LossConsent::Deny, false)
                .is_ok(),
            id != RuleId::SsXml
        );
    }
    assert_eq!(audited.len(), 3);
}
fn setup(consent: LossConsent) -> (Evaluation, WritePlan, RecordBatch) {
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("dbo".into()),
            object: "b3".into(),
        },
        mode: WriteMode::Create,
        mapping_policy: MappingPolicy::Compatible,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec![],
        update_columns: vec![],
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    let mut plan = WritePlan::compile_create(&operation, schema.clone()).unwrap();
    plan.columns[0] = column("nvarchar", "nvarchar(4)");
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let evaluation = Evaluation::new(&plan, &operation, consent, &budget).unwrap();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["abcd "]))]).unwrap();
    (evaluation, plan, batch)
}
#[test]
fn ledger_deny_rollback_uncertain_and_no_match() {
    let (mut e, p, b) = setup(LossConsent::Deny);
    assert!(e.batch(&b, &p).is_err());
    e.ledger.rollback();
    assert!(e.ledger.report().effective.is_empty());
    assert_eq!(e.ledger.report().evaluation.entries[0].would_change, 1);
    for applied in [false, true] {
        let (mut e, p, b) = setup(LossConsent::Accept);
        e.batch(&b, &p).unwrap();
        e.sent(0).unwrap();
        e.applied(
            0,
            if applied {
                MutationCounts {
                    confirmed: 1,
                    inserted: 1,
                    ..MutationCounts::default()
                }
            } else {
                MutationCounts::default()
            },
            &p,
        )
        .unwrap();
        e.prepare_commit().unwrap();
        assert_eq!(e.confirmed.effective.len(), usize::from(applied));
        assert!(e.ledger.report().effective.is_empty());
        e.ledger.uncertain().unwrap();
        if applied {
            assert_eq!(e.ledger.report().effective[0].uncertain, 1);
        }
    }
}
#[test]
fn ledger_second_batch_and_upsert_unassigned_branch() {
    let (mut e, p, b) = setup(LossConsent::Accept);
    e.batch(&b, &p).unwrap();
    e.sent(0).unwrap();
    e.applied(
        0,
        MutationCounts {
            confirmed: 1,
            inserted: 1,
            ..MutationCounts::default()
        },
        &p,
    )
    .unwrap();
    e.batch(&b, &p).unwrap();
    e.sent(0).unwrap();
    e.operation.update_columns = vec!["other".into()];
    e.applied(
        0,
        MutationCounts {
            confirmed: 1,
            updated: 1,
            ..MutationCounts::default()
        },
        &p,
    )
    .unwrap();
    e.prepare_commit().unwrap();
    assert_eq!(e.confirmed.effective[0].committed, 1);
    assert_eq!(e.confirmed.evaluation.entries[0].checked, 2);
    e.ledger.rollback();
    assert!(e.ledger.report().effective.is_empty());
}
