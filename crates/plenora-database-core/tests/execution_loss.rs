//! Invarianti del rapporto e macchina contabile, senza server.
use plenora_database_core::execution_loss::{
    Basis, ExecutionLossReport, RiskEntry, VersionedLossReport,
};
use plenora_database_core::loss::{LossConsent, MappingPolicy};
use plenora_database_core::loss_ledger::{Classification, LossLedger};
use plenora_database_core::loss_registry::RuleId;

fn plan() -> ExecutionLossReport {
    ExecutionLossReport::planned(
        MappingPolicy::Compatible,
        vec![RiskEntry::new(
            0,
            RuleId::StoreNegativeZero,
            MappingPolicy::Compatible,
            None,
            None,
        )],
    )
    .unwrap()
}
fn ledger() -> LossLedger {
    LossLedger::new(plan(), 16384, 20).unwrap()
}
fn observe(ledger: &mut LossLedger, row: u64) {
    ledger
        .observe(
            row,
            0,
            RuleId::StoreNegativeZero,
            Classification::WouldChange(Basis::Exact),
        )
        .unwrap();
}
#[test]
fn inspection_encoding_binding_and_replayed_batches_do_not_double_count() {
    let mut l = ledger();
    l.begin_batch(0).unwrap();
    for _ in 0..3 {
        observe(&mut l, 0);
    }
    l.begin_batch(0).unwrap();
    observe(&mut l, 0);
    l.sent(0).unwrap();
    l.applied(0).unwrap();
    assert!(l.report().effective.is_empty());
    l.begin_batch(1).unwrap();
    observe(&mut l, 0);
    l.sent(0).unwrap();
    l.applied(0).unwrap();
    assert!(l.begin_batch(0).is_err());
    l.complete();
    l.commit().unwrap();
    let report = l.report();
    report.validate().unwrap();
    assert_eq!(report.evaluation.entries[0].checked, 2);
    assert_eq!(report.effective[0].committed, 2);
    assert!(l.begin_batch(1).is_err());
}
#[test]
fn rollback_preserves_evaluation_and_never_claims_committed_loss() {
    let mut l = ledger();
    l.begin_batch(0).unwrap();
    observe(&mut l, 0);
    l.sent(0).unwrap();
    l.applied(0).unwrap();
    l.begin_batch(1).unwrap();
    observe(&mut l, 0);
    l.rollback();
    let report = l.report();
    report.validate().unwrap();
    assert_eq!(report.evaluation.entries[0].would_change, 2);
    assert!(report.effective.is_empty());
    assert!(!report.evaluation.complete);
}
#[test]
fn uncertain_only_counts_sent_not_merely_checked_or_savepoint_discarded() {
    let mut l = ledger();
    l.begin_batch(0).unwrap();
    for row in 0..4 {
        observe(&mut l, row);
    }
    l.sent(0).unwrap();
    l.applied(0).unwrap();
    l.sent(1).unwrap();
    l.sent(2).unwrap();
    l.applied(2).unwrap();
    l.discard(2).unwrap();
    assert!(l.commit().is_err());
    l.uncertain().unwrap();
    let report = l.report();
    report.validate().unwrap();
    assert_eq!(report.evaluation.entries[0].checked, 4);
    assert_eq!(report.effective[0].uncertain, 2);
    assert_eq!(report.effective[0].committed, 0);
}
#[test]
fn partial_commits_survive_rollback_of_later_unit() {
    let mut l = ledger();
    l.begin_batch(0).unwrap();
    observe(&mut l, 0);
    l.sent(0).unwrap();
    l.applied(0).unwrap();
    l.commit().unwrap();
    l.begin_batch(1).unwrap();
    observe(&mut l, 0);
    l.sent(0).unwrap();
    l.applied(0).unwrap();
    l.rollback();
    assert_eq!(l.report().effective[0].committed, 1);
    assert_eq!(l.report().evaluation.entries[0].checked, 2);
}
#[test]
fn no_match_and_rejected_rows_are_not_effective() {
    let mut l = ledger();
    l.begin_batch(0).unwrap();
    observe(&mut l, 0);
    l.sent(0).unwrap();
    l.discard(0).unwrap();
    observe(&mut l, 1);
    l.discard(1).unwrap();
    l.complete();
    l.commit().unwrap();
    assert!(l.report().effective.is_empty());
}
#[test]
fn consent_is_never_derived_from_policy_or_severity() {
    assert_eq!(LossConsent::default(), LossConsent::Deny);
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for consent in [LossConsent::Deny, LossConsent::Accept] {
            assert!(Classification::Exact(Basis::Exact)
                .authorize(policy, consent, false)
                .is_ok());
            for basis in [Basis::Exact, Basis::Conservative] {
                assert_eq!(
                    Classification::WouldChange(basis)
                        .authorize(policy, consent, false)
                        .is_ok(),
                    policy != MappingPolicy::Strict && consent == LossConsent::Accept
                );
                assert!(Classification::WouldChange(basis)
                    .authorize(policy, consent, true)
                    .is_err());
            }
            assert!(Classification::HardReject
                .authorize(policy, consent, false)
                .is_err());
        }
    }
}
#[test]
fn limits_reserved_before_input_and_transport_includes_escaping() {
    for (rule, max) in [(RuleId::StoreNegativeZero, 4096), (RuleId::PgNumeric, 2048)] {
        for count in [max, max + 1] {
            let risk = (0..count)
                .map(|id| RiskEntry::new(id, rule, MappingPolicy::Compatible, None, None))
                .collect();
            assert_eq!(
                ExecutionLossReport::planned(MappingPolicy::Compatible, risk).is_ok(),
                count == max
            );
        }
    }
    let mut p = plan();
    p.risk[0].source_type = Some("\"\\\n".repeat(170));
    assert!(p.validate_transport_budget(1).is_err());
    assert!(p.validate_transport_budget(16384).is_ok());
    let mut l = LossLedger::new(plan(), 16384, 1).unwrap();
    l.begin_batch(0).unwrap();
    observe(&mut l, 0);
    assert!(l
        .observe(
            1,
            0,
            RuleId::StoreNegativeZero,
            Classification::Exact(Basis::Exact)
        )
        .is_err());
}
#[test]
fn shared_rust_json_acceptance_vectors_and_legacy_discriminator() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("execution_loss_validation.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let accepted = serde_json::from_value::<ExecutionLossReport>(case["document"].clone())
            .is_ok_and(|r| r.validate().is_ok());
        assert_eq!(
            accepted,
            case["valid"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
    let legacy: VersionedLossReport =
        serde_json::from_str(r#"{"schema_version":2,"policy":"strict","losses":[]}"#).unwrap();
    assert!(matches!(legacy, VersionedLossReport::Legacy(_)));
    assert!(serde_json::from_value::<VersionedLossReport>(serde_json::Value::Null).is_err());
    assert!(serde_json::from_value::<VersionedLossReport>(serde_json::json!({})).is_err());
}

#[test]
fn alias_compilation_and_outer_cell_count_are_canonical() {
    use plenora_database_core::loss_registry::{compile_instances, RuleInstance, RuleParameters};
    use plenora_database_core::plan::ProviderKind;
    let first = RuleInstance {
        field_id: 1,
        rule_id: RuleId::PgPadText,
        parameters: RuleParameters {
            length: Some(4),
            ..RuleParameters::default()
        },
        source_type: None,
        target_type: None,
    };
    let mut alias = first.clone();
    alias.rule_id = RuleId::StorePadText;
    alias.source_type = Some("Utf8".into());
    let (controls, report) = compile_instances(
        ProviderKind::Postgres,
        MappingPolicy::Compatible,
        [first, alias.clone()],
    )
    .unwrap();
    assert_eq!(controls.len(), 1);
    assert_eq!(controls[0].source_type.as_deref(), Some("Utf8"));
    assert_eq!(report.risk.len(), 1);
    let mut l = LossLedger::new(report, 16384, 10).unwrap();
    l.begin_batch(0).unwrap();
    for _ in 0..4 {
        l.observe(
            0,
            1,
            controls[0].rule_id,
            Classification::WouldChange(Basis::Exact),
        )
        .unwrap();
    }
    assert!(l
        .observe(
            0,
            1,
            controls[0].rule_id,
            Classification::WouldChange(Basis::Conservative)
        )
        .is_err());
    l.sent(0).unwrap();
    l.applied(0).unwrap();
    l.commit().unwrap();
    assert_eq!(l.report().effective[0].committed, 1);
    alias.parameters.length = Some(5);
    assert!(compile_instances(
        ProviderKind::Postgres,
        MappingPolicy::Compatible,
        [controls[0].clone(), alias]
    )
    .is_err());
}

#[test]
fn serializer_orders_numeric_fields_rule_ids_and_bases() {
    use plenora_database_core::execution_loss::EvaluationEntry;
    let mut report = ExecutionLossReport::planned(
        MappingPolicy::Compatible,
        vec![
            RiskEntry::new(10, RuleId::PgNumeric, MappingPolicy::Compatible, None, None),
            RiskEntry::new(2, RuleId::PgTemporal, MappingPolicy::Compatible, None, None),
            RiskEntry::new(2, RuleId::PgNumeric, MappingPolicy::Compatible, None, None),
        ],
    )
    .unwrap();
    report.risk.reverse();
    for basis in [Basis::Conservative, Basis::Exact] {
        report.evaluation.entries.push(EvaluationEntry {
            field_id: 2,
            rule_id: RuleId::PgNumeric,
            basis,
            checked: 1,
            would_change: 0,
        });
    }
    let value = serde_json::to_value(report).unwrap();
    assert_eq!(value["risk"][0]["field_id"], 2);
    assert_eq!(value["risk"][0]["rule_id"], "pg.numeric");
    assert_eq!(value["risk"][2]["field_id"], 10);
    assert_eq!(value["evaluation"]["entries"][0]["basis"], "exact");
}
#[test]
fn migrated_attachment_cannot_invent_execution_and_preserves_error_axes() {
    use plenora_database_core::{
        prepared_execution, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect,
    };
    let outcome: plenora_database_core::outcome::WriteOutcome = serde_json::from_str(include_str!(
        "../../../contracts/v2/examples/outcome-committed.json"
    ))
    .unwrap();
    let missing = prepared_execution::attach_execution(plan(), Ok(outcome)).unwrap_err();
    assert_eq!(missing.category, ErrorCategory::Internal);
    assert_eq!(missing.remote_effect, RemoteEffect::Committed);
    let mut error = DatabaseError::new(
        ErrorCategory::Execution,
        ErrorPhase::Write,
        None,
        "errore senza payload",
    );
    error.remote_effect = RemoteEffect::RolledBack;
    let error = prepared_execution::attach_execution(plan(), Err(error)).unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    let Some(VersionedLossReport::Execution(report)) = error.loss_report.as_deref() else {
        panic!("v3 required");
    };
    assert!(!report.evaluation.complete);
    assert!(report.evaluation.entries.is_empty());
    assert!(report.effective.is_empty());
}
#[test]
fn unsettled_error_and_outcome_share_the_execution_report() {
    let mut outcome: plenora_database_core::outcome::WriteOutcome = serde_json::from_str(
        include_str!("../../../contracts/v2/examples/outcome-unknown.json"),
    )
    .unwrap();
    outcome.schema_version = 3;
    outcome.loss_report = Some(plan().into());
    let document = outcome.settle().unwrap_err().public_document().unwrap();
    assert_eq!(
        document["details"]["loss_report"],
        document["details"]["write_outcome"]["loss_report"]
    );
}

#[test]
fn transaction_finalizer_covers_all_writer_paths_and_terminal_axes() {
    use plenora_database_core::outcome::{RowCounts, WriteOutcome, WriteStatus};
    use plenora_database_core::plan::ProviderKind;
    use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect};
    for path in [
        "sqlserver prepared",
        "sqlserver tds bulk",
        "sqlserver staging",
        "sqlserver row diagnostics",
        "postgres copy",
        "postgres prepared",
        "postgres staging",
        "postgres row diagnostics",
    ] {
        for effect in [
            RemoteEffect::None,
            RemoteEffect::RolledBack,
            RemoteEffect::Committed,
            RemoteEffect::Unknown,
            RemoteEffect::Partial,
        ] {
            for as_error in [false, true] {
                let mut l = ledger();
                l.begin_batch(0).unwrap();
                for row in 0..4 {
                    observe(&mut l, row);
                }
                l.sent(0).unwrap();
                l.applied(0).unwrap();
                l.sent(1).unwrap();
                l.applied(1).unwrap();
                l.discard(1).unwrap(); // no-match o savepoint annullato
                                       // Una preview non prova che COMMIT sia arrivato al server.
                let mut preview = l.clone();
                preview.complete();
                preview.commit().unwrap();
                if effect == RemoteEffect::Unknown {
                    l.sent(2).unwrap();
                }
                let result = if as_error || effect == RemoteEffect::None {
                    let mut error = DatabaseError::new(
                        ErrorCategory::DataMapping,
                        ErrorPhase::Write,
                        None,
                        "write failed",
                    );
                    error.remote_effect = effect;
                    Err(error)
                } else {
                    Ok(WriteOutcome {
                        schema_version: 2,
                        loss_report: None,
                        status: match effect {
                            RemoteEffect::RolledBack => WriteStatus::RolledBack,
                            RemoteEffect::Committed => WriteStatus::Committed,
                            RemoteEffect::Partial => WriteStatus::PartiallyCommitted,
                            _ => WriteStatus::OutcomeUnknown,
                        },
                        execution_id: path.into(),
                        provider: ProviderKind::Sqlserver,
                        rows: RowCounts {
                            received: 4,
                            confirmed: 0,
                            inserted: None,
                            updated: None,
                            deleted: None,
                            failed: 0,
                            skipped: 0,
                        },
                        recovery: None,
                    })
                };
                let r = l.finalize(&result, preview.report()).unwrap();
                r.validate().unwrap();
                assert_eq!(r.evaluation.entries[0].would_change, 4, "{path}");
                match effect {
                    RemoteEffect::None | RemoteEffect::RolledBack => {
                        assert!(r.effective.is_empty(), "{path}");
                    }
                    RemoteEffect::Committed => {
                        assert_eq!(r.effective[0].committed, 1, "{path}");
                        assert_eq!(r.effective[0].uncertain, 0);
                    }
                    _ => {
                        assert_eq!(r.effective[0].committed, 0, "{path}");
                        assert_eq!(
                            r.effective[0].uncertain,
                            if effect == RemoteEffect::Unknown {
                                2
                            } else {
                                1
                            }
                        );
                    }
                }
            }
        }
    }
}
