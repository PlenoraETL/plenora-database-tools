//! Prove comuni: identita, attribuzione, budget, preview e consenso.
use plenora_database_core::execution_loss::Basis;
use plenora_database_core::execution_loss_context::ExecutionLossContext;
use plenora_database_core::loss::{LossConsent, MappingPolicy};
use plenora_database_core::loss_ledger::Classification;
use plenora_database_core::loss_registry::{RuleId, RuleInstance, RuleParameters};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLimits};
use plenora_database_core::ErrorCategory;

fn rule(field_id: u32) -> RuleInstance {
    RuleInstance {
        subject: None,
        field_id,
        rule_id: RuleId::MysqlNegativeZero,
        parameters: RuleParameters::default(),
        source_type: None,
        target_type: None,
    }
}
fn budget(memory_bytes: u64) -> ResourceBudget {
    ResourceBudget::new(ResourceLimits {
        memory_bytes,
        cell_bytes: 1,
        ..ResourceLimits::default()
    })
    .unwrap()
}
fn context(consent: LossConsent, budget: &ResourceBudget) -> ExecutionLossContext {
    ExecutionLossContext::new(
        ProviderKind::Mysql,
        MappingPolicy::Compatible,
        consent,
        vec![rule(0), rule(1)],
        budget,
    )
    .unwrap()
    .0
}
const fn changed() -> Classification {
    Classification::WouldChange(Basis::Exact)
}

#[test]
fn metadata_subjects_use_actual_consent_and_transaction_accounting() {
    use plenora_database_core::metadata_subject::{
        MetadataHop, MetadataReason, MetadataScope, MetadataSubject,
    };
    let rules: Vec<_> = (0..2)
        .map(|index| RuleInstance {
            subject: Some(MetadataSubject {
                index,
                scope: MetadataScope::Field,
                field_identity: Some("42".to_owned()),
                property: "native_metadata".to_owned(),
                key_sha256: format!("{index:064x}"),
                hop: MetadataHop::ColumnComment,
                reason: MetadataReason::ColumnLimit,
            }),
            field_id: 0,
            rule_id: RuleId::PgMetadataKeyNotPreserved,
            parameters: RuleParameters::default(),
            source_type: None,
            target_type: None,
        })
        .collect();
    for (policy, consent, allowed) in [
        (MappingPolicy::Strict, LossConsent::Accept, false),
        (MappingPolicy::Compatible, LossConsent::Deny, false),
        (MappingPolicy::Compatible, LossConsent::Accept, true),
    ] {
        let resource_budget = budget(1_000_000);
        let mut c = ExecutionLossContext::new(
            ProviderKind::Postgres,
            policy,
            consent,
            rules.clone(),
            &resource_budget,
        )
        .unwrap()
        .0;
        assert_eq!(c.ledger.report().schema_version, 4);
        assert!(c.ledger.report().evaluation.entries.is_empty());
        let before = resource_budget.remaining(ResourceKind::MemoryBytes);
        c.begin_metadata_batch(1000).unwrap();
        let remaining = resource_budget.remaining(ResourceKind::MemoryBytes);
        assert!(
            remaining < before,
            "PostgreSQL metadata reserves its compiled subjects"
        );
        assert!(
            remaining > before - 10_000,
            "sparse metadata is not a Cartesian product"
        );
        c.track(0, 0, 0).unwrap();
        // Risk alone never requests consent. An unchanged key is exact.
        c.observe_metadata(0, 0, 0, Classification::Exact(Basis::Exact))
            .unwrap();
        assert_eq!(c.observe_metadata(0, 0, 1, changed()).is_ok(), allowed);
        let evaluated = c.ledger.report();
        assert_eq!(evaluated.evaluation.entries.len(), 2);
        assert!(evaluated.effective.is_empty());
        if allowed {
            c.sent(0).unwrap();
            c.attributed(0, |_| true).unwrap();
            c.prepare_commit(true).unwrap();
            assert_eq!(c.confirmed.effective.len(), 1);
            assert_eq!(c.confirmed.effective[0].subject, Some(1));
            assert_eq!(c.confirmed.effective[0].committed, 1);
            c.confirmed.validate().unwrap();
            let mut empty_outcome: serde_json::Value = serde_json::from_str(include_str!(
                "../../../contracts/v2/examples/outcome-v3-loss.json"
            ))
            .unwrap();
            empty_outcome["provider"] = serde_json::json!("postgres");
            for value in empty_outcome["rows"].as_object_mut().unwrap().values_mut() {
                *value = serde_json::json!(0);
            }
            empty_outcome["loss_report"] = serde_json::to_value(&c.confirmed).unwrap();
            let outcome: plenora_database_core::outcome::WriteOutcome =
                serde_json::from_value(empty_outcome).unwrap();
            outcome.validate().unwrap();
            let mut uncertain = c.ledger.clone();
            uncertain.uncertain().unwrap();
            assert_eq!(uncertain.report().effective[0].uncertain, 1);
        }
        c.ledger.rollback();
        assert!(c.ledger.report().effective.is_empty());
        c.ledger.report().validate().unwrap();
    }
}

#[test]
fn source_tokens_survive_transport_batches_and_attribute_cells() {
    let mut c = context(LossConsent::Accept, &budget(1_000_000));
    c.begin_source_batch(1, 2, 2, 128).unwrap();
    let (first, transformed) = c
        .evaluate(
            0,
            &rule(0),
            false,
            || Ok((changed(), -0.0_f64)),
            |_| Ok(0.0),
        )
        .unwrap();
    assert_eq!(transformed.to_bits(), 0.0_f64.to_bits());
    let (excluded, ()) = c
        .evaluate(0, &rule(1), false, || Ok((changed(), ())), Ok)
        .unwrap();
    c.ledger.sent_token(first).unwrap();
    c.begin_source_batch(2, 1, 2, 128).unwrap();
    let (last, ()) = c
        .evaluate(0, &rule(0), false, || Ok((changed(), ())), Ok)
        .unwrap();
    c.ledger.applied_token(first).unwrap();
    c.ledger.discard_token(excluded).unwrap();
    c.ledger.sent_token(last).unwrap();
    assert!(c.prepare_commit(true).is_err());
    c.ledger.applied_token(last).unwrap();
    c.prepare_commit(true).unwrap();
    assert!(c.ledger.report().effective.is_empty());
    assert_eq!(c.confirmed.effective[0].committed, 2);
    assert_eq!(c.confirmed.effective.len(), 1);
    c.ledger.rollback();
    let report = c.ledger.report();
    assert!(report.effective.is_empty());
    assert_eq!(
        report
            .evaluation
            .entries
            .iter()
            .map(|e| e.checked)
            .sum::<u64>(),
        3
    );
}
#[test]
fn stage_only_is_never_uncertain_and_retry_does_not_recount() {
    let mut c = context(LossConsent::Accept, &budget(1_000_000));
    c.begin_source_batch(1, 2, 2, 0).unwrap();
    let (a, ()) = c
        .evaluate(0, &rule(0), false, || Ok((changed(), ())), Ok)
        .unwrap();
    let (stage, ()) = c
        .evaluate(1, &rule(0), false, || Ok((changed(), ())), Ok)
        .unwrap();
    assert!(c.ledger.retry_token(a).is_err());
    c.ledger.sent_token(a).unwrap();
    c.ledger.discard_token(a).unwrap();
    c.ledger.retry_token(a).unwrap();
    c.ledger.sent_token(a).unwrap();
    assert!(c.ledger.sent_token(a).is_err());
    assert!(c.ledger.applied_token(stage).is_err());
    c.ledger.uncertain().unwrap();
    let report = c.ledger.report();
    assert_eq!(report.effective[0].uncertain, 1);
    assert_eq!(report.evaluation.entries[0].checked, 2);
    report.validate().unwrap();
}
#[test]
fn consent_denial_keeps_evaluation_but_never_transforms() {
    let mut c = context(LossConsent::Deny, &budget(1_000_000));
    c.begin_source_batch(1, 1, 2, 0).unwrap();
    assert!(c
        .evaluate(
            0,
            &rule(0),
            false,
            || Ok((changed(), ())),
            |()| panic!("transform without consent")
        )
        .is_err());
    assert_eq!(c.ledger.report().evaluation.entries[0].would_change, 1);
    assert!(c
        .evaluate(
            0,
            &rule(1),
            false,
            || Ok((Classification::HardReject, ())),
            |()| panic!("hard reject transformed")
        )
        .is_err());
    assert_eq!(c.ledger.report().evaluation.entries.len(), 1);
}
#[test]
fn exact_values_do_not_transform_and_keys_never_authorize_loss() {
    let mut c = context(LossConsent::Accept, &budget(1_000_000));
    c.begin_source_batch(1, 1, 2, 0).unwrap();
    c.evaluate(
        0,
        &rule(0),
        true,
        || Ok((Classification::Exact(Basis::Exact), ())),
        |()| panic!("exact transformed"),
    )
    .unwrap();
    assert!(c
        .evaluate(
            0,
            &rule(1),
            true,
            || Ok((changed(), ())),
            |()| panic!("key transformed")
        )
        .is_err());
}
#[test]
fn leases_cover_empty_sources_and_release_on_drop() {
    let budget = budget(40_000);
    let initial = budget.remaining(ResourceKind::MemoryBytes);
    {
        let mut c = context(LossConsent::Accept, &budget);
        let before = budget.remaining(ResourceKind::MemoryBytes);
        c.begin_batch(0, 2).unwrap();
        assert_eq!(budget.remaining(ResourceKind::MemoryBytes), before);
    }
    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), initial);
    let mut c = context(LossConsent::Accept, &budget);
    let before = budget.remaining(ResourceKind::MemoryBytes);
    c.begin_source_batch(1, 0, 2, 0).unwrap();
    assert!(budget.remaining(ResourceKind::MemoryBytes) < before);
    assert!(c.begin_source_batch(2, 1, 1, 0).is_err());
    let error = c.begin_source_batch(2, usize::MAX, 2, 0).unwrap_err();
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    assert!(c.ledger.report().evaluation.entries.is_empty());
    assert!(c.begin_source_batch(2, 1, 2, u64::MAX).is_err());
    assert!(c
        .evaluate(
            0,
            &rule(0),
            false,
            || panic!("outside reserved rows"),
            Ok::<(), _>
        )
        .is_err());
}
#[test]
fn foreign_tokens_and_unplanned_instances_are_rejected() {
    let mut a = context(LossConsent::Accept, &budget(1_000_000));
    let mut b = context(LossConsent::Accept, &budget(1_000_000));
    a.begin_source_batch(1, 1, 2, 0).unwrap();
    b.begin_source_batch(1, 1, 2, 0).unwrap();
    let (token, ()) = a
        .evaluate(0, &rule(0), false, || Ok((changed(), ())), Ok)
        .unwrap();
    b.evaluate(0, &rule(0), false, || Ok((changed(), ())), Ok)
        .unwrap();
    assert!(b.ledger.sent_token(token).is_err());
    assert!(b.ledger.discard_token(token).is_err());
    let mut changed_instance = rule(0);
    changed_instance.parameters.length = Some(1);
    assert!(a
        .evaluate(
            0,
            &changed_instance,
            false,
            || panic!("unfrozen facts"),
            Ok::<(), _>
        )
        .is_err());
    assert!(a.begin_source_batch(1, 1, 2, 0).is_err());
    assert!(a.begin_batch(1, 2).is_err());
}

#[test]
fn accumulated_budget_failure_preserves_existing_tokens_and_evaluation() {
    let budget = budget(100_000);
    let mut c = context(LossConsent::Accept, &budget);
    c.begin_source_batch(1, 1, 2, 128).unwrap();
    let (token, ()) = c
        .evaluate(0, &rule(0), false, || Ok((changed(), ())), Ok)
        .unwrap();
    let (same, ()) = c
        .evaluate(0, &rule(0), false, || Ok((changed(), ())), Ok)
        .unwrap();
    assert_eq!(same, token);
    assert_eq!(c.ledger.report().evaluation.entries[0].checked, 1);
    c.begin_source_batch(2, 1, 2, 128).unwrap();
    let before = budget.remaining(ResourceKind::MemoryBytes);
    assert_eq!(
        c.begin_source_batch(3, 1, 2, 128).unwrap_err().category,
        ErrorCategory::ResourceLimit
    );
    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), before);
    c.ledger.sent_token(token).unwrap();
    c.ledger.applied_token(token).unwrap();
    c.prepare_commit(false).unwrap();
    assert_eq!(c.confirmed.effective[0].committed, 1);
    assert!(!c.confirmed.evaluation.complete);
}
