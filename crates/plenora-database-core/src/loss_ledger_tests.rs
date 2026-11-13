use super::*;
use crate::execution_loss::RiskEntry;
#[test]
fn exact_sent_row_requires_statement_outcome_before_commit() {
    let report = ExecutionLossReport::planned(
        MappingPolicy::Compatible,
        vec![RiskEntry::new(
            0,
            RuleId::StoreNegativeZero,
            MappingPolicy::Compatible,
            None,
            None,
        )],
    )
    .unwrap();
    let mut ledger = LossLedger::new(report, 16384, 10).unwrap();
    ledger.begin_batch(0).unwrap();
    ledger
        .observe(
            0,
            0,
            RuleId::StoreNegativeZero,
            Classification::Exact(Basis::Exact),
        )
        .unwrap();
    ledger.sent(0).unwrap();
    assert!(ledger.commit().is_err());
    assert!(ledger.begin_batch(1).is_err());
    ledger.applied(0).unwrap();
    ledger.complete();
    ledger.commit().unwrap();
    let report = ledger.report();
    report.validate().unwrap();
    assert!(report.effective.is_empty());
    assert_eq!(report.evaluation.entries[0].checked, 1);
}

#[test]
fn checked_overflow_is_atomic_and_rolls_back_without_erasing_evaluation() {
    let report = ExecutionLossReport::planned(
        MappingPolicy::Compatible,
        vec![RiskEntry::new(
            0,
            RuleId::StoreEvery,
            MappingPolicy::Compatible,
            None,
            None,
        )],
    )
    .unwrap();
    let mut ledger = LossLedger::new(report, 16384, 10).unwrap();
    ledger.begin_batch(0).unwrap();
    ledger.report.evaluation.entries.push(EvaluationEntry {
        subject: None,
        field_id: 0,
        rule_id: RuleId::StoreEvery,
        basis: Basis::Conservative,
        checked: u64::MAX,
        would_change: u64::MAX,
    });
    let error = ledger
        .observe(
            0,
            0,
            RuleId::StoreEvery,
            Classification::WouldChange(Basis::Conservative),
        )
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    assert_eq!(error.phase, ErrorPhase::Write);
    assert!(ledger.cells.is_empty());
    ledger.rollback();
    assert_eq!(ledger.report().evaluation.entries[0].checked, u64::MAX);
    assert!(ledger.report().effective.is_empty());
}
