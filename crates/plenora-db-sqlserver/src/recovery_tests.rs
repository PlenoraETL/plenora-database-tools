use super::*;

#[test]
fn statement_failure_requires_rollback() {
    let mut state = TransactionState::default();
    let _ = state.apply(TransactionEvent::BeginSucceeded);
    let decision = state.apply(TransactionEvent::StatementFailed);
    assert_eq!(decision.state, SessionState::Uncommittable);
    assert_eq!(decision.action, RecoveryAction::Rollback);
}

#[test]
fn lost_transport_in_transaction_never_becomes_success() {
    let mut state = TransactionState::default();
    let _ = state.apply(TransactionEvent::BeginSucceeded);
    let decision = state.apply(TransactionEvent::TransportLost);
    assert_eq!(decision.state, SessionState::Quarantined);
    assert_eq!(decision.action, RecoveryAction::ReconcileCommit);
}

#[test]
fn cancellation_always_quarantines() {
    let mut state = TransactionState::default();
    let decision = state.apply(TransactionEvent::Cancelled);
    assert_eq!(decision.state, SessionState::Quarantined);
    assert_eq!(decision.action, RecoveryAction::Quarantine);
}
