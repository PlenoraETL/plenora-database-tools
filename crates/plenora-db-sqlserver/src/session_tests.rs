use super::*;

#[test]
fn only_ready_session_is_reusable() {
    assert!(SessionState::Ready.is_reusable());
    for state in [
        SessionState::Transaction,
        SessionState::Uncommittable,
        SessionState::Quarantined,
        SessionState::Closed,
    ] {
        assert!(!state.is_reusable());
    }
}

#[test]
fn bootstrap_fixes_transaction_and_rowcount_semantics() {
    assert!(SESSION_BOOTSTRAP_SQL.contains("XACT_ABORT ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("IMPLICIT_TRANSACTIONS OFF"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("NOCOUNT ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("ANSI_NULLS ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("ANSI_PADDING ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("ANSI_WARNINGS ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("ARITHABORT ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("CONCAT_NULL_YIELDS_NULL ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("QUOTED_IDENTIFIER ON"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("NUMERIC_ROUNDABORT OFF"));
}
