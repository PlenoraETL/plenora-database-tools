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
    assert!(SESSION_BOOTSTRAP_SQL.contains("SET ROWCOUNT 0"));
}

#[test]
fn options_check_requires_every_pinned_bit() {
    // I bit che il bootstrap accende, e uno qualunque in piu.
    let pinned = 8 | 16 | 32 | 64 | 256 | 512 | 4096 | 16384;
    assert!(options_pinned(pinned));
    assert!(options_pinned(pinned | 1024));
    for bit in [8, 16, 32, 64, 256, 512, 4096, 16384] {
        assert!(!options_pinned(pinned & !bit), "{bit}");
    }
    for bit in [2, 8192] {
        assert!(!options_pinned(pinned | bit), "{bit}");
    }
}
