use super::*;

#[test]
fn valid_savepoint_names_are_accepted() {
    for name in ["sp1", "outbox", "step_2", "_hidden", "A", "abcABC_012"] {
        assert!(validate_savepoint_name(name).is_ok(), "atteso ok: {name}");
    }
}

#[test]
fn invalid_savepoint_names_are_rejected() {
    for name in [
        "",
        "1abc",
        "with space",
        "drop;",
        "quote\"",
        &"x".repeat(64),
    ] {
        assert!(
            validate_savepoint_name(name).is_err(),
            "atteso rifiuto: {name:?}"
        );
    }
}

#[test]
fn outcome_unknown_recovery_forbids_automatic_retry() {
    let recovery = outcome_unknown_recovery();
    assert!(!recovery.automatic_retry_allowed);
    assert_eq!(recovery.last_certain_phase, CertainPhase::CommitRequested);
}

#[test]
fn commit_outcome_serialization_roundtrip() {
    let ok = CommitOutcome::Committed;
    let json = serde_json::to_string(&ok).expect("serialize");
    assert_eq!(json, r#"{"status":"committed"}"#);
    let round: CommitOutcome = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(round, CommitOutcome::Committed);

    let unknown = CommitOutcome::OutcomeUnknown {
        recovery: outcome_unknown_recovery(),
    };
    let json = serde_json::to_string(&unknown).expect("serialize");
    assert!(json.contains("outcome_unknown"));
    let round: CommitOutcome = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(round, CommitOutcome::OutcomeUnknown { .. }));
}

/// La regola unica dopo il COMMIT: ogni errore e ignoto con la causa come
/// categoria, salvo il rollback provato; mai `none`.
#[test]
fn after_commit_sent_never_reports_no_effect() {
    use crate::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};
    let failure = DatabaseError::new(ErrorCategory::Io, ErrorPhase::Write, None, "canale");
    let unknown = after_commit_sent(failure.clone(), false);
    assert_eq!(unknown.remote_effect, RemoteEffect::Unknown);
    assert_eq!(unknown.phase, ErrorPhase::Commit);
    assert_eq!(unknown.retry, RetryDisposition::RequiresRecovery);
    assert_eq!(unknown.category, ErrorCategory::Io);
    assert!(matches!(
        commit_failure_outcome(failure.clone(), false),
        Ok(CommitOutcome::OutcomeUnknown { recovery }) if recovery.cause == Some(ErrorCategory::Io)
    ));
    let rejected = commit_failure_outcome(failure, true).expect_err("rollback provato");
    assert_eq!(rejected.remote_effect, RemoteEffect::RolledBack);
}
