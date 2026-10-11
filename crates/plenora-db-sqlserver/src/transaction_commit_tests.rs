use super::*;
use plenora_database_core::{ErrorCategory, ErrorPhase, RetryDisposition};

fn failure(effect: RemoteEffect) -> DatabaseError {
    DatabaseError {
        remote_effect: effect,
        retry: RetryDisposition::Never,
        ..DatabaseError::new(
            ErrorCategory::Execution,
            ErrorPhase::Commit,
            Some(plenora_database_core::plan::ProviderKind::Sqlserver),
            "commit",
        )
    }
}

/// Regola unica dopo il COMMIT: un errore senza prova di rollback e esito
/// ignoto, non un errore; 1205 prova il rollback; un errore di stato
/// precede l'invio.
#[test]
fn after_commit_only_a_proven_rollback_is_an_error() {
    assert!(matches!(
        commit_failure(failure(RemoteEffect::Unknown)),
        Ok(CommitOutcome::OutcomeUnknown { .. })
    ));
    let rolled_back = commit_failure(failure(RemoteEffect::RolledBack)).expect_err("1205");
    assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    let before = commit_failure(failure(RemoteEffect::None)).expect_err("stato");
    assert_eq!(before.remote_effect, RemoteEffect::None);
}
