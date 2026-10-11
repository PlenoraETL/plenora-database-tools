use super::*;
use plenora_database_core::RemoteEffect;

/// Una risposta al COMMIT che il driver non sa interpretare non prova che il
/// commit non sia avvenuto.
#[test]
fn an_uninterpretable_commit_response_is_an_unknown_outcome() {
    for error in [
        oracle_rs::Error::Protocol("risposta inattesa".to_owned()),
        oracle_rs::Error::ProtocolError("risposta inattesa".to_owned()),
    ] {
        assert!(
            matches!(
                commit_failure(&error),
                Ok(CommitOutcome::OutcomeUnknown { .. })
            ),
            "{error:?}"
        );
    }
}

/// Un rifiuto del server con codice ORA e certo: resta un errore.
#[test]
fn a_coded_server_rejection_stays_an_error() {
    let error = oracle_rs::Error::OracleError {
        code: 2_091,
        message: "ORA-02091".to_owned(),
    };
    assert!(commit_failure(&error).is_err());
}

/// ORA-03113, ORA-03114 e ORA-25408 dopo il COMMIT dicono che la connessione
/// e persa, non che il commit e stato rifiutato: esito ignoto. Solo un elenco
/// chiuso di codici prova il rifiuto.
#[test]
fn lost_connection_codes_after_commit_are_unknown() {
    for code in [3_113, 3_114, 25_408, 12_345] {
        let error = oracle_rs::Error::OracleError {
            code,
            message: "connessione".to_owned(),
        };
        assert!(
            matches!(
                commit_failure(&error),
                Ok(CommitOutcome::OutcomeUnknown { .. })
            ),
            "ORA-{code}"
        );
    }
}

/// ORA-02091: il server ha annullato la transazione al commit.
#[test]
fn a_rolled_back_commit_is_a_certain_rejection() {
    let error = oracle_rs::Error::OracleError {
        code: 2_091,
        message: "ORA-02091".to_owned(),
    };
    let rejected = commit_failure(&error).expect_err("rifiuto certo");
    assert_eq!(rejected.remote_effect, RemoteEffect::RolledBack);
}

/// Un rollback esplicito non confermato — timeout o canale perso — non
/// dichiara `none`: ERR-003 ed ERR-014.
#[test]
fn an_unconfirmed_explicit_rollback_has_an_unknown_effect() {
    let timeout = rollback_failure(DatabaseError::new(
        ErrorCategory::Timeout,
        ErrorPhase::Rollback,
        Some(ProviderKind::Oracle),
        "timeout",
    ));
    assert_eq!(timeout.category, ErrorCategory::Timeout);
    assert_eq!(timeout.phase, ErrorPhase::Rollback);
    assert_eq!(timeout.remote_effect, RemoteEffect::Unknown);
    assert_eq!(
        timeout.retry,
        plenora_database_core::RetryDisposition::RequiresRecovery
    );
}
