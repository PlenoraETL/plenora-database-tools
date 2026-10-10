use super::*;

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
