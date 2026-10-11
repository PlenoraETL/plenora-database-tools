use super::*;

#[test]
fn bootstrap_is_explicit_and_deterministic() {
    assert!(SESSION_BOOTSTRAP_SQL.contains("autocommit = 1"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("time_zone = '+00:00'"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("STRICT_TRANS_TABLES"));
}

#[test]
fn exactly_one_affected_row_is_required_for_row_scoped_success() {
    validate_row_write_affected_rows(1, &crate::profile::MYSQL_PROFILE)
        .expect("una riga confermata");
    for affected in [0, 2] {
        let error = validate_row_write_affected_rows(affected, &crate::profile::MYSQL_PROFILE)
            .expect_err("conteggio diverso da uno ambiguo");
        assert_eq!(error.phase, ErrorPhase::Write);
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.retry, RetryDisposition::Quarantine);
        assert!(error.diagnostics.is_none());
    }
}

#[test]
fn an_abandoned_transaction_marks_its_session_quarantined() {
    let mut session = MysqlSession {
        profile: &crate::profile::MYSQL_PROFILE,
        connection: None,
        state: MysqlSessionState::Ready,
        operation_timeout: std::time::Duration::from_secs(1),
        pool_permit: None,
    };
    session.quarantine_on_drop();
    assert_eq!(session.state(), MysqlSessionState::Quarantined);
}

/// Regola unica dopo il COMMIT: un codice server non basta a provare che il
/// commit non sia avvenuto; solo 1213 e 3101 provano il rollback.
#[test]
fn after_commit_only_a_closed_list_of_codes_proves_the_rollback() {
    use plenora_database_core::{ErrorPhase, RemoteEffect};
    let profile = crate::profile::MysqlProfile;
    let server = |code: u16| {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: "commit".to_owned(),
            state: "HY000".to_owned(),
        })
    };
    for code in [1_062, 1_180, 2_013, 9_999] {
        let error = super::commit_failure(&profile, &server(code));
        assert_eq!(error.remote_effect, RemoteEffect::Unknown, "{code}");
        assert_eq!(error.phase, ErrorPhase::Commit);
    }
    for code in super::COMMIT_ROLLBACK_CODES {
        let error = super::commit_failure(&profile, &server(*code));
        assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{code}");
    }
}
