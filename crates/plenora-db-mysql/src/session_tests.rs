use super::*;

#[test]
fn bootstrap_is_explicit_and_deterministic() {
    assert!(SESSION_BOOTSTRAP_SQL.contains("autocommit = 1"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("time_zone = '+00:00'"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("STRICT_TRANS_TABLES"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("NO_AUTO_VALUE_ON_ZERO"));
    for mode in [
        "NO_ZERO_IN_DATE",
        "NO_ZERO_DATE",
        "ERROR_FOR_DIVISION_BY_ZERO",
    ] {
        assert!(SESSION_BOOTSTRAP_SQL.contains(mode));
    }
    assert!(SESSION_BOOTSTRAP_SQL.contains("foreign_key_checks = 1"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("unique_checks = 1"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("sql_notes = 1"));
    assert!(SESSION_BOOTSTRAP_SQL.contains("sql_select_limit = 18446744073709551615"));
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
        expected_profile: None,
        expected_database: "warehouse".to_owned(),
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
    for code in crate::profile::ProductProfile::commit_rollback_codes(&profile) {
        let error = super::commit_failure(&profile, &server(*code));
        assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{code}");
    }
}

/// L'elenco dei rifiuti al COMMIT e del prodotto: per MariaDB non c'e prova
/// che 3101 (`ER_TRANSACTION_ROLLBACK_DURING_COMMIT`, un codice MySQL) sia un
/// rollback, quindi resta esito ignoto; 1213 (deadlock `InnoDB`) lo prova.
#[test]
fn mariadb_does_not_inherit_the_mysql_commit_rejection_list() {
    use plenora_database_core::RemoteEffect;
    let profile = crate::profile::MariadbProfile;
    let server = |code: u16| {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: "commit".to_owned(),
            state: "HY000".to_owned(),
        })
    };
    assert_eq!(
        super::commit_failure(&profile, &server(3_101)).remote_effect,
        RemoteEffect::Unknown
    );
    assert_eq!(
        super::commit_failure(&profile, &server(1_213)).remote_effect,
        RemoteEffect::RolledBack
    );
}

#[test]
fn warnings_allow_only_documented_information_without_payload() {
    for code in 0..=u16::MAX {
        assert!(!informative_warning(Some("Note"), Some(code)));
        assert_eq!(
            informative_warning(Some("Warning"), Some(code)),
            matches!(code, 1287 | 1681)
        );
        assert!(!informative_warning(Some("Error"), Some(code)));
    }
    assert!(!informative_warning(None, None));
    for kind in [
        plenora_database_core::plan::ProviderKind::Mysql,
        plenora_database_core::plan::ProviderKind::Mariadb,
    ] {
        let error = warning_error(kind, ErrorPhase::Write);
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.provider, Some(kind));
        assert_eq!(
            error.message,
            "diagnostico server non qualificato come informativo: risultato rifiutato"
        );
    }
}

#[test]
fn diagnostics_require_context_and_unsaturated_counts() {
    // ER_SQL_MODE_MERGED resta rifiutato anche nel bootstrap: il test live
    // deve provare che la combinazione completa non lo produce.
    for level in ["Note", "Warning", "Error"] {
        assert!(!informative_warning_for_sql(
            Some(level),
            Some(3135),
            SESSION_BOOTSTRAP_SQL,
        ));
    }
    assert!(warning_count_complete(2, 2));
    assert!(!warning_count_complete(2, 1));
    assert!(!warning_count_complete(u16::MAX, usize::from(u16::MAX)));
    for (code, sql) in [
        (1007, "CREATE DATABASE IF NOT EXISTS d"),
        (1008, "DROP DATABASE IF EXISTS d"),
        (1050, "CREATE TABLE IF NOT EXISTS t(id INT)"),
        (1051, "DROP TABLE IF EXISTS t"),
        (1305, "DROP PROCEDURE IF EXISTS p"),
        (1305, "DROP FUNCTION IF EXISTS f"),
        (1360, "DROP TRIGGER IF EXISTS t"),
        (1003, "EXPLAIN SELECT 1"),
        (1060, "ALTER TABLE t ADD COLUMN IF NOT EXISTS d INT"),
        (1061, "ALTER TABLE t ADD INDEX IF NOT EXISTS i(d)"),
        (1091, "ALTER TABLE t DROP COLUMN IF EXISTS missing"),
    ] {
        assert!(informative_warning_for_sql(Some("Note"), Some(code), sql));
        assert!(!informative_warning_for_sql(
            Some("Warning"),
            Some(code),
            sql
        ));
        assert!(!informative_warning_for_sql(
            Some("Note"),
            Some(code),
            "SELECT 1"
        ));
    }
    for code in [1091, 1060, 1061, 1196, 1265, 1329, 1356] {
        assert!(!informative_warning_for_sql(
            Some("Note"),
            Some(code),
            "EXPLAIN SELECT 1"
        ));
        assert!(!informative_warning_for_sql(
            Some("Warning"),
            Some(code),
            "ROLLBACK"
        ));
    }
    let rollback = warning_error(crate::profile::MYSQL_PROFILE.kind(), ErrorPhase::Rollback)
        .after_unconfirmed_rollback();
    assert_eq!(rollback.remote_effect, RemoteEffect::Unknown);
    assert_eq!(rollback.retry, RetryDisposition::RequiresRecovery);
    let commit = after_commit_sent(
        warning_error(crate::profile::MYSQL_PROFILE.kind(), ErrorPhase::Commit),
        false,
    );
    assert_eq!(commit.remote_effect, RemoteEffect::Unknown);
}

#[test]
fn text_protocol_requires_one_statement_even_with_comments_and_literals() {
    use plenora_database_core::native_query_policy::enforce_single_mysql_statement as check;
    for sql in [
        "INSERT INTO t VALUES ('1.239'); DO 0",
        "DO 0; /* x */ DO 1",
        "/*! DO 1 */",
        "DO 0; # comment\n DO 1",
    ] {
        assert!(check(sql).is_err());
    }
    for sql in [
        "DO ';'",
        "DO 0;",
        "/* comment */ DO 0",
        "DO 'x\\'; DO 1'",
        "DO 0; -- comment\n",
    ] {
        assert!(check(sql).is_ok(), "{sql}");
    }
}
