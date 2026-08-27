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
