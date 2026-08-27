use super::*;

fn map(code: &str, phase: ErrorPhase) -> Mapping {
    resolve_mapping(Some(code), false, phase)
}

#[test]
fn unique_violation_is_conflict_rolled_back() {
    let m = map("23505", ErrorPhase::Write);
    assert_eq!(m.category, ErrorCategory::Conflict);
    assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
    assert!(matches!(m.retry, RetryDisposition::Never));
}

#[test]
fn serialization_failure_is_safe_retry() {
    let m = map("40001", ErrorPhase::Commit);
    assert_eq!(m.category, ErrorCategory::Transient);
    assert!(matches!(m.retry, RetryDisposition::Safe));
    assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
}

#[test]
fn deadlock_is_safe_retry() {
    let m = map("40P01", ErrorPhase::Write);
    assert_eq!(m.category, ErrorCategory::Transient);
    assert!(matches!(m.retry, RetryDisposition::Safe));
    assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
}

#[test]
fn statement_completion_unknown_requires_recovery() {
    let m = map("40003", ErrorPhase::Commit);
    assert_eq!(m.category, ErrorCategory::Transient);
    assert!(matches!(m.retry, RetryDisposition::RequiresRecovery));
    assert_eq!(m.remote_effect, RemoteEffect::Unknown);
}

#[test]
fn insufficient_privilege_is_authorization() {
    let m = map("42501", ErrorPhase::Prepare);
    assert_eq!(m.category, ErrorCategory::Authorization);
    assert_eq!(m.remote_effect, RemoteEffect::None);
}

#[test]
fn undefined_table_is_not_found() {
    let m = map("42P01", ErrorPhase::Prepare);
    assert_eq!(m.category, ErrorCategory::NotFound);
}

#[test]
fn duplicate_table_is_conflict() {
    let m = map("42P07", ErrorPhase::Write);
    assert_eq!(m.category, ErrorCategory::Conflict);
}

#[test]
fn generated_column_write_is_conflict_rolled_back() {
    let m = map("428C9", ErrorPhase::Write);
    assert_eq!(m.category, ErrorCategory::Conflict);
    assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
    assert!(matches!(m.retry, RetryDisposition::Never));
}

#[test]
fn too_many_connections_defers_retry() {
    let m = map("53300", ErrorPhase::Connect);
    assert_eq!(m.category, ErrorCategory::ResourceLimit);
    assert!(matches!(m.retry, RetryDisposition::After(1_000)));
}

#[test]
fn query_cancelled_is_cancelled_category() {
    let m = map("57014", ErrorPhase::Read);
    assert_eq!(m.category, ErrorCategory::Cancelled);
    assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
}

#[test]
fn admin_shutdown_quarantines_connection() {
    let m = map("57P01", ErrorPhase::Write);
    assert_eq!(m.category, ErrorCategory::Transient);
    assert!(matches!(m.retry, RetryDisposition::Quarantine));
    assert_eq!(m.remote_effect, RemoteEffect::Unknown);
}

#[test]
fn syntax_error_is_invalid_plan() {
    let m = map("42601", ErrorPhase::Prepare);
    assert_eq!(m.category, ErrorCategory::InvalidPlan);
}

#[test]
fn division_by_zero_is_execution_error() {
    let m = map("22012", ErrorPhase::Write);
    assert_eq!(m.category, ErrorCategory::Execution);
    assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
}

#[test]
fn read_only_transaction_is_authorization() {
    let m = map("25006", ErrorPhase::Write);
    assert_eq!(m.category, ErrorCategory::Authorization);
}

#[test]
fn feature_not_supported_is_unsupported() {
    let m = map("0A000", ErrorPhase::Prepare);
    assert_eq!(m.category, ErrorCategory::Unsupported);
}

#[test]
fn unknown_sqlstate_falls_back_to_protocol() {
    let m = resolve_mapping(Some("99999"), false, ErrorPhase::Read);
    assert_eq!(m.category, ErrorCategory::Protocol);
    assert!(matches!(m.retry, RetryDisposition::Never));
}

#[test]
fn no_sqlstate_no_close_is_protocol() {
    let m = resolve_mapping(None, false, ErrorPhase::Read);
    assert_eq!(m.category, ErrorCategory::Protocol);
}

#[test]
fn transport_closed_without_sqlstate_is_io_quarantine() {
    let m = resolve_mapping(None, true, ErrorPhase::Read);
    assert_eq!(m.category, ErrorCategory::Io);
    assert!(matches!(m.retry, RetryDisposition::Quarantine));
}

#[test]
fn transport_closed_in_commit_is_remote_effect_unknown() {
    let m = resolve_mapping(None, true, ErrorPhase::Commit);
    assert_eq!(m.category, ErrorCategory::Io);
    assert_eq!(m.remote_effect, RemoteEffect::Unknown);
}

#[test]
fn transport_closed_in_connect_is_remote_effect_none() {
    let m = resolve_mapping(None, true, ErrorPhase::Connect);
    assert_eq!(m.category, ErrorCategory::Io);
    assert_eq!(m.remote_effect, RemoteEffect::None);
}

#[test]
fn transport_closed_upgrades_write_conflict_to_unknown_effect() {
    let m = resolve_mapping(Some("23505"), true, ErrorPhase::Commit);
    assert_eq!(m.remote_effect, RemoteEffect::Unknown);
    assert!(matches!(m.retry, RetryDisposition::RequiresRecovery));
}

#[test]
fn transport_closed_read_keeps_none_effect() {
    let m = resolve_mapping(Some("42P01"), true, ErrorPhase::Read);
    assert_eq!(m.category, ErrorCategory::NotFound);
    assert_eq!(m.remote_effect, RemoteEffect::None);
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn database_error_is_send_sync() {
    assert_send_sync::<DatabaseError>();
}

#[test]
fn message_never_contains_sqlstate_code() {
    for code in [
        "08006", "23505", "40001", "40P01", "42501", "42P01", "53300", "57014", "57P01", "22001",
        "22P02", "25P02", "0A000",
    ] {
        let m = map(code, ErrorPhase::Write);
        assert!(
            !m.message.contains(code),
            "il messaggio pubblico non deve contenere il SQLSTATE {code}"
        );
    }
}
