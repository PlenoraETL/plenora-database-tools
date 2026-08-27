use super::*;

#[test]
fn commit_timeout_never_claims_rollback() {
    let error = timeout_error(ErrorPhase::Commit, RemoteEffect::None);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
}

#[test]
fn read_cancellation_has_no_remote_mutation() {
    let error = cancellation_error(ErrorPhase::Read, RemoteEffect::None);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
}

#[test]
fn driver_messages_are_not_exposed_and_commit_io_is_unknown() {
    let driver = Error::Io {
        kind: std::io::ErrorKind::ConnectionReset,
        message: "password=unique-secret; SELECT private_column".to_owned(),
    };
    let public = driver_error(&driver, ErrorPhase::Commit, RemoteEffect::Unknown);
    assert!(!public.message.contains("unique-secret"));
    assert!(!public.message.contains("private_column"));
    assert_eq!(public.remote_effect, RemoteEffect::Unknown);
    assert_eq!(public.retry, RetryDisposition::RequiresRecovery);
}

#[test]
fn transactional_write_transport_loss_requires_recovery() {
    let driver = Error::Io {
        kind: std::io::ErrorKind::ConnectionReset,
        message: "transport detail must stay private".to_owned(),
    };
    let public = driver_error(&driver, ErrorPhase::Write, RemoteEffect::Unknown);
    assert_eq!(public.category, ErrorCategory::Io);
    assert_eq!(public.remote_effect, RemoteEffect::Unknown);
    assert_eq!(public.retry, RetryDisposition::RequiresRecovery);
    assert!(!public.message.contains("transport detail"));
}
