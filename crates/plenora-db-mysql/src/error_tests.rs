use super::*;
use std::io;

#[test]
fn tls_hostname_rejection_is_distinct_from_generic_io() {
    let tls = Error::Io(mysql_async::IoError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid peer certificate: certificate not valid for name",
    )));
    let mapped = driver_error(
        &crate::profile::MYSQL_PROFILE,
        &tls,
        ErrorPhase::Connect,
        RemoteEffect::None,
    );
    assert_eq!(mapped.category, ErrorCategory::Protocol);
    assert_eq!(mapped.message, "verifica identita TLS MySQL rifiutata");

    let dns = Error::Io(mysql_async::IoError::Io(io::Error::new(
        io::ErrorKind::NotFound,
        "host resolution failed",
    )));
    let mapped = driver_error(
        &crate::profile::MYSQL_PROFILE,
        &dns,
        ErrorPhase::Connect,
        RemoteEffect::None,
    );
    assert_eq!(mapped.category, ErrorCategory::Io);
}

#[test]
fn server_code_mappings_win_over_tls_identity_text() {
    let cases = [
        (
            1_045,
            "Access denied for user 'certificate_name'@'%' to database 'dns'",
            ErrorCategory::Authentication,
        ),
        (
            1_054,
            "Unknown column 'certificate_name' in 'field list'",
            ErrorCategory::Schema,
        ),
        (
            1_062,
            "Duplicate entry 'certificate name 7' for key 'dns_primary'",
            ErrorCategory::Conflict,
        ),
        (
            1_205,
            "Lock wait timeout exceeded; certificate name lookup dns slow",
            ErrorCategory::Timeout,
        ),
    ];
    for (code, message, expected) in cases {
        let error = Error::Server(mysql_async::ServerError {
            code,
            message: message.to_owned(),
            state: "HY000".to_owned(),
        });
        let mapped = driver_error(
            &crate::profile::MYSQL_PROFILE,
            &error,
            ErrorPhase::Read,
            RemoteEffect::None,
        );
        assert_eq!(
            mapped.category, expected,
            "server code {code} must keep its mapping"
        );
    }
}

#[test]
fn incidental_certificate_text_in_io_stays_io() {
    let error = Error::Io(mysql_async::IoError::Io(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "connection lost while exporting certificate_name column values",
    )));
    let mapped = driver_error(
        &crate::profile::MYSQL_PROFILE,
        &error,
        ErrorPhase::Read,
        RemoteEffect::None,
    );
    assert_eq!(mapped.category, ErrorCategory::Io);
    assert_eq!(mapped.message, "errore I/O protocollo MySQL redatto");
}

#[test]
fn pre_session_timeout_and_cancellation_do_not_claim_quarantine() {
    let timeout = timeout_error(
        &crate::profile::MYSQL_PROFILE,
        ErrorPhase::Connect,
        RemoteEffect::None,
    );
    assert_eq!(timeout.category, ErrorCategory::Timeout);
    assert!(
        !timeout.message.contains("quarantin"),
        "pre-sessione non esiste connessione da quarantinare: {}",
        timeout.message
    );

    let cancelled = cancellation_error(
        &crate::profile::MYSQL_PROFILE,
        ErrorPhase::Connect,
        RemoteEffect::None,
    );
    assert_eq!(cancelled.category, ErrorCategory::Cancelled);
    assert!(
        !cancelled.message.contains("quarantin"),
        "pre-sessione non esiste connessione da quarantinare: {}",
        cancelled.message
    );
}

#[test]
fn in_flight_timeout_still_reports_quarantine() {
    let error = timeout_error(
        &crate::profile::MYSQL_PROFILE,
        ErrorPhase::Read,
        RemoteEffect::None,
    );
    assert!(error.message.contains("quarantinata"));
}

#[test]
fn deadline_and_requested_cancellation_have_distinct_envelopes() {
    let deadline = CancellationToken::new();
    deadline.cancel_due_to_deadline();
    assert_eq!(
        interruption_error(
            &crate::profile::MYSQL_PROFILE,
            &deadline,
            ErrorPhase::Read,
            RemoteEffect::None
        )
        .category,
        ErrorCategory::Timeout
    );

    let requested = CancellationToken::new();
    requested.cancel();
    assert_eq!(
        interruption_error(
            &crate::profile::MYSQL_PROFILE,
            &requested,
            ErrorPhase::Read,
            RemoteEffect::None
        )
        .category,
        ErrorCategory::Cancelled
    );
}

#[test]
fn write_timeout_never_claims_rollback() {
    let error = timeout_error(
        &crate::profile::MYSQL_PROFILE,
        ErrorPhase::Commit,
        RemoteEffect::None,
    );
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
}

#[test]
fn read_cancellation_is_non_retryable_and_effect_free() {
    let error = cancellation_error(
        &crate::profile::MYSQL_PROFILE,
        ErrorPhase::Read,
        RemoteEffect::None,
    );
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
}
