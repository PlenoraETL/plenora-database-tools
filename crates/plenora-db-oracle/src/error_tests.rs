use crate::error::driver_error;
use oracle_rs::Error;
use plenora_database_core::{ErrorCategory, ErrorPhase};

#[test]
fn vendor_payload_never_crosses_the_public_error_boundary() {
    let marker = "dsn=password-secret SELECT * FROM private_table";
    let error = driver_error(
        ErrorPhase::Read,
        &Error::OracleError {
            code: 942,
            message: marker.to_owned(),
        },
    );
    assert_eq!(error.category, ErrorCategory::NotFound);
    assert!(error.message.contains("ORA-00942"));
    assert!(!error.message.contains(marker));
    assert!(!error.message.contains("private_table"));
}

#[test]
fn duplicate_key_is_conflict_without_copying_the_value() {
    let error = driver_error(
        ErrorPhase::Write,
        &Error::OracleError {
            code: 1,
            message: "unique constraint: customer@example.invalid".to_owned(),
        },
    );
    assert_eq!(error.category, ErrorCategory::Conflict);
    assert!(!error.message.contains("customer"));
}

#[test]
fn unnumbered_driver_errors_are_classified_without_copying_payloads() {
    let marker = "dsn=password-secret SELECT * FROM private_table";
    for (source, expected) in [
        (
            Error::ProtocolError(marker.to_owned()),
            ErrorCategory::Protocol,
        ),
        (Error::Internal(marker.to_owned()), ErrorCategory::Internal),
        (
            Error::AuthenticationFailed(marker.to_owned()),
            ErrorCategory::Authentication,
        ),
        (
            Error::DataConversionError(marker.to_owned()),
            ErrorCategory::DataMapping,
        ),
    ] {
        let error = driver_error(ErrorPhase::Connect, &source);
        assert_eq!(error.category, expected);
        assert!(!error.message.contains(marker));
        assert!(!error.message.contains("private_table"));
    }
}

/// Regola unica dell'interruzione dopo l'invio: una deadline o una
/// cancellazione mentre un comando mutante e in volo non provano nulla, in
/// nessuna delle due vie (timeout del driver o token).
#[test]
fn an_interruption_in_flight_during_a_mutating_phase_is_unknown() {
    use plenora_database_core::{CancellationToken, RemoteEffect, RetryDisposition};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let idle = CancellationToken::new();
    for phase in [ErrorPhase::Write, ErrorPhase::Commit, ErrorPhase::Rollback] {
        for (token, timeout, category) in [
            (
                &cancelled,
                std::time::Duration::from_secs(60),
                ErrorCategory::Cancelled,
            ),
            (&idle, std::time::Duration::ZERO, ErrorCategory::Timeout),
        ] {
            let error = runtime
                .block_on(crate::connection::with_timeout_duration(
                    timeout,
                    phase,
                    token,
                    std::future::pending::<oracle_rs::Result<()>>(),
                ))
                .expect_err("interrotta");
            assert_eq!(error.remote_effect, RemoteEffect::Unknown, "{phase:?}");
            assert_eq!(error.retry, RetryDisposition::RequiresRecovery, "{phase:?}");
            assert_eq!(error.category, category, "{phase:?}");
        }
    }
}
