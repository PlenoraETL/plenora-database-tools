use crate::error::{driver_error, interruption_error, task_error};
use odbc_api::handles::{Record, State};
use odbc_api::Error as OdbcError;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};

#[cfg(not(windows))]
fn diagnostic_message(message: &str) -> Vec<u8> {
    message.bytes().collect()
}

#[cfg(windows)]
fn diagnostic_message(message: &str) -> Vec<u16> {
    message.encode_utf16().collect()
}

fn diagnostic(state: [u8; 5], native_error: i32, vendor_message: &str) -> OdbcError {
    OdbcError::Diagnostics {
        record: Record {
            state: State(state),
            native_error,
            message: diagnostic_message(vendor_message),
        },
        function: "SQLExecute",
    }
}

#[test]
fn diagnostics_keep_codes_and_drop_vendor_payload() {
    let error = driver_error(
        &diagnostic(*b"23505", -803, "duplicate value secret-row-value"),
        ErrorPhase::Write,
    );

    assert_eq!(error.category, ErrorCategory::Conflict);
    assert_eq!(error.provider, Some(ProviderKind::Db2));
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert!(error.message.contains("23505"));
    assert!(error.message.contains("-803"));
    assert!(!error.message.contains("secret-row-value"));
    assert!(!error.message.contains("duplicate value"));
}

#[test]
fn serialization_failures_are_the_only_diagnostics_marked_safe_to_retry() {
    let serialization = driver_error(
        &diagnostic(*b"40001", -911, "transaction rolled back"),
        ErrorPhase::Commit,
    );
    let connection = driver_error(
        &diagnostic(*b"08001", -30081, "transport detail"),
        ErrorPhase::Connect,
    );

    assert_eq!(serialization.category, ErrorCategory::Transient);
    assert_eq!(serialization.retry, RetryDisposition::Safe);
    assert_eq!(serialization.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(connection.category, ErrorCategory::Io);
    assert_eq!(connection.retry, RetryDisposition::Never);
    assert_eq!(connection.remote_effect, RemoteEffect::None);
}

#[test]
fn transport_and_worker_failures_during_mutation_require_recovery() {
    let transport = driver_error(
        &diagnostic(*b"08001", -30081, "transport detail"),
        ErrorPhase::Write,
    );
    let worker = task_error(ErrorPhase::Rollback);

    for error in [&transport, &worker] {
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    }
    assert_eq!(
        task_error(ErrorPhase::Read).remote_effect,
        RemoteEffect::None
    );
}

#[test]
fn db2_security_processing_codes_override_the_transport_sqlstate() {
    for native in [-30082, -30083] {
        let error = driver_error(
            &diagnostic(*b"08001", native, "username password secret"),
            ErrorPhase::Connect,
        );
        assert_eq!(error.category, ErrorCategory::Authentication);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert!(!error.message.contains("username"));
        assert!(!error.message.contains("password"));
        assert!(!error.message.contains("secret"));
    }
}

#[test]
fn odbc_missing_object_alias_is_not_misclassified_as_schema() {
    for state in [*b"S0002", *b"42S02"] {
        let error = driver_error(
            &diagnostic(state, -204, "private schema object name"),
            ErrorPhase::Read,
        );

        assert_eq!(error.category, ErrorCategory::NotFound);
        assert!(!error.message.contains("private"));
        assert!(!error.message.contains("object name"));
    }
}

#[test]
fn cancellation_and_deadline_keep_distinct_public_categories() {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancellation_error = interruption_error(&cancelled, ErrorPhase::Read);
    assert_eq!(cancellation_error.category, ErrorCategory::Cancelled);

    let deadline = CancellationToken::with_deadline(std::time::Instant::now());
    let deadline_error = interruption_error(&deadline, ErrorPhase::Read);
    assert_eq!(deadline_error.category, ErrorCategory::Timeout);
}
