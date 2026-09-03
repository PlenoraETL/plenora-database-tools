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
