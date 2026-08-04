use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

pub fn check_cancelled(cancellation: &CancellationToken, phase: ErrorPhase) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(public_error(
            ErrorCategory::Cancelled,
            phase,
            false,
            "operazione cancellata",
        ))
    } else {
        Ok(())
    }
}

pub fn row_decode_error(_: tokio_postgres::Error) -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        false,
        "valore PostgreSQL non convertibile nel tipo Arrow",
    )
}

pub fn classify_error(phase: ErrorPhase, error: &tokio_postgres::Error) -> DatabaseError {
    let (category, retryable, message) =
        match error.code().map(tokio_postgres::error::SqlState::code) {
            Some("28P01") => (
                ErrorCategory::Authentication,
                false,
                "autenticazione PostgreSQL fallita",
            ),
            Some("42501") => (
                ErrorCategory::Authorization,
                false,
                "permesso PostgreSQL insufficiente",
            ),
            Some("42P01" | "42703" | "3F000") => (
                ErrorCategory::NotFound,
                false,
                "oggetto PostgreSQL non trovato",
            ),
            Some("40001" | "40P01" | "55P03") => (
                ErrorCategory::Transient,
                true,
                "conflitto PostgreSQL transitorio",
            ),
            Some("57014") => (
                ErrorCategory::Cancelled,
                false,
                "operazione PostgreSQL cancellata",
            ),
            _ if error.is_closed() => (
                ErrorCategory::Transient,
                true,
                "connessione PostgreSQL chiusa",
            ),
            _ => (
                ErrorCategory::Protocol,
                false,
                "operazione PostgreSQL fallita",
            ),
        };
    public_error(category, phase, retryable, message)
}

pub fn public_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    retryable: bool,
    message: &str,
) -> DatabaseError {
    public_error_envelope(
        category,
        phase,
        RemoteEffect::None,
        if retryable {
            RetryDisposition::Safe
        } else {
            RetryDisposition::Never
        },
        message,
    )
}

pub fn public_error_envelope(
    category: ErrorCategory,
    phase: ErrorPhase,
    remote_effect: RemoteEffect,
    retry: RetryDisposition,
    message: &str,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect,
        retry,
        provider: Some(ProviderKind::Postgres),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}
