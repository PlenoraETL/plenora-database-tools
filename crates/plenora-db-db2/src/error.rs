use odbc_api::Error as OdbcError;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    interruption_category, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase,
    RemoteEffect, RetryDisposition,
};

pub fn driver_error(error: &OdbcError, phase: ErrorPhase) -> DatabaseError {
    let (category, mut retry, message) = match error {
        OdbcError::Diagnostics { record, .. } => {
            let state = record.state.as_str();
            let category = match (state, record.native_error) {
                (_, -30082 | -30083) | ("28000", _) => ErrorCategory::Authentication,
                ("42501", _) => ErrorCategory::Authorization,
                ("40001", _) => ErrorCategory::Transient,
                ("23502" | "23503" | "23505", _) => ErrorCategory::Conflict,
                ("42703", _) => ErrorCategory::Schema,
                ("42704" | "42S02" | "S0002", _) => ErrorCategory::NotFound,
                ("57014" | "HYT00" | "HYT01", _) => ErrorCategory::Timeout,
                (value, _) if value.starts_with("08") => ErrorCategory::Io,
                (value, _) if value.starts_with("22") => ErrorCategory::DataMapping,
                (value, _) if value.starts_with("42") => ErrorCategory::Schema,
                _ => ErrorCategory::Execution,
            };
            let retry = if state == "40001" {
                RetryDisposition::Safe
            } else {
                RetryDisposition::Never
            };
            (
                category,
                retry,
                format!(
                    "errore Db2 redatto (SQLSTATE {}, codice {})",
                    state, record.native_error
                ),
            )
        }
        OdbcError::FailedAllocatingEnvironment | OdbcError::UnsupportedOdbcApiVersion(_) => (
            ErrorCategory::InvalidConfiguration,
            RetryDisposition::Never,
            "runtime ODBC Db2 non disponibile".to_owned(),
        ),
        OdbcError::FailedReadingInput(_) => (
            ErrorCategory::Io,
            RetryDisposition::Never,
            "lettura input ODBC Db2 fallita".to_owned(),
        ),
        _ => (
            ErrorCategory::Protocol,
            RetryDisposition::Never,
            "errore ODBC Db2 redatto".to_owned(),
        ),
    };
    let remote_effect = match error {
        OdbcError::Diagnostics { record, .. } if record.state.as_str() == "40001" => {
            RemoteEffect::RolledBack
        }
        OdbcError::Diagnostics { record, .. }
            if record.state.as_str() == "40003"
                || (record.state.as_str().starts_with("08") && mutating_phase(phase)) =>
        {
            RemoteEffect::Unknown
        }
        _ if matches!(phase, ErrorPhase::Commit | ErrorPhase::Rollback) => RemoteEffect::Unknown,
        _ if mutating_phase(phase) && !matches!(error, OdbcError::Diagnostics { .. }) => {
            RemoteEffect::Unknown
        }
        _ => RemoteEffect::None,
    };
    if remote_effect == RemoteEffect::Unknown {
        retry = RetryDisposition::RequiresRecovery;
    }
    DatabaseError {
        category,
        phase,
        remote_effect,
        retry,
        provider: Some(ProviderKind::Db2),
        execution_id: None,
        message,
        diagnostics: None,
    }
}

pub fn interruption_error(cancellation: &CancellationToken, phase: ErrorPhase) -> DatabaseError {
    let category = interruption_category(cancellation);
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Db2),
        execution_id: None,
        message: if category == ErrorCategory::Timeout {
            "timeout operazione Db2".to_owned()
        } else {
            "operazione Db2 cancellata".to_owned()
        },
        diagnostics: None,
    }
}

pub fn task_error(phase: ErrorPhase) -> DatabaseError {
    let ambiguous = mutating_phase(phase);
    DatabaseError {
        category: ErrorCategory::Protocol,
        phase,
        remote_effect: if ambiguous {
            RemoteEffect::Unknown
        } else {
            RemoteEffect::None
        },
        retry: if ambiguous {
            RetryDisposition::RequiresRecovery
        } else {
            RetryDisposition::Never
        },
        provider: Some(ProviderKind::Db2),
        execution_id: None,
        message: "worker ODBC Db2 terminato in modo anomalo".to_owned(),
        diagnostics: None,
    }
}

const fn mutating_phase(phase: ErrorPhase) -> bool {
    matches!(
        phase,
        ErrorPhase::Write | ErrorPhase::Commit | ErrorPhase::Rollback
    )
}
