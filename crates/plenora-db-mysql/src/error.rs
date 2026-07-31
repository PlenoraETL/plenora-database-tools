use mysql_async::Error;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};

pub fn driver_error(
    error: &Error,
    phase: ErrorPhase,
    requested_effect: RemoteEffect,
) -> DatabaseError {
    let code = match error {
        Error::Server(server) => Some(server.code),
        _ => None,
    };
    let (category, retry, message) = match code {
        Some(1_045) => (
            ErrorCategory::Authentication,
            RetryDisposition::Never,
            "autenticazione MySQL rifiutata (codice 1045)".to_owned(),
        ),
        Some(1_044) => (
            ErrorCategory::Authorization,
            RetryDisposition::Never,
            "autorizzazione MySQL negata (codice 1044)".to_owned(),
        ),
        Some(1_049 | 1_146) => (
            ErrorCategory::NotFound,
            RetryDisposition::Never,
            format!(
                "database o oggetto MySQL non trovato (codice {})",
                code.unwrap_or_default()
            ),
        ),
        Some(1_054) => (
            ErrorCategory::Schema,
            RetryDisposition::Never,
            "colonna MySQL non valida (codice 1054)".to_owned(),
        ),
        Some(1_062) => (
            ErrorCategory::Conflict,
            RetryDisposition::Never,
            "vincolo univoco MySQL violato (codice 1062)".to_owned(),
        ),
        Some(1_213) => (
            ErrorCategory::Transient,
            RetryDisposition::Safe,
            "deadlock MySQL; transazione vittima annullata".to_owned(),
        ),
        Some(1_205 | 3_024) => (
            ErrorCategory::Timeout,
            RetryDisposition::Never,
            format!("timeout MySQL (codice {})", code.unwrap_or_default()),
        ),
        Some(native) => (
            ErrorCategory::Execution,
            RetryDisposition::Never,
            format!("errore server MySQL redatto (codice {native})"),
        ),
        None => match error {
            Error::Io(_) => (
                ErrorCategory::Io,
                RetryDisposition::Never,
                "errore I/O protocollo MySQL redatto".to_owned(),
            ),
            Error::Driver(_) => (
                ErrorCategory::Protocol,
                RetryDisposition::Never,
                "errore driver MySQL redatto".to_owned(),
            ),
            Error::Url(_) => (
                ErrorCategory::InvalidConfiguration,
                RetryDisposition::Never,
                "configurazione endpoint MySQL non valida".to_owned(),
            ),
            Error::Other(_) => (
                ErrorCategory::Protocol,
                RetryDisposition::Never,
                "errore TLS o protocollo MySQL redatto".to_owned(),
            ),
            Error::Server(_) => unreachable!("server error always has a code"),
        },
    };
    let ambiguous = code.is_none()
        && matches!(
            phase,
            ErrorPhase::Write | ErrorPhase::Commit | ErrorPhase::Rollback
        );
    DatabaseError {
        category,
        phase,
        remote_effect: if code == Some(1_213) {
            RemoteEffect::RolledBack
        } else if ambiguous {
            RemoteEffect::Unknown
        } else {
            requested_effect
        },
        retry: if ambiguous {
            RetryDisposition::RequiresRecovery
        } else {
            retry
        },
        provider: Some(ProviderKind::Mysql),
        execution_id: None,
        message,
    }
}

pub fn timeout_error(phase: ErrorPhase, effect: RemoteEffect) -> DatabaseError {
    let ambiguous = matches!(
        phase,
        ErrorPhase::Write | ErrorPhase::Commit | ErrorPhase::Rollback
    );
    DatabaseError {
        category: ErrorCategory::Timeout,
        phase,
        remote_effect: if ambiguous {
            RemoteEffect::Unknown
        } else {
            effect
        },
        retry: if ambiguous {
            RetryDisposition::RequiresRecovery
        } else {
            RetryDisposition::Never
        },
        provider: Some(ProviderKind::Mysql),
        execution_id: None,
        message: "timeout operazione MySQL; connessione quarantinata".to_owned(),
    }
}

pub fn cancellation_error(phase: ErrorPhase, effect: RemoteEffect) -> DatabaseError {
    let ambiguous = matches!(
        phase,
        ErrorPhase::Write | ErrorPhase::Commit | ErrorPhase::Rollback
    );
    DatabaseError {
        category: ErrorCategory::Cancelled,
        phase,
        remote_effect: if ambiguous {
            RemoteEffect::Unknown
        } else {
            effect
        },
        retry: if ambiguous {
            RetryDisposition::RequiresRecovery
        } else {
            RetryDisposition::Never
        },
        provider: Some(ProviderKind::Mysql),
        execution_id: None,
        message: "operazione MySQL cancellata; connessione quarantinata".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_timeout_never_claims_rollback() {
        let error = timeout_error(ErrorPhase::Commit, RemoteEffect::None);
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    }

    #[test]
    fn read_cancellation_is_non_retryable_and_effect_free() {
        let error = cancellation_error(ErrorPhase::Read, RemoteEffect::None);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
    }
}
