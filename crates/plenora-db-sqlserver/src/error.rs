use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};
use tiberius::error::Error;

pub fn driver_error(
    error: &Error,
    phase: ErrorPhase,
    requested_effect: RemoteEffect,
) -> DatabaseError {
    let code = error.code();
    let (category, retry, message) = match code {
        Some(1_205) => (
            ErrorCategory::Transient,
            RetryDisposition::Safe,
            "deadlock SQL Server; transazione vittima annullata".to_owned(),
        ),
        Some(1_222) => (
            ErrorCategory::Timeout,
            RetryDisposition::Never,
            "timeout lock SQL Server".to_owned(),
        ),
        Some(2_601 | 2_627 | 547) => (
            ErrorCategory::Conflict,
            RetryDisposition::Never,
            format!(
                "vincolo SQL Server violato (codice {})",
                code.unwrap_or_default()
            ),
        ),
        Some(207) => (
            ErrorCategory::Schema,
            RetryDisposition::Never,
            "colonna SQL Server non valida (codice 207)".to_owned(),
        ),
        Some(208) => (
            ErrorCategory::NotFound,
            RetryDisposition::Never,
            "oggetto SQL Server non trovato (codice 208)".to_owned(),
        ),
        Some(229 | 230) => (
            ErrorCategory::Authorization,
            RetryDisposition::Never,
            format!(
                "autorizzazione SQL Server negata (codice {})",
                code.unwrap_or_default()
            ),
        ),
        Some(4_060 | 18_456) => (
            ErrorCategory::Authentication,
            RetryDisposition::Never,
            format!(
                "autenticazione o database SQL Server rifiutati (codice {})",
                code.unwrap_or_default()
            ),
        ),
        Some(native) => (
            ErrorCategory::Execution,
            RetryDisposition::Never,
            format!("errore server SQL Server redatto (codice {native})"),
        ),
        None => classify_transport(error, phase),
    };

    let transport_effect_unknown = code.is_none()
        && (phase == ErrorPhase::Commit || requested_effect == RemoteEffect::Unknown);
    DatabaseError {
        category,
        phase,
        remote_effect: if transport_effect_unknown {
            RemoteEffect::Unknown
        } else if code == Some(1_205) {
            RemoteEffect::RolledBack
        } else {
            requested_effect
        },
        retry: if transport_effect_unknown {
            RetryDisposition::RequiresRecovery
        } else {
            retry
        },
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message,
    }
}

fn classify_transport(
    error: &Error,
    phase: ErrorPhase,
) -> (ErrorCategory, RetryDisposition, String) {
    match error {
        Error::Io {
            kind: std::io::ErrorKind::InvalidData,
            ..
        } if phase == ErrorPhase::Connect => (
            ErrorCategory::Authentication,
            RetryDisposition::Never,
            "verifica certificato TLS SQL Server fallita".to_owned(),
        ),
        Error::Io { .. } => (
            ErrorCategory::Io,
            RetryDisposition::Never,
            "errore I/O TDS SQL Server redatto".to_owned(),
        ),
        Error::Tls(_) => (
            ErrorCategory::Authentication,
            RetryDisposition::Never,
            "handshake TLS SQL Server fallito".to_owned(),
        ),
        Error::Routing { .. } => (
            ErrorCategory::Unsupported,
            RetryDisposition::Never,
            "redirect SQL Server/Azure non ancora verificato".to_owned(),
        ),
        Error::Conversion(_) | Error::Encoding(_) | Error::Utf8 | Error::Utf16 => (
            ErrorCategory::DataMapping,
            RetryDisposition::Never,
            "conversione TDS SQL Server fallita".to_owned(),
        ),
        Error::BulkInput(_) => (
            ErrorCategory::DataMapping,
            RetryDisposition::Never,
            "input bulk SQL Server non valido".to_owned(),
        ),
        Error::Protocol(_) | Error::ParseInt(_) => (
            ErrorCategory::Protocol,
            RetryDisposition::Never,
            "errore protocollo TDS SQL Server redatto".to_owned(),
        ),
        Error::Server(_) => (
            ErrorCategory::Protocol,
            RetryDisposition::Never,
            "errore server TDS privo di codice classificabile".to_owned(),
        ),
        #[allow(unreachable_patterns)]
        _ => (
            ErrorCategory::Protocol,
            RetryDisposition::Never,
            "errore TDS SQL Server redatto".to_owned(),
        ),
    }
}

pub fn timeout_error(phase: ErrorPhase, remote_effect: RemoteEffect) -> DatabaseError {
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
            remote_effect
        },
        retry: if ambiguous {
            RetryDisposition::RequiresRecovery
        } else {
            RetryDisposition::Never
        },
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: "timeout operazione SQL Server".to_owned(),
    }
}

pub fn cancellation_error(phase: ErrorPhase, remote_effect: RemoteEffect) -> DatabaseError {
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
            remote_effect
        },
        retry: if ambiguous {
            RetryDisposition::RequiresRecovery
        } else {
            RetryDisposition::Never
        },
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: "operazione SQL Server cancellata; connessione quarantinata".to_owned(),
    }
}

#[cfg(test)]
mod tests {
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
}
