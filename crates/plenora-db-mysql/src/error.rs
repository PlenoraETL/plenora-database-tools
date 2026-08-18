use mysql_async::Error;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    CancellationReason, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect,
    RetryDisposition,
};

pub fn driver_error(
    profile: &dyn crate::profile::ProductProfile,
    error: &Error,
    phase: ErrorPhase,
    requested_effect: RemoteEffect,
) -> DatabaseError {
    let product = profile.product();
    let code = server_code(error);
    let (category, retry, message) = if is_tls_identity_rejection(error) {
        (
            ErrorCategory::Protocol,
            RetryDisposition::Never,
            format!("verifica identita TLS {product} rifiutata"),
        )
    } else {
        // `map_or_else` con due chiusure lunghe qui leggerebbe peggio, ma
        // clippy ha ragione sulla forma: il ramo `Some` e una chiamata sola.
        code.map_or_else(
            || match error {
                Error::Io(_) => (
                    ErrorCategory::Io,
                    RetryDisposition::Never,
                    format!("errore I/O protocollo {product} redatto"),
                ),
                Error::Driver(_) => (
                    ErrorCategory::Protocol,
                    RetryDisposition::Never,
                    format!("errore driver {product} redatto"),
                ),
                Error::Url(_) => (
                    ErrorCategory::InvalidConfiguration,
                    RetryDisposition::Never,
                    format!("configurazione endpoint {product} non valida"),
                ),
                Error::Other(_) => (
                    ErrorCategory::Protocol,
                    RetryDisposition::Never,
                    format!("errore TLS o protocollo {product} redatto"),
                ),
                Error::Server(server) => profile.classify_server_code(server.code),
            },
            |native| profile.classify_server_code(native),
        )
    };
    let ambiguous = has_ambiguous_effect(code, phase);
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
        provider: Some(profile.kind()),
        execution_id: None,
        message,
        diagnostics: None,
    }
}

fn is_tls_identity_rejection(error: &Error) -> bool {
    // Solo le varianti di trasporto/TLS possono portare un rifiuto di
    // identita TLS: gli errori Server restano classificati dal codice.
    if !matches!(error, Error::Io(_) | Error::Other(_)) {
        return false;
    }
    // Evidenza concreta: il rifiuto di identita rustls/webpki compare in un
    // singolo messaggio della catena, non come parole sparse fra i livelli.
    let mut chain = Some(error as &dyn std::error::Error);
    while let Some(current) = chain {
        let message = current.to_string().to_ascii_lowercase();
        if message.contains("certificate") && message.contains("not valid for name") {
            return true;
        }
        chain = current.source();
    }
    false
}

pub const fn server_code(error: &Error) -> Option<u16> {
    match error {
        Error::Server(server) => Some(server.code),
        _ => None,
    }
}

const fn has_ambiguous_effect(code: Option<u16>, phase: ErrorPhase) -> bool {
    code.is_none()
        && matches!(
            phase,
            ErrorPhase::Write | ErrorPhase::Commit | ErrorPhase::Rollback
        )
}

pub fn timeout_error(kind: ProviderKind, phase: ErrorPhase, effect: RemoteEffect) -> DatabaseError {
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
        provider: Some(kind),
        execution_id: None,
        message: if phase == ErrorPhase::Connect {
            "timeout connessione MySQL prima della creazione della sessione".to_owned()
        } else {
            "timeout operazione MySQL; connessione quarantinata".to_owned()
        },
        diagnostics: None,
    }
}

pub fn cancellation_error(
    kind: ProviderKind,
    phase: ErrorPhase,
    effect: RemoteEffect,
) -> DatabaseError {
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
        provider: Some(kind),
        execution_id: None,
        message: if phase == ErrorPhase::Connect {
            "connessione MySQL cancellata prima della creazione della sessione".to_owned()
        } else {
            "operazione MySQL cancellata; connessione quarantinata".to_owned()
        },
        diagnostics: None,
    }
}

pub fn interruption_error(
    kind: ProviderKind,
    cancellation: &CancellationToken,
    phase: ErrorPhase,
    effect: RemoteEffect,
) -> DatabaseError {
    if cancellation.reason() == Some(CancellationReason::Deadline) {
        timeout_error(kind, phase, effect)
    } else {
        cancellation_error(kind, phase, effect)
    }
}

#[cfg(test)]
mod tests {
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
        let timeout = timeout_error(ProviderKind::Mysql, ErrorPhase::Connect, RemoteEffect::None);
        assert_eq!(timeout.category, ErrorCategory::Timeout);
        assert!(
            !timeout.message.contains("quarantin"),
            "pre-sessione non esiste connessione da quarantinare: {}",
            timeout.message
        );

        let cancelled =
            cancellation_error(ProviderKind::Mysql, ErrorPhase::Connect, RemoteEffect::None);
        assert_eq!(cancelled.category, ErrorCategory::Cancelled);
        assert!(
            !cancelled.message.contains("quarantin"),
            "pre-sessione non esiste connessione da quarantinare: {}",
            cancelled.message
        );
    }

    #[test]
    fn in_flight_timeout_still_reports_quarantine() {
        let error = timeout_error(ProviderKind::Mysql, ErrorPhase::Read, RemoteEffect::None);
        assert!(error.message.contains("quarantinata"));
    }

    #[test]
    fn deadline_and_requested_cancellation_have_distinct_envelopes() {
        let deadline = CancellationToken::new();
        deadline.cancel_due_to_deadline();
        assert_eq!(
            interruption_error(
                ProviderKind::Mysql,
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
                ProviderKind::Mysql,
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
        let error = timeout_error(ProviderKind::Mysql, ErrorPhase::Commit, RemoteEffect::None);
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    }

    #[test]
    fn read_cancellation_is_non_retryable_and_effect_free() {
        let error = cancellation_error(ProviderKind::Mysql, ErrorPhase::Read, RemoteEffect::None);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
    }
}
