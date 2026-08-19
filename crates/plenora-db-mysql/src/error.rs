use crate::profile::ServerCodeVerdict;
use mysql_async::Error;
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
    let verdict = if is_tls_identity_rejection(error) {
        ServerCodeVerdict {
            category: ErrorCategory::Protocol,
            retry: RetryDisposition::Never,
            message: format!("verifica identita TLS {product} rifiutata"),
            remote_effect: None,
        }
    } else {
        // `map_or_else` con due chiusure lunghe qui leggerebbe peggio, ma
        // clippy ha ragione sulla forma: il ramo `Some` e una chiamata sola.
        code.map_or_else(
            || match error {
                Error::Io(_) => ServerCodeVerdict {
                    category: ErrorCategory::Io,
                    retry: RetryDisposition::Never,
                    message: format!("errore I/O protocollo {product} redatto"),
                    remote_effect: None,
                },
                Error::Driver(_) => ServerCodeVerdict {
                    category: ErrorCategory::Protocol,
                    retry: RetryDisposition::Never,
                    message: format!("errore driver {product} redatto"),
                    remote_effect: None,
                },
                Error::Url(_) => ServerCodeVerdict {
                    category: ErrorCategory::InvalidConfiguration,
                    retry: RetryDisposition::Never,
                    message: format!("configurazione endpoint {product} non valida"),
                    remote_effect: None,
                },
                Error::Other(_) => ServerCodeVerdict {
                    category: ErrorCategory::Protocol,
                    retry: RetryDisposition::Never,
                    message: format!("errore TLS o protocollo {product} redatto"),
                    remote_effect: None,
                },
                Error::Server(server) => profile.classify_server_code(server.code),
            },
            |native| profile.classify_server_code(native),
        )
    };
    let ambiguous = has_ambiguous_effect(code, phase);
    DatabaseError {
        category: verdict.category,
        phase,
        // L'effetto che il codice dichiara vince: e cio che il server dice di
        // aver fatto, e nessuna euristica locale lo sa meglio.
        remote_effect: verdict.remote_effect.unwrap_or(if ambiguous {
            RemoteEffect::Unknown
        } else {
            requested_effect
        }),
        retry: if ambiguous {
            RetryDisposition::RequiresRecovery
        } else {
            verdict.retry
        },
        provider: Some(profile.kind()),
        execution_id: None,
        message: verdict.message,
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

pub fn timeout_error(
    profile: &dyn crate::profile::ProductProfile,
    phase: ErrorPhase,
    effect: RemoteEffect,
) -> DatabaseError {
    let product = profile.product();
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
        provider: Some(profile.kind()),
        execution_id: None,
        message: if phase == ErrorPhase::Connect {
            format!("timeout connessione {product} prima della creazione della sessione")
        } else {
            format!("timeout operazione {product}; connessione quarantinata")
        },
        diagnostics: None,
    }
}

pub fn cancellation_error(
    profile: &dyn crate::profile::ProductProfile,
    phase: ErrorPhase,
    effect: RemoteEffect,
) -> DatabaseError {
    let product = profile.product();
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
        provider: Some(profile.kind()),
        execution_id: None,
        message: if phase == ErrorPhase::Connect {
            format!("connessione {product} cancellata prima della creazione della sessione")
        } else {
            format!("operazione {product} cancellata; connessione quarantinata")
        },
        diagnostics: None,
    }
}

pub fn interruption_error(
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
    phase: ErrorPhase,
    effect: RemoteEffect,
) -> DatabaseError {
    if cancellation.reason() == Some(CancellationReason::Deadline) {
        timeout_error(profile, phase, effect)
    } else {
        cancellation_error(profile, phase, effect)
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
}
