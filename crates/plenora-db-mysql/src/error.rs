use crate::profile::ServerCodeVerdict;
use mysql_async::Error;
use plenora_database_core::{
    interruption_category, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase,
    RemoteEffect, RetryDisposition,
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
    // La decisione e una sola e sta nel core: qui resta solo la forma
    // dell'errore, che e specifica del prodotto (messaggi, retry, effetto
    // remoto).
    if interruption_category(cancellation) == ErrorCategory::Timeout {
        timeout_error(profile, phase, effect)
    } else {
        cancellation_error(profile, phase, effect)
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
