use oracle_rs::Error;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    interruption_category, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase,
    RemoteEffect, RetryDisposition,
};

/// Traduce gli errori del driver senza copiarne il testo: i messaggi Oracle
/// possono contenere SQL, identificatori o valori e non sono una superficie
/// pubblica sicura.
pub fn driver_error(phase: ErrorPhase, error: &Error) -> DatabaseError {
    let code = match error {
        Error::OracleError { code, .. } | Error::ServerError { code, .. } => Some(*code),
        Error::ConnectionRefused { error_code, .. } => *error_code,
        _ => None,
    };
    code.map_or_else(
        || {
            let category = if error.is_connection_error() {
                ErrorCategory::Io
            } else {
                ErrorCategory::Execution
            };
            DatabaseError {
                category,
                phase,
                remote_effect: RemoteEffect::None,
                retry: if category == ErrorCategory::Io {
                    RetryDisposition::Safe
                } else {
                    RetryDisposition::Never
                },
                provider: Some(ProviderKind::Oracle),
                execution_id: None,
                message: "operazione Oracle non completata".to_owned(),
                diagnostics: None,
            }
        },
        |code| oracle_code_error(phase, code),
    )
}

pub fn oracle_code_error(phase: ErrorPhase, code: u32) -> DatabaseError {
    let category = match code {
        1 => ErrorCategory::Conflict,
        54 | 60 | 8177 => ErrorCategory::Transient,
        1017 => ErrorCategory::Authentication,
        942 => ErrorCategory::NotFound,
        1031 => ErrorCategory::Authorization,
        12170 => ErrorCategory::Timeout,
        12514 | 12505 => ErrorCategory::InvalidConfiguration,
        _ => ErrorCategory::Execution,
    };
    let retry = match category {
        ErrorCategory::Transient | ErrorCategory::Io | ErrorCategory::Timeout => {
            RetryDisposition::Safe
        }
        _ => RetryDisposition::Never,
    };
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry,
        provider: Some(ProviderKind::Oracle),
        execution_id: None,
        message: format!("Oracle ha rifiutato l'operazione (codice ORA-{code:05})"),
        diagnostics: None,
    }
}

pub fn interruption_error(cancellation: &CancellationToken, phase: ErrorPhase) -> DatabaseError {
    DatabaseError {
        category: interruption_category(cancellation),
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Oracle),
        execution_id: None,
        message: "operazione Oracle interrotta".to_owned(),
        diagnostics: None,
    }
}
