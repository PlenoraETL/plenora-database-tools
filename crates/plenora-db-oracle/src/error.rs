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
            let (category, message) = classify_driver_error(error);
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
                message: message.to_owned(),
                diagnostics: None,
            }
        },
        |code| oracle_code_error(phase, code),
    )
}

/// Classifica la variante del driver senza copiarne i campi, che possono
/// contenere SQL, nomi remoti o altri dati non adatti all'errore pubblico.
fn classify_driver_error(error: &Error) -> (ErrorCategory, &'static str) {
    match error {
        Error::InvalidPacketType(_)
        | Error::InvalidMessageType(_)
        | Error::PacketTooShort { .. }
        | Error::UnexpectedPacketType { .. }
        | Error::ProtocolVersionNotSupported(_, _)
        | Error::Protocol(_)
        | Error::ProtocolError(_)
        | Error::BufferUnderflow { .. }
        | Error::BufferOverflow { .. }
        | Error::InvalidLengthIndicator(_)
        | Error::ConnectionNotReady
        | Error::CursorClosed
        | Error::InvalidCursor(_) => (ErrorCategory::Protocol, "protocollo Oracle non completato"),
        Error::ConnectionRefused { .. } => (
            ErrorCategory::Io,
            "listener Oracle non ha accettato la connessione",
        ),
        Error::ConnectionRedirected { .. }
        | Error::ConnectionRedirect(_)
        | Error::InvalidConnectionString(_)
        | Error::InvalidServiceName { .. }
        | Error::InvalidSid { .. } => (
            ErrorCategory::Io,
            "instradamento connessione Oracle non completato",
        ),
        Error::ConnectionClosed | Error::ConnectionClosedByServer(_) => {
            (ErrorCategory::Io, "connessione Oracle chiusa dal peer")
        }
        Error::ConnectionTimeout(_) => (ErrorCategory::Timeout, "connessione Oracle in timeout"),
        Error::Io(error) => match error.kind() {
            std::io::ErrorKind::InvalidData => (ErrorCategory::Io, invalid_tls_message(error)),
            std::io::ErrorKind::UnexpectedEof => (
                ErrorCategory::Io,
                "trasporto Oracle terminato prematuramente",
            ),
            std::io::ErrorKind::ConnectionReset => {
                (ErrorCategory::Io, "trasporto Oracle reimpostato dal peer")
            }
            _ => (ErrorCategory::Io, "trasporto Oracle non completato"),
        },
        Error::AuthenticationFailed(_)
        | Error::InvalidCredentials
        | Error::UnsupportedVerifierType(_) => (
            ErrorCategory::Authentication,
            "autenticazione Oracle non completata",
        ),
        Error::InvalidDataType(_)
        | Error::InvalidOracleType(_)
        | Error::DataConversionError(_)
        | Error::UnexpectedNull => (
            ErrorCategory::DataMapping,
            "conversione dati Oracle non completata",
        ),
        Error::NoDataFound => (ErrorCategory::NotFound, "dato Oracle non trovato"),
        Error::FeatureNotSupported(_) | Error::NativeNetworkEncryptionRequired => (
            ErrorCategory::Unsupported,
            "funzionalita Oracle non supportata",
        ),
        Error::Internal(_) => (
            ErrorCategory::Internal,
            "inizializzazione Oracle non completata",
        ),
        Error::SqlError(_) | Error::OracleError { .. } | Error::ServerError { .. } => {
            (ErrorCategory::Execution, "operazione Oracle non completata")
        }
    }
}

fn invalid_tls_message(error: &std::io::Error) -> &'static str {
    let source = error.to_string();
    if source.contains("InvalidContentType") || source.contains("invalid content type") {
        return "record TLS Oracle con content type non valido";
    }
    if source.contains("certificate") || source.contains("Certificate") {
        return "certificato TLS Oracle non verificato";
    }
    if source.contains("alert") || source.contains("Alert") {
        return "alert TLS ricevuto dal peer Oracle";
    }
    match error
        .get_ref()
        .and_then(|source| source.downcast_ref::<rustls::Error>())
    {
        Some(
            rustls::Error::InvalidCertificate(_)
            | rustls::Error::InvalidCertRevocationList(_)
            | rustls::Error::NoCertificatesPresented
            | rustls::Error::UnsupportedNameType,
        ) => "certificato TLS Oracle non verificato",
        Some(
            rustls::Error::InvalidMessage(_)
            | rustls::Error::InappropriateMessage { .. }
            | rustls::Error::InappropriateHandshakeMessage { .. },
        ) => "record TLS Oracle non valido",
        Some(
            rustls::Error::AlertReceived(_)
            | rustls::Error::PeerIncompatible(_)
            | rustls::Error::PeerMisbehaved(_),
        ) => "handshake TLS Oracle rifiutato dal peer",
        Some(rustls::Error::HandshakeNotComplete) => "handshake TLS Oracle rimasto incompleto",
        Some(rustls::Error::DecryptError | rustls::Error::EncryptError) => {
            "cifratura TLS Oracle non completata"
        }
        Some(rustls::Error::PeerSentOversizedRecord) => "record TLS Oracle oltre il limite",
        Some(rustls::Error::NoApplicationProtocol) => {
            "protocollo applicativo TLS Oracle non negoziato"
        }
        Some(
            rustls::Error::General(_)
            | rustls::Error::Other(_)
            | rustls::Error::FailedToGetCurrentTime
            | rustls::Error::FailedToGetRandomBytes
            | rustls::Error::BadMaxFragmentSize
            | rustls::Error::InconsistentKeys(_)
            | rustls::Error::InvalidEncryptedClientHello(_),
        ) => "configurazione TLS Oracle non completata",
        Some(_) | None => "errore TLS Oracle non classificato",
    }
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
