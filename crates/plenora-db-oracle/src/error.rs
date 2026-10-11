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

/// Codici che dicono che la connessione e persa: ORA-03113 (fine del
/// canale), ORA-03114 (non connesso), ORA-03135 (connessione persa),
/// ORA-12537 (connessione chiusa), ORA-12547 (contatto perso), ORA-12570
/// (errore di lettura del pacchetto), ORA-25408 (chiamata non rieseguibile
/// dopo la perdita della connessione). Non sono un rifiuto del comando: la
/// categoria e `io` e l'esito resta ignoto.
pub const LOST_CONNECTION_CODES: &[u32] = &[3_113, 3_114, 3_135, 12_537, 12_547, 12_570, 25_408];

/// I soli codici ORA che, dopo l'invio, **provano** che il comando non ha
/// avuto effetto. Elenco chiuso: un codice entra solo con la sua
/// motivazione, e ogni altro codice dopo l'invio lascia l'esito ignoto.
///
/// Due gruppi, entrambi rifiuti del singolo comando:
///
/// - **analisi**: il comando e rifiutato durante il parsing o la verifica
///   semantica, prima di eseguire qualsiasi cosa. ORA-00900 (SQL non
///   valido), 00901 (CREATE non valido), 00902 (tipo non valido), 00903
///   (nome di tabella non valido), 00904 (identificatore non valido), 00905
///   (parola chiave mancante), 00906/00907 (parentesi mancante), 00911
///   (carattere non valido), 00917 (virgola mancante), 00918 (colonna
///   ambigua), 00921 (fine inattesa), 00922 (opzione non valida), 00923
///   (FROM mancante), 00925/00926 (INTO/VALUES mancante), 00927 (uguale
///   mancante), 00928 (SELECT mancante), 00933 (comando non terminato),
///   00936 (espressione mancante), 00942 (tabella o vista inesistente),
///   00955 (nome gia usato), 00957 (colonna duplicata), 01031 (privilegi
///   insufficienti);
/// - **esecuzione con rollback dell'istruzione**: Oracle annulla l'intera
///   istruzione che fallisce (atomicita a livello di istruzione, «Oracle
///   Database Concepts», *Statement-Level Rollback*), quindi le righe
///   dell'istruzione non restano. ORA-00001 (vincolo di unicita), 00054
///   (risorsa occupata, NOWAIT), 00060 (deadlock: Oracle annulla
///   l'istruzione che lo rileva), 01400 e 01407 (NULL non ammesso), 01438
///   (precisione superata), 01722 (numero non valido), 02290 (vincolo CHECK),
///   02291 e 02292 (integrita referenziale), 08177 (serializzazione),
///   12899 (valore troppo grande per la colonna).
///
/// L'effetto `none` riguarda il comando: le istruzioni precedenti della
/// stessa transazione restano quelle che erano.
pub const CERTAIN_STATEMENT_REJECTIONS: &[u32] = &[
    1, 54, 60, 900, 901, 902, 903, 904, 905, 906, 907, 911, 917, 918, 921, 922, 923, 925, 926, 927,
    928, 933, 936, 942, 955, 957, 1_031, 1_400, 1_407, 1_438, 1_722, 2_290, 2_291, 2_292, 8_177,
    12_899,
];

/// L'errore del driver per un comando **gia inviato**.
///
/// Solo un codice di [`CERTAIN_STATEMENT_REJECTIONS`] prova che il comando
/// non ha avuto effetto. Ogni altro esito — canale chiuso, trasporto
/// interrotto, risposta non interpretabile, codice di connessione persa,
/// codice ORA non qualificato — non lo prova: in una fase mutante l'effetto
/// e ignoto (`DatabaseError::after_interrupted_send`), con categoria `io` per
/// la connessione persa. L'elenco e chiuso come quello del COMMIT.
pub fn sent_command_error(phase: ErrorPhase, error: &Error) -> DatabaseError {
    let mut public = driver_error(phase, error);
    let code = match error {
        Error::OracleError { code, .. } | Error::ServerError { code, .. } => Some(*code),
        _ => None,
    };
    match code {
        Some(code) if CERTAIN_STATEMENT_REJECTIONS.contains(&code) => public,
        Some(code) if LOST_CONNECTION_CODES.contains(&code) => {
            public.category = ErrorCategory::Io;
            public.after_interrupted_send()
        }
        _ => public.after_interrupted_send(),
    }
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

/// L'interruzione rilevata **prima** dell'invio: nessun effetto. Con il
/// comando gia inviato gli assi vengono dalla regola unica del core
/// (`DatabaseError::after_interrupted_send`, o `abandoned_error` per il
/// rollback tentato).
/// `connection::with_timeout_duration` applica la regola al comando in volo.
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
