use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

/// La categoria pubblica di un'interruzione, secondo la causa che l'ha
/// prodotta.
///
/// Una deadline scaduta e un `Timeout`, tutto il resto e una `Cancelled`. La
/// distinzione vive nel token da sempre, ma solo due superfici la leggevano —
/// `read_stream` e `write::recovery` — mentre le altre costruivano
/// `ErrorCategory::Cancelled` a mano. La stessa scadenza usciva quindi come
/// `Timeout` o come `Cancelled` a seconda di quale strato la osservava per
/// primo, e un chiamante che decide se allungare il budget o smettere riceveva
/// risposte diverse dallo stesso evento.
pub use plenora_database_core::interruption_category;

/// Interrompe **prima** di toccare la rete, dicendo quale causa ha interrotto.
///
/// Una deadline scaduta e un `Timeout`, non una `Cancelled`: la distinzione
/// vive gia nel token, e questa funzione — che sta davanti a diciannove
/// superfici del provider — la buttava via. Lo stesso evento usciva come
/// `Timeout` da `read_stream` e da `write::recovery`, che consultano
/// `reason()`, e come `Cancelled` da qui: due risposte pubbliche diverse per
/// una sola scadenza, decise da quale strato la osservava per primo.
pub fn check_cancelled(cancellation: &CancellationToken, phase: ErrorPhase) -> Result<()> {
    if cancellation.reason().is_none() {
        return Ok(());
    }
    let category = interruption_category(cancellation);
    Err(public_error(
        category,
        phase,
        false,
        if category == ErrorCategory::Timeout {
            "durata massima operazione PostgreSQL esaurita"
        } else {
            "operazione cancellata"
        },
    ))
}

pub fn row_decode_error(_: tokio_postgres::Error) -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        false,
        "valore PostgreSQL non convertibile nel tipo Arrow",
    )
}

/// Mappa un errore `tokio_postgres` sull'errore pubblico Plenora.
///
/// Il mapping è tabellare per SQLSTATE e dipende dalla `ErrorPhase`: lo stesso
/// codice può produrre effetti remoti diversi a seconda del momento in cui
/// viene osservato (es. una connessione persa in `Commit` è
/// `RemoteEffect::Unknown`, in `Connect` è `RemoteEffect::None`).
///
/// La funzione non espone mai il SQLSTATE nel messaggio pubblico: il codice
/// vendor resta materia dei sink di tracing/metrics.
pub fn classify_error(phase: ErrorPhase, error: &tokio_postgres::Error) -> DatabaseError {
    let sqlstate = error.code().map(tokio_postgres::error::SqlState::code);
    let transport_closed = error.is_closed();
    let mapping = resolve_mapping(sqlstate, transport_closed, phase);
    public_error_envelope(
        mapping.category,
        phase,
        mapping.remote_effect,
        mapping.retry,
        mapping.message,
    )
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

struct Mapping {
    category: ErrorCategory,
    retry: RetryDisposition,
    remote_effect: RemoteEffect,
    message: &'static str,
}

/// Fase durante la quale lo stato remoto può essere alterato: gli errori
/// osservati in queste fasi devono usare `RemoteEffect::Unknown` quando il
/// canale è compromesso, perché non si può escludere un commit lato server.
const fn phase_is_state_mutating(phase: ErrorPhase) -> bool {
    matches!(
        phase,
        ErrorPhase::Write | ErrorPhase::Commit | ErrorPhase::Finalize | ErrorPhase::Rollback
    )
}

fn resolve_mapping(sqlstate: Option<&str>, transport_closed: bool, phase: ErrorPhase) -> Mapping {
    if let Some(code) = sqlstate {
        if let Some(mut mapping) = mapping_for_sqlstate(code) {
            if transport_closed && phase_is_state_mutating(phase) {
                mapping.remote_effect = RemoteEffect::Unknown;
                mapping.retry = RetryDisposition::RequiresRecovery;
            }
            return mapping;
        }
    }

    if transport_closed {
        return Mapping {
            category: ErrorCategory::Io,
            retry: RetryDisposition::Quarantine,
            remote_effect: if phase_is_state_mutating(phase) {
                RemoteEffect::Unknown
            } else {
                RemoteEffect::None
            },
            message: "connessione PostgreSQL chiusa",
        };
    }

    Mapping {
        category: ErrorCategory::Protocol,
        retry: RetryDisposition::Never,
        remote_effect: RemoteEffect::None,
        message: "operazione PostgreSQL fallita",
    }
}

#[allow(clippy::too_many_lines)] // tabella SQLSTATE lineare per mantenere visibili i mapping
fn mapping_for_sqlstate(code: &str) -> Option<Mapping> {
    let mapping = match code {
        // Class 08 — connection exception
        "08000" | "08003" | "08006" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::Safe,
            remote_effect: RemoteEffect::None,
            message: "connessione PostgreSQL interrotta",
        },
        "08001" | "08004" => Mapping {
            category: ErrorCategory::Io,
            retry: RetryDisposition::Safe,
            remote_effect: RemoteEffect::None,
            message: "connessione PostgreSQL non stabilita",
        },
        "08007" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::RequiresRecovery,
            remote_effect: RemoteEffect::Unknown,
            message: "esito transazione PostgreSQL non verificabile",
        },
        "08P01" => Mapping {
            category: ErrorCategory::Protocol,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "violazione protocollo PostgreSQL",
        },

        // Class 0A — feature not supported
        "0A000" => Mapping {
            category: ErrorCategory::Unsupported,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "funzionalità PostgreSQL non supportata",
        },

        // Class 22 — data exception
        "22001" => Mapping {
            category: ErrorCategory::DataMapping,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "valore stringa PostgreSQL troncato",
        },
        "22003" => Mapping {
            category: ErrorCategory::DataMapping,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "valore numerico PostgreSQL fuori intervallo",
        },
        "22007" | "22008" => Mapping {
            category: ErrorCategory::DataMapping,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "valore temporale PostgreSQL non valido",
        },
        "22012" => Mapping {
            category: ErrorCategory::Execution,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "divisione per zero in PostgreSQL",
        },
        "22P02" => Mapping {
            category: ErrorCategory::DataMapping,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "rappresentazione testuale PostgreSQL non valida",
        },
        "2200N" | "2200S" => Mapping {
            category: ErrorCategory::DataMapping,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "documento XML/JSON PostgreSQL non valido",
        },

        // Class 23 — integrity constraint violation
        "23502" => Mapping {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "violazione NOT NULL in PostgreSQL",
        },
        "23503" => Mapping {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "violazione foreign key in PostgreSQL",
        },
        "23505" => Mapping {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "violazione vincolo di unicità in PostgreSQL",
        },
        "23514" => Mapping {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "violazione CHECK in PostgreSQL",
        },
        "23P01" => Mapping {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "violazione vincolo di esclusione in PostgreSQL",
        },

        // Class 25 — invalid transaction state
        "25001" => Mapping {
            category: ErrorCategory::Protocol,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "transazione PostgreSQL già attiva",
        },
        "25006" => Mapping {
            category: ErrorCategory::Authorization,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "transazione PostgreSQL in sola lettura",
        },
        "25P02" => Mapping {
            category: ErrorCategory::Protocol,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "transazione PostgreSQL in stato di errore",
        },

        // Class 28 — invalid authorization specification
        "28000" | "28P01" => Mapping {
            category: ErrorCategory::Authentication,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "autenticazione PostgreSQL fallita",
        },

        // Class 3F — invalid schema name
        "3F000" => Mapping {
            category: ErrorCategory::NotFound,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "schema PostgreSQL non trovato",
        },

        // Class 40 — transaction rollback
        "40001" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::Safe,
            remote_effect: RemoteEffect::RolledBack,
            message: "serializzazione PostgreSQL fallita",
        },
        "40003" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::RequiresRecovery,
            remote_effect: RemoteEffect::Unknown,
            message: "completamento statement PostgreSQL non verificabile",
        },
        "40P01" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::Safe,
            remote_effect: RemoteEffect::RolledBack,
            message: "deadlock PostgreSQL rilevato",
        },

        // Class 42 — syntax / access rule violation
        "42501" => Mapping {
            category: ErrorCategory::Authorization,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "permesso PostgreSQL insufficiente",
        },
        "42601" => Mapping {
            category: ErrorCategory::InvalidPlan,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "sintassi SQL PostgreSQL non valida",
        },
        "42P01" | "42703" => Mapping {
            category: ErrorCategory::NotFound,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "oggetto PostgreSQL non trovato",
        },
        "42P07" => Mapping {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "oggetto PostgreSQL già esistente",
        },
        "42P16" => Mapping {
            category: ErrorCategory::Schema,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "definizione tabella PostgreSQL non valida",
        },
        "428C9" => Mapping {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "colonna generata (GENERATED ALWAYS) non scrivibile: escludila dalla lista colonne dell'INSERT/UPDATE",
        },

        // Class 53 — insufficient resources
        "53100" => Mapping {
            category: ErrorCategory::ResourceLimit,
            retry: RetryDisposition::RequiresRecovery,
            remote_effect: RemoteEffect::Unknown,
            message: "spazio disco PostgreSQL esaurito",
        },
        "53200" => Mapping {
            category: ErrorCategory::ResourceLimit,
            retry: RetryDisposition::Safe,
            remote_effect: RemoteEffect::Unknown,
            message: "memoria PostgreSQL esaurita",
        },
        "53300" => Mapping {
            category: ErrorCategory::ResourceLimit,
            retry: RetryDisposition::After(1_000),
            remote_effect: RemoteEffect::None,
            message: "connessioni PostgreSQL esaurite",
        },
        "53400" => Mapping {
            category: ErrorCategory::ResourceLimit,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::None,
            message: "limite di configurazione PostgreSQL superato",
        },

        // Class 55 — object not in prerequisite state
        "55006" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::After(500),
            remote_effect: RemoteEffect::None,
            message: "oggetto PostgreSQL in uso",
        },
        "55P03" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::Safe,
            remote_effect: RemoteEffect::None,
            message: "lock PostgreSQL non acquisibile",
        },

        // Class 57 — operator intervention
        "57014" => Mapping {
            category: ErrorCategory::Cancelled,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::RolledBack,
            message: "operazione PostgreSQL cancellata",
        },
        "57P01" | "57P02" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::Quarantine,
            remote_effect: RemoteEffect::Unknown,
            message: "shutdown PostgreSQL in corso",
        },
        "57P03" => Mapping {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::After(2_000),
            remote_effect: RemoteEffect::None,
            message: "PostgreSQL non ancora disponibile",
        },

        // Class 58 — system error
        "58000" | "58030" => Mapping {
            category: ErrorCategory::Io,
            retry: RetryDisposition::RequiresRecovery,
            remote_effect: RemoteEffect::Unknown,
            message: "errore di sistema PostgreSQL",
        },

        // Class XX — internal error
        "XX000" | "XX001" | "XX002" => Mapping {
            category: ErrorCategory::Internal,
            retry: RetryDisposition::Never,
            remote_effect: RemoteEffect::Unknown,
            message: "errore interno PostgreSQL",
        },

        _ => return None,
    };
    Some(mapping)
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;

/// Test di integrazione live: forzano ciascun SQLSTATE reale contro un
/// `PostgreSQL` raggiungibile all'hostname `dataflow-postgres`, poi verificano
/// che `classify_error` produca la mappatura attesa. Chiudono il milestone A2.
#[cfg(test)]
#[path = "error_live.rs"]
mod live;
