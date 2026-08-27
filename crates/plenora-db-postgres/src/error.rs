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
mod tests {
    use super::*;

    fn map(code: &str, phase: ErrorPhase) -> Mapping {
        resolve_mapping(Some(code), false, phase)
    }

    #[test]
    fn unique_violation_is_conflict_rolled_back() {
        let m = map("23505", ErrorPhase::Write);
        assert_eq!(m.category, ErrorCategory::Conflict);
        assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
        assert!(matches!(m.retry, RetryDisposition::Never));
    }

    #[test]
    fn serialization_failure_is_safe_retry() {
        let m = map("40001", ErrorPhase::Commit);
        assert_eq!(m.category, ErrorCategory::Transient);
        assert!(matches!(m.retry, RetryDisposition::Safe));
        assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
    }

    #[test]
    fn deadlock_is_safe_retry() {
        let m = map("40P01", ErrorPhase::Write);
        assert_eq!(m.category, ErrorCategory::Transient);
        assert!(matches!(m.retry, RetryDisposition::Safe));
        assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
    }

    #[test]
    fn statement_completion_unknown_requires_recovery() {
        let m = map("40003", ErrorPhase::Commit);
        assert_eq!(m.category, ErrorCategory::Transient);
        assert!(matches!(m.retry, RetryDisposition::RequiresRecovery));
        assert_eq!(m.remote_effect, RemoteEffect::Unknown);
    }

    #[test]
    fn insufficient_privilege_is_authorization() {
        let m = map("42501", ErrorPhase::Prepare);
        assert_eq!(m.category, ErrorCategory::Authorization);
        assert_eq!(m.remote_effect, RemoteEffect::None);
    }

    #[test]
    fn undefined_table_is_not_found() {
        let m = map("42P01", ErrorPhase::Prepare);
        assert_eq!(m.category, ErrorCategory::NotFound);
    }

    #[test]
    fn duplicate_table_is_conflict() {
        let m = map("42P07", ErrorPhase::Write);
        assert_eq!(m.category, ErrorCategory::Conflict);
    }

    #[test]
    fn generated_column_write_is_conflict_rolled_back() {
        let m = map("428C9", ErrorPhase::Write);
        assert_eq!(m.category, ErrorCategory::Conflict);
        assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
        assert!(matches!(m.retry, RetryDisposition::Never));
    }

    #[test]
    fn too_many_connections_defers_retry() {
        let m = map("53300", ErrorPhase::Connect);
        assert_eq!(m.category, ErrorCategory::ResourceLimit);
        assert!(matches!(m.retry, RetryDisposition::After(1_000)));
    }

    #[test]
    fn query_cancelled_is_cancelled_category() {
        let m = map("57014", ErrorPhase::Read);
        assert_eq!(m.category, ErrorCategory::Cancelled);
        assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
    }

    #[test]
    fn admin_shutdown_quarantines_connection() {
        let m = map("57P01", ErrorPhase::Write);
        assert_eq!(m.category, ErrorCategory::Transient);
        assert!(matches!(m.retry, RetryDisposition::Quarantine));
        assert_eq!(m.remote_effect, RemoteEffect::Unknown);
    }

    #[test]
    fn syntax_error_is_invalid_plan() {
        let m = map("42601", ErrorPhase::Prepare);
        assert_eq!(m.category, ErrorCategory::InvalidPlan);
    }

    #[test]
    fn division_by_zero_is_execution_error() {
        let m = map("22012", ErrorPhase::Write);
        assert_eq!(m.category, ErrorCategory::Execution);
        assert_eq!(m.remote_effect, RemoteEffect::RolledBack);
    }

    #[test]
    fn read_only_transaction_is_authorization() {
        let m = map("25006", ErrorPhase::Write);
        assert_eq!(m.category, ErrorCategory::Authorization);
    }

    #[test]
    fn feature_not_supported_is_unsupported() {
        let m = map("0A000", ErrorPhase::Prepare);
        assert_eq!(m.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn unknown_sqlstate_falls_back_to_protocol() {
        let m = resolve_mapping(Some("99999"), false, ErrorPhase::Read);
        assert_eq!(m.category, ErrorCategory::Protocol);
        assert!(matches!(m.retry, RetryDisposition::Never));
    }

    #[test]
    fn no_sqlstate_no_close_is_protocol() {
        let m = resolve_mapping(None, false, ErrorPhase::Read);
        assert_eq!(m.category, ErrorCategory::Protocol);
    }

    #[test]
    fn transport_closed_without_sqlstate_is_io_quarantine() {
        let m = resolve_mapping(None, true, ErrorPhase::Read);
        assert_eq!(m.category, ErrorCategory::Io);
        assert!(matches!(m.retry, RetryDisposition::Quarantine));
    }

    #[test]
    fn transport_closed_in_commit_is_remote_effect_unknown() {
        let m = resolve_mapping(None, true, ErrorPhase::Commit);
        assert_eq!(m.category, ErrorCategory::Io);
        assert_eq!(m.remote_effect, RemoteEffect::Unknown);
    }

    #[test]
    fn transport_closed_in_connect_is_remote_effect_none() {
        let m = resolve_mapping(None, true, ErrorPhase::Connect);
        assert_eq!(m.category, ErrorCategory::Io);
        assert_eq!(m.remote_effect, RemoteEffect::None);
    }

    #[test]
    fn transport_closed_upgrades_write_conflict_to_unknown_effect() {
        let m = resolve_mapping(Some("23505"), true, ErrorPhase::Commit);
        assert_eq!(m.remote_effect, RemoteEffect::Unknown);
        assert!(matches!(m.retry, RetryDisposition::RequiresRecovery));
    }

    #[test]
    fn transport_closed_read_keeps_none_effect() {
        let m = resolve_mapping(Some("42P01"), true, ErrorPhase::Read);
        assert_eq!(m.category, ErrorCategory::NotFound);
        assert_eq!(m.remote_effect, RemoteEffect::None);
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn database_error_is_send_sync() {
        assert_send_sync::<DatabaseError>();
    }

    #[test]
    fn message_never_contains_sqlstate_code() {
        for code in [
            "08006", "23505", "40001", "40P01", "42501", "42P01", "53300", "57014", "57P01",
            "22001", "22P02", "25P02", "0A000",
        ] {
            let m = map(code, ErrorPhase::Write);
            assert!(
                !m.message.contains(code),
                "il messaggio pubblico non deve contenere il SQLSTATE {code}"
            );
        }
    }
}

/// Test di integrazione live: forzano ciascun SQLSTATE reale contro un
/// `PostgreSQL` raggiungibile all'hostname `dataflow-postgres`, poi verificano
/// che `classify_error` produca la mappatura attesa. Chiudono il milestone A2.
#[cfg(test)]
mod live {
    use super::*;
    use tokio_postgres::{Client, NoTls};

    /// DSN del riferimento plaintext, usato quando il runner non ne impone
    /// uno.
    const REFERENCE_DSN: &str =
        "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test";

    /// Il DSN su cui girano questi test live.
    ///
    /// `PLENORA_TEST_POSTGRES_DSN` ha la precedenza: e cosi che la matrice
    /// delle versioni indirizza la suite verso il `PostgreSQL` che sta
    /// qualificando. Senza questa lettura i test puntavano sempre al
    /// riferimento, quindi la matrice affermava di averli superati su 14, 15
    /// e 17 mentre giravano su 16 — e funzionavano solo perche la rete
    /// privata della matrice condivideva il nome con quella del compose.
    fn live_dsn() -> String {
        std::env::var("PLENORA_TEST_POSTGRES_DSN").unwrap_or_else(|_| REFERENCE_DSN.to_owned())
    }

    async fn connect() -> Client {
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connessione al PostgreSQL live");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    async fn err_from(client: &Client, sql: &str) -> tokio_postgres::Error {
        client
            .batch_execute(sql)
            .await
            .expect_err(&format!("mi aspetto errore da: {sql}"))
    }

    fn state(err: &tokio_postgres::Error) -> Option<&str> {
        err.code().map(tokio_postgres::error::SqlState::code)
    }

    #[tokio::test]
    async fn live_unique_violation_23505_is_conflict() {
        let client = connect().await;
        client
            .batch_execute(
                "DROP TABLE IF EXISTS a2_unique;
                 CREATE TABLE a2_unique (id INT PRIMARY KEY);
                 INSERT INTO a2_unique VALUES (1);",
            )
            .await
            .expect("setup");

        let raw = err_from(&client, "INSERT INTO a2_unique VALUES (1);").await;
        assert_eq!(state(&raw), Some("23505"));

        let mapped = classify_error(ErrorPhase::Write, &raw);
        assert_eq!(mapped.category, ErrorCategory::Conflict);
        assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
        assert!(matches!(mapped.retry, RetryDisposition::Never));

        client
            .batch_execute("DROP TABLE a2_unique;")
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    async fn live_not_null_violation_23502_is_conflict() {
        let client = connect().await;
        client
            .batch_execute(
                "DROP TABLE IF EXISTS a2_notnull;
                 CREATE TABLE a2_notnull (id INT NOT NULL);",
            )
            .await
            .expect("setup");

        let raw = err_from(&client, "INSERT INTO a2_notnull VALUES (NULL);").await;
        assert_eq!(state(&raw), Some("23502"));
        let mapped = classify_error(ErrorPhase::Write, &raw);
        assert_eq!(mapped.category, ErrorCategory::Conflict);

        client
            .batch_execute("DROP TABLE a2_notnull;")
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    async fn live_foreign_key_violation_23503_is_conflict() {
        let client = connect().await;
        client
            .batch_execute(
                "DROP TABLE IF EXISTS a2_fk_child;
                 DROP TABLE IF EXISTS a2_fk_parent;
                 CREATE TABLE a2_fk_parent (id INT PRIMARY KEY);
                 CREATE TABLE a2_fk_child  (id INT, parent INT REFERENCES a2_fk_parent(id));",
            )
            .await
            .expect("setup");

        let raw = err_from(&client, "INSERT INTO a2_fk_child VALUES (1, 999);").await;
        assert_eq!(state(&raw), Some("23503"));
        let mapped = classify_error(ErrorPhase::Write, &raw);
        assert_eq!(mapped.category, ErrorCategory::Conflict);

        client
            .batch_execute("DROP TABLE a2_fk_child; DROP TABLE a2_fk_parent;")
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    async fn live_check_violation_23514_is_conflict() {
        let client = connect().await;
        client
            .batch_execute(
                "DROP TABLE IF EXISTS a2_check;
                 CREATE TABLE a2_check (v INT CHECK (v > 0));",
            )
            .await
            .expect("setup");

        let raw = err_from(&client, "INSERT INTO a2_check VALUES (-1);").await;
        assert_eq!(state(&raw), Some("23514"));
        assert_eq!(
            classify_error(ErrorPhase::Write, &raw).category,
            ErrorCategory::Conflict
        );

        client
            .batch_execute("DROP TABLE a2_check;")
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    async fn live_undefined_table_42p01_is_not_found() {
        let client = connect().await;
        let raw = err_from(&client, "SELECT * FROM a2_absent_table_xyz;").await;
        assert_eq!(state(&raw), Some("42P01"));
        let mapped = classify_error(ErrorPhase::Prepare, &raw);
        assert_eq!(mapped.category, ErrorCategory::NotFound);
        assert_eq!(mapped.remote_effect, RemoteEffect::None);
    }

    #[tokio::test]
    async fn live_undefined_column_42703_is_not_found() {
        let client = connect().await;
        let raw = err_from(&client, "SELECT a2_absent_col FROM (VALUES (1)) t(x);").await;
        assert_eq!(state(&raw), Some("42703"));
        assert_eq!(
            classify_error(ErrorPhase::Prepare, &raw).category,
            ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn live_syntax_error_42601_is_invalid_plan() {
        let client = connect().await;
        let raw = err_from(&client, "SELEKT 1;").await;
        assert_eq!(state(&raw), Some("42601"));
        assert_eq!(
            classify_error(ErrorPhase::Prepare, &raw).category,
            ErrorCategory::InvalidPlan
        );
    }

    #[tokio::test]
    async fn live_division_by_zero_22012_is_execution() {
        let client = connect().await;
        let raw = err_from(&client, "SELECT 1/0;").await;
        assert_eq!(state(&raw), Some("22012"));
        let mapped = classify_error(ErrorPhase::Read, &raw);
        assert_eq!(mapped.category, ErrorCategory::Execution);
        assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
    }

    #[tokio::test]
    async fn live_invalid_text_representation_22p02_is_data_mapping() {
        let client = connect().await;
        let raw = err_from(&client, "SELECT 'not-a-number'::integer;").await;
        assert_eq!(state(&raw), Some("22P02"));
        assert_eq!(
            classify_error(ErrorPhase::Read, &raw).category,
            ErrorCategory::DataMapping
        );
    }

    #[tokio::test]
    async fn live_numeric_out_of_range_22003_is_data_mapping() {
        let client = connect().await;
        let raw = err_from(
            &client,
            "SELECT (999999999::bigint * 999999999::bigint)::int;",
        )
        .await;
        assert_eq!(state(&raw), Some("22003"));
        assert_eq!(
            classify_error(ErrorPhase::Read, &raw).category,
            ErrorCategory::DataMapping
        );
    }

    #[tokio::test]
    async fn live_query_canceled_57014_is_cancelled() {
        let client = connect().await;
        let raw = err_from(
            &client,
            "SET statement_timeout = '50ms'; SELECT pg_sleep(2);",
        )
        .await;
        assert_eq!(state(&raw), Some("57014"));
        let mapped = classify_error(ErrorPhase::Read, &raw);
        assert_eq!(mapped.category, ErrorCategory::Cancelled);
        assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
    }

    #[tokio::test]
    async fn live_in_failed_sql_transaction_25p02_is_protocol() {
        let client = connect().await;
        client.batch_execute("BEGIN;").await.expect("begin");
        let _ = client
            .batch_execute("SELECT undefined_col FROM (VALUES (1)) t(x);")
            .await;
        let raw = err_from(&client, "SELECT 1;").await;
        assert_eq!(state(&raw), Some("25P02"));
        assert_eq!(
            classify_error(ErrorPhase::Write, &raw).category,
            ErrorCategory::Protocol
        );
        client.batch_execute("ROLLBACK;").await.ok();
    }

    #[tokio::test]
    async fn live_read_only_transaction_25006_is_authorization() {
        let client = connect().await;
        client
            .batch_execute("BEGIN READ ONLY;")
            .await
            .expect("begin ro");
        let raw = err_from(&client, "CREATE TEMP TABLE a2_ro_test (x INT);").await;
        assert_eq!(state(&raw), Some("25006"));
        assert_eq!(
            classify_error(ErrorPhase::Write, &raw).category,
            ErrorCategory::Authorization
        );
        client.batch_execute("ROLLBACK;").await.ok();
    }

    #[tokio::test]
    async fn live_invalid_schema_3f000_is_not_found() {
        let client = connect().await;
        let raw = err_from(&client, "DROP SCHEMA a2_absent_schema RESTRICT;").await;
        assert_eq!(state(&raw), Some("3F000"));
        assert_eq!(
            classify_error(ErrorPhase::Prepare, &raw).category,
            ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn live_deadlock_40p01_is_transient_safe_retry() {
        let client_a = connect().await;
        let client_b = connect().await;

        client_a
            .batch_execute(
                "DROP TABLE IF EXISTS a2_deadlock;
                 CREATE TABLE a2_deadlock (id INT PRIMARY KEY, v INT);
                 INSERT INTO a2_deadlock VALUES (1, 10), (2, 20);",
            )
            .await
            .expect("setup");

        client_a.batch_execute("BEGIN;").await.expect("begin A");
        client_b.batch_execute("BEGIN;").await.expect("begin B");

        client_a
            .execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 1;", &[])
            .await
            .expect("A locks 1");
        client_b
            .execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 2;", &[])
            .await
            .expect("B locks 2");

        let a_fut = client_a.execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 2;", &[]);
        let b_fut = client_b.execute("UPDATE a2_deadlock SET v = v + 1 WHERE id = 1;", &[]);

        let (res_a, res_b) = tokio::join!(a_fut, b_fut);
        let deadlock_err = match (res_a, res_b) {
            (Err(e), _) | (_, Err(e)) if state(&e) == Some("40P01") => e,
            other => panic!("nessun deadlock 40P01 osservato: {other:?}"),
        };

        let mapped = classify_error(ErrorPhase::Write, &deadlock_err);
        assert_eq!(mapped.category, ErrorCategory::Transient);
        assert!(matches!(mapped.retry, RetryDisposition::Safe));
        assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);

        client_a.batch_execute("ROLLBACK;").await.ok();
        client_b.batch_execute("ROLLBACK;").await.ok();
        client_a
            .batch_execute("DROP TABLE a2_deadlock;")
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    async fn live_generated_column_write_428c9_is_conflict() {
        let client = connect().await;
        client
            .batch_execute(
                "DROP TABLE IF EXISTS a2_generated;
                 CREATE TABLE a2_generated (
                     id INT PRIMARY KEY,
                     price NUMERIC,
                     tax_rate NUMERIC,
                     total NUMERIC GENERATED ALWAYS AS (price * (1 + tax_rate)) STORED
                 );",
            )
            .await
            .expect("setup");

        // INSERT che tocca la colonna generata → 428C9
        let raw = err_from(
            &client,
            "INSERT INTO a2_generated (id, price, tax_rate, total) \
             VALUES (1, 10.0, 0.22, 100.0);",
        )
        .await;
        assert_eq!(state(&raw), Some("428C9"));

        let mapped = classify_error(ErrorPhase::Write, &raw);
        assert_eq!(mapped.category, ErrorCategory::Conflict);
        assert_eq!(mapped.remote_effect, RemoteEffect::RolledBack);
        assert!(
            mapped.message.contains("generat"),
            "msg poco chiaro: {}",
            mapped.message
        );

        client
            .batch_execute("DROP TABLE a2_generated;")
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    async fn live_serialization_failure_40001_is_transient_safe_retry() {
        let client_a = connect().await;
        let client_b = connect().await;

        client_a
            .batch_execute(
                "DROP TABLE IF EXISTS a2_serial;
                 CREATE TABLE a2_serial (id INT PRIMARY KEY, v INT);
                 INSERT INTO a2_serial VALUES (1, 100), (2, 200);",
            )
            .await
            .expect("setup");

        client_a
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE;")
            .await
            .expect("begin A");
        client_b
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE;")
            .await
            .expect("begin B");

        let _ = client_a
            .query("SELECT SUM(v) FROM a2_serial;", &[])
            .await
            .expect("A read");
        let _ = client_b
            .query("SELECT SUM(v) FROM a2_serial;", &[])
            .await
            .expect("B read");
        client_a
            .execute("INSERT INTO a2_serial VALUES (3, 300);", &[])
            .await
            .expect("A insert");
        client_b
            .execute("INSERT INTO a2_serial VALUES (4, 400);", &[])
            .await
            .expect("B insert");
        client_a.batch_execute("COMMIT;").await.expect("A commit");
        let raw = client_b
            .batch_execute("COMMIT;")
            .await
            .expect_err("B deve fallire in serialization");

        assert_eq!(state(&raw), Some("40001"));
        let mapped = classify_error(ErrorPhase::Commit, &raw);
        assert_eq!(mapped.category, ErrorCategory::Transient);
        assert!(matches!(mapped.retry, RetryDisposition::Safe));

        client_a
            .batch_execute("DROP TABLE a2_serial;")
            .await
            .expect("cleanup");
    }
}
