//! La regola A su SQL Server (`plenora_database_core::write_admission`): la
//! chiave d'ammissione di una colonna e la sua ricerca nel fixture misurato.
//!
//! La chiave porta il tipo come lo pubblica `sys.types` (un `float(n)` con
//! `n <= 24` e `real`, gli altri `float`), con i parametri che decidono che
//! cosa si salva:
//!
//! - precisione e scala di `decimal` e `numeric`, la precisione di `float` e
//!   `real`, la scala frazionaria di `datetime2`, `time` e `datetimeoffset`;
//!   `money`, `smallmoney`, `datetime` e `smalldatetime` hanno parametri
//!   fissi, e sono chiavi proprie;
//! - per i testi e i binari la lunghezza, simbolica (`n`, con il limite
//!   dichiarato in `plenora_database_core::write_admission`) o `max` (una
//!   chiave distinta), `is_ansi_padded` e la nullabilita (con `ANSI_PADDING
//!   OFF` un `char(n)` nullable toglie gli spazi finali, uno `NOT NULL`
//!   completa);
//! - per `char`, `varchar` e `text` la code page della collation
//!   (`COLLATIONPROPERTY(..., 'CodePage')`, 65001 per le `_UTF8`): la
//!   collation conta per lo storage soltanto con la code page, e per il
//!   confronto delle chiavi con la regola K (`super::keys`).
//!
//! I fatti si leggono nel preflight, sulla sessione del catalogo, come gli
//! altri fatti del catalogo SQL Server (`plenora_database_core::store_rewrite`,
//! «dove si leggono le riscritture»). Per Create, Replace e le colonne
//! aggiunte dall'evoluzione dello schema i fatti sono quelli della colonna che
//! la scrittura crea (`ANSI_PADDING ON`, la collation dichiarata o quella del
//! database).

use crate::catalog::SqlServerWriteFacts;
use crate::SqlServerSession;
use plenora_database_core::arrow::schema::DataType;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::write_admission::{
    arrow_key, AdmissionKey, AdmissionTable, TargetDimensions,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::sync::OnceLock;
use tiberius::Query;

/// Il fixture misurato della regola A per SQL Server.
pub const FIXTURE: &[u8] =
    include_bytes!("../../../../tests/fixtures/scrittura_server_sqlserver.json");

/// La tabella delle misure, letta una volta.
///
/// # Errors
///
/// `internal` se il fixture incorporato non e valido.
pub fn admission_table() -> Result<&'static AdmissionTable> {
    static TABLE: OnceLock<Result<AdmissionTable>> = OnceLock::new();
    TABLE
        .get_or_init(|| AdmissionTable::parse(ProviderKind::Sqlserver, FIXTURE))
        .as_ref()
        .map_err(Clone::clone)
}

/// La versione del server e la collation del database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFacts {
    /// La major di `SERVERPROPERTY('ProductVersion')` (`16` per SQL Server
    /// 2022).
    pub major: String,
    pub version: String,
    /// La code page della collation del database: quella delle colonne di
    /// testo che la scrittura crea senza una collation dichiarata.
    pub database_code_page: Option<i32>,
}

const SERVER_FACTS_SQL: &str = "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')), \
     CONVERT(int, COLLATIONPROPERTY(CONVERT(nvarchar(128), \
     DATABASEPROPERTYEX(DB_NAME(), 'Collation')), 'CodePage'));";

const CODE_PAGE_SQL: &str = "SELECT CONVERT(int, COLLATIONPROPERTY(@P1, 'CodePage'));";

/// Legge la versione e la code page del database.
///
/// # Errors
///
/// `unsupported` se la versione non si legge: la regola A non si applica, e
/// la scrittura non parte; gli errori di interruzione restano com'erano.
pub async fn server_facts(
    session: &mut SqlServerSession,
    cancellation: &CancellationToken,
) -> Result<ServerFacts> {
    let results = session
        .execute_query(
            Query::new(SERVER_FACTS_SQL),
            ErrorPhase::Prepare,
            cancellation,
        )
        .await
        .map_err(keep_interruption)?;
    let [rows] = results.as_slice() else {
        return Err(unproven_facts());
    };
    let [row] = rows.as_slice() else {
        return Err(unproven_facts());
    };
    let version = row
        .try_get::<&str, _>(0)
        .ok()
        .flatten()
        .ok_or_else(unproven_facts)?
        .to_owned();
    let database_code_page = row.try_get::<i32, _>(1).map_err(|_| unproven_facts())?;
    let major = version
        .split('.')
        .next()
        .filter(|major| !major.is_empty() && major.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(unproven_facts)?
        .to_owned();
    Ok(ServerFacts {
        major,
        version,
        database_code_page,
    })
}

/// La code page di una collation dichiarata; assente se il server non la
/// conosce (la chiave non e misurata, e la scrittura si rifiuta).
///
/// # Errors
///
/// `unsupported` se la risposta non ha la forma attesa.
pub async fn collation_code_page(
    session: &mut SqlServerSession,
    collation: &str,
    cancellation: &CancellationToken,
) -> Result<Option<i32>> {
    let mut query = Query::new(CODE_PAGE_SQL);
    query.bind(collation.to_owned());
    let results = session
        .execute_query(query, ErrorPhase::Prepare, cancellation)
        .await
        .map_err(keep_interruption)?;
    let [rows] = results.as_slice() else {
        return Err(unproven_facts());
    };
    let [row] = rows.as_slice() else {
        return Err(unproven_facts());
    };
    row.try_get::<i32, _>(0).map_err(|_| unproven_facts())
}

/// I fatti di una colonna che la scrittura crea con `declaration`: la
/// sessione la crea con `ANSI_PADDING ON` (`crate::session`), che i tipi LOB
/// legacy non registrano.
#[must_use]
pub fn created_facts(
    name: &str,
    native_type: &str,
    declaration: &str,
    collation: Option<&str>,
    code_page: Option<i32>,
    nullable: bool,
) -> SqlServerWriteFacts {
    let arguments = declaration
        .split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'));
    let mut numbers = arguments
        .unwrap_or_default()
        .split(',')
        .map(|value| value.trim().parse::<u8>().ok());
    let first = numbers.next().flatten();
    let second = numbers.next().flatten();
    let length = || match arguments {
        Some("max") => Some(-1_i16),
        Some(value) => value.parse::<i16>().ok(),
        None => None,
    };
    let (max_length, precision, scale) = match native_type {
        "decimal" | "numeric" => (17, first.unwrap_or(0), second.unwrap_or(0)),
        "char" | "varchar" | "binary" | "varbinary" => (length().unwrap_or(0), 0, 0),
        "nchar" | "nvarchar" => (
            length().map_or(0, |value| {
                if value < 0 {
                    value
                } else {
                    value.saturating_mul(2)
                }
            }),
            0,
            0,
        ),
        "time" | "datetime2" | "datetimeoffset" => (0, 0, first.unwrap_or(7)),
        "float" => (8, 53, 0),
        "real" => (4, 24, 0),
        "money" => (8, 19, 4),
        "smallmoney" => (4, 10, 4),
        _ => (0, 0, 0),
    };
    let text = matches!(
        native_type,
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext"
    );
    SqlServerWriteFacts {
        name: name.to_owned(),
        native_type: native_type.to_owned(),
        max_length,
        precision,
        scale,
        collation: collation.map(ToOwned::to_owned),
        code_page: if text { code_page } else { None },
        // I tipi LOB legacy nascono con `is_ansi_padded = 0` anche con
        // `ANSI_PADDING ON` (misurato il 25/10).
        ansi_padded: !matches!(native_type, "text" | "ntext" | "image"),
        nullable,
    }
}

/// La chiave d'ammissione della colonna `facts` per l'ingresso `arrow`, e le
/// dimensioni con cui si istanziano le classi misurate.
#[must_use]
pub fn admission_key(
    arrow: &DataType,
    facts: &SqlServerWriteFacts,
) -> (AdmissionKey, TargetDimensions) {
    let native = facts.native_type.to_ascii_lowercase();
    let storage = |code_page: bool| {
        let page = if code_page {
            format!(
                ";cp={}",
                facts
                    .code_page
                    .map_or_else(|| "?".to_owned(), |value| value.to_string())
            )
        } else {
            String::new()
        };
        let padding = if facts.ansi_padded {
            ";ansi_padding=on"
        } else {
            ";ansi_padding=off"
        };
        let null = if facts.nullable { ";null" } else { ";not null" };
        format!("{page}{padding}{null}")
    };
    let none = TargetDimensions::default();
    // La lunghezza in byte del catalogo; `-1` per `max`.
    let declared = u64::try_from(facts.max_length).ok();
    let sized = |unit: u64, code_page: bool| {
        declared.map_or_else(
            || (format!("{native}(max){}", storage(code_page)), None, none),
            |bytes| {
                (
                    format!("{native}(n){}", storage(code_page)),
                    Some(bytes / unit),
                    TargetDimensions {
                        chars: Some(bytes / unit),
                        bytes: Some(bytes),
                    },
                )
            },
        )
    };
    let (shape, length, target) = match native.as_str() {
        "decimal" | "numeric" => (
            format!("{native}({},{})", facts.precision, facts.scale),
            None,
            none,
        ),
        "float" | "real" => (format!("{native}({})", facts.precision), None, none),
        "datetime2" | "time" | "datetimeoffset" => {
            (format!("{native}({})", facts.scale), None, none)
        }
        "char" | "varchar" => sized(1, true),
        "binary" | "varbinary" => sized(1, false),
        "nchar" | "nvarchar" => sized(2, false),
        "text" => (format!("text{}", storage(true)), None, none),
        "ntext" | "image" => (format!("{native}{}", storage(false)), None, none),
        _ => (native.clone(), None, none),
    };
    (
        AdmissionKey {
            arrow: arrow_key(arrow),
            nativo: shape,
            dimensione: length,
        },
        target,
    )
}

fn keep_interruption(error: DatabaseError) -> DatabaseError {
    if matches!(
        error.category,
        ErrorCategory::Cancelled | ErrorCategory::Timeout
    ) {
        error
    } else {
        unproven_facts()
    }
}

fn unproven_facts() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        Some(ProviderKind::Sqlserver),
        "versione o collation SQL Server non lette: la regola A non e provata",
    )
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
