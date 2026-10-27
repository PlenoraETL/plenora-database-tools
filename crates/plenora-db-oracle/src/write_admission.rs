//! La regola A su Oracle (`plenora_database_core::write_admission`): la
//! chiave d'ammissione di una colonna e la sua ricerca nel fixture misurato.
//!
//! La chiave porta il tipo come lo pubblica `ALL_TAB_COLUMNS.DATA_TYPE`
//! (`TIMESTAMP(3)`, `TIMESTAMP(6) WITH TIME ZONE`), la precisione e la scala
//! di `NUMBER` e `FLOAT`, la semantica della lunghezza dei testi
//! (`CHAR_USED`) e il charset del database (`NLS_CHARACTERSET`, o
//! `NLS_NCHAR_CHARACTERSET` per i tipi nazionali). La lunghezza di
//! `VARCHAR2`, `CHAR`, `NVARCHAR2`, `NCHAR` e `RAW` e simbolica (`n`), con il
//! limite dichiarato in `plenora_database_core::write_admission`.
//!
//! I fatti si leggono nel preflight, su una sessione del pool prima della
//! transazione, come gli altri fatti del catalogo Oracle
//! (`plenora_database_core::store_rewrite`, «dove si leggono le
//! riscritture»).
//!
//! Il bind applica inoltre i rifiuti di fedelta di `crate::parameter`:
//! le misure storiche non autorizzano date prima del calendario gregoriano
//! ne altri valori che il codec rifiuta. Il fixture e necessario, non
//! sufficiente a scavalcare un rifiuto del percorso effettivo.
//!
//! # Limite dichiarato: lo zero negativo
//!
//! - **Regola**: ogni ingresso float dichiara la riscrittura di `-0.0`.
//! - **Ambito**: tutti i tipi numerici Oracle; il round trip del driver da
//!   solo non distingue la forma non canonica di `BINARY_DOUBLE`.
//! - **Hazard**: piu stretto: anche con politiche non strette, dopo la
//!   dichiarazione nel rapporto, il bind rifiuta il valore. Il rapporto non
//!   promette che ogni valore della colonna sia scrivibile.
//! - **Rientro**: un percorso misurato di normalizzazione esplicita a `+0`
//!   per le sole colonne non chiave e con politica non stretta.
//!
//! # Limite dichiarato: le collation delle chiavi
//!
//! Si veda [`refuse_non_binary_keys`]: una chiave di testo si ammette soltanto
//! con un confronto binario provato (una collation assente si rifiuta), e la prova live di una collation linguistica di
//! colonna non c'e (la fixture `Free` ha `MAX_STRING_SIZE = STANDARD`, che non
//! le ammette): la prova e unitaria.

use crate::catalog::{optional_i64, optional_string, optional_u64, query, required_string};
use crate::config::OracleConfig;
use plenora_database_core::arrow::schema::{DataType, TimeUnit};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::write_admission::{
    arrow_key, AdmissionKey, AdmissionTable, TargetDimensions,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::sync::OnceLock;

/// Il fixture misurato della regola A per Oracle.
pub const FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/scrittura_server_oracle.json");

/// La tabella delle misure, letta una volta.
///
/// # Errors
///
/// `internal` se il fixture incorporato non e valido.
pub fn admission_table() -> Result<&'static AdmissionTable> {
    static TABLE: OnceLock<Result<AdmissionTable>> = OnceLock::new();
    TABLE
        .get_or_init(|| AdmissionTable::parse(ProviderKind::Oracle, FIXTURE))
        .as_ref()
        .map_err(Clone::clone)
}

/// La major del server e i charset del database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFacts {
    pub major: String,
    pub version: String,
    pub charset: String,
    pub national_charset: String,
    /// `NLS_COMP` della sessione: con `BINARY` il confronto dei testi e per
    /// byte, come la regola K lo fa.
    pub nls_comp: String,
}

/// Cio che il catalogo dice di una colonna, per la chiave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFacts {
    pub name: String,
    pub data_type: String,
    pub data_length: u64,
    pub char_length: u64,
    /// `B` o `C` per i testi, assente altrimenti.
    pub char_used: Option<String>,
    pub precision: Option<u64>,
    pub scale: Option<i64>,
    /// La collation dichiarata della colonna (`USING_NLS_COMP`, `BINARY`,
    /// `BINARY_CI`, ...), per i testi.
    pub collation: Option<String>,
}

const SERVER_FACTS_SQL: &str = "SELECT (SELECT VERSION FROM PRODUCT_COMPONENT_VERSION \
     WHERE PRODUCT LIKE 'Oracle%' AND ROWNUM = 1), (SELECT VALUE FROM NLS_DATABASE_PARAMETERS \
     WHERE PARAMETER = 'NLS_CHARACTERSET'), (SELECT VALUE FROM NLS_DATABASE_PARAMETERS \
     WHERE PARAMETER = 'NLS_NCHAR_CHARACTERSET'), (SELECT VALUE FROM NLS_SESSION_PARAMETERS \
     WHERE PARAMETER = 'NLS_COMP') FROM DUAL";

const COLUMN_FACTS_SQL: &str = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, CHAR_LENGTH, \
     CHAR_USED, DATA_PRECISION, DATA_SCALE, COLLATION FROM ALL_TAB_COLUMNS WHERE OWNER = :1 \
     AND TABLE_NAME = :2 ORDER BY COLUMN_ID";

/// Legge la major e i charset.
///
/// # Errors
///
/// `unsupported` se la versione o i charset non si leggono: la regola A non
/// si applica, e la scrittura non parte.
pub async fn server_facts(
    config: &OracleConfig,
    connection: &oracle_rs::Connection,
    cancellation: &CancellationToken,
) -> Result<ServerFacts> {
    let rows = query(config, connection, SERVER_FACTS_SQL, &[], cancellation)
        .await
        .map_err(|error| keep_interruption(error, unproven_facts()))?;
    let [row] = rows.as_slice() else {
        return Err(unproven_facts());
    };
    let version =
        required_string(row, 0, "versione Oracle non letta").map_err(|_| unproven_facts())?;
    let charset =
        required_string(row, 1, "charset Oracle non letto").map_err(|_| unproven_facts())?;
    let national_charset = required_string(row, 2, "charset nazionale Oracle non letto")
        .map_err(|_| unproven_facts())?;
    let nls_comp =
        required_string(row, 3, "NLS_COMP Oracle non letto").map_err(|_| unproven_facts())?;
    let major = version
        .split('.')
        .next()
        .filter(|major| !major.is_empty() && major.chars().all(|c| c.is_ascii_digit()))
        .ok_or_else(unproven_facts)?
        .to_owned();
    Ok(ServerFacts {
        major,
        version,
        charset,
        national_charset,
        nls_comp,
    })
}

/// Legge i fatti delle colonne del target.
///
/// # Errors
///
/// `unsupported` se il catalogo non risponde nella forma attesa.
pub async fn column_facts(
    config: &OracleConfig,
    connection: &oracle_rs::Connection,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<ColumnFacts>> {
    let parameters = [
        ParameterValue::String(schema.to_owned()),
        ParameterValue::String(object.to_owned()),
    ];
    let rows = query(
        config,
        connection,
        COLUMN_FACTS_SQL,
        &parameters,
        cancellation,
    )
    .await
    .map_err(|error| keep_interruption(error, unproven_facts()))?;
    rows.iter()
        .map(|row| {
            Ok(ColumnFacts {
                name: required_string(row, 0, "colonna").map_err(|_| unproven_facts())?,
                data_type: required_string(row, 1, "tipo").map_err(|_| unproven_facts())?,
                data_length: optional_u64(row, 2, "lunghezza")
                    .map_err(|_| unproven_facts())?
                    .ok_or_else(unproven_facts)?,
                char_length: optional_u64(row, 3, "caratteri")
                    .map_err(|_| unproven_facts())?
                    .unwrap_or(0),
                char_used: optional_string(row, 4).map_err(|_| unproven_facts())?,
                precision: optional_u64(row, 5, "precisione").map_err(|_| unproven_facts())?,
                scale: optional_i64(row, 6, "scala").map_err(|_| unproven_facts())?,
                collation: optional_string(row, 7).map_err(|_| unproven_facts())?,
            })
        })
        .collect()
}

/// La chiave d'ammissione della colonna `facts` per l'ingresso `arrow`, e le
/// dimensioni con cui si istanziano le classi misurate.
#[must_use]
pub fn admission_key(
    arrow: &DataType,
    facts: &ColumnFacts,
    server: &ServerFacts,
) -> (AdmissionKey, TargetDimensions) {
    let data_type = facts.data_type.to_ascii_uppercase();
    let semantics = match facts.char_used.as_deref() {
        Some("C") => "CHAR",
        Some("B") => "BYTE",
        _ => "?",
    };
    let character_dimension = if semantics == "CHAR" {
        facts.char_length
    } else {
        facts.data_length
    };
    let (native, dimension) = match data_type.as_str() {
        "NUMBER" => (
            match (facts.precision, facts.scale) {
                (Some(precision), Some(scale)) => format!("NUMBER({precision},{scale})"),
                (None, Some(scale)) => format!("NUMBER(*,{scale})"),
                (Some(precision), None) => format!("NUMBER({precision},?)"),
                (None, None) => "NUMBER".to_owned(),
            },
            None,
        ),
        "FLOAT" => (
            format!(
                "FLOAT({})",
                facts
                    .precision
                    .map_or_else(|| "?".to_owned(), |p| p.to_string())
            ),
            None,
        ),
        "VARCHAR2" | "CHAR" => (
            format!("{data_type}(n {semantics});{}", server.charset),
            Some(character_dimension),
        ),
        "NVARCHAR2" | "NCHAR" => (
            format!("{data_type}(n);{}", server.national_charset),
            Some(facts.char_length),
        ),
        "CLOB" => (format!("CLOB;{}", server.charset), None),
        "NCLOB" => (format!("NCLOB;{}", server.national_charset), None),
        "RAW" => ("RAW(n)".to_owned(), Some(facts.data_length)),
        _ => (data_type.clone(), None),
    };
    (
        AdmissionKey {
            arrow: arrow_key(arrow),
            nativo: native,
            dimensione: dimension,
        },
        TargetDimensions {
            chars: Some(facts.char_length),
            bytes: Some(facts.data_length),
        },
    )
}

/// I fatti della colonna che `mode=create` crea per `arrow`, come il catalogo
/// li pubblichera (`crate::write::native_type`).
#[must_use]
pub fn create_facts(name: &str, arrow: &DataType) -> Option<ColumnFacts> {
    let facts =
        |data_type: &str, precision: Option<u64>, scale: Option<i64>, length: u64| ColumnFacts {
            name: name.to_owned(),
            data_type: data_type.to_owned(),
            data_length: length,
            char_length: 0,
            char_used: None,
            precision,
            scale,
            collation: None,
        };
    Some(match arrow {
        DataType::Boolean => facts("BOOLEAN", None, None, 1),
        DataType::Int16 => facts("NUMBER", Some(5), Some(0), 22),
        DataType::Int32 => facts("NUMBER", Some(10), Some(0), 22),
        DataType::Int64 => facts("NUMBER", Some(19), Some(0), 22),
        DataType::Float32 => facts("BINARY_FLOAT", None, None, 4),
        DataType::Float64 => facts("BINARY_DOUBLE", None, None, 8),
        DataType::Decimal128(precision, scale) => facts(
            "NUMBER",
            Some(u64::from(*precision)),
            Some(i64::from(*scale)),
            22,
        ),
        DataType::Utf8 => facts("CLOB", None, None, 4000),
        DataType::Binary | DataType::LargeBinary => facts("BLOB", None, None, 4000),
        DataType::Date32 => facts("DATE", None, None, 7),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            facts("TIMESTAMP(6)", None, Some(6), 11)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            facts("TIMESTAMP(6) WITH TIME ZONE", None, Some(6), 13)
        }
        _ => return None,
    })
}

/// La regola K sotto collation: una chiave di testo si confronta per byte
/// soltanto con la collation `BINARY` (o `USING_NLS_COMP` con `NLS_COMP`
/// della sessione `BINARY`, il predefinito). Con un'altra (`BINARY_CI`, una
/// linguistica) l'indice unico e il `MERGE ... ON` confrontano uguali valori
/// che la regola K vede diversi (`'a'` e `'A'`), e i conteggi direbbero il
/// falso: la chiave si rifiuta, con ogni politica.
///
/// # Limite dichiarato
///
/// - **Regola**: le chiavi di testo Oracle si ammettono soltanto con un
///   confronto binario.
/// - **Ambito**: le mode per chiavi (`Update`, `Upsert`, `DeleteByKeys`).
/// - **Hazard**: piu stretto: una chiave sotto una collation linguistica non
///   si scrive, anche se i valori dell'ingresso non collidono.
/// - **Rientro**: il confronto dei duplicati fatto dal server sotto la
///   collation della colonna, nella transazione della scrittura.
///
/// # Errors
///
/// `unsupported` con il motivo, senza dati.
pub fn refuse_non_binary_keys(keys: &[&ColumnFacts], server: &ServerFacts) -> Result<()> {
    let text = |facts: &ColumnFacts| {
        matches!(
            facts.data_type.to_ascii_uppercase().as_str(),
            "VARCHAR2" | "CHAR" | "NVARCHAR2" | "NCHAR" | "CLOB" | "NCLOB"
        )
    };
    let binary = |facts: &ColumnFacts| match facts.collation.as_deref() {
        Some("BINARY") => true,
        Some("USING_NLS_COMP") => server.nls_comp.eq_ignore_ascii_case("BINARY"),
        None | Some(_) => false,
    };
    if keys.iter().any(|facts| text(facts) && !binary(facts)) {
        return Err(DatabaseError::new(
            ErrorCategory::Unsupported,
            ErrorPhase::Prepare,
            Some(ProviderKind::Oracle),
            "una chiave di testo Oracle sotto una collation non binaria confronta uguali \
             valori diversi (regola K): rifiutata con ogni politica",
        ));
    }
    Ok(())
}

/// Aggiunge la riscrittura dello zero negativo, misurata anche con confronti
/// SQL: il solo round trip del driver non prova la forma canonica Oracle.
/// Il bind continua a rifiutarlo anche con politiche non strette: nessun
/// percorso qualificato ne conserva il segno. Il rapporto delle perdite
/// descrive comunque il rischio del tipo; non autorizza quel bind.
#[must_use]
pub fn store_rewrites(
    mut measured: Vec<plenora_database_core::store_rewrite::StoreRewrite>,
    arrow: &DataType,
) -> Vec<plenora_database_core::store_rewrite::StoreRewrite> {
    use plenora_database_core::store_rewrite::StoreRewrite;
    if matches!(arrow, DataType::Float32 | DataType::Float64)
        && !measured.contains(&StoreRewrite::NegativeZero)
    {
        measured.push(StoreRewrite::NegativeZero);
    }
    measured
}

fn keep_interruption(error: DatabaseError, replacement: DatabaseError) -> DatabaseError {
    if matches!(
        error.category,
        ErrorCategory::Cancelled | ErrorCategory::Timeout
    ) {
        error
    } else {
        replacement
    }
}

fn unproven_facts() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        Some(ProviderKind::Oracle),
        "versione, charset o colonne Oracle non letti: la regola A non e provata",
    )
}

#[cfg(test)]
#[path = "write_admission_tests.rs"]
mod tests;
