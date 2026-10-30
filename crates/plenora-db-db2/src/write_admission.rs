//! La regola A su Db2 (`plenora_database_core::write_admission`): la chiave
//! d'ammissione di una colonna e la sua ricerca nel fixture misurato.
//!
//! La chiave porta il tipo come lo pubblica `SYSCAT.COLUMNS` (`TYPENAME`, con
//! lo schema del tipo se non e `SYSIBM`: per difesa, un tipo non di sistema
//! non e mai la chiave di un tipo predefinito), la precisione e la scala di
//! `DECIMAL`, la precisione frazionaria di `TIMESTAMP` (la `SCALE` del
//! catalogo: fino al 25/10 `TIMESTAMP(3)` si ammetteva come `TIMESTAMP` e
//! troncava i microsecondi in silenzio) e, per i testi, le unita della
//! lunghezza della colonna (`TYPESTRINGUNITS`), la code page della colonna
//! (`CODEPAGE`, `0` per `FOR BIT DATA`), la code page e il code set del
//! database, il suo `string_units` e il suo `varchar2_compat`
//! (`SYSIBMADM.DBCFG`). La lunghezza di `CHARACTER`, `VARCHAR` e `CLOB` e
//! simbolica (`n`), con il limite dichiarato in
//! `plenora_database_core::write_admission`.
//!
//! I fatti si leggono nel preflight, sulla connessione del catalogo, insieme
//! alla major del server (`12.1`, da `SERVICE_LEVEL`). Il `varchar2_compat`
//! si rilegge poi nella transazione della scrittura e deve essere quello
//! della chiave (`crate::write`).
//!
//! La collation della colonna (`COLLATIONSCHEMA`, `COLLATIONNAME`: quella
//! del database, che `SYSIBMADM.DBCFG` non pubblica, `collate_info` vi e
//! `NULL`) non cambia cio che si salva e non e nella chiave: decide la regola
//! K delle chiavi di testo (`crate::write`, «chiavi di testo sotto una
//! collation non binaria»).

use crate::connection::{query_text, required_text};
use plenora_database_core::arrow::schema::{DataType, TimeUnit};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::write_admission::{
    arrow_key, AdmissionKey, AdmissionTable, TargetDimensions,
};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// Il fixture misurato della regola A per Db2.
pub const FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/scrittura_server_db2.json");

/// La tabella delle misure, letta una volta.
///
/// # Errors
///
/// `internal` se il fixture incorporato non e valido.
pub fn admission_table() -> Result<&'static AdmissionTable> {
    static TABLE: OnceLock<Result<AdmissionTable>> = OnceLock::new();
    TABLE
        .get_or_init(|| AdmissionTable::parse(ProviderKind::Db2, FIXTURE))
        .as_ref()
        .map_err(Clone::clone)
}

/// La major del server e i parametri del database che decidono che cosa si
/// salva.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFacts {
    /// `versione.release`, per esempio `12.1`.
    pub major: String,
    /// Il `SERVICE_LEVEL` completo, per esempio `DB2 v12.1.5.0`.
    pub version: String,
    pub codepage: String,
    pub codeset: String,
    pub string_units: String,
    pub varchar2_compat: bool,
}

/// Cio che il catalogo dice di una colonna, per la chiave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFacts {
    pub name: String,
    pub type_schema: String,
    pub type_name: String,
    /// `LENGTH`: la precisione di `DECIMAL`, i byte dei testi.
    pub length: u64,
    /// `SCALE`: la scala di `DECIMAL`, le cifre frazionarie di `TIMESTAMP`.
    pub scale: i64,
    /// `CODEPAGE`: `0` per `FOR BIT DATA` e per i tipi non di testo.
    pub codepage: u64,
    /// `TYPESTRINGUNITS` (`OCTETS`, `CODEUNITS32`), vuoto per `FOR BIT
    /// DATA` e per i tipi non di testo.
    pub string_units: String,
    /// `STRINGUNITSLENGTH`: la lunghezza nelle unita della colonna.
    pub string_units_length: u64,
    /// `COLLATIONSCHEMA.COLLATIONNAME` (`SYSIBM.IDENTITY`), vuoto per i tipi
    /// che non ne hanno una.
    pub collation: String,
}

impl ColumnFacts {
    /// Se la collation della colonna confronta i testi byte per byte
    /// (`SYSIBM.IDENTITY`, `SYSIBM.IDENTITY_16BIT`): ogni altra (`SYSTEM_*`,
    /// `UCA*`, `CLDR*`, `NLSCHAR`), e una collation non letta, puo
    /// confrontare uguali due valori diversi.
    #[must_use]
    pub fn binary_collation(&self) -> bool {
        matches!(
            self.collation.to_ascii_uppercase().as_str(),
            "SYSIBM.IDENTITY" | "SYSIBM.IDENTITY_16BIT"
        )
    }
}

const SERVER_VERSION_SQL: &str = "SELECT SERVICE_LEVEL \
    FROM TABLE (SYSPROC.ENV_GET_INST_INFO()) AS INSTANCE_INFO";

const SERVER_PARAMETERS_SQL: &str = "SELECT TRIM(NAME), TRIM(VARCHAR(VALUE, 256)) \
    FROM SYSIBMADM.DBCFG \
    WHERE NAME IN ('codepage', 'codeset', 'string_units', 'varchar2_compat')";

const COLUMN_FACTS_SQL: &str = "SELECT TRIM(COLNAME), TRIM(TYPESCHEMA), TRIM(TYPENAME), \
    LENGTH, SCALE, CODEPAGE, COALESCE(TRIM(TYPESTRINGUNITS), ''), COALESCE(STRINGUNITSLENGTH, 0), \
    COALESCE(TRIM(COLLATIONSCHEMA) || '.' || TRIM(COLLATIONNAME), '') \
    FROM SYSCAT.COLUMNS WHERE TABSCHEMA = ? AND TABNAME = ? ORDER BY COLNO";

const PARAMETERS: [&str; 4] = ["codepage", "codeset", "string_units", "varchar2_compat"];

/// Legge la major e i parametri del database.
///
/// # Errors
///
/// `unsupported` (fase `prepare`) se la versione o un parametro non si
/// leggono, non hanno la forma attesa o differiscono fra i membri: la regola
/// A non si applica, e la scrittura non parte.
pub fn server_facts(connection: &odbc_api::Connection<'_>, timeout: usize) -> Result<ServerFacts> {
    let rows = query_text(connection, SERVER_VERSION_SQL, &[], timeout, 1, 128)
        .map_err(|_| unproven_facts())?;
    let mut versions = rows
        .iter()
        .map(|row| required_text(row, 0, "versione Db2 non letta").map_err(|_| unproven_facts()))
        .collect::<Result<Vec<_>>>()?;
    versions.sort();
    versions.dedup();
    let [version] = versions.as_slice() else {
        return Err(unproven_facts());
    };
    let major = parse_major(version).ok_or_else(unproven_facts)?;
    let rows = query_text(connection, SERVER_PARAMETERS_SQL, &[], timeout, 2, 256)
        .map_err(|_| unproven_facts())?;
    let parameters = parse_parameters(&rows)?;
    let value = |name: &str| parameters.get(name).cloned().ok_or_else(unproven_facts);
    let varchar2_compat = match value("varchar2_compat")?.to_ascii_uppercase().as_str() {
        "ON" => true,
        "OFF" => false,
        _ => return Err(unproven_facts()),
    };
    Ok(ServerFacts {
        major,
        version: version.clone(),
        codepage: value("codepage")?,
        codeset: value("codeset")?,
        string_units: value("string_units")?.to_ascii_uppercase(),
        varchar2_compat,
    })
}

/// `DB2 v12.1.5.0` -> `12.1`.
#[must_use]
pub fn parse_major(service_level: &str) -> Option<String> {
    let numbers = service_level.trim().rsplit(['v', 'V']).next()?;
    let mut parts = numbers.split('.');
    let version = parts.next()?;
    let release = parts.next()?;
    let digits = |part: &str| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit());
    (digits(version) && digits(release) && service_level.contains(['v', 'V']))
        .then(|| format!("{version}.{release}"))
}

/// I parametri letti: una riga per parametro e per membro, ogni parametro
/// presente e con lo stesso valore in ogni membro.
///
/// # Errors
///
/// `unsupported` se un parametro manca, e vuoto o differisce fra i membri.
pub fn parse_parameters(rows: &[Vec<Option<String>>]) -> Result<BTreeMap<String, String>> {
    let mut parameters = BTreeMap::new();
    for row in rows {
        let name = required_text(row, 0, "parametro Db2 senza nome")
            .map_err(|_| unproven_facts())?
            .to_ascii_lowercase();
        let value =
            required_text(row, 1, "parametro Db2 senza valore").map_err(|_| unproven_facts())?;
        if let Some(previous) = parameters.insert(name, value.clone()) {
            if previous != value {
                return Err(unproven_facts());
            }
        }
    }
    if PARAMETERS.iter().all(|name| parameters.contains_key(*name)) {
        Ok(parameters)
    } else {
        Err(unproven_facts())
    }
}

/// Legge i fatti delle colonne del target.
///
/// # Errors
///
/// `unsupported` se il catalogo non risponde nella forma attesa.
pub fn column_facts(
    connection: &odbc_api::Connection<'_>,
    timeout: usize,
    schema: &str,
    object: &str,
) -> Result<Vec<ColumnFacts>> {
    let rows = query_text(
        connection,
        COLUMN_FACTS_SQL,
        &[schema, object],
        timeout,
        9,
        512,
    )
    .map_err(|_| unproven_facts())?;
    let mut facts = rows
        .iter()
        .map(|row| parse_column(row))
        .collect::<Result<Vec<_>>>()?;
    resolve_type_collations(connection, timeout, &mut facts)?;
    Ok(facts)
}

/// Prova la collation del CAST e della Create dal tipo built-in del database.
/// La collation di una colonna esistente deve concordare; un dato assente non
/// viene sostituito da IDENTITY o dalla collation delle tabelle di catalogo.
pub fn resolve_type_collations(
    connection: &odbc_api::Connection<'_>,
    timeout: usize,
    facts: &mut [ColumnFacts],
) -> Result<()> {
    for column in facts
        .iter_mut()
        .filter(|c| matches!(c.type_name.as_str(), "CHARACTER" | "VARCHAR" | "CLOB"))
    {
        let rows = query_text(connection,
            "SELECT CODEPAGE, TRIM(COLLATIONSCHEMA), TRIM(COLLATIONNAME) FROM SYSCAT.DATATYPES WHERE TYPESCHEMA = ? AND TYPENAME = ? AND METATYPE = 'S'",
            &[&column.type_schema, &column.type_name], timeout, 3, 512)
            .map_err(|_| unproven_facts())?;
        apply_type_collation(column, &rows)?;
    }
    Ok(())
}

/// Fatti del tipo nella stessa connessione: esattamente una riga completa.
fn apply_type_collation(column: &mut ColumnFacts, rows: &[Vec<Option<String>>]) -> Result<()> {
    let [row] = rows else {
        return Err(unproven_facts());
    };
    let codepage = required_text(row, 0, "codepage del tipo assente")
        .map_err(|_| unproven_facts())?
        .parse::<u64>()
        .map_err(|_| unproven_facts())?;
    let schema =
        required_text(row, 1, "schema collation del tipo assente").map_err(|_| unproven_facts())?;
    let name = required_text(row, 2, "collation del tipo assente").map_err(|_| unproven_facts())?;
    let collation = format!("{}.{}", schema.trim(), name.trim());
    if column.type_schema != "SYSIBM"
        || codepage != column.codepage
        || schema.trim() != "SYSIBM"
        || name.trim().is_empty()
        || !name
            .trim()
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        || (!column.collation.is_empty() && column.collation != collation)
    {
        return Err(unproven_facts());
    }
    column.collation = collation;
    Ok(())
}

/// Una riga di [`COLUMN_FACTS_SQL`].
///
/// # Errors
///
/// `unsupported` se un campo manca o non e un numero.
pub fn parse_column(row: &[Option<String>]) -> Result<ColumnFacts> {
    let text = |index: usize| {
        required_text(row, index, "fatto di colonna Db2 non letto").map_err(|_| unproven_facts())
    };
    let number =
        |index: usize| -> Result<u64> { text(index)?.parse().map_err(|_| unproven_facts()) };
    Ok(ColumnFacts {
        name: text(0)?,
        type_schema: text(1)?,
        type_name: text(2)?.to_ascii_uppercase(),
        length: number(3)?,
        scale: text(4)?.parse().map_err(|_| unproven_facts())?,
        codepage: number(5)?,
        string_units: row
            .get(6)
            .and_then(Option::as_deref)
            .map(str::trim)
            .unwrap_or_default()
            .to_ascii_uppercase(),
        string_units_length: number(7)?,
        collation: row
            .get(8)
            .and_then(Option::as_deref)
            .map(str::trim)
            .unwrap_or_default()
            .to_owned(),
    })
}

/// La chiave d'ammissione della colonna `facts` per l'ingresso `arrow`, e le
/// dimensioni con cui si istanziano le classi misurate.
///
/// Per un testo la dimensione e la lunghezza nelle unita della colonna
/// (`STRINGUNITSLENGTH` per `CODEUNITS32`, `LENGTH` altrimenti), e la stessa
/// istanzia sia i caratteri sia i byte delle classi: in `OCTETS` un testo
/// con meno byte della lunghezza ha meno caratteri, in `CODEUNITS32` un
/// testo con piu caratteri della lunghezza ha piu byte, e le classi
/// (`riempie_testo`, `spazi_oltre`) predicono per eccesso.
#[must_use]
pub fn admission_key(
    arrow: &DataType,
    facts: &ColumnFacts,
    server: &ServerFacts,
) -> (AdmissionKey, TargetDimensions) {
    let type_name = if facts.type_schema.eq_ignore_ascii_case("SYSIBM") {
        facts.type_name.clone()
    } else {
        format!("{}.{}", facts.type_schema, facts.type_name)
    };
    let text_length = if facts.string_units == "CODEUNITS32" {
        facts.string_units_length
    } else {
        facts.length
    };
    let (native, dimension) = match type_name.as_str() {
        "DECIMAL" | "NUMERIC" => (
            format!("{type_name}({},{})", facts.length, facts.scale),
            None,
        ),
        "TIMESTAMP" => (format!("TIMESTAMP({})", facts.scale), None),
        "CHARACTER" | "VARCHAR" | "CLOB" => {
            // `FOR BIT DATA` non ha unita (il catalogo le lascia `NULL`).
            let (units, bit_data) = if facts.codepage == 0 {
                (String::new(), " FOR BIT DATA")
            } else if facts.string_units.is_empty() {
                (" ?".to_owned(), "")
            } else {
                (format!(" {}", facts.string_units), "")
            };
            (
                format!(
                    "{type_name}(n{units}){bit_data};cp={};db={}/{};su={};v2c={}",
                    facts.codepage,
                    server.codeset,
                    server.codepage,
                    server.string_units,
                    if server.varchar2_compat { "ON" } else { "OFF" },
                ),
                Some(text_length),
            )
        }
        _ => (type_name, None),
    };
    (
        AdmissionKey {
            arrow: arrow_key(arrow),
            nativo: native,
            dimensione: dimension,
        },
        TargetDimensions {
            chars: Some(text_length),
            bytes: Some(text_length),
        },
    )
}

/// I fatti della colonna che `mode=create` crea per `arrow`
/// (`crate::write::native_type`), come il catalogo li pubblichera.
///
/// `CLOB(2G)` si prevede soltanto con `string_units = SYSTEM` (le unita
/// della colonna sono allora `OCTETS`): con `CODEUNITS32` la colonna
/// avrebbe altre unita e un'altra lunghezza, e il tipo non e previsto
/// (`None`: rifiutato come non qualificato).
#[must_use]
pub fn create_facts(name: &str, arrow: &DataType, server: &ServerFacts) -> Option<ColumnFacts> {
    let facts = |type_name: &str, length: u64, scale: i64| ColumnFacts {
        name: name.to_owned(),
        type_schema: "SYSIBM".to_owned(),
        type_name: type_name.to_owned(),
        length,
        scale,
        codepage: 0,
        string_units: String::new(),
        string_units_length: 0,
        collation: String::new(),
    };
    Some(match arrow {
        DataType::Boolean => facts("BOOLEAN", 1, 0),
        DataType::Int16 => facts("SMALLINT", 2, 0),
        DataType::Int32 => facts("INTEGER", 4, 0),
        DataType::Int64 => facts("BIGINT", 8, 0),
        DataType::Float32 => facts("REAL", 4, 0),
        DataType::Float64 => facts("DOUBLE", 8, 0),
        DataType::Decimal128(precision, scale) => {
            facts("DECIMAL", u64::from(*precision), i64::from(*scale))
        }
        DataType::Utf8 if server.string_units == "SYSTEM" => {
            let length = 2_147_483_647;
            ColumnFacts {
                codepage: server.codepage.parse().ok()?,
                string_units: "OCTETS".to_owned(),
                string_units_length: length,
                ..facts("CLOB", length, 0)
            }
        }
        DataType::Date32 => facts("DATE", 4, 0),
        DataType::Timestamp(TimeUnit::Microsecond, None) => facts("TIMESTAMP", 10, 6),
        _ => return None,
    })
}

fn unproven_facts() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        Some(ProviderKind::Db2),
        "versione, parametri del database o colonne Db2 non letti: la regola A non e provata",
    )
}

#[cfg(test)]
#[path = "write_admission_tests.rs"]
mod tests;
