//! La regola A su `MySQL` e `MariaDB` (`plenora_database_core::write_admission`):
//! la chiave d'ammissione di una colonna e la sua ricerca nel fixture misurato
//! del prodotto (`tests/fixtures/scrittura_server_mysql.json`,
//! `tests/fixtures/scrittura_server_mariadb.json`).
//!
//! # La chiave
//!
//! Il tipo nativo e `information_schema.COLUMNS.COLUMN_TYPE` per intero, in
//! minuscolo: precisione e scala (`decimal(10,2)`, `double(10,2)`,
//! `float(7,3)`), la scala frazionaria dei tempi (`datetime(3)`,
//! `timestamp(0)`), `unsigned`, `zerofill` e la larghezza di visualizzazione
//! degli interi come il catalogo la pubblica (`tinyint(1)`, `int(11)` di
//! `MariaDB`). Le colonne di testo portano il charset della colonna
//! (`CHARACTER_SET_NAME`: `varchar(n);utf8mb4`, `text;latin1`); una colonna
//! `JSON` di `MariaDB` (un `LONGTEXT` con il vincolo `CHECK (json_valid(c))`
//! che il server aggiunge) porta `;json_valid` e si misura come documento. La
//! lunghezza di `varchar(n)` e `varbinary(n)` e simbolica (`n`), con il
//! limite dichiarato in `plenora_database_core::write_admission`.
//!
//! La collation non e nella chiave: non cambia che cosa si salva (cambia come
//! si confrontano le chiavi, e quello e la regola K, `crate::write`,
//! `KeyCollationGuard`).
//!
//! La major del server e `major.minor` di `VERSION()` (`8.4`, `11.8`), letta
//! nella stessa sessione dei fatti del catalogo, sia nel preflight sia nella
//! sessione della scrittura.
//!
//! # Limite dichiarato: i tipi di Create
//!
//! - **Regola**: con `mode=create` la chiave si calcola dai tipi che
//!   `crate::write` dichiara nel `CREATE TABLE`, nella forma in cui il catalogo
//!   del prodotto li pubblica ([`create_facts`]).
//! - **Ambito**: il preflight di `mode=create`. La sessione della scrittura,
//!   dopo il `CREATE TABLE`, rilegge la tabella creata e ricalcola le chiavi
//!   dal catalogo: se le riscritture non coincidono la scrittura si rifiuta
//!   prima della transazione (con il residuo del DDL dichiarato), e la prova
//!   live `live_a_i_tipi_di_create_sono_quelli_del_catalogo` le confronta tipo
//!   per tipo.
//! - **Hazard**: nessuno silenzioso: una forma diversa del catalogo rifiuta
//!   dopo il DDL invece che prima.
//! - **Rientro**: la lettura dei tipi di una tabella temporanea creata con lo
//!   stesso DDL nel preflight.

use crate::catalog::MysqlObjectDescription;
use crate::session::MysqlSession;
use crate::MysqlColumnKind;
use mysql_async::{Params, Value};
use plenora_database_core::arrow::schema::{DataType, SchemaRef};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::store_rewrite::ColumnRewrites;
use plenora_database_core::write_admission::{
    arrow_key, AdmissionKey, AdmissionTable, TargetDimensions,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeSet;
use std::sync::OnceLock;

/// Il fixture misurato della regola A per `MySQL`.
pub const MYSQL_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/scrittura_server_mysql.json");

/// Il fixture misurato della regola A per `MariaDB`.
pub const MARIADB_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/scrittura_server_mariadb.json");

/// Il fixture del prodotto.
#[must_use]
pub const fn fixture(kind: ProviderKind) -> &'static [u8] {
    match kind {
        ProviderKind::Mariadb => MARIADB_FIXTURE,
        _ => MYSQL_FIXTURE,
    }
}

/// La tabella delle misure del prodotto, letta una volta.
///
/// # Errors
///
/// `internal` se il fixture incorporato non e valido.
pub fn admission_table(kind: ProviderKind) -> Result<&'static AdmissionTable> {
    static MYSQL: OnceLock<Result<AdmissionTable>> = OnceLock::new();
    static MARIADB: OnceLock<Result<AdmissionTable>> = OnceLock::new();
    let (cell, kind) = if kind == ProviderKind::Mariadb {
        (&MARIADB, ProviderKind::Mariadb)
    } else {
        (&MYSQL, ProviderKind::Mysql)
    };
    cell.get_or_init(|| AdmissionTable::parse(kind, fixture(kind)))
        .as_ref()
        .map_err(Clone::clone)
}

/// La versione del server e la sua major.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFacts {
    /// `major.minor`, la forma del fixture.
    pub major: String,
    /// `VERSION()` per intero.
    pub version: String,
}

/// Cio che il catalogo dice di una colonna, per la chiave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFacts {
    pub name: String,
    /// `DATA_TYPE`, in minuscolo.
    pub data_type: String,
    /// `COLUMN_TYPE`, in minuscolo.
    pub column_type: String,
    /// `CHARACTER_SET_NAME`, per le colonne di testo.
    pub character_set: Option<String>,
    /// Il vincolo `CHECK (json_valid(c))` di una colonna `JSON` di `MariaDB`.
    pub json_valid: bool,
}

/// `major.minor` di una versione (`8.4.11`, `11.8.8-MariaDB-ubu2404`).
#[must_use]
pub fn server_major(version: &str) -> Option<String> {
    let mut parts = version.split(['.', '-']);
    let major = parts.next()?;
    let minor = parts.next()?;
    let numeric = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    (numeric(major) && numeric(minor)).then(|| format!("{major}.{minor}"))
}

/// Legge la versione del server sulla sessione.
///
/// # Errors
///
/// `unsupported` se la versione non si legge: la regola A non si applica, e
/// la scrittura non parte. Una cancellazione o un timeout restano tali.
pub async fn server_facts(
    session: &mut MysqlSession,
    cancellation: &CancellationToken,
) -> Result<ServerFacts> {
    let kind = session.kind();
    let rows = session
        .query_rows("SELECT VERSION()", ErrorPhase::Prepare, cancellation)
        .await
        .map_err(|error| keep_interruption(error, unproven_facts(kind)))?;
    let version = rows
        .first()
        .and_then(|row| row.get_opt::<String, _>(0))
        .and_then(std::result::Result::ok)
        .ok_or_else(|| unproven_facts(kind))?;
    let major = server_major(&version).ok_or_else(|| unproven_facts(kind))?;
    Ok(ServerFacts { major, version })
}

/// Le colonne di `MariaDB` con il vincolo `CHECK (json_valid(c))` del tipo
/// `JSON`. Su `MySQL` il tipo `JSON` e un tipo proprio, e l'insieme e vuoto.
///
/// # Errors
///
/// `unsupported` se il catalogo non risponde.
pub async fn json_valid_columns(
    session: &mut MysqlSession,
    schema: &str,
    table: &str,
    cancellation: &CancellationToken,
) -> Result<BTreeSet<String>> {
    let kind = session.kind();
    if kind != ProviderKind::Mariadb {
        return Ok(BTreeSet::new());
    }
    let rows = session
        .exec_rows(
            "SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS \
             WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?",
            Params::Positional(vec![Value::from(schema), Value::from(table)]),
            ErrorPhase::Prepare,
            cancellation,
        )
        .await
        .map_err(|error| keep_interruption(error, unproven_facts(kind)))?;
    let mut columns = BTreeSet::new();
    for row in rows {
        let clause = row
            .get_opt::<String, _>(0)
            .and_then(std::result::Result::ok)
            .ok_or_else(|| unproven_facts(kind))?;
        if let Some(column) = clause
            .strip_prefix("json_valid(`")
            .and_then(|rest| rest.strip_suffix("`)"))
            .filter(|column| !column.contains('`'))
        {
            columns.insert(column.to_owned());
        }
    }
    Ok(columns)
}

/// I fatti delle colonne di un target descritto.
#[must_use]
pub fn target_facts(target: &MysqlObjectDescription, json: &BTreeSet<String>) -> Vec<ColumnFacts> {
    target
        .columns
        .iter()
        .map(|column| ColumnFacts {
            name: column.name.clone(),
            data_type: column.data_type.to_ascii_lowercase(),
            column_type: column.native_declaration.trim().to_ascii_lowercase(),
            character_set: column
                .character_set
                .as_ref()
                .map(|charset| charset.to_ascii_lowercase()),
            json_valid: json.contains(&column.name),
        })
        .collect()
}

/// Il `COLUMN_TYPE` con la lunghezza simbolica e la lunghezza, per
/// `varchar(n)` e `varbinary(n)`.
fn symbolic_length(column_type: &str) -> Option<(String, u64)> {
    let (head, rest) = column_type.split_once('(')?;
    let (length, tail) = rest.split_once(')')?;
    let length = length.parse::<u64>().ok()?;
    Some((format!("{head}(n){tail}"), length))
}

/// La chiave d'ammissione della colonna `facts` per l'ingresso `arrow`, e le
/// dimensioni con cui si istanziano le classi misurate.
#[must_use]
pub fn admission_key(arrow: &DataType, facts: &ColumnFacts) -> (AdmissionKey, TargetDimensions) {
    let charset = facts.character_set.as_deref().unwrap_or("?");
    let (native, dimension, dimensions) = match facts.data_type.as_str() {
        "varchar" | "char" => match symbolic_length(&facts.column_type) {
            Some((symbolic, length)) => (
                format!("{symbolic};{charset}"),
                Some(length),
                TargetDimensions {
                    chars: Some(length),
                    bytes: None,
                },
            ),
            None => (
                format!("{};{charset}", facts.column_type),
                None,
                TargetDimensions::default(),
            ),
        },
        "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" | "json" => (
            if facts.json_valid {
                format!("{};{charset};json_valid", facts.column_type)
            } else {
                format!("{};{charset}", facts.column_type)
            },
            None,
            TargetDimensions::default(),
        ),
        "varbinary" | "binary" => match symbolic_length(&facts.column_type) {
            Some((symbolic, length)) => (
                symbolic,
                Some(length),
                TargetDimensions {
                    chars: None,
                    bytes: Some(length),
                },
            ),
            None => (facts.column_type.clone(), None, TargetDimensions::default()),
        },
        _ => (facts.column_type.clone(), None, TargetDimensions::default()),
    };
    (
        AdmissionKey {
            arrow: arrow_key(arrow),
            nativo: native,
            dimensione: dimension,
        },
        dimensions,
    )
}

/// I fatti della colonna che `mode=create` crea per `kind`, nella forma in
/// cui il catalogo del prodotto la pubblica (`crate::write`,
/// `mysql_column_ddl`). `None` per una colonna geometrica (esclusa dalla
/// regola A) o un tipo che Create non scrive.
#[must_use]
pub fn create_facts(
    name: &str,
    kind: &MysqlColumnKind,
    product: ProviderKind,
) -> Option<ColumnFacts> {
    let mariadb = product == ProviderKind::Mariadb;
    let integer = |data_type: &str, width: u8, unsigned: bool| {
        let suffix = if unsigned { " unsigned" } else { "" };
        let column_type = if mariadb {
            format!("{data_type}({width}){suffix}")
        } else {
            format!("{data_type}{suffix}")
        };
        (data_type.to_owned(), column_type, None)
    };
    let (data_type, column_type, character_set) = match kind {
        MysqlColumnKind::Bool => ("tinyint".to_owned(), "tinyint(1)".to_owned(), None),
        MysqlColumnKind::I8 => integer("tinyint", 4, false),
        MysqlColumnKind::U8 => integer("tinyint", 3, true),
        MysqlColumnKind::I16 => integer("smallint", 6, false),
        MysqlColumnKind::U16 => integer("smallint", 5, true),
        MysqlColumnKind::I32 => integer("int", 11, false),
        MysqlColumnKind::U32 => integer("int", 10, true),
        MysqlColumnKind::I64 => integer("bigint", 20, false),
        MysqlColumnKind::U64 => integer("bigint", 20, true),
        MysqlColumnKind::F32 => ("float".to_owned(), "float".to_owned(), None),
        MysqlColumnKind::F64 => ("double".to_owned(), "double".to_owned(), None),
        MysqlColumnKind::Utf8 => (
            "text".to_owned(),
            "text".to_owned(),
            Some("utf8mb4".to_owned()),
        ),
        MysqlColumnKind::Binary => ("blob".to_owned(), "blob".to_owned(), None),
        MysqlColumnKind::Date => ("date".to_owned(), "date".to_owned(), None),
        MysqlColumnKind::Timestamp => ("datetime".to_owned(), "datetime(6)".to_owned(), None),
        MysqlColumnKind::Decimal { precision, scale } => (
            "decimal".to_owned(),
            format!("decimal({precision},{scale})"),
            None,
        ),
        MysqlColumnKind::Time | MysqlColumnKind::Geometry => return None,
    };
    Some(ColumnFacts {
        name: name.to_owned(),
        data_type,
        column_type,
        character_set,
        json_valid: false,
    })
}

/// La regola A sulle colonne `columns` (indice nello schema, nome) di
/// `schema`: ogni colonna cerca la propria chiave nel fixture della major del
/// server, e le riscritture misurate diventano quelle della regola S.
///
/// La tabella della sonda (`cfg(test)`) non applica la regola: e la misura che
/// la regola legge.
///
/// # Errors
///
/// `unsupported` con il motivo della regola A per una chiave o una major non
/// misurate; `data_mapping` se una colonna manca dai fatti.
pub fn admit(
    kind: ProviderKind,
    object: &str,
    schema: &SchemaRef,
    columns: &[(usize, String)],
    facts: &[ColumnFacts],
    server: &ServerFacts,
) -> Result<Vec<ColumnRewrites>> {
    #[cfg(test)]
    if plenora_database_testkit::write_probe::is_probe_table(object) {
        return Ok(Vec::new());
    }
    #[cfg(not(test))]
    let _ = object;
    let measured = admission_table(kind)?.server(&server.major)?;
    let mut rewrites = Vec::with_capacity(columns.len());
    for (index, name) in columns {
        let field = schema.fields().get(*index).ok_or_else(|| {
            DatabaseError::new(
                ErrorCategory::Internal,
                ErrorPhase::Prepare,
                Some(kind),
                "regola A: colonna fuori dallo schema dell'ingresso",
            )
        })?;
        let fact = facts
            .iter()
            .find(|candidate| &candidate.name == name)
            .ok_or_else(|| {
                DatabaseError::new(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Prepare,
                    Some(kind),
                    "regola A: colonna del target mancante",
                )
            })?;
        let (key, dimensions) = admission_key(field.data_type(), fact);
        rewrites.push(ColumnRewrites {
            field: *index,
            rewrites: plenora_database_core::admission_gate::write_column(
                &measured, &key, dimensions,
            )?,
            target_type: key.nativo,
        });
    }
    Ok(rewrites)
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

fn unproven_facts(kind: ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        Some(kind),
        "versione o colonne del server non lette: la regola A non e provata",
    )
}

#[cfg(test)]
#[path = "write_admission_tests.rs"]
mod tests;
