//! Quoting e validazione di identificatori SQL — single-source-of-truth.
//!
//! Il compilatore portable e i renderer delegano qui per condividere limiti,
//! caratteri vietati e distinzione fra lunghezze in byte e in caratteri.
//!
//! Il modulo espone un `IdentifierDialect` locale a `core` per evitare
//! di importare il `Dialect` più ricco di `plenora-database-sql`
//! (che dipende da core — creerebbe un ciclo).

use crate::{DatabaseError, Result};

/// Dialetti supportati per quoting identificatori. Enum locale a core
/// per non creare cicli con `plenora-database-sql::Dialect`.
///
/// `plenora-database-sql::Dialect` si mappa su questo via `From`
/// (`Oracle`/`Db2`/`Sqlite`/`Duckdb` tutti su `Postgres` che è compatibile
/// double-quote SQL standard).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierDialect {
    /// `PostgreSQL`, `Db2`, `SQLite`, `DuckDB`: `"identificatore"`
    /// (double-quote, escape raddoppiato).
    Postgres,
    /// `Oracle Database`: `"identificatore"`, con limite moderno di 128 byte.
    Oracle,
    /// `MySQL` / `MariaDB`: `` `identificatore` `` (backtick, escape
    /// raddoppiato).
    Mysql,
    /// `SQL Server` / `Sybase`: `[identificatore]` (square bracket, escape
    /// solo `]` raddoppiato).
    SqlServer,
}

/// Limiti per dialetto sui nomi di identificatori.
///
/// - `PostgreSQL`: `NAMEDATALEN` = 64 → 63 byte usabili. Il vincolo è
///   in **byte** perché il nome è memorizzato in un buffer C fisso.
/// - `MySQL` / `MariaDB`: 64 **caratteri** UTF-8 (non byte) —
///   riferimento "Identifier Length Limits" `MySQL` 8.0. Con caratteri
///   multibyte l'identificatore può superare 64 byte ma resta valido.
///   Ref: <https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html>
/// - `SQL Server` / `Sybase`: 128 caratteri Unicode (per compat con
///   `nvarchar(128)`), non byte.
const MAX_IDENTIFIER_BYTES_POSTGRES: usize = 63;
const MAX_IDENTIFIER_BYTES_ORACLE: usize = 128;
const MAX_IDENTIFIER_CHARS_MYSQL: usize = 64;
const MAX_IDENTIFIER_CHARS_SQL_SERVER: usize = 128;

/// Valida un identificatore SQL secondo le regole comuni:
/// - non vuoto,
/// - senza caratteri di controllo (ASCII 0x00-0x1F, 0x7F),
/// - lunghezza entro il limite del dialetto: 63 byte per `PostgreSQL`,
///   64 caratteri per MySQL/MariaDB, 128 caratteri per SQL Server.
///
/// Non impone regole di primo carattere alfabetico né disambigua parole
/// riservate: il consumer è responsabile del quoting per usarle in SQL.
///
/// # Errors
///
/// Restituisce `InvalidPlan` per identificatori vuoti, con caratteri
/// di controllo o oltre il limite del dialetto.
pub fn validate_identifier(dialect: IdentifierDialect, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(DatabaseError::invalid_plan("identificatore vuoto"));
    }
    if name.chars().any(char::is_control) {
        return Err(DatabaseError::invalid_plan(
            "identificatore contiene caratteri di controllo",
        ));
    }
    match dialect {
        IdentifierDialect::Postgres => {
            if name.len() > MAX_IDENTIFIER_BYTES_POSTGRES {
                return Err(DatabaseError::invalid_plan(
                    "identificatore PostgreSQL eccede 63 byte (NAMEDATALEN)",
                ));
            }
        }
        IdentifierDialect::Oracle => {
            if name.len() > MAX_IDENTIFIER_BYTES_ORACLE {
                return Err(DatabaseError::invalid_plan(
                    "identificatore Oracle eccede 128 byte",
                ));
            }
        }
        IdentifierDialect::Mysql => {
            if name.chars().count() > MAX_IDENTIFIER_CHARS_MYSQL {
                return Err(DatabaseError::invalid_plan(
                    "identificatore MySQL eccede 64 caratteri",
                ));
            }
        }
        IdentifierDialect::SqlServer => {
            if name.chars().count() > MAX_IDENTIFIER_CHARS_SQL_SERVER {
                return Err(DatabaseError::invalid_plan(
                    "identificatore SQL Server oltre 128 caratteri",
                ));
            }
        }
    }
    Ok(())
}

/// Quota un identificatore per il dialetto indicato dopo averlo
/// validato. Rifiuta caratteri di controllo e identificatori vuoti.
///
/// # Errors
///
/// Vedi `validate_identifier`.
pub fn quote_identifier(dialect: IdentifierDialect, name: &str) -> Result<String> {
    validate_identifier(dialect, name)?;
    match dialect {
        IdentifierDialect::Postgres | IdentifierDialect::Oracle => {
            let escaped = name.replace('"', "\"\"");
            Ok(format!("\"{escaped}\""))
        }
        IdentifierDialect::Mysql => {
            // MySQL escape del backtick: raddoppio (`` `` `` interno).
            // Compatibile con MySQL 5.7+ e MariaDB 10.x.
            let escaped = name.replace('`', "``");
            Ok(format!("`{escaped}`"))
        }
        IdentifierDialect::SqlServer => {
            let escaped = name.replace(']', "]]");
            Ok(format!("[{escaped}]"))
        }
    }
}

#[cfg(test)]
#[path = "identifier_tests.rs"]
mod tests;
