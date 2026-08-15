//! Quoting e validazione di identificatori SQL — single-source-of-truth.
//!
//! Prima di questo modulo la stessa logica era duplicata in:
//! - `plenora-database-core/src/portable/compiler.rs` (`quote_identifier`,
//!   `validate_identifier`)
//! - `plenora-database-sql/src/lib.rs` (`Renderer::quote`,
//!   `validate_identifier`)
//!
//! Con rischio concreto di divergenza tra le regole di validazione
//! (max length, char control, byte vs char boundary). Ora entrambi
//! delegano qui.
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
    /// `PostgreSQL`, `Oracle`, `Db2`, `SQLite`, `DuckDB`: `"identificatore"`
    /// (double-quote, escape raddoppiato).
    Postgres,
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
const MAX_IDENTIFIER_CHARS_MYSQL: usize = 64;
const MAX_IDENTIFIER_CHARS_SQL_SERVER: usize = 128;

/// Valida un identificatore SQL secondo le regole comuni:
/// - non vuoto,
/// - senza caratteri di controllo (ASCII 0x00-0x1F, 0x7F),
/// - lunghezza ≤ 63 byte (Postgres/MySQL) o 128 caratteri (SQL Server).
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
        IdentifierDialect::Postgres => {
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
mod tests {
    use super::*;

    #[test]
    fn empty_identifier_is_rejected_universally() {
        for d in [
            IdentifierDialect::Postgres,
            IdentifierDialect::Mysql,
            IdentifierDialect::SqlServer,
        ] {
            assert!(quote_identifier(d, "").is_err());
        }
    }

    #[test]
    fn control_characters_are_rejected() {
        assert!(quote_identifier(IdentifierDialect::Postgres, "t\x00evil").is_err());
        assert!(quote_identifier(IdentifierDialect::Mysql, "t\nevil").is_err());
        assert!(quote_identifier(IdentifierDialect::SqlServer, "t\x1bevil").is_err());
    }

    #[test]
    fn postgres_uses_double_quotes_and_escapes_them() {
        assert_eq!(
            quote_identifier(IdentifierDialect::Postgres, "users").unwrap(),
            r#""users""#
        );
        assert_eq!(
            quote_identifier(IdentifierDialect::Postgres, r#"evil"table"#).unwrap(),
            r#""evil""table""#
        );
    }

    #[test]
    fn mysql_uses_backticks_and_escapes_them() {
        assert_eq!(
            quote_identifier(IdentifierDialect::Mysql, "users").unwrap(),
            "`users`"
        );
        // Escape doppio compat con MySQL 5.7+/MariaDB.
        assert_eq!(
            quote_identifier(IdentifierDialect::Mysql, "evil`table").unwrap(),
            "`evil``table`"
        );
    }

    #[test]
    fn sql_server_uses_brackets_and_escapes_closing_bracket() {
        assert_eq!(
            quote_identifier(IdentifierDialect::SqlServer, "users").unwrap(),
            "[users]"
        );
        assert_eq!(
            quote_identifier(IdentifierDialect::SqlServer, "evil]table").unwrap(),
            "[evil]]table]"
        );
    }

    #[test]
    fn postgres_limit_is_63_bytes_not_chars() {
        let s = "a".repeat(63);
        assert!(quote_identifier(IdentifierDialect::Postgres, &s).is_ok());
        let s64 = "a".repeat(64);
        assert!(quote_identifier(IdentifierDialect::Postgres, &s64).is_err());
        // 32 char UTF-8 accentate (2 byte cad = 64 byte) → rifiuta.
        let s_multibyte = "à".repeat(32);
        assert_eq!(s_multibyte.len(), 64);
        assert!(quote_identifier(IdentifierDialect::Postgres, &s_multibyte).is_err());
    }

    #[test]
    fn mysql_limit_is_64_chars_not_bytes() {
        // MySQL 8.0 counts characters, not bytes — riferimento:
        // https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
        // 64 char ASCII (= 64 byte) → passa.
        let ascii64 = "a".repeat(64);
        assert!(quote_identifier(IdentifierDialect::Mysql, &ascii64).is_ok());
        // 65 char ASCII → rifiuta.
        let ascii65 = "a".repeat(65);
        assert!(quote_identifier(IdentifierDialect::Mysql, &ascii65).is_err());
        // 64 char UTF-8 accentate (2 byte cad = 128 byte) → **passa**
        // (era il bug pre-fix: aggregava byte anche per MySQL).
        let multibyte64 = "à".repeat(64);
        assert_eq!(multibyte64.len(), 128);
        assert!(quote_identifier(IdentifierDialect::Mysql, &multibyte64).is_ok());
        // 65 char UTF-8 accentate → rifiuta.
        let multibyte65 = "à".repeat(65);
        assert!(quote_identifier(IdentifierDialect::Mysql, &multibyte65).is_err());
    }

    #[test]
    fn sql_server_limit_is_128_chars_not_bytes() {
        // 128 char accentate (256 byte) → passa (SQL Server usa nvarchar).
        let s = "à".repeat(128);
        assert!(quote_identifier(IdentifierDialect::SqlServer, &s).is_ok());
        let s129 = "à".repeat(129);
        assert!(quote_identifier(IdentifierDialect::SqlServer, &s129).is_err());
    }
}
