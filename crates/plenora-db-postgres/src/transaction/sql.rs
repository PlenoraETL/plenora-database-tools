//! Helper SQL puri: quoting, classificazione statement, costruzione BEGIN.

use crate::error::public_error;
use plenora_database_core::transaction::{AccessMode, IsolationLevel, TransactionOptions};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase};
use tokio_postgres::types::Type;

/// Costruisce lo statement `BEGIN` con le opzioni richieste.
///
/// Le opzioni non specificate ricadono sul default della sessione. Il
/// `statement_timeout` è applicato con `SET LOCAL` all'interno della
/// transazione, quindi viene automaticamente ripristinato al commit/rollback.
pub(super) fn build_begin_sql(options: &TransactionOptions) -> String {
    let mut parts: Vec<&'static str> = vec!["BEGIN"];
    if let Some(level) = options.isolation {
        parts.push(match level {
            IsolationLevel::ReadUncommitted => "ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "ISOLATION LEVEL READ COMMITTED",
            IsolationLevel::RepeatableRead => "ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel::Serializable => "ISOLATION LEVEL SERIALIZABLE",
        });
    }
    if let Some(mode) = options.access_mode {
        parts.push(match mode {
            AccessMode::ReadWrite => "READ WRITE",
            AccessMode::ReadOnly => "READ ONLY",
        });
    }
    if matches!(options.deferrable, Some(true)) {
        parts.push("DEFERRABLE");
    } else if matches!(options.deferrable, Some(false)) {
        parts.push("NOT DEFERRABLE");
    }
    let mut sql = parts.join(" ");
    sql.push(';');
    if let Some(ms) = options.statement_timeout_ms {
        use std::fmt::Write;
        write!(sql, " SET LOCAL statement_timeout = {ms};").expect("write to String non fallisce");
    }
    sql
}

pub(super) fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

pub(super) fn phase_of(sql: &str) -> ErrorPhase {
    let trimmed = sql.trim_start();
    let head: String = trimmed
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase();
    match head.as_str() {
        "SELECT" | "WITH" | "SHOW" | "TABLE" | "VALUES" | "EXPLAIN" => ErrorPhase::Read,
        _ => ErrorPhase::Write,
    }
}

pub(super) fn unsupported_param(message: &str) -> DatabaseError {
    public_error(ErrorCategory::Unsupported, ErrorPhase::Write, false, message)
}

pub(super) fn unsupported_column_type(pg_type: &Type) -> DatabaseError {
    public_error(
        ErrorCategory::Unsupported,
        ErrorPhase::Read,
        false,
        &format!("tipo di colonna PostgreSQL non supportato nel path OLTP: {pg_type}"),
    )
}
