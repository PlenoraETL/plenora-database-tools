//! Native-query governance.
//!
//! Il PFM richiede di poter vietare, nel profilo applicativo di produzione,
//! l'esecuzione di SQL che non sia una query CRUD parametrizzata. Il gate è
//! qui: la validazione ha lo scopo di individuare pattern comuni di leak
//! vendor-specific o comandi amministrativi passati per errore attraverso il
//! transaction scope. Non è un parser SQL completo: usa un'analisi lessicale
//! lessicale delle keyword iniziali e dei confini degli statement.
//!
//! I comandi transazionali (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`,
//! `RELEASE`, `DECLARE`, `FETCH`, `CLOSE`) sono gestiti dalla libreria; se
//! passati come `Statement` dall'utente sono errore di uso anche in modalità
//! `Allow`.

use serde::{Deserialize, Serialize};

/// Politica del transaction scope per l'esecuzione di SQL "native".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NativeQueryPolicy {
    /// Default: consente qualsiasi SQL well-formed non transazionale.
    #[default]
    Allow,
    /// Profilo applicativo (raccomandato per il PFM): permette SOLO
    /// SELECT/WITH/INSERT/UPDATE/DELETE/VALUES/TABLE/MERGE. Nega DDL,
    /// comandi di sessione, e SQL con più di uno statement.
    Deny,
}

/// Verifica che lo statement sia compatibile con il policy corrente.
///
/// # Errors
///
/// Ritorna `InvalidPlan` se il policy è `Deny` e la SQL contiene un keyword
/// non nella allowlist OLTP oppure più di uno statement.
pub fn enforce_policy(policy: NativeQueryPolicy, sql: &str) -> crate::Result<()> {
    // Senza dialetto o SQL mode non si puo assumere la semantica del backslash.
    // Le letture compatibili devono rispettare la policy prima dell'I/O.
    let has_backslash = sql.contains('\\');
    let has_comments = sql.contains("/*");
    let mut valid = false;
    for (backslash, nested_comments) in [(false, false), (false, true), (true, false), (true, true)]
    {
        if (backslash && !has_backslash) || (nested_comments && !has_comments) {
            continue;
        }
        let Ok(heads) = statement_heads(sql, backslash, nested_comments) else {
            continue;
        };
        valid = true;
        if heads.iter().any(|head| is_transaction_control(head)) {
            return Err(crate::DatabaseError::invalid_plan(
                "il transaction scope gestisce BEGIN/COMMIT/ROLLBACK/SAVEPOINT: non passarli come statement",
            ));
        }
        if policy == NativeQueryPolicy::Deny
            && (heads.len() != 1 || !is_oltp_allowed_keyword(&heads[0]))
        {
            return Err(crate::DatabaseError::invalid_plan(
                "profilo native_query=Deny: richiesto un singolo statement CRUD",
            ));
        }
    }
    if valid {
        Ok(())
    } else {
        Err(lexical_error())
    }
}

/// Estrae la keyword iniziale senza interpretare come commenti i literal SQL.
/// Un input lessicalmente incompleto non dichiara una keyword qualificata.
#[must_use]
pub fn statement_head(sql: &str) -> String {
    statement_heads(sql, false, true)
        .ok()
        .and_then(|heads| heads.into_iter().next())
        .unwrap_or_default()
}

fn lexical_error() -> crate::DatabaseError {
    crate::DatabaseError::invalid_plan("statement SQL con delimitatori non validi")
}

/// Legge solo le teste; il testo originale non viene modificato o eseguito.
fn statement_heads(
    sql: &str,
    backslash: bool,
    nested_comments: bool,
) -> crate::Result<Vec<String>> {
    let bytes = sql.as_bytes();
    let mut heads = Vec::new();
    let mut head = None;
    let mut position = 0;
    while position < bytes.len() {
        let rest = &bytes[position..];
        if rest[0].is_ascii_whitespace() {
            position += 1;
        } else if rest.starts_with(b"--") {
            position += rest
                .iter()
                .position(|byte| matches!(byte, b'\n' | b'\r'))
                .unwrap_or(rest.len());
        } else if rest.starts_with(b"/*") {
            // I commenti eseguibili MySQL/MariaDB non sono commenti inerti.
            if rest.starts_with(b"/*!") || rest.starts_with(b"/*M!") {
                return Err(lexical_error());
            }
            position = comment_end(bytes, position + 2, nested_comments)?;
        } else if rest[0] == b';' {
            if let Some(value) = head.take() {
                heads.push(value);
            }
            position += 1;
        } else {
            if head.is_none() {
                let length = rest
                    .iter()
                    .take_while(|byte| byte.is_ascii_alphabetic())
                    .count();
                head = Some(sql[position..position + length].to_ascii_uppercase());
            }
            position = token_end(sql, position, backslash)?;
        }
    }
    if let Some(value) = head {
        heads.push(value);
    }
    Ok(heads)
}

fn comment_end(bytes: &[u8], mut position: usize, nested_comments: bool) -> crate::Result<usize> {
    let mut depth = 1;
    while position < bytes.len() {
        if nested_comments && bytes[position..].starts_with(b"/*") {
            depth += 1;
            position += 2;
        } else if bytes[position..].starts_with(b"*/") {
            depth -= 1;
            position += 2;
            if depth == 0 {
                return Ok(position);
            }
        } else {
            position += 1;
        }
    }
    Err(lexical_error())
}

fn token_end(sql: &str, position: usize, backslash: bool) -> crate::Result<usize> {
    let bytes = sql.as_bytes();
    let rest = &bytes[position..];
    match rest[0] {
        b'\'' | b'"' | b'`' | b'[' => {
            let closing = if rest[0] == b'[' { b']' } else { rest[0] };
            quoted_end(bytes, position + 1, closing, backslash && rest[0] != b'[')
        }
        b'$' => {
            let length = rest[1..]
                .iter()
                .take_while(|byte| byte.is_ascii_alphanumeric() || **byte == b'_')
                .count()
                + 1;
            if rest.get(length) == Some(&b'$')
                && (length == 1 || !rest[1].is_ascii_digit())
                && (position == 0
                    || !(bytes[position - 1].is_ascii_alphanumeric()
                        || matches!(bytes[position - 1], b'_' | b'$')))
            {
                let delimiter = &sql[position..=position + length];
                let start = position + length + 1;
                sql[start..]
                    .find(delimiter)
                    .map(|offset| start + offset + delimiter.len())
                    .ok_or_else(lexical_error)
            } else {
                Ok(position + 1)
            }
        }
        // Oracle alternative quoting: q'[literal]', q'!literal!'.
        b'q' | b'Q' if rest.get(1) == Some(&b'\'') && rest.len() >= 4 => {
            let close = match rest[2] {
                b'[' => b']',
                b'(' => b')',
                b'{' => b'}',
                b'<' => b'>',
                other => other,
            };
            rest[3..]
                .windows(2)
                .position(|pair| pair == [close, b'\''])
                .map(|offset| position + offset + 5)
                .ok_or_else(lexical_error)
        }
        _ => Ok(position + 1),
    }
}

fn quoted_end(
    bytes: &[u8],
    mut position: usize,
    close: u8,
    backslash: bool,
) -> crate::Result<usize> {
    while position < bytes.len() {
        if backslash && bytes[position] == b'\\' {
            position += 2;
        } else if bytes[position] == close {
            position += 1;
            if bytes.get(position) != Some(&close) {
                return Ok(position);
            }
            position += 1;
        } else {
            position += 1;
        }
    }
    Err(lexical_error())
}

fn is_oltp_allowed_keyword(head: &str) -> bool {
    matches!(
        head,
        "SELECT" | "WITH" | "INSERT" | "UPDATE" | "DELETE" | "VALUES" | "TABLE" | "MERGE"
    )
}

fn is_transaction_control(head: &str) -> bool {
    matches!(
        head,
        "BEGIN"
            | "START"
            | "COMMIT"
            | "ROLLBACK"
            | "SAVEPOINT"
            | "RELEASE"
            | "DECLARE"
            | "FETCH"
            | "CLOSE"
    )
}

#[cfg(test)]
#[path = "native_query_policy_tests.rs"]
mod tests;
