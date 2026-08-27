//! Native-query governance.
//!
//! Il PFM richiede di poter vietare, nel profilo applicativo di produzione,
//! l'esecuzione di SQL che non sia una query CRUD parametrizzata. Il gate è
//! qui: la validazione ha lo scopo di individuare pattern comuni di leak
//! vendor-specific o comandi amministrativi passati per errore attraverso il
//! transaction scope. Non è un parser SQL completo: usa un'analisi lessicale
//! best-effort del solo primo keyword e del count di statement.
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
    let stripped = strip_comments(sql);
    if is_forbidden_transaction_control(&stripped) {
        return Err(crate::DatabaseError::invalid_plan(
            "il transaction scope gestisce BEGIN/COMMIT/ROLLBACK/SAVEPOINT: non passarli come statement",
        ));
    }
    if policy == NativeQueryPolicy::Allow {
        return Ok(());
    }
    let segments = split_statements(&stripped);
    if segments.len() > 1 {
        return Err(crate::DatabaseError::invalid_plan(
            "profilo native_query=Deny: multi-statement non consentito",
        ));
    }
    let first = segments
        .first()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| crate::DatabaseError::invalid_plan("statement SQL vuoto"))?;
    let head = extract_first_keyword(first);
    if !is_oltp_allowed_keyword(&head) {
        // La keyword viene da testo SQL libero, e sotto questo profilo il
        // chiamante puo aver mandato uno statement che non doveva: ricopiarla
        // significherebbe rimettere SQL nel messaggio.
        return Err(crate::DatabaseError::invalid_plan(
            "profilo native_query=Deny: lo statement non e fra le forme CRUD ammesse",
        ));
    }
    Ok(())
}

/// Estrae la prima keyword SQL (uppercase ASCII) di uno statement,
/// dopo aver stripped commenti `--`/`/**/` e whitespace iniziale.
///
/// Usato dai classifier (CLI `execute-sql`, policy check) per
/// discriminare CRUD verbs da altri comandi.
///
/// **Limite noto**: `strip_comments` non è literal-aware — commenti
/// dentro string literal SQL (`'-- non è commento'`) vengono comunque
/// riconosciuti come commenti. Per la use case classifier (leggere la
/// PRIMA keyword) è ininfluente: il primo commento eventuale è
/// leading e i literal arrivano dopo. Non esporre `strip_comments`
/// standalone per non incoraggiare usi in cui il bug conta.
#[must_use]
pub fn statement_head(sql: &str) -> String {
    strip_comments(sql)
        .trim_start()
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase()
}

fn strip_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '-' if chars.peek() == Some(&'-') => {
                // -- line comment
                while let Some(&n) = chars.peek() {
                    if n == '\n' {
                        break;
                    }
                    chars.next();
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                // /* block comment */
                chars.next(); // '*'
                let mut prev = ' ';
                for n in chars.by_ref() {
                    if prev == '*' && n == '/' {
                        break;
                    }
                    prev = n;
                }
            }
            _ => out.push(c),
        }
    }
    out
}

fn split_statements(sql: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_double => {
                current.push(c);
                if in_single && chars.peek() == Some(&'\'') {
                    // '' escape all'interno di stringa
                    current.push(chars.next().expect("peek"));
                } else {
                    in_single = !in_single;
                }
            }
            '"' if !in_single => {
                current.push(c);
                in_double = !in_double;
            }
            ';' if !in_single && !in_double => {
                if !current.trim().is_empty() {
                    segments.push(current.clone());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        segments.push(current);
    }
    segments
}

fn extract_first_keyword(sql: &str) -> String {
    sql.trim()
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase()
}

fn is_oltp_allowed_keyword(head: &str) -> bool {
    matches!(
        head,
        "SELECT" | "WITH" | "INSERT" | "UPDATE" | "DELETE" | "VALUES" | "TABLE" | "MERGE"
    )
}

fn is_forbidden_transaction_control(sql: &str) -> bool {
    // Rilevo anche solo se una parola-chiave transazionale è la testa del
    // primo statement (la governance della tx è della libreria, mai del
    // consumer). Uso il primo segmento perché uno stray ";" successivo
    // sarebbe già trattato dal policy Deny come multi-statement.
    if let Some(first) = sql.split(';').next() {
        let head = extract_first_keyword(first);
        return matches!(
            head.as_str(),
            "BEGIN"
                | "START"
                | "COMMIT"
                | "ROLLBACK"
                | "SAVEPOINT"
                | "RELEASE"
                | "DECLARE"
                | "FETCH"
                | "CLOSE"
        );
    }
    false
}

#[cfg(test)]
#[path = "native_query_policy_tests.rs"]
mod tests;
