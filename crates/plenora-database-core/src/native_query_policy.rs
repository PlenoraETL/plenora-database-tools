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
        return Err(crate::DatabaseError::invalid_plan(format!(
            "profilo native_query=Deny: keyword `{head}` non consentito"
        )));
    }
    Ok(())
}

/// Strip leading SQL comments (`-- line`, `/* block */`) preservando
/// il resto della stringa. Non è un parser SQL completo: interpreta
/// commenti a livello top-level, non dentro literal.
///
/// Esposto pub perché il CLI classifier duplica lo stesso pattern
/// (fix review post-review: dedup #96). Deve restare in sync con
/// `strip_leading_comments` in `pfm.rs` — meglio delegare qui.
#[must_use]
pub fn strip_leading_comments(sql: &str) -> String {
    strip_comments(sql)
}

/// Estrae la prima keyword SQL (uppercase ASCII) di uno statement,
/// dopo aver stripped commenti `--`/`/**/` e whitespace iniziale.
///
/// Usato dai classifier (CLI `execute-sql`, policy check) per
/// discriminare CRUD verbs da altri comandi.
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
mod tests {
    use super::*;

    #[test]
    fn allow_permits_any_sql() {
        assert!(enforce_policy(NativeQueryPolicy::Allow, "SELECT 1").is_ok());
        assert!(enforce_policy(NativeQueryPolicy::Allow, "CREATE TABLE t (x INT)").is_ok());
        assert!(enforce_policy(NativeQueryPolicy::Allow, "SET timezone = 'UTC'").is_ok());
    }

    #[test]
    fn deny_permits_oltp_verbs() {
        for sql in [
            "SELECT 1",
            "select id from users where id = $1",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
            "INSERT INTO t VALUES ($1)",
            "UPDATE t SET v = $1 WHERE id = $2",
            "DELETE FROM t WHERE id = $1",
            "VALUES (1), (2)",
            "TABLE users",
            "MERGE INTO t USING s ON s.id = t.id",
        ] {
            assert!(
                enforce_policy(NativeQueryPolicy::Deny, sql).is_ok(),
                "atteso ok: {sql}"
            );
        }
    }

    #[test]
    fn deny_blocks_ddl_and_session_commands() {
        for sql in [
            "CREATE TABLE t (x INT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN y INT",
            "TRUNCATE TABLE t",
            "GRANT SELECT ON t TO other",
            "REVOKE ALL ON t FROM other",
            "VACUUM t",
            "ANALYZE t",
            "SET timezone = 'UTC'",
            "SHOW server_version",
            "RESET ALL",
            "LOCK TABLE t",
            "LISTEN chan",
            "NOTIFY chan",
            "COPY t FROM STDIN",
            "EXPLAIN SELECT 1",
            "DO $$ BEGIN RAISE NOTICE 'x'; END $$",
            "CALL some_proc()",
        ] {
            assert!(
                enforce_policy(NativeQueryPolicy::Deny, sql).is_err(),
                "atteso rifiuto: {sql}"
            );
        }
    }

    #[test]
    fn deny_blocks_multi_statement() {
        let sql = "SELECT 1; DROP TABLE users;";
        assert!(enforce_policy(NativeQueryPolicy::Deny, sql).is_err());
    }

    #[test]
    fn allow_still_blocks_transaction_control() {
        for sql in [
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT sp",
            "RELEASE SAVEPOINT sp",
            "START TRANSACTION",
        ] {
            assert!(
                enforce_policy(NativeQueryPolicy::Allow, sql).is_err(),
                "atteso rifiuto tx-control: {sql}"
            );
        }
    }

    #[test]
    fn comments_before_keyword_are_stripped() {
        let sql = "-- audit intent\n/* block comment */\nSELECT 1";
        assert!(enforce_policy(NativeQueryPolicy::Deny, sql).is_ok());
    }

    #[test]
    fn semicolon_inside_string_is_ignored_by_splitter() {
        let sql = "SELECT 'not; a statement end'";
        assert!(enforce_policy(NativeQueryPolicy::Deny, sql).is_ok());
    }

    #[test]
    fn trailing_semicolon_is_ok() {
        let sql = "SELECT 1;";
        assert!(enforce_policy(NativeQueryPolicy::Deny, sql).is_ok());
    }
}
