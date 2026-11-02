//! Lessico del bootstrap MySQL/MariaDB: backslash attivo, niente `ANSI_QUOTES`.
use super::{comment_end, lexical_error, quoted_end};

/// Protegge il bootstrap anche con policy Allow e fuori transazione.
/// I comandi interni non passano da qui.
///
/// # Errors
/// Rifiuta batch, commenti eseguibili e SET diversi da assegnazioni @utente ASCII.
pub fn enforce_mysql_caller_sql(sql: &str) -> crate::Result<()> {
    let statements = statements(sql)?;
    if statements.len() != 1 {
        return Err(crate::DatabaseError::invalid_plan(
            "richiesto un solo statement MySQL/MariaDB",
        ));
    }
    let tokens = &statements[0];
    if tokens.first().is_some_and(|token| token == "USE") {
        return Err(crate::DatabaseError::invalid_plan(
            "USE non ammesso nello SQL del chiamante",
        ));
    }
    if tokens.first().is_some_and(|token| token == "SET") {
        user_assignments(&tokens[1..])?;
    }
    Ok(())
}

/// Solo DML/letture e variabili utente nel transaction scope.
///
/// L'allowlist esclude anche CALL/PREPARE/EXECUTE e DDL temporaneo non rollbackabile.
/// Riferimenti: <https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html>
/// e <https://mariadb.com/docs/server/reference/sql-statements/transactions/sql-statements-that-cause-an-implicit-commit>.
///
/// # Errors
/// Rifiuta prima dell'invio ogni verbo non qualificato per lo scope.
pub fn enforce_mysql_transaction_sql(sql: &str) -> crate::Result<()> {
    enforce_mysql_caller_sql(sql)?;
    let statements = statements(sql)?;
    let tokens = &statements[0];
    let first = tokens
        .iter()
        .position(|token| token != "(")
        .unwrap_or(tokens.len());
    let verb = tokens.get(first).map_or("", String::as_str);
    if matches!(verb, "EXPLAIN" | "DESCRIBE" | "DESC")
        && tokens
            .get(first + 1)
            .is_some_and(|token| token == "ANALYZE")
    {
        return Err(crate::DatabaseError::invalid_plan(
            "EXPLAIN ANALYZE non ammesso nel transaction scope",
        ));
    }
    if !matches!(
        verb,
        "SELECT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "REPLACE"
            | "WITH"
            | "VALUES"
            | "TABLE"
            | "DO"
            | "SET"
            | "SHOW"
            | "EXPLAIN"
            | "DESCRIBE"
            | "DESC"
    ) {
        return Err(crate::DatabaseError::invalid_plan(
            "statement MySQL/MariaDB non ammesso nel transaction scope",
        ));
    }
    Ok(())
}

fn user_assignments(tokens: &[String]) -> crate::Result<()> {
    let reject = || {
        crate::DatabaseError::invalid_plan(
            "SET MySQL/MariaDB richiede esclusivamente variabili utente ASCII",
        )
    };
    let mut start = 0;
    loop {
        if tokens.get(start).map(String::as_str) != Some("@")
            || !tokens
                .get(start + 1)
                .is_some_and(|name| ascii_user_name(name))
        {
            return Err(reject());
        }
        let mut position = start + 2;
        if tokens.get(position).map(String::as_str) == Some(":") {
            position += 1;
        }
        if tokens.get(position).map(String::as_str) != Some("=") {
            return Err(reject());
        }
        position += 1;
        let value_start = position;
        let mut depth = 0_usize;
        while position < tokens.len() {
            match tokens[position].as_str() {
                "(" => depth += 1,
                ")" => depth = depth.checked_sub(1).ok_or_else(reject)?,
                "," if depth == 0 => break,
                _ => {}
            }
            position += 1;
        }
        if position == value_start || depth != 0 {
            return Err(reject());
        }
        if position == tokens.len() {
            return Ok(());
        }
        start = position + 1;
    }
}

fn ascii_user_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'.'))
}

pub(super) fn statements(sql: &str) -> crate::Result<Vec<Vec<String>>> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut tokens = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        let rest = &bytes[position..];
        if rest[0].is_ascii_whitespace() || rest[0] == 0x0b {
            position += 1;
        } else if rest[0] == b'#'
            || (rest.starts_with(b"--")
                && rest
                    .get(2)
                    .is_none_or(|b| b.is_ascii_whitespace() || b.is_ascii_control()))
        {
            position += rest
                .iter()
                .position(|b| matches!(b, b'\r' | b'\n'))
                .unwrap_or(rest.len());
        } else if rest.starts_with(b"/*") {
            if rest.starts_with(b"/*!") || rest.starts_with(b"/*M!") {
                return Err(lexical_error());
            }
            position = comment_end(bytes, position + 2, false)?;
        } else if rest[0] == b';' {
            if !tokens.is_empty() {
                statements.push(std::mem::take(&mut tokens));
            }
            position += 1;
        } else if matches!(rest[0], b'\'' | b'"' | b'`') {
            let quote = rest[0];
            let end = quoted_end(bytes, position + 1, quote, quote != b'`')?;
            // Conservare anche i nomi quotati dei SET; i literal non sono
            // reinterpretati come delimitatori o commenti.
            let name = sql[position + 1..end - 1].to_ascii_uppercase();
            // Una parentesi o virgola nel literal non separa assegnazioni.
            tokens.push(if ascii_user_name(&name) {
                name
            } else {
                "<quoted>".to_owned()
            });
            position = end;
        } else if rest[0].is_ascii_alphanumeric() || matches!(rest[0], b'_' | b'$') {
            let length = rest
                .iter()
                .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
                .count();
            tokens.push(sql[position..position + length].to_ascii_uppercase());
            position += length;
        } else {
            // Nessun dollar quoting, q quoting o identificatore fra quadre.
            // Un byte non ASCII non puo introdurre un delimitatore SQL.
            tokens.push(char::from(rest[0]).to_string());
            position += 1;
        }
    }
    if !tokens.is_empty() {
        statements.push(tokens);
    }
    Ok(statements)
}
