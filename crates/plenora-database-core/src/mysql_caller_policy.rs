//! Lessico del bootstrap MySQL/MariaDB: backslash attivo, niente `ANSI_QUOTES`.
use super::{comment_end, lexical_error, quoted_end};

/// Protegge il profilo diagnostico anche con policy `Allow` e fuori transazione.
///
/// I SET del chiamante non possono cambiare `sql_notes`, `sql_mode`,
/// `max_error_count` o `sql_warnings`. I comandi interni non passano da qui.
///
/// # Errors
/// Rifiuta statement multipli, commenti eseguibili e SET delle variabili protette.
pub fn enforce_mysql_caller_sql(sql: &str) -> crate::Result<()> {
    let statements = statements(sql)?;
    if statements.len() != 1 {
        return Err(crate::DatabaseError::invalid_plan(
            "richiesto un solo statement MySQL/MariaDB",
        ));
    }
    let tokens = &statements[0];
    if tokens.first().is_some_and(|token| token == "SET") {
        let mut depth = 0_usize;
        let mut assignment = true;
        for (index, token) in tokens.iter().enumerate().skip(1) {
            match token.as_str() {
                "(" => depth += 1,
                ")" => depth = depth.saturating_sub(1),
                "," if depth == 0 => assignment = true,
                "=" if depth == 0 && assignment => {
                    assignment = false;
                    let name_index = if tokens[index - 1] == ":" {
                        index.saturating_sub(2)
                    } else {
                        index - 1
                    };
                    let user_variable = name_index > 0
                        && tokens[name_index - 1] == "@"
                        && (name_index < 2 || tokens[name_index - 2] != "@");
                    if !user_variable && protected_variable(&tokens[name_index]) {
                        return Err(crate::DatabaseError::invalid_plan(
                            "variabile del profilo diagnostico MySQL/MariaDB gestita dalla libreria",
                        ));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn protected_variable(name: &str) -> bool {
    matches!(
        name,
        "SQL_NOTES" | "SQL_MODE" | "MAX_ERROR_COUNT" | "SQL_WARNINGS"
    )
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
            tokens.push(if protected_variable(&name) {
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
