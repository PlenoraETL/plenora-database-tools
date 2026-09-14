//! T-SQL accepts statement boundaries without semicolons.
use plenora_database_core::{DatabaseError, NativeQueryPolicy, Result};
use sqlparser::{
    ast::Statement,
    dialect::MsSqlDialect,
    parser::{Parser, ParserOptions},
    tokenizer::{Token, Tokenizer},
};

pub fn enforce_policy(policy: NativeQueryPolicy, sql: &str) -> Result<()> {
    let dialect = MsSqlDialect {};
    let mut tokens: Vec<_> = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|_| invalid_sql())?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    // These reserved T-SQL words cannot be unquoted identifiers. Checking
    // tokens also covers transaction commands after a semicolon-free SELECT
    // or inside a conditional block; literals and quoted identifiers are inert.
    if tokens.iter().enumerate().any(|(index, token)| {
        matches!(token, Token::Word(word) if word.quote_style.is_none()
            && (matches!(word.value.to_ascii_uppercase().as_str(),
                "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVE" | "DECLARE" | "CLOSE")
                || (word.value.eq_ignore_ascii_case("FETCH")
                    && pagination_fetch_end(&tokens[index + 1..]).is_none())))
    }) {
        return Err(DatabaseError::invalid_plan(
            "il transaction scope gestisce i comandi transazionali",
        ));
    }
    if policy == NativeQueryPolicy::Deny {
        normalize_fetch_counts(&mut tokens)?;
        let options = ParserOptions {
            require_semicolon_stmt_delimiter: false,
            ..ParserOptions::default()
        };
        let mut parser = Parser::new(&dialect)
            .with_recursion_limit(128)
            .with_options(options)
            .with_tokens(tokens);
        let statements = parser.parse_statements().map_err(|_| invalid_sql())?;
        if parser.peek_token().token != Token::EOF
            || !matches!(
                statements.as_slice(),
                [Statement::Query(_)
                    | Statement::Insert(_)
                    | Statement::Update(_)
                    | Statement::Delete(_)
                    | Statement::Merge(_)]
            )
        {
            return Err(invalid_sql());
        }
    }
    Ok(())
}

fn pagination_fetch_end(tokens: &[Token]) -> Option<usize> {
    // FETCH NEXT/FIRST <row count> ROWS ONLY belongs to SELECT, whereas
    // FETCH NEXT/FIRST FROM <cursor> changes cursor state. The count may
    // contain a parenthesized scalar subquery, so FROM is only decisive
    // outside parentheses.
    if !tokens
        .first()
        .is_some_and(|token| word_is(token, "NEXT") || word_is(token, "FIRST"))
    {
        return None;
    }
    let mut depth = 0_usize;
    for (index, token) in tokens.iter().enumerate().skip(1) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen if depth > 0 => depth -= 1,
            Token::RParen | Token::SemiColon | Token::EOF => return None,
            _ if depth == 0 => {
                if word_is(token, "FROM") || word_is(token, "INTO") {
                    return None;
                }
                if word_is(token, "ROW") || word_is(token, "ROWS") {
                    return (index > 1
                        && tokens
                            .get(index + 1)
                            .is_some_and(|next| word_is(next, "ONLY")))
                    .then_some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn normalize_fetch_counts(tokens: &mut Vec<Token>) -> Result<()> {
    let dialect = MsSqlDialect {};
    // sqlparser 0.62 parses FETCH quantities as literal values. Validate the
    // T-SQL scalar expression separately, then use a literal only in the
    // analysis token stream. The original SQL sent to the server is unchanged.
    for index in (0..tokens.len()).rev() {
        if !word_is(&tokens[index], "FETCH") {
            continue;
        }
        let end = pagination_fetch_end(&tokens[index + 1..]).ok_or_else(invalid_sql)?;
        let start = index + 2;
        let end = index + 1 + end;
        let mut count = Parser::new(&dialect)
            .with_recursion_limit(128)
            .with_tokens(tokens[start..end].to_vec());
        count.parse_expr().map_err(|_| invalid_sql())?;
        if count.peek_token().token != Token::EOF {
            return Err(invalid_sql());
        }
        tokens.splice(start..end, [Token::Number("0".into(), false)]);
    }
    Ok(())
}

fn word_is(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}

fn invalid_sql() -> DatabaseError {
    DatabaseError::invalid_plan("profilo native_query: richiesto SQL compatibile con lo scope")
}

#[cfg(test)]
#[path = "native_query_policy_tests.rs"]
mod tests;
