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
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|_| invalid_sql())?;
    // These reserved T-SQL words cannot be unquoted identifiers. Checking
    // tokens also covers transaction commands after a semicolon-free SELECT
    // or inside a conditional block; literals and quoted identifiers are inert.
    if tokens.iter().any(|token| {
        matches!(token, Token::Word(word) if word.quote_style.is_none()
            && matches!(word.value.to_ascii_uppercase().as_str(),
                "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVE" | "DECLARE" | "FETCH" | "CLOSE"))
    }) {
        return Err(DatabaseError::invalid_plan(
            "il transaction scope gestisce i comandi transazionali",
        ));
    }
    if policy == NativeQueryPolicy::Deny {
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

fn invalid_sql() -> DatabaseError {
    DatabaseError::invalid_plan("profilo native_query: richiesto SQL compatibile con lo scope")
}

#[cfg(test)]
#[path = "native_query_policy_tests.rs"]
mod tests;
