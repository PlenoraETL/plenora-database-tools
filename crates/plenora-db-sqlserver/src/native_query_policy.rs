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
mod tests {
    use super::*;

    #[test]
    fn transaction_commands_cannot_hide_in_semicolon_free_batches() {
        for command in [
            "COMMIT",
            "ROLLBACK",
            "SAVE TRANSACTION s",
            "BEGIN TRANSACTION",
        ] {
            for prefix in ["", "SELECT 1 ", "SELECT 'COMMIT'; /* comment */ "] {
                for policy in [NativeQueryPolicy::Allow, NativeQueryPolicy::Deny] {
                    assert!(enforce_policy(policy, &format!("{prefix}{command}")).is_err());
                }
            }
        }
    }

    #[test]
    fn deny_checks_complete_statements_and_preserves_nested_crud() {
        for sql in [
            "SELECT 1 CREATE TABLE t (id INT)",
            "SELECT 1 SELECT 2",
            "SELECT 1 END CREATE TABLE t (id INT)",
            "SELECT 1; DROP TABLE t",
            "WITH c AS (SELECT 1 AS id) SELECT * FROM c DELETE FROM t",
        ] {
            assert!(
                enforce_policy(NativeQueryPolicy::Deny, sql).is_err(),
                "{sql}"
            );
        }
        for sql in [
            "SELECT CASE WHEN 1 = 1 THEN 'COMMIT' ELSE 'ROLLBACK' END AS [SAVE]",
            "WITH c AS (SELECT 1 AS id) SELECT * FROM c",
            "SELECT 1 UNION ALL SELECT 2",
            "INSERT INTO t (id) SELECT id FROM other WHERE id = @P1",
            "SELECT [COMMIT], \"ROLLBACK\" FROM [BEGIN] WHERE id IN (SELECT id FROM t)",
        ] {
            assert!(
                enforce_policy(NativeQueryPolicy::Deny, sql).is_ok(),
                "{sql}"
            );
        }
    }
}
