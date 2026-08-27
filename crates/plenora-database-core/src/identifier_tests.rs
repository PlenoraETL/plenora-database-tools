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
    // 64 char UTF-8 accentate (2 byte cad = 128 byte) → **passa**:
    // il limite MySQL e espresso in caratteri, non in byte.
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
