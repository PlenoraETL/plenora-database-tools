use super::*;

#[test]
fn literals_cannot_hide_a_second_statement_or_transaction_control() {
    for literal in [
        "'--'",
        "'/*'",
        "'it''s; --'",
        "\"--\"",
        "`/*`",
        "[--]",
        "$$--;/*$$",
        "$tag$'; COMMIT$tag$",
        "q'[--;/*]'",
        "q'!'; COMMIT!'",
        "'é;界'",
    ] {
        let select = format!("SELECT {literal}");
        assert!(
            enforce_policy(NativeQueryPolicy::Deny, &select).is_ok(),
            "{select}"
        );
        assert!(
            enforce_policy(NativeQueryPolicy::Deny, &format!("{select}; DROP TABLE t")).is_err()
        );
        assert!(enforce_policy(NativeQueryPolicy::Allow, &format!("{select}; COMMIT")).is_err());
    }
}

#[test]
fn all_statement_heads_are_checked_after_empty_segments_and_nested_comments() {
    for control in [
        "BEGIN",
        "START TRANSACTION",
        "COMMIT",
        "END",
        "ABORT",
        "ROLLBACK",
        "SAVEPOINT s",
        "RELEASE SAVEPOINT s",
        "DECLARE c CURSOR FOR SELECT 1",
        "FETCH c",
        "CLOSE c",
    ] {
        for prefix in [
            ";",
            ";;; /* outer /* nested */ rest */",
            "SELECT 1;",
            "SELECT 1; -- note\r\n;",
        ] {
            assert!(
                enforce_policy(NativeQueryPolicy::Allow, &format!("{prefix}{control}")).is_err()
            );
        }
    }
    assert!(enforce_policy(NativeQueryPolicy::Allow, "SELECT 1; SELECT 2").is_ok());
    assert_eq!(statement_head("/* comment */ SELECT/**/1"), "SELECT");
}

#[test]
fn malformed_and_executable_comments_are_not_qualified() {
    for sql in [
        "SELECT 'unterminated",
        "SELECT /* unfinished",
        "SELECT $$unfinished",
        "/*! COMMIT */",
        "/*M! COMMIT */",
    ] {
        assert!(enforce_policy(NativeQueryPolicy::Allow, sql).is_err());
        assert!(enforce_policy(NativeQueryPolicy::Deny, sql).is_err());
    }
    for sql in [
        r"SELECT 'it\'s fine'",
        r"SELECT E'it\'s fine'",
        "SELECT /* outer /* inner */ end */ 1",
    ] {
        assert!(
            enforce_policy(NativeQueryPolicy::Deny, sql).is_ok(),
            "{sql}"
        );
    }
}

#[test]
fn dialect_ambiguity_cannot_hide_a_second_statement() {
    for sql in [
        "SELECT 1 /* outer /* inner */; COMMIT; /* end */",
        "SELECT 1 /* outer /* inner */; DROP TABLE t; /* end */",
        "SELECT a_$tag$; COMMIT; SELECT b_$tag$",
        "SELECT 1--1; COMMIT",
        "SELECT 1--1; DROP TABLE t",
    ] {
        assert!(enforce_policy(NativeQueryPolicy::Deny, sql).is_err());
    }
    for sql in ["SELECT 1--1; COMMIT", "# note\nCOMMIT"] {
        assert!(enforce_policy(NativeQueryPolicy::Allow, sql).is_err());
    }
    assert!(enforce_policy(NativeQueryPolicy::Deny, "SELECT 1--1").is_ok());
}

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

#[test]
fn mysql_lexer_uses_only_mysql_delimiters() {
    for sql in [
        "SELECT 1 AS $a$; INSERT INTO t(d) VALUES ('1.239'); SELECT 1 AS $a$",
        "SELECT q'[' FROM (SELECT 1 q) x; INSERT INTO t(d) VALUES ('1.239'); SELECT ']' FROM DUAL",
        "SELECT [x; SELECT 2]",
        "SELECT `x\\`; SELECT 2",
        "SELECT 1--x; SELECT 2",
        "/*! SELECT 1 */",
        "/*M! SELECT 1 */",
        "SELECT 'unfinished",
        "SELECT `unfinished",
        "SELECT 1 /* unfinished",
        "SELECT 1 /* outer /* inner */; SELECT 2 */",
        "# only a comment",
    ] {
        assert!(super::enforce_single_mysql_statement(sql).is_err(), "{sql}");
    }
    for sql in [
        "SELECT '$a$;x$a$', q'[x]', '[x;y]'",
        "SELECT 'a\\';b', \"a\\\";b\", `a``;b`",
        "SELECT 1 # ; SELECT 2\n;",
        "SELECT 1 -- ; SELECT 2\r;",
        "SELECT 1--\t; SELECT 2\n;",
        "SELECT '/*!', '/*M!', '#', '-- '",
        "/* prefix */ SELECT '\u{00e8};\u{65e5}\u{672c}'; /* suffix */",
        "SELECT $a$",
    ] {
        assert!(super::enforce_single_mysql_statement(sql).is_ok(), "{sql}");
    }
}

#[test]
fn mysql_caller_cannot_change_diagnostic_variables() {
    for variable in ["sql_notes", "sql_mode", "max_error_count", "sql_warnings"] {
        for sql in [
            format!("SET {variable}=0"),
            format!("SET {variable}:=0"),
            format!("SET STATEMENT {variable}=0 FOR SELECT 1"),
            format!("SET SESSION {variable}=0"),
            format!("\u{000b}SET\u{000b}SESSION {variable}=0"),
            format!("SET LOCAL {variable}=0"),
            format!("SET @@session.{variable}=0"),
            format!("SET @@{variable}=0"),
            format!("SET @@global.`{variable}`=0"),
            format!("SET @innocent=1, {variable}=0"),
            format!("SET @innocent='(', {variable}=0"),
            format!("SET @innocent=CONCAT('a,b', 'c'), SESSION {variable}=0"),
            format!("/*prefix*/ sEt /*x*/ `{variable}` /*x*/ = 0"),
        ] {
            let error = super::enforce_mysql_caller_sql(&sql).unwrap_err();
            assert_eq!(error.remote_effect, crate::RemoteEffect::None);
            assert!(!error.message.contains(variable));
        }
    }
    for sql in [
        "SET @sql_notes = 0",
        "SET @`sql_mode` = 'ordinary user variable'",
        "SELECT @@session.sql_notes",
        "SELECT 'SET sql_notes=0'",
        "SET @x='sql_notes=0, sql_mode=0'",
        "SET @x='sql_notes'=0",
        "SET @x=(SELECT sql_notes=0 FROM t)",
    ] {
        assert!(super::enforce_mysql_caller_sql(sql).is_ok(), "{sql}");
    }
}

#[test]
fn mysql_structural_session_and_transaction_boundary() {
    for sql in [
        "SET NAMES gbk",
        "SET NAMES sjis",
        "SET CHARACTER SET big5",
        "SET NAMES latin1",
        "SET character_set_client='gbk'",
        "SET collation_connection='latin1_bin'",
        "SET \u{017f}ql_mode=''",
        "SET @\u{017f}=1",
        "SET @x=1, time_zone='+00:00'",
        "SET GLOBAL x=1",
        "SET SESSION x=1",
        "SET LOCAL x=1",
        "SET PERSIST x=1",
        "SET @@autocommit=1",
        "SET @x=1, @@x=2",
        "SET sql_select_limit=1",
        "SET foreign_key_checks=0",
        "SET unique_checks=0",
        "SET time_zone='+00:00'",
        "SET autocommit=1",
        "SET @x=1,",
        "SET @x=",
        "SET @x=(1",
        "SET @x=1)",
    ] {
        assert!(super::enforce_mysql_caller_sql(sql).is_err(), "{sql}");
    }
    for verb in [
        "CREATE", "TRUNCATE", "ALTER", "DROP", "RENAME", "LOCK", "UNLOCK", "START", "BEGIN",
        "LOAD", "ANALYZE", "OPTIMIZE", "REPAIR", "GRANT", "REVOKE", "CALL", "EXECUTE", "PREPARE",
        "FLUSH", "RESET",
    ] {
        let error = super::enforce_mysql_transaction_sql(&format!("{verb} t")).unwrap_err();
        assert_eq!(error.remote_effect, crate::RemoteEffect::None);
    }
    for sql in [
        "SET @x=1, @y=CONCAT('a,b', 'c')",
        "SET @`x`:=1",
        r"SELECT 'gbk\\'",
    ] {
        assert!(super::enforce_mysql_transaction_sql(sql).is_ok(), "{sql}");
    }
}

#[test]
fn mysql_charset_cannot_change_backslash_lexing() {
    // U+7E17 contiene un trail byte backslash in GBK; la connessione deve
    // conservare UTF-8, il solo lessico con cui analizziamo lo SQL.
    for sql in [
        "SET NAMES gbk",
        "SET character_set_client='gbk'",
        "SET @x='\u{7e17}', character_set_connection='gbk'",
        "SET NAMES sjis",
        "SET NAMES big5",
    ] {
        assert!(super::enforce_mysql_caller_sql(sql).is_err());
    }
    assert!(super::enforce_mysql_caller_sql("SELECT '\u{7e17}\\\\'; DO 0").is_err());
    assert!(super::enforce_mysql_caller_sql("SELECT '\u{7e17}\\\\'").is_ok());
}

#[test]
fn mysql_read_verbs_and_parenthesized_queries_are_admitted() {
    for sql in [
        "SHOW TABLES",
        "EXPLAIN SELECT 1",
        "EXPLAIN FORMAT=JSON SELECT 1",
        "DESCRIBE t",
        "DESC t",
        "(SELECT 1) UNION (SELECT 2)",
        "((SELECT 1))",
        "(/*comment*/ SELECT 1)",
    ] {
        assert!(super::enforce_mysql_transaction_sql(sql).is_ok(), "{sql}");
    }
    for sql in [
        "EXPLAIN ANALYZE SELECT 1",
        "DESC ANALYZE SELECT 1",
        "USE secret",
        "(USE secret)",
    ] {
        assert!(super::enforce_mysql_transaction_sql(sql).is_err(), "{sql}");
    }
}

#[test]
fn mysql_use_and_administrative_set_are_refused_outside_transactions() {
    for sql in [
        "USE secret",
        "/*c*/ USE `secret`",
        "SET GLOBAL sql_notes=0",
        "SET PASSWORD='secret'",
        "SET ROLE role1",
        "SET DEFAULT ROLE role1",
    ] {
        let error = super::enforce_mysql_caller_sql(sql).unwrap_err();
        assert!(!error.message.contains("secret"));
        assert_eq!(error.remote_effect, crate::RemoteEffect::None);
    }
}
