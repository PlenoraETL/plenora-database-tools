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
