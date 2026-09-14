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
