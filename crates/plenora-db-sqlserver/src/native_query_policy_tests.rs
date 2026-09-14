use super::*;

#[test]
fn transaction_commands_cannot_hide_in_semicolon_free_batches() {
    for command in [
        "COMMIT",
        "ROLLBACK",
        "SAVE TRANSACTION s",
        "BEGIN TRANSACTION",
        "FETCH NEXT FROM c",
        "FETCH FIRST FROM c",
        "FETCH c",
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
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY COMMIT",
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY FETCH NEXT FROM c",
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT (1) SELECT 2 ROWS ONLY",
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT (SELECT 1; COMMIT) ROWS ONLY",
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
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH FIRST @P1 ROW ONLY",
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT (SELECT 1) ROWS ONLY",
    ] {
        assert!(
            enforce_policy(NativeQueryPolicy::Deny, sql).is_ok(),
            "{sql}"
        );
        assert!(
            enforce_policy(NativeQueryPolicy::Allow, sql).is_ok(),
            "{sql}"
        );
    }
}
