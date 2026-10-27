use crate::parameter_catalog::bindings;

#[test]
fn direct_markers_keep_positions_and_identifier_case() {
    for (sql, expected) in [
        (
            "INSERT INTO s.t (id, c) VALUES (?, ?)",
            vec![Some("ID"), Some("C")],
        ),
        (
            "INSERT INTO s.t (id, c) VALUES (1, ?), (2, ?)",
            vec![Some("C"), Some("C")],
        ),
        (
            "SELECT '?' FROM s.t WHERE id = ? AND c = ? -- ?",
            vec![Some("ID"), Some("C")],
        ),
        (
            "SELECT * FROM s.t WHERE c IN (?, ?)",
            vec![Some("C"), Some("C")],
        ),
        (
            "UPDATE s.t SET c = ? WHERE id = ?",
            vec![Some("C"), Some("ID")],
        ),
        ("DELETE FROM s.t WHERE c = ?", vec![Some("C")]),
        (
            "INSERT INTO \"s\".\"t\" (\"Mixed\") VALUES (?)",
            vec![Some("Mixed")],
        ),
        (
            "SELECT * FROM s.t WHERE LENGTH(c) = ? AND c = ?",
            vec![None, Some("C")],
        ),
    ] {
        let (_, actual) = bindings(sql, expected.len()).expect("SQL diretto risolto");
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|name| name.map(str::to_owned))
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
}

#[test]
fn unproven_scopes_and_expressions_do_not_override_descriptors() {
    for sql in [
        "WITH t AS (SELECT 'x' AS c FROM s.t) SELECT * FROM t WHERE c = ?",
        "SELECT * FROM s.t JOIN s.u ON t.id = u.id WHERE c = ?",
        "SELECT * FROM (SELECT c FROM s.t) x WHERE c = ?",
        "SELECT * FROM s.t AS x(other, c) WHERE c = ?",
        "INSERT INTO s.t (c) SELECT ? FROM s.u",
        "SELECT * FROM s.t WHERE c = ?; DELETE FROM s.t",
    ] {
        assert!(bindings(sql, 1).is_none(), "{sql}");
    }
    for sql in [
        "INSERT INTO s.t (c) VALUES (CAST(? AS VARCHAR(2)))",
        "SELECT * FROM s.t WHERE c = SUBSTR(?, 1, 2)",
        "SELECT * FROM s.t WHERE other.c = ?",
    ] {
        assert_eq!(bindings(sql, 1).expect("analizzabile").1, vec![None]);
    }
}
