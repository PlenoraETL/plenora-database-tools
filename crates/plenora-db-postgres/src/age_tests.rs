use super::*;
use plenora_database_core::graph::GraphValue;
use serde_json::json;

#[test]
fn sql_binds_parameters_and_never_interpolates_values() {
    let statement = GraphStatement::new(
        "people",
        "MATCH (p) WHERE p.name = $name RETURN p",
        vec!["person".to_owned()],
    )
    .with_params(BTreeMap::from([("name".to_owned(), json!("secret"))]));
    let sql = build_cypher_sql(&statement).expect("valid statement");
    assert!(sql.contains(", $1)"));
    assert!(!sql.contains("secret"));
    assert!(sql.contains("ag_catalog.agtype_out"));
    assert!(sql.ends_with("LIMIT 10001"));
}

#[test]
fn sql_selects_a_delimiter_absent_from_cypher() {
    let statement =
        GraphStatement::new("people", "RETURN '$plenora_age$'", vec!["value".to_owned()]);
    let sql = build_cypher_sql(&statement).expect("valid statement");
    assert!(sql.contains("$plenora_age_1$RETURN '$plenora_age$'$plenora_age_1$"));
}

#[test]
fn parser_preserves_vertex_edge_path_and_nested_values() {
    let raw = r#"[{"id": 1, "label": "Person", "properties": {"name": "Alice", "ok": true}}::vertex, {"id": 2, "label": "KNOWS", "end_id": 3, "start_id": 1, "properties": {"weight": 1.5}}::edge]::path"#;
    let GraphValue::Path(items) = parse_agtype(raw).expect("valid path") else {
        panic!("expected path");
    };
    assert!(matches!(items[0], GraphValue::Vertex(_)));
    assert!(matches!(items[1], GraphValue::Edge(_)));
}

#[test]
fn parser_rejects_unknown_annotations_without_echoing_payload() {
    let error = parse_agtype(r#"{"token":"very-secret"}::unknown"#)
        .expect_err("unknown annotation must fail");
    assert!(!error.message.contains("very-secret"));
}

#[test]
fn age_parameter_debug_is_redacted() {
    let parameter = AgeParameter::new("{\"token\":\"very-secret\"}".to_owned());
    let debug = format!("{parameter:?}");
    assert!(!debug.contains("very-secret"));
    assert!(debug.contains("REDACTED"));
}

#[cfg(test)]
mod live {
    use super::*;
    use crate::PostgresProvider;
    use plenora_database_core::graph::{GraphStatement, GraphValue};
    use plenora_database_core::provider::{Provider, SecretString};
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use plenora_database_core::transaction::{Statement, TransactionOptions};
    use plenora_database_core::CancellationToken;
    use std::collections::BTreeMap;
    use tokio_postgres::NoTls;

    fn configured_dsn() -> Option<String> {
        let configured = std::env::var("PLENORA_TEST_AGE_DSN").ok();
        assert!(
            configured.is_some() || std::env::var_os("PLENORA_REQUIRE_LIVE_AGE").is_none(),
            "PLENORA_TEST_AGE_DSN obbligatoria per il gate AGE"
        );
        configured
    }

    async fn raw_parameter_probe(dsn: &str) {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .expect("connect AGE parameter probe");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.batch_execute("LOAD 'age'").await.expect("load AGE");
        let statement =
            GraphStatement::new("plenora_age_gate", "RETURN $name", vec!["name".to_owned()])
                .with_params(BTreeMap::from([("name".to_owned(), json!("Alice"))]));
        let sql = build_cypher_sql(&statement).expect("build parameter probe");
        let prepared = client.prepare(&sql).await.expect("prepare parameter probe");
        assert_eq!(prepared.params().len(), 1);
        assert_eq!(prepared.params()[0].name(), "agtype");
        let parameter = AgeParameter::new(
            serde_json::to_string(&statement.params).expect("encode parameter probe"),
        );
        client
            .query(&prepared, &[&parameter])
            .await
            .expect("bind parameter probe");
    }

    fn budget() -> ResourceBudget {
        ResourceBudget::new(ResourceLimits::default()).expect("budget")
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // matrice live unica: il gate esige un solo test --exact
    async fn live_age_1_7_pg18_parameters_types_and_transactions() {
        let Some(dsn) = configured_dsn() else {
            return;
        };
        let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
        let secret = SecretString::new(dsn);
        let cancel = CancellationToken::new();
        let capabilities = provider
            .age_capabilities(&secret, &cancel)
            .await
            .expect("AGE capabilities");
        assert!(capabilities.qualified());
        assert_eq!(capabilities.extension_version.as_deref(), Some("1.7.0"));
        assert_eq!(capabilities.postgres_major, Some(18));
        let admin = provider
            .age_admin_capabilities(&secret, &cancel)
            .await
            .expect("AGE admin capabilities");
        assert!(admin.qualified());
        for graph in ["plenora_age_gate", "plenora_age_admin_gate"] {
            if provider
                .list_graphs(&secret, &cancel)
                .await
                .expect("list before cleanup")
                .iter()
                .any(|candidate| candidate == graph)
            {
                provider
                    .drop_graph(&secret, graph, true, &cancel)
                    .await
                    .expect("drop stale graph");
            }
        }
        provider
            .create_graph(&secret, "plenora_age_admin_gate", &cancel)
            .await
            .expect("create admin graph");
        assert!(provider
            .list_graphs(&secret, &cancel)
            .await
            .expect("list admin graph")
            .iter()
            .any(|graph| graph == "plenora_age_admin_gate"));
        provider
            .drop_graph(&secret, "plenora_age_admin_gate", true, &cancel)
            .await
            .expect("drop admin graph");
        provider
            .create_graph(&secret, "plenora_age_gate", &cancel)
            .await
            .expect("create test graph");
        raw_parameter_probe(secret.expose()).await;

        let mut tx = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin AGE");
        let search_path_before = tx
            .query(
                &Statement::new("SELECT current_setting('search_path')"),
                &cancel,
            )
            .await
            .expect("search path before AGE");
        let create = GraphStatement::new(
            "plenora_age_gate",
            "CREATE (a:Person {name: $name, active: true}), (b:Person {name: 'Bob'}), (a)-[:KNOWS {since: 2024}]->(b) RETURN a",
            vec!["person".to_owned()],
        )
        .with_params(BTreeMap::from([("name".to_owned(), json!("Alice"))]));
        let rows = tx
            .execute_graph(&create, &cancel)
            .await
            .expect("parameterized create");
        assert!(matches!(
            rows[0].values.get("person"),
            Some(GraphValue::Vertex(_))
        ));

        let typed = GraphStatement::new(
            "plenora_age_gate",
            "MATCH p=(a:Person)-[e:KNOWS]->(b:Person) RETURN e, p, [a.name, a.active], {label: a.name}, null",
            vec![
                "edge".to_owned(),
                "path".to_owned(),
                "items".to_owned(),
                "meta".to_owned(),
                "missing".to_owned(),
            ],
        );
        let rows = tx.execute_graph(&typed, &cancel).await.expect("typed read");
        assert!(matches!(
            rows[0].values.get("edge"),
            Some(GraphValue::Edge(_))
        ));
        assert!(matches!(
            rows[0].values.get("path"),
            Some(GraphValue::Path(_))
        ));
        assert!(matches!(
            rows[0].values.get("items"),
            Some(GraphValue::List(_))
        ));
        assert!(matches!(
            rows[0].values.get("meta"),
            Some(GraphValue::Map(_))
        ));
        assert_eq!(rows[0].values.get("missing"), Some(&GraphValue::Null));

        let merge = GraphStatement::new(
            "plenora_age_gate",
            "MERGE (c:Person {name: 'Carol'}) SET c.rank = 3 RETURN c.rank",
            vec!["rank".to_owned()],
        );
        let rows = tx
            .execute_graph(&merge, &cancel)
            .await
            .expect("MERGE and SET");
        assert_eq!(rows[0].values.get("rank"), Some(&GraphValue::Integer(3)));
        let remove = GraphStatement::new(
            "plenora_age_gate",
            "MATCH (c:Person {name: 'Carol'}) REMOVE c.rank RETURN c.rank",
            vec!["rank".to_owned()],
        );
        let rows = tx.execute_graph(&remove, &cancel).await.expect("REMOVE");
        assert_eq!(rows[0].values.get("rank"), Some(&GraphValue::Null));

        let unwind = GraphStatement::new(
            "plenora_age_gate",
            "UNWIND $values AS value WITH value ORDER BY value SKIP 1 LIMIT 2 RETURN value",
            vec!["value".to_owned()],
        )
        .with_params(BTreeMap::from([("values".to_owned(), json!([4, 1, 3, 2]))]));
        let rows = tx
            .execute_graph(&unwind, &cancel)
            .await
            .expect("UNWIND WITH");
        assert_eq!(
            rows.iter()
                .map(|row| row.values.get("value"))
                .collect::<Vec<_>>(),
            vec![Some(&GraphValue::Integer(2)), Some(&GraphValue::Integer(3))]
        );

        let variable_path = GraphStatement::new(
            "plenora_age_gate",
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN length(p), nodes(p), relationships(p)",
            vec!["length".to_owned(), "nodes".to_owned(), "edges".to_owned()],
        );
        let rows = tx
            .execute_graph(&variable_path, &cancel)
            .await
            .expect("variable path and functions");
        assert_eq!(rows[0].values.get("length"), Some(&GraphValue::Integer(1)));
        assert!(matches!(
            rows[0].values.get("nodes"),
            Some(GraphValue::List(_))
        ));
        assert!(matches!(
            rows[0].values.get("edges"),
            Some(GraphValue::List(_))
        ));

        let terminal = GraphStatement::new(
            "plenora_age_gate",
            "CREATE (:Disposable {name: 'terminal'})",
            vec!["unused".to_owned()],
        );
        assert!(tx
            .execute_graph(&terminal, &cancel)
            .await
            .expect("terminal CREATE")
            .is_empty());
        let delete = GraphStatement::new(
            "plenora_age_gate",
            "MATCH (d:Disposable {name: 'terminal'}) DELETE d RETURN d",
            vec!["deleted".to_owned()],
        );
        assert!(matches!(
            tx.execute_graph(&delete, &cancel)
                .await
                .expect("DELETE with RETURN")[0]
                .values
                .get("deleted"),
            Some(GraphValue::Vertex(_))
        ));

        let bounded = GraphStatement::new(
            "plenora_age_gate",
            "UNWIND [1, 2, 3] AS value RETURN value",
            vec!["value".to_owned()],
        )
        .with_max_rows(2);
        let error = tx
            .execute_graph(&bounded, &cancel)
            .await
            .expect_err("row limit must fail closed");
        assert_eq!(error.category, ErrorCategory::ResourceLimit);
        let search_path_after = tx
            .query(
                &Statement::new("SELECT current_setting('search_path')"),
                &cancel,
            )
            .await
            .expect("search path after AGE");
        assert_eq!(
            search_path_after[0].values(),
            search_path_before[0].values()
        );

        tx.savepoint("age_graph_savepoint", &cancel)
            .await
            .expect("AGE savepoint");
        tx.execute_graph(
            &GraphStatement::new(
                "plenora_age_gate",
                "CREATE (:Person {name: 'SavepointRollback'}) RETURN 1",
                vec!["one".to_owned()],
            ),
            &cancel,
        )
        .await
        .expect("AGE write after savepoint");
        tx.rollback_to_savepoint("age_graph_savepoint", &cancel)
            .await
            .expect("AGE rollback to savepoint");
        tx.release_savepoint("age_graph_savepoint", &cancel)
            .await
            .expect("AGE release savepoint");
        tx.commit(&cancel).await.expect("commit AGE");

        let mut rollback_tx = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin rollback AGE");
        rollback_tx
            .execute_graph(
                &GraphStatement::new(
                    "plenora_age_gate",
                    "CREATE (:Person {name: 'Rollback'}) RETURN 1",
                    vec!["one".to_owned()],
                ),
                &cancel,
            )
            .await
            .expect("create before rollback");
        rollback_tx.rollback(&cancel).await.expect("rollback AGE");

        let mut verify_tx = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin verify AGE");
        let rows = verify_tx
            .execute_graph(
                &GraphStatement::new(
                    "plenora_age_gate",
                    "MATCH (p:Person {name: 'Rollback'}) RETURN count(p)",
                    vec!["total".to_owned()],
                ),
                &cancel,
            )
            .await
            .expect("verify rollback");
        assert_eq!(rows[0].values.get("total"), Some(&GraphValue::Integer(0)));
        let savepoint_rows = verify_tx
            .execute_graph(
                &GraphStatement::new(
                    "plenora_age_gate",
                    "MATCH (p:Person {name: 'SavepointRollback'}) RETURN count(p)",
                    vec!["total".to_owned()],
                ),
                &cancel,
            )
            .await
            .expect("verify savepoint rollback");
        assert_eq!(
            savepoint_rows[0].values.get("total"),
            Some(&GraphValue::Integer(0))
        );
        verify_tx.rollback(&cancel).await.expect("close verify AGE");

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let mut cancelled_tx = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin cancellation probe");
        let error = cancelled_tx
            .execute_graph(
                &GraphStatement::new("plenora_age_gate", "RETURN 1", vec!["one".to_owned()]),
                &cancelled,
            )
            .await
            .expect_err("pre-cancelled graph query must fail");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        cancelled_tx
            .rollback(&cancel)
            .await
            .expect("rollback cancellation probe");

        let timeout_options = TransactionOptions {
            statement_timeout_ms: Some(10),
            ..TransactionOptions::default()
        };
        let mut timeout_tx = provider
            .begin_transaction(&secret, &timeout_options, &budget(), &cancel)
            .await
            .expect("begin timeout probe");
        let error = timeout_tx
            .execute_graph(
                &GraphStatement::new(
                    "plenora_age_gate",
                    "UNWIND range(1, 100000) AS first \
                     UNWIND range(1, 100000) AS second RETURN count(first)",
                    vec!["total".to_owned()],
                ),
                &cancel,
            )
            .await
            .expect_err("statement timeout must interrupt graph query");
        // PostgreSQL usa SQLSTATE 57014 sia per statement_timeout sia per un
        // CancelRequest esplicito; il contratto pubblico esistente lo espone
        // quindi come Cancelled, senza indovinare una causa dal messaggio.
        assert_eq!(error.category, ErrorCategory::Cancelled);
        timeout_tx
            .rollback(&cancel)
            .await
            .expect("rollback timeout probe");

        let mut first = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin first concurrent session");
        let mut second = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin second concurrent session");
        let count = GraphStatement::new(
            "plenora_age_gate",
            "MATCH (p:Person) RETURN count(p)",
            vec!["total".to_owned()],
        );
        let (first_rows, second_rows) = tokio::join!(
            first.execute_graph(&count, &cancel),
            second.execute_graph(&count, &cancel)
        );
        assert_eq!(first_rows.expect("first concurrent result").len(), 1);
        assert_eq!(second_rows.expect("second concurrent result").len(), 1);
        first
            .rollback(&cancel)
            .await
            .expect("rollback first concurrent");
        second
            .rollback(&cancel)
            .await
            .expect("rollback second concurrent");

        provider
            .drop_graph(&secret, "plenora_age_gate", true, &cancel)
            .await
            .expect("final graph cleanup");
    }
}
