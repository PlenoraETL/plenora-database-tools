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
    use plenora_database_core::transaction::TransactionOptions;
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

    async fn setup_graph(dsn: &str) {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .expect("connect AGE setup");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("LOAD 'age'; SET search_path = ag_catalog, public")
            .await
            .expect("load AGE");
        client
            .batch_execute(
                "SELECT drop_graph(name, true) FROM ag_graph WHERE name = 'plenora_age_gate';
                 SELECT create_graph('plenora_age_gate');",
            )
            .await
            .expect("create graph");
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
    async fn live_age_1_7_pg18_parameters_types_and_transactions() {
        let Some(dsn) = configured_dsn() else {
            return;
        };
        setup_graph(&dsn).await;
        raw_parameter_probe(&dsn).await;
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

        let mut tx = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin AGE");
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
        verify_tx.rollback(&cancel).await.expect("close verify AGE");
    }
}
