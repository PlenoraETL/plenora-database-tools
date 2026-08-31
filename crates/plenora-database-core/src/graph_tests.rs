use super::*;
use serde_json::json;

#[test]
fn capabilities_are_fail_closed_outside_exact_qualified_pair() {
    assert!(!AgeCapabilities::default().qualified());
    assert!(!AgeCapabilities::from_probe(17, Some("1.7.0".to_owned()), true).qualified());
    assert!(!AgeCapabilities::from_probe(18, Some("1.6.0".to_owned()), true).qualified());
    assert!(!AgeCapabilities::from_probe(18, Some("1.7.0".to_owned()), false).qualified());
    assert!(AgeCapabilities::from_probe(18, Some("1.7.0".to_owned()), true).qualified());
    assert!(!AgeAdminCapabilities::default().qualified());
    assert!(AgeAdminCapabilities::from_age(&AgeCapabilities::from_probe(
        18,
        Some("1.7.0".to_owned()),
        true,
    ))
    .qualified());
}

#[test]
fn graph_statement_bounds_materialized_rows() {
    assert_eq!(
        GraphStatement::new("people", "RETURN 1", vec!["one".to_owned()]).max_rows,
        DEFAULT_GRAPH_MAX_ROWS
    );
    assert!(
        GraphStatement::new("people", "RETURN 1", vec!["one".to_owned()])
            .with_max_rows(0)
            .validate()
            .is_err()
    );
    assert!(
        GraphStatement::new("people", "RETURN 1", vec!["one".to_owned()])
            .with_max_rows(MAX_GRAPH_ROWS + 1)
            .validate()
            .is_err()
    );
}

#[test]
fn graph_statement_accepts_bound_parameters() {
    let statement = GraphStatement::new("people", "RETURN $name", vec!["name".to_owned()])
        .with_params(BTreeMap::from([("name".to_owned(), json!("Alice"))]));
    assert!(statement.validate().is_ok());
}

#[test]
fn graph_statement_rejects_interpolated_identifiers_and_duplicates() {
    let bad_graph = GraphStatement::new("people'); DROP", "RETURN 1", vec!["one".to_owned()]);
    assert!(bad_graph.validate().is_err());
    let duplicate = GraphStatement::new(
        "people",
        "RETURN 1, 2",
        vec!["value".to_owned(), "value".to_owned()],
    );
    assert!(duplicate.validate().is_err());
}
