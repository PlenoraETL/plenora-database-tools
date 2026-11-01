use super::*;

fn row(columns: &[&str], values: Vec<ParameterValue>) -> Row {
    Row::try_new(
        columns
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
            .into(),
        values,
    )
    .expect("riga di test")
}

fn single(value: i64) -> QueryResult {
    QueryResult::from_rows(vec![row(&["value"], vec![ParameterValue::I64(value)])])
        .expect("result set")
}

#[test]
fn terminal_methods_enforce_cardinality_without_discarding_rows() {
    assert_eq!(single(7).scalar_one(), Ok(ParameterValue::I64(7)));
    assert_eq!(single(7).scalar(), Ok(Some(ParameterValue::I64(7))));
    assert_eq!(single(7).one().expect("one").len(), 1);
    assert!(QueryResult::from_rows(Vec::new())
        .expect("vuoto")
        .one_or_none()
        .expect("zero o uno")
        .is_none());

    let multiple = QueryResult::from_rows(vec![
        row(&["value"], vec![ParameterValue::I64(1)]),
        row(&["value"], vec![ParameterValue::I64(2)]),
    ])
    .expect("result set");
    assert_eq!(
        multiple
            .scalar()
            .expect_err("non scarta la seconda riga")
            .category,
        ErrorCategory::Conflict
    );
}

#[test]
fn columns_are_available_before_consumption_and_on_empty_results() {
    let result = single(7);
    assert_eq!(result.columns(), Some(["value".to_owned()].as_slice()));
    let descriptor = &result.column_descriptors().expect("descriptors")[0];
    assert_eq!(descriptor.index(), 0);
    assert_eq!(descriptor.name(), "value");
    assert_eq!(result.len(), 1);

    let columns: Arc<[String]> = vec!["id".to_owned(), "name".to_owned()].into();
    let empty =
        QueryResult::with_columns(Arc::clone(&columns), Vec::new()).expect("vuoto tipizzato");
    assert_eq!(empty.columns(), Some(columns.as_ref()));
    assert_eq!(empty.column_descriptors().expect("descriptors").len(), 2);
    assert!(empty.is_empty());
}

#[test]
fn inconsistent_row_metadata_is_rejected_without_column_names_in_the_error() {
    let error = QueryResult::from_rows(vec![
        row(&["private_a"], vec![ParameterValue::I64(1)]),
        row(&["private_b"], vec![ParameterValue::I64(2)]),
    ])
    .expect_err("metadata divergenti");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(!error.message.contains("private_a"));
    assert!(!error.message.contains("private_b"));
}

#[test]
fn mapping_conversion_is_explicit_and_preserves_column_value_pairs() {
    let mappings = QueryResult::from_rows(vec![row(
        &["id", "name"],
        vec![
            ParameterValue::I64(3),
            ParameterValue::String("Ada".to_owned()),
        ],
    )])
    .expect("result set")
    .into_mappings()
    .unwrap();
    assert_eq!(mappings[0].get("id"), Some(&ParameterValue::I64(3)));
    assert_eq!(
        mappings[0].get("name"),
        Some(&ParameterValue::String("Ada".to_owned()))
    );
}

#[test]
fn mappings_refuse_duplicate_names_without_exposing_payload() {
    let row = plenora_database_core::Row::try_new(
        std::sync::Arc::from(["duplicate".to_owned(), "duplicate".to_owned()]),
        vec![ParameterValue::I64(1), ParameterValue::I64(2)],
    )
    .unwrap();
    let result = QueryResult::from_rows(vec![row]).unwrap();
    let error = result.into_mappings().unwrap_err();
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::DataMapping
    );
    assert!(!error.message.contains("duplicate"));
}

#[test]
fn empty_mappings_also_refuse_duplicate_column_metadata() {
    let result = QueryResult::with_columns(
        Arc::from(["secret".to_owned(), "secret".to_owned()]),
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        result.into_mappings().unwrap_err().category,
        ErrorCategory::DataMapping
    );
}
