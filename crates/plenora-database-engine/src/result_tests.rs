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
    .try_into_mappings()
    .expect("nomi univoci");
    assert_eq!(mappings[0].get("id"), Some(&ParameterValue::I64(3)));
    assert_eq!(
        mappings[0].get("name"),
        Some(&ParameterValue::String("Ada".to_owned()))
    );
}

#[test]
fn mappings_reject_duplicate_metadata_even_without_rows_without_payload() {
    let columns: Arc<[String]> = vec!["private_column".to_owned(); 2].into();
    for rows in [
        Vec::new(),
        vec![row(
            &["private_column", "private_column"],
            vec![ParameterValue::String("private_value".to_owned()); 2],
        )],
    ] {
        let error = QueryResult::with_columns(Arc::clone(&columns), rows)
            .expect("metadata coerenti")
            .try_into_mappings()
            .expect_err("nomi duplicati");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.phase, ErrorPhase::Read);
        assert_eq!(
            error.message,
            "nomi di colonna duplicati: usare alias distinti o accesso per posizione"
        );
        assert!(error.diagnostics.is_none());
    }
}

#[test]
#[allow(
    deprecated,
    reason = "verifica la compatibilita del metodo legacy in 8.x"
)]
fn mappings_preserve_legacy_results_for_unique_names_and_empty_shapes() {
    for rows in [
        Vec::new(),
        vec![row(&[], Vec::new())],
        vec![
            row(
                &["id", "ID"],
                vec![ParameterValue::I64(1), ParameterValue::I64(2)],
            ),
            row(
                &["id", "ID"],
                vec![ParameterValue::I64(3), ParameterValue::I64(4)],
            ),
        ],
    ] {
        let legacy = QueryResult::from_rows(rows.clone())
            .expect("result set")
            .into_mappings();
        let checked = QueryResult::from_rows(rows)
            .expect("result set")
            .try_into_mappings()
            .expect("nomi univoci");
        assert_eq!(checked, legacy);
    }
    let empty = QueryResult::with_columns(vec!["id".to_owned()].into(), Vec::new())
        .expect("vuoto tipizzato")
        .try_into_mappings()
        .expect("nome univoco");
    assert!(empty.is_empty());
}

#[test]
#[allow(deprecated, reason = "documenta la perdita del metodo legacy in 8.x")]
fn legacy_mappings_keep_the_last_duplicate_value() {
    let mappings = QueryResult::from_rows(vec![row(
        &["id", "id"],
        vec![ParameterValue::I64(1), ParameterValue::I64(2)],
    )])
    .expect("result set")
    .into_mappings();
    assert_eq!(
        mappings,
        vec![BTreeMap::from([("id".to_owned(), ParameterValue::I64(2))])]
    );
}
