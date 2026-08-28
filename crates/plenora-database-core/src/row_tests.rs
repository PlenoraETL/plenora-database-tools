use super::*;

fn sample_row() -> Row {
    Row::try_new(
        Arc::from(vec!["id".to_owned(), "name".to_owned()]),
        vec![
            ParameterValue::I32(42),
            ParameterValue::String("plenora".to_owned()),
        ],
    )
    .expect("fixture coerente")
}

#[test]
fn row_len_matches_values() {
    assert_eq!(sample_row().len(), 2);
    assert!(!sample_row().is_empty());
}

#[test]
fn get_by_name_returns_the_value() {
    let row = sample_row();
    assert!(matches!(row.get("id"), Some(ParameterValue::I32(42))));
    assert!(matches!(
        row.get("name"),
        Some(ParameterValue::String(s)) if s == "plenora"
    ));
    assert!(row.get("missing").is_none());
}

#[test]
fn get_by_index_returns_the_value() {
    let row = sample_row();
    assert!(matches!(row.get_index(0), Some(ParameterValue::I32(42))));
    assert!(matches!(row.get_index(1), Some(ParameterValue::String(_))));
    assert!(row.get_index(2).is_none());
}

#[test]
fn descriptor_access_rejects_a_descriptor_from_a_different_schema() {
    let row = sample_row();
    let name = ColumnDescriptor::new(1, "name".to_owned());
    assert!(matches!(
        row.get_descriptor(&name),
        Some(ParameterValue::String(value)) if value == "plenora"
    ));
    assert!(row
        .get_descriptor(&ColumnDescriptor::new(1, "other".to_owned()))
        .is_none());
    assert!(row
        .get_descriptor(&ColumnDescriptor::new(8, "name".to_owned()))
        .is_none());
}

#[test]
fn index_by_name_shortcut_works() {
    let row = sample_row();
    assert!(matches!(&row["id"], ParameterValue::I32(42)));
}

#[test]
fn index_by_position_shortcut_works() {
    let row = sample_row();
    assert!(matches!(&row[0], ParameterValue::I32(42)));
}

/// Il panico dice che la colonna manca, non quali ci sono.
///
/// Il messaggio elencava `self.columns`: un panico raggiungibile da un
/// nome sbagliato pubblicava l'intero schema della riga nei log.
#[test]
#[should_panic(expected = "colonna non presente in una Row di 2 colonne")]
fn index_by_name_panics_when_missing() {
    let row = sample_row();
    let _ = &row["absent"];
}

#[test]
fn index_by_position_panics_out_of_range() {
    let row = sample_row();
    let panicked = std::panic::catch_unwind(move || {
        let _ = &row[7];
    });
    assert!(panicked.is_err());
}

/// La parita fra nomi e valori non e piu un `debug_assert`: vale anche in
/// release, che e dove i driver girano.
#[test]
fn a_row_whose_names_and_values_disagree_is_rejected() {
    let error = Row::try_new(
        Arc::from(vec!["id".to_owned(), "name".to_owned()]),
        vec![ParameterValue::I32(42)],
    )
    .expect_err("2 nomi e 1 valore");
    assert_eq!(error.category, crate::ErrorCategory::DataMapping);
    // Conteggi si, nomi no.
    assert!(error.message.contains('2'), "{}", error.message);
    assert!(!error.message.contains("name"), "{}", error.message);
}

#[test]
fn into_values_consumes_and_returns_values() {
    let row = sample_row();
    let values = row.into_values();
    assert_eq!(values.len(), 2);
}

#[test]
fn shared_columns_avoid_per_row_allocations() {
    let columns: Arc<[String]> = Arc::from(vec!["a".to_owned(), "b".to_owned()]);
    let row1 = Row::try_new(
        Arc::clone(&columns),
        vec![ParameterValue::I32(1), ParameterValue::I32(2)],
    )
    .expect("fixture coerente");
    let row2 = Row::try_new(
        Arc::clone(&columns),
        vec![ParameterValue::I32(3), ParameterValue::I32(4)],
    )
    .expect("fixture coerente");
    assert!(Arc::ptr_eq(&row1.columns, &row2.columns));
}

#[test]
fn columns_and_values_are_accessible_for_manual_serialization() {
    let row = sample_row();
    let json = serde_json::json!({
        "columns": row.columns(),
        "values": row.values(),
    });
    let text = serde_json::to_string(&json).expect("serialize");
    assert!(text.contains("id"));
    assert!(text.contains("plenora"));
}
