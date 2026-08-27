use super::*;

#[test]
fn expect_single_row_rejects_empty() {
    let err = expect_single_row(vec![]).unwrap_err();
    assert_eq!(err.category, ErrorCategory::NotFound);
}

fn row_i32(name: &str, v: i32) -> Row {
    Row::try_new(
        std::sync::Arc::from(vec![name.to_owned()]),
        vec![ParameterValue::I32(v)],
    )
    .expect("fixture coerente")
}

#[test]
fn expect_single_row_rejects_multiple() {
    let err = expect_single_row(vec![row_i32("id", 1), row_i32("id", 2)]).unwrap_err();
    assert_eq!(err.category, ErrorCategory::Conflict);
}

#[test]
fn expect_at_most_one_row_returns_none_for_empty() {
    let out = expect_at_most_one_row(vec![]).expect("ok");
    assert!(out.is_none());
}

#[test]
fn expect_single_column_rejects_wrong_width() {
    let row = Row::try_new(
        std::sync::Arc::from(vec!["a".to_owned(), "b".to_owned()]),
        vec![ParameterValue::I32(1), ParameterValue::I32(2)],
    )
    .expect("fixture coerente");
    let err = expect_single_column(row).unwrap_err();
    assert_eq!(err.category, ErrorCategory::DataMapping);
}

#[test]
fn scalar_type_mismatch_reports_actual_type() {
    let err = scalar_type_mismatch("i64", &ParameterValue::String("nope".into()));
    assert!(err.message.contains("i64"));
    assert!(err.message.contains("string"));
}
