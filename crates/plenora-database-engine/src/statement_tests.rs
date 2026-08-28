use super::*;
use plenora_database_core::ErrorCategory;

#[test]
fn one_template_is_reused_with_isolated_values_and_a_stable_fingerprint() {
    let template = NativeStatement::new("SELECT $1::text", 1).expect("template");
    let clone = template.clone();
    let first = template
        .bind(vec![ParameterValue::String("private-first".to_owned())])
        .expect("first bind");
    let second = clone
        .bind(vec![ParameterValue::String("private-second".to_owned())])
        .expect("second bind");

    assert_eq!(
        first.template().fingerprint(),
        second.template().fingerprint()
    );
    assert_ne!(first.parameters(), second.parameters());
    assert_eq!(first.legacy().sql, "SELECT $1::text");
    assert_eq!(first.legacy().params, first.parameters());
    assert_eq!(template.sql(), "SELECT $1::text");
}

#[test]
fn debug_and_bind_errors_do_not_expose_sql_or_values() {
    let template =
        NativeStatement::new("SELECT private_column FROM private_table WHERE id = $1", 1)
            .expect("template");
    let bound = template
        .bind(vec![ParameterValue::String("private-value".to_owned())])
        .expect("bind");
    let rendered = format!("{template:?} {bound:?}");
    assert!(!rendered.contains("private_column"));
    assert!(!rendered.contains("private-value"));

    let error = template.bind(Vec::new()).expect_err("arity mismatch");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert!(!error.message.contains("private"));
}

#[test]
fn empty_or_nul_sql_is_rejected_without_echoing_the_input() {
    for sql in [" ", "SELECT private\0payload"] {
        let error = NativeStatement::new(sql, 0).expect_err("invalid SQL");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert!(!error.message.contains("private"));
    }
}
