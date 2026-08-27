use super::*;
use serde_json::json;

static FORMAT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn strip_format_extracts_value_and_returns_remaining_args() {
    let _guard = FORMAT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let out = strip_output_format(vec![
        "dsn-env".into(),
        "--format".into(),
        "markdown".into(),
        "extra".into(),
    ])
    .expect("parse");
    assert_eq!(out, vec!["dsn-env", "extra"]);
    assert_eq!(OutputFormat::active(), OutputFormat::Markdown);
    OutputFormat::Json.set_active();
}

#[test]
fn strip_format_rejects_unknown_value() {
    let err = strip_output_format(vec!["--format".into(), "yaml".into()]).expect_err("must fail");
    assert!(format!("{err:?}").contains("format"));
}

#[test]
fn markdown_object_renders_key_value_bullets() {
    let md = render_markdown("test-x", &json!({"a": 1, "b": "ciao"}));
    assert!(md.contains("# test-x"));
    assert!(md.contains("- **a**: 1"));
    assert!(md.contains("- **b**: ciao"));
}

#[test]
fn markdown_array_of_objects_renders_table() {
    let md = render_markdown(
        "list",
        &json!([{"name": "a", "n": 1}, {"name": "b", "n": 2}]),
    );
    // BTreeMap alfabetico: colonne "n" prima di "name".
    assert!(md.contains("| n | name |"));
    assert!(md.contains("|---|---|"));
    assert!(md.contains("| 1 | a |"));
    assert!(md.contains("| 2 | b |"));
}

#[test]
fn junit_wraps_status_ok_as_system_out() {
    let x = render_junit("test-x", &json!({"status": "ok"}));
    assert!(x.contains("<testsuite name=\"plenora-database-cli\""));
    assert!(x.contains("failures=\"0\""));
    assert!(x.contains("<system-out>"));
}

#[test]
fn junit_wraps_failure_status_as_failure_element() {
    let x = render_junit("test-x", &json!({"status": "unhealthy"}));
    assert!(x.contains("failures=\"1\""));
    assert!(x.contains("<failure message=\"unhealthy\""));
}
