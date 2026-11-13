use super::*;
use serde_json::json;

static FORMAT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn mysql_surface_vectors_preserve_reports_in_every_formatter() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/mysql_permissive/surfaces.json"
    ))
    .unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        let outcome: plenora_database_core::outcome::WriteOutcome =
            serde_json::from_value(case["outcome"].clone()).unwrap();
        outcome.validate().unwrap();
        let value = match outcome.settle() {
            Ok(outcome) => serde_json::to_value(outcome).unwrap(),
            Err(error) => {
                serde_json::from_str(&crate::CliError::from(error).to_json().unwrap()).unwrap()
            }
        };
        for rendered in [
            serde_json::to_string(&value).unwrap(),
            render_markdown("write", &value),
            render_junit("write", &value),
        ] {
            for key in ["risk", "evaluation", "effective", "mysql."] {
                assert!(rendered.contains(key), "{case}");
            }
            assert!(!rendered.contains("P6_SECRET_SENTINEL"));
        }
    }
}

#[test]
fn loss_report_survives_all_output_renderers() {
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../contracts/v2/examples/outcome-v3-loss.json"
    ))
    .unwrap();
    value["loss_report"] = serde_json::from_str(include_str!(
        "../../../contracts/v2/examples/execution-loss-v3.json"
    ))
    .unwrap();
    for rendered in [
        serde_json::to_string(&value).unwrap(),
        render_markdown("write", &value),
        render_junit("write", &value),
    ] {
        assert!(rendered.contains("loss_report"));
        assert!(rendered.contains("data_loss"));
        for key in [
            "risk",
            "evaluation",
            "effective",
            "would_change",
            "committed",
            "uncertain",
        ] {
            assert!(rendered.contains(key));
        }
    }
}

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

#[test]
fn prepared_error_report_survives_all_renderers() {
    let mut error = plenora_database_core::DatabaseError::invalid_plan("esecuzione interrotta");
    let mut report: plenora_database_core::execution_loss::ExecutionLossReport =
        serde_json::from_str(include_str!(
            "../../../contracts/v2/examples/execution-loss-v3.json"
        ))
        .unwrap();
    report.effective.clear();
    error.loss_report = Some(Box::new(report.into()));
    error.remote_effect = plenora_database_core::RemoteEffect::RolledBack;
    let value = serde_json::to_value(error.public_projection()).unwrap();
    for rendered in [
        serde_json::to_string(&value).unwrap(),
        render_markdown("write", &value),
        render_junit("write", &value),
    ] {
        assert!(rendered.contains("loss_report"));
        for key in [
            "risk",
            "evaluation",
            "effective",
            "rolled_back",
            "retry",
            "phase",
        ] {
            assert!(rendered.contains(key));
        }
    }
}

#[test]
fn markdown_distinguishes_empty_effective_from_absent_measurement() {
    let value = json!({"loss_report": {"risk": [], "evaluation": {"complete": false, "entries": []}, "effective": []}});
    let rendered = render_markdown("write", &value);
    assert!(rendered.contains("**effective**: []"));
    assert!(rendered.contains("**entries**: []"));
    assert!(rendered.contains("**complete**: false"));
}
