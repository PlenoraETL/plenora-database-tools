use super::*;

#[test]
fn embedded_golden_suite_is_valid() {
    let suite = golden_suite().expect("golden");
    assert_eq!(suite.cases.len(), 17);
}

#[test]
fn secret_markers_are_case_insensitive() {
    assert!(contains_secret_marker(
        "Authorization: Bearer TOP-SECRET",
        &["top-secret"]
    ));
    assert_redacted("authentication failed", &["top-secret"]).expect("redacted");
}

#[test]
fn golden_reader_rejects_repeated_keys_in_inputs_and_expectations() {
    let clean = serde_json::to_string(&golden_suite().unwrap()).unwrap();
    assert_eq!(
        parse_golden_suite(clean.as_bytes()).unwrap(),
        golden_suite().unwrap()
    );
    for text in [
        clean.replacen('{', r#"{"schema_version":2,"#, 1),
        clean.replacen(r#""input":{"#, r#""input":{"SECRET":1,"SECRET":2,"#, 1),
        clean.replacen(
            r#""expected":{"#,
            r#""expected":{"SECRET":1,"SECRET":2,"#,
            1,
        ),
    ] {
        assert_ne!(text, clean);
        let error = parse_golden_suite(text.as_bytes()).unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
        assert_eq!(error.message, "golden non valido");
    }
}
