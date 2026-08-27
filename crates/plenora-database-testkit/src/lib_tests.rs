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
