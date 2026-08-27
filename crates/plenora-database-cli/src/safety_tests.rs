use super::*;
use std::sync::Mutex;

// I test manipolano lo store globale; serializziamoli per non falsare le
// assertion quando cargo test li esegue in parallelo.
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn strip_extracts_allow_write_tests_flag() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rest = strip_safety_flags(vec![
        "dsn".into(),
        "--allow-write-tests".into(),
        "iterations".into(),
    ])
    .expect("parse");
    assert_eq!(rest, vec!["dsn", "iterations"]);
    assert!(active().allow_write_tests);
    let _ = strip_safety_flags(vec![]);
}

#[test]
fn strip_extracts_ephemeral_schema_name() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rest = strip_safety_flags(vec![
        "--ephemeral-schema".into(),
        "probe_schema".into(),
        "dsn".into(),
    ])
    .expect("parse");
    assert_eq!(rest, vec!["dsn"]);
    assert_eq!(active().ephemeral_schema.as_deref(), Some("probe_schema"));
    let _ = strip_safety_flags(vec![]);
}

#[test]
fn ephemeral_schema_requires_valid_identifier() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(strip_safety_flags(vec!["--ephemeral-schema".into(), "1invalid".into(),]).is_err());
    assert!(strip_safety_flags(vec!["--ephemeral-schema".into(), "has space".into(),]).is_err());
    let _ = strip_safety_flags(vec![]);
}

#[test]
fn require_write_tests_gates_correctly() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ = strip_safety_flags(vec![]);
    assert!(require_write_tests("cmd").is_err());
    let _ = strip_safety_flags(vec!["--allow-write-tests".into()]);
    assert!(require_write_tests("cmd").is_ok());
    let _ = strip_safety_flags(vec![]);
}
