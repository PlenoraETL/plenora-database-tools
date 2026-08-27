use super::*;

// I test manipolano lo store globale; serializzati per non falsare le
// assertion sotto cargo test parallel.
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn strip_extracts_ordered_entries() {
    let _g = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rest = strip_session_context(vec![
        "cmd".into(),
        "--session-context".into(),
        "app.tenant_id=t42:string".into(),
        "arg1".into(),
        "--session-context".into(),
        "app.user_id=99:int".into(),
    ])
    .unwrap();
    assert_eq!(rest, vec!["cmd", "arg1"]);
    let ctx = active();
    assert!(ctx.get("app.tenant_id").is_some());
    assert!(ctx.get("app.user_id").is_some());
    let _ = strip_session_context(vec![]);
}

#[test]
fn missing_argument_fails() {
    let err = strip_session_context(vec!["--session-context".into()]).unwrap_err();
    assert!(format!("{err:?}").contains("session-context"));
}
