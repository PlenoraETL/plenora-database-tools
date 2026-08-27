use super::*;

#[test]
fn valid_namespaced_keys_are_accepted() {
    for name in [
        "app.tenant",
        "plenora.actor_id",
        "audit.correlation_id",
        "sec.policy_v1",
        "x.y",
    ] {
        assert!(validate_context_key(name).is_ok(), "atteso ok: {name}");
    }
}

#[test]
fn invalid_keys_are_rejected() {
    for name in [
        "",
        "no_namespace",
        "app.",
        ".name",
        "app..name",
        "App.tenant",
        "app.Tenant",
        "app.name-with-dash",
        "1app.name",
        "app.1name",
        "app.name with space",
        &format!("app.{}", "x".repeat(70)),
    ] {
        assert!(
            validate_context_key(name).is_err(),
            "atteso rifiuto: {name}"
        );
    }
}

#[test]
fn values_with_control_chars_are_rejected() {
    let mut ctx = SessionContext::new();
    assert!(ctx
        .insert(
            "app.actor",
            SessionEntry::public(SessionValue::Text("evil\n".into())),
        )
        .is_err());
    assert!(ctx
        .insert(
            "app.actor",
            SessionEntry::public(SessionValue::Text("bad\0nul".into())),
        )
        .is_err());
}

#[test]
fn debug_of_sensitive_entry_is_redacted() {
    let mut ctx = SessionContext::new();
    ctx.insert(
        "app.token",
        SessionEntry::sensitive(SessionValue::Text("must-not-leak".into())),
    )
    .expect("insert");
    let s = format!("{ctx:?}");
    assert!(!s.contains("must-not-leak"), "atteso redacted: {s}");
    assert!(s.contains("REDACTED"), "atteso marker REDACTED: {s}");
}

#[test]
fn debug_of_public_entry_shows_value() {
    let mut ctx = SessionContext::new();
    ctx.insert(
        "app.tenant",
        SessionEntry::public(SessionValue::Text("acme".into())),
    )
    .expect("insert");
    let s = format!("{ctx:?}");
    assert!(s.contains("acme"), "public deve essere visibile: {s}");
}

#[test]
fn provider_string_encoding_covers_all_variants() {
    assert_eq!(SessionValue::Text("x".into()).as_provider_string(), "x");
    assert_eq!(SessionValue::Integer(42).as_provider_string(), "42");
    assert_eq!(SessionValue::Boolean(true).as_provider_string(), "true");
    assert_eq!(SessionValue::Boolean(false).as_provider_string(), "false");
}
