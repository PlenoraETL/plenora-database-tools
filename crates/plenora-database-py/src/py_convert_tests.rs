use super::parse_portable_json;

#[test]
fn portable_json_rejects_ambiguous_user_values_before_materialization() {
    let template = r#"{"type":"insert","table":{"name":"t"},"columns":["payload"],"values":[[{"kind":"literal","value":{"type":"json","value":PAYLOAD}}]]}"#;
    for payload in [
        r#"{"SECRET_KEY":"SECRET_VALUE","SECRET_KEY":2}"#,
        r#"{"items":[{"SECRET_KEY":"SECRET_VALUE","SECRET_KEY":2}]}"#,
    ] {
        let error = parse_portable_json(&template.replace("PAYLOAD", payload)).unwrap_err();
        let message = error.message;
        assert!(message.contains("chiave JSON ripetuta"), "{message}");
        assert!(!message.contains("SECRET"));
    }
    let clean = template.replace("PAYLOAD", r#"{"items":[{"a":1},{"a":2}]}"#);
    assert!(parse_portable_json(&clean).is_ok());
    let duplicate_root = clean.replacen('{', r#"{"type":"insert","#, 1);
    assert!(parse_portable_json(&duplicate_root)
        .unwrap_err()
        .message
        .contains("chiave JSON ripetuta"));
}
