use std::process::Command;

#[test]
fn cli_errors_are_canonical_json_on_stdout_with_nonzero_exit() {
    let output = Command::new(env!("CARGO_BIN_EXE_plenora-database"))
        .arg("unknown-command")
        .output()
        .expect("run plenora-database");
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 stdout");
    let envelope: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("JSON error envelope");
    assert_eq!(envelope["status"], "error");
    assert_eq!(envelope["protocol_version"], 1);
    assert_eq!(envelope["error"]["category"], "invalid_plan");
    assert_eq!(envelope["error"]["phase"], "validate");
    assert_eq!(envelope["error"]["remote_effect"], "none");
    assert_eq!(envelope["error"]["retry"]["kind"], "never");
    assert!(envelope["error"]["message"]
        .as_str()
        .is_some_and(|message| message.starts_with("uso:")));
}
