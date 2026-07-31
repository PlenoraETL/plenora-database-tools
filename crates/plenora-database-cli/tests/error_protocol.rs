use std::process::Command;

#[test]
fn cli_errors_are_json_on_stderr_with_nonzero_exit() {
    let output = Command::new(env!("CARGO_BIN_EXE_plenora-database"))
        .arg("unknown-command")
        .output()
        .expect("run plenora-database");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    let envelope: serde_json::Value =
        serde_json::from_str(stderr.trim()).expect("JSON error envelope");
    assert_eq!(envelope["status"], "error");
    assert_eq!(envelope["protocol_version"], 1);
    assert_eq!(envelope["error"]["category"], "InvalidPlan");
    assert_eq!(envelope["error"]["phase"], "Validate");
    assert_eq!(envelope["error"]["remote_effect"], "none");
    assert_eq!(envelope["error"]["retry"], "never");
    assert!(envelope["error"]["message"]
        .as_str()
        .is_some_and(|message| message.starts_with("uso:")));
}
