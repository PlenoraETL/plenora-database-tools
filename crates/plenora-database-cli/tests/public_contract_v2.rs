use serde_json::Value;
use std::process::{Command, Output};

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_plenora-database"))
        .args(arguments)
        .output()
        .expect("esecuzione CLI")
}

fn json(output: &Output) -> Value {
    assert!(output.stderr.is_empty(), "stderr non vuoto");
    let stdout = String::from_utf8(output.stdout.clone()).expect("UTF-8");
    assert_eq!(stdout.matches('\n').count(), 1, "serve un solo documento");
    serde_json::from_str(stdout.trim()).expect("JSON")
}

#[test]
fn version_uses_the_v2_success_envelope() {
    let output = run(&["--version", "--format", "json"]);
    assert!(output.status.success());
    let document = json(&output);
    assert_eq!(document["status"], "ok");
    assert_eq!(document["protocol_version"], 2);
    assert_eq!(document["component"], "plenora-database-tools");
    assert_eq!(document["component_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(document["command"], "version");
    assert_eq!(document["result"]["protocol_version"], 2);
}

#[test]
fn capabilities_describe_the_running_cli_artifact() {
    let output = run(&["capabilities", "--format", "json"]);
    assert!(output.status.success());
    let document = json(&output);
    let result = &document["result"];
    assert_eq!(document["contract"], "plenora-capabilities-v2");
    assert_eq!(result["schema_version"], 2);
    assert_eq!(result["component_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(result["interfaces"][0]["kind"], "cli");
    assert_eq!(result["interfaces"][0]["contract"], "plenora-cli-v2");
    let operations = result["operations"].as_array().expect("operations");
    for required in [
        "database.test_connection",
        "database.list_catalogs",
        "database.list_schemas",
        "database.list_objects",
        "database.describe_object",
        "database.read",
        "database.write",
    ] {
        assert!(operations
            .iter()
            .any(|operation| operation["id"] == required));
    }
}

#[test]
fn invalid_invocation_uses_typed_exit_code_and_v2_error() {
    let output = run(&["read", "--format", "json"]);
    assert_eq!(output.status.code(), Some(2));
    let document = json(&output);
    assert_eq!(document["status"], "error");
    assert_eq!(document["protocol_version"], 2);
    assert_eq!(document["error"]["category"], "invalid_plan");
    assert_eq!(document["error"]["remote_effect"], "none");
}
