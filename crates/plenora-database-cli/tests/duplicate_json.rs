//! I confini CLI rifiutano documenti ambigui prima della validazione.
use std::{fs, path::PathBuf, process::Command};

const BIN: &str = env!("CARGO_BIN_EXE_plenora-database");

struct Document(PathBuf);
impl Document {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "plenora-duplicates-{}-{name}.json",
            std::process::id()
        )))
    }
    fn run(&self, document: &str, arguments: &[&str], valid: bool) {
        fs::write(&self.0, document).unwrap();
        let output = Command::new(BIN)
            .args(arguments)
            .arg(&self.0)
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(output.status.success(), valid, "{stdout}");
        assert!(output.stderr.is_empty());
        if !valid {
            assert!(stdout.contains("chiave JSON ripetuta"), "{stdout}");
            assert!(!stdout.contains("SECRET"), "{stdout}");
        }
    }
}
impl Drop for Document {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn plan_capabilities_reject_duplicates_at_root_and_in_extension_map() {
    let document = Document::new("capabilities");
    let plan = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/v2/examples/plan-postgres-read.json");
    let args = ["validate-plan", plan.to_str().unwrap(), "--capabilities"];
    let clean = include_str!("../../../contracts/v2/examples/capabilities-postgres.json");
    document.run(clean, &args, true);
    document.run(
        &clean.replacen('{', r#"{"provider_version":"SECRET_VALUE","#, 1),
        &args,
        false,
    );
    document.run(
        &clean.replace(
            r#""postgis": "3.5.2""#,
            r#""SECRET_KEY":"SECRET_VALUE","SECRET_KEY":"2""#,
        ),
        &args,
        false,
    );
}

#[test]
fn portable_compile_rejects_duplicates_including_database_json_values() {
    let document = Document::new("portable");
    let args = ["portable-compile", "postgres"];
    let template = r#"{"type":"insert","table":{"name":"t"},"columns":["payload"],"values":[[{"kind":"literal","value":{"type":"json","value":PAYLOAD}}]]}"#;
    let clean = template.replace("PAYLOAD", r#"{"items":[{"a":1},{"a":2}]}"#);
    document.run(&clean, &args, true);
    document.run(
        &clean.replacen('{', r#"{"type":"SECRET_VALUE","#, 1),
        &args,
        false,
    );
    for payload in [
        r#"{"SECRET_KEY":"SECRET_VALUE","SECRET_KEY":2}"#,
        r#"{"items":[{"SECRET_KEY":"SECRET_VALUE","SECRET_KEY":2}]}"#,
    ] {
        document.run(&template.replace("PAYLOAD", payload), &args, false);
    }
}

#[test]
fn validate_plan_rejects_repeated_keys() {
    let file = Document::new("plan");
    let clean = include_str!("../../../contracts/v2/examples/plan-postgres-read.json");
    for (text, valid) in [
        (clean.to_owned(), true),
        (clean.replacen('{', r#"{"schema_version":2,"#, 1), false),
        (
            clean.replacen(r#""operation": {"#, r#""operation": {"id":"SECRET","#, 1),
            false,
        ),
    ] {
        fs::write(&file.0, text).unwrap();
        let output = Command::new(BIN)
            .arg("validate-plan")
            .arg(&file.0)
            .output()
            .unwrap();
        assert_eq!(output.status.success(), valid);
        assert!(output.stderr.is_empty());
        let envelope: serde_json::Value =
            plenora_database_core::strict_json::from_slice(&output.stdout).unwrap();
        if !valid {
            assert_eq!(output.status.code(), Some(2));
            assert_eq!(envelope["error"]["category"], "invalid_plan");
            assert_eq!(envelope["error"]["message"], "JSON piano non valido");
        }
    }
}

#[cfg(feature = "postgres")]
#[test]
fn typed_operation_files_reject_duplicates_before_io() {
    let query = r#"{"source":{"object":{"schema":"public","object":"t"}},"projection":[],"joins":[],"group_by":[],"order_by":[],"distinct":false}"#;
    let read = r#"{"source":{"schema":"public","object":"t"},"projection":[]}"#;
    let write = r#"{"target":{"schema":"public","object":"t"},"mode":"append","mapping_policy":"strict","transaction_profile":"single_transaction","keys":[],"update_columns":[],"create_spatial_index":false,"allow_partial":false}"#;
    for (command, clean, key, nested, slot) in [
        ("postgres-query", query, "source", "object", "QUERY.json"),
        ("bulk-write", write, "target", "object", "WRITE_OP.json"),
        ("query", query, "source", "object", "QUERY.json"),
        ("read", read, "source", "object", "READ.json"),
        ("write", write, "target", "object", "WRITE.json"),
    ] {
        let file = Document::new(command);
        for (text, valid) in [
            (clean.to_owned(), true),
            (
                clean.replacen('{', &format!(r#"{{"{key}":"SECRET","#), 1),
                false,
            ),
            (
                clean.replacen(
                    &format!(r#""{nested}":"#),
                    &format!(r#""{nested}":"SECRET","{nested}":"#),
                    1,
                ),
                false,
            ),
        ] {
            fs::write(&file.0, text).unwrap();
            let mut child = Command::new(BIN);
            child.arg(command);
            let request = Document::new(&format!("{command}-request"));
            if matches!(command, "query" | "read" | "write") {
                fs::write(
                    &request.0,
                    serde_json::json!({
                        "provider": "postgres", "secret_environment": "STRICT_JSON_TEST_DSN",
                        "operation_path": file.0,
                    })
                    .to_string(),
                )
                .unwrap();
                child.arg("--input").arg(&request.0);
                if command == "read" {
                    child
                        .arg("--output")
                        .arg(file.0.with_extension("missing.arrow"));
                } else if command == "write" {
                    child
                        .arg("--data")
                        .arg(file.0.with_extension("missing.arrow"));
                }
            } else {
                child.arg("STRICT_JSON_TEST_DSN").arg(&file.0);
                if command == "bulk-write" {
                    child.arg(file.0.with_extension("missing.arrow"));
                }
            }
            // Invalid DSN / absent IPC stops clean documents before any server I/O.
            let output = child
                .env("STRICT_JSON_TEST_DSN", "invalid")
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(output.stderr.is_empty());
            let envelope: serde_json::Value =
                plenora_database_core::strict_json::from_slice(&output.stdout).unwrap();
            let message = envelope["error"]["message"].as_str().unwrap();
            assert!(!message.contains("SECRET"));
            if valid {
                assert!(!message.contains("non parsabile"), "{command}: {message}");
            } else {
                assert_eq!(output.status.code(), Some(2));
                assert_eq!(envelope["error"]["category"], "invalid_plan");
                assert!(
                    message.starts_with(&format!("{slot} non parsabile a riga ")),
                    "{command}: {message}"
                );
            }
        }
    }
}
