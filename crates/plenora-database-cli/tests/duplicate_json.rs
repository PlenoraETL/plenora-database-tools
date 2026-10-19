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
