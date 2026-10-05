//! Contratti del CLI verificabili senza un database.

#![cfg(feature = "postgres")]

use serde_json::Value;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_plenora-database");

fn run_expect_error(args: &[&str]) -> Value {
    let output = Command::new(BIN)
        .args(args)
        .env_remove("PG_DSN")
        .output()
        .expect("spawn CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !output.status.success(),
        "CLI doveva fallire ma ha avuto successo: args={args:?}\nstdout={stdout}"
    );
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("output errore non JSON: {e}\nstdout={stdout}");
    })
}

#[test]
fn benchmark_write_requires_explicit_gate() {
    // Senza --allow-write-tests → invalid_plan
    let err = run_expect_error(&["benchmark-write", "PG_DSN", "5", "2"]);
    assert_eq!(err["status"], "error");
    assert_eq!(err["error"]["category"], "invalid_plan");
    assert!(err["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("--allow-write-tests"));
}

#[test]
fn format_junit_wraps_ok_status_as_system_out() {
    // JUnit non è JSON: parse manuale minimale.
    let output = Command::new(BIN)
        .args(["--format", "junit", "profile-list"])
        .env_remove("PG_DSN")
        .output()
        .expect("spawn");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("<?xml version=\"1.0\""));
    assert!(stdout.contains("<testsuite name=\"plenora-database-cli\""));
    assert!(stdout.contains("failures=\"0\""));
    assert!(stdout.contains("<system-out>"));
}

#[test]
fn format_markdown_renders_title_and_bullets() {
    let output = Command::new(BIN)
        .args(["--format", "markdown", "profile-list"])
        .env_remove("PG_DSN")
        .output()
        .expect("spawn");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("# profile-list"));
    assert!(stdout.contains("- **profiles**:"));
}

/// I comandi che leggono un documento da un percorso posizionale non lo
/// ripetono quando il file non e leggibile: in quello slot puo finire una
/// DSN o un token, e il messaggio dice quale file manca, non cosa e stato
/// ricevuto.
#[test]
fn unreadable_positional_paths_stay_out_of_the_message() {
    let path = "postgres://utente:segreto@host/inesistente.json";
    for (args, slot) in [
        (vec!["portable-compile", "postgres", path], "PORTABLE.json"),
        (vec!["portable-execute", "PG_DSN", path], "PORTABLE.json"),
        (vec!["postgres-query", "PG_DSN", path], "QUERY.json"),
        (
            vec!["bulk-write", "PG_DSN", path, "input.arrow"],
            "WRITE_OP.json",
        ),
    ] {
        let output = Command::new(BIN)
            .args(&args)
            .env("PG_DSN", "postgres://localhost/plenora")
            .output()
            .expect("spawn CLI");
        assert!(!output.status.success(), "{args:?}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let envelope: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|e| panic!("{args:?}: output errore non JSON: {e}"));
        let message = envelope["error"]["message"].as_str().unwrap_or("");
        assert!(
            message.starts_with(&format!("{slot} non leggibile")),
            "{args:?}: {message}"
        );
        assert!(!stdout.contains("segreto"), "{args:?}: {stdout}");
        assert!(!stderr.contains("segreto"), "{args:?}: {stderr}");
    }
}
