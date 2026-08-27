//! Snapshot contract tests: fissano lo shape del JSON di output della CLI
//! per i sub-comandi che il PFM (o qualsiasi consumer subprocess) userà.
//!
//! Se un test qui fallisce, significa che la struttura JSON dell'output è
//! cambiata — probabile breaking change per il consumer downstream.
//! Aggiornare il test SOLO dopo aver deciso consapevolmente il breaking.
//!
//! Alcuni test sono live (`#[ignore]`), altri sono puramente offline
//! (error envelopes, usage).

#![cfg(test)]
#![allow(clippy::items_after_statements, clippy::doc_markdown)]

use serde_json::Value;
use std::collections::BTreeSet;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_plenora-database");
const DSN: &str =
    "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test";

fn run_json_env(args: &[&str], envs: &[(&str, &str)]) -> Value {
    let mut cmd = Command::new(BIN);
    cmd.args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let output = cmd.output().expect("spawn");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() || !stdout.is_empty(),
        "CLI produced neither success nor JSON: args={args:?}\nstdout={stdout}\nstderr={stderr}"
    );
    serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("output non JSON: {e}\nstdout={stdout}"))
}

fn top_level_keys(v: &Value) -> BTreeSet<String> {
    match v {
        Value::Object(map) => map.keys().cloned().collect(),
        _ => BTreeSet::new(),
    }
}

fn assert_has_keys(v: &Value, keys: &[&str]) {
    let actual = top_level_keys(v);
    for k in keys {
        assert!(
            actual.contains(*k),
            "chiave attesa '{k}' assente. keys presenti: {actual:?}"
        );
    }
}

// ============================================================================
//  Offline snapshot: error envelope canonico
// ============================================================================

#[test]
fn snapshot_error_envelope_has_stable_shape() {
    let output = Command::new(BIN)
        .args(["completely-unknown-command-xyz"])
        .env("PG_DSN", DSN)
        .output()
        .expect("spawn");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let v: Value = serde_json::from_str(&stdout).expect("valid JSON");
    // Contract v1 envelope errore:
    //   { "status": "error", "protocol_version": 1, "error": { ... } }
    assert_eq!(v["status"], "error");
    assert_eq!(v["protocol_version"], 1);
    assert_has_keys(&v, &["error", "protocol_version", "status"]);
    // error.* fields fissi
    assert_has_keys(
        &v["error"],
        &[
            "category",
            "phase",
            "remote_effect",
            "retry",
            "message",
            "execution_id",
            "provider",
        ],
    );
    // retry ha SEMPRE un campo "kind"
    assert_has_keys(&v["error"]["retry"], &["kind"]);
}

// ============================================================================
//  Live snapshot: profile-list
// ============================================================================

#[ignore = "live: richiede Postgres per far bootstrap del CLI"]
#[test]
fn snapshot_profile_list_shape() {
    let v = run_json_env(&["profile-list"], &[("PG_DSN", DSN)]);
    assert_has_keys(&v, &["profiles"]);
    let profiles = v["profiles"].as_array().expect("profiles array");
    assert!(profiles.len() >= 3, "atteso ≥3 profili");
    for p in profiles {
        assert_has_keys(p, &["name", "required", "required_count"]);
    }
    // Nomi profili canonici — se cambiano, il PFM va aggiornato.
    let names: BTreeSet<String> = profiles
        .iter()
        .filter_map(|p| p["name"].as_str().map(str::to_owned))
        .collect();
    assert!(names.contains("APPLICATION_OLTP_V1"));
    assert!(names.contains("PFM_CORE_V1"));
    assert!(names.contains("PFM_GIS_V1"));
}

// ============================================================================
//  Live snapshot: profile-check APPLICATION_OLTP_V1
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_profile_check_shape() {
    let v = run_json_env(
        &["profile-check", "PG_DSN", "APPLICATION_OLTP_V1"],
        &[("PG_DSN", DSN)],
    );
    // Contract:
    //   { "profile": "APPLICATION_OLTP_V1", "status": "pass|fail",
    //     "evidence": [{...}], "missing": [], "failed": [] }
    assert_has_keys(&v, &["profile", "status", "evidence", "missing", "failed"]);
    assert_eq!(v["profile"], "APPLICATION_OLTP_V1");
    assert!(v["evidence"].is_array());
    // Ogni evidence entry ha shape stabile.
    let ev = v["evidence"].as_array().unwrap();
    if let Some(first) = ev.first() {
        assert_has_keys(first, &["capability", "kind", "notes"]);
    }
}

// ============================================================================
//  Live snapshot: doctor
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_doctor_shape() {
    let v = run_json_env(&["doctor", "PG_DSN"], &[("PG_DSN", DSN)]);
    assert_has_keys(&v, &["status", "connection", "capabilities", "profiles"]);
    assert_has_keys(
        &v["profiles"],
        &["APPLICATION_OLTP_V1", "PFM_CORE_V1", "PFM_GIS_V1"],
    );
    assert_has_keys(
        &v["connection"],
        &[
            "status",
            "provider",
            "server_version",
            "connection_identity",
        ],
    );
}

// ============================================================================
//  Live snapshot: diagnose
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_diagnose_shape() {
    let v = run_json_env(&["diagnose", "PG_DSN"], &[("PG_DSN", DSN)]);
    // Superset di doctor + connect_ms + server_config + findings.
    assert_has_keys(
        &v,
        &[
            "status",
            "connection",
            "capabilities",
            "server_config",
            "profiles",
            "findings",
        ],
    );
    assert_has_keys(&v["connection"], &["connect_ms"]);
    assert!(v["findings"].is_array());
}

// ============================================================================
//  Live snapshot: execute-sql result shape (rows and affected_rows)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_execute_sql_rows_shape() {
    let v = run_json_env(
        &["execute-sql", "PG_DSN", "SELECT 42::INT AS n"],
        &[("PG_DSN", DSN)],
    );
    assert_has_keys(&v, &["status", "commit", "result"]);
    assert_eq!(v["status"], "ok");
    assert_has_keys(&v["result"], &["kind", "rows", "count"]);
    assert_eq!(v["result"]["kind"], "rows");
    let rows = v["result"]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1);
    // ogni cella è { "type": "...", "value": ... } — contract v1.
    assert_has_keys(&rows[0]["n"], &["type", "value"]);
    assert_eq!(rows[0]["n"]["value"], 42);
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_execute_sql_affected_rows_shape() {
    // execute-ddl per non richiedere gate write. Un CREATE INDEX su tabella
    // temporanea che non esiste è validato lato Postgres; useremo un DDL
    // no-op safe: CREATE EXTENSION IF NOT EXISTS plpgsql (già presente).
    let _ = run_json_env(
        &[
            "execute-ddl",
            "PG_DSN",
            "CREATE EXTENSION IF NOT EXISTS plpgsql",
        ],
        &[("PG_DSN", DSN)],
    );
    // Verifica la forma `affected_rows` con una tabella temporanea.
    // Uso una tx unica multiline non è possibile — usiamo un UPDATE su
    // una tabella non esistente: ci basta che il payload sia strutturato.
    // Alternative pulito: usiamo execute-sql su una query che modifica 0
    // righe di una tabella nota (spatial_ref_sys) con WHERE false.
    let v = run_json_env(
        &[
            "execute-sql",
            "PG_DSN",
            "UPDATE spatial_ref_sys SET srtext = srtext WHERE false",
        ],
        &[("PG_DSN", DSN)],
    );
    assert_has_keys(&v, &["status", "commit", "result"]);
    assert_has_keys(&v["result"], &["kind", "count"]);
    assert_eq!(v["result"]["kind"], "affected_rows");
    assert_eq!(v["result"]["count"], 0);
}

// ============================================================================
//  Live snapshot: execute-scalar
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_execute_scalar_shape() {
    let v = run_json_env(
        &["execute-scalar", "PG_DSN", "SELECT 42::INT", "--type=i32"],
        &[("PG_DSN", DSN)],
    );
    assert_has_keys(&v, &["status", "type", "value"]);
    assert_eq!(v["type"], "i32");
    assert_eq!(v["value"], 42);
}

// ============================================================================
//  Live snapshot: inspect-database
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_inspect_database_shape() {
    let v = run_json_env(&["inspect-database", "PG_DSN"], &[("PG_DSN", DSN)]);
    assert_has_keys(
        &v,
        &[
            "database",
            "user",
            "version",
            "version_num",
            "encoding",
            "timezone",
            "size",
            "extensions",
        ],
    );
    assert!(v["extensions"].is_array());
}

// ============================================================================
//  Live snapshot: portable-compile
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_portable_compile_shape() {
    let dir = std::env::temp_dir().join(format!("plenora-snap-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("portable.json");
    let portable_json = serde_json::json!({
        "type": "select",
        "table": { "name": "spatial_ref_sys" },
        "projection": { "kind": "columns", "value": ["srid"] },
        "filter": null,
        "order_by": [],
        "limit": 1
    });
    std::fs::write(&p, serde_json::to_vec(&portable_json).unwrap()).unwrap();

    let v = run_json_env(
        &["portable-compile", "postgres", p.to_str().unwrap()],
        &[("PG_DSN", DSN)],
    );
    // Contract: { "status": "ok", "provider": "postgres", "sql": "...",
    //             "param_count": N }
    assert_has_keys(&v, &["status", "provider", "sql", "param_count"]);
    assert_eq!(v["status"], "ok");
    assert_eq!(v["provider"], "postgres");
    assert!(v["sql"].as_str().unwrap().contains("spatial_ref_sys"));
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
//  Live snapshot: bulk-write --dry-run
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_bulk_write_dry_run_shape() {
    let dir = std::env::temp_dir().join(format!("plenora-snap-bw-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let input_path = dir.join("in.arrow");
    let op_path = dir.join("op.json");

    // Materializzo un input Arrow via postgres-read-ipc.
    let _ = run_json_env(
        &[
            "postgres-read-ipc",
            "PG_DSN",
            "public",
            "spatial_ref_sys",
            input_path.to_str().unwrap(),
            "--limit",
            "1",
        ],
        &[("PG_DSN", DSN)],
    );

    let write_op = serde_json::json!({
        "target": {
            "catalog": null, "schema": "public", "object": "_snap_never"
        },
        "mode": "create",
        "mapping_policy": "strict",
        "transaction_profile": "single_transaction",
        "keys": [], "update_columns": [],
        "srid_policy": null, "create_spatial_index": false,
        "allow_partial": false
    });
    std::fs::write(&op_path, serde_json::to_vec(&write_op).unwrap()).unwrap();

    let v = run_json_env(
        &[
            "bulk-write",
            "PG_DSN",
            op_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
            "--dry-run",
        ],
        &[("PG_DSN", DSN)],
    );
    assert_has_keys(&v, &["status", "operation", "input_schema"]);
    assert_eq!(v["status"], "dry_run");
    assert_has_keys(&v["input_schema"], &["fields"]);

    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
//  Live snapshot: format=junit envelope contract
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn snapshot_junit_format_wraps_output() {
    let output = Command::new(BIN)
        .args(["--format", "junit", "profile-list"])
        .env("PG_DSN", DSN)
        .output()
        .expect("spawn");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Contract JUnit: XML con testsuite + testcase.
    assert!(stdout.starts_with("<?xml version=\"1.0\""));
    assert!(stdout.contains("<testsuite name=\"plenora-database-cli\""));
    assert!(stdout.contains("<testcase classname=\"profile-list\""));
}
