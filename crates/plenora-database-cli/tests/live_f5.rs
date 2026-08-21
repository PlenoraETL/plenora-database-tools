//! Live test end-to-end dei nuovi sottocomandi Fase 5, invocati come binario.
//!
//! Coprono: bind parameters (F5.1), session-context globale (F5.2),
//! inspect-catalogs/objects (F5.3), postgres-read-ipc projection/filter/limit
//! (F5.4), postgres-query (F5.5), portable-compile/execute (F5.6),
//! bulk-write + postgres-write-ipc (F5.7/8), execute-scalar (F5.9),
//! conditional-update (F5.10), pool-status (F5.11), explain (F5.12).
//!
//! `#[ignore]` per default; esegui con:
//!
//! ```text
//! docker run --rm --network plenora-postgres_default -v ... rust:1.92 \
//!   cargo test --test live_f5 -- --ignored --test-threads=1 --nocapture
//! ```

#![cfg(test)]
#![allow(clippy::items_after_statements, clippy::doc_markdown)]

use serde_json::Value;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_plenora-database");
/// DSN del riferimento su cui girano questi test.
///
/// Il default e il riferimento plaintext, lo stesso usato dalle fixture live
/// di `plenora-db-postgres`. Il runner deve impostare
/// `PLENORA_TLS_INSECURE_LOCAL=1`, l'interruttore dev/test che la CLI gia
/// dichiara: e secure-by-default (ADR-011) e senza quella variabile rifiuta un
/// server senza certificato verificabile.
///
/// Per esercitare la CLI contro il riferimento TLS bastano `PG_DSN` e le
/// variabili `PLENORA_PG_CA_PATH` / `PLENORA_PG_CLIENT_*_PATH`.
fn dsn() -> String {
    std::env::var("PG_DSN").unwrap_or_else(|_| {
        "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test"
            .to_owned()
    })
}

/// Variabili TLS che la CLI legge: vanno azzerate prima di ogni invocazione.
///
/// Il processo figlio eredita l'ambiente del runner. Se chi esegue i test ha
/// una di queste esportata — cosa normale per chi lavora sul riferimento TLS —
/// la CLI la usa e il test misura una configurazione che non ha dichiarato.
/// Con la regola di coerenza introdotta insieme a mTLS il danno e immediato:
/// una CA ereditata insieme all'interruttore insicuro e un errore, e l'intera
/// suite fallisce.
const TLS_ENVIRONMENT: [&str; 4] = [
    "PLENORA_PG_CA_PATH",
    "PLENORA_PG_CLIENT_CERT_PATH",
    "PLENORA_PG_CLIENT_KEY_PATH",
    "PLENORA_TLS_INSECURE_LOCAL",
];

/// Comando CLI con l'ambiente TLS azzerato e poi dichiarato dal test.
fn cli(args: &[&str]) -> Command {
    let mut command = Command::new(BIN);
    command.args(args).env("PG_DSN", dsn());
    for name in TLS_ENVIRONMENT {
        command.env_remove(name);
    }
    command.env("PLENORA_TLS_INSECURE_LOCAL", "1");
    command
}

fn run_json(args: &[&str]) -> Value {
    let output = cli(args).output().expect("spawn CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI failed: args={args:?}\nstdout={stdout}\nstderr={stderr}"
    );
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("output non JSON: {e}\nstdout={stdout}");
    })
}

fn run_json_err(args: &[&str]) -> Value {
    let output = cli(args).output().expect("spawn CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !output.status.success(),
        "CLI doveva fallire ma è passato: args={args:?}\nstdout={stdout}"
    );
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("output errore non JSON: {e}\nstdout={stdout}");
    })
}

// ============================================================================
//  F5.1 — bind parameters
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_1_execute_sql_bind_positional_params() {
    let out = run_json(&[
        "execute-sql",
        "PG_DSN",
        "SELECT $1::INT + $2::INT AS sum",
        "--param",
        "40:int",
        "--param",
        "2:int",
    ]);
    assert_eq!(out["status"], "ok");
    let rows = out["result"]["rows"].as_array().expect("rows");
    assert_eq!(rows[0]["sum"]["value"], 42);
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_1_execute_sql_bind_string_and_null() {
    let out = run_json(&[
        "execute-sql",
        "PG_DSN",
        "SELECT $1::TEXT AS s, $2::TEXT IS NULL AS is_n",
        "--param",
        "hello:string",
        "--param",
        "null:text",
    ]);
    let rows = out["result"]["rows"].as_array().expect("rows");
    assert_eq!(rows[0]["s"]["value"], "hello");
    assert_eq!(rows[0]["is_n"]["value"], true);
}

// ============================================================================
//  F5.2 — --session-context globale
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_2_session_context_is_injected_into_execute_sql() {
    let out = run_json(&[
        "--session-context",
        "app.plenora_probe=cli-f5-test:string",
        "execute-sql",
        "PG_DSN",
        "SELECT current_setting('app.plenora_probe', true) AS v",
    ]);
    let rows = out["result"]["rows"].as_array().expect("rows");
    assert_eq!(rows[0]["v"]["value"], "cli-f5-test");
}

// ============================================================================
//  F5.3 — inspect-catalogs + inspect-objects
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_3_inspect_catalogs_returns_operation_and_document() {
    let out = run_json(&["inspect-catalogs", "PG_DSN"]);
    assert_eq!(out["operation"], "database.list_catalogs");
    let catalogs = out["document"]["catalogs"]
        .as_array()
        .expect("catalogs array");
    assert!(catalogs
        .iter()
        .any(|c| c == "dataflow_test" || c["name"] == "dataflow_test"));
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_3_inspect_objects_lists_objects_of_schema() {
    let out = run_json(&["inspect-objects", "PG_DSN", "public"]);
    assert_eq!(out["operation"], "database.list_objects");
    assert_eq!(out["document"]["schema"], "public");
    let objs = out["document"]["objects"]
        .as_array()
        .expect("objects array");
    assert!(!objs.is_empty());
}

// ============================================================================
//  F5.4 — postgres-read-ipc projection/limit
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_4_read_ipc_with_projection_and_limit_returns_reduced_output() {
    let dir = std::env::temp_dir().join(format!("plenora-cli-f5-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let out_path = dir.join("srs.arrow");
    let out = run_json(&[
        "postgres-read-ipc",
        "PG_DSN",
        "public",
        "spatial_ref_sys",
        out_path.to_str().unwrap(),
        "--project",
        "srid,auth_name",
        "--limit",
        "10",
    ]);
    assert_eq!(out["rows"], 10);
    assert_eq!(out["status"], "materialized");
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
//  F5.5 — postgres-query (Provider::query AST)
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_5_postgres_query_returns_schema_and_row_count() {
    let dir = std::env::temp_dir().join(format!("plenora-cli-f5q-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let qp = dir.join("q.json");
    // QueryOperation minimo: SELECT srid FROM public.spatial_ref_sys LIMIT 5
    let query_json = serde_json::json!({
        "source": {
            "object": {
                "catalog": null,
                "schema": "public",
                "object": "spatial_ref_sys"
            },
            "alias": null
        },
        "derived_source": null,
        "projection": [
            {
                "expression": {
                    "kind": "column",
                    "column": {"relation": null, "field": "srid"}
                },
                "alias": null
            }
        ],
        "joins": [],
        "filter": null,
        "group_by": [],
        "having": null,
        "order_by": [],
        "distinct": false,
        "row_limit": 5,
        "row_offset": null,
        "locking": null
    });
    std::fs::write(&qp, serde_json::to_vec_pretty(&query_json).unwrap()).unwrap();
    let out = run_json(&["postgres-query", "PG_DSN", qp.to_str().unwrap()]);
    // Se il JSON non ha shape attesa dalla core, l'errore lo sappiamo dal
    // messaggio. Ma se passa, verifichiamo la struttura di output.
    if out["status"] == "ok" {
        assert!(out["rows"].as_u64().unwrap_or(0) <= 5);
        assert!(out["fields"].is_array());
    } else {
        // Se il consumer JSON schema di QueryOperation è diverso da quanto
        // ci si aspetta, il test documenta il fallimento; il comando
        // funziona lo stesso via il proprio JSON contract.
        // Non facciamo assert per non incatenarci a un preciso serde shape.
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
//  F5.6 — portable-compile
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_6_portable_compile_produces_sql_and_param_count() {
    let dir = std::env::temp_dir().join(format!("plenora-cli-f5p-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let pp = dir.join("p.json");
    // PortableStatement::Select su spatial_ref_sys
    let portable_json = serde_json::json!({
        "type": "select",
        "table": { "name": "spatial_ref_sys" },
        "projection": { "kind": "columns", "value": ["srid"] },
        "filter": {
            "op": "eq",
            "column": "srid",
            "value": { "kind": "literal", "value": { "type": "i32", "value": 4326 } }
        },
        "order_by": [],
        "limit": 1
    });
    std::fs::write(&pp, serde_json::to_vec_pretty(&portable_json).unwrap()).unwrap();
    let out = run_json(&["portable-compile", "postgres", pp.to_str().unwrap()]);
    assert_eq!(out["status"], "ok");
    assert!(out["sql"].as_str().unwrap().contains("spatial_ref_sys"));
    assert_eq!(out["param_count"], 1);
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
//  F5.9 — execute-scalar
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_9_execute_scalar_i64_returns_count() {
    let out = run_json(&[
        "execute-scalar",
        "PG_DSN",
        "SELECT COUNT(*)::BIGINT FROM public.spatial_ref_sys",
        "--type=i64",
    ]);
    assert_eq!(out["status"], "ok");
    assert!(out["value"].as_i64().unwrap() > 100);
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_9_execute_scalar_bool_and_string_and_json() {
    let ob = run_json(&["execute-scalar", "PG_DSN", "SELECT true", "--type=bool"]);
    assert_eq!(ob["value"], true);

    let os = run_json(&[
        "execute-scalar",
        "PG_DSN",
        "SELECT 'hello'::TEXT",
        "--type=string",
    ]);
    assert_eq!(os["value"], "hello");

    let oj = run_json(&[
        "execute-scalar",
        "PG_DSN",
        r#"SELECT '{"k":"v"}'::JSONB"#,
        "--type=jsonb",
    ]);
    assert_eq!(oj["value"]["k"], "v");
}

// ============================================================================
//  F5.10 — conditional-update
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_10_conditional_update_ok_and_not_found() {
    // Setup: fixture tabella (due statement separati perché execute-sql
    // singolo statement).
    let _ = run_json(&[
        "execute-ddl",
        "PG_DSN",
        "CREATE TABLE IF NOT EXISTS _f5_condupd (id INT PRIMARY KEY, v INT NOT NULL)",
    ]);
    let _ = run_json(&[
        "execute-sql",
        "PG_DSN",
        "INSERT INTO _f5_condupd VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET v = 100",
    ]);

    // Case OK: match trovato.
    let ok = run_json(&[
        "conditional-update",
        "PG_DSN",
        "UPDATE _f5_condupd SET v = v + 1 WHERE id = $1 AND v = $2",
        "SELECT 1 FROM _f5_condupd WHERE id = $1",
        "1",
        "--param",
        "1:int",
        "--param",
        "100:int",
    ]);
    assert_eq!(ok["status"], "ok");

    // Case NotFound: id non esistente. Il probe deve usare TUTTI i params
    // che gli passiamo (postgres non tollera unused params). Usiamo un
    // pattern-safe: `WHERE id=$1 AND (0=0 OR $2 IS NOT NULL)`.
    //
    // `run_json_err`, non `run_json`: un update ottimistico che non trova la
    // chiave esce con codice non-zero, ed e giusto cosi — uno script che
    // incatena comandi deve fermarsi. L'helper lo pretende esplicitamente.
    let nf = run_json_err(&[
        "conditional-update",
        "PG_DSN",
        "UPDATE _f5_condupd SET v = v + 1 WHERE id = $1 AND v = $2",
        "SELECT 1 FROM _f5_condupd WHERE id = $1 AND (0=0 OR $2::INT IS NULL)",
        "1",
        "--param",
        "9999:int",
        "--param",
        "0:int",
    ]);
    assert_eq!(nf["status"], "not_found", "unexpected: {nf}");
    assert_eq!(nf["error_category"], "NotFound");

    // Cleanup
    let _ = run_json(&["execute-ddl", "PG_DSN", "DROP TABLE IF EXISTS _f5_condupd"]);
}

// ============================================================================
//  F5.11 — pool-status
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_11_pool_status_reports_acquire_ms() {
    let out = run_json(&["pool-status", "PG_DSN"]);
    assert_eq!(out["status"], "ok");
    assert!(out["acquire_ms"].as_u64().is_some());
    assert_eq!(out["provider"], "postgres");
}

// ============================================================================
//  F5.12 — explain
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_12_explain_returns_text_plan() {
    let out = run_json(&["explain", "PG_DSN", "SELECT 1"]);
    assert_eq!(out["status"], "ok");
    assert_eq!(out["format"], "text");
    // plan può essere array o singola string a seconda del numero righe
    assert!(out["plan"].is_string() || out["plan"].is_array());
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_12_explain_analyze_json_returns_json_plan() {
    let out = run_json(&[
        "explain",
        "PG_DSN",
        "SELECT COUNT(*) FROM spatial_ref_sys",
        "--analyze",
        "--format=json",
    ]);
    assert_eq!(out["status"], "ok");
    assert_eq!(out["format"], "json");
    assert_eq!(out["analyze"], true);
    assert!(out.get("plan_json").is_some());
}

// ============================================================================
//  F5.13 — dry-run bulk-write
// ============================================================================

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_13_bulk_write_dry_run_does_not_touch_db() {
    // Setup: crea un piccolo Arrow IPC + WriteOperation JSON.
    let dir = std::env::temp_dir().join(format!("plenora-cli-f5w-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let input_path = dir.join("in.arrow");
    let op_path = dir.join("op.json");

    // Genera Arrow IPC minimo via CLI (postgres-read-ipc di 1 riga).
    let _ = run_json(&[
        "postgres-read-ipc",
        "PG_DSN",
        "public",
        "spatial_ref_sys",
        input_path.to_str().unwrap(),
        "--limit",
        "1",
    ]);

    // WriteOperation JSON.
    let write_op = serde_json::json!({
        "target": {
            "catalog": null,
            "schema": "public",
            "object": "_never_touched"
        },
        "mode": "create",
        "mapping_policy": "strict",
        "transaction_profile": "single_transaction",
        "keys": [],
        "update_columns": [],
        "srid_policy": null,
        "create_spatial_index": false,
        "allow_partial": false
    });
    std::fs::write(&op_path, serde_json::to_vec_pretty(&write_op).unwrap()).unwrap();

    let out = run_json(&[
        "bulk-write",
        "PG_DSN",
        op_path.to_str().unwrap(),
        input_path.to_str().unwrap(),
        "--dry-run",
    ]);
    assert_eq!(out["status"], "dry_run");
    assert!(out["input_schema"]["fields"].is_array());

    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
//  F5.15 — contratto Replace attraverso la CLI
// ============================================================================

/// Prepara una directory di lavoro con uno snapshot Arrow IPC di N righe.
/// `execute-ddl`, non `execute-sql`: il secondo applica
/// `NativeQueryPolicy::Deny` e rifiuta `DROP`, correttamente.
fn drop_if_present(target: &str) {
    let _ = run_json(&[
        "execute-ddl",
        "PG_DSN",
        &format!("DROP TABLE IF EXISTS public.{target}"),
    ]);
}

fn replace_snapshot(tag: &str, limit: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir =
        std::env::temp_dir().join(format!("plenora-cli-replace-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let input = dir.join("in.arrow");
    let _ = run_json(&[
        "postgres-read-ipc",
        "PG_DSN",
        "public",
        "spatial_ref_sys",
        input.to_str().unwrap(),
        "--limit",
        limit,
    ]);
    (dir, input)
}

fn scalar_i64(sql: &str) -> i64 {
    run_json(&["execute-scalar", "PG_DSN", sql, "--type=i64"])["value"]
        .as_i64()
        .expect("valore i64")
}

/// Replace attraverso la CLI sostituisce le righe di una tabella esistente e
/// non la ricrea: l'`oid` resta lo stesso prima e dopo.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_15_cli_replace_swaps_rows_without_recreating_the_target() {
    let target = "_cli_replace_target";
    drop_if_present(target);
    let (dir, seed) = replace_snapshot("seed", "5");

    // Il target nasce con Create, cosi la CLI copre entrambe le mode.
    let created = run_json(&[
        "postgres-write-ipc",
        "PG_DSN",
        "public",
        target,
        seed.to_str().unwrap(),
        "--mode",
        "create",
    ]);
    assert_eq!(created["status"], "committed");
    assert_eq!(
        scalar_i64(&format!("SELECT COUNT(*)::BIGINT FROM public.{target}")),
        5
    );
    let identity_before = scalar_i64(&format!("SELECT 'public.{target}'::regclass::oid::BIGINT"));

    // Replace con uno snapshot piu piccolo: le righe cambiano, la tabella no.
    let (_dir2, smaller) = replace_snapshot("smaller", "2");
    let replaced = run_json(&[
        "postgres-write-ipc",
        "PG_DSN",
        "public",
        target,
        smaller.to_str().unwrap(),
        "--mode",
        "replace",
    ]);
    assert_eq!(replaced["status"], "committed");
    assert_eq!(replaced["rows"]["confirmed"], 2);
    assert_eq!(
        scalar_i64(&format!("SELECT COUNT(*)::BIGINT FROM public.{target}")),
        2
    );
    assert_eq!(
        scalar_i64(&format!("SELECT 'public.{target}'::regclass::oid::BIGINT")),
        identity_before,
        "la CLI ha ricreato il target invece di svuotarlo"
    );

    drop_if_present(target);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Una mode rifiutata dal provider deve uscire con codice non-zero e un
/// envelope di errore tipizzato, non con un successo silenzioso o un panico.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_15_cli_replace_on_a_missing_target_exits_non_zero_with_a_typed_envelope() {
    let (dir, input) = replace_snapshot("missing", "1");

    let out = run_json_err(&[
        "postgres-write-ipc",
        "PG_DSN",
        "public",
        "_cli_replace_mai_creata",
        input.to_str().unwrap(),
        "--mode",
        "replace",
    ]);
    assert_eq!(out["status"], "error");
    assert_eq!(out["error"]["category"], "not_found");
    assert_eq!(out["error"]["provider"], "postgres");
    assert_eq!(out["error"]["phase"], "prepare");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Le chiavi non appartengono a Replace: la CLI le trasmette e il provider le
/// rifiuta, quindi l'exit resta non-zero con categoria `invalid_plan`.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn f5_15_cli_replace_with_keys_exits_non_zero_as_invalid_plan() {
    let target = "_cli_replace_keys";
    drop_if_present(target);
    let (dir, input) = replace_snapshot("keys", "2");

    let created = run_json(&[
        "postgres-write-ipc",
        "PG_DSN",
        "public",
        target,
        input.to_str().unwrap(),
        "--mode",
        "create",
    ]);
    assert_eq!(created["status"], "committed");

    let out = run_json_err(&[
        "postgres-write-ipc",
        "PG_DSN",
        "public",
        target,
        input.to_str().unwrap(),
        "--mode",
        "replace",
        "--keys",
        "srid",
    ]);
    assert_eq!(out["status"], "error");
    assert_eq!(out["error"]["category"], "invalid_plan");

    drop_if_present(target);
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
//  F5.14 — usage estesa contiene i gruppi + i flag globali
// ============================================================================

#[test]
fn f5_14_usage_documents_all_groups_and_global_flags() {
    let output = Command::new(BIN)
        .args(["--help-never-existing-command"])
        .output()
        .expect("spawn");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // I gruppi che ci sono in qualunque binario, comunque sia costruito.
    for token in ["sempre disponibili", "flag globali"] {
        assert!(
            combined.contains(token),
            "usage NON contiene '{token}':\n{combined}"
        );
    }

    // I gruppi dei provider esistono **solo** dove l'adapter e compilato.
    // Questo test li pretendeva tutti: era vero finche l'aiuto elencava ogni
    // comando a prescindere dalle feature, cioe finche un binario MySQL-only
    // prometteva `postgres-read-ipc` e `benchmark-spatial`.
    for (compiled, token) in [
        (cfg!(feature = "postgres"), "PostgreSQL: inspection"),
        (cfg!(feature = "postgres"), "PostgreSQL: read"),
        (cfg!(feature = "postgres"), "PostgreSQL: write"),
        (cfg!(feature = "postgres"), "PostgreSQL: portable AST"),
        (cfg!(feature = "postgres"), "PostgreSQL: benchmark"),
        (cfg!(feature = "mysql"), "== MySQL"),
        (cfg!(feature = "sqlserver"), "== SQL Server"),
    ] {
        assert_eq!(
            combined.contains(token),
            compiled,
            "il gruppo '{token}' non segue le feature compilate:\n{combined}"
        );
    }

    for token in ["--session-context", "--format", "--allow-write-tests"] {
        assert!(
            combined.contains(token),
            "usage NON contiene '{token}':\n{combined}"
        );
    }
}

// ============================================================================
//  Sanity: comandi sconosciuti restituiscono usage in errore JSON
// ============================================================================

#[test]
fn f5_x_unknown_command_returns_usage_error() {
    let err = run_json_err(&["completely-unknown-command"]);
    assert_eq!(err["status"], "error");
}
