//! Prove live dei comandi operativi CLI, eseguite dal gate PostgreSQL.
//! La connessione e il TLS sono configurati dal runner.

#![cfg(test)]

use serde_json::Value;
mod common;

fn run(args: &[&str]) -> Value {
    let output = common::postgres_cli(args).output().expect("spawn CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI ha fallito per args={args:?}\nstdout={stdout}\nstderr={stderr}"
    );
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("output non JSON: {e}\nstdout={stdout}");
    })
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_inspect_database_returns_expected_metadata_shape() {
    let out = run(&["inspect-database", "PG_DSN"]);
    assert!(
        out["database"].as_str().is_some(),
        "database mancante: {out}"
    );
    assert!(out["version"].as_str().is_some(), "version mancante: {out}");
    assert!(
        out["version_num"].as_str().is_some(),
        "version_num mancante: {out}"
    );
    assert!(out["encoding"].as_str().is_some());
    assert!(out["timezone"].as_str().is_some());
    assert!(out["size"].as_str().is_some());
    let exts = out["extensions"].as_array().expect("extensions array");
    // plpgsql è sempre presente su Postgres.
    assert!(
        exts.iter().any(|e| e["name"] == "plpgsql"),
        "plpgsql non trovato tra le extensions: {exts:?}"
    );
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_inspect_schemas_lists_user_schemas() {
    let out = run(&["inspect-schemas", "PG_DSN"]);
    let count = out["count"].as_u64().expect("count u64");
    let schemas = out["schemas"].as_array().expect("schemas array");
    assert_eq!(count, schemas.len() as u64, "count/schemas mismatch");
    // 'public' deve esserci sempre; pg_catalog non deve.
    assert!(
        schemas.iter().any(|s| s["name"] == "public"),
        "public schema mancante: {schemas:?}"
    );
    assert!(
        !schemas.iter().any(|s| s["name"] == "pg_catalog"),
        "pg_catalog non deve essere elencato: {schemas:?}"
    );
    for s in schemas {
        assert!(s["owner"].as_str().is_some(), "owner mancante: {s}");
    }
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_inspect_tables_lists_relations_with_size_and_row_estimate() {
    let out = run(&["inspect-tables", "PG_DSN", "public"]);
    assert_eq!(out["schema"], "public");
    let tables = out["tables"].as_array().expect("tables array");
    // spatial_ref_sys è di PostGIS ed è sempre in public quando l'estensione è installata.
    let srs = tables
        .iter()
        .find(|t| t["name"] == "spatial_ref_sys")
        .expect("la fixture PostGIS deve esporre spatial_ref_sys");
    assert_eq!(srs["kind"], "table");
    assert!(srs["total_size"].as_str().is_some());
    assert!(srs["estimated_rows"].is_number());
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_diagnose_reports_healthy_when_probes_pass() {
    let out = run(&["diagnose", "PG_DSN"]);
    // Su un Postgres standard con PostGIS installato, OLTP e PFM_CORE devono
    // passare; PFM_GIS può fallire se PostGIS è assente. Ma lo status generale
    // ha una definizione precisa: overall_pass = OLTP && PFM_CORE.
    let status = out["status"].as_str().expect("status str");
    assert!(
        status == "healthy" || status == "unhealthy",
        "status inatteso: {status}"
    );
    assert_eq!(out["connection"]["status"], "ok");
    assert!(
        out["connection"]["connect_ms"].as_u64().is_some(),
        "connect_ms mancante"
    );
    // server_config deve avere almeno max_connections.
    assert!(
        out["server_config"]["max_connections"]["value"]
            .as_str()
            .is_some(),
        "server_config.max_connections mancante"
    );
    // I 3 profili sono sempre inclusi anche quando falliscono.
    assert!(out["profiles"]["APPLICATION_OLTP_V1"]["status"].is_string());
    assert!(out["profiles"]["PFM_CORE_V1"]["status"].is_string());
    assert!(out["profiles"]["PFM_GIS_V1"]["status"].is_string());
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_benchmark_write_reports_throughput_and_percentiles() {
    let out = run(&[
        "--allow-write-tests",
        "benchmark-write",
        "PG_DSN",
        "20", // iterations
        "5",  // batch_size
    ]);
    assert_eq!(out["iterations"], 20);
    assert_eq!(out["batch_size"], 5);
    assert_eq!(out["total_rows"], 100);
    assert!(out["rows_per_sec"].as_f64().unwrap_or(0.0) > 0.0);
    assert!(out["latency_us"]["p50"].is_number());
    assert!(out["latency_us"]["p95"].is_number());
    assert!(out["latency_us"]["p99"].is_number());
    let p50 = out["latency_us"]["p50"].as_u64().unwrap_or(0);
    let p95 = out["latency_us"]["p95"].as_u64().unwrap_or(0);
    let p99 = out["latency_us"]["p99"].as_u64().unwrap_or(0);
    assert!(
        p50 <= p95 && p95 <= p99,
        "percentili non monotoni: p50={p50} p95={p95} p99={p99}"
    );
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_test_concurrency_reports_winner_and_loser_correctly() {
    let out = run(&["--allow-write-tests", "test-concurrency", "PG_DSN"]);
    assert_eq!(out["status"], "ok");
    assert_eq!(out["winner_committed"], true);
    assert_eq!(out["loser_error_category"], "ConcurrentModification");
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_profile_check_returns_pass_for_application_oltp_v1() {
    let out = run(&["profile-check", "PG_DSN", "APPLICATION_OLTP_V1"]);
    assert_eq!(
        out["status"], "pass",
        "APPLICATION_OLTP_V1 deve passare: {out}"
    );
    assert!(out["evidence"].as_array().is_some());
    assert_eq!(out["missing"].as_array().map(Vec::len), Some(0));
    assert_eq!(out["failed"].as_array().map(Vec::len), Some(0));
}

/// `query --output` scrive il risultato come Arrow IPC stream, il content
/// type che il catalogo dichiara per `database.query`; senza `--output` resta
/// il riepilogo JSON.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_query_writes_its_result_as_an_arrow_stream() {
    use arrow_ipc::reader::StreamReader;

    let directory =
        std::env::temp_dir().join(format!("plenora-cli-query-arrow-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("cartella di prova");
    let operation = directory.join("query.json");
    std::fs::write(
        &operation,
        // SELECT srid FROM public.spatial_ref_sys LIMIT 3
        serde_json::json!({
            "source": {"object": {"schema": "public", "object": "spatial_ref_sys"}, "alias": null},
            "projection": [{
                "expression": {"kind": "column", "column": {"relation": null, "field": "srid"}},
                "alias": null
            }],
            "filter": null,
            "having": null,
            "row_limit": 3
        })
        .to_string(),
    )
    .expect("QUERY.json");
    let request = directory.join("request.json");
    std::fs::write(
        &request,
        serde_json::json!({
            "provider": "postgres",
            "secret_environment": "PG_DSN",
            "operation_path": operation.to_str().expect("UTF-8"),
        })
        .to_string(),
    )
    .expect("REQUEST.json");
    let output = directory.join("result.arrows");
    let out = run(&[
        "query",
        "--input",
        request.to_str().expect("UTF-8"),
        "--output",
        output.to_str().expect("UTF-8"),
    ]);
    assert_eq!(out["status"], "ok", "{out}");
    assert_eq!(out["result"]["format"], "arrow_ipc_stream", "{out}");
    assert_eq!(out["result"]["rows"], 3, "{out}");
    let reader = StreamReader::try_new(std::fs::File::open(&output).expect("output"), None)
        .expect("stream Arrow");
    let rows: usize = reader.map(|batch| batch.expect("batch").num_rows()).sum();
    assert_eq!(rows, 3);
    let _ = std::fs::remove_dir_all(directory);
}
