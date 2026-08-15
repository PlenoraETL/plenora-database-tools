//! Sottocomandi PFM: profile-check, doctor, execute-ddl, execute-sql,
//! transaction-test, session-context-test.

use crate::{ensure_end, print_json, secret_from_env, CliError, CliResult};
use plenora_database_core::conformance::{
    check_profile, probe_application_oltp_v1, probe_pfm_core_v1, probe_pfm_gis_v1, ProfileStatus,
    APPLICATION_OLTP_V1, PFM_CORE_V1, PFM_GIS_V1,
};
use plenora_database_core::facade::execute_scalar_string;
use plenora_database_core::provider::Provider;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::session_context::{SessionContext, SessionEntry, SessionValue};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::{PostgresProvider, PostgresTlsMode};
use serde_json::json;

pub(crate) fn postgres_provider_for_pfm() -> PostgresProvider {
    PostgresProvider::default().with_tls_mode(PostgresTlsMode::Disabled)
}

pub(crate) fn pfm_budget() -> CliResult<ResourceBudget> {
    ResourceBudget::new(ResourceLimits::default()).map_err(CliError::from)
}

pub(crate) async fn profile_check(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let profile_name = args.next().ok_or("manca il nome del profilo")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let cancel = CancellationToken::new();

    let (profile, evidence) = match profile_name.as_str() {
        "APPLICATION_OLTP_V1" => (
            &APPLICATION_OLTP_V1,
            probe_application_oltp_v1(&provider, &secret, &cancel).await,
        ),
        "PFM_CORE_V1" => (
            &PFM_CORE_V1,
            probe_pfm_core_v1(&provider, &secret, &cancel).await,
        ),
        "PFM_GIS_V1" => (
            &PFM_GIS_V1,
            probe_pfm_gis_v1(&provider, &secret, &cancel).await,
        ),
        _ => {
            return Err("profilo sconosciuto (usa APPLICATION_OLTP_V1 | PFM_CORE_V1 | PFM_GIS_V1)"
                .into())
        }
    };
    let report = check_profile(profile, &evidence);
    print_json(&serde_json::to_value(&report).map_err(|_| "report non serializzabile".to_owned())?)
}

pub(crate) async fn doctor(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let cancel = CancellationToken::new();

    let connection = match provider.test_connection(&secret, &cancel).await {
        Ok(info) => json!({
            "status": "ok",
            "provider": info.provider,
            "server_version": info.server_version,
            "connection_identity": info.connection_identity,
        }),
        Err(e) => json!({ "status": "fail", "error": e }),
    };

    let capabilities = match provider.probe_capabilities(&secret, &cancel).await {
        Ok(caps) => serde_json::to_value(&caps).unwrap_or(serde_json::Value::Null),
        Err(e) => json!({ "error": e }),
    };

    let oltp_evidence = probe_application_oltp_v1(&provider, &secret, &cancel).await;
    let oltp_report = check_profile(&APPLICATION_OLTP_V1, &oltp_evidence);

    let pfm_core_evidence = probe_pfm_core_v1(&provider, &secret, &cancel).await;
    let pfm_core_report = check_profile(&PFM_CORE_V1, &pfm_core_evidence);

    let pfm_gis_evidence = probe_pfm_gis_v1(&provider, &secret, &cancel).await;
    let pfm_gis_report = check_profile(&PFM_GIS_V1, &pfm_gis_evidence);

    let overall_pass = matches!(oltp_report.status, ProfileStatus::Pass)
        && matches!(pfm_core_report.status, ProfileStatus::Pass);

    print_json(&json!({
        "status": if overall_pass { "healthy" } else { "unhealthy" },
        "connection": connection,
        "capabilities": capabilities,
        "profiles": {
            "APPLICATION_OLTP_V1": oltp_report,
            "PFM_CORE_V1": pfm_core_report,
            "PFM_GIS_V1": pfm_gis_report,
        }
    }))?;
    // Fix review #10: exit code non-zero se logicamente unhealthy,
    // così `doctor` è affidabile in CI (`sh -c 'plenora-db-cli doctor ... || fail'`).
    if overall_pass {
        Ok(())
    } else {
        Err(CliError::Silent)
    }
}

pub(crate) async fn execute_ddl_cmd(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let sql = args.next().ok_or("manca lo statement SQL")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let cancel = CancellationToken::new();

    Provider::execute_ddl(&provider, &secret, &sql, &cancel).await?;
    print_json(&json!({
        "status": "ok",
        "operation": "execute_ddl",
    }))
}

pub(crate) async fn execute_sql_cmd(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    // Estrae --param VALUE:TYPE (bind positional in ordine).
    let collected: Vec<String> = args.by_ref().collect();
    let (rest, params) = crate::typed_params::strip_bind_params(collected)?;
    let mut rest_iter = rest.into_iter();
    let dsn_env = rest_iter.next().ok_or("manca variabile ambiente DSN")?;
    let sql = rest_iter.next().ok_or("manca lo statement SQL")?;
    ensure_end(&mut rest_iter)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let cancel = CancellationToken::new();
    let budget = pfm_budget()?;

    let opts = TransactionOptions {
        context: crate::session_ctx::active(),
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(&secret, &opts, &budget, &cancel)
        .await?;

    // Euristica: se lo statement inizia con SELECT/WITH/VALUES/TABLE → query,
    // altrimenti execute (rows affected).
    let head = sql
        .trim_start()
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase();
    let stmt = Statement::new(sql.clone()).with_params(params.into_inner());
    let payload = match head.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "SHOW" => {
            let rows = tx.query(&stmt, &cancel).await?;
            let out: Vec<_> = rows
                .iter()
                .map(|r| {
                    let mut obj = serde_json::Map::new();
                    for (i, col) in r.columns().iter().enumerate() {
                        let value = r
                            .get_index(i)
                            .and_then(|v| serde_json::to_value(v).ok())
                            .unwrap_or(serde_json::Value::Null);
                        obj.insert(col.clone(), value);
                    }
                    serde_json::Value::Object(obj)
                })
                .collect();
            json!({ "kind": "rows", "rows": out, "count": rows.len() })
        }
        _ => {
            let affected = tx.execute(&stmt, &cancel).await?;
            json!({ "kind": "affected_rows", "count": affected })
        }
    };
    let commit = tx.commit(&cancel).await?;
    print_json(&json!({
        "status": "ok",
        "commit": commit,
        "result": payload,
    }))
}

pub(crate) async fn transaction_test(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let cancel = CancellationToken::new();
    let budget = pfm_budget()?;

    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    let steps = vec![
        ("begin", "ok".to_owned()),
        (
            "select_one",
            format!(
                "affected={}",
                tx.execute(&Statement::new("SELECT 1"), &cancel).await?
            ),
        ),
        (
            "savepoint",
            match tx.savepoint("smoke", &cancel).await {
                Ok(()) => "ok".to_owned(),
                Err(e) => format!("fail: {}", e.message),
            },
        ),
        (
            "release_savepoint",
            match tx.release_savepoint("smoke", &cancel).await {
                Ok(()) => "ok".to_owned(),
                Err(e) => format!("fail: {}", e.message),
            },
        ),
    ];
    let commit = tx.commit(&cancel).await?;
    print_json(&json!({
        "status": "ok",
        "steps": steps.into_iter().map(|(k, v)| json!({ "step": k, "result": v })).collect::<Vec<_>>(),
        "commit": commit,
    }))
}

pub(crate) async fn session_context_test(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let cancel = CancellationToken::new();
    let budget = pfm_budget()?;

    let mut ctx = SessionContext::new();
    ctx.insert(
        "app.leak_probe",
        SessionEntry::public(SessionValue::Text("tx1-marker".into())),
    )?;
    let opts_with = TransactionOptions {
        context: ctx,
        ..TransactionOptions::default()
    };

    // tx1 con context → leggo current_setting → deve essere "tx1-marker"
    let mut tx1 = provider
        .begin_transaction(&secret, &opts_with, &budget, &cancel)
        .await?;
    let inside = execute_scalar_string(
        tx1.as_mut(),
        &Statement::new("SELECT current_setting('app.leak_probe', true)"),
        &cancel,
    )
    .await?;
    tx1.commit(&cancel).await?;

    // tx2 senza context sulla connessione riusata dal pool → deve essere ""
    let mut tx2 = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    let after = execute_scalar_string(
        tx2.as_mut(),
        &Statement::new("SELECT current_setting('app.leak_probe', true)"),
        &cancel,
    )
    .await?;
    tx2.rollback(&cancel).await?;

    let leak_free = after.is_empty();
    print_json(&json!({
        "status": if leak_free { "ok" } else { "leaked" },
        "context_inside_tx1": inside,
        "context_after_commit": after,
        "leak_free": leak_free,
    }))?;
    // Fix review #10: exit code non-zero se leak rilevato.
    if leak_free {
        Ok(())
    } else {
        Err(CliError::Silent)
    }
}

// ============================================================================
//  Fase 4: CLI arricchita — profile catalog, test avanzati, benchmark.
// ============================================================================

