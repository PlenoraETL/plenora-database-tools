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
use plenora_db_postgres::{PostgresProvider, PostgresTlsConfig, PostgresTlsMode};
use serde_json::json;

/// Helper storico per test / call sites legacy non ancora migrati
/// al `PostgresCommandContext`. Post ADR-011, riflette il nuovo
/// default `Require`. Per test/dev locali usare `insecure_local()`
/// esplicitamente.
/// Variabili che indicano i *percorsi* del materiale TLS con cui il provider
/// verifica il server — e, quando il server lo esige, si identifica.
///
/// Stessa convenzione di `database-probe` e di `PLENORA_MYSQL_CA` nei gate: il
/// valore e un percorso, non il PEM.
pub(crate) const POSTGRES_CA_PATH_ENV: &str = "PLENORA_PG_CA_PATH";
pub(crate) const POSTGRES_CLIENT_CERT_PATH_ENV: &str = "PLENORA_PG_CLIENT_CERT_PATH";
pub(crate) const POSTGRES_CLIENT_KEY_PATH_ENV: &str = "PLENORA_PG_CLIENT_KEY_PATH";

/// Interruttore dev/test pre-esistente: disattiva TLS del tutto.
pub(crate) const POSTGRES_INSECURE_LOCAL_ENV: &str = "PLENORA_TLS_INSECURE_LOCAL";

/// Il provider `PostgreSQL` di **tutti** i sottocomandi.
///
/// Prima esistevano quattro sorgenti dello stesso provider — due
/// `PostgresProvider::default()` in `pfm.rs` e `context.rs`, e tre siti in
/// `main.rs` che onoravano `PLENORA_TLS_INSECURE_LOCAL` — con il risultato che
/// alcuni comandi potevano parlare con un riferimento di test e altri no,
/// nello stesso binario. Le sorgenti divergono; questa e l'unica.
///
/// Tre configurazioni, in ordine di precedenza:
///
/// 1. `PLENORA_TLS_INSECURE_LOCAL` impostata — TLS disattivato. Interruttore
///    dev/test gia esistente, semantica invariata: il nome dice il rischio.
/// 2. `PLENORA_PG_CA_PATH` impostata — TLS obbligatorio e verifica attiva
///    contro quella CA invece dei root pubblici `WebPKI`. Con
///    `PLENORA_PG_CLIENT_CERT_PATH` e `PLENORA_PG_CLIENT_KEY_PATH` si aggiunge
///    l'identita client, per i server che richiedono `clientcert`.
/// 3. niente — `default()`: TLS obbligatorio, root pubblici. ADR-011.
///
/// Il caso (2) non e un opt-out: la verifica resta piena, cambia la radice di
/// fiducia. Un riferimento con certificato privato e la norma nei test e negli
/// ambienti interni.
///
/// # Errors
///
/// Fallisce quando una variabile indica materiale illeggibile o non valido, e
/// quando certificato e chiave client non sono forniti insieme.
pub(crate) fn postgres_provider_for_pfm() -> CliResult<PostgresProvider> {
    if std::env::var_os(POSTGRES_INSECURE_LOCAL_ENV).is_some() {
        return Ok(PostgresProvider::insecure_local());
    }
    let Some(ca) = crate::prepare_private_ca_material(Some(POSTGRES_CA_PATH_ENV))? else {
        return Ok(PostgresProvider::default());
    };
    let certificate = crate::tls_material_from_environment(POSTGRES_CLIENT_CERT_PATH_ENV)?;
    let key = crate::tls_material_from_environment(POSTGRES_CLIENT_KEY_PATH_ENV)?;
    let tls_config = match (certificate, key) {
        (None, None) => PostgresTlsConfig::private_ca_pem(&ca)?,
        (Some(certificate), Some(key)) => {
            PostgresTlsConfig::private_ca_with_client_identity_pem(&ca, &certificate, &key)?
        }
        _ => {
            return Err(format!(
                "l'identita client TLS richiede {POSTGRES_CLIENT_CERT_PATH_ENV} e \
                 {POSTGRES_CLIENT_KEY_PATH_ENV} insieme"
            )
            .into());
        }
    };
    Ok(PostgresProvider::default()
        .with_tls_mode(PostgresTlsMode::Require)
        .with_tls_config(tls_config))
}

pub(crate) fn pfm_budget() -> CliResult<ResourceBudget> {
    ResourceBudget::new(ResourceLimits::default()).map_err(CliError::from)
}

pub(crate) async fn profile_check(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let profile_name = args.next().ok_or("manca il nome del profilo")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm()?;
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
            return Err(
                "profilo sconosciuto (usa APPLICATION_OLTP_V1 | PFM_CORE_V1 | PFM_GIS_V1)".into(),
            )
        }
    };
    let report = check_profile(profile, &evidence);
    let is_pass = matches!(report.status, ProfileStatus::Pass);
    print_json(
        &serde_json::to_value(&report).map_err(|_| "report non serializzabile".to_owned())?,
    )?;
    // Fix review #10 residuo: profile-check ritornava sempre Ok, anche
    // per report Fail. Ora exit=1 se profilo non è Pass, così CI può
    // gate su risultato conformance.
    if is_pass {
        Ok(())
    } else {
        Err(CliError::Silent)
    }
}

pub(crate) async fn doctor(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm()?;
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

    // Fase C: PostgresCommandContext racchiude secret + provider +
    // cancel + budget con un unico costruttore nominato.
    let ctx = crate::context::PostgresCommandContext::for_pfm(&dsn_env)?;
    Provider::execute_ddl(&ctx.provider, &ctx.secret, &sql, &ctx.cancel).await?;
    print_json(&json!({
        "status": "ok",
        "operation": "execute_ddl",
    }))
}

pub(crate) async fn execute_sql_cmd(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    // Estrae --param VALUE:TYPE (bind positional in ordine).
    let collected: Vec<String> = args.by_ref().collect();
    let (rest, params) = crate::typed_params::strip_bind_params(collected)?;
    // CHG-003: `--allow-raw` è opt-in esplicito per SQL non-CRUD (DDL,
    // GRANT/REVOKE, ecc.). Senza il flag il policy è `Deny` (PFM
    // baseline): solo SELECT/INSERT/UPDATE/DELETE/WITH/VALUES/TABLE/MERGE.
    let mut rest_vec: Vec<String> = rest;
    let allow_raw = rest_vec
        .iter()
        .position(|arg| arg == "--allow-raw")
        .inspect(|&idx| {
            rest_vec.remove(idx);
        })
        .is_some();
    let mut rest_iter = rest_vec.into_iter();
    let dsn_env = rest_iter.next().ok_or("manca variabile ambiente DSN")?;
    let sql = rest_iter.next().ok_or("manca lo statement SQL")?;
    ensure_end(&mut rest_iter)?;

    let ctx = crate::context::PostgresCommandContext::for_pfm(&dsn_env)?;
    let opts = if allow_raw {
        TransactionOptions {
            context: crate::session_ctx::active(),
            ..TransactionOptions::default()
        }
    } else {
        ctx.pfm_transaction_options()
    };
    let mut tx = ctx
        .provider
        .begin_transaction(&ctx.secret, &opts, &ctx.budget, &ctx.cancel)
        .await?;

    // Euristica: se lo statement inizia con SELECT/WITH/VALUES/TABLE → query,
    // altrimenti execute (rows affected).
    //
    // Fix review: strip commenti prima di estrarre il keyword. Prima
    // uno statement come `-- audit\nSELECT ...` finiva nel ramo
    // `affected_rows` perché il classifier vedeva `--` (non alfabetico
    // → head vuoto → default match). Ora usa lo stesso comment-stripper
    // di `native_query_policy` per coerenza.
    let head = extract_statement_head(&sql);
    let stmt = Statement::new(sql.clone()).with_params(params.into_inner());
    let payload = match head.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "SHOW" => {
            let rows = tx.query(&stmt, &ctx.cancel).await?;
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
            let affected = tx.execute(&stmt, &ctx.cancel).await?;
            json!({ "kind": "affected_rows", "count": affected })
        }
    };
    let commit = tx.commit(&ctx.cancel).await?;
    print_json(&json!({
        "status": "ok",
        "commit": commit,
        "result": payload,
    }))
}

pub(crate) async fn transaction_test(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let ctx = crate::context::PostgresCommandContext::for_pfm(&dsn_env)?;
    // CHG-003: `pfm_transaction_options()` include `native_query_policy: Deny`.
    let mut tx = ctx
        .provider
        .begin_transaction(
            &ctx.secret,
            &ctx.pfm_transaction_options(),
            &ctx.budget,
            &ctx.cancel,
        )
        .await?;
    let savepoint_result = tx.savepoint("smoke", &ctx.cancel).await;
    let release_result = tx.release_savepoint("smoke", &ctx.cancel).await;
    let steps = vec![
        ("begin", "ok".to_owned()),
        (
            "select_one",
            format!(
                "affected={}",
                tx.execute(&Statement::new("SELECT 1"), &ctx.cancel).await?
            ),
        ),
        (
            "savepoint",
            match &savepoint_result {
                Ok(()) => "ok".to_owned(),
                Err(e) => format!("fail: {}", e.message),
            },
        ),
        (
            "release_savepoint",
            match &release_result {
                Ok(()) => "ok".to_owned(),
                Err(e) => format!("fail: {}", e.message),
            },
        ),
    ];
    // Fix review #10 residuo: prima "status: ok" era hardcoded anche
    // se savepoint o release_savepoint fallivano. Ora status derivato
    // dagli step reali; exit=1 se qualche step ha fallito.
    let all_steps_ok = savepoint_result.is_ok() && release_result.is_ok();
    let commit = tx.commit(&ctx.cancel).await?;
    print_json(&json!({
        "status": if all_steps_ok { "ok" } else { "fail" },
        "steps": steps.into_iter().map(|(k, v)| json!({ "step": k, "result": v })).collect::<Vec<_>>(),
        "commit": commit,
    }))?;
    if all_steps_ok {
        Ok(())
    } else {
        Err(CliError::Silent)
    }
}

pub(crate) async fn session_context_test(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let cmd_ctx = crate::context::PostgresCommandContext::for_pfm(&dsn_env)?;
    // Alias per ridurre diff col codice esistente. Le prossime iterazioni
    // possono sostituire questi shadow con `cmd_ctx.<field>` diretto.
    let secret = cmd_ctx.secret;
    let provider = cmd_ctx.provider;
    let cancel = cmd_ctx.cancel;
    let budget = cmd_ctx.budget;

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

/// Estrae il primo keyword SQL (uppercase ASCII).
///
/// Delega a `plenora_database_core::native_query_policy::statement_head`
/// per non duplicare il lexer di commenti che era già presente lì.
/// Fix review post-review (dedup #96).
fn extract_statement_head(sql: &str) -> String {
    plenora_database_core::native_query_policy::statement_head(sql)
}

// ============================================================================
//  Fase 4: CLI arricchita — profile catalog, test avanzati, benchmark.
// ============================================================================
