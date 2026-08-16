//! Sottocomando `diagnose`: superset di `doctor` con dettagli operativi
//! (config `PostgreSQL` rilevanti, capability negative + suggerimenti fix,
//! versione estensioni `PostGIS`/`pg_stat_statements`, latenza connessione).

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliResult};
use plenora_database_core::conformance::{
    check_profile, probe_application_oltp_v1, probe_pfm_core_v1, probe_pfm_gis_v1, Capability,
    CapabilityEvidence, EvidenceKind, ProfileStatus, APPLICATION_OLTP_V1, PFM_CORE_V1, PFM_GIS_V1,
};
use plenora_database_core::provider::{ParameterValue, Provider};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use serde_json::{json, Value};

pub(crate) async fn diagnose(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let connect_start = std::time::Instant::now();
    let connection = match provider.test_connection(&secret, &cancel).await {
        Ok(info) => json!({
            "status": "ok",
            "connect_ms": connect_start.elapsed().as_millis(),
            "provider": info.provider,
            "server_version": info.server_version,
            "connection_identity": info.connection_identity,
        }),
        Err(e) => json!({
            "status": "fail",
            "connect_ms": connect_start.elapsed().as_millis(),
            "error": e,
        }),
    };

    let capabilities = match provider.probe_capabilities(&secret, &cancel).await {
        Ok(caps) => serde_json::to_value(&caps).unwrap_or(Value::Null),
        Err(e) => json!({ "error": e }),
    };

    // Server config rilevante (best-effort: se il ruolo non ha grants, i valori
    // vengono nulli senza far fallire diagnose).
    let server_config = match probe_server_config(&provider, &secret, &budget, &cancel).await {
        Ok(v) => v,
        Err(msg) => json!({ "status": "unavailable", "reason": msg }),
    };

    let oltp_evidence = probe_application_oltp_v1(&provider, &secret, &cancel).await;
    let oltp_report = check_profile(&APPLICATION_OLTP_V1, &oltp_evidence);
    let pfm_core_evidence = probe_pfm_core_v1(&provider, &secret, &cancel).await;
    let pfm_core_report = check_profile(&PFM_CORE_V1, &pfm_core_evidence);
    let pfm_gis_evidence = probe_pfm_gis_v1(&provider, &secret, &cancel).await;
    let pfm_gis_report = check_profile(&PFM_GIS_V1, &pfm_gis_evidence);

    let mut findings: Vec<Value> = Vec::new();
    collect_findings("APPLICATION_OLTP_V1", &oltp_evidence, &mut findings);
    collect_findings("PFM_CORE_V1", &pfm_core_evidence, &mut findings);
    collect_findings("PFM_GIS_V1", &pfm_gis_evidence, &mut findings);

    let overall_pass = matches!(oltp_report.status, ProfileStatus::Pass)
        && matches!(pfm_core_report.status, ProfileStatus::Pass);

    print_json(&json!({
        "status": if overall_pass { "healthy" } else { "unhealthy" },
        "connection": connection,
        "capabilities": capabilities,
        "server_config": server_config,
        "profiles": {
            "APPLICATION_OLTP_V1": oltp_report,
            "PFM_CORE_V1": pfm_core_report,
            "PFM_GIS_V1": pfm_gis_report,
        },
        "findings": findings,
    }))?;
    // Fix review #10.
    if overall_pass {
        Ok(())
    } else {
        Err(crate::CliError::Silent)
    }
}

async fn probe_server_config(
    provider: &plenora_db_postgres::PostgresProvider,
    secret: &plenora_database_core::provider::SecretString,
    budget: &plenora_database_core::resource::ResourceBudget,
    cancel: &CancellationToken,
) -> Result<Value, String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| e.message)?;
    let rows = tx
        .query(
            &Statement::new(
                "SELECT name, setting, unit \
                 FROM pg_settings \
                 WHERE name IN ( \
                   'shared_buffers','max_connections','work_mem','effective_cache_size', \
                   'wal_level','max_wal_size','checkpoint_timeout','random_page_cost', \
                   'statement_timeout','idle_in_transaction_session_timeout', \
                   'default_transaction_isolation','timezone','server_encoding') \
                 ORDER BY name",
            ),
            cancel,
        )
        .await
        .map_err(|e| e.message)?;
    let _ = tx.rollback(cancel).await;

    let mut out = serde_json::Map::new();
    for row in &rows {
        let name = match row.get_index(0) {
            Some(ParameterValue::String(s)) => s.clone(),
            _ => continue,
        };
        let setting = match row.get_index(1) {
            Some(ParameterValue::String(s)) => Value::String(s.clone()),
            _ => Value::Null,
        };
        let unit = match row.get_index(2) {
            Some(ParameterValue::String(s)) if !s.is_empty() => Value::String(s.clone()),
            _ => Value::Null,
        };
        out.insert(name, json!({ "value": setting, "unit": unit }));
    }
    Ok(Value::Object(out))
}

fn collect_findings(profile: &str, evidence: &[CapabilityEvidence], out: &mut Vec<Value>) {
    for e in evidence {
        if !matches!(e.kind, EvidenceKind::Failed) {
            continue;
        }
        let suggestion = suggestion_for(e.capability);
        out.push(json!({
            "profile": profile,
            "capability": e.capability,
            "reason": e.notes.clone(),
            "suggestion": suggestion,
        }));
    }
}

const fn suggestion_for(cap: Capability) -> &'static str {
    match cap {
        Capability::SpatialGeometryRead
        | Capability::SpatialGeometryWrite
        | Capability::SpatialWkbRoundtrip
        | Capability::SpatialSridPreservation
        | Capability::SpatialBbox
        | Capability::SpatialIntersects
        | Capability::SpatialContains
        | Capability::SpatialWithin
        | Capability::SpatialDistance
        | Capability::SpatialDWithin
        | Capability::SpatialCentroid
        | Capability::SpatialEnvelope
        | Capability::SpatialNearest
        | Capability::SpatialCrossSridPolicy => "CREATE EXTENSION postgis; e ricontrollare",
        Capability::SpatialIndexAvailable => "CREATE INDEX ... USING GIST(geom) sui layer PFM",
        Capability::SpatialInvalidGeometryRejected
        | Capability::SpatialNullGeometryHandled
        | Capability::SpatialLargeGeometryStreaming => {
            "verifica versione PostGIS ≥ 3.0 e permessi ruolo su ST_* / geometry_columns"
        }
        Capability::OptimisticConcurrency
        | Capability::Savepoints
        | Capability::Transactions
        | Capability::SessionContext
        | Capability::PoolContextLeakageIsolation => {
            "il probe usa TEMP TABLE + SET LOCAL: verificare permessi ruolo e log server"
        }
        Capability::UuidRoundtrip => "assicurarsi che extension uuid-ossp o gen_random_uuid() sia disponibile",
        Capability::DeadlockClassification | Capability::SerializationFailureClassification => {
            "verificare che il ruolo veda pg_locks; nessuna azione richiesta se il probe passa altrove"
        }
        Capability::StatementTimeout => "SET statement_timeout richiede permesso al ruolo (default: ok)",
        Capability::SchemaInspection => {
            "GRANT SELECT su pg_class/pg_namespace al ruolo o usare un ruolo con più privilegi"
        }
        Capability::Cancellation => "richiede TCP keepalive e permesso pg_cancel_backend",
        _ => "vedere documentazione capability nel manuale plenora-database",
    }
}
