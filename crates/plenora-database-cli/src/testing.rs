//! Sottocomandi test: profile-list, test-cancellation, test-streaming,
//! test-spatial.

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliResult};
use plenora_database_core::conformance::{APPLICATION_OLTP_V1, PFM_CORE_V1, PFM_GIS_V1};
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::portable::{
    compile_portable, select as p_select, spatial as p_spatial,
};
use plenora_database_core::provider::Provider;
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory, SpatialPredicate, SpatialReference};
use serde_json::json;

pub(crate) fn profile_list(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    ensure_end(args)?;
    print_json(&json!({
        "profiles": [
            {
                "name": APPLICATION_OLTP_V1.name,
                "required_count": APPLICATION_OLTP_V1.required.len(),
                "required": APPLICATION_OLTP_V1.required,
            },
            {
                "name": PFM_CORE_V1.name,
                "required_count": PFM_CORE_V1.required.len(),
                "required": PFM_CORE_V1.required,
            },
            {
                "name": PFM_GIS_V1.name,
                "required_count": PFM_GIS_V1.required.len(),
                "required": PFM_GIS_V1.required,
            },
        ]
    }))
}

pub(crate) async fn test_cancellation(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    // Verifica statement_timeout: SET LOCAL statement_timeout=50ms + pg_sleep(2)
    // → deve tornare errore Cancelled entro il timeout, non timeout della CLI.
    let opts = TransactionOptions {
        statement_timeout_ms: Some(50),
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(&secret, &opts, &budget, &cancel)
        .await?;
    let start = std::time::Instant::now();
    let outcome = tx.execute(&Statement::new("SELECT pg_sleep(2)"), &cancel).await;
    let elapsed_ms = start.elapsed().as_millis();
    let _ = tx.rollback(&cancel).await;

    let (status, category, message) = match outcome {
        Ok(_) => (
            "unexpected_ok",
            String::new(),
            "pg_sleep(2) ha completato senza cancel".to_owned(),
        ),
        Err(e) => (
            if e.category == ErrorCategory::Cancelled {
                "ok"
            } else {
                "wrong_category"
            },
            format!("{:?}", e.category),
            e.message,
        ),
    };
    print_json(&json!({
        "status": status,
        "elapsed_ms": elapsed_ms,
        "error_category": category,
        "message": message,
    }))
}

pub(crate) async fn test_streaming(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let row_count: u32 = args
        .next()
        .map(|s| s.parse().map_err(|_| "row_count non valido".to_owned()))
        .transpose()?
        .unwrap_or(500);
    let batch_size: u32 = args
        .next()
        .map(|s| s.parse().map_err(|_| "batch_size non valido".to_owned()))
        .transpose()?
        .unwrap_or(100);
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    let stmt =
        Statement::new(format!("SELECT gs::BIGINT FROM generate_series(1, {row_count}) gs"));

    let start = std::time::Instant::now();
    let mut stream = tx.query_stream(&stmt, batch_size, &cancel).await?;
    let mut total = 0_u32;
    let mut batches = 0_u32;
    while let Some(batch) = stream.next_batch(&cancel).await? {
        total = total.saturating_add(u32::try_from(batch.len()).unwrap_or(u32::MAX));
        batches = batches.saturating_add(1);
    }
    let elapsed_ms = start.elapsed().as_millis();
    drop(stream);
    tx.rollback(&cancel).await?;

    print_json(&json!({
        "status": if total == row_count { "ok" } else { "row_count_mismatch" },
        "expected_rows": row_count,
        "actual_rows": total,
        "batches": batches,
        "batch_size": batch_size,
        "elapsed_ms": elapsed_ms,
    }))
}

pub(crate) async fn test_spatial(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    // Test end-to-end del portable spatial AST contro PostGIS.
    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE cli_spatial_probe (id INT, geom geometry(Point, 4326)) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await?;
    tx.execute(
        &Statement::new(
            "INSERT INTO cli_spatial_probe VALUES \
             (1, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326)), \
             (2, ST_SetSRID(ST_MakePoint(12.49, 41.90), 4326)), \
             (3, ST_SetSRID(ST_MakePoint(2.35, 48.86), 4326))",
        ),
        &cancel,
    )
    .await?;

    // Recupera bbox via query
    let bbox_row = tx
        .query(
            &Statement::new(
                "SELECT ST_AsEWKB(ST_SetSRID(ST_MakeEnvelope(6.0, 40.0, 14.0, 46.0), 4326))",
            ),
            &cancel,
        )
        .await?;
    let ewkb = match bbox_row.first().and_then(|r| r.get_index(0)) {
        Some(plenora_database_core::provider::ParameterValue::Bytes(b)) => b.clone(),
        _ => return Err("bbox EWKB non decodificato".into()),
    };

    // Query portable spatial: WHERE ST_Intersects(geom, bbox)
    let ast = p_select("cli_spatial_probe", vec!["id"])
        .where_(p_spatial(
            "geom",
            SpatialPredicate::Intersects,
            SpatialReference {
                ewkb,
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geometry,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &ast)?;
    let start = std::time::Instant::now();
    let rows = tx.query(&compiled, &cancel).await?;
    let elapsed_ms = start.elapsed().as_millis();
    let _ = tx.rollback(&cancel).await;

    let ids: Vec<i32> = rows
        .iter()
        .filter_map(|r| match r.get_index(0) {
            Some(plenora_database_core::provider::ParameterValue::I32(v)) => Some(*v),
            _ => None,
        })
        .collect();
    print_json(&json!({
        "status": if ids == vec![1, 2] || ids == vec![2, 1] { "ok" } else { "unexpected_ids" },
        "expected_ids_any_order": [1, 2],
        "actual_ids": ids,
        "elapsed_ms": elapsed_ms,
    }))
}
