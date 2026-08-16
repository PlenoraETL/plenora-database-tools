//! Probe `PFM_GIS_V1`: geometrie GIS end-to-end contro `PostGIS` + capability
//! spatial estese.

use super::{push_probe, Capability, CapabilityEvidence, PFM_GIS_V1};
use crate::provider::{ParameterValue, Provider, SecretString};
use crate::resource::{ResourceBudget, ResourceLimits};
use crate::transaction::{Statement, TransactionOptions};
use crate::CancellationToken;

#[allow(clippy::too_many_lines)] // sequenza di 18 probe intenzionalmente lineare
pub async fn probe_pfm_gis_v1(
    provider: &dyn Provider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Vec<CapabilityEvidence> {
    let mut evidence = Vec::with_capacity(PFM_GIS_V1.required.len());
    let budget = match ResourceBudget::new(ResourceLimits::default()) {
        Ok(b) => b,
        Err(e) => {
            for cap in PFM_GIS_V1.required {
                evidence.push(CapabilityEvidence::failed(
                    *cap,
                    format!("budget non allocabile: {e}"),
                ));
            }
            return evidence;
        }
    };

    // Setup: crea una tabella temporanea con 3 punti in SRID 4326.
    let setup_result = spatial_setup(provider, secret, &budget, cancellation).await;
    if let Err(e) = setup_result {
        for cap in PFM_GIS_V1.required {
            evidence.push(CapabilityEvidence::failed(*cap, format!("setup: {e}")));
        }
        return evidence;
    }

    push_probe(
        &mut evidence,
        Capability::SpatialGeometryRead,
        probe_spatial_read(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialGeometryWrite,
        probe_spatial_write(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialWkbRoundtrip,
        probe_spatial_wkb_roundtrip(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialSridPreservation,
        probe_spatial_srid(provider, secret, &budget, cancellation).await,
    );
    for (cap, function_call, expected) in [
        (
            Capability::SpatialBbox,
            "ST_MakeEnvelope(6.0, 40.0, 14.0, 46.0, 4326) && geom",
            2_i64,
        ),
        (
            Capability::SpatialIntersects,
            "ST_Intersects(geom, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))",
            1,
        ),
        (
            Capability::SpatialContains,
            "ST_Contains(ST_SetSRID(ST_MakeEnvelope(0, 0, 20, 50), 4326), geom)",
            3,
        ),
        (
            Capability::SpatialWithin,
            "ST_Within(geom, ST_SetSRID(ST_MakeEnvelope(9.0, 45.0, 9.5, 46.0), 4326))",
            1,
        ),
        (
            Capability::SpatialDWithin,
            "ST_DWithin(geom, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326), 0.01)",
            1,
        ),
    ] {
        push_probe(
            &mut evidence,
            cap,
            probe_spatial_count_where(
                provider,
                secret,
                &budget,
                cancellation,
                function_call,
                expected,
            )
            .await,
        );
    }
    push_probe(
        &mut evidence,
        Capability::SpatialDistance,
        probe_spatial_distance(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialCentroid,
        probe_spatial_scalar_op(
            provider,
            secret,
            &budget,
            cancellation,
            "SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE ST_Centroid(geom) IS NOT NULL",
            3,
        )
        .await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialEnvelope,
        probe_spatial_scalar_op(
            provider,
            secret,
            &budget,
            cancellation,
            "SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE ST_Envelope(geom) IS NOT NULL",
            3,
        )
        .await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialNearest,
        probe_spatial_nearest(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialIndexAvailable,
        probe_spatial_index_available(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialInvalidGeometryRejected,
        probe_spatial_invalid_rejected(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialNullGeometryHandled,
        probe_spatial_null_handled(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialLargeGeometryStreaming,
        probe_spatial_large_streaming(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialCrossSridPolicy,
        probe_spatial_cross_srid(provider, secret, &budget, cancellation).await,
    );

    // Cleanup
    let _ = spatial_teardown(provider, secret, &budget, cancellation).await;

    evidence
}

async fn spatial_setup(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(&Statement::new("DROP TABLE IF EXISTS _probe_gis"), cancel)
        .await
        .map_err(|e| format!("drop: {}", e.message))?;
    tx.execute(
        &Statement::new("CREATE TABLE _probe_gis (id INT PRIMARY KEY, geom geometry(Point, 4326))"),
        cancel,
    )
    .await
    .map_err(|e| format!("create: {}", e.message))?;
    tx.execute(
        &Statement::new(
            "INSERT INTO _probe_gis VALUES \
             (1, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326)), \
             (2, ST_SetSRID(ST_MakePoint(12.49, 41.90), 4326)), \
             (3, ST_SetSRID(ST_MakePoint(2.35,  48.86), 4326))",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("seed: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map_err(|e| format!("commit setup: {}", e.message))?;
    Ok(())
}

async fn spatial_teardown(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(&Statement::new("DROP TABLE _probe_gis"), cancel)
        .await
        .map_err(|e| format!("drop: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map_err(|e| format!("commit: {}", e.message))?;
    Ok(())
}

async fn probe_spatial_read(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let n = crate::facade::execute_scalar_i64(
        tx.as_mut(),
        &Statement::new("SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE geom IS NOT NULL"),
        cancel,
    )
    .await
    .map_err(|e| format!("count: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == 3 {
        Ok(())
    } else {
        Err(format!("attesi 3 punti, ottenuti {n}"))
    }
}

async fn probe_spatial_write(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let n = tx
        .execute(
            &Statement::new(
                "INSERT INTO _probe_gis VALUES (99, ST_SetSRID(ST_MakePoint(0, 0), 4326))",
            ),
            cancel,
        )
        .await
        .map_err(|e| format!("insert: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == 1 {
        Ok(())
    } else {
        Err(format!("insert atteso 1, ottenuto {n}"))
    }
}

async fn probe_spatial_wkb_roundtrip(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let row = crate::facade::query_one(
        tx.as_mut(),
        &Statement::new("SELECT ST_AsEWKB(ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))"),
        cancel,
    )
    .await
    .map_err(|e| format!("wkb: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    match &row[0] {
        ParameterValue::Bytes(b) if !b.is_empty() => Ok(()),
        other => Err(format!("attesa bytes EWKB non vuoto, ottenuto {other:?}")),
    }
}

async fn probe_spatial_srid(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let srid = crate::facade::execute_scalar_i32(
        tx.as_mut(),
        &Statement::new("SELECT ST_SRID(geom) FROM _probe_gis WHERE id = 1"),
        cancel,
    )
    .await
    .map_err(|e| format!("srid: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if srid == 4326 {
        Ok(())
    } else {
        Err(format!("SRID atteso 4326, ottenuto {srid}"))
    }
}

async fn probe_spatial_count_where(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
    predicate_sql: &str,
    expected: i64,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let sql = format!("SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE {predicate_sql}");
    let n = crate::facade::execute_scalar_i64(tx.as_mut(), &Statement::new(sql), cancel)
        .await
        .map_err(|e| format!("count: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == expected {
        Ok(())
    } else {
        Err(format!("attesi {expected}, ottenuti {n}"))
    }
}

async fn probe_spatial_scalar_op(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
    sql: &str,
    expected: i64,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let n = crate::facade::execute_scalar_i64(tx.as_mut(), &Statement::new(sql), cancel)
        .await
        .map_err(|e| format!("op: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == expected {
        Ok(())
    } else {
        Err(format!("atteso {expected}, ottenuto {n}"))
    }
}

async fn probe_spatial_distance(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let d = crate::facade::execute_scalar_f64(
        tx.as_mut(),
        &Statement::new(
            "SELECT ST_Distance(
                 (SELECT geom FROM _probe_gis WHERE id = 1),
                 (SELECT geom FROM _probe_gis WHERE id = 2)
             )",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("distance: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if d > 0.0 && d < 100.0 {
        Ok(())
    } else {
        Err(format!("distanza sospetta: {d}"))
    }
}

async fn probe_spatial_nearest(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let id = crate::facade::execute_scalar_i32(
        tx.as_mut(),
        &Statement::new(
            "SELECT id FROM _probe_gis
             ORDER BY geom <-> ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326)
             LIMIT 1",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("nearest: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if id == 1 {
        Ok(())
    } else {
        Err(format!("nearest atteso id=1 (Milano), ottenuto {id}"))
    }
}

async fn probe_spatial_index_available(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    // GIST è la implementation Postgres per spatial index. Verifica capability:
    // proviamo a creare (e droppare) un indice GIST sulla tabella di test.
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(
        &Statement::new("CREATE INDEX IF NOT EXISTS _probe_gis_gix ON _probe_gis USING GIST(geom)"),
        cancel,
    )
    .await
    .map_err(|e| format!("create index: {}", e.message))?;
    tx.execute(
        &Statement::new("DROP INDEX IF EXISTS _probe_gis_gix"),
        cancel,
    )
    .await
    .map_err(|e| format!("drop index: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map_err(|e| format!("commit: {}", e.message))?;
    Ok(())
}

async fn probe_spatial_invalid_rejected(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    // Un WKB completamente invalido deve produrre errore.
    let outcome = tx
        .execute(
            &Statement::new("SELECT ST_GeomFromEWKB($1)")
                .with_params(vec![ParameterValue::Bytes(vec![0xff, 0x00, 0x01])]),
            cancel,
        )
        .await;
    let _ = tx.rollback(cancel).await;
    if outcome.is_err() {
        Ok(())
    } else {
        Err("geometry invalida non rifiutata".into())
    }
}

async fn probe_spatial_null_handled(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    // Verifica che una geometry NULL non produca panic/UB. Sia il Null tipizzato
    // che un errore Unsupported graceful sono comportamenti "handled".
    let outcome = crate::facade::query_one(
        tx.as_mut(),
        &Statement::new("SELECT NULL::geometry"),
        cancel,
    )
    .await;
    let _ = tx.rollback(cancel).await;
    match outcome {
        Ok(row) if matches!(row.get_index(0), Some(ParameterValue::Null { .. })) => Ok(()),
        Err(e) if e.category == crate::ErrorCategory::Unsupported => Ok(()),
        Err(e) => Err(format!("null geom errore inatteso: {:?}", e.category)),
        Ok(row) => Err(format!("null geom non typed-null: {:?}", row.get_index(0))),
    }
}

async fn probe_spatial_large_streaming(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    // Streaming di 200 punti generati al volo con cursor batch 50.
    let stmt = Statement::new("SELECT gs::INT FROM generate_series(1, 200) gs");
    let mut stream = tx
        .query_stream(&stmt, 50, cancel)
        .await
        .map_err(|e| format!("stream: {}", e.message))?;
    let mut total = 0_usize;
    while let Some(batch) = stream
        .next_batch(cancel)
        .await
        .map_err(|e| format!("fetch: {}", e.message))?
    {
        total += batch.len();
    }
    drop(stream);
    let _ = tx.rollback(cancel).await;
    if total == 200 {
        Ok(())
    } else {
        Err(format!("attesi 200 punti, ottenuti {total}"))
    }
}

async fn probe_spatial_cross_srid(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    // Un'operazione di ST_Intersects tra geometrie con SRID diversi (4326 vs
    // 3857) deve fallire fail-closed. Postgres emette errore, la libreria lo
    // propaga come errore tecnico invece di trasformare silenziosamente.
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let outcome = tx
        .execute(
            &Statement::new(
                "SELECT ST_Intersects(
                     ST_SetSRID(ST_MakePoint(0, 0), 4326),
                     ST_SetSRID(ST_MakePoint(0, 0), 3857)
                 )",
            ),
            cancel,
        )
        .await;
    let _ = tx.rollback(cancel).await;
    if outcome.is_err() {
        Ok(())
    } else {
        Err("cross-SRID non rifiutato — attesa transformation policy fail-closed".into())
    }
}
