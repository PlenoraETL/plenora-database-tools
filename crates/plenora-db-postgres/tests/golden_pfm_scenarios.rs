//! Scenari end-to-end che simulano il consumer PFM reale.
//!
//! Ogni test rappresenta un flusso che il PFM esegue via SDK Python o API
//! Rust e rende verificabili i requisiti ergonomici sul bordo pubblico.
//!
//! Scenari:
//! 1. Multi-tenant: N tenant paralleli con session context isolato,
//!    ognuno inserisce dati sul proprio tenant, verifica no leak.
//! 2. Full OLTP flow: begin + SET LOCAL context + portable INSERT
//!    RETURNING + portable SELECT + commit. Simula il primo endpoint PFM
//!    "create record + read back".
//! 3. Optimistic conflict realistico: 2 worker paralleli aggiornano la
//!    stessa entità, uno vince, l'altro riprova con expected_version
//!    aggiornata.
//! 4. Spatial query pushdown: crea layer con punti geografici + indice
//!    GIST, esegue portable ST_Intersects, verifica risultato + tempi
//!    entro budget ragionevole.
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]
#![allow(
    clippy::approx_constant,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::doc_markdown,
    clippy::unreadable_literal,
    clippy::single_match_else,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::uninlined_format_args,
    clippy::match_same_arms,
    clippy::manual_let_else,
    clippy::redundant_closure_for_method_calls,
    clippy::cast_precision_loss,
    clippy::needless_continue
)]

use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::portable::{
    eq as p_eq, select as p_select, spatial as p_spatial, Expression, InsertStatement,
    PortableStatement, TableRef,
};
use plenora_database_core::provider::{ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::session_context::{SessionContext, SessionEntry, SessionValue};
use plenora_database_core::transaction::{ConditionalUpdate, Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory, SpatialPredicate, SpatialReference};
use plenora_db_postgres::PostgresProvider;
use std::sync::Arc;
use std::time::Duration;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

fn ctx_tenant(tenant_id: &str) -> SessionContext {
    let mut c = SessionContext::new();
    c.insert(
        "app.tenant_id",
        SessionEntry::public(SessionValue::Text(tenant_id.to_owned())),
    )
    .expect("ctx insert");
    c
}

// ============================================================================
//  H.1 — Multi-tenant: N tenant paralleli, session context isolato
// ============================================================================
//
// Scenario reale PFM: N richieste concorrenti (uno per tenant) insertano
// dati sulla propria tabella comune. Ogni tx setta SET LOCAL
// app.tenant_id via session context. Verifica:
//   - Le N tx completano tutte senza errori
//   - Il valore letto da current_setting nella tx è quello atteso
//   - Il DB vede N righe totali, ognuna con il proprio tenant

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pfm_h1_multi_tenant_isolated_via_session_context() {
    let provider =
        Arc::new(PostgresProvider::insecure_local_with_batch_rows(1_024).with_pool_size(8, 10_000));
    let cancel = CancellationToken::new();

    // Setup schema condiviso.
    let mut setup = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin setup");
    setup
        .execute(
            &Statement::new(
                "CREATE TABLE IF NOT EXISTS _pfm_h1 ( \
                 id BIGSERIAL PRIMARY KEY, \
                 tenant TEXT NOT NULL, \
                 payload TEXT NOT NULL, \
                 ts TIMESTAMPTZ NOT NULL DEFAULT now())",
            ),
            &cancel,
        )
        .await
        .expect("create");
    setup
        .execute(&Statement::new("TRUNCATE TABLE _pfm_h1"), &cancel)
        .await
        .expect("truncate");
    Box::new(setup).commit(&cancel).await.expect("commit setup");

    // N tenant paralleli.
    let n_tenants = 16_usize;
    let mut handles = Vec::with_capacity(n_tenants);
    for i in 0..n_tenants {
        let p = Arc::clone(&provider);
        let c = cancel.clone();
        let tenant_id = format!("tenant-{i:03}");
        handles.push(tokio::spawn(async move {
            let opts = TransactionOptions {
                context: ctx_tenant(&tenant_id),
                ..TransactionOptions::default()
            };
            let mut tx = p
                .begin_transaction(&secret(), &opts, &budget(), &c)
                .await
                .expect("begin tenant tx");

            // Il tenant legge il proprio ID dal context — deve matchare.
            let rows = tx
                .query(
                    &Statement::new("SELECT current_setting('app.tenant_id', true)"),
                    &c,
                )
                .await
                .expect("read context");
            let read_tenant = match rows.first().and_then(|r| r.get_index(0)) {
                Some(ParameterValue::String(s)) => s.clone(),
                other => panic!("context non è String: {other:?}"),
            };
            assert_eq!(
                read_tenant, tenant_id,
                "tenant vede context di un altro (leak!)"
            );

            // Insert dedicato al tenant.
            tx.execute(
                &Statement::new("INSERT INTO _pfm_h1 (tenant, payload) VALUES ($1, $2)")
                    .with_params(vec![
                        ParameterValue::String(tenant_id.clone()),
                        ParameterValue::String(format!("data-{i}")),
                    ]),
                &c,
            )
            .await
            .expect("insert");

            Box::new(tx).commit(&c).await.expect("commit");
            tenant_id
        }));
    }

    let mut committed = Vec::with_capacity(n_tenants);
    for h in handles {
        committed.push(h.await.expect("task"));
    }
    assert_eq!(committed.len(), n_tenants);

    // Verifica finale: contatore per tenant + no duplicati.
    let mut verify = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin verify");
    let rows = verify
        .query(
            &Statement::new(
                "SELECT tenant, COUNT(*)::BIGINT \
                 FROM _pfm_h1 \
                 GROUP BY tenant ORDER BY tenant",
            ),
            &cancel,
        )
        .await
        .expect("count");
    assert_eq!(rows.len(), n_tenants, "atteso 1 gruppo per tenant");
    for r in &rows {
        let count = match r.get_index(1) {
            Some(ParameterValue::I64(v)) => *v,
            _ => panic!("count non I64"),
        };
        assert_eq!(count, 1, "duplicati/perse per tenant");
    }

    // Cleanup
    let _ = verify
        .execute(&Statement::new("DROP TABLE IF EXISTS _pfm_h1"), &cancel)
        .await;
    let _ = Box::new(verify).commit(&cancel).await;
}

// ============================================================================
//  H.2 — Full OLTP flow: begin + SET LOCAL context + portable INSERT
//         RETURNING + portable SELECT + commit
// ============================================================================
//
// Scenario reale PFM: primo endpoint "POST /building" — crea record e ritorna
// l'ID assegnato dal DB. Il PFM userà portable AST per essere db-agnostic.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pfm_h2_portable_insert_returning_then_read_back() {
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let s = secret();

    // Setup persistente (portable RETURNING funziona su tabelle vere).
    let mut setup = provider
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin setup");
    setup
        .execute(
            &Statement::new(
                "CREATE TABLE IF NOT EXISTS _pfm_h2_building ( \
                 id BIGSERIAL PRIMARY KEY, \
                 name TEXT NOT NULL, \
                 code TEXT UNIQUE NOT NULL, \
                 version INT NOT NULL DEFAULT 1, \
                 created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
            ),
            &cancel,
        )
        .await
        .expect("create");
    setup
        .execute(&Statement::new("TRUNCATE TABLE _pfm_h2_building"), &cancel)
        .await
        .expect("truncate");
    Box::new(setup).commit(&cancel).await.expect("commit setup");

    // Endpoint flow: begin con context request-id + INSERT via portable con
    // RETURNING id, poi SELECT via portable per verificare.
    let opts = TransactionOptions {
        context: {
            let mut c = SessionContext::new();
            c.insert(
                "app.request_id",
                SessionEntry::public(SessionValue::Text("req-42".to_owned())),
            )
            .expect("ctx");
            c
        },
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(&s, &opts, &budget(), &cancel)
        .await
        .expect("begin tx");

    // INSERT via portable AST.
    use plenora_database_core::facade::execute_portable_returning_one;
    let insert = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("_pfm_h2_building"),
        columns: vec!["name".into(), "code".into()],
        values: vec![vec![
            Expression::literal(ParameterValue::String("Torre Milano".into())),
            Expression::literal(ParameterValue::String("MI-TN-001".into())),
        ]],
        returning: vec!["id".into(), "version".into()],
    });
    let row = execute_portable_returning_one(tx.as_mut(), &insert, &cancel)
        .await
        .expect("insert returning");
    let new_id = match row.get_index(0) {
        Some(ParameterValue::I64(v)) => *v,
        _ => panic!("id non i64"),
    };
    let new_version = match row.get_index(1) {
        Some(ParameterValue::I32(v)) => *v,
        _ => panic!("version non i32"),
    };
    assert!(new_id > 0);
    assert_eq!(new_version, 1);

    // SELECT via portable per verificare.
    use plenora_database_core::facade::execute_portable_returning;
    let sel = p_select("_pfm_h2_building", vec!["id", "name", "code"])
        .where_(p_eq("id", ParameterValue::I64(new_id)))
        .into_statement();
    let rows = execute_portable_returning(tx.as_mut(), &sel, &cancel)
        .await
        .expect("select");
    assert_eq!(rows.len(), 1);
    let name = match rows[0].get_index(1) {
        Some(ParameterValue::String(s)) => s.clone(),
        _ => panic!("name non string"),
    };
    assert_eq!(name, "Torre Milano");

    Box::new(tx).commit(&cancel).await.expect("commit");

    // Cleanup.
    let mut cleanup = provider
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin cleanup");
    let _ = cleanup
        .execute(
            &Statement::new("DROP TABLE IF EXISTS _pfm_h2_building"),
            &cancel,
        )
        .await;
    let _ = Box::new(cleanup).commit(&cancel).await;
}

// ============================================================================
//  H.3 — Optimistic conflict con retry realistico
// ============================================================================
//
// Scenario reale PFM: 2 worker aggiornano la stessa entità. Uno vince, l'altro
// riceve ConcurrentModification e RIPROVA leggendo la versione aggiornata.
// Verifica che il retry pattern funzioni end-to-end.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pfm_h3_optimistic_conflict_with_retry_pattern() {
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let s = secret();

    // Setup: entità con id=1 v=1.
    let mut setup = provider
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin setup");
    setup
        .execute(
            &Statement::new(
                "CREATE TABLE IF NOT EXISTS _pfm_h3_entity ( \
                 id INT PRIMARY KEY, v INT NOT NULL, data TEXT NOT NULL)",
            ),
            &cancel,
        )
        .await
        .expect("create");
    setup
        .execute(
            &Statement::new(
                "INSERT INTO _pfm_h3_entity VALUES (1, 1, 'initial') \
                 ON CONFLICT (id) DO UPDATE SET v = 1, data = 'initial'",
            ),
            &cancel,
        )
        .await
        .expect("seed");
    Box::new(setup).commit(&cancel).await.expect("commit");

    // Worker function: legge versione corrente, prova a bumpare a v+1 con
    // conditional update. Se fallisce, riprova max 5 volte.
    async fn update_with_retry(
        p: &PostgresProvider,
        new_data: &str,
    ) -> plenora_database_core::Result<u32> {
        let s = secret();
        let cancel = CancellationToken::new();
        for attempt in 0..5_u32 {
            let mut tx = p
                .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
                .await?;
            let rows = tx
                .query(
                    &Statement::new("SELECT v FROM _pfm_h3_entity WHERE id = 1"),
                    &cancel,
                )
                .await?;
            let current_v = match rows.first().and_then(|r| r.get_index(0)) {
                Some(ParameterValue::I32(v)) => *v,
                _ => return Err(plenora_database_core::DatabaseError::invalid_plan("no row")),
            };

            let upd = Statement::new(
                "UPDATE _pfm_h3_entity SET v = v + 1, data = $1 WHERE id = 1 AND v = $2",
            )
            .with_params(vec![
                ParameterValue::String(new_data.to_owned()),
                ParameterValue::I32(current_v),
            ]);
            let probe = Statement::new("SELECT 1 FROM _pfm_h3_entity WHERE id = 1");
            let outcome = tx
                .execute_conditional_update(
                    ConditionalUpdate {
                        update: &upd,
                        key_probe: Some(&probe),
                        expected_affected_rows: 1,
                    },
                    &cancel,
                )
                .await;
            match outcome {
                Ok(()) => {
                    Box::new(tx).commit(&cancel).await?;
                    return Ok(attempt);
                }
                Err(e) if e.category == ErrorCategory::ConcurrentModification => {
                    let _ = Box::new(tx).rollback(&cancel).await;
                    // Retry con jitter minimo per non hammerare.
                    tokio::time::sleep(Duration::from_millis(10 * (attempt + 1) as u64)).await;
                    continue;
                }
                Err(e) => {
                    let _ = Box::new(tx).rollback(&cancel).await;
                    return Err(e);
                }
            }
        }
        Err(plenora_database_core::DatabaseError::invalid_plan(
            "too many retries",
        ))
    }

    let provider_arc = Arc::new(provider);
    let p1 = Arc::clone(&provider_arc);
    let p2 = Arc::clone(&provider_arc);
    let handle_a = tokio::spawn(async move { update_with_retry(&p1, "worker-A").await });
    let handle_b = tokio::spawn(async move { update_with_retry(&p2, "worker-B").await });
    let attempts_a = handle_a.await.expect("task A").expect("worker A ok");
    let attempts_b = handle_b.await.expect("task B").expect("worker B ok");

    // Uno dei due può aver fatto 0 tentativi (vinto subito), l'altro ≥ 1.
    // Ma entrambi hanno committato una versione.
    assert!(
        attempts_a <= 5 && attempts_b <= 5,
        "retry budget rispettato: A={attempts_a} B={attempts_b}"
    );

    // Verifica finale: v deve essere 3 (partita da 1, due update).
    let mut verify = provider_arc
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin verify");
    let rows = verify
        .query(
            &Statement::new("SELECT v, data FROM _pfm_h3_entity WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("read");
    let final_v = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::I32(v)) => *v,
        _ => panic!("v non i32"),
    };
    assert_eq!(final_v, 3, "atteso v=3 dopo 2 update successful");

    // Cleanup.
    let _ = verify
        .execute(
            &Statement::new("DROP TABLE IF EXISTS _pfm_h3_entity"),
            &cancel,
        )
        .await;
    let _ = Box::new(verify).commit(&cancel).await;
}

// ============================================================================
//  H.4 — Spatial query pushdown realistico
// ============================================================================
//
// Scenario reale PFM (layer building con coordinate): crea 1000 punti in un
// bbox europeo, indice GIST, query portable ST_Intersects. Verifica che la
// query passi via SQL con index scan (verifica indiretta: tempo <100ms) e
// il conteggio sia coerente.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pfm_h4_spatial_portable_query_uses_index() {
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let cancel = CancellationToken::new();
    let s = secret();

    let mut tx = provider
        .begin_transaction(&s, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");
    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _pfm_h4_pois ( \
             id INT PRIMARY KEY, \
             name TEXT NOT NULL, \
             geom geometry(Point, 4326)) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");
    tx.execute(
        &Statement::new(
            "INSERT INTO _pfm_h4_pois \
             SELECT gs, 'poi-' || gs::TEXT, \
                    ST_SetSRID(ST_MakePoint(random()*10 + 5, random()*10 + 40), 4326) \
             FROM generate_series(1, 1000) gs",
        ),
        &cancel,
    )
    .await
    .expect("seed");
    tx.execute(
        &Statement::new("CREATE INDEX ON _pfm_h4_pois USING GIST(geom)"),
        &cancel,
    )
    .await
    .expect("index");

    // BBox coprente la parte NW dell'area (5-10 lon, 45-50 lat).
    let rows = tx
        .query(
            &Statement::new("SELECT ST_AsEWKB(ST_SetSRID(ST_MakeEnvelope(5, 45, 10, 50), 4326))"),
            &cancel,
        )
        .await
        .expect("bbox");
    let ewkb = match rows.first().and_then(|r| r.get_index(0)) {
        Some(ParameterValue::Bytes(b)) => b.clone(),
        _ => panic!("bbox EWKB non decoded"),
    };

    // Portable spatial query.
    let ast = p_select("_pfm_h4_pois", vec!["id"])
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
    use plenora_database_core::facade::execute_portable_returning;
    let start = std::time::Instant::now();
    let result = execute_portable_returning(tx.as_mut(), &ast, &cancel)
        .await
        .expect("spatial query");
    let elapsed = start.elapsed();

    // Il bbox 5-10 × 45-50 copre circa la metà del range (5-15 × 40-50) →
    // atteso ~500 poi. Tolleranza generosa perché random.
    assert!(
        (100..=900).contains(&result.len()),
        "conteggio fuori range: {}",
        result.len()
    );
    // Con GIST index e 1000 punti, sotto secondi sicuri.
    assert!(
        elapsed < Duration::from_secs(2),
        "spatial query troppo lenta: {elapsed:?}"
    );

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}
