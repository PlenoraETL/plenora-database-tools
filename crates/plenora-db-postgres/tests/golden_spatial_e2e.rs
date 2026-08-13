//! Test end-to-end del profilo spaziale (PostGIS).
//!
//! Chiude il buco di copertura identificato in P0.5 pre-Fase 3: prima di
//! questo file esisteva un solo scenario spatial end-to-end
//! (`pfm_h4_spatial_portable_query_uses_index`). Il Python SDK sarà consumer
//! di prima classe del profilo spaziale (layer building del PFM): serve
//! copertura più larga prima di aprire il bindings layer.
//!
//! Aree coperte:
//!   1. WKB (senza SRID) roundtrip write/read
//!   2. EWKB (con SRID) roundtrip byte-consistent
//!   3. SRID preservato in read + rifiuto SRID mismatch in write
//!   4. `geography(Point,4326)` con `ST_Distance` in metri (vs geometry in gradi)
//!   5. Predicati portable ST_Contains / ST_Within / ST_DWithin end-to-end
//!   6. Geometrie 3D (POINT Z / POINT ZM) preserva Z/M
//!
//! Il test "estensione PostGIS assente" vive in un file dedicato (Task #4 di P0.5).
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres` con
//! PostGIS 3.4+ installato.

#![cfg(test)]
#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::uninlined_format_args,
    clippy::float_cmp,
    clippy::approx_constant,
    clippy::unreadable_literal,
    clippy::similar_names,
)]

use plenora_database_core::facade::execute_portable_returning;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::portable::{select as p_select, spatial as p_spatial, Direction};
use plenora_database_core::provider::{ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory, SpatialPredicate, SpatialReference};
use plenora_db_postgres::PostgresProvider;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

fn provider() -> PostgresProvider {
    PostgresProvider::new(1_024)
}

fn bytes_of(v: Option<&ParameterValue>) -> Vec<u8> {
    match v {
        Some(ParameterValue::Bytes(b)) => b.clone(),
        other => panic!("atteso Bytes, trovato {other:?}"),
    }
}

fn i32_of(v: Option<&ParameterValue>) -> i32 {
    match v {
        Some(ParameterValue::I32(x)) => *x,
        other => panic!("atteso I32, trovato {other:?}"),
    }
}

fn i64_of(v: Option<&ParameterValue>) -> i64 {
    match v {
        Some(ParameterValue::I64(x)) => *x,
        other => panic!("atteso I64, trovato {other:?}"),
    }
}

fn f64_of(v: Option<&ParameterValue>) -> f64 {
    match v {
        Some(ParameterValue::F64(x)) => *x,
        other => panic!("atteso F64, trovato {other:?}"),
    }
}

fn bool_of(v: Option<&ParameterValue>) -> bool {
    match v {
        Some(ParameterValue::Bool(b)) => *b,
        other => panic!("atteso Bool, trovato {other:?}"),
    }
}

fn text_of(v: Option<&ParameterValue>) -> String {
    match v {
        Some(ParameterValue::String(s)) => s.clone(),
        other => panic!("atteso String, trovato {other:?}"),
    }
}

// ============================================================================
//  S.1 — WKB roundtrip (senza SRID)
// ============================================================================
//
// Il consumer PFM potrebbe ricevere geometrie standard WKB (senza SRID prefix)
// da fonti OGC. Verifica:
//   - Insert via ST_GeomFromText → serialize via ST_AsBinary produce WKB puro
//   - Reinsert via ST_GeomFromWKB con quei bytes → geometria ST_Equals
//   - SRID resta 0 (WKB non porta SRID)
//   - Lunghezza WKB per POINT 2D = 21 byte (1 byte order + 4 type + 8 x + 8 y)

#[ignore = "live: richiede Postgres su dataflow-postgres con PostGIS"]
#[tokio::test]
async fn spatial_s1_wkb_roundtrip_without_srid() {
    let provider = provider();
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _sp_s1 ( \
             id INT PRIMARY KEY, \
             g geometry) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");

    tx.execute(
        &Statement::new(
            "INSERT INTO _sp_s1 (id, g) VALUES (1, ST_GeomFromText('POINT(5 45)'))",
        ),
        &cancel,
    )
    .await
    .expect("insert #1");

    // Estrai WKB (senza SRID).
    let rows = tx
        .query(
            &Statement::new("SELECT ST_AsBinary(g) FROM _sp_s1 WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("read wkb");
    let wkb = bytes_of(rows.first().and_then(|r| r.get_index(0)));

    // WKB point 2D = 1 (byte order) + 4 (type) + 8 (x) + 8 (y) = 21 byte.
    assert_eq!(
        wkb.len(),
        21,
        "WKB POINT 2D atteso 21 byte, trovato {}",
        wkb.len()
    );
    // Il primo byte è l'endianness (0x00 big / 0x01 little). PostGIS emette LE.
    assert_eq!(wkb[0], 0x01, "atteso little-endian byte order");

    // Reinserisci come nuova riga via ST_GeomFromWKB.
    tx.execute(
        &Statement::new("INSERT INTO _sp_s1 (id, g) VALUES (2, ST_GeomFromWKB($1))")
            .with_params(vec![ParameterValue::Bytes(wkb.clone())]),
        &cancel,
    )
    .await
    .expect("insert #2");

    // Le due geometrie devono essere ST_Equals.
    let rows = tx
        .query(
            &Statement::new(
                "SELECT ST_Equals( \
                   (SELECT g FROM _sp_s1 WHERE id = 1), \
                   (SELECT g FROM _sp_s1 WHERE id = 2))",
            ),
            &cancel,
        )
        .await
        .expect("st_equals");
    assert!(bool_of(rows.first().and_then(|r| r.get_index(0))),
        "roundtrip WKB non ST_Equals");

    // WKB non porta SRID: entrambe le righe devono avere SRID 0.
    let rows = tx
        .query(
            &Statement::new("SELECT id, ST_SRID(g) FROM _sp_s1 ORDER BY id"),
            &cancel,
        )
        .await
        .expect("srid");
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert_eq!(i32_of(r.get_index(1)), 0, "atteso SRID=0 su WKB");
    }

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  S.2 — EWKB roundtrip byte-consistent (con SRID)
// ============================================================================
//
// EWKB è il formato preferito dal profilo spatial di Plenora (SpatialReference
// porta ewkb + srid espliciti). Verifica:
//   - ST_AsEWKB include il SRID prefix (byte 0..=5 header, poi 4 byte SRID)
//   - Reinsert via ST_GeomFromEWKB → serializza allo stesso EWKB byte-perfect
//   - SRID preservato

#[ignore = "live: richiede Postgres su dataflow-postgres con PostGIS"]
#[tokio::test]
async fn spatial_s2_ewkb_roundtrip_byte_consistent() {
    let provider = provider();
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _sp_s2 ( \
             id INT PRIMARY KEY, \
             g geometry(Point, 4326)) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");

    tx.execute(
        &Statement::new(
            "INSERT INTO _sp_s2 (id, g) \
             VALUES (1, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))",
        ),
        &cancel,
    )
    .await
    .expect("insert seed");

    // EWKB del seed.
    let rows = tx
        .query(
            &Statement::new("SELECT ST_AsEWKB(g) FROM _sp_s2 WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("ewkb seed");
    let ewkb_seed = bytes_of(rows.first().and_then(|r| r.get_index(0)));

    // EWKB POINT 2D con SRID: 1 (order) + 4 (type|0x20000000) + 4 (SRID) + 8 (x) + 8 (y) = 25 byte.
    assert_eq!(
        ewkb_seed.len(),
        25,
        "EWKB POINT 2D+SRID atteso 25 byte, trovato {}",
        ewkb_seed.len()
    );
    // SRID little-endian in byte 5..=8 = 4326 = 0x000010E6 → bytes [0xE6,0x10,0x00,0x00].
    assert_eq!(
        &ewkb_seed[5..9],
        &[0xE6, 0x10, 0x00, 0x00],
        "SRID nel prefix EWKB non è 4326 LE"
    );

    // Reinserisci via ST_GeomFromEWKB e verifica byte-perfect equivalence del roundtrip.
    tx.execute(
        &Statement::new(
            "INSERT INTO _sp_s2 (id, g) VALUES (2, ST_GeomFromEWKB($1))",
        )
        .with_params(vec![ParameterValue::Bytes(ewkb_seed.clone())]),
        &cancel,
    )
    .await
    .expect("insert #2");

    let rows = tx
        .query(
            &Statement::new("SELECT ST_AsEWKB(g) FROM _sp_s2 WHERE id = 2"),
            &cancel,
        )
        .await
        .expect("ewkb #2");
    let ewkb_out = bytes_of(rows.first().and_then(|r| r.get_index(0)));
    assert_eq!(
        ewkb_out, ewkb_seed,
        "EWKB roundtrip non byte-perfect (in={:02x?}, out={:02x?})",
        ewkb_seed, ewkb_out
    );

    // SRID preservato.
    let rows = tx
        .query(
            &Statement::new("SELECT ST_SRID(g) FROM _sp_s2 ORDER BY id"),
            &cancel,
        )
        .await
        .expect("srid");
    for r in &rows {
        assert_eq!(i32_of(r.get_index(0)), 4326);
    }

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  S.3 — SRID preservato in read, mismatch respinto in write
// ============================================================================
//
// Il layer building del PFM userà tabelle typed geometry(Point, 4326). Un
// insert con SRID diverso deve fallire in modo esplicito. Usa savepoint per
// contenere l'errore senza abortire la tx.

#[ignore = "live: richiede Postgres su dataflow-postgres con PostGIS"]
#[tokio::test]
async fn spatial_s3_srid_preserved_read_mismatch_rejected_write() {
    let provider = provider();
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _sp_s3 ( \
             id INT PRIMARY KEY, \
             g geometry(Point, 4326)) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");

    // OK: SRID 4326 corretto.
    tx.execute(
        &Statement::new(
            "INSERT INTO _sp_s3 (id, g) \
             VALUES (1, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))",
        ),
        &cancel,
    )
    .await
    .expect("insert ok");

    // Verifica SRID preservato.
    let rows = tx
        .query(
            &Statement::new("SELECT ST_SRID(g) FROM _sp_s3 WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("srid ok");
    assert_eq!(i32_of(rows.first().and_then(|r| r.get_index(0))), 4326);

    // Savepoint per contenere il fallimento dell'insert con SRID sbagliato.
    tx.savepoint("sp_mismatch", &cancel).await.expect("savepoint");

    let mismatch_err = tx
        .execute(
            &Statement::new(
                "INSERT INTO _sp_s3 (id, g) \
                 VALUES (2, ST_SetSRID(ST_MakePoint(1000000, 5000000), 3857))",
            ),
            &cancel,
        )
        .await;
    let err = mismatch_err.expect_err("SRID mismatch deve fallire");
    // Il driver wrappa il messaggio Postgres in uno stabile ("operazione
    // PostgreSQL fallita" / "sintassi SQL PostgreSQL non valida"). Non
    // asseriamo il testo — verifichiamo che la categoria non sia Internal
    // (bug interno) né Cancelled/Timeout, e che il provider sia Postgres.
    assert!(
        !matches!(
            err.category,
            ErrorCategory::Internal | ErrorCategory::Cancelled | ErrorCategory::Timeout
        ),
        "categoria inattesa {:?} per SRID mismatch: {}",
        err.category,
        err.message
    );
    assert_eq!(
        err.provider,
        Some(plenora_database_core::plan::ProviderKind::Postgres),
        "provider deve essere Postgres"
    );

    // Rollback del savepoint: la tx resta viva.
    tx.rollback_to_savepoint("sp_mismatch", &cancel)
        .await
        .expect("rollback to sp");
    tx.release_savepoint("sp_mismatch", &cancel)
        .await
        .expect("release sp");

    // Verifica che nessuna riga aggiuntiva sia stata scritta.
    let rows = tx
        .query(
            &Statement::new("SELECT COUNT(*)::BIGINT FROM _sp_s3"),
            &cancel,
        )
        .await
        .expect("count");
    assert_eq!(
        i64_of(rows.first().and_then(|r| r.get_index(0))),
        1,
        "atteso solo 1 row dopo il rollback del mismatch"
    );

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  S.4 — geography(Point,4326): ST_Distance in metri
// ============================================================================
//
// La differenza semantica geometry vs geography è cruciale per il PFM: query
// su geography ritornano distanze in metri (calcoli geodetici), su geometry
// in gradi (unità dell'SRS). Verifica con distanza nota Milano-Roma ≈ 477 km.
//
// Nota: il portable SpatialPredicate::DWithin è cast::geometry-only nell'attuale
// builder (`spatial.rs`), quindi il DWithin verso una colonna geography non è
// ancora supportato via portable AST. Documentato come limitazione: qui uso
// SQL nativo per il calcolo distanze.

#[ignore = "live: richiede Postgres su dataflow-postgres con PostGIS"]
#[tokio::test]
async fn spatial_s4_geography_distance_in_meters() {
    let provider = provider();
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _sp_s4 ( \
             id INT PRIMARY KEY, \
             name TEXT NOT NULL, \
             g_geog geography(Point, 4326), \
             g_geom geometry(Point, 4326)) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");

    // Milano (9.190, 45.464), Roma (12.496, 41.902).
    tx.execute(
        &Statement::new(
            "INSERT INTO _sp_s4 (id, name, g_geog, g_geom) VALUES \
             (1, 'Milano', \
              ST_SetSRID(ST_MakePoint(9.190, 45.464), 4326)::geography, \
              ST_SetSRID(ST_MakePoint(9.190, 45.464), 4326)), \
             (2, 'Roma', \
              ST_SetSRID(ST_MakePoint(12.496, 41.902), 4326)::geography, \
              ST_SetSRID(ST_MakePoint(12.496, 41.902), 4326))",
        ),
        &cancel,
    )
    .await
    .expect("seed");

    // Distanza in metri via geography.
    let rows = tx
        .query(
            &Statement::new(
                "SELECT ST_Distance(a.g_geog, b.g_geog) \
                 FROM _sp_s4 a, _sp_s4 b WHERE a.id = 1 AND b.id = 2",
            ),
            &cancel,
        )
        .await
        .expect("dist geog");
    let dist_meters = f64_of(rows.first().and_then(|r| r.get_index(0)));
    // Milano-Roma ≈ 477 km; tolleranza generosa 460..500 km.
    assert!(
        (460_000.0..=500_000.0).contains(&dist_meters),
        "distanza geography attesa ~477 km, trovata {} m",
        dist_meters
    );

    // Distanza in "gradi" via geometry (piatto). Milano-Roma diff coordinate
    // ~5 gradi combinata → ST_Distance in gradi ~4.9. Verifica che sia
    // largamente < 100 (cioè non in metri).
    let rows = tx
        .query(
            &Statement::new(
                "SELECT ST_Distance(a.g_geom, b.g_geom) \
                 FROM _sp_s4 a, _sp_s4 b WHERE a.id = 1 AND b.id = 2",
            ),
            &cancel,
        )
        .await
        .expect("dist geom");
    let dist_deg = f64_of(rows.first().and_then(|r| r.get_index(0)));
    assert!(
        dist_deg < 10.0,
        "distanza geometry attesa in gradi (~4.9), trovata {}",
        dist_deg
    );

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  S.5 — Predicati portable ST_Contains / ST_Within / ST_DWithin
// ============================================================================
//
// L'unico test end-to-end preesistente era ST_Intersects. Qui estendiamo agli
// altri 3 predicati del catalogo portable via `p_spatial`.
//
// Setup: 3 polygon "regioni" quadrate + 1 polygon reference ridotto interno.
//   R1 = quadrato grande (0..10, 0..10)      — contiene tutto
//   R2 = quadrato medio  (2..4, 2..4)         — contiene ref e più piccolo di R1
//   R3 = quadrato esterno (100..101, 100..101) — nessuna relazione
//   REF = quadrato piccolo (2.5..3.5, 2.5..3.5) — dentro R1 e R2
//
// Verifiche:
//   - ST_Contains(g, REF): quali g contengono REF? → {R1, R2}
//   - ST_Within(g, REF): quali g stanno dentro REF? → {} (nessuno più piccolo)
//   - ST_DWithin(g, REF, 50°): quali g sono entro 50 gradi da REF? → {R1, R2}
//     (R3 è a >90 gradi). NB: distanza in unità SRS (gradi per 4326).

#[ignore = "live: richiede Postgres su dataflow-postgres con PostGIS"]
#[tokio::test]
async fn spatial_s5_portable_contains_within_dwithin_e2e() {
    let provider = provider();
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _sp_s5 ( \
             id INT PRIMARY KEY, \
             name TEXT NOT NULL, \
             g geometry(Polygon, 4326)) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");

    tx.execute(
        &Statement::new(
            "INSERT INTO _sp_s5 (id, name, g) VALUES \
             (1, 'R1_big',   ST_SetSRID(ST_MakeEnvelope(0, 0, 10, 10), 4326)), \
             (2, 'R2_mid',   ST_SetSRID(ST_MakeEnvelope(2, 2, 4, 4), 4326)), \
             (3, 'R3_far',   ST_SetSRID(ST_MakeEnvelope(100, 100, 101, 101), 4326))",
        ),
        &cancel,
    )
    .await
    .expect("seed");

    // Estrai EWKB del polygon di riferimento.
    let rows = tx
        .query(
            &Statement::new(
                "SELECT ST_AsEWKB(ST_SetSRID(ST_MakeEnvelope(2.5, 2.5, 3.5, 3.5), 4326))",
            ),
            &cancel,
        )
        .await
        .expect("ref ewkb");
    let ref_ewkb = bytes_of(rows.first().and_then(|r| r.get_index(0)));

    let reference = SpatialReference {
        ewkb: ref_ewkb,
        srid: 4326,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    };

    // Portable Contains.
    let ast_contains = p_select("_sp_s5", vec!["id", "name"])
        .where_(p_spatial(
            "g",
            SpatialPredicate::Contains,
            reference.clone(),
        ))
        .order_by("id", Direction::Asc)
        .into_statement();
    let rows = execute_portable_returning(tx.as_mut(), &ast_contains, &cancel)
        .await
        .expect("contains");
    let names_contains: Vec<String> = rows
        .iter()
        .map(|r| text_of(r.get_index(1)))
        .collect();
    assert_eq!(
        names_contains,
        vec!["R1_big".to_string(), "R2_mid".to_string()],
        "ST_Contains atteso {{R1,R2}}"
    );

    // Portable Within — ref è più grande di nulla in tabella (nessun polygon
    // interamente contenuto in REF).
    let ast_within = p_select("_sp_s5", vec!["id", "name"])
        .where_(p_spatial(
            "g",
            SpatialPredicate::Within,
            reference.clone(),
        ))
        .into_statement();
    let rows = execute_portable_returning(tx.as_mut(), &ast_within, &cancel)
        .await
        .expect("within");
    assert!(
        rows.is_empty(),
        "ST_Within atteso vuoto, trovato {} riga/e",
        rows.len()
    );

    // Portable DWithin — 50 gradi coprono R1 e R2 (adiacenti/sovrapposti) ma
    // non R3 (a ~100 gradi). NB: DWithin su geometry usa unità SRS (gradi).
    let ast_dwithin = p_select("_sp_s5", vec!["id", "name"])
        .where_(p_spatial(
            "g",
            SpatialPredicate::DWithin { distance_meters: 50.0 },
            reference.clone(),
        ))
        .order_by("id", Direction::Asc)
        .into_statement();
    let rows = execute_portable_returning(tx.as_mut(), &ast_dwithin, &cancel)
        .await
        .expect("dwithin");
    let names_dw: Vec<String> = rows.iter().map(|r| text_of(r.get_index(1))).collect();
    assert_eq!(
        names_dw,
        vec!["R1_big".to_string(), "R2_mid".to_string()],
        "ST_DWithin(50°) atteso {{R1,R2}}"
    );

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  S.6 — Geometrie 3D: POINT Z e POINT ZM preservano Z/M
// ============================================================================
//
// Il PFM ha use case 3D (edifici multi-piano, elevazione). Il profilo
// canonico ha `Dimensions::Xyz` e `Dimensions::Xyzm`. Verifica:
//   - Insert POINT Z (3D) roundtrip: ST_Z ritorna il valore atteso
//   - Insert POINT ZM (4D) roundtrip: ST_Z e ST_M ritornano attesi
//   - EWKB include il flag della dimensione (bit 0x80000000 per Z, 0x40000000 per M)

#[ignore = "live: richiede Postgres su dataflow-postgres con PostGIS"]
#[tokio::test]
async fn spatial_s6_3d_zm_roundtrip() {
    let provider = provider();
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    // Tabella con column type `geometry` generico (accetta qualsiasi dim).
    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _sp_s6 ( \
             id INT PRIMARY KEY, \
             kind TEXT NOT NULL, \
             g geometry) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");

    // POINT Z (X Y Z) e POINT ZM (X Y Z M).
    tx.execute(
        &Statement::new(
            "INSERT INTO _sp_s6 (id, kind, g) VALUES \
             (1, 'z',  ST_GeomFromText('POINT Z (5 45 100)')), \
             (2, 'zm', ST_GeomFromText('POINT ZM (5 45 100 7.5)'))",
        ),
        &cancel,
    )
    .await
    .expect("seed");

    // Verifica Z su riga 1.
    let rows = tx
        .query(
            &Statement::new(
                "SELECT ST_Z(g), ST_NDims(g), GeometryType(g) FROM _sp_s6 WHERE id = 1",
            ),
            &cancel,
        )
        .await
        .expect("z");
    let row = rows.first().expect("almeno una riga");
    assert_eq!(f64_of(row.get_index(0)), 100.0, "Z non preservato");
    assert_eq!(i32_of(row.get_index(1)), 3, "atteso 3D (NDims=3)");
    assert_eq!(text_of(row.get_index(2)), "POINT");

    // Verifica Z e M su riga 2.
    let rows = tx
        .query(
            &Statement::new(
                "SELECT ST_Z(g), ST_M(g), ST_NDims(g) FROM _sp_s6 WHERE id = 2",
            ),
            &cancel,
        )
        .await
        .expect("zm");
    let row = rows.first().expect("almeno una riga");
    assert_eq!(f64_of(row.get_index(0)), 100.0, "Z non preservato in ZM");
    assert_eq!(f64_of(row.get_index(1)), 7.5, "M non preservato in ZM");
    assert_eq!(i32_of(row.get_index(2)), 4, "atteso 4D (NDims=4)");

    // EWKB deve avere i flag di dimensione settati.
    // POINT Z: type = 0x80000001 (bit 31 = Z) — LE encoding: [01, 00, 00, 80]
    // POINT ZM: type = 0xC0000001 (bit 31=Z, bit 30=M) — LE encoding: [01, 00, 00, C0]
    let rows = tx
        .query(
            &Statement::new(
                "SELECT id, ST_AsEWKB(g) FROM _sp_s6 ORDER BY id",
            ),
            &cancel,
        )
        .await
        .expect("ewkb 3d");
    for r in &rows {
        let id = i32_of(r.get_index(0));
        let ewkb = bytes_of(r.get_index(1));
        // byte 0 = order (0x01 LE), byte 1..=4 = type (LE u32).
        assert_eq!(ewkb[0], 0x01, "atteso LE byte order");
        let type_bytes = [ewkb[1], ewkb[2], ewkb[3], ewkb[4]];
        let type_val = u32::from_le_bytes(type_bytes);
        // POINT = 1. Flag Z = 0x80000000. Flag M = 0x40000000.
        let has_z = (type_val & 0x80000000) != 0;
        let has_m = (type_val & 0x40000000) != 0;
        let base_type = type_val & 0x0FFFFFFF;
        assert_eq!(base_type, 1, "base type deve essere POINT (1)");
        match id {
            1 => {
                assert!(has_z, "riga id=1 (POINT Z) deve avere flag Z");
                assert!(!has_m, "riga id=1 (POINT Z) non deve avere flag M");
            }
            2 => {
                assert!(has_z, "riga id=2 (POINT ZM) deve avere flag Z");
                assert!(has_m, "riga id=2 (POINT ZM) deve avere flag M");
            }
            other => panic!("id inatteso: {other}"),
        }
    }

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}
