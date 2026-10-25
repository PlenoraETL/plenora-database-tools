//! Le sonde spatial contro lo SRID della colonna, sul `PostGIS` reale.
//!
//! Ogni caso e un risultato sbagliato senza errore misurato su fd471c2 dalla
//! rilettura indipendente del 25/10: una sonda senza SRID su `geography`
//! (diventava 4326 per cast implicito), una sonda in un altro SRID con gli
//! operatori di bounding box (che non confrontano gli SRID), una sonda lontana
//! in un altro SRID con un `ST_Intersects` indicizzato (zero righe, nessun
//! errore), un `SpatialValue` il cui SRID incorporato veniva sovrascritto.
//! Ora sono `crs` prima di ogni riga; le sonde con lo SRID giusto passano,
//! anche un WKB senza SRID con lo SRID dichiarato nel valore `wkb`.
//!
//! Girano con `PLENORA_TEST_POSTGRES_DSN`; con `PLENORA_REQUIRE_LIVE_POSTGRES`
//! una DSN assente e un fallimento, non un salto.

#![cfg(test)]
#![allow(clippy::doc_markdown, clippy::too_many_lines)]

use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::{FilterExpression, ObjectRef, ReadOperation};
use plenora_database_core::portable::{select as p_select, spatial as p_spatial};
use plenora_database_core::provider::{ParameterBag, ParameterValue, Provider, SecretString};
use plenora_database_core::relational::{
    ColumnRef, CommonTableExpression, JoinKind, QueryDerivedSource, QueryExpression, QueryJoin,
    QueryOperation, QueryProjection, QuerySource, SpatialFunction, SpatialOperator,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{
    compile_portable, CancellationToken, DatabaseError, ErrorCategory, SpatialFilter,
    SpatialPredicate, SpatialReference,
};
use plenora_db_postgres::{build_spatial_select, PostgresProvider};
use std::collections::BTreeMap;

fn live_dsn_or_skip() -> Option<String> {
    let declared = std::env::var("PLENORA_TEST_POSTGRES_DSN").ok();
    if declared.is_none() {
        assert!(
            std::env::var("PLENORA_REQUIRE_LIVE_POSTGRES").is_err(),
            "PLENORA_REQUIRE_LIVE_POSTGRES e acceso ma PLENORA_TEST_POSTGRES_DSN manca"
        );
    }
    declared
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

/// Una tabella per test: 2000 punti su una griglia di un grado, `POINT(1 1)`
/// compreso, in `geometry(Point,4326)` indicizzata e in `geography(Point,4326)`.
async fn fixture(dsn: &str, table: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connessione di setup");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{table};
             CREATE TABLE public.{table} (
                 id int PRIMARY KEY,
                 g geometry(Point, 4326),
                 h geography(Point, 4326),
                 libera geometry
             );
             INSERT INTO public.{table}
                 SELECT i, ST_SetSRID(ST_MakePoint(i % 100, i / 100), 4326),
                        ST_SetSRID(ST_MakePoint(i % 100, i / 100), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(i % 100, i / 100), 4326)
                 FROM generate_series(1, 2000) i;
             CREATE INDEX ON public.{table} USING gist (g);
             ANALYZE public.{table};"
        ))
        .await
        .expect("fixture");
}

async fn drop_fixture(dsn: &str, table: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connessione di pulizia");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{table}"))
        .await
        .expect("pulizia");
}

fn wkb_point(x: f64, y: f64) -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes
}

fn ewkb_point(srid: u32, x: f64, y: f64) -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
    bytes.extend_from_slice(&srid.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes
}

fn wkb(bytes: Vec<u8>, srid: Option<u32>) -> ParameterValue {
    ParameterValue::Wkb {
        bytes,
        srid,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    }
}

fn probe(value: ParameterValue) -> ParameterBag {
    ParameterBag::new(BTreeMap::from([("p".to_owned(), value)]))
}

fn column(field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    }
}

fn parameter() -> QueryExpression {
    QueryExpression::Parameter {
        name: "p".to_owned(),
    }
}

fn query(table: &str, filter: QueryExpression) -> QueryOperation {
    QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("public".to_owned()),
                object: table.to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("id"),
            alias: Some("id".to_owned()),
        }],
        joins: Vec::new(),
        filter: Some(filter),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
        declared_crs: Vec::new(),
    }
}

fn spatial(function: SpatialFunction, field: &str, argument: QueryExpression) -> QueryExpression {
    QueryExpression::Spatial {
        function,
        arguments: vec![column(field), argument],
    }
}

fn read(table: &str, field: &str) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: table.to_owned(),
        },
        projection: vec!["id".to_owned()],
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Spatial {
            function: SpatialFunction::Intersects,
            field: field.to_owned(),
            geometry_parameter: Some("p".to_owned()),
            distance_parameter: None,
        }),
        declared_crs: Vec::new(),
    }
}

/// Le righe di uno stream, o il suo errore.
async fn rows(
    stream: Result<Box<dyn plenora_database_core::provider::BatchStream>, DatabaseError>,
) -> Result<usize, DatabaseError> {
    let mut stream = stream?;
    let cancel = CancellationToken::new();
    let mut total = 0;
    while let Some(batch) = stream.next_batch(&cancel).await? {
        total += batch.num_rows();
    }
    Ok(total)
}

fn crs(observed: Result<usize, DatabaseError>, case: &str) {
    match observed {
        Err(error) => assert_eq!(error.category, ErrorCategory::Crs, "{case}: {error:?}"),
        Ok(rows) => panic!("{case}: accettato con {rows} righe invece di crs"),
    }
}

#[tokio::test]
async fn live_read_filter_probes_carry_the_column_srid() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_read";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let run = |field: &'static str, value: ParameterValue| {
        let provider = &provider;
        let secret = &secret;
        let cancel = &cancel;
        let budget = &budget;
        async move {
            let operation = read(table, field);
            let parameters = probe(value);
            rows(
                provider
                    .read(secret, &operation, &parameters, budget, cancel)
                    .await,
            )
            .await
        }
    };

    // fd471c2: 1 riga. Il WKB senza SRID diventava 4326 verso `geography`.
    crs(
        run("h", ParameterValue::Bytes(wkb_point(1.0, 1.0))).await,
        "geography, WKB senza SRID",
    );
    // fd471c2: `internal` (mixed SRID 4326 != 0).
    crs(
        run("g", ParameterValue::Bytes(wkb_point(1.0, 1.0))).await,
        "geometry, WKB senza SRID",
    );
    // fd471c2: 0 righe senza errore, l'indice non trova candidati.
    crs(
        run(
            "g",
            ParameterValue::Bytes(ewkb_point(3857, 100_000.0, 100_000.0)),
        )
        .await,
        "geometry indicizzata, sonda 3857 lontana",
    );
    crs(
        run("h", wkb(wkb_point(1.0, 1.0), Some(3857))).await,
        "geography, sonda wkb 3857",
    );
    // Una colonna senza typmod non ha uno SRID con cui confrontare.
    crs(
        run("libera", ParameterValue::Bytes(ewkb_point(4326, 1.0, 1.0))).await,
        "geometry senza typmod",
    );
    // Lo SRID giusto passa, dal valore `wkb` o dall'EWKB.
    assert_eq!(
        run("g", wkb(wkb_point(1.0, 1.0), Some(4326)))
            .await
            .expect("geometry, wkb 4326"),
        1
    );
    assert_eq!(
        run("h", wkb(wkb_point(1.0, 1.0), Some(4326)))
            .await
            .expect("geography, wkb 4326"),
        1
    );
    assert_eq!(
        run("g", ParameterValue::Bytes(ewkb_point(4326, 1.0, 1.0)))
            .await
            .expect("geometry, EWKB 4326"),
        1
    );
    drop_fixture(&dsn, table).await;
}

#[tokio::test]
async fn live_query_probes_carry_the_column_srid() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_query";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let run = |filter: QueryExpression, value: ParameterValue| {
        let provider = &provider;
        let secret = &secret;
        let cancel = &cancel;
        let budget = &budget;
        async move {
            let operation = query(table, filter);
            let parameters = probe(value);
            rows(
                provider
                    .query(secret, &operation, &parameters, budget, cancel)
                    .await,
            )
            .await
        }
    };
    let bbox = |field: &str| QueryExpression::SpatialOperator {
        operator: SpatialOperator::BoundingBoxIntersects,
        left: Box::new(column(field)),
        right: Box::new(parameter()),
    };

    // fd471c2: 1 riga. `&&` non confronta gli SRID.
    crs(
        run(bbox("g"), ParameterValue::Bytes(ewkb_point(3857, 1.0, 1.0))).await,
        "&&, EWKB 3857",
    );
    crs(
        run(bbox("g"), wkb(wkb_point(1.0, 1.0), Some(3857))).await,
        "&&, wkb 3857",
    );
    crs(
        run(bbox("g"), ParameterValue::Bytes(wkb_point(1.0, 1.0))).await,
        "&&, WKB senza SRID",
    );
    // fd471c2: 0 righe senza errore.
    crs(
        run(
            spatial(SpatialFunction::Intersects, "g", parameter()),
            ParameterValue::Bytes(ewkb_point(3857, 100_000.0, 100_000.0)),
        )
        .await,
        "ST_Intersects indicizzato, sonda 3857 lontana",
    );
    // fd471c2: 1 riga per cast implicito a 4326.
    crs(
        run(
            spatial(SpatialFunction::Intersects, "h", parameter()),
            ParameterValue::Bytes(wkb_point(1.0, 1.0)),
        )
        .await,
        "geography, WKB senza SRID",
    );
    // La sonda dentro una funzione che conserva il frame resta una sonda.
    crs(
        run(
            spatial(
                SpatialFunction::Intersects,
                "g",
                QueryExpression::Spatial {
                    function: SpatialFunction::Centroid,
                    arguments: vec![parameter()],
                },
            ),
            ParameterValue::Bytes(ewkb_point(3857, 1.0, 1.0)),
        )
        .await,
        "ST_Centroid della sonda 3857",
    );
    // fd471c2: 1 riga. `ST_SetSRID` sovrascriveva lo SRID incorporato.
    crs(
        run(
            spatial(
                SpatialFunction::Intersects,
                "g",
                QueryExpression::SpatialValue {
                    expression: Box::new(parameter()),
                    srid: 4326,
                    semantics: SpatialSemantics::Geometry,
                },
            ),
            ParameterValue::Bytes(ewkb_point(3857, 1.0, 1.0)),
        )
        .await,
        "SpatialValue 4326 con EWKB 3857",
    );
    assert_eq!(
        run(bbox("g"), wkb(wkb_point(1.0, 1.0), Some(4326)))
            .await
            .expect("&&, wkb 4326"),
        1
    );
    assert_eq!(
        run(
            spatial(SpatialFunction::Intersects, "h", parameter()),
            wkb(wkb_point(1.0, 1.0), Some(4326)),
        )
        .await
        .expect("geography, wkb 4326"),
        1
    );
    assert_eq!(
        run(
            spatial(
                SpatialFunction::Intersects,
                "g",
                QueryExpression::SpatialValue {
                    expression: Box::new(parameter()),
                    srid: 4326,
                    semantics: SpatialSemantics::Geometry,
                },
            ),
            ParameterValue::Bytes(wkb_point(1.0, 1.0)),
        )
        .await
        .expect("SpatialValue 4326 con WKB senza SRID"),
        1
    );
    drop_fixture(&dsn, table).await;
}

/// Il compilatore portable e `build_spatial_select` non hanno il catalogo:
/// il server confronta lo SRID dichiarato con quello della colonna prima di
/// ogni riga, e il rifiuto `PostGIS` esce `crs`.
#[tokio::test]
async fn live_compiled_spatial_statements_compare_the_column_srid() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_portable";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let reference = |ewkb: Vec<u8>, srid: u32, semantics: SpatialSemantics| SpatialReference {
        ewkb,
        srid,
        dimensions: Dimensions::Xy,
        semantics,
    };
    let execute = |statement: Statement| {
        let provider = &provider;
        let secret = &secret;
        let cancel = &cancel;
        let budget = &budget;
        async move {
            let mut transaction = provider
                .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
                .await?;
            let result = transaction.query(&statement, cancel).await;
            let _ = transaction.rollback(cancel).await;
            result.map(|rows| rows.len())
        }
    };
    let portable = |predicate: SpatialPredicate, field: &str, reference: SpatialReference| {
        compile_portable(
            plenora_database_core::plan::ProviderKind::Postgres,
            &p_select(table, vec!["id"])
                .where_(p_spatial(field, predicate, reference))
                .into_statement(),
        )
        .expect("compila")
    };
    let built = |predicate: SpatialPredicate, field: &str, reference: SpatialReference| {
        build_spatial_select(
            Some("public"),
            table,
            &["id"],
            &SpatialFilter {
                geometry_column: field.to_owned(),
                predicate,
                reference,
            },
            None,
        )
        .expect("costruisce")
    };

    for (name, statement) in [
        (
            "portable &&, 3857 contro 4326",
            portable(
                SpatialPredicate::BoundingBox,
                "g",
                reference(ewkb_point(3857, 1.0, 1.0), 3857, SpatialSemantics::Geometry),
            ),
        ),
        (
            "portable ST_Intersects indicizzato, 3857 lontana",
            portable(
                SpatialPredicate::Intersects,
                "g",
                reference(
                    ewkb_point(3857, 100_000.0, 100_000.0),
                    3857,
                    SpatialSemantics::Geometry,
                ),
            ),
        ),
        (
            "portable geography, 4258 contro 4326",
            portable(
                SpatialPredicate::Intersects,
                "h",
                reference(wkb_point(1.0, 1.0), 4258, SpatialSemantics::Geography),
            ),
        ),
        (
            "build_spatial_select &&, 3857 contro 4326",
            built(
                SpatialPredicate::BoundingBox,
                "g",
                reference(ewkb_point(3857, 1.0, 1.0), 3857, SpatialSemantics::Geometry),
            ),
        ),
        (
            "build_spatial_select geography, 4258 contro 4326",
            built(
                SpatialPredicate::Intersects,
                "h",
                reference(wkb_point(1.0, 1.0), 4258, SpatialSemantics::Geography),
            ),
        ),
        (
            "build_spatial_select su colonna senza typmod",
            built(
                SpatialPredicate::BoundingBox,
                "libera",
                reference(ewkb_point(4326, 1.0, 1.0), 4326, SpatialSemantics::Geometry),
            ),
        ),
    ] {
        crs(execute(statement).await, name);
    }
    for (name, statement) in [
        (
            "portable &&, 4326",
            portable(
                SpatialPredicate::BoundingBox,
                "g",
                reference(wkb_point(1.0, 1.0), 4326, SpatialSemantics::Geometry),
            ),
        ),
        (
            "build_spatial_select geography, WKB senza SRID dichiarato 4326",
            built(
                SpatialPredicate::Intersects,
                "h",
                reference(wkb_point(1.0, 1.0), 4326, SpatialSemantics::Geography),
            ),
        ),
        (
            "build_spatial_select &&, EWKB 4326",
            built(
                SpatialPredicate::BoundingBox,
                "g",
                reference(ewkb_point(4326, 1.0, 1.0), 4326, SpatialSemantics::Geometry),
            ),
        ),
    ] {
        assert_eq!(execute(statement).await.expect(name), 1, "{name}");
    }
    drop_fixture(&dsn, table).await;
}

/// Il renderer relazionale come lo usa lo SDK, senza catalogo: una sonda
/// passa soltanto dalla guardia del server sul typmod della colonna di
/// fronte, e la guardia rifiuta un altro SRID prima di ogni riga. Fino al
/// 25/10 lo stesso SQL senza guardia rendeva righe (`&&` con una sonda 3857,
/// la sonda lontana indicizzata, `geography` per cast implicito), e un bind
/// nudo in posizione geometrica si rendeva senza che nessuno ne confrontasse
/// lo SRID. Anche la scrittura di un valore in un altro SRID e rifiutata dal
/// server (typmod) e pubblicata `crs`.
#[tokio::test]
async fn live_sdk_relational_probes_pass_only_through_the_server_guard() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_sdk";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let sdk = plenora_database_sql::Renderer::new(
        plenora_database_sql::Dialect::Postgres,
        plenora_database_sql::DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(plenora_database_core::plan::ProviderKind::Postgres);
    let value = |srid: u32, semantics: SpatialSemantics| QueryExpression::SpatialValue {
        expression: Box::new(parameter()),
        srid,
        semantics,
    };
    let statement = |operation: QueryOperation, bound: Vec<u8>| {
        let rendered = sdk.render_query(&operation).expect("lo SDK rende la sonda");
        assert!(
            rendered.sql.contains("postgis_typmod_srid"),
            "{}",
            rendered.sql
        );
        Statement {
            sql: rendered.sql,
            params: rendered
                .binds
                .iter()
                .map(|_| ParameterValue::Bytes(bound.clone()))
                .collect(),
        }
    };
    let execute = |statement: Statement| {
        let provider = &provider;
        let secret = &secret;
        let cancel = &cancel;
        let budget = &budget;
        async move {
            let mut transaction = provider
                .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
                .await?;
            let result = transaction.query(&statement, cancel).await;
            let _ = transaction.rollback(cancel).await;
            result.map(|rows| rows.len())
        }
    };
    let bbox = |field: &str, right: QueryExpression| QueryExpression::SpatialOperator {
        operator: SpatialOperator::BoundingBoxIntersects,
        left: Box::new(column(field)),
        right: Box::new(right),
    };
    let far = wkb_point(100_000.0, 100_000.0);
    let near = wkb_point(1.0, 1.0);
    for (name, operation, bound) in [
        (
            "&&, valore 3857",
            query(table, bbox("g", value(3857, SpatialSemantics::Geometry))),
            near.clone(),
        ),
        (
            "ST_Intersects indicizzato, valore 3857 lontano",
            query(
                table,
                spatial(
                    SpatialFunction::Intersects,
                    "g",
                    value(3857, SpatialSemantics::Geometry),
                ),
            ),
            far.clone(),
        ),
        (
            "NOT ST_Intersects, valore 3857",
            query(
                table,
                QueryExpression::Not {
                    expression: Box::new(spatial(
                        SpatialFunction::Intersects,
                        "g",
                        value(3857, SpatialSemantics::Geometry),
                    )),
                },
            ),
            far.clone(),
        ),
        (
            "geography, valore 4258",
            query(
                table,
                spatial(
                    SpatialFunction::Intersects,
                    "h",
                    value(4258, SpatialSemantics::Geography),
                ),
            ),
            near.clone(),
        ),
        (
            "colonna senza typmod, valore 4326",
            query(
                table,
                bbox("libera", value(4326, SpatialSemantics::Geometry)),
            ),
            near.clone(),
        ),
    ] {
        crs(execute(statement(operation, bound)).await, name);
    }
    for (name, operation) in [
        (
            "&&, valore 4326",
            query(table, bbox("g", value(4326, SpatialSemantics::Geometry))),
        ),
        (
            "ST_Intersects, valore 4326",
            query(
                table,
                spatial(
                    SpatialFunction::Intersects,
                    "g",
                    value(4326, SpatialSemantics::Geometry),
                ),
            ),
        ),
        (
            "geography, valore 4326",
            query(
                table,
                spatial(
                    SpatialFunction::Intersects,
                    "h",
                    value(4326, SpatialSemantics::Geography),
                ),
            ),
        ),
    ] {
        assert_eq!(
            execute(statement(operation, near.clone()))
                .await
                .expect(name),
            1,
            "{name}"
        );
    }
    // Un bind nudo non ha SRID noto a chi compila: lo SDK lo rifiuta.
    let error = sdk
        .render_query(&query(table, bbox("g", parameter())))
        .expect_err("bind nudo");
    assert_eq!(error.category, ErrorCategory::Unsupported);

    // La scrittura dello SDK: il typmod della colonna rifiuta un valore in un
    // altro SRID, e il rifiuto e `crs` (era `protocol`).
    let insert = |srid: u32| {
        let lowered = plenora_database_core::compile_relational_mutation(
            plenora_database_core::plan::ProviderKind::Postgres,
            &plenora_database_core::relational::MutationOperation::Insert(
                plenora_database_core::relational::InsertOperation {
                    target: ObjectRef {
                        catalog: None,
                        schema: Some("public".to_owned()),
                        object: table.to_owned(),
                    },
                    columns: vec!["id".to_owned(), "g".to_owned()],
                    rows: vec![vec![
                        QueryExpression::Parameter {
                            name: "id".to_owned(),
                        },
                        value(srid, SpatialSemantics::Geometry),
                    ]],
                    returning: Vec::new(),
                },
            ),
        )
        .expect("mutazione");
        Statement {
            sql: lowered.sql,
            params: vec![
                ParameterValue::I32(9_002),
                ParameterValue::Bytes(near.clone()),
            ],
        }
    };
    let mut transaction = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await
        .expect("begin");
    let error = transaction
        .execute(&insert(3857), &cancel)
        .await
        .expect_err("valore 3857 nella colonna 4326");
    assert_eq!(error.category, ErrorCategory::Crs, "{error:?}");
    let _ = transaction.rollback(&cancel).await;
    let mut transaction = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await
        .expect("begin");
    transaction
        .execute(&insert(4326), &cancel)
        .await
        .expect("valore 4326");
    let _ = transaction.rollback(&cancel).await;
    drop_fixture(&dsn, table).await;
}

/// Il commento di `postgres_guarded_probe`, misurato: su una tabella vuota
/// letta in scansione sequenziale la guardia non si valuta e la lettura rende
/// zero righe senza errore — lo stesso risultato di qualunque sonda; con un
/// indice la guardia si valuta come chiave anche sulla tabella vuota.
#[tokio::test]
async fn live_guard_on_an_empty_table_returns_no_rows_or_refuses() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_empty";
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connessione");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{table};
             CREATE TABLE public.{table} (id int, g geometry(Point, 4326));
             CREATE INDEX ON public.{table} USING gist (g);"
        ))
        .await
        .expect("fixture vuota");
    let compiled = compile_portable(
        plenora_database_core::plan::ProviderKind::Postgres,
        &p_select(table, vec!["id"])
            .where_(p_spatial(
                "g",
                SpatialPredicate::Intersects,
                SpatialReference {
                    ewkb: ewkb_point(3857, 1.0, 1.0),
                    srid: 3857,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement(),
    )
    .expect("compila");
    let params = compiled
        .params
        .iter()
        .map(|value| match value {
            ParameterValue::Bytes(bytes) => {
                Box::new(bytes.clone()) as Box<dyn tokio_postgres::types::ToSql + Sync>
            }
            ParameterValue::I32(value) => Box::new(*value),
            ParameterValue::String(value) => Box::new(value.clone()),
            other => panic!("parametro inatteso {other:?}"),
        })
        .collect::<Vec<_>>();
    let refs = params
        .iter()
        .map(|value| value.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect::<Vec<_>>();
    client
        .batch_execute("SET enable_indexscan = off; SET enable_bitmapscan = off")
        .await
        .expect("seqscan");
    assert_eq!(
        client
            .query(&compiled.sql, &refs)
            .await
            .expect("seqscan")
            .len(),
        0
    );
    client
        .batch_execute(
            "SET enable_indexscan = on; SET enable_bitmapscan = on; SET enable_seqscan = off",
        )
        .await
        .expect("indexscan");
    let error = client
        .query(&compiled.sql, &refs)
        .await
        .expect_err("la guardia come chiave dell'indice");
    assert!(
        error
            .as_db_error()
            .is_some_and(|db| db.message().contains("mixed SRID")),
        "{error:?}"
    );
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{table}"))
        .await
        .expect("pulizia");
}

/// `SpatialValue` del path portable: lo SRID incorporato diverso da quello
/// dichiarato si rifiuta compilando (il valore e noto) e, se arriva al
/// server come bind, dal server. fd471c2 memorizzava `SRID=4326` con le
/// coordinate dell'EWKB 3857.
#[tokio::test]
async fn live_spatial_value_keeps_or_refuses_the_embedded_srid() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_value";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let insert = |value: ParameterValue| {
        plenora_database_core::portable::PortableStatement::Insert(
            plenora_database_core::portable::InsertStatement {
                table: plenora_database_core::portable::TableRef {
                    schema: Some("public".to_owned()),
                    name: table.to_owned(),
                },
                columns: vec!["id".to_owned(), "g".to_owned()],
                values: vec![vec![
                    plenora_database_core::portable::Expression::Literal(ParameterValue::I32(
                        9_001,
                    )),
                    plenora_database_core::portable::Expression::SpatialValue {
                        expression: Box::new(plenora_database_core::portable::Expression::Literal(
                            value,
                        )),
                        srid: 4326,
                        semantics: SpatialSemantics::Geometry,
                    },
                ]],
                returning: Vec::new(),
            },
        )
    };
    let refused = compile_portable(
        plenora_database_core::plan::ProviderKind::Postgres,
        &insert(ParameterValue::Bytes(ewkb_point(3857, 1.0, 1.0))),
    )
    .expect_err("EWKB 3857 dichiarato 4326");
    assert_eq!(refused.category, ErrorCategory::Crs);

    // Lo stesso SQL con un bind che arriva dopo la compilazione, come dallo
    // SDK: il rifiuto e del server.
    let compiled = compile_portable(
        plenora_database_core::plan::ProviderKind::Postgres,
        &insert(ParameterValue::Bytes(wkb_point(1.0, 1.0))),
    )
    .expect("WKB senza SRID");
    let mut late = compiled.clone();
    late.params[1] = ParameterValue::Bytes(ewkb_point(3857, 1.0, 1.0));
    let mut transaction = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await
        .expect("begin");
    let error = transaction
        .execute(&late, &cancel)
        .await
        .expect_err("bind 3857 dichiarato 4326");
    assert_eq!(error.category, ErrorCategory::Crs, "{error:?}");
    let _ = transaction.rollback(&cancel).await;

    for value in [
        ParameterValue::Bytes(wkb_point(1.0, 1.0)),
        ParameterValue::Bytes(ewkb_point(4326, 1.0, 1.0)),
    ] {
        let statement = compile_portable(
            plenora_database_core::plan::ProviderKind::Postgres,
            &insert(value),
        )
        .expect("compila");
        let mut transaction = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");
        transaction
            .execute(&statement, &cancel)
            .await
            .expect("insert");
        let srid = transaction
            .query(
                &Statement::new(format!(
                    "SELECT ST_SRID(g) FROM public.{table} WHERE id = 9001"
                )),
                &cancel,
            )
            .await
            .expect("srid");
        assert_eq!(srid[0][0], ParameterValue::I32(4326));
        let _ = transaction.rollback(&cancel).await;
    }
    drop_fixture(&dsn, table).await;
}

/// La regola P sul `PostGIS` reale: `g = ST_Centroid(sonda 3857)` su
/// `geometry(Point,4326)`. Su 14960f9 rendeva zero righe senza errore su tre
/// strade (rilettura del 25/10): il renderer dello SDK
/// (`compile_relational_query`), `PostgresProvider::query` con un
/// `SpatialValue` e con un bind nudo EWKB 3857. `=` non confronta gli SRID, e
/// `ST_Centroid` ha una sola geometria: nessuno giudicava la sonda. Ora e
/// `unsupported` prima del server, sulle tre strade.
#[tokio::test]
async fn live_a_probe_under_a_scalar_comparison_is_refused_before_the_server() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_scalar";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let centroid_equals = |probe: QueryExpression| QueryExpression::Compare {
        left: Box::new(column("g")),
        operator: plenora_database_core::plan::ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Spatial {
            function: SpatialFunction::Centroid,
            arguments: vec![probe],
        }),
    };
    let declared = || QueryExpression::SpatialValue {
        expression: Box::new(parameter()),
        srid: 3857,
        semantics: SpatialSemantics::Geometry,
    };
    let refused = |observed: Result<usize, DatabaseError>, case: &str| match observed {
        Err(error) => {
            assert_eq!(
                error.category,
                ErrorCategory::Unsupported,
                "{case}: {error:?}"
            );
            assert_eq!(
                error.message,
                plenora_database_core::spatial_probe::SPATIAL_VALUE_OUTSIDE_SPATIAL,
                "{case}"
            );
        }
        Ok(rows) => panic!("{case}: accettato con {rows} righe"),
    };
    // Lo SDK: il renderer relazionale senza catalogo.
    let sdk = plenora_database_sql::Renderer::new(
        plenora_database_sql::Dialect::Postgres,
        plenora_database_sql::DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(plenora_database_core::plan::ProviderKind::Postgres);
    refused(
        sdk.render_query(&query(table, centroid_equals(declared())))
            .map(|_| 0),
        "SDK, SpatialValue 3857",
    );
    // Il provider, che giudica il piano sul catalogo.
    for (case, filter, value) in [
        (
            "query, SpatialValue 3857",
            centroid_equals(declared()),
            ParameterValue::Bytes(wkb_point(1.0, 1.0)),
        ),
        (
            "query, bind nudo EWKB 3857",
            centroid_equals(parameter()),
            ParameterValue::Bytes(ewkb_point(3857, 1.0, 1.0)),
        ),
    ] {
        let operation = query(table, filter);
        let parameters = probe(value);
        refused(
            rows(
                provider
                    .query(&secret, &operation, &parameters, &budget, &cancel)
                    .await,
            )
            .await,
            case,
        );
    }
    // Il controllo positivo: la stessa sonda in un nodo con due geometrie e
    // giudicata li, e lo SRID diverso e `crs`.
    crs(
        rows(
            provider
                .query(
                    &secret,
                    &query(table, spatial(SpatialFunction::Intersects, "g", declared())),
                    &probe(ParameterValue::Bytes(wkb_point(1.0, 1.0))),
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await,
        "ST_Intersects(g, SpatialValue 3857)",
    );
    drop_fixture(&dsn, table).await;
}

/// Un blocco `SELECT projection [FROM source] [WHERE filter]`.
fn block(
    source: Option<QuerySource>,
    projection: Vec<(QueryExpression, &str)>,
    filter: Option<QueryExpression>,
) -> QueryOperation {
    QueryOperation {
        common_table_expressions: Vec::new(),
        source,
        derived_source: None,
        projection: projection
            .into_iter()
            .map(|(expression, alias)| QueryProjection {
                expression,
                alias: Some(alias.to_owned()),
            })
            .collect(),
        joins: Vec::new(),
        filter,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
        declared_crs: Vec::new(),
    }
}

fn qualified(relation: &str, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    }
}

fn derived_join(query: QueryOperation, alias: &str, on: QueryExpression) -> QueryJoin {
    QueryJoin {
        kind: JoinKind::Inner,
        source: None,
        derived_source: Some(QueryDerivedSource {
            query: Box::new(query),
            alias: alias.to_owned(),
        }),
        lateral: false,
        on: Some(on),
    }
}

/// La regola P strutturale sul `PostGIS` reale, le forme P1 e P2 della
/// rilettura del 25/10 su d30d1d2: una sonda 3857 nella projection di una
/// subquery `IN` (`g IN (SELECT sonda)`, `g IN (SELECT ST_Centroid(sonda))`)
/// e una sonda nominata per alias (`JOIN (SELECT sonda AS s) d ON t.g =
/// d.s`, la stessa come CTE e dentro `EXISTS`, `ON ST_Intersects(t.g, d.s)`
/// con una sonda lontana) su `geometry(Point,4326)` rendevano zero righe
/// senza errore, dallo SDK e da `PostgresProvider::query`. Nessun nodo
/// spatial vedeva la sonda: la colonna della projection ne portava il frame a
/// chi la nominava. Ora sono `unsupported` prima del server su entrambe le
/// strade.
#[tokio::test]
async fn live_a_probe_outside_a_spatial_predicate_of_its_block_is_refused() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_position";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let source = || QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: table.to_owned(),
        },
        alias: Some("t".to_owned()),
    };
    let base = || block(Some(source()), vec![(qualified("t", "id"), "id")], None);
    let sv = || QueryExpression::SpatialValue {
        expression: Box::new(parameter()),
        srid: 3857,
        semantics: SpatialSemantics::Geometry,
    };
    let centroid = |argument: QueryExpression| QueryExpression::Spatial {
        function: SpatialFunction::Centroid,
        arguments: vec![argument],
    };
    let tg = || qualified("t", "g");
    let equals = |left: QueryExpression, right: QueryExpression| QueryExpression::Compare {
        left: Box::new(left),
        operator: plenora_database_core::plan::ComparisonOperator::Eq,
        right: Box::new(right),
    };
    let projected = |probe: QueryExpression| block(None, vec![(probe, "s")], None);
    let near = wkb_point(1.0, 1.0);
    let far = wkb_point(100_000.0, 100_000.0);

    let mut cases = Vec::new();
    for (name, probe) in [("sonda", sv()), ("ST_Centroid(sonda)", centroid(sv()))] {
        let mut operation = base();
        operation.filter = Some(QueryExpression::InSubquery {
            expression: Box::new(tg()),
            query: Box::new(projected(probe)),
            negated: false,
        });
        cases.push((format!("P1 g IN (SELECT {name})"), operation, near.clone()));
    }
    let mut operation = base();
    operation.joins = vec![derived_join(
        projected(sv()),
        "d",
        equals(tg(), qualified("d", "s")),
    )];
    cases.push((
        "P2 JOIN (SELECT sonda AS s) d ON t.g = d.s".to_owned(),
        operation,
        near.clone(),
    ));
    let mut operation = base();
    operation.common_table_expressions = vec![CommonTableExpression {
        name: "p".to_owned(),
        recursive: false,
        query: Box::new(projected(sv())),
    }];
    operation.joins = vec![QueryJoin {
        kind: JoinKind::Inner,
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: None,
                object: "p".to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        lateral: false,
        on: Some(equals(tg(), qualified("p", "s"))),
    }];
    cases.push((
        "P2 WITH p AS (SELECT sonda AS s) JOIN p ON t.g = p.s".to_owned(),
        operation,
        near.clone(),
    ));
    let mut exists = block(
        None,
        vec![(qualified("d", "s"), "s")],
        Some(equals(tg(), qualified("d", "s"))),
    );
    exists.derived_source = Some(QueryDerivedSource {
        query: Box::new(projected(sv())),
        alias: "d".to_owned(),
    });
    let mut operation = base();
    operation.filter = Some(QueryExpression::Exists {
        query: Box::new(exists),
        negated: false,
    });
    cases.push((
        "P2 EXISTS (SELECT d.s FROM (SELECT sonda AS s) d WHERE t.g = d.s)".to_owned(),
        operation,
        near.clone(),
    ));
    let mut operation = base();
    operation.joins = vec![derived_join(
        projected(sv()),
        "d",
        QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![tg(), qualified("d", "s")],
        },
    )];
    cases.push((
        "P2 JOIN (SELECT sonda AS s) d ON ST_Intersects(t.g, d.s), sonda lontana".to_owned(),
        operation,
        far,
    ));

    let sdk = plenora_database_sql::Renderer::new(
        plenora_database_sql::Dialect::Postgres,
        plenora_database_sql::DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(plenora_database_core::plan::ProviderKind::Postgres);
    let refused = |error: &DatabaseError| {
        error.category == ErrorCategory::Unsupported
            && error.message == plenora_database_core::spatial_probe::PROBE_OUTSIDE_PREDICATE
    };
    let mut accepted = Vec::new();
    for (name, operation, bound) in &cases {
        // Lo SDK: il renderer relazionale senza catalogo. Se rendesse, lo
        // statement si esegue per misurare che cosa il chiamante riceverebbe.
        match sdk.render_query(operation) {
            Err(error) if refused(&error) => {}
            Err(error) => accepted.push(format!("SDK {name}: {error:?}")),
            Ok(rendered) => {
                let statement = Statement {
                    sql: rendered.sql,
                    params: rendered
                        .binds
                        .iter()
                        .map(|_| ParameterValue::Bytes(bound.clone()))
                        .collect(),
                };
                let mut transaction = provider
                    .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
                    .await
                    .expect("begin");
                let result = transaction.query(&statement, &cancel).await;
                let _ = transaction.rollback(&cancel).await;
                accepted.push(format!(
                    "SDK {name}: accettato, {:?}",
                    result.map(|rows| rows.len())
                ));
            }
        }
        // Il provider, che giudica il piano sul catalogo.
        match rows(
            provider
                .query(
                    &secret,
                    operation,
                    &probe(ParameterValue::Bytes(bound.clone())),
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await
        {
            Err(error) if refused(&error) => {}
            other => accepted.push(format!("query {name}: {other:?}")),
        }
    }
    drop_fixture(&dsn, table).await;
    assert!(accepted.is_empty(), "{}", accepted.join("\n"));
}

/// La settima rilettura, misurata su `PostGIS`: una sonda nascosta da un
/// involucro non spatial in posizione geometrica non era riconosciuta come
/// sonda, ne dal provider ne dallo SDK, e arrivava al server senza confronto
/// di SRID. Su 9fb015b, sul path query del provider: `g && CAST(:p AS
/// BINARY)` con EWKB 3857 vicino rendeva 1 riga (anche con
/// `ParameterValue::Wkb { srid: Some(3857) }`, il cui SRID si perdeva);
/// `ST_Intersects`/`ST_DWithin(g, CAST(:p AS BINARY))` con una sonda 3857
/// lontana 0 righe; `CAST(:p AS STRING)` con `'SRID=3857;POINT(1 1)'` 1
/// riga; `COALESCE(:p)` 0 righe. Ora ogni forma si rifiuta prima del server
/// con `unsupported`, motivo `GEOMETRY_ARGUMENT_UNCLASSIFIED`.
#[tokio::test]
async fn live_a_probe_hidden_by_a_wrapper_is_refused_before_the_server() {
    use plenora_database_core::relational::{QueryParameterType, ScalarFunction};
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_probe_wrapped";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let cast = |target_type: QueryParameterType| QueryExpression::Cast {
        expression: Box::new(parameter()),
        target_type,
    };
    let coalesce = || QueryExpression::Scalar {
        function: ScalarFunction::Coalesce,
        arguments: vec![parameter()],
    };
    let bbox = |right: QueryExpression| QueryExpression::SpatialOperator {
        operator: SpatialOperator::BoundingBoxIntersects,
        left: Box::new(column("g")),
        right: Box::new(right),
    };
    let dwithin = |argument: QueryExpression| QueryExpression::Spatial {
        function: SpatialFunction::DWithin,
        arguments: vec![
            column("g"),
            argument,
            QueryExpression::Parameter {
                name: "d".to_owned(),
            },
        ],
    };
    let near = ewkb_point(3857, 1.0, 1.0);
    let far = ewkb_point(3857, 100_000.0, 100_000.0);
    let cases = [
        (
            "g && CAST(:p AS BINARY), EWKB 3857 vicino",
            bbox(cast(QueryParameterType::Binary)),
            ParameterValue::Bytes(near.clone()),
        ),
        (
            "g && CAST(:p AS BINARY), Wkb srid 3857",
            bbox(cast(QueryParameterType::Binary)),
            wkb(wkb_point(1.0, 1.0), Some(3857)),
        ),
        (
            "ST_Intersects(g, CAST(:p AS BINARY)), EWKB 3857 lontano",
            spatial(
                SpatialFunction::Intersects,
                "g",
                cast(QueryParameterType::Binary),
            ),
            ParameterValue::Bytes(far.clone()),
        ),
        (
            "ST_DWithin(g, CAST(:p AS BINARY), d), EWKB 3857 lontano",
            dwithin(cast(QueryParameterType::Binary)),
            ParameterValue::Bytes(far.clone()),
        ),
        (
            "ST_Intersects(g, CAST(:p AS STRING)), testo SRID=3857",
            spatial(
                SpatialFunction::Intersects,
                "g",
                cast(QueryParameterType::String),
            ),
            ParameterValue::String("SRID=3857;POINT(1 1)".to_owned()),
        ),
        (
            "ST_Intersects(g, COALESCE(:p)), EWKB 3857 lontano",
            spatial(SpatialFunction::Intersects, "g", coalesce()),
            ParameterValue::Bytes(far),
        ),
    ];
    let unclassified = |observed: Result<usize, DatabaseError>, case: &str| match observed {
        Err(error) => {
            assert_eq!(
                error.category,
                ErrorCategory::Unsupported,
                "{case}: {error:?}"
            );
            assert_eq!(
                error.message,
                plenora_database_core::spatial_probe::GEOMETRY_ARGUMENT_UNCLASSIFIED,
                "{case}"
            );
        }
        Ok(rows) => panic!("{case}: accettato con {rows} righe"),
    };
    let sdk = plenora_database_sql::Renderer::new(
        plenora_database_sql::Dialect::Postgres,
        plenora_database_sql::DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(plenora_database_core::plan::ProviderKind::Postgres);
    for (case, filter, value) in cases {
        let operation = query(table, filter);
        let parameters = ParameterBag::new(BTreeMap::from([
            ("p".to_owned(), value),
            ("d".to_owned(), ParameterValue::F64(1.0)),
        ]));
        unclassified(
            rows(
                provider
                    .query(&secret, &operation, &parameters, &budget, &cancel)
                    .await,
            )
            .await,
            &format!("provider, {case}"),
        );
        unclassified(
            sdk.render_query(&operation).map(|_| 0),
            &format!("SDK, {case}"),
        );
    }
    // Il controllo positivo: la stessa sonda diretta e giudicata, e lo SRID
    // diverso e `crs`; con lo SRID della colonna rende la riga.
    crs(
        rows(
            provider
                .query(
                    &secret,
                    &query(table, bbox(parameter())),
                    &probe(ParameterValue::Bytes(near)),
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await,
        "g && :p, EWKB 3857",
    );
    assert_eq!(
        rows(
            provider
                .query(
                    &secret,
                    &query(table, bbox(parameter())),
                    &probe(ParameterValue::Bytes(ewkb_point(4326, 1.0, 1.0))),
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await
        .expect("g && :p, EWKB 4326"),
        1
    );
    drop_fixture(&dsn, table).await;
}
