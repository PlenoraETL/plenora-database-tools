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

/// Una colonna geometrica sotto un operatore che non e spatial si rifiuta
/// prima del server, come sugli altri provider
/// (`plenora_database_core::spatial_operand`).
///
/// `PostGIS` rifiuta da se `g = $1::bytea` e `g = $1::text` («operator is
/// not unique»), ma ordina per geometria e accetta `CAST(g AS TEXT) LIKE`;
/// ora il rifiuto e `unsupported`, uguale per ogni forma e su ogni path. Le
/// stesse forme sulla colonna intera restano.
#[tokio::test]
async fn live_a_geometry_column_under_a_scalar_operator_is_refused_before_the_server() {
    use plenora_database_core::plan::{ComparisonOperator, OrderBy, SortDirection};
    use plenora_database_core::relational::{QueryOrdering, QueryParameterType};
    use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_scalar_geometry_operand";
    fixture(&dsn, table).await;
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let typed = |parameter_type| QueryExpression::TypedParameter {
        name: "p".to_owned(),
        parameter_type,
    };
    let equals = |left: QueryExpression, right: QueryExpression| QueryExpression::Compare {
        left: Box::new(left),
        operator: ComparisonOperator::Eq,
        right: Box::new(right),
    };
    let unsupported = |observed: Result<usize, DatabaseError>, case: &str| match observed {
        Err(error) => {
            assert_eq!(
                error.category,
                ErrorCategory::Unsupported,
                "{case}: {error:?}"
            );
            assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{case}: {error:?}");
        }
        Ok(rows) => panic!("{case}: accettato con {rows} righe"),
    };
    let bytes = probe(ParameterValue::Bytes(ewkb_point(4326, 1.0, 1.0)));
    let text = probe(ParameterValue::String("SRID=4326;POINT(1 1)".to_owned()));
    for (case, filter, parameters) in [
        (
            "= bytea",
            equals(column("g"), typed(QueryParameterType::Binary)),
            &bytes,
        ),
        (
            "= text",
            equals(column("g"), typed(QueryParameterType::String)),
            &text,
        ),
        ("= non tipizzato", equals(column("g"), parameter()), &text),
        (
            "geography =",
            equals(column("h"), typed(QueryParameterType::Binary)),
            &bytes,
        ),
        (
            "LIKE su CAST",
            QueryExpression::Like {
                expression: Box::new(QueryExpression::Cast {
                    expression: Box::new(column("g")),
                    target_type: QueryParameterType::String,
                }),
                pattern: Box::new(typed(QueryParameterType::String)),
                case_insensitive: false,
                negated: false,
            },
            &text,
        ),
    ] {
        unsupported(
            rows(
                provider
                    .query(&secret, &query(table, filter), parameters, &budget, &cancel)
                    .await,
            )
            .await,
            case,
        );
    }
    let mut ordered = query(
        table,
        QueryExpression::IsNull {
            expression: Box::new(column("g")),
            negated: true,
        },
    );
    ordered.order_by = vec![QueryOrdering {
        expression: column("g"),
        direction: SortDirection::Asc,
    }];
    unsupported(
        rows(
            provider
                .query(
                    &secret,
                    &ordered,
                    &ParameterBag::default(),
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await,
        "ORDER BY",
    );
    let mut read_eq = read(table, "g");
    read_eq.filter = Some(FilterExpression::Eq {
        field: "g".to_owned(),
        parameter: "p".to_owned(),
    });
    unsupported(
        rows(
            provider
                .read(&secret, &read_eq, &bytes, &budget, &cancel)
                .await,
        )
        .await,
        "lettura =",
    );
    let mut read_order = read(table, "g");
    read_order.filter = None;
    read_order.order_by = vec![OrderBy {
        field: "g".to_owned(),
        direction: SortDirection::Asc,
    }];
    unsupported(
        rows(
            provider
                .read(
                    &secret,
                    &read_order,
                    &ParameterBag::default(),
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await,
        "lettura ORDER BY",
    );

    // Nessun falso rifiuto: la colonna intera, e la geometria sotto IS NULL.
    let id = query(
        table,
        equals(column("id"), typed(QueryParameterType::Integer)),
    );
    let one = probe(ParameterValue::I32(1));
    assert_eq!(
        rows(provider.query(&secret, &id, &one, &budget, &cancel).await)
            .await
            .expect("id ="),
        1
    );
    let not_null = query(
        table,
        QueryExpression::IsNull {
            expression: Box::new(column("g")),
            negated: true,
        },
    );
    assert_eq!(
        rows(
            provider
                .query(
                    &secret,
                    &not_null,
                    &ParameterBag::default(),
                    &budget,
                    &cancel
                )
                .await
        )
        .await
        .expect("IS NOT NULL"),
        2000
    );
    drop_fixture(&dsn, table).await;
}

/// La regola degli operandi geometrici a mondo chiuso, sul `PostGIS` reale.
///
/// Le forme della nona rilettura (25/10), ammesse su cf49fa0 e rese dal
/// server senza errore: `ORDER BY k` di una `UNION ALL` il cui secondo ramo e
/// l'EWKB di una geometria; una colonna senza alias di una sorgente derivata
/// nominata con il nome che le da il server (`st_centroid`, `g` per un
/// `CAST(t.g AS text)`) in `ORDER BY`, `GROUP BY`, `LIKE`; `DISTINCT *` di
/// una geometria in testo; `ORDER BY` di un `geometry[]`, di un dominio su
/// `geometry[]`, di un composito con una geometria, di un array di compositi;
/// la lettura con `ORDER BY` di un `geometry[]`. Ora si rifiutano prima del
/// server; le colonne scalari della stessa tabella restano, con le righe
/// attese.
#[tokio::test]
async fn live_unresolved_geometry_operands_are_refused_before_the_server() {
    use plenora_database_core::plan::{OrderBy, SortDirection};
    use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
    use serde_json::{json, Value};
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_operandi_r9";
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connessione di setup");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{table};
             DROP TABLE IF EXISTS public.{table}_scalari;
             DROP DOMAIN IF EXISTS public.{table}_dominio;
             DROP TYPE IF EXISTS public.{table}_coppia;
             CREATE TYPE public.{table}_coppia AS (label text, shape geometry);
             CREATE DOMAIN public.{table}_dominio AS geometry[];
             CREATE TABLE public.{table} (
                 id int PRIMARY KEY,
                 g geometry(Point, 4326),
                 name text,
                 ga geometry[],
                 gd public.{table}_dominio,
                 gc public.{table}_coppia,
                 gca public.{table}_coppia[]
             );
             INSERT INTO public.{table}
                 SELECT i, ST_SetSRID(ST_MakePoint(i, i), 4326), 'riga-' || i,
                        ARRAY[ST_MakePoint(i, i)], ARRAY[ST_MakePoint(i, i)],
                        ROW('c'::text, ST_MakePoint(i, i))::public.{table}_coppia, ARRAY[ROW('c'::text, ST_MakePoint(i, i))::public.{table}_coppia]
                 FROM generate_series(1, 3) i;
             CREATE TABLE public.{table}_scalari (id int PRIMARY KEY, name text);
             INSERT INTO public.{table}_scalari VALUES (1, 'uno'), (2, 'uno');"
        ))
        .await
        .expect("fixture");
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let column = |relation: Option<&str>, field: &str| json!({"kind": "column", "column": {"relation": relation, "field": field}});
    let item =
        |expression: &Value, alias: Option<&str>| json!({"expression": expression, "alias": alias});
    let text = |expression: &Value| json!({"kind": "cast", "expression": expression, "target_type": "string"});
    let order = |expression: &Value| json!([{"expression": expression, "direction": "desc"}]);
    let like = |expression: &Value| {
        json!({"kind": "like", "expression": expression,
            "pattern": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}})
    };
    let over = |object: &str, extra: &Value| {
        let mut operation = json!({
            "source": {"object": {"catalog": null, "schema": "public", "object": object}, "alias": "t"},
            "projection": [item(&column(Some("t"), "id"), Some("id"))],
            "filter": null, "having": null, "row_limit": null
        });
        for (key, value) in extra.as_object().expect("oggetto") {
            operation[key] = value.clone();
        }
        operation
    };
    let over_derived = |inner: &Value, extra: &Value| {
        let mut operation = json!({
            "source": null,
            "derived_source": {"query": inner, "alias": "d"},
            "projection": [item(&json!({"kind": "wildcard", "relation": null}), None)],
            "filter": null, "having": null, "row_limit": null
        });
        for (key, value) in extra.as_object().expect("oggetto") {
            operation[key] = value.clone();
        }
        operation
    };
    let plan = |value: Value| -> QueryOperation { serde_json::from_value(value).expect("piano") };
    let g = column(Some("t"), "g");
    let id = column(Some("t"), "id");
    let count = json!({"kind": "scalar", "function": "count", "arguments": [{"kind": "wildcard", "relation": null}]});
    let output = json!({"kind": "spatial_output", "expression": g, "semantics": "geometry"});
    let centroid = json!({"kind": "spatial", "function": "centroid", "arguments": [g]});
    let generated = |expression: &Value| {
        over(
            table,
            &json!({"projection": [item(&id, None), item(expression, None)]}),
        )
    };
    let k = column(None, "k");
    let pattern = probe(ParameterValue::String("0101%".to_owned()));
    let none = ParameterBag::default();
    let binary = probe(ParameterValue::Bytes(vec![0]));
    let mut refused = vec![
        (
            "ORDER BY k su UNION ALL".to_owned(),
            over(
                table,
                &json!({
                    "projection": [item(&json!({"kind": "typed_parameter", "name": "p", "parameter_type": "binary"}), Some("k"))],
                    "set_operations": [{"operator": "union", "all": true,
                        "query": over(table, &json!({"projection": [item(&output, Some("k"))]}))}],
                    "order_by": order(&k)
                }),
            ),
            binary.clone(),
        ),
        (
            "ORDER BY d.st_centroid".to_owned(),
            over_derived(
                &generated(&centroid),
                &json!({
                    "projection": [item(&column(Some("d"), "id"), Some("id"))],
                    "order_by": order(&column(Some("d"), "st_centroid")), "row_limit": 1
                }),
            ),
            none.clone(),
        ),
        (
            "GROUP BY d.st_centroid".to_owned(),
            over_derived(
                &generated(&centroid),
                &json!({
                    "projection": [item(&count, Some("n"))],
                    "group_by": [column(Some("d"), "st_centroid")]
                }),
            ),
            none.clone(),
        ),
        (
            "d.g LIKE su CAST senza alias".to_owned(),
            over_derived(
                &generated(&text(&g)),
                &json!({
                    "projection": [item(&column(Some("d"), "id"), Some("id"))],
                    "filter": like(&column(Some("d"), "g"))
                }),
            ),
            pattern.clone(),
        ),
        (
            "DISTINCT * su una geometria in testo".to_owned(),
            over_derived(
                &over(table, &json!({"projection": [item(&text(&g), Some("c"))]})),
                &json!({"distinct": true}),
            ),
            none.clone(),
        ),
    ];
    for field in ["ga", "gd", "gc", "gca"] {
        refused.push((
            format!("ORDER BY t.{field}"),
            over(
                table,
                &json!({"order_by": order(&column(Some("t"), field)), "row_limit": 1}),
            ),
            none.clone(),
        ));
    }
    let mut accepted = Vec::new();
    for (form, operation, parameters) in &refused {
        match rows(
            provider
                .query(
                    &secret,
                    &plan(operation.clone()),
                    parameters,
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await
        {
            Err(error) => {
                assert_eq!(
                    error.category,
                    ErrorCategory::Unsupported,
                    "{form}: {error:?}"
                );
                assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{form}: {error:?}");
            }
            Ok(count) => accepted.push(format!("{form}: {count} righe")),
        }
    }
    let mut read_array = read(table, "g");
    read_array.filter = None;
    read_array.order_by = vec![OrderBy {
        field: "ga".to_owned(),
        direction: SortDirection::Desc,
    }];
    match rows(
        provider
            .read(&secret, &read_array, &none, &budget, &cancel)
            .await,
    )
    .await
    {
        Err(error) => assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "lettura: {error:?}"),
        Ok(count) => accepted.push(format!("lettura ORDER BY ga: {count} righe")),
    }
    assert!(accepted.is_empty(), "ammesse: {accepted:?}");
    // I controlli: le colonne scalari della stessa tabella e una tabella
    // senza geometrie, con le righe attese.
    let name = column(Some("t"), "name");
    let admitted = [
        (
            "ORDER BY k su UNION ALL scalare",
            over(
                table,
                &json!({
                    "projection": [item(&id, Some("k"))],
                    "set_operations": [{"operator": "union", "all": true,
                        "query": over(table, &json!({"projection": [item(&id, Some("k"))]}))}],
                    "order_by": order(&k)
                }),
            ),
            none.clone(),
            6,
        ),
        (
            "ORDER BY d.id con una geometria senza alias",
            over_derived(
                &generated(&centroid),
                &json!({
                    "projection": [item(&column(Some("d"), "id"), Some("id"))],
                    "order_by": order(&column(Some("d"), "id"))
                }),
            ),
            none.clone(),
            3,
        ),
        (
            "DISTINCT * scalare",
            over_derived(
                &over(table, &json!({"projection": [item(&name, Some("c"))]})),
                &json!({"distinct": true}),
            ),
            none.clone(),
            3,
        ),
        (
            "LIKE su una colonna scalare",
            over(table, &json!({"filter": like(&name)})),
            probe(ParameterValue::String("riga-%".to_owned())),
            3,
        ),
        (
            "tabella senza geometrie, DISTINCT *",
            over_derived(
                &over(
                    &format!("{table}_scalari"),
                    &json!({"projection": [item(&name, Some("c"))]}),
                ),
                &json!({"distinct": true}),
            ),
            none.clone(),
            1,
        ),
    ];
    for (form, operation, parameters, expected) in admitted {
        let count = rows(
            provider
                .query(&secret, &plan(operation), &parameters, &budget, &cancel)
                .await,
        )
        .await
        .unwrap_or_else(|error| panic!("{form}: {error:?}"));
        assert_eq!(count, expected, "{form}");
    }
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{table};
             DROP TABLE IF EXISTS public.{table}_scalari;
             DROP DOMAIN IF EXISTS public.{table}_dominio;
             DROP TYPE IF EXISTS public.{table}_coppia;"
        ))
        .await
        .expect("pulizia");
}

/// Decima rilettura (25/10, d11a957), sul `PostGIS` reale: le forme ammesse
/// e rese dal server senza errore, ora rifiutate prima di ogni riga.
///
/// - R1: due alias che differiscono soltanto per le maiuscole (`"k"`, `"K"`,
///   il secondo la geometria in WKB), `ORDER BY "K", "k"`: ordinava per byte;
/// - R2: alias di relazione `t` esterno e `T` interno in un `EXISTS`;
/// - R3: il multirange di un range di geometrie (`rngmultitypid`) non era
///   riconosciuto, e `ORDER BY` lo ordinava;
/// - R4: lo SQL compilato dallo SDK si eseguiva senza il catalogo: una
///   colonna geometrica di una tabella non tipizzata o non mappata
///   (`ORDER BY g`, `ORDER BY h`) ordinava per byte. Ora la transazione la
///   giudica sul catalogo (`TransactionScope::operand_catalog`) prima di
///   eseguire, e non esegue niente se la rifiuta.
///
/// I controlli (alias distinti, riferimenti qualificati, colonne scalari,
/// geometrie nei nodi spatial, un `UPDATE` filtrato per chiave) restano, con
/// le righe attese.
#[tokio::test]
async fn live_name_shapes_multiranges_and_the_transaction_catalog_are_judged() {
    use plenora_database_core::facade::{execute_judged, query_judged};
    use plenora_database_core::relational::MutationOperation;
    use plenora_database_core::spatial_operand::{OperandPlan, AMBIGUOUS_NAME_SHAPE};
    use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
    use serde_json::{json, Value};
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_nomi_r10";
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connessione di setup");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{table};
             DROP TABLE IF EXISTS public.{table}_u;
             DROP TYPE IF EXISTS public.{table}_range;
             CREATE TYPE public.{table}_range AS RANGE (subtype = geometry);
             CREATE TABLE public.{table} (
                 id int PRIMARY KEY,
                 g geometry(Point, 4326),
                 h geometry(Point, 4326),
                 name text,
                 gm public.{table}_multirange
             );
             INSERT INTO public.{table}
                 SELECT i, ST_SetSRID(ST_MakePoint(4 - i, 4 - i), 4326),
                        ST_SetSRID(ST_MakePoint(i, i), 4326), 'riga-' || i,
                        public.{table}_multirange(public.{table}_range(
                            ST_MakePoint(i, i), ST_MakePoint(i + 1, i + 1)))
                 FROM generate_series(1, 3) i;
             CREATE TABLE public.{table}_u (id int PRIMARY KEY, g bytea);
             INSERT INTO public.{table}_u VALUES (1, decode('00', 'hex'));"
        ))
        .await
        .expect("fixture");
    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let column = |relation: Option<&str>, field: &str| json!({"kind": "column", "column": {"relation": relation, "field": field}});
    let item =
        |expression: &Value, alias: Option<&str>| json!({"expression": expression, "alias": alias});
    let order = |expressions: &[Value]| {
        Value::Array(
            expressions
                .iter()
                .map(|expression| json!({"expression": expression, "direction": "asc"}))
                .collect(),
        )
    };
    let block = |object: &str, alias: Option<&str>, extra: &Value| {
        let name = alias.unwrap_or(object);
        let mut operation = json!({
            "source": {"object": {"catalog": null, "schema": "public", "object": object}, "alias": alias},
            "projection": [item(&column(Some(name), "id"), Some("id"))],
            "filter": null, "having": null, "row_limit": null
        });
        for (key, value) in extra.as_object().expect("oggetto") {
            operation[key] = value.clone();
        }
        operation
    };
    let plan = |value: Value| -> QueryOperation { serde_json::from_value(value).expect("piano") };
    let binary = probe(ParameterValue::Bytes(vec![0]));
    let none = ParameterBag::default();
    let output = json!({"kind": "spatial_output", "expression": column(Some("t"), "g"), "semantics": "geometry"});
    let u = format!("{table}_u");
    let refused = [
        (
            "R1 alias k e K, ORDER BY K, k",
            block(
                table,
                Some("t"),
                &json!({"projection": [item(&column(Some("t"), "id"), Some("k")), item(&output, Some("K"))],
                    "order_by": order(&[column(None, "K"), column(None, "k")])}),
            ),
            none.clone(),
            AMBIGUOUS_NAME_SHAPE,
        ),
        (
            "R2 t esterno, T interno",
            block(
                table,
                Some("t"),
                &json!({"filter": {"kind": "exists", "query": block(&u, Some("T"), &json!({
                    "projection": [item(&column(Some("T"), "id"), None)],
                    "filter": {"kind": "compare",
                        "left": {"kind": "cast", "expression": column(Some("t"), "g"), "target_type": "binary"},
                        "operator": "eq",
                        "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "binary"}}
                }))}}),
            ),
            binary.clone(),
            AMBIGUOUS_NAME_SHAPE,
        ),
        (
            "R3 ORDER BY multirange di geometrie",
            block(
                table,
                Some("t"),
                &json!({"order_by": order(&[column(Some("t"), "gm")])}),
            ),
            none.clone(),
            GEOMETRY_SCALAR_OPERAND,
        ),
    ];
    let mut accepted = Vec::new();
    for (form, operation, parameters, reason) in &refused {
        match rows(
            provider
                .query(
                    &secret,
                    &plan(operation.clone()),
                    parameters,
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await
        {
            Err(error) => {
                assert_eq!(
                    error.category,
                    ErrorCategory::Unsupported,
                    "{form}: {error:?}"
                );
                // Una forma di nomi su una relazione con geometrie si rifiuta come forma o
                // come operando geometrico, che la stessa visita puo trovare prima.
                assert!(
                    error.message == *reason
                        || (*reason == AMBIGUOUS_NAME_SHAPE
                            && error.message == GEOMETRY_SCALAR_OPERAND),
                    "{form}: {error:?}"
                );
            }
            Ok(count) => accepted.push(format!("{form}: {count} righe")),
        }
    }
    assert!(accepted.is_empty(), "ammesse: {accepted:?}");
    // R4: il percorso dello SDK, nella transazione, con il piano.
    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await
        .expect("transazione");
    let statement = Statement::new(format!("SELECT id FROM public.{table} ORDER BY g"));
    let untyped = plan(block(
        table,
        None,
        &json!({"order_by": order(&[column(Some(table), "g")])}),
    ));
    let unmapped = plan(block(
        table,
        None,
        &json!({"order_by": order(&[column(None, "h")])}),
    ));
    for (form, operation) in [
        ("ORDER BY g non tipizzata", &untyped),
        ("ORDER BY h non mappata", &unmapped),
    ] {
        let error = query_judged(
            tx.as_mut(),
            &statement,
            Some(&OperandPlan::Query(operation)),
            &cancel,
        )
        .await
        .expect_err(form);
        assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{form}: {error:?}");
    }
    let portable: plenora_database_core::portable::PortableStatement =
        serde_json::from_value(json!({
            "type": "select",
            "table": {"schema": "public", "name": table},
            "projection": {"kind": "columns", "value": ["id"]},
            "order_by": [{"column": "g", "direction": "asc"}]
        }))
        .expect("portable");
    let error =
        plenora_database_core::facade::execute_portable_returning(tx.as_mut(), &portable, &cancel)
            .await
            .expect_err("portable ORDER BY g");
    assert_eq!(
        error.message, GEOMETRY_SCALAR_OPERAND,
        "portable: {error:?}"
    );
    // I controlli nella stessa transazione: una query scalare con una
    // geometria in un nodo spatial e un UPDATE filtrato per chiave.
    let ordered = plan(block(
        table,
        None,
        &json!({"order_by": order(&[column(Some(table), "id")]),
            "filter": {"kind": "spatial", "function": "intersects",
                "arguments": [column(Some(table), "g"), column(Some(table), "g")]}}),
    ));
    let rows_seen = query_judged(
        tx.as_mut(),
        &Statement::new(format!(
            "SELECT id FROM public.{table} WHERE ST_Intersects(g, g) ORDER BY id"
        )),
        Some(&OperandPlan::Query(&ordered)),
        &cancel,
    )
    .await
    .expect("controllo scalare");
    assert_eq!(rows_seen.len(), 3);
    let update: MutationOperation = serde_json::from_value(json!({
        "type": "update",
        "target": {"schema": "public", "object": table},
        "assignments": [{"column": "name", "value": {"kind": "parameter", "name": "v"}}],
        "filter": {"kind": "compare", "left": column(None, "id"), "operator": "eq",
            "right": {"kind": "parameter", "name": "k"}}
    }))
    .expect("update");
    let affected = execute_judged(
        tx.as_mut(),
        &Statement::new(format!("UPDATE public.{table} SET name = $1 WHERE id = $2")).with_params(
            vec![
                ParameterValue::String("nuovo".to_owned()),
                ParameterValue::I32(2),
            ],
        ),
        Some(&OperandPlan::Mutation(&update)),
        &cancel,
    )
    .await
    .expect("UPDATE per chiave");
    assert_eq!(affected, 1);
    tx.rollback(&cancel).await.expect("rollback");
    // Il controllo di R1 e R2 con alias distinti e riferimenti qualificati.
    let admitted = block(
        table,
        Some("t"),
        &json!({"projection": [item(&column(Some("t"), "id"), Some("k")), item(&output, Some("w"))],
            "order_by": order(&[column(None, "k")]),
            "filter": {"kind": "exists", "query": block(&u, Some("x"), &json!({
                "projection": [item(&column(Some("x"), "id"), None)],
                "filter": {"kind": "compare", "left": column(Some("x"), "id"), "operator": "eq",
                    "right": column(Some("t"), "id")}}))}}),
    );
    let count = rows(
        provider
            .query(&secret, &plan(admitted), &none, &budget, &cancel)
            .await,
    )
    .await
    .expect("controllo alias distinti");
    assert_eq!(count, 1);
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{table};
             DROP TABLE IF EXISTS public.{table}_u;
             DROP TYPE IF EXISTS public.{table}_range;"
        ))
        .await
        .expect("pulizia");
}

/// Regola B (`spatial_operand::catalog_column_is_geometric`), rilettura del
/// 25/10, sul `PostGIS` reale: una vista rende la geometria come `bytea`
/// (`ST_AsBinary(g)`, `g::bytea`, un dominio su `bytea`) e il server ne
/// confronta i byte. Due poligoni uguali per `ST_Equals` con i vertici in
/// ordine diverso: lo SQL senza giudizio `w = ST_AsBinary(...)` rende soltanto
/// il primo, un risultato sbagliato senza errore. Il giudizio rifiuta la
/// colonna `bytea` di una vista e di una vista materializzata (anche quella
/// che ripete una colonna `bytea` della tabella: il costo dichiarato), nel
/// path query, nella transazione e nella lettura; la colonna `bytea` della
/// tabella, l'id e il testo WKT della vista restano.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_binary_columns_of_derived_relations_are_judged_geometric() {
    use plenora_database_core::facade::query_judged;
    use plenora_database_core::plan::{OrderBy, SortDirection};
    use plenora_database_core::spatial_operand::OperandPlan;
    use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
    use serde_json::{json, Value};
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let table = "plenora_derivate_b";
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connessione di setup");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let cleanup = format!(
        "DROP MATERIALIZED VIEW IF EXISTS public.{table}_mv;
         DROP VIEW IF EXISTS public.{table}_v;
         DROP TABLE IF EXISTS public.{table};
         DROP DOMAIN IF EXISTS public.{table}_wkb;"
    );
    client
        .batch_execute(&cleanup)
        .await
        .expect("pulizia iniziale");
    client
        .batch_execute(&format!(
            "CREATE DOMAIN public.{table}_wkb AS bytea;
             CREATE TABLE public.{table} (id int PRIMARY KEY, b bytea, g geometry(Polygon, 4326));
             INSERT INTO public.{table} VALUES
                 (3, decode('03', 'hex'), ST_GeomFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)),
                 (4, decode('04', 'hex'), ST_GeomFromText('POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))', 4326));
             CREATE VIEW public.{table}_v AS SELECT id, ST_AsBinary(g) AS w, g::bytea AS e,
                 ST_AsBinary(g)::public.{table}_wkb AS d, b, ST_AsText(g) AS x
                 FROM public.{table};
             CREATE MATERIALIZED VIEW public.{table}_mv AS SELECT id, ST_AsBinary(g) AS w
                 FROM public.{table};"
        ))
        .await
        .expect("fixture");
    // I due poligoni sono la stessa geometria; i loro WKB no.
    let equal: i64 = client
        .query_one(
            &format!(
                "SELECT count(*) FROM public.{table} a, public.{table} b \
                 WHERE a.id = 3 AND b.id = 4 AND ST_Equals(a.g, b.g)"
            ),
            &[],
        )
        .await
        .expect("ST_Equals")
        .get(0);
    assert_eq!(equal, 1);
    let wkb: Vec<u8> = client
        .query_one(
            &format!("SELECT ST_AsBinary(g) FROM public.{table} WHERE id = 3"),
            &[],
        )
        .await
        .expect("WKB")
        .get(0);
    let silent: Vec<i32> = client
        .query(
            &format!("SELECT id FROM public.{table}_v WHERE w = $1 ORDER BY id"),
            &[&wkb],
        )
        .await
        .expect("senza giudizio")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(silent, vec![3], "il server confronta i byte");

    let provider = PostgresProvider::insecure_local_with_batch_rows(1_024);
    let secret = SecretString::new(dsn.clone());
    let cancel = CancellationToken::new();
    let budget = budget();
    let column =
        |field: &str| json!({"kind": "column", "column": {"relation": "t", "field": field}});
    let over = |object: &str, extra: &Value| {
        let mut operation = json!({
            "source": {"object": {"catalog": null, "schema": "public", "object": object}, "alias": "t"},
            "projection": [{"expression": column("id"), "alias": "id"}],
            "filter": null, "having": null, "row_limit": null
        });
        for (key, value) in extra.as_object().expect("oggetto") {
            operation[key] = value.clone();
        }
        operation
    };
    let equal_to = |field: &str, parameter_type: &str| {
        json!({"filter": {"kind": "compare", "left": column(field), "operator": "eq",
            "right": {"kind": "typed_parameter", "name": "p", "parameter_type": parameter_type}}})
    };
    let ordered =
        |field: &str| json!({"order_by": [{"expression": column(field), "direction": "asc"}]});
    let plan = |value: Value| -> QueryOperation { serde_json::from_value(value).expect("piano") };
    let view = format!("{table}_v");
    let materialized = format!("{table}_mv");
    let bytes = probe(ParameterValue::Bytes(wkb.clone()));
    let none = ParameterBag::default();
    let refused = [
        (
            "w = ? sulla vista",
            over(&view, &equal_to("w", "binary")),
            bytes.clone(),
        ),
        (
            "ORDER BY w sulla vista",
            over(&view, &ordered("w")),
            none.clone(),
        ),
        (
            "DISTINCT w sulla vista",
            over(
                &view,
                &json!({"distinct": true, "projection": [{"expression": column("w"), "alias": "w"}]}),
            ),
            none.clone(),
        ),
        (
            "e (g::bytea) = ?",
            over(&view, &equal_to("e", "binary")),
            bytes.clone(),
        ),
        (
            "d (dominio su bytea) = ?",
            over(&view, &equal_to("d", "binary")),
            bytes.clone(),
        ),
        (
            "b della vista = ?",
            over(&view, &equal_to("b", "binary")),
            bytes.clone(),
        ),
        (
            "w = ? sulla vista materializzata",
            over(&materialized, &equal_to("w", "binary")),
            bytes.clone(),
        ),
    ];
    for (form, operation, parameters) in &refused {
        let error = rows(
            provider
                .query(
                    &secret,
                    &plan(operation.clone()),
                    parameters,
                    &budget,
                    &cancel,
                )
                .await,
        )
        .await
        .expect_err(form);
        assert_eq!(
            error.category,
            ErrorCategory::Unsupported,
            "{form}: {error:?}"
        );
        assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{form}: {error:?}");
    }
    let admitted = [
        (
            "b = ? sulla tabella",
            over(table, &equal_to("b", "binary")),
            probe(ParameterValue::Bytes(vec![4])),
            1,
        ),
        (
            "id = ? sulla vista",
            over(&view, &equal_to("id", "integer")),
            probe(ParameterValue::I32(3)),
            1,
        ),
        (
            "x LIKE ? sulla vista (testo)",
            over(
                &view,
                &json!({"filter": {"kind": "like", "expression": column("x"),
                    "pattern": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}}}),
            ),
            probe(ParameterValue::String("POLYGON%".to_owned())),
            2,
        ),
        (
            "ORDER BY id sulla vista materializzata",
            over(&materialized, &ordered("id")),
            none.clone(),
            2,
        ),
    ];
    for (form, operation, parameters, expected) in admitted {
        let count = rows(
            provider
                .query(&secret, &plan(operation), &parameters, &budget, &cancel)
                .await,
        )
        .await
        .unwrap_or_else(|error| panic!("{form}: {error:?}"));
        assert_eq!(count, expected, "{form}");
    }
    // La transazione (SDK, facade, CLI portable): lo stesso giudizio.
    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await
        .expect("transazione");
    let judged = plan(over(&view, &equal_to("w", "binary")));
    let error = query_judged(
        tx.as_mut(),
        &Statement::new(format!("SELECT t.id FROM public.{view} t WHERE t.w = $1"))
            .with_params(vec![ParameterValue::Bytes(wkb)]),
        Some(&OperandPlan::Query(&judged)),
        &cancel,
    )
    .await
    .expect_err("w = ? nella transazione");
    assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{error:?}");
    tx.rollback(&cancel).await.expect("rollback");
    // La lettura del provider.
    let mut reading = read(&view, "w");
    reading.filter = None;
    reading.order_by = vec![OrderBy {
        field: "w".to_owned(),
        direction: SortDirection::Asc,
    }];
    let error = rows(
        provider
            .read(&secret, &reading, &none, &budget, &cancel)
            .await,
    )
    .await
    .expect_err("lettura ORDER BY w");
    assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "lettura: {error:?}");
    reading.order_by[0].field = "id".to_owned();
    assert_eq!(
        rows(
            provider
                .read(&secret, &reading, &none, &budget, &cancel)
                .await
        )
        .await
        .expect("lettura ORDER BY id"),
        2
    );
    client.batch_execute(&cleanup).await.expect("pulizia");
}
