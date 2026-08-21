//! Microbenchmark offline del rendering SQL multi-dialect.
//!
//! Superficie misurata: `Renderer::render_select`, `Renderer::render_filter` e
//! `Renderer::render_query`. E' il costo che ogni operazione paga prima di
//! toccare la rete, quindi resta interamente nel perimetro offline.
//!
//! Le forme sono tre: `narrow` (4 colonne, 2 termini), `wide` (64 colonne, 16
//! termini) e `spatial` (predicato `ST_Intersects`, non rappresentabile in
//! SQL Server senza tipo e SRID risolti e quindi assente per quel dialect).
//!
//! Uso: `bench_sql_render [iterazioni] [ripetizioni]`.
//! Emette una riga JSON per scenario su stdout (JSONL).
//!
//! Nessuna soglia e' codificata qui: il binario misura e basta. I budget
//! prestazionali sono una decisione di prodotto e vanno fissati altrove.

use plenora_database_core::plan::{ComparisonOperator, ObjectRef, SortDirection};
use plenora_database_core::query::{
    ColumnRef, JoinKind, QueryExpression, QueryJoin, QueryOperation, QueryOrdering,
    QueryProjection, QuerySource, ScalarFunction,
};
use plenora_database_sql::{
    Dialect, DialectCapabilities, Expression, Identifier, ObjectName, Ordering, Renderer, Select,
};
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

fn identifier(value: &str) -> Identifier {
    Identifier::new(value).expect("identificatore valido")
}

fn object(schema: &str, name: &str) -> ObjectName {
    ObjectName {
        catalog: None,
        schema: Some(identifier(schema)),
        object: identifier(name),
    }
}

/// SELECT con `columns` proiezioni, `terms` confronti in AND e ordinamento.
fn select(columns: usize, terms: usize) -> Select {
    Select {
        source: object("public", "events"),
        projection: (0..columns).map(|i| identifier(&format!("c{i}"))).collect(),
        filter: Some(Expression::And(
            (0..terms)
                .map(|i| Expression::Compare {
                    field: identifier(&format!("c{}", i % columns)),
                    operator: ComparisonOperator::Gte,
                    parameter: format!("p{i}"),
                })
                .collect(),
        )),
        order_by: vec![Ordering {
            field: identifier("c0"),
            direction: SortDirection::Asc,
        }],
        limit: Some(1_000),
    }
}

/// SELECT con predicato spatial: forma tipica di una lettura geografica.
fn spatial_select() -> Select {
    Select {
        source: object("public", "parcels"),
        projection: vec![identifier("id"), identifier("geom")],
        filter: Some(Expression::And(vec![
            Expression::IsNotNull(identifier("geom")),
            Expression::SpatialIntersects {
                field: identifier("geom"),
                wkb_parameter: "bbox".to_owned(),
            },
        ])),
        order_by: vec![Ordering {
            field: identifier("id"),
            direction: SortDirection::Asc,
        }],
        limit: Some(1_000),
    }
}

/// Albero booleano bilanciato di profondita' `depth`: misura la ricorsione del
/// renderer, non la sola concatenazione di stringhe.
fn boolean_tree(depth: usize, index: &mut usize) -> Expression {
    if depth == 0 {
        *index += 1;
        return Expression::Compare {
            field: identifier("c0"),
            operator: ComparisonOperator::Eq,
            parameter: format!("p{index}"),
        };
    }
    let left = boolean_tree(depth - 1, index);
    let right = boolean_tree(depth - 1, index);
    if depth.is_multiple_of(2) {
        Expression::And(vec![left, right])
    } else {
        Expression::Or(vec![left, right])
    }
}

fn column(relation: &str, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    }
}

/// `QueryOperation` con join, aggregazione, HAVING e ORDER BY: la forma piu'
/// costosa che il renderer accetta senza sottoquery.
fn rich_query() -> QueryOperation {
    let source = |object: &str, alias: &str| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: object.to_owned(),
        },
        alias: Some(alias.to_owned()),
    };
    QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source("events", "e")),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: column("e", "tenant"),
                alias: Some("tenant".to_owned()),
            },
            QueryProjection {
                expression: QueryExpression::Scalar {
                    function: ScalarFunction::Count,
                    arguments: vec![column("e", "id")],
                },
                alias: Some("events".to_owned()),
            },
            QueryProjection {
                expression: QueryExpression::Scalar {
                    function: ScalarFunction::Sum,
                    arguments: vec![column("e", "amount")],
                },
                alias: Some("total".to_owned()),
            },
        ],
        joins: vec![QueryJoin {
            kind: JoinKind::Inner,
            source: Some(source("tenants", "t")),
            derived_source: None,
            lateral: false,
            on: Some(QueryExpression::Compare {
                left: Box::new(column("e", "tenant")),
                operator: ComparisonOperator::Eq,
                right: Box::new(column("t", "id")),
            }),
        }],
        filter: Some(QueryExpression::And {
            arguments: vec![
                QueryExpression::Compare {
                    left: Box::new(column("e", "occurred_at")),
                    operator: ComparisonOperator::Gte,
                    right: Box::new(QueryExpression::Parameter {
                        name: "from_timestamp".to_owned(),
                    }),
                },
                QueryExpression::IsNull {
                    expression: Box::new(column("e", "deleted_at")),
                    negated: true,
                },
            ],
        }),
        group_by: vec![column("e", "tenant")],
        having: Some(QueryExpression::Compare {
            left: Box::new(QueryExpression::Scalar {
                function: ScalarFunction::Count,
                arguments: vec![column("e", "id")],
            }),
            operator: ComparisonOperator::Gt,
            right: Box::new(QueryExpression::Parameter {
                name: "min_events".to_owned(),
            }),
        }),
        order_by: vec![QueryOrdering {
            expression: column("e", "tenant"),
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(1_000),
        row_offset: None,
        locking: None,
    }
}

const fn renderer(dialect: Dialect) -> Renderer {
    Renderer::new(
        dialect,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}

/// Peak RSS del processo; assente fuori da Linux.
fn peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

/// Esegue `iterations` chiamate per ripetizione e riporta la mediana.
fn run_scenario(name: &str, iterations: usize, repetitions: usize, execute: impl Fn() -> usize) {
    black_box(execute());
    let mut durations = Vec::with_capacity(repetitions);
    let mut checksum: usize = 0;
    for _ in 0..repetitions {
        let start = Instant::now();
        for _ in 0..iterations {
            checksum = checksum.wrapping_add(execute());
        }
        durations.push(start.elapsed().as_secs_f64());
    }
    black_box(checksum);
    durations.sort_by(f64::total_cmp);
    let median = durations[durations.len() / 2];
    // Conteggi di benchmark: ben sotto 2^53, la conversione e' esatta.
    #[allow(clippy::cast_precision_loss)]
    let iterations_f64 = iterations as f64;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "bench": "sql_render",
            "scenario": name,
            "iterations": iterations,
            "repetitions": repetitions,
            "median_seconds": median,
            "nanoseconds_per_operation": median / iterations_f64 * 1e9,
            "operations_per_second": iterations_f64 / median,
            "checksum": checksum,
            "peak_rss_kib": peak_rss_kib(),
        }))
        .expect("record JSON serializzabile")
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let iterations: usize = args
        .next()
        .as_deref()
        .unwrap_or("5000")
        .parse()
        .expect("iterazioni");
    let repetitions: usize = args
        .next()
        .as_deref()
        .unwrap_or("9")
        .parse()
        .expect("ripetizioni");

    let narrow = select(4, 2);
    let wide = select(64, 16);
    let spatial = spatial_select();
    let mut index = 0;
    let tree = boolean_tree(8, &mut index);
    let query = rich_query();

    for (label, dialect) in [
        ("postgres", Dialect::Postgres),
        ("mysql", Dialect::Mysql),
        ("sqlserver", Dialect::SqlServer),
    ] {
        let renderer = renderer(dialect);
        run_scenario(
            &format!("render_select_narrow_{label}"),
            iterations,
            repetitions,
            || {
                renderer
                    .render_select(&narrow)
                    .expect("SELECT narrow renderizzabile")
                    .sql
                    .len()
            },
        );
        run_scenario(
            &format!("render_select_wide_{label}"),
            iterations,
            repetitions,
            || {
                renderer
                    .render_select(&wide)
                    .expect("SELECT wide renderizzabile")
                    .sql
                    .len()
            },
        );
        run_scenario(
            &format!("render_filter_tree_depth8_{label}"),
            iterations,
            repetitions,
            || {
                renderer
                    .render_filter(&tree)
                    .expect("albero booleano renderizzabile")
                    .sql
                    .len()
            },
        );
        run_scenario(
            &format!("render_query_join_group_{label}"),
            iterations,
            repetitions,
            || {
                renderer
                    .render_query(&query)
                    .expect("QueryOperation renderizzabile")
                    .sql
                    .len()
            },
        );
    }

    // `ST_Intersects` richiede tipo e SRID risolti in SQL Server: il dialect
    // rifiuta la forma generica, quindi lo scenario spatial resta su
    // PostgreSQL e MySQL.
    for (label, dialect) in [("postgres", Dialect::Postgres), ("mysql", Dialect::Mysql)] {
        let renderer = renderer(dialect);
        run_scenario(
            &format!("render_select_spatial_{label}"),
            iterations,
            repetitions,
            || {
                renderer
                    .render_select(&spatial)
                    .expect("SELECT spatial renderizzabile")
                    .sql
                    .len()
            },
        );
    }
}
