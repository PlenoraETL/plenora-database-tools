//! Microbenchmark offline della compilazione del read plan `MySQL`.
//!
//! Superficie misurata: `MysqlReadPlan::compile`, cioe' catalogo -> proiezione
//! SQL e schema Arrow contrattuale. Gira interamente in memoria: la
//! descrizione dell'oggetto e' sintetizzata qui, nessun server e' coinvolto.
//!
//! Il piano viene ricompilato a ogni preparazione che non trova il token di
//! schema in cache, quindi il costo e' sul percorso caldo delle letture.
//!
//! Uso: `bench_mysql_read_plan [iterazioni] [ripetizioni]`.
//! Emette una riga JSON per scenario su stdout (JSONL).
//!
//! Nessuna soglia e' codificata qui: il binario misura e basta. I budget
//! prestazionali sono una decisione di prodotto e vanno fissati altrove.

use plenora_database_core::plan::{
    FilterExpression, ObjectRef, OrderBy, ReadOperation, SortDirection,
};
use plenora_db_mysql::{MysqlColumn, MysqlObjectDescription, MysqlReadPlan, MysqlSchemaToken};
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

/// Colonna di catalogo con i soli campi che `from_catalog` interpreta.
fn column(index: usize, data_type: &str, declaration: &str, srid: Option<u32>) -> MysqlColumn {
    MysqlColumn {
        name: format!("c{index}"),
        ordinal: u64::try_from(index).expect("ordinale rappresentabile") + 1,
        data_type: data_type.to_owned(),
        native_declaration: declaration.to_owned(),
        nullable: true,
        default_expression: None,
        character_set: None,
        collation: Some("utf8mb4_0900_ai_ci".to_owned()),
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        spatial_srid: srid,
        extra: String::new(),
        generation_expression: String::new(),
    }
}

/// Descrizione con `scalars` colonne scalari e `geometries` colonne spatial.
fn description(scalars: usize, geometries: usize) -> MysqlObjectDescription {
    let scalar_types = [
        ("bigint", "bigint(20)"),
        ("varchar", "varchar(255)"),
        ("double", "double"),
        ("datetime", "datetime(6)"),
        ("date", "date"),
    ];
    let mut columns: Vec<MysqlColumn> = (0..scalars)
        .map(|index| {
            let (data_type, declaration) = scalar_types[index % scalar_types.len()];
            column(index, data_type, declaration, None)
        })
        .collect();
    columns.extend(
        (0..geometries).map(|index| column(scalars + index, "polygon", "polygon", Some(4326))),
    );
    MysqlObjectDescription {
        schema: "dataflow_test".to_owned(),
        name: "events".to_owned(),
        kind: "BASE TABLE".to_owned(),
        engine: Some("InnoDB".to_owned()),
        columns,
        indexes: Vec::new(),
        token: MysqlSchemaToken("mysql-schema-token-benchmark".to_owned()),
    }
}

/// Lettura con proiezione implicita, filtro in AND e ordinamento esplicito.
fn read_operation(terms: usize) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "events".to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "c0".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(10_000),
        filter: Some(FilterExpression::And {
            args: (0..terms)
                .map(|index| FilterExpression::Gte {
                    field: "c0".to_owned(),
                    parameter: format!("p{index}"),
                })
                .collect(),
        }),
    }
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

/// Esegue `iterations` compilazioni per ripetizione e riporta la mediana.
fn run_scenario(
    name: &str,
    columns: usize,
    iterations: usize,
    repetitions: usize,
    execute: impl Fn() -> usize,
) {
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
    #[allow(clippy::cast_precision_loss)]
    let columns_f64 = columns as f64;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "bench": "mysql_read_plan",
            "scenario": name,
            "columns": columns,
            "iterations": iterations,
            "repetitions": repetitions,
            "median_seconds": median,
            "nanoseconds_per_operation": median / iterations_f64 * 1e9,
            "nanoseconds_per_column": median / iterations_f64 / columns_f64 * 1e9,
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
        .unwrap_or("2000")
        .parse()
        .expect("iterazioni");
    let repetitions: usize = args
        .next()
        .as_deref()
        .unwrap_or("9")
        .parse()
        .expect("ripetizioni");

    let narrow = description(8, 0);
    let wide = description(120, 8);
    let spatial = description(8, 8);
    let simple_read = read_operation(2);
    let complex_read = read_operation(32);

    for (name, description, operation) in [
        ("compile_narrow_8c_2f", &narrow, &simple_read),
        ("compile_wide_128c_2f", &wide, &simple_read),
        ("compile_wide_128c_32f", &wide, &complex_read),
        ("compile_spatial_16c_2f", &spatial, &simple_read),
    ] {
        let columns = description.columns.len();
        run_scenario(name, columns, iterations, repetitions, || {
            MysqlReadPlan::compile(description, operation)
                .expect("piano MySQL compilabile")
                .sql
                .len()
        });
    }
}
