//! Microbenchmark offline della compilazione del read plan SQL Server.
//!
//! Superficie misurata: `SqlServerReadPlan::compile`, cioe' catalogo ->
//! proiezione T-SQL deterministica + schema Arrow contrattuale. La
//! descrizione dell'oggetto e' sintetizzata qui: nessun TDS, nessun server.
//!
//! La proiezione SQL Server e' piu' costosa di quella `MySQL` perche' molte
//! colonne vengono convertite esplicitamente (`CONVERT(varchar(...))` per
//! decimal e temporali, WKB per le spatial): lo scenario `wide` serve a
//! rendere visibile quel costo per colonna.
//!
//! Uso: `bench_sqlserver_read_plan [iterazioni] [ripetizioni]`.
//! Emette una riga JSON per scenario su stdout (JSONL).
//!
//! Nessuna soglia e' codificata qui: il binario misura e basta. I budget
//! prestazionali sono una decisione di prodotto e vanno fissati altrove.

use plenora_db_sqlserver::{
    SqlServerColumn, SqlServerObjectDescription, SqlServerReadPlan, SqlServerSchemaToken,
};
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

/// Colonna di catalogo con i soli campi che `from_catalog` interpreta.
fn column(index: usize, native_type: &str, precision: u8, scale: u8) -> SqlServerColumn {
    SqlServerColumn {
        ordinal: i32::try_from(index).expect("ordinale rappresentabile") + 1,
        name: format!("c{index}"),
        type_schema: "sys".to_owned(),
        native_type: native_type.to_owned(),
        max_length: 8,
        precision,
        scale,
        nullable: true,
        identity: false,
        computed: false,
        generated_always_type: 0,
        collation: Some("SQL_Latin1_General_CP1_CI_AS".to_owned()),
        default_definition: None,
        computed_definition: None,
        computed_persisted: false,
    }
}

/// Descrizione con `scalars` colonne scalari e `geometries` colonne spatial.
fn description(scalars: usize, geometries: usize) -> SqlServerObjectDescription {
    let scalar_types: [(&str, u8, u8); 5] = [
        ("bigint", 19, 0),
        ("nvarchar", 0, 0),
        ("float", 53, 0),
        ("datetime2", 27, 7),
        ("decimal", 38, 12),
    ];
    let mut columns: Vec<SqlServerColumn> = (0..scalars)
        .map(|index| {
            let (native_type, precision, scale) = scalar_types[index % scalar_types.len()];
            column(index, native_type, precision, scale)
        })
        .collect();
    columns.extend((0..geometries).map(|index| column(scalars + index, "geometry", 0, 0)));
    SqlServerObjectDescription {
        database_id: 1,
        object_id: 2,
        catalog: "dataflow_test".to_owned(),
        schema: "dbo".to_owned(),
        name: "events".to_owned(),
        kind: "USER_TABLE".to_owned(),
        temporal_type: 0,
        temporal: None,
        graph_kind: None,
        external: None,
        partitioning: None,
        owner: "dbo".to_owned(),
        security_predicates: Vec::new(),
        permissions: Vec::new(),
        view: None,
        memory_optimized: false,
        durability: None,
        columns,
        constraints: Vec::new(),
        indexes: Vec::new(),
        token: SqlServerSchemaToken {
            schema_version: 1,
            database_id: 1,
            object_id: 2,
            structural_fingerprint: "benchmark-offline".to_owned(),
        },
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
            "bench": "sqlserver_read_plan",
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

    for (name, description) in [
        ("compile_narrow_8c", &narrow),
        ("compile_wide_128c", &wide),
        ("compile_spatial_16c", &spatial),
    ] {
        let columns = description.columns.len();
        run_scenario(name, columns, iterations, repetitions, || {
            SqlServerReadPlan::compile(description)
                .expect("piano SQL Server compilabile")
                .sql
                .len()
        });
    }
}
