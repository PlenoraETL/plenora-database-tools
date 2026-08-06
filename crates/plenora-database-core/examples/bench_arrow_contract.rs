//! Microbenchmark offline della validazione del contratto Arrow.
//!
//! Superficie misurata: `field_contract::validate_schema_contract` e
//! `FieldContract::parse`. E' il confine Arrow del workspace: ogni schema
//! prodotto da un provider o riletto da un dataset IPC ci passa attraverso,
//! e il costo scala con il numero di campi e di metadati geometrici.
//!
//! Uso: `bench_arrow_contract [iterazioni] [ripetizioni]`.
//! Emette una riga JSON per scenario su stdout (JSONL).
//!
//! Nessuna soglia e' codificata qui: il binario misura e basta. I budget
//! prestazionali sono una decisione di prodotto e vanno fissati altrove.

use plenora_database_core::arrow::schema::{DataType, Field, Schema, TimeUnit};
use plenora_database_core::field_contract::{validate_schema_contract, FieldContract};
use plenora_database_core::protocol;
use serde_json::json;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

/// Campo scalare con i soli metadati nativi che i provider allegano sempre.
fn scalar_field(index: usize) -> Field {
    let data_type = match index % 5 {
        0 => DataType::Int64,
        1 => DataType::Utf8,
        2 => DataType::Float64,
        3 => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => DataType::Decimal128(38, 12),
    };
    let metadata = HashMap::from([
        (protocol::POSTGRES_NATIVE_TYPE.to_owned(), "int8".to_owned()),
        (protocol::FIELD_ID.to_owned(), index.to_string()),
    ]);
    Field::new(format!("c{index}"), data_type, true).with_metadata(metadata)
}

/// Campo geometrico con il set canonico completo richiesto da
/// `validate_current`: e' il caso peggiore per campo.
fn geometry_field(index: usize) -> Field {
    let metadata = HashMap::from([
        (
            protocol::GEOARROW_EXTENSION_NAME.to_owned(),
            "geoarrow.wkb".to_owned(),
        ),
        (protocol::GEOMETRY_ENCODING.to_owned(), "ewkb".to_owned()),
        (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
        (
            protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
            "exact".to_owned(),
        ),
        (protocol::GEOMETRY_TYPES.to_owned(), "polygon".to_owned()),
        (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
        (
            protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
            "declared_unresolved".to_owned(),
        ),
        (
            protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
            "geometry".to_owned(),
        ),
        (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
        (protocol::FIELD_ID.to_owned(), index.to_string()),
    ]);
    Field::new(format!("geom{index}"), DataType::Binary, true).with_metadata(metadata)
}

/// Schema con `scalars` campi scalari e `geometries` campi geometrici.
fn schema(scalars: usize, geometries: usize) -> Schema {
    let mut fields: Vec<Field> = (0..scalars).map(scalar_field).collect();
    fields.extend((0..geometries).map(|index| geometry_field(scalars + index)));
    Schema::new_with_metadata(
        fields,
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
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
fn run_scenario(
    name: &str,
    fields: usize,
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
    let fields_f64 = fields as f64;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "bench": "arrow_contract",
            "scenario": name,
            "fields": fields,
            "iterations": iterations,
            "repetitions": repetitions,
            "median_seconds": median,
            "nanoseconds_per_operation": median / iterations_f64 * 1e9,
            "nanoseconds_per_field": median / iterations_f64 / fields_f64 * 1e9,
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

    let narrow = schema(8, 1);
    let wide = schema(120, 8);
    let spatial_heavy = schema(8, 56);

    for (name, schema) in [
        ("validate_schema_narrow_9f", &narrow),
        ("validate_schema_wide_128f", &wide),
        ("validate_schema_spatial_64f", &spatial_heavy),
    ] {
        let fields = schema.fields().len();
        run_scenario(name, fields, iterations, repetitions, || {
            validate_schema_contract(schema).expect("schema conforme");
            fields
        });
    }

    let fields = wide.fields().len();
    run_scenario(
        "parse_field_contract_wide_128f",
        fields,
        iterations,
        repetitions,
        || {
            wide.fields()
                .iter()
                .map(|field| {
                    usize::from(FieldContract::parse(field).expect("campo conforme").spatial)
                })
                .sum()
        },
    );
}
