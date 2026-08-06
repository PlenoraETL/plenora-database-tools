//! Microbenchmark offline dell'ispezione EWKB.
//!
//! Superficie misurata: `plenora_database_core::ewkb::inspect_ewkb_detailed`.
//! E' il costo per cella geometrica sul percorso di lettura di ogni provider
//! spatial, e non richiede alcun database: le EWKB sono sintetizzate qui.
//!
//! Le forme coprono i quattro regimi che il parser attraversa: header
//! dominante (point), salto di un blocco di coordinate dichiarate
//! (linestring), anelli multipli in una sola geometria (polygon) e
//! attraversamento di figli (multipolygon, geometrycollection annidata).
//!
//! Uso: `bench_ewkb [iterazioni] [ripetizioni]`.
//! Emette una riga JSON per scenario su stdout (JSONL).
//!
//! Non viene riportato un throughput in byte: lo scanner non legge le
//! coordinate, le salta con un controllo di lunghezza. Un rapporto
//! byte/secondo darebbe numeri enormi e privi di significato; il costo reale
//! dipende dal numero di header attraversati, non dalla dimensione del
//! payload. `payload_bytes` resta nel record solo per identificare la forma.
//!
//! Nessuna soglia e' codificata qui: il binario misura e basta. I budget
//! prestazionali sono una decisione di prodotto e vanno fissati altrove.

use plenora_database_core::ewkb::inspect_ewkb_detailed;
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

/// Limiti usati dalla CLI su `inspect-dataset`: stesso perimetro del gate.
const MAX_COMPONENTS: u64 = 16_777_216;
const MAX_DEPTH: u64 = 64;

const FLAG_SRID: u32 = 0x2000_0000;

/// Serializza l'header EWKB little-endian di una geometria.
fn push_header(out: &mut Vec<u8>, base_type: u32, srid: Option<u32>) {
    out.push(1);
    let type_word = base_type | if srid.is_some() { FLAG_SRID } else { 0 };
    out.extend_from_slice(&type_word.to_le_bytes());
    if let Some(srid) = srid {
        out.extend_from_slice(&srid.to_le_bytes());
    }
}

/// Coordinate deterministiche: nessun RNG, la forma del payload e' fissa.
fn push_points(out: &mut Vec<u8>, count: u32) {
    out.extend_from_slice(&count.to_le_bytes());
    for index in 0..count {
        let x = f64::from(index) * 0.5;
        let y = f64::from(index) * 0.25;
        out.extend_from_slice(&x.to_le_bytes());
        out.extend_from_slice(&y.to_le_bytes());
    }
}

fn point(srid: Option<u32>) -> Vec<u8> {
    let mut out = Vec::new();
    push_header(&mut out, 1, srid);
    out.extend_from_slice(&1.5_f64.to_le_bytes());
    out.extend_from_slice(&2.5_f64.to_le_bytes());
    out
}

fn linestring(points: u32, srid: Option<u32>) -> Vec<u8> {
    let mut out = Vec::new();
    push_header(&mut out, 2, srid);
    push_points(&mut out, points);
    out
}

fn polygon(rings: u32, points_per_ring: u32, srid: Option<u32>) -> Vec<u8> {
    let mut out = Vec::new();
    push_header(&mut out, 3, srid);
    out.extend_from_slice(&rings.to_le_bytes());
    for _ in 0..rings {
        push_points(&mut out, points_per_ring);
    }
    out
}

fn multipolygon(parts: u32, rings: u32, points_per_ring: u32) -> Vec<u8> {
    let mut out = Vec::new();
    push_header(&mut out, 6, Some(4326));
    out.extend_from_slice(&parts.to_le_bytes());
    for _ in 0..parts {
        out.extend_from_slice(&polygon(rings, points_per_ring, None));
    }
    out
}

/// `GeometryCollection` annidata: esercita lo stack di frame del parser.
fn nested_collection(depth: u32, leaf_points: u32) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..depth {
        push_header(&mut out, 7, None);
        out.extend_from_slice(&1_u32.to_le_bytes());
    }
    out.extend_from_slice(&linestring(leaf_points, None));
    out
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

/// Esegue `iterations` ispezioni per ripetizione e riporta la mediana.
fn run_scenario(name: &str, payload: &[u8], iterations: usize, repetitions: usize) {
    let inspect = || {
        let inspection = inspect_ewkb_detailed(payload, MAX_COMPONENTS, MAX_DEPTH)
            .expect("EWKB sintetica valida");
        usize::try_from(inspection.stats.components).unwrap_or(usize::MAX)
    };
    black_box(inspect());
    let mut durations = Vec::with_capacity(repetitions);
    let mut checksum: usize = 0;
    for _ in 0..repetitions {
        let start = Instant::now();
        for _ in 0..iterations {
            checksum = checksum.wrapping_add(inspect());
        }
        durations.push(start.elapsed().as_secs_f64());
    }
    black_box(checksum);
    durations.sort_by(f64::total_cmp);
    let median = durations[durations.len() / 2];
    // Conteggi di benchmark: ben sotto 2^53, la conversione e' esatta.
    #[allow(clippy::cast_precision_loss)]
    let iterations_f64 = iterations as f64;
    let stats = inspect_ewkb_detailed(payload, MAX_COMPONENTS, MAX_DEPTH).expect("EWKB valida");
    println!(
        "{}",
        serde_json::to_string(&json!({
            "bench": "ewkb_inspect",
            "scenario": name,
            "payload_bytes": payload.len(),
            "components": stats.stats.components,
            "max_depth": stats.stats.max_depth,
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
        .unwrap_or("2000")
        .parse()
        .expect("iterazioni");
    let repetitions: usize = args
        .next()
        .as_deref()
        .unwrap_or("9")
        .parse()
        .expect("ripetizioni");

    let cases = [
        ("point_srid", point(Some(4326))),
        ("linestring_64", linestring(64, Some(4326))),
        ("linestring_4096", linestring(4_096, Some(4326))),
        ("polygon_4rings_256", polygon(4, 256, Some(4326))),
        ("multipolygon_64x2x128", multipolygon(64, 2, 128)),
        ("collection_depth32", nested_collection(32, 16)),
    ];
    for (name, payload) in &cases {
        run_scenario(name, payload, iterations, repetitions);
    }
}
