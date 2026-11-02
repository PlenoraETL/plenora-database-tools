//! Controllo offline delle restrizioni del writer, senza dedurre successi dal server.
use crate::write_probe::{battery, ProbeSpec};
use plenora_database_core::arrow::array::Array;
use plenora_database_core::arrow::schema::{DataType, TimeUnit};
use plenora_database_core::write_admission::{arrow_key, AdmissionFixture, MeasuredKey};

/// Visita ogni valore di ogni chiave; una guardia pura puo soltanto aggiungere rifiuti.
/// I rifiuti del server non devono essere riproducibili senza server.
///
/// # Panics
/// Se manca una batteria o il profilo ammette un valore rifiutato dal writer.
pub fn check(fixture: &[u8], guard: impl Fn(&MeasuredKey, &ProbeSpec, &dyn Array, usize) -> bool) {
    let fixture: AdmissionFixture = serde_json::from_slice(fixture).expect("profilo");
    let types = battery_types();
    let mut missing = Vec::new();
    for measurement in &fixture.misure {
        for key in &measurement.chiavi {
            let arrow = types
                .iter()
                .find(|t| arrow_key(t) == key.arrow)
                .expect("tipo batteria");
            let mut spec = ProbeSpec::new(&key.nativo, arrow.clone());
            spec.dimension = key.dimensione;
            if key.nativo.eq_ignore_ascii_case("JSON") || key.nativo.contains(":json") {
                spec = spec.document();
            }
            if key.nativo == "uniqueidentifier" || key.nativo == "scalare:uuid;UTF8" {
                spec = spec.with_text_domain(crate::write_probe::TextDomain::Uuid);
            }
            if key.nativo.starts_with("datetimeoffset") && key.arrow == "utf8" {
                spec = spec.with_text_domain(crate::write_probe::TextDomain::OffsetInstant);
            }
            let battery = battery(&spec).expect("batteria");
            assert!(
                key.rifiutati
                    .iter()
                    .chain(&key.riscritti)
                    .chain(&key.per_eccesso)
                    .all(|name| battery.names.contains(name)),
                "batteria diversa: {} {}",
                key.arrow,
                key.nativo
            );
            let refused: Vec<_> = battery
                .names
                .iter()
                .enumerate()
                .filter(|(row, name)| {
                    guard(key, &spec, battery.values.as_ref(), *row)
                        && !key.rifiutati.contains(name)
                })
                .map(|(_, name)| name.clone())
                .collect();
            let fully_refused = (0..battery.values.len())
                .all(|row| guard(key, &spec, battery.values.as_ref(), row));
            if !refused.is_empty()
                || (fully_refused
                    && key.esito
                        != plenora_database_core::write_admission::MeasuredOutcome::Rifiuta)
            {
                missing.push(serde_json::json!({"major": measurement.server_major,
                    "before": key, "refused": refused, "battery": battery.names, "fully_refused": fully_refused}));
            }
        }
    }
    if let Ok(directory) = std::env::var("PLENORA_PROFILE_GUARD_REPORT") {
        std::fs::write(
            std::path::Path::new(&directory).join(format!("{}.json", fixture.provider)),
            serde_json::to_vec_pretty(&missing).unwrap(),
        )
        .expect("referto guardie");
    }
    assert!(
        missing.is_empty(),
        "rifiuti del writer assenti dal profilo: {}",
        serde_json::to_string(&missing).unwrap()
    );
}

fn battery_types() -> Vec<DataType> {
    let mut types = vec![
        DataType::Int8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Interval(plenora_database_core::arrow::schema::IntervalUnit::MonthDayNano),
        DataType::Boolean,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
        DataType::Time64(TimeUnit::Microsecond),
    ];
    for precision in 1..=38 {
        for scale in 0..=precision {
            types.push(DataType::Decimal128(
                precision,
                i8::try_from(scale).unwrap(),
            ));
        }
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, None));
        for zone in ["UTC", "+00:00", "Europe/Rome"] {
            types.push(DataType::Timestamp(unit, Some(zone.into())));
        }
    }
    types
}
