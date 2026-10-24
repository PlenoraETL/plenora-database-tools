use super::*;
use crate::geometry::{Dimensions, SpatialSemantics};

const PG: ProviderKind = ProviderKind::Postgres;

fn wkb_point() -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes
}

fn ewkb_point(srid: u32) -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
    bytes.extend_from_slice(&srid.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
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

fn category(result: Result<impl std::fmt::Debug>) -> ErrorCategory {
    result.expect_err("rifiuto atteso").category
}

/// Una sonda senza SRID non si confronta con niente: era il WKB nudo che
/// `PostGIS` riceveva con SRID 0 e confrontava senza errore.
#[test]
fn a_probe_without_srid_is_refused() {
    assert_eq!(
        category(probe_srid(PG, &ParameterValue::Bytes(wkb_point()), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(
        category(probe_srid(PG, &wkb(wkb_point(), None), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(
        category(probe_srid(PG, &wkb(wkb_point(), Some(0)), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(
        category(probe_srid(PG, &ParameterValue::Bytes(ewkb_point(0)), 32)),
        ErrorCategory::Crs
    );
}

#[test]
fn the_probe_srid_comes_from_the_value_or_the_ewkb() {
    assert_eq!(
        probe_srid(PG, &ParameterValue::Bytes(ewkb_point(3857)), 32).unwrap(),
        Some(3857)
    );
    assert_eq!(
        probe_srid(PG, &wkb(wkb_point(), Some(4326)), 32).unwrap(),
        Some(4326)
    );
    assert_eq!(
        probe_srid(PG, &wkb(ewkb_point(4326), Some(4326)), 32).unwrap(),
        Some(4326)
    );
    assert_eq!(
        category(probe_srid(PG, &wkb(ewkb_point(3857), Some(4326)), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(probe_srid(PG, &ParameterValue::I64(1), 32).unwrap(), None);
}

/// I byte legati portano lo SRID: il server non lo deve indovinare.
#[test]
fn the_bound_probe_carries_its_srid() {
    let (srid, bytes) = stamped_probe(PG, &wkb(wkb_point(), Some(4326)), 32)
        .unwrap()
        .unwrap();
    assert_eq!(srid, 4326);
    assert_eq!(bytes, ewkb_point(4326));
    let (_, untouched) = stamped_probe(PG, &ParameterValue::Bytes(ewkb_point(3857)), 32)
        .unwrap()
        .unwrap();
    assert_eq!(untouched, ewkb_point(3857));
}

/// `ST_SetSRID(ST_GeomFromEWKB(..), 4326)` riscriveva uno SRID 3857
/// incorporato: un valore che ne porta uno diverso si rifiuta.
#[test]
fn a_spatial_value_with_another_srid_is_refused() {
    for value in [
        ParameterValue::Bytes(ewkb_point(3857)),
        wkb(wkb_point(), Some(3857)),
        wkb(ewkb_point(3857), None),
    ] {
        assert_eq!(
            category(check_spatial_value(PG, &value, 4326, 32)),
            ErrorCategory::Crs
        );
    }
    for value in [
        ParameterValue::Bytes(wkb_point()),
        ParameterValue::Bytes(ewkb_point(4326)),
        wkb(wkb_point(), Some(4326)),
        ParameterValue::I64(0),
    ] {
        check_spatial_value(PG, &value, 4326, 32).expect("valore coerente");
    }
}

#[test]
fn frames_must_be_known_and_equal() {
    compare_frames(PG, 4326, Some(4326)).expect("stesso SRID");
    assert_eq!(
        category(compare_frames(PG, 3857, Some(4326))),
        ErrorCategory::Crs
    );
    assert_eq!(category(compare_frames(PG, 4326, None)), ErrorCategory::Crs);
    assert_eq!(
        category(compare_frames(PG, 4326, Some(0))),
        ErrorCategory::Crs
    );
}
