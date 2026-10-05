use crate::{OracleColumn, OracleColumnKind, OracleColumnSpec};
use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::protocol::GEOMETRY_SPATIAL_SEMANTICS;

fn column(data_type: &str) -> OracleColumn {
    OracleColumn {
        name: "TS".to_owned(),
        ordinal: 1,
        data_type: data_type.to_owned(),
        data_length: 11,
        char_length: 0,
        precision: None,
        scale: None,
        nullable: false,
        default_expression: None,
        identity: false,
        virtual_column: false,
        spatial_srid: None,
        spatial_dimensions: None,
        spatial_semantics: None,
    }
}

#[test]
fn geography_catalog_semantics_reaches_the_arrow_contract() {
    let mut spatial = column("SDO_GEOMETRY");
    spatial.spatial_srid = Some(4326);
    spatial.spatial_dimensions = Some(2);
    spatial.spatial_semantics = Some(SpatialSemantics::Geography);
    let field = OracleColumnSpec::from_catalog(&spatial)
        .expect("geography Oracle catalogata")
        .arrow_field()
        .expect("campo Arrow geography");
    assert_eq!(
        field.metadata().get(GEOMETRY_SPATIAL_SEMANTICS),
        Some(&"geography".to_owned())
    );
}

#[test]
fn timestamp_precision_is_generic_but_local_time_zone_stays_closed() {
    let timestamp = OracleColumnSpec::from_catalog(&column("TIMESTAMP(9)"))
        .expect("TIMESTAMP con precisione Oracle");
    assert_eq!(timestamp.kind, OracleColumnKind::DateTime);

    let timestamp_tz = OracleColumnSpec::from_catalog(&column("TIMESTAMP(3) WITH TIME ZONE"))
        .expect("TIMESTAMP WITH TIME ZONE Oracle");
    assert_eq!(timestamp_tz.kind, OracleColumnKind::TimestampTz);

    OracleColumnSpec::from_catalog(&column("TIMESTAMP(6) WITH LOCAL TIME ZONE"))
        .expect_err("TIMESTAMP WITH LOCAL TIME ZONE non qualificato");
}

#[test]
fn geometry_spec_without_srid_is_an_error_not_a_panic() {
    // Una spec costruita a mano, fuori da `from_catalog`, puo mancare dello
    // SRID: prima `arrow_field` andava in panic, ora rifiuta con `Crs`.
    let spec = OracleColumnSpec {
        name: "SHAPE".to_owned(),
        native_type: "SDO_GEOMETRY".to_owned(),
        nullable: true,
        kind: OracleColumnKind::Geometry,
        spatial_srid: None,
        spatial_dimensions: Some(2),
        spatial_semantics: Some(SpatialSemantics::Geometry),
    };
    let error = spec.arrow_field().expect_err("geometry senza SRID");
    assert_eq!(error.category, plenora_database_core::ErrorCategory::Crs);
}
