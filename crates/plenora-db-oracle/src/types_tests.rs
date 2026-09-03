use crate::{OracleColumn, OracleColumnKind, OracleColumnSpec};

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
    }
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
