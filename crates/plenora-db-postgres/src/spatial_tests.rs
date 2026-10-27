use super::*;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};

/// `POINT(1 1)` WKB senza SRID: `validate` lo accetta con lo SRID dichiarato.
fn dummy_ewkb() -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes
}

fn dummy_reference() -> SpatialReference {
    SpatialReference {
        ewkb: dummy_ewkb(),
        srid: 4326,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    }
}

/// La sonda guardata con i bind del builder: `$1` EWKB, `$2` SRID, `$3`
/// relazione e `$4` colonna di cui il server legge lo SRID.
fn guarded() -> String {
    plenora_database_core::spatial_probe::postgres_guarded_probe(
        "$1", "$2::int4", "$3::text", "$4::text",
    )
}

#[test]
fn intersects_builds_st_intersects() {
    let filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::Intersects,
        reference: dummy_reference(),
    };
    let stmt = build_spatial_select(
        Some("plenora_fixture"),
        "events",
        &["event_id", "geom"],
        &filter,
        Some(100),
    )
    .expect("build");
    assert_eq!(
        stmt.sql,
        format!(
            r#"SELECT "event_id", "geom" FROM "plenora_fixture"."events" WHERE ST_Intersects("geom", {}::geometry) LIMIT 100"#,
            guarded()
        )
    );
    assert_eq!(
        stmt.params,
        vec![
            ParameterValue::Bytes(dummy_ewkb()),
            ParameterValue::I32(4326),
            ParameterValue::String(r#""plenora_fixture"."events""#.to_owned()),
            ParameterValue::String("geom".to_owned()),
        ]
    );
}

#[test]
fn dwithin_binds_distance_parameter() {
    // DWithin geometry con SRID geografico è fail-closed. Uso SRID 3857
    // (web mercator, unità metri) per esercitare il
    // path DWithin senza il check spatial_policy.
    let reference = SpatialReference {
        ewkb: dummy_ewkb(),
        srid: 3857,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    };
    let filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::DWithin {
            distance_meters: 250.0,
        },
        reference,
    };
    let stmt = build_spatial_select(None, "poi", &["id"], &filter, None).expect("build");
    assert_eq!(
        stmt.sql,
        format!(
            r#"SELECT "id" FROM "poi" WHERE ST_DWithin("geom", {}::geometry, $5)"#,
            guarded()
        )
    );
    assert_eq!(stmt.params.len(), 5);
    assert!(matches!(stmt.params[4], ParameterValue::F64(v) if v == 250.0));
}

#[test]
fn bounding_box_uses_index_friendly_operator() {
    let filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::BoundingBox,
        reference: dummy_reference(),
    };
    let stmt = build_spatial_select(None, "buildings", &["id"], &filter, None).expect("build");
    assert!(stmt
        .sql
        .contains(&format!(r#""geom" && {}::geometry"#, guarded())));
}

#[test]
fn contains_and_within_are_distinct() {
    let mut filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::Contains,
        reference: dummy_reference(),
    };
    let a = build_spatial_select(None, "t", &["id"], &filter, None).unwrap();
    filter.predicate = SpatialPredicate::Within;
    let b = build_spatial_select(None, "t", &["id"], &filter, None).unwrap();
    assert!(a.sql.contains("ST_Contains"));
    assert!(b.sql.contains("ST_Within"));
}

#[test]
fn empty_projection_is_invalid() {
    let filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::Intersects,
        reference: dummy_reference(),
    };
    assert!(build_spatial_select(None, "t", &[], &filter, None).is_err());
}

#[test]
fn dwithin_negative_distance_is_invalid() {
    let filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::DWithin {
            distance_meters: -1.0,
        },
        reference: dummy_reference(),
    };
    assert!(build_spatial_select(None, "t", &["id"], &filter, None).is_err());
}

/// `validate` prima del SQL: uno SRID incorporato diverso da quello
/// dichiarato non arriva al server. Fino al 25/10 il builder non lo chiamava.
#[test]
fn embedded_srid_must_match_the_declared_one() {
    let mut ewkb = vec![0x01];
    ewkb.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
    ewkb.extend_from_slice(&3857_u32.to_le_bytes());
    ewkb.extend_from_slice(&1.0_f64.to_le_bytes());
    ewkb.extend_from_slice(&1.0_f64.to_le_bytes());
    let filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::BoundingBox,
        reference: SpatialReference {
            ewkb,
            srid: 4326,
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geometry,
        },
    };
    assert!(build_spatial_select(None, "t", &["id"], &filter, None).is_err());
}

#[test]
fn identifiers_are_quoted_and_escaped() {
    let filter = SpatialFilter {
        geometry_column: "geo\"m".into(),
        predicate: SpatialPredicate::Intersects,
        reference: dummy_reference(),
    };
    let stmt = build_spatial_select(None, "e\"vil", &["c\"ol"], &filter, None).unwrap();
    assert!(stmt.sql.contains("\"e\"\"vil\""));
    assert!(stmt.sql.contains("\"c\"\"ol\""));
    assert!(stmt.sql.contains("\"geo\"\"m\""));
    // La colonna di cui il server legge lo SRID viaggia come bind, non nel
    // testo: nessun nome entra in un letterale SQL.
    assert_eq!(stmt.params[3], ParameterValue::String("geo\"m".to_owned()));
}

#[test]
fn control_char_in_identifier_is_rejected() {
    let filter = SpatialFilter {
        geometry_column: "geom\n".into(),
        predicate: SpatialPredicate::Intersects,
        reference: dummy_reference(),
    };
    assert!(build_spatial_select(None, "t", &["id"], &filter, None).is_err());
}

#[test]
fn geography_semantics_casts_reference_to_geography() {
    // Con semantics=Geography il riferimento è castato a ::geography,
    // necessario per query verso
    // colonne geography (PostGIS non fa cast implicito cross-type).
    let filter = SpatialFilter {
        geometry_column: "g".into(),
        predicate: SpatialPredicate::DWithin {
            distance_meters: 500.0,
        },
        reference: SpatialReference {
            ewkb: dummy_ewkb(),
            srid: 4326,
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geography,
        },
    };
    let stmt = build_spatial_select(None, "poi", &["id"], &filter, None).expect("build");
    assert!(
        stmt.sql.contains(&format!("{}::geography", guarded())),
        "atteso cast ::geography, sql: {}",
        stmt.sql
    );
    assert!(
        !stmt.sql.contains("END)::geometry"),
        "non deve contenere ::geometry con semantics=Geography, sql: {}",
        stmt.sql
    );
}
