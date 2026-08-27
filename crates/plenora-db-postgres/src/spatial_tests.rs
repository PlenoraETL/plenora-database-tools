use super::*;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};

fn dummy_ewkb() -> Vec<u8> {
    vec![0x01, 0x02, 0x03, 0x04]
}

fn dummy_reference() -> SpatialReference {
    SpatialReference {
        ewkb: dummy_ewkb(),
        srid: 4326,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    }
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
        "SELECT \"event_id\", \"geom\" FROM \"plenora_fixture\".\"events\" \
         WHERE ST_Intersects(\"geom\", ST_GeomFromEWKB($1)::geometry) LIMIT 100"
    );
    assert_eq!(stmt.params.len(), 1);
    assert!(matches!(stmt.params[0], ParameterValue::Bytes(_)));
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
        "SELECT \"id\" FROM \"poi\" \
         WHERE ST_DWithin(\"geom\", ST_GeomFromEWKB($1)::geometry, $2)"
    );
    assert_eq!(stmt.params.len(), 2);
    assert!(matches!(stmt.params[1], ParameterValue::F64(v) if v == 250.0));
}

#[test]
fn bounding_box_uses_index_friendly_operator() {
    let filter = SpatialFilter {
        geometry_column: "geom".into(),
        predicate: SpatialPredicate::BoundingBox,
        reference: dummy_reference(),
    };
    let stmt = build_spatial_select(None, "buildings", &["id"], &filter, None).expect("build");
    assert!(stmt.sql.contains("\"geom\" && ST_GeomFromEWKB"));
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
        stmt.sql.contains("ST_GeomFromEWKB($1)::geography"),
        "atteso cast ::geography, sql: {}",
        stmt.sql
    );
    assert!(
        !stmt.sql.contains("::geometry"),
        "non deve contenere ::geometry con semantics=Geography, sql: {}",
        stmt.sql
    );
}
