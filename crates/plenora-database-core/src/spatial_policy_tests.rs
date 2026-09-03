use super::*;
use crate::geometry::Dimensions;

fn ref_with(srid: u32, semantics: SpatialSemantics) -> SpatialReference {
    SpatialReference {
        ewkb: vec![0x01],
        srid,
        dimensions: Dimensions::Xy,
        semantics,
    }
}

#[test]
fn geographic_srids_include_wgs84_and_nad_family() {
    assert!(is_geographic_srid(4326));
    assert!(is_geographic_srid(4269));
    assert!(is_geographic_srid(4267));
    assert!(is_geographic_srid(4258));
    assert!(is_geographic_srid(4283));
}

#[test]
fn projected_srids_are_not_geographic() {
    // Web mercator, UTM 32N, UTM 33N — tutti planari con unità
    // metri, tipici per PFM.
    assert!(!is_geographic_srid(3857));
    assert!(!is_geographic_srid(25832));
    assert!(!is_geographic_srid(32633));
}

#[test]
fn cast_dispatch_matches_semantics() {
    assert_eq!(postgres_cast_for(SpatialSemantics::Geometry), "::geometry");
    assert_eq!(
        postgres_cast_for(SpatialSemantics::Geography),
        "::geography"
    );
}

#[test]
fn dwithin_geometry_on_wgs84_is_rejected_for_postgres() {
    let err = validate_predicate(
        ProviderKind::Postgres,
        &SpatialPredicate::DWithin {
            distance_meters: 100.0,
        },
        &ref_with(4326, SpatialSemantics::Geometry),
    )
    .unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
    assert!(err.message.contains("Geography"));
}

#[test]
fn dwithin_geography_on_wgs84_is_accepted() {
    validate_predicate(
        ProviderKind::Postgres,
        &SpatialPredicate::DWithin {
            distance_meters: 100.0,
        },
        &ref_with(4326, SpatialSemantics::Geography),
    )
    .unwrap();
}

#[test]
fn dwithin_geometry_on_projected_srid_is_accepted() {
    validate_predicate(
        ProviderKind::Postgres,
        &SpatialPredicate::DWithin {
            distance_meters: 100.0,
        },
        &ref_with(3857, SpatialSemantics::Geometry),
    )
    .unwrap();
}

#[test]
fn bounding_box_with_geography_is_rejected_for_postgres() {
    let err = validate_predicate(
        ProviderKind::Postgres,
        &SpatialPredicate::BoundingBox,
        &ref_with(4326, SpatialSemantics::Geography),
    )
    .unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::Unsupported);
}

#[test]
fn contains_and_within_with_geography_are_rejected_for_postgres() {
    // PostGIS non espone ST_Contains/ST_Within per geography.
    for predicate in [SpatialPredicate::Contains, SpatialPredicate::Within] {
        let err = validate_predicate(
            ProviderKind::Postgres,
            &predicate,
            &ref_with(4326, SpatialSemantics::Geography),
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::Unsupported);
    }
}

#[test]
fn intersects_with_geography_is_accepted() {
    // ST_Intersects è disponibile sia per geometry che per geography.
    validate_predicate(
        ProviderKind::Postgres,
        &SpatialPredicate::Intersects,
        &ref_with(4326, SpatialSemantics::Geography),
    )
    .unwrap();
}

#[test]
fn dwithin_on_mysql_is_unsupported() {
    let err = validate_predicate(
        ProviderKind::Mysql,
        &SpatialPredicate::DWithin {
            distance_meters: 100.0,
        },
        &ref_with(4326, SpatialSemantics::Geography),
    )
    .unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::Unsupported);
}

#[test]
fn dwithin_negative_distance_is_rejected_universally() {
    for provider in [ProviderKind::Postgres, ProviderKind::Mysql] {
        let err = validate_predicate(
            provider,
            &SpatialPredicate::DWithin {
                distance_meters: -1.0,
            },
            &ref_with(3857, SpatialSemantics::Geometry),
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
    }
}

#[test]
fn dwithin_nan_is_rejected() {
    let err = validate_predicate(
        ProviderKind::Postgres,
        &SpatialPredicate::DWithin {
            distance_meters: f64::NAN,
        },
        &ref_with(3857, SpatialSemantics::Geometry),
    )
    .unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn db2_accepts_only_the_qualified_geometry_predicates() {
    let reference = ref_with(4326, SpatialSemantics::Geometry);
    for predicate in [
        SpatialPredicate::Intersects,
        SpatialPredicate::Contains,
        SpatialPredicate::Within,
    ] {
        validate_predicate(ProviderKind::Db2, &predicate, &reference)
            .expect("predicato Db2 qualificato");
    }
    for predicate in [
        SpatialPredicate::BoundingBox,
        SpatialPredicate::DWithin {
            distance_meters: 1.0,
        },
    ] {
        assert_eq!(
            validate_predicate(ProviderKind::Db2, &predicate, &reference)
                .expect_err("predicato Db2 non qualificato")
                .category,
            crate::ErrorCategory::Unsupported
        );
    }
    assert_eq!(
        validate_predicate(
            ProviderKind::Db2,
            &SpatialPredicate::Intersects,
            &ref_with(4326, SpatialSemantics::Geography),
        )
        .expect_err("geography Db2")
        .category,
        crate::ErrorCategory::Unsupported
    );
}

#[test]
fn oracle_accepts_geometry_predicates_and_bounds_metric_distance() {
    let geographic = ref_with(4326, SpatialSemantics::Geometry);
    for predicate in [
        SpatialPredicate::Intersects,
        SpatialPredicate::Contains,
        SpatialPredicate::Within,
        SpatialPredicate::BoundingBox,
        SpatialPredicate::DWithin {
            distance_meters: 10.0,
        },
    ] {
        validate_predicate(ProviderKind::Oracle, &predicate, &geographic)
            .expect("predicato Oracle qualificato");
    }
    assert_eq!(
        validate_predicate(
            ProviderKind::Oracle,
            &SpatialPredicate::DWithin {
                distance_meters: 10.0,
            },
            &ref_with(3857, SpatialSemantics::Geometry),
        )
        .expect_err("unita proiettata Oracle non dedotta")
        .category,
        crate::ErrorCategory::Unsupported
    );
    assert_eq!(
        validate_predicate(
            ProviderKind::Oracle,
            &SpatialPredicate::Intersects,
            &ref_with(4326, SpatialSemantics::Geography),
        )
        .expect_err("geography Oracle")
        .category,
        crate::ErrorCategory::Unsupported
    );
}
