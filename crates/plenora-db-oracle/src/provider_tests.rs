use super::{oracle_capabilities, OracleProvider};
use crate::OracleConfig;
use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::relational::SpatialFunction;

#[test]
fn spatial_probe_opens_geometry_and_geography_together() {
    let capabilities = oracle_capabilities("26.0".to_owned(), true);
    assert!(capabilities.spatial.geometry);
    assert!(capabilities.spatial.geography);
    for semantics in [SpatialSemantics::Geometry, SpatialSemantics::Geography] {
        assert_eq!(
            capabilities.spatial.functions_by_semantics[&semantics],
            vec![
                SpatialFunction::Srid,
                SpatialFunction::Dimensions,
                SpatialFunction::Intersects,
                SpatialFunction::Contains,
                SpatialFunction::Within,
                SpatialFunction::DWithin,
            ]
        );
    }
}

#[test]
fn zero_capacity_pool_is_rejected_before_io() {
    let error =
        OracleProvider::new_with_pool(OracleConfig::new("oracle", "FREEPDB1", "plenora"), 0)
            .expect_err("capacita zero");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidConfiguration
    );
}
