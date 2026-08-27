use super::*;

fn base_contract() -> GeometryContract {
    GeometryContract {
        field_id: FieldId(7),
        encoding: GeometryEncoding::Ewkb,
        dimensions: Dimensions::Xyzm,
        nullable: true,
        types_declaration: TypesDeclaration::Exact,
        geometry_types: vec![GeometryType::Point, GeometryType::Multipolygon],
        crs: CrsResolution::Resolved(ResolvedCrs {
            id: Some("EPSG:4326".to_owned()),
            srid: Some(4326),
            definition: None,
            definition_format: None,
            axis_order: AxisOrder::LatLon,
        }),
        spatial_semantics: Some(SpatialSemantics::Geometry),
        precision: Some(CoordinatePrecision::Float64),
    }
}

#[test]
fn all_geometry_types_and_dimensions_round_trip() {
    for geometry_type in GeometryType::CANONICAL_ORDER {
        let encoded = serde_json::to_string(&geometry_type).expect("serialize geometry type");
        let decoded: GeometryType =
            serde_json::from_str(&encoded).expect("deserialize geometry type");
        assert_eq!(decoded, geometry_type);
    }
    for dimensions in [
        Dimensions::Xy,
        Dimensions::Xyz,
        Dimensions::Xym,
        Dimensions::Xyzm,
        Dimensions::Unknown,
    ] {
        let encoded = serde_json::to_string(&dimensions).expect("serialize dimensions");
        let decoded: Dimensions = serde_json::from_str(&encoded).expect("deserialize dimensions");
        assert_eq!(decoded, dimensions);
    }
}

#[test]
fn exact_types_are_non_empty_unique_and_canonical() {
    assert!(base_contract().validate().is_ok());

    let mut empty = base_contract();
    empty.geometry_types.clear();
    assert!(empty.validate().is_err());

    let mut duplicate = base_contract();
    duplicate.geometry_types = vec![GeometryType::Point, GeometryType::Point];
    assert!(duplicate.validate().is_err());

    let mut reversed = base_contract();
    reversed.geometry_types = vec![GeometryType::Polygon, GeometryType::Point];
    assert!(reversed.validate().is_err());
}

#[test]
fn unresolved_types_cannot_claim_an_observed_type() {
    let mut contract = base_contract();
    contract.types_declaration = TypesDeclaration::Unresolved;
    assert!(contract.validate().is_err());
    contract.geometry_types.clear();
    assert!(contract.validate().is_ok());
}

#[test]
fn crs_definition_requires_format_and_axis_order() {
    let mut contract = base_contract();
    contract.crs = CrsResolution::Resolved(ResolvedCrs {
        id: None,
        srid: None,
        definition: Some("GEOGCRS[...]".to_owned()),
        definition_format: None,
        axis_order: AxisOrder::LatLon,
    });
    assert!(contract.validate().is_err());
}
