use super::*;
use std::collections::HashMap;

fn spatial_field(extra: &[(&str, &str)]) -> Field {
    let mut metadata = HashMap::from([
        (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
        (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
        (
            protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
            "exact".to_owned(),
        ),
        (protocol::GEOMETRY_TYPES.to_owned(), "point".to_owned()),
        (
            protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
            "missing".to_owned(),
        ),
    ]);
    metadata.extend(
        extra
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
    );
    Field::new("geometry", DataType::Binary, true).with_metadata(metadata)
}

fn current_schema(field: Field) -> Schema {
    Schema::new_with_metadata(
        vec![field],
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    )
}

#[test]
fn current_contract_requires_schema_version() {
    let schema = Schema::new(vec![spatial_field(&[])]);
    assert!(validate_schema_contract(&schema).is_err());
    assert!(validate_schema_contract(&current_schema(spatial_field(&[]))).is_ok());
}

#[test]
fn future_contract_version_fails_closed() {
    let mut schema = current_schema(spatial_field(&[]));
    schema
        .metadata
        .insert(protocol::CONTRACT_VERSION_KEY.to_owned(), "2".to_owned());
    let error = validate_schema_contract(&schema).expect_err("future version");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn conflicting_crs_representations_are_rejected() {
    let field = spatial_field(&[
        (protocol::GEOMETRY_CRS_RESOLUTION, "resolved"),
        (protocol::GEOMETRY_CRS_ID, "EPSG:4326"),
        (protocol::GEOMETRY_SRID, "3003"),
        (protocol::GEOMETRY_AXIS_ORDER, "lat_lon"),
    ]);
    let error = FieldContract::parse(&field).expect_err("conflicting CRS");
    assert_eq!(error.category, ErrorCategory::Crs);
}

#[test]
fn geometry_type_order_and_unresolved_state_are_closed() {
    let reversed = spatial_field(&[(protocol::GEOMETRY_TYPES, "polygon,point")]);
    assert!(FieldContract::parse(&reversed).is_err());
    let unresolved = spatial_field(&[(protocol::GEOMETRY_TYPES_DECLARATION, "unresolved")]);
    assert!(FieldContract::parse(&unresolved).is_err());
}

#[test]
fn canonical_and_legacy_values_must_agree() {
    let field = spatial_field(&[(LEGACY_DIMENSIONS, "xyz")]);
    assert!(FieldContract::parse(&field).is_err());
}
