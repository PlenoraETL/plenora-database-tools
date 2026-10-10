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

/// Un campo geometrico pubblicabile: tutte le chiavi canoniche di §4.
fn published_geometry() -> std::collections::HashMap<String, String> {
    [
        ("ARROW:extension:name", "geoarrow.wkb"),
        ("plenora.field_id", "1"),
        ("plenora.geometry.encoding", "ewkb"),
        ("plenora.geometry.dimensions", "xy"),
        ("plenora.geometry.spatial_semantics", "geometry"),
        ("plenora.geometry.precision", "float64"),
        ("plenora.geometry.types_declaration", "exact"),
        ("plenora.geometry.types", "point"),
        ("plenora.geometry.crs_resolution", "resolved"),
        ("plenora.geometry.crs_id", "EPSG:4326"),
        ("plenora.geometry.srid", "4326"),
        ("plenora.geometry.axis_order", "lat_lon"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect()
}

fn published(metadata: std::collections::HashMap<String, String>) -> Result<()> {
    let field = Field::new("geom", DataType::Binary, true).with_metadata(metadata);
    let schema = Schema::new_with_metadata(
        vec![field],
        std::collections::HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    );
    validate_published_schema(&schema)
}

#[test]
fn a_complete_geometry_field_is_publishable() {
    published(published_geometry()).expect("campo completo");
}

/// §4: un campo con metadati geometrici dichiara l'estensione.
#[test]
fn a_geometry_without_the_extension_is_not_publishable() {
    let mut metadata = published_geometry();
    metadata.remove("ARROW:extension:name");
    assert!(published(metadata).is_err());
}

/// §4: `resolved` e `declared_unresolved` chiedono un CRS (id o
/// definizione) e l'ordine degli assi.
#[test]
fn a_resolved_crs_needs_an_identity_and_an_axis_order() {
    for resolution in ["resolved", "declared_unresolved"] {
        let mut without_crs = published_geometry();
        without_crs.insert(
            "plenora.geometry.crs_resolution".to_owned(),
            resolution.to_owned(),
        );
        without_crs.remove("plenora.geometry.crs_id");
        without_crs.remove("plenora.geometry.srid");
        assert!(published(without_crs).is_err(), "{resolution} senza CRS");
        let mut without_axis = published_geometry();
        without_axis.insert(
            "plenora.geometry.crs_resolution".to_owned(),
            resolution.to_owned(),
        );
        without_axis.remove("plenora.geometry.axis_order");
        assert!(published(without_axis).is_err(), "{resolution} senza assi");
    }
}

/// §3: il vocabolario e case-sensitive.
#[test]
fn geometry_types_are_case_sensitive_when_published() {
    let mut metadata = published_geometry();
    metadata.insert("plenora.geometry.types".to_owned(), "POINT".to_owned());
    assert!(published(metadata).is_err());
}

/// Una chiave legacy non soddisfa un requisito di pubblicazione.
#[test]
fn legacy_keys_do_not_satisfy_publication() {
    for (canonical, legacy, value) in [
        ("plenora.geometry.dimensions", "plenora.dimensions", "xy"),
        (
            "plenora.geometry.spatial_semantics",
            "plenora.spatial_semantics",
            "geometry",
        ),
    ] {
        let mut metadata = published_geometry();
        metadata.remove(canonical);
        metadata.insert(legacy.to_owned(), value.to_owned());
        assert!(published(metadata).is_err(), "{legacy}");
    }
}
