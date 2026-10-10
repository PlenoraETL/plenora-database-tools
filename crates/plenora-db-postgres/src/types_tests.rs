use super::*;
use plenora_database_core::field_contract::{validate_schema_contract, FieldContract};
use plenora_database_core::protocol::contract_schema;

fn geometry_column(spatial_type: Option<&str>, dimensions: Option<&str>) -> ColumnSpec {
    ColumnSpec {
        name: "geom".to_owned(),
        native_type: "geometry".to_owned(),
        nullable: true,
        numeric_precision: None,
        numeric_scale: None,
        spatial_srid: Some(4326),
        spatial_dimensions: dimensions.map(str::to_owned),
        spatial_type: spatial_type.map(str::to_owned),
        spatial_crs_id: Some("EPSG:4326".to_owned()),
        default_expression: None,
        identity_kind: None,
        generated_kind: None,
        native_declaration: None,
        type_kind: None,
        composite_fields: Vec::new(),
        enum_labels: Vec::new(),
        domain_base_type: None,
        domain_constraints: Vec::new(),
        collation: None,
        kind: ColumnKind::Geometry,
    }
}

/// ARROW-VOCABULARY §4: un campo `geoarrow.wkb` dichiara field id, encoding,
/// dimensioni, semantica, precisione, dichiarazione dei tipi e risoluzione
/// del CRS — anche quando il catalogo non dice tipo o dimensioni.
#[test]
fn a_read_geometry_field_declares_every_required_key() {
    for column in [
        geometry_column(Some("Point"), Some("XY")),
        geometry_column(None, None),
    ] {
        let schema = contract_schema(vec![column.arrow_field()]);
        validate_schema_contract(&schema).expect("schema conforme");
        let field = schema.field(0);
        let metadata = field.metadata();
        for key in [
            protocol::FIELD_ID,
            protocol::GEOMETRY_ENCODING,
            protocol::GEOMETRY_DIMENSIONS,
            protocol::GEOMETRY_SPATIAL_SEMANTICS,
            protocol::GEOMETRY_PRECISION,
            protocol::GEOMETRY_TYPES_DECLARATION,
            protocol::GEOMETRY_CRS_RESOLUTION,
        ] {
            assert!(metadata.contains_key(key), "{key} assente: {metadata:?}");
        }
        assert_eq!(metadata[protocol::GEOMETRY_PRECISION], "float64");
        FieldContract::parse(field)
            .and_then(FieldContract::validate_current)
            .expect("campo corrente conforme");
    }
}
