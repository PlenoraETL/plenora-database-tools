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
        source: None,
        kind: ColumnKind::Geometry,
    }
}

/// L'id di una colonna letta da una tabella viene da `attrelid` e `attnum`:
/// un alias non lo cambia (ARROW-VOCABULARY §2, ARROW-INTERCHANGE §3).
#[test]
fn a_table_column_keeps_its_field_id_under_an_alias() {
    let mut original = geometry_column(Some("Point"), Some("XY"));
    original.source = Some((16_384, 2));
    let mut alias = original.clone();
    alias.name = "posizione".to_owned();
    let id = |column: &ColumnSpec| column.arrow_field().metadata()[protocol::FIELD_ID].clone();
    assert_eq!(id(&original), id(&alias));
    let mut other = original.clone();
    other.source = Some((16_384, 3));
    assert_ne!(id(&original), id(&other));
}

/// ARROW-VOCABULARY §4: un campo `geoarrow.wkb` dichiara field id, encoding,
/// dimensioni, semantica, precisione, dichiarazione dei tipi e risoluzione
/// del CRS — anche quando il catalogo non dice tipo o dimensioni.
#[test]
fn a_read_geometry_field_declares_every_required_key() {
    let mut declared_only = geometry_column(None, None);
    declared_only.spatial_crs_id = None;
    let mut without_srid = geometry_column(None, None);
    without_srid.spatial_crs_id = None;
    without_srid.spatial_srid = None;
    for column in [
        geometry_column(Some("Point"), Some("XY")),
        geometry_column(None, None),
        declared_only,
        without_srid,
    ] {
        let schema = contract_schema(vec![column.arrow_field()]).expect("schema");
        validate_schema_contract(&schema).expect("schema conforme");
        plenora_database_core::field_contract::validate_published_schema(&schema)
            .expect("schema pubblicabile");
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
