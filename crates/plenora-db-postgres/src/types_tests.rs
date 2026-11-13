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
        registered_field_id: None,
        arrow_metadata: None,
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

/// Un id ricevuto prevale sulla provenienza anche dopo una rinomina.
#[test]
fn registered_identity_wins_over_provenance() {
    let mut column = geometry_column(Some("Point"), Some("XY"));
    column.source = Some((16384, 2));
    column.registered_field_id = Some("123456789012345678901234567890".to_owned());
    column.name = "renamed".to_owned();
    assert_eq!(
        column.arrow_field().metadata()[protocol::FIELD_ID],
        "123456789012345678901234567890"
    );
}

#[test]
fn registered_descriptor_and_ddl_share_the_typmod_decision() {
    use crate::column_metadata::ColumnMetadata;
    use crate::geometry_properties::PhysicalGeometry;
    for (declaration, types, dimensions, physical_type, expected_sql) in [
        (
            "exact",
            "point,polygon",
            "xy",
            Some("Geometry"),
            "geometry(Geometry,0)",
        ),
        ("mixed", "point", "xy", Some("Point"), "geometry(point,0)"),
        ("exact", "point", "unknown", None, "geometry"),
    ] {
        let mut column = geometry_column(physical_type, (dimensions != "unknown").then_some("XY"));
        column.spatial_srid = None;
        column.spatial_crs_id = None;
        let field = column.arrow_field();
        let mut metadata = field.metadata().clone();
        metadata.insert(protocol::GEOMETRY_TYPES_DECLARATION, declaration);
        metadata.insert(protocol::GEOMETRY_TYPES, types);
        let field = field.with_metadata(metadata);
        assert_eq!(PhysicalGeometry::declaration(&field).unwrap(), expected_sql);
        let schema = contract_schema(vec![field.clone()]).unwrap();
        let saved = ColumnMetadata::plan(schema.field(0), &schema).unwrap();
        column.registered_field_id = Some(saved.id.clone());
        column.arrow_metadata = Some(saved);
        crate::geometry_properties::validate_catalog(&column).unwrap();
        column.spatial_dimensions = Some("XYZ".into());
        assert!(crate::geometry_properties::validate_catalog(&column).is_err());
    }
}

#[test]
fn schema_omissions_are_consistent_and_counted_once_after_projection() {
    let mut columns = vec![geometry_column(Some("Point"), Some("XY")); 2];
    columns[0].name = "left".to_owned();
    columns[1].name = "right".to_owned();
    let base = contract_schema(columns.iter().map(ColumnSpec::arrow_field).collect()).unwrap();
    let mut metadata = base.metadata().clone();
    metadata.insert("opaque", "x".repeat(crate::column_metadata::COLUMN_LIMIT));
    let schema = arrow_schema::Schema::new_with_metadata(base.fields().clone(), metadata);
    let plan = crate::geometry_properties::SchemaPlan::compile(&schema).unwrap();
    for (column, saved) in columns.iter_mut().zip(plan.columns) {
        column.registered_field_id = Some(saved.id.clone());
        column.arrow_metadata = Some(saved);
    }
    crate::geometry_properties::read_schema(&columns).unwrap();
    for projected in [&columns[..], &columns[1..]] {
        let report = crate::geometry_properties::read_loss(projected);
        assert_eq!(report.schema_version, 2);
        assert_eq!(report.entries.len(), 1);
    }
    columns[1].arrow_metadata.as_mut().unwrap().omitted.clear();
    assert!(crate::geometry_properties::read_schema(&columns).is_err());
}
