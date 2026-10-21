use super::*;
use plenora_database_core::protocol::{canonical_field_id, name_field_id, FIELD_ID};

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
        kind: crate::types::ColumnKind::Geometry,
    }
}

/// La copia non sottrae l'id ne alla prima proiezione ne a campi successivi.
#[test]
fn registered_duplicates_reserve_all_existing_ids_before_assignment() {
    let mut first = geometry_column(Some("Point"), Some("XY"));
    first.name = "a".to_owned();
    first.source = Some((42, 1));
    first.registered_field_id = Some(format!("00{}", name_field_id("b")));
    let mut second = first.clone();
    second.name = "b".to_owned();
    let mut third = first.clone();
    third.source = Some((42, 2));
    third.name = "c".to_owned();
    third.registered_field_id = Some((u64::from(name_field_id("b")) + 1).to_string());
    let columns = vec![first.clone(), second, third.clone()];
    let resolve = || {
        contract_schema(
            without_shared_origins(columns.clone())
                .iter()
                .map(ColumnSpec::arrow_field)
                .collect(),
        )
        .expect("unique schema")
    };
    let schema = resolve();
    assert_eq!(schema, resolve());
    assert_eq!(
        schema.field(0).metadata()[FIELD_ID],
        first.registered_field_id.unwrap()
    );
    assert_eq!(
        schema.field(2).metadata()[FIELD_ID],
        third.registered_field_id.unwrap()
    );
    assert_eq!(
        canonical_field_id(&schema.field(1).metadata()[FIELD_ID]).unwrap(),
        (u64::from(name_field_id("b")) + 2).to_string()
    );
}

/// Le colonne mai registrate mantengono il fallback storico per nome.
#[test]
fn unregistered_duplicates_keep_the_name_fallback() {
    let mut first = geometry_column(Some("Point"), Some("XY"));
    first.source = Some((42, 1));
    first.name = "a".to_owned();
    let mut second = first.clone();
    second.name = "b".to_owned();
    let columns = without_shared_origins(vec![first, second]);
    let schema =
        contract_schema(columns.iter().map(ColumnSpec::arrow_field).collect()).expect("schema");
    for field in schema.fields() {
        assert_eq!(
            field.metadata()[FIELD_ID],
            name_field_id(field.name()).to_string()
        );
    }
}
