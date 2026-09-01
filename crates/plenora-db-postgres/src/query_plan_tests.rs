use super::*;
use plenora_database_core::plan::{ObjectRef, OrderBy, SortDirection};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::ReadCheckpoint;

fn column(name: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.to_owned(),
        native_type: "int4".to_owned(),
        nullable: false,
        numeric_precision: None,
        numeric_scale: None,
        spatial_srid: None,
        spatial_dimensions: None,
        spatial_type: None,
        spatial_crs_id: None,
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
        kind: ColumnKind::I32,
    }
}

/// `PostgreSQL` accetta `OFFSET` da solo, e questo test fissa la
/// differenza dagli altri due dialetti.
///
/// `MySQL` pretende un tetto — `OFFSET` senza `LIMIT` non e sintassi
/// valida — e SQL Server vuole `OFFSET n ROWS`, dopo un `ORDER BY` che
/// scrive comunque. Qui non serve niente di tutto questo, e la clausola
/// esce nuda. Tre forme per la stessa richiesta del piano: e la ragione
/// per cui il campo sta nel contratto e non nel SQL che il chiamante
/// scrive.
#[test]
fn the_window_renders_bare_on_postgres() {
    let columns = vec![column("id")];
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: "events".to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: Some(20),
        filter: None,
        declared_crs: Vec::new(),
    };
    let plan = plan_read(&operation, &columns).expect("finestra senza tetto");
    assert!(
        plan.sql.ends_with("ORDER BY \"id\" ASC OFFSET 20"),
        "la finestra esce nuda su PostgreSQL: {}",
        plan.sql
    );

    let bounded = ReadOperation {
        row_limit: Some(5),
        ..operation
    };
    let plan = plan_read(&bounded, &columns).expect("finestra con tetto");
    assert!(
        plan.sql.ends_with("LIMIT 5 OFFSET 20"),
        "il tetto precede la finestra: {}",
        plan.sql
    );
}

#[test]
fn qualified_checkpoint_renders_as_a_bound_postgres_keyset() {
    let columns = vec![column("id")];
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: "events".to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(100),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let checkpoint = ReadCheckpoint::new(
        plenora_database_core::plan::ProviderKind::Postgres,
        &operation,
        &ParameterBag::default(),
        vec![ParameterValue::I32(41)],
    )
    .expect("checkpoint");
    let (resumed, _) = checkpoint
        .resume(
            plenora_database_core::plan::ProviderKind::Postgres,
            &operation,
            &ParameterBag::default(),
        )
        .expect("resume");
    let plan = plan_read(&resumed, &columns).expect("piano ripreso");
    assert_eq!(
        plan.sql,
        "SELECT \"id\" AS \"id\" FROM \"public\".\"events\" WHERE \"id\" > $1 ORDER BY \"id\" ASC LIMIT 100"
    );
    assert_eq!(plan.bind_names, ["__plenora_resume_0"]);
}
