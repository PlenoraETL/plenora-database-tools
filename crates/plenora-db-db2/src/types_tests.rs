use crate::{Db2Column, Db2ObjectDescription, Db2ReadPlan};
use plenora_database_core::plan::{DeclaredCrs, ObjectRef, OrderBy, ReadOperation, SortDirection};
use plenora_database_core::protocol;
use plenora_database_core::ErrorCategory;

fn description() -> Db2ObjectDescription {
    Db2ObjectDescription {
        schema: "PLENORA_TEST".to_owned(),
        name: "READ_PROBE".to_owned(),
        kind: "TABLE".to_owned(),
        columns: vec![
            Db2Column {
                name: "ID".to_owned(),
                ordinal: 1,
                data_type: "INTEGER".to_owned(),
                length: 4,
                scale: 0,
                nullable: false,
                default_expression: None,
                generated: false,
                identity: false,
            },
            Db2Column {
                name: "LABEL".to_owned(),
                ordinal: 2,
                data_type: "VARCHAR".to_owned(),
                length: 128,
                scale: 0,
                nullable: true,
                default_expression: None,
                generated: false,
                identity: false,
            },
        ],
        indexes: Vec::new(),
        schema_token: "sha256:test".to_owned(),
    }
}

fn operation() -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: Some("PLENORA".to_owned()),
            schema: Some("PLENORA_TEST".to_owned()),
            object: "READ_PROBE".to_owned(),
        },
        projection: vec!["LABEL".to_owned(), "ID".to_owned()],
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(10),
        row_offset: Some(2),
        filter: None,
        declared_crs: Vec::new(),
    }
}

#[test]
fn read_plan_quotes_identifiers_and_preserves_projection_order() {
    let plan = Db2ReadPlan::compile(&description(), &operation(), 1024).expect("piano Db2");

    assert_eq!(plan.columns[0].name, "LABEL");
    assert_eq!(plan.columns[1].name, "ID");
    assert_eq!(
        plan.sql,
        "SELECT \"LABEL\", \"ID\" FROM \"PLENORA_TEST\".\"READ_PROBE\" ORDER BY \"ID\" ASC OFFSET 2 ROWS FETCH FIRST 10 ROWS ONLY"
    );
}

#[test]
fn pagination_without_ordering_fails_before_the_network() {
    let mut operation = operation();
    operation.order_by.clear();
    let error = Db2ReadPlan::compile(&description(), &operation, 1024)
        .expect_err("paginazione non deterministica");

    assert_eq!(error.category, ErrorCategory::InvalidPlan);
}

#[test]
fn unsupported_native_types_fail_closed() {
    let mut description = description();
    description.columns[0].data_type = "BLOB".to_owned();
    let error =
        Db2ReadPlan::compile(&description, &operation(), 1024).expect_err("BLOB non qualificato");

    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn unsupported_unprojected_columns_do_not_block_a_supported_projection() {
    let mut description = description();
    description.columns.push(Db2Column {
        name: "PAYLOAD".to_owned(),
        ordinal: 3,
        data_type: "BLOB".to_owned(),
        length: 1_024,
        scale: 0,
        nullable: true,
        default_expression: None,
        generated: false,
        identity: false,
    });

    let plan = Db2ReadPlan::compile(&description, &operation(), 1_024)
        .expect("la colonna non proiettata non entra nel contratto Arrow");

    assert_eq!(plan.columns.len(), 2);
}

#[test]
fn spatial_projection_publishes_geoarrow_and_adds_row_checks() {
    let mut description = description();
    description.columns.push(Db2Column {
        name: "SHAPE".to_owned(),
        ordinal: 3,
        data_type: "ST_GEOMETRY".to_owned(),
        length: 0,
        scale: 0,
        nullable: true,
        default_expression: None,
        generated: false,
        identity: false,
    });
    let mut operation = operation();
    operation.projection = vec!["ID".to_owned(), "SHAPE".to_owned()];
    operation.declared_crs = vec![DeclaredCrs {
        column: "SHAPE".to_owned(),
        srid: 4_326,
    }];

    let plan = Db2ReadPlan::compile(&description, &operation, 1_024).expect("piano spatial Db2");

    assert_eq!(plan.spatial_checks.len(), 1);
    assert_eq!(plan.spatial_checks[0].expected_srid, 4_326);
    assert!(plan.sql.contains("ST_ASBINARY(\"SHAPE\") AS \"SHAPE\""));
    assert!(plan.sql.contains("ST_SRID(\"SHAPE\")"));
    assert!(plan.sql.contains("ST_COORDDIM(\"SHAPE\")"));
    let field = plan.schema.field_with_name("SHAPE").expect("campo shape");
    assert_eq!(
        field.data_type(),
        &plenora_database_core::arrow::DataType::Binary
    );
    assert_eq!(
        field.metadata().get(protocol::GEOARROW_EXTENSION_NAME),
        Some(&"geoarrow.wkb".to_owned())
    );
    assert_eq!(
        field.metadata().get(protocol::GEOMETRY_SRID),
        Some(&"4326".to_owned())
    );
    assert_eq!(
        field.metadata().get(protocol::GEOMETRY_DIMENSIONS),
        Some(&"unknown".to_owned())
    );
}

#[test]
fn spatial_projection_requires_one_valid_declared_crs() {
    let mut description = description();
    description.columns.push(Db2Column {
        name: "SHAPE".to_owned(),
        ordinal: 3,
        data_type: "ST_GEOMETRY".to_owned(),
        length: 0,
        scale: 0,
        nullable: true,
        default_expression: None,
        generated: false,
        identity: false,
    });
    let mut operation = operation();
    operation.projection = vec!["SHAPE".to_owned()];

    let missing =
        Db2ReadPlan::compile(&description, &operation, 1_024).expect_err("CRS spatial Db2 assente");
    assert_eq!(missing.category, ErrorCategory::Crs);

    operation.declared_crs = vec![DeclaredCrs {
        column: "ID".to_owned(),
        srid: 4_326,
    }];
    let scalar = Db2ReadPlan::compile(&description, &operation, 1_024)
        .expect_err("CRS dichiarato su scalare");
    assert_eq!(scalar.category, ErrorCategory::InvalidPlan);
}
