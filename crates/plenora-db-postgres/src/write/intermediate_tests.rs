use super::*;
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::{plan::WriteMode, ErrorCategory};
use std::sync::Arc;

fn target(oid: u32, declaration: &str, collation: Option<&str>) -> TargetType {
    TargetType {
        oid,
        declaration: declaration.to_owned(),
        collation: collation.map(str::to_owned),
    }
}

#[test]
fn parameters_preserve_catalog_modifiers_and_collation() {
    for declaration in [
        "numeric(10,2)",
        "character varying(19)",
        "character(7)",
        "timestamp(3) with time zone",
        "time(6) without time zone",
        "bit varying(11)",
        "\"app\".\"Domain\"",
        "numeric(8,4)[]",
    ] {
        let descriptor = target(1, declaration, None);
        assert_eq!(descriptor.parameter(2), format!("$2::{declaration}"));
        let descriptor = target(1, declaration, Some("\"app\".\"Collation\""));
        assert_eq!(
            descriptor.parameter(2),
            format!("($2::{declaration} COLLATE \"app\".\"Collation\")")
        );
    }
}

#[test]
fn parameter_order_and_missing_catalog_are_fail_closed() {
    let mut plans = [
        WriteColumnPlan::compile(&Field::new("i", DataType::Int64, false)).unwrap(),
        WriteColumnPlan::compile(&Field::new("s", DataType::Utf8, false)).unwrap(),
    ];
    assert!(verify_parameters(&plans, &[0], &[Type::INT8]).is_err());
    plans[0].target_type = Some(target(Type::INT8.oid(), "bigint", None));
    plans[1].target_type = Some(target(Type::TEXT.oid(), "pg_catalog.text", None));
    verify_parameters(&plans, &[1, 0], &[Type::TEXT, Type::INT8]).unwrap();
    for (indexes, types) in [
        (vec![0, 1], vec![Type::TEXT, Type::INT8]),
        (vec![0], vec![]),
        (vec![2], vec![Type::INT8]),
    ] {
        let error = verify_parameters(&plans, &indexes, &types).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }
}

#[test]
fn same_wire_representation_does_not_prove_same_sql_type() {
    for (expected, actual) in [
        (Type::VARCHAR, Type::TEXT),
        (Type::BPCHAR, Type::TEXT),
        (Type::TIMESTAMPTZ, Type::TIMESTAMP),
        (Type::UUID, Type::TEXT),
        (Type::JSONB, Type::JSON),
    ] {
        assert!(target(expected.oid(), expected.name(), None)
            .verify(&actual)
            .is_err());
    }
}

#[test]
fn keyed_statements_do_not_cast_the_column_to_the_opclass() {
    use plenora_database_core::plan::{ObjectRef, TransactionProfile};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let mut operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: "t".to_owned(),
        },
        mode: WriteMode::Update,
        mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec!["id".to_owned()],
        update_columns: vec!["value".to_owned()],
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let mut plans = super::super::compile_schema_plan(&schema, &operation).unwrap();
    plans[0].target_type = Some(target(
        Type::BPCHAR.oid(),
        "character(7)",
        Some("pg_catalog.\"C\""),
    ));
    plans[0].key_equality = Some(super::super::visibility::KeyEquality {
        operator: "pg_catalog.=".to_owned(),
        cast: "pg_catalog.bpchar".to_owned(),
    });
    for (mode, ordinal) in [(WriteMode::Update, 2), (WriteMode::DeleteByKeys, 1)] {
        operation.mode = mode;
        let (sql, _) =
            super::super::sql::statement(&operation, &operation.target, &schema, &plans).unwrap();
        assert!(sql.contains(&format!(
            "\"id\" OPERATOR(pg_catalog.=) (${ordinal}::character(7) COLLATE pg_catalog.\"C\")"
        )));
        assert!(!sql.contains("::pg_catalog.bpchar"));
    }
}
