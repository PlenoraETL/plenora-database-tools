use super::*;
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::{
    plan::{WriteMode, WriteOperation},
    ErrorCategory,
};
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
fn copy_describes_domain_parameters_without_erasing_the_domain() {
    let mut plan = WriteColumnPlan::compile(&Field::new("v", DataType::Utf8, true)).unwrap();
    assert!(copy_type_probe(std::slice::from_ref(&plan), "\"app\".\"t\"").is_err());
    let domain = Type::new(
        "checked_text".to_owned(),
        900_002,
        tokio_postgres::types::Kind::Domain(Type::TEXT),
        "app".to_owned(),
    );
    plan.target_type = Some(target(domain.oid(), "\"app\".\"checked_text\"", None));
    assert_eq!(
        copy_type_probe(std::slice::from_ref(&plan), "\"app\".\"t\"").unwrap(),
        "SELECT $1::\"app\".\"checked_text\" FROM \"app\".\"t\" LIMIT 0"
    );
    verify_parameters(std::slice::from_ref(&plan), &[0], &[domain]).unwrap();
    assert!(verify_parameters(&[plan], &[0], &[Type::TEXT]).is_err());
}

#[test]
fn prepared_spatial_values_use_the_target_binary_type_including_nulls() {
    use arrow_array::BinaryArray;
    use bytes::BytesMut;
    use tokio_postgres::types::{IsNull, Kind};
    for (native, semantics) in [
        ("geometry", super::super::plan::ColumnSemantics::Geometry),
        ("geography", super::super::plan::ColumnSemantics::Geography),
    ] {
        let mut plan = WriteColumnPlan::compile(&Field::new("v", DataType::Binary, true)).unwrap();
        plan.semantics = semantics;
        let actual = Type::new(native.to_owned(), 900_003, Kind::Simple, "gis".to_owned());
        plan.target_type = Some(target(actual.oid(), &format!("\"gis\".{native}"), None));
        let bytes = [1_u8, 2, 3];
        let array = BinaryArray::from(vec![Some(bytes.as_slice()), None]);
        assert_eq!(super::super::sql::placeholder_expression(&plan, 1), "$1");
        verify_parameters(
            std::slice::from_ref(&plan),
            &[0],
            std::slice::from_ref(&actual),
        )
        .unwrap();
        assert!(verify_parameters(std::slice::from_ref(&plan), &[0], &[Type::BYTEA]).is_err());
        for row in 0..2 {
            let value = super::super::prepared_codec::arrow_value(&array, &plan, row).unwrap();
            let mut encoded = BytesMut::new();
            let null = value.to_sql_checked(&actual, &mut encoded).unwrap();
            if row == 0 {
                assert!(matches!(null, IsNull::No));
                assert_eq!(encoded.as_ref(), bytes);
            } else {
                assert!(matches!(null, IsNull::Yes));
                assert!(encoded.is_empty());
            }
            assert!(value
                .to_sql_checked(&Type::BYTEA, &mut BytesMut::new())
                .is_err());
        }
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

/// Le forme vengono dal fixture, non da una seconda lista di casi scelti.
/// COPY binario passa dalla stessa verifica OID; COPY testo scrive direttamente
/// nelle colonne del target, senza una tabella o un tipo SQL intermedio.
#[test]
fn fixture_matrix_preserves_target_on_all_statement_paths_and_copy() {
    use plenora_database_core::plan::{ObjectRef, TransactionProfile};
    let fixture: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../tests/fixtures/scrittura_server_postgres.json"
    ))
    .unwrap();
    let forms = fixture["misure"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|m| m["chiavi"].as_array().unwrap())
        .map(|k| k["nativo"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
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
    for native in &forms {
        let declaration = native
            .strip_prefix("scalare:")
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .replace("(n)", "(137)");
        // Un enum e un tipo nominato del catalogo, non la parola SQL enum.
        let declaration = if declaration == "enum" {
            "\"app\".\"enum_type\"".to_owned()
        } else {
            declaration
        };
        let actual = fixture_type(&declaration);
        let collation = (declaration.starts_with("character")
            || matches!(declaration.as_str(), "text" | "bpchar"))
        .then_some("pg_catalog.\"C\"");
        let descriptor = target(actual.oid(), &declaration, collation);
        let mut plans = super::super::compile_schema_plan(&schema, &operation).unwrap();
        for plan in &mut plans {
            plan.target_type = Some(descriptor.clone());
            if actual != Type::JSON {
                plan.key_equality = Some(super::super::visibility::KeyEquality {
                    operator: "pg_catalog.=".to_owned(),
                    cast: declaration.clone(),
                });
            }
        }
        for mode in [
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Update,
            WriteMode::DeleteByKeys,
        ] {
            operation.mode = mode;
            let result =
                super::super::sql::statement(&operation, &operation.target, &schema, &plans);
            if actual == Type::JSON && matches!(mode, WriteMode::Update | WriteMode::DeleteByKeys) {
                assert_eq!(result.unwrap_err().category, ErrorCategory::Unsupported);
                continue;
            }
            let (sql, indexes) = result.unwrap();
            if matches!(mode, WriteMode::Update | WriteMode::DeleteByKeys) {
                let ordinal = if mode == WriteMode::Update { 2 } else { 1 };
                assert!(
                    sql.contains(&descriptor.parameter(ordinal)),
                    "{native}: {sql}"
                );
            } else {
                // INSERT/UPSERT infer the target type from the destination;
                // verify_parameters below refuses a different server OID.
                assert!(sql.contains("VALUES ($1, $2)"), "{native}: {sql}");
            }
            let types = vec![actual.clone(); indexes.len()];
            verify_parameters(&plans, &indexes, &types).unwrap();
        }
        verify_parameters(&plans, &[0, 1], &[actual.clone(), actual]).unwrap();
        assert!(verify_parameters(&plans, &[0], &[Type::VOID]).is_err());
    }
    eprintln!(
        "PostgreSQL: {} forms x 5 paths = {} identity checks",
        forms.len(),
        forms.len() * 5
    );
}

fn fixture_type(declaration: &str) -> Type {
    match declaration.split('(').next().unwrap() {
        "bigint" => Type::INT8,
        "integer" => Type::INT4,
        "smallint" => Type::INT2,
        "boolean" => Type::BOOL,
        "bytea" => Type::BYTEA,
        "date" => Type::DATE,
        "double precision" => Type::FLOAT8,
        "real" => Type::FLOAT4,
        "numeric" => Type::NUMERIC,
        "text" => Type::TEXT,
        "character varying" => Type::VARCHAR,
        "character" | "bpchar" => Type::BPCHAR,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        "uuid" => Type::UUID,
        "interval" => Type::INTERVAL,
        _ if declaration.starts_with("timestamp") => {
            if declaration.contains("with time zone") {
                Type::TIMESTAMPTZ
            } else {
                Type::TIMESTAMP
            }
        }
        _ if declaration.starts_with("time") => Type::TIME,
        "\"app\".\"enum_type\"" => Type::new(
            "enum_type".to_owned(),
            900_001,
            tokio_postgres::types::Kind::Enum(vec!["a".to_owned()]),
            "app".to_owned(),
        ),
        _ => panic!("unmapped fixture type: {declaration}"),
    }
}
