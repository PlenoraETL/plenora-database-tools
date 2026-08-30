use super::*;
use crate::geometry::SpatialSemantics;
use crate::plan::{ComparisonOperator, ObjectRef};
use crate::relational::{
    ColumnRef, MutationAssignment, QueryExpression, UpdateOperation, UpsertOperation,
};

fn target() -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("app".to_owned()),
        object: "users".to_owned(),
    }
}

fn parameter(name: &str) -> QueryExpression {
    QueryExpression::Parameter {
        name: name.to_owned(),
    }
}

fn column(name: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some("users".to_owned()),
            field: name.to_owned(),
        },
    }
}

#[test]
fn insert_uses_named_layout_without_carrying_values() {
    let operation = MutationOperation::Insert(InsertOperation {
        target: target(),
        columns: vec!["name".to_owned(), "tenant_id".to_owned()],
        rows: vec![vec![parameter("name"), parameter("tenant")]],
        returning: vec!["id".to_owned()],
    });

    let postgres =
        compile_relational_mutation(ProviderKind::Postgres, &operation).expect("insert PostgreSQL");
    assert_eq!(postgres.bind_names, ["name", "tenant"]);
    assert!(postgres.sql.contains("RETURNING \"id\""));
    assert!(!postgres.sql.contains("name-value-private"));
    assert!(postgres.returns_rows);

    let sqlserver = compile_relational_mutation(ProviderKind::Sqlserver, &operation)
        .expect("insert SQL Server");
    assert!(sqlserver.sql.contains("OUTPUT INSERTED.[id]"));
    assert_eq!(sqlserver.bind_names, ["name", "tenant"]);

    assert!(compile_relational_mutation(ProviderKind::Mysql, &operation).is_err());
    assert!(compile_relational_mutation(ProviderKind::Db2, &operation).is_err());
}

#[test]
fn postgres_spatial_value_keeps_payload_in_a_named_bind() {
    let operation = MutationOperation::Insert(InsertOperation {
        target: target(),
        columns: vec!["shape".to_owned()],
        rows: vec![vec![QueryExpression::SpatialValue {
            expression: Box::new(parameter("shape")),
            srid: 4_326,
            semantics: SpatialSemantics::Geometry,
        }]],
        returning: Vec::new(),
    });

    let postgres = compile_relational_mutation(ProviderKind::Postgres, &operation)
        .expect("bind spatial PostgreSQL");
    assert_eq!(postgres.bind_names, ["shape"]);
    assert!(
        postgres
            .sql
            .contains("ST_SetSRID(ST_GeomFromEWKB($1), 4326)"),
        "{}",
        postgres.sql
    );
    assert!(compile_relational_mutation(ProviderKind::Mysql, &operation).is_err());
}

#[test]
fn update_orders_assignment_binds_before_filter_binds() {
    let operation = MutationOperation::Update(UpdateOperation {
        target: target(),
        assignments: vec![MutationAssignment {
            column: "name".to_owned(),
            value: parameter("new_name"),
        }],
        filter: Some(QueryExpression::Compare {
            left: Box::new(column("id")),
            operator: ComparisonOperator::Eq,
            right: Box::new(parameter("identity")),
        }),
        returning: Vec::new(),
    });

    for provider in [
        ProviderKind::Postgres,
        ProviderKind::Mysql,
        ProviderKind::Mariadb,
        ProviderKind::Sqlserver,
        ProviderKind::Db2,
    ] {
        let lowered = compile_relational_mutation(provider, &operation).expect("update portabile");
        assert_eq!(lowered.bind_names, ["new_name", "identity"]);
        assert!(!lowered.returns_rows);
    }
}

#[test]
fn upsert_keeps_insert_and_conflict_binds_in_one_named_layout() {
    let operation = MutationOperation::Upsert(UpsertOperation {
        target: target(),
        columns: vec!["id".to_owned(), "name".to_owned()],
        rows: vec![vec![parameter("identity"), parameter("insert_name")]],
        conflict_target: vec!["id".to_owned()],
        update_on_conflict: vec![MutationAssignment {
            column: "name".to_owned(),
            value: parameter("updated_name"),
        }],
        returning: Vec::new(),
    });

    for provider in [
        ProviderKind::Postgres,
        ProviderKind::Mysql,
        ProviderKind::Mariadb,
        ProviderKind::Sqlserver,
        ProviderKind::Db2,
    ] {
        let lowered = compile_relational_mutation(provider, &operation).expect("upsert portabile");
        assert_eq!(
            lowered.bind_names,
            ["identity", "insert_name", "updated_name"]
        );
        assert!(!lowered.returns_rows);
    }
}

#[test]
fn unsupported_dml_expression_fails_closed() {
    let operation = MutationOperation::Delete(DeleteOperation {
        target: target(),
        filter: Some(QueryExpression::Exists {
            query: Box::new(crate::relational::QueryOperation {
                common_table_expressions: Vec::new(),
                source: None,
                derived_source: None,
                projection: Vec::new(),
                joins: Vec::new(),
                filter: None,
                group_by: Vec::new(),
                having: None,
                order_by: Vec::new(),
                distinct: false,
                distinct_on: Vec::new(),
                set_operations: Vec::new(),
                row_limit: None,
                row_offset: None,
                locking: None,
                declared_crs: Vec::new(),
            }),
            negated: false,
        }),
        returning: Vec::new(),
    });

    assert!(compile_relational_mutation(ProviderKind::Postgres, &operation).is_err());
}
