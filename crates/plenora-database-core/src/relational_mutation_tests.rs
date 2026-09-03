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
fn oracle_timestamptz_mutations_keep_the_required_conversion_wrapper() {
    let timestamp = QueryExpression::TypedParameter {
        name: "observed_at".to_owned(),
        parameter_type: crate::relational::QueryParameterType::TimestampTz,
    };
    let insert = MutationOperation::Insert(InsertOperation {
        target: target(),
        columns: vec!["observed_at".to_owned()],
        rows: vec![vec![timestamp.clone()]],
        returning: Vec::new(),
    });
    let insert = compile_relational_mutation(ProviderKind::Oracle, &insert)
        .expect("INSERT TIMESTAMP WITH TIME ZONE Oracle");
    assert_eq!(insert.bind_names, ["observed_at"]);
    assert!(insert
        .sql
        .contains("TO_TIMESTAMP_TZ(:1, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')"));

    let update = MutationOperation::Update(UpdateOperation {
        target: target(),
        assignments: vec![MutationAssignment {
            column: "observed_at".to_owned(),
            value: timestamp.clone(),
        }],
        filter: Some(QueryExpression::Compare {
            left: Box::new(column("observed_at")),
            operator: ComparisonOperator::Eq,
            right: Box::new(timestamp),
        }),
        returning: Vec::new(),
    });
    let update = compile_relational_mutation(ProviderKind::Oracle, &update)
        .expect("UPDATE TIMESTAMP WITH TIME ZONE Oracle");
    assert_eq!(update.bind_names, ["observed_at", "observed_at"]);
    assert_eq!(update.sql.matches("TO_TIMESTAMP_TZ").count(), 2);
}

#[test]
fn qualified_spatial_values_keep_payloads_in_named_binds() {
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
    for provider in [ProviderKind::Mysql, ProviderKind::Mariadb] {
        let lowered =
            compile_relational_mutation(provider, &operation).expect("bind spatial MySQL/MariaDB");
        assert_eq!(lowered.bind_names, ["shape"]);
        assert!(
            lowered
                .sql
                .contains("ST_GeomFromWKB(CAST(? AS BINARY), 4326)"),
            "{}",
            lowered.sql
        );
    }
    let sqlserver = compile_relational_mutation(ProviderKind::Sqlserver, &operation)
        .expect("bind spatial SQL Server");
    assert_eq!(sqlserver.bind_names, ["shape"]);
    assert!(
        sqlserver.sql.contains("geometry::STGeomFromWKB(@P1, 4326)"),
        "{}",
        sqlserver.sql
    );
    let db2 = compile_relational_mutation(ProviderKind::Db2, &operation).expect("bind spatial Db2");
    assert_eq!(db2.bind_names, ["shape"]);
    assert!(
        db2.sql.contains("ST_GEOMETRY(BLOB(HEXTORAW(?)), 4326)"),
        "{}",
        db2.sql
    );

    let geography = MutationOperation::Insert(InsertOperation {
        target: target(),
        columns: vec!["shape".to_owned()],
        rows: vec![vec![QueryExpression::SpatialValue {
            expression: Box::new(parameter("shape")),
            srid: 4_326,
            semantics: SpatialSemantics::Geography,
        }]],
        returning: Vec::new(),
    });
    assert!(compile_relational_mutation(ProviderKind::Mysql, &geography).is_err());
    assert!(compile_relational_mutation(ProviderKind::Mariadb, &geography).is_err());
    assert!(compile_relational_mutation(ProviderKind::Db2, &geography).is_err());
    let sqlserver_geography = compile_relational_mutation(ProviderKind::Sqlserver, &geography)
        .expect("bind geography SQL Server");
    assert!(sqlserver_geography
        .sql
        .contains("geography::STGeomFromWKB(@P1, 4326)"));
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
