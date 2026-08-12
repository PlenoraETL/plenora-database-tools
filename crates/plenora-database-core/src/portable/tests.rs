use super::compiler::compile_portable;
use super::*;
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;

#[test]
fn select_all_produces_select_star() {
    let stmt = select_all("users").into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(compiled.sql, r#"SELECT * FROM "users""#);
    assert!(compiled.params.is_empty());
}

#[test]
fn select_columns_where_order_limit() {
    let stmt = select("users", vec!["id", "email"])
        .schema("app")
        .where_(eq("tenant_id", ParameterValue::I64(42)))
        .order_by("id", Direction::Asc)
        .limit(100)
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"SELECT "id", "email" FROM "app"."users" WHERE "tenant_id" = $1 ORDER BY "id" ASC LIMIT 100"#
    );
    assert_eq!(compiled.params, vec![ParameterValue::I64(42)]);
}

#[test]
fn insert_binds_positional_and_returns() {
    let stmt = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("t"),
        columns: vec!["a".into(), "b".into()],
        values: vec![vec![
            Expression::literal(ParameterValue::I32(1)),
            Expression::literal(ParameterValue::String("x".into())),
        ]],
        returning: vec!["id".into()],
    });
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"INSERT INTO "t" ("a", "b") VALUES ($1, $2) RETURNING "id""#
    );
    assert_eq!(
        compiled.params,
        vec![ParameterValue::I32(1), ParameterValue::String("x".into())]
    );
}

#[test]
fn update_with_where_and_returning() {
    let stmt = PortableStatement::Update(UpdateStatement {
        table: TableRef::new("work_order"),
        assignments: vec![
            (
                "status".into(),
                Expression::literal(ParameterValue::String("done".into())),
            ),
            (
                "version".into(),
                Expression::literal(ParameterValue::I64(18)),
            ),
        ],
        filter: Some(and(vec![
            eq("id", ParameterValue::I64(42)),
            eq("version", ParameterValue::I64(17)),
        ])),
        returning: vec!["version".into()],
    });
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"UPDATE "work_order" SET "status" = $1, "version" = $2 WHERE ("id" = $3 AND "version" = $4) RETURNING "version""#
    );
    assert_eq!(compiled.params.len(), 4);
}

#[test]
fn delete_with_where() {
    let stmt = PortableStatement::Delete(DeleteStatement {
        table: TableRef::new("session"),
        filter: Some(eq("token", ParameterValue::String("abc".into()))),
        returning: Vec::new(),
    });
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"DELETE FROM "session" WHERE "token" = $1"#
    );
}

#[test]
fn upsert_do_nothing() {
    let stmt = PortableStatement::Upsert(UpsertStatement {
        table: TableRef::new("cache"),
        columns: vec!["k".into(), "v".into()],
        values: vec![vec![
            Expression::literal(ParameterValue::String("x".into())),
            Expression::literal(ParameterValue::I32(1)),
        ]],
        conflict_target: vec!["k".into()],
        update_on_conflict: Vec::new(),
        returning: Vec::new(),
    });
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"INSERT INTO "cache" ("k", "v") VALUES ($1, $2) ON CONFLICT ("k") DO NOTHING"#
    );
}

#[test]
fn upsert_do_update_set() {
    let stmt = PortableStatement::Upsert(UpsertStatement {
        table: TableRef::new("cache"),
        columns: vec!["k".into(), "v".into()],
        values: vec![vec![
            Expression::literal(ParameterValue::String("x".into())),
            Expression::literal(ParameterValue::I32(1)),
        ]],
        conflict_target: vec!["k".into()],
        update_on_conflict: vec![("v".into(), Expression::literal(ParameterValue::I32(2)))],
        returning: Vec::new(),
    });
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"INSERT INTO "cache" ("k", "v") VALUES ($1, $2) ON CONFLICT ("k") DO UPDATE SET "v" = $3"#
    );
}

#[test]
fn predicates_compose_and_or_not() {
    let stmt = select("t", vec!["id"])
        .where_(and(vec![
            Predicate::Or {
                predicates: vec![
                    eq("a", ParameterValue::I32(1)),
                    eq("b", ParameterValue::I32(2)),
                ],
            },
            Predicate::Not {
                predicate: Box::new(Predicate::IsNull {
                    column: "c".into(),
                }),
            },
        ]))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"SELECT "id" FROM "t" WHERE (("a" = $1 OR "b" = $2) AND NOT ("c" IS NULL))"#
    );
}

#[test]
fn in_predicate_binds_each_value() {
    let stmt = select("t", vec!["id"])
        .where_(Predicate::In {
            column: "status".into(),
            values: vec![
                Expression::literal(ParameterValue::String("a".into())),
                Expression::literal(ParameterValue::String("b".into())),
                Expression::literal(ParameterValue::String("c".into())),
            ],
        })
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert_eq!(
        compiled.sql,
        r#"SELECT "id" FROM "t" WHERE "status" IN ($1, $2, $3)"#
    );
    assert_eq!(compiled.params.len(), 3);
}

#[test]
fn between_and_like() {
    let stmt = select("t", vec!["id"])
        .where_(and(vec![
            Predicate::Between {
                column: "created".into(),
                low: Expression::literal(ParameterValue::Date("2026-01-01".into())),
                high: Expression::literal(ParameterValue::Date("2026-12-31".into())),
            },
            Predicate::Like {
                column: "name".into(),
                pattern: Expression::literal(ParameterValue::String("acme%".into())),
            },
        ]))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(compiled.sql.contains("BETWEEN $1 AND $2"));
    assert!(compiled.sql.contains(r#""name" LIKE $3"#));
}

#[test]
fn invalid_identifier_is_rejected() {
    let stmt = select("t\x00evil", vec!["id"]).into_statement();
    let err = compile_portable(ProviderKind::Postgres, &stmt).unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn identifiers_are_quoted_and_double_quotes_escaped() {
    let stmt = select("evil\"table", vec![r#"c"ol"#]).into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(compiled.sql.contains(r#""evil""table""#));
    assert!(compiled.sql.contains(r#""c""ol""#));
}

#[test]
fn unsupported_provider_returns_unsupported() {
    let stmt = select_all("t").into_statement();
    let err = compile_portable(ProviderKind::Mysql, &stmt).unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::Unsupported);
}

#[test]
fn empty_in_predicate_is_rejected() {
    let stmt = select("t", vec!["id"])
        .where_(Predicate::In {
            column: "x".into(),
            values: Vec::new(),
        })
        .into_statement();
    assert!(compile_portable(ProviderKind::Postgres, &stmt).is_err());
}

#[test]
fn spatial_predicate_intersects_binds_ewkb() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    let stmt = select("buildings", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::Intersects,
            SpatialReference {
                ewkb: vec![0x01, 0x02, 0x03],
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geometry,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(
        compiled
            .sql
            .contains(r#"ST_Intersects("geom", ST_GeomFromEWKB($1)::geometry)"#),
        "sql inatteso: {}",
        compiled.sql
    );
    assert_eq!(compiled.params.len(), 1);
    assert!(matches!(&compiled.params[0], ParameterValue::Bytes(b) if b == &[0x01, 0x02, 0x03]));
}

#[test]
fn spatial_predicate_dwithin_binds_distance() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    let stmt = select("poi", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::DWithin {
                distance_meters: 250.0,
            },
            SpatialReference {
                ewkb: vec![0xff],
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geometry,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(compiled
        .sql
        .contains(r#"ST_DWithin("geom", ST_GeomFromEWKB($1)::geometry, $2)"#));
    assert_eq!(compiled.params.len(), 2);
    assert!(matches!(&compiled.params[1], ParameterValue::F64(v) if *v == 250.0));
}

#[test]
fn spatial_dwithin_negative_distance_is_rejected() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    let stmt = select("t", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::DWithin {
                distance_meters: -1.0,
            },
            SpatialReference {
                ewkb: vec![0x00],
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geometry,
            },
        ))
        .into_statement();
    assert!(compile_portable(ProviderKind::Postgres, &stmt).is_err());
}

#[test]
fn spatial_bounding_box_uses_index_operator() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    let stmt = select("t", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::BoundingBox,
            SpatialReference {
                ewkb: vec![0x01],
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geometry,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(compiled.sql.contains(r#""geom" && ST_GeomFromEWKB"#));
}

#[test]
fn spatial_composes_with_scalar_predicates() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    // Filtro composto: bbox AND status = 'active'
    let stmt = select("buildings", vec!["id", "name"])
        .where_(and(vec![
            spatial(
                "geom",
                SpatialPredicate::Intersects,
                SpatialReference {
                    ewkb: vec![0x0a, 0x0b],
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ),
            eq("status", ParameterValue::String("active".into())),
        ]))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(compiled.sql.contains("ST_Intersects"));
    assert!(compiled.sql.contains(r#""status" = $2"#));
    assert_eq!(compiled.params.len(), 2);
}

#[test]
fn insert_arity_mismatch_is_rejected() {
    let stmt = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("t"),
        columns: vec!["a".into(), "b".into()],
        values: vec![vec![Expression::literal(ParameterValue::I32(1))]],
        returning: Vec::new(),
    });
    assert!(compile_portable(ProviderKind::Postgres, &stmt).is_err());
}
