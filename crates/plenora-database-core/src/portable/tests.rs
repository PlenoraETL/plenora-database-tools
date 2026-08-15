#![allow(clippy::float_cmp)] // parametri letterali f64 usati come chiave di assertion

use super::compiler::compile_portable;
use super::*;
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;

/// EWKB Point 2D con SRID prefixed — little-endian.
/// Formato: 0x01 (byte order LE) + `type_with_srid_flag` (0x20000001)
/// + srid (u32 LE) + x (f64 LE) + y (f64 LE).
///
/// Usato dai test golden compiler post-fix EWKB obbligatorio: prima
/// bastavano dummy `vec![0x01, 0x02, 0x03]`, ora il compiler chiama
/// `reference.validate()` che richiede EWKB parsabile.
fn ewkb_point_2d(srid: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(25);
    b.push(0x01);
    b.extend_from_slice(&0x2000_0001_u32.to_le_bytes()); // Point + SRID flag
    b.extend_from_slice(&srid.to_le_bytes());
    b.extend_from_slice(&9.19_f64.to_le_bytes());
    b.extend_from_slice(&45.46_f64.to_le_bytes());
    b
}

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
    // SQL Server e Oracle non sono ancora supportati dal compiler
    // portable — MySQL è stato aggiunto in sessione C.
    let stmt = select_all("t").into_statement();
    let err = compile_portable(ProviderKind::Sqlserver, &stmt).unwrap_err();
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
                ewkb: ewkb_point_2d(4326),
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
            .contains(r#"ST_Intersects("geom", ST_SetSRID(ST_GeomFromEWKB($1), $2)::geometry)"#),
        "sql inatteso: {}",
        compiled.sql
    );
    // Post fix review: 2 params — [0]=ewkb, [1]=srid.
    assert_eq!(compiled.params.len(), 2);
    assert!(matches!(&compiled.params[0], ParameterValue::Bytes(b) if b == &ewkb_point_2d(4326)));
    assert!(matches!(&compiled.params[1], ParameterValue::I32(v) if *v == 4326));
}

#[test]
fn spatial_predicate_dwithin_binds_distance() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    // SRID 3857 (web mercator, unità metri) + Geometry: unità coerenti.
    let stmt = select("poi", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::DWithin {
                distance_meters: 250.0,
            },
            SpatialReference {
                ewkb: ewkb_point_2d(3857),
                srid: 3857,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geometry,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(compiled
        .sql
        .contains(r#"ST_DWithin("geom", ST_SetSRID(ST_GeomFromEWKB($1), $2)::geometry, $3)"#));
    // Post fix review: 3 params ora — [0]=ewkb, [1]=srid, [2]=distance.
    assert_eq!(compiled.params.len(), 3);
    assert!(matches!(&compiled.params[2], ParameterValue::F64(v) if *v == 250.0));
}

#[test]
fn spatial_dwithin_negative_distance_is_rejected() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    // Uso 3857 per isolare il check "distanza negativa" da quello
    // "Geometry + SRID geografico" (entrambi rifiuterebbero — vogliamo
    // esercitare specifico il primo).
    let stmt = select("t", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::DWithin {
                distance_meters: -1.0,
            },
            SpatialReference {
                ewkb: ewkb_point_2d(3857),
                srid: 3857,
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
                ewkb: ewkb_point_2d(4326),
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geometry,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    assert!(compiled.sql.contains(r#""geom" && ST_SetSRID(ST_GeomFromEWKB"#));
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
                    ewkb: ewkb_point_2d(4326),
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
    // Post fix review: 3 params ora — [0]=ewkb, [1]=srid, [2]=status.
    assert!(compiled.sql.contains(r#""status" = $3"#));
    assert_eq!(compiled.params.len(), 3);
}

// ---- Review #4 + #5: SpatialSemantics + DWithin unit safety ----------------

#[test]
fn spatial_geography_uses_geography_cast_postgres() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    let stmt = select("poi", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::Intersects,
            SpatialReference {
                ewkb: ewkb_point_2d(4326),
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geography,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    // Fix #4: cast della colonna + del riferimento a geography.
    assert!(
        compiled.sql.contains(r#"ST_Intersects("geom"::geography, ST_SetSRID(ST_GeomFromEWKB($1), $2)::geography)"#),
        "sql inatteso: {}",
        compiled.sql
    );
}

#[test]
fn spatial_dwithin_geography_wgs84_is_accepted_and_uses_meters() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    let stmt = select("poi", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::DWithin {
                distance_meters: 500.0,
            },
            SpatialReference {
                ewkb: ewkb_point_2d(4326),
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geography,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
    // Geography su 4326 → cast a geography, DWithin usa metri veri.
    assert!(compiled
        .sql
        .contains(r#"ST_DWithin("geom"::geography, ST_SetSRID(ST_GeomFromEWKB($1), $2)::geography, $3)"#));
    // params: [0]=ewkb, [1]=srid, [2]=distance.
    assert!(matches!(&compiled.params[2], ParameterValue::F64(v) if *v == 500.0));
}

#[test]
fn spatial_dwithin_geometry_on_geographic_srid_is_rejected() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    // Fix #5: Geometry + SRID 4326 + DWithin = silent wrong result
    // (distanza in gradi invece che metri) → fail-closed InvalidPlan.
    for srid in [4326_u32, 4269, 4267, 4258, 4283] {
        let stmt = select("poi", vec!["id"])
            .where_(spatial(
                "geom",
                SpatialPredicate::DWithin {
                    distance_meters: 100.0,
                },
                SpatialReference {
                    ewkb: ewkb_point_2d(srid),
                    srid,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();
        let err = compile_portable(ProviderKind::Postgres, &stmt).unwrap_err();
        assert_eq!(
            err.category,
            crate::ErrorCategory::InvalidPlan,
            "SRID {srid} doveva essere rifiutato"
        );
        assert!(
            err.message.contains("SpatialSemantics::Geography"),
            "err message deve suggerire Geography: {}",
            err.message
        );
    }
}

#[test]
fn spatial_dwithin_geometry_on_projected_srid_is_allowed() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    // Geometry + SRID proiettato (3857 web-mercator, 25832 ETRS89/UTM 32N) →
    // le unità del SRID sono metri, distance_meters è semanticamente corretto.
    for srid in [3857_u32, 25832, 32633] {
        let stmt = select("poi", vec!["id"])
            .where_(spatial(
                "geom",
                SpatialPredicate::DWithin {
                    distance_meters: 100.0,
                },
                SpatialReference {
                    ewkb: ewkb_point_2d(srid),
                    srid,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert!(
            compiled.sql.contains("ST_DWithin"),
            "SRID {srid}: sql inatteso {}",
            compiled.sql
        );
        assert!(compiled.sql.contains("::geometry"));
    }
}

#[test]
fn spatial_bounding_box_with_geography_is_rejected() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    // BoundingBox usa operator `&&` che esiste solo su geometry.
    let stmt = select("t", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::BoundingBox,
            SpatialReference {
                ewkb: ewkb_point_2d(4326),
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geography,
            },
        ))
        .into_statement();
    let err = compile_portable(ProviderKind::Postgres, &stmt).unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::Unsupported);
    assert!(err.message.contains("BoundingBox"));
}

#[test]
fn spatial_mysql_geography_is_accepted_as_hint_only() {
    use crate::geometry::{Dimensions, SpatialSemantics};
    // MySQL non ha `::geography` — Geography è hint semantico, non cast.
    let stmt = select("poi", vec!["id"])
        .where_(spatial(
            "geom",
            SpatialPredicate::Intersects,
            SpatialReference {
                ewkb: ewkb_point_2d(4326),
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geography,
            },
        ))
        .into_statement();
    let compiled = compile_portable(ProviderKind::Mysql, &stmt).unwrap();
    // Nessun cast, solo ST_GeomFromWKB.
    // Post fix review #5 completo: MySQL usa ST_GeomFromWKB(wkb, srid).
    assert!(compiled.sql.contains("ST_Intersects(`geom`, ST_GeomFromWKB(?, ?))"));
    // Sanity: no `::geography`.
    assert!(!compiled.sql.contains("::geography"));
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
