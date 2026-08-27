#![allow(clippy::float_cmp)] // parametri letterali f64 usati come chiave di assertion

use super::compiler::compile_portable;
use super::*;
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;

/// EWKB Point 2D con SRID prefixed — little-endian.
/// Formato: 0x01 (byte order LE) + `type_with_srid_flag` (0x20000001)
/// + srid (u32 LE) + x (f64 LE) + y (f64 LE).
///
/// Il compilatore chiama `reference.validate()`, quindi i test devono usare
/// un EWKB parsabile e coerente con il riferimento dichiarato.
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
    assert_eq!(compiled.sql, r#"DELETE FROM "session" WHERE "token" = $1"#);
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
                predicate: Box::new(Predicate::IsNull { column: "c".into() }),
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
fn a_provider_without_a_dialect_is_refused_and_names_itself() {
    // La prova nominava `Sqlserver`, ed e stata vera finche il compilatore non
    // ha imparato il T-SQL. Il refuso da evitare e piu grande di un nome
    // sbagliato: un elenco di «non supportati» che invecchia in silenzio e
    // esattamente cio che teneva `Mariadb` nel ramo di scarto mentre il
    // repository pubblicava il provider.
    //
    // Ora cammina su **tutti** i tipi di provider: quelli con un dialetto
    // devono compilare, gli altri devono rifiutare nominandosi. Se domani
    // qualcuno aggiunge un dialetto e dimentica questa prova, e la prova a
    // seguirlo invece di restare indietro.
    let stmt = select_all("t").into_statement();
    for kind in [
        ProviderKind::Postgres,
        ProviderKind::Mysql,
        ProviderKind::Mariadb,
        ProviderKind::Sqlserver,
        ProviderKind::Oracle,
        ProviderKind::Db2,
        ProviderKind::Sqlite,
        ProviderKind::Duckdb,
    ] {
        let compiled = compile_portable(kind, &stmt);
        let has_provider = matches!(
            kind,
            ProviderKind::Postgres
                | ProviderKind::Mysql
                | ProviderKind::Mariadb
                | ProviderKind::Sqlserver
        );
        match compiled {
            Ok(statement) => assert!(
                has_provider,
                "{kind:?} non ha un provider in questo repository e compila: {}",
                statement.sql
            ),
            Err(error) => {
                assert!(!has_provider, "{kind:?} ha un provider e non compila");
                assert_eq!(error.category, crate::ErrorCategory::Unsupported);
                assert!(
                    error.message.contains(&format!("{kind:?}")),
                    "il rifiuto non nomina il provider: {}",
                    error.message
                );
            }
        }
    }
}

#[test]
fn sqlserver_speaks_tsql_where_the_others_speak_sql() {
    // Le quattro divergenze in una prova sola, perche sono la stessa
    // decisione: `@P1` invece di `$1` o `?`, `[ident]` invece di `"ident"`,
    // `TOP` prima della projection invece di `LIMIT` dopo, e nessun `LIMIT` in
    // coda a ricordarlo.
    let mut select = select("t", vec!["id"]);
    select.filter = Some(eq("id", ParameterValue::I64(7)));
    select.order_by = vec![OrderBy {
        column: "id".into(),
        direction: Direction::Asc,
        nulls: None,
    }];
    select.limit = Some(10);
    let compiled =
        compile_portable(ProviderKind::Sqlserver, &select.into_statement()).expect("compila");
    assert_eq!(
        compiled.sql,
        "SELECT TOP (10) [id] FROM [t] WHERE [id] = @P1 ORDER BY [id] ASC"
    );
    assert_eq!(compiled.params.len(), 1);
}

#[test]
fn what_a_tsql_write_returns_is_asked_in_the_middle() {
    // `RETURNING` sta in coda su tre dialetti su quattro. `OUTPUT` no, e
    // appenderlo avrebbe prodotto SQL che il server rifiuta: sta prima di
    // `VALUES`, prima di `WHERE`, e subito dopo `DELETE FROM t`.
    let insert = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("t"),
        columns: vec!["a".into()],
        values: vec![vec![Expression::literal(ParameterValue::I64(1))]],
        returning: vec!["id".into()],
    });
    let compiled = compile_portable(ProviderKind::Sqlserver, &insert).expect("insert");
    assert_eq!(
        compiled.sql,
        "INSERT INTO [t] ([a]) OUTPUT INSERTED.[id] VALUES (@P1)"
    );

    let delete = PortableStatement::Delete(DeleteStatement {
        table: TableRef::new("t"),
        filter: Some(eq("id", ParameterValue::I64(1))),
        returning: vec!["id".into()],
    });
    let compiled = compile_portable(ProviderKind::Sqlserver, &delete).expect("delete");
    // `DELETED`, non `INSERTED`: chiedere la riga inserita a una cancellazione
    // renderebbe colonne nulle invece di un errore.
    assert_eq!(
        compiled.sql,
        "DELETE FROM [t] OUTPUT DELETED.[id] WHERE [id] = @P1"
    );

    let update = PortableStatement::Update(UpdateStatement {
        table: TableRef::new("t"),
        assignments: vec![("v".into(), Expression::literal(ParameterValue::I64(2)))],
        filter: Some(eq("id", ParameterValue::I64(1))),
        returning: vec!["v".into()],
    });
    let compiled = compile_portable(ProviderKind::Sqlserver, &update).expect("update");
    assert_eq!(
        compiled.sql,
        "UPDATE [t] SET [v] = @P1 OUTPUT INSERTED.[v] WHERE [id] = @P2"
    );
}

#[test]
fn the_tsql_upsert_locks_and_stays_on_one_row() {
    let upsert =
        |values: Vec<Vec<Expression>>, target: Vec<String>, sets: Vec<(String, Expression)>| {
            PortableStatement::Upsert(UpsertStatement {
                table: TableRef::new("t"),
                columns: vec!["id".into(), "v".into()],
                values,
                conflict_target: target,
                update_on_conflict: sets,
                returning: Vec::new(),
            })
        };
    let row = || {
        vec![
            Expression::literal(ParameterValue::I64(1)),
            Expression::literal(ParameterValue::I64(2)),
        ]
    };
    let stmt = upsert(
        vec![row()],
        vec!["id".into()],
        vec![("v".into(), Expression::literal(ParameterValue::I64(3)))],
    );
    let compiled = compile_portable(ProviderKind::Sqlserver, &stmt).expect("upsert");
    // Il segnaposto della chiave compare due volte e lega lo stesso valore: e
    // cio che un `?` non permetterebbe.
    assert!(
        compiled.sql.contains("WHERE [id] = @P1"),
        "{}",
        compiled.sql
    );
    assert!(
        compiled.sql.contains("VALUES (@P1, @P2)"),
        "{}",
        compiled.sql
    );
    assert!(
        compiled.sql.contains("WITH (UPDLOCK, HOLDLOCK)"),
        "{}",
        compiled.sql
    );
    assert!(
        compiled.sql.contains("IF @@ROWCOUNT = 0"),
        "{}",
        compiled.sql
    );
    // Tre valori legati: due della riga e uno dell'assegnamento.
    assert_eq!(compiled.params.len(), 3);

    // Il «non fare niente» guarda sotto lock e inserisce solo se non c'e.
    let nothing = upsert(vec![row()], vec!["id".into()], Vec::new());
    let compiled = compile_portable(ProviderKind::Sqlserver, &nothing).expect("do nothing");
    assert!(
        compiled
            .sql
            .starts_with("IF NOT EXISTS (SELECT 1 FROM [t] WITH (UPDLOCK, HOLDLOCK)"),
        "{}",
        compiled.sql
    );

    let two_rows = upsert(vec![row(), row()], vec!["id".into()], Vec::new());
    let error = compile_portable(ProviderKind::Sqlserver, &two_rows).expect_err("due righe");
    assert_eq!(error.category, crate::ErrorCategory::Unsupported);

    let no_target = upsert(vec![row()], Vec::new(), Vec::new());
    let error = compile_portable(ProviderKind::Sqlserver, &no_target).expect_err("senza chiave");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn the_tsql_upsert_refuses_to_say_which_of_its_two_statements_acted() {
    let stmt = PortableStatement::Upsert(UpsertStatement {
        table: TableRef::new("t"),
        columns: vec!["id".into()],
        values: vec![vec![Expression::literal(ParameterValue::I64(1))]],
        conflict_target: vec!["id".into()],
        update_on_conflict: Vec::new(),
        returning: vec!["id".into()],
    });
    let error = compile_portable(ProviderKind::Sqlserver, &stmt).expect_err("returning upsert");
    assert_eq!(error.category, crate::ErrorCategory::Unsupported);
    assert!(error.message.contains("OUTPUT"), "{}", error.message);
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
    // Il costruttore spaziale occupa due bind: EWKB e SRID.
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
    // Il terzo bind è la distanza, dopo EWKB e SRID.
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
    assert!(compiled
        .sql
        .contains(r#""geom" && ST_SetSRID(ST_GeomFromEWKB"#));
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
    // Il predicato scalare segue i due bind del riferimento spaziale.
    assert!(compiled.sql.contains(r#""status" = $3"#));
    assert_eq!(compiled.params.len(), 3);
}

// Semantica spaziale e unità di misura di DWithin.

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
        compiled.sql.contains(
            r#"ST_Intersects("geom"::geography, ST_SetSRID(ST_GeomFromEWKB($1), $2)::geography)"#
        ),
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
    assert!(compiled.sql.contains(
        r#"ST_DWithin("geom"::geography, ST_SetSRID(ST_GeomFromEWKB($1), $2)::geography, $3)"#
    ));
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
    // MySQL costruisce la geometria passando WKB e SRID separatamente.
    assert!(compiled
        .sql
        .contains("ST_Intersects(`geom`, ST_GeomFromWKB(?, ?))"));
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

// === MariaDB: un prodotto diverso, e una sola divergenza misurata ===========

/// Uno statement banale, per chiedere solo del dialetto.
fn mariadb_insert(returning: Vec<String>) -> PortableStatement {
    PortableStatement::Insert(InsertStatement {
        table: TableRef::new("t"),
        columns: vec!["a".into()],
        values: vec![vec![Expression::literal(ParameterValue::I32(1))]],
        returning,
    })
}

/// `MariaDB` usa la sintassi `MySQL` anche nello strato portabile condiviso da
/// `execute_portable` e `query_portable`.
#[test]
fn mariadb_compiles_with_the_mysql_syntax() {
    let compiled = compile_portable(ProviderKind::Mariadb, &mariadb_insert(Vec::new()))
        .expect("MariaDB e un dialetto del compilatore");
    assert_eq!(compiled.sql, "INSERT INTO `t` (`a`) VALUES (?)");
    // Il segnaposto e `?`, non `$1`: dove i due prodotti si somigliano, si
    // somigliano davvero.
    let mysql = compile_portable(ProviderKind::Mysql, &mariadb_insert(Vec::new())).expect("MySQL");
    assert_eq!(compiled.sql, mysql.sql);
}

/// `RETURNING` su `MySQL` e chiuso su **ogni** forma, e il messaggio non cita
/// una versione che non esiste.
#[test]
fn mysql_refuses_returning_on_every_form() {
    let error = compile_portable(ProviderKind::Mysql, &mariadb_insert(vec!["id".into()]))
        .expect_err("MySQL non ha RETURNING");
    assert_eq!(error.category, crate::ErrorCategory::Unsupported);
    // Il messaggio diceva «solo 8.0.20+ per INSERT», che e falso: interrogato
    // con le cinque forme, MySQL 9.7 risponde 1064 a tutte.
    assert!(
        !error.message.contains("8.0.20"),
        "il messaggio cita una versione che non c'entra: {}",
        error.message
    );
}

/// Su `MariaDB` `RETURNING` dipende dalla **forma**, e la tabella e quella
/// misurata sui riferimenti: aperta su `INSERT`, `DELETE` e upsert, chiusa
/// sull'`UPDATE`, che il server rifiuta con un errore di sintassi.
#[test]
fn mariadb_returning_follows_the_measured_form_table() {
    let insert = compile_portable(ProviderKind::Mariadb, &mariadb_insert(vec!["id".into()]))
        .expect("INSERT ... RETURNING e misurato aperto");
    assert_eq!(
        insert.sql,
        "INSERT INTO `t` (`a`) VALUES (?) RETURNING `id`"
    );

    let delete = compile_portable(
        ProviderKind::Mariadb,
        &PortableStatement::Delete(DeleteStatement {
            table: TableRef::new("t"),
            filter: Some(eq("a", ParameterValue::I32(1))),
            returning: vec!["id".into()],
        }),
    )
    .expect("DELETE ... RETURNING e misurato aperto");
    assert_eq!(delete.sql, "DELETE FROM `t` WHERE `a` = ? RETURNING `id`");

    let upsert = compile_portable(
        ProviderKind::Mariadb,
        &PortableStatement::Upsert(UpsertStatement {
            table: TableRef::new("t"),
            columns: vec!["a".into()],
            values: vec![vec![Expression::literal(ParameterValue::I32(1))]],
            conflict_target: vec!["a".into()],
            update_on_conflict: vec![("a".into(), Expression::literal(ParameterValue::I32(2)))],
            returning: vec!["id".into()],
        }),
    )
    .expect("l'upsert rende le righe su entrambi i rami, misurato");
    assert!(
        upsert.sql.ends_with(" RETURNING `id`"),
        "l'upsert perde la clausola: {}",
        upsert.sql
    );

    let error = compile_portable(
        ProviderKind::Mariadb,
        &PortableStatement::Update(UpdateStatement {
            table: TableRef::new("t"),
            assignments: vec![("a".into(), Expression::literal(ParameterValue::I32(2)))],
            filter: None,
            returning: vec!["id".into()],
        }),
    )
    .expect_err("UPDATE ... RETURNING e l'unica forma che MariaDB rifiuta");
    assert_eq!(error.category, crate::ErrorCategory::Unsupported);
    assert!(
        error.message.contains("UPDATE"),
        "il rifiuto non dice quale forma: {}",
        error.message
    );
}

/// Il rifiuto dell'`UPDATE` non e un rifiuto del `RETURNING` in generale.
///
/// La distinzione conta perche il modo piu semplice di scrivere questa
/// funzione — una bandiera sul dialetto — le avrebbe confuse, e avrebbe
/// chiuso tre forme che il server accetta per colpa della quarta.
#[test]
fn mariadb_update_without_returning_still_compiles() {
    let compiled = compile_portable(
        ProviderKind::Mariadb,
        &PortableStatement::Update(UpdateStatement {
            table: TableRef::new("t"),
            assignments: vec![("a".into(), Expression::literal(ParameterValue::I32(2)))],
            filter: None,
            returning: Vec::new(),
        }),
    )
    .expect("un UPDATE senza RETURNING non ha niente di divergente");
    assert_eq!(compiled.sql, "UPDATE `t` SET `a` = ?");
}
