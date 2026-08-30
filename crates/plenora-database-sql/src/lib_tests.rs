use super::*;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::relational::{
    ColumnRef, CommonTableExpression, QueryJoin, QueryLock, QueryOrdering, QueryProjection,
    QuerySetOperation,
};

fn identifier(value: &str) -> Identifier {
    Identifier::new(value).expect("identifier fixture")
}

fn source() -> ObjectName {
    ObjectName {
        catalog: None,
        schema: Some(identifier("public")),
        object: identifier("events"),
    }
}

fn query_column(relation: &str, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    }
}

fn query_source(object: &str, alias: &str) -> QuerySource {
    QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: None,
            object: object.to_owned(),
        },
        alias: Some(alias.to_owned()),
    }
}

fn simple_query() -> QueryOperation {
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("e", "id"),
            alias: None,
        }],
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
    }
}

#[test]
fn postgres_spatial_output_encodes_a_column_as_ewkb() {
    let mut query = simple_query();
    query.projection = vec![QueryProjection {
        expression: QueryExpression::SpatialOutput {
            expression: Box::new(query_column("e", "shape")),
            semantics: SpatialSemantics::Geometry,
        },
        alias: Some("shape".to_owned()),
    }];
    let rendered = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .expect("projection EWKB PostgreSQL");
    assert!(
        rendered
            .sql
            .contains("ST_AsEWKB(\"e\".\"shape\") AS \"shape\""),
        "{}",
        rendered.sql
    );

    assert!(Renderer::new(
        Dialect::Mysql,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .is_err());
}

#[test]
fn source_free_parameter_query_uses_each_dialect_bind() {
    let mut query = simple_query();
    query.source = None;
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Parameter {
            name: "answer".to_owned(),
        },
        alias: Some("answer".to_owned()),
    }];
    for (dialect, placeholder) in [
        (Dialect::Postgres, "$1"),
        (Dialect::Mysql, "?"),
        (Dialect::SqlServer, "@p1"),
    ] {
        let rendered = Renderer::new(
            dialect,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_query(&query)
        .expect("source-free parameter query");
        assert!(rendered.sql.starts_with("SELECT "), "{dialect:?}");
        assert!(!rendered.sql.contains(" FROM "), "{dialect:?}");
        assert!(rendered.sql.contains(placeholder), "{dialect:?}");
        assert_eq!(rendered.binds[0].name, "answer");
    }
}

#[test]
fn db2_rejects_an_untyped_parameter_projection_before_execution() {
    let mut query = simple_query();
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Parameter {
            name: "answer".to_owned(),
        },
        alias: Some("answer".to_owned()),
    }];
    let error = Renderer::new(
        Dialect::Db2,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .expect_err("Db2 non puo inferire il tipo del parametro nella projection");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Unsupported
    );
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Prepare);
}

#[test]
fn db2_renders_a_typed_parameter_projection_with_a_type_context() {
    let mut query = simple_query();
    query.source = None;
    query.projection = vec![QueryProjection {
        expression: QueryExpression::TypedParameter {
            name: "answer".to_owned(),
            parameter_type: QueryParameterType::Integer,
        },
        alias: Some("answer".to_owned()),
    }];
    let rendered = Renderer::new(
        Dialect::Db2,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .expect("il cast rende il bind tipizzabile da Db2");
    assert_eq!(
        rendered.sql,
        "SELECT CAST(? AS INTEGER) AS \"answer\" FROM SYSIBM.SYSDUMMY1"
    );
    assert_eq!(rendered.binds[0].name, "answer");
}

#[test]
fn db2_correlates_an_unaliased_schema_qualified_table() {
    let mut query = simple_query();
    query.source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("SYSCAT".to_owned()),
            object: "TABLES".to_owned(),
        },
        alias: None,
    });
    query.projection = vec![QueryProjection {
        expression: query_column("TABLES", "TABSCHEMA"),
        alias: None,
    }];
    let rendered = Renderer::new(
        Dialect::Db2,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .expect("source Db2 qualificata senza alias esplicito");
    assert!(rendered
        .sql
        .contains("FROM \"SYSCAT\".\"TABLES\" AS \"TABLES\""));
}

#[test]
fn postgres_uses_quoted_identifiers_and_binds() {
    let select = Select {
        source: source(),
        projection: vec![identifier("select"), identifier("a\"b")],
        filter: Some(Expression::Compare {
            field: identifier("name"),
            operator: ComparisonOperator::Eq,
            parameter: "secret_value".to_owned(),
        }),
        order_by: vec![],
        limit: Some(10),
    };
    let rendered = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_select(&select)
    .expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT \"select\", \"a\"\"b\" FROM \"public\".\"events\" WHERE \"name\" = $1 LIMIT 10"
    );
    assert_eq!(rendered.binds[0].name, "secret_value");
    assert!(!rendered.sql.contains("secret_value"));
}

#[test]
fn legacy_select_lowers_through_the_canonical_ir_on_two_dialects() {
    let select = Select {
        source: source(),
        projection: vec![identifier("id"), identifier("name")],
        filter: Some(Expression::And(vec![
            Expression::In {
                field: identifier("id"),
                parameters: vec!["first".to_owned(), "second".to_owned()],
            },
            Expression::Between {
                field: identifier("score"),
                lower_parameter: "lower".to_owned(),
                upper_parameter: "upper".to_owned(),
            },
            Expression::Like {
                field: identifier("name"),
                parameter: "pattern".to_owned(),
                case_insensitive: false,
            },
        ])),
        order_by: vec![Ordering {
            field: identifier("id"),
            direction: SortDirection::Desc,
        }],
        limit: Some(5),
    };

    for (dialect, expected) in [
        (
            Dialect::Postgres,
            "SELECT \"id\", \"name\" FROM \"public\".\"events\" WHERE (\"id\" IN ($1, $2) AND \"score\" BETWEEN $3 AND $4 AND \"name\" LIKE $5) ORDER BY \"id\" DESC LIMIT 5",
        ),
        (
            Dialect::Mysql,
            "SELECT `id`, `name` FROM `public`.`events` WHERE (`id` IN (?, ?) AND `score` BETWEEN ? AND ? AND `name` LIKE ?) ORDER BY `id` DESC LIMIT 5",
        ),
    ] {
        let rendered = Renderer::new(
            dialect,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_select(&select)
        .expect("canonical lowering");
        assert_eq!(rendered.sql, expected);
        assert_eq!(
            rendered
                .binds
                .iter()
                .map(|bind| bind.name.as_str())
                .collect::<Vec<_>>(),
            ["first", "second", "lower", "upper", "pattern"]
        );
    }
}

#[test]
fn postgres_spatial_renderer_matches_the_versioned_catalog() {
    let catalog = plenora_database_core::spatial_catalog::spatial_function_catalog()
        .expect("embedded spatial catalog");
    assert_eq!(SpatialFunction::ALL.len(), catalog.functions.len());
    for (function, specification) in SpatialFunction::ALL.iter().zip(&catalog.functions) {
        assert_eq!(
            spatial_name(*function),
            specification.postgres,
            "{}",
            specification.id
        );
    }
}

#[test]
fn sqlserver_escapes_closing_bracket() {
    let select = Select {
        source: source(),
        projection: vec![identifier("a]b")],
        filter: None,
        order_by: vec![],
        limit: Some(5),
    };
    let rendered = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_select(&select)
    .expect("render");
    assert_eq!(rendered.sql, "SELECT TOP (5) [a]]b] FROM [public].[events]");
}

#[test]
fn sqlserver_rejects_identifier_over_128_characters() {
    let select = Select {
        source: source(),
        projection: vec![identifier(&"x".repeat(129))],
        filter: None,
        order_by: Vec::new(),
        limit: None,
    };
    let error = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_select(&select)
    .expect_err("identifier must fail");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
}

#[test]
fn sqlserver_enforces_2100_bind_limit() {
    let render = |count| {
        Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_filter(&Expression::In {
            field: identifier("id"),
            parameters: (0..count).map(|index| format!("p{index}")).collect(),
        })
    };
    assert_eq!(render(2_100).expect("2100 binds").binds.len(), 2_100);
    let error = render(2_101).expect_err("2101 binds must fail");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
}

/// Una `Column` in posizione geometrica non viene tipizzata.
///
/// E' il fatto da cui dipende `SpatialFunction::is_aggregate`: se il
/// renderer tipizzasse ogni argomento in posizione geometrica, allora
/// `ST_Union(x, y)` sarebbe sempre la scalare `(geometry, geometry)` e la
/// classificazione potrebbe smettere di rispondere «aggregata possibile».
/// Non lo fa: il wrapping tocca i soli `Parameter`, quindi
/// `ST_Union(geom, gridsize)` con due colonne e formabile e il server
/// risolve l'aggregata `(geometry set, float8)`. Se questo test cade,
/// `is_aggregate` va rivista.
#[test]
fn a_column_in_a_geometry_position_is_not_typed_by_the_renderer() {
    let mut query = simple_query();
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Spatial {
            function: SpatialFunction::Union,
            arguments: vec![query_column("e", "geom"), query_column("e", "grid_size")],
        },
        alias: None,
    }];
    let sql = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query_with_spatial_encoding(&query, false)
    .expect("render")
    .sql;
    assert!(
        sql.contains("ST_Union(\"e\".\"geom\", \"e\".\"grid_size\")"),
        "{sql}"
    );
    assert!(!sql.contains("ST_GeomFromEWKB"), "{sql}");

    // Con un `Parameter` nella stessa posizione il wrapping c'e, e la
    // forma diventa senza ambiguita quella scalare.
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Spatial {
            function: SpatialFunction::Union,
            arguments: vec![
                query_column("e", "geom"),
                QueryExpression::Parameter {
                    name: "other".to_owned(),
                },
            ],
        },
        alias: None,
    }];
    let sql = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query_with_spatial_encoding(&query, false)
    .expect("render")
    .sql;
    assert!(sql.contains("ST_GeomFromEWKB"), "{sql}");
}

#[test]
fn sqlserver_uses_offset_fetch_without_top() {
    let mut query = simple_query();
    query.order_by.push(QueryOrdering {
        expression: query_column("e", "id"),
        direction: SortDirection::Asc,
    });
    query.row_offset = Some(5);
    query.row_limit = Some(10);
    let sql = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .expect("SQL Server pagination")
    .sql;
    assert!(sql.ends_with("ORDER BY [e].[id] ASC OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"));
    assert!(!sql.contains("TOP"));
}

#[test]
fn sqlserver_renders_count_big_and_recursive_cte_syntax() {
    let cte_body = simple_query();
    let mut query = simple_query();
    query.common_table_expressions.push(CommonTableExpression {
        name: "tree".to_owned(),
        recursive: true,
        query: Box::new(cte_body),
    });
    query.projection[0].expression = QueryExpression::Scalar {
        function: ScalarFunction::Count,
        arguments: vec![query_column("e", "id")],
    };
    let sql = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .expect("SQL Server CTE")
    .sql;
    assert!(sql.starts_with("WITH [tree] AS ("));
    assert!(!sql.starts_with("WITH RECURSIVE"));
    assert!(sql.contains("COUNT_BIG([e].[id])"));
}

#[test]
fn sqlserver_exposes_cte_and_native_body_without_sql_reparsing() {
    let cte_body = simple_query();
    let mut query = simple_query();
    query.common_table_expressions.push(CommonTableExpression {
        name: "filtered".to_owned(),
        recursive: false,
        query: Box::new(cte_body),
    });
    let parts = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query_native_spatial_parts(&query)
    .expect("structured SQL Server query parts");
    assert!(parts.with_clause.starts_with("WITH [filtered] AS ("));
    assert!(!parts.body.contains("WITH [filtered]"));
    assert!(parts.body.starts_with("SELECT "));
    assert!(parts.binds.is_empty());
}

#[test]
fn sqlserver_spatial_ast_fails_without_resolved_type_and_srid() {
    let mut query = simple_query();
    query.filter = Some(QueryExpression::Spatial {
        function: SpatialFunction::Intersects,
        arguments: vec![
            query_column("e", "geom"),
            QueryExpression::Parameter {
                name: "probe".to_owned(),
            },
        ],
    });
    let error = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect_err("unresolved spatial input must fail");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Unsupported
    );
}

#[test]
fn sqlserver_spatial_ast_uses_typed_wkb_constructor_and_bound_value() {
    let mut query = simple_query();
    query.filter = Some(QueryExpression::Spatial {
        function: SpatialFunction::Intersects,
        arguments: vec![
            query_column("e", "shape"),
            QueryExpression::Parameter {
                name: "needle".to_owned(),
            },
        ],
    });
    let rendered = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_sql_server_spatial_parameters(BTreeMap::from([(
        "needle".to_owned(),
        SqlServerSpatialParameter {
            semantics: SpatialSemantics::Geometry,
            srid: 4_326,
        },
    )]))
    .render_query(&query)
    .expect("typed SQL Server spatial query");
    assert!(rendered
        .sql
        .contains("([e].[shape].STIntersects(geometry::STGeomFromWKB(@p1, 4326)) = 1)"));
    assert_eq!(rendered.binds[0].name, "needle");
    assert!(!rendered.sql.contains("needle"));
}

#[test]
#[allow(clippy::too_many_lines)]
fn sqlserver_renders_only_the_verified_native_scalar_spatial_subset() {
    let renderer = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_sql_server_spatial_parameters(BTreeMap::from([(
        "needle".to_owned(),
        SqlServerSpatialParameter {
            semantics: SpatialSemantics::Geography,
            srid: 4_326,
        },
    )]));
    for (function, fragment) in [
        (
            SpatialFunction::GeometryType,
            "[e].[shape].STGeometryType()",
        ),
        (SpatialFunction::Srid, "[e].[shape].STSrid"),
        (SpatialFunction::Dimensions, "[e].[shape].STDimension()"),
        (SpatialFunction::NPoints, "[e].[shape].STNumPoints()"),
        (SpatialFunction::IsEmpty, "[e].[shape].STIsEmpty()"),
        (SpatialFunction::IsValid, "[e].[shape].STIsValid()"),
        (SpatialFunction::IsClosed, "[e].[shape].STIsClosed()"),
        (SpatialFunction::Area, "[e].[shape].STArea()"),
        (SpatialFunction::Length, "[e].[shape].STLength()"),
        (SpatialFunction::StartPoint, "[e].[shape].STStartPoint()"),
        (SpatialFunction::EndPoint, "[e].[shape].STEndPoint()"),
        (SpatialFunction::ConvexHull, "[e].[shape].STConvexHull()"),
    ] {
        let mut query = simple_query();
        query.projection[0] = QueryProjection {
            expression: QueryExpression::Spatial {
                function,
                arguments: vec![query_column("e", "shape")],
            },
            alias: Some("value".to_owned()),
        };
        let rendered_sql = renderer.render_query(&query).expect("unary spatial method");
        assert!(rendered_sql.sql.contains(fragment), "{function:?}");
        if function.returns_boolean(1) {
            assert!(!rendered_sql.sql.contains("CASE WHEN"));
            assert!(!rendered_sql.sql.contains(" = 1) AS [value]"));
        }
        if function.returns_geometry() {
            assert!(rendered_sql
                .sql
                .contains(&format!("({fragment}).AsBinaryZM() AS [value]")));
            let native = renderer
                .render_query_native_spatial(&query)
                .expect("native spatial profile");
            assert!(native.sql.contains(&format!("{fragment} AS [value]")));
            assert!(!native.sql.contains("AsBinaryZM"));
        }
        assert!(rendered_sql.binds.is_empty());
    }
    for (function, method) in [
        (SpatialFunction::Intersects, "STIntersects"),
        (SpatialFunction::Contains, "STContains"),
        (SpatialFunction::Within, "STWithin"),
        (SpatialFunction::Disjoint, "STDisjoint"),
        (SpatialFunction::Equals, "STEquals"),
        (SpatialFunction::Distance, "STDistance"),
        (SpatialFunction::Intersection, "STIntersection"),
        (SpatialFunction::Difference, "STDifference"),
        (SpatialFunction::SymDifference, "STSymDifference"),
        (SpatialFunction::Union, "STUnion"),
    ] {
        let mut query = simple_query();
        query.projection[0] = QueryProjection {
            expression: QueryExpression::Spatial {
                function,
                arguments: vec![
                    query_column("e", "shape"),
                    QueryExpression::Parameter {
                        name: "needle".to_owned(),
                    },
                ],
            },
            alias: Some("value".to_owned()),
        };
        let rendered_sql = renderer
            .render_query(&query)
            .expect("binary spatial method");
        assert!(
            rendered_sql.sql.contains(&format!(
                "[e].[shape].{method}(geography::STGeomFromWKB(@p1, 4326))"
            )),
            "{function:?}"
        );
        if function.returns_boolean(2) {
            assert!(!rendered_sql.sql.contains("CASE WHEN"));
            assert!(!rendered_sql.sql.contains(" = 1) AS [value]"));
        }
        assert_eq!(rendered_sql.binds[0].name, "needle");
    }
    for (function, method, parameter) in [
        (SpatialFunction::PointN, "STPointN", "point_index"),
        (SpatialFunction::Buffer, "STBuffer", "distance"),
    ] {
        let mut query = simple_query();
        query.projection[0] = QueryProjection {
            expression: QueryExpression::Spatial {
                function,
                arguments: vec![
                    query_column("e", "shape"),
                    QueryExpression::Parameter {
                        name: parameter.to_owned(),
                    },
                ],
            },
            alias: Some("value".to_owned()),
        };
        let rendered_sql = renderer
            .render_query(&query)
            .expect("numeric spatial method");
        assert!(rendered_sql
            .sql
            .contains(&format!("([e].[shape].{method}(@p1)).AsBinaryZM()")));
        assert_eq!(rendered_sql.binds[0].name, parameter);
    }

    // `Reverse` è assente su entrambe le semantiche SQL Server e accetta
    // un solo argomento: il rifiuto misura quindi il catalogo, non un piano
    // malformato o una divergenza geometry/geography.
    let mut unsupported = simple_query();
    unsupported.projection[0] = QueryProjection {
        expression: QueryExpression::Spatial {
            function: SpatialFunction::Reverse,
            arguments: vec![query_column("e", "shape")],
        },
        alias: Some("value".to_owned()),
    };
    assert_eq!(
        renderer
            .render_query(&unsupported)
            .expect_err("unverified spatial method")
            .category,
        plenora_database_core::ErrorCategory::Unsupported
    );
}

#[test]
fn sqlserver_renders_cross_apply_without_an_on_clause() {
    let lateral = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("details", "d")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("d", "shape"),
            alias: Some("shape".to_owned()),
        }],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(1),
        row_offset: None,
        locking: None,
    };
    let mut query = simple_query();
    query.joins.push(QueryJoin {
        kind: JoinKind::Cross,
        source: None,
        derived_source: Some(QueryDerivedSource {
            query: Box::new(lateral),
            alias: "latest".to_owned(),
        }),
        lateral: true,
        on: None,
    });
    let sql = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("CROSS APPLY")
    .sql;
    assert!(sql.contains(" CROSS APPLY (SELECT TOP (1)"));
    assert!(sql.contains(") AS [latest]"));
    assert!(!sql.contains("LATERAL"));
}

#[test]
fn sqlserver_locking_is_explicitly_mapped_to_safe_table_hints() {
    let renderer = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    );
    let mut query = simple_query();
    query.locking = Some(QueryLock {
        strength: QueryLockStrength::Update,
        relations: vec!["e".to_owned()],
        wait: QueryLockWait::NoWait,
    });
    let sql = renderer.render_query(&query).expect("UPDLOCK NOWAIT").sql;
    assert!(sql.contains("[events] AS [e] WITH (UPDLOCK, NOWAIT)"));
    assert!(!sql.contains("FOR UPDATE"));

    query.locking = Some(QueryLock {
        strength: QueryLockStrength::Share,
        relations: Vec::new(),
        wait: QueryLockWait::Wait,
    });
    assert!(renderer
        .render_query(&query)
        .expect("shared lock")
        .sql
        .contains(" WITH (HOLDLOCK)"));

    query.locking = Some(QueryLock {
        strength: QueryLockStrength::NoKeyUpdate,
        relations: vec!["e".to_owned()],
        wait: QueryLockWait::Wait,
    });
    assert_eq!(
        renderer
            .render_query(&query)
            .expect_err("NO KEY UPDATE has no exact T-SQL equivalent")
            .category,
        plenora_database_core::ErrorCategory::Unsupported
    );

    query.locking = Some(QueryLock {
        strength: QueryLockStrength::Update,
        relations: vec!["e".to_owned()],
        wait: QueryLockWait::SkipLocked,
    });
    assert_eq!(
        renderer
            .render_query(&query)
            .expect_err("READPAST is not portable SKIP LOCKED")
            .category,
        plenora_database_core::ErrorCategory::Unsupported
    );

    query.locking = Some(QueryLock {
        strength: QueryLockStrength::Update,
        relations: vec!["missing".to_owned()],
        wait: QueryLockWait::Wait,
    });
    assert_eq!(
        renderer
            .render_query(&query)
            .expect_err("unknown lock target")
            .category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
}

#[test]
fn spatial_is_capability_gated() {
    let select = Select {
        source: source(),
        projection: vec![identifier("geom")],
        filter: Some(Expression::SpatialIntersects {
            field: identifier("geom"),
            wkb_parameter: "area".to_owned(),
        }),
        order_by: vec![],
        limit: None,
    };
    let error = Renderer::new(
        Dialect::Db2,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_select(&select)
    .expect_err("capability must fail");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Unsupported
    );
}

#[test]
fn postgres_renders_typed_d_within_with_ewkb_binds() {
    let rendered = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_filter(&Expression::SpatialPredicate {
        function: SpatialFunction::DWithin,
        field: identifier("geom"),
        geometry_parameter: Some("probe".to_owned()),
        distance_parameter: Some("radius".to_owned()),
    })
    .expect("spatial render");
    assert_eq!(
        rendered.sql,
        "ST_DWithin(\"geom\", ST_GeomFromEWKB($1), $2)"
    );
    assert_eq!(rendered.binds[0].name, "probe");
    assert_eq!(rendered.binds[1].name, "radius");
}

#[test]
fn postgres_query_ast_wraps_spatial_wkb_parameters() {
    let query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("e", "id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(QueryExpression::Spatial {
            function: SpatialFunction::DWithin,
            arguments: vec![
                query_column("e", "geom"),
                QueryExpression::Parameter {
                    name: "probe".to_owned(),
                },
                QueryExpression::Parameter {
                    name: "radius".to_owned(),
                },
            ],
        }),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(10),
        row_offset: None,
        locking: None,
    };
    let rendered = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("query spatial");
    assert!(rendered
        .sql
        .contains("ST_DWithin(\"e\".\"geom\", ST_GeomFromEWKB($1), $2)"));
    assert_eq!(rendered.binds[0].name, "probe");
    assert_eq!(rendered.binds[1].name, "radius");
}

#[test]
fn query_ast_limit_uses_each_dialect_syntax() {
    let query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("e", "id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: true,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(7),
        row_offset: None,
        locking: None,
    };
    for (dialect, expected) in [
        (Dialect::Postgres, " LIMIT 7"),
        (Dialect::Mysql, " LIMIT 7"),
        (Dialect::Sqlite, " LIMIT 7"),
        (Dialect::Duckdb, " LIMIT 7"),
        (Dialect::Oracle, " FETCH FIRST 7 ROWS ONLY"),
        (Dialect::Db2, " FETCH FIRST 7 ROWS ONLY"),
    ] {
        let sql = Renderer::new(
            dialect,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("dialect query")
        .sql;
        assert!(sql.ends_with(expected), "{dialect:?}: {sql}");
    }
    let sql = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("SQL Server query")
    .sql;
    assert!(sql.starts_with("SELECT DISTINCT TOP (7) "), "{sql}");
    assert!(!sql.contains(" LIMIT "));
}

#[test]
fn oracle_converts_wkb_and_db2_refuses_to_invent_an_srid() {
    let select = Select {
        source: source(),
        projection: vec![identifier("id")],
        filter: Some(Expression::SpatialIntersects {
            field: identifier("geom"),
            wkb_parameter: "probe".to_owned(),
        }),
        order_by: Vec::new(),
        limit: None,
    };
    let oracle = Renderer::new(
        Dialect::Oracle,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_select(&select)
    .expect("Oracle spatial")
    .sql;
    assert!(oracle.contains("SDO_UTIL.FROM_WKBGEOMETRY(:1)"));
    let error = Renderer::new(
        Dialect::Db2,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_select(&select)
    .expect_err("Db2 spatial senza SRID");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Unsupported
    );
}

#[test]
fn db2_uses_the_measured_coordinate_dimension_name() {
    assert_eq!(
        spatial_function_name(Dialect::Db2, SpatialFunction::Dimensions),
        "ST_COORDDIM"
    );
    assert_eq!(
        spatial_function_name(Dialect::Db2, SpatialFunction::Srid),
        "ST_SRID"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn query_ast_renders_cte_join_group_having_and_stable_binds() {
    let count_id = QueryExpression::Scalar {
        function: ScalarFunction::Count,
        arguments: vec![query_column("f", "id")],
    };
    let cte = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: query_column("e", "id"),
                alias: None,
            },
            QueryProjection {
                expression: query_column("e", "owner_id"),
                alias: None,
            },
        ],
        joins: Vec::new(),
        filter: Some(QueryExpression::Compare {
            left: Box::new(query_column("e", "id")),
            operator: ComparisonOperator::Gt,
            right: Box::new(QueryExpression::Parameter {
                name: "minimum_id".to_owned(),
            }),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    };
    let query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: vec![CommonTableExpression {
            name: "filtered".to_owned(),
            recursive: false,
            query: Box::new(cte),
        }],
        source: Some(query_source("filtered", "f")),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: query_column("o", "name"),
                alias: Some("owner".to_owned()),
            },
            QueryProjection {
                expression: count_id.clone(),
                alias: Some("events".to_owned()),
            },
        ],
        joins: vec![QueryJoin {
            kind: JoinKind::Inner,
            source: Some(query_source("owners", "o")),
            derived_source: None,
            lateral: false,
            on: Some(QueryExpression::Compare {
                left: Box::new(query_column("f", "owner_id")),
                operator: ComparisonOperator::Eq,
                right: Box::new(query_column("o", "id")),
            }),
        }],
        filter: None,
        group_by: vec![query_column("o", "name")],
        having: Some(QueryExpression::Compare {
            left: Box::new(count_id.clone()),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "minimum_count".to_owned(),
            }),
        }),
        order_by: vec![QueryOrdering {
            expression: count_id,
            direction: SortDirection::Desc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(25),
        row_offset: None,
        locking: None,
    };
    let rendered = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("query render");
    assert_eq!(
            rendered.sql,
            "WITH \"filtered\" AS (SELECT \"e\".\"id\", \"e\".\"owner_id\" FROM \"events\" AS \"e\" WHERE \"e\".\"id\" > $1) SELECT \"o\".\"name\" AS \"owner\", COUNT(\"f\".\"id\") AS \"events\" FROM \"filtered\" AS \"f\" INNER JOIN \"owners\" AS \"o\" ON \"f\".\"owner_id\" = \"o\".\"id\" GROUP BY \"o\".\"name\" HAVING COUNT(\"f\".\"id\") >= $2 ORDER BY COUNT(\"f\".\"id\") DESC LIMIT 25"
        );
    assert_eq!(
        rendered
            .binds
            .iter()
            .map(|bind| bind.name.as_str())
            .collect::<Vec<_>>(),
        ["minimum_id", "minimum_count"]
    );
}

#[test]
fn postgres_renders_index_aware_spatial_query() {
    let query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("e", "id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(QueryExpression::SpatialOperator {
            operator: SpatialOperator::BoundingBoxIntersects,
            left: Box::new(query_column("e", "geom")),
            right: Box::new(QueryExpression::Parameter {
                name: "probe".to_owned(),
            }),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: QueryExpression::SpatialOperator {
                operator: SpatialOperator::KnnDistance,
                left: Box::new(query_column("e", "geom")),
                right: Box::new(QueryExpression::Parameter {
                    name: "probe".to_owned(),
                }),
            },
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(5),
        row_offset: None,
        locking: None,
    };
    let rendered = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("index-aware spatial query");
    assert!(rendered
        .sql
        .contains("\"e\".\"geom\" && ST_GeomFromEWKB($1)"));
    assert!(rendered
        .sql
        .contains("\"e\".\"geom\" <-> ST_GeomFromEWKB($2) ASC"));
    assert_eq!(
        rendered
            .binds
            .iter()
            .map(|bind| bind.name.as_str())
            .collect::<Vec<_>>(),
        ["probe", "probe"]
    );
}

#[test]
fn postgres_renders_spatial_clustering_as_a_window() {
    let query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: QueryExpression::SpatialWindow {
                function: SpatialFunction::ClusterDbscan,
                arguments: vec![
                    query_column("e", "geom"),
                    QueryExpression::Parameter {
                        name: "epsilon".to_owned(),
                    },
                    QueryExpression::Parameter {
                        name: "minimum_points".to_owned(),
                    },
                ],
                partition_by: vec![query_column("e", "region_id")],
                order_by: Vec::new(),
                frame: None,
            },
            alias: Some("cluster_id".to_owned()),
        }],
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
    };
    let sql = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("spatial clustering")
    .sql;
    assert!(sql.contains(
        "ST_ClusterDBSCAN(\"e\".\"geom\", $1, $2) OVER \
             (PARTITION BY \"e\".\"region_id\")"
    ));
}

#[test]
#[allow(clippy::too_many_lines)]
fn postgres_renders_derived_window_lateral_pagination_and_locking() {
    let inner = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("e", "id"),
            alias: Some("id".to_owned()),
        }],
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
    };
    let lateral = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("details", "x")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("x", "event_id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(QueryExpression::Compare {
            left: Box::new(query_column("x", "event_id")),
            operator: ComparisonOperator::Eq,
            right: Box::new(query_column("d", "id")),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(1),
        row_offset: None,
        locking: None,
    };
    let query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: None,
        derived_source: Some(QueryDerivedSource {
            query: Box::new(inner),
            alias: "d".to_owned(),
        }),
        projection: vec![
            QueryProjection {
                expression: query_column("d", "id"),
                alias: None,
            },
            QueryProjection {
                expression: QueryExpression::Window {
                    function: ScalarFunction::RowNumber,
                    arguments: Vec::new(),
                    partition_by: Vec::new(),
                    order_by: vec![QueryOrdering {
                        expression: query_column("d", "id"),
                        direction: SortDirection::Asc,
                    }],
                    frame: None,
                },
                alias: Some("ordinal".to_owned()),
            },
        ],
        joins: vec![QueryJoin {
            kind: JoinKind::Cross,
            source: None,
            derived_source: Some(QueryDerivedSource {
                query: Box::new(lateral),
                alias: "latest".to_owned(),
            }),
            lateral: true,
            on: None,
        }],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: query_column("d", "id"),
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: vec![query_column("d", "id")],
        set_operations: Vec::new(),
        row_limit: Some(10),
        row_offset: Some(5),
        locking: None,
    };
    let sql = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("advanced PostgreSQL query")
    .sql;
    assert!(sql.contains("DISTINCT ON (\"d\".\"id\")"));
    assert!(sql.contains("ROW_NUMBER() OVER (ORDER BY \"d\".\"id\" ASC)"));
    assert!(sql.contains("CROSS JOIN LATERAL (SELECT"));
    assert!(sql.ends_with("LIMIT 10 OFFSET 5"));

    let mut locking_query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("e", "id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(1),
        row_offset: None,
        locking: None,
    };
    locking_query.locking = Some(QueryLock {
        strength: QueryLockStrength::Share,
        relations: vec!["e".to_owned()],
        wait: QueryLockWait::SkipLocked,
    });
    let locking_sql = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&locking_query)
    .expect("locking PostgreSQL query")
    .sql;
    assert!(locking_sql.ends_with("LIMIT 1 FOR SHARE OF \"e\" SKIP LOCKED"));
}

#[test]
fn postgres_renders_set_operations_and_recursive_cte() {
    let leaf = |table: &str, alias: &str| QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(query_source(table, alias)),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column(alias, "id"),
            alias: None,
        }],
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
    };
    let mut cte_body = leaf("roots", "r");
    cte_body.set_operations.push(QuerySetOperation {
        operator: QuerySetOperator::Union,
        all: true,
        query: Box::new(leaf("tree", "t")),
    });
    let query = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: vec![CommonTableExpression {
            name: "tree".to_owned(),
            recursive: true,
            query: Box::new(cte_body),
        }],
        source: Some(query_source("tree", "result")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: query_column("result", "id"),
            alias: None,
        }],
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
    };
    let sql = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .render_query(&query)
    .expect("recursive CTE")
    .sql;
    assert!(sql.starts_with("WITH RECURSIVE \"tree\" AS ("));
    assert!(sql.contains(" UNION ALL (SELECT "));
}

#[test]
fn lowered_statement_fingerprint_is_stable_and_bind_sensitive() {
    let rendered = RenderedSql {
        sql: "SELECT $1".to_owned(),
        binds: vec![BindParameter {
            ordinal: 1,
            name: "tenant".to_owned(),
        }],
    };
    assert_eq!(
        rendered.fingerprint(),
        [
            193, 200, 110, 50, 129, 226, 80, 227, 57, 129, 116, 96, 134, 96, 188, 191, 121, 121,
            159, 150, 137, 49, 1, 5, 148, 250, 122, 108, 160, 43, 227, 32,
        ]
    );
    assert_eq!(rendered.fingerprint().len(), 32);

    let mut different_layout = rendered.clone();
    different_layout.binds[0].name = "account".to_owned();
    assert_ne!(rendered.fingerprint(), different_layout.fingerprint());

    let mut different_sql = rendered.clone();
    different_sql.sql.push_str(" WHERE active");
    assert_ne!(rendered.fingerprint(), different_sql.fingerprint());
}
