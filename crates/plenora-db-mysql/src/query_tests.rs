use super::*;
use crate::MysqlColumnKind;
use mysql_async::consts::{ColumnFlags, ColumnType};
use plenora_database_core::arrow::schema::DataType;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::plan::{ComparisonOperator, ObjectRef, SortDirection};
use plenora_database_core::protocol;
use plenora_database_core::query::{
    ColumnRef, CommonTableExpression, JoinKind, QueryDerivedSource, QueryJoin, QueryLock,
    QueryLockStrength, QueryLockWait, QueryProjection, QuerySetOperation, QuerySetOperator,
    SpatialFunction,
};

fn source(object: &str) -> QuerySource {
    QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("warehouse".to_owned()),
            object: object.to_owned(),
        },
        alias: None,
    }
}

fn column(field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    }
}

fn base_query() -> QueryOperation {
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(source("events")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("event_id"),
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

/// Una query che proietta una geometria calcolata sulla colonna `geom`.
///
/// Non passa da `ensure_qualified_shape` in queste prove, ed e voluto: le
/// funzioni che rendono geometria non sono ancora qualificate su nessuno
/// dei due prodotti, e non lo saranno finche una misura live non le avra
/// attraversate. Cio che qui si verifica e il **meccanismo** che quella
/// misura userà, non il permesso di usarlo.
fn geometry_query(function: SpatialFunction, arguments: Vec<QueryExpression>) -> QueryOperation {
    let mut query = base_query();
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Spatial {
            function,
            arguments,
        },
        alias: Some("shape".to_owned()),
    }];
    query.declared_crs = vec![plenora_database_core::plan::DeclaredCrs {
        column: "geom".to_owned(),
        srid: 4_326,
    }];
    query
}

fn resolve(query: &QueryOperation) -> Result<Vec<ResolvedGeometry>> {
    let declared = declared_query_crs(query, &crate::profile::MYSQL_PROFILE)?;
    resolve_query_geometries(query, &declared, &crate::profile::MYSQL_PROFILE)
}

#[test]
fn a_preserving_function_inherits_the_declared_crs_of_its_column() {
    let query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    let resolved = resolve(&query).expect("risolta");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].result_index, 0);
    assert_eq!(resolved[0].source_column, "geom");
    assert_eq!(resolved[0].srid, 4_326);
}

#[test]
fn a_scalar_argument_is_not_a_place_a_frame_could_come_from() {
    // `ST_Buffer` prende una geometria e una distanza: soltanto l'argomento
    // geometrico partecipa alla verifica del sistema di riferimento.
    let query = geometry_query(
        SpatialFunction::Buffer,
        vec![
            column("geom"),
            QueryExpression::Parameter {
                name: "distanza".to_owned(),
            },
        ],
    );
    let resolved = resolve(&query).expect("il buffer eredita il frame della sua geometria");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].source_column, "geom");
    assert_eq!(resolved[0].srid, 4_326);
}

#[test]
fn without_a_declaration_there_is_nothing_to_inherit() {
    let mut query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    query.declared_crs.clear();
    let error = resolve(&query).expect_err("nessun CRS da ereditare");
    assert_eq!(error.category, ErrorCategory::Crs);
}

#[test]
fn a_declaration_on_another_column_does_not_travel() {
    // Il nome sbagliato e il caso pericoloso: se la ricerca fosse per
    // posizione invece che per nome, questa dichiarazione verrebbe
    // attribuita a `geom` e pubblicherebbe un CRS che nessuno ha detto.
    let mut query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    query.declared_crs[0].column = "footprint".to_owned();
    let error = resolve(&query).expect_err("dichiarazione altrove");
    assert_eq!(error.category, ErrorCategory::Crs);
}

#[test]
fn a_rule_that_is_not_preserves_stays_closed() {
    for function in [
        // Due geometrie: il frame del risultato non e derivabile.
        SpatialFunction::Intersection,
        // L'SRID e in un argomento, e nessuna misura l'ha attraversato.
        SpatialFunction::Transform,
    ] {
        let query = geometry_query(function, vec![column("geom"), column("geom")]);
        let error = resolve(&query).expect_err("{function:?} non e preserves");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }
}

#[test]
fn a_computed_argument_moves_the_question_instead_of_answering_it() {
    let query = geometry_query(
        SpatialFunction::Envelope,
        vec![QueryExpression::Spatial {
            function: SpatialFunction::Centroid,
            arguments: vec![column("geom")],
        }],
    );
    let error = resolve(&query).expect_err("argomento non colonna");
    assert_eq!(error.category, ErrorCategory::Crs);
}

#[test]
fn a_grouped_query_cannot_carry_the_confirmation() {
    let mut query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    query.group_by = vec![column("event_id")];
    let error = resolve(&query).expect_err("gruppo");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    let mut query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    query.distinct = true;
    let error = resolve(&query).expect_err("distinct");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn zero_is_the_ogc_undefined_and_is_not_a_declaration() {
    let mut query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    query.declared_crs[0].srid = 0;
    let error = resolve(&query).expect_err("zero");
    assert_eq!(error.category, ErrorCategory::Crs);
}

#[test]
fn one_column_gets_one_confirmation_however_many_times_it_is_projected() {
    let mut query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    query.projection.push(QueryProjection {
        expression: QueryExpression::Spatial {
            function: SpatialFunction::Centroid,
            arguments: vec![column("geom")],
        },
        alias: Some("middle".to_owned()),
    });
    let resolved = resolve(&query).expect("risolte");
    assert_eq!(resolved.len(), 2);
    let (checked, checks) =
        append_crs_checks(&query, &resolved, &crate::profile::MYSQL_PROFILE).expect("accodate");
    assert_eq!(checks.len(), 1, "una colonna, una conferma");
    assert_eq!(checks[0].result_index, 2);
    assert_eq!(checks[0].column, "geom");
    assert_eq!(checks[0].expected, 4_326);
    // La conferma sta **in coda**: le posizioni delle colonne del chiamante
    // non si spostano, ed e cio che la rende invisibile.
    assert_eq!(checked.projection.len(), 3);
    assert_eq!(checked.projection[0], query.projection[0]);
    assert_eq!(checked.projection[1], query.projection[1]);
    assert_eq!(checked.projection[2].alias, None);
}

#[test]
fn the_confirmation_column_renders_as_srid_of_the_source() {
    let query = geometry_query(SpatialFunction::Envelope, vec![column("geom")]);
    let resolved = resolve(&query).expect("risolta");
    let (checked, _) =
        append_crs_checks(&query, &resolved, &crate::profile::MYSQL_PROFILE).expect("accodate");
    let sql = mysql_renderer().render_query(&checked).expect("render").sql;
    assert!(
        sql.contains("ST_AsBinary(ST_Envelope(`geom`)) AS `shape`"),
        "{sql}"
    );
    assert!(sql.contains("ST_SRID(`geom`)"), "{sql}");
    // Senza alias: nessun nome che qualcuno possa scambiare per una colonna
    // del risultato.
    assert!(!sql.contains("ST_SRID(`geom`) AS"), "{sql}");
}

#[test]
fn scalar_single_source_renders_with_backticks_and_positional_binds() {
    let mut query = base_query();
    query.projection.push(QueryProjection {
        expression: QueryExpression::Scalar {
            function: ScalarFunction::Lower,
            arguments: vec![column("label")],
        },
        alias: Some("lowered".to_owned()),
    });
    query.filter = Some(QueryExpression::Compare {
        left: Box::new(column("event_id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    query.order_by = vec![QueryOrdering {
        expression: column("event_id"),
        direction: SortDirection::Asc,
    }];
    query.row_limit = Some(10);
    let rendered = render_query(&query, "warehouse").expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT `event_id`, LOWER(`label`) AS `lowered` FROM `warehouse`.`events` \
         WHERE `event_id` >= ? ORDER BY `event_id` ASC LIMIT 10"
    );
    assert_eq!(rendered.binds.len(), 1);
    assert_eq!(rendered.binds[0].name, "floor");
    assert_eq!(rendered.binds[0].ordinal, 1);

    let mut unordered_limit = base_query();
    unordered_limit.row_limit = Some(1);
    let error = render_query(&unordered_limit, "warehouse")
        .expect_err("LIMIT senza ORDER BY deve restare fail-closed");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert!(error.message.contains("LIMIT"), "{}", error.message);
    assert!(error.message.contains("ORDER BY"), "{}", error.message);
}

#[test]
fn unqualified_ast_subsets_stay_unsupported() {
    let mut cases: Vec<(&str, QueryOperation)> = Vec::new();

    let mut cte = base_query();
    cte.common_table_expressions = vec![CommonTableExpression {
        name: "walk".to_owned(),
        recursive: false,
        query: Box::new(base_query()),
    }];
    cases.push(("cte", cte));

    let mut union = base_query();
    union.set_operations = vec![QuerySetOperation {
        operator: QuerySetOperator::Union,
        all: true,
        query: Box::new(base_query()),
    }];
    cases.push(("set operation", union));

    // Anche la sorgente di base derivata resta fuori: il provider qualifica
    // solo relazioni fisiche, in FROM come in ogni join.
    let mut derived_base = base_query();
    derived_base.source = None;
    derived_base.derived_source = Some(QueryDerivedSource {
        query: Box::new(base_query()),
        alias: "recent".to_owned(),
    });
    cases.push(("derived source di base", derived_base));

    // La window scalare e qualificata; la window
    // spatial resta fuori. Non insieme al resto dell'AST spatial — le
    // funzioni di `VERIFIED_SPATIAL_FUNCTIONS` sono accettate, e poche
    // righe piu in basso questo stesso test lo mostra — ma per la sola
    // forma window, che nessuna prova attraversa.
    let mut spatial_window = base_query();
    spatial_window.projection = vec![QueryProjection {
        expression: QueryExpression::SpatialWindow {
            function: SpatialFunction::ClusterDbscan,
            arguments: vec![
                column("geom"),
                QueryExpression::Parameter {
                    name: "eps".to_owned(),
                },
                QueryExpression::Parameter {
                    name: "minimum".to_owned(),
                },
            ],
            partition_by: vec![column("actor_id")],
            order_by: Vec::new(),
            frame: None,
        },
        alias: Some("cluster".to_owned()),
    }];
    cases.push(("spatial window", spatial_window));

    // Le funzioni in VERIFIED_SPATIAL_FUNCTIONS sono accettate. Il test
    // fail-closed usa una funzione non verificata (AsGeoJson) perche il subset
    // resti conservativo.
    let mut spatial = base_query();
    spatial.projection = vec![QueryProjection {
        expression: QueryExpression::Spatial {
            function: SpatialFunction::NRings,
            arguments: vec![column("geom")],
        },
        alias: None,
    }];
    cases.push(("spatial non verified", spatial));

    let mut subquery = base_query();
    subquery.filter = Some(QueryExpression::Exists {
        query: Box::new(base_query()),
        negated: false,
    });
    cases.push(("subquery", subquery));

    let mut locking = base_query();
    locking.locking = Some(QueryLock {
        strength: QueryLockStrength::Update,
        relations: Vec::new(),
        wait: QueryLockWait::NoWait,
    });
    cases.push(("locking", locking));

    for (label, query) in cases {
        let error = match render_query(&query, "warehouse") {
            Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
            Err(error) => error,
        };
        assert_eq!(error.category, ErrorCategory::Unsupported, "{label}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
        // «non ancora qualificat…» oppure «non qualificata su <prodotto>»:
        // il cancello spatial legge il profilo e nomina il prodotto.
        assert!(
            error.message.contains("non ancora qualificat")
                || error.message.contains("non qualificata su"),
            "{label}: {}",
            error.message
        );
    }
}

fn aliased_source(object: &str, alias: &str) -> QuerySource {
    QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("warehouse".to_owned()),
            object: object.to_owned(),
        },
        alias: Some(alias.to_owned()),
    }
}

fn qualified(relation: &str, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    }
}

fn equality(left: QueryExpression, right: QueryExpression) -> QueryExpression {
    QueryExpression::Compare {
        left: Box::new(left),
        operator: ComparisonOperator::Eq,
        right: Box::new(right),
    }
}

fn physical_join(kind: JoinKind, source: QuerySource, on: Option<QueryExpression>) -> QueryJoin {
    QueryJoin {
        kind,
        source: Some(source),
        derived_source: None,
        lateral: false,
        on,
    }
}

/// Base `events` AS `e` con un solo INNER JOIN su `actors` AS `a`.
fn joined_query() -> QueryOperation {
    let mut query = base_query();
    query.source = Some(aliased_source("events", "e"));
    query.projection = vec![QueryProjection {
        expression: qualified("e", "event_id"),
        alias: None,
    }];
    query.joins = vec![physical_join(
        JoinKind::Inner,
        aliased_source("actors", "a"),
        Some(equality(
            qualified("e", "actor_id"),
            qualified("a", "actor_id"),
        )),
    )];
    query
}

#[test]
fn physical_joins_render_with_relation_qualified_columns_and_ordered_binds() {
    let mut query = base_query();
    query.source = Some(aliased_source("events", "e"));
    query.projection = vec![
        QueryProjection {
            expression: qualified("e", "event_id"),
            alias: Some("event_id".to_owned()),
        },
        QueryProjection {
            expression: qualified("a", "name"),
            alias: Some("actor".to_owned()),
        },
        QueryProjection {
            expression: qualified("r", "name"),
            alias: Some("region".to_owned()),
        },
    ];
    query.joins = vec![
        physical_join(
            JoinKind::Inner,
            aliased_source("actors", "a"),
            Some(QueryExpression::And {
                arguments: vec![
                    equality(qualified("e", "actor_id"), qualified("a", "actor_id")),
                    QueryExpression::Compare {
                        left: Box::new(qualified("a", "tier")),
                        operator: ComparisonOperator::Gte,
                        right: Box::new(QueryExpression::Parameter {
                            name: "tier".to_owned(),
                        }),
                    },
                ],
            }),
        ),
        physical_join(
            JoinKind::Left,
            aliased_source("regions", "r"),
            Some(equality(
                qualified("a", "region_id"),
                qualified("r", "region_id"),
            )),
        ),
        physical_join(JoinKind::Cross, aliased_source("calendar", "c"), None),
    ];
    query.filter = Some(QueryExpression::Compare {
        left: Box::new(qualified("e", "event_id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    query.order_by = vec![QueryOrdering {
        expression: qualified("e", "event_id"),
        direction: SortDirection::Asc,
    }];
    query.row_limit = Some(10);
    let rendered = render_query(&query, "warehouse").expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT `e`.`event_id` AS `event_id`, `a`.`name` AS `actor`, \
         `r`.`name` AS `region` FROM `warehouse`.`events` AS `e` \
         INNER JOIN `warehouse`.`actors` AS `a` \
         ON (`e`.`actor_id` = `a`.`actor_id` AND `a`.`tier` >= ?) \
         LEFT JOIN `warehouse`.`regions` AS `r` \
         ON `a`.`region_id` = `r`.`region_id` \
         CROSS JOIN `warehouse`.`calendar` AS `c` \
         WHERE `e`.`event_id` >= ? ORDER BY `e`.`event_id` ASC LIMIT 10"
    );
    // Il bind di ON precede quello di WHERE: l'ordine posizionale segue
    // la posizione sintattica, non l'ordine di dichiarazione.
    assert_eq!(
        rendered
            .binds
            .iter()
            .map(|bind| (bind.name.as_str(), bind.ordinal))
            .collect::<Vec<_>>(),
        vec![("tier", 1), ("floor", 2)]
    );
}

#[test]
fn join_on_cannot_reference_a_relation_introduced_later() {
    let mut query = base_query();
    query.source = Some(aliased_source("events", "e"));
    query.projection = vec![QueryProjection {
        expression: qualified("e", "event_id"),
        alias: Some("event_id".to_owned()),
    }];
    query.joins = vec![
        physical_join(
            JoinKind::Inner,
            aliased_source("actors", "a"),
            Some(equality(
                qualified("a", "region_id"),
                qualified("r", "region_id"),
            )),
        ),
        physical_join(
            JoinKind::Left,
            aliased_source("regions", "r"),
            Some(equality(
                qualified("a", "region_id"),
                qualified("r", "region_id"),
            )),
        ),
    ];

    let error = render_query(&query, "warehouse")
        .expect_err("ON non puo vedere una relazione introdotta da un join successivo");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert!(error.message.contains("assente da FROM e dai join"));
}

/// `MySQL` non ha FULL JOIN nativo: il rifiuto descrive un'assenza del
/// motore, non una qualificazione mancante.
#[test]
fn right_join_renders_while_full_join_is_reported_as_absent_from_mysql() {
    let mut right = joined_query();
    right.joins[0].kind = JoinKind::Right;
    right.projection = vec![QueryProjection {
        expression: qualified("a", "name"),
        alias: Some("actor".to_owned()),
    }];
    assert_eq!(
        render_query(&right, "warehouse").expect("render").sql,
        "SELECT `a`.`name` AS `actor` FROM `warehouse`.`events` AS `e` \
         RIGHT JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id`"
    );

    let mut full = joined_query();
    full.joins[0].kind = JoinKind::Full;
    let error = render_query(&full, "warehouse").expect_err("FULL JOIN");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert!(
        error.message.contains("non esiste in questo dialetto"),
        "{}",
        error.message
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn join_shapes_outside_the_qualified_subset_fail_closed() {
    let mut cases: Vec<(&str, QueryOperation, ErrorCategory)> = Vec::new();

    let mut derived = joined_query();
    derived.joins[0].source = None;
    derived.joins[0].derived_source = Some(QueryDerivedSource {
        query: Box::new(base_query()),
        alias: "recent".to_owned(),
    });
    derived.joins[0].on = Some(equality(
        qualified("e", "actor_id"),
        qualified("recent", "event_id"),
    ));
    cases.push(("join su subquery", derived, ErrorCategory::Unsupported));

    let mut cross_database = joined_query();
    cross_database.joins[0].source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("other".to_owned()),
            object: "actors".to_owned(),
        },
        alias: Some("a".to_owned()),
    });
    cases.push((
        "join fuori dal database configurato",
        cross_database,
        ErrorCategory::Unsupported,
    ));

    let mut three_part = joined_query();
    three_part.joins[0].source = Some(QuerySource {
        object: ObjectRef {
            catalog: Some("warehouse".to_owned()),
            schema: Some("warehouse".to_owned()),
            object: "actors".to_owned(),
        },
        alias: Some("a".to_owned()),
    });
    cases.push((
        "join a tre componenti",
        three_part,
        ErrorCategory::Unsupported,
    ));

    let mut duplicate_alias = joined_query();
    duplicate_alias.joins[0].source = Some(aliased_source("actors", "e"));
    duplicate_alias.joins[0].on = Some(equality(
        qualified("e", "actor_id"),
        qualified("e", "actor_id"),
    ));
    cases.push((
        "alias duplicato tra base e join",
        duplicate_alias,
        ErrorCategory::InvalidPlan,
    ));

    let mut duplicate_between_joins = joined_query();
    duplicate_between_joins.joins.push(physical_join(
        JoinKind::Left,
        aliased_source("regions", "a"),
        Some(equality(
            qualified("e", "region_id"),
            qualified("a", "region_id"),
        )),
    ));
    cases.push((
        "alias duplicato tra due join",
        duplicate_between_joins,
        ErrorCategory::InvalidPlan,
    ));

    // Senza alias la relazione e visibile con il nome della tabella: un
    // join sulla stessa tabella e ambiguo esattamente come un alias ripetuto.
    let mut duplicate_object = base_query();
    duplicate_object.joins = vec![physical_join(
        JoinKind::Inner,
        source("events"),
        Some(equality(
            QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("events".to_owned()),
                    field: "event_id".to_owned(),
                },
            },
            QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("events".to_owned()),
                    field: "event_id".to_owned(),
                },
            },
        )),
    )];
    cases.push((
        "self join senza alias",
        duplicate_object,
        ErrorCategory::InvalidPlan,
    ));

    let mut aggregate_on = joined_query();
    aggregate_on.joins[0].on = Some(QueryExpression::Compare {
        left: Box::new(aggregate(
            ScalarFunction::Count,
            vec![qualified("a", "actor_id")],
        )),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    cases.push(("aggregato in ON", aggregate_on, ErrorCategory::InvalidPlan));

    let mut window_on = joined_query();
    window_on.joins[0].on = Some(equality(
        QueryExpression::Window {
            function: ScalarFunction::Rank,
            arguments: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        },
        qualified("a", "actor_id"),
    ));
    cases.push(("window in ON", window_on, ErrorCategory::InvalidPlan));

    // Il test JOIN spatial usa una funzione non verificata per controllare
    // che JOIN-on-spatial resti
    // conservativo — ma qualsiasi spatial in ON è già rifiutato dalle
    // regole di join (spatial expression non ammessa come predicato di JOIN).
    let mut spatial_on = joined_query();
    spatial_on.joins[0].on = Some(equality(
        QueryExpression::Spatial {
            function: SpatialFunction::NRings,
            arguments: vec![qualified("a", "geom")],
        },
        qualified("e", "geom"),
    ));
    cases.push(("spatial in ON", spatial_on, ErrorCategory::Unsupported));

    let mut subquery_on = joined_query();
    subquery_on.joins[0].on = Some(QueryExpression::Exists {
        query: Box::new(base_query()),
        negated: false,
    });
    cases.push(("subquery in ON", subquery_on, ErrorCategory::Unsupported));

    let mut oversized_alias = joined_query();
    oversized_alias.joins[0].source = Some(aliased_source(
        "actors",
        &"a".repeat(MAX_IDENTIFIER_CHARACTERS + 1),
    ));
    cases.push((
        "alias join oltre 64 caratteri",
        oversized_alias,
        ErrorCategory::InvalidPlan,
    ));

    let mut unknown_relation = joined_query();
    unknown_relation.projection = vec![QueryProjection {
        expression: qualified("missing", "event_id"),
        alias: None,
    }];
    cases.push((
        "colonna su relazione inesistente",
        unknown_relation,
        ErrorCategory::InvalidPlan,
    ));

    let mut unknown_wildcard = joined_query();
    unknown_wildcard.projection = vec![QueryProjection {
        expression: QueryExpression::Wildcard {
            relation: Some("missing".to_owned()),
        },
        alias: None,
    }];
    cases.push((
        "wildcard su relazione inesistente",
        unknown_wildcard,
        ErrorCategory::InvalidPlan,
    ));

    // Alcuni casi non sono limiti di `MySQL`: sono SQL invalido per
    // qualunque motore e `validate_query_operation` li rifiuta prima del
    // renderer. Rispondono percio in fase `Validate` e senza provider.
    for (label, query, category) in cases {
        let error = match render_query(&query, "warehouse") {
            Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
            Err(error) => error,
        };
        assert_eq!(error.category, category, "{label}: {}", error.message);
        if PORTABLE_REJECTIONS.contains(&label) {
            assert_eq!(error.phase, ErrorPhase::Validate, "{label}");
            assert_eq!(error.provider, None, "{label}");
        } else {
            assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
            assert_eq!(error.provider, Some(ProviderKind::Mysql), "{label}");
        }
    }
}

/// La presenza della clausola ON e gia rifiutata dalla validazione
/// portabile, che risponde in fase `Validate`. Il provider ripete il
/// controllo per conto proprio: la copertura di quel confine appartiene
/// al path `MySQL` e non deve dipendere dall'ordine dei validatori.
#[test]
fn the_on_clause_boundary_is_covered_by_the_core_and_by_the_provider() {
    let mut cross_with_on = joined_query();
    cross_with_on.joins[0].kind = JoinKind::Cross;
    let mut inner_without_on = joined_query();
    inner_without_on.joins[0].on = None;

    for (label, query) in [
        ("CROSS JOIN con ON", cross_with_on),
        ("INNER JOIN senza ON", inner_without_on),
    ] {
        let core = render_query(&query, "warehouse").expect_err(label);
        assert_eq!(core.category, ErrorCategory::InvalidPlan, "{label}");
        assert_eq!(core.phase, ErrorPhase::Validate, "{label}");

        let provider = ensure_qualified_shape(&query, "warehouse", &crate::profile::MYSQL_PROFILE)
            .expect_err(label);
        assert_eq!(provider.category, ErrorCategory::InvalidPlan, "{label}");
        assert_eq!(provider.phase, ErrorPhase::Prepare, "{label}");
        assert_eq!(provider.provider, Some(ProviderKind::Mysql), "{label}");
        assert!(provider.message.contains("JOIN"), "{}", provider.message);
    }
}

#[test]
fn distinct_stays_deterministic_across_joins() {
    let mut distinct = joined_query();
    distinct.projection = vec![
        QueryProjection {
            expression: qualified("e", "event_id"),
            alias: Some("event".to_owned()),
        },
        QueryProjection {
            expression: QueryExpression::Scalar {
                function: ScalarFunction::Lower,
                arguments: vec![qualified("a", "name")],
            },
            alias: Some("actor".to_owned()),
        },
    ];
    distinct.distinct = true;
    distinct.order_by = vec![
        QueryOrdering {
            expression: qualified("e", "event_id"),
            direction: SortDirection::Asc,
        },
        QueryOrdering {
            expression: column("actor"),
            direction: SortDirection::Desc,
        },
    ];
    assert_eq!(
        render_query(&distinct, "warehouse").expect("render").sql,
        "SELECT DISTINCT `e`.`event_id` AS `event`, LOWER(`a`.`name`) AS `actor` \
         FROM `warehouse`.`events` AS `e` \
         INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
         ORDER BY `e`.`event_id` ASC, `actor` DESC"
    );

    // Una colonna qualificata su un'altra relazione non appartiene alla
    // proiezione DISTINCT: l'ordine non sarebbe riproducibile.
    distinct.order_by = vec![QueryOrdering {
        expression: qualified("a", "tier"),
        direction: SortDirection::Asc,
    }];
    assert_eq!(
        render_query(&distinct, "warehouse")
            .expect_err("ordine fuori dalla proiezione DISTINCT")
            .category,
        ErrorCategory::InvalidPlan
    );

    // Un wildcard qualificato copre solo la sua relazione: con DISTINCT
    // l'ordine su un'altra relazione resta non riproducibile.
    let mut qualified_wildcard = joined_query();
    qualified_wildcard.projection = vec![QueryProjection {
        expression: QueryExpression::Wildcard {
            relation: Some("e".to_owned()),
        },
        alias: None,
    }];
    qualified_wildcard.distinct = true;
    qualified_wildcard.order_by = vec![QueryOrdering {
        expression: qualified("a", "name"),
        direction: SortDirection::Asc,
    }];
    assert_eq!(
        render_query(&qualified_wildcard, "warehouse")
            .expect_err("wildcard qualificato non copre l'altra relazione")
            .category,
        ErrorCategory::InvalidPlan
    );
    qualified_wildcard.order_by = vec![QueryOrdering {
        expression: qualified("e", "event_id"),
        direction: SortDirection::Asc,
    }];
    assert_eq!(
        render_query(&qualified_wildcard, "warehouse")
            .expect("render")
            .sql,
        "SELECT DISTINCT `e`.* FROM `warehouse`.`events` AS `e` \
         INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
         ORDER BY `e`.`event_id` ASC"
    );
}

#[test]
fn group_determinism_reads_the_relation_qualifier_as_part_of_the_key() {
    let mut grouped = joined_query();
    grouped.projection = vec![
        QueryProjection {
            expression: qualified("a", "name"),
            alias: Some("actor".to_owned()),
        },
        QueryProjection {
            expression: count_qualified("e", "event_id"),
            alias: Some("events".to_owned()),
        },
    ];
    grouped.group_by = vec![qualified("a", "name")];
    grouped.having = Some(QueryExpression::Compare {
        left: Box::new(count_qualified("e", "event_id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    grouped.order_by = vec![QueryOrdering {
        expression: qualified("a", "name"),
        direction: SortDirection::Asc,
    }];
    let rendered = render_query(&grouped, "warehouse").expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT `a`.`name` AS `actor`, COUNT(`e`.`event_id`) AS `events` \
         FROM `warehouse`.`events` AS `e` \
         INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
         GROUP BY `a`.`name` HAVING COUNT(`e`.`event_id`) >= ? ORDER BY `a`.`name` ASC"
    );
    assert_eq!(rendered.binds.len(), 1);
    assert_eq!(rendered.binds[0].ordinal, 1);

    // Con GROUP BY su `a`.`name` la stessa colonna non qualificata non e
    // la stessa chiave: il gruppo resta non dimostrato.
    grouped.group_by = vec![column("name")];
    assert_eq!(
        render_query(&grouped, "warehouse")
            .expect_err("chiave di gruppo non qualificata")
            .category,
        ErrorCategory::InvalidPlan
    );
}

fn aggregate(function: ScalarFunction, arguments: Vec<QueryExpression>) -> QueryExpression {
    QueryExpression::Scalar {
        function,
        arguments,
    }
}

fn count_qualified(relation: &str, field: &str) -> QueryExpression {
    aggregate(ScalarFunction::Count, vec![qualified(relation, field)])
}

fn count(field: &str) -> QueryExpression {
    aggregate(ScalarFunction::Count, vec![column(field)])
}

#[test]
fn distinct_projection_renders_and_orders_inside_the_projection() {
    let mut query = base_query();
    query.projection = vec![
        QueryProjection {
            expression: column("actor_id"),
            alias: None,
        },
        QueryProjection {
            expression: QueryExpression::Scalar {
                function: ScalarFunction::Lower,
                arguments: vec![column("label")],
            },
            alias: Some("lowered".to_owned()),
        },
    ];
    query.distinct = true;
    query.order_by = vec![
        QueryOrdering {
            expression: column("actor_id"),
            direction: SortDirection::Asc,
        },
        QueryOrdering {
            expression: column("lowered"),
            direction: SortDirection::Desc,
        },
    ];
    let rendered = render_query(&query, "warehouse").expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT DISTINCT `actor_id`, LOWER(`label`) AS `lowered` \
         FROM `warehouse`.`events` ORDER BY `actor_id` ASC, `lowered` DESC"
    );
    assert!(rendered.binds.is_empty());
}

#[test]
fn grouped_aggregates_render_with_binds_ordered_by_clause() {
    let mut query = base_query();
    query.projection = vec![
        QueryProjection {
            expression: column("actor_id"),
            alias: None,
        },
        QueryProjection {
            expression: count("event_id"),
            alias: Some("events".to_owned()),
        },
        QueryProjection {
            expression: aggregate(ScalarFunction::Sum, vec![column("amount")]),
            alias: Some("total".to_owned()),
        },
        QueryProjection {
            expression: aggregate(ScalarFunction::Average, vec![column("amount")]),
            alias: Some("mean".to_owned()),
        },
        QueryProjection {
            expression: aggregate(ScalarFunction::Minimum, vec![column("event_id")]),
            alias: Some("first_event".to_owned()),
        },
        QueryProjection {
            expression: aggregate(ScalarFunction::Maximum, vec![column("event_id")]),
            alias: Some("last_event".to_owned()),
        },
    ];
    query.filter = Some(QueryExpression::Compare {
        left: Box::new(column("event_id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "since".to_owned(),
        }),
    });
    query.group_by = vec![column("actor_id")];
    query.having = Some(QueryExpression::Compare {
        left: Box::new(count("event_id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    query.order_by = vec![QueryOrdering {
        expression: column("actor_id"),
        direction: SortDirection::Asc,
    }];
    query.row_limit = Some(25);
    let rendered = render_query(&query, "warehouse").expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT `actor_id`, COUNT(`event_id`) AS `events`, SUM(`amount`) AS `total`, \
         AVG(`amount`) AS `mean`, MIN(`event_id`) AS `first_event`, \
         MAX(`event_id`) AS `last_event` FROM `warehouse`.`events` \
         WHERE `event_id` >= ? GROUP BY `actor_id` HAVING COUNT(`event_id`) >= ? \
         ORDER BY `actor_id` ASC LIMIT 25"
    );
    assert_eq!(rendered.binds.len(), 2);
    assert_eq!(rendered.binds[0].name, "since");
    assert_eq!(rendered.binds[0].ordinal, 1);
    assert_eq!(rendered.binds[1].name, "floor");
    assert_eq!(rendered.binds[1].ordinal, 2);
}

#[test]
fn count_star_is_the_only_wildcard_accepted_inside_an_aggregate() {
    let mut query = base_query();
    query.projection = vec![QueryProjection {
        expression: aggregate(
            ScalarFunction::Count,
            vec![QueryExpression::Wildcard { relation: None }],
        ),
        alias: Some("rows".to_owned()),
    }];
    let rendered = render_query(&query, "warehouse").expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT COUNT(*) AS `rows` FROM `warehouse`.`events`"
    );

    let mut aliased_star = base_query();
    aliased_star.projection = vec![QueryProjection {
        expression: aggregate(
            ScalarFunction::Count,
            vec![QueryExpression::Wildcard {
                relation: Some("events".to_owned()),
            }],
        ),
        alias: None,
    }];
    assert_eq!(
        render_query(&aliased_star, "warehouse")
            .expect_err("COUNT(t.*) non e valido in MySQL")
            .category,
        ErrorCategory::InvalidPlan
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn grouped_shapes_without_a_deterministic_group_fail_closed() {
    let mut cases: Vec<(&str, QueryOperation)> = Vec::new();

    let mut ungrouped_column = base_query();
    ungrouped_column.projection = vec![
        QueryProjection {
            expression: column("label"),
            alias: None,
        },
        QueryProjection {
            expression: count("event_id"),
            alias: None,
        },
    ];
    ungrouped_column.group_by = vec![column("actor_id")];
    cases.push(("colonna fuori da GROUP BY", ungrouped_column));

    let mut grouped_wildcard = base_query();
    grouped_wildcard.projection = vec![
        QueryProjection {
            expression: QueryExpression::Wildcard { relation: None },
            alias: None,
        },
        QueryProjection {
            expression: count("event_id"),
            alias: None,
        },
    ];
    grouped_wildcard.group_by = vec![column("actor_id")];
    cases.push(("wildcard in query aggregata", grouped_wildcard));

    let mut aggregate_in_where = base_query();
    aggregate_in_where.filter = Some(QueryExpression::Compare {
        left: Box::new(count("event_id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    cases.push(("aggregato in WHERE", aggregate_in_where));

    let mut aggregate_in_group = base_query();
    aggregate_in_group.projection = vec![QueryProjection {
        expression: count("event_id"),
        alias: None,
    }];
    aggregate_in_group.group_by = vec![count("event_id")];
    cases.push(("aggregato in GROUP BY", aggregate_in_group));

    let mut nested_aggregate = base_query();
    nested_aggregate.projection = vec![QueryProjection {
        expression: aggregate(ScalarFunction::Sum, vec![count("event_id")]),
        alias: None,
    }];
    cases.push(("aggregato annidato", nested_aggregate));

    let mut having_without_aggregation = base_query();
    having_without_aggregation.having = Some(QueryExpression::Compare {
        left: Box::new(column("event_id")),
        operator: ComparisonOperator::Gt,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    cases.push(("HAVING senza aggregazione", having_without_aggregation));

    let mut wildcard_in_sum = base_query();
    wildcard_in_sum.projection = vec![QueryProjection {
        expression: aggregate(
            ScalarFunction::Sum,
            vec![QueryExpression::Wildcard { relation: None }],
        ),
        alias: None,
    }];
    cases.push(("wildcard dentro SUM", wildcard_in_sum));

    let mut parameter_group = base_query();
    parameter_group.projection = vec![QueryProjection {
        expression: count("event_id"),
        alias: None,
    }];
    parameter_group.group_by = vec![QueryExpression::Parameter {
        name: "key".to_owned(),
    }];
    cases.push(("parametro in GROUP BY", parameter_group));

    let mut ordering_outside_group = base_query();
    ordering_outside_group.projection = vec![
        QueryProjection {
            expression: column("actor_id"),
            alias: None,
        },
        QueryProjection {
            expression: count("event_id"),
            alias: None,
        },
    ];
    ordering_outside_group.group_by = vec![column("actor_id")];
    ordering_outside_group.order_by = vec![QueryOrdering {
        expression: column("label"),
        direction: SortDirection::Asc,
    }];
    cases.push(("ORDER BY fuori dal gruppo", ordering_outside_group));

    let mut having_outside_group = base_query();
    having_outside_group.projection = vec![QueryProjection {
        expression: column("actor_id"),
        alias: None,
    }];
    having_outside_group.group_by = vec![column("actor_id")];
    having_outside_group.having = Some(QueryExpression::Compare {
        left: Box::new(column("label")),
        operator: ComparisonOperator::Gt,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    cases.push(("HAVING fuori dal gruppo", having_outside_group));

    for (label, query) in cases {
        let error = match render_query(&query, "warehouse") {
            Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
            Err(error) => error,
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan, "{label}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
        assert_eq!(error.provider, Some(ProviderKind::Mysql), "{label}");
    }
}

#[test]
fn distinct_ordering_must_belong_to_the_projection() {
    let mut outside = base_query();
    outside.distinct = true;
    outside.order_by = vec![QueryOrdering {
        expression: column("label"),
        direction: SortDirection::Asc,
    }];
    let error = render_query(&outside, "warehouse").expect_err("ordine non riproducibile");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);

    let mut wildcard = base_query();
    wildcard.projection = vec![QueryProjection {
        expression: QueryExpression::Wildcard { relation: None },
        alias: None,
    }];
    wildcard.distinct = true;
    wildcard.order_by = vec![QueryOrdering {
        expression: column("label"),
        direction: SortDirection::Asc,
    }];
    assert_eq!(
        render_query(&wildcard, "warehouse").expect("render").sql,
        "SELECT DISTINCT * FROM `warehouse`.`events` ORDER BY `label` ASC"
    );
}

/// `MySQL` non ha DISTINCT ON: il rifiuto descrive un'assenza del motore,
/// non una qualificazione mancante.
#[test]
fn distinct_on_is_reported_as_absent_from_mysql() {
    let mut query = base_query();
    query.distinct_on = vec![column("event_id")];
    query.order_by = vec![QueryOrdering {
        expression: column("event_id"),
        direction: SortDirection::Asc,
    }];
    let error = render_query(&query, "warehouse").expect_err("DISTINCT ON");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert!(
        error.message.contains("non esiste in questo dialetto"),
        "{}",
        error.message
    );
}

#[test]
fn window_only_functions_and_argument_counts_fail_before_the_network() {
    let mut bare_window = base_query();
    bare_window.projection = vec![QueryProjection {
        expression: aggregate(ScalarFunction::Rank, Vec::new()),
        alias: None,
    }];
    let error = render_query(&bare_window, "warehouse").expect_err("RANK senza OVER");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);

    let mut wrong_arity = base_query();
    wrong_arity.projection = vec![QueryProjection {
        expression: aggregate(
            ScalarFunction::Count,
            vec![column("event_id"), column("actor_id")],
        ),
        alias: None,
    }];
    assert_eq!(
        render_query(&wrong_arity, "warehouse")
            .expect_err("COUNT con due argomenti")
            .category,
        ErrorCategory::InvalidPlan
    );

    let mut empty_coalesce = base_query();
    empty_coalesce.projection = vec![QueryProjection {
        expression: aggregate(ScalarFunction::Coalesce, Vec::new()),
        alias: None,
    }];
    assert_eq!(
        render_query(&empty_coalesce, "warehouse")
            .expect_err("COALESCE senza argomenti")
            .category,
        ErrorCategory::InvalidPlan
    );
}

/// `MySQL` supporta LATERAL: il rifiuto indica una qualificazione
/// mancante, non un'assenza del motore.
#[test]
fn lateral_is_reported_as_not_yet_qualified_instead_of_absent() {
    let mut lateral = base_query();
    lateral.joins = vec![QueryJoin {
        kind: JoinKind::Inner,
        source: None,
        derived_source: Some(QueryDerivedSource {
            query: Box::new(base_query()),
            alias: "recent".to_owned(),
        }),
        lateral: true,
        on: Some(QueryExpression::Compare {
            left: Box::new(column("event_id")),
            operator: ComparisonOperator::Eq,
            right: Box::new(column("event_id")),
        }),
    }];
    let error = render_query(&lateral, "warehouse").expect_err("lateral join");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert!(!error.message.contains("non esiste"));
    assert!(error.message.contains("non ancora qualificati"));
}

#[test]
fn cross_database_and_oversized_identifiers_are_rejected_before_rendering() {
    let mut cross = base_query();
    cross.source = Some(QuerySource {
        object: ObjectRef {
            catalog: Some("other".to_owned()),
            schema: None,
            object: "events".to_owned(),
        },
        alias: None,
    });
    assert_eq!(
        render_query(&cross, "warehouse")
            .expect_err("cross database")
            .category,
        ErrorCategory::Unsupported
    );

    let mut cross_schema = base_query();
    cross_schema.source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("other".to_owned()),
            object: "events".to_owned(),
        },
        alias: None,
    });
    assert_eq!(
        render_query(&cross_schema, "warehouse")
            .expect_err("schema MySQL diverso dal database configurato")
            .category,
        ErrorCategory::Unsupported
    );

    let mut three_part = base_query();
    three_part.source = Some(QuerySource {
        object: ObjectRef {
            catalog: Some("warehouse".to_owned()),
            schema: Some("warehouse".to_owned()),
            object: "events".to_owned(),
        },
        alias: None,
    });
    assert_eq!(
        render_query(&three_part, "warehouse")
            .expect_err("nome a tre componenti")
            .category,
        ErrorCategory::Unsupported
    );

    let mut long_identifier = base_query();
    long_identifier.projection = vec![QueryProjection {
        expression: column(&"a".repeat(MAX_IDENTIFIER_CHARACTERS + 1)),
        alias: None,
    }];
    assert_eq!(
        render_query(&long_identifier, "warehouse")
            .expect_err("identificatore oltre 64 caratteri")
            .category,
        ErrorCategory::InvalidPlan
    );
}

fn ascending(expression: QueryExpression) -> QueryOrdering {
    QueryOrdering {
        expression,
        direction: SortDirection::Asc,
    }
}

fn scalar_window(
    function: ScalarFunction,
    arguments: Vec<QueryExpression>,
    partition_by: Vec<QueryExpression>,
    order_by: Vec<QueryOrdering>,
    frame: Option<WindowFrame>,
) -> QueryExpression {
    QueryExpression::Window {
        function,
        arguments,
        partition_by,
        order_by,
        frame,
    }
}

const fn frame(
    units: WindowFrameUnits,
    start: WindowFrameBound,
    end: WindowFrameBound,
) -> WindowFrame {
    WindowFrame {
        units,
        start,
        end: Some(end),
    }
}

/// `joined_query()` con una sola proiezione window, sempre con alias
/// `value`: i test sulle forme rifiutate non devono ripetere il piano.
fn windowed_query(expression: QueryExpression) -> QueryOperation {
    let mut query = joined_query();
    query.projection = vec![QueryProjection {
        expression,
        alias: Some("value".to_owned()),
    }];
    query
}

/// Prefisso FROM di `windowed_query`, comune a ogni SQL atteso.
const JOINED_FROM: &str = "FROM `warehouse`.`events` AS `e` \
     INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id`";

/// Una window in SELECT vede l'insieme finale delle relazioni, quindi
/// anche quelle introdotte da un join successivo, mentre i bind della
/// proiezione precedono quelli di WHERE.
#[test]
fn scalar_windows_render_over_the_final_relation_set_with_projection_binds_first() {
    let mut query = joined_query();
    query.projection = vec![
        QueryProjection {
            expression: qualified("e", "event_id"),
            alias: Some("event_id".to_owned()),
        },
        QueryProjection {
            expression: scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                vec![qualified("a", "region_id")],
                vec![ascending(qualified("e", "event_id"))],
                None,
            ),
            alias: Some("ranked".to_owned()),
        },
        QueryProjection {
            expression: scalar_window(
                ScalarFunction::DenseRank,
                Vec::new(),
                Vec::new(),
                vec![ascending(qualified("e", "event_id"))],
                None,
            ),
            alias: Some("dense".to_owned()),
        },
        QueryProjection {
            expression: scalar_window(
                ScalarFunction::Sum,
                vec![qualified("e", "amount")],
                vec![qualified("a", "region_id")],
                vec![ascending(qualified("e", "event_id"))],
                Some(frame(
                    WindowFrameUnits::Range,
                    WindowFrameBound::UnboundedPreceding,
                    WindowFrameBound::CurrentRow,
                )),
            ),
            alias: Some("running".to_owned()),
        },
        QueryProjection {
            expression: scalar_window(
                ScalarFunction::Count,
                vec![QueryExpression::Wildcard { relation: None }],
                vec![qualified("a", "region_id")],
                Vec::new(),
                None,
            ),
            alias: Some("peers".to_owned()),
        },
        // Il bind della proiezione non appartiene piu a una window: la
        // prova sull'ordine posizionale resta, senza dipendere da una
        // funzione che il provider non pubblica.
        QueryProjection {
            expression: QueryExpression::Scalar {
                function: ScalarFunction::Coalesce,
                arguments: vec![
                    qualified("e", "amount"),
                    QueryExpression::Parameter {
                        name: "fallback".to_owned(),
                    },
                ],
            },
            alias: Some("amount".to_owned()),
        },
    ];
    query.filter = Some(QueryExpression::Compare {
        left: Box::new(qualified("e", "event_id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    query.order_by = vec![ascending(qualified("e", "event_id"))];
    query.row_limit = Some(10);

    let rendered = render_query(&query, "warehouse").expect("render");
    assert_eq!(
        rendered.sql,
        "SELECT `e`.`event_id` AS `event_id`, \
         RANK() OVER (PARTITION BY `a`.`region_id` ORDER BY `e`.`event_id` ASC) \
         AS `ranked`, \
         DENSE_RANK() OVER (ORDER BY `e`.`event_id` ASC) AS `dense`, \
         SUM(`e`.`amount`) OVER (PARTITION BY `a`.`region_id` \
         ORDER BY `e`.`event_id` ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
         AS `running`, \
         COUNT(*) OVER (PARTITION BY `a`.`region_id`) AS `peers`, \
         COALESCE(`e`.`amount`, ?) AS `amount` \
         FROM `warehouse`.`events` AS `e` \
         INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
         WHERE `e`.`event_id` >= ? ORDER BY `e`.`event_id` ASC LIMIT 10"
    );
    assert_eq!(
        rendered
            .binds
            .iter()
            .map(|bind| (bind.name.as_str(), bind.ordinal))
            .collect::<Vec<_>>(),
        vec![("fallback", 1), ("floor", 2)]
    );
}

/// Un aggregato di finestra senza ORDER BY copre l'intera partizione ed e
/// gia deterministico: il frame invece taglia la partizione per posizione
/// e senza un ordine dichiarato la porzione non sarebbe determinata.
#[test]
fn aggregate_windows_render_without_an_order_but_a_frame_requires_one() {
    let whole_partition = windowed_query(scalar_window(
        ScalarFunction::Average,
        vec![qualified("e", "amount")],
        vec![qualified("a", "region_id")],
        Vec::new(),
        None,
    ));
    assert_eq!(
        render_query(&whole_partition, "warehouse")
            .expect("render")
            .sql,
        format!(
            "SELECT AVG(`e`.`amount`) OVER (PARTITION BY `a`.`region_id`) AS `value` \
             {JOINED_FROM}"
        )
    );

    let unordered_frame = windowed_query(scalar_window(
        ScalarFunction::Sum,
        vec![qualified("e", "amount")],
        Vec::new(),
        Vec::new(),
        Some(frame(
            WindowFrameUnits::Range,
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::CurrentRow,
        )),
    ));
    let error = render_query(&unordered_frame, "warehouse").expect_err("frame senza ORDER BY");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert!(
        error.message.contains("senza ORDER BY"),
        "{}",
        error.message
    );
}

/// `MySQL` 8.4 non ha GROUPS nel parser, mentre un offset RANGE confronta
/// valori e pretende un INTERVAL quando la chiave d'ordine e temporale:
/// l'AST portabile esprime solo un offset numerico nudo. Un frame ROWS
/// conta invece le posizioni, che fra righe pari sono arbitrarie finche
/// l'unicita dell'ordine non e dimostrata.
#[test]
#[allow(clippy::too_many_lines)]
fn window_frames_are_limited_to_the_units_mysql_can_represent() {
    let range = windowed_query(scalar_window(
        ScalarFunction::Sum,
        vec![qualified("e", "amount")],
        Vec::new(),
        vec![ascending(qualified("e", "event_id"))],
        Some(frame(
            WindowFrameUnits::Range,
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::CurrentRow,
        )),
    ));
    assert_eq!(
        render_query(&range, "warehouse").expect("render").sql,
        format!(
            "SELECT SUM(`e`.`amount`) OVER (ORDER BY `e`.`event_id` ASC \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `value` {JOINED_FROM}"
        )
    );

    let range_offset = windowed_query(scalar_window(
        ScalarFunction::Sum,
        vec![qualified("e", "amount")],
        Vec::new(),
        vec![ascending(qualified("e", "event_id"))],
        Some(frame(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(2),
            WindowFrameBound::CurrentRow,
        )),
    ));
    let offset = render_query(&range_offset, "warehouse").expect_err("RANGE con offset");
    assert_eq!(offset.category, ErrorCategory::Unsupported);
    assert_eq!(offset.phase, ErrorPhase::Prepare);
    assert!(
        offset.message.contains("non ancora qualificat"),
        "{}",
        offset.message
    );

    let rows = windowed_query(scalar_window(
        ScalarFunction::Sum,
        vec![qualified("e", "amount")],
        Vec::new(),
        vec![ascending(qualified("e", "event_id"))],
        Some(frame(
            WindowFrameUnits::Rows,
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::CurrentRow,
        )),
    ));
    let positional = render_query(&rows, "warehouse").expect_err("frame ROWS");
    assert_eq!(positional.category, ErrorCategory::Unsupported);
    assert_eq!(positional.phase, ErrorPhase::Prepare);
    assert!(
        positional.message.contains("ordine totale"),
        "{}",
        positional.message
    );
    // ROWS esiste nel parser di MySQL: il rifiuto indica una qualificazione
    // mancante, non un'assenza del motore.
    assert!(
        !positional.message.contains("non esiste in questo dialetto"),
        "{}",
        positional.message
    );

    let groups = windowed_query(scalar_window(
        ScalarFunction::Sum,
        vec![qualified("e", "amount")],
        Vec::new(),
        vec![ascending(qualified("e", "event_id"))],
        Some(frame(
            WindowFrameUnits::Groups,
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::CurrentRow,
        )),
    ));
    let absent = render_query(&groups, "warehouse").expect_err("GROUPS");
    assert_eq!(absent.category, ErrorCategory::Unsupported);
    assert_eq!(absent.phase, ErrorPhase::Prepare);
    assert!(
        absent.message.contains("non esiste in questo dialetto"),
        "{}",
        absent.message
    );

    for (label, invalid_frame) in [
        (
            "start UNBOUNDED FOLLOWING abbreviato",
            WindowFrame {
                units: WindowFrameUnits::Range,
                start: WindowFrameBound::UnboundedFollowing,
                end: None,
            },
        ),
        (
            "end UNBOUNDED PRECEDING",
            WindowFrame {
                units: WindowFrameUnits::Range,
                start: WindowFrameBound::UnboundedPreceding,
                end: Some(WindowFrameBound::UnboundedPreceding),
            },
        ),
    ] {
        let invalid = windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![qualified("e", "amount")],
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            Some(invalid_frame),
        ));
        let error = render_query(&invalid, "warehouse").expect_err(label);
        assert_eq!(error.category, ErrorCategory::InvalidPlan, "{label}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
        assert!(
            error.message.contains("limite"),
            "{label}: {}",
            error.message
        );
    }
}

/// `RANK` e `DENSE_RANK` sono stabili fra pari: due righe con la stessa
/// chiave d'ordine ricevono lo stesso valore, quindi un ORDER BY non vuoto
/// basta a renderle riproducibili anche quando la chiave ha duplicati.
/// `ROW_NUMBER`, `LAG` e `LEAD` leggono invece una posizione dentro i pari
/// e sono riproducibili solo se la chiave e un ordine totale univoco: una
/// proprieta che l'AST portabile non esprime e che il provider non puo
/// dedurre prima della rete. Il frame esplicito e infine accettato dal
/// parser di `MySQL` 8.4 e poi ignorato, quindi renderizzarlo
/// pubblicherebbe un piano che il motore non esegue.
#[test]
fn peer_stable_ranking_renders_while_total_order_windows_stay_closed() {
    for (function, call) in [
        (ScalarFunction::Rank, "RANK()"),
        (ScalarFunction::DenseRank, "DENSE_RANK()"),
    ] {
        let ordered = windowed_query(scalar_window(
            function,
            Vec::new(),
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            None,
        ));
        assert_eq!(
            render_query(&ordered, "warehouse")
                .unwrap_or_else(|error| panic!("{call}: {}", error.message))
                .sql,
            format!("SELECT {call} OVER (ORDER BY `e`.`event_id` ASC) AS `value` {JOINED_FROM}")
        );

        let unordered = windowed_query(scalar_window(
            function,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        ));
        let missing = render_query(&unordered, "warehouse").expect_err(call);
        assert_eq!(missing.category, ErrorCategory::InvalidPlan, "{call}");
        assert_eq!(missing.phase, ErrorPhase::Prepare, "{call}");
        assert!(missing.message.contains("senza ORDER BY"), "{call}");

        let framed = windowed_query(scalar_window(
            function,
            Vec::new(),
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            Some(frame(
                WindowFrameUnits::Rows,
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
        ));
        let ignored = render_query(&framed, "warehouse").expect_err(call);
        assert_eq!(ignored.category, ErrorCategory::InvalidPlan, "{call}");
        assert_eq!(ignored.phase, ErrorPhase::Prepare, "{call}");
        assert!(
            ignored.message.contains("frame"),
            "{call}: {}",
            ignored.message
        );
    }

    for (function, arguments, call) in [
        (ScalarFunction::RowNumber, Vec::new(), "ROW_NUMBER"),
        (ScalarFunction::Lag, vec![qualified("e", "amount")], "LAG"),
        (ScalarFunction::Lead, vec![qualified("e", "amount")], "LEAD"),
    ] {
        let ordered = windowed_query(scalar_window(
            function,
            arguments,
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            None,
        ));
        let error = match render_query(&ordered, "warehouse") {
            Ok(rendered) => panic!("{call} deve restare fail-closed, reso: {}", rendered.sql),
            Err(error) => error,
        };
        assert_eq!(error.category, ErrorCategory::Unsupported, "{call}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{call}");
        assert_eq!(error.provider, Some(ProviderKind::Mysql), "{call}");
        assert!(
            error.message.contains("ordine totale"),
            "{call}: {}",
            error.message
        );
        assert!(
            error.message.contains("non ancora qualificat"),
            "{call}: {}",
            error.message
        );
        // MySQL ha ROW_NUMBER, LAG e LEAD: il rifiuto indica una
        // qualificazione mancante, non un'assenza.
        assert!(
            !error.message.contains("non esiste in questo dialetto"),
            "{call}: {}",
            error.message
        );
    }
}

/// I casi che la validazione portabile rifiuta prima del renderer.
///
/// Non sono limiti di `MySQL`: nessun motore ammette una window in una
/// clausola `ON` o dentro gli argomenti di un'altra window.
const PORTABLE_REJECTIONS: &[&str] = &["window in ON", "window annidata in una window"];

#[test]
#[allow(clippy::too_many_lines)]
fn window_operands_stay_row_only_scalar_and_relation_valid() {
    let ordering = || vec![ascending(qualified("e", "event_id"))];
    let mut cases: Vec<(&str, QueryOperation, ErrorCategory)> = Vec::new();

    cases.push((
        "window annidata in una window",
        windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                Vec::new(),
                ordering(),
                None,
            )],
            Vec::new(),
            Vec::new(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    cases.push((
        "aggregato dentro una window",
        windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![count_qualified("e", "event_id")],
            Vec::new(),
            Vec::new(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    cases.push((
        "aggregato in PARTITION BY",
        windowed_query(scalar_window(
            ScalarFunction::Rank,
            Vec::new(),
            vec![count_qualified("e", "event_id")],
            ordering(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    cases.push((
        "aggregato nell'ORDER BY della window",
        windowed_query(scalar_window(
            ScalarFunction::Rank,
            Vec::new(),
            Vec::new(),
            vec![ascending(count_qualified("e", "event_id"))],
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    // SpatialFunction::Area e verificata anche dentro una window. Il caso
    // fail-closed usa invece una funzione non verificata.
    cases.push((
        "spatial non-verified dentro una window",
        windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![QueryExpression::Spatial {
                function: SpatialFunction::NRings,
                arguments: vec![qualified("e", "geom")],
            }],
            Vec::new(),
            Vec::new(),
            None,
        )),
        ErrorCategory::Unsupported,
    ));

    cases.push((
        "subquery dentro una window",
        windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![QueryExpression::ScalarSubquery {
                query: Box::new(base_query()),
            }],
            Vec::new(),
            Vec::new(),
            None,
        )),
        ErrorCategory::Unsupported,
    ));

    cases.push((
        "relazione assente nell'argomento",
        windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![qualified("missing", "amount")],
            Vec::new(),
            Vec::new(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    cases.push((
        "relazione assente in PARTITION BY",
        windowed_query(scalar_window(
            ScalarFunction::Rank,
            Vec::new(),
            vec![qualified("missing", "region_id")],
            ordering(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    cases.push((
        "relazione assente nell'ORDER BY della window",
        windowed_query(scalar_window(
            ScalarFunction::Rank,
            Vec::new(),
            Vec::new(),
            vec![ascending(qualified("missing", "event_id"))],
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    // `COUNT(*)` sopravvive come unica forma di wildcard: `COUNT(t.*)` e
    // ogni altro aggregato con `*` restano fuori anche sotto OVER.
    cases.push((
        "COUNT qualificato con wildcard",
        windowed_query(scalar_window(
            ScalarFunction::Count,
            vec![QueryExpression::Wildcard {
                relation: Some("e".to_owned()),
            }],
            Vec::new(),
            Vec::new(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    cases.push((
        "wildcard dentro SUM OVER",
        windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![QueryExpression::Wildcard { relation: None }],
            Vec::new(),
            Vec::new(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    cases.push((
        "identificatore oltre 64 caratteri in PARTITION BY",
        windowed_query(scalar_window(
            ScalarFunction::Rank,
            Vec::new(),
            vec![column(&"a".repeat(MAX_IDENTIFIER_CHARACTERS + 1))],
            ordering(),
            None,
        )),
        ErrorCategory::InvalidPlan,
    ));

    // Alcuni casi non sono limiti di `MySQL`: sono SQL invalido per
    // qualunque motore, e da quando `validate_query_operation` conosce la
    // clausola di provenienza di una window li rifiuta prima del
    // renderer. Rispondono percio in fase `Validate` e senza provider —
    // il confine giusto, e va misurato dove cade davvero.
    for (label, query, category) in cases {
        let error = match render_query(&query, "warehouse") {
            Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
            Err(error) => error,
        };
        assert_eq!(error.category, category, "{label}: {}", error.message);
        if PORTABLE_REJECTIONS.contains(&label) {
            assert_eq!(error.phase, ErrorPhase::Validate, "{label}");
            assert_eq!(error.provider, None, "{label}");
        } else {
            assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
            assert_eq!(error.provider, Some(ProviderKind::Mysql), "{label}");
        }
    }
}

/// `MySQL` valuta una window dopo WHERE e GROUP BY e prima di ORDER BY:
/// fuori dalla lista di selezione la forma non e dimostrata qui.
#[test]
fn windows_outside_the_projection_stay_closed() {
    let position = || {
        scalar_window(
            ScalarFunction::Rank,
            Vec::new(),
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            None,
        )
    };
    let threshold = || QueryExpression::Parameter {
        name: "floor".to_owned(),
    };

    let mut filtered = joined_query();
    filtered.filter = Some(QueryExpression::Compare {
        left: Box::new(position()),
        operator: ComparisonOperator::Lte,
        right: Box::new(threshold()),
    });

    let mut ordered = joined_query();
    ordered.order_by = vec![ascending(position())];

    let mut keyed = joined_query();
    keyed.projection = vec![QueryProjection {
        expression: count_qualified("e", "event_id"),
        alias: Some("events".to_owned()),
    }];
    keyed.group_by = vec![position()];

    let mut filtered_group = joined_query();
    filtered_group.projection = vec![QueryProjection {
        expression: count_qualified("e", "event_id"),
        alias: Some("events".to_owned()),
    }];
    filtered_group.group_by = vec![qualified("a", "region_id")];
    filtered_group.having = Some(QueryExpression::Compare {
        left: Box::new(position()),
        operator: ComparisonOperator::Lte,
        right: Box::new(threshold()),
    });

    // `WHERE`, `GROUP BY` e `HAVING` non sono clausole in cui una window
    // sia SQL valido: le rifiuta la validazione portabile, per tutti i
    // provider e prima del renderer. `ORDER BY` invece **e** valido — solo
    // non ancora qualificato qui — e resta il caso che misura la chiusura
    // di questo provider.
    for (label, query) in [
        ("window in WHERE", filtered),
        ("window in GROUP BY", keyed),
        ("window in HAVING", filtered_group),
    ] {
        let error = match render_query(&query, "warehouse") {
            Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
            Err(error) => error,
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan, "{label}");
        assert_eq!(error.phase, ErrorPhase::Validate, "{label}");
        assert!(
            error.message.contains("fuori da projection"),
            "{label}: {}",
            error.message
        );
    }

    let error = match render_query(&ordered, "warehouse") {
        Ok(rendered) => panic!(
            "window in ORDER BY deve restare fail-closed: {}",
            rendered.sql
        ),
        Err(error) => error,
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert_eq!(error.provider, Some(ProviderKind::Mysql));
    assert!(
        error.message.contains("non ancora qualificat"),
        "{}",
        error.message
    );
}

/// Una window e valutata dopo il raggruppamento e prima di DISTINCT: le
/// due combinazioni hanno semantiche precise non ancora dimostrate, quindi
/// restano chiuse invece di essere renderizzate.
#[test]
fn windows_combined_with_grouping_or_distinct_stay_closed() {
    let position = || {
        scalar_window(
            ScalarFunction::Rank,
            Vec::new(),
            Vec::new(),
            vec![ascending(qualified("a", "region_id"))],
            None,
        )
    };

    let mut grouped = windowed_query(position());
    grouped.group_by = vec![qualified("a", "region_id")];

    let mut with_aggregate = joined_query();
    with_aggregate.projection = vec![
        QueryProjection {
            expression: count_qualified("e", "event_id"),
            alias: Some("events".to_owned()),
        },
        QueryProjection {
            expression: position(),
            alias: Some("value".to_owned()),
        },
    ];

    let mut distinct = windowed_query(position());
    distinct.distinct = true;

    for (label, query) in [
        ("window con GROUP BY", grouped),
        ("window con aggregato di gruppo", with_aggregate),
        ("window con DISTINCT", distinct),
    ] {
        let error = match render_query(&query, "warehouse") {
            Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
            Err(error) => error,
        };
        assert_eq!(error.category, ErrorCategory::Unsupported, "{label}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
        // «non ancora qualificat…» oppure «non qualificata su <prodotto>»:
        // il secondo e il messaggio del cancello spatial, che da quando
        // legge la lista del profilo nomina il prodotto invece di rinviare
        // a una costante — su due prodotti quella costante non e piu una.
        assert!(
            error.message.contains("non ancora qualificat")
                || error.message.contains("non qualificata su"),
            "{label}: {}",
            error.message
        );
    }
}

/// Funzione non ammessa sotto OVER e rango con argomenti sono gia
/// rifiutati dalla validazione portabile in fase `Validate`. Il provider
/// ripete il controllo per conto proprio: quel confine appartiene al path
/// `MySQL` e non deve dipendere dall'ordine dei validatori.
#[test]
fn the_window_function_boundary_is_covered_by_the_core_and_by_the_provider() {
    let scalar_over = windowed_query(scalar_window(
        ScalarFunction::Lower,
        vec![qualified("e", "label")],
        Vec::new(),
        vec![ascending(qualified("e", "event_id"))],
        None,
    ));
    let ranked_with_arguments = windowed_query(scalar_window(
        ScalarFunction::Rank,
        vec![qualified("e", "event_id")],
        Vec::new(),
        vec![ascending(qualified("e", "event_id"))],
        None,
    ));

    for (label, query) in [
        ("funzione scalare sotto OVER", scalar_over),
        ("rango con argomenti", ranked_with_arguments),
    ] {
        let core = render_query(&query, "warehouse").expect_err(label);
        assert_eq!(core.category, ErrorCategory::InvalidPlan, "{label}");
        assert_eq!(core.phase, ErrorPhase::Validate, "{label}");

        let provider = ensure_qualified_shape(&query, "warehouse", &crate::profile::MYSQL_PROFILE)
            .expect_err(label);
        assert_eq!(provider.category, ErrorCategory::InvalidPlan, "{label}");
        assert_eq!(provider.phase, ErrorPhase::Prepare, "{label}");
        assert_eq!(provider.provider, Some(ProviderKind::Mysql), "{label}");
    }
}

fn wire_column(name: &str, column_type: ColumnType) -> Column {
    Column::new(column_type)
        .with_name(name.as_bytes())
        .with_character_set(255)
}

#[test]
fn statement_metadata_maps_to_the_arrow_contract() {
    let columns = vec![
        wire_column("id", ColumnType::MYSQL_TYPE_LONGLONG)
            .with_flags(ColumnFlags::NOT_NULL_FLAG | ColumnFlags::UNSIGNED_FLAG),
        wire_column("label", ColumnType::MYSQL_TYPE_VAR_STRING),
        wire_column("payload", ColumnType::MYSQL_TYPE_BLOB).with_character_set(63),
        wire_column("moment", ColumnType::MYSQL_TYPE_DATETIME),
    ];
    let specs = query_result_columns(&columns).expect("mapping");
    assert_eq!(specs[0].kind, MysqlColumnKind::U64);
    assert!(!specs[0].nullable);
    assert!(specs
        .iter()
        .all(|specification| specification.native_declaration.is_empty()));
    assert!(specs.iter().all(|specification| !specification
        .arrow_field()
        .metadata()
        .contains_key(protocol::MYSQL_NATIVE_DECLARATION)));
    assert_eq!(specs[1].kind, MysqlColumnKind::Utf8);
    assert!(specs[1].nullable);
    assert_eq!(specs[2].kind, MysqlColumnKind::Binary);
    assert_eq!(specs[2].native_type, "blob");
    assert_eq!(specs[3].kind, MysqlColumnKind::Timestamp);
    assert_eq!(
        specs[3]
            .arrow_field()
            .metadata()
            .get(protocol::MYSQL_NATIVE_TYPE),
        Some(&"datetime".to_owned())
    );
    assert_eq!(specs[1].arrow_field().data_type(), &DataType::Utf8);
}

#[test]
fn boolean_width_matches_the_catalog_read_path() {
    let boolean = wire_column("flag", ColumnType::MYSQL_TYPE_TINY).with_column_length(1);
    assert_eq!(
        query_result_columns(std::slice::from_ref(&boolean)).expect("bool")[0].kind,
        MysqlColumnKind::Bool
    );
    let signed = wire_column("small", ColumnType::MYSQL_TYPE_TINY).with_column_length(4);
    assert_eq!(
        query_result_columns(std::slice::from_ref(&signed)).expect("tinyint")[0].kind,
        MysqlColumnKind::I8
    );
}

#[test]
fn decimal_precision_is_reconstructed_and_bounded() {
    let signed = wire_column("amount", ColumnType::MYSQL_TYPE_NEWDECIMAL)
        .with_column_length(12)
        .with_decimals(2);
    assert_eq!(
        query_result_columns(std::slice::from_ref(&signed)).expect("decimal")[0].kind,
        MysqlColumnKind::Decimal {
            precision: 10,
            scale: 2,
        }
    );

    let unsigned = wire_column("total", ColumnType::MYSQL_TYPE_NEWDECIMAL)
        .with_column_length(10)
        .with_decimals(0)
        .with_flags(ColumnFlags::UNSIGNED_FLAG);
    assert_eq!(
        query_result_columns(std::slice::from_ref(&unsigned)).expect("decimal")[0].kind,
        MysqlColumnKind::Decimal {
            precision: 10,
            scale: 0,
        }
    );

    let wide = wire_column("wide", ColumnType::MYSQL_TYPE_NEWDECIMAL)
        .with_column_length(67)
        .with_decimals(0);
    assert_eq!(
        query_result_columns(std::slice::from_ref(&wide))
            .expect_err("oltre Decimal128")
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn geometry_and_empty_result_sets_fail_closed() {
    let geometry = wire_column("geom", ColumnType::MYSQL_TYPE_GEOMETRY).with_character_set(63);
    assert_eq!(
        query_result_columns(std::slice::from_ref(&geometry))
            .expect_err("geometria")
            .category,
        ErrorCategory::Unsupported
    );
    assert_eq!(
        query_result_columns(&[])
            .expect_err("nessuna colonna")
            .category,
        ErrorCategory::Schema
    );
    let anonymous = Column::new(ColumnType::MYSQL_TYPE_LONG);
    assert_eq!(
        query_result_columns(std::slice::from_ref(&anonymous))
            .expect_err("nome vuoto")
            .category,
        ErrorCategory::Schema
    );
    let duplicate = vec![
        wire_column("same", ColumnType::MYSQL_TYPE_LONG),
        wire_column("same", ColumnType::MYSQL_TYPE_VAR_STRING),
    ];
    assert_eq!(
        query_result_columns(&duplicate)
            .expect_err("nomi output duplicati")
            .category,
        ErrorCategory::Schema
    );
}

#[test]
fn renderer_is_the_shared_mysql_dialect() {
    let identifier = plenora_database_sql::Identifier::new("we`ird").expect("identifier");
    assert_eq!(
        mysql_renderer().quote_identifier(&identifier).unwrap(),
        "`we``ird`"
    );
}

/// Una funzione verified che rende geometria porta una regola usabile.
///
/// Il provider sa propagare la regola `preserves`; una funzione pubblicata
/// con una regola diversa supererebbe il renderer ma fallirebbe nel
/// risolutore del CRS. La guardia verifica la coerenza fra lista delle
/// funzioni e catalogo delle regole.
#[test]
fn a_verified_geometry_function_carries_a_rule_the_provider_can_use() {
    for (product, functions) in [
        ("MySQL", VERIFIED_SPATIAL_FUNCTIONS),
        ("MariaDB", MARIADB_VERIFIED_SPATIAL_FUNCTIONS),
    ] {
        let unusable = functions
            .iter()
            .filter(|function| function.returns_geometry())
            .filter(|function| {
                function.crs_rule()
                    != Some(plenora_database_core::spatial_catalog::CrsRule::Preserves)
            })
            .map(|function| format!("{function:?}"))
            .collect::<Vec<_>>();
        assert!(
            unusable.is_empty(),
            "{product} pubblica funzioni geometriche la cui regola di CRS il                  provider non sa propagare: {}",
            unusable.join(", ")
        );
    }
}
