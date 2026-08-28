use super::*;
use crate::limits::Limits;
use crate::plan::ObjectRef;

fn query_with_filter(filter: QueryExpression) -> QueryOperation {
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("public".to_owned()),
                object: "events".to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            },
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(filter),
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
fn source_free_query_accepts_parameters_but_not_relation_references() {
    let mut query = query_with_filter(QueryExpression::Compare {
        left: Box::new(QueryExpression::Parameter {
            name: "left".to_owned(),
        }),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "right".to_owned(),
        }),
    });
    query.source = None;
    query.projection[0].expression = QueryExpression::Parameter {
        name: "projected".to_owned(),
    };
    validate_query_operation(&query, &Limits::default()).expect("source-free parameters");

    query.projection[0].expression = QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: "orphan".to_owned(),
        },
    };
    let error = validate_query_operation(&query, &Limits::default())
        .expect_err("source-free column must fail");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn query_walker_reaches_sources_inside_subqueries() {
    let nested = query_with_filter(QueryExpression::IsNull {
        expression: Box::new(QueryExpression::Parameter {
            name: "nested".to_owned(),
        }),
        negated: false,
    });
    let query = query_with_filter(QueryExpression::Exists {
        query: Box::new(nested),
        negated: false,
    });
    let mut sources = 0;
    let mut parameters = Vec::new();

    assert!(walk_query(&query, |node| {
        match node {
            QueryWalkNode::Source(_) => sources += 1,
            QueryWalkNode::Expression(QueryExpression::Parameter { name }) => {
                parameters.push(name.as_str());
            }
            QueryWalkNode::Operation(_) | QueryWalkNode::Expression(_) => {}
        }
        QueryWalkControl::Continue
    }));

    assert_eq!(sources, 2);
    assert_eq!(parameters, ["nested"]);
}

#[test]
fn query_walker_can_skip_or_break_a_subtree() {
    let expression = QueryExpression::And {
        arguments: vec![
            QueryExpression::Parameter {
                name: "first".to_owned(),
            },
            QueryExpression::IsNull {
                expression: Box::new(QueryExpression::Parameter {
                    name: "hidden".to_owned(),
                }),
                negated: false,
            },
        ],
    };
    let mut visited = Vec::new();
    assert!(walk_query_expression(&expression, |node| {
        if let QueryWalkNode::Expression(expression) = node {
            match expression {
                QueryExpression::IsNull { .. } => return QueryWalkControl::Skip,
                QueryExpression::Parameter { name } => visited.push(name.as_str()),
                _ => {}
            }
        }
        QueryWalkControl::Continue
    }));
    assert_eq!(visited, ["first"]);

    assert!(!walk_query_expression(&expression, |_| {
        QueryWalkControl::Break
    }));
}

#[test]
fn canonical_predicates_share_validation_and_traversal() {
    let column = || QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: "name".to_owned(),
        },
    };
    let parameter = |name: &str| QueryExpression::Parameter {
        name: name.to_owned(),
    };
    let expression = QueryExpression::Not {
        expression: Box::new(QueryExpression::And {
            arguments: vec![
                QueryExpression::InList {
                    expression: Box::new(column()),
                    values: vec![parameter("first"), parameter("second")],
                    negated: false,
                },
                QueryExpression::Between {
                    expression: Box::new(column()),
                    lower: Box::new(parameter("lower")),
                    upper: Box::new(parameter("upper")),
                    negated: false,
                },
                QueryExpression::Like {
                    expression: Box::new(column()),
                    pattern: Box::new(parameter("pattern")),
                    case_insensitive: false,
                    negated: false,
                },
            ],
        }),
    };
    let query = query_with_filter(expression);
    validate_query_operation(&query, &Limits::default()).expect("canonical predicates");

    let mut parameters = Vec::new();
    assert!(walk_query(&query, |node| {
        if let QueryWalkNode::Expression(QueryExpression::Parameter { name }) = node {
            parameters.push(name.clone());
        }
        QueryWalkControl::Continue
    }));
    parameters.sort();
    assert_eq!(parameters, ["first", "lower", "pattern", "second", "upper"]);
}

#[test]
fn canonical_in_list_rejects_an_empty_value_set() {
    let error = validate_query_operation(
        &query_with_filter(QueryExpression::InList {
            expression: Box::new(QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            }),
            values: Vec::new(),
            negated: false,
        }),
        &Limits::default(),
    )
    .expect_err("empty IN list");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn rejects_deep_query_without_recursive_validation() {
    let mut expression = QueryExpression::Parameter {
        name: "value".to_owned(),
    };
    for _ in 0..80 {
        expression = QueryExpression::IsNull {
            expression: Box::new(expression),
            negated: false,
        };
    }
    let error = validate_query_operation(&query_with_filter(expression), &Limits::default())
        .expect_err("depth limit");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn rejects_query_over_node_budget() {
    let arguments = (0..4_096)
        .map(|_| QueryExpression::Parameter {
            name: "value".to_owned(),
        })
        .collect();
    let error = validate_query_operation(
        &query_with_filter(QueryExpression::And { arguments }),
        &Limits::default(),
    )
    .expect_err("node limit");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn rejects_non_boolean_filters_and_invalid_spatial_arity() {
    let error = validate_query_operation(
        &query_with_filter(QueryExpression::SpatialOperator {
            operator: SpatialOperator::KnnDistance,
            left: Box::new(QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "geom".to_owned(),
                },
            }),
            right: Box::new(QueryExpression::Parameter {
                name: "probe".to_owned(),
            }),
        }),
        &Limits::default(),
    )
    .expect_err("KNN distance is not boolean");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);

    let error = validate_query_operation(
        &query_with_filter(QueryExpression::Spatial {
            function: SpatialFunction::DWithin,
            arguments: vec![QueryExpression::Parameter {
                name: "probe".to_owned(),
            }],
        }),
        &Limits::default(),
    )
    .expect_err("invalid spatial arity");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
}

#[test]
fn postgis_overload_arities_are_fail_closed() {
    assert!(!SpatialFunction::Buffer.accepts_argument_count(1));
    assert!(SpatialFunction::Buffer.accepts_argument_count(2));
    assert!(SpatialFunction::Buffer.accepts_argument_count(3));
    assert!(SpatialFunction::Union.accepts_argument_count(1));
    assert!(SpatialFunction::Union.accepts_argument_count(3));
    assert!(SpatialFunction::UnaryUnion.accepts_argument_count(2));
    assert!(SpatialFunction::Transform.accepts_argument_count(3));
    assert!(SpatialFunction::Force4d.accepts_argument_count(3));
    assert!(SpatialFunction::SnapToGrid.accepts_argument_count(5));
    assert!(!SpatialFunction::SnapToGrid.accepts_argument_count(4));
    assert!(!SpatialFunction::SnapToGrid.accepts_argument_count(6));
}

#[test]
fn rejects_reversed_window_frame() {
    let mut query = query_with_filter(QueryExpression::Compare {
        left: Box::new(QueryExpression::Parameter {
            name: "left".to_owned(),
        }),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "right".to_owned(),
        }),
    });
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Window {
            function: ScalarFunction::Sum,
            arguments: vec![QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            }],
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: Some(WindowFrame {
                units: WindowFrameUnits::Rows,
                start: WindowFrameBound::Following(2),
                end: Some(WindowFrameBound::Preceding(1)),
            }),
        },
        alias: None,
    }];
    let error = validate_query_operation(&query, &Limits::default()).expect_err("reversed frame");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
}
fn trivial_predicate() -> QueryExpression {
    QueryExpression::Compare {
        left: Box::new(QueryExpression::Parameter {
            name: "left".to_owned(),
        }),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "right".to_owned(),
        }),
    }
}

fn row_number() -> QueryExpression {
    QueryExpression::Window {
        function: ScalarFunction::RowNumber,
        arguments: Vec::new(),
        partition_by: Vec::new(),
        order_by: Vec::new(),
        frame: None,
    }
}

/// Una query minima con la window nella posizione indicata dal chiamante.
fn query_without_filter() -> QueryOperation {
    let mut query = query_with_filter(trivial_predicate());
    query.filter = None;
    query
}

#[test]
fn a_window_in_the_projection_and_in_order_by_is_valid() {
    let mut query = query_without_filter();
    query.projection = vec![QueryProjection {
        expression: row_number(),
        alias: Some("position".to_owned()),
    }];
    query.order_by = vec![QueryOrdering {
        expression: row_number(),
        direction: SortDirection::Asc,
    }];
    validate_query_operation(&query, &Limits::default())
        .expect("le due sole clausole che ammettono una window");
}

#[test]
fn a_window_in_the_order_by_of_a_set_operation_is_rejected() {
    // Lo stesso `ORDER BY` che sopra e valido diventa invalido appena la
    // query acquista un ramo: il renderer lo emette dopo l'unione.
    let mut query = query_without_filter();
    query.order_by = vec![QueryOrdering {
        expression: row_number(),
        direction: SortDirection::Asc,
    }];
    query.set_operations = vec![QuerySetOperation {
        operator: QuerySetOperator::Union,
        all: false,
        query: Box::new(query_without_filter()),
    }];
    let error = validate_query_operation(&query, &Limits::default())
        .expect_err("window nell'ORDER BY di una UNION");
    assert!(error.message.contains("set operation"), "{error:?}");
}

#[test]
fn a_window_below_a_comparison_in_the_projection_stays_valid() {
    // `SELECT row_number() OVER () = $1 AS first` e SQL valido: la regola
    // e sulla clausola, non sulla profondita, e restringerla al solo nodo
    // di testa rifiuterebbe piani corretti.
    let mut query = query_without_filter();
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Compare {
            left: Box::new(row_number()),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "first".to_owned(),
            }),
        },
        alias: Some("first".to_owned()),
    }];
    validate_query_operation(&query, &Limits::default())
        .expect("una window annidata nella projection resta valida");
}

#[test]
fn a_window_in_the_filter_is_rejected() {
    // `WHERE row_number() OVER () = $1`: il `Compare` supera il controllo
    // di booleanita, e senza la posizione sintattica il piano arrivava
    // intatto al provider.
    let query = query_with_filter(QueryExpression::Compare {
        left: Box::new(row_number()),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "first".to_owned(),
        }),
    });
    let error = validate_query_operation(&query, &Limits::default()).expect_err("window in WHERE");
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
    assert!(error.message.contains("fuori da projection"), "{error:?}");
}

#[test]
fn a_window_in_group_by_and_in_having_is_rejected() {
    for clause in ["group_by", "having"] {
        let mut query = query_without_filter();
        let expression = QueryExpression::Compare {
            left: Box::new(row_number()),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "first".to_owned(),
            }),
        };
        if clause == "group_by" {
            query.group_by = vec![expression];
        } else {
            query.having = Some(expression);
        }
        let error = validate_query_operation(&query, &Limits::default())
            .expect_err("window fuori clausola");
        assert_eq!(
            error.category,
            crate::ErrorCategory::InvalidPlan,
            "{clause}"
        );
    }
}

#[test]
fn a_window_nested_in_another_window_is_rejected() {
    for position in ["argument", "partition_by", "order_by"] {
        let mut query = query_without_filter();
        let mut outer = QueryExpression::Window {
            function: ScalarFunction::Lag,
            arguments: vec![QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            }],
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        };
        if let QueryExpression::Window {
            arguments,
            partition_by,
            order_by,
            ..
        } = &mut outer
        {
            match position {
                "argument" => arguments.push(row_number()),
                "partition_by" => partition_by.push(row_number()),
                _ => order_by.push(QueryOrdering {
                    expression: row_number(),
                    direction: SortDirection::Asc,
                }),
            }
        }
        query.projection = vec![QueryProjection {
            expression: outer,
            alias: None,
        }];
        let error =
            validate_query_operation(&query, &Limits::default()).expect_err("window annidata");
        assert_eq!(
            error.category,
            crate::ErrorCategory::InvalidPlan,
            "{position}"
        );
        assert!(error.message.contains("annidata"), "{position}: {error:?}");
    }
}

fn geometry_column() -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: "geom".to_owned(),
        },
    }
}

fn lag_geometry() -> QueryExpression {
    QueryExpression::Window {
        function: ScalarFunction::Lag,
        arguments: vec![geometry_column()],
        partition_by: Vec::new(),
        order_by: Vec::new(),
        frame: None,
    }
}

fn spatial_projection(
    function: SpatialFunction,
    arguments: Vec<QueryExpression>,
) -> QueryOperation {
    let mut query = query_without_filter();
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Spatial {
            function,
            arguments,
        },
        alias: Some("shape".to_owned()),
    }];
    query
}

/// `ST_Union` e `ST_Collect` esistono in due forme omonime: l'aggregata
/// unaria su un insieme di righe e quella scalare che combina le geometrie
/// che riceve. Solo la prima chiude la finestra ai suoi argomenti.
#[test]
fn a_window_inside_an_unambiguously_scalar_overload_stays_valid() {
    // `ST_Collect(x, y)` e `ST_Union(x, y, z)`: a quelle arita `PostGIS`
    // pubblica solo la forma scalare, quindi la window non e annidata in
    // nessuna aggregata e il piano resta valido.
    let collect = spatial_projection(
        SpatialFunction::Collect,
        vec![geometry_column(), lag_geometry()],
    );
    validate_query_operation(&collect, &Limits::default())
        .expect("ST_Collect binaria e solo scalare");

    let union = spatial_projection(
        SpatialFunction::Union,
        vec![geometry_column(), lag_geometry(), geometry_column()],
    );
    validate_query_operation(&union, &Limits::default()).expect("ST_Union ternaria e solo scalare");
}

#[test]
fn a_window_inside_an_ambiguous_overload_is_rejected() {
    // `ST_Union(x, y)` puo essere l'aggregata `(geometry set, float8)`: il
    // piano non porta i tipi che lo escluderebbero, e rifiutare prima
    // della rete costa meno che scoprirlo dal server.
    let query = spatial_projection(
        SpatialFunction::Union,
        vec![geometry_column(), lag_geometry()],
    );
    let error = validate_query_operation(&query, &Limits::default())
        .expect_err("ST_Union binaria e ambigua");
    assert!(error.message.contains("annidata"), "{error:?}");
}

/// Dove `PostGIS` pubblica un'aggregata a quell'arita, la risposta e
/// `true` — anche quando esiste **anche** una scalare.
///
/// La tabella viene da `pg_proc.prokind` misurato su `PostGIS` 3.4, non da
/// una lettura della documentazione. Il caso che conta e
/// `ST_Union` a due argomenti: `(geometry, geometry)` e scalare e
/// `(geometry set, float8)` e aggregata, e il piano non porta i tipi che
/// li distinguerebbero. La risposta deve essere `true`: il renderer tipizza
/// solo i `Parameter` e lascia passare invariata una `Column`.
#[test]
fn an_ambiguous_arity_answers_that_the_aggregate_is_possible() {
    // Ambigue: aggregata **o** scalare, e il piano non lo dice.
    assert!(SpatialFunction::Collect.is_aggregate(1));
    assert!(SpatialFunction::Union.is_aggregate(1));
    assert!(SpatialFunction::Union.is_aggregate(2));

    // Non ambigue: a quell'arita `PostGIS` ha solo la scalare.
    assert!(!SpatialFunction::Collect.is_aggregate(2));
    assert!(!SpatialFunction::Union.is_aggregate(3));

    // Solo aggregate, ma **alle arita che PostGIS pubblica**: fuori da
    // quelle la chiamata non esiste, e dirne qualcosa sarebbe una risposta
    // su un piano che la validazione delle arita rifiutera comunque.
    for (function, valid) in [
        (SpatialFunction::Extent, vec![1]),
        (SpatialFunction::AsMvt, vec![1, 2, 3, 4, 5]),
        (SpatialFunction::AsGeobuf, vec![1, 2]),
    ] {
        for count in 0..=6 {
            assert_eq!(
                function.is_aggregate(count),
                valid.contains(&count),
                "{function:?} a {count}"
            );
            assert_eq!(
                function.is_aggregate(count),
                function.accepts_argument_count(count),
                "{function:?} a {count}: aggregata e arita valida devono coincidere"
            );
        }
    }
    // Nemmeno le due ambigue rispondono fuori dalle proprie arita.
    assert!(!SpatialFunction::Collect.is_aggregate(0));
    assert!(!SpatialFunction::Union.is_aggregate(0));
    assert!(!SpatialFunction::Union.is_aggregate(4));

    // Nessuna delle due e una window: la domanda resta distinta.
    assert!(!SpatialFunction::Union.is_window_only());
    assert!(!SpatialFunction::Collect.is_window_only());
}

// Il fatto che rende ambigua l'arita — il renderer tipizza come geometria
// solo i `Parameter`, e lascia passare invariata una `Column` — e fissato
// dove quel comportamento vive: `plenora_database_sql`,
// `a_column_in_a_geometry_position_is_not_typed_by_the_renderer`.

#[test]
fn a_window_inside_the_unary_spatial_aggregate_is_rejected() {
    for function in [SpatialFunction::Union, SpatialFunction::Collect] {
        let query = spatial_projection(function, vec![lag_geometry()]);
        let error = validate_query_operation(&query, &Limits::default())
            .expect_err("window dentro l'aggregata unaria");
        assert!(
            error.message.contains("annidata"),
            "{function:?}: {error:?}"
        );
    }
}

/// La stessa distinzione vale per `FOR UPDATE`: e l'aggregata a essere
/// incompatibile con il locking di riga, non il nome della funzione.
#[test]
fn locking_survives_a_scalar_spatial_overload_and_not_the_aggregate() {
    let lock = QueryLock {
        strength: QueryLockStrength::Update,
        relations: Vec::new(),
        wait: QueryLockWait::Wait,
    };

    let mut scalar = spatial_projection(
        SpatialFunction::Collect,
        vec![geometry_column(), geometry_column()],
    );
    scalar.locking = Some(lock.clone());
    validate_query_operation(&scalar, &Limits::default())
        .expect("ST_Collect binaria non aggrega niente");

    let mut aggregate = spatial_projection(SpatialFunction::Union, vec![geometry_column()]);
    aggregate.locking = Some(lock);
    let error = validate_query_operation(&aggregate, &Limits::default())
        .expect_err("ST_Union unaria e aggregata");
    assert!(error.message.contains("locking non ammesso"), "{error:?}");
}

#[test]
fn a_window_in_an_aggregate_argument_is_rejected() {
    let mut query = query_without_filter();
    query.projection = vec![QueryProjection {
        expression: QueryExpression::Scalar {
            function: ScalarFunction::Sum,
            arguments: vec![row_number()],
        },
        alias: None,
    }];
    let error = validate_query_operation(&query, &Limits::default())
        .expect_err("window dentro un'aggregata");
    assert!(error.message.contains("annidata"), "{error:?}");
}

#[test]
fn a_window_in_the_projection_of_a_subquery_is_valid() {
    // Ogni operazione apre la propria projection: la posizione si
    // ricalcola, non si eredita dal contesto che contiene la subquery.
    let mut inner = query_without_filter();
    inner.projection = vec![QueryProjection {
        expression: row_number(),
        alias: None,
    }];
    let query = query_with_filter(QueryExpression::Compare {
        left: Box::new(QueryExpression::ScalarSubquery {
            query: Box::new(inner),
        }),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "first".to_owned(),
        }),
    });
    validate_query_operation(&query, &Limits::default())
        .expect("la subquery ha la propria projection");
}

/// Il filtro `spatial` del piano ammette esattamente i predicati che questo
/// motore sa valutare come booleani.
///
/// Il test confronta direttamente lo schema v2 con `returns_boolean`, cosi
/// il contratto non puo omettere un predicato che l'engine valuta.
///
/// `relate` resta fuori di proposito: e booleana solo con tre argomenti, e
/// la forma del filtro nel piano ne prevede due.
#[test]
fn the_plan_admits_exactly_the_spatial_predicates_this_engine_evaluates() {
    /// La variante `spatial` del filtro, cercata per struttura: e l'unico
    /// sottoschema che fissa `op` a `spatial`.
    fn spatial_variant(node: &serde_json::Value) -> Option<&serde_json::Value> {
        if node
            .pointer("/properties/op/const")
            .and_then(serde_json::Value::as_str)
            == Some("spatial")
        {
            return Some(node);
        }
        match node {
            serde_json::Value::Object(map) => map.values().find_map(spatial_variant),
            serde_json::Value::Array(items) => items.iter().find_map(spatial_variant),
            _ => None,
        }
    }

    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../../../contracts/v2/plan.schema.json"))
            .expect("schema del piano");

    let declared: std::collections::BTreeSet<String> = spatial_variant(&schema)
        .and_then(|variant| variant.pointer("/properties/function/enum"))
        .and_then(serde_json::Value::as_array)
        .expect("enum delle funzioni nel filtro spatial")
        .iter()
        .map(|item| item.as_str().expect("funzione come stringa").to_owned())
        .collect();

    let evaluated: std::collections::BTreeSet<String> = SpatialFunction::ALL
        .iter()
        .filter(|function| function.returns_boolean(2) && **function != SpatialFunction::Relate)
        .map(|function| {
            serde_json::to_value(function)
                .expect("funzione serializzabile")
                .as_str()
                .expect("funzione come stringa")
                .to_owned()
        })
        .collect();

    assert_eq!(declared, evaluated);
}
