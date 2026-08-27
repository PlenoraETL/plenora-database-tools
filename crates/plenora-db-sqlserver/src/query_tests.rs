use super::*;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::query::{
    ColumnRef, CommonTableExpression, QueryProjection, QuerySource, ScalarFunction,
};

fn source(object: &str, alias: &str) -> QuerySource {
    QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("dbo".to_owned()),
            object: object.to_owned(),
        },
        alias: Some(alias.to_owned()),
    }
}

fn column(relation: &str, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    }
}

fn base_query() -> QueryOperation {
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(source("events", "e")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("e", "id"),
            alias: Some("event_id".to_owned()),
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
fn rich_query_is_rendered_instead_of_lowered_to_table_read() {
    let mut query = base_query();
    query.common_table_expressions.push(CommonTableExpression {
        name: "filtered".to_owned(),
        recursive: false,
        query: Box::new(base_query()),
    });
    query.projection.push(QueryProjection {
        expression: QueryExpression::Scalar {
            function: ScalarFunction::Count,
            arguments: vec![column("e", "id")],
        },
        alias: Some("event_count".to_owned()),
    });
    let budget =
        ResourceBudget::new(plenora_database_core::ResourceLimits::default()).expect("budget");
    let rendered = render_query(&query, &ParameterBag::default(), &budget, &BTreeMap::new())
        .expect("rendered");
    assert!(rendered.sql.starts_with("WITH [filtered] AS"));
    assert!(rendered
        .sql
        .contains("COUNT_BIG([e].[id]) AS [event_count]"));
}

#[test]
fn nested_cross_database_source_fails_before_io() {
    let mut inner = base_query();
    inner.source.as_mut().expect("source").object.catalog = Some("other".to_owned());
    let mut query = base_query();
    query.projection[0].expression = QueryExpression::ScalarSubquery {
        query: Box::new(inner),
    };
    assert_eq!(
        validate_query_sources(&query, "dataflow_test")
            .expect_err("cross database")
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn spatial_cte_tracks_physical_source_and_rejects_correlation() {
    let inner = base_query();
    let mut query = base_query();
    query.common_table_expressions = vec![CommonTableExpression {
        name: "filtered".to_owned(),
        recursive: false,
        query: Box::new(inner),
    }];
    query.source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: None,
            object: "filtered".to_owned(),
        },
        alias: Some("scope".to_owned()),
    });
    let mut objects = Vec::new();
    collect_physical_spatial_sources(&query, &BTreeSet::new(), &mut objects)
        .expect("physical CTE sources");
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].schema.as_deref(), Some("dbo"));
    assert_eq!(objects[0].object, "events");

    let relations = local_relation_names(&query);
    assert!(validate_local_spatial_relation(Some("scope"), &relations).is_ok());
    assert_eq!(
        validate_local_spatial_relation(Some("outer"), &relations)
            .expect_err("correlated spatial relation")
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn the_tsql_shape_does_not_depend_on_the_semantics() {
    // `sql_server_spatial_shape` conserva soltanto la forma ricavata da una
    // semantica. Questo test impedisce di usare la scorciatoia se un membro
    // diventasse metodo su un tipo e proprieta sull'altro.
    for function in SpatialFunction::ALL {
        let geometry = plenora_database_sql::sql_server_spatial_method(
            *function,
            Some(SpatialSemantics::Geometry),
        );
        let geography = plenora_database_sql::sql_server_spatial_method(
            *function,
            Some(SpatialSemantics::Geography),
        );
        assert_eq!(
            geometry.map(|(_, shape)| shape),
            geography.map(|(_, shape)| shape),
            "{function:?} ha forme diverse sulle due semantiche"
        );
    }
}

#[test]
fn what_the_provider_offers_and_what_the_renderer_can_write_are_the_same_list() {
    // Due elenchi scritti a mano in due crate diversi, e nessuno li
    // incrociava. Una funzione pubblicata che il renderer non sa scrivere
    // e una promessa che muore in prepare; un nome che il renderer sa
    // scrivere e che nessuno offre e lavoro fatto e non consegnato.
    //
    // La stessa classe che teneva `Relate` fra le verified di `MariaDB` e
    // che ha prodotto la guardia sul catalogo spatial del core.
    // `SpatialFunction` non e `Ord` — non ha un ordine naturale, e imporgliene
    // uno per una guardia sarebbe una modifica al core per comodita di un
    // test. Si confrontano gli elenchi nell'ordine canonico di `ALL`.
    // Il provider offre l'intersezione e l'estensione valida soltanto su
    // `geometry`.
    let offered = |function: &SpatialFunction| {
        VERIFIED_SPATIAL_FUNCTIONS.contains(function)
            || GEOMETRY_ONLY_SPATIAL_FUNCTIONS.contains(function)
    };
    let writable_but_unoffered = SpatialFunction::ALL
        .iter()
        .filter(|function| sql_server_spatial_shape(**function).is_some() && !offered(function))
        .map(|function| format!("{function:?}"))
        .collect::<Vec<_>>();
    let offered_but_unwritable = SpatialFunction::ALL
        .iter()
        .filter(|function| offered(function) && sql_server_spatial_shape(**function).is_none())
        .map(|function| format!("{function:?}"))
        .collect::<Vec<_>>();
    // Le due liste non si sovrappongono, e non e pedanteria: una funzione
    // in entrambe verrebbe pubblicata come garantita su ogni semantica
    // **e** come valida su una sola, che sono due affermazioni che non
    // possono essere vere insieme.
    let in_both = VERIFIED_SPATIAL_FUNCTIONS
        .iter()
        .filter(|function| GEOMETRY_ONLY_SPATIAL_FUNCTIONS.contains(function))
        .map(|function| format!("{function:?}"))
        .collect::<Vec<_>>();
    assert!(
        in_both.is_empty(),
        "pubblicate come garantite ovunque e come valide solo su geometry: {in_both:?}"
    );
    assert!(
        offered_but_unwritable.is_empty(),
        "pubblicate e non scrivibili dal renderer: {offered_but_unwritable:?}"
    );
    assert!(
        writable_but_unoffered.is_empty(),
        "scrivibili dal renderer e non pubblicate: {writable_but_unoffered:?}"
    );
}

#[test]
fn spatial_use_collection_accepts_only_verified_sql_server_signatures() {
    // L'unione, non la sola lista garantita: le sette che valgono su
    // `geometry` hanno una firma quanto le altre, e una firma sbagliata le
    // farebbe morire in prepare sul tipo su cui invece funzionano.
    for function in VERIFIED_SPATIAL_FUNCTIONS
        .iter()
        .chain(GEOMETRY_ONLY_SPATIAL_FUNCTIONS)
    {
        // La forma viene dal contratto, non da un elenco scritto qui.
        // Elencarla a mano faceva coincidere la domanda con la risposta: il
        // test costruiva soltanto le firme che gia conosceva, e il giorno
        // in cui `Reduce` e entrata nella lista la prova ha chiesto una
        // funzione a un argomento a qualcosa che ne vuole due.
        let numeric = if *function == SpatialFunction::PointN {
            "point_index"
        } else {
            "distance"
        };
        let mut arguments = vec![column("e", "shape")];
        if sql_server_binary_spatial_function(*function) {
            arguments.push(QueryExpression::Parameter {
                name: "needle".to_owned(),
            });
        } else if sql_server_numeric_spatial_function(*function) {
            arguments.push(QueryExpression::Parameter {
                name: numeric.to_owned(),
            });
        }
        let mut uses = Vec::new();
        collect_expression_spatial_uses(
            &QueryExpression::Spatial {
                function: *function,
                arguments,
            },
            &mut uses,
        )
        .expect("verified spatial signature");
        assert_eq!(uses.len(), 1);
        let expected = if sql_server_binary_spatial_function(*function) {
            SpatialArgument::Geometry("needle".to_owned())
        } else if *function == SpatialFunction::PointN {
            SpatialArgument::PointIndex(numeric.to_owned())
        } else if sql_server_numeric_spatial_function(*function) {
            SpatialArgument::Distance(numeric.to_owned())
        } else {
            SpatialArgument::None
        };
        assert_eq!(uses[0].argument, expected);
    }

    let mut column_uses = Vec::new();
    collect_expression_spatial_uses(
        &QueryExpression::Spatial {
            function: SpatialFunction::Distance,
            arguments: vec![column("left", "shape"), column("right", "shape")],
        },
        &mut column_uses,
    )
    .expect("verified spatial column signature");
    assert_eq!(
        column_uses[0].argument,
        SpatialArgument::GeometryColumn(SpatialColumnRef {
            relation: Some("right".to_owned()),
            field: "shape".to_owned(),
        })
    );

    for expression in [
        // L'esempio ha cambiato funzione due volte in un pomeriggio —
        // `MakeValid`, poi `Centroid` — e tutte e due si sono aperte. La
        // morale non e che sceglievo male: e che una prova che dimostra un
        // rifiuto va ancorata a un fatto **del prodotto**, non a una
        // decisione di questo repository, se non si vuole che invecchi ogni
        // volta che il repository decide qualcosa.
        //
        // `Reverse` lo e: SQL Server non ha `STReverse`, su nessuna delle
        // due semantiche, e nessun lavoro qui dentro lo apre.
        QueryExpression::Spatial {
            function: SpatialFunction::Reverse,
            arguments: vec![column("e", "shape")],
        },
        QueryExpression::Spatial {
            function: SpatialFunction::IsValid,
            arguments: vec![
                column("e", "shape"),
                QueryExpression::Parameter {
                    name: "flags".to_owned(),
                },
            ],
        },
        QueryExpression::Spatial {
            function: SpatialFunction::Transform,
            arguments: vec![
                column("e", "shape"),
                QueryExpression::Parameter {
                    name: "source_srid".to_owned(),
                },
                QueryExpression::Parameter {
                    name: "target_srid".to_owned(),
                },
            ],
        },
    ] {
        assert_eq!(
            collect_expression_spatial_uses(&expression, &mut Vec::new())
                .expect_err("unverified spatial signature")
                .category,
            ErrorCategory::Unsupported
        );
    }
}

#[test]
fn numeric_spatial_arguments_fail_before_database_io() {
    let budget =
        ResourceBudget::new(plenora_database_core::ResourceLimits::default()).expect("budget");
    for (function, value) in [
        (SpatialFunction::PointN, ParameterValue::I32(0)),
        (SpatialFunction::Buffer, ParameterValue::F64(f64::NAN)),
    ] {
        let mut query = base_query();
        query.projection[0] = QueryProjection {
            expression: QueryExpression::Spatial {
                function,
                arguments: vec![
                    column("e", "shape"),
                    QueryExpression::Parameter {
                        name: "value".to_owned(),
                    },
                ],
            },
            alias: Some("result".to_owned()),
        };
        let parameters = ParameterBag::new(BTreeMap::from([("value".to_owned(), value)]));
        assert_eq!(
            render_query(&query, &parameters, &budget, &BTreeMap::new())
                .expect_err("invalid numeric spatial argument")
                .category,
            ErrorCategory::InvalidPlan
        );
    }
}
