use super::*;
use crate::plan::SortDirection;
use serde_json::{json, Value};

const MYSQL: ProviderKind = ProviderKind::Mysql;

fn column(relation: Option<&str>, field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": relation, "field": field}})
}

fn bind() -> Value {
    json!({"kind": "typed_parameter", "name": "p", "parameter_type": "binary"})
}

fn compare(left: &Value, right: &Value) -> Value {
    json!({"kind": "compare", "left": left, "operator": "eq", "right": right})
}

fn source(object: &str, alias: Option<&str>) -> Value {
    json!({"object": {"catalog": null, "schema": "s", "object": object}, "alias": alias})
}

/// Una query su `s.t` (alias `t`) con `id` in projection e `extra` sopra.
fn query(extra: &Value) -> QueryOperation {
    let mut operation = json!({
        "source": source("t", Some("t")),
        "projection": [{"expression": column(None, "id"), "alias": null}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("oggetto") {
        operation[key] = value.clone();
    }
    serde_json::from_value(operation).expect("query")
}

/// Il catalogo di prova: `s.t(id, g geometrica, b)`, `s.u(id, g scalare)`,
/// `s.plain(id, name)` senza geometrie.
fn catalog(provider: ProviderKind) -> OperandCatalog {
    let mut catalog = OperandCatalog::new(provider);
    let known = |columns: &[(&str, bool)]| {
        RelationColumns::Known(
            columns
                .iter()
                .map(|(name, geometric)| ((*name).to_owned(), *geometric))
                .collect(),
        )
    };
    catalog.insert(
        Some("s".to_owned()),
        "t".to_owned(),
        known(&[("id", false), ("g", true), ("b", false)]),
    );
    catalog.insert(
        Some("s".to_owned()),
        "u".to_owned(),
        known(&[("id", false), ("g", false)]),
    );
    catalog.insert(
        Some("s".to_owned()),
        "plain".to_owned(),
        known(&[("id", false), ("name", false)]),
    );
    catalog
}

/// Il giudizio con il catalogo di prova.
fn judged_as(provider: ProviderKind, operation: &QueryOperation) -> Result<()> {
    scalar_operands(provider, operation)?.judge(&catalog(provider))
}

fn judged(operation: &QueryOperation) -> Result<()> {
    judged_as(MYSQL, operation)
}

fn assert_refused(outcome: Result<()>, form: &str) {
    let error = outcome.expect_err(form);
    assert_eq!(error.category, crate::ErrorCategory::Unsupported, "{form}");
    assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{form}");
}

/// Ogni posizione di operando, con la colonna geometrica: si rifiuta.
#[test]
fn every_scalar_position_of_a_geometry_column_is_refused() {
    let g = column(Some("t"), "g");
    let cases = [
        ("=", json!({"filter": compare(&g, &bind())})),
        (
            "= senza qualificazione",
            json!({"filter": compare(&column(None, "g"), &bind())}),
        ),
        (
            "IN",
            json!({"filter": {"kind": "in_list", "expression": g, "values": [bind()]}}),
        ),
        (
            "valore di IN",
            json!({"filter": {"kind": "in_list", "expression": bind(), "values": [g]}}),
        ),
        (
            "BETWEEN",
            json!({"filter": {"kind": "between", "expression": g, "lower": bind(), "upper": bind()}}),
        ),
        (
            "LIKE su CAST",
            json!({"filter": {
                "kind": "like",
                "expression": {"kind": "cast", "expression": g, "target_type": "string"},
                "pattern": bind()
            }}),
        ),
        (
            "COALESCE",
            json!({"filter": compare(
                &json!({"kind": "scalar", "function": "coalesce", "arguments": [g, bind()]}),
                &bind()
            )}),
        ),
        (
            "CASE",
            json!({"filter": compare(
                &json!({"kind": "case", "branches": [{"when": {"kind": "is_null", "expression": g, "negated": false}, "then": g}]}),
                &bind()
            )}),
        ),
        (
            "aritmetica",
            json!({"filter": compare(
                &json!({"kind": "arithmetic", "left": g, "operator": "add", "right": bind()}),
                &bind()
            )}),
        ),
    ];
    for (form, extra) in cases {
        let operation = query(&extra);
        assert_refused(judged(&operation), form);
    }
}

/// Una subquery su `s.t` (alias `i`, distinto da quello esterno, colonne
/// qualificate) con `extra` sopra.
fn inner(extra: &Value) -> QueryOperation {
    let mut operation = serde_json::to_value(query(extra)).expect("json");
    operation["source"] = source("t", Some("i"));
    if extra.get("projection").is_none() {
        operation["projection"] = json!([{"expression": column(Some("i"), "id"), "alias": null}]);
    }
    serde_json::from_value(operation).expect("query")
}

/// Ogni clausola che ordina, raggruppa o confronta righe, con la colonna
/// geometrica: si rifiuta.
#[test]
fn every_ordering_and_grouping_position_of_a_geometry_column_is_refused() {
    let g = column(Some("t"), "g");
    let cases = [
        (
            "ORDER BY",
            json!({"order_by": [{"expression": g, "direction": "asc"}]}),
        ),
        ("GROUP BY", json!({"group_by": [g]})),
        (
            "ORDER BY per alias",
            json!({
                "projection": [{"expression": g, "alias": "a"}],
                "order_by": [{"expression": column(None, "a"), "direction": "asc"}]
            }),
        ),
        (
            "HAVING per alias",
            json!({
                "projection": [{"expression": g, "alias": "a"}],
                "having": compare(&column(None, "a"), &bind())
            }),
        ),
        (
            "DISTINCT",
            json!({"projection": [{"expression": g, "alias": null}], "distinct": true}),
        ),
        (
            "MAX",
            json!({"projection": [{"expression": {"kind": "scalar", "function": "maximum", "arguments": [g]}, "alias": "m"}]}),
        ),
        (
            "PARTITION BY",
            json!({"projection": [{"expression": {
                "kind": "window", "function": "row_number", "arguments": [],
                "partition_by": [g], "order_by": [], "frame": null
            }, "alias": "n"}]}),
        ),
        (
            "IN subquery",
            json!({"filter": {
                "kind": "in_subquery",
                "expression": bind(),
                "query": inner(&json!({"projection": [{"expression": column(Some("i"), "g"), "alias": null}]}))
            }}),
        ),
        (
            "subquery scalare",
            json!({"filter": compare(
                &json!({"kind": "scalar_subquery", "query": inner(&json!({"projection": [{"expression": column(Some("i"), "g"), "alias": null}]}))}),
                &bind()
            )}),
        ),
        (
            "nella subquery EXISTS",
            json!({"filter": {"kind": "exists", "query": inner(&json!({"filter": compare(&column(Some("i"), "g"), &bind())}))}}),
        ),
        (
            "correlata nella subquery EXISTS",
            json!({"filter": {"kind": "exists", "query": inner(&json!({"filter": compare(&column(Some("t"), "g"), &bind())}))}}),
        ),
        (
            "UNION",
            json!({
                "projection": [{"expression": g, "alias": null}],
                "set_operations": [{"operator": "union", "all": false, "query": query(&json!({}))}]
            }),
        ),
    ];
    for (form, extra) in cases {
        let operation = query(&extra);
        assert_refused(judged(&operation), form);
    }
}

/// Una colonna di sorgente derivata o di CTE si segue fino alla tabella,
/// anche rinominata, attraverso `SELECT *` e ogni ramo di set operation.
#[test]
fn derived_and_common_table_columns_are_followed_to_the_table() {
    let derived = |inner: Value| {
        json!({
            "source": null,
            "derived_source": {"query": inner, "alias": "d"},
        })
    };
    let renamed =
        query(&json!({"projection": [{"expression": column(Some("t"), "g"), "alias": "x"}]}));
    let star = query(
        &json!({"projection": [{"expression": {"kind": "wildcard", "relation": null}, "alias": null}]}),
    );
    let union = query(&json!({
        "projection": [{"expression": column(Some("t"), "id"), "alias": "x"}],
        "set_operations": [{
            "operator": "union", "all": true,
            "query": query(&json!({"projection": [{"expression": column(Some("t"), "g"), "alias": "x"}]}))
        }]
    }));
    for (form, inner, field) in [
        ("alias", &renamed, "x"),
        ("SELECT *", &star, "g"),
        ("ramo di UNION ALL", &union, "x"),
    ] {
        for relation in [Some("d"), None] {
            let mut extra = derived(serde_json::to_value(inner).expect("json"));
            extra["filter"] = compare(&column(relation, field), &bind());
            assert_refused(judged(&query(&extra)), form);
        }
    }
    let cte = query(&json!({
        "common_table_expressions": [{"name": "c", "query": renamed}],
        "source": {"object": {"catalog": null, "schema": null, "object": "c"}, "alias": null},
        "filter": compare(&column(Some("c"), "x"), &bind())
    }));
    assert_refused(judged(&cte), "CTE");
}

/// Le funzioni spatial che rendono una geometria e i valori spatial si
/// rifiutano senza catalogo.
#[test]
fn produced_geometries_are_refused_without_a_catalog() {
    let centroid =
        json!({"kind": "spatial", "function": "centroid", "arguments": [column(Some("t"), "g")]});
    for (form, extra) in [
        ("=", json!({"filter": compare(&centroid, &bind())})),
        (
            "ORDER BY",
            json!({"order_by": [{"expression": centroid, "direction": "asc"}]}),
        ),
    ] {
        let error = scalar_operands(MYSQL, &query(&extra)).expect_err(form);
        assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{form}");
    }
}

/// Nessun falso rifiuto: colonne scalari, `IS NULL`, `COUNT`, gli argomenti
/// dei nodi spatial, le funzioni spatial che rendono uno scalare, `UNION ALL`.
#[test]
fn scalar_columns_and_spatial_arguments_are_admitted() {
    let g = column(Some("t"), "g");
    let cases = [
        (
            "binaria",
            json!({"filter": compare(&column(Some("t"), "b"), &bind())}),
        ),
        (
            "IS NULL",
            json!({"filter": {"kind": "is_null", "expression": g, "negated": true}}),
        ),
        (
            "COUNT",
            json!({"projection": [{"expression": {"kind": "scalar", "function": "count", "arguments": [g]}, "alias": "n"}]}),
        ),
        (
            "ST_Intersects",
            json!({"filter": {"kind": "spatial", "function": "intersects", "arguments": [g, g]}}),
        ),
        (
            "ST_Area confrontata",
            json!({"filter": compare(&json!({"kind": "spatial", "function": "area", "arguments": [g]}), &bind())}),
        ),
        (
            "projection",
            json!({"projection": [{"expression": g, "alias": null}]}),
        ),
        (
            "UNION ALL",
            json!({
                "projection": [{"expression": g, "alias": null}],
                "set_operations": [{"operator": "union", "all": true, "query": query(&json!({}))}]
            }),
        ),
        (
            "alias che si nominano a vicenda",
            json!({
                "projection": [
                    {"expression": column(None, "b"), "alias": "a"},
                    {"expression": column(None, "a"), "alias": "b"}
                ],
                "order_by": [{"expression": column(None, "a"), "direction": "asc"}]
            }),
        ),
        (
            "colonna omonima di un'altra tabella, qualificata",
            json!({
                "joins": [{"kind": "inner", "source": source("u", Some("u")), "on": null}],
                "filter": compare(&column(Some("u"), "g"), &bind())
            }),
        ),
    ];
    for (form, extra) in cases {
        judged(&query(&extra)).unwrap_or_else(|error| panic!("{form}: {error:?}"));
    }
}

/// Le relazioni da leggere sono le tabelle di base, non le CTE.
#[test]
fn relations_name_base_tables_only() {
    let operation = query(&json!({
        "common_table_expressions": [{"name": "c", "query": query(&json!({}))}],
        "joins": [{"kind": "cross", "source": {"object": {"catalog": null, "schema": null, "object": "c"}, "alias": null}, "on": null}],
        "filter": compare(&column(Some("c"), "id"), &bind())
    }));
    let operands = scalar_operands(MYSQL, &operation).expect("operandi");
    assert_eq!(
        operands.relations(),
        [(Some("s".to_owned()), "t".to_owned())]
    );
}

#[test]
fn read_filters_refuse_geometry_fields_under_scalar_operators() {
    let geometric = |field: &str| field == "g";
    for filter in [
        FilterExpression::Eq {
            field: "g".to_owned(),
            parameter: "p".to_owned(),
        },
        FilterExpression::Or {
            args: vec![FilterExpression::In {
                field: "g".to_owned(),
                parameters: vec!["p".to_owned()],
            }],
        },
    ] {
        let error = ensure_scalar_filter_fields(MYSQL, Some(&filter), &[], geometric)
            .expect_err("campo geometrico");
        assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND);
    }
    let order = [OrderBy {
        field: "g".to_owned(),
        direction: SortDirection::Asc,
    }];
    assert!(ensure_scalar_filter_fields(MYSQL, None, &order, geometric).is_err());
    for filter in [
        FilterExpression::IsNull {
            field: "g".to_owned(),
        },
        FilterExpression::Eq {
            field: "b".to_owned(),
            parameter: "p".to_owned(),
        },
    ] {
        ensure_scalar_filter_fields(MYSQL, Some(&filter), &[], geometric).expect("ammesso");
    }
}

fn assert_ambiguous(outcome: Result<()>, form: &str) {
    let error = outcome.expect_err(form);
    assert_eq!(error.category, crate::ErrorCategory::Unsupported, "{form}");
    assert_eq!(error.message, AMBIGUOUS_NAME_SHAPE, "{form}");
}

fn assert_unresolved(outcome: Result<()>, form: &str) {
    let error = outcome.expect_err(form);
    assert_eq!(error.category, crate::ErrorCategory::Unsupported, "{form}");
    assert_eq!(error.message, GEOMETRY_OPERAND_UNRESOLVED, "{form}");
}

fn string_bind() -> Value {
    json!({"kind": "typed_parameter", "name": "p", "parameter_type": "string"})
}

fn cast(expression: &Value) -> Value {
    json!({"kind": "cast", "expression": expression, "target_type": "string"})
}

fn like(expression: &Value) -> Value {
    json!({"kind": "like", "expression": expression, "pattern": string_bind()})
}

fn order(expression: &Value) -> Value {
    json!([{"expression": expression, "direction": "desc"}])
}

fn item(expression: &Value, alias: Option<&str>) -> Value {
    json!({"expression": expression, "alias": alias})
}

fn star() -> Value {
    json!({"kind": "wildcard", "relation": null})
}

/// `FROM (inner) d` con `extra` sopra.
fn over_derived(inner: &QueryOperation, extra: &Value) -> QueryOperation {
    let mut operation = json!({
        "source": null,
        "derived_source": {"query": inner, "alias": "d"},
        "projection": [item(&column(Some("d"), "id"), None)],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("oggetto") {
        operation[key] = value.clone();
    }
    serde_json::from_value(operation).expect("query")
}

/// Le sei forme della nona rilettura (25/10), ognuna ammessa prima di questa
/// regola: ORDER BY di una set operation per nome di uscita, nomi generati
/// dal server in una sorgente derivata, `*` in posizione di operando, nomi
/// piegati di SQL Server e `MySQL`, sinonimi non risolti, colonne il cui
/// tipo contiene una geometria.
#[test]
#[allow(clippy::too_many_lines)]
fn the_six_unresolved_shapes_are_refused() {
    let g = column(Some("t"), "g");
    let output = json!({"kind": "spatial_output", "expression": g, "semantics": "geometry"});
    // 1. ORDER BY k su UNION ALL: k e la geometria del secondo ramo.
    let union = query(&json!({
        "projection": [item(&bind(), Some("k"))],
        "set_operations": [{"operator": "union", "all": true,
            "query": query(&json!({"projection": [item(&output, Some("k"))]}))}],
        "order_by": order(&column(None, "k"))
    }));
    assert_refused(judged(&union), "ORDER BY di UNION ALL per nome");
    let centroid = json!({"kind": "spatial", "function": "centroid", "arguments": [g]});
    let generated = |expression: &Value| {
        query(
            &json!({"projection": [item(&column(Some("t"), "id"), None), item(expression, None)]}),
        )
    };
    // 2. Nomi generati dal server.
    for (form, inner, field) in [
        ("st_centroid", generated(&centroid), "st_centroid"),
        ("CAST senza alias", generated(&cast(&g)), "g"),
        (
            "coalesce",
            generated(&json!({"kind": "scalar", "function": "coalesce", "arguments": [g, bind()]})),
            "coalesce",
        ),
        (
            "case",
            generated(
                &json!({"kind": "case", "branches": [{"when": {"kind": "is_null", "expression": g, "negated": true}, "then": g}]}),
            ),
            "case",
        ),
    ] {
        for extra in [
            json!({"order_by": order(&column(Some("d"), field))}),
            json!({"group_by": [column(Some("d"), field)]}),
            json!({"filter": like(&cast(&column(Some("d"), field)))}),
        ] {
            assert_refused(judged(&over_derived(&inner, &extra)), form);
        }
    }
    // 3. DISTINCT * di una sorgente derivata con una geometria.
    let text = query(&json!({"projection": [item(&cast(&g), Some("c"))]}));
    let distinct = over_derived(
        &text,
        &json!({"projection": [item(&star(), None)], "distinct": true}),
    );
    assert_refused(judged(&distinct), "DISTINCT *");
    let distinct_table = query(&json!({"projection": [item(&star(), None)], "distinct": true}));
    assert_refused(judged(&distinct_table), "DISTINCT * della tabella");
    // 4. Nomi che differiscono soltanto per le maiuscole: `MySQL` e
    // `MariaDB` non le distinguono nei nomi di colonna (la colonna e la
    // geometria); SQL Server dipende dalla collation e `PostgreSQL` le
    // distingue: la forma si rifiuta (regola N). Un qualificatore che
    // incontra la relazione soltanto senza maiuscole si rifiuta ovunque.
    for provider in [
        ProviderKind::Sqlserver,
        MYSQL,
        ProviderKind::Mariadb,
        ProviderKind::Postgres,
    ] {
        let folded = matches!(provider, ProviderKind::Mysql | ProviderKind::Mariadb);
        let expected = |outcome: Result<()>, form: &str| {
            if folded {
                assert_refused(outcome, form);
            } else {
                assert_ambiguous(outcome, form);
            }
        };
        let upper = query(&json!({"filter": like(&cast(&column(Some("t"), "G")))}));
        expected(judged_as(provider, &upper), "colonna G");
        let alias = over_derived(
            &query(&json!({"projection": [item(&g, Some("X"))]})),
            &json!({"filter": compare(&column(Some("d"), "x"), &bind())}),
        );
        expected(judged_as(provider, &alias), "alias X");
        let relation = query(&json!({"filter": compare(&column(Some("T"), "g"), &bind())}));
        assert_ambiguous(judged_as(provider, &relation), "relazione T");
    }
    // 5. Una relazione di cui il catalogo non legge le colonne (sinonimo).
    let synonym: QueryOperation = serde_json::from_value(json!({
        "source": source("syn", Some("s")),
        "projection": [item(&column(Some("s"), "id"), None)],
        "filter": like(&cast(&column(Some("s"), "g"))),
        "having": null,
        "row_limit": null
    }))
    .expect("query");
    assert_unresolved(judged(&synonym), "sinonimo");
    // 6. Una colonna il cui tipo contiene una geometria (array, composito):
    // il catalogo del provider la dice geometrica.
    let mut array = catalog(ProviderKind::Postgres);
    array.insert(
        Some("s".to_owned()),
        "a".to_owned(),
        RelationColumns::Known(vec![("id".to_owned(), false), ("ga".to_owned(), true)]),
    );
    let ordered: QueryOperation = serde_json::from_value(json!({
        "source": source("a", Some("a")),
        "projection": [item(&column(Some("a"), "id"), None)],
        "filter": null, "having": null, "row_limit": null,
        "order_by": order(&column(Some("a"), "ga"))
    }))
    .expect("query");
    let error = scalar_operands(ProviderKind::Postgres, &ordered)
        .expect("operandi")
        .judge(&array)
        .expect_err("geometry[]");
    assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND);
}

/// `PostgreSQL` quota ogni identificatore: `G` non e `g`, e un nome che
/// incontra la colonna soltanto senza maiuscole si rifiuta (regola N). Un
/// nome che non si risolve in un piano con geometrie si rifiuta.
#[test]
fn postgres_names_are_exact_and_unresolved_names_are_refused() {
    let upper = query(&json!({"filter": like(&cast(&column(Some("t"), "G")))}));
    assert_ambiguous(judged_as(ProviderKind::Postgres, &upper), "G su PostgreSQL");
    let absent = query(&json!({"filter": like(&cast(&column(Some("t"), "q")))}));
    assert_unresolved(
        judged_as(ProviderKind::Postgres, &absent),
        "q su PostgreSQL",
    );
    for (form, extra) in [
        (
            "colonna assente",
            json!({"order_by": order(&column(None, "missing"))}),
        ),
        (
            "relazione assente",
            json!({"order_by": order(&column(Some("x"), "id"))}),
        ),
        (
            "riga intera",
            json!({"order_by": order(&column(None, "t"))}),
        ),
    ] {
        assert_unresolved(judged(&query(&extra)), form);
    }
}

/// Nessun falso rifiuto: le colonne scalari di una tabella con geometrie, e
/// ogni forma su una tabella senza geometrie, anche quelle che non si
/// risolvono.
#[test]
fn scalar_references_and_plans_without_geometry_are_admitted() {
    let g = column(Some("t"), "g");
    let id = column(Some("t"), "id");
    let area = json!({"kind": "spatial", "function": "area", "arguments": [g]});
    let derived = query(&json!({"projection": [item(&id, None), item(&area, None)]}));
    let union = query(&json!({
        "projection": [item(&id, Some("k"))],
        "set_operations": [{"operator": "union", "all": true,
            "query": query(&json!({"projection": [item(&column(Some("t"), "b"), Some("k"))]}))}],
        "order_by": order(&column(None, "k"))
    }));
    let scalar_star =
        query(&json!({"projection": [item(&id, None), item(&column(Some("t"), "b"), None)]}));
    let admitted = [
        ("ORDER BY t.id", query(&json!({"order_by": order(&id)}))),
        (
            "GROUP BY id",
            query(&json!({"group_by": [column(None, "id")]})),
        ),
        (
            "ORDER BY alias",
            query(
                &json!({"projection": [item(&area, Some("a"))], "order_by": order(&column(None, "a"))}),
            ),
        ),
        ("DISTINCT t.id", query(&json!({"distinct": true}))),
        ("UNION ALL scalare per nome", union),
        (
            "colonna derivata per nome",
            over_derived(
                &derived,
                &json!({"order_by": order(&column(Some("d"), "id"))}),
            ),
        ),
        (
            "nome generato di uno scalare",
            over_derived(
                &derived,
                &json!({"order_by": order(&column(Some("d"), "st_area"))}),
            ),
        ),
        (
            "DISTINCT * di colonne scalari",
            over_derived(
                &scalar_star,
                &json!({"projection": [item(&star(), None)], "distinct": true}),
            ),
        ),
        (
            "* della tabella nella projection",
            query(&json!({"projection": [item(&star(), None)], "order_by": order(&id)})),
        ),
    ];
    for (form, operation) in admitted {
        judged(&operation).unwrap_or_else(|error| panic!("{form}: {error:?}"));
    }
    let plain = |extra: &Value| {
        let mut operation = serde_json::to_value(query(extra)).expect("json");
        operation["source"] = source("plain", Some("t"));
        serde_json::from_value::<QueryOperation>(operation).expect("query")
    };
    for (form, extra) in [
        (
            "ORDER BY colonna",
            json!({"order_by": order(&column(Some("t"), "name"))}),
        ),
        (
            "DISTINCT *",
            json!({"projection": [item(&star(), None)], "distinct": true}),
        ),
        (
            "ORDER BY non risolto",
            json!({"order_by": order(&column(None, "missing"))}),
        ),
        (
            "GROUP BY NAME piegato",
            json!({"group_by": [column(None, "NAME")]}),
        ),
    ] {
        judged(&plain(&extra)).unwrap_or_else(|error| panic!("{form}: {error:?}"));
    }
}

/// Senza catalogo la visita rifiuta soltanto cio che sa geometrico da se,
/// anche attraverso un nome generato dal server.
#[test]
fn generated_names_of_produced_geometries_are_refused_without_a_catalog() {
    let centroid =
        json!({"kind": "spatial", "function": "centroid", "arguments": [column(Some("t"), "g")]});
    let inner = query(&json!({"projection": [item(&centroid, None)]}));
    let operation = over_derived(
        &inner,
        &json!({"order_by": order(&column(Some("d"), "st_centroid"))}),
    );
    let error = scalar_operands(MYSQL, &operation).expect_err("st_centroid");
    assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND);
}

/// Seconda lettura della regola N (25/10), forme silenziose del primo
/// disegno, ognuna ammessa prima della correzione:
///
/// 1. la stessa colonna di uscita nominata qualificata (`t.b`, per la
///    colonna della projection) e senza qualificatore (`b`, per nome) in
///    una set operation: la seconda si dava per gia seguita;
/// 2. un corpo di CTE che nomina se stessa senza `recursive`, o una CTE
///    successiva: il server la lega alla tabella o la rifiuta;
/// 3. la visibilita di una CTE presa dal blocco che la nomina e non da
///    dove e definita: una CTE omonima di una sorgente derivata nascondeva
///    la tabella;
/// 4. una relazione di un altro catalogo letta come quella del database
///    corrente.
#[test]
fn second_reading_shapes_are_refused() {
    let g = column(Some("t"), "g");
    let b = column(Some("t"), "b");
    // 1.
    let union = query(&json!({
        "projection": [item(&g, Some("b")), item(&b, Some("c"))],
        "filter": compare(&b, &bind()),
        "set_operations": [{"operator": "union", "all": true,
            "query": query(&json!({"projection": [item(&g, None), item(&b, None)]}))}],
        "order_by": order(&column(None, "b"))
    }));
    assert_refused(judged(&union), "t.b e b nella stessa set operation");
    // 2.
    let cte = |name: &str, recursive: bool, body: &QueryOperation| json!({"name": name, "recursive": recursive, "query": body});
    let plain_source = |object: &str, alias: &str| json!({"object": {"catalog": null, "schema": null, "object": object}, "alias": alias});
    let body = |object: &str| -> QueryOperation {
        serde_json::from_value(json!({
            "source": plain_source(object, "x"),
            "projection": [item(&column(Some("x"), "g"), Some("g"))],
            "filter": null, "having": null, "row_limit": null
        }))
        .expect("corpo")
    };
    let main = |ctes: Value| -> QueryOperation {
        serde_json::from_value(json!({
            "common_table_expressions": ctes,
            "source": plain_source("a", "m"),
            "projection": [item(&column(Some("m"), "g"), Some("g"))],
            "filter": null, "having": null, "row_limit": null
        }))
        .expect("query")
    };
    let own = main(json!([cte("a", false, &body("a"))]));
    assert_ambiguous(judged(&own), "CTE che nomina se stessa senza recursive");
    let forward = main(json!([
        cte("a", false, &body("b")),
        cte("b", false, &body("t"))
    ]));
    assert_ambiguous(judged(&forward), "CTE che nomina la successiva");
    let earlier = main(json!([
        cte("b", false, &body("t")),
        cte("a", false, &body("b"))
    ]));
    judged(&earlier).expect("una CTE che nomina la precedente resta");
    // 3. `a` legge la tabella `t` (senza schema); una derivata definisce
    // la propria CTE `t` e seleziona da `a`.
    let mut with_base_t = catalog(MYSQL);
    with_base_t.insert(
        None,
        "t".to_owned(),
        RelationColumns::Known(vec![("id".to_owned(), false), ("g".to_owned(), true)]),
    );
    let inner_t: QueryOperation = serde_json::from_value(json!({
        "projection": [item(&bind(), Some("g"))],
        "filter": null, "having": null, "row_limit": null
    }))
    .expect("t interna");
    let leak: QueryOperation = serde_json::from_value(json!({
        "common_table_expressions": [cte("a", false, &body("t"))],
        "derived_source": {"alias": "d", "query": {
            "common_table_expressions": [cte("t", false, &inner_t)],
            "source": plain_source("a", "y"),
            "projection": [item(&column(Some("y"), "g"), Some("g"))],
            "filter": null, "having": null, "row_limit": null
        }},
        "projection": [item(&column(Some("d"), "g"), Some("w"))],
        "order_by": order(&column(Some("d"), "g")),
        "filter": null, "having": null, "row_limit": null
    }))
    .expect("query");
    let error = scalar_operands(MYSQL, &leak)
        .expect("operandi")
        .judge(&with_base_t)
        .expect_err("la CTE vede la tabella t dove e definita");
    assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND);
    // 4.
    let foreign: QueryOperation = serde_json::from_value(json!({
        "source": {"object": {"catalog": "altro", "schema": "s", "object": "plain"}, "alias": "t"},
        "projection": [item(&column(Some("t"), "id"), None)],
        "filter": like(&cast(&column(Some("t"), "name"))),
        "having": null, "row_limit": null
    }))
    .expect("query");
    assert_unresolved(judged(&foreign), "relazione di un altro catalogo");
}

/// Regola B: la tabella della verita della regola condivisa dai cataloghi.
#[test]
fn a_binary_column_is_geometric_only_in_a_derived_relation() {
    use ColumnCarrier::{Binary, Geometry, Scalar};
    use RelationOrigin::{Base, Derived};
    for (origin, carrier, expected) in [
        (Base, Geometry, true),
        (Derived, Geometry, true),
        (Base, Binary, false),
        (Derived, Binary, true),
        (Base, Scalar, false),
        (Derived, Scalar, false),
    ] {
        assert_eq!(
            catalog_column_is_geometric(origin, carrier),
            expected,
            "{origin:?} {carrier:?}"
        );
    }
}
