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

fn base(object: &str, field: &str) -> BaseColumn {
    BaseColumn {
        schema: Some("s".to_owned()),
        object: object.to_owned(),
        column: field.to_owned(),
    }
}

/// Il giudizio con un catalogo in cui la sola colonna geometrica e `s.t.g`.
fn judged(operation: &QueryOperation) -> Result<()> {
    let operands = scalar_operands(MYSQL, operation)?;
    operands.judge(
        MYSQL,
        |column| *column == base("t", "g"),
        |name| name == "g",
    )
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
                "query": query(&json!({"projection": [{"expression": g, "alias": null}]}))
            }}),
        ),
        (
            "subquery scalare",
            json!({"filter": compare(
                &json!({"kind": "scalar_subquery", "query": query(&json!({"projection": [{"expression": g, "alias": null}]}))}),
                &bind()
            )}),
        ),
        (
            "nella subquery EXISTS",
            json!({"filter": {"kind": "exists", "query": query(&json!({"filter": compare(&column(Some("t"), "g"), &bind())}))}}),
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
