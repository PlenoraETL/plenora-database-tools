use super::*;
use crate::plan::{FilterExpression, ProviderKind};
use crate::provider::ParameterBag;
use crate::relational::QueryOperation;
use crate::spatial_operand::{
    ensure_scalar_filter_fields, scalar_operands, OperandCatalog, OperandPlan, RelationColumns,
};
use crate::spatial_probe::GEOMETRY_SCALAR_OPERAND;
use serde_json::{json, Value};

const PRODOTTI: [ProviderKind; 6] = [
    ProviderKind::Postgres,
    ProviderKind::Mysql,
    ProviderKind::Mariadb,
    ProviderKind::Sqlserver,
    ProviderKind::Oracle,
    ProviderKind::Db2,
];

/// La tabella delle famiglie dei valori legati, variante per variante.
#[test]
fn ogni_valore_legato_ha_la_famiglia_della_tabella() {
    use OperandFamily::{Binary, Geometry, Numeric, Other, Temporal, Text};
    let geometry = crate::geometry::SpatialSemantics::Geometry;
    for (value, family) in [
        (ParameterValue::Bool(true), Some(Numeric)),
        (ParameterValue::I32(1), Some(Numeric)),
        (ParameterValue::I64(1), Some(Numeric)),
        (ParameterValue::F64(1.0), Some(Numeric)),
        (ParameterValue::Decimal("1.5".to_owned()), Some(Numeric)),
        (ParameterValue::String("a".to_owned()), Some(Text)),
        (
            ParameterValue::Enum {
                type_name: "t".to_owned(),
                label: "a".to_owned(),
            },
            Some(Text),
        ),
        (ParameterValue::Bytes(vec![1]), Some(Binary)),
        (
            ParameterValue::Date("2026-10-25".to_owned()),
            Some(Temporal),
        ),
        (
            ParameterValue::Timestamp("2026-10-25T00:00:00".to_owned()),
            Some(Temporal),
        ),
        (
            ParameterValue::TimestampTz("2026-10-25T00:00:00Z".to_owned()),
            Some(Temporal),
        ),
        (
            ParameterValue::Uuid("00000000-0000-0000-0000-000000000000".to_owned()),
            Some(Other),
        ),
        (ParameterValue::Json(json!({})), Some(Other)),
        (
            ParameterValue::Wkb {
                bytes: vec![1],
                srid: Some(4326),
                dimensions: crate::geometry::Dimensions::Xy,
                semantics: geometry,
            },
            Some(Geometry),
        ),
        (
            ParameterValue::Null {
                type_name: "text".to_owned(),
            },
            None,
        ),
    ] {
        assert_eq!(parameter_family(&value), family, "{value:?}");
    }
    for (declared, family) in [
        (QueryParameterType::Boolean, Numeric),
        (QueryParameterType::Integer, Numeric),
        (QueryParameterType::BigInteger, Numeric),
        (QueryParameterType::Float, Numeric),
        (QueryParameterType::Decimal, Numeric),
        (QueryParameterType::String, Text),
        (QueryParameterType::Binary, Binary),
        (QueryParameterType::Date, Temporal),
        (QueryParameterType::Timestamp, Temporal),
        (QueryParameterType::TimestampTz, Temporal),
        (QueryParameterType::Uuid, Other),
        (QueryParameterType::Json, Other),
    ] {
        assert_eq!(query_parameter_family(declared), family, "{declared:?}");
    }
}

/// I valori posizionali si legano ai nomi soltanto se sono tanti quanti; un
/// nome legato due volte con famiglie diverse non ha famiglia.
#[test]
fn i_valori_posizionali_si_legano_ai_nomi() {
    let names = ["a".to_owned(), "b".to_owned(), "a".to_owned()];
    let values = [
        ParameterValue::I64(1),
        ParameterValue::String("x".to_owned()),
        ParameterValue::I32(2),
    ];
    let families = ParameterFamilies::from_positional(&names, &values);
    assert_eq!(families.get("a"), Some(OperandFamily::Numeric));
    assert_eq!(families.get("b"), Some(OperandFamily::Text));
    let conflicting = ParameterFamilies::from_positional(
        &names,
        &[
            ParameterValue::I64(1),
            ParameterValue::String("x".to_owned()),
            ParameterValue::Bytes(vec![1]),
        ],
    );
    assert_eq!(conflicting.get("a"), None);
    assert_eq!(conflicting.get("b"), Some(OperandFamily::Text));
    let short = ParameterFamilies::from_positional(&names, &values[..2]);
    assert_eq!(short.get("b"), None);
}

/// Il catalogo di prova: `s.f` con una colonna per famiglia, una binaria
/// memorizzata (`b`), una binaria generata (`w`), e la vista `s.v` con una
/// colonna testo (`x`, la forma testuale di una geometria) e una binaria
/// (`vb`).
fn catalogo(prodotto: ProviderKind) -> OperandCatalog {
    use ColumnOrigin::{Derived, Stored};
    use OperandFamily::{Binary, Numeric, Other, Temporal, Text};
    let mut catalogo = OperandCatalog::new(prodotto);
    catalogo.insert(
        Some("s".to_owned()),
        "f".to_owned(),
        RelationColumns::Known(vec![
            CatalogColumn::new("id", Stored, Numeric),
            CatalogColumn::new("name", Stored, Text),
            CatalogColumn::new("b", Stored, Binary),
            CatalogColumn::new("w", Derived, Binary),
            CatalogColumn::new("ts", Stored, Temporal),
            CatalogColumn::new("u", Stored, Other),
            CatalogColumn::new("wt", Derived, Text),
        ]),
    );
    catalogo.insert(
        Some("s".to_owned()),
        "v".to_owned(),
        RelationColumns::Known(vec![
            CatalogColumn::new("id", Derived, Numeric),
            CatalogColumn::new("x", Derived, Text),
            CatalogColumn::new("vb", Derived, Binary),
        ]),
    );
    catalogo
}

fn colonna(relation: &str, field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": relation, "field": field}})
}

fn parametro(name: &str) -> Value {
    json!({"kind": "parameter", "name": name})
}

fn cast(expression: &Value, target: &str) -> Value {
    json!({"kind": "cast", "expression": expression, "target_type": target})
}

fn uguale(left: &Value, right: &Value) -> Value {
    json!({"kind": "compare", "left": left, "operator": "eq", "right": right})
}

fn piano(relation: &str, extra: &Value) -> QueryOperation {
    let mut operation = json!({
        "source": {"object": {"catalog": null, "schema": "s", "object": relation}, "alias": relation},
        "projection": [{"expression": colonna(relation, "id"), "alias": null}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("oggetto") {
        operation[key] = value.clone();
    }
    serde_json::from_value(operation).expect("piano")
}

/// I valori legati di prova: un nome per famiglia.
fn valori() -> ParameterFamilies {
    ParameterFamilies::from_bag(&ParameterBag::new(
        [
            ("i", ParameterValue::I64(1)),
            ("d", ParameterValue::Decimal("1.5".to_owned())),
            ("s", ParameterValue::String("a".to_owned())),
            ("y", ParameterValue::Bytes(vec![1])),
            ("t", ParameterValue::Date("2026-10-25".to_owned())),
            (
                "z",
                ParameterValue::TimestampTz("2026-10-25T00:00:00Z".to_owned()),
            ),
            (
                "q",
                ParameterValue::Uuid("00000000-0000-0000-0000-000000000000".to_owned()),
            ),
            (
                "k",
                ParameterValue::Wkb {
                    bytes: vec![1],
                    srid: Some(4326),
                    dimensions: crate::geometry::Dimensions::Xy,
                    semantics: crate::geometry::SpatialSemantics::Geometry,
                },
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect(),
    ))
}

fn giudica(prodotto: ProviderKind, operation: &QueryOperation) -> Result<()> {
    scalar_operands(prodotto, operation)?.judge_bound(&catalogo(prodotto), &valori())
}

/// Dentro una famiglia il confronto resta ammesso; la famiglia «altra» e un
/// parametro senza valore noto non si giudicano.
#[test]
fn i_confronti_dentro_una_famiglia_sono_ammessi() {
    let f = |field| colonna("f", field);
    let cases = [
        (
            "intero e intero",
            json!({"filter": uguale(&f("id"), &parametro("i"))}),
        ),
        (
            "intero e decimale",
            json!({"filter": uguale(&f("id"), &parametro("d"))}),
        ),
        (
            "intero e CAST float",
            json!({"filter": uguale(&f("id"), &cast(&parametro("s"), "float"))}),
        ),
        (
            "testo e testo",
            json!({"filter": uguale(&f("name"), &parametro("s"))}),
        ),
        (
            "LIKE su testo",
            json!({"filter": {"kind": "like", "expression": f("name"), "pattern": parametro("s")}}),
        ),
        (
            "data e timestamp con fuso",
            json!({"filter": uguale(&f("ts"), &parametro("z"))}),
        ),
        (
            "binario memorizzato e byte (scelta del chiamante)",
            json!({"filter": uguale(&f("b"), &parametro("y"))}),
        ),
        (
            "binario memorizzato e CAST binario di un parametro",
            json!({"filter": uguale(&f("b"), &cast(&parametro("s"), "binary"))}),
        ),
        (
            "altra e testo",
            json!({"filter": uguale(&f("u"), &parametro("q"))}),
        ),
        (
            "altra e byte",
            json!({"filter": uguale(&f("u"), &parametro("y"))}),
        ),
        (
            "parametro senza valore",
            json!({"filter": uguale(&f("name"), &parametro("nessuno"))}),
        ),
        (
            "IN con intero e decimale",
            json!({"filter": {"kind": "in_list", "expression": f("id"), "values": [parametro("i"), parametro("d")]}}),
        ),
        (
            "BETWEEN date",
            json!({"filter": {"kind": "between", "expression": f("ts"), "lower": parametro("t"), "upper": parametro("z")}}),
        ),
        (
            "aritmetica numerica",
            json!({"filter": uguale(&json!({"kind": "arithmetic", "left": f("id"), "operator": "add", "right": parametro("i")}), &parametro("d"))}),
        ),
        (
            "LOWER su testo",
            json!({"filter": uguale(&json!({"kind": "scalar", "function": "lower", "arguments": [f("name")]}), &parametro("s"))}),
        ),
        (
            "COALESCE di una famiglia",
            json!({"order_by": [{"expression": {"kind": "scalar", "function": "coalesce", "arguments": [f("id"), parametro("d")]}, "direction": "asc"}]}),
        ),
        (
            "forma testuale di una geometria confrontata come testo",
            json!({"filter": uguale(&f("wt"), &parametro("s"))}),
        ),
        (
            "CAST a testo di un binario derivato, in projection",
            json!({"projection": [{"expression": cast(&f("w"), "string"), "alias": "w"}]}),
        ),
    ];
    for prodotto in PRODOTTI {
        for (form, extra) in &cases {
            giudica(prodotto, &piano("f", extra))
                .unwrap_or_else(|error| panic!("{prodotto:?} {form}: {error:?}"));
        }
    }
}

fn assert_conversione(outcome: Result<()>, form: &str) {
    let error = outcome.expect_err(form);
    assert_eq!(error.category, crate::ErrorCategory::InvalidPlan, "{form}");
    assert_eq!(error.phase, crate::ErrorPhase::Prepare, "{form}");
    assert_eq!(error.message, OPERAND_FAMILY_MISMATCH, "{form}");
}

/// Regola F: due famiglie diverse in un confronto, un ordinamento, un
/// raggruppamento, `IN`, `LIKE`, fra i rami e gli argomenti si rifiutano.
#[test]
#[allow(clippy::too_many_lines)]
fn le_conversioni_implicite_fra_famiglie_si_rifiutano() {
    let f = |field| colonna("f", field);
    let v = |field| colonna("v", field);
    let cases = [
        (
            "testo e byte",
            "f",
            json!({"filter": uguale(&f("name"), &parametro("y"))}),
        ),
        (
            "testo e CAST binario (R13-2)",
            "f",
            json!({"filter": uguale(&f("name"), &cast(&parametro("s"), "binary"))}),
        ),
        (
            "testo di una vista e CAST binario (R13-2)",
            "v",
            json!({"filter": uguale(&v("x"), &cast(&parametro("s"), "binary"))}),
        ),
        (
            "testo di una vista e byte (R13-2)",
            "v",
            json!({"filter": uguale(&v("x"), &parametro("y"))}),
        ),
        (
            "numero e testo",
            "f",
            json!({"filter": uguale(&f("id"), &parametro("s"))}),
        ),
        (
            "binario e testo",
            "f",
            json!({"filter": uguale(&f("b"), &parametro("s"))}),
        ),
        (
            "data e testo",
            "f",
            json!({"filter": uguale(&f("ts"), &parametro("s"))}),
        ),
        (
            "numero e data",
            "f",
            json!({"filter": uguale(&f("id"), &parametro("t"))}),
        ),
        (
            "due colonne",
            "f",
            json!({"filter": uguale(&f("id"), &f("name"))}),
        ),
        (
            "IN misto",
            "f",
            json!({"filter": {"kind": "in_list", "expression": f("id"), "values": [parametro("i"), parametro("s")]}}),
        ),
        (
            "BETWEEN misto",
            "f",
            json!({"filter": {"kind": "between", "expression": f("ts"), "lower": parametro("t"), "upper": parametro("s")}}),
        ),
        (
            "LIKE su numero",
            "f",
            json!({"filter": {"kind": "like", "expression": f("id"), "pattern": parametro("s")}}),
        ),
        (
            "LIKE su binario",
            "f",
            json!({"filter": {"kind": "like", "expression": f("b"), "pattern": parametro("y")}}),
        ),
        (
            "aritmetica su testo",
            "f",
            json!({"filter": uguale(&json!({"kind": "arithmetic", "left": f("id"), "operator": "add", "right": f("name")}), &parametro("i"))}),
        ),
        (
            "SUM di testo",
            "f",
            json!({"projection": [{"expression": {"kind": "scalar", "function": "sum", "arguments": [f("name")]}, "alias": "s"}]}),
        ),
        (
            "LOWER di numero",
            "f",
            json!({"filter": uguale(&json!({"kind": "scalar", "function": "lower", "arguments": [f("id")]}), &parametro("s"))}),
        ),
        (
            "COALESCE misto in ORDER BY",
            "f",
            json!({"order_by": [{"expression": {"kind": "scalar", "function": "coalesce", "arguments": [f("name"), f("id")]}, "direction": "asc"}]}),
        ),
        (
            "CASE misto in GROUP BY",
            "f",
            json!({"group_by": [{"kind": "case", "branches": [{"when": uguale(&f("id"), &parametro("i")), "then": f("name")}], "else_expression": f("id")}]}),
        ),
        (
            "rami di UNION",
            "f",
            json!({
                "projection": [{"expression": f("name"), "alias": "a"}],
                "set_operations": [{"operator": "union", "all": false, "query": piano_json("f", "id")}]
            }),
        ),
        (
            "aritmetica temporale (data piu numero)",
            "f",
            json!({"filter": uguale(&json!({"kind": "arithmetic", "left": f("ts"), "operator": "add", "right": parametro("i")}), &parametro("t"))}),
        ),
        (
            "differenza di date confrontata con un testo",
            "f",
            json!({"filter": uguale(&json!({"kind": "arithmetic", "left": f("ts"), "operator": "subtract", "right": f("ts")}), &parametro("s"))}),
        ),
        (
            "COALESCE misto nella sola projection",
            "f",
            json!({"projection": [{"expression": {"kind": "scalar", "function": "coalesce", "arguments": [f("id"), f("name")]}, "alias": "c"}]}),
        ),
        (
            "COALESCE di un binario derivato e di un testo nella projection",
            "f",
            json!({"projection": [{"expression": {"kind": "scalar", "function": "coalesce", "arguments": [f("w"), f("name")]}, "alias": "c"}]}),
        ),
        (
            "LOWER di un binario derivato nella projection",
            "f",
            json!({"projection": [{"expression": {"kind": "scalar", "function": "lower", "arguments": [f("w")]}, "alias": "c"}]}),
        ),
        (
            "due parametri senza colonne",
            "f",
            json!({"filter": uguale(&parametro("i"), &parametro("s"))}),
        ),
        (
            "IN subquery",
            "f",
            json!({"filter": {"kind": "in_subquery", "expression": f("name"), "query": piano_json("f", "id")}}),
        ),
    ];
    for prodotto in PRODOTTI {
        for (form, relation, extra) in &cases {
            assert_conversione(
                giudica(prodotto, &piano(relation, extra)),
                &format!("{prodotto:?} {form}"),
            );
        }
    }
}

fn piano_json(relation: &str, field: &str) -> Value {
    json!({
        "source": {"object": {"catalog": null, "schema": "s", "object": relation}, "alias": "q"},
        "projection": [{"expression": colonna("q", field), "alias": null}],
        "filter": null,
        "having": null,
        "row_limit": null
    })
}

/// Regola B per colonna e binario derivato da un `CAST` del piano: si
/// giudicano geometrici.
#[test]
fn i_binari_derivati_si_giudicano_geometrici() {
    let f = |field| colonna("f", field);
    let v = |field| colonna("v", field);
    let cases = [
        (
            "colonna binaria generata (R13-1)",
            "f",
            json!({"filter": uguale(&f("w"), &parametro("y"))}),
        ),
        (
            "ORDER BY colonna binaria generata",
            "f",
            json!({"order_by": [{"expression": f("w"), "direction": "asc"}]}),
        ),
        (
            "colonna binaria di una vista",
            "v",
            json!({"filter": uguale(&v("vb"), &parametro("y"))}),
        ),
        (
            "binario e geometria legata (un bind Wkb e una geometria)",
            "f",
            json!({"filter": uguale(&f("b"), &parametro("k"))}),
        ),
        (
            "CAST binario di un testo di una vista (R13-2)",
            "v",
            json!({"filter": uguale(&cast(&v("x"), "binary"), &parametro("y"))}),
        ),
        (
            "CAST binario di un testo memorizzato",
            "f",
            json!({"filter": uguale(&cast(&f("name"), "binary"), &parametro("y"))}),
        ),
        (
            "CAST binario di una colonna derivata numerica",
            "v",
            json!({"filter": uguale(&cast(&v("id"), "binary"), &parametro("y"))}),
        ),
        (
            "CAST binario di una colonna generata",
            "f",
            json!({"order_by": [{"expression": cast(&f("w"), "binary"), "direction": "asc"}]}),
        ),
    ];
    for prodotto in PRODOTTI {
        for (form, relation, extra) in &cases {
            let error = giudica(prodotto, &piano(relation, extra))
                .expect_err(&format!("{prodotto:?} {form}"));
            assert_eq!(
                error.message, GEOMETRY_SCALAR_OPERAND,
                "{prodotto:?} {form}"
            );
        }
    }
    // Il CAST binario di un binario memorizzato resta byte del chiamante.
    for prodotto in PRODOTTI {
        giudica(
            prodotto,
            &piano(
                "f",
                &json!({"filter": uguale(&cast(&f("b"), "binary"), &parametro("y"))}),
            ),
        )
        .unwrap_or_else(|error| panic!("{prodotto:?}: {error:?}"));
    }
}

/// Uno statement portable porta da se le famiglie dei suoi letterali.
#[test]
fn i_letterali_portable_hanno_la_loro_famiglia() {
    let statement = |filter: Value| -> crate::portable::PortableStatement {
        serde_json::from_value(json!({
            "type": "select",
            "table": {"schema": "s", "name": "f"},
            "projection": {"kind": "all"},
            "filter": filter
        }))
        .expect("statement")
    };
    let bytes = json!({"kind": "literal", "value": {"type": "bytes", "value": [1]}});
    let text = json!({"kind": "literal", "value": {"type": "string", "value": "a"}});
    let integer = json!({"kind": "literal", "value": {"type": "i64", "value": 1}});
    for prodotto in PRODOTTI {
        let judged = |filter: Value| {
            OperandPlan::Portable(&statement(filter))
                .operands(prodotto)?
                .judge(&catalogo(prodotto))
        };
        assert_conversione(
            judged(json!({"op": "eq", "column": "name", "value": bytes})),
            "testo = byte",
        );
        assert_conversione(
            judged(json!({"op": "like", "column": "id", "pattern": text})),
            "LIKE su numero",
        );
        assert_conversione(
            judged(json!({"op": "in", "column": "id", "values": [integer, text]})),
            "IN misto",
        );
        judged(json!({"op": "eq", "column": "name", "value": text})).expect("testo = testo");
        judged(json!({"op": "like", "column": "name", "pattern": text})).expect("LIKE su testo");
        let error = judged(json!({"op": "eq", "column": "w", "value": bytes}))
            .expect_err("colonna generata binaria");
        assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND);
    }
}

/// La lettura del provider applica la stessa regola F ai campi.
#[test]
fn la_lettura_giudica_le_famiglie_dei_campi() {
    let family = |field: &str| match field {
        "id" => Some(OperandFamily::Numeric),
        "name" => Some(OperandFamily::Text),
        "u" => Some(OperandFamily::Other),
        _ => None,
    };
    let eq = |field: &str, parameter: &str| FilterExpression::Eq {
        field: field.to_owned(),
        parameter: parameter.to_owned(),
    };
    let like = |field: &str| FilterExpression::Like {
        field: field.to_owned(),
        parameter: "s".to_owned(),
        case_insensitive: false,
    };
    let judged = |filter: FilterExpression| {
        ensure_scalar_filter_fields(
            ProviderKind::Mysql,
            Some(&filter),
            &[],
            |_| false,
            family,
            &valori(),
        )
    };
    judged(eq("id", "d")).expect("intero e decimale");
    judged(eq("name", "s")).expect("testo e testo");
    judged(eq("u", "y")).expect("altra");
    judged(like("name")).expect("LIKE su testo");
    assert_conversione(judged(eq("name", "y")), "testo e byte");
    assert_conversione(judged(eq("id", "s")), "numero e testo");
    assert_conversione(judged(like("id")), "LIKE su numero");
    assert_conversione(
        judged(FilterExpression::In {
            field: "id".to_owned(),
            parameters: vec!["i".to_owned(), "s".to_owned()],
        }),
        "IN misto",
    );
}

/// Un nome legato a valori di famiglie diverse porta tutte le famiglie: ogni
/// suo uso si rifiuta (seconda lettura del 25/10).
#[test]
fn un_nome_legato_a_famiglie_diverse_si_rifiuta() {
    let names = ["p".to_owned(), "p".to_owned()];
    let values = [
        ParameterValue::I32(1),
        ParameterValue::String("01".to_owned()),
    ];
    let families = ParameterFamilies::from_positional(&names, &values);
    assert_eq!(families.get("p"), None);
    let operation = piano(
        "f",
        &json!({"filter": {"kind": "or", "arguments": [
            uguale(&colonna("f", "id"), &parametro("p")),
            uguale(&colonna("f", "id"), &parametro("p"))
        ]}}),
    );
    for prodotto in PRODOTTI {
        let outcome = scalar_operands(prodotto, &operation)
            .and_then(|operands| operands.judge_bound(&catalogo(prodotto), &families));
        assert_conversione(outcome, "nome legato a due famiglie");
    }
}

/// Un piano senza colonne in posizione di operando ma con un confronto fra
/// parametri chiede il giudizio.
#[test]
fn un_confronto_fra_parametri_chiede_il_giudizio() {
    let operation = piano(
        "f",
        &json!({"filter": uguale(&parametro("i"), &parametro("s"))}),
    );
    assert!(!scalar_operands(ProviderKind::Mysql, &operation)
        .expect("visita")
        .is_empty());
}

/// `ST_Relate` a due argomenti rende un testo derivato: il suo `CAST` binario
/// e un binario derivato; con tre argomenti e un predicato.
#[test]
fn st_relate_a_due_argomenti_e_un_testo_derivato() {
    let g = colonna("f", "u");
    let relate = |arguments: Vec<Value>| json!({"kind": "spatial", "function": "relate", "arguments": arguments});
    for prodotto in PRODOTTI {
        let error = giudica(
            prodotto,
            &piano("f", &json!({"filter": uguale(&cast(&relate(vec![g.clone(), g.clone()]), "binary"), &parametro("y"))})),
        )
        .expect_err("CAST binario di ST_Relate");
        assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{prodotto:?}");
        giudica(
            prodotto,
            &piano(
                "f",
                &json!({"filter": uguale(&relate(vec![g.clone(), g.clone()]), &parametro("s"))}),
            ),
        )
        .unwrap_or_else(|error| panic!("{prodotto:?} ST_Relate e testo: {error:?}"));
    }
}
