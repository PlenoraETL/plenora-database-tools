use super::*;
use crate::geometry::{Dimensions, SpatialSemantics};
use crate::relational::QueryOperation;
use serde_json::{json, Value};

const PRODOTTI: [ProviderKind; 6] = [
    ProviderKind::Postgres,
    ProviderKind::Mysql,
    ProviderKind::Mariadb,
    ProviderKind::Sqlserver,
    ProviderKind::Oracle,
    ProviderKind::Db2,
];

fn tipizzato(parameter_type: &str) -> Value {
    json!({"kind": "typed_parameter", "name": "p", "parameter_type": parameter_type})
}

fn colonna(field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": null, "field": field}})
}

fn query(filter: &Value) -> QueryOperation {
    serde_json::from_value(json!({
        "source": {"object": {"catalog": null, "schema": null, "object": "t"}, "alias": null},
        "projection": [{"expression": colonna("id"), "alias": "id"}],
        "filter": filter, "having": null, "row_limit": null
    }))
    .expect("piano")
}

fn uguale(left: &Value, right: &Value) -> Value {
    json!({"kind": "compare", "left": left, "operator": "eq", "right": right})
}

fn valori(value: &ParameterValue) -> BoundValues {
    let mut bound = BoundValues::default();
    bound.add("p", value);
    bound
}

fn wkb() -> ParameterValue {
    ParameterValue::Wkb {
        bytes: vec![1, 1, 0, 0, 0],
        srid: Some(4326),
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    }
}

/// Ogni tipo dichiarato contro ogni genere di valore: ammessi soltanto
/// quelli della tabella del modulo.
#[test]
fn la_tabella_dei_valori_ammessi() {
    let values = [
        ("bool", ParameterValue::Bool(true)),
        ("i32", ParameterValue::I32(1)),
        ("i64", ParameterValue::I64(1)),
        ("f64", ParameterValue::F64(1.5)),
        ("decimal", ParameterValue::Decimal("1.5".to_owned())),
        ("string", ParameterValue::String("1".to_owned())),
        (
            "enum",
            ParameterValue::Enum {
                type_name: "mood".to_owned(),
                label: "ok".to_owned(),
            },
        ),
        ("bytes", ParameterValue::Bytes(vec![1])),
        ("date", ParameterValue::Date("2026-10-25".to_owned())),
        (
            "timestamp",
            ParameterValue::Timestamp("2026-10-25T10:00:00".to_owned()),
        ),
        (
            "timestamp_tz",
            ParameterValue::TimestampTz("2026-10-25T10:00:00+01:00".to_owned()),
        ),
        (
            "uuid",
            ParameterValue::Uuid("00000000-0000-0000-0000-000000000000".to_owned()),
        ),
        ("json", ParameterValue::Json(json!({"a": 1}))),
        ("wkb", wkb()),
    ];
    let table: [(&str, &[&str]); 12] = [
        ("boolean", &["bool"]),
        ("integer", &["i32", "i64"]),
        ("big_integer", &["i32", "i64"]),
        ("float", &["f64", "i32"]),
        ("decimal", &["decimal", "i32", "i64"]),
        ("string", &["string", "enum"]),
        ("binary", &["bytes"]),
        ("date", &["date"]),
        ("timestamp", &["timestamp"]),
        ("timestamp_tz", &["timestamp_tz"]),
        ("uuid", &["uuid"]),
        ("json", &["json"]),
    ];
    for (declared, admitted) in table {
        let operation = query(&uguale(&colonna("c"), &tipizzato(declared)));
        for (kind, value) in &values {
            let outcome = valori(value).judge_query(ProviderKind::Postgres, &operation);
            if admitted.contains(kind) {
                outcome.unwrap_or_else(|error| panic!("{declared} con {kind}: {error:?}"));
            } else {
                let error = outcome.expect_err(&format!("{declared} con {kind}"));
                assert_eq!(error.category, ErrorCategory::InvalidPlan);
                assert_eq!(error.phase, ErrorPhase::Prepare);
                assert_eq!(error.message, TYPED_PARAMETER_MISMATCH);
            }
        }
        // Un `Null` e ammesso da ogni tipo, e cosi un parametro senza valore.
        valori(&ParameterValue::Null {
            type_name: "unknown".to_owned(),
        })
        .judge_query(ProviderKind::Postgres, &operation)
        .expect("null");
        BoundValues::default()
            .judge_query(ProviderKind::Postgres, &operation)
            .expect("senza valori");
    }
}

/// Il caso della quattordicesima lettura: un `string` dichiarato con il
/// valore `I64` 1, in ogni posizione e su ogni provider.
#[test]
fn un_numero_per_un_testo_dichiarato_si_rifiuta_ovunque() {
    let value = || valori(&ParameterValue::I64(1));
    let positions = [
        uguale(&colonna("name"), &tipizzato("string")),
        json!({"kind": "in_list", "expression": colonna("name"), "values": [tipizzato("string")]}),
        json!({"kind": "and", "arguments": [{"kind": "like", "expression": colonna("name"), "pattern": tipizzato("string")}]}),
    ];
    for prodotto in PRODOTTI {
        for filter in &positions {
            let error = value()
                .judge_query(prodotto, &query(filter))
                .expect_err("query");
            assert_eq!(error.message, TYPED_PARAMETER_MISMATCH, "{prodotto:?}");
            let mutation: MutationOperation = serde_json::from_value(json!({
                "type": "delete",
                "target": {"catalog": null, "schema": null, "object": "t"},
                "filter": filter
            }))
            .expect("mutazione");
            let error = value()
                .judge_mutation(prodotto, &mutation)
                .expect_err("mutazione");
            assert_eq!(error.message, TYPED_PARAMETER_MISMATCH, "{prodotto:?}");
        }
        // Anche un valore scritto e del suo tipo.
        let insert: MutationOperation = serde_json::from_value(json!({
            "type": "insert",
            "target": {"catalog": null, "schema": null, "object": "t"},
            "columns": ["name"],
            "rows": [[tipizzato("string")]]
        }))
        .expect("insert");
        let error = value()
            .judge_mutation(prodotto, &insert)
            .expect_err("insert");
        assert_eq!(error.message, TYPED_PARAMETER_MISMATCH, "{prodotto:?}");
        valori(&ParameterValue::String("1".to_owned()))
            .judge_mutation(prodotto, &insert)
            .expect("testo per testo");
    }
}

/// I limiti del `CAST` reso valgono dove il `CAST` si rende: nei predicati,
/// non nei valori scritti.
#[test]
fn i_limiti_del_cast_valgono_soltanto_dove_si_rende() {
    let cases = [
        (
            ProviderKind::Oracle,
            "string",
            ParameterValue::String("x".repeat(4001)),
            ParameterValue::String("x".repeat(4000)),
        ),
        (
            ProviderKind::Db2,
            "string",
            ParameterValue::String("x".repeat(32673)),
            ParameterValue::String("x".repeat(32672)),
        ),
        (
            ProviderKind::Oracle,
            "binary",
            ParameterValue::Bytes(vec![0; 2001]),
            ParameterValue::Bytes(vec![0; 2000]),
        ),
        (
            ProviderKind::Db2,
            "binary",
            ParameterValue::Bytes(vec![0; 32673]),
            ParameterValue::Bytes(vec![0; 32672]),
        ),
        (
            ProviderKind::Postgres,
            "decimal",
            ParameterValue::Decimal("1.00000000001".to_owned()),
            ParameterValue::Decimal("1.00000000010000".to_owned()),
        ),
    ];
    for (prodotto, declared, over, within) in cases {
        let filter = uguale(&colonna("c"), &tipizzato(declared));
        let error = valori(&over)
            .judge_query(prodotto, &query(&filter))
            .expect_err("oltre il limite");
        assert_eq!(
            error.message, TYPED_PARAMETER_MISMATCH,
            "{prodotto:?} {declared}"
        );
        valori(&within)
            .judge_query(prodotto, &query(&filter))
            .unwrap_or_else(|error| panic!("{prodotto:?} {declared}: {error:?}"));
        let update: MutationOperation = serde_json::from_value(json!({
            "type": "update",
            "target": {"catalog": null, "schema": null, "object": "t"},
            "assignments": [{"column": "c", "value": tipizzato(declared)}],
            "filter": null
        }))
        .expect("update");
        valori(&over)
            .judge_mutation(prodotto, &update)
            .unwrap_or_else(|error| panic!("scritto {prodotto:?} {declared}: {error:?}"));
    }
    // Su PostgreSQL, MySQL e SQL Server il testo e i byte non hanno limite.
    for prodotto in [
        ProviderKind::Postgres,
        ProviderKind::Mysql,
        ProviderKind::Sqlserver,
    ] {
        valori(&ParameterValue::String("x".repeat(40_000)))
            .judge_query(
                prodotto,
                &query(&uguale(&colonna("c"), &tipizzato("string"))),
            )
            .expect("senza limite");
    }
}

#[test]
fn i_decimali_che_decimal_38_10_tiene() {
    for (text, fits) in [
        ("0", true),
        ("-1.5", true),
        ("+1.5", true),
        (".5", true),
        ("5.", true),
        ("1234567890123456789012345678", true),
        ("12345678901234567890123456789", false),
        ("0001.0000000001", true),
        ("1.00000000001", false),
        ("1.000000000010", false),
        ("1.1000000000000", true),
        ("1e5", false),
        (" 1", false),
        ("", false),
        (".", false),
        ("-", false),
        ("1.2.3", false),
    ] {
        assert_eq!(decimal_fits(text), fits, "{text:?}");
    }
}

/// Un `Wkb` per un `binary` dichiarato soltanto come input di un costruttore
/// spatial; in un confronto e una geometria confrontata come byte.
#[test]
fn il_wkb_e_ammesso_soltanto_come_argomento_spatial() {
    let spatial = json!({"kind": "spatial_operator", "operator": "bounding_box_intersects",
        "left": colonna("g"),
        "right": {"kind": "spatial_value", "expression": tipizzato("binary"), "srid": 4326, "semantics": "geometry"}});
    valori(&wkb())
        .judge_query(ProviderKind::Postgres, &query(&spatial))
        .expect("valore spatial");
    let function = json!({"kind": "spatial", "function": "intersects",
        "arguments": [colonna("g"), tipizzato("binary")]});
    valori(&wkb())
        .judge_query(ProviderKind::Postgres, &query(&function))
        .expect("argomento spatial");
    let error = valori(&wkb())
        .judge_query(
            ProviderKind::Postgres,
            &query(&uguale(&colonna("w"), &tipizzato("binary"))),
        )
        .expect_err("confronto");
    assert_eq!(error.message, TYPED_PARAMETER_MISMATCH);
}

/// Un parametro tipizzato in una subquery, in un `CASE`, in una projection:
/// la regola guarda ogni posizione.
#[test]
fn la_regola_visita_ogni_posizione_della_query() {
    let operation: QueryOperation = serde_json::from_value(json!({
        "source": {"object": {"catalog": null, "schema": null, "object": "t"}, "alias": null},
        "projection": [{"expression": tipizzato("integer"), "alias": "x"}],
        "filter": null, "having": null, "row_limit": null
    }))
    .expect("piano");
    let error = valori(&ParameterValue::String("1".to_owned()))
        .judge_query(ProviderKind::Mysql, &operation)
        .expect_err("projection");
    assert_eq!(error.message, TYPED_PARAMETER_MISMATCH);
    let nested = json!({"kind": "exists", "query": {
        "source": {"object": {"catalog": null, "schema": null, "object": "u"}, "alias": null},
        "projection": [{"expression": colonna("id"), "alias": "id"}],
        "filter": uguale(&colonna("id"), &tipizzato("integer")),
        "having": null, "row_limit": null
    }});
    let error = valori(&ParameterValue::F64(1.5))
        .judge_query(ProviderKind::Oracle, &query(&nested))
        .expect_err("subquery");
    assert_eq!(error.message, TYPED_PARAMETER_MISMATCH);
}

/// La forma unica: il renderer delle query e il lowering delle mutazioni la
/// prendono da qui.
#[test]
fn la_forma_unica_del_parametro_tipizzato() {
    // Il testo e l'uuid su `MySQL`: il segnaposto nudo (la collation).
    assert_eq!(
        CastDialect::Mysql.render("?", QueryParameterType::String),
        "?"
    );
    assert_eq!(
        CastDialect::Mysql.render("?", QueryParameterType::Uuid),
        "?"
    );
    assert_eq!(
        CastDialect::Mysql.render("?", QueryParameterType::Integer),
        "CAST(? AS SIGNED)"
    );
    assert_eq!(
        CastDialect::Oracle.render(":1", QueryParameterType::Boolean),
        "TO_BOOLEAN(:1)"
    );
    assert!(CastDialect::Oracle
        .render(":1", QueryParameterType::TimestampTz)
        .starts_with("TO_TIMESTAMP_TZ(:1, "));
    assert_eq!(CastDialect::of(ProviderKind::Mariadb), CastDialect::Mysql);
    assert_eq!(
        CastDialect::Db2.render("?", QueryParameterType::Binary),
        "CAST(? AS VARCHAR(32672) FOR BIT DATA)"
    );
}
