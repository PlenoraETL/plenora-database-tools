//! La regola degli operandi geometrici, nella parte che non chiede un
//! catalogo, provata per dialetto e forma.
//!
//! Una funzione spatial che rende una geometria sotto un operatore che non e
//! spatial si rifiuta compilando, `unsupported` con
//! `spatial_probe::GEOMETRY_SCALAR_OPERAND`, su ogni dialetto: il server
//! confronterebbe il proprio formato interno con il valore legato. Fino al
//! 25/10 `ST_Centroid(t.g) = :p` si rendeva su ogni dialetto. Le colonne le
//! giudica il provider sul catalogo (`spatial_operand::ScalarOperands`), e
//! le prove sono le sue; qui resta la tabella del renderer.

use plenora_database_core::plan::ProviderKind;
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
use plenora_database_core::ErrorCategory;
use plenora_database_sql::{Dialect, DialectCapabilities, Renderer};
use serde_json::{json, Value};

const PRODOTTI: [(ProviderKind, Dialect); 6] = [
    (ProviderKind::Postgres, Dialect::Postgres),
    (ProviderKind::Mysql, Dialect::Mysql),
    (ProviderKind::Mariadb, Dialect::Mysql),
    (ProviderKind::Sqlserver, Dialect::SqlServer),
    (ProviderKind::Oracle, Dialect::Oracle),
    (ProviderKind::Db2, Dialect::Db2),
];

const fn renderer(prodotto: ProviderKind, dialect: Dialect) -> Renderer {
    Renderer::new(
        dialect,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(prodotto)
}

fn colonna(field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": "t", "field": field}})
}

fn bind() -> Value {
    json!({"kind": "typed_parameter", "name": "p", "parameter_type": "binary"})
}

fn centroide() -> Value {
    json!({"kind": "spatial", "function": "centroid", "arguments": [colonna("g")]})
}

/// `SELECT t.id FROM public.features t` con `extra` sopra.
fn query(extra: &Value) -> QueryOperation {
    let mut operation = json!({
        "source": {"object": {"catalog": null, "schema": "public", "object": "features"}, "alias": "t"},
        "projection": [{"expression": colonna("id"), "alias": "id"}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("oggetto") {
        operation[key] = value.clone();
    }
    serde_json::from_value(operation).expect("query")
}

fn rifiutate() -> Vec<(&'static str, Value)> {
    vec![
        (
            "=",
            json!({"filter": {"kind": "compare", "left": centroide(), "operator": "eq", "right": bind()}}),
        ),
        (
            "<>",
            json!({"filter": {"kind": "compare", "left": bind(), "operator": "ne", "right": centroide()}}),
        ),
        (
            "IN",
            json!({"filter": {"kind": "in_list", "expression": centroide(), "values": [bind()]}}),
        ),
        (
            "BETWEEN",
            json!({"filter": {"kind": "between", "expression": centroide(), "lower": bind(), "upper": bind()}}),
        ),
        (
            "LIKE su CAST",
            json!({"filter": {
                "kind": "like",
                "expression": {"kind": "cast", "expression": centroide(), "target_type": "string"},
                "pattern": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}
            }}),
        ),
        (
            "ORDER BY",
            json!({"order_by": [{"expression": centroide(), "direction": "asc"}]}),
        ),
        ("GROUP BY", json!({"group_by": [centroide()]})),
    ]
}

/// Ogni dialetto rifiuta ogni forma, prima di rendere.
#[test]
fn una_geometria_calcolata_sotto_un_operatore_non_spatial_si_rifiuta_su_ogni_dialetto() {
    for (prodotto, dialect) in PRODOTTI {
        for (forma, extra) in rifiutate() {
            let outcome = renderer(prodotto, dialect).render_query(&query(&extra));
            let error = outcome
                .err()
                .unwrap_or_else(|| panic!("{prodotto:?} {forma}: reso"));
            assert_eq!(
                error.category,
                ErrorCategory::Unsupported,
                "{prodotto:?} {forma}: {error:?}"
            );
            assert_eq!(
                error.message, GEOMETRY_SCALAR_OPERAND,
                "{prodotto:?} {forma}: {error:?}"
            );
        }
    }
}

/// Nessun falso rifiuto: una funzione spatial che rende uno scalare si
/// confronta, una colonna si confronta (la giudica il provider), `IS NULL`
/// di una geometria calcolata resta.
#[test]
fn uno_scalare_spatial_e_una_colonna_restano_ammessi() {
    let ammesse = [
        (
            "ST_Area confrontata",
            json!({"filter": {
                "kind": "compare",
                "left": {"kind": "spatial", "function": "area", "arguments": [colonna("g")]},
                "operator": "gt",
                "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "float"}
            }}),
        ),
        (
            "colonna binaria",
            json!({"filter": {"kind": "compare", "left": colonna("b"), "operator": "eq", "right": bind()}}),
        ),
        (
            "IS NULL",
            json!({"filter": {"kind": "is_null", "expression": centroide(), "negated": false}}),
        ),
    ];
    for (prodotto, dialect) in PRODOTTI {
        for (forma, extra) in &ammesse {
            renderer(prodotto, dialect)
                .render_query(&query(extra))
                .unwrap_or_else(|error| panic!("{prodotto:?} {forma}: {error:?}"));
        }
    }
}
