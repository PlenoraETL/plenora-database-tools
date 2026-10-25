//! La regola degli operandi geometrici, nella parte che non chiede un
//! catalogo, provata per dialetto e forma.
//!
//! Una funzione spatial che rende una geometria sotto un operatore che non e
//! spatial si rifiuta compilando, `unsupported` con
//! `spatial_probe::GEOMETRY_SCALAR_OPERAND`, su ogni dialetto: il server
//! confronterebbe il proprio formato interno con il valore legato. Fino al
//! 25/10 `ST_Centroid(t.g) = :p` si rendeva su ogni dialetto. Le colonne le
//! giudica il provider sul catalogo (`spatial_operand::ScalarOperands`), a
//! mondo chiuso: qui la tabella del renderer e quella del giudizio sul
//! catalogo che ogni provider chiede al core, per dialetto.

use plenora_database_core::plan::ProviderKind;
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::spatial_operand::{
    scalar_operands, OperandCatalog, RelationColumns, AMBIGUOUS_NAME_SHAPE,
    GEOMETRY_OPERAND_UNRESOLVED,
};
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

/// Il catalogo di `public.features(id, g geometrica, b)`, di
/// `public.plain(id, name)` senza geometrie e di `public.syn` (un sinonimo
/// di cui il catalogo non legge le colonne).
fn catalogo(prodotto: ProviderKind) -> OperandCatalog {
    let mut catalogo = OperandCatalog::new(prodotto);
    let public = || Some("public".to_owned());
    catalogo.insert(
        public(),
        "features".to_owned(),
        RelationColumns::Known(vec![
            ("id".to_owned(), false),
            ("g".to_owned(), true),
            ("b".to_owned(), false),
            ("ga".to_owned(), true),
        ]),
    );
    catalogo.insert(
        public(),
        "plain".to_owned(),
        RelationColumns::Known(vec![("id".to_owned(), false), ("name".to_owned(), false)]),
    );
    catalogo.insert(public(), "syn".to_owned(), RelationColumns::Unknown);
    catalogo
}

fn voce(expression: &Value, alias: Option<&str>) -> Value {
    json!({"expression": expression, "alias": alias})
}

fn testo(expression: &Value) -> Value {
    json!({"kind": "cast", "expression": expression, "target_type": "string"})
}

fn like(expression: &Value) -> Value {
    json!({"kind": "like", "expression": expression,
        "pattern": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}})
}

fn ordine(expression: &Value) -> Value {
    json!([{"expression": expression, "direction": "desc"}])
}

fn colonna_di(relation: &str, field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": relation, "field": field}})
}

/// `SELECT d.id FROM (inner) d` con `extra` sopra.
fn su_derivata(inner: &QueryOperation, extra: &Value) -> QueryOperation {
    let mut operation = json!({
        "source": null,
        "derived_source": {"query": inner, "alias": "d"},
        "projection": [voce(&colonna_di("d", "id"), None)],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("oggetto") {
        operation[key] = value.clone();
    }
    serde_json::from_value(operation).expect("query")
}

/// `SELECT s.id FROM public.<object> s` con `extra` sopra.
fn su(object: &str, extra: &Value) -> QueryOperation {
    let mut operation = serde_json::to_value(query(extra)).expect("json");
    operation["source"] =
        json!({"object": {"catalog": null, "schema": "public", "object": object}, "alias": "t"});
    serde_json::from_value(operation).expect("query")
}

/// Le sei forme della nona rilettura (25/10), ciascuna ammessa su
/// cf49fa0: il motivo atteso per dialetto. `ORDER BY` di una `UNION ALL` per
/// nome di uscita, nomi generati dal server, `*` in posizione di operando,
/// nomi piegati (SQL Server, `MySQL`, `MariaDB`; esatti altrove), sinonimi,
/// array di geometrie.
fn sei_forme(prodotto: ProviderKind) -> Vec<(&'static str, QueryOperation, &'static str)> {
    let g = colonna("g");
    let output = json!({"kind": "spatial_output", "expression": g, "semantics": "geometry"});
    let union = query(&json!({
        "projection": [voce(&bind(), Some("k"))],
        "set_operations": [{"operator": "union", "all": true,
            "query": query(&json!({"projection": [voce(&output, Some("k"))]}))}],
        "order_by": ordine(&json!({"kind": "column", "column": {"relation": null, "field": "k"}}))
    }));
    let generata = |expression: &Value| {
        query(&json!({"projection": [voce(&colonna("id"), None), voce(expression, None)]}))
    };
    let centroide_senza_alias = generata(&centroide());
    let testo_senza_alias = generata(&testo(&g));
    // `MySQL` e `MariaDB` non distinguono le maiuscole nei nomi di colonna:
    // `G` e la geometria. Altrove (SQL Server secondo la collation) la forma
    // si rifiuta per la regola N.
    let piegata = match prodotto {
        ProviderKind::Mysql | ProviderKind::Mariadb => GEOMETRY_SCALAR_OPERAND,
        _ => AMBIGUOUS_NAME_SHAPE,
    };
    vec![
        ("1 ORDER BY k su UNION ALL", union, GEOMETRY_SCALAR_OPERAND),
        (
            "2 ORDER BY d.st_centroid",
            su_derivata(
                &centroide_senza_alias,
                &json!({"order_by": ordine(&colonna_di("d", "st_centroid")), "row_limit": 1}),
            ),
            GEOMETRY_SCALAR_OPERAND,
        ),
        (
            "2 GROUP BY d.st_centroid",
            su_derivata(
                &centroide_senza_alias,
                &json!({"group_by": [colonna_di("d", "st_centroid")]}),
            ),
            GEOMETRY_SCALAR_OPERAND,
        ),
        (
            "2 d.g LIKE su CAST senza alias",
            su_derivata(
                &testo_senza_alias,
                &json!({"filter": like(&colonna_di("d", "g"))}),
            ),
            GEOMETRY_SCALAR_OPERAND,
        ),
        (
            "3 DISTINCT *",
            su_derivata(
                &query(&json!({"projection": [voce(&testo(&g), Some("c"))]})),
                &json!({"projection": [voce(&json!({"kind": "wildcard", "relation": null}), None)], "distinct": true}),
            ),
            GEOMETRY_SCALAR_OPERAND,
        ),
        (
            "4 CAST(t.G) LIKE",
            query(&json!({"filter": like(&testo(&colonna("G")))})),
            piegata,
        ),
        (
            "5 sinonimo",
            su("syn", &json!({"filter": like(&testo(&colonna("g")))})),
            GEOMETRY_OPERAND_UNRESOLVED,
        ),
        (
            "6 ORDER BY t.ga",
            query(&json!({"order_by": ordine(&colonna("ga")), "row_limit": 1})),
            GEOMETRY_SCALAR_OPERAND,
        ),
    ]
}

/// Le sei forme, per dialetto, sul catalogo: il giudizio che il provider
/// chiede al core.
#[test]
fn le_sei_forme_non_risolte_si_rifiutano_su_ogni_dialetto() {
    for (prodotto, _) in PRODOTTI {
        for (forma, operation, motivo) in sei_forme(prodotto) {
            let outcome = scalar_operands(prodotto, &operation)
                .and_then(|operands| operands.judge(&catalogo(prodotto)));
            let error = outcome
                .err()
                .unwrap_or_else(|| panic!("{prodotto:?} {forma}: ammessa"));
            assert_eq!(
                error.category,
                ErrorCategory::Unsupported,
                "{prodotto:?} {forma}: {error:?}"
            );
            assert_eq!(error.message, motivo, "{prodotto:?} {forma}: {error:?}");
        }
    }
}

/// La forma 2 con una funzione che rende una geometria si rifiuta gia
/// rendendo, senza catalogo, su ogni dialetto.
#[test]
fn un_nome_generato_da_una_geometria_calcolata_si_rifiuta_rendendo() {
    let inner =
        query(&json!({"projection": [voce(&colonna("id"), None), voce(&centroide(), None)]}));
    let operation = su_derivata(
        &inner,
        &json!({"order_by": ordine(&colonna_di("d", "st_centroid"))}),
    );
    for (prodotto, dialect) in PRODOTTI {
        let error = renderer(prodotto, dialect)
            .render_query(&operation)
            .err()
            .unwrap_or_else(|| panic!("{prodotto:?}: reso"));
        assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{prodotto:?}");
    }
}

/// Nessun falso rifiuto: le colonne scalari di una tabella con geometrie e
/// le stesse forme su una tabella senza geometrie.
#[test]
fn colonne_scalari_e_tabelle_senza_geometrie_restano_ammesse() {
    let id = colonna("id");
    let area = json!({"kind": "spatial", "function": "area", "arguments": [colonna("g")]});
    let derivata = query(&json!({"projection": [voce(&id, None), voce(&area, None)]}));
    let union = query(&json!({
        "projection": [voce(&id, Some("k"))],
        "set_operations": [{"operator": "union", "all": true,
            "query": query(&json!({"projection": [voce(&colonna("b"), Some("k"))]}))}],
        "order_by": ordine(&json!({"kind": "column", "column": {"relation": null, "field": "k"}}))
    }));
    let star = json!({"kind": "wildcard", "relation": null});
    let ammesse = [
        ("ORDER BY t.id", query(&json!({"order_by": ordine(&id)}))),
        ("UNION ALL scalare", union),
        (
            "d.id",
            su_derivata(
                &derivata,
                &json!({"order_by": ordine(&colonna_di("d", "id"))}),
            ),
        ),
        (
            "d.st_area",
            su_derivata(
                &derivata,
                &json!({"order_by": ordine(&colonna_di("d", "st_area"))}),
            ),
        ),
        (
            "plain DISTINCT *",
            su(
                "plain",
                &json!({"projection": [voce(&star, None)], "distinct": true}),
            ),
        ),
        (
            "plain ORDER BY nome assente",
            su("plain", &json!({"order_by": ordine(&colonna("missing"))})),
        ),
        (
            "plain d.lower generato",
            su_derivata(
                &su(
                    "plain",
                    &json!({"projection": [voce(&id, None), voce(&json!({"kind": "scalar", "function": "lower", "arguments": [colonna("name")]}), None)]}),
                ),
                &json!({"order_by": ordine(&colonna_di("d", "lower"))}),
            ),
        ),
    ];
    for (prodotto, dialect) in PRODOTTI {
        for (forma, operation) in &ammesse {
            scalar_operands(prodotto, operation)
                .and_then(|operands| operands.judge(&catalogo(prodotto)))
                .unwrap_or_else(|error| panic!("{prodotto:?} {forma}: {error:?}"));
            renderer(prodotto, dialect)
                .render_query(operation)
                .unwrap_or_else(|error| panic!("{prodotto:?} {forma} reso: {error:?}"));
        }
    }
}
