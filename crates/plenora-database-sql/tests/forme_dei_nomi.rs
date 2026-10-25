//! Regola N: le forme di nomi il cui legame dipende dalle regole di nome del
//! dialetto si rifiutano, `unsupported` con
//! `spatial_operand::AMBIGUOUS_NAME_SHAPE`, su ogni dialetto, rendendo e sul
//! catalogo. Con queste forme rifiutate il legame di ogni nome e lo stesso
//! sotto ogni regola: il giudizio degli operandi geometrici risolve una volta
//! sola, per byte.
//!
//! Decima rilettura (25/10, d11a957), verificata live: R1 `SELECT t.id AS
//! "k", ST_AsBinary(t.g) AS "K" ... ORDER BY "K", "k"` ammessa su
//! `PostgreSQL` (ordinava per byte WKB); R2 `... AS t WHERE EXISTS (SELECT
//! T.id FROM r10u AS T WHERE t.g = CAST(? AS BINARY))` ammessa su `MySQL`,
//! `MariaDB` e SQL Server `CS`, che legano `t` al blocco esterno.

use plenora_database_core::plan::ProviderKind;
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::spatial_operand::{
    scalar_operands, OperandCatalog, RelationColumns, AMBIGUOUS_NAME_SHAPE,
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

fn colonna(relation: Option<&str>, field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": relation, "field": field}})
}

fn bind() -> Value {
    json!({"kind": "typed_parameter", "name": "w", "parameter_type": "binary"})
}

fn confronto(left: &Value, right: &Value) -> Value {
    json!({"kind": "compare", "left": left, "operator": "eq", "right": right})
}

fn binario(expression: &Value) -> Value {
    json!({"kind": "cast", "expression": expression, "target_type": "binary"})
}

fn voce(expression: &Value, alias: Option<&str>) -> Value {
    json!({"expression": expression, "alias": alias})
}

fn ordine(expressions: &[Value]) -> Value {
    Value::Array(
        expressions
            .iter()
            .map(|expression| json!({"expression": expression, "direction": "asc"}))
            .collect(),
    )
}

fn sorgente(object: &str, alias: &str) -> Value {
    json!({"object": {"catalog": null, "schema": "public", "object": object}, "alias": alias})
}

/// `SELECT <alias>.id FROM public.<object> <alias>` con `extra` sopra.
fn blocco(object: &str, alias: &str, extra: &Value) -> Value {
    let mut operation = json!({
        "source": sorgente(object, alias),
        "projection": [voce(&colonna(Some(alias), "id"), None)],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("oggetto") {
        operation[key] = value.clone();
    }
    operation
}

fn query(value: Value) -> QueryOperation {
    serde_json::from_value(value).expect("query")
}

fn esiste(inner: &Value) -> Value {
    json!({"filter": {"kind": "exists", "query": inner}})
}

/// Il catalogo: `r10g(id, g geometrica, h geometrica)`, `r10u(id, g
/// binaria)`, `plain(id, name)`.
fn catalogo(prodotto: ProviderKind) -> OperandCatalog {
    let mut catalogo = OperandCatalog::new(prodotto);
    let colonne = |columns: &[(&str, bool)]| {
        RelationColumns::Known(
            columns
                .iter()
                .map(|(name, geometric)| ((*name).to_owned(), *geometric))
                .collect(),
        )
    };
    let public = || Some("public".to_owned());
    catalogo.insert(
        public(),
        "r10g".to_owned(),
        colonne(&[("id", false), ("g", true), ("h", true)]),
    );
    catalogo.insert(
        public(),
        "r10u".to_owned(),
        colonne(&[("id", false), ("g", false)]),
    );
    catalogo.insert(
        public(),
        "plain".to_owned(),
        colonne(&[("id", false), ("name", false)]),
    );
    catalogo
}

/// Le forme della regola N. Ognuna, su d11a957, si rendeva e passava il
/// giudizio sul catalogo su almeno un dialetto.
#[allow(clippy::too_many_lines)]
fn forme() -> Vec<(&'static str, QueryOperation)> {
    let output = |relation: &str, field: &str| json!({"kind": "spatial_output", "expression": colonna(Some(relation), field), "semantics": "geometry"});
    vec![
        (
            "R1 alias k e K, ORDER BY K, k",
            query(blocco(
                "r10g",
                "t",
                &json!({
                    "projection": [voce(&colonna(Some("t"), "id"), Some("k")), voce(&output("t", "g"), Some("K"))],
                    "order_by": ordine(&[colonna(None, "K"), colonna(None, "k")])
                }),
            )),
        ),
        (
            "R2 t esterno, T interno",
            query(blocco(
                "r10g",
                "t",
                &esiste(&blocco(
                    "r10u",
                    "T",
                    &json!({"projection": [voce(&colonna(Some("T"), "id"), None)],
                        "filter": confronto(&binario(&colonna(Some("t"), "g")), &bind())}),
                )),
            )),
        ),
        (
            "(a) stesso alias in ambiti annidati",
            query(blocco(
                "r10g",
                "t",
                &esiste(&blocco(
                    "r10u",
                    "t",
                    &json!({"filter": confronto(&colonna(Some("t"), "g"), &bind())}),
                )),
            )),
        ),
        (
            "(a) alias t e T nello stesso blocco",
            query(blocco(
                "r10g",
                "t",
                &json!({"joins": [{"kind": "cross", "source": sorgente("r10u", "T"), "on": null}]}),
            )),
        ),
        (
            "(a) qualificatore T per l'alias t",
            query(blocco(
                "r10u",
                "t",
                &json!({"filter": confronto(&colonna(Some("T"), "g"), &bind())}),
            )),
        ),
        (
            "(a) relazione C per la CTE c",
            query(json!({
                "common_table_expressions": [{"name": "c", "query": blocco("r10u", "u", &json!({}))}],
                "source": {"object": {"catalog": null, "schema": null, "object": "C"}, "alias": "x"},
                "projection": [voce(&colonna(Some("x"), "id"), None)],
                "filter": null, "having": null, "row_limit": null
            })),
        ),
        (
            "(b) due uscite id",
            query(blocco(
                "r10g",
                "t",
                &json!({
                    "joins": [{"kind": "cross", "source": sorgente("plain", "p"), "on": null}],
                    "projection": [voce(&colonna(Some("t"), "id"), None), voce(&colonna(Some("p"), "id"), None)]
                }),
            )),
        ),
        (
            "(c) colonna senza qualificatore in EXISTS",
            query(blocco(
                "r10g",
                "t",
                &esiste(&blocco(
                    "r10u",
                    "u",
                    &json!({"filter": confronto(&colonna(None, "g"), &bind())}),
                )),
            )),
        ),
        (
            "(c) colonna senza qualificatore in una subquery scalare",
            query(blocco(
                "r10g",
                "t",
                &json!({"filter": confronto(
                    &json!({"kind": "scalar_subquery", "query": blocco("plain", "p", &json!({"projection": [voce(&colonna(None, "id"), None)]}))}),
                    &bind()
                )}),
            )),
        ),
        (
            "(c) colonna senza qualificatore in LATERAL",
            query(blocco(
                "r10g",
                "t",
                &json!({"joins": [{"kind": "cross", "lateral": true, "source": null,
                    "derived_source": {"alias": "l", "query": blocco("plain", "p", &json!({"projection": [voce(&colonna(None, "name"), None)]}))},
                    "on": null}]}),
            )),
        ),
    ]
}

/// Ogni forma si rifiuta rendendo, su ogni dialetto.
#[test]
fn ogni_forma_di_nomi_ambigua_si_rifiuta_rendendo_su_ogni_dialetto() {
    for (prodotto, dialect) in PRODOTTI {
        for (forma, operation) in forme() {
            let error = renderer(prodotto, dialect)
                .render_query(&operation)
                .err()
                .unwrap_or_else(|| panic!("{prodotto:?} {forma}: resa"));
            assert_eq!(
                error.category,
                ErrorCategory::Unsupported,
                "{prodotto:?} {forma}"
            );
            assert_eq!(error.message, AMBIGUOUS_NAME_SHAPE, "{prodotto:?} {forma}");
        }
    }
}

/// Ogni forma si rifiuta anche nel giudizio sul catalogo che i provider e
/// l'esecuzione dello SDK chiedono al core.
#[test]
fn ogni_forma_di_nomi_ambigua_si_rifiuta_sul_catalogo_su_ogni_dialetto() {
    for (prodotto, _) in PRODOTTI {
        for (forma, operation) in forme() {
            let error = scalar_operands(prodotto, &operation)
                .and_then(|operands| operands.judge(&catalogo(prodotto)))
                .err()
                .unwrap_or_else(|| panic!("{prodotto:?} {forma}: ammessa"));
            assert_eq!(error.message, AMBIGUOUS_NAME_SHAPE, "{prodotto:?} {forma}");
        }
    }
}

/// (d) Un nome che incontra la colonna del catalogo soltanto senza
/// maiuscole: si rifiuta dove il dialetto distingue o puo distinguere le
/// maiuscole (`PostgreSQL`, Oracle, Db2; SQL Server secondo la collation);
/// `MySQL` e `MariaDB` non le distinguono nei nomi di colonna, e la colonna e
/// la geometria.
#[test]
fn un_nome_diverso_dal_catalogo_soltanto_per_le_maiuscole_si_rifiuta_dove_conta() {
    let operation = query(blocco(
        "r10g",
        "t",
        &json!({"order_by": ordine(&[colonna(Some("t"), "G")])}),
    ));
    for (prodotto, _) in PRODOTTI {
        let error = scalar_operands(prodotto, &operation)
            .and_then(|operands| operands.judge(&catalogo(prodotto)))
            .err()
            .unwrap_or_else(|| panic!("{prodotto:?}: ammessa"));
        let atteso = match prodotto {
            ProviderKind::Mysql | ProviderKind::Mariadb => GEOMETRY_SCALAR_OPERAND,
            _ => AMBIGUOUS_NAME_SHAPE,
        };
        assert_eq!(error.message, atteso, "{prodotto:?}");
    }
}

/// Nessun falso rifiuto: alias distinti, riferimenti qualificati, la stessa
/// tabella in rami di set operation o in sorgenti derivate non correlate, le
/// geometrie dentro i nodi spatial, una self join con due alias.
#[test]
fn le_forme_ordinarie_restano_ammesse() {
    let ammesse = [
        (
            "EXISTS correlata, alias distinti, qualificata",
            query(blocco(
                "r10g",
                "t",
                &esiste(&blocco(
                    "plain",
                    "p",
                    &json!({"filter": confronto(&colonna(Some("p"), "id"), &colonna(Some("t"), "id"))}),
                )),
            )),
        ),
        (
            "UNION ALL con lo stesso alias nei rami",
            query(blocco(
                "plain",
                "t",
                &json!({"set_operations": [{"operator": "union", "all": true,
                    "query": blocco("plain", "t", &json!({}))}]}),
            )),
        ),
        (
            "derivata non correlata, colonne senza qualificatore",
            query(json!({
                "source": null,
                "derived_source": {"alias": "d", "query": blocco("plain", "t",
                    &json!({"projection": [voce(&colonna(None, "id"), None), voce(&colonna(None, "name"), None)],
                        "order_by": ordine(&[colonna(None, "name")])}))},
                "projection": [voce(&colonna(Some("d"), "id"), None)],
                "filter": null, "having": null, "row_limit": null,
                "order_by": ordine(&[colonna(Some("d"), "name")])
            })),
        ),
        (
            "colonne senza qualificatore nel blocco esterno",
            query(blocco(
                "r10g",
                "t",
                &json!({"projection": [voce(&colonna(None, "id"), Some("k"))],
                    "order_by": ordine(&[colonna(None, "k"), colonna(None, "id")])}),
            )),
        ),
        (
            "geometrie in un nodo spatial di una subquery",
            query(blocco(
                "r10g",
                "t",
                &esiste(&blocco(
                    "r10g",
                    "o",
                    &json!({"filter": {"kind": "spatial", "function": "intersects",
                        "arguments": [colonna(Some("o"), "g"), colonna(Some("t"), "h")]}}),
                )),
            )),
        ),
        (
            "self join con due alias",
            query(blocco(
                "plain",
                "a",
                &json!({"joins": [{"kind": "inner", "source": sorgente("plain", "b"),
                    "on": confronto(&colonna(Some("a"), "id"), &colonna(Some("b"), "id"))}],
                    "projection": [voce(&colonna(Some("a"), "id"), Some("a_id")), voce(&colonna(Some("b"), "name"), None)]}),
            )),
        ),
    ];
    for (prodotto, dialect) in PRODOTTI {
        for (forma, operation) in &ammesse {
            renderer(prodotto, dialect)
                .render_query(operation)
                .unwrap_or_else(|error| panic!("{prodotto:?} {forma} resa: {error:?}"));
            scalar_operands(prodotto, operation)
                .and_then(|operands| operands.judge(&catalogo(prodotto)))
                .unwrap_or_else(|error| panic!("{prodotto:?} {forma}: {error:?}"));
        }
    }
}

/// Un nome non ASCII in un piano con geometrie su SQL Server, `MySQL` e
/// `MariaDB`: le collation possono uguagliare nomi che la piegatura
/// distingue (accenti, `ß`/`ss`), e il legame non si prova. Si rifiuta; dove i
/// nomi si quotano esatti (`PostgreSQL`, Oracle, Db2) e su una tabella senza
/// geometrie resta.
#[test]
fn un_nome_non_ascii_si_rifiuta_dove_la_collation_puo_uguagliarlo() {
    let geometrica = query(blocco(
        "r10g",
        "città",
        &json!({"order_by": ordine(&[colonna(Some("città"), "id")])}),
    ));
    let scalare = query(blocco(
        "plain",
        "città",
        &json!({"order_by": ordine(&[colonna(Some("città"), "id")])}),
    ));
    for (prodotto, _) in PRODOTTI {
        let outcome = scalar_operands(prodotto, &geometrica)
            .and_then(|operands| operands.judge(&catalogo(prodotto)));
        match prodotto {
            ProviderKind::Sqlserver | ProviderKind::Mysql | ProviderKind::Mariadb => {
                let error = outcome
                    .err()
                    .unwrap_or_else(|| panic!("{prodotto:?}: ammessa"));
                assert_eq!(error.message, AMBIGUOUS_NAME_SHAPE, "{prodotto:?}");
            }
            _ => outcome.unwrap_or_else(|error| panic!("{prodotto:?}: {error:?}")),
        }
        scalar_operands(prodotto, &scalare)
            .and_then(|operands| operands.judge(&catalogo(prodotto)))
            .unwrap_or_else(|error| panic!("{prodotto:?} senza geometrie: {error:?}"));
    }
}
