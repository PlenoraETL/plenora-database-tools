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
    operand_server_guard, scalar_operands, CatalogColumn, OperandCatalog, RelationColumns,
    AMBIGUOUS_NAME_SHAPE, GEOMETRY_OPERAND_UNRESOLVED,
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
            colonna_di_prova("id", false),
            CatalogColumn::from_flag("g", true),
            colonna_di_prova("b", false),
            CatalogColumn::from_flag("ga", true),
        ]),
    );
    catalogo.insert(
        public(),
        "plain".to_owned(),
        RelationColumns::Known(vec![
            colonna_di_prova("id", false),
            colonna_di_prova("name", false),
        ]),
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
    // Dove il server lega i nomi (regola A) il catalogo di prova lo fa come
    // un database `CI`: `G` e la geometria. Altrove ogni identificatore e
    // quotato, e la forma si rifiuta per la regola N.
    let piegata = match prodotto {
        ProviderKind::Mysql | ProviderKind::Mariadb | ProviderKind::Sqlserver => {
            GEOMETRY_SCALAR_OPERAND
        }
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
            let outcome = scalar_operands(prodotto, &operation).and_then(|operands| {
                operands.judge(&come_il_server(&operands, catalogo(prodotto)))
            });
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
    // Regola A (quindicesima lettura del 25/10): un nome che non si lega si
    // rifiuta anche senza geometrie, prima del server.
    let assente = su("plain", &json!({"order_by": ordine(&colonna("missing"))}));
    for (prodotto, _) in PRODOTTI {
        let error = scalar_operands(prodotto, &assente)
            .and_then(|operands| operands.judge(&come_il_server(&operands, catalogo(prodotto))))
            .expect_err("nome assente");
        assert_eq!(
            error.message,
            plenora_database_core::spatial_operand::NAME_UNRESOLVED,
            "{prodotto:?}"
        );
    }
    for (prodotto, dialect) in PRODOTTI {
        for (forma, operation) in &ammesse {
            scalar_operands(prodotto, operation)
                .and_then(|operands| operands.judge(&come_il_server(&operands, catalogo(prodotto))))
                .unwrap_or_else(|error| panic!("{prodotto:?} {forma}: {error:?}"));
            renderer(prodotto, dialect)
                .render_query(operation)
                .unwrap_or_else(|error| panic!("{prodotto:?} {forma} reso: {error:?}"));
        }
    }
}

/// Le forme della guardia del server sugli operandi scalari
/// (`spatial_operand::operand_server_guard`), con lo SQL reso per Oracle e
/// Db2 e gli esiti misurati sul server da `scripts/sonde_operandi_server.py`
/// e dal live test Oracle.
const FIXTURE_GUARDIA: &str = include_str!("../../../tests/fixtures/operandi_server_guard.json");

const DIALETTI_GUARDIA: [(ProviderKind, Dialect, &str); 2] = [
    (ProviderKind::Oracle, Dialect::Oracle, "oracle"),
    (ProviderKind::Db2, Dialect::Db2, "db2"),
];

fn forme_della_guardia() -> Vec<Value> {
    let fixture: Value = serde_json::from_str(FIXTURE_GUARDIA).expect("fixture della guardia");
    fixture["forme"].as_array().expect("forme").clone()
}

/// `SELECT T.ID FROM PLENORA.SONDE_OPERANDI T` con il piano della forma sopra.
fn piano_della_forma(piano: &Value) -> Value {
    let mut operation = json!({
        "source": {"object": {"catalog": null, "schema": "PLENORA", "object": "SONDE_OPERANDI"}, "alias": "T"},
        "projection": [{"expression": {"kind": "column", "column": {"relation": "T", "field": "ID"}}, "alias": "ID"}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in piano.as_object().expect("piano") {
        operation[key] = value.clone();
    }
    operation
}

/// Il piano con la colonna `G` (e il suo alias di uscita) sostituita da
/// `colonna`: il controllo della forma.
fn con_colonna(value: &Value, colonna: &str) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, inner)| {
                    let sostituito = (key == "field" || key == "alias") && inner == "G";
                    let inner = if sostituito {
                        Value::String(colonna.to_owned())
                    } else {
                        con_colonna(inner, colonna)
                    };
                    (key.clone(), inner)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| con_colonna(item, colonna))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn resa(prodotto: ProviderKind, dialect: Dialect, piano: &Value) -> (String, Vec<String>) {
    let operation: QueryOperation = serde_json::from_value(piano.clone()).expect("piano");
    let rendered = renderer(prodotto, dialect)
        .render_query(&operation)
        .unwrap_or_else(|error| panic!("{prodotto:?}: {error:?}"));
    (
        rendered.sql,
        rendered.binds.into_iter().map(|bind| bind.name).collect(),
    )
}

/// Lo SQL del fixture e quello del renderer, per ogni forma e dialetto: lo
/// script misura sul server proprio lo SQL che il repository rende. Il
/// renderer applica l'operatore direttamente alla colonna, senza
/// conversioni; il controllo (la forma sulla colonna non geometrica) e lo
/// stesso SQL con un altro nome di colonna.
#[test]
fn il_fixture_della_guardia_e_lo_sql_del_renderer() {
    for forma in forme_della_guardia() {
        let nome = forma["forma"].as_str().expect("nome della forma");
        let piano = piano_della_forma(&forma["piano"]);
        for (prodotto, dialect, chiave) in DIALETTI_GUARDIA {
            let (sql, binds) = resa(prodotto, dialect, &piano);
            let atteso = &forma["sql"][chiave];
            assert_eq!(sql, atteso["sql"].as_str().expect("sql"), "{chiave} {nome}");
            let binds_attesi: Vec<String> =
                serde_json::from_value(atteso["binds"].clone()).expect("bind del fixture");
            assert_eq!(binds, binds_attesi, "{chiave} {nome}");
            assert!(
                !sql.contains("SDO_UTIL") && !sql.contains("ST_As"),
                "{chiave} {nome}: conversione implicita in {sql}"
            );
            if let Some(colonna) = forma["controllo"].as_str() {
                let (controllo, binds_controllo) =
                    resa(prodotto, dialect, &con_colonna(&piano, colonna));
                assert_eq!(
                    controllo,
                    sql.replace("\"G\"", &format!("\"{colonna}\"")),
                    "{chiave} {nome}: controllo"
                );
                assert_eq!(binds_controllo, binds, "{chiave} {nome}: controllo");
            }
        }
    }
}

/// La tabella della guardia e le misure: un prodotto ha la guardia se e
/// soltanto se ogni forma misurata che la regola non ammette e rifiutata dal
/// server. Le forme su una colonna che la query non legge mai non contano:
/// nessuna riga dipende dall'operatore.
#[test]
fn la_guardia_del_server_e_dove_ogni_forma_misurata_e_rifiutata() {
    let forme = forme_della_guardia();
    for (prodotto, _, chiave) in DIALETTI_GUARDIA {
        let mut senza_guardia = Vec::new();
        let mut rifiutate = 0;
        for forma in &forme {
            let esito = &forma["esito"][chiave];
            if let Some(codici) = esito.get("errore") {
                assert!(
                    !codici.as_array().expect("codici").is_empty(),
                    "{chiave} {}: errore senza codice",
                    forma["forma"]
                );
                assert!(
                    forma.get("ammessa").is_none(),
                    "{chiave} {}",
                    forma["forma"]
                );
                rifiutate += 1;
            } else {
                assert_eq!(esito, &json!({"ok": true}), "{chiave} {}", forma["forma"]);
                if forma.get("ammessa").is_none() && forma.get("colonna_mai_letta").is_none() {
                    senza_guardia.push(forma["forma"].clone());
                }
            }
        }
        if operand_server_guard(prodotto).is_some() {
            assert!(senza_guardia.is_empty(), "{chiave}: {senza_guardia:?}");
            assert!(rifiutate > 0, "{chiave}");
        } else {
            assert!(
                !senza_guardia.is_empty(),
                "{chiave}: nessuna misura senza guardia"
            );
        }
    }
    for (prodotto, _) in PRODOTTI {
        if !matches!(prodotto, ProviderKind::Oracle | ProviderKind::Db2) {
            assert!(operand_server_guard(prodotto).is_none(), "{prodotto:?}");
        }
    }
}

/// Dove il server lega i nomi (regola A, `server_resolves_names`) il
/// catalogo di prova fa da server: un database che non distingue le
/// maiuscole lega ogni nome del piano alle colonne che incontra.
fn come_il_server(
    operands: &plenora_database_core::spatial_operand::ScalarOperands,
    mut catalogo: OperandCatalog,
) -> OperandCatalog {
    if !plenora_database_core::spatial_operand::server_resolves_names(prodotto_di(operands)) {
        return catalogo;
    }
    for (schema, object) in operands.relations() {
        let Some(RelationColumns::Known(columns)) =
            catalogo.relation(schema.as_ref(), object).cloned()
        else {
            continue;
        };
        for name in operands.names() {
            let bound = columns
                .iter()
                .filter(|column| column.name.to_lowercase() == name.to_lowercase())
                .map(|column| column.name.clone())
                .collect();
            catalogo.insert_resolution(schema.clone(), object.clone(), name, bound);
        }
    }
    catalogo
}

const fn prodotto_di(
    operands: &plenora_database_core::spatial_operand::ScalarOperands,
) -> ProviderKind {
    operands.provider()
}

/// Una colonna di prova: geometrica; testo `name` e `label`; ogni altra
/// binaria memorizzata (si confronta con i parametri binari delle prove).
fn colonna_di_prova(name: &str, geometric: bool) -> CatalogColumn {
    use plenora_database_core::spatial_operand::OperandFamily;
    if geometric {
        return CatalogColumn::from_flag(name, true);
    }
    let family = if matches!(name, "name" | "label") {
        OperandFamily::Text
    } else {
        OperandFamily::Binary
    };
    CatalogColumn::new(
        name,
        plenora_database_core::spatial_operand::ColumnOrigin::Stored,
        family,
    )
}
