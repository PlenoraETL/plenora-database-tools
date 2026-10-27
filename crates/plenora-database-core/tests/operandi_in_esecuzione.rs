//! Regola S: i percorsi che eseguono SQL compilato (SDK, ORM, facade e CLI
//! portable) giudicano il piano sul catalogo che il provider legge nella
//! transazione (`TransactionScope::operand_catalog`), non sulle colonne che il
//! chiamante dichiara, prima di eseguire.
//!
//! Fino al 25/10 (d11a957) lo SDK eseguiva lo SQL compilato con
//! `execute_returning_rows(sql, values)`, giudicato soltanto sulle colonne
//! `Geometry` dichiarate: una tabella non tipizzata (`p.table("t", "id", "g")`)
//! o una colonna non mappata (`p.column("h")`) arrivavano al server, e
//! `ORDER BY g` ordinava per byte su `PostgreSQL`, `MySQL` e `MariaDB`. Qui
//! il finto della transazione ha il catalogo del server: ogni forma si
//! rifiuta prima dell'esecuzione, su ogni dialetto.

use plenora_database_core::facade::{execute_judged, query_judged, verify_operands};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::relational::{MutationOperation, QueryOperation};
use plenora_database_core::row::Row;
use plenora_database_core::spatial_operand::{
    CatalogColumn, OperandCatalog, OperandPlan, RelationColumns, AMBIGUOUS_NAME_SHAPE,
    GEOMETRY_OPERAND_UNRESOLVED, NAME_UNRESOLVED,
};
use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
use plenora_database_core::transaction::{
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase};
use serde_json::{json, Value};

const PRODOTTI: [ProviderKind; 6] = [
    ProviderKind::Postgres,
    ProviderKind::Mysql,
    ProviderKind::Mariadb,
    ProviderKind::Sqlserver,
    ProviderKind::Oracle,
    ProviderKind::Db2,
];

/// Le tabelle del catalogo finto: nome, colonne (nome, geometrica).
type Tabelle = Vec<(String, Vec<(String, bool)>)>;

/// Una transazione finta: il catalogo del server (`None`: il provider non lo
/// legge, e vale il default del trait) e quante istruzioni ha eseguito.
struct Finta {
    prodotto: ProviderKind,
    catalogo: Option<Tabelle>,
    eseguite: usize,
}

fn rifiuto(prodotto: ProviderKind) -> DatabaseError {
    DatabaseError::unsupported(prodotto, ErrorPhase::Prepare, "non usato dal finto")
}

impl TransactionScope for Finta {
    fn provider_kind(&self) -> ProviderKind {
        self.prodotto
    }

    fn execute<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        self.eseguite += 1;
        Box::pin(async { Ok(1) })
    }

    fn query<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        self.eseguite += 1;
        Box::pin(async { Ok(Vec::new()) })
    }

    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        let prodotto = self.prodotto;
        Box::pin(async move { Err(rifiuto(prodotto)) })
    }

    fn savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let prodotto = self.prodotto;
        Box::pin(async move { Err(rifiuto(prodotto)) })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let prodotto = self.prodotto;
        Box::pin(async move { Err(rifiuto(prodotto)) })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let prodotto = self.prodotto;
        Box::pin(async move { Err(rifiuto(prodotto)) })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        _request: ConditionalUpdate<'a>,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let prodotto = self.prodotto;
        Box::pin(async move { Err(rifiuto(prodotto)) })
    }

    fn operand_catalog<'a>(
        &'a mut self,
        relations: &'a [(Option<String>, String)],
        names: &'a [String],
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, OperandCatalog> {
        let _ = cancellation;
        let Some(tabelle) = &self.catalogo else {
            // Come il default del trait (provato in `caller_sql_tests`): il
            // provider non legge il catalogo.
            let prodotto = self.prodotto;
            return Box::pin(async move { Ok(OperandCatalog::new(prodotto)) });
        };
        let mut catalogo = OperandCatalog::new(self.prodotto);
        for (schema, object) in relations {
            let colonne = tabelle.iter().find(|(nome, _)| nome == object).map_or(
                RelationColumns::Absent,
                |(_, colonne)| {
                    RelationColumns::Known(
                        colonne
                            .iter()
                            .map(|(nome, geometrica)| colonna_finta(nome, *geometrica))
                            .collect(),
                    )
                },
            );
            // Dove il server lega i nomi (regola A) il finto fa da server:
            // un database che non distingue le maiuscole.
            if plenora_database_core::spatial_operand::server_resolves_names(self.prodotto) {
                if let RelationColumns::Known(known) = &colonne {
                    for name in names {
                        let legate = known
                            .iter()
                            .filter(|column| column.name.to_lowercase() == name.to_lowercase())
                            .map(|column| column.name.clone())
                            .collect();
                        catalogo.insert_resolution(
                            schema.clone(),
                            object.clone(),
                            name.clone(),
                            legate,
                        );
                    }
                }
            }
            catalogo.insert(schema.clone(), object.clone(), colonne);
        }
        Box::pin(async move { Ok(catalogo) })
    }

    fn commit(
        self: Box<Self>,
        _cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        let prodotto = self.prodotto;
        Box::pin(async move { Err(rifiuto(prodotto)) })
    }

    fn rollback(self: Box<Self>, _cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Una colonna del catalogo finto: geometrica, oppure `id` numerica e ogni
/// altra testo (regola F con famiglie note, regola B senza «altra»).
fn colonna_finta(nome: &str, geometrica: bool) -> CatalogColumn {
    use plenora_database_core::spatial_operand::{ColumnOrigin, OperandFamily};
    if geometrica {
        return CatalogColumn::from_flag(nome, true);
    }
    let famiglia = if nome == "id" {
        OperandFamily::Numeric
    } else {
        OperandFamily::Text
    };
    CatalogColumn::new(nome, ColumnOrigin::Stored, famiglia)
}

/// Il catalogo del server: `r10g(id, g geometrica, h geometrica)` e
/// `plain(id, name)`. Lo SDK non ha dichiarato nessuna delle due.
fn finta(prodotto: ProviderKind) -> Finta {
    Finta {
        prodotto,
        catalogo: Some(vec![
            (
                "r10g".to_owned(),
                vec![
                    ("id".to_owned(), false),
                    ("g".to_owned(), true),
                    ("h".to_owned(), true),
                ],
            ),
            (
                "plain".to_owned(),
                vec![("id".to_owned(), false), ("name".to_owned(), false)],
            ),
        ]),
        eseguite: 0,
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("il finto e sincrono"),
    }
}

fn colonna(relation: Option<&str>, field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": relation, "field": field}})
}

fn bind() -> Value {
    json!({"kind": "typed_parameter", "name": "w", "parameter_type": "binary"})
}

/// `SELECT r10g.id FROM public.r10g` (la tabella senza alias, come la
/// nomina lo SDK) con `extra` sopra.
fn query(object: &str, extra: &Value) -> QueryOperation {
    let mut operation = json!({
        "source": {"object": {"catalog": null, "schema": "public", "object": object}, "alias": null},
        "projection": [{"expression": colonna(Some(object), "id"), "alias": null}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("oggetto") {
        operation[key] = value.clone();
    }
    serde_json::from_value(operation).expect("query")
}

fn ordine(expression: &Value) -> Value {
    json!([{"expression": expression, "direction": "asc"}])
}

fn mutazione(value: &Value) -> MutationOperation {
    serde_json::from_value(value.clone()).expect("mutazione")
}

fn portable(value: &Value) -> PortableStatement {
    serde_json::from_value(value.clone()).expect("portable")
}

fn esito(prodotto: ProviderKind, plan: OperandPlan<'_>) -> (Result<(), DatabaseError>, usize) {
    let mut tx = finta(prodotto);
    let cancel = CancellationToken::new();
    let statement = Statement::new("SELECT 1");
    let outcome = match plan {
        OperandPlan::Query(_) => {
            block_on(query_judged(&mut tx, &statement, Some(&plan), &[], &cancel)).map(|_| ())
        }
        _ => block_on(execute_judged(
            &mut tx,
            &statement,
            Some(&plan),
            &[],
            &cancel,
        ))
        .map(|_| ()),
    };
    (outcome, tx.eseguite)
}

fn rifiutata(prodotto: ProviderKind, forma: &str, plan: OperandPlan<'_>, motivo: &str) {
    let (outcome, eseguite) = esito(prodotto, plan);
    let error = outcome
        .err()
        .unwrap_or_else(|| panic!("{prodotto:?} {forma}: ammessa"));
    assert_eq!(
        error.category,
        ErrorCategory::Unsupported,
        "{prodotto:?} {forma}"
    );
    assert_eq!(error.message, motivo, "{prodotto:?} {forma}");
    assert_eq!(eseguite, 0, "{prodotto:?} {forma}: eseguita");
}

fn ammessa(prodotto: ProviderKind, forma: &str, plan: OperandPlan<'_>) {
    let (outcome, eseguite) = esito(prodotto, plan);
    outcome.unwrap_or_else(|error| panic!("{prodotto:?} {forma}: {error:?}"));
    assert_eq!(eseguite, 1, "{prodotto:?} {forma}");
}

/// R4: le forme dello SDK che il giudizio dichiarato lasciava passare, sul
/// catalogo della transazione, su ogni dialetto.
#[test]
fn le_colonne_non_dichiarate_si_giudicano_sul_catalogo_della_transazione() {
    let non_tipizzata = query(
        "r10g",
        &json!({"order_by": ordine(&colonna(Some("r10g"), "g"))}),
    );
    let non_mappata = query("r10g", &json!({"order_by": ordine(&colonna(None, "h"))}));
    let confronto = query(
        "r10g",
        &json!({"filter": {"kind": "compare", "left": colonna(Some("r10g"), "h"), "operator": "eq", "right": bind()}}),
    );
    let aggiorna = mutazione(&json!({
        "type": "update",
        "target": {"schema": "public", "object": "r10g"},
        "assignments": [{"column": "id", "value": {"kind": "parameter", "name": "v"}}],
        "filter": {"kind": "compare", "left": colonna(None, "g"), "operator": "eq", "right": bind()}
    }));
    let cancella = mutazione(&json!({
        "type": "delete",
        "target": {"schema": "public", "object": "r10g"},
        "filter": {"kind": "in_list", "expression": colonna(None, "h"), "values": [bind()]}
    }));
    let seleziona = portable(&json!({
        "type": "select",
        "table": {"schema": "public", "name": "r10g"},
        "projection": {"kind": "all"},
        "order_by": [{"column": "g", "direction": "asc"}]
    }));
    let cancella_portable = portable(&json!({
        "type": "delete",
        "table": {"schema": "public", "name": "r10g"},
        "filter": {"op": "eq", "column": "h", "value": {"kind": "literal", "value": {"type": "bytes", "value": [0]}}}
    }));
    // La chiave di conflitto di un upsert si confronta (`ON CONFLICT (g)`,
    // l'`ON` di un `MERGE`): seconda lettura, 25/10.
    let upsert = mutazione(&json!({
        "type": "upsert",
        "target": {"schema": "public", "object": "r10g"},
        "columns": ["id", "g"],
        "rows": [[{"kind": "parameter", "name": "v"}, {"kind": "parameter", "name": "w"}]],
        "conflict_target": ["g"]
    }));
    let upsert_portable = portable(&json!({
        "type": "upsert",
        "table": {"schema": "public", "name": "r10g"},
        "columns": ["id"],
        "values": [[{"kind": "literal", "value": {"type": "i64", "value": 1}}]],
        "conflict_target": ["h"]
    }));
    for prodotto in PRODOTTI {
        for (forma, plan) in [
            ("UPSERT con chiave g", OperandPlan::Mutation(&upsert)),
            (
                "portable UPSERT con chiave h",
                OperandPlan::Portable(&upsert_portable),
            ),
            (
                "ORDER BY g, tabella non tipizzata",
                OperandPlan::Query(&non_tipizzata),
            ),
            (
                "ORDER BY h, colonna non mappata",
                OperandPlan::Query(&non_mappata),
            ),
            ("h = ?, colonna non mappata", OperandPlan::Query(&confronto)),
            ("UPDATE ... WHERE g = ?", OperandPlan::Mutation(&aggiorna)),
            (
                "DELETE ... WHERE h IN (?)",
                OperandPlan::Mutation(&cancella),
            ),
            ("portable ORDER BY g", OperandPlan::Portable(&seleziona)),
            (
                "portable DELETE WHERE h = ?",
                OperandPlan::Portable(&cancella_portable),
            ),
        ] {
            // Anche Oracle, con la guardia del server: il giudizio legge il
            // catalogo (regola B, 25/10).
            rifiutata(prodotto, forma, plan, GEOMETRY_SCALAR_OPERAND);
        }
    }
}

/// Oracle (con la guardia del server) e Db2 (senza, a mondo chiuso): cio
/// che la guardia non protegge resta rifiutato senza catalogo, prima di
/// eseguire: il binario canonico di una
/// geometria (`SpatialOutput`, un `BLOB` WKB) e una funzione spatial che
/// rende una geometria sotto un operatore scalare.
#[test]
fn su_oracle_e_db2_resta_rifiutato_cio_che_il_server_non_protegge() {
    let output = json!({"kind": "spatial_output", "expression": colonna(Some("r10g"), "g"), "semantics": "geometry"});
    let centroid = json!({"kind": "spatial", "function": "centroid", "arguments": [colonna(Some("r10g"), "g")]});
    let binario = |expression: &Value| json!({"kind": "cast", "expression": expression, "target_type": "binary"});
    let forme = [
        (
            "CAST(WKB AS binario) = ?",
            query(
                "r10g",
                &json!({"filter": {"kind": "compare", "left": binario(&output), "operator": "eq", "right": bind()}}),
            ),
        ),
        (
            "ORDER BY per alias di WKB",
            query(
                "r10g",
                &json!({"projection": [{"expression": output, "alias": "w"}],
                    "order_by": ordine(&colonna(None, "w"))}),
            ),
        ),
        (
            "ST_Centroid = ?",
            query(
                "r10g",
                &json!({"filter": {"kind": "compare", "left": centroid, "operator": "eq", "right": bind()}}),
            ),
        ),
    ];
    for prodotto in [ProviderKind::Oracle, ProviderKind::Db2] {
        for (forma, operation) in &forme {
            rifiutata(
                prodotto,
                forma,
                OperandPlan::Query(operation),
                GEOMETRY_SCALAR_OPERAND,
            );
        }
    }
}

/// Nessun falso rifiuto: le colonne scalari e le geometrie nei nodi spatial.
#[test]
fn le_colonne_scalari_restano_ammesse() {
    let ordinata = query(
        "r10g",
        &json!({"order_by": ordine(&colonna(Some("r10g"), "id"))}),
    );
    let spatial = query(
        "r10g",
        &json!({"filter": {"kind": "spatial", "function": "intersects", "arguments": [colonna(Some("r10g"), "g"), colonna(Some("r10g"), "h")]}}),
    );
    let senza = query(
        "plain",
        &json!({"order_by": ordine(&colonna(None, "name"))}),
    );
    let aggiorna = mutazione(&json!({
        "type": "update",
        "target": {"schema": "public", "object": "r10g"},
        "assignments": [{"column": "g", "value": {"kind": "parameter", "name": "v"}}],
        "filter": {"kind": "compare", "left": colonna(None, "id"), "operator": "eq",
            "right": {"kind": "typed_parameter", "name": "w", "parameter_type": "integer"}}
    }));
    let inserisci = mutazione(&json!({
        "type": "insert",
        "target": {"schema": "public", "object": "r10g"},
        "columns": ["id"],
        "rows": [[{"kind": "parameter", "name": "v"}]]
    }));
    let seleziona = portable(&json!({
        "type": "select",
        "table": {"schema": "public", "name": "r10g"},
        "projection": {"kind": "all"},
        "filter": {"op": "is_not_null", "column": "g"},
        "order_by": [{"column": "id", "direction": "asc"}]
    }));
    for prodotto in PRODOTTI {
        for (forma, plan) in [
            ("ORDER BY id", OperandPlan::Query(&ordinata)),
            ("ST_Intersects(g, h)", OperandPlan::Query(&spatial)),
            ("tabella senza geometrie", OperandPlan::Query(&senza)),
            ("UPDATE SET g WHERE id", OperandPlan::Mutation(&aggiorna)),
            ("INSERT", OperandPlan::Mutation(&inserisci)),
            (
                "portable g IS NOT NULL ORDER BY id",
                OperandPlan::Portable(&seleziona),
            ),
        ] {
            ammessa(prodotto, forma, plan);
        }
    }
}

/// Un provider che non legge il catalogo (il default del trait): ogni
/// riferimento in posizione di operando si rifiuta, una query senza
/// riferimenti resta.
#[test]
fn senza_catalogo_un_riferimento_si_rifiuta_dove_puo_essere_geometrico() {
    let filtrata = query(
        "plain",
        &json!({"filter": {"kind": "compare", "left": colonna(Some("plain"), "id"), "operator": "eq", "right": bind()}}),
    );
    let semplice = query("plain", &json!({}));
    for prodotto in PRODOTTI {
        let cancel = CancellationToken::new();
        let mut tx = finta(prodotto);
        tx.catalogo = None;
        let outcome = block_on(verify_operands(
            &mut tx,
            &OperandPlan::Query(&filtrata),
            &cancel,
        ));
        let error = outcome.expect_err("riferimento senza catalogo");
        assert_eq!(error.message, GEOMETRY_OPERAND_UNRESOLVED, "{prodotto:?}");
        // Un piano che non nomina relazioni non tocca colonne sconosciute.
        let senza_relazioni: QueryOperation = serde_json::from_value(json!({
            "projection": [{"expression": bind(), "alias": "k"}],
            "order_by": ordine(&colonna(None, "k")),
            "filter": null, "having": null, "row_limit": null
        }))
        .expect("query");
        block_on(verify_operands(
            &mut tx,
            &OperandPlan::Query(&senza_relazioni),
            &cancel,
        ))
        .unwrap_or_else(|error| panic!("{prodotto:?} senza relazioni: {error:?}"));
        block_on(verify_operands(
            &mut tx,
            &OperandPlan::Query(&semplice),
            &cancel,
        ))
        .unwrap_or_else(|error| panic!("{prodotto:?}: {error:?}"));
    }
}

/// La regola N vale anche in esecuzione, in ogni piano (sedicesima lettura
/// del 25/10: fino ad allora soltanto con geometrie).
#[test]
fn la_regola_dei_nomi_vale_in_esecuzione_in_ogni_piano() {
    let alias = |object: &str| {
        query(
            object,
            &json!({"projection": [
                {"expression": colonna(Some(object), "id"), "alias": "k"},
                {"expression": colonna(Some(object), "name"), "alias": "K"}
            ], "order_by": ordine(&colonna(None, "K"))}),
        )
    };
    let geometrica = alias("r10g");
    let scalare = alias("plain");
    for prodotto in PRODOTTI {
        rifiutata(
            prodotto,
            "alias k e K su una tabella con geometrie",
            OperandPlan::Query(&geometrica),
            AMBIGUOUS_NAME_SHAPE,
        );
        // Senza geometrie il legame conta per la regola F e per la regola C:
        // la forma si rifiuta. SQL Server non lega il nome secondo la
        // collation (regola A), e lo dice prima.
        let (outcome, eseguite) = esito(prodotto, OperandPlan::Query(&scalare));
        let error = outcome.expect_err("k e K");
        let expected = if prodotto == ProviderKind::Sqlserver {
            NAME_UNRESOLVED
        } else {
            AMBIGUOUS_NAME_SHAPE
        };
        assert_eq!(error.message, expected, "{prodotto:?}");
        assert_eq!(eseguite, 0, "{prodotto:?}");
    }
}

/// Regola T in esecuzione (quattordicesima lettura del 25/10): lo SDK manda
/// i valori nell'ordine dei bind, convertiti per tipo Python
/// (`py_convert`), senza guardare il `BindType`. Un intero per
/// `bind("p", BindType.STRING)` in un `UPDATE ... WHERE` (e in una query, e
/// scritto) si rifiuta prima di eseguire, anche dove il piano non ha
/// operandi da leggere; un valore posizionale in piu non si lega.
#[test]
fn un_valore_fuori_dal_tipo_dichiarato_si_rifiuta_prima_di_eseguire() {
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::spatial_operand::BIND_LAYOUT_MISMATCH;
    use plenora_database_core::typed_parameter::TYPED_PARAMETER_MISMATCH;
    let typed = json!({"kind": "typed_parameter", "name": "p", "parameter_type": "string"});
    let target = json!({"catalog": null, "schema": "public", "object": "plain"});
    let filter =
        json!({"kind": "compare", "left": colonna(None, "name"), "operator": "eq", "right": typed});
    let update = mutazione(&json!({
        "type": "update", "target": target,
        "assignments": [{"column": "id", "value": {"kind": "parameter", "name": "q"}}],
        "filter": filter
    }));
    let insert = mutazione(&json!({
        "type": "insert", "target": target, "columns": ["name"], "rows": [[typed]]
    }));
    let select = query("plain", &json!({"filter": filter}));
    let names = |plan: &OperandPlan<'_>| -> Vec<String> {
        match plan {
            OperandPlan::Mutation(MutationOperation::Update(_)) => vec!["q".into(), "p".into()],
            _ => vec!["p".into()],
        }
    };
    let run = |prodotto, plan: OperandPlan<'_>, values: Vec<ParameterValue>| {
        let mut tx = finta(prodotto);
        let cancel = CancellationToken::new();
        let statement = Statement::new("SQL").with_params(values);
        let outcome = match plan {
            OperandPlan::Query(_) => block_on(query_judged(
                &mut tx,
                &statement,
                Some(&plan),
                &names(&plan),
                &cancel,
            ))
            .map(|_| ()),
            _ => block_on(execute_judged(
                &mut tx,
                &statement,
                Some(&plan),
                &names(&plan),
                &cancel,
            ))
            .map(|_| ()),
        };
        (outcome, tx.eseguite)
    };
    for prodotto in PRODOTTI {
        for (forma, plan) in [
            ("update", OperandPlan::Mutation(&update)),
            ("insert", OperandPlan::Mutation(&insert)),
            ("select", OperandPlan::Query(&select)),
        ] {
            let mut values = vec![ParameterValue::I64(1)];
            if forma == "update" {
                values.insert(0, ParameterValue::I32(7));
            }
            let (outcome, eseguite) = run(prodotto, plan, values.clone());
            let error = outcome.expect_err(forma);
            assert_eq!(
                error.category,
                ErrorCategory::InvalidPlan,
                "{prodotto:?} {forma}"
            );
            assert_eq!(error.phase, ErrorPhase::Prepare, "{prodotto:?} {forma}");
            assert_eq!(
                error.message, TYPED_PARAMETER_MISMATCH,
                "{prodotto:?} {forma}"
            );
            assert_eq!(eseguite, 0, "{prodotto:?} {forma}: eseguita");
            let last = values.len() - 1;
            values[last] = ParameterValue::String("1".to_owned());
            let (outcome, eseguite) = run(prodotto, plan, values.clone());
            outcome.unwrap_or_else(|error| panic!("{prodotto:?} {forma}: {error:?}"));
            assert_eq!(eseguite, 1, "{prodotto:?} {forma}");
            values.push(ParameterValue::String("in piu".to_owned()));
            let (outcome, eseguite) = run(prodotto, plan, values);
            let error = outcome.expect_err("valore in piu");
            assert_eq!(error.message, BIND_LAYOUT_MISMATCH, "{prodotto:?} {forma}");
            assert_eq!(eseguite, 0, "{prodotto:?} {forma}: eseguita");
        }
    }
}
