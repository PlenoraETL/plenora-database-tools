//! Il giudizio degli operandi geometrici sul catalogo di Db2
//! (`crate::operand_catalog`), sul server: le forme della guardia misurate
//! (`tests/fixtures/operandi_server_guard.json`), che su Db2 passano e
//! confrontano i byte, si rifiutano prima del server; le stesse forme su una
//! colonna non geometrica arrivano al server e rendono le righe dello SQL
//! eseguito senza giudizio; i filtri e gli ordinamenti scalari rendono le
//! righe attese, anche attraverso viste, alias, alias di alias e alias
//! pubblici; le geometrie si rifiutano attraverso le stesse relazioni, con
//! i sottotipi spatial e la colonna `BLOB` calcolata di una vista.

use crate::{Db2Config, Db2Provider, Db2TlsMode};
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, OrderBy, ProviderKind, ReadOperation, SortDirection,
};
use plenora_database_core::portable::{eq, select, Direction};
use plenora_database_core::provider::{ParameterBag, ParameterValue, Provider, SecretString};
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::spatial_operand::{
    OperandPlan, AMBIGUOUS_NAME_SHAPE, GEOMETRY_OPERAND_UNRESOLVED,
};
use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase};
use plenora_database_sql::{Dialect, DialectCapabilities, Renderer};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const FIXTURE_GUARDIA: &str = include_str!("../../../tests/fixtures/operandi_server_guard.json");

const TABELLA: &str = "SONDE_OPERANDI";

/// Un budget per ogni operazione: la durata del budget e quella di una
/// operazione, non dell'intero test.
fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget operandi Db2")
}

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn provider() -> Db2Provider {
    Db2Provider::new(
        Db2Config::new(
            environment("PLENORA_DB2_HOST", "db2"),
            environment("PLENORA_DB2_DATABASE", "plenora"),
            environment("PLENORA_DB2_USER", "db2inst1"),
        )
        .with_port(
            environment("PLENORA_DB2_PORT", "50000")
                .parse()
                .expect("porta Db2 operandi live"),
        )
        .with_tls_mode(Db2TlsMode::Disable),
    )
    .expect("provider Db2 operandi live")
}

/// La preparazione, su una connessione in autocommit: la tabella del fixture, una tabella con i sottotipi
/// spatial e un omonimo di `ST_GEOMETRY` di un altro schema, una vista con
/// una colonna resa da `ST_CENTROID`, alias, alias di alias, un alias
/// pubblico e un alias verso un oggetto assente.
const PREPARAZIONE: [&str; 12] = [
    // Le tabelle temporanee di una subquery con una colonna spatial non
    // stanno nello spazio temporaneo da 4K (SQL1585N).
    "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END; \
     EXECUTE IMMEDIATE 'CREATE SYSTEM TEMPORARY TABLESPACE PLENORA_TMP32K PAGESIZE 32K \
     BUFFERPOOL PLENORA_BP32K'; END",
    "CREATE TABLE PLENORA.SONDE_OPERANDI (ID INTEGER, NAME VARCHAR(20), \
     B VARCHAR(16) FOR BIT DATA, G ST_GEOMETRY) IN PLENORA_TS8K",
    "INSERT INTO PLENORA.SONDE_OPERANDI VALUES \
     (1, 'a', BX'01', ST_GEOMETRY('POINT (1 2)', 4326)), \
     (2, 'b', BX'02', ST_GEOMETRY('POINT (3 4)', 4326))",
    "CREATE DISTINCT TYPE SONDE_TIPI.ST_GEOMETRY AS INTEGER WITH COMPARISONS",
    "CREATE TABLE PLENORA.SONDE_OPERANDI_TIPI (ID INTEGER, P ST_POINT, S ST_SURFACE, \
     X SONDE_TIPI.ST_GEOMETRY) IN PLENORA_TS32K",
    "INSERT INTO PLENORA.SONDE_OPERANDI_TIPI VALUES \
     (1, ST_POINT('POINT (1 2)', 4326), NULL, SONDE_TIPI.ST_GEOMETRY(10)), \
     (2, ST_POINT('POINT (3 4)', 4326), NULL, SONDE_TIPI.ST_GEOMETRY(20))",
    "CREATE VIEW PLENORA.SONDE_OPERANDI_V AS \
     SELECT ID, NAME, G, ST_CENTROID(G) AS C FROM PLENORA.SONDE_OPERANDI",
    "CREATE ALIAS PLENORA.SONDE_OPERANDI_A FOR PLENORA.SONDE_OPERANDI",
    "CREATE ALIAS PLENORA.SONDE_OPERANDI_AA FOR PLENORA.SONDE_OPERANDI_A",
    "CREATE ALIAS PLENORA.SONDE_OPERANDI_AV FOR PLENORA.SONDE_OPERANDI_V",
    "CREATE ALIAS PLENORA.SONDE_OPERANDI_AX FOR PLENORA.SONDE_OPERANDI_ASSENTE",
    "CREATE PUBLIC ALIAS SONDE_OPERANDI_PA FOR PLENORA.SONDE_OPERANDI",
];

const PULIZIA: [&str; 10] = [
    "DROP PUBLIC ALIAS SONDE_OPERANDI_PA",
    "DROP ALIAS PLENORA.SONDE_OPERANDI_AX",
    "DROP ALIAS PLENORA.SONDE_OPERANDI_AV",
    "DROP ALIAS PLENORA.SONDE_OPERANDI_AA",
    "DROP ALIAS PLENORA.SONDE_OPERANDI_A",
    "DROP VIEW PLENORA.SONDE_OPERANDI_V",
    "DROP TABLE PLENORA.SONDE_OPERANDI_TIPI",
    "DROP TABLE PLENORA.SONDE_OPERANDI",
    "DROP TYPE SONDE_TIPI.ST_GEOMETRY",
    "DROP SCHEMA SONDE_TIPI RESTRICT",
];

async fn pulisci(provider: &Db2Provider, secret: &SecretString, cancellation: &CancellationToken) {
    // Una pulizia di un'esecuzione precedente: gli oggetti possono mancare.
    for sql in PULIZIA {
        let _ = provider.run_ddl(secret, sql, cancellation).await;
    }
}

/// `SELECT T.ID FROM PLENORA.SONDE_OPERANDI T` con il piano della forma sopra.
fn piano_della_forma(piano: &Value) -> Value {
    let mut operation = json!({
        "source": {"object": {"catalog": null, "schema": "PLENORA", "object": TABELLA}, "alias": "T"},
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

/// Il piano con `G` (e il suo alias di uscita) sostituita da `colonna`.
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

/// Il valore di un bind tipato del piano: binario, testo, intero.
fn valore_del_bind(piano: &Value, name: &str) -> ParameterValue {
    let mut stack = vec![piano];
    while let Some(node) = stack.pop() {
        match node {
            Value::Object(map) => {
                if map.get("kind").and_then(Value::as_str) == Some("typed_parameter")
                    && map.get("name").and_then(Value::as_str) == Some(name)
                {
                    return match map["parameter_type"].as_str() {
                        Some("binary") => ParameterValue::Bytes(vec![1]),
                        Some("string") => ParameterValue::String("a".to_owned()),
                        Some("integer") => ParameterValue::I64(1),
                        other => panic!("tipo di bind del fixture {other:?}"),
                    };
                }
                stack.extend(map.values());
            }
            Value::Array(items) => stack.extend(items),
            _ => {}
        }
    }
    panic!("bind {name} assente dal piano");
}

/// Lo SQL che il renderer del repository rende per il piano, con i valori.
fn resa(
    plan: &Value,
    valori: Option<&[ParameterValue]>,
) -> (QueryOperation, String, Statement, Vec<String>) {
    let operation: QueryOperation = serde_json::from_value(plan.clone()).expect("piano");
    let rendered = Renderer::new(
        Dialect::Db2,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(ProviderKind::Db2)
    .render_query(&operation)
    .expect("resa Db2");
    let values = valori.map_or_else(
        || {
            rendered
                .binds
                .iter()
                .map(|bind| valore_del_bind(plan, &bind.name))
                .collect()
        },
        <[ParameterValue]>::to_vec,
    );
    let statement = Statement::new(rendered.sql.clone()).with_params(values);
    let binds = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect();
    (operation, rendered.sql, statement, binds)
}

/// Le righe come elenco della colonna `ID` (o `V`, l'uscita delle forme
/// che non proiettano `ID`), nell'ordine reso.
fn identificativi(rows: &[plenora_database_core::Row]) -> Vec<String> {
    rows.iter()
        .map(|row| match row.get("ID").or_else(|| row.get("V")) {
            Some(ParameterValue::I32(value)) => value.to_string(),
            Some(ParameterValue::I64(value)) => value.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Esegue il piano attraverso il percorso giudicato dell'esecuzione
/// (`facade::query_judged`, come lo SDK, la facade e la CLI portable), e
/// lo stesso SQL senza giudizio, nella stessa transazione.
async fn esegui(
    provider: &Db2Provider,
    secret: &SecretString,
    plan: &Value,
    valori: Option<&[ParameterValue]>,
    cancellation: &CancellationToken,
) -> (
    String,
    plenora_database_core::Result<Vec<String>>,
    plenora_database_core::Result<Vec<String>>,
) {
    let (operation, sql, statement, binds) = resa(plan, valori);
    let mut transaction = provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget(),
            cancellation,
        )
        .await
        .expect("transazione operandi Db2");
    let judged = plenora_database_core::facade::query_judged(
        transaction.as_mut(),
        &statement,
        Some(&OperandPlan::Query(&operation)),
        &binds,
        cancellation,
    )
    .await
    .map(|rows| identificativi(&rows));
    let direct = transaction
        .query(&statement, cancellation)
        .await
        .map(|rows| identificativi(&rows));
    let _ = transaction.rollback(cancellation).await;
    (sql, judged, direct)
}

/// Il rifiuto del giudizio, prima del server: `unsupported`, fase di
/// preparazione, con uno dei motivi del giudizio.
fn rifiuto_del_giudizio(error: &plenora_database_core::DatabaseError, motivo: &str) -> bool {
    error.category == ErrorCategory::Unsupported
        && error.phase == ErrorPhase::Prepare
        && error.message == motivo
}

/// Il codice del driver di un codice misurato dal client del server:
/// `SQL0440N` e `codice -440` nel messaggio redatto del provider.
fn codice_del_driver(codice: &Value) -> Option<String> {
    let numero = codice.as_str()?.strip_prefix("SQL")?.strip_suffix('N')?;
    Some(format!("codice -{}", numero.trim_start_matches('0')))
}

fn colonna(relation: &str, field: &str) -> Value {
    json!({"kind": "column", "column": {"relation": relation, "field": field}})
}

/// `SELECT T.ID FROM <schema>.<object> T` con `extra`.
fn su(schema: Option<&str>, object: &str, extra: &Value) -> Value {
    let mut operation = json!({
        "source": {"object": {"catalog": null, "schema": schema, "object": object}, "alias": "T"},
        "projection": [{"expression": colonna("T", "ID"), "alias": "ID"}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("extra") {
        operation[key] = value.clone();
    }
    operation
}

fn uguale(field: &str) -> Value {
    json!({"filter": {"kind": "compare", "left": colonna("T", field), "operator": "eq", "right": {"kind": "parameter", "name": "p"}}})
}

fn ordinato(field: &str, direction: &str) -> Value {
    json!({"order_by": [{"expression": colonna("T", field), "direction": direction}]})
}

/// Un caso esplicito: il piano, i valori e l'esito atteso (le righe, o il
/// motivo del rifiuto del giudizio).
struct Caso {
    nome: &'static str,
    piano: Value,
    valori: Vec<ParameterValue>,
    atteso: Result<Vec<i64>, &'static str>,
}

#[allow(clippy::too_many_lines)]
fn casi() -> Vec<Caso> {
    let uno = || vec![ParameterValue::I32(1)];
    let byte = || vec![ParameterValue::Bytes(vec![1])];
    let caso = |nome, piano, valori, atteso| Caso {
        nome,
        piano,
        valori,
        atteso,
    };
    let tabella = Some("PLENORA");
    vec![
        caso(
            "ID = ? sulla tabella",
            su(tabella, TABELLA, &uguale("ID")),
            uno(),
            Ok(vec![1]),
        ),
        caso(
            "ORDER BY ID DESC sulla tabella",
            su(tabella, TABELLA, &ordinato("ID", "desc")),
            Vec::new(),
            Ok(vec![2, 1]),
        ),
        caso(
            "ORDER BY NAME sulla tabella",
            su(tabella, TABELLA, &ordinato("NAME", "asc")),
            Vec::new(),
            Ok(vec![1, 2]),
        ),
        caso(
            "NAME LIKE ? sulla tabella",
            su(
                tabella,
                TABELLA,
                &json!({"filter": {"kind": "like", "expression": colonna("T", "NAME"), "pattern": {"kind": "parameter", "name": "p"}, "negated": false, "case_insensitive": false}}),
            ),
            vec![ParameterValue::String("b%".to_owned())],
            Ok(vec![2]),
        ),
        caso(
            "G = ? sulla tabella",
            su(tabella, TABELLA, &uguale("G")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "id = ? (soltanto senza maiuscole) sulla tabella",
            su(tabella, TABELLA, &uguale("id")),
            uno(),
            Err(AMBIGUOUS_NAME_SHAPE),
        ),
        caso(
            "ID = ? sulla vista",
            su(tabella, "SONDE_OPERANDI_V", &uguale("ID")),
            uno(),
            Ok(vec![1]),
        ),
        caso(
            "ORDER BY ID DESC sulla vista",
            su(tabella, "SONDE_OPERANDI_V", &ordinato("ID", "desc")),
            Vec::new(),
            Ok(vec![2, 1]),
        ),
        caso(
            "G = ? sulla vista",
            su(tabella, "SONDE_OPERANDI_V", &uguale("G")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "C (BLOB di ST_CENTROID) = ? sulla vista",
            su(tabella, "SONDE_OPERANDI_V", &uguale("C")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "ID = ? sull'alias",
            su(tabella, "SONDE_OPERANDI_A", &uguale("ID")),
            uno(),
            Ok(vec![1]),
        ),
        caso(
            "ORDER BY ID DESC sull'alias di alias",
            su(tabella, "SONDE_OPERANDI_AA", &ordinato("ID", "desc")),
            Vec::new(),
            Ok(vec![2, 1]),
        ),
        caso(
            "G = ? sull'alias",
            su(tabella, "SONDE_OPERANDI_A", &uguale("G")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "G = ? sull'alias di alias",
            su(tabella, "SONDE_OPERANDI_AA", &uguale("G")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "C = ? sull'alias della vista",
            su(tabella, "SONDE_OPERANDI_AV", &uguale("C")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "ID = ? sull'alias pubblico, senza schema",
            su(None, "SONDE_OPERANDI_PA", &uguale("ID")),
            uno(),
            Ok(vec![1]),
        ),
        caso(
            "G = ? sull'alias pubblico, senza schema",
            su(None, "SONDE_OPERANDI_PA", &uguale("G")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "ID = ? su un alias verso un oggetto assente",
            su(tabella, "SONDE_OPERANDI_AX", &uguale("ID")),
            uno(),
            Err(GEOMETRY_OPERAND_UNRESOLVED),
        ),
        caso(
            "P (ST_POINT) = ?",
            su(tabella, "SONDE_OPERANDI_TIPI", &uguale("P")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "S (ST_SURFACE) = ?",
            su(tabella, "SONDE_OPERANDI_TIPI", &uguale("S")),
            byte(),
            Err(GEOMETRY_SCALAR_OPERAND),
        ),
        caso(
            "ORDER BY X (omonimo di ST_GEOMETRY su INTEGER) DESC",
            su(tabella, "SONDE_OPERANDI_TIPI", &ordinato("X", "desc")),
            Vec::new(),
            Ok(vec![2, 1]),
        ),
        caso(
            "ID = ? sulla tabella dei sottotipi",
            su(tabella, "SONDE_OPERANDI_TIPI", &uguale("ID")),
            vec![ParameterValue::I32(2)],
            Ok(vec![2]),
        ),
    ]
}

/// Il giudizio degli operandi sul catalogo Db2 nella transazione
/// (`TransactionScope::operand_catalog`) e nella lettura del provider.
#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_operand_catalog_judges_geometry_and_admits_scalars() {
    let provider = provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    pulisci(&provider, &secret, &cancellation).await;
    for (index, sql) in PREPARAZIONE.iter().enumerate() {
        provider
            .run_ddl(&secret, sql, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("preparazione operandi Db2 {index}: {error}"));
    }

    // Le forme della guardia: rifiutate prima del server; i controlli
    // arrivano al server e rendono le righe dello SQL senza giudizio.
    let fixture: Value = serde_json::from_str(FIXTURE_GUARDIA).expect("fixture della guardia");
    let forms = fixture["forme"].as_array().expect("forme");
    assert!(!forms.is_empty());
    let ragioni = [GEOMETRY_SCALAR_OPERAND, GEOMETRY_OPERAND_UNRESOLVED];
    for form in forms {
        let name = form["forma"].as_str().expect("nome della forma");
        let plan = piano_della_forma(&form["piano"]);
        let (sql, judged, direct) = esegui(&provider, &secret, &plan, None, &cancellation).await;
        assert_eq!(
            sql,
            form["sql"]["db2"]["sql"].as_str().expect("sql"),
            "{name}"
        );
        // Lo SQL senza giudizio fa cio che il server ha misurato.
        assert_eq!(
            direct.is_ok(),
            form["esito"]["db2"].get("errore").is_none(),
            "{name}: {direct:?}"
        );
        if form.get("ammessa").is_some() {
            let rows = judged.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(Ok(rows), direct, "{name}");
        } else {
            // Rifiutata dal giudizio prima del server, o, dove il giudizio
            // non la dice un operando (`UPPER(G)` in una projection che non
            // si confronta), dal server con uno dei codici misurati.
            let error = judged.expect_err(name);
            let misurati = form["esito"]["db2"]["errore"]
                .as_array()
                .map(|codici| {
                    codici
                        .iter()
                        .filter_map(codice_del_driver)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            // Regola F (`spatial_operand::OPERAND_FAMILY_MISMATCH`, rilettura
            // 13): `UPPER(G)` converte una geometria in testo, e si rifiuta
            // anche fuori da un confronto, prima del server.
            let conversione = error.category == ErrorCategory::InvalidPlan
                && error.phase == ErrorPhase::Prepare
                && error.message == plenora_database_core::spatial_operand::OPERAND_FAMILY_MISMATCH;
            assert!(
                ragioni
                    .iter()
                    .any(|motivo| rifiuto_del_giudizio(&error, motivo))
                    || conversione
                    || misurati.iter().any(|codice| error.message.contains(codice)),
                "{name}: {error}"
            );
        }
        if let Some(column) = form["controllo"].as_str() {
            let control = con_colonna(&plan, column);
            let (_, judged, direct) =
                esegui(&provider, &secret, &control, None, &cancellation).await;
            let rows = judged.unwrap_or_else(|error| panic!("{name} su {column}: {error}"));
            assert_eq!(Ok(rows), direct, "{name} su {column}");
        }
    }

    // I casi espliciti: righe attese e rifiuti, attraverso viste e alias.
    for caso in casi() {
        let (_, judged, _) = esegui(
            &provider,
            &secret,
            &caso.piano,
            Some(&caso.valori),
            &cancellation,
        )
        .await;
        match caso.atteso {
            Ok(rows) => assert_eq!(
                judged.unwrap_or_else(|error| panic!("{}: {error}", caso.nome)),
                rows.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "{}",
                caso.nome
            ),
            Err(motivo) => {
                let error = judged.expect_err(caso.nome);
                assert!(
                    rifiuto_del_giudizio(&error, motivo),
                    "{}: {error}",
                    caso.nome
                );
            }
        }
    }

    // Lo statement portable (facade, CLI portable).
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione portable operandi Db2");
    let scalare = select(TABELLA, vec!["ID"])
        .schema("PLENORA")
        .where_(eq("ID", ParameterValue::I32(2)))
        .order_by("ID", Direction::Asc)
        .into_statement();
    let rows = plenora_database_core::facade::execute_portable_returning(
        transaction.as_mut(),
        &scalare,
        &cancellation,
    )
    .await
    .expect("portable scalare Db2");
    assert_eq!(identificativi(&rows), vec!["2".to_owned()]);
    let geometrico = select(TABELLA, vec!["ID"])
        .schema("PLENORA")
        .where_(eq("G", ParameterValue::Bytes(vec![1])))
        .into_statement();
    let error = plenora_database_core::facade::execute_portable_returning(
        transaction.as_mut(),
        &geometrico,
        &cancellation,
    )
    .await
    .expect_err("portable geometrico Db2");
    assert!(
        rifiuto_del_giudizio(&error, GEOMETRY_SCALAR_OPERAND),
        "{error}"
    );
    let _ = transaction.rollback(&cancellation).await;

    // La lettura del provider: un filtro su un sottotipo spatial e sulla
    // colonna BLOB calcolata di una vista si rifiuta; sull'ID passa.
    let lettura = |object: &str, field: &str| ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: object.to_owned(),
        },
        projection: vec!["ID".to_owned()],
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: field.to_owned(),
            parameter: "p".to_owned(),
        }),
        declared_crs: Vec::new(),
    };
    let bytes = ParameterBag::new(BTreeMap::from([(
        "p".to_owned(),
        ParameterValue::Bytes(vec![1]),
    )]));
    for (object, field) in [
        ("SONDE_OPERANDI_TIPI", "S"),
        ("SONDE_OPERANDI_TIPI", "P"),
        ("SONDE_OPERANDI_V", "C"),
        ("SONDE_OPERANDI_V", "G"),
    ] {
        let error = provider
            .read(
                &secret,
                &lettura(object, field),
                &bytes,
                &budget(),
                &cancellation,
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("lettura {object}.{field} ammessa"));
        assert!(
            rifiuto_del_giudizio(&error, GEOMETRY_SCALAR_OPERAND),
            "{object}.{field}: {error}"
        );
    }
    let id_uno = ParameterBag::new(BTreeMap::from([("p".to_owned(), ParameterValue::I32(1))]));
    for object in [TABELLA, "SONDE_OPERANDI_V", "SONDE_OPERANDI_TIPI"] {
        let mut stream = provider
            .read(
                &secret,
                &lettura(object, "ID"),
                &id_uno,
                &budget(),
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("lettura {object}.ID: {error}"));
        let mut rows = 0;
        while let Some(batch) = stream
            .next_batch(&cancellation)
            .await
            .unwrap_or_else(|error| panic!("lettura {object}.ID: {error}"))
        {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 1, "{object}");
    }

    pulisci(&provider, &secret, &cancellation).await;
}

/// `T.<campo> = CAST(? AS VARCHAR(32672) FOR BIT DATA)`: il bind binario
/// tipato, come le forme del fixture.
fn uguale_binario(field: &str) -> Value {
    json!({"filter": {"kind": "compare", "left": colonna("T", field), "operator": "eq",
        "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "binary"}}})
}

/// La preparazione della regola B: due poligoni uguali per `ST_EQUALS` con i
/// vertici in ordine diverso, e una vista che li rende binari in ogni forma
/// misurata il 25/10 (`CAST(G AS VARCHAR FOR BIT DATA)`, `CAST(G AS
/// VARBINARY)`, `CAST(ST_ASBINARY(G) AS VARCHAR FOR BIT DATA)`, un tipo
/// distinto su `VARBINARY`, il `BLOB` di `ST_CENTROID`), una colonna `FOR BIT
/// DATA` che ripete quella della tabella, il WKT.
const PREPARAZIONE_DERIVATE: [&str; 5] = [
    "CREATE DISTINCT TYPE SONDE_DERIVATE_T.WKB AS VARBINARY(4000) WITH COMPARISONS",
    "CREATE TABLE PLENORA.SONDE_DERIVATE (ID INTEGER, B VARCHAR(16) FOR BIT DATA, \
     G ST_GEOMETRY) IN PLENORA_TS8K",
    "INSERT INTO PLENORA.SONDE_DERIVATE VALUES \
     (3, BX'03', ST_GEOMETRY('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)), \
     (4, BX'04', ST_GEOMETRY('POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))', 4326))",
    "CREATE VIEW PLENORA.SONDE_DERIVATE_V AS SELECT ID, \
     CAST(G AS VARCHAR(4000) FOR BIT DATA) AS CVB, CAST(G AS VARBINARY(4000)) AS VB, \
     CAST(ST_ASBINARY(G) AS VARCHAR(4000) FOR BIT DATA) AS WKBV, \
     SONDE_DERIVATE_T.WKB(CAST(ST_ASBINARY(G) AS VARBINARY(4000))) AS DT, \
     ST_CENTROID(G) AS C, B, ST_ASTEXT(G) AS X FROM PLENORA.SONDE_DERIVATE",
    "CREATE ALIAS PLENORA.SONDE_DERIVATE_A FOR PLENORA.SONDE_DERIVATE_V",
];

const PULIZIA_DERIVATE: [&str; 5] = [
    "DROP ALIAS PLENORA.SONDE_DERIVATE_A",
    "DROP VIEW PLENORA.SONDE_DERIVATE_V",
    "DROP TABLE PLENORA.SONDE_DERIVATE",
    "DROP TYPE SONDE_DERIVATE_T.WKB",
    "DROP SCHEMA SONDE_DERIVATE_T RESTRICT",
];

/// Regola B (`spatial_operand::catalog_column_is_geometric`), rilettura del
/// 25/10 (F2): fino a 1bda911 il catalogo diceva geometriche soltanto le
/// colonne `SYSIBM.BLOB` calcolate di una vista; `VARCHAR FOR BIT DATA`,
/// `VARBINARY` e un tipo distinto che vi arriva si giudicavano scalari, e
/// `=`, `ORDER BY`, `DISTINCT` ne confrontavano i byte. Lo SQL senza giudizio
/// `WKBV = ?` con il WKB del primo poligono rende soltanto il primo, mentre
/// `ST_EQUALS` dice uguali i due. Ora si rifiutano prima del server, anche
/// attraverso un alias e nella lettura del provider; la colonna `FOR BIT
/// DATA` della tabella, l'id e il WKT della vista restano.
#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_binary_columns_of_derived_relations_are_judged_geometric() {
    let provider = provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    for sql in PULIZIA_DERIVATE {
        let _ = provider.run_ddl(&secret, sql, &cancellation).await;
    }
    for (index, sql) in PREPARAZIONE_DERIVATE.iter().enumerate() {
        provider
            .run_ddl(&secret, sql, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("preparazione regola B Db2 {index}: {error}"));
    }
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione regola B Db2");
    let equal = transaction
        .query(
            &Statement::new(
                "SELECT A.ID FROM PLENORA.SONDE_DERIVATE A, PLENORA.SONDE_DERIVATE B \
                 WHERE A.ID = 3 AND B.ID = 4 AND ST_EQUALS(A.G, B.G) = 1",
            ),
            &cancellation,
        )
        .await
        .expect("ST_EQUALS regola B Db2");
    assert_eq!(identificativi(&equal), vec!["3".to_owned()]);
    // F2: lo SQL senza giudizio confronta i byte (il WKB del primo poligono,
    // dalla stessa vista: il secondo, uguale, manca); il giudizio lo rifiuta.
    let silent = transaction
        .query(
            &Statement::new(
                "SELECT T.ID FROM PLENORA.SONDE_DERIVATE_V T WHERE T.WKBV = \
                 (SELECT U.WKBV FROM PLENORA.SONDE_DERIVATE_V U WHERE U.ID = 3)",
            ),
            &cancellation,
        )
        .await
        .expect("senza giudizio regola B Db2");
    assert_eq!(
        identificativi(&silent),
        vec!["3".to_owned()],
        "il server confronta i byte: il secondo poligono, uguale, manca"
    );
    let _ = transaction.rollback(&cancellation).await;
    let bytes = vec![ParameterValue::Bytes(vec![1])];
    let tabella = Some("PLENORA");
    let (_, judged, _) = esegui(
        &provider,
        &secret,
        &su(tabella, "SONDE_DERIVATE_V", &uguale_binario("WKBV")),
        Some(&bytes),
        &cancellation,
    )
    .await;
    let error = judged.expect_err("WKBV = ? giudicato");
    assert!(
        rifiuto_del_giudizio(&error, GEOMETRY_SCALAR_OPERAND),
        "{error}"
    );

    let distinct = |field: &str| {
        json!({"distinct": true,
            "projection": [{"expression": colonna("T", field), "alias": "V"}]})
    };
    let rifiutati = [
        (
            "CVB = ?",
            su(tabella, "SONDE_DERIVATE_V", &uguale_binario("CVB")),
            bytes.clone(),
        ),
        (
            "VB = ?",
            su(tabella, "SONDE_DERIVATE_V", &uguale_binario("VB")),
            bytes.clone(),
        ),
        (
            "DT = ? (distinto su VARBINARY)",
            su(tabella, "SONDE_DERIVATE_V", &uguale_binario("DT")),
            bytes.clone(),
        ),
        (
            "C = ? (BLOB di ST_CENTROID)",
            su(tabella, "SONDE_DERIVATE_V", &uguale_binario("C")),
            bytes.clone(),
        ),
        (
            "B della vista = ?",
            su(tabella, "SONDE_DERIVATE_V", &uguale_binario("B")),
            bytes.clone(),
        ),
        (
            "ORDER BY CVB",
            su(tabella, "SONDE_DERIVATE_V", &ordinato("CVB", "asc")),
            Vec::new(),
        ),
        (
            "ORDER BY WKBV",
            su(tabella, "SONDE_DERIVATE_V", &ordinato("WKBV", "asc")),
            Vec::new(),
        ),
        (
            "DISTINCT VB",
            su(tabella, "SONDE_DERIVATE_V", &distinct("VB")),
            Vec::new(),
        ),
        (
            "WKBV = ? sull'alias",
            su(tabella, "SONDE_DERIVATE_A", &uguale_binario("WKBV")),
            bytes.clone(),
        ),
    ];
    for (nome, piano, valori) in rifiutati {
        let (_, judged, _) = esegui(&provider, &secret, &piano, Some(&valori), &cancellation).await;
        let error = judged.expect_err(nome);
        assert!(
            rifiuto_del_giudizio(&error, GEOMETRY_SCALAR_OPERAND),
            "{nome}: {error}"
        );
    }
    // La colonna FOR BIT DATA della tabella resta scalare: il giudizio la
    // ammette e lo SQL arriva al server.
    let (_, judged, direct) = esegui(
        &provider,
        &secret,
        &su(tabella, "SONDE_DERIVATE", &uguale_binario("B")),
        Some(&bytes),
        &cancellation,
    )
    .await;
    let rows = judged.unwrap_or_else(|error| panic!("B = ? sulla tabella: {error}"));
    assert_eq!(Ok(rows), direct, "B = ? sulla tabella");
    let ammessi = [
        (
            "ID = ? sulla vista",
            su(tabella, "SONDE_DERIVATE_V", &uguale("ID")),
            vec![ParameterValue::I32(3)],
            vec!["3"],
        ),
        (
            "ORDER BY ID DESC sull'alias",
            su(tabella, "SONDE_DERIVATE_A", &ordinato("ID", "desc")),
            Vec::new(),
            vec!["4", "3"],
        ),
    ];
    for (nome, piano, valori, attese) in ammessi {
        let (_, judged, direct) =
            esegui(&provider, &secret, &piano, Some(&valori), &cancellation).await;
        let rows = judged.unwrap_or_else(|error| panic!("{nome}: {error}"));
        assert_eq!(rows, attese, "{nome}");
        assert_eq!(Ok(rows), direct, "{nome}");
    }

    // La lettura del provider: la colonna FOR BIT DATA e il tipo distinto
    // della vista si rifiutano, con il giudizio sul catalogo.
    let lettura = |field: &str| ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: "SONDE_DERIVATE_V".to_owned(),
        },
        projection: vec!["ID".to_owned()],
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: field.to_owned(),
            parameter: "p".to_owned(),
        }),
        declared_crs: Vec::new(),
    };
    let parametri = ParameterBag::new(BTreeMap::from([("p".to_owned(), bytes[0].clone())]));
    for field in ["WKBV", "CVB", "VB"] {
        let error = provider
            .read(
                &secret,
                &lettura(field),
                &parametri,
                &budget(),
                &cancellation,
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("lettura {field} ammessa"));
        assert!(
            rifiuto_del_giudizio(&error, GEOMETRY_SCALAR_OPERAND),
            "lettura {field}: {error}"
        );
    }
    for sql in PULIZIA_DERIVATE {
        let _ = provider.run_ddl(&secret, sql, &cancellation).await;
    }
}

/// La preparazione della regola B per colonna: una tabella con una colonna
/// generata dal server con il WKB della geometria (`W`), una binaria
/// memorizzata (`B`) e una generata numerica (`N`); due poligoni uguali per
/// `ST_EQUALS` con i vertici in ordine diverso.
const PREPARAZIONE_GENERATE: [&str; 2] = [
    "CREATE TABLE PLENORA.SONDE_GENERATE (ID INTEGER NOT NULL, G ST_GEOMETRY, \
     B VARBINARY(2000), \
     W VARBINARY(2000) GENERATED ALWAYS AS (CAST(ST_ASBINARY(G) AS VARBINARY(2000))), \
     N INTEGER GENERATED ALWAYS AS (ID * 10)) IN PLENORA_TS8K",
    "INSERT INTO PLENORA.SONDE_GENERATE (ID, G, B) VALUES \
     (3, ST_GEOMETRY('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326), BX'03'), \
     (4, ST_GEOMETRY('POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))', 4326), BX'04')",
];

const PULIZIA_GENERATE: [&str; 1] = ["DROP TABLE PLENORA.SONDE_GENERATE"];

/// Regola B per colonna (rilettura 13, R13-1): fino al 25/10 l'origine era
/// della relazione, e la colonna generata binaria di una tabella
/// (`SYSCAT.COLUMNS.GENERATED` `A`) restava scalare. Lo SQL senza giudizio
/// `W = <WKB del primo poligono>` rende soltanto il primo, mentre `ST_EQUALS`
/// dice uguali i due: un risultato sbagliato senza errore. Ora `W` si giudica
/// geometrica (rifiuto prima del server) nell'esecuzione giudicata e nella
/// lettura del provider; la colonna binaria memorizzata `B`, la generata
/// numerica `N` e l'id restano.
#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_server_computed_binary_columns_are_judged_geometric() {
    let provider = provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    for sql in PULIZIA_GENERATE {
        let _ = provider.run_ddl(&secret, sql, &cancellation).await;
    }
    for (index, sql) in PREPARAZIONE_GENERATE.iter().enumerate() {
        provider
            .run_ddl(&secret, sql, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("preparazione colonne generate Db2 {index}: {error}"));
    }
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione colonne generate Db2");
    let equal = transaction
        .query(
            &Statement::new(
                "SELECT A.ID FROM PLENORA.SONDE_GENERATE A, PLENORA.SONDE_GENERATE B \
                 WHERE A.ID = 3 AND B.ID = 4 AND ST_EQUALS(A.G, B.G) = 1",
            ),
            &cancellation,
        )
        .await
        .expect("ST_EQUALS colonne generate Db2");
    assert_eq!(identificativi(&equal), vec!["3".to_owned()]);
    let silent = transaction
        .query(
            &Statement::new(
                "SELECT T.ID FROM PLENORA.SONDE_GENERATE T WHERE T.W = \
                 (SELECT U.W FROM PLENORA.SONDE_GENERATE U WHERE U.ID = 3) ORDER BY T.ID",
            ),
            &cancellation,
        )
        .await
        .expect("senza giudizio colonne generate Db2");
    assert_eq!(
        identificativi(&silent),
        vec!["3".to_owned()],
        "il server confronta i byte: il secondo poligono, uguale, manca"
    );
    let _ = transaction.rollback(&cancellation).await;
    let tabella = Some("PLENORA");
    let bytes = vec![ParameterValue::Bytes(vec![1])];
    let rifiutati = [
        (
            "W = ?",
            su(tabella, "SONDE_GENERATE", &uguale_binario("W")),
            bytes.clone(),
        ),
        (
            "ORDER BY W",
            su(tabella, "SONDE_GENERATE", &ordinato("W", "asc")),
            Vec::new(),
        ),
    ];
    for (nome, piano, valori) in rifiutati {
        let (_, judged, _) = esegui(&provider, &secret, &piano, Some(&valori), &cancellation).await;
        let error = judged.expect_err(nome);
        assert!(
            rifiuto_del_giudizio(&error, GEOMETRY_SCALAR_OPERAND),
            "{nome}: {error}"
        );
    }
    // La colonna binaria memorizzata resta scalare: il giudizio la ammette e
    // lo SQL arriva al server, con le righe dello SQL senza giudizio (il
    // valore del bind `Bytes` lo decide il legame dei parametri, che non e di
    // questa prova).
    let (_, judged, direct) = esegui(
        &provider,
        &secret,
        &su(tabella, "SONDE_GENERATE", &uguale_binario("B")),
        Some(&[ParameterValue::Bytes(vec![4])]),
        &cancellation,
    )
    .await;
    let rows = judged.unwrap_or_else(|error| panic!("B = ? (binaria memorizzata): {error}"));
    assert_eq!(Ok(rows), direct, "B = ? (binaria memorizzata)");
    let ammessi = [
        (
            "N = ? (generata numerica)",
            su(tabella, "SONDE_GENERATE", &uguale("N")),
            vec![ParameterValue::I32(30)],
            vec!["3"],
        ),
        (
            "ORDER BY ID DESC",
            su(tabella, "SONDE_GENERATE", &ordinato("ID", "desc")),
            Vec::new(),
            vec!["4", "3"],
        ),
    ];
    for (nome, piano, valori, attese) in ammessi {
        let (_, judged, direct) =
            esegui(&provider, &secret, &piano, Some(&valori), &cancellation).await;
        let rows = judged.unwrap_or_else(|error| panic!("{nome}: {error}"));
        assert_eq!(rows, attese, "{nome}");
        assert_eq!(Ok(rows), direct, "{nome}");
    }
    let lettura = |field: &str| ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: "SONDE_GENERATE".to_owned(),
        },
        projection: vec!["ID".to_owned()],
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: field.to_owned(),
            parameter: "p".to_owned(),
        }),
        declared_crs: Vec::new(),
    };
    let parametri = ParameterBag::new(BTreeMap::from([("p".to_owned(), bytes[0].clone())]));
    let error = provider
        .read(&secret, &lettura("W"), &parametri, &budget(), &cancellation)
        .await
        .err()
        .unwrap_or_else(|| panic!("lettura W ammessa"));
    assert!(
        rifiuto_del_giudizio(&error, GEOMETRY_SCALAR_OPERAND),
        "lettura W: {error}"
    );
    // La lettura di Db2 non lega ancora un parametro `Bytes` (rifiuto
    // esplicito del provider): il controllo ammesso e la colonna generata
    // numerica.
    let numero = ParameterBag::new(BTreeMap::from([("p".to_owned(), ParameterValue::I32(30))]));
    provider
        .read(&secret, &lettura("N"), &numero, &budget(), &cancellation)
        .await
        .unwrap_or_else(|error| panic!("lettura N: {error}"));
    for sql in PULIZIA_GENERATE {
        let _ = provider.run_ddl(&secret, sql, &cancellation).await;
    }
}

/// Regola T (quattordicesima lettura del 25/10, difetto 1) su Db2: un
/// parametro dichiarato `string` legato al valore `I64` 1. Lo SQL senza
/// giudizio `NAME = ?` da al segnaposto il tipo della colonna, e il server
/// converte il numero in testo (misurato su 12.1: incontra `'1'`, non `'01'`
/// ne `'1.0'`): una conversione che il chiamante non ha chiesto. Su d5d7fa1
/// il lowering delle mutazioni rendeva quel segnaposto nudo, e il giudizio
/// prendeva la famiglia dal tipo dichiarato. Ora il valore si rifiuta prima dell'esecuzione nella
/// mutazione e nella query della transazione; un testo per il testo cancella
/// una riga sola.
#[tokio::test]
#[ignore = "richiede Db2 live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_typed_parameters_refuse_values_of_another_type() {
    use plenora_database_core::facade::execute_judged;
    use plenora_database_core::relational::MutationOperation;
    use plenora_database_core::typed_parameter::TYPED_PARAMETER_MISMATCH;
    let provider = provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let _ = provider
        .run_ddl(&secret, "DROP TABLE PLENORA.TIPI_T", &cancellation)
        .await;
    for sql in [
        "CREATE TABLE PLENORA.TIPI_T (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(10) NOT NULL)",
        "INSERT INTO PLENORA.TIPI_T VALUES (1, '1'), (2, '01'), (3, '1.0')",
    ] {
        provider
            .run_ddl(&secret, sql, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("preparazione regola T Db2: {error}"));
    }
    let mutation: MutationOperation = serde_json::from_value(json!({
        "type": "delete",
        "target": {"catalog": null, "schema": "PLENORA", "object": "TIPI_T"},
        "filter": {"kind": "compare",
            "left": {"kind": "column", "column": {"relation": null, "field": "NAME"}},
            "operator": "eq",
            "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}}
    }))
    .expect("mutazione");
    let lowered = plenora_database_core::compile_relational_mutation(ProviderKind::Db2, &mutation)
        .expect("lowering");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione regola T Db2");
    let silent = transaction
        .query(
            &Statement::new("SELECT ID FROM PLENORA.TIPI_T WHERE NAME = ? ORDER BY ID")
                .with_params(vec![ParameterValue::I64(1)]),
            &cancellation,
        )
        .await
        .expect("senza giudizio regola T Db2");
    assert_eq!(
        identificativi(&silent),
        vec!["1".to_owned()],
        "il segnaposto prende il tipo della colonna, e il server converte il valore"
    );
    let statement =
        |value: ParameterValue| Statement::new(lowered.sql.clone()).with_params(vec![value]);
    let plan = OperandPlan::Mutation(&mutation);
    let error = execute_judged(
        transaction.as_mut(),
        &statement(ParameterValue::I64(1)),
        Some(&plan),
        &lowered.bind_names,
        &cancellation,
    )
    .await
    .expect_err("intero per un testo dichiarato");
    assert_eq!(error.category, ErrorCategory::InvalidPlan, "{error:?}");
    assert_eq!(error.message, TYPED_PARAMETER_MISMATCH, "{error:?}");
    let deleted = execute_judged(
        transaction.as_mut(),
        &statement(ParameterValue::String("1".to_owned())),
        Some(&plan),
        &lowered.bind_names,
        &cancellation,
    )
    .await
    .expect("testo per il testo");
    // La forma del renderer delle query.
    assert!(
        lowered.sql.ends_with("= CAST(? AS VARCHAR(32672))"),
        "{}",
        lowered.sql
    );
    assert_eq!(deleted, 1, "il CAST confronta il testo");
    let _ = transaction.rollback(&cancellation).await;
    // La query della transazione (lo SDK), con lo stesso predicato.
    let query = su(
        Some("PLENORA"),
        "TIPI_T",
        &json!({"filter": {"kind": "compare", "left": colonna("T", "NAME"), "operator": "eq",
            "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}}}),
    );
    let (_, judged, _) = esegui(
        &provider,
        &secret,
        &query,
        Some(&[ParameterValue::I64(1)]),
        &cancellation,
    )
    .await;
    let error = judged.expect_err("query con un intero");
    assert_eq!(error.message, TYPED_PARAMETER_MISMATCH, "{error:?}");
    let (_, judged, _) = esegui(
        &provider,
        &secret,
        &query,
        Some(&[ParameterValue::String("1".to_owned())]),
        &cancellation,
    )
    .await;
    assert_eq!(judged.expect("query con un testo"), vec!["1".to_owned()]);
    provider
        .run_ddl(&secret, "DROP TABLE PLENORA.TIPI_T", &cancellation)
        .await
        .expect("pulizia regola T Db2");
}

/// Quindicesima lettura del 25/10 sul Db2 reale, regole C e A.
///
/// - **Regola C.** Db2 non ha `DECIMAL(38, 10)` (precisione massima 31,
///   `CAST(? AS DECIMAL(38, 10))` fallisce, misurato): la libreria rende
///   `DECIMAL(31, 10)`, che arrotonda `0.12345678901` e ritrova
///   `DECIMAL(30, 12)` `0.123456789` (misurato). `D = CAST(:p AS decimal)`
///   oltre la scala e il `CAST` di una colonna `DECIMAL(30, 12)` si rifiutano
///   (`CAST_NOT_EXACT`); entro la scala e da `DECIMAL(20, 4)` restano.
///   `CAST(C AS VARCHAR(32672))` di un `CLOB` di 40000 caratteri tronca senza
///   errore (misurato): il `CAST` a `string` di un `CLOB` si
///   rifiuta. `TIMESTAMP WITH TIME ZONE` non esiste (la query fallisce, misurato): un
///   `timestamp_tz` tipizzato si rifiuta (`TYPED_PARAMETER_MISMATCH`).
/// - **Regola A.** Il server ignora gli spazi finali di un identificatore:
///   `T.M` lega la colonna `"M "` (misurato), e su f4989dc il riferimento non
///   si giudicava in un piano senza geometrie (`T.M = '01'` rendeva la riga
///   per conversione); ora si rifiuta (`NAME_UNRESOLVED`), come `T."k"` per
///   la colonna `K`.
#[tokio::test]
#[ignore = "richiede Db2 live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_rilettura_quindici_cast_e_nomi() {
    use plenora_database_core::cast_table::CAST_NOT_EXACT;
    use plenora_database_core::spatial_operand::NAME_UNRESOLVED;
    use plenora_database_core::typed_parameter::TYPED_PARAMETER_MISMATCH;
    let provider = provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let _ = provider
        .run_ddl(&secret, "DROP TABLE PLENORA.R15_T", &cancellation)
        .await;
    for sql in [
        "CREATE TABLE PLENORA.R15_T (ID INTEGER NOT NULL PRIMARY KEY, D DECIMAL(30, 12) NOT NULL, \
         E DECIMAL(20, 4) NOT NULL, C CLOB(100K) NOT NULL, \"M \" INTEGER NOT NULL, \
         K INTEGER NOT NULL, TS TIMESTAMP NOT NULL)",
        "INSERT INTO PLENORA.R15_T VALUES \
         (1, 0.123456789, 1.5, REPEAT(CAST('a' AS CLOB(100K)), 40000), 1, 1, '2024-01-01-08.00.00'), \
         (2, 0.5, 2.5, 'b', 2, 2, '2024-01-01-10.00.00')",
    ] {
        provider
            .run_ddl(&secret, sql, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("preparazione Db2: {error}"));
    }
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione");
    // La tabella del modulo `cast_table`, rimisurata.
    let wide = transaction
        .query(
            &Statement::new("SELECT ID FROM PLENORA.R15_T WHERE D = CAST(? AS DECIMAL(38, 10))")
                .with_params(vec![ParameterValue::String("0.5".to_owned())]),
            &cancellation,
        )
        .await
        .expect_err("DECIMAL(38, 10) su Db2");
    // L'errore del server, non un rifiuto del giudizio.
    assert_ne!(wide.category, ErrorCategory::InvalidPlan, "{wide:?}");
    let rounded = transaction
        .query(
            &Statement::new("SELECT ID FROM PLENORA.R15_T WHERE D = CAST(? AS DECIMAL(31, 10))")
                .with_params(vec![ParameterValue::String("0.12345678901".to_owned())]),
            &cancellation,
        )
        .await
        .expect("DECIMAL(31, 10)");
    assert_eq!(
        identificativi(&rounded),
        vec!["1".to_owned()],
        "il CAST arrotonda"
    );
    let truncated = transaction
        .query(
            &Statement::new(
                "SELECT LENGTH(CAST(C AS VARCHAR(32672))) AS V FROM PLENORA.R15_T WHERE ID = 1",
            ),
            &cancellation,
        )
        .await
        .expect("CLOB troncato senza errore");
    assert_eq!(identificativi(&truncated), vec!["32672".to_owned()]);
    transaction
        .query(
            &Statement::new(
                "SELECT ID FROM PLENORA.R15_T WHERE TS = CAST(? AS TIMESTAMP WITH TIME ZONE)",
            )
            .with_params(vec![ParameterValue::String(
                "2024-01-01T10:00:00+02:00".to_owned(),
            )]),
            &cancellation,
        )
        .await
        .expect_err("TIMESTAMP WITH TIME ZONE su Db2");
    let trailing = transaction
        .query(
            &Statement::new("SELECT ID FROM PLENORA.R15_T WHERE \"M\" = ?")
                .with_params(vec![ParameterValue::String("01".to_owned())]),
            &cancellation,
        )
        .await
        .expect("\"M\" lega \"M \"");
    assert_eq!(identificativi(&trailing), vec!["1".to_owned()]);
    let _ = transaction.rollback(&cancellation).await;

    let typed = |parameter_type: &str| json!({"kind": "typed_parameter", "name": "p", "parameter_type": parameter_type});
    let parameter = || json!({"kind": "parameter", "name": "p"});
    let cast = |expression: Value, target: &str| json!({"kind": "cast", "expression": expression, "target_type": target});
    let compare = |left: Value, right: Value| json!({"filter": {"kind": "compare", "left": left, "operator": "eq", "right": right}});
    let plan = |left: Value, right: Value| su(Some("PLENORA"), "R15_T", &compare(left, right));
    let refused = [
        (
            "D = CAST(:p AS decimal) oltre la scala",
            plan(colonna("T", "D"), cast(parameter(), "decimal")),
            ParameterValue::Decimal("0.12345678901".to_owned()),
            CAST_NOT_EXACT,
        ),
        (
            "CAST(D DECIMAL(30, 12) AS decimal) = ?",
            plan(cast(colonna("T", "D"), "decimal"), typed("decimal")),
            ParameterValue::Decimal("0.5".to_owned()),
            CAST_NOT_EXACT,
        ),
        (
            "CAST(C CLOB AS string) = ?",
            plan(cast(colonna("T", "C"), "string"), typed("string")),
            ParameterValue::String("b".to_owned()),
            CAST_NOT_EXACT,
        ),
        (
            "TS = timestamp_tz tipizzato",
            plan(colonna("T", "TS"), typed("timestamp_tz")),
            ParameterValue::TimestampTz("2024-01-01T10:00:00+02:00".to_owned()),
            TYPED_PARAMETER_MISMATCH,
        ),
        (
            "T.M lega \"M \" soltanto per il server",
            plan(colonna("T", "M"), typed("string")),
            ParameterValue::String("01".to_owned()),
            NAME_UNRESOLVED,
        ),
        (
            "T.\"k\" non lega K",
            plan(colonna("T", "k"), typed("integer")),
            ParameterValue::I32(1),
            NAME_UNRESOLVED,
        ),
    ];
    for (form, plan, value, message) in refused {
        let (sql, judged, _) =
            esegui(&provider, &secret, &plan, Some(&[value]), &cancellation).await;
        let error = judged.expect_err(form);
        assert_eq!(
            error.category,
            ErrorCategory::InvalidPlan,
            "{form} ({sql}): {error:?}"
        );
        assert_eq!(error.message, message, "{form} ({sql}): {error:?}");
    }
    let admitted = [
        (
            "D = CAST(:p AS decimal) entro la scala",
            plan(colonna("T", "D"), cast(parameter(), "decimal")),
            ParameterValue::Decimal("0.5".to_owned()),
            "2",
        ),
        (
            "CAST(E DECIMAL(20, 4) AS decimal) = ?",
            plan(cast(colonna("T", "E"), "decimal"), typed("decimal")),
            ParameterValue::Decimal("2.5".to_owned()),
            "2",
        ),
    ];
    for (form, plan, value, id) in admitted {
        let (sql, judged, _) =
            esegui(&provider, &secret, &plan, Some(&[value]), &cancellation).await;
        assert_eq!(
            judged.unwrap_or_else(|error| panic!("{form} ({sql}): {error:?}")),
            vec![id.to_owned()],
            "{form}"
        );
    }
    provider
        .run_ddl(&secret, "DROP TABLE PLENORA.R15_T", &cancellation)
        .await
        .expect("pulizia");
}
