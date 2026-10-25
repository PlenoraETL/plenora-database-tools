//! Oracolo live del formato NUMBER: ogni valore legato dal provider viene
//! riletto dal server con `TO_CHAR(D, 'TM9')` e `DUMP(D)`, cioe senza il
//! decoder del driver, e confrontato con il riferimento indipendente di
//! `number_oracle_tests`. `WHERE D = :1` deve trovare esattamente la propria
//! riga: un valore codificato come dieci volte se stesso trova quella di un
//! altro, o nessuna.
//!
//! Il secondo test misura una per una le superfici pubbliche che legano un
//! decimale (scrittura Arrow create e upsert con chiave decimale, statement
//! portabile, read con filtro, parametro JSON) e il controllo `F64`, che
//! viaggia come `BINARY_DOUBLE` e non passa dal codec NUMBER.

use crate::live_tests::{fixture, VecBatchStream};
use crate::number_oracle_tests::{generated, parse_canon, parse_dump, reference_bytes, Canon};
use crate::OracleProvider;
use plenora_database_core::arrow::array::{
    Array, Decimal128Array, Int32Array, Int64Array, StringArray,
};
use plenora_database_core::arrow::schema::{DataType, Field};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, ProviderKind, ReadOperation, TransactionProfile, WriteMode,
    WriteOperation,
};
use plenora_database_core::portable::{
    compile_portable, eq, Expression, InsertStatement, PortableStatement, Projection,
    SelectStatement, TableRef,
};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{ParameterBag, ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{CancellationToken, Row};
use std::collections::BTreeMap;
use std::sync::Arc;

const TABLE: &str = "PLENORA_ORACLE_NUMBER_ORACLE";
const KEYS: &str = "PLENORA_ORACLE_NUMBER_KEYS";
const ARROW: &str = "PLENORA_ORACLE_NUMBER_ARROW";
const DOCS: &str = "PLENORA_ORACLE_NUMBER_JSON";
const ARROW_SCALE: i8 = 20;
/// Righe per istruzione nei blocchi di insert e di verifica.
const CHUNK: usize = 50;

/// Valori in (-1, 1) con un numero pari e dispari di zeri dopo il punto, il
/// ramo che il difetto colpiva, piu un vicino oltre l'unita.
const PROBES: [&str; 8] = [
    "0.05", "0.5", "-0.05", "-0.5", "0.0005", "0.005", "0.07", "1.5",
];

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget NUMBER Oracle")
}

async fn drop_table(provider: &OracleProvider, secret: &SecretString, table: &str) {
    let _ = provider
        .execute_ddl(
            secret,
            &format!("DROP TABLE {table} PURGE"),
            &CancellationToken::new(),
        )
        .await;
}

async fn create_table(provider: &OracleProvider, secret: &SecretString, ddl: &str) {
    provider
        .execute_ddl(secret, ddl, &CancellationToken::new())
        .await
        .expect("crea fixture NUMBER Oracle");
}

async fn begin(provider: &OracleProvider, secret: &SecretString) -> Box<dyn TransactionScope> {
    provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .expect("begin NUMBER Oracle")
}

fn text(row: &Row, name: &str) -> String {
    match row.get(name) {
        Some(ParameterValue::String(value)) => value.clone(),
        other => panic!("colonna {name} testuale attesa: {other:?}"),
    }
}

fn optional_text(row: &Row, name: &str) -> Option<String> {
    match row.get(name) {
        Some(ParameterValue::String(value)) => Some(value.clone()),
        _ => None,
    }
}

fn integer(row: &Row, name: &str) -> i64 {
    match row.get(name) {
        Some(ParameterValue::I64(value)) => *value,
        Some(ParameterValue::Decimal(value)) => value.parse().expect("intero"),
        other => panic!("colonna {name} intera attesa: {other:?}"),
    }
}

fn report(path: &str, mismatches: &[String], total: usize) -> bool {
    eprintln!(
        "NUMBER Oracle [{path}]: {} discrepanze su {total} {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(5)]
    );
    mismatches.is_empty()
}

/// Confronta ogni riga (ID, D) di `table` con il valore atteso usando solo
/// funzioni del server, poi il decoder del driver, poi verifica che
/// `WHERE D = :1` trovi la sola riga. Rende tutte le discrepanze.
async fn server_mismatches(
    transaction: &mut dyn TransactionScope,
    table: &str,
    expected: &BTreeMap<i64, Canon>,
) -> Vec<String> {
    let cancellation = CancellationToken::new();
    let mut mismatches = Vec::new();
    let rows = transaction
        .query(
            &Statement::new(format!(
                "SELECT ID, TO_CHAR(D, 'TM9') AS T, DUMP(D) AS DMP, D FROM {table} ORDER BY ID"
            )),
            &cancellation,
        )
        .await
        .expect("rilettura NUMBER Oracle");
    if rows.len() != expected.len() {
        mismatches.push(format!("righe {} attese {}", rows.len(), expected.len()));
    }
    for row in &rows {
        let id = integer(row, "ID");
        let Some(value) = expected.get(&id) else {
            mismatches.push(format!("id {id} inatteso"));
            continue;
        };
        let label = value.bind_text(true);
        // NULL (o un tipo inatteso) e una discrepanza, non un panic: sotto il
        // codec difettoso la misura deve contare ogni riga.
        let server_text = optional_text(row, "T");
        if server_text.as_deref().and_then(parse_canon).as_ref() != Some(value) {
            mismatches.push(format!("{label}: TO_CHAR {server_text:?}"));
        }
        let dump = optional_text(row, "DMP");
        if dump.as_deref().and_then(parse_dump) != reference_bytes(value) {
            mismatches.push(format!("{label}: DUMP {dump:?}"));
        }
        let decoded = match row.get("D") {
            Some(ParameterValue::Decimal(decoded)) => parse_canon(decoded),
            Some(ParameterValue::I64(decoded)) => parse_canon(&decoded.to_string()),
            _ => None,
        };
        if decoded.as_ref() != Some(value) {
            mismatches.push(format!("{label}: decoder {:?}", row.get("D")));
        }
    }
    // Una sola istruzione per blocco: il driver non chiude sul server i
    // cursori delle istruzioni gia eseguite (ORA-01000 oltre OPEN_CURSORS).
    let entries = expected.iter().collect::<Vec<_>>();
    for chunk in entries.chunks(CHUNK) {
        let probes = (1..=chunk.len())
            .map(|position| format!("SELECT {position} AS P, :{position} AS V FROM DUAL"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let found = transaction
            .query(
                &Statement::new(format!(
                    "SELECT B.P, (SELECT COUNT(*) FROM {table} T WHERE T.D = B.V) AS N,                      (SELECT MIN(T.ID) FROM {table} T WHERE T.D = B.V) AS I                      FROM ({probes}) B ORDER BY B.P"
                ))
                .with_params(
                    chunk
                        .iter()
                        .map(|(id, value)| ParameterValue::Decimal(value.bind_text(*id % 2 == 0)))
                        .collect(),
                ),
                &cancellation,
            )
            .await
            .expect("uguaglianza NUMBER Oracle");
        assert_eq!(found.len(), chunk.len());
        for (row, (id, value)) in found.iter().zip(chunk) {
            let count = integer(row, "N");
            let first = match row.get("I") {
                Some(ParameterValue::Null { .. }) => None,
                _ => Some(integer(row, "I")),
            };
            if count != 1 || first != Some(**id) {
                mismatches.push(format!(
                    "{}: WHERE D = :1 trova {count} righe",
                    value.bind_text(true)
                ));
            }
        }
    }
    mismatches
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_number_binds_match_server_text_dump_and_equality() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    drop_table(&provider, &secret, TABLE).await;
    create_table(
        &provider,
        &secret,
        &format!("CREATE TABLE {TABLE} (ID NUMBER(10) PRIMARY KEY, D NUMBER)"),
    )
    .await;
    // Ogni posizione del punto fra 1e-130 e 1e125, con 1, 2, 3 e 38 cifre,
    // due forme delle cifre, entrambi i segni, piu lo zero.
    let values = generated(&[1, 2, 3, 38], 2);
    assert!(values.len() >= 2_000, "{}", values.len());
    let expected = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| (i64::try_from(index).expect("id") + 1, value))
        .collect::<BTreeMap<_, _>>();
    let mut transaction = begin(&provider, &secret).await;
    let entries = expected.iter().collect::<Vec<_>>();
    for chunk in entries.chunks(CHUNK) {
        let targets = (0..chunk.len())
            .map(|position| {
                format!(
                    "INTO {TABLE} (ID, D) VALUES (:{}, :{})",
                    2 * position + 1,
                    2 * position + 2
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let params = chunk
            .iter()
            .flat_map(|(id, value)| {
                [
                    ParameterValue::I64(**id),
                    // Alterna notazione posizionale e scientifica.
                    ParameterValue::Decimal(value.bind_text(*id % 2 == 1)),
                ]
            })
            .collect();
        transaction
            .execute(
                &Statement::new(format!("INSERT ALL {targets} SELECT 1 FROM DUAL"))
                    .with_params(params),
                &cancellation,
            )
            .await
            .expect("insert NUMBER Oracle");
    }
    let mismatches = server_mismatches(transaction.as_mut(), TABLE, &expected).await;
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback NUMBER Oracle");
    drop_table(&provider, &secret, TABLE).await;
    assert!(
        report("statement nativo", &mismatches, expected.len()),
        "{} discrepanze",
        mismatches.len()
    );
}

/// Il valore come Decimal128(38, 20), se rappresentabile.
fn arrow_decimal(value: &Canon) -> Option<i128> {
    if value.digits.is_empty() {
        return None;
    }
    let shift = value.point + i32::from(ARROW_SCALE) - i32::try_from(value.digits.len()).ok()?;
    if shift < 0 {
        return None;
    }
    let mut digits = value
        .digits
        .iter()
        .map(|digit| char::from(b'0' + digit))
        .collect::<String>();
    digits.push_str(&"0".repeat(usize::try_from(shift).ok()?));
    if digits.len() > 38 {
        return None;
    }
    let magnitude = digits.parse::<i128>().ok()?;
    Some(if value.negative {
        -magnitude
    } else {
        magnitude
    })
}

fn decimal_batch(
    precision: u8,
    scale: i8,
    ids: Vec<i32>,
    decimals: Vec<i128>,
    labels: Option<Vec<&str>>,
) -> RecordBatch {
    let mut fields = vec![
        Field::new("ID", DataType::Int32, false),
        Field::new(
            if labels.is_some() { "K" } else { "D" },
            DataType::Decimal128(precision, scale),
            false,
        ),
    ];
    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int32Array::from(ids)),
        Arc::new(
            Decimal128Array::from(decimals)
                .with_precision_and_scale(precision, scale)
                .expect("Decimal128"),
        ),
    ];
    if let Some(labels) = labels {
        fields.push(Field::new("LABEL", DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(labels)));
    }
    RecordBatch::try_new(contract_schema(fields).expect("schema"), columns)
        .expect("batch NUMBER Oracle")
}

fn write_operation(table: &str, mode: WriteMode, keys: &[&str], update: &[&str]) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: table.to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: if mode == WriteMode::Create {
            TransactionProfile::BestEffortDdl
        } else {
            TransactionProfile::SingleTransaction
        },
        keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        update_columns: update.iter().map(|column| (*column).to_owned()).collect(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn arrow_write(
    provider: &OracleProvider,
    secret: &SecretString,
    operation: &WriteOperation,
    batch: RecordBatch,
) -> Result<u64, String> {
    let cancellation = CancellationToken::new();
    let budget = budget();
    let prepared = provider
        .prepare_write(secret, operation, batch.schema(), &budget, &cancellation)
        .await
        .map_err(|error| format!("{error:?}"))?;
    provider
        .write(
            secret,
            prepared,
            Box::new(VecBatchStream::new(vec![batch])),
            &budget,
            &cancellation,
        )
        .await
        .map(|outcome| outcome.rows.confirmed)
        .map_err(|error| format!("{error:?}"))
}

fn arrow_integers(array: &dyn Array) -> Vec<i64> {
    let any = array.as_any();
    if let Some(values) = any.downcast_ref::<Int64Array>() {
        return values.values().to_vec();
    }
    if let Some(values) = any.downcast_ref::<Int32Array>() {
        return values.values().iter().map(|v| i64::from(*v)).collect();
    }
    if let Some(values) = any.downcast_ref::<Decimal128Array>() {
        return values
            .values()
            .iter()
            .map(|v| i64::try_from(*v).expect("id"))
            .collect();
    }
    panic!("colonna ID Arrow intera attesa: {:?}", array.data_type());
}

fn probe_id(index: usize) -> i64 {
    i64::try_from(index).expect("id") + 1
}

/// Le chiavi scritte dal server con letterali SQL, quindi senza il codec del
/// driver: le superfici dopo le cercano o le aggiornano per valore.
async fn seed_keys(provider: &OracleProvider, secret: &SecretString) {
    let cancellation = CancellationToken::new();
    drop_table(provider, secret, KEYS).await;
    create_table(
        provider,
        secret,
        &format!(
            "CREATE TABLE {KEYS} (ID NUMBER(10), K NUMBER(38, 4) PRIMARY KEY, LABEL VARCHAR2(20))"
        ),
    )
    .await;
    let mut transaction = begin(provider, secret).await;
    for (index, probe) in PROBES.iter().enumerate() {
        transaction
            .execute(
                &Statement::new(format!(
                    "INSERT INTO {KEYS} (ID, K, LABEL) VALUES ({}, {probe}, 'server')",
                    probe_id(index)
                )),
                &cancellation,
            )
            .await
            .expect("letterale NUMBER Oracle");
    }
    transaction
        .commit(&cancellation)
        .await
        .expect("commit chiavi NUMBER Oracle");
}

async fn arrow_create_path(provider: &OracleProvider, secret: &SecretString) -> bool {
    let cancellation = CancellationToken::new();
    drop_table(provider, secret, ARROW).await;
    let values = generated(&[1, 2, 3, 38], 2)
        .into_iter()
        .filter_map(|value| arrow_decimal(&value).map(|decimal| (value, decimal)))
        .collect::<Vec<_>>();
    assert!(values.len() > 200, "{}", values.len());
    let ids = (1..=i32::try_from(values.len()).expect("id")).collect::<Vec<_>>();
    let batch = decimal_batch(
        38,
        ARROW_SCALE,
        ids.clone(),
        values.iter().map(|(_, decimal)| *decimal).collect(),
        None,
    );
    let written = arrow_write(
        provider,
        secret,
        &write_operation(ARROW, WriteMode::Create, &["ID"], &[]),
        batch,
    )
    .await
    .expect("create Arrow NUMBER Oracle");
    assert_eq!(written, u64::try_from(values.len()).expect("righe"));
    let expected = ids
        .iter()
        .zip(&values)
        .map(|(id, (value, _))| (i64::from(*id), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut transaction = begin(provider, secret).await;
    let mismatches = server_mismatches(transaction.as_mut(), ARROW, &expected).await;
    transaction.rollback(&cancellation).await.expect("rollback");
    drop_table(provider, secret, ARROW).await;
    report("Arrow Decimal128 create", &mismatches, expected.len())
}

async fn portable_select_path(provider: &OracleProvider, secret: &SecretString) -> bool {
    let cancellation = CancellationToken::new();
    let mut misses = Vec::new();
    let mut transaction = begin(provider, secret).await;
    for (index, probe) in PROBES.iter().enumerate() {
        let select = compile_portable(
            ProviderKind::Oracle,
            &PortableStatement::Select(SelectStatement {
                table: TableRef::new(KEYS),
                projection: Projection::Columns(vec!["ID".to_owned()]),
                filter: Some(eq("K", ParameterValue::Decimal((*probe).to_owned()))),
                order_by: Vec::new(),
                limit: None,
            }),
        )
        .expect("compila select portabile");
        let rows = transaction
            .query(&select, &cancellation)
            .await
            .expect("select portabile");
        let ids = rows
            .iter()
            .map(|row| integer(row, "ID"))
            .collect::<Vec<_>>();
        if ids != [probe_id(index)] {
            misses.push(format!("{probe}: righe {ids:?}"));
        }
    }
    transaction.rollback(&cancellation).await.expect("rollback");
    report("portabile SELECT eq", &misses, PROBES.len())
}

async fn read_filter_path(provider: &OracleProvider, secret: &SecretString) -> bool {
    let cancellation = CancellationToken::new();
    let mut misses = Vec::new();
    for (index, probe) in PROBES.iter().enumerate() {
        let read = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("PLENORA".to_owned()),
                object: KEYS.to_owned(),
            },
            projection: vec!["ID".to_owned()],
            filter: Some(FilterExpression::Eq {
                field: "K".to_owned(),
                parameter: "k".to_owned(),
            }),
            order_by: Vec::new(),
            row_limit: None,
            row_offset: None,
            declared_crs: Vec::new(),
        };
        let parameters = ParameterBag::new(BTreeMap::from([(
            "k".to_owned(),
            ParameterValue::Decimal((*probe).to_owned()),
        )]));
        let mut stream = provider
            .read(secret, &read, &parameters, &budget(), &cancellation)
            .await
            .expect("read con filtro decimale");
        let mut ids = Vec::new();
        while let Some(batch) = stream.next_batch(&cancellation).await.expect("batch read") {
            ids.extend(arrow_integers(batch.column(0).as_ref()));
        }
        if ids != [probe_id(index)] {
            misses.push(format!("{probe}: righe {ids:?}"));
        }
    }
    report("read con filtro", &misses, PROBES.len())
}

async fn arrow_upsert_path(provider: &OracleProvider, secret: &SecretString) -> bool {
    let cancellation = CancellationToken::new();
    // 0.05, -0.05 e 0.50 a scala 4: ciascuna aggiorna la propria riga.
    let batch = decimal_batch(
        38,
        4,
        vec![101, 102, 103],
        vec![500, -500, 5_000],
        Some(vec!["upsert"; 3]),
    );
    let upserted = arrow_write(
        provider,
        secret,
        &write_operation(KEYS, WriteMode::Upsert, &["K"], &["ID", "LABEL"]),
        batch,
    )
    .await;
    let mut transaction = begin(provider, secret).await;
    let rows = transaction
        .query(
            &Statement::new(format!(
                "SELECT TO_CHAR(K, 'TM9') AS T, LABEL FROM {KEYS} ORDER BY K"
            )),
            &cancellation,
        )
        .await
        .expect("rilettura upsert");
    transaction.rollback(&cancellation).await.expect("rollback");
    let mut misses = Vec::new();
    if let Err(error) = &upserted {
        misses.push(format!("upsert rifiutato: {error}"));
    }
    if rows.len() != PROBES.len() {
        misses.push(format!("righe dopo upsert {}", rows.len()));
    }
    for row in &rows {
        let key = text(row, "T");
        let label = text(row, "LABEL");
        let should_change = matches!(key.as_str(), ".05" | "-.05" | ".5");
        if should_change != (label == "upsert") {
            misses.push(format!("{key}: LABEL {label}"));
        }
    }
    report("Arrow upsert chiave decimale", &misses, 3)
}

async fn portable_insert_path(provider: &OracleProvider, secret: &SecretString) -> bool {
    let cancellation = CancellationToken::new();
    let mut misses = Vec::new();
    let mut transaction = begin(provider, secret).await;
    transaction
        .execute(
            &Statement::new(format!("DELETE FROM {KEYS}")),
            &cancellation,
        )
        .await
        .expect("svuota chiavi");
    for (index, probe) in PROBES.iter().enumerate() {
        let insert = compile_portable(
            ProviderKind::Oracle,
            &PortableStatement::Insert(InsertStatement {
                table: TableRef::new(KEYS),
                columns: vec!["ID".to_owned(), "K".to_owned(), "LABEL".to_owned()],
                values: vec![vec![
                    Expression::literal(ParameterValue::I64(probe_id(index))),
                    Expression::literal(ParameterValue::Decimal((*probe).to_owned())),
                    Expression::literal(ParameterValue::String("portabile".to_owned())),
                ]],
                returning: Vec::new(),
            }),
        )
        .expect("compila insert portabile");
        if let Err(error) = transaction.execute(&insert, &cancellation).await {
            misses.push(format!("{probe}: insert rifiutato {error:?}"));
            break;
        }
        let rows = transaction
            .query(
                &Statement::new(format!(
                    "SELECT COUNT(*) AS N FROM {KEYS} WHERE ID = {} AND K = {probe}",
                    probe_id(index)
                )),
                &cancellation,
            )
            .await
            .expect("verifica insert portabile");
        if integer(&rows[0], "N") != 1 {
            misses.push(format!("{probe}: salvato diverso"));
        }
    }
    transaction.rollback(&cancellation).await.expect("rollback");
    report("portabile INSERT", &misses, PROBES.len())
}

async fn json_parameter_path(provider: &OracleProvider, secret: &SecretString) -> bool {
    let cancellation = CancellationToken::new();
    drop_table(provider, secret, DOCS).await;
    create_table(
        provider,
        secret,
        &format!("CREATE TABLE {DOCS} (ID NUMBER(10), DOC JSON)"),
    )
    .await;
    let mut misses = Vec::new();
    let mut transaction = begin(provider, secret).await;
    for (index, probe) in PROBES.iter().enumerate() {
        let number = serde_json::from_str::<serde_json::Value>(probe).expect("numero JSON");
        let inserted = transaction
            .execute(
                &Statement::new(format!("INSERT INTO {DOCS} (ID, DOC) VALUES (:1, :2)"))
                    .with_params(vec![
                        ParameterValue::I64(probe_id(index)),
                        ParameterValue::Json(serde_json::json!({ "x": number })),
                    ]),
                &cancellation,
            )
            .await;
        if let Err(error) = inserted {
            misses.push(format!("{probe}: rifiutato {error:?}"));
            continue;
        }
        let rows = transaction
            .query(
                &Statement::new(format!(
                    "SELECT TO_CHAR(JSON_VALUE(DOC, '$.x' RETURNING NUMBER), 'TM9') AS T \
                     FROM {DOCS} WHERE ID = {}",
                    probe_id(index)
                )),
                &cancellation,
            )
            .await
            .expect("rilettura JSON");
        let stored = text(&rows[0], "T");
        if parse_canon(&stored) != parse_canon(probe) {
            misses.push(format!("{probe}: salvato {stored}"));
        }
    }
    transaction.rollback(&cancellation).await.expect("rollback");
    drop_table(provider, secret, DOCS).await;
    report("parametro JSON", &misses, PROBES.len())
}

async fn float_control_path(provider: &OracleProvider, secret: &SecretString) -> bool {
    let cancellation = CancellationToken::new();
    let mut misses = Vec::new();
    let mut transaction = begin(provider, secret).await;
    for probe in PROBES {
        let value = probe.parse::<f64>().expect("f64");
        let rows = transaction
            .query(
                &Statement::new("SELECT TO_CHAR(TO_NUMBER(:1), 'TM9') AS T FROM DUAL")
                    .with_params(vec![ParameterValue::F64(value)]),
                &cancellation,
            )
            .await
            .expect("F64 Oracle");
        let stored = text(&rows[0], "T");
        if stored.parse::<f64>().ok() != Some(value) {
            misses.push(format!("{probe}: server {stored}"));
        }
    }
    transaction.rollback(&cancellation).await.expect("rollback");
    report("controllo F64", &misses, PROBES.len())
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_number_every_public_bind_path_is_exact() {
    let (provider, secret) = fixture();
    let mut verdicts = vec![arrow_create_path(&provider, &secret).await];
    seed_keys(&provider, &secret).await;
    verdicts.push(portable_select_path(&provider, &secret).await);
    verdicts.push(read_filter_path(&provider, &secret).await);
    verdicts.push(arrow_upsert_path(&provider, &secret).await);
    verdicts.push(portable_insert_path(&provider, &secret).await);
    drop_table(&provider, &secret, KEYS).await;
    verdicts.push(json_parameter_path(&provider, &secret).await);
    verdicts.push(float_control_path(&provider, &secret).await);
    assert!(verdicts.iter().all(|ok| *ok), "{verdicts:?}");
}
