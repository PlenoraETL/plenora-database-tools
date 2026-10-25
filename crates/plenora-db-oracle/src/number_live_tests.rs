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
    Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
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
use plenora_database_core::{CancellationToken, ErrorCategory, Row};
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

/// Scrittura Arrow `Decimal128(38, 20)` con `mode` (create, o append su una
/// tabella creata dal DDL), verifica lato server e rilettura Arrow.
async fn arrow_write_path(
    provider: &OracleProvider,
    secret: &SecretString,
    mode: WriteMode,
) -> bool {
    let cancellation = CancellationToken::new();
    drop_table(provider, secret, ARROW).await;
    if mode == WriteMode::Append {
        create_table(
            provider,
            secret,
            &format!("CREATE TABLE {ARROW} (ID NUMBER(10) PRIMARY KEY, D NUMBER(38, 20))"),
        )
        .await;
    }
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
        &write_operation(
            ARROW,
            mode,
            if mode == WriteMode::Append {
                &[]
            } else {
                &["ID"]
            },
            &[],
        ),
        batch,
    )
    .await
    .expect("scrittura Arrow NUMBER Oracle");
    assert_eq!(written, u64::try_from(values.len()).expect("righe"));
    let expected = ids
        .iter()
        .zip(&values)
        .map(|(id, (value, _))| (i64::from(*id), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut transaction = begin(provider, secret).await;
    let mut mismatches = server_mismatches(transaction.as_mut(), ARROW, &expected).await;
    transaction.rollback(&cancellation).await.expect("rollback");
    // Rilettura Arrow: lo stesso Decimal128 scritto, riga per riga.
    let read_back = arrow_read_decimals(provider, secret, ARROW).await;
    let written = ids
        .iter()
        .zip(&values)
        .map(|(id, (_, decimal))| (i64::from(*id), *decimal))
        .collect::<Vec<_>>();
    if read_back != written {
        let differing = read_back
            .iter()
            .zip(&written)
            .filter(|(left, right)| left != right)
            .count();
        mismatches.push(format!(
            "read Arrow: {} righe, {differing} diverse",
            read_back.len()
        ));
    }
    drop_table(provider, secret, ARROW).await;
    let label = if mode == WriteMode::Append {
        "Arrow Decimal128 append + read"
    } else {
        "Arrow Decimal128 create + read"
    };
    report(label, &mismatches, expected.len())
}

fn read_operation(table: &str) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: table.to_owned(),
        },
        projection: vec!["ID".to_owned(), "D".to_owned()],
        filter: None,
        order_by: vec![plenora_database_core::plan::OrderBy {
            field: "ID".to_owned(),
            direction: plenora_database_core::plan::SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        declared_crs: Vec::new(),
    }
}

/// (ID, D) letti dal provider come Arrow, con D `Decimal128`.
async fn arrow_read_decimals(
    provider: &OracleProvider,
    secret: &SecretString,
    table: &str,
) -> Vec<(i64, i128)> {
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .read(
            secret,
            &read_operation(table),
            &ParameterBag::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("read Arrow NUMBER Oracle");
    let mut rows = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await.expect("batch") {
        let ids = arrow_integers(batch.column(0).as_ref());
        let decimals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("D come Decimal128");
        rows.extend(ids.into_iter().zip(decimals.values().iter().copied()));
    }
    rows
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
    let mut verdicts = vec![
        arrow_write_path(&provider, &secret, WriteMode::Create).await,
        arrow_write_path(&provider, &secret, WriteMode::Append).await,
    ];
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

/// Valori che il server conserva ma che non sono decimali, scritti con
/// `UTL_RAW.CAST_TO_NUMBER`: -infinito, +infinito e (se il server lo accetta)
/// la cifra 100 dopo la prima posizione. Ogni lettura deve fallire: prima
/// rendevano `""` (e 0 in Arrow), 1e126 e 11.
const INVALID: [(&str, &str); 3] = [
    ("00", "-infinito"),
    ("FF65", "+infinito"),
    ("C10265", "cifra 100"),
];
const INVALID_TABLE: &str = "PLENORA_ORACLE_NUMBER_INVALID";

/// `None` se `outcome` e l'errore `DataMapping` atteso; altrimenti la
/// discrepanza: un valore reso, o un errore d'altra categoria (trasporto,
/// esecuzione), che non proverebbe il rifiuto del valore.
fn expect_data_mapping<T: std::fmt::Debug>(
    label: &str,
    path: &str,
    outcome: plenora_database_core::Result<T>,
) -> Option<String> {
    match outcome {
        Err(error) if error.category == ErrorCategory::DataMapping => None,
        Err(error) => Some(format!("{label}: {path} fallita con {:?}", error.category)),
        Ok(value) => Some(format!("{label}: {path} resa {value:?}")),
    }
}

async fn arrow_read_by_id(
    provider: &OracleProvider,
    secret: &SecretString,
    id: i64,
) -> plenora_database_core::Result<usize> {
    let cancellation = CancellationToken::new();
    let mut operation = read_operation(INVALID_TABLE);
    operation.filter = Some(FilterExpression::Eq {
        field: "ID".to_owned(),
        parameter: "id".to_owned(),
    });
    let parameters =
        ParameterBag::new(BTreeMap::from([("id".to_owned(), ParameterValue::I64(id))]));
    let mut stream = provider
        .read(secret, &operation, &parameters, &budget(), &cancellation)
        .await?;
    let mut rows = 0;
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        rows += batch.num_rows();
    }
    Ok(rows)
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_number_infinities_and_invalid_bytes_fail_every_read() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    drop_table(&provider, &secret, INVALID_TABLE).await;
    create_table(
        &provider,
        &secret,
        &format!("CREATE TABLE {INVALID_TABLE} (ID NUMBER(10) PRIMARY KEY, D NUMBER)"),
    )
    .await;
    let mut stored = Vec::new();
    let mut transaction = begin(&provider, &secret).await;
    for (index, (raw, label)) in INVALID.iter().enumerate() {
        let id = probe_id(index);
        let sql = format!(
            "INSERT INTO {INVALID_TABLE} (ID, D) \
             VALUES ({id}, UTL_RAW.CAST_TO_NUMBER(HEXTORAW('{raw}')))"
        );
        match transaction
            .execute(&Statement::new(sql), &cancellation)
            .await
        {
            Ok(_) => stored.push((id, *label)),
            Err(error) => eprintln!("NUMBER Oracle: il server non conserva {label}: {error:?}"),
        }
    }
    transaction.commit(&cancellation).await.expect("commit");
    // -infinito e +infinito sono quelli osservati dal secondo lettore.
    assert!(stored.len() >= 2, "{stored:?}");

    let mut misses = Vec::new();
    // Un errore di decodifica a meta risposta lascia la sessione inservibile:
    // ogni lettura che deve fallire ha la propria transazione.
    for (id, label) in &stored {
        let mut transaction = begin(&provider, &secret).await;
        let dump = transaction
            .query(
                &Statement::new(format!(
                    "SELECT DUMP(D) AS DMP FROM {INVALID_TABLE} WHERE ID = {id}"
                )),
                &cancellation,
            )
            .await
            .expect("DUMP del valore non decimale");
        eprintln!(
            "NUMBER Oracle: {label} -> {:?}",
            optional_text(&dump[0], "DMP")
        );
        // Query di riga: decoder del driver. Deve fallire per mappatura
        // dati, non per un errore qualsiasi.
        let outcome = transaction
            .query(
                &Statement::new(format!("SELECT D FROM {INVALID_TABLE} WHERE ID = {id}")),
                &cancellation,
            )
            .await;
        if let Some(miss) = expect_data_mapping(label, "query", outcome.map(|rows| rows.len())) {
            misses.push(miss);
        }
        let _ = transaction.rollback(&cancellation).await;
    }
    // Read Arrow: Decimal128(38, 0) sulla colonna NUMBER libera.
    for (id, label) in &stored {
        let outcome = arrow_read_by_id(&provider, &secret, *id).await;
        if let Some(miss) = expect_data_mapping(label, "read Arrow", outcome) {
            misses.push(miss);
        }
    }
    drop_table(&provider, &secret, INVALID_TABLE).await;
    assert!(
        report("letture di valori non decimali", &misses, stored.len()),
        "{misses:?}"
    );
}

/// Un numero dentro un documento JSON costruito dal server, letto in una
/// transazione propria (una lettura che fallisce chiude la sessione).
async fn json_number(
    provider: &OracleProvider,
    secret: &SecretString,
    text: &str,
) -> plenora_database_core::Result<Option<ParameterValue>> {
    let mut transaction = begin(provider, secret).await;
    let rows = transaction
        .query(
            &Statement::new(format!(
                "SELECT JSON_OBJECT('x' VALUE TO_NUMBER('{text}') RETURNING JSON) AS DOC \
                 FROM DUAL"
            )),
            &CancellationToken::new(),
        )
        .await;
    let _ = transaction.rollback(&CancellationToken::new()).await;
    Ok(rows?[0].get("DOC").cloned())
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_number_inside_json_reads_back_exactly_or_fails() {
    let (provider, secret) = fixture();
    let mut misses = Vec::new();
    // Rappresentabili esattamente da i64, u64 o f64: devono tornare uguali.
    let exact = PROBES
        .iter()
        .copied()
        .chain([
            "-9223372036854775808",
            "18446744073709551615",
            "1e-130",
            "1.5e125",
        ])
        .collect::<Vec<_>>();
    for probe in &exact {
        match json_number(&provider, &secret, probe).await {
            Ok(Some(ParameterValue::Json(document))) => {
                let text = document["x"].to_string();
                if parse_canon(&text) != parse_canon(probe) {
                    misses.push(format!("{probe}: JSON {text}"));
                }
            }
            other => misses.push(format!("{probe}: {other:?}")),
        }
    }
    // Non rappresentabili senza perdita: la lettura deve fallire, non rendere
    // il double piu vicino.
    for probe in ["123456789012345678901234567890", "0.12345678901234567891"] {
        let outcome = json_number(&provider, &secret, probe).await;
        if let Some(miss) = expect_data_mapping(probe, "JSON", outcome) {
            misses.push(miss);
        }
    }
    assert!(
        report("numeri dentro JSON", &misses, exact.len() + 2),
        "{misses:?}"
    );
}

const FLOATS: &str = "PLENORA_ORACLE_NUMBER_FLOAT";

fn float_batch(columns: &[&str], ids: Vec<i32>, values: &[Vec<f64>]) -> RecordBatch {
    let mut fields = vec![Field::new("ID", DataType::Int32, false)];
    let mut arrays: Vec<Arc<dyn Array>> = vec![Arc::new(Int32Array::from(ids))];
    for (name, column) in columns.iter().zip(values) {
        fields.push(Field::new(*name, DataType::Float64, false));
        arrays.push(Arc::new(Float64Array::from(column.clone())));
    }
    RecordBatch::try_new(contract_schema(fields).expect("schema"), arrays).expect("batch FLOAT")
}

/// (ID, colonne) lette come Arrow; ogni colonna Float64 resa come bit.
async fn arrow_read_floats(
    provider: &OracleProvider,
    secret: &SecretString,
    columns: &[&str],
) -> plenora_database_core::Result<Vec<Vec<u64>>> {
    let cancellation = CancellationToken::new();
    let mut operation = read_operation(FLOATS);
    operation.projection = std::iter::once("ID")
        .chain(columns.iter().copied())
        .map(str::to_owned)
        .collect();
    let mut stream = provider
        .read(
            secret,
            &operation,
            &ParameterBag::default(),
            &budget(),
            &cancellation,
        )
        .await?;
    let mut rows = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                (1..batch.num_columns())
                    .map(|index| {
                        let column = batch.column(index);
                        column
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .map(|values| values.value(row).to_bits())
                            .or_else(|| {
                                column
                                    .as_any()
                                    .downcast_ref::<Float32Array>()
                                    .map(|values| u64::from(values.value(row).to_bits()))
                            })
                            .expect("colonna float")
                    })
                    .collect(),
            );
        }
    }
    Ok(rows)
}

async fn reset_floats(provider: &OracleProvider, secret: &SecretString) {
    drop_table(provider, secret, FLOATS).await;
    create_table(
        provider,
        secret,
        &format!(
            "CREATE TABLE {FLOATS} (ID NUMBER(10) PRIMARY KEY, F FLOAT, F10 FLOAT(10), \
             BD BINARY_DOUBLE, BF BINARY_FLOAT)"
        ),
    )
    .await;
}

async fn execute_sql(provider: &OracleProvider, secret: &SecretString, sql: &str) {
    let cancellation = CancellationToken::new();
    let mut transaction = begin(provider, secret).await;
    transaction
        .execute(&Statement::new(sql), &cancellation)
        .await
        .expect("SQL fixture FLOAT");
    transaction.commit(&cancellation).await.expect("commit");
}

/// `FLOAT(b)` e un NUMBER decimale fino a 38 cifre: in lettura diventa
/// Float64 solo se il valore se ne ricava di nuovo, in scrittura si lega il
/// decimale piu breve che ricostruisce l'f64 (o si rifiuta). `BINARY_DOUBLE`
/// e `BINARY_FLOAT` si leggono dai byte IEEE, senza passare da NUMBER.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_number_float_columns_read_and_write_exactly_or_fail() {
    let (provider, secret) = fixture();
    let mut misses = Vec::new();

    // 1. Lettura di valori scritti dal server con letterali.
    reset_floats(&provider, &secret).await;
    execute_sql(
        &provider,
        &secret,
        &format!(
            "INSERT INTO {FLOATS} (ID, F, F10, BD, BF) VALUES (1, 0.1, 0.1234, \
             TO_BINARY_DOUBLE('1e-300'), TO_BINARY_FLOAT('1e-40'))"
        ),
    )
    .await;
    execute_sql(
        &provider,
        &secret,
        &format!(
            "INSERT INTO {FLOATS} (ID, F, F10, BD, BF) VALUES (2, -2.5, -1, \
             TO_BINARY_DOUBLE('0.30000000000000004'), TO_BINARY_FLOAT('-1.5'))"
        ),
    )
    .await;
    let expected = vec![
        vec![
            0.1_f64.to_bits(),
            0.1234_f64.to_bits(),
            1e-300_f64.to_bits(),
            u64::from(1e-40_f32.to_bits()),
        ],
        vec![
            (-2.5_f64).to_bits(),
            (-1.0_f64).to_bits(),
            0.300_000_000_000_000_04_f64.to_bits(),
            u64::from((-1.5_f32).to_bits()),
        ],
    ];
    match arrow_read_floats(&provider, &secret, &["F", "F10", "BD", "BF"]).await {
        Ok(rows) if rows == expected => {}
        other => misses.push(format!("read FLOAT/BINARY_*: {other:?}")),
    }

    // 2. Un FLOAT che f64 non ricostruisce: la read fallisce per mappatura.
    for literal in [
        "9007199254740993",
        "12345678901234567890123",
        "123456789.123456789",
        "1/3",
        // Distinti da 0.1 e da 1.0000000000000002 ma con gli stessi bit: una
        // conversione non iniettiva li renderebbe uguali.
        "0.10000000000000001",
        "1.0000000000000002220446049250313080847",
    ] {
        reset_floats(&provider, &secret).await;
        execute_sql(
            &provider,
            &secret,
            &format!("INSERT INTO {FLOATS} (ID, F) VALUES (1, {literal})"),
        )
        .await;
        let outcome = arrow_read_floats(&provider, &secret, &["F"]).await;
        if let Some(miss) = expect_data_mapping(literal, "read FLOAT", outcome) {
            misses.push(miss);
        }
    }

    // 3. Scrittura Float64 su FLOAT e BINARY_DOUBLE, riletta da Arrow e dal
    //    server (TO_CHAR): il FLOAT conserva il decimale piu breve dell'f64.
    reset_floats(&provider, &secret).await;
    let written = [
        0.1_f64,
        9_007_199_254_740_992.0,
        0.300_000_000_000_000_04,
        -1e-120,
    ];
    let batch = float_batch(
        &["F", "BD"],
        (1..=4).collect(),
        &[written.to_vec(), written.to_vec()],
    );
    match arrow_write(
        &provider,
        &secret,
        &write_operation(FLOATS, WriteMode::Append, &[], &[]),
        batch,
    )
    .await
    {
        Ok(4) => {
            let expected = written
                .iter()
                .map(|value| vec![value.to_bits(), value.to_bits()])
                .collect::<Vec<_>>();
            match arrow_read_floats(&provider, &secret, &["F", "BD"]).await {
                Ok(rows) if rows == expected => {}
                other => misses.push(format!("rilettura write FLOAT: {other:?}")),
            }
            let mut transaction = begin(&provider, &secret).await;
            let rows = transaction
                .query(
                    &Statement::new(format!(
                        "SELECT TO_CHAR(F, 'TM9') AS T FROM {FLOATS} ORDER BY ID"
                    )),
                    &CancellationToken::new(),
                )
                .await
                .expect("TO_CHAR FLOAT");
            let _ = transaction.rollback(&CancellationToken::new()).await;
            for (row, value) in rows.iter().zip(written) {
                let stored = text(row, "T");
                if parse_canon(&stored) != parse_canon(&format!("{value:e}")) {
                    misses.push(format!("{value:e}: FLOAT salvato {stored}"));
                }
            }
        }
        other => misses.push(format!("write Float64 su FLOAT: {other:?}")),
    }

    // 4. Valori che il FLOAT di destinazione non conserva: rifiuto esplicito,
    //    non zero ne arrotondamento.
    for (column, value) in [("F", 1e-300_f64), ("F10", 0.123_45)] {
        reset_floats(&provider, &secret).await;
        let outcome = arrow_write(
            &provider,
            &secret,
            &write_operation(FLOATS, WriteMode::Append, &[], &[]),
            float_batch(&[column], vec![1], &[vec![value]]),
        )
        .await;
        match outcome {
            Err(error) if error.contains("DataMapping") => {}
            other => misses.push(format!("{column} {value:e}: {other:?}")),
        }
    }

    // 5. -0.0 non e conservabile da Oracle (misurato: +0 in BINARY_FLOAT,
    //    FLOAT e NUMBER, byte non canonici in BINARY_DOUBLE): ogni write lo
    //    rifiuta per mappatura, Float64 su FLOAT/BINARY_DOUBLE e Float32 su
    //    BINARY_FLOAT, e cosi il bind di uno statement nativo.
    for column in ["F", "BD"] {
        reset_floats(&provider, &secret).await;
        let outcome = arrow_write(
            &provider,
            &secret,
            &write_operation(FLOATS, WriteMode::Append, &[], &[]),
            float_batch(&[column], vec![1], &[vec![-0.0]]),
        )
        .await;
        match outcome {
            Err(error) if error.contains("DataMapping") => {}
            other => misses.push(format!("{column} -0.0: {other:?}")),
        }
    }
    reset_floats(&provider, &secret).await;
    let float32 = RecordBatch::try_new(
        contract_schema(vec![
            Field::new("ID", DataType::Int32, false),
            Field::new("BF", DataType::Float32, false),
        ])
        .expect("schema"),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float32Array::from(vec![-0.0_f32])),
        ],
    )
    .expect("batch Float32");
    match arrow_write(
        &provider,
        &secret,
        &write_operation(FLOATS, WriteMode::Append, &[], &[]),
        float32,
    )
    .await
    {
        Err(error) if error.contains("DataMapping") => {}
        other => misses.push(format!("BF -0.0: {other:?}")),
    }
    let mut transaction = begin(&provider, &secret).await;
    let outcome = transaction
        .execute(
            &Statement::new(format!("INSERT INTO {FLOATS} (ID, BD) VALUES (1, :1)"))
                .with_params(vec![ParameterValue::F64(-0.0)]),
            &CancellationToken::new(),
        )
        .await;
    let _ = transaction.rollback(&CancellationToken::new()).await;
    if let Some(miss) = expect_data_mapping("-0.0", "bind nativo", outcome) {
        misses.push(miss);
    }

    // 6. Un -0.0 gia salvato dalle versioni precedenti (byte 7f ff ...,
    //    scritto qui con il driver senza il provider): la read lo rifiuta
    //    invece di renderlo +0.
    reset_floats(&provider, &secret).await;
    legacy_negative_zero().await;
    let outcome = arrow_read_floats(&provider, &secret, &["BD"]).await;
    if let Some(miss) = expect_data_mapping("-0.0 non canonico", "read BINARY_DOUBLE", outcome) {
        misses.push(miss);
    }
    drop_table(&provider, &secret, FLOATS).await;
    assert!(report("colonne FLOAT", &misses, 18), "{misses:?}");
}

/// Scrive con il driver, senza il provider, il -0.0 che le versioni fino ad
/// a6f07e2 legavano: Oracle lo conserva con byte non canonici.
async fn legacy_negative_zero() {
    let port = std::env::var("PLENORA_ORACLE_PORT").unwrap_or_else(|_| "1521".to_owned());
    let host = std::env::var("PLENORA_ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let service = std::env::var("PLENORA_ORACLE_SERVICE").unwrap_or_else(|_| "FREEPDB1".to_owned());
    let user = std::env::var("PLENORA_ORACLE_USER").unwrap_or_else(|_| "plenora".to_owned());
    let password =
        std::env::var("PLENORA_ORACLE_PASSWORD").unwrap_or_else(|_| "Plenora_Test_2026".to_owned());
    let connection =
        oracle_rs::Connection::connect(&format!("{host}:{port}/{service}"), &user, &password)
            .await
            .expect("connessione driver");
    connection
        .execute(
            &format!("INSERT INTO {FLOATS} (ID, BD) VALUES (1, :1)"),
            &[oracle_rs::Value::Float(-0.0)],
        )
        .await
        .expect("insert -0.0 legacy");
    connection.commit().await.expect("commit legacy");
    let dump = connection
        .query(&format!("SELECT DUMP(BD) FROM {FLOATS}"), &[])
        .await
        .expect("DUMP legacy");
    eprintln!("NUMBER Oracle: -0.0 legacy -> {:?}", dump.rows.first());
    let _ = connection.close().await;
}
