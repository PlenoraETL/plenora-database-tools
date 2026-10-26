//! La regola S su Oracle, live (`plenora_database_core::store_rewrite`):
//! un valore che Oracle riscrive al salvataggio non si scrive in silenzio.
//!
//! Sesta rilettura indipendente del 25/10, su f500235: Append `Strict` di
//! `[(1,""),(2,"x")]` dava `committed`, `inserted 2`, `LossReport` vuoto e
//! la riga 1 con `V IS NULL`; due Upsert `Strict` sulla chiave `V = ''` di
//! un `UNIQUE (V)` confermavano 1 ciascuno e lasciavano due righe con `V`
//! nullo.

use crate::{OracleConfig, OracleProvider, OracleTlsMode};
use plenora_database_core::arrow::array::{
    ArrayRef, BinaryArray, Float64Array, Int32Array, StringArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    ObjectRef, ProviderKind, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{
    BatchStream, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory, RemoteEffect};
use std::collections::VecDeque;
use std::sync::Arc;

fn fixture() -> (OracleProvider, SecretString) {
    let host = std::env::var("PLENORA_ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("PLENORA_ORACLE_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1521);
    let service = std::env::var("PLENORA_ORACLE_SERVICE").unwrap_or_else(|_| "FREEPDB1".to_owned());
    let user = std::env::var("PLENORA_ORACLE_USER").unwrap_or_else(|_| "plenora".to_owned());
    let password =
        std::env::var("PLENORA_ORACLE_PASSWORD").unwrap_or_else(|_| "Plenora_Test_2026".to_owned());
    let config = OracleConfig::new(host, service, user)
        .with_port(port)
        .with_tls_mode(OracleTlsMode::Disable);
    (
        OracleProvider::new(config).expect("config fixture Oracle"),
        SecretString::new(password),
    )
}

struct Batches {
    schema: SchemaRef,
    queue: VecDeque<RecordBatch>,
    rows: u64,
}

impl Batches {
    fn new(batch: RecordBatch) -> Self {
        Self {
            schema: batch.schema(),
            rows: batch.num_rows() as u64,
            queue: VecDeque::from([batch]),
        }
    }
}

impl BatchStream for Batches {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.queue.pop_front()) })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.rows)
    }
}

fn operation(table: &str, mode: WriteMode, keys: &[&str]) -> WriteOperation {
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
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

async fn ddl(provider: &OracleProvider, secret: &SecretString, sql: &str) {
    provider
        .execute_ddl(secret, sql, &CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("DDL Oracle: {error:?}"));
}

async fn dml(provider: &OracleProvider, secret: &SecretString, sql: &str) {
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione DML");
    transaction
        .execute(&Statement::new(sql.to_owned()), &cancellation)
        .await
        .expect("DML Oracle");
    transaction.commit(&cancellation).await.expect("COMMIT DML");
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

async fn write(
    provider: &OracleProvider,
    secret: &SecretString,
    operation: &WriteOperation,
    batch: RecordBatch,
) -> plenora_database_core::Result<plenora_database_core::outcome::WriteOutcome> {
    let cancellation = CancellationToken::new();
    let budget = budget();
    let prepared = provider
        .prepare_write(secret, operation, batch.schema(), &budget, &cancellation)
        .await?;
    assert_eq!(
        prepared.loss_report.policy, operation.mapping_policy,
        "il LossReport pubblica la politica richiesta"
    );
    provider
        .write(
            secret,
            prepared,
            Box::new(Batches::new(batch)),
            &budget,
            &cancellation,
        )
        .await
}

async fn count(provider: &OracleProvider, secret: &SecretString, sql: &str) -> i64 {
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione di verifica");
    let rows = transaction
        .query(&Statement::new(sql.to_owned()), &cancellation)
        .await
        .expect("conteggio");
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback verifica");
    match rows[0].get("N") {
        Some(ParameterValue::I64(value)) => *value,
        other => panic!("conteggio inatteso: {other:?}"),
    }
}

fn assert_rewritten(error: &plenora_database_core::DatabaseError, key: bool) {
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert_eq!(error.provider, Some(ProviderKind::Oracle));
    let expected = if key {
        plenora_database_core::store_rewrite::REWRITTEN_KEY
    } else {
        plenora_database_core::store_rewrite::REWRITTEN_VALUE
    };
    assert!(error.message.contains(expected), "{error:?}");
}

fn text_batch(fields: &[(&str, bool)], ids: Vec<i32>, columns: Vec<ArrayRef>) -> RecordBatch {
    let mut schema = vec![Field::new("ID", DataType::Int32, false)];
    schema.extend(fields.iter().map(|(name, _)| {
        let data_type = if name.starts_with('R') || name.starts_with('B') {
            DataType::Binary
        } else if name.starts_with('F') {
            DataType::Float64
        } else {
            DataType::Utf8
        };
        Field::new(*name, data_type, true)
    }));
    let schema = contract_schema(schema).expect("schema");
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(ids))];
    arrays.extend(columns);
    RecordBatch::try_new(schema, arrays).expect("batch")
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_s_testo_e_binario_vuoti_non_diventano_null_in_silenzio() {
    let (provider, secret) = fixture();
    let table = "PLN_S_VUOTI";
    drop_table(&provider, &secret, table).await;
    ddl(
        &provider,
        &secret,
        &format!(
            "CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, V VARCHAR2(10), C CHAR(3), \
             CL CLOB, NV NVARCHAR2(10), R RAW(8), B BLOB)"
        ),
    )
    .await;
    let append = operation(table, WriteMode::Append, &[]);
    // Il caso della rilettura: [(1,""),(2,"x")].
    let error = write(
        &provider,
        &secret,
        &append,
        text_batch(
            &[("V", true)],
            vec![1, 2],
            vec![Arc::new(StringArray::from(vec![Some(""), Some("x")]))],
        ),
    )
    .await
    .expect_err("il testo vuoto diventerebbe NULL");
    assert_rewritten(&error, false);
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{error:?}");
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!("SELECT COUNT(*) AS N FROM {table}")
        )
        .await,
        0
    );
    // Ogni tipo di testo e di binario.
    for column in ["C", "CL", "NV"] {
        let error = write(
            &provider,
            &secret,
            &append,
            text_batch(
                &[(column, true)],
                vec![1],
                vec![Arc::new(StringArray::from(vec![Some("")]))],
            ),
        )
        .await
        .expect_err("testo vuoto");
        assert_rewritten(&error, false);
    }
    let error = write(
        &provider,
        &secret,
        &append,
        text_batch(
            &[("R", true)],
            vec![1],
            vec![Arc::new(BinaryArray::from(vec![Some(&b""[..])]))],
        ),
    )
    .await
    .expect_err("binario vuoto in RAW");
    assert_rewritten(&error, false);
    // Un BLOB vuoto si scrive come LOB vuoto, non come NULL: dal 25/10
    // (round 8) il driver lega i binari con la lunghezza a pezzi, e la regola
    // A lo misura esatto (`tests/fixtures/scrittura_server_oracle.json`).
    write(
        &provider,
        &secret,
        &append,
        text_batch(
            &[("B", true)],
            vec![1],
            vec![Arc::new(BinaryArray::from(vec![Some(&b""[..])]))],
        ),
    )
    .await
    .expect("BLOB vuoto");
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!("SELECT COUNT(*) AS N FROM {table} WHERE B IS NOT NULL AND DBMS_LOB.GETLENGTH(B) = 0")
        )
        .await,
        1,
        "il BLOB vuoto resta vuoto, non NULL"
    );
    dml(&provider, &secret, &format!("DELETE FROM {table}")).await;
    // CHAR(3) completa con spazi un testo piu corto.
    let error = write(
        &provider,
        &secret,
        &append,
        text_batch(
            &[("C", true)],
            vec![1],
            vec![Arc::new(StringArray::from(vec![Some("a")]))],
        ),
    )
    .await
    .expect_err("CHAR(3) completerebbe 'a'");
    assert_rewritten(&error, false);
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!("SELECT COUNT(*) AS N FROM {table}")
        )
        .await,
        0
    );
    // Il controllo: i valori che Oracle conserva si scrivono e si rileggono.
    let outcome = write(
        &provider,
        &secret,
        &append,
        text_batch(
            &[("V", true), ("C", true), ("CL", true), ("R", true)],
            vec![1, 2],
            vec![
                Arc::new(StringArray::from(vec![Some("x"), None])),
                Arc::new(StringArray::from(vec![Some("abc"), None])),
                Arc::new(StringArray::from(vec![Some("a\0b"), None])),
                Arc::new(BinaryArray::from(vec![Some(&b"\0"[..]), None])),
            ],
        ),
    )
    .await
    .expect("valori conservati");
    assert_eq!(outcome.rows.confirmed, 2);
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!(
                "SELECT COUNT(*) AS N FROM {table} WHERE ID = 1 AND V = 'x' AND C = 'abc' \
                 AND DBMS_LOB.COMPARE(CL, TO_CLOB('a' || CHR(0) || 'b')) = 0 AND R = HEXTORAW('00')"
            )
        )
        .await,
        1
    );
    drop_table(&provider, &secret, table).await;

    // Create + scrittura: CLOB e BLOB, lo stesso rifiuto.
    let created = "PLN_S_VUOTI_CREATE";
    drop_table(&provider, &secret, created).await;
    let error = write(
        &provider,
        &secret,
        &operation(created, WriteMode::Create, &["ID"]),
        text_batch(
            &[("V", true)],
            vec![1],
            vec![Arc::new(StringArray::from(vec![Some("")]))],
        ),
    )
    .await
    .expect_err("Create con testo vuoto");
    assert_rewritten(&error, false);
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!("SELECT COUNT(*) AS N FROM {created}")
        )
        .await,
        0
    );
    drop_table(&provider, &secret, created).await;
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_s_una_chiave_vuota_o_completata_non_e_una_chiave() {
    let (provider, secret) = fixture();
    let table = "PLN_S_CHIAVI";
    drop_table(&provider, &secret, table).await;
    ddl(
        &provider,
        &secret,
        &format!(
            "CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, V VARCHAR2(10) UNIQUE, \
             C CHAR(3) UNIQUE)"
        ),
    )
    .await;
    // Due Upsert `Strict` sulla chiave V = '': su f500235 confermavano 1
    // ciascuno e lasciavano due righe con V nullo.
    for id in [1, 2] {
        let error = write(
            &provider,
            &secret,
            &operation(table, WriteMode::Upsert, &["V"]),
            text_batch(
                &[("V", true)],
                vec![id],
                vec![Arc::new(StringArray::from(vec![Some("")]))],
            ),
        )
        .await
        .expect_err("chiave vuota");
        assert_rewritten(&error, true);
    }
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!("SELECT COUNT(*) AS N FROM {table}")
        )
        .await,
        0
    );
    // CHAR(3): 'a' si salva come 'a  ', e `C = :1` con il bind 'a' non la
    // ritrova piu.
    let error = write(
        &provider,
        &secret,
        &operation(table, WriteMode::Upsert, &["C"]),
        text_batch(
            &[("C", true)],
            vec![1],
            vec![Arc::new(StringArray::from(vec![Some("a")]))],
        ),
    )
    .await
    .expect_err("chiave completata");
    assert_rewritten(&error, true);
    // Update e DeleteByKeys: la stessa chiave non si confronta.
    dml(
        &provider,
        &secret,
        &format!("INSERT INTO {table} (ID, V, C) VALUES (7, 'k', 'a')"),
    )
    .await;
    let error = write(
        &provider,
        &secret,
        &operation(table, WriteMode::Update, &["C"]),
        text_batch(
            &[("C", true)],
            vec![8],
            vec![Arc::new(StringArray::from(vec![Some("a")]))],
        ),
    )
    .await
    .expect_err("Update con chiave completata");
    assert_rewritten(&error, true);
    let keys_only = {
        let schema = contract_schema(vec![Field::new("V", DataType::Utf8, true)]).expect("schema");
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![Some("")]))])
            .expect("batch")
    };
    let error = write(
        &provider,
        &secret,
        &operation(table, WriteMode::DeleteByKeys, &["V"]),
        keys_only,
    )
    .await
    .expect_err("DeleteByKeys con chiave vuota");
    assert_rewritten(&error, true);
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!("SELECT COUNT(*) AS N FROM {table}")
        )
        .await,
        1
    );
    // Il controllo: una chiave piena si scrive.
    let outcome = write(
        &provider,
        &secret,
        &operation(table, WriteMode::Upsert, &["C"]),
        text_batch(
            &[("C", true)],
            vec![9],
            vec![Arc::new(StringArray::from(vec![Some("abc")]))],
        ),
    )
    .await
    .expect("chiave piena");
    assert_eq!(outcome.rows.confirmed, 1);
    drop_table(&provider, &secret, table).await;
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_s_float_di_oracle_non_arrotonda_in_silenzio() {
    let (provider, secret) = fixture();
    let table = "PLN_S_FLOAT";
    drop_table(&provider, &secret, table).await;
    ddl(
        &provider,
        &secret,
        &format!(
            "CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, F FLOAT(126), FB BINARY_DOUBLE, \
             FP FLOAT(10))"
        ),
    )
    .await;
    let append = operation(table, WriteMode::Append, &[]);
    let floats = |name: &str, values: Vec<f64>| {
        let ids = (1..=i32::try_from(values.len()).expect("righe")).collect();
        text_batch(
            &[(name, true)],
            ids,
            vec![Arc::new(Float64Array::from(values))],
        )
    };
    for value in [1e-200, -0.0] {
        let error = write(&provider, &secret, &append, floats("F", vec![value]))
            .await
            .expect_err("FLOAT salverebbe 0");
        assert_rewritten(&error, false);
    }
    let error = write(&provider, &secret, &append, floats("FP", vec![1.23]))
        .await
        .expect_err("FLOAT(10) arrotonda");
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!("SELECT COUNT(*) AS N FROM {table}")
        )
        .await,
        0
    );
    // Il controllo: BINARY_DOUBLE conserva -0.0 (la codifica interna di
    // Oracle inverte i bit dei negativi: 0x7FFF... e -0.0) e 1e-300, FLOAT
    // un valore nel proprio intervallo.
    write(
        &provider,
        &secret,
        &append,
        floats("FB", vec![-0.0, 1e-300]),
    )
    .await
    .expect("BINARY_DOUBLE");
    assert_eq!(
        count(
            &provider,
            &secret,
            &format!(
                "SELECT COUNT(*) AS N FROM {table} WHERE ID = 1 \
                 AND DUMP(FB) = 'Typ=101 Len=8: 127,255,255,255,255,255,255,255' \
                 AND EXISTS (SELECT 1 FROM {table} WHERE ID = 2 AND FB > 0)"
            )
        )
        .await,
        1,
        "BINARY_DOUBLE conserva il segno di zero e 1e-300"
    );
    dml(&provider, &secret, &format!("DELETE FROM {table}")).await;
    write(&provider, &secret, &append, floats("F", vec![0.1, 1e-100]))
        .await
        .expect("FLOAT nel proprio intervallo");
    drop_table(&provider, &secret, table).await;
}
