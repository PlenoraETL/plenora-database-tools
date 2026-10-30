//! La regola S su Db2, live (`plenora_database_core::store_rewrite`): un
//! valore che Db2 riscrive al salvataggio non si scrive in silenzio.
//!
//! Misurato il 25/10 su Db2 12.1.5 (sesta rilettura indipendente, la stessa
//! classe del testo vuoto di Oracle): sul percorso dei bind, `'abc   '`
//! verso `VARCHAR(3)` si salva come `'abc'`, `'a'` verso `CHAR(3)` come
//! `'a  '`, e un database creato con `DB2_COMPATIBILITY_VECTOR=ORA`
//! (`varchar2_compat = ON`) salva il testo vuoto come `NULL`.
//!
//! La prova della semantica `VARCHAR2` vuole un secondo database, creato con
//! il vettore di compatibilita, e sta in `write_rewrite_ora_tests.rs`.

use crate::{Db2Config, Db2Provider, Db2TlsMode};
use plenora_database_core::arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
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

pub fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

pub fn provider(database: &str) -> (Db2Provider, SecretString) {
    let port = environment("PLENORA_DB2_PORT", "50000")
        .parse()
        .expect("porta Db2 live");
    (
        Db2Provider::new(
            Db2Config::new(
                environment("PLENORA_DB2_HOST", "db2"),
                database.to_owned(),
                environment("PLENORA_DB2_USER", "db2inst1"),
            )
            .with_port(port)
            .with_tls_mode(Db2TlsMode::Disable),
        )
        .expect("provider Db2 live"),
        SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test")),
    )
}

struct Batches {
    schema: SchemaRef,
    queue: VecDeque<RecordBatch>,
    rows: u64,
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

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

pub fn operation(table: &str, mode: WriteMode, keys: &[&str]) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("DB2INST1".to_owned()),
            object: table.to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

/// Una colonna `ID` e una colonna di testo o di float.
pub fn batch(column: &str, ids: Vec<i32>, values: ArrayRef) -> RecordBatch {
    let data_type = values.data_type().clone();
    let schema = contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new(column, data_type, true),
    ])
    .expect("schema");
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids)), values]).expect("batch")
}

pub fn text(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

pub async fn write(
    provider: &Db2Provider,
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
    let rows = batch.num_rows() as u64;
    provider
        .write(
            secret,
            prepared,
            Box::new(Batches {
                schema: batch.schema(),
                queue: VecDeque::from([batch]),
                rows,
            }),
            &budget,
            &cancellation,
        )
        .await
}

/// Esegue `sql` (con `params`) e fa COMMIT, o ROLLBACK con `keep = false`;
/// restituisce le righe di `query`, lette nella stessa transazione.
pub async fn run(
    provider: &Db2Provider,
    secret: &SecretString,
    statements: &[(&str, Vec<ParameterValue>)],
    query: Option<&str>,
    keep: bool,
) -> Vec<plenora_database_core::row::Row> {
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione Db2");
    for (sql, params) in statements {
        let mut statement = Statement::new((*sql).to_owned());
        statement.params.clone_from(params);
        transaction
            .execute(&statement, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("statement Db2: {error:?}"));
    }
    let rows = match query {
        Some(sql) => transaction
            .query(&Statement::new(sql.to_owned()), &cancellation)
            .await
            .expect("lettura Db2"),
        None => Vec::new(),
    };
    if keep {
        transaction.commit(&cancellation).await.expect("COMMIT Db2");
    } else {
        transaction
            .rollback(&cancellation)
            .await
            .expect("ROLLBACK Db2");
    }
    rows
}

pub async fn reset(provider: &Db2Provider, secret: &SecretString, table: &str, columns: &str) {
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .expect("transazione DDL Db2");
    let _ = transaction
        .execute(
            &Statement::new(format!("DROP TABLE DB2INST1.{table}")),
            &cancellation,
        )
        .await;
    transaction
        .commit(&cancellation)
        .await
        .expect("COMMIT DROP");
    // Un indice unico ammette i NULL dove il vincolo UNIQUE di Db2 vuole
    // NOT NULL (SQL0542).
    run(
        provider,
        secret,
        &[
            (
                &format!("CREATE TABLE DB2INST1.{table} ({columns})"),
                Vec::new(),
            ),
            (
                &format!("CREATE UNIQUE INDEX DB2INST1.{table}_K ON DB2INST1.{table} (K)"),
                Vec::new(),
            ),
        ],
        None,
        true,
    )
    .await;
}

pub fn single_text(rows: &[plenora_database_core::row::Row]) -> Option<String> {
    match rows.first().and_then(|row| row.get_index(0)) {
        Some(ParameterValue::String(value)) => Some(value.clone()),
        Some(ParameterValue::Null { .. }) | None => None,
        other => panic!("valore inatteso: {other:?}"),
    }
}

/// Misura il driver senza le guardie del prodotto, sempre con rollback.
pub async fn measure_raw(
    provider: &Db2Provider,
    secret: &SecretString,
    sql: &str,
    value: ParameterValue,
    query: &str,
) -> Option<String> {
    use odbc_api::{parameter::InputParameter, IntoParameter};
    let config = provider.config().clone();
    let secret = secret.clone();
    let sql = sql.to_owned();
    let query = query.to_owned();
    tokio::task::spawn_blocking(move || {
        let (connection, timeout) = crate::connection::open_connection(&config, &secret)
            .expect("connessione della misura ODBC");
        connection.set_autocommit(false).expect("autocommit off");
        let value: Box<dyn InputParameter> = match value {
            ParameterValue::String(value) => Box::new(value.into_parameter()),
            ParameterValue::F64(value) => Box::new(value),
            _ => panic!("tipo non previsto dalla misura"),
        };
        let parameters: Vec<Box<dyn InputParameter>> = vec![Box::new(1_i32), value];
        let written = connection.execute(&sql, parameters.as_slice(), Some(timeout));
        let rows = written.map(|cursor| {
            drop(cursor);
            crate::connection::query_text(&connection, &query, &[], timeout, 1, 64)
        });
        connection.rollback().expect("rollback della misura ODBC");
        rows.expect("scrittura ODBC")
            .expect("lettura ODBC")
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().flatten())
    })
    .await
    .expect("worker della misura ODBC")
}

pub fn assert_rewritten(error: &plenora_database_core::DatabaseError, key: bool) {
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert_eq!(error.provider, Some(ProviderKind::Db2));
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{error:?}");
    let expected = if key {
        plenora_database_core::store_rewrite::REWRITTEN_KEY
    } else {
        plenora_database_core::store_rewrite::REWRITTEN_VALUE
    };
    assert!(error.message.contains(expected), "{error:?}");
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_s_db2_char_e_spazi_oltre_la_lunghezza_non_si_riscrivono() {
    let (provider, secret) = provider(&environment("PLENORA_DB2_DATABASE", "plenora"));
    let table = "PLN_S_TESTO";
    reset(
        &provider,
        &secret,
        table,
        "ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(3), C CHAR(3), K CHAR(3), D DOUBLE",
    )
    .await;
    // L'oracolo usa ODBC direttamente: l'API pubblica deve rifiutare
    // proprio i valori di cui qui misuriamo la riscrittura del server.
    let insert =
        |column: &str| format!("INSERT INTO DB2INST1.{table} (ID, {column}) VALUES (?, ?)");
    for (column, value, expression, expected) in [
        ("V", ParameterValue::String("abc   ".to_owned()), "V", "abc"),
        ("C", ParameterValue::String("a".to_owned()), "C", "a  "),
        (
            "D",
            ParameterValue::F64(-0.0),
            "VARCHAR(HEX(D), 16)",
            "0000000000000000",
        ),
    ] {
        let measured = measure_raw(
            &provider,
            &secret,
            &insert(column),
            value,
            &format!("SELECT {expression} FROM DB2INST1.{table} WHERE ID = 1"),
        )
        .await;
        assert_eq!(measured.as_deref(), Some(expected), "{column}");
    }
    // Il carattere NUL si conserva (la classe dei testi, misurata).
    let measured = run(
        &provider,
        &secret,
        &[(
            &insert("V"),
            vec![
                ParameterValue::I32(1),
                ParameterValue::String("a\0b".to_owned()),
            ],
        )],
        Some(&format!(
            "SELECT VARCHAR(HEX(V), 16) FROM DB2INST1.{table} WHERE ID = 1"
        )),
        false,
    )
    .await;
    assert_eq!(
        single_text(&measured).as_deref(),
        Some("610062"),
        "NUL conservato"
    );

    // La scrittura: ogni riscrittura si rifiuta con Strict.
    let append = operation(table, WriteMode::Append, &[]);
    for (column, value) in [("V", "abc   "), ("C", "a"), ("C", "abc ")] {
        let error = write(
            &provider,
            &secret,
            &append,
            batch(column, vec![1], text(vec![Some(value)])),
        )
        .await
        .expect_err("valore riscritto");
        if column == "C" && value == "a" {
            assert_rewritten(&error, false);
        } else {
            assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
            assert_eq!(error.provider, Some(ProviderKind::Db2));
            assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{error:?}");
            assert_eq!(error.message, "testo oltre la lunghezza della colonna: il server lo troncherebbe o lo rifiuterebbe");
        }
    }
    let error = write(
        &provider,
        &secret,
        &append,
        batch("D", vec![1], Arc::new(Float64Array::from(vec![-0.0]))),
    )
    .await
    .expect_err("-0.0 diventerebbe 0");
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{error:?}");
    // Una chiave completata: Upsert e Update rifiutano con ogni politica.
    for mode in [WriteMode::Upsert, WriteMode::Update] {
        let error = write(
            &provider,
            &secret,
            &operation(table, mode, &["K"]),
            batch("K", vec![2], text(vec![Some("a")])),
        )
        .await
        .expect_err("chiave completata");
        assert_rewritten(&error, true);
    }
    let rows = run(
        &provider,
        &secret,
        &[],
        Some(&format!("SELECT COUNT(*) FROM DB2INST1.{table}")),
        false,
    )
    .await;
    assert_eq!(
        rows[0].get_index(0),
        Some(&ParameterValue::I32(0)),
        "nessuna riga riscritta"
    );
    // Il controllo: una chiave piena si scrive; senza la semantica VARCHAR2
    // il testo vuoto resta vuoto.
    write(
        &provider,
        &secret,
        &operation(table, WriteMode::Upsert, &["K"]),
        batch("K", vec![3], text(vec![Some("abc")])),
    )
    .await
    .expect("chiave piena");
    run(
        &provider,
        &secret,
        &[
            (&format!("DELETE FROM DB2INST1.{table}"), Vec::new()),
            (&format!("DROP INDEX DB2INST1.{table}_K"), Vec::new()),
        ],
        None,
        true,
    )
    .await;
    write(
        &provider,
        &secret,
        &append,
        batch("V", vec![1, 2], text(vec![Some(""), Some("abc")])),
    )
    .await
    .expect("testo conservato");
    let rows = run(
        &provider,
        &secret,
        &[],
        Some(&format!(
            "SELECT COUNT(*) FROM DB2INST1.{table} WHERE (ID = 1 AND V = '') OR (ID = 2 AND V = 'abc')"
        )),
        false,
    )
    .await;
    assert_eq!(rows[0].get_index(0), Some(&ParameterValue::I32(2)));
}
