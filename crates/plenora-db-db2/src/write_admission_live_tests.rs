//! La regola A su Db2, live (`plenora_database_core::write_admission`).
//!
//! - [`live_sonde_scritture_server`]: la sonda. Misura ogni chiave
//!   d'ammissione di [`specs`] con il percorso di scrittura della libreria,
//!   scrive il referto (`PLENORA_SONDE_SCRITTURE_REFERTO`) e verifica che il
//!   server dia ancora il fixture: e l'oracolo di regressione. La esegue
//!   `scripts/sonde_scritture_server.py`, dentro il container Db2.
//! - Il difetto della settima rilettura indipendente del 25/10 (su 092c859,
//!   `Strict`, `committed` e `LossReport` vuoto): `TIMESTAMP(3)` che tronca
//!   i microsecondi, perche la `SCALE` del catalogo non entrava nel
//!   confronto dei tipi.

use super::write_rewrite_live_tests::{environment, provider};
use crate::connection::open_connection;
use crate::Db2Provider;
use plenora_database_core::arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::{WriteOutcome, WriteStatus};
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{BatchStream, Provider, ProviderFuture, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::write_admission::{AdmissionKey, TargetDimensions};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect,
};
use plenora_database_testkit::write_probe::{self, ProbeHooks, ProbeSpec, PROBE_TABLE};
use std::collections::VecDeque;
use std::sync::Arc;

pub const SCHEMA: &str = "DB2INST1";

fn database() -> String {
    environment("PLENORA_DB2_DATABASE", "plenora")
}

struct Hooks {
    provider: Db2Provider,
    secret: SecretString,
}

impl Hooks {
    fn new() -> Self {
        let (provider, secret) = provider(&database());
        Self { provider, secret }
    }
}

impl ProbeHooks for Hooks {
    fn provider(&self) -> &dyn Provider {
        &self.provider
    }

    fn secret(&self) -> &SecretString {
        &self.secret
    }

    fn schema(&self) -> Option<String> {
        Some(SCHEMA.to_owned())
    }

    fn columns(&self) -> (&'static str, &'static str) {
        ("ID", "V")
    }

    fn create_sql(&self, natives: &[String]) -> Vec<String> {
        let columns = natives
            .iter()
            .enumerate()
            .map(|(index, native)| format!("V{index} {native}"))
            .collect::<Vec<_>>()
            .join(", ");
        vec![format!(
            "CREATE TABLE {SCHEMA}.{PROBE_TABLE} (ID BIGINT NOT NULL PRIMARY KEY, {columns})"
        )]
    }

    fn drop_sql(&self) -> String {
        format!("DROP TABLE {SCHEMA}.{PROBE_TABLE}")
    }

    fn truncate_sql(&self) -> String {
        format!("TRUNCATE TABLE {SCHEMA}.{PROBE_TABLE} IMMEDIATE")
    }

    fn admission_keys<'a>(
        &'a self,
        columns: &'a [(String, DataType)],
    ) -> ProviderFuture<'a, Vec<(AdmissionKey, TargetDimensions)>> {
        Box::pin(async move {
            let config = self.provider.config().clone();
            let secret = self.secret.clone();
            let (server, facts) = tokio::task::spawn_blocking(move || {
                let (connection, timeout) = open_connection(&config, &secret)?;
                let server = crate::write_admission::server_facts(&connection, timeout)?;
                let facts = crate::write_admission::column_facts(
                    &connection,
                    timeout,
                    SCHEMA,
                    PROBE_TABLE,
                )?;
                Ok::<_, DatabaseError>((server, facts))
            })
            .await
            .expect("lettura dei fatti della sonda")?;
            Ok(columns
                .iter()
                .map(|(name, arrow)| {
                    let value = facts
                        .iter()
                        .find(|column| &column.name == name)
                        .expect("colonna della sonda");
                    crate::write_admission::admission_key(arrow, value, &server)
                })
                .collect())
        })
    }
}

/// Le chiavi che Db2 ammette in scrittura (ogni coppia che passa
/// `native_type_matches` di `crate::write`, con i parametri enumerati): ogni
/// altra coppia resta rifiutata (mondo chiuso).
///
/// `NUMERIC(p,s)` non c'e: il catalogo lo pubblica come `DECIMAL`, e la
/// chiave e la stessa.
///
/// `CHAR` e `VARCHAR` `FOR BIT DATA` nemmeno: il catalogo della libreria li
/// pubblica come `... FOR BIT DATA`, e `native_type_matches` li rifiuta.
fn specs() -> Vec<ProbeSpec> {
    let mut specs = vec![
        ProbeSpec::new("BOOLEAN", DataType::Boolean),
        ProbeSpec::new("SMALLINT", DataType::Int16),
        ProbeSpec::new("INTEGER", DataType::Int32),
        ProbeSpec::new("BIGINT", DataType::Int64),
        ProbeSpec::new("REAL", DataType::Float32),
        ProbeSpec::new("DOUBLE", DataType::Float64),
    ];
    for precision in 1..=31_u8 {
        for scale in 0..=precision {
            specs.push(ProbeSpec::new(
                format!("DECIMAL({precision},{scale})"),
                DataType::Decimal128(precision, i8::try_from(scale).expect("scala")),
            ));
        }
    }
    for (native, lengths) in [
        ("CHAR({} OCTETS)", [1_u64, 10, 255]),
        ("VARCHAR({} OCTETS)", [1, 10, 32_672]),
        ("CLOB({} OCTETS)", [1, 10, 2_147_483_647]),
        ("CHAR({} CODEUNITS32)", [1, 10, 63]),
        ("VARCHAR({} CODEUNITS32)", [1, 10, 8_168]),
        ("CLOB({} CODEUNITS32)", [1, 10, 536_870_911]),
    ] {
        for length in lengths {
            specs.push(
                ProbeSpec::new(native.replace("{}", &length.to_string()), DataType::Utf8)
                    .with_dimension(length),
            );
        }
    }
    specs.push(ProbeSpec::new("DATE", DataType::Date32));
    for precision in 0..=12 {
        specs.push(ProbeSpec::new(
            format!("TIMESTAMP({precision})"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ));
    }
    specs
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_sonde_scritture_server() {
    let hooks = Hooks::new();
    let config = hooks.provider.config().clone();
    let secret = hooks.secret.clone();
    let server = tokio::task::spawn_blocking(move || {
        let (connection, timeout) = open_connection(&config, &secret)?;
        crate::write_admission::server_facts(&connection, timeout)
    })
    .await
    .expect("lettura della versione")
    .expect("versione del server");
    let measured =
        write_probe::measure_server(&hooks, &specs(), &server.major, &server.version).await;
    write_probe::report_and_check(&measured, crate::write_admission::FIXTURE);
}

pub struct Batches {
    pub schema: SchemaRef,
    pub queue: VecDeque<RecordBatch>,
    pub rows: u64,
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

pub async fn ddl(provider: &Db2Provider, secret: &SecretString, sql: &str) {
    provider
        .execute_ddl(secret, sql, &CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("DDL Db2: {error:?}"));
}

pub async fn drop_table(provider: &Db2Provider, secret: &SecretString, table: &str) {
    let _ = provider
        .execute_ddl(
            secret,
            &format!("DROP TABLE {SCHEMA}.{table}"),
            &CancellationToken::new(),
        )
        .await;
}

/// Scrive `ids` e `values` in `table` (`ID`, `V`) con la mode e le chiavi
/// date, sotto `Strict`.
pub async fn write(
    provider: &Db2Provider,
    secret: &SecretString,
    table: &str,
    mode: WriteMode,
    keys: &[&str],
    ids: ArrayRef,
    values: ArrayRef,
) -> Result<WriteOutcome, DatabaseError> {
    let schema = contract_schema(vec![
        Field::new("ID", ids.data_type().clone(), false),
        Field::new("V", values.data_type().clone(), true),
    ])
    .expect("schema");
    let rows = values.len();
    let batch = RecordBatch::try_new(schema, vec![ids, values]).expect("batch");
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
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
    };
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = provider
        .prepare_write(secret, &operation, batch.schema(), &budget, &cancellation)
        .await?;
    assert!(
        prepared.loss_report.losses.is_empty(),
        "Strict: nessuna perdita dichiarata"
    );
    provider
        .write(
            secret,
            prepared,
            Box::new(Batches {
                schema: batch.schema(),
                rows: rows as u64,
                queue: VecDeque::from([batch]),
            }),
            &budget,
            &cancellation,
        )
        .await
}

fn ids(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

/// Il testo di `SELECT VARCHAR(V) ... WHERE ID = id`, letto fuori dalla
/// libreria.
async fn stored_text(
    provider: &Db2Provider,
    secret: &SecretString,
    table: &str,
    id: i64,
) -> Option<String> {
    let config = provider.config().clone();
    let secret = secret.clone();
    let sql = format!("SELECT VARCHAR(V) FROM {SCHEMA}.{table} WHERE ID = {id}");
    tokio::task::spawn_blocking(move || {
        let (connection, timeout) = open_connection(&config, &secret).expect("connessione");
        crate::connection::query_text(&connection, &sql, &[], timeout, 1, 64)
            .expect("lettura")
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().flatten())
    })
    .await
    .expect("lettura")
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_a_db2_timestamp_3_non_tronca_in_silenzio() {
    let (provider, secret) = provider(&database());
    let table = "PLN_A_TS3";
    drop_table(&provider, &secret, table).await;
    ddl(
        &provider,
        &secret,
        &format!("CREATE TABLE {SCHEMA}.{table} (ID BIGINT NOT NULL PRIMARY KEY, V TIMESTAMP(3))"),
    )
    .await;
    // 2026-01-01T00:00:00.123456: TIMESTAMP(3) lo salvava come .123, con
    // `Strict`, `committed` e il `LossReport` vuoto.
    let micro = 1_767_225_600_123_456_i64;
    let error = write(
        &provider,
        &secret,
        table,
        WriteMode::Append,
        &[],
        ids(vec![1]),
        Arc::new(TimestampMicrosecondArray::from(vec![micro])),
    )
    .await
    .expect_err("un valore troncato al salvataggio non si scrive con Strict");
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    // Il parametro viene rifiutato contro la precisione dell'operando,
    // prima della guardia sulle riscritture del server.
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(
        error.message,
        "timestamp oltre la precisione dell'operando: il server la arrotonderebbe"
    );
    assert_eq!(stored_text(&provider, &secret, table, 1).await, None);
    // Il controllo: un valore con tre cifre frazionarie si scrive e si
    // rilegge uguale.
    let outcome = write(
        &provider,
        &secret,
        table,
        WriteMode::Append,
        &[],
        ids(vec![2]),
        Arc::new(TimestampMicrosecondArray::from(vec![micro - 456])),
    )
    .await
    .expect("tre cifre frazionarie si conservano");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(
        stored_text(&provider, &secret, table, 2).await.as_deref(),
        Some("2026-01-01-00.00.00.123")
    );
    drop_table(&provider, &secret, table).await;
}
