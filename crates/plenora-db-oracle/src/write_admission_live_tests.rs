//! La regola A su Oracle, live (`plenora_database_core::write_admission`).
//!
//! - [`live_sonde_scritture_server`]: la sonda. Misura ogni chiave
//!   d'ammissione di [`specs`], usando anche l'oracolo diretto per le coppie
//!   che le guardie o i tipi intermedi della libreria non lasciano misurare,
//!   scrive il referto (`PLENORA_SONDE_SCRITTURE_REFERTO`) e verifica che il
//!   server dia ancora il fixture: e l'oracolo di regressione. La esegue
//!   `scripts/sonde_scritture_server.py`.
//! - I difetti della settima rilettura indipendente del 25/10 (su 092c859,
//!   tutti `Strict`, `committed` e `LossReport` vuoto): `TIMESTAMP(3)` che
//!   arrotonda, il tipo `JSON` che normalizza i numeri, `BINARY_FLOAT` che
//!   salva `-0.0` come `+0`, e `TIMESTAMP WITH LOCAL TIME ZONE` che passava
//!   il preflight e si rifiutava al primo valore.

use crate::{OracleConfig, OracleProvider, OracleTlsMode};
use plenora_database_core::arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteStatus;
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

struct Hooks {
    provider: OracleProvider,
    secret: SecretString,
}

impl Hooks {
    fn new() -> Self {
        let (provider, secret) = fixture();
        Self { provider, secret }
    }

    fn schema_name(&self) -> String {
        self.provider.config().username().to_ascii_uppercase()
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
        Some(self.schema_name())
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
            "CREATE TABLE {PROBE_TABLE} (ID NUMBER(19) PRIMARY KEY, {columns})"
        )]
    }

    fn drop_sql(&self) -> String {
        format!("DROP TABLE {PROBE_TABLE} PURGE")
    }

    fn truncate_sql(&self) -> String {
        format!("TRUNCATE TABLE {PROBE_TABLE}")
    }

    fn direct_probe<'a>(
        &'a self,
        spec: &'a ProbeSpec,
        values: &'a dyn plenora_database_core::arrow::array::Array,
        row: usize,
    ) -> Option<ProviderFuture<'a, write_probe::DirectObservation>> {
        let native = spec.native_ddl.as_str();
        if native == "CLOB" || native == "BLOB" {
            return None; // I locator qualificati sono parte del trasporto LOB.
        }
        if matches!(
            spec.arrow,
            DataType::Utf8 | DataType::Binary | DataType::LargeBinary
        ) || native.contains("WITH LOCAL TIME ZONE")
        {
            Some(Box::pin(direct::observe(self, values, row)))
        } else {
            None
        }
    }

    fn admission_keys<'a>(
        &'a self,
        columns: &'a [(String, DataType)],
    ) -> ProviderFuture<'a, Vec<(AdmissionKey, TargetDimensions)>> {
        Box::pin(async move {
            let cancellation = CancellationToken::new();
            let pool = self.provider.pool_for(&self.secret)?;
            let mut connection = pool.checkout(&cancellation).await?;
            let config = self.provider.config();
            let server = crate::write_admission::server_facts(
                config,
                connection.connection()?,
                &cancellation,
            )
            .await?;
            let facts = crate::write_admission::column_facts(
                config,
                connection.connection()?,
                &self.schema_name(),
                PROBE_TABLE,
                &cancellation,
            )
            .await?;
            connection.allow_reuse();
            drop(connection);
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

/// Le chiavi che Oracle ammette in scrittura: ogni altra coppia resta
/// rifiutata (mondo chiuso).
fn specs() -> Vec<ProbeSpec> {
    let mut specs = vec![ProbeSpec::new("BOOLEAN", DataType::Boolean)];
    for arrow in [DataType::Int16, DataType::Int32, DataType::Int64] {
        specs.push(ProbeSpec::new("NUMBER", arrow.clone()));
        specs.push(ProbeSpec::new("NUMBER(*,0)", arrow.clone()));
        for precision in 1..=38 {
            specs.push(ProbeSpec::new(
                format!("NUMBER({precision})"),
                arrow.clone(),
            ));
        }
    }
    specs.push(ProbeSpec::new("BINARY_FLOAT", DataType::Float32));
    specs.push(ProbeSpec::new("BINARY_DOUBLE", DataType::Float64));
    specs.push(ProbeSpec::new("FLOAT(126)", DataType::Float64));
    for precision in 1..=38_u8 {
        for scale in 0..=precision {
            specs.push(ProbeSpec::new(
                format!("NUMBER({precision},{scale})"),
                DataType::Decimal128(precision, i8::try_from(scale).expect("scala")),
            ));
        }
    }
    for (native, lengths) in [
        ("VARCHAR2({} CHAR)", [1_u64, 10, 4000]),
        ("VARCHAR2({} BYTE)", [1, 10, 4000]),
        ("NVARCHAR2({})", [1, 10, 2000]),
        ("CHAR({} CHAR)", [1, 10, 2000]),
        ("CHAR({} BYTE)", [1, 10, 2000]),
        ("NCHAR({})", [1, 10, 1000]),
    ] {
        for length in lengths {
            specs.push(
                ProbeSpec::new(native.replace("{}", &length.to_string()), DataType::Utf8)
                    .with_dimension(length),
            );
        }
    }
    specs.push(ProbeSpec::new("CLOB", DataType::Utf8));
    specs.push(ProbeSpec::new("NCLOB", DataType::Utf8));
    specs.push(ProbeSpec::new("JSON", DataType::Utf8).document());
    for arrow in [DataType::Binary, DataType::LargeBinary] {
        for length in [1_u64, 10, 2000] {
            specs.push(
                ProbeSpec::new(format!("RAW({length})"), arrow.clone()).with_dimension(length),
            );
        }
        specs.push(ProbeSpec::new("BLOB", arrow));
    }
    specs.push(ProbeSpec::new("DATE", DataType::Date32));
    for precision in 0..=9 {
        specs.push(ProbeSpec::new(
            format!("TIMESTAMP({precision})"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ));
        for zone in ["UTC", "+00:00", "Europe/Rome"] {
            let arrow = DataType::Timestamp(TimeUnit::Microsecond, Some(zone.into()));
            specs.push(ProbeSpec::new(
                format!("TIMESTAMP({precision}) WITH TIME ZONE"),
                arrow.clone(),
            ));
            specs.push(ProbeSpec::new(
                format!("TIMESTAMP({precision}) WITH LOCAL TIME ZONE"),
                arrow,
            ));
        }
    }
    specs
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_sonde_scritture_server() {
    let hooks = Hooks::new();
    let cancellation = CancellationToken::new();
    let pool = hooks.provider.pool_for(&hooks.secret).expect("pool");
    let mut connection = pool.checkout(&cancellation).await.expect("connessione");
    let server = crate::write_admission::server_facts(
        hooks.provider.config(),
        connection.connection().expect("connessione"),
        &cancellation,
    )
    .await
    .expect("versione del server");
    connection.allow_reuse();
    drop(connection);
    let measured =
        write_probe::measure_server(&hooks, &specs(), &server.major, &server.version).await;
    write_probe::report_and_check(&measured, crate::write_admission::FIXTURE);
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

async fn ddl(hooks: &Hooks, sql: &str) {
    hooks
        .provider
        .execute_ddl(&hooks.secret, sql, &CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("DDL Oracle: {error:?}"));
}

async fn drop_table(hooks: &Hooks, table: &str) {
    let _ = hooks
        .provider
        .execute_ddl(
            &hooks.secret,
            &format!("DROP TABLE {table} PURGE"),
            &CancellationToken::new(),
        )
        .await;
}

async fn append(
    hooks: &Hooks,
    table: &str,
    value: ArrayRef,
) -> Result<plenora_database_core::outcome::WriteOutcome, DatabaseError> {
    let schema = contract_schema(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("V", value.data_type().clone(), true),
    ])
    .expect("schema");
    let rows = value.len();
    let ids = Int64Array::from((0..i64::try_from(rows).expect("righe")).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), value]).expect("batch");
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(hooks.schema_name()),
            object: table.to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = hooks
        .provider
        .prepare_write(
            &hooks.secret,
            &operation,
            batch.schema(),
            &budget,
            &cancellation,
        )
        .await?;
    assert!(
        prepared.loss_report.losses.is_empty(),
        "Strict: nessuna perdita dichiarata"
    );
    hooks
        .provider
        .write(
            &hooks.secret,
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

fn assert_rewritten(result: Result<plenora_database_core::outcome::WriteOutcome, DatabaseError>) {
    let error = result.expect_err("un valore riscritto al salvataggio non si scrive con Strict");
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert!(
        error
            .message
            .contains(plenora_database_core::store_rewrite::REWRITTEN_VALUE),
        "{error:?}"
    );
}

fn assert_committed(result: Result<plenora_database_core::outcome::WriteOutcome, DatabaseError>) {
    let outcome = result.expect("un valore che si rilegge uguale si scrive");
    assert_eq!(outcome.status, WriteStatus::Committed);
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_a_timestamp_3_non_arrotonda_in_silenzio() {
    let hooks = Hooks::new();
    let table = "PLN_A_TS3";
    drop_table(&hooks, table).await;
    ddl(
        &hooks,
        &format!("CREATE TABLE {table} (ID NUMBER(19) PRIMARY KEY, V TIMESTAMP(3))"),
    )
    .await;
    // 2026-01-01T00:00:00.123456: TIMESTAMP(3) lo salvava come .123.
    let micro = 1_767_225_600_123_456_i64;
    assert_rewritten(
        append(
            &hooks,
            table,
            Arc::new(TimestampMicrosecondArray::from(vec![micro])),
        )
        .await,
    );
    assert_committed(
        append(
            &hooks,
            table,
            Arc::new(TimestampMicrosecondArray::from(vec![micro - 456])),
        )
        .await,
    );
    drop_table(&hooks, table).await;
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_a_json_non_normalizza_i_numeri_in_silenzio() {
    let hooks = Hooks::new();
    let table = "PLN_A_JSON";
    drop_table(&hooks, table).await;
    ddl(
        &hooks,
        &format!("CREATE TABLE {table} (ID NUMBER(19) PRIMARY KEY, V JSON)"),
    )
    .await;
    // La coppia Utf8/JSON e chiusa: anche il documento semplice si
    // rifiuta prima di leggere i valori, non solo i numeri non rappresentabili.
    for document in [
        r#"{"a":1.0}"#,
        r#"{"a":12345678901234567890123456789012345678901234567890}"#,
        r#"{"a":1e400}"#,
        r#"{"a":1}"#,
    ] {
        let error = append(&hooks, table, Arc::new(StringArray::from(vec![document])))
            .await
            .expect_err("coppia JSON non qualificata");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(
            error.message,
            plenora_database_core::write_admission::REFUSED_PAIR
        );
    }
    drop_table(&hooks, table).await;
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_a_binary_float_non_perde_lo_zero_negativo_in_silenzio() {
    // Oracle non conserva `-0.0` in nessun tipo numerico (PR #136, misurato):
    // `BINARY_FLOAT`, `FLOAT` e `NUMBER` lo salvano `+0`, `BINARY_DOUBLE` in
    // una forma non canonica che il server non confronta uguale a `0`. Fino a
    // 092c859 si scriveva con `Strict` e `committed`; ora si rifiuta.
    let hooks = Hooks::new();
    for (table, native, value) in [
        (
            "PLN_A_BF",
            "BINARY_FLOAT",
            Arc::new(Float32Array::from(vec![-0.0_f32])) as ArrayRef,
        ),
        (
            "PLN_A_BD",
            "BINARY_DOUBLE",
            Arc::new(Float64Array::from(vec![-0.0_f64])) as ArrayRef,
        ),
        (
            "PLN_A_FL",
            "FLOAT(126)",
            Arc::new(Float64Array::from(vec![-0.0_f64])) as ArrayRef,
        ),
    ] {
        drop_table(&hooks, table).await;
        ddl(
            &hooks,
            &format!("CREATE TABLE {table} (ID NUMBER(19) PRIMARY KEY, V {native})"),
        )
        .await;
        let error = append(&hooks, table, value)
            .await
            .expect_err("lo zero negativo non si scrive in silenzio");
        assert_eq!(
            error.category,
            ErrorCategory::DataMapping,
            "{native}: {error:?}"
        );
        assert_ne!(
            error.remote_effect,
            plenora_database_core::RemoteEffect::Committed,
            "{native}: {error:?}"
        );
        drop_table(&hooks, table).await;
    }
    let table = "PLN_A_BF";
    ddl(
        &hooks,
        &format!("CREATE TABLE {table} (ID NUMBER(19) PRIMARY KEY, V BINARY_FLOAT)"),
    )
    .await;
    assert_committed(append(&hooks, table, Arc::new(Float32Array::from(vec![1.5_f32]))).await);
    drop_table(&hooks, table).await;
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_a_timestamp_con_zona_locale_si_rifiuta_nel_preflight() {
    let hooks = Hooks::new();
    let table = "PLN_A_TSLTZ";
    drop_table(&hooks, table).await;
    ddl(
        &hooks,
        &format!(
            "CREATE TABLE {table} (ID NUMBER(19) PRIMARY KEY, V TIMESTAMP(6) WITH LOCAL TIME ZONE)"
        ),
    )
    .await;
    let value = Arc::new(
        TimestampMicrosecondArray::from(vec![1_767_225_600_000_000_i64]).with_timezone("UTC"),
    );
    let error = append(&hooks, table, value)
        .await
        .expect_err("la coppia non scrive alcun valore");
    assert_eq!(error.category, ErrorCategory::Unsupported, "{error:?}");
    assert_eq!(
        error.phase,
        plenora_database_core::ErrorPhase::Prepare,
        "rifiutata nel preflight, non al primo valore: {error:?}"
    );
    drop_table(&hooks, table).await;
}

#[cfg(test)]
#[path = "write_probe_direct_live_tests.rs"]
mod direct;
