use plenora_database_core::arrow::array::Array;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    ObjectRef, SridPolicy, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::provider::{BatchStream, ProviderFuture, SecretString};
use plenora_database_core::{CancellationToken, ResourceBudget, ResourceLimits};
use plenora_db_sqlserver::{
    prepare_write_with_mode, read_object, write_prepared, CertificatePolicy, SqlServerConfig,
    SqlServerInsertMode, SqlServerPool,
};
use serde::Serialize;
use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const RESULT_PREFIX: &str = "PLENORA_SQLSERVER_PERF_RESULT=";
type AdminClient = Client<Compat<TcpStream>>;

struct MemoryBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl MemoryBatchStream {
    fn new(schema: SchemaRef, batches: &[RecordBatch]) -> Self {
        Self {
            schema,
            batches: batches.iter().cloned().collect(),
        }
    }
}

impl BatchStream for MemoryBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(&'a mut self, _cancellation: &'a plenora_database_core::CancellationToken) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.batches.pop_front())))
    }
}

#[derive(Debug, Serialize)]
struct ReadSample {
    iteration: usize,
    prepare_micros: u128,
    first_batch_micros: u128,
    total_micros: u128,
    rows: u64,
    batches: usize,
    materialized_bytes: u64,
    rows_per_second: u64,
}

#[derive(Debug, Serialize)]
struct WriteSample {
    iteration: usize,
    mode: &'static str,
    prepare_micros: u128,
    execute_micros: u128,
    total_micros: u128,
    rows: u64,
    rows_per_second: u64,
    differences: i64,
}

#[derive(Debug, Serialize)]
struct LifecycleSample {
    iteration: usize,
    mode: &'static str,
    total_micros: u128,
    rows: u64,
    rows_per_second: u64,
}

#[derive(Debug, Serialize)]
struct CampaignResult {
    schema_version: u32,
    profile: &'static str,
    rows: u64,
    batch_rows: usize,
    warmup: usize,
    repeat: usize,
    peak_rss_bytes: Option<u64>,
    reads: Vec<ReadSample>,
    writes: Vec<WriteSample>,
    lifecycle: Vec<LifecycleSample>,
}

fn config() -> SqlServerConfig {
    let host = std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".to_owned());
    let database =
        std::env::var("PLENORA_SQLSERVER_DATABASE").unwrap_or_else(|_| "dataflow_test".to_owned());
    let username =
        std::env::var("PLENORA_SQLSERVER_USER").unwrap_or_else(|_| "dataflow".to_owned());
    let password = std::env::var("PLENORA_SQLSERVER_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    SqlServerConfig::new(host, database, username, SecretString::new(password))
        .with_certificate_policy(CertificatePolicy::TrustServerCertificate)
}

fn env_usize(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    Ok(std::env::var(name).map_or_else(|_| Ok(default), |value| value.parse())?)
}

fn operation(object: &str, mode: WriteMode) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("plenora_perf".to_owned()),
            object: object.to_owned(),
            layer_id: None,
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: Some(SridPolicy::RequireMatch),
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn lifecycle_operation(object: &str, mode: WriteMode) -> WriteOperation {
    let mut value = operation(object, mode);
    value.keys = vec!["id".to_owned()];
    if mode == WriteMode::Replace {
        value.transaction_profile = TransactionProfile::StagedSwap;
    }
    value
}

fn rows_per_second(rows: u64, micros: u128) -> u64 {
    let scaled = u128::from(rows).saturating_mul(1_000_000);
    u64::try_from(scaled / micros.max(1)).unwrap_or(u64::MAX)
}

async fn drain(
    mut stream: Box<dyn BatchStream>,
    cancellation: &plenora_database_core::CancellationToken,
) -> Result<(u64, usize, u64, Vec<RecordBatch>), Box<dyn Error>> {
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch(cancellation).await? {
        rows = rows.saturating_add(u64::try_from(batch.num_rows())?);
        for array in batch.columns() {
            bytes = bytes.saturating_add(u64::try_from(array.get_array_memory_size())?);
        }
        batches.push(batch);
    }
    Ok((rows, batches.len(), bytes, batches))
}

async fn scalar_i64(client: &mut AdminClient, sql: String) -> Result<i64, Box<dyn Error>> {
    let mut results = client.simple_query(sql).await?.into_results().await?;
    Ok(results
        .pop()
        .and_then(|mut rows| rows.pop())
        .and_then(|row| row.try_get::<i64, _>(0).ok().flatten())
        .ok_or("risultato scalare SQL Server assente")?)
}

async fn admin_client() -> Result<AdminClient, Box<dyn Error>> {
    let host = std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".to_owned());
    let database =
        std::env::var("PLENORA_SQLSERVER_DATABASE").unwrap_or_else(|_| "dataflow_test".to_owned());
    let username =
        std::env::var("PLENORA_SQLSERVER_USER").unwrap_or_else(|_| "dataflow".to_owned());
    let password = std::env::var("PLENORA_SQLSERVER_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    let mut config = Config::new();
    config.host(host);
    config.port(1_433);
    config.database(database);
    config.authentication(AuthMethod::sql_server(username, password));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Client::connect(config, tcp.compat_write()).await?)
}

async fn execute_admin(
    client: &mut AdminClient,
    sql: impl Into<String>,
) -> Result<(), Box<dyn Error>> {
    client
        .simple_query(sql.into())
        .await?
        .into_results()
        .await?;
    Ok(())
}

fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1_024)
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    let rows = env_usize("PLENORA_SQLSERVER_PERF_ROWS", 5_000)?;
    let batch_rows = env_usize("PLENORA_SQLSERVER_PERF_BATCH_ROWS", 512)?;
    let warmup = env_usize("PLENORA_SQLSERVER_PERF_WARMUP", 1)?;
    let repeat = env_usize("PLENORA_SQLSERVER_PERF_REPEAT", 5)?;
    if rows == 0 || rows > 1_000_000 || batch_rows == 0 || repeat == 0 {
        return Err("parametri campagna SQL Server fuori intervallo".into());
    }
    let rows_i64 = i64::try_from(rows)?;
    let cancellation = CancellationToken::new();
    let sqlserver_config = config();
    let pool = SqlServerPool::new(sqlserver_config.clone(), 4)?;
    let mut admin = admin_client().await?;
    execute_admin(
        &mut admin,
        format!(
            "IF SCHEMA_ID(N'plenora_perf') IS NULL EXEC(N'CREATE SCHEMA [plenora_perf]'); \
                 DROP TABLE IF EXISTS [plenora_perf].[source]; \
                 DROP TABLE IF EXISTS [plenora_perf].[prepared_target]; \
                 DROP TABLE IF EXISTS [plenora_perf].[bulk_target]; \
                 DROP TABLE IF EXISTS [plenora_perf].[create_target]; \
                 DROP TABLE IF EXISTS [plenora_perf].[replace_target]; \
                 CREATE TABLE [plenora_perf].[source] \
                    ([id] int NOT NULL, [label] nvarchar(100) NOT NULL, [metric] float NOT NULL); \
                 CREATE TABLE [plenora_perf].[prepared_target] \
                    ([id] int NOT NULL, [label] nvarchar(100) NOT NULL, [metric] float NOT NULL); \
                 CREATE TABLE [plenora_perf].[bulk_target] \
                    ([id] int NOT NULL, [label] nvarchar(100) NOT NULL, [metric] float NOT NULL); \
                 WITH numbers AS \
                    (SELECT TOP ({rows_i64}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n \
                     FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b) \
                 INSERT INTO [plenora_perf].[source] ([id], [label], [metric]) \
                 SELECT CONVERT(int, n), CONCAT(N'row-', n), CONVERT(float, n) / 7.0 \
                 FROM numbers;"
        ),
    )
    .await?;

    let source_budget = ResourceBudget::new(ResourceLimits::default())?;
    let source_stream = read_object(
        &pool,
        "plenora_perf",
        "source",
        batch_rows,
        &source_budget,
        &cancellation,
    )
    .await?;
    let schema = source_stream.schema();
    let (source_rows, _, _, batches) = drain(source_stream, &cancellation).await?;
    if source_rows != u64::try_from(rows)? {
        return Err("fixture prestazionale SQL Server incompleta".into());
    }

    let mut reads = Vec::with_capacity(repeat);
    for iteration in 0..warmup.saturating_add(repeat) {
        let budget = ResourceBudget::new(ResourceLimits::default())?;
        let started = Instant::now();
        let mut stream = read_object(
            &pool,
            "plenora_perf",
            "source",
            batch_rows,
            &budget,
            &cancellation,
        )
        .await?;
        let prepared_at = started.elapsed().as_micros();
        let first = stream.next_batch(&cancellation).await?;
        let first_at = started.elapsed().as_micros();
        let mut measured_rows = 0_u64;
        let mut measured_batches = 0_usize;
        let mut measured_bytes = 0_u64;
        if let Some(batch) = first {
            measured_rows = u64::try_from(batch.num_rows())?;
            measured_batches = 1;
            for array in batch.columns() {
                measured_bytes =
                    measured_bytes.saturating_add(u64::try_from(array.get_array_memory_size())?);
            }
        }
        let (remaining_rows, remaining_batches, remaining_bytes, _) = drain(stream, &cancellation).await?;
        measured_rows = measured_rows.saturating_add(remaining_rows);
        measured_batches = measured_batches.saturating_add(remaining_batches);
        measured_bytes = measured_bytes.saturating_add(remaining_bytes);
        let total = started.elapsed().as_micros();
        if iteration >= warmup {
            reads.push(ReadSample {
                iteration: iteration - warmup,
                prepare_micros: prepared_at,
                first_batch_micros: first_at,
                total_micros: total,
                rows: measured_rows,
                batches: measured_batches,
                materialized_bytes: measured_bytes,
                rows_per_second: rows_per_second(measured_rows, total),
            });
        }
    }

    let mut writes = Vec::with_capacity(repeat * 2);
    for iteration in 0..warmup.saturating_add(repeat) {
        for (mode_name, target, insert_mode) in [
            ("prepared", "prepared_target", SqlServerInsertMode::Prepared),
            ("tds_bulk", "bulk_target", SqlServerInsertMode::TdsBulk),
        ] {
            execute_admin(
                &mut admin,
                format!("TRUNCATE TABLE [plenora_perf].[{target}];"),
            )
            .await?;
            let budget = ResourceBudget::new(ResourceLimits::default())?;
            let started = Instant::now();
            let prepared = prepare_write_with_mode(
                &pool,
                &operation(target, WriteMode::Append),
                Arc::clone(&schema),
                &budget,
                &cancellation,
                insert_mode,
            )
            .await?;
            let prepare_micros = started.elapsed().as_micros();
            let outcome = write_prepared(
                prepared,
                Box::new(MemoryBatchStream::new(Arc::clone(&schema), &batches)),
                &cancellation,
            )
            .await?;
            let total = started.elapsed().as_micros();
            let execute_micros = total.saturating_sub(prepare_micros);
            let confirmed = outcome.rows.confirmed;
            let differences = scalar_i64(
                &mut admin,
                format!(
                    "SELECT COUNT_BIG(*) FROM \
                     ((SELECT [id], [label], [metric] FROM [plenora_perf].[source] \
                       EXCEPT SELECT [id], [label], [metric] FROM [plenora_perf].[{target}]) \
                      UNION ALL \
                      (SELECT [id], [label], [metric] FROM [plenora_perf].[{target}] \
                       EXCEPT SELECT [id], [label], [metric] FROM [plenora_perf].[source])) AS d;"
                ),
            )
            .await?;
            if iteration >= warmup {
                writes.push(WriteSample {
                    iteration: iteration - warmup,
                    mode: mode_name,
                    prepare_micros,
                    execute_micros,
                    total_micros: total,
                    rows: confirmed,
                    rows_per_second: rows_per_second(confirmed, total),
                    differences,
                });
            }
        }
    }

    let mut lifecycle = Vec::with_capacity(repeat * 2);
    for iteration in 0..warmup.saturating_add(repeat) {
        execute_admin(
            &mut admin,
            "DROP TABLE IF EXISTS [plenora_perf].[create_target]; \
                     DROP TABLE IF EXISTS [plenora_perf].[replace_target]; \
                     CREATE TABLE [plenora_perf].[replace_target] \
                     ([id] int NOT NULL PRIMARY KEY, [label] nvarchar(100) NOT NULL, \
                      [metric] float NOT NULL); \
                     INSERT INTO [plenora_perf].[replace_target] VALUES (0, N'sentinel', 0.0);",
        )
        .await?;
        for (mode_name, target, write_mode) in [
            ("create", "create_target", WriteMode::Create),
            ("replace", "replace_target", WriteMode::Replace),
        ] {
            let budget = ResourceBudget::new(ResourceLimits::default())?;
            let started = Instant::now();
            let prepared = prepare_write_with_mode(
                &pool,
                &lifecycle_operation(target, write_mode),
                Arc::clone(&schema),
                &budget,
                &cancellation,
                SqlServerInsertMode::Prepared,
            )
            .await?;
            let outcome = write_prepared(
                prepared,
                Box::new(MemoryBatchStream::new(Arc::clone(&schema), &batches)),
                &cancellation,
            )
            .await?;
            let total = started.elapsed().as_micros();
            if iteration >= warmup {
                lifecycle.push(LifecycleSample {
                    iteration: iteration - warmup,
                    mode: mode_name,
                    total_micros: total,
                    rows: outcome.rows.confirmed,
                    rows_per_second: rows_per_second(outcome.rows.confirmed, total),
                });
            }
        }
    }

    execute_admin(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_perf].[source]; \
                 DROP TABLE IF EXISTS [plenora_perf].[prepared_target]; \
                 DROP TABLE IF EXISTS [plenora_perf].[bulk_target]; \
                 DROP TABLE IF EXISTS [plenora_perf].[create_target]; \
                 DROP TABLE IF EXISTS [plenora_perf].[replace_target];",
    )
    .await?;

    let result = CampaignResult {
        schema_version: 1,
        profile: "narrow",
        rows: u64::try_from(rows)?,
        batch_rows,
        warmup,
        repeat,
        peak_rss_bytes: peak_rss_bytes(),
        reads,
        writes,
        lifecycle,
    };
    println!("{RESULT_PREFIX}{}", serde_json::to_string(&result)?);
    Ok(())
}
