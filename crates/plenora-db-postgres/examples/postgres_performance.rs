use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, ReadOperation, SridPolicy, TransactionProfile, WriteMode,
    WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::query::{
    ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::{PostgresInsertMode, PostgresPerformanceProfile, PostgresProvider};
use serde::Serialize;
use std::collections::VecDeque;
use std::error::Error;
use std::time::Instant;
use tokio_postgres::NoTls;

const RESULT_PREFIX: &str = "PLENORA_PERF_RESULT=";

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
        self.schema.clone()
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a plenora_database_core::CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.batches.pop_front())))
    }
}

#[derive(Clone, Copy)]
enum Profile {
    Narrow,
    Wide,
    Spatial,
}

impl Profile {
    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "narrow" => Ok(Self::Narrow),
            "wide" => Ok(Self::Wide),
            "spatial" => Ok(Self::Spatial),
            _ => Err("PLENORA_PERF_PROFILE deve essere narrow, wide o spatial".into()),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Narrow => "narrow",
            Self::Wide => "wide",
            Self::Spatial => "spatial",
        }
    }

    const fn source_table(self) -> &'static str {
        match self {
            Self::Narrow => "source_narrow",
            Self::Wide => "source_wide",
            Self::Spatial => "source_spatial",
        }
    }

    fn fixture_sql(self, rows: u64) -> String {
        let table = self.source_table();
        match self {
            Self::Narrow => format!(
                "DROP TABLE IF EXISTS plenora_perf.{table};
                 CREATE TABLE plenora_perf.{table} AS
                 SELECT value::bigint AS event_id,
                        (value::double precision / 7.0) AS metric,
                        (value % 2 = 0) AS active,
                        CASE WHEN value % 10 = 0 THEN NULL
                             ELSE ('n-' || value::text)::text END AS note
                 FROM generate_series(1, {rows}) AS value;
                 ANALYZE plenora_perf.{table};"
            ),
            Self::Wide => format!(
                "DROP TABLE IF EXISTS plenora_perf.{table};
                 CREATE TABLE plenora_perf.{table} AS
                 SELECT value::bigint AS event_id,
                        ('event-' || value::text || '-' ||
                         repeat(md5(value::text), 2))::text AS name,
                        (value::numeric / 100)::numeric(18,4) AS amount,
                        (value % 2 = 0) AS active,
                        timestamptz '2025-01-01 00:00:00+00' +
                            value * interval '1 second' AS observed_at,
                        jsonb_build_object(
                            'id', value,
                            'group', value % 17,
                            'enabled', value % 2 = 0
                        ) AS payload,
                        decode(repeat(substr(md5(value::text), 1, 16), 4), 'hex') AS bytes
                 FROM generate_series(1, {rows}) AS value;
                 ANALYZE plenora_perf.{table};"
            ),
            Self::Spatial => format!(
                "DROP TABLE IF EXISTS plenora_perf.{table};
                 CREATE TABLE plenora_perf.{table} AS
                 SELECT value::bigint AS event_id,
                        ('feature-' || value::text)::text AS name,
                        ST_SetSRID(
                            ST_MakePoint(
                                -180.0 + (value % 36000)::double precision / 100.0,
                                -90.0 + (value % 18000)::double precision / 100.0,
                                (value % 1000)::double precision
                            ),
                            4326
                        )::geometry(PointZ,4326) AS geom,
                        ST_SetSRID(
                            ST_MakePoint(
                                -180.0 + (value % 36000)::double precision / 100.0,
                                -90.0 + (value % 18000)::double precision / 100.0
                            ),
                            4326
                        )::geography(Point,4326) AS geog,
                        jsonb_build_object('rank', value % 101) AS attrs
                 FROM generate_series(1, {rows}) AS value;
                 ANALYZE plenora_perf.{table};"
            ),
        }
    }
}

#[derive(Debug, Serialize)]
struct ReadSample {
    iteration: usize,
    acquire_micros: u128,
    first_batch_micros: u128,
    remaining_micros: u128,
    total_micros: u128,
    rows: u64,
    batches: usize,
    min_batch_rows: usize,
    max_batch_rows: usize,
    max_batch_bytes: u64,
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
    wal_bytes: i64,
    differences: i64,
}

#[derive(Debug, Serialize)]
struct CampaignResult {
    schema_version: u32,
    profile: &'static str,
    rows: u64,
    batch_rows: usize,
    warmup: usize,
    repeat: usize,
    modes: Vec<&'static str>,
    configured_target_batch_bytes: Option<u64>,
    configured_schema_cache_entries: usize,
    configured_parameterized_read: bool,
    configured_parameterized_fast_path: bool,
    configured_query_ast: bool,
    postgres_version: String,
    postgis_version: String,
    peak_rss_bytes: Option<u64>,
    reads: Vec<ReadSample>,
    writes: Vec<WriteSample>,
    metrics: plenora_db_postgres::PostgresMetricsSnapshot,
}

fn required_env<T>(name: &str) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    Ok(std::env::var(name)?.parse()?)
}

fn optional_env<T>(name: &str, default: T) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn optional_env_value<T>(name: &str) -> Result<Option<T>, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    match std::env::var(name) {
        Ok(value) => Ok(Some(value.parse()?)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn target_name(profile: Profile, mode: PostgresInsertMode) -> String {
    let mode = match mode {
        PostgresInsertMode::CopyText => "copy_text",
        PostgresInsertMode::CopyBinary => "copy_binary",
        PostgresInsertMode::Prepared => "prepared",
    };
    format!("target_{}_{mode}", profile.name())
}

fn parse_modes() -> Result<Vec<PostgresInsertMode>, Box<dyn Error>> {
    let value = std::env::var("PLENORA_PERF_MODES")
        .unwrap_or_else(|_| "copy_text,copy_binary,prepared".to_owned());
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut modes = Vec::new();
    for name in value.split(',') {
        let mode = match name {
            "copy_text" => PostgresInsertMode::CopyText,
            "copy_binary" => PostgresInsertMode::CopyBinary,
            "prepared" => PostgresInsertMode::Prepared,
            _ => return Err("PLENORA_PERF_MODES contiene una strategia non valida".into()),
        };
        if !modes.contains(&mode) {
            modes.push(mode);
        }
    }
    Ok(modes)
}

const fn mode_name(mode: PostgresInsertMode) -> &'static str {
    match mode {
        PostgresInsertMode::CopyText => "copy_text",
        PostgresInsertMode::CopyBinary => "copy_binary",
        PostgresInsertMode::Prepared => "prepared",
    }
}

fn object(table: impl Into<String>) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("plenora_perf".to_owned()),
        object: table.into(),
    }
}

fn write_operation(profile: Profile, target: String) -> WriteOperation {
    WriteOperation {
        target: object(target),
        mode: WriteMode::Create,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: matches!(profile, Profile::Spatial).then_some(SridPolicy::RequireMatch),
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn profiled_provider(batch_rows: usize, mode: PostgresInsertMode) -> PostgresProvider {
    // Esempio benchmark contro Docker senza TLS. In produzione:
    // rimuovere `insecure_local_with_batch_rows` e usare `new(N)` che
    // ha TLS `Require` di default (ADR-011).
    match (batch_rows, mode) {
        (1_024, PostgresInsertMode::CopyText) => PostgresProvider::insecure_local(),
        (8_192, PostgresInsertMode::CopyBinary) => PostgresProvider::insecure_local()
            .with_performance_profile(PostgresPerformanceProfile::BalancedBulk),
        _ => PostgresProvider::insecure_local_with_batch_rows(batch_rows).with_insert_mode(mode),
    }
}

#[allow(clippy::too_many_lines)]
async fn materialize(
    provider: &PostgresProvider,
    secret: &SecretString,
    profile: Profile,
    iteration: usize,
    parameterized_read: bool,
    query_ast: bool,
) -> Result<(ReadSample, SchemaRef, Vec<RecordBatch>), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let operation = ReadOperation {
        source: object(profile.source_table()),
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: parameterized_read.then(|| FilterExpression::Gt {
            field: "event_id".to_owned(),
            parameter: "minimum_id".to_owned(),
        }),
        declared_crs: Vec::new(),
    };
    let parameters = if parameterized_read {
        ParameterBag::new(std::collections::BTreeMap::from([(
            "minimum_id".to_owned(),
            ParameterValue::I64(0),
        )]))
    } else {
        ParameterBag::default()
    };
    let total_started = Instant::now();
    let acquire_started = Instant::now();
    let mut stream = if query_ast {
        if !matches!(profile, Profile::Narrow) {
            return Err("il benchmark QueryOperation usa il profilo narrow".into());
        }
        let query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: object(profile.source_table()),
                alias: Some("source".to_owned()),
            }),
            derived_source: None,
            projection: ["event_id", "metric", "active", "note"]
                .into_iter()
                .map(|field| QueryProjection {
                    expression: QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("source".to_owned()),
                            field: field.to_owned(),
                        },
                    },
                    alias: None,
                })
                .collect(),
            joins: Vec::new(),
            filter: parameterized_read.then(|| QueryExpression::Compare {
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("source".to_owned()),
                        field: "event_id".to_owned(),
                    },
                }),
                operator: plenora_database_core::plan::ComparisonOperator::Gt,
                right: Box::new(QueryExpression::Parameter {
                    name: "minimum_id".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        };
        provider
            .query(secret, &query, &parameters, &budget, &cancellation)
            .await?
    } else {
        provider
            .read(secret, &operation, &parameters, &budget, &cancellation)
            .await?
    };
    let acquire_micros = acquire_started.elapsed().as_micros();
    let schema = stream.schema();

    let first_started = Instant::now();
    let first = stream.next_batch(&cancellation).await?;
    let first_batch_micros = first_started.elapsed().as_micros();
    let remaining_started = Instant::now();
    let mut batches = Vec::new();
    if let Some(batch) = first {
        batches.push(batch);
    }
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        batches.push(batch);
    }
    let remaining_micros = remaining_started.elapsed().as_micros();
    let total_micros = total_started.elapsed().as_micros();
    let rows = batches.iter().try_fold(0_u64, |total, batch| {
        let batch_rows = u64::try_from(batch.num_rows()).map_err(std::io::Error::other)?;
        total
            .checked_add(batch_rows)
            .ok_or_else(|| std::io::Error::other("conteggio righe oltre u64"))
    })?;
    let batch_bytes = batches
        .iter()
        .map(|batch| {
            batch
                .columns()
                .iter()
                .try_fold(0_u64, |column_total, column| {
                    let column_bytes = u64::try_from(column.get_array_memory_size())
                        .map_err(std::io::Error::other)?;
                    column_total
                        .checked_add(column_bytes)
                        .ok_or_else(|| std::io::Error::other("dimensione Arrow oltre u64"))
                })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let materialized_bytes = batch_bytes.iter().try_fold(0_u64, |total, bytes| {
        total
            .checked_add(*bytes)
            .ok_or_else(|| std::io::Error::other("dimensione Arrow oltre u64"))
    })?;
    let rows_per_second = throughput(rows, total_micros);
    Ok((
        ReadSample {
            iteration,
            acquire_micros,
            first_batch_micros,
            remaining_micros,
            total_micros,
            rows,
            batches: batches.len(),
            min_batch_rows: batches.iter().map(RecordBatch::num_rows).min().unwrap_or(0),
            max_batch_rows: batches.iter().map(RecordBatch::num_rows).max().unwrap_or(0),
            max_batch_bytes: batch_bytes.into_iter().max().unwrap_or(0),
            materialized_bytes,
            rows_per_second,
        },
        schema,
        batches,
    ))
}

async fn wal_position(client: &tokio_postgres::Client) -> Result<i64, tokio_postgres::Error> {
    Ok(client
        .query_one(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'::pg_lsn)::bigint",
            &[],
        )
        .await?
        .get(0))
}

struct WriteDataset<'a> {
    profile: Profile,
    batch_rows: usize,
    schema: SchemaRef,
    batches: &'a [RecordBatch],
}

async fn write_once(
    client: &tokio_postgres::Client,
    secret: &SecretString,
    iteration: usize,
    mode: PostgresInsertMode,
    dataset: &WriteDataset<'_>,
) -> Result<WriteSample, Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let target = target_name(dataset.profile, mode);
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS plenora_perf.{target} CASCADE"
        ))
        .await?;
    let provider = profiled_provider(dataset.batch_rows, mode);
    let operation = write_operation(dataset.profile, target.clone());
    let total_started = Instant::now();
    let prepare_started = Instant::now();
    let prepared = provider
        .prepare_write(
            secret,
            &operation,
            dataset.schema.clone(),
            &budget,
            &cancellation,
        )
        .await?;
    let prepare_micros = prepare_started.elapsed().as_micros();
    let before_wal = wal_position(client).await?;
    let execute_started = Instant::now();
    let outcome = provider
        .write(
            secret,
            prepared,
            Box::new(MemoryBatchStream::new(
                dataset.schema.clone(),
                dataset.batches,
            )),
            &budget,
            &cancellation,
        )
        .await?;
    let execute_micros = execute_started.elapsed().as_micros();
    let total_micros = total_started.elapsed().as_micros();
    let wal_bytes = wal_position(client).await?.saturating_sub(before_wal);
    let differences: i64 = client
        .query_one(
            &format!(
                "SELECT count(*)::bigint
                 FROM (
                    (SELECT * FROM plenora_perf.{source}
                     EXCEPT ALL
                     SELECT * FROM plenora_perf.{target})
                    UNION ALL
                    (SELECT * FROM plenora_perf.{target}
                     EXCEPT ALL
                     SELECT * FROM plenora_perf.{source})
                ) AS differences",
                source = dataset.profile.source_table()
            ),
            &[],
        )
        .await?
        .get(0);
    if differences != 0 || outcome.rows.confirmed != outcome.rows.received {
        return Err("scrittura non equivalente alla sorgente".into());
    }
    Ok(WriteSample {
        iteration,
        mode: mode_name(mode),
        prepare_micros,
        execute_micros,
        total_micros,
        rows: outcome.rows.confirmed,
        rows_per_second: throughput(outcome.rows.confirmed, execute_micros),
        wal_bytes,
        differences,
    })
}

fn throughput(rows: u64, micros: u128) -> u64 {
    if micros == 0 {
        return 0;
    }
    let per_second = u128::from(rows).saturating_mul(1_000_000) / micros;
    u64::try_from(per_second).unwrap_or(u64::MAX)
}

fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PLENORA_TEST_POSTGRES_DSN")?;
    let rows = required_env::<u64>("PLENORA_PERF_ROWS")?;
    if rows == 0 || rows > 100_000_000 {
        return Err("PLENORA_PERF_ROWS fuori intervallo 1..=100000000".into());
    }
    let batch_rows = optional_env("PLENORA_PERF_BATCH_ROWS", 8_192_usize)?;
    let warmup = optional_env("PLENORA_PERF_WARMUP", 1_usize)?;
    let repeat = optional_env("PLENORA_PERF_REPEAT", 5_usize)?;
    let modes = parse_modes()?;
    let configured_target_batch_bytes =
        optional_env_value::<u64>("PLENORA_PERF_TARGET_BATCH_BYTES")?;
    let configured_schema_cache_entries =
        optional_env("PLENORA_PERF_SCHEMA_CACHE_ENTRIES", 256_usize)?;
    let configured_parameterized_read = optional_env("PLENORA_PERF_PARAMETERIZED_READ", false)?;
    let configured_parameterized_fast_path =
        optional_env("PLENORA_PERF_PARAMETERIZED_FAST_PATH", true)?;
    let configured_query_ast = optional_env("PLENORA_PERF_QUERY_AST", false)?;
    if batch_rows == 0 || repeat == 0 {
        return Err("batch_rows e repeat devono essere maggiori di zero".into());
    }
    let profile = Profile::parse(
        &std::env::var("PLENORA_PERF_PROFILE").unwrap_or_else(|_| "narrow".to_owned()),
    )?;
    let secret = SecretString::new(dsn.clone());
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connessione controllo benchmark terminata: {error}");
        }
    });
    client
        .batch_execute(
            "CREATE EXTENSION IF NOT EXISTS postgis;
             CREATE SCHEMA IF NOT EXISTS plenora_perf;",
        )
        .await?;
    client.batch_execute(&profile.fixture_sql(rows)).await?;
    let postgres_version: String = client.query_one("SHOW server_version", &[]).await?.get(0);
    let postgis_version: String = client
        .query_one("SELECT postgis_lib_version()", &[])
        .await?
        .get(0);

    let mut provider = match batch_rows {
        1_024 => PostgresProvider::insecure_local(),
        8_192 => PostgresProvider::insecure_local()
            .with_performance_profile(PostgresPerformanceProfile::BalancedBulk),
        _ => PostgresProvider::insecure_local_with_batch_rows(batch_rows),
    };
    if let Some(target) = configured_target_batch_bytes {
        provider = provider.with_target_batch_bytes(target);
    }
    let provider = provider
        .with_pool_size(4, 10_000)
        .with_schema_cache_capacity(configured_schema_cache_entries)
        .with_parameterized_read_fast_path(configured_parameterized_fast_path);
    let mut reads = Vec::with_capacity(repeat);
    let mut writes = Vec::with_capacity(repeat.saturating_mul(modes.len()));
    for iteration in 0..warmup.saturating_add(repeat) {
        let (read, schema, batches) = materialize(
            &provider,
            &secret,
            profile,
            iteration,
            configured_parameterized_read,
            configured_query_ast,
        )
        .await?;
        let dataset = WriteDataset {
            profile,
            batch_rows,
            schema,
            batches: &batches,
        };
        let mut iteration_writes = Vec::with_capacity(modes.len());
        for &mode in &modes {
            iteration_writes.push(write_once(&client, &secret, iteration, mode, &dataset).await?);
        }
        if iteration >= warmup {
            reads.push(ReadSample {
                iteration: iteration - warmup,
                ..read
            });
            writes.extend(iteration_writes.into_iter().map(|sample| WriteSample {
                iteration: iteration - warmup,
                ..sample
            }));
        }
    }
    let result = CampaignResult {
        schema_version: 1,
        profile: profile.name(),
        rows,
        batch_rows,
        warmup,
        repeat,
        modes: modes.iter().copied().map(mode_name).collect(),
        configured_target_batch_bytes,
        configured_schema_cache_entries,
        configured_parameterized_read,
        configured_parameterized_fast_path,
        configured_query_ast,
        postgres_version,
        postgis_version,
        peak_rss_bytes: peak_rss_bytes(),
        reads,
        writes,
        metrics: provider.metrics_snapshot(),
    };
    println!("{RESULT_PREFIX}{}", serde_json::to_string(&result)?);
    Ok(())
}
