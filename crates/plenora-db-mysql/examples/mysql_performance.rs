//! Campagna prestazionale riproducibile del provider `MySQL`.
//!
//! La campagna misura soltanto cio che il provider ha gia qualificato live:
//! la lettura Arrow streaming e la scrittura `Append` dentro il profilo
//! `SingleTransaction`. Create, replace, upsert, `LOAD DATA LOCAL INFILE` e
//! qualunque DDL restano fuori: il DDL di `MySQL` non e transazionale e non
//! puo comparire dentro una transazione promessa atomica. Il reset del target
//! e la verifica differenziale usano una connessione amministrativa separata,
//! fuori dalla transazione del provider e fuori dalle finestre misurate.

use mysql_async::prelude::Queryable;
use plenora_database_core::arrow::array::Array;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    ObjectRef, OrderBy, ReadOperation, SortDirection, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::{CancellationToken, ResourceBudget, ResourceLimits};
use plenora_db_mysql::{
    read_operation, MysqlConfig, MysqlPool, MysqlProvider, SESSION_BOOTSTRAP_SQL,
};
use serde::Serialize;
use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

const RESULT_PREFIX: &str = "PLENORA_MYSQL_PERF_RESULT=";
const SOURCE_TABLE: &str = "plenora_perf_source";
const TARGET_TABLE: &str = "plenora_perf_append_target";
const APPEND_MODE: &str = "append_single_transaction";
const PERF_TABLE_DDL: &str = "(\
     id BIGINT NOT NULL PRIMARY KEY, \
     label VARCHAR(64) NULL, \
     amount DECIMAL(12, 2) NULL, \
     active TINYINT(1) NOT NULL, \
     day DATE NULL, \
     moment DATETIME(6) NULL, \
     payload VARBINARY(32) NULL) ENGINE=InnoDB";

struct MemoryBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl MemoryBatchStream {
    fn new(schema: &SchemaRef, batches: &[RecordBatch]) -> Self {
        Self {
            schema: Arc::clone(schema),
            batches: batches.iter().cloned().collect(),
        }
    }
}

impl BatchStream for MemoryBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a plenora_database_core::CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
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
    transactions: u64,
}

/// I parametri della campagna, tutti limitati dall'ambiente prima di
/// raggiungere il server.
#[derive(Debug, Clone, Copy)]
struct Campaign {
    rows: usize,
    batch_rows: usize,
    warmup: usize,
    repeat: usize,
}

impl Campaign {
    const fn iterations(self) -> usize {
        self.warmup.saturating_add(self.repeat)
    }
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
}

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn env_bounded(
    name: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, Box<dyn Error>> {
    let value: usize = std::env::var(name).map_or_else(|_| Ok(default), |raw| raw.parse())?;
    if value < minimum || value > maximum {
        return Err(format!("{name} fuori intervallo [{minimum}, {maximum}]").into());
    }
    Ok(value)
}

fn required_environment(name: &str) -> Result<String, Box<dyn Error>> {
    let value = std::env::var(name).map_err(|_| format!("{name} obbligatoria"))?;
    if value.is_empty() {
        return Err(format!("{name} vuota").into());
    }
    Ok(value)
}

fn secret() -> Result<SecretString, Box<dyn Error>> {
    Ok(SecretString::new(required_environment(
        "PLENORA_MYSQL_PASSWORD",
    )?))
}

fn config() -> Result<MysqlConfig, Box<dyn Error>> {
    let config = MysqlConfig::new(
        environment("PLENORA_MYSQL_HOST", "mysql"),
        environment("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        environment("PLENORA_MYSQL_USER", "dataflow"),
        secret()?,
    )
    .with_port(environment("PLENORA_MYSQL_PORT", "3306").parse()?);
    Ok(config.with_private_ca_certificate(required_environment("PLENORA_MYSQL_CA")?))
}

/// La connessione amministrativa resta separata dal provider: mantiene TLS
/// obbligatorio e lo stesso bootstrap di sessione, cosi la fixture e la
/// verifica differenziale osservano lo stesso fuso e la stessa `sql_mode`.
async fn admin_connection() -> Result<mysql_async::Conn, Box<dyn Error>> {
    let ssl = mysql_async::SslOpts::default().with_root_certs(vec![std::path::PathBuf::from(
        required_environment("PLENORA_MYSQL_CA")?,
    )
    .into()]);
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(environment("PLENORA_MYSQL_HOST", "mysql"))
        .tcp_port(environment("PLENORA_MYSQL_PORT", "3306").parse()?)
        .db_name(Some(environment("PLENORA_MYSQL_DATABASE", "dataflow_test")))
        .user(Some(environment("PLENORA_MYSQL_USER", "dataflow")))
        .pass(Some(secret()?.expose().to_owned()))
        .prefer_socket(Some(false))
        .tcp_nodelay(true)
        .ssl_opts(Some(ssl))
        .setup(vec![SESSION_BOOTSTRAP_SQL]);
    Ok(mysql_async::Conn::new(opts).await?)
}

fn read_operation_plan(database: &str, table: &str) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(database.to_owned()),
            object: table.to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    }
}

/// L'unica forma di scrittura misurata: `Append` dentro una singola
/// transazione, senza chiavi, senza colonne di aggiornamento e senza
/// politica spaziale, come il piano write qualificato dai test live.
fn append_operation(database: &str) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(database.to_owned()),
            object: TARGET_TABLE.to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn campaign_budget(rows: usize) -> Result<ResourceBudget, Box<dyn Error>> {
    let bound = u64::try_from(rows)?
        .checked_mul(4)
        .ok_or("budget righe campagna MySQL fuori intervallo")?;
    Ok(ResourceBudget::new(ResourceLimits {
        rows: bound,
        memory_bytes: 512 * 1024 * 1024,
        output_bytes: 2 * 1024 * 1024 * 1024,
        // Il tetto per cella governa il residuo che il lettore esige prima di
        // accettare un'altra riga: tenerlo stretto rende `batch_rows` la
        // grandezza che davvero decide il batch emesso.
        cell_bytes: 1024 * 1024,
        duration_ms: 600_000,
        ..ResourceLimits::default()
    })?)
}

fn rows_per_second(rows: u64, micros: u128) -> u64 {
    let scaled = u128::from(rows).saturating_mul(1_000_000);
    u64::try_from(scaled / micros.max(1)).unwrap_or(u64::MAX)
}

fn batch_bytes(batch: &RecordBatch) -> Result<u64, Box<dyn Error>> {
    let mut bytes = 0_u64;
    for array in batch.columns() {
        bytes = bytes.saturating_add(u64::try_from(array.get_array_memory_size())?);
    }
    Ok(bytes)
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
        bytes = bytes.saturating_add(batch_bytes(&batch)?);
        batches.push(batch);
    }
    Ok((rows, batches.len(), bytes, batches))
}

/// DDL e popolamento della fixture: sempre sulla connessione amministrativa,
/// mai dentro la transazione del provider.
async fn prepare_fixture(admin: &mut mysql_async::Conn, rows: usize) -> Result<(), Box<dyn Error>> {
    let rows_i64 = i64::try_from(rows)?;
    let depth = rows.max(2).saturating_add(1);
    admin
        .query_drop(format!("DROP TABLE IF EXISTS `{SOURCE_TABLE}`"))
        .await?;
    admin
        .query_drop(format!("DROP TABLE IF EXISTS `{TARGET_TABLE}`"))
        .await?;
    admin
        .query_drop(format!("CREATE TABLE `{SOURCE_TABLE}` {PERF_TABLE_DDL}"))
        .await?;
    admin
        .query_drop(format!("CREATE TABLE `{TARGET_TABLE}` {PERF_TABLE_DDL}"))
        .await?;
    admin
        .query_drop(format!("SET SESSION cte_max_recursion_depth = {depth}"))
        .await?;
    admin
        .query_drop(format!(
            "INSERT INTO `{SOURCE_TABLE}` \
             (id, label, amount, active, day, moment, payload) \
             WITH RECURSIVE sequence (n) AS ( \
                 SELECT 1 UNION ALL SELECT n + 1 FROM sequence WHERE n < {rows_i64}) \
             SELECT n, \
                    IF(n % 2 = 1, CONCAT('etichetta-', n), NULL), \
                    IF(n % 2 = 1, CAST(n AS DECIMAL(12, 2)) / 100, NULL), \
                    n % 2, \
                    IF(n % 2 = 1, DATE '2026-01-02', NULL), \
                    IF(n % 2 = 1, TIMESTAMP '2026-01-02 03:04:05.123456', NULL), \
                    IF(n % 2 = 1, UNHEX('010203'), NULL) \
             FROM sequence"
        ))
        .await?;
    Ok(())
}

/// Reset del target fuori dalla transazione promessa atomica: `TRUNCATE` e
/// DDL implicito in `MySQL` e non puo essere osservato dentro l'`Append`.
async fn reset_target(admin: &mut mysql_async::Conn) -> Result<(), Box<dyn Error>> {
    admin
        .query_drop(format!("TRUNCATE TABLE `{TARGET_TABLE}`"))
        .await?;
    Ok(())
}

/// Differenziale simmetrico esatto fra sorgente e target: l'uguaglianza usa
/// `<=>` cosi le celle nulle contano come uguali soltanto fra loro.
async fn differences(admin: &mut mysql_async::Conn) -> Result<i64, Box<dyn Error>> {
    admin
        .query_first::<i64, _>(format!(
            "SELECT (SELECT COUNT(*) FROM `{SOURCE_TABLE}`) \
                  + (SELECT COUNT(*) FROM `{TARGET_TABLE}`) \
                  - 2 * (SELECT COUNT(*) \
                         FROM `{SOURCE_TABLE}` AS s \
                         JOIN `{TARGET_TABLE}` AS t \
                           ON t.id <=> s.id AND t.label <=> s.label \
                          AND t.amount <=> s.amount AND t.active <=> s.active \
                          AND t.day <=> s.day AND t.moment <=> s.moment \
                          AND t.payload <=> s.payload)"
        ))
        .await?
        .ok_or_else(|| "differenziale MySQL assente".into())
}

async fn global_commit_count(admin: &mut mysql_async::Conn) -> Result<u64, Box<dyn Error>> {
    let row = admin
        .query_first::<(String, String), _>("SHOW GLOBAL STATUS LIKE 'Com_commit'")
        .await?
        .ok_or("status globale MySQL Com_commit assente")?;
    Ok(row.1.parse()?)
}

async fn drop_fixture(admin: &mut mysql_async::Conn) -> Result<(), Box<dyn Error>> {
    admin
        .query_drop(format!("DROP TABLE IF EXISTS `{TARGET_TABLE}`"))
        .await?;
    admin
        .query_drop(format!("DROP TABLE IF EXISTS `{SOURCE_TABLE}`"))
        .await?;
    Ok(())
}

fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1_024)
}

async fn measure_reads(
    pool: &Arc<MysqlPool>,
    plan: &ReadOperation,
    campaign: Campaign,
    cancellation: &CancellationToken,
) -> Result<Vec<ReadSample>, Box<dyn Error>> {
    let mut reads = Vec::with_capacity(campaign.repeat);
    for iteration in 0..campaign.iterations() {
        let budget = campaign_budget(campaign.rows)?;
        let started = Instant::now();
        let mut stream = read_operation(
            pool,
            plan,
            &ParameterBag::default(),
            campaign.batch_rows,
            &budget,
            cancellation,
        )
        .await?;
        let prepare_micros = started.elapsed().as_micros();
        let first = stream.next_batch(cancellation).await?;
        let first_batch_micros = started.elapsed().as_micros();
        let (mut measured_rows, mut measured_batches, mut measured_bytes) = match first {
            Some(batch) => (
                u64::try_from(batch.num_rows())?,
                1_usize,
                batch_bytes(&batch)?,
            ),
            None => (0_u64, 0_usize, 0_u64),
        };
        let (remaining_rows, remaining_batches, remaining_bytes, _) =
            drain(stream, cancellation).await?;
        measured_rows = measured_rows.saturating_add(remaining_rows);
        measured_batches = measured_batches.saturating_add(remaining_batches);
        measured_bytes = measured_bytes.saturating_add(remaining_bytes);
        let total_micros = started.elapsed().as_micros();
        if measured_rows != u64::try_from(campaign.rows)? {
            return Err("lettura prestazionale MySQL incompleta".into());
        }
        if iteration >= campaign.warmup {
            reads.push(ReadSample {
                iteration: iteration - campaign.warmup,
                prepare_micros,
                first_batch_micros,
                total_micros,
                rows: measured_rows,
                batches: measured_batches,
                materialized_bytes: measured_bytes,
                rows_per_second: rows_per_second(measured_rows, total_micros),
            });
        }
    }
    Ok(reads)
}

async fn measure_appends(
    provider: &MysqlProvider,
    admin: &mut mysql_async::Conn,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    database: &str,
    campaign: Campaign,
    cancellation: &CancellationToken,
) -> Result<Vec<WriteSample>, Box<dyn Error>> {
    let operation = append_operation(database);
    let mut writes = Vec::with_capacity(campaign.repeat);
    for iteration in 0..campaign.iterations() {
        reset_target(admin).await?;
        let commits_before = global_commit_count(admin).await?;
        let budget = campaign_budget(campaign.rows)?;
        let started = Instant::now();
        let prepare_credential = secret()?;
        let prepared = provider
            .prepare_write(
                &prepare_credential,
                &operation,
                Arc::clone(schema),
                &budget,
                cancellation,
            )
            .await?;
        if !prepared.loss_report.permits_execution() || !prepared.loss_report.losses.is_empty() {
            return Err("preflight append MySQL con perdite dichiarate".into());
        }
        let prepare_micros = started.elapsed().as_micros();
        let write_credential = secret()?;
        let outcome = provider
            .write(
                &write_credential,
                prepared,
                Box::new(MemoryBatchStream::new(schema, batches)),
                &budget,
                cancellation,
            )
            .await?;
        let total_micros = started.elapsed().as_micros();
        outcome.validate()?;
        if outcome.status != plenora_database_core::outcome::WriteStatus::Committed
            || outcome.rows.confirmed != u64::try_from(campaign.rows)?
            || outcome.rows.failed != 0
            || outcome.rows.skipped != 0
        {
            return Err("append MySQL non committato per intero".into());
        }
        let observed = differences(admin).await?;
        let commits_after = global_commit_count(admin).await?;
        let transactions = commits_after
            .checked_sub(commits_before)
            .ok_or("contatore commit MySQL non monotono")?;
        if observed != 0 {
            return Err("differenziale append MySQL non nullo".into());
        }
        if transactions != 1 {
            return Err(format!("append MySQL ha osservato {transactions} commit").into());
        }
        if iteration >= campaign.warmup {
            writes.push(WriteSample {
                iteration: iteration - campaign.warmup,
                mode: APPEND_MODE,
                prepare_micros,
                execute_micros: total_micros.saturating_sub(prepare_micros),
                total_micros,
                rows: outcome.rows.confirmed,
                rows_per_second: rows_per_second(outcome.rows.confirmed, total_micros),
                differences: observed,
                transactions,
            });
        }
    }
    Ok(writes)
}

async fn run_campaign(
    campaign: Campaign,
    config: plenora_db_mysql::MysqlConfig,
    cancellation: &CancellationToken,
    admin: &mut mysql_async::Conn,
) -> Result<CampaignResult, Box<dyn Error>> {
    let database = config.database().to_owned();
    prepare_fixture(admin, campaign.rows).await?;

    let pool = Arc::new(MysqlPool::new(&config, 4)?);
    let plan = read_operation_plan(&database, SOURCE_TABLE);
    let fixture_budget = campaign_budget(campaign.rows)?;
    let stream = read_operation(
        &pool,
        &plan,
        &ParameterBag::default(),
        campaign.batch_rows,
        &fixture_budget,
        cancellation,
    )
    .await?;
    let schema = stream.schema();
    let (fixture_rows, _, _, batches) = drain(stream, cancellation).await?;
    if fixture_rows != u64::try_from(campaign.rows)? {
        return Err("fixture prestazionale MySQL incompleta".into());
    }

    let reads = measure_reads(&pool, &plan, campaign, cancellation).await?;

    let provider = MysqlProvider::new(config, 4)?;
    let writes = measure_appends(
        &provider,
        admin,
        &schema,
        &batches,
        &database,
        campaign,
        cancellation,
    )
    .await?;

    Ok(CampaignResult {
        schema_version: 1,
        profile: "append-single-transaction",
        rows: u64::try_from(campaign.rows)?,
        batch_rows: campaign.batch_rows,
        warmup: campaign.warmup,
        repeat: campaign.repeat,
        peak_rss_bytes: peak_rss_bytes(),
        reads,
        writes,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let campaign = Campaign {
        rows: env_bounded("PLENORA_MYSQL_PERF_ROWS", 2_000, 1, 1_000_000)?,
        batch_rows: env_bounded(
            "PLENORA_MYSQL_PERF_BATCH_ROWS",
            256,
            1,
            plenora_db_mysql::MAX_BATCH_ROWS,
        )?,
        warmup: env_bounded("PLENORA_MYSQL_PERF_WARMUP", 1, 0, 100)?,
        repeat: env_bounded("PLENORA_MYSQL_PERF_REPEAT", 3, 1, 100)?,
    };

    let config = config()?;
    let cancellation = CancellationToken::new();
    let mut admin = admin_connection().await?;
    let campaign_result = run_campaign(campaign, config, &cancellation, &mut admin).await;
    let cleanup_result = drop_fixture(&mut admin).await;
    let disconnect_result = admin.disconnect().await;

    let result = campaign_result?;
    cleanup_result?;
    disconnect_result?;
    println!("{RESULT_PREFIX}{}", serde_json::to_string(&result)?);
    Ok(())
}
