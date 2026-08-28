//! Microbenchmark offline delle primitive applicative Core v3.
//!
//! Misura soltanto l'overhead dello strato comune: compilazione dello
//! statement, hit della cache metadata, lifecycle di una sessione e consumo
//! del protocollo streaming. Il provider sintetico non apre socket e non
//! rappresenta il checkout, il cursor o la latenza di un database reale.
//!
//! Uso: `bench_core_application [iterazioni] [ripetizioni]`.

use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    ObjectRef, Operation, ProviderKind, ReadOperation, WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, ParameterValue, PreparedWrite, Provider,
    ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase, Result, Row};
use plenora_database_engine::{Engine, NativeStatement};
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

struct OfflineProvider;

impl OfflineProvider {
    fn unused<T>(phase: ErrorPhase) -> Result<T> {
        Err(DatabaseError::unsupported(
            ProviderKind::Postgres,
            phase,
            "operazione fuori dallo scenario microbenchmark",
        ))
    }
}

impl Provider for OfflineProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn test_connection<'a>(
        &'a self,
        _secret: &'a SecretString,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async { Self::unused(ErrorPhase::Connect) })
    }

    fn probe_capabilities<'a>(
        &'a self,
        _secret: &'a SecretString,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async { Self::unused(ErrorPhase::Probe) })
    }

    fn inspect<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a Operation,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async {
            Ok(Inspection {
                operation: "database.describe_object".to_owned(),
                document: json!({
                    "columns": [{
                        "name": "id", "native_type": "int8", "nullable": false,
                        "numeric_precision": null, "numeric_scale": null,
                        "spatial_srid": null, "spatial_dimensions": null,
                        "spatial_type": null, "spatial_crs_id": null,
                        "default_expression": null, "identity_kind": null,
                        "generated_kind": null, "native_declaration": "bigint",
                        "type_kind": "b", "composite_fields": [], "enum_labels": [],
                        "domain_base_type": null, "domain_constraints": [],
                        "collation": null
                    }],
                    "schema_token": {
                        "schema_version": 1, "database_oid": 1,
                        "namespace_oid": 2, "relation_oid": 3,
                        "structural_fingerprint": "offline-benchmark"
                    },
                    "relation": {
                        "kind": "table", "is_partition": false,
                        "partition_key": null, "view_definition": null,
                        "comment": null, "row_security": false,
                        "force_row_security": false,
                        "replica_identity": "default", "persistence": "permanent",
                        "is_populated": true, "partition_bound": null,
                        "owner": "owner", "tablespace": "default",
                        "parents": [], "partitions": []
                    },
                    "constraints": [], "indexes": [], "policies": [], "privileges": []
                }),
            })
        })
    }

    fn read<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a ReadOperation,
        _parameters: &'a ParameterBag,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async { Self::unused(ErrorPhase::Read) })
    }

    fn prepare_write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a WriteOperation,
        _input_schema: SchemaRef,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async { Self::unused(ErrorPhase::Prepare) })
    }

    fn write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _prepared: PreparedWrite,
        _input: Box<dyn BatchStream>,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async { Self::unused(ErrorPhase::Write) })
    }

    fn begin_transaction<'a>(
        &'a self,
        _secret: &'a SecretString,
        _options: &'a TransactionOptions,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn TransactionScope>> {
        Box::pin(async { Ok(Box::new(OfflineTransaction) as Box<dyn TransactionScope>) })
    }
}

struct OfflineTransaction;

impl TransactionScope for OfflineTransaction {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn execute<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        Box::pin(async { Ok(1) })
    }

    fn query<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async { Ok(Box::new(OfflineRowStream::new()) as Box<dyn RowStream + Send>) })
    }

    fn savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        _request: ConditionalUpdate<'a>,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit(
        self: Box<Self>,
        _cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async { Ok(CommitOutcome::Committed) })
    }

    fn rollback(self: Box<Self>, _cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct OfflineRowStream {
    rows: Vec<Row>,
    remaining_batches: usize,
}

impl OfflineRowStream {
    fn new() -> Self {
        let columns: Arc<[String]> = vec![
            "id".to_owned(),
            "tenant_id".to_owned(),
            "name".to_owned(),
            "active".to_owned(),
        ]
        .into();
        let rows = (0..256)
            .map(|index| {
                Row::try_new(
                    Arc::clone(&columns),
                    vec![
                        ParameterValue::I64(index),
                        ParameterValue::I64(index % 16),
                        ParameterValue::String("application-row".to_owned()),
                        ParameterValue::Bool(true),
                    ],
                )
                .expect("riga benchmark valida")
            })
            .collect();
        Self {
            rows,
            remaining_batches: 4,
        }
    }
}

impl RowStream for OfflineRowStream {
    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<Vec<Row>>> {
        Box::pin(async move {
            if self.remaining_batches == 0 {
                return Ok(None);
            }
            self.remaining_batches -= 1;
            Ok(Some(self.rows.clone()))
        })
    }
}

fn peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn run_scenario(name: &str, iterations: usize, repetitions: usize, execute: impl Fn() -> usize) {
    black_box(execute());
    let mut durations = Vec::with_capacity(repetitions);
    let mut checksum = 0usize;
    for _ in 0..repetitions {
        let start = Instant::now();
        for _ in 0..iterations {
            checksum = checksum.wrapping_add(execute());
        }
        durations.push(start.elapsed().as_secs_f64());
    }
    black_box(checksum);
    durations.sort_by(f64::total_cmp);
    let median = durations[durations.len() / 2];
    #[allow(clippy::cast_precision_loss)]
    let iterations_f64 = iterations as f64;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "bench": "core_application",
            "scenario": name,
            "iterations": iterations,
            "repetitions": repetitions,
            "median_seconds": median,
            "nanoseconds_per_operation": median / iterations_f64 * 1e9,
            "operations_per_second": iterations_f64 / median,
            "checksum": checksum,
            "peak_rss_kib": peak_rss_kib(),
        }))
        .expect("record benchmark serializzabile")
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let iterations = args
        .next()
        .as_deref()
        .unwrap_or("2000")
        .parse::<usize>()
        .expect("iterazioni");
    let repetitions = args
        .next()
        .as_deref()
        .unwrap_or("9")
        .parse::<usize>()
        .expect("ripetizioni");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime benchmark");
    let engine = Engine::new(Arc::new(OfflineProvider), SecretString::new("offline"));
    let source = ObjectRef {
        catalog: None,
        schema: Some("application".to_owned()),
        object: "events".to_owned(),
    };
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget benchmark");
    runtime
        .block_on(engine.reflect_table(&source, false, &cancellation))
        .expect("warmup cache metadata");

    run_scenario("compile_native_statement", iterations, repetitions, || {
        NativeStatement::new("SELECT id, name FROM events WHERE tenant_id = $1", 1)
            .expect("statement benchmark")
            .fingerprint()[0] as usize
    });
    run_scenario("metadata_cache_hit", iterations, repetitions, || {
        runtime
            .block_on(engine.reflect_table(&source, false, &cancellation))
            .expect("hit cache metadata")
            .tables()
            .len()
    });
    run_scenario("session_boundary_checkout", iterations, repetitions, || {
        let session = engine.session().expect("checkout sessione applicativa");
        let active = usize::try_from(engine.statistics().active_sessions)
            .expect("contatore sessioni rappresentabile");
        drop(session);
        active
    });
    run_scenario(
        "transaction_checkout_rollback",
        iterations,
        repetitions,
        || {
            runtime.block_on(async {
                let mut session = engine.session().expect("sessione benchmark");
                let transaction = session
                    .begin_transaction(&TransactionOptions::default(), &budget, &cancellation)
                    .await
                    .expect("transazione sintetica");
                transaction.rollback().await.expect("rollback sintetico");
                1
            })
        },
    );

    let streaming_iterations = iterations.div_ceil(20);
    run_scenario(
        "stream_four_batches_256_rows",
        streaming_iterations,
        repetitions,
        || {
            runtime.block_on(async {
                let mut session = engine.session().expect("sessione streaming");
                let mut transaction = session
                    .begin_transaction(&TransactionOptions::default(), &budget, &cancellation)
                    .await
                    .expect("transazione streaming");
                let statement = Statement::new("SELECT id, tenant_id, name, active FROM events");
                let mut rows = 0;
                {
                    let mut stream = transaction
                        .query_stream(&statement, 256)
                        .await
                        .expect("stream sintetico");
                    while let Some(result) = stream.next_result().await.expect("batch sintetico") {
                        rows += result.len();
                    }
                }
                transaction.rollback().await.expect("rollback streaming");
                rows
            })
        },
    );
}
