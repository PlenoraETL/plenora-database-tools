use super::*;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{Operation, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, Inspection, ParameterBag, PreparedWrite, ProviderFuture,
};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::{CollectRecorder, ErrorPhase};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
struct ProviderState {
    health_checks: u64,
    probes: u64,
    observed_secrets: Vec<String>,
    inspections: u64,
    inspected_secrets: Vec<String>,
}

#[derive(Default)]
struct TestProvider {
    state: Mutex<ProviderState>,
    executions: Arc<AtomicU64>,
    block_next_probe: AtomicBool,
    probe_started: Notify,
    probe_release: Notify,
}

impl TestProvider {
    fn observe(&self, secret: &SecretString, probe: bool) {
        let mut state = mutex(&self.state);
        state.observed_secrets.push(secret.expose().to_owned());
        if probe {
            state.probes += 1;
        } else {
            state.health_checks += 1;
        }
    }

    fn observe_inspection(&self, secret: &SecretString) {
        let mut state = mutex(&self.state);
        state.inspections += 1;
        state.inspected_secrets.push(secret.expose().to_owned());
    }
}

fn reflected_postgres_object() -> Inspection {
    Inspection {
        operation: "database.describe_object".to_owned(),
        document: serde_json::json!({
            "columns": [{
                "name": "id", "native_type": "int8", "nullable": false,
                "numeric_precision": null, "numeric_scale": null,
                "spatial_srid": null, "spatial_dimensions": null,
                "spatial_type": null, "spatial_crs_id": null,
                "default_expression": null, "identity_kind": null,
                "generated_kind": null, "native_declaration": "bigint",
                "type_kind": "b", "composite_fields": [], "enum_labels": [],
                "domain_base_type": null, "domain_constraints": [], "collation": null
            }],
            "schema_token": {
                "schema_version": 1, "database_oid": 1, "namespace_oid": 2,
                "relation_oid": 3, "structural_fingerprint": "test-token"
            },
            "relation": {
                "kind": "table", "is_partition": false, "partition_key": null,
                "view_definition": null, "comment": null, "row_security": false,
                "force_row_security": false, "replica_identity": "default",
                "persistence": "permanent", "is_populated": true,
                "partition_bound": null, "owner": "owner", "tablespace": "default",
                "parents": [], "partitions": []
            },
            "constraints": [], "indexes": [], "policies": [], "privileges": []
        }),
    }
}

fn interrupted(cancellation: &CancellationToken, phase: ErrorPhase) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(DatabaseError::interrupted(
            cancellation,
            Some(ProviderKind::Postgres),
            phase,
            "operazione di test interrotta",
        ))
    } else {
        Ok(())
    }
}

impl Provider for TestProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        self.observe(secret, false);
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Connect)?;
            Ok(ConnectionInfo {
                provider: ProviderKind::Postgres,
                server_version: "test".to_owned(),
                connection_identity: None,
            })
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        self.observe(secret, true);
        let block = self.block_next_probe.swap(false, Ordering::AcqRel);
        Box::pin(async move {
            if block {
                self.probe_started.notify_one();
                self.probe_release.notified().await;
            }
            interrupted(cancellation, ErrorPhase::Probe)?;
            serde_json::from_slice(include_bytes!(
                "../../../contracts/v2/examples/capabilities-postgres.json"
            ))
            .map_err(|_| DatabaseError::invalid_plan("fixture capability non valida"))
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        self.observe_inspection(secret);
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Probe)?;
            if !matches!(operation, Operation::DatabaseDescribeObject { .. }) {
                return Err(DatabaseError::invalid_plan(
                    "operazione reflection inattesa nel test",
                ));
            }
            Ok(reflected_postgres_object())
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
        Box::pin(async {
            Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                ErrorPhase::Read,
                "read non usata dal test engine",
            ))
        })
    }

    fn prepare_write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a WriteOperation,
        _input_schema: SchemaRef,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async {
            Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                ErrorPhase::Prepare,
                "write non usata dal test engine",
            ))
        })
    }

    fn write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _prepared: PreparedWrite,
        _input: Box<dyn BatchStream>,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async {
            Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                ErrorPhase::Write,
                "write non usata dal test engine",
            ))
        })
    }

    fn begin_transaction<'a>(
        &'a self,
        _secret: &'a SecretString,
        _options: &'a TransactionOptions,
        _budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn TransactionScope>> {
        let executions = Arc::clone(&self.executions);
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Connect)?;
            Ok(Box::new(TestTransaction { executions }) as Box<dyn TransactionScope>)
        })
    }
}

struct TestTransaction {
    executions: Arc<AtomicU64>,
}

impl TransactionScope for TestTransaction {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn execute<'a>(
        &'a mut self,
        _statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Write)?;
            self.executions.fetch_add(1, Ordering::Relaxed);
            Ok(1)
        })
    }

    fn query<'a>(
        &'a mut self,
        _statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Read)?;
            Ok(Vec::new())
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Read)?;
            Ok(Box::new(EmptyRowStream) as Box<dyn RowStream + Send>)
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { interrupted(cancellation, ErrorPhase::Prepare) })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { interrupted(cancellation, ErrorPhase::Rollback) })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { interrupted(cancellation, ErrorPhase::Finalize) })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        _request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { interrupted(cancellation, ErrorPhase::Write) })
    }

    fn commit(
        self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Commit)?;
            Ok(CommitOutcome::Committed)
        })
    }

    fn rollback(self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async move { interrupted(cancellation, ErrorPhase::Rollback) })
    }
}

struct EmptyRowStream;

impl RowStream for EmptyRowStream {
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<Vec<Row>>> {
        Box::pin(async move {
            interrupted(cancellation, ErrorPhase::Read)?;
            Ok(None)
        })
    }
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget di test")
}

#[tokio::test]
async fn capabilities_are_cached_and_secret_rotation_invalidates_them() {
    let provider = Arc::new(TestProvider::default());
    let recorder = Arc::new(CollectRecorder::new());
    let engine = Engine::with_recorder(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("first"),
        Arc::clone(&recorder) as SharedRecorder,
    );
    let cancellation = CancellationToken::new();

    engine
        .capabilities(false, &cancellation)
        .await
        .expect("prima misura");
    engine
        .capabilities(false, &cancellation)
        .await
        .expect("cache");
    engine
        .rotate_secret(SecretString::new("second"))
        .expect("rotazione");
    engine
        .capabilities(false, &cancellation)
        .await
        .expect("misura dopo rotazione");
    engine
        .health_check(&cancellation)
        .await
        .expect("health check");

    let state = mutex(&provider.state);
    assert_eq!(state.probes, 2);
    assert_eq!(state.health_checks, 1);
    assert_eq!(state.observed_secrets, ["first", "second", "second"]);
    drop(state);
    assert_eq!(
        recorder
            .snapshot()
            .iter()
            .filter(|event| event.name == MetricName::DbOperationDuration)
            .count(),
        3
    );
    assert!(!format!("{engine:?}").contains("second"));
}

#[tokio::test]
async fn metadata_cache_honours_ttl_refresh_invalidation_and_secret_rotation() {
    let provider = Arc::new(TestProvider::default());
    let engine = Engine::with_options(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("first"),
        noop_recorder(),
        EngineOptions::new(Duration::from_millis(20)),
    );
    let source = ObjectRef {
        catalog: None,
        schema: Some("app".to_owned()),
        object: "items".to_owned(),
    };
    let cancellation = CancellationToken::new();

    let first = engine
        .reflect_table(&source, false, &cancellation)
        .await
        .expect("prima reflection");
    let cached = engine
        .reflect_table(&source, false, &cancellation)
        .await
        .expect("reflection in cache");
    assert!(Arc::ptr_eq(&first, &cached));
    assert_eq!(engine.metadata_cache_entries(), 1);

    tokio::time::sleep(Duration::from_millis(25)).await;
    engine
        .reflect_table(&source, false, &cancellation)
        .await
        .expect("reflection dopo TTL");
    engine
        .reflect_table(&source, true, &cancellation)
        .await
        .expect("refresh forzato");
    assert_eq!(engine.invalidate_metadata(Some(&source)), 1);
    assert_eq!(engine.invalidate_metadata(Some(&source)), 0);
    engine
        .reflect_table(&source, false, &cancellation)
        .await
        .expect("reflection dopo invalidazione");
    engine
        .rotate_secret(SecretString::new("second"))
        .expect("rotazione secret");
    assert_eq!(engine.metadata_cache_entries(), 0);
    engine
        .reflect_table(&source, false, &cancellation)
        .await
        .expect("reflection dopo rotazione");

    let state = mutex(&provider.state);
    assert_eq!(state.inspections, 5);
    assert_eq!(
        state.inspected_secrets,
        ["first", "first", "first", "first", "second"]
    );
}

#[tokio::test]
async fn a_probe_crossing_secret_rotation_is_repeated_before_caching() {
    let provider = Arc::new(TestProvider::default());
    provider.block_next_probe.store(true, Ordering::Release);
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("first"),
    );
    let concurrent = engine.clone();
    let probe = tokio::spawn(async move {
        concurrent
            .capabilities(true, &CancellationToken::new())
            .await
    });
    provider.probe_started.notified().await;
    engine
        .rotate_secret(SecretString::new("second"))
        .expect("rotazione concorrente");
    provider.probe_release.notify_one();
    probe
        .await
        .expect("task probe")
        .expect("probe ripetuto sul secret corrente");

    let state = mutex(&provider.state);
    assert_eq!(state.probes, 2);
    assert_eq!(state.observed_secrets, ["first", "second"]);
}

#[tokio::test]
async fn session_owns_an_exclusive_transaction_and_updates_lifecycle() {
    let provider = Arc::new(TestProvider::default());
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("secret"),
    );
    let mut session = engine.session().expect("sessione");
    assert_eq!(engine.statistics().active_sessions, 1);

    let cancellation = CancellationToken::new();
    let mut transaction = session
        .begin_transaction(&TransactionOptions::default(), &budget(), &cancellation)
        .await
        .expect("transazione");
    assert_eq!(
        transaction.execute(&Statement::new("SELECT 1")).await,
        Ok(1)
    );
    assert_eq!(transaction.commit().await, Ok(CommitOutcome::Committed));
    session.close();
    assert_eq!(engine.statistics().active_sessions, 0);
    assert_eq!(provider.executions.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dispose_closes_existing_and_future_work_without_exposing_the_secret() {
    let provider = Arc::new(TestProvider::default());
    let engine = Engine::new(provider as Arc<dyn Provider>, SecretString::new("private"));
    let mut session = engine.session().expect("sessione prima del dispose");
    engine.dispose();

    let cancellation = CancellationToken::new();
    let result = session
        .begin_transaction(&TransactionOptions::default(), &budget(), &cancellation)
        .await;
    let Err(error) = result else {
        panic!("sessione cancellata dal dispose");
    };
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
    assert!(!error.message.contains("private"));
    assert!(engine.session().is_err());
    assert!(engine.statistics().disposed);
}

#[tokio::test]
async fn transaction_cancellation_reaches_every_statement() {
    let engine = Engine::new(
        Arc::new(TestProvider::default()),
        SecretString::new("secret"),
    );
    let mut session = engine.session().expect("sessione");
    let cancellation = CancellationToken::new();
    let mut transaction = session
        .begin_transaction(&TransactionOptions::default(), &budget(), &cancellation)
        .await
        .expect("transazione");
    cancellation.cancel();
    let error = transaction
        .execute(&Statement::new("SELECT 1"))
        .await
        .expect_err("cancellazione collegata");
    assert_eq!(error.category, ErrorCategory::Cancelled);
}

#[tokio::test]
async fn transaction_forwards_query_stream_savepoints_and_rollback() {
    let engine = Engine::new(
        Arc::new(TestProvider::default()),
        SecretString::new("secret"),
    );
    let mut session = engine.session().expect("sessione");
    let mut transaction = session
        .begin_transaction(
            &TransactionOptions::default(),
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .expect("transazione");
    let statement = Statement::new("SELECT 1");
    assert_eq!(transaction.query(&statement).await, Ok(Vec::new()));
    {
        let mut stream = transaction
            .query_stream(&statement, 8)
            .await
            .expect("stream");
        assert_eq!(stream.next_batch().await, Ok(None));
    }
    transaction.savepoint("nested").await.expect("savepoint");
    transaction
        .rollback_to_savepoint("nested")
        .await
        .expect("rollback savepoint");
    transaction
        .release_savepoint("nested")
        .await
        .expect("release savepoint");
    transaction.rollback().await.expect("rollback");
}
