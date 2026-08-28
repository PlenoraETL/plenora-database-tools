use crate::{Db2Config, Db2ObjectDescription, Db2Provider, Db2TlsMode};
use plenora_database_core::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::{WriteOutcome, WriteStatus};
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, OrderBy, ProviderKind, ReadOperation, SortDirection,
    TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::portable::{
    compile_portable, eq, select, Expression, InsertStatement, PortableStatement, TableRef,
    UpsertStatement,
};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{
    CommitOutcome, IsolationLevel, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase};
use plenora_database_engine::{Engine, Observation};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn live_provider() -> Db2Provider {
    let port = environment("PLENORA_DB2_PORT", "50000")
        .parse()
        .expect("porta Db2 live");
    Db2Provider::new(
        Db2Config::new(
            environment("PLENORA_DB2_HOST", "db2"),
            environment("PLENORA_DB2_DATABASE", "plenora"),
            environment("PLENORA_DB2_USER", "db2inst1"),
        )
        .with_port(port)
        // La fixture Community locale non espone TLS. Il test lo dichiara:
        // non degrada il default di produzione, che resta Verify.
        .with_tls_mode(Db2TlsMode::Disable),
    )
    .expect("provider Db2 live")
}

async fn assert_live_catalog(
    provider: &Db2Provider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) {
    let catalogs = provider
        .inspect(secret, &Operation::DatabaseListCatalogs, cancellation)
        .await
        .expect("cataloghi Db2 live");
    assert_eq!(catalogs.document["catalogs"][0], "PLENORA");

    let schemas = provider
        .inspect(
            secret,
            &Operation::DatabaseListSchemas { source: None },
            cancellation,
        )
        .await
        .expect("schemi Db2 live");
    assert!(schemas.document["schemas"]
        .as_array()
        .expect("elenco schemi Db2")
        .iter()
        .any(|schema| schema == "PLENORA_TEST"));

    let source = ObjectRef {
        catalog: Some("PLENORA".to_owned()),
        schema: Some("PLENORA_TEST".to_owned()),
        object: "CATALOG_PROBE".to_owned(),
    };
    let objects = provider
        .inspect(
            secret,
            &Operation::DatabaseListObjects {
                source: Some(source.clone()),
            },
            cancellation,
        )
        .await
        .expect("oggetti Db2 live");
    let objects = objects.document["objects"]
        .as_array()
        .expect("elenco oggetti Db2");
    assert!(objects
        .iter()
        .any(|object| object["name"] == "CATALOG_PROBE"));
    assert!(objects
        .iter()
        .any(|object| object["name"] == "CATALOG_PROBE_VIEW"));

    let first = provider
        .inspect(
            secret,
            &Operation::DatabaseDescribeObject {
                source: source.clone(),
            },
            cancellation,
        )
        .await
        .expect("descrizione Db2 live");
    let engine = Engine::new(Arc::new(live_provider()), secret.clone());
    let typed = engine
        .reflect_table(&source, false, cancellation)
        .await
        .expect("typed Db2 reflection");
    let typed_table = typed.one_table().expect("one reflected table");
    assert_eq!(typed_table.name(), "CATALOG_PROBE");
    assert_eq!(typed_table.foreign_keys(), Observation::NotMeasured);
    let description: Db2ObjectDescription =
        serde_json::from_value(first.document).expect("documento catalogo Db2");
    assert_eq!(description.columns.len(), 5);
    assert_eq!(description.columns[0].name, "ID");
    assert!(!description.columns[0].nullable);
    assert!(description.indexes.iter().any(|index| index.primary));
    let unique = description
        .indexes
        .iter()
        .find(|index| index.name == "UQ_CATALOG_PROBE")
        .expect("indice unique composto Db2");
    assert_eq!(unique.columns, ["CODE", "REV"]);
    assert_eq!(unique.descending, [false, true]);
    assert!(description.schema_token.starts_with("sha256:"));

    let second = provider
        .inspect(
            secret,
            &Operation::DatabaseDescribeObject { source },
            cancellation,
        )
        .await
        .expect("descrizione Db2 live ripetuta");
    let repeated: Db2ObjectDescription =
        serde_json::from_value(second.document).expect("secondo documento catalogo Db2");
    assert_eq!(description.schema_token, repeated.schema_token);
}

fn scalar_read_operation() -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: Some("PLENORA".to_owned()),
            schema: Some("PLENORA_TEST".to_owned()),
            object: "READ_PROBE".to_owned(),
        },
        projection: [
            "ID",
            "FLAG",
            "COUNT_BIG",
            "AMOUNT",
            "RATIO",
            "LABEL",
            "CREATED_ON",
            "CREATED_AT",
        ]
        .map(str::to_owned)
        .to_vec(),
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    }
}

fn assert_scalar_batch(batch: &RecordBatch) {
    assert_eq!(batch.num_rows(), 2);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("ID Int32");
    let flags = batch
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("FLAG Boolean");
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT_BIG Int64");
    let amounts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("AMOUNT Decimal128");
    let ratios = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("RATIO Float64");
    let labels = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("LABEL Utf8");
    let dates = batch
        .column(6)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("CREATED_ON Date32");
    let timestamps = batch
        .column(7)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("CREATED_AT Timestamp");

    assert_eq!(ids.values(), &[1, 2]);
    assert!(flags.value(0));
    assert!(flags.is_null(1));
    assert_eq!(counts.value(0), 9_223_372_036_854_770_000);
    assert!(counts.is_null(1));
    assert_eq!(amounts.value(0), 123_456_789_012_345_678);
    assert!(amounts.is_null(1));
    assert!((ratios.value(0) - 3.5).abs() <= f64::EPSILON);
    assert!(ratios.is_null(1));
    assert_eq!(labels.value(0), "alpha");
    assert!(labels.is_null(1));
    assert!(!dates.is_null(0));
    assert!(dates.is_null(1));
    assert!(!timestamps.is_null(0));
    assert!(timestamps.is_null(1));
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_reference_probe_catalog_and_capabilities() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();

    let connection = provider
        .test_connection(&secret, &cancellation)
        .await
        .expect("connessione Db2 live");
    assert_eq!(connection.provider, ProviderKind::Db2);
    assert_eq!(connection.connection_identity.as_deref(), Some("PLENORA"));
    assert!(
        connection.server_version.starts_with("DB2 v12.1."),
        "versione Db2 osservata: {:?}",
        connection.server_version
    );

    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("capability Db2 live");
    assert_eq!(capabilities.provider_version, connection.server_version);
    assert!(capabilities.reads.streaming);
    assert!(capabilities.reads.pagination);
    assert!(capabilities.reads.projection);
    assert!(capabilities.reads.filter);
    assert!(capabilities.reads.ordering);
    assert!(!capabilities.reads.server_cursor);
    assert!(!capabilities.reads.resumable);
    assert!(capabilities.writes.create);
    assert!(capabilities.writes.append);
    assert!(capabilities.writes.update);
    assert!(capabilities.writes.upsert);
    assert!(capabilities.writes.replace);
    assert!(capabilities.writes.delete_by_keys);
    assert!(capabilities.writes.rollback_on_failure);
    assert!(!capabilities.writes.truncate_insert);
    assert!(!capabilities.writes.bulk);
    assert!(capabilities.transactions.single_transaction);
    assert!(capabilities.transactions.savepoints);
    assert!(capabilities.transactions.transactional_ddl);
    assert!(capabilities.spatial.geometry);
    assert!(capabilities.spatial.read_wkb);
    assert!(capabilities.spatial.write_wkb);
    assert!(capabilities.spatial.requires_declared_crs);
    assert!(!capabilities.spatial.spatial_index);
    assert_live_catalog(&provider, &secret, &cancellation).await;
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_errors_classify_security_and_missing_objects_without_payloads() {
    let provider = live_provider();
    let wrong_password = "wrong-password-must-not-leak";
    let cancellation = CancellationToken::new();
    let authentication = provider
        .test_connection(&SecretString::new(wrong_password.to_owned()), &cancellation)
        .await
        .expect_err("credenziale Db2 errata");
    assert_eq!(authentication.category, ErrorCategory::Authentication);
    assert_eq!(authentication.phase, ErrorPhase::Connect);
    assert!(!authentication.message.contains(wrong_password));

    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget errori Db2 live");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione errori Db2 live");
    let missing_name = "PLENORA_TEST.MISSING_PRIVATE_OBJECT";
    let missing = transaction
        .query(
            &Statement::new(format!("SELECT ID FROM {missing_name}")),
            &cancellation,
        )
        .await
        .expect_err("oggetto Db2 assente");
    assert_eq!(
        missing.category,
        ErrorCategory::NotFound,
        "{}",
        missing.message
    );
    assert_eq!(missing.phase, ErrorPhase::Read);
    assert!(!missing.message.contains(missing_name));
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback transazione errori Db2");
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_streaming_read_preserves_supported_scalar_values_and_nulls() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    let operation = scalar_read_operation();

    let mut stream = provider
        .read(
            &secret,
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream Db2 live");
    assert_eq!(stream.schema().fields().len(), 8);
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch Db2 live")
        .expect("prima pagina Db2 live");
    assert_scalar_batch(&batch);

    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream Db2 live")
        .is_none());
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_filter_and_deterministic_pagination_are_bound_and_exact() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: Some("PLENORA".to_owned()),
            schema: Some("PLENORA_TEST".to_owned()),
            object: "READ_PROBE".to_owned(),
        },
        projection: ["ID", "LABEL"].map(str::to_owned).to_vec(),
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(1),
        row_offset: Some(1),
        filter: Some(FilterExpression::Gte {
            field: "ID".to_owned(),
            parameter: "minimum_id".to_owned(),
        }),
        declared_crs: Vec::new(),
    };
    let parameters = ParameterBag::new(BTreeMap::from([(
        "minimum_id".to_owned(),
        ParameterValue::I32(1),
    )]));

    let mut stream = provider
        .read(&secret, &operation, &parameters, &budget, &cancellation)
        .await
        .expect("stream Db2 filtrato e paginato");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch Db2 filtrato e paginato")
        .expect("riga Db2 filtrata e paginata");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("ID Int32");
    let labels = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("LABEL Utf8");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(ids.value(0), 2);
    assert!(labels.is_null(0));
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream Db2 filtrato")
        .is_none());
}

async fn exercise_commit_and_savepoint(
    mut transaction: Box<dyn TransactionScope>,
    cancellation: &CancellationToken,
) {
    let inserted = transaction
        .execute(
            &Statement::new(
                "INSERT INTO PLENORA_TEST.TX_PROBE (ID, VALUE, VERSION) VALUES (?, ?, ?)",
            )
            .with_params(vec![
                ParameterValue::I32(10),
                ParameterValue::String("committed".to_owned()),
                ParameterValue::I32(1),
            ]),
            cancellation,
        )
        .await
        .expect("insert Db2 in transazione");
    assert_eq!(inserted, 1);

    transaction
        .savepoint("before_change", cancellation)
        .await
        .expect("savepoint Db2");
    transaction
        .execute(
            &Statement::new("UPDATE PLENORA_TEST.TX_PROBE SET VALUE = ? WHERE ID = ?").with_params(
                vec![
                    ParameterValue::String("rolled-back".to_owned()),
                    ParameterValue::I32(10),
                ],
            ),
            cancellation,
        )
        .await
        .expect("update dopo savepoint Db2");
    transaction
        .rollback_to_savepoint("before_change", cancellation)
        .await
        .expect("rollback al savepoint Db2");
    transaction
        .release_savepoint("before_change", cancellation)
        .await
        .expect("release savepoint Db2");

    let rows = transaction
        .query(
            &Statement::new("SELECT ID, VALUE, VERSION FROM PLENORA_TEST.TX_PROBE WHERE ID = ?")
                .with_params(vec![ParameterValue::I32(10)]),
            cancellation,
        )
        .await
        .expect("query tipizzata Db2");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ID"), Some(&ParameterValue::I32(10)));
    assert_eq!(
        rows[0].get("VALUE"),
        Some(&ParameterValue::String("committed".to_owned()))
    );
    assert_eq!(
        transaction.commit(cancellation).await.expect("commit Db2"),
        CommitOutcome::Committed
    );
}

async fn exercise_rollback(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("seconda transazione Db2");
    transaction
        .execute(
            &Statement::new(
                "INSERT INTO PLENORA_TEST.TX_PROBE (ID, VALUE, VERSION) VALUES (?, ?, ?)",
            )
            .with_params(vec![
                ParameterValue::I32(11),
                ParameterValue::String("must-disappear".to_owned()),
                ParameterValue::I32(1),
            ]),
            cancellation,
        )
        .await
        .expect("insert da annullare Db2");
    transaction
        .rollback(cancellation)
        .await
        .expect("rollback Db2");
}

async fn verify_transaction_outcome(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione verifica Db2");
    let rows = transaction
        .query(
            &Statement::new("SELECT ID FROM PLENORA_TEST.TX_PROBE WHERE ID IN (?, ?) ORDER BY ID")
                .with_params(vec![ParameterValue::I32(10), ParameterValue::I32(11)]),
            cancellation,
        )
        .await
        .expect("verifica commit e rollback Db2");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ID"), Some(&ParameterValue::I32(10)));
    transaction
        .rollback(cancellation)
        .await
        .expect("chiusura verifica Db2");
}

async fn reset_transaction_probe(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione reset probe Db2");
    transaction
        .execute(
            &Statement::new("DELETE FROM PLENORA_TEST.TX_PROBE"),
            cancellation,
        )
        .await
        .expect("reset probe transazionale Db2");
    transaction
        .commit(cancellation)
        .await
        .expect("commit reset probe Db2");
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_transaction_commit_rollback_savepoint_and_typed_query() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    let options = TransactionOptions {
        isolation: Some(IsolationLevel::ReadCommitted),
        ..TransactionOptions::default()
    };
    reset_transaction_probe(&provider, &secret, &budget, &cancellation).await;
    let transaction = provider
        .begin_transaction(&secret, &options, &budget, &cancellation)
        .await
        .expect("transazione Db2 live");

    exercise_commit_and_savepoint(transaction, &cancellation).await;
    exercise_rollback(&provider, &secret, &budget, &cancellation).await;
    verify_transaction_outcome(&provider, &secret, &budget, &cancellation).await;
}

async fn concurrent_insert(
    provider: Arc<Db2Provider>,
    secret: SecretString,
    budget: ResourceBudget,
    barrier: Arc<tokio::sync::Barrier>,
    id: i32,
) {
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("connessione writer Db2 concorrente");
    barrier.wait().await;
    let affected = transaction
        .execute(
            &Statement::new(
                "INSERT INTO PLENORA_TEST.TX_PROBE (ID, VALUE, VERSION) VALUES (?, ?, ?)",
            )
            .with_params(vec![
                ParameterValue::I32(id),
                ParameterValue::String(format!("worker-{id}")),
                ParameterValue::I32(1),
            ]),
            &cancellation,
        )
        .await
        .expect("insert writer Db2 concorrente");
    assert_eq!(affected, 1);
    assert_eq!(
        transaction
            .commit(&cancellation)
            .await
            .expect("commit writer Db2 concorrente"),
        CommitOutcome::Committed
    );
}

async fn concurrent_read(
    provider: Arc<Db2Provider>,
    secret: SecretString,
    budget: ResourceBudget,
    barrier: Arc<tokio::sync::Barrier>,
    id: i32,
) {
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("connessione reader Db2 concorrente");
    barrier.wait().await;
    let rows = transaction
        .query(
            &Statement::new("SELECT VALUE FROM PLENORA_TEST.TX_PROBE WHERE ID = ?")
                .with_params(vec![ParameterValue::I32(id)]),
            &cancellation,
        )
        .await
        .expect("query reader Db2 concorrente");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("VALUE"),
        Some(&ParameterValue::String(format!("worker-{id}")))
    );
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback reader Db2 concorrente");
}

async fn join_workers(workers: Vec<tokio::task::JoinHandle<()>>) {
    for worker in workers {
        worker.await.expect("worker Db2 concorrente terminato");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_concurrent_users_keep_connections_and_rows_isolated() {
    const USERS: i32 = 8;
    let provider = Arc::new(live_provider());
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let limits = ResourceLimits {
        duration_ms: 120_000,
        ..ResourceLimits::default()
    };
    let budget = ResourceBudget::new(limits).expect("budget concorrenza Db2 live");
    reset_transaction_probe(&provider, &secret, &budget, &cancellation).await;

    let writers_barrier = Arc::new(tokio::sync::Barrier::new(USERS as usize));
    let mut writers = Vec::new();
    for id in 100..100 + USERS {
        writers.push(tokio::spawn(concurrent_insert(
            Arc::clone(&provider),
            secret.clone(),
            budget.clone(),
            Arc::clone(&writers_barrier),
            id,
        )));
    }
    join_workers(writers).await;

    let readers_barrier = Arc::new(tokio::sync::Barrier::new(USERS as usize));
    let mut readers = Vec::new();
    for id in 100..100 + USERS {
        readers.push(tokio::spawn(concurrent_read(
            Arc::clone(&provider),
            secret.clone(),
            budget.clone(),
            Arc::clone(&readers_barrier),
            id,
        )));
    }
    join_workers(readers).await;
}

fn portable_probe_insert() -> PortableStatement {
    PortableStatement::Insert(InsertStatement {
        table: TableRef::qualified("PLENORA_TEST", "TX_PROBE"),
        columns: vec!["ID".into(), "VALUE".into(), "VERSION".into()],
        values: vec![vec![
            Expression::literal(ParameterValue::I32(20)),
            Expression::literal(ParameterValue::String("portable-insert".into())),
            Expression::literal(ParameterValue::I32(1)),
        ]],
        returning: Vec::new(),
    })
}

fn portable_probe_upsert() -> PortableStatement {
    PortableStatement::Upsert(UpsertStatement {
        table: TableRef::qualified("PLENORA_TEST", "TX_PROBE"),
        columns: vec!["ID".into(), "VALUE".into(), "VERSION".into()],
        values: vec![vec![
            Expression::literal(ParameterValue::I32(20)),
            Expression::literal(ParameterValue::String("source-value".into())),
            Expression::literal(ParameterValue::I32(1)),
        ]],
        conflict_target: vec!["ID".into()],
        update_on_conflict: vec![
            (
                "VALUE".into(),
                Expression::literal(ParameterValue::String("portable-merge".into())),
            ),
            (
                "VERSION".into(),
                Expression::literal(ParameterValue::I32(2)),
            ),
        ],
        returning: Vec::new(),
    })
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_portable_sql_executes_insert_merge_select_and_rollback() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    reset_transaction_probe(&provider, &secret, &budget, &cancellation).await;
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione portable Db2");

    let insert = portable_probe_insert();
    let insert = compile_portable(ProviderKind::Db2, &insert).expect("compila INSERT Db2");
    assert_eq!(
        transaction
            .execute(&insert, &cancellation)
            .await
            .expect("esegue INSERT portable Db2"),
        1
    );

    let upsert = portable_probe_upsert();
    let upsert = compile_portable(ProviderKind::Db2, &upsert).expect("compila MERGE Db2");
    assert_eq!(
        transaction
            .execute(&upsert, &cancellation)
            .await
            .expect("esegue MERGE portable Db2"),
        1
    );

    let select_statement = select("TX_PROBE", vec!["ID", "VALUE", "VERSION"])
        .schema("PLENORA_TEST")
        .where_(eq("ID", ParameterValue::I32(20)))
        .limit(1)
        .into_statement();
    let compiled_select =
        compile_portable(ProviderKind::Db2, &select_statement).expect("compila SELECT Db2");
    let rows = transaction
        .query(&compiled_select, &cancellation)
        .await
        .expect("esegue SELECT portable Db2");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("VALUE"),
        Some(&ParameterValue::String("portable-merge".into()))
    );
    assert_eq!(rows[0].get("VERSION"), Some(&ParameterValue::I32(2)));
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback SQL portable Db2");

    let mut verification = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione verifica rollback portable Db2");
    let select_statement = select("TX_PROBE", vec!["ID"])
        .schema("PLENORA_TEST")
        .where_(eq("ID", ParameterValue::I32(20)))
        .into_statement();
    let select =
        compile_portable(ProviderKind::Db2, &select_statement).expect("compila verifica Db2");
    let rows = verification
        .query(&select, &cancellation)
        .await
        .expect("verifica rollback portable Db2");
    assert!(rows.is_empty());
    verification
        .rollback(&cancellation)
        .await
        .expect("chiusura verifica portable Db2");
}

struct VecBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    rows: u64,
}

impl VecBatchStream {
    fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = batches.first().expect("almeno un batch Db2 live").schema();
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
        Self {
            schema,
            batches: batches.into(),
            rows,
        }
    }
}

impl BatchStream for VecBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(plenora_database_core::DatabaseError::cancelled(
                    Some(ProviderKind::Db2),
                    plenora_database_core::ErrorPhase::Write,
                    "stream fixture Db2 cancellato",
                ));
            }
            Ok(self.batches.pop_front())
        })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.rows)
    }
}

fn write_operation(mode: WriteMode, schema: &SchemaRef) -> WriteOperation {
    let keys = match mode {
        WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys => vec!["ID".to_owned()],
        _ => Vec::new(),
    };
    let update_columns = matches!(mode, WriteMode::Update | WriteMode::Upsert)
        .then(|| {
            schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .filter(|name| name != "ID")
                .collect()
        })
        .unwrap_or_default();
    WriteOperation {
        target: ObjectRef {
            catalog: Some("PLENORA".to_owned()),
            schema: Some("PLENORA_TEST".to_owned()),
            object: "WRITE_PROBE".to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys,
        update_columns,
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn write_batch(ids: Vec<i32>, values: Vec<Option<&str>>, versions: Vec<i32>) -> RecordBatch {
    let schema = contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("VALUE", DataType::Utf8, true),
        Field::new("VERSION", DataType::Int32, false),
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(values)),
            Arc::new(Int32Array::from(versions)),
        ],
    )
    .expect("batch write Db2 live")
}

async fn run_write(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    operation: WriteOperation,
    batch: RecordBatch,
    cancellation: &CancellationToken,
) -> plenora_database_core::Result<WriteOutcome> {
    let prepared = provider
        .prepare_write(secret, &operation, batch.schema(), budget, cancellation)
        .await?;
    provider
        .write(
            secret,
            prepared,
            Box::new(VecBatchStream::new(vec![batch])),
            budget,
            cancellation,
        )
        .await
}

async fn reset_write_probe(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione reset write Db2");
    transaction
        .execute(
            &Statement::new("DELETE FROM PLENORA_TEST.WRITE_PROBE"),
            cancellation,
        )
        .await
        .expect("reset write probe Db2");
    transaction
        .commit(cancellation)
        .await
        .expect("commit reset write Db2");
}

async fn exercise_append_and_atomic_rollback(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let append = write_batch(vec![1, 2], vec![Some("one"), None], vec![1, 1]);
    let outcome = run_write(
        provider,
        secret,
        budget,
        write_operation(WriteMode::Append, &append.schema()),
        append,
        cancellation,
    )
    .await
    .expect("append Arrow Db2");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.inserted, Some(2));

    let duplicate = write_batch(
        vec![1, 3],
        vec![Some("duplicate"), Some("three")],
        vec![2, 1],
    );
    let error = run_write(
        provider,
        secret,
        budget,
        write_operation(WriteMode::Append, &duplicate.schema()),
        duplicate,
        cancellation,
    )
    .await
    .expect_err("append Db2 atomica su duplicato");
    assert_eq!(error.category, ErrorCategory::Conflict);
    assert!(!error.message.contains("duplicate"));
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::RolledBack
    );
}

async fn exercise_keyed_write_modes(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let update = write_batch(
        vec![1, 999],
        vec![Some("updated"), Some("missing")],
        vec![2, 1],
    );
    let outcome = run_write(
        provider,
        secret,
        budget,
        write_operation(WriteMode::Update, &update.schema()),
        update,
        cancellation,
    )
    .await
    .expect("update Arrow Db2");
    assert_eq!(outcome.rows.updated, Some(1));
    assert_eq!(outcome.rows.skipped, 1);

    let upsert = write_batch(vec![1, 3], vec![Some("merged"), Some("three")], vec![3, 1]);
    let outcome = run_write(
        provider,
        secret,
        budget,
        write_operation(WriteMode::Upsert, &upsert.schema()),
        upsert,
        cancellation,
    )
    .await
    .expect("upsert Arrow Db2");
    assert_eq!(outcome.rows.confirmed, 2);

    let delete_schema = contract_schema(vec![Field::new("ID", DataType::Int32, false)]);
    let delete = RecordBatch::try_new(
        Arc::clone(&delete_schema),
        vec![Arc::new(Int32Array::from(vec![2, 999]))],
    )
    .expect("batch delete Db2");
    let outcome = run_write(
        provider,
        secret,
        budget,
        write_operation(WriteMode::DeleteByKeys, &delete_schema),
        delete,
        cancellation,
    )
    .await
    .expect("delete Arrow Db2");
    assert_eq!(outcome.rows.deleted, Some(1));
    assert_eq!(outcome.rows.skipped, 1);
}

async fn exercise_replace_and_verify(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let replace = write_batch(vec![4], vec![Some("only")], vec![1]);
    let outcome = run_write(
        provider,
        secret,
        budget,
        write_operation(WriteMode::Replace, &replace.schema()),
        replace,
        cancellation,
    )
    .await
    .expect("replace Arrow Db2");
    assert_eq!(outcome.rows.inserted, Some(1));

    let mut verification = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("verifica write Db2");
    let rows = verification
        .query(
            &Statement::new("SELECT ID, VALUE FROM PLENORA_TEST.WRITE_PROBE ORDER BY ID"),
            cancellation,
        )
        .await
        .expect("righe finali write Db2");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ID"), Some(&ParameterValue::I32(4)));
    assert_eq!(
        rows[0].get("VALUE"),
        Some(&ParameterValue::String("only".to_owned()))
    );
    verification
        .rollback(cancellation)
        .await
        .expect("rollback verifica write Db2");
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_arrow_write_modes_are_atomic_and_accounted() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    reset_write_probe(&provider, &secret, &budget, &cancellation).await;

    exercise_append_and_atomic_rollback(&provider, &secret, &budget, &cancellation).await;
    exercise_keyed_write_modes(&provider, &secret, &budget, &cancellation).await;
    exercise_replace_and_verify(&provider, &secret, &budget, &cancellation).await;
}

fn create_operation(object: &str) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: Some("PLENORA".to_owned()),
            schema: Some("PLENORA_TEST".to_owned()),
            object: object.to_owned(),
        },
        mode: WriteMode::Create,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec!["ID".to_owned()],
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn scalar_create_batch(ids: Vec<i32>) -> RecordBatch {
    let row_count = ids.len();
    let schema = contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("FLAG", DataType::Boolean, true),
        Field::new("COUNT_SMALL", DataType::Int16, true),
        Field::new("COUNT_BIG", DataType::Int64, true),
        Field::new("RATIO_REAL", DataType::Float32, true),
        Field::new("RATIO_DOUBLE", DataType::Float64, true),
        Field::new("AMOUNT", DataType::Decimal128(18, 4), true),
        Field::new("LABEL", DataType::Utf8, true),
        Field::new("CREATED_ON", DataType::Date32, true),
        Field::new(
            "CREATED_AT",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]);
    let decimals = Decimal128Array::from(first_then_null(row_count, 1_234_567_i128))
        .with_precision_and_scale(18, 4)
        .expect("decimal fixture Db2");
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(BooleanArray::from(first_then_null(row_count, true))),
            Arc::new(Int16Array::from(first_then_null(row_count, 12_i16))),
            Arc::new(Int64Array::from(first_then_null(
                row_count,
                9_223_372_036_i64,
            ))),
            Arc::new(Float32Array::from(first_then_null(row_count, 1.25_f32))),
            Arc::new(Float64Array::from(first_then_null(row_count, 3.5_f64))),
            Arc::new(decimals),
            Arc::new(StringArray::from(first_then_null(row_count, "created"))),
            Arc::new(Date32Array::from(first_then_null(row_count, 20_692_i32))),
            Arc::new(TimestampMicrosecondArray::from(first_then_null(
                row_count,
                1_787_835_296_123_456_i64,
            ))),
        ],
    )
    .expect("batch create scalare Db2")
}

fn first_then_null<T: Clone>(rows: usize, value: T) -> Vec<Option<T>> {
    (0..rows)
        .map(|index| (index == 0).then(|| value.clone()))
        .collect()
}

async fn drop_table_if_present(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    object: &str,
) {
    let source = ObjectRef {
        catalog: Some("PLENORA".to_owned()),
        schema: Some("PLENORA_TEST".to_owned()),
        object: object.to_owned(),
    };
    match provider
        .inspect(
            secret,
            &Operation::DatabaseDescribeObject {
                source: source.clone(),
            },
            cancellation,
        )
        .await
    {
        Err(error) if error.category == plenora_database_core::ErrorCategory::NotFound => return,
        Err(error) => panic!("introspezione cleanup Db2: {error}"),
        Ok(_) => {}
    }
    let mut transaction = crate::transaction::Db2Transaction::begin(
        provider.config(),
        secret,
        &TransactionOptions::default(),
        budget,
        cancellation,
    )
    .await
    .expect("transazione cleanup DDL Db2");
    transaction
        .execute_control_statement(
            format!("DROP TABLE PLENORA_TEST.\"{object}\""),
            cancellation,
        )
        .await
        .expect("drop tabella fixture Db2");
    Box::new(transaction)
        .commit(cancellation)
        .await
        .expect("commit cleanup DDL Db2");
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_create_write_is_transactional_and_scalar_complete() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    for object in ["WRITE_CREATED", "WRITE_CREATE_ROLLBACK"] {
        drop_table_if_present(&provider, &secret, &budget, &cancellation, object).await;
    }

    let batch = scalar_create_batch(vec![1, 2]);
    let outcome = run_write(
        &provider,
        &secret,
        &budget,
        create_operation("WRITE_CREATED"),
        batch,
        &cancellation,
    )
    .await
    .expect("create scalare Db2");
    assert_eq!(outcome.rows.inserted, Some(2));

    let duplicate = scalar_create_batch(vec![1, 1]);
    let error = run_write(
        &provider,
        &secret,
        &budget,
        create_operation("WRITE_CREATE_ROLLBACK"),
        duplicate,
        &cancellation,
    )
    .await
    .expect_err("create Db2 con righe duplicate");
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::RolledBack
    );
    let absent = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: create_operation("WRITE_CREATE_ROLLBACK").target,
            },
            &cancellation,
        )
        .await
        .expect_err("DDL Create Db2 deve essere annullato");
    assert_eq!(
        absent.category,
        plenora_database_core::ErrorCategory::NotFound
    );

    drop_table_if_present(&provider, &secret, &budget, &cancellation, "WRITE_CREATED").await;
}
