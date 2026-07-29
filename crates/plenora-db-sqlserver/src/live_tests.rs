use crate::{
    describe_object, list_objects, list_schemas, prepare_write, prepare_write_with_mode,
    probe_server, read_object, write_prepared, CertificatePolicy, SqlServerColumnKind,
    SqlServerColumnSpec, SqlServerConfig, SqlServerInsertMode, SqlServerPool, SqlServerProvider,
    SqlServerSession, SqlServerWireEncoding,
};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, Decimal128Array, Int32Array, Int64Array, StringArray,
};
use plenora_database_core::arrow::{DataType, Field, RecordBatch, Schema, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteStatus;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, OrderBy, ReadOperation, SortDirection, SridPolicy,
    TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::protocol;
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::query::{
    ColumnRef, CommonTableExpression, JoinKind, QueryExpression, QueryJoin, QueryOperation,
    QueryOrdering, QueryProjection, QuerySetOperation, QuerySetOperator, QuerySource,
    ScalarFunction,
};
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, RemoteEffect, ResourceBudget, ResourceLimits,
    RetryDisposition,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tiberius::Query;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProxyMode {
    Forward,
    Blackhole,
    Cut,
}

fn live_config(policy: CertificatePolicy) -> SqlServerConfig {
    let host = std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".to_owned());
    let database =
        std::env::var("PLENORA_SQLSERVER_DATABASE").unwrap_or_else(|_| "dataflow_test".to_owned());
    let username =
        std::env::var("PLENORA_SQLSERVER_USER").unwrap_or_else(|_| "dataflow".to_owned());
    let password = std::env::var("PLENORA_SQLSERVER_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    SqlServerConfig::new(host, database, username, SecretString::new(password))
        .with_certificate_policy(policy)
}

fn live_secret() -> SecretString {
    SecretString::new(
        std::env::var("PLENORA_SQLSERVER_PASSWORD")
            .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned()),
    )
}

fn live_provider() -> SqlServerProvider {
    SqlServerProvider::new(live_config(CertificatePolicy::TrustServerCertificate), 2, 3)
        .expect("live provider")
}

fn proxied_live_config(port: u16) -> SqlServerConfig {
    let database =
        std::env::var("PLENORA_SQLSERVER_DATABASE").unwrap_or_else(|_| "dataflow_test".to_owned());
    let username =
        std::env::var("PLENORA_SQLSERVER_USER").unwrap_or_else(|_| "dataflow".to_owned());
    let password = std::env::var("PLENORA_SQLSERVER_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    SqlServerConfig::new("127.0.0.1", database, username, SecretString::new(password))
        .with_port(port)
        .with_certificate_policy(CertificatePolicy::TrustServerCertificate)
}

fn live_admin_config() -> SqlServerConfig {
    let host = std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".to_owned());
    let database =
        std::env::var("PLENORA_SQLSERVER_DATABASE").unwrap_or_else(|_| "dataflow_test".to_owned());
    let password = std::env::var("PLENORA_SQLSERVER_SA_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    SqlServerConfig::new(host, database, "sa", SecretString::new(password))
        .with_certificate_policy(CertificatePolicy::TrustServerCertificate)
}

struct TcpCutProxy {
    port: u16,
    mode: watch::Sender<ProxyMode>,
    active_connections: Arc<AtomicUsize>,
    blackholed_connections: Arc<AtomicUsize>,
    mode_applied: Arc<Notify>,
    connection_dropped: Arc<Notify>,
    accept_task: JoinHandle<()>,
}

impl TcpCutProxy {
    async fn start(upstream_host: &str, upstream_port: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind TDS cut proxy");
        let port = listener.local_addr().expect("proxy local address").port();
        let upstream = format!("{upstream_host}:{upstream_port}");
        let (mode, mut accept_mode) = watch::channel(ProxyMode::Forward);
        let active_connections = Arc::new(AtomicUsize::new(0));
        let blackholed_connections = Arc::new(AtomicUsize::new(0));
        let mode_applied = Arc::new(Notify::new());
        let connection_dropped = Arc::new(Notify::new());
        let task_active = Arc::clone(&active_connections);
        let task_blackholed = Arc::clone(&blackholed_connections);
        let task_mode_applied = Arc::clone(&mode_applied);
        let task_dropped = Arc::clone(&connection_dropped);
        let accept_task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    changed = accept_mode.changed() => {
                        assert!(changed.is_ok(), "proxy mode sender dropped unexpectedly");
                        if *accept_mode.borrow() == ProxyMode::Cut {
                            break;
                        }
                        continue;
                    }
                    accepted = listener.accept() => accepted,
                };
                let (mut client, _) = accepted.expect("accept proxied TDS connection");
                let mut server = TcpStream::connect(&upstream)
                    .await
                    .expect("connect proxy upstream");
                client.set_nodelay(true).expect("client proxy nodelay");
                server.set_nodelay(true).expect("server proxy nodelay");
                let mut connection_mode = accept_mode.clone();
                let active = Arc::clone(&task_active);
                let blackholed = Arc::clone(&task_blackholed);
                let mode_applied = Arc::clone(&task_mode_applied);
                let dropped = Arc::clone(&task_dropped);
                active.fetch_add(1, Ordering::AcqRel);
                tokio::spawn(async move {
                    'connection: loop {
                        let current_mode = *connection_mode.borrow();
                        match current_mode {
                            ProxyMode::Forward => {
                                let transfer =
                                    tokio::io::copy_bidirectional(&mut client, &mut server);
                                tokio::pin!(transfer);
                                tokio::select! {
                                    changed = connection_mode.changed() => {
                                        assert!(changed.is_ok(), "proxy mode sender dropped");
                                    }
                                    result = &mut transfer => {
                                        assert!(
                                            result.is_ok()
                                                || *connection_mode.borrow() == ProxyMode::Cut,
                                            "unexpected proxy TDS transfer failure"
                                        );
                                        break 'connection;
                                    }
                                }
                            }
                            ProxyMode::Blackhole => {
                                blackholed.fetch_add(1, Ordering::AcqRel);
                                mode_applied.notify_waiters();
                                let changed = connection_mode.changed().await;
                                blackholed.fetch_sub(1, Ordering::AcqRel);
                                assert!(changed.is_ok(), "proxy mode sender dropped");
                            }
                            ProxyMode::Cut => break,
                        }
                    }
                    drop(client);
                    drop(server);
                    active.fetch_sub(1, Ordering::AcqRel);
                    dropped.notify_waiters();
                });
            }
        });
        Self {
            port,
            mode,
            active_connections,
            blackholed_connections,
            mode_applied,
            connection_dropped,
            accept_task,
        }
    }

    async fn blackhole(&self) {
        let active = self.active_connections.load(Ordering::Acquire);
        assert!(active > 0, "blackhole requires an active connection");
        self.mode
            .send(ProxyMode::Blackhole)
            .expect("signal TDS blackhole");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let applied = self.mode_applied.notified();
                if self.blackholed_connections.load(Ordering::Acquire) == active {
                    break;
                }
                applied.await;
            }
        })
        .await
        .expect("all proxied TDS connections must enter blackhole");
    }

    async fn cut(&self) {
        assert!(
            self.active_connections.load(Ordering::Acquire) > 0,
            "physical fault requires an active proxied connection"
        );
        self.mode
            .send(ProxyMode::Cut)
            .expect("signal physical TDS cut");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let dropped = self.connection_dropped.notified();
                if self.active_connections.load(Ordering::Acquire) == 0 {
                    break;
                }
                dropped.await;
            }
        })
        .await
        .expect("proxied TDS connection must be physically dropped");
    }
}

impl Drop for TcpCutProxy {
    fn drop(&mut self) {
        let _send_result = self.mode.send(ProxyMode::Cut);
        self.accept_task.abort();
    }
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito"]
async fn live_reference_probe_and_catalog() {
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(
        &live_config(CertificatePolicy::TrustServerCertificate),
        &cancellation,
    )
    .await
    .expect("open live SQL Server");

    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe live");
    assert!(probe.product_version.starts_with("16."));
    assert_eq!(probe.compatibility_level, 160);
    assert!(probe.geometry_type_id.is_some());
    assert!(probe.geography_type_id.is_some());

    let schemas = list_schemas(&mut session, &cancellation)
        .await
        .expect("list schemas");
    assert!(schemas.iter().any(|schema| schema == "plenora_test"));

    let objects = list_objects(&mut session, Some("plenora_test"), &cancellation)
        .await
        .expect("list objects");
    assert!(objects.iter().any(|object| object.name == "catalog_probe"));

    let description = describe_object(&mut session, "plenora_test", "catalog_probe", &cancellation)
        .await
        .expect("describe reference");
    assert!(description
        .columns
        .iter()
        .any(|column| column.name == "shape" && column.native_type == "geometry"));
    assert!(description
        .columns
        .iter()
        .any(|column| column.name == "position" && column.native_type == "geography"));
    assert!(description
        .columns
        .iter()
        .any(|column| column.name == "computed_name" && column.computed));
    assert!(description
        .constraints
        .iter()
        .any(|constraint| constraint.kind == "PRIMARY_KEY_CONSTRAINT"));
    assert!(description.indexes.iter().any(|index| index.primary_key));
    assert_eq!(description.token.structural_fingerprint.len(), 64);
    assert!(session.is_reusable());
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta la fixture write"]
#[allow(clippy::too_many_lines)]
async fn live_common_provider_contract_read_and_write() {
    let provider = live_provider();
    let secret = live_secret();
    let supported = [
        Operation::DatabaseListCatalogs,
        Operation::DatabaseListSchemas { source: None },
        Operation::DatabaseListObjects {
            source: Some(ObjectRef {
                catalog: None,
                schema: Some("plenora_test".to_owned()),
                object: String::new(),
                layer_id: None,
            }),
        },
        Operation::DatabaseDescribeObject {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_test".to_owned()),
                object: "catalog_probe".to_owned(),
                layer_id: None,
            },
        },
    ];
    let report = plenora_database_testkit::verify_provider_contract(
        &provider,
        &secret,
        &supported,
        Some(&Operation::ArcgisTestConnection),
    )
    .await
    .expect("common provider conformance");
    assert_eq!(
        report.provider,
        plenora_database_core::plan::ProviderKind::Sqlserver
    );
    assert_eq!(report.inspected_operations.len(), supported.len());
    assert!(report.pre_cancelled_connection_verified);
    assert!(report.unsupported_inspection_verified);

    let cancellation = CancellationToken::new();
    let bounded_operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: "stream_probe".to_owned(),
            layer_id: None,
        },
        projection: vec!["id".to_owned(), "label".to_owned()],
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Desc,
        }],
        row_limit: Some(2),
        filter: Some(FilterExpression::Gte {
            field: "id".to_owned(),
            parameter: "minimum_id".to_owned(),
        }),
    };
    let bounded_parameters = ParameterBag::new(BTreeMap::from([(
        "minimum_id".to_owned(),
        ParameterValue::I32(3),
    )]));
    let column = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: Some("source".to_owned()),
            field: field.to_owned(),
        },
    };
    let query_operation = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: bounded_operation.source.clone(),
            alias: Some("source".to_owned()),
        }),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: column("id"),
                alias: None,
            },
            QueryProjection {
                expression: column("label"),
                alias: None,
            },
        ],
        joins: Vec::new(),
        filter: Some(QueryExpression::Compare {
            left: Box::new(column("id")),
            operator: plenora_database_core::plan::ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "minimum_id".to_owned(),
            }),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: column("id"),
            direction: SortDirection::Desc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(2),
        row_offset: None,
        locking: None,
    };
    let query_budget = ResourceBudget::new(ResourceLimits::default()).expect("query budget");
    let mut query_stream = provider
        .query(
            &secret,
            &query_operation,
            &bounded_parameters,
            &query_budget,
            &cancellation,
        )
        .await
        .expect("provider QueryOperation");
    let query_batch = query_stream
        .next_batch()
        .await
        .expect("query batch")
        .expect("query rows");
    let query_ids = query_batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .expect("query ids");
    assert_eq!(query_ids.values(), &[5, 4]);
    assert!(query_stream
        .next_batch()
        .await
        .expect("query end")
        .is_none());

    let bounded_budget = ResourceBudget::new(ResourceLimits::default()).expect("bounded budget");
    let mut bounded = provider
        .read(
            &secret,
            &bounded_operation,
            &bounded_parameters,
            &bounded_budget,
            &cancellation,
        )
        .await
        .expect("provider bounded read");
    assert_eq!(bounded.schema().fields().len(), 2);
    let batch = bounded
        .next_batch()
        .await
        .expect("bounded batch")
        .expect("bounded rows");
    let ids = batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .expect("bounded ids");
    assert_eq!(ids.values(), &[5, 4]);
    assert!(bounded.next_batch().await.expect("bounded end").is_none());

    let read_operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: "stream_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        filter: None,
    };
    let read_budget = ResourceBudget::new(ResourceLimits::default()).expect("read budget");
    let source = provider
        .read(
            &secret,
            &read_operation,
            &ParameterBag::default(),
            &read_budget,
            &cancellation,
        )
        .await
        .expect("provider read");
    let input_schema = source.schema();

    let write_operation = write_operation("write_probe", WriteMode::TruncateInsert);
    let write_budget = ResourceBudget::new(ResourceLimits::default()).expect("write budget");
    let prepared = provider
        .prepare_write(
            &secret,
            &write_operation,
            input_schema,
            &write_budget,
            &cancellation,
        )
        .await
        .expect("provider prepare write");
    let outcome = provider
        .write(&secret, prepared, source, &write_budget, &cancellation)
        .await
        .expect("provider write");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.confirmed, 5);

    let verify_operation = ReadOperation {
        source: ObjectRef {
            object: "write_probe".to_owned(),
            ..read_operation.source
        },
        ..read_operation
    };
    let verify_budget = ResourceBudget::new(ResourceLimits::default()).expect("verify budget");
    let mut verify = provider
        .read(
            &secret,
            &verify_operation,
            &ParameterBag::default(),
            &verify_budget,
            &cancellation,
        )
        .await
        .expect("provider verify");
    let mut rows = 0_usize;
    while let Some(batch) = verify.next_batch().await.expect("verify batch") {
        rows = rows.saturating_add(batch.num_rows());
    }
    assert_eq!(rows, 5);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito per QueryOperation ricca"]
#[allow(clippy::too_many_lines)]
async fn live_rich_query_cte_join_aggregate_window_set_offset_and_empty_schema() {
    let cancellation = CancellationToken::new();
    let provider = SqlServerProvider::new(
        live_config(CertificatePolicy::TrustServerCertificate),
        64,
        3,
    )
    .expect("rich query provider");
    let secret = live_secret();
    let source = |object: &str, alias: &str| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: object.to_owned(),
            layer_id: None,
        },
        alias: Some(alias.to_owned()),
    };
    let cte_source = |name: &str, alias: &str| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: None,
            object: name.to_owned(),
            layer_id: None,
        },
        alias: Some(alias.to_owned()),
    };
    let column = |relation: &str, field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    };
    let parameter = |name: &str| QueryExpression::Parameter {
        name: name.to_owned(),
    };
    let comparison = |left: QueryExpression,
                      operator: plenora_database_core::plan::ComparisonOperator,
                      right: QueryExpression| QueryExpression::Compare {
        left: Box::new(left),
        operator,
        right: Box::new(right),
    };
    let count = |relation: &str| QueryExpression::Scalar {
        function: ScalarFunction::Count,
        arguments: vec![column(relation, "id")],
    };

    let cte = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source("stream_probe", "base")),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: column("base", "id"),
                alias: Some("id".to_owned()),
            },
            QueryProjection {
                expression: column("base", "label"),
                alias: Some("label".to_owned()),
            },
        ],
        joins: Vec::new(),
        filter: Some(comparison(
            column("base", "id"),
            plenora_database_core::plan::ComparisonOperator::Gte,
            parameter("minimum_id"),
        )),
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
    let aggregate = QueryOperation {
        common_table_expressions: vec![CommonTableExpression {
            name: "filtered".to_owned(),
            recursive: false,
            query: Box::new(cte),
        }],
        source: Some(cte_source("filtered", "f")),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: column("joined", "label"),
                alias: Some("label".to_owned()),
            },
            QueryProjection {
                expression: count("f"),
                alias: Some("event_count".to_owned()),
            },
        ],
        joins: vec![QueryJoin {
            kind: JoinKind::Inner,
            source: Some(source("stream_probe", "joined")),
            derived_source: None,
            lateral: false,
            on: Some(comparison(
                column("f", "id"),
                plenora_database_core::plan::ComparisonOperator::Eq,
                column("joined", "id"),
            )),
        }],
        filter: None,
        group_by: vec![column("joined", "label")],
        having: Some(comparison(
            count("f"),
            plenora_database_core::plan::ComparisonOperator::Gte,
            parameter("minimum_count"),
        )),
        order_by: vec![QueryOrdering {
            expression: column("joined", "label"),
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(10),
        row_offset: None,
        locking: None,
    };
    let aggregate_parameters = ParameterBag::new(BTreeMap::from([
        ("minimum_count".to_owned(), ParameterValue::I64(1)),
        ("minimum_id".to_owned(), ParameterValue::I32(3)),
    ]));
    let aggregate_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("aggregate budget");
    let mut aggregate_stream = provider
        .query(
            &secret,
            &aggregate,
            &aggregate_parameters,
            &aggregate_budget,
            &cancellation,
        )
        .await
        .expect("aggregate query");
    let aggregate_batch = aggregate_stream
        .next_batch()
        .await
        .expect("aggregate batch")
        .expect("aggregate rows");
    let labels = aggregate_batch
        .column_by_name("label")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .expect("aggregate labels");
    assert_eq!(
        labels.iter().flatten().collect::<Vec<_>>(),
        ["row-3", "row-4", "row-5"]
    );
    let counts = aggregate_batch
        .column_by_name("event_count")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .expect("aggregate counts");
    assert_eq!(counts.values(), &[1, 1, 1]);
    assert!(aggregate_stream
        .next_batch()
        .await
        .expect("aggregate end")
        .is_none());

    let window = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source("stream_probe", "source")),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: column("source", "id"),
                alias: Some("id".to_owned()),
            },
            QueryProjection {
                expression: QueryExpression::Window {
                    function: ScalarFunction::RowNumber,
                    arguments: Vec::new(),
                    partition_by: Vec::new(),
                    order_by: vec![QueryOrdering {
                        expression: column("source", "id"),
                        direction: SortDirection::Asc,
                    }],
                    frame: None,
                },
                alias: Some("row_number".to_owned()),
            },
        ],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: column("source", "id"),
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(2),
        row_offset: Some(2),
        locking: None,
    };
    let window_budget = ResourceBudget::new(ResourceLimits::default()).expect("window budget");
    let mut window_stream = provider
        .query(
            &secret,
            &window,
            &ParameterBag::default(),
            &window_budget,
            &cancellation,
        )
        .await
        .expect("window query");
    let window_batch = window_stream
        .next_batch()
        .await
        .expect("window batch")
        .expect("window rows");
    let ids = window_batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .expect("window ids");
    let row_numbers = window_batch
        .column_by_name("row_number")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .expect("window row numbers");
    assert_eq!(ids.values(), &[3, 4]);
    assert_eq!(row_numbers.values(), &[3, 4]);

    let set_rhs = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source("stream_probe", "right_source")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("right_source", "id"),
            alias: Some("id".to_owned()),
        }],
        joins: Vec::new(),
        filter: Some(comparison(
            column("right_source", "id"),
            plenora_database_core::plan::ComparisonOperator::Gte,
            parameter("lower_tail"),
        )),
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
    let set_query = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source("stream_probe", "left_source")),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("left_source", "id"),
            alias: Some("id".to_owned()),
        }],
        joins: Vec::new(),
        filter: Some(comparison(
            column("left_source", "id"),
            plenora_database_core::plan::ComparisonOperator::Lte,
            parameter("upper_head"),
        )),
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "id".to_owned(),
                },
            },
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: vec![QuerySetOperation {
            operator: QuerySetOperator::Union,
            all: true,
            query: Box::new(set_rhs),
        }],
        row_limit: None,
        row_offset: None,
        locking: None,
    };
    let set_parameters = ParameterBag::new(BTreeMap::from([
        ("lower_tail".to_owned(), ParameterValue::I32(5)),
        ("upper_head".to_owned(), ParameterValue::I32(2)),
    ]));
    let set_budget = ResourceBudget::new(ResourceLimits::default()).expect("set budget");
    let mut set_stream = provider
        .query(
            &secret,
            &set_query,
            &set_parameters,
            &set_budget,
            &cancellation,
        )
        .await
        .expect("set query");
    let set_batch = set_stream
        .next_batch()
        .await
        .expect("set batch")
        .expect("set rows");
    let set_ids = set_batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .expect("set ids");
    assert_eq!(set_ids.values(), &[1, 2, 5]);

    let native_projection = |parameter_name: &str| QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source("stream_probe", "native_source")),
        derived_source: None,
        projection: [
            "exact_value",
            "calendar_date",
            "clock_time",
            "local_timestamp",
            "offset_timestamp",
            "external_id",
            "document",
        ]
        .into_iter()
        .map(|name| QueryProjection {
            expression: column("native_source", name),
            alias: Some(name.to_owned()),
        })
        .collect(),
        joins: Vec::new(),
        filter: Some(comparison(
            column("native_source", "id"),
            plenora_database_core::plan::ComparisonOperator::Eq,
            parameter(parameter_name),
        )),
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
    let native_budget = ResourceBudget::new(ResourceLimits::default()).expect("native budget");
    let mut native_stream = provider
        .query(
            &secret,
            &native_projection("selected_id"),
            &ParameterBag::new(BTreeMap::from([(
                "selected_id".to_owned(),
                ParameterValue::I32(1),
            )])),
            &native_budget,
            &cancellation,
        )
        .await
        .expect("native query");
    assert_eq!(
        native_stream
            .schema()
            .field_with_name("exact_value")
            .expect("native decimal")
            .data_type(),
        &DataType::Decimal128(20, 6)
    );
    let native_batch = native_stream
        .next_batch()
        .await
        .expect("native batch")
        .expect("native row");
    let decimals = native_batch
        .column_by_name("exact_value")
        .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
        .expect("native decimal values");
    assert_eq!(decimals.value(0), 123_456_789);
    let uuids = native_batch
        .column_by_name("external_id")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .expect("native UUID");
    assert_eq!(uuids.value(0), "00000000-0000-0000-0000-000000000001");
    let xml = native_batch
        .column_by_name("document")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .expect("native XML");
    assert_eq!(xml.value(0), "<row id=\"1\"/>");

    let empty_budget = ResourceBudget::new(ResourceLimits::default()).expect("empty budget");
    let mut empty_stream = provider
        .query(
            &secret,
            &native_projection("missing_id"),
            &ParameterBag::new(BTreeMap::from([(
                "missing_id".to_owned(),
                ParameterValue::I32(999),
            )])),
            &empty_budget,
            &cancellation,
        )
        .await
        .expect("empty query");
    assert_eq!(empty_stream.schema().fields().len(), 7);
    assert!(empty_stream
        .next_batch()
        .await
        .expect("empty result")
        .is_none());

    let mut unnamed = native_projection("selected_id");
    unnamed.projection = vec![QueryProjection {
        expression: count("native_source"),
        alias: None,
    }];
    let unnamed_budget = ResourceBudget::new(ResourceLimits::default()).expect("unnamed budget");
    let unnamed_result = provider
        .query(
            &secret,
            &unnamed,
            &ParameterBag::new(BTreeMap::from([(
                "selected_id".to_owned(),
                ParameterValue::I32(1),
            )])),
            &unnamed_budget,
            &cancellation,
        )
        .await;
    let Err(unnamed_error) = unnamed_result else {
        panic!("unnamed calculated projection must fail");
    };
    assert_eq!(unnamed_error.category, ErrorCategory::Schema);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta la fixture guard"]
async fn live_common_provider_executes_opt_in_tds_bulk() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;
    let provider = SqlServerProvider::new(config, 8, 2)
        .expect("bulk provider")
        .with_insert_mode(SqlServerInsertMode::TdsBulk);
    let secret = live_secret();
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("bulk capabilities");
    assert!(capabilities.writes.bulk);

    let schema = guard_schema();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let operation = write_operation("write_guard_probe", WriteMode::Append);
    let prepared = provider
        .prepare_write(
            &secret,
            &operation,
            Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("provider bulk prepare");
    let outcome = provider
        .write(
            &secret,
            prepared,
            Box::new(VecBatchStream {
                schema: Arc::clone(&schema),
                batches: VecDeque::from([guard_batch(schema, 72, "provider-bulk")]),
            }),
            &budget,
            &cancellation,
        )
        .await
        .expect("provider bulk write");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.confirmed, 1);
    assert_eq!(guard_id_count(&mut admin, 72, &cancellation).await, 1);
    admin
        .execute_query(
            Query::new("DELETE FROM [plenora_test].[write_guard_probe] WHERE [id] = 72;"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("provider bulk cleanup");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito"]
async fn live_self_signed_tls_is_rejected_by_default() {
    let cancellation = CancellationToken::new();
    let error = SqlServerSession::open(&live_config(CertificatePolicy::Verify), &cancellation)
        .await
        .expect_err("self-signed development certificate must fail verification");
    assert_eq!(error.category, ErrorCategory::Authentication);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta la fixture"]
async fn live_schema_token_detects_ddl() {
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(
        &live_config(CertificatePolicy::TrustServerCertificate),
        &cancellation,
    )
    .await
    .expect("open live SQL Server");

    let cleanup = r"
IF COL_LENGTH(N'plenora_test.catalog_probe', N'token_probe') IS NOT NULL
    ALTER TABLE [plenora_test].[catalog_probe] DROP COLUMN [token_probe];
";
    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("normalize token fixture");
    let before = describe_object(&mut session, "plenora_test", "catalog_probe", &cancellation)
        .await
        .expect("token before");
    session
        .execute_query(
            Query::new("ALTER TABLE [plenora_test].[catalog_probe] ADD [token_probe] int NULL;"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("DDL token mutation");
    let after = describe_object(&mut session, "plenora_test", "catalog_probe", &cancellation)
        .await
        .expect("token after");
    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("cleanup token fixture");
    assert_ne!(
        before.token.structural_fingerprint,
        after.token.structural_fingerprint
    );
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito"]
async fn live_bounded_arrow_stream_maps_scalars_and_spatial() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let pool = SqlServerPool::new(config, 2).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut stream = read_object(
        &pool,
        "plenora_test",
        "stream_probe",
        2,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare read");
    let schema = stream.schema();
    assert_eq!(
        schema.field_with_name("id").expect("id").data_type(),
        &DataType::Int32
    );
    assert_eq!(
        schema
            .field_with_name("exact_value")
            .expect("decimal")
            .data_type(),
        &DataType::Decimal128(20, 6)
    );
    for spatial_name in ["shape", "position"] {
        let field = schema.field_with_name(spatial_name).expect("spatial field");
        assert_eq!(field.data_type(), &DataType::Binary);
        assert_eq!(
            field.metadata().get(protocol::GEOARROW_EXTENSION_NAME),
            Some(&"geoarrow.wkb".to_owned())
        );
        assert_eq!(
            field.metadata().get(protocol::GEOMETRY_SRID),
            Some(&"4326".to_owned())
        );
    }

    let mut sizes = Vec::new();
    let mut rows = 0_usize;
    let mut first_checked = false;
    while let Some(batch) = stream
        .next_batch_with_cancellation(&cancellation)
        .await
        .expect("next batch")
    {
        sizes.push(batch.num_rows());
        rows = rows.saturating_add(batch.num_rows());
        if !first_checked {
            let ids = batch
                .column_by_name("id")
                .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
                .expect("id array");
            assert!(!ids.is_empty());
            let decimals = batch
                .column_by_name("exact_value")
                .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
                .expect("decimal array");
            assert!(!decimals.is_empty());
            for name in ["shape", "position"] {
                let spatial = batch
                    .column_by_name(name)
                    .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                    .expect("spatial array");
                assert!(spatial.iter().flatten().all(|value| !value.is_empty()));
            }
            first_checked = true;
        }
    }
    assert_eq!(sizes, vec![2, 2, 1]);
    assert_eq!(rows, 5);
    assert!(first_checked);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(pool.idle_connections(), 1);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito"]
async fn live_drop_of_partial_stream_quarantines_connection() {
    let cancellation = CancellationToken::new();
    let pool = SqlServerPool::new(live_config(CertificatePolicy::TrustServerCertificate), 1)
        .expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let stream = read_object(
        &pool,
        "plenora_test",
        "stream_probe",
        2,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare read");
    drop(stream);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(pool.idle_connections(), 0);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta una fixture isolata"]
async fn live_spatial_preflight_rejects_mixed_srid_and_z() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin session");
    let normalize = r"
IF OBJECT_ID(N'plenora_test.spatial_guard_probe', N'U') IS NOT NULL
    DROP TABLE [plenora_test].[spatial_guard_probe];
CREATE TABLE [plenora_test].[spatial_guard_probe]
(
    [id] int NOT NULL PRIMARY KEY,
    [shape] geometry NOT NULL
);
";
    admin
        .execute_query(Query::new(normalize), ErrorPhase::Write, &cancellation)
        .await
        .expect("normalize spatial guard");
    admin
        .execute_query(
            Query::new(
                "INSERT INTO [plenora_test].[spatial_guard_probe] VALUES \
                 (1, geometry::STGeomFromText('POINT (1 2)', 4326)), \
                 (2, geometry::STGeomFromText('POINT (1 2)', 3857));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("mixed SRID fixture");

    let pool = SqlServerPool::new(config.clone(), 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let Err(mixed_error) = read_object(
        &pool,
        "plenora_test",
        "spatial_guard_probe",
        2,
        &budget,
        &cancellation,
    )
    .await
    else {
        panic!("mixed SRID must fail closed");
    };
    assert_eq!(mixed_error.category, ErrorCategory::DataMapping);

    admin
        .execute_query(
            Query::new(
                "TRUNCATE TABLE [plenora_test].[spatial_guard_probe]; \
                 INSERT INTO [plenora_test].[spatial_guard_probe] VALUES \
                 (1, geometry::STGeomFromText('POINT (1 2 3)', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("Z fixture");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("second budget");
    let Err(z_error) = read_object(
        &pool,
        "plenora_test",
        "spatial_guard_probe",
        2,
        &budget,
        &cancellation,
    )
    .await
    else {
        panic!("Z geometry must fail closed");
    };
    assert_eq!(z_error.category, ErrorCategory::Unsupported);
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[spatial_guard_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup spatial guard");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta la fixture write"]
#[allow(clippy::too_many_lines)]
async fn live_prepared_write_round_trips_all_reference_types() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let pool = SqlServerPool::new(config.clone(), 3).expect("pool");
    let read_budget = ResourceBudget::new(ResourceLimits::default()).expect("read budget");
    let source = read_object(
        &pool,
        "plenora_test",
        "stream_probe",
        2,
        &read_budget,
        &cancellation,
    )
    .await
    .expect("source stream");
    let input_schema = source.schema();
    let write_budget = ResourceBudget::new(ResourceLimits::default()).expect("write budget");
    let operation = write_operation("write_probe", WriteMode::TruncateInsert);
    let prepared = prepare_write(
        &pool,
        &operation,
        Arc::clone(&input_schema),
        &write_budget,
        &cancellation,
    )
    .await
    .expect("prepare write");
    assert!(prepared.loss_report().losses.is_empty());
    let outcome = write_prepared(prepared, source, &cancellation)
        .await
        .expect("write committed");
    assert_eq!(
        outcome.status,
        plenora_database_core::outcome::WriteStatus::Committed
    );
    assert_eq!(outcome.rows.confirmed, 5);
    assert_eq!(outcome.rows.inserted, Some(5));

    let verify_budget = ResourceBudget::new(ResourceLimits::default()).expect("verify budget");
    let mut verify = read_object(
        &pool,
        "plenora_test",
        "write_probe",
        3,
        &verify_budget,
        &cancellation,
    )
    .await
    .expect("verify stream");
    let mut rows = 0_usize;
    while let Some(batch) = verify.next_batch().await.expect("verify batch") {
        rows = rows.saturating_add(batch.num_rows());
    }
    assert_eq!(rows, 5);
    drop(verify);

    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("differential session");
    let mut differential = admin
        .execute_query(
            Query::new(
                r"
WITH source_values AS
(
    SELECT
        [id], [flag], [unsigned_small], [signed_small], [signed_big],
        [single_value], [double_value], [exact_value], [money_value],
        [calendar_date], [clock_time], [local_timestamp],
        CONVERT(nvarchar(40), [offset_timestamp], 127) AS [offset_timestamp],
        [label], [payload], [external_id],
        CONVERT(nvarchar(max), [document]) AS [document],
        [shape].STAsBinary() AS [shape],
        [position].STAsBinary() AS [position]
    FROM [plenora_test].[stream_probe]
),
target_values AS
(
    SELECT
        [id], [flag], [unsigned_small], [signed_small], [signed_big],
        [single_value], [double_value], [exact_value], [money_value],
        [calendar_date], [clock_time], [local_timestamp],
        CONVERT(nvarchar(40), [offset_timestamp], 127) AS [offset_timestamp],
        [label], [payload], [external_id],
        CONVERT(nvarchar(max), [document]) AS [document],
        [shape].STAsBinary() AS [shape],
        [position].STAsBinary() AS [position]
    FROM [plenora_test].[write_probe]
)
SELECT COUNT_BIG(*)
FROM
(
    (SELECT * FROM source_values EXCEPT SELECT * FROM target_values)
    UNION ALL
    (SELECT * FROM target_values EXCEPT SELECT * FROM source_values)
) AS differences;
",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("differential query");
    let differences: Option<i64> = differential
        .pop()
        .and_then(|mut rows| rows.pop())
        .expect("differential row")
        .try_get(0)
        .expect("differential count");
    assert_eq!(differences, Some(0));
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture isolate"]
#[allow(clippy::too_many_lines)]
async fn live_tds_bulk_round_trips_verified_scalar_types() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[bulk_native_probe]; \
                 CREATE TABLE [plenora_test].[bulk_native_probe] \
                 ( \
                    [id] int NOT NULL PRIMARY KEY, [flag] bit NULL, \
                    [unsigned_small] tinyint NULL, [signed_small] smallint NULL, \
                    [signed_big] bigint NULL, [single_value] real NULL, \
                    [double_value] float(53) NULL, \
                    [exact_value] decimal(20, 6) NULL, \
                    [label] nvarchar(100) NULL, [payload] varbinary(32) NULL \
                 );",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create native bulk fixture");
    let columns = [
        "id",
        "flag",
        "unsigned_small",
        "signed_small",
        "signed_big",
        "single_value",
        "double_value",
        "exact_value",
        "label",
        "payload",
    ]
    .map(str::to_owned)
    .to_vec();
    let pool = SqlServerPool::new(config, 2).expect("pool");
    let read_budget = ResourceBudget::new(ResourceLimits::default()).expect("read budget");
    let source = crate::read::read_operation(
        &pool,
        &ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_test".to_owned()),
                object: "stream_probe".to_owned(),
                layer_id: None,
            },
            projection: columns,
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        },
        &ParameterBag::default(),
        2,
        &read_budget,
        &cancellation,
    )
    .await
    .expect("native source");
    let schema = source.schema();
    let write_budget = ResourceBudget::new(ResourceLimits::default()).expect("write budget");
    let prepared = prepare_write_with_mode(
        &pool,
        &write_operation("bulk_native_probe", WriteMode::Append),
        schema,
        &write_budget,
        &cancellation,
        SqlServerInsertMode::TdsBulk,
    )
    .await
    .expect("prepare native bulk");
    let outcome = write_prepared(prepared, source, &cancellation)
        .await
        .expect("native bulk write");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.confirmed, 5);
    let mut differences = admin
        .execute_query(
            Query::new(
                "SELECT COUNT_BIG(*) FROM \
                 ((SELECT [id], [flag], [unsigned_small], [signed_small], [signed_big], \
                          [single_value], [double_value], [exact_value], \
                          [label], [payload] \
                   FROM [plenora_test].[stream_probe] \
                   EXCEPT \
                   SELECT [id], [flag], [unsigned_small], [signed_small], [signed_big], \
                          [single_value], [double_value], [exact_value], \
                          [label], [payload] \
                   FROM [plenora_test].[bulk_native_probe]) \
                  UNION ALL \
                  (SELECT [id], [flag], [unsigned_small], [signed_small], [signed_big], \
                          [single_value], [double_value], [exact_value], \
                          [label], [payload] \
                   FROM [plenora_test].[bulk_native_probe] \
                   EXCEPT \
                   SELECT [id], [flag], [unsigned_small], [signed_small], [signed_big], \
                          [single_value], [double_value], [exact_value], \
                          [label], [payload] \
                   FROM [plenora_test].[stream_probe])) AS d;",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("native differential");
    let count = differences
        .pop()
        .and_then(|mut rows| rows.pop())
        .expect("native difference row")
        .try_get::<i64, _>(0)
        .expect("native difference count");
    assert_eq!(count, Some(0));
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[bulk_native_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup native bulk fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture isolate"]
async fn live_tds_bulk_matches_prepared_across_multiple_batches() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[bulk_prepared_probe]; \
                 DROP TABLE IF EXISTS [plenora_test].[bulk_tds_probe]; \
                 CREATE TABLE [plenora_test].[bulk_prepared_probe] \
                    ([id] int NOT NULL PRIMARY KEY, [label] nvarchar(100) NOT NULL); \
                 CREATE TABLE [plenora_test].[bulk_tds_probe] \
                    ([id] int NOT NULL PRIMARY KEY, [label] nvarchar(100) NOT NULL);",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create differential fixtures");

    let schema = guard_schema();
    let pool = SqlServerPool::new(config, 2).expect("pool");
    let prepared_budget = ResourceBudget::new(ResourceLimits::default()).expect("prepared budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("bulk_prepared_probe", WriteMode::Append),
        Arc::clone(&schema),
        &prepared_budget,
        &cancellation,
    )
    .await
    .expect("prepare row codec");
    let prepared_outcome = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: differential_batches(&schema),
        }),
        &cancellation,
    )
    .await
    .expect("prepared differential write");

    let bulk_budget = ResourceBudget::new(ResourceLimits::default()).expect("bulk budget");
    let bulk = prepare_write_with_mode(
        &pool,
        &write_operation("bulk_tds_probe", WriteMode::Append),
        Arc::clone(&schema),
        &bulk_budget,
        &cancellation,
        SqlServerInsertMode::TdsBulk,
    )
    .await
    .expect("prepare TDS bulk");
    let bulk_outcome = write_prepared(
        bulk,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: differential_batches(&schema),
        }),
        &cancellation,
    )
    .await
    .expect("TDS bulk differential write");

    assert_eq!(prepared_outcome.status, WriteStatus::Committed);
    assert_eq!(bulk_outcome.status, WriteStatus::Committed);
    assert_eq!(prepared_outcome.rows.confirmed, 100);
    assert_eq!(bulk_outcome.rows.confirmed, 100);
    let mut differences = admin
        .execute_query(
            Query::new(
                "SELECT COUNT_BIG(*) FROM \
                 ((SELECT [id], [label] FROM [plenora_test].[bulk_prepared_probe] \
                   EXCEPT SELECT [id], [label] FROM [plenora_test].[bulk_tds_probe]) \
                  UNION ALL \
                  (SELECT [id], [label] FROM [plenora_test].[bulk_tds_probe] \
                   EXCEPT SELECT [id], [label] FROM [plenora_test].[bulk_prepared_probe])) AS d;",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("differential compare");
    let count = differences
        .pop()
        .and_then(|mut rows| rows.pop())
        .expect("difference row")
        .try_get::<i64, _>(0)
        .expect("difference count");
    assert_eq!(count, Some(0));
    admin
        .execute_query(
            Query::new(
                "DROP TABLE [plenora_test].[bulk_prepared_probe]; \
                 DROP TABLE [plenora_test].[bulk_tds_probe];",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup differential fixtures");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e verifica rollback TDS bulk"]
async fn live_tds_bulk_constraint_failure_rolls_back_prior_batches() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;
    let schema = guard_schema();
    let input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([
            guard_batch(Arc::clone(&schema), 71, "first-bulk"),
            guard_batch(Arc::clone(&schema), 71, "duplicate-bulk"),
        ]),
    });
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write_with_mode(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        schema,
        &budget,
        &cancellation,
        SqlServerInsertMode::TdsBulk,
    )
    .await
    .expect("prepare bulk guard");
    let error = write_prepared(prepared, input, &cancellation)
        .await
        .expect_err("duplicate bulk key must fail");
    assert!(matches!(
        error.category,
        ErrorCategory::Conflict | ErrorCategory::Execution
    ));
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(guard_id_count(&mut admin, 71, &cancellation).await, 0);
    assert_eq!(guard_id_count(&mut admin, 99, &cancellation).await, 1);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e verifica rollback"]
async fn live_constraint_failure_rolls_back_truncate_and_prior_batches() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    admin
        .execute_query(
            Query::new(
                "TRUNCATE TABLE [plenora_test].[write_guard_probe]; \
                 INSERT INTO [plenora_test].[write_guard_probe] ([id], [label]) \
                 VALUES (99, N'sentinel');",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("normalize guard");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let first = guard_batch(Arc::clone(&schema), 1, "first");
    let duplicate = guard_batch(Arc::clone(&schema), 1, "duplicate");
    let input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([first, duplicate]),
    });
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::TruncateInsert),
        schema,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare guard");
    let error = write_prepared(prepared, input, &cancellation)
        .await
        .expect_err("duplicate key must fail");
    assert!(matches!(
        error.category,
        ErrorCategory::Conflict | ErrorCategory::Execution
    ));
    let mut results = admin
        .execute_query(
            Query::new("SELECT [id], [label] FROM [plenora_test].[write_guard_probe];"),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify rollback");
    let rows = results.pop().expect("result set");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<i32, _>(0).expect("id"), Some(99));
    assert_eq!(
        rows[0].try_get::<&str, _>(1).expect("label"),
        Some("sentinel")
    );
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e fault injection pre-commit"]
async fn live_fault_before_commit_confirms_rollback() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;

    let schema = guard_schema();
    let input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([guard_batch(Arc::clone(&schema), 11, "pre-commit")]),
    });
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        schema,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare fault write");

    let error = crate::write::write_prepared_with_fault(
        prepared,
        input,
        &cancellation,
        crate::write::WriteFaultPoint::BeforeCommit,
    )
    .await
    .expect_err("pre-commit fault must fail");

    assert_eq!(error.category, ErrorCategory::Timeout);
    assert_eq!(error.phase, ErrorPhase::Write);
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(guard_id_count(&mut admin, 11, &cancellation).await, 0);
    assert_eq!(guard_id_count(&mut admin, 99, &cancellation).await, 1);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e perdita trasporto TDS"]
async fn live_fault_transport_loss_requires_recovery_and_server_rolls_back() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;

    let schema = guard_schema();
    let input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([guard_batch(Arc::clone(&schema), 12, "transport")]),
    });
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        schema,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare fault write");

    let error = crate::write::write_prepared_with_fault(
        prepared,
        input,
        &cancellation,
        crate::write::WriteFaultPoint::TransportLostAfterFirstInsert,
    )
    .await
    .expect_err("transport fault must fail");

    assert_eq!(error.category, ErrorCategory::Io);
    assert_eq!(error.phase, ErrorPhase::Write);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    assert!(error.execution_id.is_some());
    assert_eq!(guard_id_count(&mut admin, 12, &cancellation).await, 0);
    assert_eq!(guard_id_count(&mut admin, 99, &cancellation).await, 1);
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e perdita conferma commit"]
async fn live_fault_commit_confirmation_lost_is_outcome_unknown() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;

    let schema = guard_schema();
    let input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([guard_batch(Arc::clone(&schema), 13, "commit-lost")]),
    });
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        schema,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare fault write");

    let outcome = crate::write::write_prepared_with_fault(
        prepared,
        input,
        &cancellation,
        crate::write::WriteFaultPoint::CommitConfirmationLost,
    )
    .await
    .expect("lost confirmation is a valid uncertain outcome");

    assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
    assert_eq!(outcome.rows.received, 1);
    assert_eq!(outcome.rows.confirmed, 0);
    let recovery = outcome.recovery.expect("recovery contract");
    assert!(!recovery.automatic_retry_allowed);
    assert!(recovery.verification_action.is_some());
    assert_eq!(guard_id_count(&mut admin, 13, &cancellation).await, 1);
}

#[tokio::test]
#[ignore = "richiede SQL Server live e blackhole fisico durante read TDS"]
async fn live_physical_blackhole_during_read_times_out_and_quarantines() {
    let cancellation = CancellationToken::new();
    let direct_config = live_config(CertificatePolicy::TrustServerCertificate);
    let proxy = TcpCutProxy::start(direct_config.host(), direct_config.port()).await;
    let config = proxied_live_config(proxy.port)
        .with_application_name("plenora-blackhole-read")
        .with_timeouts(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(2),
        );
    let mut session = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("proxied read session");
    let mut admin = SqlServerSession::open(&live_admin_config(), &cancellation)
        .await
        .expect("server-state admin");
    let (sender, _receiver) = mpsc::channel(1);
    let worker_cancellation = cancellation.clone();
    let expected_columns = vec![SqlServerColumnSpec {
        name: "value".to_owned(),
        native_type: "int".to_owned(),
        native_declaration: "int".to_owned(),
        nullable: false,
        collation: None,
        kind: SqlServerColumnKind::I32,
        spatial_srid: None,
        wire_encoding: SqlServerWireEncoding::Native,
    }];
    let worker = tokio::spawn(async move {
        let result = session
            .pump_query_rows(
                Query::new("WAITFOR DELAY '00:00:10'; SELECT 1 AS [value];"),
                sender,
                &expected_columns,
                &worker_cancellation,
            )
            .await;
        (result, session)
    });

    wait_for_application_request(&mut admin, "plenora-blackhole-read", &cancellation).await;
    proxy.blackhole().await;
    let (result, session) = tokio::time::timeout(std::time::Duration::from_secs(3), worker)
        .await
        .expect("read must time out under blackhole")
        .expect("read worker");
    let error = result.expect_err("blackholed read must not succeed");
    assert_eq!(error.category, ErrorCategory::Timeout);
    assert_eq!(error.phase, ErrorPhase::Read);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert!(!session.is_reusable());
    proxy.cut().await;
}

#[tokio::test]
#[ignore = "richiede SQL Server live e taglio fisico del trasporto TDS durante write"]
async fn live_physical_tds_cut_during_write_requires_recovery() {
    let cancellation = CancellationToken::new();
    let direct_config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&direct_config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;
    let proxy = TcpCutProxy::start(direct_config.host(), direct_config.port()).await;

    let schema = guard_schema();
    let (barrier_reached, barrier_wait) = oneshot::channel();
    let (release, release_wait) = oneshot::channel();
    let input: Box<dyn BatchStream> = Box::new(BarrierBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([
            guard_batch(Arc::clone(&schema), 14, "physical-first"),
            guard_batch(Arc::clone(&schema), 15, "must-not-arrive"),
        ]),
        emitted: 0,
        barrier_reached: Some(barrier_reached),
        release: Some(release_wait),
    });
    let pool = SqlServerPool::new(proxied_live_config(proxy.port), 1).expect("proxied pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        schema,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare proxied write");
    let writer_cancellation = cancellation.clone();
    let writer =
        tokio::spawn(async move { write_prepared(prepared, input, &writer_cancellation).await });

    tokio::time::timeout(std::time::Duration::from_secs(2), barrier_wait)
        .await
        .expect("writer must reach physical cut barrier")
        .expect("writer barrier sender");
    proxy.cut().await;
    release.send(()).expect("release writer after TDS cut");
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .expect("writer must terminate after physical cut")
        .expect("writer task")
        .expect_err("physical write cut must not succeed");

    assert_eq!(error.category, ErrorCategory::Io);
    assert_eq!(error.phase, ErrorPhase::Write);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            guard_id_count(&mut admin, 14, &cancellation),
        )
        .await
        .expect("server must release target after physical cut"),
        0
    );
    assert_eq!(guard_id_count(&mut admin, 15, &cancellation).await, 0);
    assert_eq!(guard_id_count(&mut admin, 99, &cancellation).await, 1);
}

#[tokio::test]
#[ignore = "richiede SQL Server live e taglio fisico della risposta commit TDS"]
async fn live_physical_tds_cut_after_server_commit_is_outcome_unknown() {
    let cancellation = CancellationToken::new();
    let direct_config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&direct_config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;
    let proxy = TcpCutProxy::start(direct_config.host(), direct_config.port()).await;

    let schema = guard_schema();
    let (barrier_reached, barrier_wait) = oneshot::channel();
    let (release, release_wait) = oneshot::channel();
    let input: Box<dyn BatchStream> = Box::new(BarrierBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([guard_batch(Arc::clone(&schema), 16, "physical-commit")]),
        emitted: 0,
        barrier_reached: Some(barrier_reached),
        release: Some(release_wait),
    });
    let pool = SqlServerPool::new(proxied_live_config(proxy.port), 1).expect("proxied pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        schema,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare proxied commit");
    let writer_cancellation = cancellation.clone();
    let writer = tokio::spawn(async move {
        crate::write::write_prepared_with_fault(
            prepared,
            input,
            &writer_cancellation,
            crate::write::WriteFaultPoint::DelayCommitResponse,
        )
        .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(2), barrier_wait)
        .await
        .expect("writer must reach commit barrier")
        .expect("commit barrier sender");
    release.send(()).expect("release delayed commit");
    let committed_rows = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        guard_id_count(&mut admin, 16, &cancellation),
    )
    .await
    .expect("independent session must observe committed row");
    assert_eq!(committed_rows, 1);
    proxy.cut().await;
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .expect("writer must terminate after commit response cut")
        .expect("writer task")
        .expect("commit response loss is represented as an outcome");

    assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
    assert_eq!(outcome.rows.received, 1);
    assert_eq!(outcome.rows.confirmed, 0);
    let recovery = outcome.recovery.expect("recovery contract");
    assert!(!recovery.automatic_retry_allowed);
    assert!(recovery.verification_action.is_some());
    assert_eq!(guard_id_count(&mut admin, 16, &cancellation).await, 1);
}

#[tokio::test]
#[ignore = "richiede SQL Server live e blackhole della risposta rollback TDS"]
async fn live_physical_blackhole_after_server_rollback_requires_recovery() {
    let cancellation = CancellationToken::new();
    let direct_config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&direct_config, &cancellation)
        .await
        .expect("admin");
    normalize_guard_fixture(&mut admin, &cancellation).await;
    let proxy = TcpCutProxy::start(direct_config.host(), direct_config.port()).await;

    let schema = guard_schema();
    let (barrier_reached, barrier_wait) = oneshot::channel();
    let (release, release_wait) = oneshot::channel();
    let input: Box<dyn BatchStream> = Box::new(BarrierBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([guard_batch(Arc::clone(&schema), 17, "rollback-blackhole")]),
        emitted: 0,
        barrier_reached: Some(barrier_reached),
        release: Some(release_wait),
    });
    let config = proxied_live_config(proxy.port).with_timeouts(
        std::time::Duration::from_secs(2),
        std::time::Duration::from_secs(3),
        std::time::Duration::from_secs(2),
    );
    let pool = SqlServerPool::new(config, 1).expect("proxied pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        schema,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare proxied rollback");
    let writer_cancellation = cancellation.clone();
    let writer = tokio::spawn(async move {
        crate::write::write_prepared_with_fault(
            prepared,
            input,
            &writer_cancellation,
            crate::write::WriteFaultPoint::DelayRollbackResponse,
        )
        .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(2), barrier_wait)
        .await
        .expect("writer must reach rollback barrier")
        .expect("rollback barrier sender");
    release.send(()).expect("release delayed rollback");
    let rolled_back_rows = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        guard_id_count(&mut admin, 17, &cancellation),
    )
    .await
    .expect("independent session must observe server rollback");
    assert_eq!(rolled_back_rows, 0);
    proxy.blackhole().await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .expect("writer must terminate after rollback response blackhole")
        .expect("writer task")
        .expect_err("lost rollback response must not become success");

    assert_eq!(error.category, ErrorCategory::Execution);
    assert_eq!(error.phase, ErrorPhase::Write);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    assert!(error.execution_id.is_some());
    assert_eq!(guard_id_count(&mut admin, 99, &cancellation).await, 1);
    proxy.cut().await;
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta lo schema guard"]
async fn live_schema_drift_after_prepare_fails_before_mutation() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    admin
        .execute_query(
            Query::new(
                "IF COL_LENGTH(N'plenora_test.write_guard_probe', N'token_probe') IS NOT NULL \
                 ALTER TABLE [plenora_test].[write_guard_probe] DROP COLUMN [token_probe]; \
                 DELETE FROM [plenora_test].[write_guard_probe] WHERE [id] <> 99;",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("normalize drift fixture");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write(
        &pool,
        &write_operation("write_guard_probe", WriteMode::Append),
        Arc::clone(&schema),
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare before DDL");
    admin
        .execute_query(
            Query::new(
                "ALTER TABLE [plenora_test].[write_guard_probe] ADD [token_probe] int NULL;",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("mutate schema");
    let input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([guard_batch(schema, 1, "must-not-commit")]),
    });
    let error = write_prepared(prepared, input, &cancellation)
        .await
        .expect_err("schema drift must fail");
    admin
        .execute_query(
            Query::new("ALTER TABLE [plenora_test].[write_guard_probe] DROP COLUMN [token_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup drift");
    let mut results = admin
        .execute_query(
            Query::new(
                "SELECT COUNT_BIG(*) FROM [plenora_test].[write_guard_probe] WHERE [id] = 1;",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify no mutation");
    let count: Option<i64> = results
        .pop()
        .and_then(|mut rows| rows.pop())
        .expect("count row")
        .try_get(0)
        .expect("count");
    assert_eq!(error.category, ErrorCategory::Schema);
    assert_eq!(count, Some(0));
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta una fixture temporale isolata"]
async fn live_submicrosecond_temporal_values_fail_closed() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    admin
        .execute_query(
            Query::new(
                "IF OBJECT_ID(N'plenora_test.temporal_precision_probe', N'U') IS NOT NULL \
                 DROP TABLE [plenora_test].[temporal_precision_probe]; \
                 CREATE TABLE [plenora_test].[temporal_precision_probe] \
                 ([id] int NOT NULL, [clock] time(7) NOT NULL, \
                  [local_time] datetime2(7) NOT NULL, \
                  [offset_time] datetimeoffset(7) NOT NULL); \
                 INSERT INTO [plenora_test].[temporal_precision_probe] VALUES \
                 (1, '01:02:03.1234567', '2026-01-01T01:02:03.1234567', \
                  '2026-01-01T01:02:03.1234567+01:00');",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("temporal fixture");
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut stream = read_object(
        &pool,
        "plenora_test",
        "temporal_precision_probe",
        1,
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare temporal read");
    let error = stream
        .next_batch()
        .await
        .expect_err("100 ns precision must not be truncated");
    drop(stream);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[temporal_precision_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup temporal fixture");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}

fn write_operation(object: &str, mode: WriteMode) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
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

fn guard_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

async fn normalize_guard_fixture(admin: &mut SqlServerSession, cancellation: &CancellationToken) {
    admin
        .execute_query(
            Query::new(
                "DELETE FROM [plenora_test].[write_guard_probe] WHERE [id] <> 99; \
                 IF NOT EXISTS \
                    (SELECT 1 FROM [plenora_test].[write_guard_probe] WHERE [id] = 99) \
                 INSERT INTO [plenora_test].[write_guard_probe] ([id], [label]) \
                 VALUES (99, N'sentinel');",
            ),
            ErrorPhase::Write,
            cancellation,
        )
        .await
        .expect("normalize guard fixture");
}

async fn guard_id_count(
    admin: &mut SqlServerSession,
    id: i32,
    cancellation: &CancellationToken,
) -> i64 {
    let mut query =
        Query::new("SELECT COUNT_BIG(*) FROM [plenora_test].[write_guard_probe] WHERE [id] = @P1;");
    query.bind(id);
    let mut results = admin
        .execute_query(query, ErrorPhase::Probe, cancellation)
        .await
        .expect("guard count");
    results
        .pop()
        .and_then(|mut rows| rows.pop())
        .expect("guard count row")
        .try_get::<i64, _>(0)
        .expect("guard count type")
        .expect("guard count value")
}

async fn wait_for_application_request(
    admin: &mut SqlServerSession,
    application_name: &str,
    cancellation: &CancellationToken,
) {
    for _ in 0..100 {
        let mut query = Query::new(
            "SELECT COUNT_BIG(*) \
             FROM sys.dm_exec_requests AS r \
             INNER JOIN sys.dm_exec_sessions AS s ON s.session_id = r.session_id \
             WHERE s.program_name = @P1 AND r.session_id <> @@SPID;",
        );
        query.bind(application_name.to_owned());
        let mut results = admin
            .execute_query(query, ErrorPhase::Probe, cancellation)
            .await
            .expect("observe active SQL Server request");
        let count = results
            .pop()
            .and_then(|mut rows| rows.pop())
            .expect("active request count row")
            .try_get::<i64, _>(0)
            .expect("active request count type")
            .expect("active request count value");
        if count > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("SQL Server request did not reach the observed execution phase");
}

fn guard_batch(schema: SchemaRef, id: i32, label: &str) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![id])),
            Arc::new(StringArray::from(vec![label])),
        ],
    )
    .expect("guard batch")
}

fn differential_batches(schema: &SchemaRef) -> VecDeque<RecordBatch> {
    (0_i32..4)
        .map(|batch_index| {
            let start = batch_index * 25;
            let ids = (start..start + 25).collect::<Vec<_>>();
            let labels = ids
                .iter()
                .map(|id| format!("bulk-row-{id:03}"))
                .collect::<Vec<_>>();
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(labels)),
                ],
            )
            .expect("differential batch")
        })
        .collect()
}

struct VecBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl BatchStream for VecBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }
}

struct BarrierBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    emitted: usize,
    barrier_reached: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

impl BatchStream for BarrierBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
        Box::pin(async move {
            if self.emitted == 1 {
                if let Some(barrier_reached) = self.barrier_reached.take() {
                    barrier_reached
                        .send(())
                        .expect("physical fault barrier receiver");
                }
                if let Some(release) = self.release.take() {
                    release.await.expect("physical fault barrier release");
                }
            }
            let batch = self.batches.pop_front();
            if batch.is_some() {
                self.emitted = self
                    .emitted
                    .checked_add(1)
                    .expect("test batch counter overflow");
            }
            Ok(batch)
        })
    }
}
