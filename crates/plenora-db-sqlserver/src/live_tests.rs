use crate::{
    describe_object, list_objects, list_schemas, prepare_write, prepare_write_with_mode,
    probe_server, read_object, write_prepared, CertificatePolicy, SqlServerColumnKind,
    SqlServerColumnSpec, SqlServerConfig, SqlServerGraphKind, SqlServerInsertMode, SqlServerPool,
    SqlServerProvider, SqlServerSchemaEvolution, SqlServerSession, SqlServerWireEncoding,
};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use plenora_database_core::arrow::{DataType, Field, RecordBatch, Schema, SchemaRef};
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::loss::{LossSeverity, MappingPolicy};
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
    ColumnRef, CommonTableExpression, JoinKind, QueryDerivedSource, QueryExpression, QueryJoin,
    QueryLock, QueryLockStrength, QueryLockWait, QueryOperation, QueryOrdering, QueryProjection,
    QuerySetOperation, QuerySetOperator, QuerySource, ScalarFunction, SpatialFunction,
};
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, RemoteEffect, ResourceBudget, ResourceLimits,
    RetryDisposition,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
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

fn private_ca_live_config(host: String) -> SqlServerConfig {
    let database =
        std::env::var("PLENORA_SQLSERVER_DATABASE").unwrap_or_else(|_| "dataflow_test".to_owned());
    let username =
        std::env::var("PLENORA_SQLSERVER_USER").unwrap_or_else(|_| "dataflow".to_owned());
    let password = std::env::var("PLENORA_SQLSERVER_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    let ca = std::env::var_os("PLENORA_SQLSERVER_PRIVATE_CA")
        .expect("PLENORA_SQLSERVER_PRIVATE_CA required for the private CA campaign");
    SqlServerConfig::new(host, database, username, SecretString::new(password))
        .with_private_ca_certificate(ca)
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
                                mode_applied.notify_waiters();
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

    async fn forward(&self) {
        self.mode
            .send(ProxyMode::Forward)
            .expect("restore proxied TDS forwarding");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let applied = self.mode_applied.notified();
                if self.blackholed_connections.load(Ordering::Acquire) == 0 {
                    break;
                }
                applied.await;
            }
        })
        .await
        .expect("all proxied TDS connections must leave blackhole");
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
    let expected_major =
        std::env::var("PLENORA_SQLSERVER_EXPECTED_MAJOR").unwrap_or_else(|_| "16".to_owned());
    let expected_compatibility = std::env::var("PLENORA_SQLSERVER_EXPECTED_COMPATIBILITY")
        .ok()
        .and_then(|value| value.parse::<u8>().ok())
        .unwrap_or(160);
    assert!(probe
        .product_version
        .starts_with(&format!("{expected_major}.")));
    assert_eq!(probe.compatibility_level, expected_compatibility);
    assert!(probe.geometry_type_id.is_some());
    assert!(probe.geography_type_id.is_some());
    assert!(!probe.polybase_installed);

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
#[ignore = "richiede istanza PolyBase e fixture plenora_test.external_probe"]
async fn polybase_external_catalog_is_structural_and_not_implicit() {
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(
        &live_config(CertificatePolicy::TrustServerCertificate),
        &cancellation,
    )
    .await
    .expect("open PolyBase SQL Server");
    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe PolyBase server");
    assert!(
        probe.polybase_installed,
        "il gate PolyBase non accetta un server privo della feature"
    );
    let objects = list_objects(&mut session, Some("plenora_test"), &cancellation)
        .await
        .expect("list external fixture");
    assert!(objects
        .iter()
        .any(|object| { object.name == "external_probe" && object.kind == "EXTERNAL_TABLE" }));
    let description = describe_object(
        &mut session,
        "plenora_test",
        "external_probe",
        &cancellation,
    )
    .await
    .expect("describe external fixture");
    assert_eq!(description.kind, "EXTERNAL_TABLE");
    let external = description.external.expect("external metadata");
    assert!(!external.data_source.is_empty());
    assert!(!external.location.is_empty());
    assert_eq!(description.token.structural_fingerprint.len(), 64);
}

#[tokio::test]
#[ignore = "richiede credenziali Azure SQL e TLS pubblico verificabile"]
async fn azure_sql_probe_uses_verified_tls_and_native_spatial_types() {
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(
        &live_config(CertificatePolicy::Verify),
        &cancellation,
    )
    .await
    .expect("open Azure SQL with verified TLS");
    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe Azure SQL");
    assert_eq!(probe.engine_edition, 5, "il gate richiede Azure SQL Database");
    assert!(probe.geometry_type_id.is_some());
    assert!(probe.geography_type_id.is_some());
    let schemas = list_schemas(&mut session, &cancellation)
        .await
        .expect("list Azure SQL schemas");
    assert!(!schemas.is_empty());
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

    let spatial_query = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: bounded_operation.source.clone(),
            alias: Some("source".to_owned()),
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![
                column("shape"),
                QueryExpression::Parameter {
                    name: "needle".to_owned(),
                },
            ],
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
    let spatial_parameters = |srid, semantics| {
        ParameterBag::new(BTreeMap::from([(
            "needle".to_owned(),
            ParameterValue::Wkb {
                bytes: ewkb_point(1, &[3.0, 3.0]),
                srid: Some(srid),
                dimensions: Dimensions::Xy,
                semantics,
            },
        )]))
    };
    let spatial_budget = ResourceBudget::new(ResourceLimits::default()).expect("spatial budget");
    let mut spatial_stream = provider
        .query(
            &secret,
            &spatial_query,
            &spatial_parameters(4326, SpatialSemantics::Geometry),
            &spatial_budget,
            &cancellation,
        )
        .await
        .expect("typed spatial QueryOperation");
    let spatial_batch = spatial_stream
        .next_batch()
        .await
        .expect("spatial query batch")
        .expect("spatial query row");
    let spatial_ids = spatial_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("spatial query ids");
    assert_eq!(spatial_ids.values(), &[3]);
    assert!(spatial_stream
        .next_batch()
        .await
        .expect("spatial query end")
        .is_none());
    for invalid in [
        spatial_parameters(3857, SpatialSemantics::Geometry),
        spatial_parameters(4326, SpatialSemantics::Geography),
    ] {
        let invalid_budget =
            ResourceBudget::new(ResourceLimits::default()).expect("invalid spatial budget");
        let Err(error) = provider
            .query(
                &secret,
                &spatial_query,
                &invalid,
                &invalid_budget,
                &cancellation,
            )
            .await
        else {
            panic!("incompatible spatial contract must fail");
        };
        assert_eq!(error.category, ErrorCategory::DataMapping);
    }

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
#[ignore = "richiede SQL Server live esplicito per il sottoinsieme spatial nativo"]
#[allow(clippy::too_many_lines)]
async fn live_native_scalar_spatial_methods_cover_geometry_and_geography() {
    let cancellation = CancellationToken::new();
    let provider = SqlServerProvider::new(
        live_config(CertificatePolicy::TrustServerCertificate),
        32,
        2,
    )
    .expect("spatial method provider");
    let secret = live_secret();
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("spatial method capabilities");
    assert_eq!(
        capabilities.spatial.functions,
        crate::query::VERIFIED_SPATIAL_FUNCTIONS
    );

    for (field, semantics, coordinates) in [
        ("shape", SpatialSemantics::Geometry, [3.0, 3.0]),
        ("position", SpatialSemantics::Geography, [13.0, 43.0]),
    ] {
        let column = || QueryExpression::Column {
            column: ColumnRef {
                relation: Some("source".to_owned()),
                field: field.to_owned(),
            },
        };
        let needle = || QueryExpression::Parameter {
            name: "needle".to_owned(),
        };
        let unary = |function| QueryExpression::Spatial {
            function,
            arguments: vec![column()],
        };
        let binary = |function| QueryExpression::Spatial {
            function,
            arguments: vec![column(), needle()],
        };
        let projection = [
            ("geometry_type", unary(SpatialFunction::GeometryType)),
            ("srid", unary(SpatialFunction::Srid)),
            ("dimensions", unary(SpatialFunction::Dimensions)),
            ("npoints", unary(SpatialFunction::NPoints)),
            ("is_empty", unary(SpatialFunction::IsEmpty)),
            ("is_valid", unary(SpatialFunction::IsValid)),
            ("is_closed", unary(SpatialFunction::IsClosed)),
            ("intersects", binary(SpatialFunction::Intersects)),
            ("contains", binary(SpatialFunction::Contains)),
            ("within", binary(SpatialFunction::Within)),
            ("disjoint", binary(SpatialFunction::Disjoint)),
            ("equals", binary(SpatialFunction::Equals)),
            ("distance", binary(SpatialFunction::Distance)),
            ("area", unary(SpatialFunction::Area)),
            ("length", unary(SpatialFunction::Length)),
        ]
        .into_iter()
        .map(|(alias, expression)| QueryProjection {
            expression,
            alias: Some(alias.to_owned()),
        })
        .collect();
        let operation = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("plenora_test".to_owned()),
                    object: "stream_probe".to_owned(),
                    layer_id: None,
                },
                alias: Some("source".to_owned()),
            }),
            derived_source: None,
            projection,
            joins: Vec::new(),
            filter: Some(binary(SpatialFunction::Equals)),
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
        let parameters = ParameterBag::new(BTreeMap::from([(
            "needle".to_owned(),
            ParameterValue::Wkb {
                bytes: ewkb_point(1, &coordinates),
                srid: Some(4_326),
                dimensions: Dimensions::Xy,
                semantics,
            },
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("spatial method budget");
        let mut stream = provider
            .query(&secret, &operation, &parameters, &budget, &cancellation)
            .await
            .expect("native scalar spatial query");
        let batch = stream
            .next_batch()
            .await
            .expect("spatial method batch")
            .expect("spatial method row");
        assert_eq!(batch.num_rows(), 1);
        let text = batch
            .column_by_name("geometry_type")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .expect("geometry type");
        assert_eq!(text.value(0), "Point");
        for name in ["srid", "dimensions", "npoints"] {
            let values = batch
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
                .expect("integer spatial result");
            let expected = match name {
                "srid" => 4_326,
                "dimensions" => 0,
                _ => 1,
            };
            assert_eq!(values.value(0), expected);
        }
        for (name, expected) in [
            ("is_empty", false),
            ("is_valid", true),
            ("intersects", true),
            ("contains", true),
            ("within", true),
            ("disjoint", false),
            ("equals", true),
        ] {
            let values = batch
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
                .expect("boolean spatial result");
            assert_eq!(values.value(0), expected, "{field}.{name}");
        }
        assert!(batch
            .column_by_name("is_closed")
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .is_some());
        for name in ["distance", "area", "length"] {
            let values = batch
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<Float64Array>())
                .expect("numeric spatial result");
            assert!(values.value(0).abs() <= f64::EPSILON, "{field}.{name}");
        }
        assert!(stream
            .next_batch()
            .await
            .expect("spatial method end")
            .is_none());
    }
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito per output spatial WKB Z/M"]
#[allow(clippy::too_many_lines)]
async fn live_native_spatial_outputs_preserve_contract_and_zm() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("spatial output admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[spatial_output_probe]; \
                 CREATE TABLE [plenora_test].[spatial_output_probe] \
                 ([id] int NOT NULL PRIMARY KEY, [shape] geometry NULL, [position] geography NULL); \
                 INSERT INTO [plenora_test].[spatial_output_probe] VALUES \
                 (1, geometry::STGeomFromText('LINESTRING (1 2 3 4, 5 6 7 8)', 4326), \
                     geography::STGeomFromText('LINESTRING (13 43 3 4, 14 44 7 8)', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("spatial output fixture");
    let provider = SqlServerProvider::new(config, 8, 1).expect("spatial output provider");
    let secret = live_secret();

    for (field, semantics, start, end) in [
        (
            "shape",
            "geometry",
            [1.0_f64, 2.0, 3.0, 4.0],
            [5.0_f64, 6.0, 7.0, 8.0],
        ),
        (
            "position",
            "geography",
            [13.0_f64, 43.0, 3.0, 4.0],
            [14.0_f64, 44.0, 7.0, 8.0],
        ),
    ] {
        let spatial = |function| QueryExpression::Spatial {
            function,
            arguments: vec![QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("source".to_owned()),
                    field: field.to_owned(),
                },
            }],
        };
        let operation = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("plenora_test".to_owned()),
                    object: "spatial_output_probe".to_owned(),
                    layer_id: None,
                },
                alias: Some("source".to_owned()),
            }),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: spatial(SpatialFunction::StartPoint),
                    alias: Some("start_point".to_owned()),
                },
                QueryProjection {
                    expression: spatial(SpatialFunction::EndPoint),
                    alias: Some("end_point".to_owned()),
                },
                QueryProjection {
                    expression: QueryExpression::Spatial {
                        function: SpatialFunction::PointN,
                        arguments: vec![
                            QueryExpression::Column {
                                column: ColumnRef {
                                    relation: Some("source".to_owned()),
                                    field: field.to_owned(),
                                },
                            },
                            QueryExpression::Parameter {
                                name: "point_index".to_owned(),
                            },
                        ],
                    },
                    alias: Some("point_n".to_owned()),
                },
            ],
            joins: Vec::new(),
            filter: None,
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
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("spatial output budget");
        let mut stream = provider
            .query(
                &secret,
                &operation,
                &ParameterBag::new(BTreeMap::from([(
                    "point_index".to_owned(),
                    ParameterValue::I32(2),
                )])),
                &budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{semantics} spatial output: {error}"));
        let output_schema = stream.schema();
        for name in ["start_point", "end_point", "point_n"] {
            let metadata = output_schema
                .field_with_name(name)
                .expect("spatial output field")
                .metadata();
            assert_eq!(metadata[protocol::GEOMETRY_DIMENSIONS], "xyzm");
            assert_eq!(metadata[protocol::GEOMETRY_SPATIAL_SEMANTICS], semantics);
            assert_eq!(metadata[protocol::GEOMETRY_SRID], "4326");
        }
        let batch = stream
            .next_batch()
            .await
            .expect("spatial output batch")
            .expect("spatial output row");
        for (name, coordinates) in [("start_point", start), ("end_point", end), ("point_n", end)] {
            let values = batch
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                .expect("spatial output WKB");
            assert_eq!(values.value(0), ewkb_point(3_001, &coordinates));
        }
        assert!(stream
            .next_batch()
            .await
            .expect("spatial output end")
            .is_none());
    }
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[spatial_output_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup spatial output fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito per processing spatial geometry/geography"]
#[allow(clippy::too_many_lines)]
async fn live_native_spatial_processing_covers_geometry_and_geography() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("spatial processing admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[spatial_processing_probe]; \
                 CREATE TABLE [plenora_test].[spatial_processing_probe] \
                 ([id] int NOT NULL PRIMARY KEY, [shape] geometry NULL, [position] geography NULL); \
                 INSERT INTO [plenora_test].[spatial_processing_probe] VALUES \
                 (1, geometry::STGeomFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))', 4326), \
                     geography::STGeomFromText( \
                       'POLYGON ((-122.358 47.653, -122.348 47.649, -122.348 47.658, \
                                  -122.358 47.658, -122.358 47.653))', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("spatial processing fixture");
    let provider = SqlServerProvider::new(config, 16, 1).expect("spatial processing provider");
    let secret = live_secret();

    for (field, semantics, distance, polygon) in [
        (
            "shape",
            SpatialSemantics::Geometry,
            0.25,
            vec![[2.0, 2.0], [2.0, 6.0], [6.0, 6.0], [6.0, 2.0], [2.0, 2.0]],
        ),
        (
            "position",
            SpatialSemantics::Geography,
            100.0,
            vec![
                [-122.351, 47.656],
                [-122.341, 47.656],
                [-122.341, 47.661],
                [-122.351, 47.661],
                [-122.351, 47.656],
            ],
        ),
    ] {
        let semantics_label = match semantics {
            SpatialSemantics::Geometry => "geometry",
            SpatialSemantics::Geography => "geography",
        };
        let column = || QueryExpression::Column {
            column: ColumnRef {
                relation: Some("source".to_owned()),
                field: field.to_owned(),
            },
        };
        let parameter = |name: &str| QueryExpression::Parameter {
            name: name.to_owned(),
        };
        let unary = |function| QueryExpression::Spatial {
            function,
            arguments: vec![column()],
        };
        let geometry_binary = |function| QueryExpression::Spatial {
            function,
            arguments: vec![column(), parameter("needle")],
        };
        let projection = [
            (
                "buffered",
                QueryExpression::Spatial {
                    function: SpatialFunction::Buffer,
                    arguments: vec![column(), parameter("distance")],
                },
            ),
            (
                "intersection",
                geometry_binary(SpatialFunction::Intersection),
            ),
            ("difference", geometry_binary(SpatialFunction::Difference)),
            (
                "symmetric_difference",
                geometry_binary(SpatialFunction::SymDifference),
            ),
            ("unioned", geometry_binary(SpatialFunction::Union)),
            ("convex_hull", unary(SpatialFunction::ConvexHull)),
        ]
        .into_iter()
        .map(|(alias, expression)| QueryProjection {
            expression,
            alias: Some(alias.to_owned()),
        })
        .collect();
        let operation = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("plenora_test".to_owned()),
                    object: "spatial_processing_probe".to_owned(),
                    layer_id: None,
                },
                alias: Some("source".to_owned()),
            }),
            derived_source: None,
            projection,
            joins: Vec::new(),
            filter: None,
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
        let parameters = ParameterBag::new(BTreeMap::from([
            ("distance".to_owned(), ParameterValue::F64(distance)),
            (
                "needle".to_owned(),
                ParameterValue::Wkb {
                    bytes: wkb_polygon_xy(&polygon),
                    srid: Some(4_326),
                    dimensions: Dimensions::Xy,
                    semantics,
                },
            ),
        ]));
        let budget =
            ResourceBudget::new(ResourceLimits::default()).expect("spatial processing budget");
        let mut stream = provider
            .query(&secret, &operation, &parameters, &budget, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("{semantics:?} spatial processing: {error}"));
        for field in stream.schema().fields() {
            assert_eq!(field.data_type(), &DataType::Binary);
            assert_eq!(field.metadata()[protocol::GEOMETRY_DIMENSIONS], "xy");
            assert_eq!(
                field.metadata()[protocol::GEOMETRY_SPATIAL_SEMANTICS],
                semantics_label
            );
            assert_eq!(field.metadata()[protocol::GEOMETRY_SRID], "4326");
        }
        let batch = stream
            .next_batch()
            .await
            .expect("spatial processing batch")
            .expect("spatial processing row");
        assert_eq!(batch.num_rows(), 1);
        for name in [
            "buffered",
            "intersection",
            "difference",
            "symmetric_difference",
            "unioned",
            "convex_hull",
        ] {
            let values = batch
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                .expect("spatial processing WKB");
            assert!(!values.is_null(0), "{semantics:?}.{name}");
            let inspected =
                plenora_database_core::ewkb::inspect_ewkb_detailed(values.value(0), 1_000_000, 64)
                    .unwrap_or_else(|error| panic!("{semantics:?}.{name}: {error}"));
            assert_eq!(inspected.root.dimensions_label(), "xy");
            assert_eq!(inspected.root.srid, None);
            assert!(
                inspected.root.geometry_type_name().is_some(),
                "{semantics:?}.{name}"
            );
        }
        assert!(stream
            .next_batch()
            .await
            .expect("spatial processing end")
            .is_none());
    }
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[spatial_processing_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup spatial processing fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito per join spatial tra source fisiche"]
#[allow(clippy::too_many_lines)]
async fn live_spatial_join_resolves_columns_and_guards_every_source() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("spatial join admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[spatial_join_right]; \
                 DROP TABLE IF EXISTS [plenora_test].[spatial_join_left]; \
                 CREATE TABLE [plenora_test].[spatial_join_left] \
                 ([id] int NOT NULL PRIMARY KEY, [shape] geometry NULL, [position] geography NULL); \
                 CREATE TABLE [plenora_test].[spatial_join_right] \
                 ([id] int NOT NULL PRIMARY KEY, [shape] geometry NULL, [position] geography NULL); \
                 INSERT INTO [plenora_test].[spatial_join_left] VALUES \
                 (1, geometry::STGeomFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))', 4326), \
                     geography::STGeomFromText( \
                       'POLYGON ((-122.358 47.653, -122.348 47.649, -122.348 47.658, \
                                  -122.358 47.658, -122.358 47.653))', 4326)); \
                 INSERT INTO [plenora_test].[spatial_join_right] VALUES \
                 (2, geometry::STGeomFromText('POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))', 4326), \
                     geography::STGeomFromText( \
                       'POLYGON ((-122.351 47.656, -122.341 47.656, -122.341 47.661, \
                                  -122.351 47.661, -122.351 47.656))', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("spatial join fixtures");
    let provider = SqlServerProvider::new(config, 16, 1).expect("spatial join provider");
    let secret = live_secret();

    for (field, semantics) in [("shape", "geometry"), ("position", "geography")] {
        let column = |relation: &str| QueryExpression::Column {
            column: ColumnRef {
                relation: Some(relation.to_owned()),
                field: field.to_owned(),
            },
        };
        let intersects = QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![column("left"), column("right")],
        };
        let operation = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("plenora_test".to_owned()),
                    object: "spatial_join_left".to_owned(),
                    layer_id: None,
                },
                alias: Some("left".to_owned()),
            }),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::Intersection,
                    arguments: vec![column("left"), column("right")],
                },
                alias: Some("overlap".to_owned()),
            }],
            joins: vec![QueryJoin {
                kind: JoinKind::Inner,
                source: Some(QuerySource {
                    object: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_test".to_owned()),
                        object: "spatial_join_right".to_owned(),
                        layer_id: None,
                    },
                    alias: Some("right".to_owned()),
                }),
                derived_source: None,
                lateral: false,
                on: Some(intersects),
            }],
            filter: None,
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
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("spatial join budget");
        let mut stream = provider
            .query(
                &secret,
                &operation,
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{semantics} spatial join: {error}"));
        let output_field = stream
            .schema()
            .field_with_name("overlap")
            .expect("spatial join output")
            .clone();
        assert_eq!(output_field.data_type(), &DataType::Binary);
        assert_eq!(
            output_field.metadata()[protocol::GEOMETRY_SPATIAL_SEMANTICS],
            semantics
        );
        assert_eq!(output_field.metadata()[protocol::GEOMETRY_SRID], "4326");
        let batch = stream
            .next_batch()
            .await
            .expect("spatial join batch")
            .expect("spatial join row");
        let values = batch
            .column_by_name("overlap")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .expect("spatial join WKB");
        assert!(!values.is_null(0));
        let inspected =
            plenora_database_core::ewkb::inspect_ewkb_detailed(values.value(0), 1_000_000, 64)
                .expect("spatial join output inspection");
        assert_eq!(inspected.root.geometry_type_name(), Some("Polygon"));
        assert!(stream
            .next_batch()
            .await
            .expect("spatial join end")
            .is_none());
    }

    admin
        .execute_query(
            Query::new(
                "DROP TABLE [plenora_test].[spatial_join_right]; \
                 DROP TABLE [plenora_test].[spatial_join_left];",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup spatial join fixtures");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito per scope spatial CTE, derived e subquery"]
#[allow(clippy::too_many_lines)]
async fn live_spatial_cte_derived_and_subquery_preserve_native_contract() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("spatial scope admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[spatial_scope_probe]; \
                 CREATE TABLE [plenora_test].[spatial_scope_probe] \
                 ([id] int NOT NULL PRIMARY KEY, [shape] geometry NULL, [position] geography NULL); \
                 INSERT INTO [plenora_test].[spatial_scope_probe] VALUES \
                 (1, geometry::STGeomFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))', 4326), \
                     geography::STGeomFromText( \
                       'POLYGON ((-122.358 47.653, -122.348 47.649, -122.348 47.658, \
                                  -122.358 47.658, -122.358 47.653))', 4326)), \
                 (2, geometry::STGeomFromText('POINT (2 2)', 4326), \
                     geography::STGeomFromText('POINT (-122.35 47.65)', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("spatial scope fixture");
    let provider = SqlServerProvider::new(config, 16, 1).expect("spatial scope provider");
    let secret = live_secret();
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("mixed spatial capabilities");
    assert!(capabilities.spatial.geometry);
    assert!(capabilities.spatial.geography);
    assert!(capabilities.spatial.mixed_geometry_types);
    let physical_source = || QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: "spatial_scope_probe".to_owned(),
            layer_id: None,
        },
        alias: Some("base".to_owned()),
    };
    let virtual_source = |name: &str, alias: &str| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: None,
            object: name.to_owned(),
            layer_id: None,
        },
        alias: Some(alias.to_owned()),
    };
    let empty_operation = |source, derived_source, projection| QueryOperation {
        common_table_expressions: Vec::new(),
        source,
        derived_source,
        projection,
        joins: Vec::new(),
        filter: None,
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

    for (field, semantics) in [("shape", "geometry"), ("position", "geography")] {
        let base_column = QueryExpression::Column {
            column: ColumnRef {
                relation: Some("base".to_owned()),
                field: field.to_owned(),
            },
        };
        let inner = empty_operation(
            Some(physical_source()),
            None,
            vec![QueryProjection {
                expression: base_column,
                alias: Some("spatial_value".to_owned()),
            }],
        );
        let scoped_column = || QueryExpression::Column {
            column: ColumnRef {
                relation: Some("scope".to_owned()),
                field: "spatial_value".to_owned(),
            },
        };
        let output_projection = || {
            vec![QueryProjection {
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::ConvexHull,
                    arguments: vec![scoped_column()],
                },
                alias: Some("spatial_result".to_owned()),
            }]
        };
        let mut cte_operation = empty_operation(
            Some(virtual_source("spatial_scope", "scope")),
            None,
            output_projection(),
        );
        cte_operation
            .common_table_expressions
            .push(CommonTableExpression {
                name: "spatial_scope".to_owned(),
                recursive: false,
                query: Box::new(inner.clone()),
            });
        let derived_operation = empty_operation(
            None,
            Some(QueryDerivedSource {
                query: Box::new(inner.clone()),
                alias: "scope".to_owned(),
            }),
            output_projection(),
        );

        for (scope_kind, operation) in [
            ("cte", cte_operation.clone()),
            ("derived", derived_operation),
        ] {
            let budget =
                ResourceBudget::new(ResourceLimits::default()).expect("spatial scope budget");
            let mut stream = provider
                .query(
                    &secret,
                    &operation,
                    &ParameterBag::default(),
                    &budget,
                    &cancellation,
                )
                .await
                .unwrap_or_else(|error| panic!("{semantics}.{scope_kind}: {error}"));
            let output = stream
                .schema()
                .field_with_name("spatial_result")
                .expect("scoped spatial output")
                .clone();
            assert_eq!(output.data_type(), &DataType::Binary);
            assert_eq!(
                output.metadata()[protocol::GEOMETRY_SPATIAL_SEMANTICS],
                semantics
            );
            assert_eq!(output.metadata()[protocol::GEOMETRY_SRID], "4326");
            let batch = stream
                .next_batch()
                .await
                .expect("spatial scope batch")
                .expect("spatial scope row");
            let values = batch
                .column_by_name("spatial_result")
                .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                .expect("scoped spatial WKB");
            assert_eq!(values.len(), 2);
            let mut observed_types = std::collections::BTreeSet::new();
            for row in 0..values.len() {
                assert!(!values.is_null(row));
                let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(
                    values.value(row),
                    1_000_000,
                    64,
                )
                .expect("mixed scoped spatial inspection");
                observed_types.insert(
                    inspection
                        .root
                        .geometry_type_name()
                        .expect("mixed scoped geometry type"),
                );
            }
            assert_eq!(
                observed_types,
                std::collections::BTreeSet::from(["Point", "Polygon"])
            );
            assert!(stream
                .next_batch()
                .await
                .expect("spatial scope end")
                .is_none());
        }

        let mut mismatched = cte_operation;
        mismatched.filter = Some(QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![
                scoped_column(),
                QueryExpression::Parameter {
                    name: "wrong_srid".to_owned(),
                },
            ],
        });
        let parameter_semantics = if semantics == "geometry" {
            SpatialSemantics::Geometry
        } else {
            SpatialSemantics::Geography
        };
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wrong_srid".to_owned(),
            ParameterValue::Wkb {
                bytes: wkb_polygon_xy(&[
                    [0.0, 0.0],
                    [0.0, 1.0],
                    [1.0, 1.0],
                    [1.0, 0.0],
                    [0.0, 0.0],
                ]),
                srid: Some(3_857),
                dimensions: Dimensions::Xy,
                semantics: parameter_semantics,
            },
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("mismatch budget");
        let Err(error) = provider
            .query(&secret, &mismatched, &parameters, &budget, &cancellation)
            .await
        else {
            panic!("{semantics} CTE accepted mismatched SRID");
        };
        assert_eq!(error.category, ErrorCategory::DataMapping);

        let mut scalar_inner = inner;
        scalar_inner.projection = vec![QueryProjection {
            expression: QueryExpression::Spatial {
                function: SpatialFunction::Area,
                arguments: vec![QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("base".to_owned()),
                        field: field.to_owned(),
                    },
                }],
            },
            alias: Some("area".to_owned()),
        }];
        scalar_inner.row_limit = Some(1);
        let subquery_operation = empty_operation(
            Some(physical_source()),
            None,
            vec![QueryProjection {
                expression: QueryExpression::ScalarSubquery {
                    query: Box::new(scalar_inner),
                },
                alias: Some("spatial_area".to_owned()),
            }],
        );
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("subquery budget");
        let mut stream = provider
            .query(
                &secret,
                &subquery_operation,
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{semantics}.subquery: {error}"));
        let batch = stream
            .next_batch()
            .await
            .expect("spatial subquery batch")
            .expect("spatial subquery row");
        let values = batch
            .column_by_name("spatial_area")
            .and_then(|array| array.as_any().downcast_ref::<Float64Array>())
            .expect("spatial subquery float");
        assert!(values.value(0) > 0.0);
    }

    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[spatial_scope_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup spatial scope fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live per scope spatial ricorsivi, set e APPLY"]
#[allow(clippy::too_many_lines)]
async fn live_spatial_recursive_nested_set_and_cross_apply_are_server_profiled() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("spatial advanced scope admin");
    admin
        .execute_query(
            Query::new(
                r"
DROP TABLE IF EXISTS [plenora_test].[spatial_advanced_scope];
CREATE TABLE [plenora_test].[spatial_advanced_scope]
(
    [id] int NOT NULL PRIMARY KEY,
    [shape] geometry NOT NULL,
    [position] geography NOT NULL
);
INSERT INTO [plenora_test].[spatial_advanced_scope] VALUES
(1, geometry::STGeomFromText('POINT (1 1)', 4326),
    geography::STGeomFromText('POINT (13 43)', 4326)),
(2, geometry::STGeomFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))', 4326),
    geography::STGeomFromText(
      'POLYGON ((12.9 42.9, 13.1 42.9, 13.1 43.1, 12.9 43.1, 12.9 42.9))', 4326));
",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("spatial advanced scope fixture");
    let provider = SqlServerProvider::new(config, 16, 1).expect("advanced scope provider");
    let secret = live_secret();
    let source = |alias: &str| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: "spatial_advanced_scope".to_owned(),
            layer_id: None,
        },
        alias: Some(alias.to_owned()),
    };
    let virtual_source = |name: &str, alias: &str| QuerySource {
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
    let equals_id = |relation: &str, parameter_name: &str| QueryExpression::Compare {
        left: Box::new(column(relation, "id")),
        operator: plenora_database_core::plan::ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: parameter_name.to_owned(),
        }),
    };
    let operation = |source, projection| QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source),
        derived_source: None,
        projection,
        joins: Vec::new(),
        filter: None,
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

    for (field, semantics) in [("shape", "geometry"), ("position", "geography")] {
        let spatial_projection = |relation: &str, alias: &str| QueryProjection {
            expression: QueryExpression::Spatial {
                function: SpatialFunction::ConvexHull,
                arguments: vec![column(relation, field)],
            },
            alias: Some(alias.to_owned()),
        };
        let mut set_left = operation(
            source("set_left"),
            vec![spatial_projection("set_left", "spatial_result")],
        );
        set_left.filter = Some(equals_id("set_left", "left_id"));
        let mut set_right = operation(
            source("set_right"),
            vec![spatial_projection("set_right", "spatial_result")],
        );
        set_right.filter = Some(equals_id("set_right", "right_id"));
        set_left.set_operations.push(QuerySetOperation {
            operator: QuerySetOperator::Union,
            all: true,
            query: Box::new(set_right),
        });
        let parameters = ParameterBag::new(BTreeMap::from([
            ("left_id".to_owned(), ParameterValue::I32(1)),
            ("right_id".to_owned(), ParameterValue::I32(2)),
        ]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("set spatial budget");
        let mut stream = provider
            .query(&secret, &set_left, &parameters, &budget, &cancellation)
            .await
            .unwrap_or_else(|error| panic!("{semantics}.set: {error}"));
        assert_eq!(
            stream.schema().field(0).metadata()[protocol::GEOMETRY_SPATIAL_SEMANTICS],
            semantics
        );
        let batch = stream
            .next_batch()
            .await
            .expect("set spatial batch")
            .expect("set spatial rows");
        assert_eq!(batch.num_rows(), 2);

        let mut lateral_inner = operation(
            source("inside"),
            vec![QueryProjection {
                expression: column("inside", field),
                alias: Some("spatial_value".to_owned()),
            }],
        );
        lateral_inner.filter = Some(QueryExpression::Compare {
            left: Box::new(column("inside", "id")),
            operator: plenora_database_core::plan::ComparisonOperator::Eq,
            right: Box::new(column("outside", "id")),
        });
        lateral_inner.row_limit = Some(1);
        let mut apply = operation(
            source("outside"),
            vec![QueryProjection {
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::ConvexHull,
                    arguments: vec![column("latest", "spatial_value")],
                },
                alias: Some("spatial_result".to_owned()),
            }],
        );
        apply.joins.push(QueryJoin {
            kind: JoinKind::Cross,
            source: None,
            derived_source: Some(QueryDerivedSource {
                query: Box::new(lateral_inner),
                alias: "latest".to_owned(),
            }),
            lateral: true,
            on: None,
        });
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("APPLY spatial budget");
        let mut stream = provider
            .query(
                &secret,
                &apply,
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{semantics}.cross_apply: {error}"));
        assert_eq!(
            stream.schema().field(0).metadata()[protocol::GEOMETRY_SPATIAL_SEMANTICS],
            semantics
        );
        let batch = stream
            .next_batch()
            .await
            .expect("APPLY spatial batch")
            .expect("APPLY spatial rows");
        assert_eq!(batch.num_rows(), 2);

        let nested_base = operation(
            source("nested_base"),
            vec![QueryProjection {
                expression: column("nested_base", field),
                alias: Some("spatial_value".to_owned()),
            }],
        );
        let mut nested_cte = operation(
            virtual_source("nested_values", "nested"),
            vec![QueryProjection {
                expression: column("nested", "spatial_value"),
                alias: Some("spatial_value".to_owned()),
            }],
        );
        nested_cte
            .common_table_expressions
            .push(CommonTableExpression {
                name: "nested_values".to_owned(),
                recursive: false,
                query: Box::new(nested_base),
            });
        let nested_outer = QueryOperation {
            common_table_expressions: Vec::new(),
            source: None,
            derived_source: Some(QueryDerivedSource {
                query: Box::new(nested_cte),
                alias: "scope".to_owned(),
            }),
            projection: vec![QueryProjection {
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::ConvexHull,
                    arguments: vec![column("scope", "spatial_value")],
                },
                alias: Some("spatial_result".to_owned()),
            }],
            joins: Vec::new(),
            filter: None,
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
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("nested CTE budget");
        let Err(nested_error) = provider
            .query(
                &secret,
                &nested_outer,
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await
        else {
            panic!("{semantics}.nested_cte must be rejected by SQL Server 2022");
        };
        assert_eq!(nested_error.category, ErrorCategory::Unsupported);

        let mut correlated_inner = operation(
            source("correlated_inside"),
            vec![QueryProjection {
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::Area,
                    arguments: vec![column("correlated_inside", field)],
                },
                alias: Some("area".to_owned()),
            }],
        );
        correlated_inner.filter = Some(QueryExpression::Compare {
            left: Box::new(column("correlated_inside", "id")),
            operator: plenora_database_core::plan::ComparisonOperator::Eq,
            right: Box::new(column("correlated_outside", "id")),
        });
        correlated_inner.row_limit = Some(1);
        let correlated = operation(
            source("correlated_outside"),
            vec![QueryProjection {
                expression: QueryExpression::ScalarSubquery {
                    query: Box::new(correlated_inner),
                },
                alias: Some("spatial_area".to_owned()),
            }],
        );
        let budget =
            ResourceBudget::new(ResourceLimits::default()).expect("correlated spatial budget");
        let mut stream = provider
            .query(
                &secret,
                &correlated,
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{semantics}.correlated: {error}"));
        let batch = stream
            .next_batch()
            .await
            .expect("correlated spatial batch")
            .expect("correlated spatial rows");
        assert_eq!(batch.num_rows(), 2);
        assert!(batch
            .column_by_name("spatial_area")
            .and_then(|array| array.as_any().downcast_ref::<Float64Array>())
            .is_some());

        let mut anchor = operation(
            source("anchor"),
            vec![
                QueryProjection {
                    expression: column("anchor", "id"),
                    alias: Some("id".to_owned()),
                },
                QueryProjection {
                    expression: column("anchor", field),
                    alias: Some("spatial_value".to_owned()),
                },
            ],
        );
        anchor.filter = Some(equals_id("anchor", "anchor_id"));
        let mut recursive = operation(
            source("next"),
            vec![
                QueryProjection {
                    expression: column("next", "id"),
                    alias: Some("id".to_owned()),
                },
                QueryProjection {
                    expression: column("next", field),
                    alias: Some("spatial_value".to_owned()),
                },
            ],
        );
        recursive.joins.push(QueryJoin {
            kind: JoinKind::Inner,
            source: Some(virtual_source("walk", "previous")),
            derived_source: None,
            lateral: false,
            on: Some(QueryExpression::Compare {
                left: Box::new(column("next", "id")),
                operator: plenora_database_core::plan::ComparisonOperator::Gt,
                right: Box::new(column("previous", "id")),
            }),
        });
        recursive.filter = Some(equals_id("next", "recursive_id"));
        anchor.set_operations.push(QuerySetOperation {
            operator: QuerySetOperator::Union,
            all: true,
            query: Box::new(recursive),
        });
        let mut recursive_query = operation(
            virtual_source("walk", "scope"),
            vec![QueryProjection {
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::ConvexHull,
                    arguments: vec![column("scope", "spatial_value")],
                },
                alias: Some("spatial_result".to_owned()),
            }],
        );
        recursive_query
            .common_table_expressions
            .push(CommonTableExpression {
                name: "walk".to_owned(),
                recursive: true,
                query: Box::new(anchor),
            });
        let parameters = ParameterBag::new(BTreeMap::from([
            ("anchor_id".to_owned(), ParameterValue::I32(1)),
            ("recursive_id".to_owned(), ParameterValue::I32(2)),
        ]));
        let budget =
            ResourceBudget::new(ResourceLimits::default()).expect("recursive spatial budget");
        let mut stream = provider
            .query(
                &secret,
                &recursive_query,
                &parameters,
                &budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{semantics}.recursive: {error}"));
        assert_eq!(
            stream.schema().field(0).metadata()[protocol::GEOMETRY_SPATIAL_SEMANTICS],
            semantics
        );
        let batch = stream
            .next_batch()
            .await
            .expect("recursive spatial batch")
            .expect("recursive spatial rows");
        assert_eq!(batch.num_rows(), 2);
    }
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[spatial_advanced_scope];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup advanced spatial scope fixture");
}

fn locking_spatial_operation() -> QueryOperation {
    QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("plenora_test".to_owned()),
                object: "stream_probe".to_owned(),
                layer_id: None,
            },
            alias: Some("locked".to_owned()),
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: QueryExpression::Spatial {
                function: SpatialFunction::Dimensions,
                arguments: vec![QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("locked".to_owned()),
                        field: "shape".to_owned(),
                    },
                }],
            },
            alias: Some("spatial_dimension".to_owned()),
        }],
        joins: Vec::new(),
        filter: Some(QueryExpression::Compare {
            left: Box::new(QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("locked".to_owned()),
                    field: "id".to_owned(),
                },
            }),
            operator: plenora_database_core::plan::ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "selected_id".to_owned(),
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
        locking: Some(QueryLock {
            strength: QueryLockStrength::Update,
            relations: vec!["locked".to_owned()],
            wait: QueryLockWait::NoWait,
        }),
    }
}

#[tokio::test]
#[ignore = "richiede SQL Server live e contesa deterministica di un row lock"]
async fn live_sqlserver_lock_hints_are_nowait_and_spatial_safe() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut blocker = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("locking blocker");
    blocker
        .begin(&cancellation)
        .await
        .expect("begin locking transaction");
    blocker
        .execute_write_query(
            Query::new(
                "UPDATE [plenora_test].[stream_probe] \
                 SET [label] = [label] WHERE [id] = 1;",
            ),
            &cancellation,
        )
        .await
        .expect("hold exclusive row lock");

    let provider = SqlServerProvider::new(config, 8, 1).expect("locking provider");
    let operation = locking_spatial_operation();
    let parameters = ParameterBag::new(BTreeMap::from([(
        "selected_id".to_owned(),
        ParameterValue::I32(1),
    )]));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("locking budget");
    let error = match provider
        .query(
            &live_secret(),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await
    {
        Err(error) => error,
        Ok(mut stream) => stream
            .next_batch()
            .await
            .expect_err("NOWAIT query unexpectedly acquired a contended row"),
    };
    assert_eq!(error.category, ErrorCategory::Timeout);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(error.remote_effect, RemoteEffect::None);

    blocker
        .rollback(&cancellation)
        .await
        .expect("release update lock");
    let mut stream = provider
        .query(
            &live_secret(),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("locking query after release");
    let batch = stream
        .next_batch()
        .await
        .expect("locking spatial batch")
        .expect("locking spatial row");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("spatial_dimension")
            .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
            .expect("spatial dimension")
            .value(0),
        0
    );
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
    assert!(capabilities.spatial.spatial_index);

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
#[ignore = "richiede la fixture SQL Server live con CA privata"]
async fn live_private_ca_tls_validates_chain_and_hostname() {
    let cancellation = CancellationToken::new();
    let verified_host =
        std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".to_owned());
    let verified = SqlServerSession::open(&private_ca_live_config(verified_host), &cancellation)
        .await
        .expect("private CA chain and matching hostname must be accepted");
    assert!(verified.is_reusable());
    drop(verified);

    let mismatch_host = std::env::var("PLENORA_SQLSERVER_MISMATCH_HOST")
        .unwrap_or_else(|_| "sqlserver-hostname-mismatch".to_owned());
    let error = SqlServerSession::open(&private_ca_live_config(mismatch_host), &cancellation)
        .await
        .expect_err("private CA must not bypass hostname verification");
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
#[ignore = "richiede SQL Server live esplicito e muta il catalogo avanzato"]
#[allow(clippy::too_many_lines)]
async fn live_advanced_catalog_observes_temporal_graph_and_partitioning() {
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(
        &live_config(CertificatePolicy::TrustServerCertificate),
        &cancellation,
    )
    .await
    .expect("open live SQL Server");

    let cleanup = r"
IF OBJECT_ID(N'plenora_test.catalog_temporal', N'U') IS NOT NULL
BEGIN
    IF (SELECT temporal_type FROM sys.tables WHERE object_id = OBJECT_ID(N'plenora_test.catalog_temporal')) = 2
        ALTER TABLE [plenora_test].[catalog_temporal] SET (SYSTEM_VERSIONING = OFF);
    DROP TABLE [plenora_test].[catalog_temporal];
END;
IF OBJECT_ID(N'plenora_test.catalog_temporal_history', N'U') IS NOT NULL
    DROP TABLE [plenora_test].[catalog_temporal_history];
IF OBJECT_ID(N'plenora_test.catalog_edge', N'U') IS NOT NULL
    DROP TABLE [plenora_test].[catalog_edge];
IF OBJECT_ID(N'plenora_test.catalog_node', N'U') IS NOT NULL
    DROP TABLE [plenora_test].[catalog_node];
IF OBJECT_ID(N'plenora_test.catalog_partitioned', N'U') IS NOT NULL
    DROP TABLE [plenora_test].[catalog_partitioned];
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'plenora_catalog_ps')
    DROP PARTITION SCHEME [plenora_catalog_ps];
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'plenora_catalog_pf')
    DROP PARTITION FUNCTION [plenora_catalog_pf];
";
    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("normalize advanced catalog fixture");
    let create = r"
CREATE TABLE [plenora_test].[catalog_temporal]
(
    [id] int NOT NULL PRIMARY KEY,
    [payload] nvarchar(40) NULL,
    [valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL,
    [valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])
)
WITH (SYSTEM_VERSIONING = ON
      (HISTORY_TABLE = [plenora_test].[catalog_temporal_history], DATA_CONSISTENCY_CHECK = ON));
CREATE TABLE [plenora_test].[catalog_node] ([id] int NOT NULL PRIMARY KEY) AS NODE;
CREATE TABLE [plenora_test].[catalog_edge] ([weight] int NULL) AS EDGE;
CREATE PARTITION FUNCTION [plenora_catalog_pf] (int)
AS RANGE RIGHT FOR VALUES (10, 20);
CREATE PARTITION SCHEME [plenora_catalog_ps]
AS PARTITION [plenora_catalog_pf] ALL TO ([PRIMARY]);
CREATE TABLE [plenora_test].[catalog_partitioned]
(
    [id] int NOT NULL,
    [payload] nvarchar(40) NULL
) ON [plenora_catalog_ps] ([id]);
";
    session
        .execute_query(Query::new(create), ErrorPhase::Write, &cancellation)
        .await
        .expect("create advanced catalog fixture");

    let temporal = describe_object(
        &mut session,
        "plenora_test",
        "catalog_temporal",
        &cancellation,
    )
    .await
    .expect("describe temporal table");
    assert_eq!(temporal.temporal_type, 2);
    let temporal_metadata = temporal.temporal.as_ref().expect("temporal metadata");
    assert_eq!(temporal_metadata.kind, "SYSTEM_VERSIONED_TEMPORAL_TABLE");
    let history = temporal_metadata.history.as_ref().expect("history table");
    assert_eq!(history.schema, "plenora_test");
    assert_eq!(history.name, "catalog_temporal_history");
    assert_eq!(
        temporal_metadata.period_start_column.as_deref(),
        Some("valid_from")
    );
    assert_eq!(
        temporal_metadata.period_end_column.as_deref(),
        Some("valid_to")
    );

    let node = describe_object(&mut session, "plenora_test", "catalog_node", &cancellation)
        .await
        .expect("describe graph node");
    let edge = describe_object(&mut session, "plenora_test", "catalog_edge", &cancellation)
        .await
        .expect("describe graph edge");
    assert_eq!(node.graph_kind, Some(SqlServerGraphKind::Node));
    assert_eq!(edge.graph_kind, Some(SqlServerGraphKind::Edge));

    let partitioned = describe_object(
        &mut session,
        "plenora_test",
        "catalog_partitioned",
        &cancellation,
    )
    .await
    .expect("describe partitioned table");
    let partitioning = partitioned.partitioning.expect("partition metadata");
    assert_eq!(partitioning.scheme, "plenora_catalog_ps");
    assert_eq!(partitioning.function, "plenora_catalog_pf");
    assert_eq!(partitioning.partition_column, "id");
    assert!(partitioning.boundary_value_on_right);
    assert_eq!(partitioning.partition_count, 3);

    session
        .execute_query(
            Query::new(
                "ALTER TABLE [plenora_test].[catalog_temporal] SET (SYSTEM_VERSIONING = OFF);",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("disable system versioning");
    let non_versioned = describe_object(
        &mut session,
        "plenora_test",
        "catalog_temporal",
        &cancellation,
    )
    .await
    .expect("describe disabled temporal table");
    assert_eq!(non_versioned.temporal_type, 0);
    assert!(non_versioned.temporal.is_none());
    assert_ne!(
        temporal.token.structural_fingerprint,
        non_versioned.token.structural_fingerprint
    );

    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("cleanup advanced catalog fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta RLS e permessi"]
#[allow(clippy::too_many_lines)]
async fn live_catalog_observes_rls_owner_and_explicit_permissions() {
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(
        &live_config(CertificatePolicy::TrustServerCertificate),
        &cancellation,
    )
    .await
    .expect("open live SQL Server");
    let cleanup = r"
IF EXISTS (
    SELECT 1 FROM sys.security_policies
    WHERE object_id = OBJECT_ID(N'plenora_test.catalog_rls_policy')
)
    DROP SECURITY POLICY [plenora_test].[catalog_rls_policy];
IF OBJECT_ID(N'plenora_test.catalog_rls', N'U') IS NOT NULL
    DROP TABLE [plenora_test].[catalog_rls];
IF OBJECT_ID(N'plenora_test.catalog_rls_filter', N'IF') IS NOT NULL
    DROP FUNCTION [plenora_test].[catalog_rls_filter];
IF USER_ID(N'plenora_catalog_reader') IS NOT NULL
    DROP USER [plenora_catalog_reader];
";
    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("normalize RLS catalog fixture");
    session
        .execute_query(
            Query::new(
                r"
EXEC(N'CREATE FUNCTION [plenora_test].[catalog_rls_filter](@tenant_id int)
RETURNS TABLE WITH SCHEMABINDING
AS RETURN SELECT 1 AS [permitted] WHERE @tenant_id = 7;');
",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create RLS predicate function");
    session
        .execute_query(
            Query::new(
                r"
CREATE TABLE [plenora_test].[catalog_rls]
(
    [id] int NOT NULL PRIMARY KEY,
    [tenant_id] int NOT NULL,
    [payload] nvarchar(40) NULL
);
CREATE USER [plenora_catalog_reader] WITHOUT LOGIN;
",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create RLS target and principal");
    session
        .execute_query(
            Query::new(
                r"
CREATE SECURITY POLICY [plenora_test].[catalog_rls_policy]
ADD FILTER PREDICATE [plenora_test].[catalog_rls_filter]([tenant_id])
ON [plenora_test].[catalog_rls]
WITH (STATE = ON, SCHEMABINDING = ON);
GRANT SELECT ON OBJECT::[plenora_test].[catalog_rls] TO [plenora_catalog_reader];
DENY DELETE ON OBJECT::[plenora_test].[catalog_rls] TO [plenora_catalog_reader];
GRANT UPDATE ON OBJECT::[plenora_test].[catalog_rls] ([payload]) TO [plenora_catalog_reader];
",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create RLS and permission fixture");

    let enabled = describe_object(&mut session, "plenora_test", "catalog_rls", &cancellation)
        .await
        .expect("describe RLS table");
    assert!(!enabled.owner.is_empty());
    assert_eq!(enabled.security_predicates.len(), 1);
    let predicate = &enabled.security_predicates[0];
    assert_eq!(predicate.policy_schema, "plenora_test");
    assert_eq!(predicate.policy_name, "catalog_rls_policy");
    assert!(predicate.policy_enabled);
    assert!(predicate.policy_schema_bound);
    assert_eq!(predicate.kind, "FILTER");
    assert_eq!(predicate.operation, None);
    assert!(predicate
        .predicate_definition
        .contains("catalog_rls_filter"));
    assert!(enabled.permissions.iter().any(|permission| {
        permission.grantee == "plenora_catalog_reader"
            && permission.permission == "SELECT"
            && permission.state == "GRANT"
    }));
    assert!(enabled.permissions.iter().any(|permission| {
        permission.grantee == "plenora_catalog_reader"
            && permission.permission == "DELETE"
            && permission.state == "DENY"
    }));
    assert!(enabled.permissions.iter().any(|permission| {
        permission.grantee == "plenora_catalog_reader"
            && permission.permission == "UPDATE"
            && permission.state == "GRANT"
            && permission.column.as_deref() == Some("payload")
    }));

    session
        .execute_query(
            Query::new(
                "ALTER SECURITY POLICY [plenora_test].[catalog_rls_policy] WITH (STATE = OFF);",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("disable RLS policy");
    let disabled = describe_object(&mut session, "plenora_test", "catalog_rls", &cancellation)
        .await
        .expect("describe disabled RLS table");
    assert!(!disabled.security_predicates[0].policy_enabled);
    assert_ne!(
        enabled.token.structural_fingerprint,
        disabled.token.structural_fingerprint
    );

    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("cleanup RLS catalog fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta una view isolata"]
async fn live_catalog_observes_view_definition_and_session_semantics() {
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(
        &live_config(CertificatePolicy::TrustServerCertificate),
        &cancellation,
    )
    .await
    .expect("open live SQL Server");
    let cleanup = r"
IF OBJECT_ID(N'plenora_test.catalog_view', N'V') IS NOT NULL
    DROP VIEW [plenora_test].[catalog_view];
IF OBJECT_ID(N'plenora_test.catalog_view_source', N'U') IS NOT NULL
    DROP TABLE [plenora_test].[catalog_view_source];
";
    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("normalize view catalog fixture");
    session
        .execute_query(
            Query::new(
                r"
CREATE TABLE [plenora_test].[catalog_view_source]
(
    [id] int NOT NULL PRIMARY KEY,
    [payload] nvarchar(40) NULL
);
",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create view source");
    session
        .execute_query(
            Query::new(
                r"
EXEC(N'CREATE VIEW [plenora_test].[catalog_view]
WITH SCHEMABINDING
AS SELECT [id], [payload]
FROM [plenora_test].[catalog_view_source]
WHERE [id] >= 0;');
",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create schema-bound view");
    let before = describe_object(&mut session, "plenora_test", "catalog_view", &cancellation)
        .await
        .expect("describe schema-bound view");
    assert_eq!(before.kind, "VIEW");
    let view = before.view.as_ref().expect("view metadata");
    assert!(view.schema_bound);
    assert!(view.uses_ansi_nulls);
    assert!(view.uses_quoted_identifier);
    assert!(view.definition.as_deref().is_some_and(|definition| {
        definition.contains("catalog_view_source") && definition.contains("[id] >= 0")
    }));

    session
        .execute_query(
            Query::new(
                r"
EXEC(N'ALTER VIEW [plenora_test].[catalog_view]
WITH SCHEMABINDING
AS SELECT [id], [payload]
FROM [plenora_test].[catalog_view_source]
WHERE [id] > 0;');
",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("alter view without changing its columns");
    let after = describe_object(&mut session, "plenora_test", "catalog_view", &cancellation)
        .await
        .expect("describe altered view");
    assert_ne!(
        before.token.structural_fingerprint,
        after.token.structural_fingerprint
    );
    assert!(after.view.as_ref().is_some_and(|view| {
        view.definition
            .as_deref()
            .is_some_and(|definition| definition.contains("[id] > 0"))
    }));

    session
        .execute_query(Query::new(cleanup), ErrorPhase::Write, &cancellation)
        .await
        .expect("cleanup view catalog fixture");
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
#[allow(clippy::too_many_lines)]
async fn live_spatial_preflight_preserves_z_m_zm_and_rejects_ambiguity() {
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
    [shape] geometry NULL,
    [position] geography NULL
);
";
    admin
        .execute_query(Query::new(normalize), ErrorPhase::Write, &cancellation)
        .await
        .expect("normalize spatial guard");
    admin
        .execute_query(
            Query::new(
                "INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [shape]) VALUES \
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

    for (label, field, dimensions, insert) in [
        (
            "geometry Z",
            "shape",
            "xyz",
            "INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [shape]) VALUES \
             (1, geometry::STGeomFromText('POINT (1 2 3)', 4326));",
        ),
        (
            "geometry M",
            "shape",
            "xym",
            "INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [shape]) VALUES \
             (1, geometry::STGeomFromText('POINT (1 2 NULL 4)', 4326));",
        ),
        (
            "geometry ZM",
            "shape",
            "xyzm",
            "INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [shape]) VALUES \
             (1, geometry::STGeomFromText('POINT (1 2 3 4)', 4326));",
        ),
        (
            "geography Z",
            "position",
            "xyz",
            "INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [position]) VALUES \
             (1, geography::STGeomFromText('POINT (1 2 3)', 4326));",
        ),
        (
            "geography M",
            "position",
            "xym",
            "INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [position]) VALUES \
             (1, geography::STGeomFromText('POINT (1 2 NULL 4)', 4326));",
        ),
        (
            "geography ZM",
            "position",
            "xyzm",
            "INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [position]) VALUES \
             (1, geography::STGeomFromText('POINT (1 2 3 4)', 4326));",
        ),
    ] {
        admin
            .execute_query(
                Query::new(format!(
                    "TRUNCATE TABLE [plenora_test].[spatial_guard_probe]; {insert}"
                )),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{label} fixture: {error}"));
        let budget = ResourceBudget::new(ResourceLimits::default())
            .unwrap_or_else(|error| panic!("{label} budget: {error}"));
        let stream = read_object(
            &pool,
            "plenora_test",
            "spatial_guard_probe",
            2,
            &budget,
            &cancellation,
        )
        .await
        .unwrap_or_else(|error| panic!("{label} read: {error}"));
        assert_eq!(
            stream
                .schema()
                .field_with_name(field)
                .expect("spatial field")
                .metadata()[protocol::GEOMETRY_DIMENSIONS],
            dimensions
        );
    }
    admin
        .execute_query(
            Query::new(
                "TRUNCATE TABLE [plenora_test].[spatial_guard_probe]; \
                 INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [shape]) VALUES \
                 (1, geometry::STGeomFromText('POINT (1 2)', 4326)), \
                 (2, geometry::STGeomFromText('POINT (1 2 3)', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("mixed dimensions fixture");
    let mixed_dimensions = read_object(
        &pool,
        "plenora_test",
        "spatial_guard_probe",
        2,
        &ResourceBudget::new(ResourceLimits::default()).expect("mixed dimensions budget"),
        &cancellation,
    )
    .await;
    assert_eq!(
        mixed_dimensions
            .err()
            .expect("mixed dimensions must fail")
            .category,
        ErrorCategory::DataMapping
    );
    admin
        .execute_query(
            Query::new(
                "TRUNCATE TABLE [plenora_test].[spatial_guard_probe]; \
                 INSERT INTO [plenora_test].[spatial_guard_probe] ([id], [position]) VALUES \
                 (1, geography::STGeomFromText('FULLGLOBE', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("FullGlobe fixture");
    let full_globe = read_object(
        &pool,
        "plenora_test",
        "spatial_guard_probe",
        2,
        &ResourceBudget::new(ResourceLimits::default()).expect("FullGlobe budget"),
        &cancellation,
    )
    .await;
    assert_eq!(
        full_globe.err().expect("FullGlobe must fail").category,
        ErrorCategory::Unsupported
    );
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
#[ignore = "richiede SQL Server live per i tipi curvi geometry/geography"]
async fn live_curved_spatial_types_are_read_as_bounded_wkb() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let pool = SqlServerPool::new(config.clone(), 1).expect("curve pool");
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("curve admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[curve_probe]; \
                 CREATE TABLE [plenora_test].[curve_probe] \
                 ([id] int NOT NULL, [shape] geometry NOT NULL, [position] geography NOT NULL); \
                 INSERT INTO [plenora_test].[curve_probe] VALUES \
                 (1, geometry::STGeomFromText('CIRCULARSTRING (1 0, 0 1, -1 0)', 4326), \
                     geography::STGeomFromText('CIRCULARSTRING (1 0, 0 1, -1 0)', 4326)), \
                 (2, geometry::STGeomFromText('COMPOUNDCURVE (CIRCULARSTRING (1 0, 0 1, -1 0), (-1 0, -2 0))', 4326), \
                     geography::STGeomFromText('COMPOUNDCURVE (CIRCULARSTRING (1 0, 0 1, -1 0), (-1 0, -2 0))', 4326)), \
                 (3, geometry::STGeomFromText('CURVEPOLYGON (CIRCULARSTRING (1 0, 0 1, -1 0, 0 -1, 1 0))', 4326), \
                     geography::STGeomFromText('CURVEPOLYGON (CIRCULARSTRING (1 0, 0 1, -1 0, 0 -1, 1 0))', 4326));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("create curved fixture");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("curve read budget");
    let mut stream = read_object(
        &pool,
        "plenora_test",
        "curve_probe",
        8,
        &budget,
        &cancellation,
    )
    .await
    .expect("read curved fixture");
    let batch = stream
        .next_batch()
        .await
        .expect("curve batch")
        .expect("curve rows");
    for column in ["shape", "position"] {
        let values = batch
            .column_by_name(column)
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .expect("curve WKB column");
        let observed = (0..values.len())
            .map(|row| {
                plenora_database_core::ewkb::inspect_ewkb_detailed(values.value(row), 1_000_000, 64)
                    .expect("bounded curved WKB")
                    .root
                    .geometry_type_name()
                    .expect("known curved WKB type")
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            observed,
            std::collections::BTreeSet::from(["CircularString", "CompoundCurve", "CurvePolygon"])
        );
    }
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[curve_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup curved fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live per il roundtrip write dei tipi curvi"]
async fn live_circular_string_write_is_lossless_for_geometry_and_geography() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let pool = SqlServerPool::new(config.clone(), 1).expect("curve write pool");
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("curve write admin");
    let value = wkb_circular_string_xy(&[(1.0, 0.0), (0.0, 1.0), (-1.0, 0.0)]);
    for semantics in ["geometry", "geography"] {
        admin
            .execute_query(
                Query::new(format!(
                    "DROP TABLE IF EXISTS [plenora_test].[curve_write_probe]; \
                     CREATE TABLE [plenora_test].[curve_write_probe] ([shape] {semantics} NULL);"
                )),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .expect("create curve write target");
        let schema = spatial_write_schema(semantics, "xy");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("curve write budget");
        let prepared = prepare_write(
            &pool,
            &write_operation("curve_write_probe", WriteMode::Append),
            Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare curve write");
        write_prepared(
            prepared,
            Box::new(VecBatchStream {
                schema: Arc::clone(&schema),
                batches: VecDeque::from([spatial_write_batch(Arc::clone(&schema), &value)]),
            }),
            &cancellation,
        )
        .await
        .expect("write curve");
        let rows = admin
            .execute_query(
                Query::new(
                    "SELECT [shape].STGeometryType(), [shape].AsBinaryZM() \
                     FROM [plenora_test].[curve_write_probe];",
                ),
                ErrorPhase::Read,
                &cancellation,
            )
            .await
            .expect("verify curve write");
        assert_eq!(
            rows[0][0].try_get::<&str, _>(0).unwrap(),
            Some("CircularString")
        );
        assert_eq!(
            rows[0][0].try_get::<&[u8], _>(1).unwrap(),
            Some(value.as_slice())
        );
    }
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[curve_write_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup curve write target");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta una fixture isolata"]
#[allow(clippy::too_many_lines)]
async fn live_spatial_write_round_trips_z_m_and_zm_losslessly() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let pool = SqlServerPool::new(config.clone(), 1).expect("pool");
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin session");

    for semantics in ["geometry", "geography"] {
        admin
            .execute_query(
                Query::new(format!(
                    "DROP TABLE IF EXISTS [plenora_test].[spatial_write_guard]; \
                     CREATE TABLE [plenora_test].[spatial_write_guard] \
                     ([shape] {semantics} NULL);"
                )),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("{semantics} target fixture: {error}"));
        for (dimensions, type_code, coordinates) in [
            ("xyz", 1_001_u32, &[1.0_f64, 2.0, 3.0][..]),
            ("xym", 2_001_u32, &[1.0_f64, 2.0, 4.0][..]),
            ("xyzm", 3_001_u32, &[1.0_f64, 2.0, 3.0, 4.0][..]),
        ] {
            admin
                .execute_query(
                    Query::new("TRUNCATE TABLE [plenora_test].[spatial_write_guard];"),
                    ErrorPhase::Write,
                    &cancellation,
                )
                .await
                .expect("truncate dimensional target");
            let schema = spatial_write_schema(semantics, dimensions);
            let budget =
                ResourceBudget::new(ResourceLimits::default()).expect("write guard budget");
            let prepared = prepare_write(
                &pool,
                &write_operation("spatial_write_guard", WriteMode::Append),
                Arc::clone(&schema),
                &budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("prepare {semantics} {dimensions} write guard: {error}")
            });
            let value = ewkb_point(type_code, coordinates);
            let batch = spatial_write_batch(Arc::clone(&schema), &value);
            let outcome = write_prepared(
                prepared,
                Box::new(VecBatchStream {
                    schema: Arc::clone(&schema),
                    batches: VecDeque::from([batch]),
                }),
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("write {semantics} {dimensions}: {error}"));
            assert_eq!(outcome.status, WriteStatus::Committed);

            let existing_budget =
                ResourceBudget::new(ResourceLimits::default()).expect("existing target budget");
            let existing = prepare_write(
                &pool,
                &write_operation("spatial_write_guard", WriteMode::Append),
                Arc::clone(&schema),
                &existing_budget,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("prepare existing {semantics} {dimensions} target: {error}")
            });
            let second = write_prepared(
                existing,
                Box::new(VecBatchStream {
                    schema: Arc::clone(&schema),
                    batches: VecDeque::from([spatial_write_batch(Arc::clone(&schema), &value)]),
                }),
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("append existing {semantics} {dimensions} target: {error}")
            });
            assert_eq!(second.status, WriteStatus::Committed);

            let conflicting_dimensions = if dimensions == "xyz" { "xym" } else { "xy" };
            let conflicting_schema = spatial_write_schema(semantics, conflicting_dimensions);
            let conflicting_budget = ResourceBudget::new(ResourceLimits::default())
                .expect("conflicting dimensions budget");
            let conflict = prepare_write(
                &pool,
                &write_operation("spatial_write_guard", WriteMode::Append),
                conflicting_schema,
                &conflicting_budget,
                &cancellation,
            )
            .await
            .expect_err("existing dimensional target must reject a different profile");
            assert_eq!(conflict.category, ErrorCategory::DataMapping);

            let mut stream = read_object(
                &pool,
                "plenora_test",
                "spatial_write_guard",
                1,
                &ResourceBudget::new(ResourceLimits::default()).expect("read dimensional budget"),
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("read {semantics} {dimensions}: {error}"));
            assert_eq!(
                stream.schema().field(0).metadata()[protocol::GEOMETRY_DIMENSIONS],
                dimensions
            );
            for _ in 0..2 {
                let returned = stream
                    .next_batch()
                    .await
                    .expect("dimensional read batch")
                    .expect("dimensional row");
                let bytes = returned
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("dimensional binary")
                    .value(0);
                assert_eq!(
                    bytes, value,
                    "{semantics} {dimensions} must round-trip byte-for-byte"
                );
            }
            assert!(stream
                .next_batch()
                .await
                .expect("dimensional stream end")
                .is_none());
        }
    }
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[spatial_write_guard];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup spatial write guard");
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
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
#[allow(clippy::too_many_lines)]
async fn live_update_upsert_and_delete_by_keys_are_exact_and_atomic() {
    let cancellation = CancellationToken::new();
    let provider = live_provider();
    let secret = live_secret();
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("mutation capabilities");
    assert!(capabilities.writes.update);
    assert!(capabilities.writes.upsert);
    assert!(capabilities.writes.delete_by_keys);
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[mutation_non_unique_probe]; \
                 DROP TABLE IF EXISTS [plenora_test].[mutation_probe]; \
                 CREATE TABLE [plenora_test].[mutation_probe] \
                 ([id] int NOT NULL PRIMARY KEY, [label] nvarchar(100) NOT NULL, \
                  [score] int NOT NULL, CONSTRAINT [CK_mutation_score] \
                  CHECK ([score] >= 0)); \
                 INSERT INTO [plenora_test].[mutation_probe] VALUES \
                 (1, N'old', 10), (2, N'keep', 20); \
                 CREATE TABLE [plenora_test].[mutation_non_unique_probe] \
                 ([id] int NOT NULL, [label] nvarchar(100) NOT NULL);",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("mutation fixtures");

    let pool = SqlServerPool::new(config, 2).expect("pool");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));
    let batch = |ids: Vec<i32>, labels: Vec<&str>, scores: Vec<i32>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(labels)),
                Arc::new(Int32Array::from(scores)),
            ],
        )
        .expect("mutation batch")
    };

    let mut update = write_operation("mutation_probe", WriteMode::Update);
    update.keys = vec!["id".to_owned()];
    update.update_columns = vec!["label".to_owned(), "score".to_owned()];
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("update budget");
    let prepared = prepare_write(&pool, &update, Arc::clone(&schema), &budget, &cancellation)
        .await
        .expect("prepare update");
    let outcome = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([batch(vec![1, 9], vec!["updated", "missing"], vec![11, 90])]),
        }),
        &cancellation,
    )
    .await
    .expect("execute update");
    assert_eq!(outcome.rows.received, 2);
    assert_eq!(outcome.rows.confirmed, 1);
    assert_eq!(outcome.rows.updated, Some(1));
    assert_eq!(outcome.rows.skipped, 1);

    let mut upsert = write_operation("mutation_probe", WriteMode::Upsert);
    upsert.keys = vec!["id".to_owned()];
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("upsert budget");
    let prepared = prepare_write(&pool, &upsert, Arc::clone(&schema), &budget, &cancellation)
        .await
        .expect("prepare upsert");
    let outcome = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([batch(
                vec![1, 3],
                vec!["upserted", "inserted"],
                vec![12, 30],
            )]),
        }),
        &cancellation,
    )
    .await
    .expect("execute upsert");
    assert_eq!(outcome.rows.confirmed, 2);
    assert_eq!(outcome.rows.inserted, Some(1));
    assert_eq!(outcome.rows.updated, Some(1));

    let delete_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let delete_batch = RecordBatch::try_new(
        Arc::clone(&delete_schema),
        vec![Arc::new(Int32Array::from(vec![2, 9]))],
    )
    .expect("delete batch");
    let mut delete = write_operation("mutation_probe", WriteMode::DeleteByKeys);
    delete.keys = vec!["id".to_owned()];
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("delete budget");
    let prepared = prepare_write(
        &pool,
        &delete,
        Arc::clone(&delete_schema),
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare delete");
    let outcome = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: delete_schema,
            batches: VecDeque::from([delete_batch]),
        }),
        &cancellation,
    )
    .await
    .expect("execute delete");
    assert_eq!(outcome.rows.confirmed, 1);
    assert_eq!(outcome.rows.deleted, Some(1));
    assert_eq!(outcome.rows.skipped, 1);

    let non_unique_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let mut unsafe_update = write_operation("mutation_non_unique_probe", WriteMode::Update);
    unsafe_update.keys = vec!["id".to_owned()];
    unsafe_update.update_columns = vec!["label".to_owned()];
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("negative budget");
    let error = prepare_write(
        &pool,
        &unsafe_update,
        non_unique_schema,
        &budget,
        &cancellation,
    )
    .await
    .expect_err("non unique key must fail before mutation");
    assert_eq!(error.category, ErrorCategory::Schema);

    let budget = ResourceBudget::new(ResourceLimits::default()).expect("rollback budget");
    let prepared = prepare_write(&pool, &update, Arc::clone(&schema), &budget, &cancellation)
        .await
        .expect("prepare rollback update");
    let error = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([
                batch(vec![1], vec!["must-rollback"], vec![13]),
                batch(vec![3], vec!["invalid"], vec![-1]),
            ]),
        }),
        &cancellation,
    )
    .await
    .expect_err("later constraint failure must roll back prior update");
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);

    let mut results = admin
        .execute_query(
            Query::new(
                "SELECT [id], [label], [score] FROM [plenora_test].[mutation_probe] \
                 ORDER BY [id];",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify mutations");
    let rows = results.pop().expect("mutation result set");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].try_get::<i32, _>(0).expect("id"), Some(1));
    assert_eq!(
        rows[0].try_get::<&str, _>(1).expect("label"),
        Some("upserted")
    );
    assert_eq!(rows[0].try_get::<i32, _>(2).expect("score"), Some(12));
    assert_eq!(rows[1].try_get::<i32, _>(0).expect("id"), Some(3));
    assert_eq!(
        rows[1].try_get::<&str, _>(1).expect("label"),
        Some("inserted")
    );
    assert_eq!(rows[1].try_get::<i32, _>(2).expect("score"), Some(30));
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
                    [clock_time] time(7) NULL, [local_timestamp] datetime2(7) NULL, \
                    [offset_timestamp] datetimeoffset(7) NULL, \
                    [label] nvarchar(100) NULL, [payload] varbinary(32) NULL, \
                    [external_id] uniqueidentifier NULL \
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
        "clock_time",
        "local_timestamp",
        "offset_timestamp",
        "label",
        "payload",
        "external_id",
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
                          [clock_time], [local_timestamp], [offset_timestamp], \
                          [label], [payload], [external_id] \
                   FROM [plenora_test].[stream_probe] \
                   EXCEPT \
                   SELECT [id], [flag], [unsigned_small], [signed_small], [signed_big], \
                          [single_value], [double_value], [exact_value], \
                          [clock_time], [local_timestamp], [offset_timestamp], \
                          [label], [payload], [external_id] \
                   FROM [plenora_test].[bulk_native_probe]) \
                  UNION ALL \
                  (SELECT [id], [flag], [unsigned_small], [signed_small], [signed_big], \
                          [single_value], [double_value], [exact_value], \
                          [clock_time], [local_timestamp], [offset_timestamp], \
                          [label], [payload], [external_id] \
                   FROM [plenora_test].[bulk_native_probe] \
                   EXCEPT \
                   SELECT [id], [flag], [unsigned_small], [signed_small], [signed_big], \
                          [single_value], [double_value], [exact_value], \
                          [clock_time], [local_timestamp], [offset_timestamp], \
                          [label], [payload], [external_id] \
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
        spatial_dimensions: None,
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
#[ignore = "richiede SQL Server live e perdita totale temporanea del trasporto TDS"]
async fn live_temporary_total_packet_loss_adds_latency_without_corruption() {
    let cancellation = CancellationToken::new();
    let direct_config = live_config(CertificatePolicy::TrustServerCertificate);
    let proxy = TcpCutProxy::start(direct_config.host(), direct_config.port()).await;
    let config = proxied_live_config(proxy.port)
        .with_application_name("plenora-temporary-packet-loss")
        .with_timeouts(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(2),
        );
    let mut session = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("proxied packet-loss session");
    let mut admin = SqlServerSession::open(&live_admin_config(), &cancellation)
        .await
        .expect("packet-loss server-state admin");
    let (sender, mut receiver) = mpsc::channel(1);
    let worker_cancellation = cancellation.clone();
    let expected_columns = vec![SqlServerColumnSpec {
        name: "value".to_owned(),
        native_type: "int".to_owned(),
        native_declaration: "int".to_owned(),
        nullable: false,
        collation: None,
        kind: SqlServerColumnKind::I32,
        spatial_srid: None,
        spatial_dimensions: None,
        wire_encoding: SqlServerWireEncoding::Native,
    }];
    let worker = tokio::spawn(async move {
        let result = session
            .pump_query_rows(
                Query::new("WAITFOR DELAY '00:00:00.200'; SELECT 1 AS [value];"),
                sender,
                &expected_columns,
                &worker_cancellation,
            )
            .await;
        (result, session)
    });

    wait_for_application_request(
        &mut admin,
        "plenora-temporary-packet-loss",
        &cancellation,
    )
    .await;
    proxy.blackhole().await;
    let fault_started = tokio::time::Instant::now();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    proxy.forward().await;

    let (result, session) = tokio::time::timeout(std::time::Duration::from_secs(3), worker)
        .await
        .expect("read must recover after temporary packet loss")
        .expect("packet-loss worker");
    result.expect("temporary packet loss below timeout must preserve the response");
    assert!(fault_started.elapsed() >= std::time::Duration::from_millis(400));
    assert!(session.is_reusable());
    let row = receiver
        .recv()
        .await
        .expect("one row expected")
        .expect("row must decode after forwarding resumes");
    assert_eq!(row.get::<i32, _>(0), Some(1));
    assert!(receiver.recv().await.is_none());
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
#[ignore = "richiede SQL Server live esplicito e verifica schema evolution transazionale"]
#[allow(clippy::too_many_lines)]
async fn live_additive_schema_evolution_is_opt_in_atomic_and_reported() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("schema evolution admin");
    let reset = || {
        Query::new(
            "DROP TABLE IF EXISTS [plenora_test].[schema_evolution_probe]; \
             CREATE TABLE [plenora_test].[schema_evolution_probe] \
             ([id] int NOT NULL PRIMARY KEY);",
        )
    };
    admin
        .execute_query(reset(), ErrorPhase::Write, &cancellation)
        .await
        .expect("schema evolution fixture");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let operation = write_operation("schema_evolution_probe", WriteMode::Append);
    let secret = live_secret();
    let disabled = SqlServerProvider::new(config.clone(), 16, 1).expect("disabled provider");
    let disabled_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("disabled evolution budget");
    let Err(disabled_error) = disabled
        .prepare_write(
            &secret,
            &operation,
            Arc::clone(&schema),
            &disabled_budget,
            &cancellation,
        )
        .await
    else {
        panic!("schema evolution must be opt-in");
    };
    assert_eq!(disabled_error.category, ErrorCategory::Schema);

    let provider = SqlServerProvider::new(config.clone(), 16, 1)
        .expect("evolution provider")
        .with_schema_evolution(SqlServerSchemaEvolution::AddNullableColumns);
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("evolution budget");
    let prepared = provider
        .prepare_write(
            &secret,
            &operation,
            Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare additive evolution");
    assert_eq!(prepared.loss_report.losses.len(), 1);
    assert_eq!(
        prepared.loss_report.losses[0].severity,
        LossSeverity::Information
    );
    let before: i64 = admin
        .execute_query(
            Query::new(
                "SELECT COUNT_BIG(*) FROM sys.columns \
                 WHERE object_id = OBJECT_ID(N'plenora_test.schema_evolution_probe') \
                   AND name = N'note';",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("inspect prepare side effects")
        .remove(0)
        .remove(0)
        .get(0)
        .expect("prepare side effect count");
    assert_eq!(before, 0, "prepare must not mutate the target");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    )
    .expect("schema evolution batch");
    let input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([batch]),
    });
    let outcome = provider
        .write(&secret, prepared, input, &budget, &cancellation)
        .await
        .expect("execute additive evolution");
    assert_eq!(outcome.status, WriteStatus::Committed);

    let committed: i64 = admin
        .execute_query(
            Query::new(
                "SELECT COUNT_BIG(*) FROM [plenora_test].[schema_evolution_probe] \
                 WHERE ([id] = 1 AND [note] = N'a') OR ([id] = 2 AND [note] IS NULL);",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify evolved rows")
        .remove(0)
        .remove(0)
        .get(0)
        .expect("evolved row count");
    assert_eq!(committed, 2);

    admin
        .execute_query(reset(), ErrorPhase::Write, &cancellation)
        .await
        .expect("reset rollback fixture");
    let rollback_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("rollback evolution budget");
    let rollback_prepared = provider
        .prepare_write(
            &secret,
            &operation,
            Arc::clone(&schema),
            &rollback_budget,
            &cancellation,
        )
        .await
        .expect("prepare rollback evolution");
    let duplicate = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![3, 3])),
            Arc::new(StringArray::from(vec![Some("first"), Some("duplicate")])),
        ],
    )
    .expect("duplicate evolution batch");
    let duplicate_input: Box<dyn BatchStream> = Box::new(VecBatchStream {
        schema,
        batches: VecDeque::from([duplicate]),
    });
    provider
        .write(
            &secret,
            rollback_prepared,
            duplicate_input,
            &rollback_budget,
            &cancellation,
        )
        .await
        .expect_err("duplicate key must roll back DDL and rows");
    let rollback_state = admin
        .execute_query(
            Query::new(
                "SELECT \
                   COUNT_BIG(CASE WHEN c.name = N'note' THEN 1 END), \
                   (SELECT COUNT_BIG(*) FROM [plenora_test].[schema_evolution_probe]) \
                 FROM sys.columns AS c \
                 WHERE c.object_id = OBJECT_ID(N'plenora_test.schema_evolution_probe');",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify evolution rollback")
        .remove(0)
        .remove(0);
    assert_eq!(
        rollback_state
            .get::<i64, _>(0)
            .expect("rolled back column count"),
        0
    );
    assert_eq!(
        rollback_state.get::<i64, _>(1).expect("rolled back rows"),
        0
    );
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[schema_evolution_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup schema evolution fixture");
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

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture lifecycle isolate"]
#[allow(clippy::too_many_lines)]
async fn live_create_and_staged_replace_publish_exact_schema_and_rows() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("lifecycle admin");
    admin
        .execute_query(
            Query::new("DROP TABLE IF EXISTS [plenora_test].[lifecycle_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("clean lifecycle fixture");

    let pool = SqlServerPool::new(config, 2).expect("lifecycle pool");
    let create_schema = guard_schema();
    let mut create = write_operation("lifecycle_probe", WriteMode::Create);
    create.keys = vec!["id".to_owned()];
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("create budget");
    let prepared = prepare_write(
        &pool,
        &create,
        Arc::clone(&create_schema),
        &budget,
        &cancellation,
    )
    .await
    .expect("prepare create");
    let create_outcome = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&create_schema),
            batches: VecDeque::from([RecordBatch::try_new(
                Arc::clone(&create_schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["first", "second"])),
                ],
            )
            .expect("create batch")]),
        }),
        &cancellation,
    )
    .await
    .expect("execute create");
    assert_eq!(create_outcome.status, WriteStatus::Committed);
    assert_eq!(create_outcome.rows.inserted, Some(2));

    let duplicate_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("duplicate create budget");
    let duplicate_error = prepare_write(
        &pool,
        &create,
        Arc::clone(&create_schema),
        &duplicate_budget,
        &cancellation,
    )
    .await
    .expect_err("create existing target");
    assert_eq!(duplicate_error.category, ErrorCategory::Schema);

    let replace_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));
    let mut replace = write_operation("lifecycle_probe", WriteMode::Replace);
    replace.keys = vec!["id".to_owned()];
    replace.transaction_profile = TransactionProfile::StagedSwap;
    let replace_budget = ResourceBudget::new(ResourceLimits::default()).expect("replace budget");
    let prepared = prepare_write(
        &pool,
        &replace,
        Arc::clone(&replace_schema),
        &replace_budget,
        &cancellation,
    )
    .await
    .expect("prepare replace");
    let replace_outcome = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&replace_schema),
            batches: VecDeque::from([RecordBatch::try_new(
                Arc::clone(&replace_schema),
                vec![
                    Arc::new(Int32Array::from(vec![7, 8])),
                    Arc::new(StringArray::from(vec!["new-a", "new-b"])),
                    Arc::new(Int32Array::from(vec![70, 80])),
                ],
            )
            .expect("replace batch")]),
        }),
        &cancellation,
    )
    .await
    .expect("execute replace");
    assert_eq!(replace_outcome.status, WriteStatus::Committed);
    assert_eq!(replace_outcome.rows.inserted, Some(2));

    let results = admin
        .execute_query(
            Query::new(
                "SELECT [id], [label], [score] \
                 FROM [plenora_test].[lifecycle_probe] ORDER BY [id]; \
                 SELECT COUNT_BIG(*) FROM sys.objects AS o \
                 JOIN sys.schemas AS s ON s.schema_id = o.schema_id \
                 WHERE s.name = N'plenora_test' \
                   AND o.name LIKE N'lifecycle_probe__pln[_]%';",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify replacement");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].len(), 2);
    assert_eq!(results[0][0].try_get::<i32, _>(0).expect("id"), Some(7));
    assert_eq!(results[0][1].try_get::<i32, _>(2).expect("score"), Some(80));
    assert_eq!(
        results[1][0].try_get::<i64, _>(0).expect("orphan count"),
        Some(0)
    );
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[lifecycle_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup lifecycle fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e qualifica tutti i tipi create/replace"]
#[allow(clippy::too_many_lines)]
async fn live_create_and_replace_round_trip_all_reference_types() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("typed lifecycle admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[lifecycle_types_probe]; \
                 DROP TABLE IF EXISTS [plenora_test].[lifecycle_empty_spatial_probe];",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("clean typed lifecycle fixture");
    let pool = SqlServerPool::new(config, 3).expect("typed lifecycle pool");

    let source_budget = ResourceBudget::new(ResourceLimits::default()).expect("source budget");
    let source = read_object(
        &pool,
        "plenora_test",
        "stream_probe",
        2,
        &source_budget,
        &cancellation,
    )
    .await
    .expect("typed create source");
    let schema = source.schema();
    let mut create = write_operation("lifecycle_types_probe", WriteMode::Create);
    create.keys = vec!["id".to_owned()];
    create.create_spatial_index = true;
    let create_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("typed create budget");
    let prepared = prepare_write(
        &pool,
        &create,
        Arc::clone(&schema),
        &create_budget,
        &cancellation,
    )
    .await
    .expect("prepare typed create");
    let outcome = write_prepared(prepared, source, &cancellation)
        .await
        .expect("execute typed create");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.inserted, Some(5));

    let source_description =
        describe_object(&mut admin, "plenora_test", "stream_probe", &cancellation)
            .await
            .expect("source description");
    let target_description = describe_object(
        &mut admin,
        "plenora_test",
        "lifecycle_types_probe",
        &cancellation,
    )
    .await
    .expect("created description");
    let source_specs = source_description
        .columns
        .iter()
        .map(SqlServerColumnSpec::from_catalog)
        .collect::<plenora_database_core::Result<Vec<_>>>()
        .expect("source native contracts");
    let created_specs = target_description
        .columns
        .iter()
        .map(SqlServerColumnSpec::from_catalog)
        .collect::<plenora_database_core::Result<Vec<_>>>()
        .expect("created native contracts");
    assert_eq!(created_specs, source_specs);
    let created_spatial_indexes = target_description
        .indexes
        .iter()
        .filter_map(|index| index.spatial.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(created_spatial_indexes.len(), 2);
    assert!(created_spatial_indexes.iter().any(|index| {
        index.spatial_type == "GEOMETRY"
            && index.tessellation_scheme == "GEOMETRY_AUTO_GRID"
            && index.bounding_box.is_some()
    }));
    assert!(created_spatial_indexes.iter().any(|index| {
        index.spatial_type == "GEOGRAPHY"
            && index.tessellation_scheme == "GEOGRAPHY_AUTO_GRID"
            && index.bounding_box.is_none()
    }));

    let replacement_source_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("replacement source budget");
    let replacement_source = read_object(
        &pool,
        "plenora_test",
        "stream_probe",
        3,
        &replacement_source_budget,
        &cancellation,
    )
    .await
    .expect("typed replace source");
    let mut replace = write_operation("lifecycle_types_probe", WriteMode::Replace);
    replace.keys = vec!["id".to_owned()];
    replace.transaction_profile = TransactionProfile::StagedSwap;
    replace.create_spatial_index = true;
    let replace_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("typed replace budget");
    let prepared = prepare_write(
        &pool,
        &replace,
        Arc::clone(&schema),
        &replace_budget,
        &cancellation,
    )
    .await
    .expect("prepare typed replace");
    let outcome = write_prepared(prepared, replacement_source, &cancellation)
        .await
        .expect("execute typed replace");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.inserted, Some(5));

    let replaced_description = describe_object(
        &mut admin,
        "plenora_test",
        "lifecycle_types_probe",
        &cancellation,
    )
    .await
    .expect("replaced description");
    let replaced_specs = replaced_description
        .columns
        .iter()
        .map(SqlServerColumnSpec::from_catalog)
        .collect::<plenora_database_core::Result<Vec<_>>>()
        .expect("replaced native contracts");
    assert_eq!(replaced_specs, source_specs);
    let replaced_spatial_indexes = replaced_description
        .indexes
        .iter()
        .filter_map(|index| index.spatial.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(replaced_spatial_indexes.len(), 2);
    assert!(replaced_spatial_indexes.iter().any(|index| {
        index.spatial_type == "GEOMETRY"
            && index.tessellation_scheme == "GEOMETRY_AUTO_GRID"
            && index.bounding_box.is_some()
    }));
    assert!(replaced_spatial_indexes.iter().any(|index| {
        index.spatial_type == "GEOGRAPHY"
            && index.tessellation_scheme == "GEOGRAPHY_AUTO_GRID"
            && index.bounding_box.is_none()
    }));

    let geometry_index_name = replaced_description
        .indexes
        .iter()
        .find(|index| {
            index
                .spatial
                .as_ref()
                .is_some_and(|spatial| spatial.spatial_type == "GEOMETRY")
        })
        .and_then(|index| index.name.as_deref())
        .expect("geometry spatial index name");
    assert!(geometry_index_name.starts_with("IX_pln_spatial_"));
    let quoted_geometry_index = format!("[{}]", geometry_index_name.replace(']', "]]"));
    let sample = admin
        .execute_query(
            Query::new(
                "SELECT TOP (1) [shape].STAsText(), [shape].STSrid \
                 FROM [plenora_test].[lifecycle_types_probe] \
                 WHERE [shape] IS NOT NULL;",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("load indexed geometry sample");
    let sample_wkt = sample[0][0]
        .try_get::<&str, _>(0)
        .expect("geometry sample WKT")
        .expect("geometry sample WKT value")
        .to_owned();
    let sample_srid = sample[0][0]
        .try_get::<i32, _>(1)
        .expect("geometry sample SRID")
        .expect("geometry sample SRID value");
    let mut forced_spatial_query = Query::new(format!(
        "SELECT COUNT_BIG(*) \
         FROM [plenora_test].[lifecycle_types_probe] WITH (INDEX({quoted_geometry_index})) \
         WHERE [shape].STIntersects(geometry::STGeomFromText(@P1, @P2)) = 1;"
    ));
    forced_spatial_query.bind(sample_wkt);
    forced_spatial_query.bind(sample_srid);
    let indexed_count = admin
        .execute_query(forced_spatial_query, ErrorPhase::Probe, &cancellation)
        .await
        .expect("force geometry spatial access path");
    assert!(indexed_count[0][0]
        .try_get::<i64, _>(0)
        .expect("indexed row count")
        .is_some_and(|count| count > 0));

    let mut empty_spatial_create =
        write_operation("lifecycle_empty_spatial_probe", WriteMode::Create);
    empty_spatial_create.keys = vec!["id".to_owned()];
    empty_spatial_create.create_spatial_index = true;
    let empty_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("empty spatial create budget");
    let empty_prepared = prepare_write(
        &pool,
        &empty_spatial_create,
        Arc::clone(&schema),
        &empty_budget,
        &cancellation,
    )
    .await
    .expect("prepare empty spatial create");
    let empty_error = write_prepared(
        empty_prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: VecDeque::new(),
        }),
        &cancellation,
    )
    .await
    .expect_err("geometry spatial index without bounds must fail closed");
    assert_eq!(empty_error.category, ErrorCategory::InvalidPlan);
    assert_eq!(empty_error.remote_effect, RemoteEffect::RolledBack);
    let empty_state = admin
        .execute_query(
            Query::new(
                "SELECT CASE WHEN OBJECT_ID(\
                    N'plenora_test.lifecycle_empty_spatial_probe', N'U'\
                 ) IS NULL THEN 0 ELSE 1 END; \
                 SELECT COUNT_BIG(*) FROM sys.objects AS o \
                 JOIN sys.schemas AS s ON s.schema_id = o.schema_id \
                 WHERE s.name = N'plenora_test' \
                   AND o.name LIKE N'lifecycle_empty_spatial_probe__pln[_]%';",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify empty spatial create rollback");
    assert_eq!(
        empty_state[0][0]
            .try_get::<i32, _>(0)
            .expect("empty target state"),
        Some(0)
    );
    assert_eq!(
        empty_state[1][0]
            .try_get::<i64, _>(0)
            .expect("empty staging state"),
        Some(0)
    );

    let count = admin
        .execute_query(
            Query::new(
                "SELECT COUNT_BIG(*) FROM [plenora_test].[lifecycle_types_probe]; \
                 SELECT COUNT_BIG(*) FROM sys.objects AS o \
                 JOIN sys.schemas AS s ON s.schema_id = o.schema_id \
                 WHERE s.name = N'plenora_test' \
                   AND o.name LIKE N'lifecycle_types_probe__pln[_]%';",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify typed replace");
    assert_eq!(
        count[0][0].try_get::<i64, _>(0).expect("row count"),
        Some(5)
    );
    assert_eq!(
        count[1][0].try_get::<i64, _>(0).expect("orphan count"),
        Some(0)
    );
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[lifecycle_types_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup typed lifecycle fixture");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e verifica rollback lifecycle"]
#[allow(clippy::too_many_lines)]
async fn live_replace_rolls_back_load_and_published_swap_on_failure() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("rollback lifecycle admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[replace_rollback_dependent]; \
                 DROP TABLE IF EXISTS [plenora_test].[replace_rollback_probe]; \
                 CREATE TABLE [plenora_test].[replace_rollback_probe] \
                    ([id] int NOT NULL PRIMARY KEY, [label] nvarchar(max) NOT NULL); \
                 INSERT INTO [plenora_test].[replace_rollback_probe] \
                    VALUES (99, N'sentinel');",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("replace rollback fixture");
    let pool = SqlServerPool::new(config, 2).expect("rollback lifecycle pool");
    let schema = guard_schema();
    let mut replace = write_operation("replace_rollback_probe", WriteMode::Replace);
    replace.keys = vec!["id".to_owned()];
    replace.transaction_profile = TransactionProfile::StagedSwap;

    let budget = ResourceBudget::new(ResourceLimits::default()).expect("constraint budget");
    let prepared = prepare_write(&pool, &replace, Arc::clone(&schema), &budget, &cancellation)
        .await
        .expect("prepare duplicate replace");
    let duplicate_error = write_prepared(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([
                guard_batch(Arc::clone(&schema), 1, "first"),
                guard_batch(Arc::clone(&schema), 1, "duplicate"),
            ]),
        }),
        &cancellation,
    )
    .await
    .expect_err("duplicate staged key");
    assert_eq!(duplicate_error.remote_effect, RemoteEffect::RolledBack);

    let budget = ResourceBudget::new(ResourceLimits::default()).expect("fault budget");
    let prepared = prepare_write(&pool, &replace, Arc::clone(&schema), &budget, &cancellation)
        .await
        .expect("prepare pre-commit replace");
    let fault_error = crate::write::write_prepared_with_fault(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([guard_batch(Arc::clone(&schema), 2, "published")]),
        }),
        &cancellation,
        crate::write::WriteFaultPoint::BeforeCommit,
    )
    .await
    .expect_err("rollback published rename");
    assert_eq!(fault_error.remote_effect, RemoteEffect::RolledBack);

    let verification = admin
        .execute_query(
            Query::new(
                "SELECT [id], [label] FROM [plenora_test].[replace_rollback_probe]; \
                 SELECT COUNT_BIG(*) FROM sys.objects AS o \
                 JOIN sys.schemas AS s ON s.schema_id = o.schema_id \
                 WHERE s.name = N'plenora_test' \
                   AND o.name LIKE N'replace_rollback_probe__pln[_]%';",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify lifecycle rollback");
    assert_eq!(verification[0].len(), 1);
    assert_eq!(
        verification[0][0].try_get::<i32, _>(0).expect("sentinel"),
        Some(99)
    );
    assert_eq!(
        verification[1][0]
            .try_get::<i64, _>(0)
            .expect("orphan count"),
        Some(0)
    );

    let budget = ResourceBudget::new(ResourceLimits::default()).expect("unknown budget");
    let prepared = prepare_write(&pool, &replace, Arc::clone(&schema), &budget, &cancellation)
        .await
        .expect("prepare uncertain replace");
    let uncertain = crate::write::write_prepared_with_fault(
        prepared,
        Box::new(VecBatchStream {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([guard_batch(Arc::clone(&schema), 3, "committed-unknown")]),
        }),
        &cancellation,
        crate::write::WriteFaultPoint::CommitConfirmationLost,
    )
    .await
    .expect("commit confirmation loss");
    assert_eq!(uncertain.status, WriteStatus::OutcomeUnknown);
    let recovery = uncertain.recovery.expect("replace recovery");
    assert!(!recovery.automatic_retry_allowed);
    assert!(recovery.staging_object.is_none());
    let published = admin
        .execute_query(
            Query::new(
                "SELECT [id] FROM [plenora_test].[replace_rollback_probe]; \
                 SELECT COUNT_BIG(*) FROM sys.objects AS o \
                 JOIN sys.schemas AS s ON s.schema_id = o.schema_id \
                 WHERE s.name = N'plenora_test' \
                   AND o.name LIKE N'replace_rollback_probe__pln[_]%';",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify uncertain publish");
    assert_eq!(published[0].len(), 1);
    assert_eq!(
        published[0][0].try_get::<i32, _>(0).expect("new id"),
        Some(3)
    );
    assert_eq!(
        published[1][0].try_get::<i64, _>(0).expect("orphan count"),
        Some(0)
    );

    admin
        .execute_query(
            Query::new(
                "CREATE TABLE [plenora_test].[replace_rollback_dependent] \
                 ([id] int NOT NULL PRIMARY KEY, \
                  CONSTRAINT [FK_replace_rollback_dependent] FOREIGN KEY ([id]) \
                  REFERENCES [plenora_test].[replace_rollback_probe] ([id]));",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("dependency foreign key");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("dependency budget");
    let dependency_error = prepare_write(&pool, &replace, schema, &budget, &cancellation)
        .await
        .expect_err("enabled trigger must fail closed");
    assert_eq!(dependency_error.category, ErrorCategory::Unsupported);
    admin
        .execute_query(
            Query::new(
                "DROP TABLE [plenora_test].[replace_rollback_dependent]; \
                 DROP TABLE [plenora_test].[replace_rollback_probe];",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup replace rollback");
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e verifica visibilita staged swap"]
#[allow(clippy::too_many_lines)]
async fn live_replace_keeps_old_target_readable_while_staging_then_publishes_new() {
    let cancellation = CancellationToken::new();
    let config = live_config(CertificatePolicy::TrustServerCertificate);
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("visibility admin");
    admin
        .execute_query(
            Query::new(
                "DROP TABLE IF EXISTS [plenora_test].[replace_visibility_probe]; \
                 CREATE TABLE [plenora_test].[replace_visibility_probe] \
                    ([id] int NOT NULL PRIMARY KEY, [label] nvarchar(max) NOT NULL); \
                 INSERT INTO [plenora_test].[replace_visibility_probe] \
                    VALUES (99, N'old-visible');",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("visibility fixture");
    let pool = SqlServerPool::new(config, 2).expect("visibility pool");
    let schema = guard_schema();
    let mut replace = write_operation("replace_visibility_probe", WriteMode::Replace);
    replace.keys = vec!["id".to_owned()];
    replace.transaction_profile = TransactionProfile::StagedSwap;
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("visibility budget");
    let prepared = prepare_write(&pool, &replace, Arc::clone(&schema), &budget, &cancellation)
        .await
        .expect("prepare visibility replace");
    let (barrier_reached, barrier_wait) = oneshot::channel();
    let (release, release_wait) = oneshot::channel();
    let write_cancellation = cancellation.clone();
    let write_schema = Arc::clone(&schema);
    let task = tokio::spawn(async move {
        crate::write::write_prepared(
            prepared,
            Box::new(BarrierBatchStream {
                schema: Arc::clone(&write_schema),
                batches: VecDeque::from([
                    guard_batch(Arc::clone(&write_schema), 1, "new-a"),
                    guard_batch(Arc::clone(&write_schema), 2, "new-b"),
                ]),
                emitted: 0,
                barrier_reached: Some(barrier_reached),
                release: Some(release_wait),
            }),
            &write_cancellation,
        )
        .await
    });
    barrier_wait.await.expect("staging barrier");

    let old_read = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        admin.execute_query(
            Query::new(
                "SELECT [label] FROM [plenora_test].[replace_visibility_probe] WHERE [id] = 99;",
            ),
            ErrorPhase::Probe,
            &cancellation,
        ),
    )
    .await
    .expect("old target must not be locked during staging")
    .expect("read old target");
    assert_eq!(
        old_read[0][0].try_get::<&str, _>(0).expect("old label"),
        Some("old-visible")
    );
    release.send(()).expect("release staged writer");
    let outcome = task
        .await
        .expect("replace task")
        .expect("replace committed");
    assert_eq!(outcome.status, WriteStatus::Committed);

    let new_rows = admin
        .execute_query(
            Query::new("SELECT [id] FROM [plenora_test].[replace_visibility_probe] ORDER BY [id];"),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("read published target");
    assert_eq!(new_rows[0].len(), 2);
    assert_eq!(
        new_rows[0][0].try_get::<i32, _>(0).expect("new id"),
        Some(1)
    );
    assert_eq!(
        new_rows[0][1].try_get::<i32, _>(0).expect("new id"),
        Some(2)
    );

    let drift_budget =
        ResourceBudget::new(ResourceLimits::default()).expect("drift replace budget");
    let prepared = prepare_write(
        &pool,
        &replace,
        Arc::clone(&schema),
        &drift_budget,
        &cancellation,
    )
    .await
    .expect("prepare drift replace");
    let (drift_reached, drift_wait) = oneshot::channel();
    let (drift_release, drift_release_wait) = oneshot::channel();
    let drift_cancellation = cancellation.clone();
    let drift_schema = Arc::clone(&schema);
    let drift_task = tokio::spawn(async move {
        crate::write::write_prepared(
            prepared,
            Box::new(BarrierBatchStream {
                schema: Arc::clone(&drift_schema),
                batches: VecDeque::from([
                    guard_batch(Arc::clone(&drift_schema), 5, "must-not-publish"),
                    guard_batch(Arc::clone(&drift_schema), 6, "must-not-publish"),
                ]),
                emitted: 0,
                barrier_reached: Some(drift_reached),
                release: Some(drift_release_wait),
            }),
            &drift_cancellation,
        )
        .await
    });
    drift_wait.await.expect("drift staging barrier");
    admin
        .execute_query(
            Query::new(
                "ALTER TABLE [plenora_test].[replace_visibility_probe] \
                 ADD [concurrent_drift] int NULL;",
            ),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("concurrent target drift");
    drift_release.send(()).expect("release drift writer");
    let drift_error = drift_task
        .await
        .expect("drift replace task")
        .expect_err("schema drift must prevent publish");
    assert_eq!(drift_error.category, ErrorCategory::Schema);
    assert_eq!(drift_error.remote_effect, RemoteEffect::RolledBack);

    let after_drift = admin
        .execute_query(
            Query::new(
                "SELECT [id] FROM [plenora_test].[replace_visibility_probe] ORDER BY [id]; \
                 SELECT CASE WHEN COL_LENGTH(\
                    N'plenora_test.replace_visibility_probe', N'concurrent_drift'\
                 ) IS NULL THEN 0 ELSE 1 END; \
                 SELECT COUNT_BIG(*) FROM sys.objects AS o \
                 JOIN sys.schemas AS s ON s.schema_id = o.schema_id \
                 WHERE s.name = N'plenora_test' \
                   AND o.name LIKE N'replace_visibility_probe__pln[_]%';",
            ),
            ErrorPhase::Probe,
            &cancellation,
        )
        .await
        .expect("verify drift rollback");
    assert_eq!(after_drift[0].len(), 2);
    assert_eq!(
        after_drift[0][0].try_get::<i32, _>(0).expect("old id"),
        Some(1)
    );
    assert_eq!(
        after_drift[0][1].try_get::<i32, _>(0).expect("old id"),
        Some(2)
    );
    assert_eq!(
        after_drift[1][0]
            .try_get::<i32, _>(0)
            .expect("drift column"),
        Some(1)
    );
    assert_eq!(
        after_drift[2][0]
            .try_get::<i64, _>(0)
            .expect("orphan count"),
        Some(0)
    );
    admin
        .execute_query(
            Query::new("DROP TABLE [plenora_test].[replace_visibility_probe];"),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("cleanup visibility fixture");
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

fn spatial_write_schema(semantics: &str, dimensions: &str) -> SchemaRef {
    let metadata = HashMap::from([
        (
            protocol::GEOARROW_EXTENSION_NAME.to_owned(),
            "geoarrow.wkb".to_owned(),
        ),
        (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
        (
            protocol::GEOMETRY_DIMENSIONS.to_owned(),
            dimensions.to_owned(),
        ),
        (
            protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
            "mixed".to_owned(),
        ),
        (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
        (
            protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
            semantics.to_owned(),
        ),
        (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
        (
            protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
            "declared_unresolved".to_owned(),
        ),
    ]);
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("shape", DataType::Binary, true).with_metadata(metadata)],
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn ewkb_point(type_code: u32, coordinates: &[f64]) -> Vec<u8> {
    let mut value = Vec::with_capacity(5 + std::mem::size_of_val(coordinates));
    value.push(1);
    value.extend_from_slice(&type_code.to_le_bytes());
    for coordinate in coordinates {
        value.extend_from_slice(&coordinate.to_le_bytes());
    }
    value
}

fn wkb_circular_string_xy(points: &[(f64, f64)]) -> Vec<u8> {
    let mut value = Vec::with_capacity(9 + points.len() * 16);
    value.push(1);
    value.extend_from_slice(&8_u32.to_le_bytes());
    value.extend_from_slice(
        &u32::try_from(points.len())
            .expect("curve point count")
            .to_le_bytes(),
    );
    for (x, y) in points {
        value.extend_from_slice(&x.to_le_bytes());
        value.extend_from_slice(&y.to_le_bytes());
    }
    value
}

fn wkb_polygon_xy(points: &[[f64; 2]]) -> Vec<u8> {
    let mut value = Vec::with_capacity(13 + points.len() * 16);
    value.push(1);
    value.extend_from_slice(&3_u32.to_le_bytes());
    value.extend_from_slice(&1_u32.to_le_bytes());
    value.extend_from_slice(
        &u32::try_from(points.len())
            .expect("numero punti WKB rappresentabile")
            .to_le_bytes(),
    );
    for [x, y] in points {
        value.extend_from_slice(&x.to_le_bytes());
        value.extend_from_slice(&y.to_le_bytes());
    }
    value
}

fn spatial_write_batch(schema: SchemaRef, value: &[u8]) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from(vec![Some(value)]))])
        .expect("spatial write guard batch")
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
