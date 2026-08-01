use crate::{
    describe_object, list_objects, list_schemas, probe_server, MysqlCertificatePolicy, MysqlConfig,
    MysqlProvider, MysqlSession,
};
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::{CancellationToken, ErrorCategory, ResourceBudget, ResourceLimits};

const DEFAULT_PASSWORD: &str = "DataFlow_Test_2026!";

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn live_secret() -> SecretString {
    SecretString::new(environment("PLENORA_MYSQL_PASSWORD", DEFAULT_PASSWORD))
}

fn live_config() -> MysqlConfig {
    live_config_for_host(environment("PLENORA_MYSQL_HOST", "127.0.0.1"))
}

fn live_config_for_host(host: String) -> MysqlConfig {
    let config = MysqlConfig::new(
        host,
        environment("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        environment("PLENORA_MYSQL_USER", "dataflow"),
        live_secret(),
    )
    .with_port(
        environment("PLENORA_MYSQL_PORT", "3306")
            .parse()
            .expect("porta MySQL live"),
    );
    if let Ok(ca_path) = std::env::var("PLENORA_MYSQL_CA") {
        config.with_private_ca_certificate(ca_path)
    } else {
        config.with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate)
    }
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito"]
async fn live_reference_probe_catalog_and_spatial_metadata() {
    let cancellation = CancellationToken::new();
    let config = live_config();
    let mut session = MysqlSession::open(&config, &cancellation)
        .await
        .expect("connessione MySQL live");

    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe MySQL live");
    assert!(probe.product_version.starts_with("8.4."), "{probe:?}");
    assert_eq!(probe.database, "dataflow_test");
    assert_eq!(probe.time_zone, "+00:00");
    assert!(probe.sql_mode.contains("STRICT_TRANS_TABLES"));
    assert!(!probe.tls_cipher.is_empty());

    let schemas = list_schemas(&mut session, &cancellation)
        .await
        .expect("schemi MySQL live");
    assert!(schemas.iter().any(|schema| schema == "dataflow_test"));
    let objects = list_objects(&mut session, "dataflow_test", &cancellation)
        .await
        .expect("oggetti MySQL live");
    assert!(objects.iter().any(|object| object.name == "catalog_probe"));
    assert!(objects
        .iter()
        .any(|object| object.name == "catalog_probe_view"));

    let first = describe_object(
        &mut session,
        "dataflow_test",
        "catalog_probe",
        &cancellation,
    )
    .await
    .expect("descrizione MySQL live");
    let second = describe_object(
        &mut session,
        "dataflow_test",
        "catalog_probe",
        &cancellation,
    )
    .await
    .expect("descrizione MySQL live ripetuta");
    drop(session);
    assert_eq!(first.token, second.token);
    assert_eq!(first.engine.as_deref(), Some("InnoDB"));
    let geometry = first
        .columns
        .iter()
        .find(|column| column.name == "geom")
        .expect("colonna geometry");
    assert_eq!(geometry.data_type, "geometry");
    assert_eq!(geometry.spatial_srid, Some(4_326));
    let point = first
        .columns
        .iter()
        .find(|column| column.name == "geom_point")
        .expect("colonna point");
    assert_eq!(point.data_type, "point");
    assert_eq!(point.spatial_srid, Some(4_326));
    let collection = first
        .columns
        .iter()
        .find(|column| column.name == "geom_collection")
        .expect("colonna geometrycollection");
    assert_eq!(collection.data_type, "geomcollection");
    assert_eq!(collection.spatial_srid, Some(4_326));
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito con CA privata"]
async fn live_verified_tls_rejects_a_hostname_mismatch() {
    let cancellation = CancellationToken::new();
    let ca_path = std::env::var("PLENORA_MYSQL_CA").expect("CA privata richiesta dal gate live");
    let config = live_config_for_host("mysql-hostname-mismatch".to_owned())
        .with_private_ca_certificate(ca_path);
    let Err(error) = MysqlSession::open(&config, &cancellation).await else {
        panic!("hostname TLS errato accettato");
    };
    assert_eq!(error.category, ErrorCategory::Protocol);
    assert_eq!(error.message, "verifica identita TLS MySQL rifiutata");
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per reset pool"]
async fn live_pool_reset_reapplies_deterministic_session_bootstrap() {
    use mysql_async::prelude::Queryable;

    let config = live_config();
    let pool = mysql_async::Pool::new(
        config
            .driver_opts_with_pool(Some(1))
            .expect("pool opts MySQL live"),
    );
    let mut connection = pool.get_conn().await.expect("checkout");
    let first_connection_id: Option<u64> = connection
        .query_first("SELECT CONNECTION_ID()")
        .await
        .expect("id prima del reset");
    connection
        .query_drop("SET SESSION autocommit = 0, time_zone = '+05:00', sql_mode = ''")
        .await
        .expect("altera stato sessione");
    connection.reset().await.expect("reset connessione");
    let reset_connection_id: Option<u64> = connection
        .query_first("SELECT CONNECTION_ID()")
        .await
        .expect("id dopo il reset");
    assert_eq!(reset_connection_id, first_connection_id);
    let state: Option<(u8, String, String)> = connection
        .query_first("SELECT @@session.autocommit, @@session.time_zone, @@session.sql_mode")
        .await
        .expect("legge stato sessione");
    let (autocommit, time_zone, sql_mode) = state.expect("riga stato sessione");
    assert_eq!(autocommit, 1);
    assert_eq!(time_zone, "+00:00");
    assert!(sql_mode.contains("STRICT_TRANS_TABLES"));
    assert!(sql_mode.contains("ERROR_FOR_DIVISION_BY_ZERO"));
    assert!(sql_mode.contains("NO_ENGINE_SUBSTITUTION"));
    drop(connection);
    pool.disconnect().await.expect("disconnect pool MySQL");
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per cancellazione"]
async fn live_inflight_cancellation_quarantines_the_session() {
    let cancellation = CancellationToken::new();
    let mut session = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("sessione MySQL live");
    let toggle = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        toggle.cancel();
    });

    let error = session
        .query_rows(
            "SELECT SLEEP(5)",
            plenora_database_core::ErrorPhase::Read,
            &cancellation,
        )
        .await
        .expect_err("query MySQL cancellata");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Cancelled
    );
    assert_eq!(session.state(), crate::MysqlSessionState::Quarantined);
    assert!(!session.is_reusable());
    drop(session);
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per deadline"]
async fn live_deadline_reports_timeout_and_quarantines_the_session() {
    let cancellation = CancellationToken::new();
    let mut session = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("sessione MySQL live");
    let toggle = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        toggle.cancel_due_to_deadline();
    });

    let error = session
        .query_rows(
            "SELECT SLEEP(5)",
            plenora_database_core::ErrorPhase::Read,
            &cancellation,
        )
        .await
        .expect_err("query MySQL oltre deadline");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Timeout
    );
    assert_eq!(session.state(), crate::MysqlSessionState::Quarantined);
    assert!(!session.is_reusable());
    drop(session);
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per acquire pool"]
async fn live_pool_acquire_timeout_is_independent_from_connect_timeout() {
    let config = live_config().with_timeouts(
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(50),
    );
    let pool = crate::MysqlPool::new(&config, 1).expect("pool MySQL live");
    let cancellation = CancellationToken::new();
    let first = pool.checkout(&cancellation).await.expect("primo checkout");

    let error = pool
        .checkout(&cancellation)
        .await
        .expect_err("pool saturo oltre acquire timeout");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Timeout
    );

    drop(first);
    let recovered = pool
        .checkout(&cancellation)
        .await
        .expect("checkout dopo rilascio permit");
    assert!(recovered.is_reusable());
    drop(recovered);
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per timeout"]
async fn live_operation_timeout_quarantines_the_session() {
    let cancellation = CancellationToken::new();
    let config = live_config().with_timeouts(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_millis(50),
        std::time::Duration::from_secs(10),
    );
    let mut session = MysqlSession::open(&config, &cancellation)
        .await
        .expect("sessione MySQL live");

    let error = session
        .query_rows(
            "SELECT SLEEP(1)",
            plenora_database_core::ErrorPhase::Read,
            &cancellation,
        )
        .await
        .expect_err("query MySQL in timeout");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Timeout
    );
    assert_eq!(session.state(), crate::MysqlSessionState::Quarantined);
    assert!(!session.is_reusable());
    drop(session);
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito"]
async fn live_provider_connection_capabilities_and_inspect() {
    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let secret = live_secret();

    let connection = provider
        .test_connection(&secret, &cancellation)
        .await
        .expect("test connessione MySQL live");
    assert_eq!(connection.provider, ProviderKind::Mysql);
    assert!(connection.server_version.starts_with("8.4."));

    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("capability MySQL live");
    assert_eq!(capabilities.provider, ProviderKind::Mysql);
    assert!(capabilities.reads.streaming);
    assert!(capabilities.reads.projection);
    assert!(capabilities.reads.filter);
    assert!(capabilities.reads.ordering);
    assert!(!capabilities.writes.create);
    assert!(capabilities.spatial.read_wkb);
    assert!(capabilities.spatial.geometry);
    assert!(capabilities.spatial.mixed_geometry_types);
    assert_eq!(
        capabilities.spatial.dimensions,
        vec![plenora_database_core::geometry::Dimensions::Xy]
    );

    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: None,
                    schema: Some("dataflow_test".to_owned()),
                    object: "catalog_probe".to_owned(),
                    layer_id: None,
                },
            },
            &cancellation,
        )
        .await
        .expect("inspect MySQL live");
    assert_eq!(inspection.operation, "database.describe_object");
    assert_eq!(inspection.document["name"], "catalog_probe");
    assert_eq!(inspection.document["engine"], "InnoDB");
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per drop stream"]
async fn live_early_stream_drop_cancels_worker_and_keeps_provider_usable() {
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 1).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 2_048,
        memory_bytes: 96 * 1_024,
        output_bytes: 4 * 1_024 * 1_024,
        cell_bytes: 2 * 1_024,
        ..ResourceLimits::default()
    })
    .expect("budget drop stream MySQL");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "stream_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        filter: None,
    };
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream lungo MySQL");
    let first = stream
        .next_batch()
        .await
        .expect("primo batch bounded")
        .expect("batch non vuoto");
    assert!(first.num_rows() > 0);
    assert!(first.num_rows() < 2_048);
    drop(stream);

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    provider
        .test_connection(&live_secret(), &CancellationToken::new())
        .await
        .expect("provider utilizzabile dopo drop anticipato");
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per righe variabili multi-batch"]
async fn live_variable_rows_are_not_consumed_past_the_current_batch_budget() {
    use mysql_async::prelude::Queryable;
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};

    let config = live_config();
    let setup = mysql_async::Pool::new(
        config
            .driver_opts_with_pool(Some(1))
            .expect("pool setup righe variabili"),
    );
    let mut connection = setup.get_conn().await.expect("checkout setup");
    connection
        .query_drop("DROP TABLE IF EXISTS variable_stream_probe")
        .await
        .expect("drop fixture precedente");
    connection
        .query_drop(
            "CREATE TABLE variable_stream_probe (id BIGINT NOT NULL PRIMARY KEY, payload VARBINARY(4096) NOT NULL)",
        )
        .await
        .expect("crea fixture righe variabili");
    connection
        .query_drop(
            "INSERT INTO variable_stream_probe VALUES (1, REPEAT(X'41', 1000)), (2, REPEAT(X'42', 4000))",
        )
        .await
        .expect("popola fixture righe variabili");
    drop(connection);
    setup.disconnect().await.expect("disconnect setup");

    let provider = MysqlProvider::new(config, 1).expect("provider righe variabili");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "variable_stream_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        filter: None,
    };
    let budget = ResourceBudget::new(ResourceLimits {
        memory_bytes: 16_000,
        output_bytes: 16_000,
        cell_bytes: 4_096,
        ..ResourceLimits::default()
    })
    .expect("budget righe variabili");
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &CancellationToken::new(),
        )
        .await
        .expect("stream righe variabili");
    let mut ids = Vec::new();
    let mut batches = 0_usize;
    while let Some(batch) = stream.next_batch().await.expect("batch righe variabili") {
        batches = batches.saturating_add(1);
        let values = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .expect("id int64");
        ids.extend((0..values.len()).map(|index| values.value(index)));
    }
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(batches, 2, "una riga valida per ciascun budget residuo");
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per filtri prepared"]
async fn live_read_projection_filter_order_and_default_schema() {
    use plenora_database_core::arrow::array::{Int64Array, StringArray};
    use plenora_database_core::plan::{FilterExpression, OrderBy, SortDirection};
    use plenora_database_core::provider::ParameterValue;
    use std::collections::BTreeMap;

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 2,
        memory_bytes: 1024 * 1024,
        output_bytes: 1024 * 1024,
        cell_bytes: 64 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget filter MySQL");
    let operation = plenora_database_core::plan::ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: None,
            object: "catalog_probe".to_owned(),
            layer_id: None,
        },
        projection: vec!["id".to_owned(), "name".to_owned()],
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Desc,
        }],
        row_limit: Some(1),
        filter: Some(FilterExpression::Eq {
            field: "id".to_owned(),
            parameter: "wanted_id".to_owned(),
        }),
    };
    let parameters = ParameterBag::new(BTreeMap::from([(
        "wanted_id".to_owned(),
        ParameterValue::I64(1),
    )]));
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("filtered stream MySQL");
    assert_eq!(stream.schema().fields().len(), 2);
    let batch = stream
        .next_batch()
        .await
        .expect("filtered batch")
        .expect("filtered row");
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("filtered id")
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("filtered name")
            .value(0),
        "reference"
    );
}

#[tokio::test]
#[ignore = "richiede MySQL 8.4 live esplicito per lettura Arrow"]
#[allow(clippy::too_many_lines)]
async fn live_streaming_read_maps_scalar_and_xy_geometry_exactly() {
    use plenora_database_core::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use plenora_database_core::field_contract::validate_schema_contract;
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};
    use plenora_database_core::protocol;

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 10,
        memory_bytes: 8 * 1024 * 1024,
        output_bytes: 8 * 1024 * 1024,
        cell_bytes: 1024 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget read MySQL");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "catalog_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(1),
        filter: None,
    };
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream MySQL live");
    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("schema MySQL canonico");
    let geometry_field = schema.field_with_name("geom").expect("campo geometry");
    assert_eq!(
        geometry_field.metadata().get(protocol::GEOMETRY_DIMENSIONS),
        Some(&"xy".to_owned())
    );
    assert_eq!(
        geometry_field.metadata().get(protocol::GEOMETRY_SRID),
        Some(&"4326".to_owned())
    );
    assert_eq!(
        geometry_field
            .metadata()
            .get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"mixed".to_owned())
    );
    let point_field = schema.field_with_name("geom_point").expect("campo point");
    assert_eq!(
        point_field
            .metadata()
            .get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"exact".to_owned())
    );
    assert_eq!(
        point_field.metadata().get(protocol::GEOMETRY_TYPES),
        Some(&"point".to_owned())
    );
    let collection_field = schema
        .field_with_name("geom_collection")
        .expect("campo geometrycollection");
    assert_eq!(
        collection_field
            .metadata()
            .get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"exact".to_owned())
    );
    assert_eq!(
        collection_field.metadata().get(protocol::GEOMETRY_TYPES),
        Some(&"geometrycollection".to_owned())
    );

    let batch = stream
        .next_batch()
        .await
        .expect("batch MySQL")
        .expect("prima riga MySQL");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .expect("id int64")
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column_by_name("name")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .expect("name utf8")
            .value(0),
        "reference"
    );
    assert_eq!(
        batch
            .column_by_name("amount")
            .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
            .expect("amount decimal")
            .value(0),
        12_345_000
    );
    assert!(batch
        .column_by_name("active")
        .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
        .expect("active bool")
        .value(0));
    assert!(
        batch
            .column_by_name("event_date")
            .and_then(|array| array.as_any().downcast_ref::<Date32Array>())
            .expect("date32")
            .value(0)
            > 0
    );
    assert!(
        batch
            .column_by_name("event_ts")
            .and_then(|array| { array.as_any().downcast_ref::<TimestampMicrosecondArray>() })
            .expect("timestamp micros")
            .value(0)
            > 0
    );
    assert_eq!(
        batch
            .column_by_name("payload")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .expect("json utf8")
            .value(0),
        r#"{"qualified": true}"#
    );
    assert_eq!(
        batch
            .column_by_name("payload_bin")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .expect("varbinary")
            .value(0),
        &[0x00, 0x11, 0x22, 0x33, 0xAA, 0xBB, 0xCC, 0xDD]
    );
    let wkb = batch
        .column_by_name("geom")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .expect("geometry WKB")
        .value(0);
    let inspection =
        plenora_database_core::ewkb::inspect_ewkb_detailed(wkb, 16, 8).expect("WKB MySQL valido");
    assert_eq!(inspection.root.dimensions_label(), "xy");
    assert!(inspection.root.srid.is_none());
    let point_wkb = batch
        .column_by_name("geom_point")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .expect("point WKB")
        .value(0);
    let point_inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(point_wkb, 16, 8)
        .expect("POINT WKB MySQL valido");
    assert_eq!(point_inspection.root.dimensions_label(), "xy");
    let collection_wkb = batch
        .column_by_name("geom_collection")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .expect("geometrycollection WKB")
        .value(0);
    let collection_inspection =
        plenora_database_core::ewkb::inspect_ewkb_detailed(collection_wkb, 16, 8)
            .expect("GEOMETRYCOLLECTION WKB MySQL valido");
    assert_eq!(collection_inspection.root.dimensions_label(), "xy");
    assert!(stream
        .next_batch()
        .await
        .expect("fine stream MySQL")
        .is_none());
}
