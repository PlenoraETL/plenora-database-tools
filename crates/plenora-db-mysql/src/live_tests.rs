use crate::{
    describe_object, list_objects, list_schemas, probe_server, MysqlCertificatePolicy, MysqlConfig,
    MysqlProvider, MysqlSession,
};
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::{CancellationToken, ResourceBudget, ResourceLimits};

const DEFAULT_PASSWORD: &str = "DataFlow_Test_2026!";

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn live_secret() -> SecretString {
    SecretString::new(environment("PLENORA_MYSQL_PASSWORD", DEFAULT_PASSWORD))
}

fn live_config() -> MysqlConfig {
    MysqlConfig::new(
        environment("PLENORA_MYSQL_HOST", "127.0.0.1"),
        environment("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        environment("PLENORA_MYSQL_USER", "dataflow"),
        live_secret(),
    )
    .with_port(
        environment("PLENORA_MYSQL_PORT", "3306")
            .parse()
            .expect("porta MySQL live"),
    )
    .with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate)
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
    assert_eq!(first.token, second.token);
    assert_eq!(first.engine.as_deref(), Some("InnoDB"));
    let geometry = first
        .columns
        .iter()
        .find(|column| column.name == "geom")
        .expect("colonna geometry");
    assert_eq!(geometry.data_type, "geometry");
    assert_eq!(geometry.spatial_srid, Some(4_326));
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
    let geometry_field = schema.field_with_name("geom").expect("campo geometry");
    assert_eq!(
        geometry_field.metadata().get(protocol::GEOMETRY_DIMENSIONS),
        Some(&"xy".to_owned())
    );
    assert_eq!(
        geometry_field.metadata().get(protocol::GEOMETRY_SRID),
        Some(&"4326".to_owned())
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
    let wkb = batch
        .column_by_name("geom")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .expect("geometry WKB")
        .value(0);
    let inspection =
        plenora_database_core::ewkb::inspect_ewkb_detailed(wkb, 16, 8).expect("WKB MySQL valido");
    assert_eq!(inspection.root.dimensions_label(), "xy");
    assert!(inspection.root.srid.is_none());
    assert!(stream
        .next_batch()
        .await
        .expect("fine stream MySQL")
        .is_none());
}
