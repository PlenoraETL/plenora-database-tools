//! Fixture B2 riusate sui runner PostgreSQL, mai eseguite implicitamente.
use crate::*;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use plenora_database_core::loss::LossConsent;
use plenora_database_core::plan::WriteMode;
use plenora_database_core::provider::Provider;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use serde_json::Value;
use std::collections::VecDeque;

struct Input(SchemaRef, VecDeque<RecordBatch>);
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.1.pop_front())))
    }
}

fn binary(value: &Value) -> Option<Vec<u8>> {
    value.as_str().map(|s| {
        s.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    })
}

#[tokio::test]
#[ignore = "richiede il fixture PostgreSQL della campagna"]
async fn live_b07_empty_create_metadata_consent_and_read_loss() {
    use plenora_database_core::loss::MappingPolicy;
    let _serial = crate::write_visibility_live_tests::SERIAL.lock().await;
    let secret =
        SecretString::new(crate::write_visibility_live_tests::live_dsn().expect("DSN runner"));
    let provider = PostgresProvider::insecure_local_with_batch_rows(1);
    let client = PostgresProvider::connect(&secret).await.unwrap();
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS plenora_fixture")
        .await
        .unwrap();
    let base = plenora_database_core::protocol::contract_schema(vec![
        arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, true),
        arrow_schema::Field::new("b", arrow_schema::DataType::Utf8, true),
    ])
    .unwrap();
    let mut metadata = base.metadata().clone();
    metadata.insert("opaque", "x".repeat(crate::column_metadata::COLUMN_LIMIT));
    let schema = Arc::new(Schema::new_with_metadata(base.fields().clone(), metadata));
    for (policy, consent, accepted) in [
        (MappingPolicy::Strict, LossConsent::Accept, false),
        (MappingPolicy::Compatible, LossConsent::Deny, false),
        (MappingPolicy::Compatible, LossConsent::Accept, true),
    ] {
        client
            .batch_execute("DROP TABLE IF EXISTS plenora_fixture.b07_empty")
            .await
            .unwrap();
        let mut operation =
            crate::write_visibility_live_tests::operation("b07_empty", WriteMode::Create);
        operation.target.schema = Some("plenora_fixture".to_owned());
        operation.mapping_policy = policy;
        let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
        let cancel = CancellationToken::new();
        let prepared = provider
            .prepare_write(&secret, &operation, schema.clone(), &budget, &cancel)
            .await
            .unwrap();
        let result = provider
            .write(
                &secret,
                prepared,
                Box::new(Input(schema.clone(), VecDeque::new())),
                &budget,
                &cancel,
                consent,
            )
            .await;
        assert_eq!(result.is_ok(), accepted);
        let exists: bool = client
            .query_one(
                "SELECT to_regclass('plenora_fixture.b07_empty') IS NOT NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(exists, accepted, "rifiuto prima della DDL");
        if let Ok(outcome) = result {
            outcome.validate().unwrap();
            assert_eq!(outcome.rows.confirmed, 0);
            let plenora_database_core::execution_loss::VersionedLossReport::Execution(report) =
                outcome.loss_report.unwrap()
            else {
                panic!("rapporto esecutivo");
            };
            assert_eq!(report.effective.len(), 1);
            assert_eq!(report.effective[0].committed, 1);
            let read = ReadOperation {
                source: operation.target,
                projection: Vec::new(),
                order_by: Vec::new(),
                row_limit: None,
                row_offset: None,
                filter: None,
                declared_crs: Vec::new(),
            };
            let mut stream = provider
                .read(&secret, &read, &ParameterBag::default(), &budget, &cancel)
                .await
                .unwrap();
            assert!(stream.next_batch(&cancel).await.unwrap().is_none());
            let loss = stream.read_loss().unwrap();
            assert_eq!(loss.schema_version, 2);
            assert_eq!(
                loss.entries.len(),
                1,
                "schema loss deduplicated across replicas"
            );
        } else {
            assert_eq!(
                result.unwrap_err().remote_effect,
                plenora_database_core::RemoteEffect::None
            );
        }
    }
    client
        .batch_execute("DROP TABLE plenora_fixture.b07_empty")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "richiede il fixture PostgreSQL/PostGIS della campagna"]
#[allow(clippy::too_many_lines)]
async fn live_geometry_qualification_registered_arrow_roundtrip() {
    let _serial = crate::write_visibility_live_tests::SERIAL.lock().await;
    let dsn = crate::write_visibility_live_tests::live_dsn().expect("DSN del runner");
    let secret = SecretString::new(dsn);
    let provider = PostgresProvider::insecure_local_with_batch_rows(1);
    let client = PostgresProvider::connect(&secret).await.unwrap();
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS plenora_fixture")
        .await
        .unwrap();
    let source: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/postgres_geometry_qualification.json"
    ))
    .unwrap();
    for (index, fixture) in source["fixtures"].as_array().unwrap().iter().enumerate() {
        let values = fixture["fields"].as_array().unwrap();
        let fields = values
            .iter()
            .map(crate::column_metadata::tests::fixture_field)
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            serde_json::from_value::<std::collections::HashMap<String, String>>(
                fixture["schema"].clone(),
            )
            .unwrap(),
        ));
        let object = format!("geometry_b2_{index}");
        let mut operation =
            crate::write_visibility_live_tests::operation(&object, WriteMode::Create);
        operation.target.schema = Some("plenora_fixture".to_owned());
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS plenora_fixture.{object}"))
            .await
            .unwrap();
        if matches!(
            fixture["name"].as_str().unwrap(),
            "axis_lat_lon" | "conflicting_crs" | "dimensions_unknown" | "uint64_ordering"
        ) {
            assert!(
                compile_write_plan(&schema, &operation).is_err(),
                "{}",
                fixture["name"]
            );
            continue;
        }
        let arrays: Vec<ArrayRef> = values
            .iter()
            .map(|field| {
                let values = field["values"].as_array().unwrap();
                match field["type"].as_str().unwrap() {
                    "int64" => Arc::new(Int64Array::from(
                        values.iter().map(Value::as_i64).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    "string" => Arc::new(StringArray::from(
                        values.iter().map(Value::as_str).collect::<Vec<_>>(),
                    )),
                    "binary" => {
                        let bytes = values.iter().map(binary).collect::<Vec<_>>();
                        Arc::new(BinaryArray::from_iter(bytes.iter().map(Option::as_deref)))
                    }
                    _ => panic!("tipo fixture non ammesso"),
                }
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
        let cancellation = CancellationToken::new();
        let prepared = provider
            .prepare_write(&secret, &operation, schema.clone(), &budget, &cancellation)
            .await
            .unwrap();
        provider
            .write(
                &secret,
                prepared,
                Box::new(Input(schema.clone(), VecDeque::from([batch.clone()]))),
                &budget,
                &cancellation,
                LossConsent::Deny,
            )
            .await
            .unwrap();
        let read = ReadOperation {
            source: operation.target.clone(),
            projection: Vec::new(),
            order_by: Vec::new(),
            row_limit: None,
            row_offset: None,
            filter: None,
            declared_crs: Vec::new(),
        };
        let read_budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
        let mut stream = provider
            .read(
                &secret,
                &read,
                &ParameterBag::default(),
                &read_budget,
                &cancellation,
            )
            .await
            .unwrap();
        assert_eq!(stream.schema(), schema, "{}", fixture["name"]);
        let mut row = 0;
        while let Some(actual) = stream.next_batch(&cancellation).await.unwrap() {
            assert_eq!(
                actual,
                batch.slice(row, actual.num_rows()),
                "{}",
                fixture["name"]
            );
            row += actual.num_rows();
        }
        assert_eq!(row, batch.num_rows());
        let report = stream.read_loss().unwrap();
        assert_eq!(
            report.status,
            plenora_database_core::read_loss::ReadStatus::Complete
        );
        assert!(report.entries.is_empty());
        client
            .batch_execute(&format!("DROP TABLE plenora_fixture.{object}"))
            .await
            .unwrap();
    }
}
