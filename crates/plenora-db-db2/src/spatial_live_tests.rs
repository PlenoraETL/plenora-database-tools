use crate::{Db2Config, Db2Provider, Db2TlsMode};
use plenora_database_core::arrow::array::{Array, ArrayRef, BinaryArray, Int32Array};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    DeclaredCrs, FilterExpression, ObjectRef, OrderBy, ProviderKind, ReadOperation, SortDirection,
    SridPolicy, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::portable::{compile_portable, select, spatial};
use plenora_database_core::protocol::{self, contract_schema};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::spatial_predicate::{SpatialPredicate, SpatialReference};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn provider() -> Db2Provider {
    Db2Provider::new(
        Db2Config::new(
            environment("PLENORA_DB2_HOST", "db2"),
            environment("PLENORA_DB2_DATABASE", "plenora"),
            environment("PLENORA_DB2_USER", "db2inst1"),
        )
        .with_port(
            environment("PLENORA_DB2_PORT", "50000")
                .parse()
                .expect("porta Db2 spatial live"),
        )
        .with_tls_mode(Db2TlsMode::Disable),
    )
    .expect("provider Db2 spatial live")
}

fn secret() -> SecretString {
    SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"))
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 spatial live")
}

fn point_xy(x: f64, y: f64) -> Vec<u8> {
    let mut bytes = vec![1_u8];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes
}

fn point_xyz(x: f64, y: f64, z: f64) -> Vec<u8> {
    let mut bytes = vec![1_u8];
    bytes.extend_from_slice(&1_001_u32.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes.extend_from_slice(&z.to_le_bytes());
    bytes
}

fn read_operation(srid: u32) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: Some("PLENORA".to_owned()),
            schema: Some("PLENORA_TEST".to_owned()),
            object: "SPATIAL_PROBE".to_owned(),
        },
        projection: vec!["ID".to_owned(), "SHAPE".to_owned()],
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: vec![DeclaredCrs {
            column: "SHAPE".to_owned(),
            srid,
        }],
    }
}

fn fixture_read_operation(srid: u32) -> ReadOperation {
    let mut operation = read_operation(srid);
    operation.filter = Some(FilterExpression::Lte {
        field: "ID".to_owned(),
        parameter: "fixture_maximum_id".to_owned(),
    });
    operation
}

fn fixture_read_parameters() -> ParameterBag {
    ParameterBag::new(BTreeMap::from([(
        "fixture_maximum_id".to_owned(),
        ParameterValue::I32(4),
    )]))
}

fn write_schema(dimensions: &str) -> SchemaRef {
    contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("SHAPE", DataType::Binary, true).with_metadata(HashMap::from([
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
                "exact".to_owned(),
            ),
            (protocol::GEOMETRY_TYPES.to_owned(), "point".to_owned()),
            (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
            (
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "declared_unresolved".to_owned(),
            ),
            (
                protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                "geometry".to_owned(),
            ),
            (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
        ])),
    ])
}

struct OneBatch {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl OneBatch {
    fn new(batch: RecordBatch) -> Self {
        Self {
            schema: batch.schema(),
            batch: Some(batch),
        }
    }
}

impl BatchStream for OneBatch {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.batch.take()) })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(1)
    }
}

async fn cleanup(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione pulizia spatial Db2");
    transaction
        .execute(
            &Statement::new("DELETE FROM PLENORA_TEST.SPATIAL_PROBE WHERE ID >= 100"),
            cancellation,
        )
        .await
        .expect("pulizia righe spatial Db2");
    transaction
        .commit(cancellation)
        .await
        .expect("commit pulizia spatial Db2");
}

async fn write_batch(
    provider: &Db2Provider,
    secret: &SecretString,
    operation: &WriteOperation,
    schema: SchemaRef,
    batch: RecordBatch,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> WriteOutcome {
    let prepared = provider
        .prepare_write(secret, operation, schema, budget, cancellation)
        .await
        .expect("prepare write spatial Db2");
    provider
        .write(
            secret,
            prepared,
            Box::new(OneBatch::new(batch)),
            budget,
            cancellation,
        )
        .await
        .expect("write spatial Db2")
}

async fn collect_spatial_rows(
    stream: &mut dyn BatchStream,
    cancellation: &CancellationToken,
) -> Vec<(i32, Option<Vec<u8>>)> {
    let mut rows = Vec::new();
    while let Some(batch) = stream
        .next_batch(cancellation)
        .await
        .expect("batch spatial Db2")
    {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ID spatial Db2");
        let shapes = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("SHAPE spatial Db2");
        rows.extend((0..batch.num_rows()).map(|index| {
            (
                ids.value(index),
                (!shapes.is_null(index)).then(|| shapes.value(index).to_vec()),
            )
        }));
    }
    rows
}

async fn verify_spatial_roundtrip(
    provider: &Db2Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let mut verification = read_operation(4326);
    verification.filter = Some(FilterExpression::Gte {
        field: "ID".to_owned(),
        parameter: "minimum_id".to_owned(),
    });
    let parameters = ParameterBag::new(BTreeMap::from([(
        "minimum_id".to_owned(),
        ParameterValue::I32(100),
    )]));
    let mut stream = provider
        .read(secret, &verification, &parameters, budget, cancellation)
        .await
        .expect("verifica write spatial Db2");
    let rows = collect_spatial_rows(stream.as_mut(), cancellation).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (100, Some(point_xy(9.0, 10.0))));
    assert_eq!(rows[1], (102, Some(point_xyz(9.0, 10.0, 11.0))));
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_spatial_capabilities_and_streaming_wkb_are_evidence_backed() {
    let provider = provider();
    let secret = secret();
    let budget = budget();
    let cancellation = CancellationToken::new();
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("capability spatial Db2");
    assert!(capabilities.spatial.geometry);
    assert!(capabilities.spatial.read_wkb);
    assert!(capabilities.spatial.write_wkb);
    assert!(capabilities.spatial.requires_declared_crs);
    assert!(!capabilities.spatial.geography);
    assert!(!capabilities.spatial.spatial_index);

    let operation = fixture_read_operation(4326);
    let parameters = fixture_read_parameters();
    let mut stream = provider
        .read(&secret, &operation, &parameters, &budget, &cancellation)
        .await
        .expect("lettura WKB Db2");
    let output_schema = stream.schema();
    let field = output_schema.field_with_name("SHAPE").expect("campo SHAPE");
    assert_eq!(
        field.metadata().get(protocol::GEOARROW_EXTENSION_NAME),
        Some(&"geoarrow.wkb".to_owned())
    );
    let rows = collect_spatial_rows(stream.as_mut(), &cancellation).await;
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], (1, Some(point_xy(1.0, 2.0))));
    assert_eq!(rows[2], (3, None));

    let mismatch_operation = fixture_read_operation(3857);
    let mismatch_parameters = fixture_read_parameters();
    let mut mismatch = provider
        .read(
            &secret,
            &mismatch_operation,
            &mismatch_parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare CRS differente");
    assert_eq!(
        mismatch
            .next_batch(&cancellation)
            .await
            .expect_err("SRID diverso rifiutato per riga")
            .category,
        ErrorCategory::Crs
    );
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_spatial_write_round_trips_and_invalid_wkb_rolls_back() {
    let provider = provider();
    let secret = secret();
    let budget = budget();
    let cancellation = CancellationToken::new();
    cleanup(&provider, &secret, &budget, &cancellation).await;
    let schema = write_schema("xy");
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: Some("PLENORA".to_owned()),
            schema: Some("PLENORA_TEST".to_owned()),
            object: "SPATIAL_PROBE".to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: Some(SridPolicy::RequireMatch),
        create_spatial_index: false,
        allow_partial: false,
    };
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![100])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![Some(
                point_xy(9.0, 10.0).as_slice(),
            )])) as ArrayRef,
        ],
    )
    .expect("batch spatial Db2");
    let outcome = write_batch(
        &provider,
        &secret,
        &operation,
        schema.clone(),
        batch,
        &budget,
        &cancellation,
    )
    .await;
    assert_eq!(outcome.rows.confirmed, 1);

    let xyz_schema = write_schema("xyz");
    let xyz_batch = RecordBatch::try_new(
        xyz_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![102])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![Some(
                point_xyz(9.0, 10.0, 11.0).as_slice(),
            )])) as ArrayRef,
        ],
    )
    .expect("batch spatial XYZ Db2");
    write_batch(
        &provider,
        &secret,
        &operation,
        xyz_schema,
        xyz_batch,
        &budget,
        &cancellation,
    )
    .await;

    let invalid = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![101])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![Some(&[1_u8, 2, 3][..])])) as ArrayRef,
        ],
    )
    .expect("batch WKB invalido");
    let prepared = provider
        .prepare_write(&secret, &operation, schema, &budget, &cancellation)
        .await
        .expect("prepare write WKB invalido");
    let error = provider
        .write(
            &secret,
            prepared,
            Box::new(OneBatch::new(invalid)),
            &budget,
            &cancellation,
        )
        .await
        .expect_err("WKB invalido rifiutato");
    assert_eq!(error.category, ErrorCategory::DataMapping);

    verify_spatial_roundtrip(&provider, &secret, &budget, &cancellation).await;
    cleanup(&provider, &secret, &budget, &cancellation).await;
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_spatial_portable_predicates_execute_with_bound_wkb_and_srid() {
    let provider = provider();
    let secret = secret();
    let budget = budget();
    let cancellation = CancellationToken::new();
    for predicate in [
        SpatialPredicate::Intersects,
        SpatialPredicate::Contains,
        SpatialPredicate::Within,
    ] {
        let statement = select("SPATIAL_PROBE", vec!["ID"])
            .schema("PLENORA_TEST")
            .where_(spatial(
                "SHAPE",
                predicate,
                SpatialReference {
                    ewkb: point_xy(1.0, 2.0),
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();
        let statement =
            compile_portable(ProviderKind::Db2, &statement).expect("compila predicato spatial Db2");
        let mut transaction = provider
            .begin_transaction(
                &secret,
                &TransactionOptions::default(),
                &budget,
                &cancellation,
            )
            .await
            .expect("transazione predicato spatial Db2");
        let rows = transaction
            .query(&statement, &cancellation)
            .await
            .expect("esegue predicato spatial Db2");
        assert!(!rows.is_empty());
        transaction
            .rollback(&cancellation)
            .await
            .expect("chiude predicato spatial Db2");
    }
}
