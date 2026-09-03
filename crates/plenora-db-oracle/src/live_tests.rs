use crate::{OracleConfig, OracleProvider, OracleTlsMode};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, OrderBy, ProviderKind, ReadOperation, SortDirection,
    SridPolicy, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::portable::{
    compile_portable, eq, Expression, InsertStatement, PortableStatement, SelectStatement,
    TableRef, UpsertStatement,
};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, CommitOutcome, RemoteEffect};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

fn fixture() -> (OracleProvider, SecretString) {
    let (config, secret) = fixture_config();
    (
        OracleProvider::new(config).expect("config fixture Oracle"),
        secret,
    )
}

fn fixture_config() -> (OracleConfig, SecretString) {
    let host = std::env::var("PLENORA_ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("PLENORA_ORACLE_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1521);
    let service = std::env::var("PLENORA_ORACLE_SERVICE").unwrap_or_else(|_| "FREEPDB1".to_owned());
    let user = std::env::var("PLENORA_ORACLE_USER").unwrap_or_else(|_| "plenora".to_owned());
    let password =
        std::env::var("PLENORA_ORACLE_PASSWORD").unwrap_or_else(|_| "Plenora_Test_2026".to_owned());
    let config = OracleConfig::new(host, service, user)
        .with_port(port)
        .with_tls_mode(OracleTlsMode::Disable);
    (config, SecretString::new(password))
}

fn point_xy(x: f64, y: f64) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes
}

fn line_string_xy(points: usize) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(points)
            .expect("punti WKB Oracle")
            .to_le_bytes(),
    );
    for index in 0..points {
        let offset = f64::from(u32::try_from(index).expect("indice WKB Oracle")) * 0.01;
        bytes.extend_from_slice(&(12.0 + offset).to_le_bytes());
        bytes.extend_from_slice(&(41.0 + offset).to_le_bytes());
    }
    bytes
}

struct VecBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    rows: u64,
}

impl VecBatchStream {
    fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = batches
            .first()
            .expect("almeno un batch Oracle live")
            .schema();
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
                    Some(ProviderKind::Oracle),
                    plenora_database_core::ErrorPhase::Write,
                    "stream fixture Oracle cancellato",
                ));
            }
            Ok(self.batches.pop_front())
        })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.rows)
    }
}

fn spatial_write_batch(ids: Vec<i32>, points: Vec<Vec<u8>>) -> RecordBatch {
    let geometry = crate::types::OracleColumnSpec {
        name: "SHAPE".to_owned(),
        native_type: "SDO_GEOMETRY".to_owned(),
        nullable: false,
        kind: crate::types::OracleColumnKind::Geometry,
        spatial_srid: Some(4326),
        spatial_dimensions: Some(2),
        spatial_semantics: Some(plenora_database_core::geometry::SpatialSemantics::Geometry),
    }
    .arrow_field();
    let schema = contract_schema(vec![Field::new("ID", DataType::Int32, false), geometry]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(BinaryArray::from_iter_values(points)),
        ],
    )
    .expect("batch spatial Oracle")
}

fn spatial_write_operation(mode: WriteMode, create_index: bool) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: "PLENORA_ORACLE_ARROW_SPATIAL".to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: if mode == WriteMode::Create {
            TransactionProfile::BestEffortDdl
        } else {
            TransactionProfile::SingleTransaction
        },
        keys: matches!(
            mode,
            WriteMode::Create | WriteMode::Update | WriteMode::Upsert
        )
        .then(|| vec!["ID".to_owned()])
        .unwrap_or_default(),
        update_columns: matches!(mode, WriteMode::Update | WriteMode::Upsert)
            .then(|| vec!["SHAPE".to_owned()])
            .unwrap_or_default(),
        srid_policy: Some(SridPolicy::RequireMatch),
        create_spatial_index: create_index,
        allow_partial: false,
    }
}

fn key_delete_batch(ids: Vec<i32>) -> RecordBatch {
    let schema = contract_schema(vec![Field::new("ID", DataType::Int32, false)]);
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))])
        .expect("batch delete Oracle")
}

fn key_delete_operation() -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: "PLENORA_ORACLE_ARROW_SPATIAL".to_owned(),
        },
        mode: WriteMode::DeleteByKeys,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec!["ID".to_owned()],
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn run_spatial_write(
    provider: &OracleProvider,
    secret: &SecretString,
    budget: &ResourceBudget,
    operation: WriteOperation,
    batch: RecordBatch,
    cancellation: &CancellationToken,
) -> plenora_database_core::Result<plenora_database_core::outcome::WriteOutcome> {
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

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_arrow_scalar_create_and_read_preserves_supported_types() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Arrow Oracle");
    let table = "PLENORA_ORACLE_ARROW_SCALARS";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    let schema = contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("FLAG", DataType::Boolean, false),
        Field::new("BIG_VALUE", DataType::Int64, false),
        Field::new("F32_VALUE", DataType::Float32, false),
        Field::new("F64_VALUE", DataType::Float64, false),
        Field::new("AMOUNT", DataType::Decimal128(12, 2), false),
        Field::new("LABEL", DataType::Utf8, false),
        Field::new("PAYLOAD", DataType::Binary, false),
        Field::new("EVENT_DATE", DataType::Date32, false),
        Field::new(
            "OBSERVED_AT",
            DataType::Timestamp(
                plenora_database_core::arrow::schema::TimeUnit::Microsecond,
                None,
            ),
            false,
        ),
    ]);
    let decimal = Decimal128Array::from(vec![12_345_i128, 67_890_i128])
        .with_precision_and_scale(12, 2)
        .expect("decimal Arrow Oracle");
    let large_label = "€".repeat(1_669);
    let large_payload = vec![0xAB; 4_809];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int64Array::from(vec![9_000_000_000_i64, 9_000_000_001_i64])),
            Arc::new(Float32Array::from(vec![1.25_f32, 3.5_f32])),
            Arc::new(Float64Array::from(vec![2.5_f64, 4.75_f64])),
            Arc::new(decimal),
            Arc::new(StringArray::from(vec![large_label.as_str(), "second-row"])),
            Arc::new(BinaryArray::from_iter_values([
                large_payload.as_slice(),
                &b"\x01\xfe"[..],
            ])),
            Arc::new(Date32Array::from(vec![0, 1])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_234_567_i64,
                2_345_678_i64,
            ])),
        ],
    )
    .expect("batch scalare Arrow Oracle");
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: table.to_owned(),
        },
        mode: WriteMode::Create,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::BestEffortDdl,
        keys: vec!["ID".to_owned()],
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let prepared = provider
        .prepare_write(&secret, &operation, schema, &budget, &cancellation)
        .await
        .expect("prepare create scalare Arrow Oracle");
    provider
        .write(
            &secret,
            prepared,
            Box::new(VecBatchStream::new(vec![batch])),
            &budget,
            &cancellation,
        )
        .await
        .expect("create scalare Arrow Oracle");

    let read = ReadOperation {
        source: operation.target,
        projection: Vec::new(),
        filter: None,
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(2),
        row_offset: None,
        declared_crs: Vec::new(),
    };
    let mut stream = provider
        .read(
            &secret,
            &read,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("read scalare Arrow Oracle");
    let result = stream
        .next_batch(&cancellation)
        .await
        .expect("batch scalare Arrow Oracle")
        .expect("riga scalare Arrow Oracle");
    assert_eq!(result.num_rows(), 2);
    let flags = result
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("BOOLEAN Arrow Oracle");
    assert!(flags.value(0));
    assert!(!flags.value(1));
    assert!(result
        .column(5)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .is_some_and(|values| values.value(0) == 12_345));
    assert!(result
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .is_some_and(|values| values.value(0) == large_label));
    assert!(result
        .column(7)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .is_some_and(|values| values.value(0) == large_payload));
    let timestamps = [8_usize, 9_usize].map(|index| {
        result
            .column(index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("temporale Arrow Oracle")
            .value(0)
    });
    assert_eq!(timestamps, [0, 1_234_567]);
}

#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
async fn live_large_wkb_temporary_blob_bind_is_lossless() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget BLOB Oracle");
    let bytes = line_string_xy(300);
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin temporary BLOB Oracle");
    let rows = transaction
        .query(
            &Statement::new("SELECT DBMS_LOB.GETLENGTH(:1) AS N FROM DUAL").with_params(vec![
                ParameterValue::Wkb {
                    bytes: bytes.clone(),
                    srid: Some(4326),
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ]),
            &cancellation,
        )
        .await
        .expect("lunghezza temporary BLOB Oracle");
    assert_eq!(rows[0].get("N"), Some(&ParameterValue::I64(4_809)));
    let validation = transaction
        .query(
            &Statement::new(
                "SELECT MDSYS.SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(\
                 MDSYS.SDO_UTIL.FROM_WKBGEOMETRY(:1), 0.005) AS VALID FROM DUAL",
            )
            .with_params(vec![ParameterValue::Wkb {
                bytes,
                srid: Some(4326),
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geography,
            }]),
            &cancellation,
        )
        .await
        .expect("conversione temporary BLOB in SDO_GEOMETRY Oracle");
    assert_eq!(
        validation[0].get("VALID"),
        Some(&ParameterValue::String("TRUE".to_owned()))
    );
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback temporary BLOB Oracle");
}

#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_arrow_spatial_write_covers_create_append_update_upsert_replace_and_index() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget write Oracle");
    let mut cleanup = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("cleanup metadata begin");
    cleanup
        .execute(
            &Statement::new("DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = 'PLENORA_ORACLE_ARROW_SPATIAL'"),
            &cancellation,
        )
        .await
        .expect("cleanup metadata");
    cleanup
        .commit(&cancellation)
        .await
        .expect("cleanup metadata commit");
    let _ = provider
        .execute_ddl(
            &secret,
            "DROP TABLE PLENORA_ORACLE_ARROW_SPATIAL PURGE",
            &cancellation,
        )
        .await;

    let create = run_spatial_write(
        &provider,
        &secret,
        &budget,
        spatial_write_operation(WriteMode::Create, true),
        spatial_write_batch(vec![1], vec![point_xy(12.0, 41.0)]),
        &cancellation,
    )
    .await
    .expect("create Arrow Spatial Oracle");
    assert_eq!(create.rows.inserted, Some(1));

    run_spatial_write(
        &provider,
        &secret,
        &budget,
        spatial_write_operation(WriteMode::Append, false),
        spatial_write_batch(vec![2], vec![point_xy(13.0, 42.0)]),
        &cancellation,
    )
    .await
    .expect("append Arrow Spatial Oracle");
    run_spatial_write(
        &provider,
        &secret,
        &budget,
        spatial_write_operation(WriteMode::Update, false),
        spatial_write_batch(vec![2], vec![point_xy(13.5, 42.5)]),
        &cancellation,
    )
    .await
    .expect("update Arrow Spatial Oracle");
    run_spatial_write(
        &provider,
        &secret,
        &budget,
        spatial_write_operation(WriteMode::Upsert, false),
        spatial_write_batch(vec![2, 3], vec![point_xy(14.0, 43.0), point_xy(15.0, 44.0)]),
        &cancellation,
    )
    .await
    .expect("upsert Arrow Spatial Oracle");
    let deleted = run_spatial_write(
        &provider,
        &secret,
        &budget,
        key_delete_operation(),
        key_delete_batch(vec![3]),
        &cancellation,
    )
    .await
    .expect("delete_by_keys Arrow Oracle");
    assert_eq!(deleted.rows.deleted, Some(1));
    run_spatial_write(
        &provider,
        &secret,
        &budget,
        spatial_write_operation(WriteMode::Replace, false),
        spatial_write_batch(vec![4], vec![point_xy(16.0, 45.0)]),
        &cancellation,
    )
    .await
    .expect("replace Arrow Spatial Oracle");
    let rollback_error = run_spatial_write(
        &provider,
        &secret,
        &budget,
        spatial_write_operation(WriteMode::Append, false),
        spatial_write_batch(vec![5, 4], vec![point_xy(17.0, 46.0), point_xy(18.0, 47.0)]),
        &cancellation,
    )
    .await
    .expect_err("la seconda chiave duplicata deve annullare l'intero batch Oracle");
    assert_eq!(
        rollback_error.remote_effect,
        RemoteEffect::RolledBack,
        "{rollback_error:?}"
    );

    let description = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: None,
                    schema: Some("PLENORA".to_owned()),
                    object: "PLENORA_ORACLE_ARROW_SPATIAL".to_owned(),
                },
            },
            &cancellation,
        )
        .await
        .expect("catalogo write Spatial Oracle");
    assert!(description.document["indexes"]
        .as_array()
        .is_some_and(|indexes| indexes.iter().any(|index| index["spatial"] == true)));

    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: "PLENORA_ORACLE_ARROW_SPATIAL".to_owned(),
        },
        projection: vec!["ID".to_owned(), "SHAPE".to_owned()],
        filter: None,
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(10),
        row_offset: None,
        declared_crs: Vec::new(),
    };
    let mut stream = provider
        .read(
            &secret,
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("read dopo write Spatial Oracle");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch dopo write Spatial Oracle")
        .expect("batch presente");
    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<plenora_database_core::arrow::array::Int64Array>()
        .expect("ID NUMBER Oracle letto come Int64");
    assert_eq!(ids.value(0), 4);
}

#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_spatial_catalog_portable_predicates_and_arrow_wkb() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget spatial Oracle");
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("capability Spatial Oracle");
    assert!(capabilities.spatial.geometry);
    assert!(capabilities.spatial.geography);
    assert!(capabilities.spatial.read_wkb);
    assert!(capabilities.spatial.write_wkb);
    assert!(capabilities.spatial.spatial_index);

    let _ = provider
        .execute_ddl(
            &secret,
            "DROP TABLE PLENORA_ORACLE_SPATIAL PURGE",
            &cancellation,
        )
        .await;
    let mut metadata = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione metadata Spatial Oracle");
    metadata
        .execute(
            &Statement::new(
                "DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = 'PLENORA_ORACLE_SPATIAL'",
            ),
            &cancellation,
        )
        .await
        .expect("pulisce metadata Spatial Oracle");
    metadata
        .commit(&cancellation)
        .await
        .expect("commit pulizia metadata Spatial Oracle");
    provider
        .execute_ddl(
            &secret,
            "CREATE TABLE PLENORA_ORACLE_SPATIAL (ID NUMBER(10) PRIMARY KEY, SHAPE MDSYS.SDO_GEOMETRY)",
            &cancellation,
        )
        .await
        .expect("crea tabella Spatial Oracle");
    let mut setup = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione setup Spatial Oracle");
    setup
        .execute(
            &Statement::new(
                "INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES ('PLENORA_ORACLE_SPATIAL', 'SHAPE', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 4326)",
            ),
            &cancellation,
        )
        .await
        .expect("registra metadata Spatial Oracle");
    let point = point_xy(1.0, 2.0);
    let insert = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("PLENORA_ORACLE_SPATIAL"),
        columns: vec!["ID".to_owned(), "SHAPE".to_owned()],
        values: vec![vec![
            Expression::literal(ParameterValue::I32(1)),
            Expression::SpatialValue {
                expression: Box::new(Expression::literal(ParameterValue::Bytes(point.clone()))),
                srid: 4326,
                semantics: SpatialSemantics::Geography,
            },
        ]],
        returning: Vec::new(),
    });
    let insert =
        compile_portable(ProviderKind::Oracle, &insert).expect("compila insert Spatial Oracle");
    setup
        .execute(&insert, &cancellation)
        .await
        .expect("inserisce WKB Spatial Oracle");
    setup
        .commit(&cancellation)
        .await
        .expect("commit setup Spatial Oracle");
    provider
        .execute_ddl(
            &secret,
            "CREATE INDEX PLENORA_ORACLE_SPATIAL_SX ON PLENORA_ORACLE_SPATIAL (SHAPE) INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2",
            &cancellation,
        )
        .await
        .expect("crea indice Spatial Oracle");

    let description = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: Some("FREEPDB1".to_owned()),
                    schema: Some("PLENORA".to_owned()),
                    object: "PLENORA_ORACLE_SPATIAL".to_owned(),
                },
            },
            &cancellation,
        )
        .await
        .expect("catalogo Spatial Oracle");
    let description: crate::OracleObjectDescription =
        serde_json::from_value(description.document).expect("documento catalogo Spatial Oracle");
    let shape = description
        .columns
        .iter()
        .find(|column| column.name == "SHAPE")
        .expect("colonna SHAPE");
    assert_eq!(shape.spatial_srid, Some(4326));
    assert_eq!(shape.spatial_dimensions, Some(2));
    assert_eq!(shape.spatial_semantics, Some(SpatialSemantics::Geography));
    assert!(description.indexes.iter().any(|index| index.spatial));

    let predicate = plenora_database_core::portable::select("PLENORA_ORACLE_SPATIAL", vec!["ID"])
        .where_(plenora_database_core::portable::spatial(
            "SHAPE",
            plenora_database_core::spatial_predicate::SpatialPredicate::Intersects,
            plenora_database_core::spatial_predicate::SpatialReference {
                ewkb: point.clone(),
                srid: 4326,
                dimensions: plenora_database_core::geometry::Dimensions::Xy,
                semantics: SpatialSemantics::Geography,
            },
        ))
        .into_statement();
    let predicate = compile_portable(ProviderKind::Oracle, &predicate)
        .expect("compila predicato Spatial Oracle");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione predicato Spatial Oracle");
    assert_eq!(
        transaction
            .query(&predicate, &cancellation)
            .await
            .expect("esegue predicato Spatial Oracle")
            .len(),
        1
    );
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback predicato Spatial Oracle");

    let read = ReadOperation {
        source: ObjectRef {
            catalog: Some("FREEPDB1".to_owned()),
            schema: Some("PLENORA".to_owned()),
            object: "PLENORA_ORACLE_SPATIAL".to_owned(),
        },
        projection: vec!["ID".to_owned(), "SHAPE".to_owned()],
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Spatial {
            function: plenora_database_core::relational::SpatialFunction::Intersects,
            field: "SHAPE".to_owned(),
            geometry_parameter: Some("geometry".to_owned()),
            distance_parameter: None,
        }),
        declared_crs: Vec::new(),
    };
    let parameters = ParameterBag::new(BTreeMap::from([(
        "geometry".to_owned(),
        ParameterValue::Wkb {
            bytes: point.clone(),
            srid: Some(4326),
            dimensions: plenora_database_core::geometry::Dimensions::Xy,
            semantics: SpatialSemantics::Geography,
        },
    )]));
    let mut stream = provider
        .read(&secret, &read, &parameters, &budget, &cancellation)
        .await
        .expect("prepara read WKB Oracle");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("legge batch WKB Oracle")
        .expect("batch WKB Oracle presente");
    let shapes = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("SHAPE BinaryArray");
    assert!(!shapes.is_null(0));
    assert_eq!(shapes.value(0).len(), 21);
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream WKB Oracle")
        .is_none());
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_thin_driver_crud_merge_stream_and_rollback() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let info = provider
        .test_connection(&secret, &cancellation)
        .await
        .expect("connessione Oracle thin");
    assert_eq!(info.provider, ProviderKind::Oracle);

    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut bind_probe = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await
        .expect("bind probe begin");
    let bind_rows = bind_probe
        .query(
            &Statement::new("SELECT :1 AS VALUE FROM DUAL")
                .with_params(vec![ParameterValue::String("bound".to_owned())]),
            &cancellation,
        )
        .await
        .expect("bind probe query");
    assert!(
        matches!(bind_rows[0].get("VALUE"), Some(ParameterValue::String(value)) if value == "bound")
    );
    bind_probe
        .rollback(&cancellation)
        .await
        .expect("bind probe rollback");

    let _ = provider
        .execute_ddl(
            &secret,
            "DROP TABLE PLENORA_ORACLE_PROBE PURGE",
            &cancellation,
        )
        .await;
    provider
        .execute_ddl(
            &secret,
            "CREATE TABLE PLENORA_ORACLE_PROBE (ID NUMBER(19) PRIMARY KEY, VALUE VARCHAR2(100), VERSION_NO NUMBER(10))",
            &cancellation,
        )
        .await
        .expect("create probe");

    provider
        .inspect(&secret, &Operation::DatabaseListCatalogs, &cancellation)
        .await
        .expect("list catalogs");
    provider
        .inspect(
            &secret,
            &Operation::DatabaseListSchemas { source: None },
            &cancellation,
        )
        .await
        .expect("list schemas");
    provider
        .inspect(
            &secret,
            &Operation::DatabaseListObjects {
                source: Some(ObjectRef {
                    catalog: None,
                    schema: Some("PLENORA".to_owned()),
                    object: String::new(),
                }),
            },
            &cancellation,
        )
        .await
        .expect("list objects");

    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: None,
                    schema: Some("PLENORA".to_owned()),
                    object: "PLENORA_ORACLE_PROBE".to_owned(),
                },
            },
            &cancellation,
        )
        .await
        .expect("describe probe");
    assert_eq!(inspection.operation, "database.describe_object");
    assert_eq!(
        inspection.document["columns"].as_array().map(Vec::len),
        Some(3)
    );
    assert!(inspection.document["schema_token"]
        .as_str()
        .is_some_and(|token| token.starts_with("sha256:")));

    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");

    let insert = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("PLENORA_ORACLE_PROBE"),
        columns: vec!["ID".to_owned(), "VALUE".to_owned(), "VERSION_NO".to_owned()],
        values: vec![vec![
            Expression::literal(ParameterValue::I64(1)),
            Expression::literal(ParameterValue::String("first".to_owned())),
            Expression::literal(ParameterValue::I32(1)),
        ]],
        returning: Vec::new(),
    });
    let insert = compile_portable(ProviderKind::Oracle, &insert).expect("compile insert");
    assert_eq!(
        transaction
            .execute(&insert, &cancellation)
            .await
            .expect("insert"),
        1
    );

    transaction
        .savepoint("before_merge", &cancellation)
        .await
        .expect("savepoint");
    let merge = PortableStatement::Upsert(UpsertStatement {
        table: TableRef::new("PLENORA_ORACLE_PROBE"),
        columns: vec!["ID".to_owned(), "VALUE".to_owned(), "VERSION_NO".to_owned()],
        values: vec![vec![
            Expression::literal(ParameterValue::I64(1)),
            Expression::literal(ParameterValue::String("source".to_owned())),
            Expression::literal(ParameterValue::I32(2)),
        ]],
        conflict_target: vec!["ID".to_owned()],
        update_on_conflict: vec![
            (
                "VALUE".to_owned(),
                Expression::literal(ParameterValue::String("merged".to_owned())),
            ),
            (
                "VERSION_NO".to_owned(),
                Expression::literal(ParameterValue::I32(2)),
            ),
        ],
        returning: Vec::new(),
    });
    let merge = compile_portable(ProviderKind::Oracle, &merge).expect("compile merge");
    assert_eq!(
        transaction
            .execute(&merge, &cancellation)
            .await
            .expect("merge"),
        1
    );

    let select = PortableStatement::Select(SelectStatement {
        table: TableRef::new("PLENORA_ORACLE_PROBE"),
        projection: plenora_database_core::portable::Projection::Columns(vec![
            "ID".to_owned(),
            "VALUE".to_owned(),
            "VERSION_NO".to_owned(),
        ]),
        filter: Some(eq("ID", ParameterValue::I64(1))),
        order_by: Vec::new(),
        limit: Some(1),
    });
    let select = compile_portable(ProviderKind::Oracle, &select).expect("compile select");
    let rows = transaction
        .query(&select, &cancellation)
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(rows[0].get("VALUE"), Some(ParameterValue::String(value)) if value == "merged")
    );

    let mut stream = transaction
        .query_stream(&select, 1, &cancellation)
        .await
        .expect("query stream");
    assert_eq!(
        stream
            .next_batch(&cancellation)
            .await
            .expect("batch")
            .expect("row")
            .len(),
        1
    );
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("end")
        .is_none());
    drop(stream);

    transaction
        .rollback_to_savepoint("before_merge", &cancellation)
        .await
        .expect("rollback savepoint");
    transaction
        .release_savepoint("before_merge", &cancellation)
        .await
        .expect("release emulata");
    assert!(matches!(
        transaction.commit(&cancellation).await.expect("commit"),
        CommitOutcome::Committed
    ));

    let mut verify = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await
        .expect("verify begin");
    let rows = verify
        .query(&select, &cancellation)
        .await
        .expect("verify query");
    assert!(
        matches!(rows[0].get("VALUE"), Some(ParameterValue::String(value)) if value == "first")
    );
    verify
        .rollback(&cancellation)
        .await
        .expect("verify rollback");

    provider
        .execute_ddl(
            &secret,
            "DROP TABLE PLENORA_ORACLE_PROBE PURGE",
            &cancellation,
        )
        .await
        .expect("drop probe");
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_driver_errors_are_redacted() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");
    let marker = "PLENORA_SECRET_IDENTIFIER_79F";
    let error = transaction
        .query(
            &Statement::new(format!("SELECT * FROM {marker}")),
            &cancellation,
        )
        .await
        .expect_err("tabella inesistente");
    assert!(!error.message.contains(marker));
    let rollback = transaction
        .rollback(&cancellation)
        .await
        .expect_err("il driver chiude il canale dopo l'errore server");
    assert_eq!(rollback.category, plenora_database_core::ErrorCategory::Io);
    assert!(!rollback.message.contains(marker));
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_type_fidelity_includes_utc_timestamptz_and_lobs() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");
    let rows = transaction
        .query(
            &Statement::new(
                "SELECT CAST(:1 AS NUMBER(38,10)) AS N, CAST(:2 AS DATE) AS D, \
                 CAST(:3 AS TIMESTAMP) AS TS, \
                 TO_TIMESTAMP_TZ(:4, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM') AS TZ, \
                 CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS NULL_TZ, \
                 TO_CLOB(RPAD('x', 4000, 'x')) || RPAD('x', 4000, 'x') || \
                   RPAD('x', 4000, 'x') || RPAD('x', 4000, 'x') || \
                   RPAD('x', 4000, 'x') AS TEXT_LOB, \
                 TO_BLOB(HEXTORAW('0001FF')) AS BINARY_LOB FROM DUAL",
            )
            .with_params(vec![
                ParameterValue::Decimal("12345678901234567890.1234567890".to_owned()),
                ParameterValue::Date("2026-03-19".to_owned()),
                ParameterValue::Timestamp("2026-03-19T10:11:12.123456".to_owned()),
                ParameterValue::TimestampTz("2026-03-19T10:11:12.123456+02:30".to_owned()),
            ]),
            &cancellation,
        )
        .await
        .expect("type query");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert!(
        matches!(row.get("N"), Some(ParameterValue::Decimal(value)) if value == "12345678901234567890.123456789")
    );
    assert!(
        matches!(row.get("D"), Some(ParameterValue::Timestamp(value)) if value == "2026-03-19T00:00:00")
    );
    assert!(
        matches!(row.get("TS"), Some(ParameterValue::Timestamp(value)) if value == "2026-03-19T10:11:12.123456")
    );
    assert_eq!(
        row.get("TZ"),
        Some(&ParameterValue::TimestampTz(
            "2026-03-19T10:11:12.123456+02:30".to_owned()
        ))
    );
    assert!(
        matches!(row.get("NULL_TZ"), Some(ParameterValue::Null { type_name }) if type_name == "timestamptz")
    );
    assert!(
        matches!(row.get("TEXT_LOB"), Some(ParameterValue::String(value)) if value.len() == 20_000)
    );
    assert!(
        matches!(row.get("BINARY_LOB"), Some(ParameterValue::Bytes(value)) if value == &[0, 1, 255])
    );
    transaction.rollback(&cancellation).await.expect("rollback");
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_configurable_pool_bounds_waiters_and_reuses_after_rollback() {
    let (config, secret) = fixture_config();
    let provider = OracleProvider::new_with_pool(
        config.with_acquire_timeout(std::time::Duration::from_millis(100)),
        1,
    )
    .expect("pool Oracle bounded");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget pool Oracle");
    let transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("primo lease Oracle");
    let error = provider
        .test_connection(&secret, &cancellation)
        .await
        .expect_err("secondo lease oltre capacita");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Timeout
    );
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback libera lease Oracle");
    provider
        .test_connection(&secret, &cancellation)
        .await
        .expect("connessione riusata dopo rollback");
    provider
        .execute_ddl(
            &secret,
            "DROP TABLE PLENORA_ORACLE_POOL_MISSING PURGE",
            &cancellation,
        )
        .await
        .expect_err("DDL fallita per provare la quarantena del canale");
    provider
        .test_connection(&secret, &cancellation)
        .await
        .expect("nuova connessione dopo quarantena del canale fallito");
}

#[tokio::test]
#[ignore = "richiede listener Oracle TCPS e CA del gate"]
async fn live_tcps_verifies_private_ca_and_rejects_untrusted_server() {
    let (plain, secret) = fixture_config();
    let port = std::env::var("PLENORA_ORACLE_TCPS_PORT")
        .expect("porta TCPS del gate")
        .parse::<u16>()
        .expect("porta TCPS numerica");
    let ca = std::env::var("PLENORA_ORACLE_TCPS_CA").expect("CA TCPS del gate");
    let trusted = OracleProvider::new(
        OracleConfig::new(plain.host(), plain.service_name(), plain.username())
            .with_port(port)
            .with_private_ca_certificate(ca),
    )
    .expect("config TCPS trusted");
    let cancellation = CancellationToken::new();
    trusted
        .test_connection(&secret, &cancellation)
        .await
        .expect("TCPS con CA privata verificata");

    let untrusted = OracleProvider::new(
        OracleConfig::new(plain.host(), plain.service_name(), plain.username()).with_port(port),
    )
    .expect("config TCPS senza CA privata");
    let error = untrusted
        .test_connection(&secret, &cancellation)
        .await
        .expect_err("certificato fixture non deve essere pubblicamente trusted");
    assert!(!error.message.contains(secret.expose()));
}
