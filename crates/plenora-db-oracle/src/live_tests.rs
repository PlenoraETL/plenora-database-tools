use crate::{OracleConfig, OracleProvider, OracleTlsMode};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::checkpoint::ReadCheckpoint;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::identifier::{quote_identifier, validate_identifier, IdentifierDialect};
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
use plenora_database_core::relational::{
    ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource, SpatialFunction,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, CommitOutcome, RemoteEffect};
use plenora_database_sql::{
    oracle_spatial_shape, Dialect, DialectCapabilities, OraclePointPosition, OracleSpatialShape,
    Renderer,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

pub fn fixture() -> (OracleProvider, SecretString) {
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

fn point_xyz(x: f64, y: f64, z: f64) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&0x8000_0001_u32.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes.extend_from_slice(&z.to_le_bytes());
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

fn polygon_xy(min: f64, max: f64) -> Vec<u8> {
    let coordinates = [(min, min), (max, min), (max, max), (min, max), (min, min)];
    let mut bytes = vec![1];
    bytes.extend_from_slice(&3_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&5_u32.to_le_bytes());
    for (x, y) in coordinates {
        bytes.extend_from_slice(&x.to_le_bytes());
        bytes.extend_from_slice(&y.to_le_bytes());
    }
    bytes
}

fn spatial_query(table: &str, expression: QueryExpression) -> QueryOperation {
    QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("PLENORA".to_owned()),
                object: table.to_owned(),
            },
            alias: Some("T".to_owned()),
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression,
            alias: Some("VALUE".to_owned()),
        }],
        joins: Vec::new(),
        filter: Some(QueryExpression::Compare {
            left: Box::new(QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("T".to_owned()),
                    field: "ID".to_owned(),
                },
            }),
            operator: plenora_database_core::plan::ComparisonOperator::Eq,
            right: Box::new(QueryExpression::TypedParameter {
                name: "row_id".to_owned(),
                parameter_type: plenora_database_core::relational::QueryParameterType::Integer,
            }),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(1),
        row_offset: None,
        locking: None,
        declared_crs: Vec::new(),
    }
}

fn spatial_column(field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some("T".to_owned()),
            field: field.to_owned(),
        },
    }
}

fn spatial_parameter(bytes: Vec<u8>, srid: u32, semantics: SpatialSemantics) -> ParameterValue {
    ParameterValue::Wkb {
        bytes,
        srid: Some(srid),
        dimensions: Dimensions::Xy,
        semantics,
    }
}

#[allow(clippy::too_many_lines)]
async fn qualify_spatial_surface(
    provider: &OracleProvider,
    secret: &SecretString,
    table: &str,
    srid: u32,
    semantics: SpatialSemantics,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let renderer = Renderer::new(
        Dialect::Oracle,
        DialectCapabilities {
            spatial_intersects: true,
        },
    );
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione censimento Spatial Oracle");
    for function in SpatialFunction::ALL {
        let Some(shape) = oracle_spatial_shape(*function) else {
            continue;
        };
        let field = match function {
            SpatialFunction::X | SpatialFunction::Y | SpatialFunction::Z => "POINT_SHAPE",
            SpatialFunction::NPoints
            | SpatialFunction::StartPoint
            | SpatialFunction::EndPoint
            | SpatialFunction::PointN
            | SpatialFunction::Length => "LINE_SHAPE",
            _ => "POLY_SHAPE",
        };
        let mut arguments = vec![spatial_column(field)];
        let mut parameters = BTreeMap::from([("row_id".to_owned(), ParameterValue::I32(1))]);
        // Regola P (`spatial_probe::judge_probe_positions`, 9fb015b): una
        // sonda nella projection non e ammessa, anche dove il catalogo la
        // proverebbe. La forma con la sonda si rifiuta compilando; la funzione
        // si esegue con la seconda geometria presa da una colonna della stessa
        // tabella.
        let probed = matches!(
            shape,
            OracleSpatialShape::Relate(_)
                | OracleSpatialShape::Disjoint
                | OracleSpatialShape::BinaryGeometry(_)
                | OracleSpatialShape::BinaryMeasure { .. }
                | OracleSpatialShape::DWithin
        );
        match shape {
            OracleSpatialShape::Relate(_) => {
                let bytes = match function {
                    SpatialFunction::Within | SpatialFunction::CoveredBy => polygon_xy(-1.0, 11.0),
                    SpatialFunction::Touches => polygon_xy(10.0, 20.0),
                    SpatialFunction::Overlaps => polygon_xy(5.0, 15.0),
                    SpatialFunction::Equals => polygon_xy(0.0, 10.0),
                    _ => point_xy(5.0, 5.0),
                };
                arguments.push(QueryExpression::Parameter {
                    name: "geometry".to_owned(),
                });
                parameters.insert(
                    "geometry".to_owned(),
                    spatial_parameter(bytes, srid, semantics),
                );
            }
            OracleSpatialShape::Disjoint => {
                arguments.push(QueryExpression::Parameter {
                    name: "geometry".to_owned(),
                });
                parameters.insert(
                    "geometry".to_owned(),
                    spatial_parameter(point_xy(30.0, 30.0), srid, semantics),
                );
            }
            OracleSpatialShape::BinaryGeometry(_) | OracleSpatialShape::BinaryMeasure { .. } => {
                arguments.push(QueryExpression::Parameter {
                    name: "geometry".to_owned(),
                });
                parameters.insert(
                    "geometry".to_owned(),
                    spatial_parameter(polygon_xy(5.0, 15.0), srid, semantics),
                );
            }
            OracleSpatialShape::DWithin => {
                arguments.push(QueryExpression::Parameter {
                    name: "geometry".to_owned(),
                });
                arguments.push(QueryExpression::Parameter {
                    name: "distance".to_owned(),
                });
                parameters.insert(
                    "geometry".to_owned(),
                    spatial_parameter(point_xy(5.0, 5.0), srid, semantics),
                );
                parameters.insert("distance".to_owned(), ParameterValue::F64(10.0));
            }
            OracleSpatialShape::Point(OraclePointPosition::Argument) => {
                arguments.push(QueryExpression::Parameter {
                    name: "position".to_owned(),
                });
                parameters.insert("position".to_owned(), ParameterValue::I32(2));
            }
            OracleSpatialShape::SetSrid | OracleSpatialShape::Transform => {
                arguments.push(QueryExpression::Parameter {
                    name: "srid".to_owned(),
                });
                parameters.insert("srid".to_owned(), ParameterValue::I64(i64::from(srid)));
            }
            OracleSpatialShape::Buffer => {
                arguments.push(QueryExpression::Parameter {
                    name: "distance".to_owned(),
                });
                parameters.insert("distance".to_owned(), ParameterValue::F64(10.0));
            }
            _ => {}
        }
        if probed {
            let refused = renderer
                .render_query(&spatial_query(
                    table,
                    QueryExpression::Spatial {
                        function: *function,
                        arguments: arguments.clone(),
                    },
                ))
                .expect_err("sonda nella projection rifiutata");
            assert_eq!(
                refused.message,
                plenora_database_core::spatial_probe::PROBE_OUTSIDE_PREDICATE,
                "{function:?}"
            );
            parameters.remove("geometry");
            let other = if field == "POINT_SHAPE" {
                "POLY_SHAPE"
            } else {
                "POINT_SHAPE"
            };
            arguments[1] = spatial_column(other);
        }
        let query = spatial_query(
            table,
            QueryExpression::Spatial {
                function: *function,
                arguments,
            },
        );
        let rendered_query = renderer
            .render_query(&query)
            .unwrap_or_else(|error| panic!("render {function:?} Oracle: {error}"));
        let values = rendered_query
            .binds
            .iter()
            .map(|bind| {
                parameters
                    .get(&bind.name)
                    .unwrap_or_else(|| panic!("bind {} per {function:?}", bind.name))
                    .clone()
            })
            .collect();
        let rows = transaction
            .query(
                &Statement::new(rendered_query.sql).with_params(values),
                cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("esecuzione {function:?} Oracle: {error}"));
        assert_eq!(rows.len(), 1, "{function:?}");
        assert!(rows[0].get("VALUE").is_some(), "{function:?}");
        if function.returns_geometry() {
            assert!(
                matches!(rows[0].get("VALUE"), Some(ParameterValue::Bytes(bytes)) if !bytes.is_empty()),
                "{function:?}"
            );
        }
    }
    transaction
        .rollback(cancellation)
        .await
        .expect("rollback censimento Spatial Oracle");
}

pub struct VecBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    rows: u64,
}

impl VecBatchStream {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
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

/// Il produttore che, finiti i batch, non risponde piu: la scrittura resta
/// in attesa e il chiamante la abbandona.
struct StallingBatchStream(VecBatchStream);

impl BatchStream for StallingBatchStream {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        if self.0.batches.is_empty() {
            return Box::pin(std::future::pending());
        }
        self.0.next_batch(cancellation)
    }

    fn declared_input_rows(&self) -> Option<u64> {
        self.0.declared_input_rows()
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
    .arrow_field()
    .expect("campo Arrow geometry");
    let schema =
        contract_schema(vec![Field::new("ID", DataType::Int32, false), geometry]).expect("schema");
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(BinaryArray::from_iter_values(points)),
        ],
    )
    .expect("batch spatial Oracle")
}

fn spatial_xyz_write_batch(ids: Vec<i32>, points: Vec<Vec<u8>>) -> RecordBatch {
    let geometry = crate::types::OracleColumnSpec {
        name: "SHAPE".to_owned(),
        native_type: "SDO_GEOMETRY".to_owned(),
        nullable: false,
        kind: crate::types::OracleColumnKind::Geometry,
        spatial_srid: Some(3_857),
        spatial_dimensions: Some(3),
        spatial_semantics: Some(SpatialSemantics::Geometry),
    }
    .arrow_field()
    .expect("campo Arrow geometry");
    let schema =
        contract_schema(vec![Field::new("ID", DataType::Int32, false), geometry]).expect("schema");
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(BinaryArray::from_iter_values(points)),
        ],
    )
    .expect("batch spatial XYZ Oracle")
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
    let schema = contract_schema(vec![Field::new("ID", DataType::Int32, false)]).expect("schema");
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
    ])
    .expect("schema");
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
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_array_dml_crosses_batches_and_rolls_back_atomically() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget array DML");
    let table = "PLENORA_ORACLE_ARRAY_DML";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, LABEL VARCHAR2(40))"),
            &cancellation,
        )
        .await
        .expect("crea fixture array DML Oracle");
    let schema = contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("LABEL", DataType::Utf8, false),
    ])
    .expect("schema");
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: table.to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let make_batch = |ids: Vec<i32>| {
        let labels = ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .expect("batch array DML Oracle")
    };
    let mut invalid_ids = (1..=300).collect::<Vec<_>>();
    invalid_ids.push(1);
    let invalid = make_batch(invalid_ids);
    let prepared = provider
        .prepare_write(
            &secret,
            &operation,
            Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare array DML Oracle fallibile");
    let error = provider
        .write(
            &secret,
            prepared,
            Box::new(VecBatchStream::new(vec![invalid])),
            &budget,
            &cancellation,
        )
        .await
        .expect_err("duplicato array DML deve fallire");
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{error:?}");

    let valid = make_batch((1..=600).collect());
    let prepared = provider
        .prepare_write(
            &secret,
            &operation,
            Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare array DML Oracle");
    let outcome = provider
        .write(
            &secret,
            prepared,
            Box::new(VecBatchStream::new(vec![valid])),
            &budget,
            &cancellation,
        )
        .await
        .expect("array DML Oracle");
    assert_eq!(outcome.rows.confirmed, 600);

    let mut verify = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("verifica array DML");
    let rows = verify
        .query(
            &Statement::new(format!("SELECT COUNT(*) AS N FROM {table}")),
            &cancellation,
        )
        .await
        .expect("conteggio array DML");
    assert_eq!(rows[0].get("N"), Some(&ParameterValue::I64(600)));
    verify
        .rollback(&cancellation)
        .await
        .expect("rollback verifica array DML");
    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await
        .expect("rimuove fixture array DML");
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_identifier_limit_matches_oracle_at_128_bytes() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let accepted = format!("T{}", "A".repeat(127));
    validate_identifier(IdentifierDialect::Oracle, &accepted)
        .expect("identificatore Oracle di 128 byte");
    let accepted = quote_identifier(IdentifierDialect::Oracle, &accepted)
        .expect("quoting identificatore Oracle di 128 byte");
    let _ = provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {accepted} PURGE"),
            &cancellation,
        )
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {accepted} (ID NUMBER(10))"),
            &cancellation,
        )
        .await
        .expect("Oracle accetta un identificatore di 128 byte");
    provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {accepted} PURGE"),
            &cancellation,
        )
        .await
        .expect("rimuove identificatore Oracle di 128 byte");

    let rejected = format!("T{}", "B".repeat(128));
    assert!(validate_identifier(IdentifierDialect::Oracle, &rejected).is_err());
    let error = provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE \"{rejected}\" (ID NUMBER(10))"),
            &cancellation,
        )
        .await
        .expect_err("Oracle deve rifiutare un identificatore di 129 byte");
    assert!(!error.message.contains(&rejected));
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_closed_capabilities_fail_closed_and_match_ddl_semantics() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget capability chiuse");
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("capability Oracle");
    assert!(!capabilities.reads.server_cursor);
    assert!(!capabilities.writes.truncate_insert);
    assert!(!capabilities.writes.returning);
    assert!(!capabilities.transactions.transactional_ddl);
    assert!(!capabilities.transactions.staged_swap);

    let schema = contract_schema(vec![Field::new("ID", DataType::Int32, false)]).expect("schema");
    for (mode, transaction_profile) in [
        (
            WriteMode::TruncateInsert,
            TransactionProfile::SingleTransaction,
        ),
        (WriteMode::Append, TransactionProfile::StagedSwap),
    ] {
        let operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("PLENORA".to_owned()),
                object: "PLENORA_ORACLE_CLOSED_CAPABILITY".to_owned(),
            },
            mode,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        };
        let error = provider
            .prepare_write(
                &secret,
                &operation,
                Arc::clone(&schema),
                &budget,
                &cancellation,
            )
            .await
            .err()
            .expect("capability write Oracle chiusa");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported
        );
        assert_eq!(error.remote_effect, RemoteEffect::None);
    }

    let returning = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("PLENORA_ORACLE_CLOSED_CAPABILITY"),
        columns: vec!["ID".to_owned()],
        values: vec![vec![Expression::literal(ParameterValue::I32(1))]],
        returning: vec!["ID".to_owned()],
    });
    assert!(compile_portable(ProviderKind::Oracle, &returning).is_err());

    let anchor = "PLENORA_ORACLE_DDL_ANCHOR";
    let side_effect = "PLENORA_ORACLE_DDL_SIDE_EFFECT";
    for table in [side_effect, anchor] {
        let _ = provider
            .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
            .await;
    }
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {anchor} (ID NUMBER(10))"),
            &cancellation,
        )
        .await
        .expect("crea anchor DDL Oracle");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione semantica DDL Oracle");
    transaction
        .execute(
            &Statement::new(format!("INSERT INTO {anchor} VALUES (1)")),
            &cancellation,
        )
        .await
        .expect("DML prima del DDL Oracle");
    transaction
        .execute(
            &Statement::new(format!("CREATE TABLE {side_effect} (ID NUMBER(10))")),
            &cancellation,
        )
        .await
        .expect("DDL Oracle con commit implicito");
    transaction
        .rollback(&cancellation)
        .await
        .expect("rollback dopo DDL Oracle");

    let mut verify = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("verifica semantica DDL Oracle");
    let rows = verify
        .query(
            &Statement::new(format!("SELECT COUNT(*) AS N FROM {anchor}")),
            &cancellation,
        )
        .await
        .expect("conteggio dopo DDL Oracle");
    assert_eq!(rows[0].get("N"), Some(&ParameterValue::I64(1)));
    verify
        .query(
            &Statement::new(format!("SELECT COUNT(*) AS N FROM {side_effect}")),
            &cancellation,
        )
        .await
        .expect("tabella DDL Oracle sopravvive al rollback");
    verify
        .rollback(&cancellation)
        .await
        .expect("chiude verifica semantica DDL Oracle");
    for table in [side_effect, anchor] {
        provider
            .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
            .await
            .expect("rimuove fixture semantica DDL Oracle");
    }
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
async fn live_xyz_wkb_roundtrip_matches_declared_dimension() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget XYZ Oracle");
    let table = "PLENORA_ORACLE_ARROW_SPATIAL";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    let mut metadata = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("pulizia metadata XYZ Oracle");
    metadata
        .execute(
            &Statement::new(format!(
                "DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '{table}'"
            )),
            &cancellation,
        )
        .await
        .expect("pulisce metadata XYZ Oracle");
    metadata
        .commit(&cancellation)
        .await
        .expect("commit pulizia metadata XYZ Oracle");

    let point = point_xyz(12.0, 41.0, 250.5);
    run_spatial_write(
        &provider,
        &secret,
        &budget,
        spatial_write_operation(WriteMode::Create, false),
        spatial_xyz_write_batch(vec![1], vec![point]),
        &cancellation,
    )
    .await
    .expect("create XYZ Arrow Spatial Oracle");

    let description = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: None,
                    schema: Some("PLENORA".to_owned()),
                    object: table.to_owned(),
                },
            },
            &cancellation,
        )
        .await
        .expect("catalogo XYZ Oracle");
    let description: crate::OracleObjectDescription =
        serde_json::from_value(description.document).expect("documento catalogo XYZ Oracle");
    assert_eq!(description.columns[1].spatial_dimensions, Some(3));

    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: table.to_owned(),
        },
        projection: vec!["ID".to_owned(), "SHAPE".to_owned()],
        filter: None,
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(1),
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
        .expect("read XYZ Oracle");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch XYZ Oracle")
        .expect("riga XYZ Oracle");
    assert_eq!(
        batch.schema().field(1).metadata()["plenora.geometry.dimensions"],
        "xyz"
    );
    let shapes = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("WKB XYZ Oracle");
    assert_eq!(shapes.value(0).len(), 29);

    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await
        .expect("rimuove fixture XYZ Oracle");
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
    // Regola delle sonde (`spatial_probe::admit_probe`, 14960f9): Oracle non
    // ha una guardia misurata sullo SRID della colonna, e il compilatore
    // portable non legge il catalogo: il valore spatial scritto si rifiuta.
    let refused = compile_portable(ProviderKind::Oracle, &insert)
        .expect_err("valore spatial portable senza prova dello SRID");
    assert_eq!(
        refused.category,
        plenora_database_core::ErrorCategory::Unsupported
    );
    assert_eq!(
        refused.message,
        plenora_database_core::spatial_probe::VALUE_SRID_UNPROVEN
    );
    setup
        .execute(
            &Statement::new(
                "INSERT INTO PLENORA_ORACLE_SPATIAL (ID, SHAPE) VALUES (1, MDSYS.SDO_GEOMETRY(2001, 4326, MDSYS.SDO_POINT_TYPE(1, 2, NULL), NULL, NULL))",
            ),
            &cancellation,
        )
        .await
        .expect("inserisce geometria Spatial Oracle");
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
    // La stessa regola per la sonda di un predicato portable; il predicato
    // si esegue dalla lettura del provider, che prova lo SRID sul catalogo.
    let refused = compile_portable(ProviderKind::Oracle, &predicate)
        .expect_err("sonda portable senza prova dello SRID");
    assert_eq!(
        refused.category,
        plenora_database_core::ErrorCategory::Unsupported
    );
    assert_eq!(
        refused.message,
        plenora_database_core::spatial_probe::PROBE_SRID_UNPROVEN
    );

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
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_every_declared_spatial_function_executes_on_geometry_and_geography() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget censimento spatial");
    for (table, srid, semantics) in [
        (
            "PLENORA_ORACLE_SPATIAL_GEOM",
            3_857_u32,
            SpatialSemantics::Geometry,
        ),
        (
            "PLENORA_ORACLE_SPATIAL_GEOG",
            4_326_u32,
            SpatialSemantics::Geography,
        ),
    ] {
        let _ = provider
            .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
            .await;
        let mut metadata = provider
            .begin_transaction(
                &secret,
                &TransactionOptions::default(),
                &budget,
                &cancellation,
            )
            .await
            .expect("transazione pulizia metadata censimento");
        metadata
            .execute(
                &Statement::new(format!(
                    "DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '{table}'"
                )),
                &cancellation,
            )
            .await
            .expect("pulisce metadata censimento");
        metadata
            .commit(&cancellation)
            .await
            .expect("commit pulizia metadata censimento");
        provider
            .execute_ddl(
                &secret,
                &format!(
                    "CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, POINT_SHAPE MDSYS.SDO_GEOMETRY, LINE_SHAPE MDSYS.SDO_GEOMETRY, POLY_SHAPE MDSYS.SDO_GEOMETRY)"
                ),
                &cancellation,
            )
            .await
            .expect("crea tabella censimento Spatial Oracle");
        let mut setup = provider
            .begin_transaction(
                &secret,
                &TransactionOptions::default(),
                &budget,
                &cancellation,
            )
            .await
            .expect("transazione setup censimento");
        for column in ["POINT_SHAPE", "LINE_SHAPE", "POLY_SHAPE"] {
            setup
                .execute(
                    &Statement::new(format!(
                        "INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES ('{table}', '{column}', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', -1000, 1000, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -1000, 1000, 0.005)), {srid})"
                    )),
                    &cancellation,
                )
                .await
                .expect("registra metadata censimento");
        }
        setup
            .execute(
                &Statement::new(format!(
                    "INSERT INTO {table} (ID, POINT_SHAPE, LINE_SHAPE, POLY_SHAPE) VALUES (1, MDSYS.SDO_GEOMETRY(2001, {srid}, MDSYS.SDO_POINT_TYPE(5, 5, NULL), NULL, NULL), MDSYS.SDO_GEOMETRY(2002, {srid}, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1, 2, 1), MDSYS.SDO_ORDINATE_ARRAY(0, 0, 5, 5, 10, 0)), MDSYS.SDO_GEOMETRY(2003, {srid}, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1, 1003, 1), MDSYS.SDO_ORDINATE_ARRAY(0, 0, 10, 0, 10, 10, 0, 10, 0, 0)))"
                )),
                &cancellation,
            )
            .await
            .expect("inserisce geometrie censimento");
        setup
            .commit(&cancellation)
            .await
            .expect("commit setup censimento");
        provider
            .execute_ddl(
                &secret,
                &format!(
                    "CREATE INDEX {table}_SX ON {table} (POLY_SHAPE) INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2"
                ),
                &cancellation,
            )
            .await
            .expect("crea indice censimento Spatial Oracle");

        qualify_spatial_surface(
            &provider,
            &secret,
            table,
            srid,
            semantics,
            &budget,
            &cancellation,
        )
        .await;
        provider
            .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
            .await
            .expect("rimuove tabella censimento Spatial Oracle");
    }
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
async fn live_keyset_checkpoint_resumes_after_the_last_delivered_row() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget checkpoint");
    let table = "PLENORA_ORACLE_CHECKPOINT";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {table} (ID NUMBER(18) PRIMARY KEY, VALUE VARCHAR2(20))"),
            &cancellation,
        )
        .await
        .expect("crea fixture checkpoint Oracle");
    let mut setup = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione setup checkpoint");
    setup
        .execute(
            &Statement::new(format!(
                "INSERT ALL INTO {table} VALUES (1, 'one') INTO {table} VALUES (2, 'two') INTO {table} VALUES (3, 'three') SELECT 1 FROM DUAL"
            )),
            &cancellation,
        )
        .await
        .expect("popola fixture checkpoint");
    setup
        .commit(&cancellation)
        .await
        .expect("commit fixture checkpoint");

    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: table.to_owned(),
        },
        projection: vec!["ID".to_owned(), "VALUE".to_owned()],
        filter: None,
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(2),
        row_offset: None,
        declared_crs: Vec::new(),
    };
    let parameters = ParameterBag::default();
    let checkpoint = ReadCheckpoint::new(
        ProviderKind::Oracle,
        &operation,
        &parameters,
        vec![ParameterValue::I64(2)],
    )
    .expect("checkpoint Oracle persistibile");
    let encoded = checkpoint.to_json().expect("serializza checkpoint Oracle");
    let restored = ReadCheckpoint::from_json(&encoded).expect("ripristina checkpoint Oracle");
    let (resumed, resumed_parameters) = restored
        .resume(ProviderKind::Oracle, &operation, &parameters)
        .expect("applica checkpoint Oracle");
    let mut stream = provider
        .read(
            &secret,
            &resumed,
            &resumed_parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("riapre read Oracle dal checkpoint");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("legge dopo checkpoint")
        .expect("batch dopo checkpoint");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ID Oracle Int64");
    assert_eq!(ids.values(), &[3]);
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine read ripresa")
        .is_none());

    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await
        .expect("rimuove fixture checkpoint");
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_driver_errors_are_redacted() {
    let (config, secret) = fixture_config();
    let provider = OracleProvider::new_with_pool(config, 1).expect("pool per verifica rollback");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let table = "PLENORA_RESET_ROLLBACK";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {table} (ID NUMBER(10))"),
            &cancellation,
        )
        .await
        .expect("crea fixture rollback");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");
    transaction
        .execute(
            &Statement::new(format!("INSERT INTO {table} (ID) VALUES (1)")),
            &cancellation,
        )
        .await
        .expect("scrittura prima dell'errore");
    let session_query = Statement::new("SELECT SYS_CONTEXT('USERENV', 'SID') AS SID FROM DUAL");
    let session = transaction
        .query(&session_query, &cancellation)
        .await
        .expect("sessione prima del reset");
    let marker = "PLENORA_SECRET_IDENTIFIER_79F";
    let error = transaction
        .query(
            &Statement::new(format!("SELECT * FROM {marker}")),
            &cancellation,
        )
        .await
        .expect_err("tabella inesistente");
    assert!(!error.message.contains(marker));
    assert!(error.message.contains("ORA-00942"));
    // NUMBER(10) dichiara scala zero per la verifica I64; il literal non la garantisce.
    // Il reset mantiene utilizzabile la stessa sessione prima del rollback.
    let rows = transaction
        .query(
            &Statement::new("SELECT CAST(1 AS NUMBER(10)) AS N FROM DUAL"),
            &cancellation,
        )
        .await
        .expect("sessione utilizzabile dopo l'errore server");
    assert_eq!(rows[0].get("N"), Some(&ParameterValue::I64(1)));
    // L'errore deve lasciare la scrittura pendente, altrimenti il rollback non e provato.
    let pending = transaction
        .query(
            &Statement::new(format!("SELECT ID FROM {table}")),
            &cancellation,
        )
        .await
        .expect("scrittura ancora pendente dopo il reset");
    assert_eq!(pending.len(), 1);
    let rollback = transaction.rollback(&cancellation).await;
    if let Err(error) = &rollback {
        assert!(!error.message.contains(marker));
    }
    rollback.expect("rollback confermato dopo il reset");
    let mut verification = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione successiva");
    // Riutilizzare la stessa sessione esclude un falso positivo da disconnessione.
    let next_session = verification
        .query(&session_query, &cancellation)
        .await
        .expect("sessione dopo rollback");
    assert_eq!(session[0].get("SID"), next_session[0].get("SID"));
    let rows = verification
        .query(
            &Statement::new(format!("SELECT ID FROM {table}")),
            &cancellation,
        )
        .await
        .expect("verifica rollback effettivo");
    assert!(rows.is_empty(), "la scrittura deve essere annullata");
    verification
        .rollback(&cancellation)
        .await
        .expect("chiude verifica");
    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await
        .expect("rimuove fixture rollback");
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

/// Una scrittura `create` abbandonata dopo il CREATE confermato (il
/// chiamante lascia cadere la future mentre il produttore non risponde) non
/// arriva a un esito: il target resta osservabile nel registro dei residui
/// del provider.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_an_abandoned_create_is_recorded_as_a_possible_residue() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let table = "PLENORA_ORACLE_ABANDONED_CREATE";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    let schema = contract_schema(vec![Field::new("ID", DataType::Int32, false)]).expect("schema");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .expect("batch");
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
        .expect("prepare");
    assert_eq!(provider.ddl_residues().total, 0);
    let abandoned = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.write(
            &secret,
            prepared,
            Box::new(StallingBatchStream(VecBatchStream::new(vec![batch]))),
            &budget,
            &cancellation,
        ),
    )
    .await;
    assert!(
        abandoned.is_err(),
        "la scrittura doveva attendere il produttore"
    );
    let residues = provider.ddl_residues();
    assert_eq!(residues.total, 1);
    assert_eq!(residues.recent[0].object, table);
    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await
        .expect("la tabella creata esiste e si rimuove");
}

/// SQL nativo nella transazione: un INSERT, poi un CREATE TABLE che fallisce
/// con ORA-00955. Il commit implicito prima del DDL ha reso permanente
/// l'INSERT, quindi l'errore non puo dichiarare `none`.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_a_native_ddl_failure_after_a_write_is_not_reported_as_no_effect() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let table = "PLENORA_ORACLE_NATIVE_DDL";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {table} (ID NUMBER(10))"),
            &cancellation,
        )
        .await
        .expect("fixture");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");
    transaction
        .execute(
            &Statement::new(format!("INSERT INTO {table} (ID) VALUES (1)")),
            &cancellation,
        )
        .await
        .expect("insert");
    let error = transaction
        .execute(
            &Statement::new(format!("CREATE TABLE {table} (ID NUMBER(10))")),
            &cancellation,
        )
        .await
        .expect_err("ORA-00955");
    // La CI ha ricevuto ORA-00000 dal driver per questo DDL fallito.
    // Non equivale a un rifiuto certo: accettiamo solo i due codici osservati
    // o attesi e verifichiamo sotto l'effetto persistente con un'altra sessione.
    assert!(
        error.message.contains("ORA-00955") || error.message.contains("ORA-00000"),
        "ramo di errore inatteso: {:?} {}",
        error.category,
        error.message
    );
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    let _ = transaction.rollback(&cancellation).await;
    // L'INSERT e stato reso permanente dal commit implicito del DDL.
    let mut verify = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("verifica");
    let rows = verify
        .query(
            &Statement::new(format!("SELECT COUNT(*) AS N FROM {table}")),
            &cancellation,
        )
        .await
        .expect("conteggio");
    assert_eq!(rows[0].get("N"), Some(&ParameterValue::I64(1)));
    let _ = verify.rollback(&cancellation).await;
    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_caller_stream_fetch_failure_requires_recovery() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .open_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin reale");
    // query_inner apre con il prefetch di 100 righe del driver. Le righe 101
    // e 102 provano due fetch_more sullo stesso cursore; la 103 deve fallire.
    // Nessun ORDER BY: un sort potrebbe valutare tutte le righe in apertura.
    let statement =
        Statement::new("SELECT LEVEL / (103 - LEVEL) AS N FROM DUAL CONNECT BY LEVEL <= 103");
    let mut stream = tx
        .query_stream(&statement, 1, &cancellation)
        .await
        .expect("prima pagina valida");
    for _ in 0..102 {
        let rows = stream
            .next_batch(&cancellation)
            .await
            .expect("prefetch e primi due fetch devono riuscire")
            .expect("il cursore deve consegnare tutte le 102 righe valide");
        assert_eq!(rows.len(), 1);
    }
    let error = stream
        .next_batch(&cancellation)
        .await
        .expect_err("il fetch della riga 103 deve raggiungere la divisione per zero");
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Read);
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Execution
    );
    assert!(error.message.contains("ORA-01476"), "{}", error.message);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(
        error.retry,
        plenora_database_core::RetryDisposition::RequiresRecovery
    );
    drop(stream);
    // NUMBER(10) dichiara scala zero anche per la query successiva al reset.
    let rows = tx
        .query(
            &Statement::new("SELECT CAST(1 AS NUMBER(10)) AS N FROM DUAL"),
            &cancellation,
        )
        .await
        .expect("la stessa connessione deve rispondere dopo il reset del fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("N"), Some(&ParameterValue::I64(1)));
}

#[tokio::test]
#[ignore = "richiede fixture Oracle"]
async fn live_cached_query_reexecutes_with_an_unexhausted_first_page() {
    let (config, secret) = fixture_config();
    let driver = config
        .driver_config(&secret)
        .expect("driver config")
        .into_inner()
        .with_statement_cache_size(4);
    let connection = oracle_rs::Connection::connect_with_config(driver)
        .await
        .expect("connessione cache probe");
    let sql = "SELECT LEVEL AS N FROM DUAL CONNECT BY LEVEL <= 250";
    for _ in 0..2 {
        let page = connection
            .query(sql, &[])
            .await
            .expect("pagina query cache");
        assert!(page.has_more_rows);
        assert!(!page.rows.is_empty());
        assert!(page.rows.len() < 250);
        assert_eq!(page.columns[0].name, "N");
        assert_eq!(
            page.rows[0].get(0).and_then(oracle_rs::Value::as_str),
            Some("1")
        );
    }
    connection.close().await.expect("chiusura cache probe");
}

/// Le forme della guardia del server sugli operandi scalari, con lo SQL
/// reso e gli esiti misurati (`scripts/sonde_operandi_server.py`).
const FIXTURE_GUARDIA: &str = include_str!("../../../tests/fixtures/operandi_server_guard.json");

/// `SELECT T.ID FROM PLENORA.SONDE_OPERANDI T` con il piano della forma sopra.
fn guard_plan(piano: &serde_json::Value) -> serde_json::Value {
    let mut operation = serde_json::json!({
        "source": {"object": {"catalog": null, "schema": "PLENORA", "object": "SONDE_OPERANDI"}, "alias": "T"},
        "projection": [{"expression": {"kind": "column", "column": {"relation": "T", "field": "ID"}}, "alias": "ID"}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in piano.as_object().expect("piano") {
        operation[key] = value.clone();
    }
    operation
}

/// Il piano con `G` (e il suo alias di uscita) sostituita da `colonna`.
fn guard_control(value: &serde_json::Value, colonna: &str) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, inner)| {
                    let replaced = (key == "field" || key == "alias") && inner == "G";
                    let inner = if replaced {
                        serde_json::Value::String(colonna.to_owned())
                    } else {
                        guard_control(inner, colonna)
                    };
                    (key.clone(), inner)
                })
                .collect(),
        ),
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| guard_control(item, colonna))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Il valore di un bind tipato del piano: binario, testo, intero.
fn guard_bind(piano: &serde_json::Value, name: &str) -> ParameterValue {
    let mut stack = vec![piano];
    while let Some(node) = stack.pop() {
        match node {
            serde_json::Value::Object(map) => {
                if map.get("kind").and_then(serde_json::Value::as_str) == Some("typed_parameter")
                    && map.get("name").and_then(serde_json::Value::as_str) == Some(name)
                {
                    return match map["parameter_type"].as_str() {
                        Some("binary") => ParameterValue::Bytes(vec![1]),
                        Some("string") => ParameterValue::String("a".to_owned()),
                        Some("integer") => ParameterValue::I64(1),
                        other => panic!("tipo di bind del fixture {other:?}"),
                    };
                }
                stack.extend(map.values());
            }
            serde_json::Value::Array(items) => stack.extend(items),
            _ => {}
        }
    }
    panic!("bind {name} assente dal piano");
}

/// Le righe come elenco ordinato della colonna `ID`.
fn guard_ids(rows: &[plenora_database_core::Row]) -> Vec<String> {
    let mut ids: Vec<String> = rows
        .iter()
        .map(|row| match row.get("ID") {
            Some(ParameterValue::I64(value)) => value.to_string(),
            Some(ParameterValue::I32(value)) => value.to_string(),
            Some(ParameterValue::Decimal(value) | ParameterValue::String(value)) => value.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

/// Esegue il piano reso dal renderer del repository attraverso il percorso
/// giudicato dell'esecuzione (`facade::query_judged`, come lo SDK, la
/// facade e la CLI portable), e lo stesso SQL senza giudizio, nella stessa
/// transazione. `values`: i valori dei bind, altrimenti quelli del fixture.
async fn run_guard_plan(
    provider: &OracleProvider,
    secret: &SecretString,
    budget: &ResourceBudget,
    plan: &serde_json::Value,
    values: Option<&[ParameterValue]>,
    cancellation: &CancellationToken,
) -> (
    String,
    plenora_database_core::Result<Vec<String>>,
    plenora_database_core::Result<Vec<String>>,
) {
    let operation: QueryOperation = serde_json::from_value(plan.clone()).expect("piano");
    let rendered = Renderer::new(
        Dialect::Oracle,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(ProviderKind::Oracle)
    .render_query(&operation)
    .expect("resa Oracle");
    let values = values.map_or_else(
        || {
            rendered
                .binds
                .iter()
                .map(|bind| guard_bind(plan, &bind.name))
                .collect()
        },
        <[ParameterValue]>::to_vec,
    );
    let statement = Statement::new(rendered.sql.clone()).with_params(values);
    let binds = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione guardia Oracle");
    let judged = plenora_database_core::facade::query_judged(
        transaction.as_mut(),
        &statement,
        Some(&plenora_database_core::spatial_operand::OperandPlan::Query(
            &operation,
        )),
        &binds,
        cancellation,
    )
    .await
    .map(|rows| guard_ids(&rows));
    let direct = transaction
        .query(&statement, cancellation)
        .await
        .map(|rows| guard_ids(&rows));
    let _ = transaction.rollback(cancellation).await;
    (rendered.sql, judged, direct)
}

/// Il rifiuto del giudizio, prima del server: `unsupported`, fase di
/// preparazione, con uno dei motivi del giudizio.
fn judgement_refusal(error: &plenora_database_core::DatabaseError, reason: &str) -> bool {
    error.category == plenora_database_core::ErrorCategory::Unsupported
        && error.phase == plenora_database_core::ErrorPhase::Prepare
        && error.message == reason
}

/// La guardia del server sugli operandi scalari
/// (`spatial_operand::operand_server_guard`), misurata sul server, e il
/// giudizio sul catalogo che dal 25/10 la precede: per ogni forma del
/// fixture lo SQL reso dal renderer del repository, eseguito senza giudizio,
/// e rifiutato dal server con uno dei codici misurati (le forme che la regola
/// ammette e quelle su una colonna derivata mai letta passano); attraverso il
/// percorso giudicato si rifiuta prima del server, o dove il giudizio non la
/// dice un operando dal server con il codice misurato; la stessa forma su una
/// colonna non geometrica arriva al server e rende le righe dello SQL senza
/// giudizio. Il catalogo dice `MDSYS.SDO_GEOMETRY`, proprietario compreso.
#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_operand_server_guard_rejects_every_measured_form() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget guardia Oracle");
    let table = "SONDE_OPERANDI";
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!(
                "CREATE TABLE {table} (ID NUMBER(10), NAME VARCHAR2(20), B RAW(16), G MDSYS.SDO_GEOMETRY)"
            ),
            &cancellation,
        )
        .await
        .expect("tabella guardia Oracle");
    let mut setup = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione preparazione guardia Oracle");
    for sql in [
        format!("DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '{table}'"),
        format!(
            "INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES ('{table}', 'G', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 4326)"
        ),
        format!("INSERT INTO {table} VALUES (1, 'a', HEXTORAW('01'), MDSYS.SDO_GEOMETRY(2001, 4326, MDSYS.SDO_POINT_TYPE(1, 2, NULL), NULL, NULL))"),
        format!("INSERT INTO {table} VALUES (2, 'b', HEXTORAW('02'), MDSYS.SDO_GEOMETRY(2001, 4326, MDSYS.SDO_POINT_TYPE(1, 2, NULL), NULL, NULL))"),
    ] {
        setup
            .execute(&Statement::new(sql), &cancellation)
            .await
            .expect("preparazione guardia Oracle");
    }
    setup
        .commit(&cancellation)
        .await
        .expect("commit preparazione guardia Oracle");

    let description = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: None,
                    schema: Some("PLENORA".to_owned()),
                    object: table.to_owned(),
                },
            },
            &cancellation,
        )
        .await
        .expect("catalogo guardia Oracle");
    let description: crate::OracleObjectDescription =
        serde_json::from_value(description.document).expect("documento catalogo guardia Oracle");
    let geometry = description
        .columns
        .iter()
        .find(|column| column.name == "G")
        .expect("colonna G");
    assert_eq!(geometry.data_type_owner.as_deref(), Some("MDSYS"));
    assert!(geometry.is_sdo_geometry());

    let fixture: serde_json::Value =
        serde_json::from_str(FIXTURE_GUARDIA).expect("fixture della guardia");
    let forms = fixture["forme"].as_array().expect("forme");
    assert!(!forms.is_empty());
    let reasons = [
        plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND,
        plenora_database_core::spatial_operand::GEOMETRY_OPERAND_UNRESOLVED,
    ];
    for form in forms {
        // Una sessione per forma: il driver non chiude i cursori, e una
        // sessione arriva a ORA-01000 dopo `open_cursors` istruzioni (il
        // limite dichiarato in `operand_catalog`).
        let (provider, secret) = self::fixture();
        let name = form["forma"].as_str().expect("nome della forma");
        let plan = guard_plan(&form["piano"]);
        let (sql, judged, direct) =
            run_guard_plan(&provider, &secret, &budget, &plan, None, &cancellation).await;
        assert_eq!(
            sql,
            form["sql"]["oracle"]["sql"].as_str().expect("sql"),
            "{name}"
        );
        let codes: Vec<&str> = form["esito"]["oracle"]["errore"]
            .as_array()
            .map(|codes| {
                codes
                    .iter()
                    .map(|code| code.as_str().expect("codice"))
                    .collect()
            })
            .unwrap_or_default();
        // Lo SQL senza giudizio fa cio che il server ha misurato: la guardia.
        match &direct {
            Err(error) => assert!(
                codes.iter().any(|code| error.message.contains(code)),
                "{name}: {}",
                error.message
            ),
            Ok(_) => assert!(codes.is_empty(), "{name}: il server non ha rifiutato"),
        }
        let control = match form["controllo"].as_str() {
            Some(column) => {
                let (_, judged, direct) = run_guard_plan(
                    &provider,
                    &secret,
                    &budget,
                    &guard_control(&plan, column),
                    None,
                    &cancellation,
                )
                .await;
                let rows = judged.unwrap_or_else(|error| panic!("{name} controllo: {error}"));
                assert_eq!(Ok(rows.clone()), direct, "{name} controllo");
                Some(rows)
            }
            None => None,
        };
        if form.get("ammessa").is_some() {
            let rows = judged.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(Ok(rows), direct, "{name}");
        } else {
            let error = judged.expect_err(name);
            // Regola F (`spatial_operand::OPERAND_FAMILY_MISMATCH`, seconda
            // lettura del 25/10): `UPPER`/`LOWER` di una geometria e una
            // conversione implicita, rifiutata prima del server anche in
            // projection.
            assert!(
                reasons
                    .iter()
                    .any(|reason| judgement_refusal(&error, reason))
                    || family_refusal(&error)
                    || codes.iter().any(|code| error.message.contains(code)),
                "{name}: {}",
                error.message
            );
        }
        if form.get("colonna_mai_letta").is_some() {
            // Il server elimina la colonna prima di controllarne il tipo:
            // nessuna riga ne dipende.
            assert_eq!(direct.ok(), control, "{name}");
        }
    }
    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table} PURGE"), &cancellation)
        .await
        .expect("pulizia guardia Oracle");
}

/// La relazione di base della regola B: `G` dichiarata `SDO_GEOMETRY` senza
/// schema (proprietario `PUBLIC` nel catalogo, il sinonimo pubblico), due
/// poligoni uguali per `SDO_GEOM.RELATE` con i vertici in ordine diverso.
const DERIVED_TABLE: &str = "SONDE_DERIVATE";

/// La vista: WKB (`BLOB`), `GET_WKB` (`BLOB`), il WKB in `RAW`, una colonna
/// `RAW` che ripete quella della tabella, il WKT (`CLOB`).
const DERIVED_VIEW_SQL: &str = "CREATE OR REPLACE VIEW SONDE_DERIVATE_V AS SELECT T.ID AS ID, \
    SDO_UTIL.TO_WKBGEOMETRY(T.G) AS W, T.G.GET_WKB() AS W2, \
    DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(T.G), 2000, 1) AS R, T.B AS B, \
    SDO_UTIL.TO_WKTGEOMETRY(T.G) AS X FROM SONDE_DERIVATE T";

async fn drop_derived(
    provider: &OracleProvider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) {
    for sql in [
        "DROP SYNONYM SONDE_DERIVATE_S",
        "DROP MATERIALIZED VIEW SONDE_DERIVATE_MV",
        "DROP VIEW SONDE_DERIVATE_V",
        "DROP TABLE SONDE_DERIVATE PURGE",
    ] {
        let _ = provider.execute_ddl(secret, sql, cancellation).await;
    }
}

/// `SELECT V.ID FROM PLENORA.<object> V` con `extra`.
fn derived_plan(object: &str, extra: &serde_json::Value) -> serde_json::Value {
    let mut operation = serde_json::json!({
        "source": {"object": {"catalog": null, "schema": "PLENORA", "object": object}, "alias": "V"},
        "projection": [{"expression": {"kind": "column", "column": {"relation": "V", "field": "ID"}}, "alias": "ID"}],
        "filter": null,
        "having": null,
        "row_limit": null
    });
    for (key, value) in extra.as_object().expect("extra") {
        operation[key] = value.clone();
    }
    operation
}

fn derived_column(field: &str) -> serde_json::Value {
    serde_json::json!({"kind": "column", "column": {"relation": "V", "field": field}})
}

fn derived_binary(expression: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({"kind": "cast", "expression": expression, "target_type": "binary"})
}

fn derived_equal(left: &serde_json::Value, parameter_type: &str) -> serde_json::Value {
    serde_json::json!({"filter": {"kind": "compare", "left": left, "operator": "eq",
        "right": {"kind": "typed_parameter", "name": "p", "parameter_type": parameter_type}}})
}

/// Regola B (`spatial_operand::catalog_column_is_geometric`) e il
/// proprietario risolto di `SDO_GEOMETRY`, sul server. Rilettura del 25/10:
/// - F1: la guardia del server non vede la geometria che una vista rende
///   binaria. Lo SQL senza giudizio di `CAST(V.W AS RAW(2000)) = ?` con il
///   WKB del primo poligono rende soltanto il primo, mentre
///   `SDO_GEOM.RELATE` dice uguali i due: un risultato sbagliato senza
///   errore. Il giudizio lo rifiuta prima del server, con `ORDER BY`, il
///   `RAW` di `DBMS_LOB.SUBSTR`, `GET_WKB`, un sinonimo e una vista
///   materializzata; la colonna `RAW` della tabella resta scalare, quella
///   della vista che la ripete si rifiuta (il costo dichiarato);
/// - F3: `G SDO_GEOMETRY` senza schema ha `DATA_TYPE_OWNER` `PUBLIC`; la
///   descrizione lo risolve in `MDSYS`, la lettura proietta `G` e `G = ?` si
///   rifiuta al giudizio, non in esecuzione.
#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_operand_catalog_refuses_derived_binary_and_resolves_public_sdo_geometry() {
    use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget regola B Oracle");
    drop_derived(&provider, &secret, &cancellation).await;
    provider
        .execute_ddl(
            &secret,
            &format!(
                "CREATE TABLE {DERIVED_TABLE} (ID NUMBER(10), NAME VARCHAR2(20), B RAW(16), G SDO_GEOMETRY)"
            ),
            &cancellation,
        )
        .await
        .expect("tabella regola B Oracle");
    let mut setup = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione preparazione regola B Oracle");
    for sql in [
        format!("DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '{DERIVED_TABLE}'"),
        format!(
            "INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES ('{DERIVED_TABLE}', 'G', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 4326)"
        ),
        format!("INSERT INTO {DERIVED_TABLE} VALUES (3, 'p', HEXTORAW('03'), SDO_UTIL.FROM_WKTGEOMETRY('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326))"),
        format!("INSERT INTO {DERIVED_TABLE} VALUES (4, 'q', HEXTORAW('04'), SDO_UTIL.FROM_WKTGEOMETRY('POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))', 4326))"),
    ] {
        setup
            .execute(&Statement::new(sql), &cancellation)
            .await
            .expect("preparazione regola B Oracle");
    }
    // I due poligoni sono la stessa geometria per il server.
    let equal = setup
        .query(
            &Statement::new(format!(
                "SELECT A.ID FROM {DERIVED_TABLE} A, {DERIVED_TABLE} B WHERE A.ID = 3 AND B.ID = 4 \
                 AND SDO_GEOM.RELATE(A.G, 'EQUAL', B.G, 0.005) = 'EQUAL'"
            )),
            &cancellation,
        )
        .await
        .expect("RELATE regola B Oracle");
    assert_eq!(guard_ids(&equal), vec!["3".to_owned()]);
    // Il WKB del primo poligono, come lo rende il server.
    let wkb = setup
        .query(
            &Statement::new(format!(
                "SELECT DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(G), 2000, 1) AS W \
                 FROM {DERIVED_TABLE} WHERE ID = 3"
            )),
            &cancellation,
        )
        .await
        .expect("WKB regola B Oracle");
    let wkb = match wkb.first().and_then(|row| row.get("W")) {
        Some(ParameterValue::Bytes(bytes)) => bytes.clone(),
        other => panic!("WKB regola B Oracle: {other:?}"),
    };
    setup
        .commit(&cancellation)
        .await
        .expect("commit preparazione regola B Oracle");
    provider
        .execute_ddl(&secret, DERIVED_VIEW_SQL, &cancellation)
        .await
        .expect("vista regola B Oracle");
    provider
        .execute_ddl(
            &secret,
            "CREATE SYNONYM SONDE_DERIVATE_S FOR SONDE_DERIVATE_V",
            &cancellation,
        )
        .await
        .expect("sinonimo regola B Oracle");
    provider
        .execute_ddl(
            &secret,
            "CREATE MATERIALIZED VIEW SONDE_DERIVATE_MV AS SELECT ID, \
             DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(G), 2000, 1) AS R FROM SONDE_DERIVATE",
            &cancellation,
        )
        .await
        .expect("vista materializzata regola B Oracle");

    // F1: il risultato sbagliato senza giudizio, il rifiuto con il giudizio.
    let bytes = [ParameterValue::Bytes(wkb.clone())];
    let cast_equal = derived_plan(
        "SONDE_DERIVATE_V",
        &derived_equal(&derived_binary(&derived_column("W")), "binary"),
    );
    let (sql, judged, direct) = run_guard_plan(
        &provider,
        &secret,
        &budget,
        &cast_equal,
        Some(&bytes),
        &cancellation,
    )
    .await;
    assert!(
        sql.contains("CAST(\"V\".\"W\" AS RAW(2000)) = CAST(:1 AS RAW(2000))"),
        "{sql}"
    );
    assert_eq!(
        direct.expect("senza giudizio"),
        vec!["3".to_owned()],
        "il server confronta i byte: il secondo poligono, uguale, manca"
    );
    let error = judged.expect_err("CAST(W) = ? giudicato");
    assert!(
        judgement_refusal(&error, GEOMETRY_SCALAR_OPERAND),
        "{error}"
    );

    let order = |object: &str, expression: serde_json::Value| {
        derived_plan(
            object,
            &serde_json::json!({"order_by": [{"expression": expression, "direction": "asc"}]}),
        )
    };
    let distinct = serde_json::json!({"distinct": true,
        "projection": [{"expression": derived_column("R"), "alias": "R"}]});
    let refused = [
        (
            "ORDER BY CAST(W)",
            order("SONDE_DERIVATE_V", derived_binary(&derived_column("W"))),
            Some(&bytes[..0]),
        ),
        (
            "R = ?",
            derived_plan(
                "SONDE_DERIVATE_V",
                &derived_equal(&derived_column("R"), "binary"),
            ),
            Some(&bytes[..]),
        ),
        (
            "ORDER BY R",
            order("SONDE_DERIVATE_V", derived_column("R")),
            Some(&bytes[..0]),
        ),
        (
            "DISTINCT R",
            derived_plan("SONDE_DERIVATE_V", &distinct),
            Some(&bytes[..0]),
        ),
        (
            "CAST(W2) = ?",
            derived_plan(
                "SONDE_DERIVATE_V",
                &derived_equal(&derived_binary(&derived_column("W2")), "binary"),
            ),
            Some(&bytes[..]),
        ),
        (
            "B della vista = ?",
            derived_plan(
                "SONDE_DERIVATE_V",
                &derived_equal(&derived_column("B"), "binary"),
            ),
            None,
        ),
        (
            "R = ? sul sinonimo",
            derived_plan(
                "SONDE_DERIVATE_S",
                &derived_equal(&derived_column("R"), "binary"),
            ),
            Some(&bytes[..]),
        ),
        (
            "R = ? sulla vista materializzata",
            derived_plan(
                "SONDE_DERIVATE_MV",
                &derived_equal(&derived_column("R"), "binary"),
            ),
            Some(&bytes[..]),
        ),
        (
            "G = ? sulla tabella (SDO_GEOMETRY senza schema)",
            derived_plan(
                DERIVED_TABLE,
                &derived_equal(&derived_column("G"), "binary"),
            ),
            Some(&bytes[..]),
        ),
        (
            "ORDER BY G sulla tabella",
            order(DERIVED_TABLE, derived_column("G")),
            Some(&bytes[..0]),
        ),
    ];
    for (name, plan, values) in refused {
        let (_, judged, _) =
            run_guard_plan(&provider, &secret, &budget, &plan, values, &cancellation).await;
        let error = judged.expect_err(name);
        assert!(
            judgement_refusal(&error, GEOMETRY_SCALAR_OPERAND),
            "{name}: {error}"
        );
    }
    let admitted = [
        (
            "B = ? sulla tabella",
            derived_plan(
                DERIVED_TABLE,
                &derived_equal(&derived_column("B"), "binary"),
            ),
            vec![ParameterValue::Bytes(vec![4])],
            vec!["4"],
        ),
        (
            "ID = ? sulla vista",
            derived_plan(
                "SONDE_DERIVATE_V",
                &derived_equal(&derived_column("ID"), "integer"),
            ),
            vec![ParameterValue::I64(3)],
            vec!["3"],
        ),
        (
            "ID = ? sul sinonimo",
            derived_plan(
                "SONDE_DERIVATE_S",
                &derived_equal(&derived_column("ID"), "integer"),
            ),
            vec![ParameterValue::I64(4)],
            vec!["4"],
        ),
        (
            "ORDER BY ID sulla vista materializzata",
            order("SONDE_DERIVATE_MV", derived_column("ID")),
            Vec::new(),
            vec!["3", "4"],
        ),
    ];
    for (name, plan, values, expected) in admitted {
        let (_, judged, direct) = run_guard_plan(
            &provider,
            &secret,
            &budget,
            &plan,
            Some(&values),
            &cancellation,
        )
        .await;
        let rows = judged.unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(rows, expected, "{name}");
        assert_eq!(Ok(rows), direct, "{name}");
    }

    // F3: la descrizione risolve il proprietario, la lettura proietta G.
    let description = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: None,
                    schema: Some("PLENORA".to_owned()),
                    object: DERIVED_TABLE.to_owned(),
                },
            },
            &cancellation,
        )
        .await
        .expect("catalogo regola B Oracle");
    let description: crate::OracleObjectDescription =
        serde_json::from_value(description.document).expect("documento regola B Oracle");
    let geometry = description
        .columns
        .iter()
        .find(|column| column.name == "G")
        .expect("colonna G");
    assert_eq!(geometry.data_type_owner.as_deref(), Some("MDSYS"));
    assert!(geometry.is_sdo_geometry());
    assert_eq!(geometry.spatial_srid, Some(4326));
    let read = |object: &str, projection: &[&str], filter: Option<&str>| ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: object.to_owned(),
        },
        projection: projection.iter().map(|field| (*field).to_owned()).collect(),
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: filter.map(|field| FilterExpression::Eq {
            field: field.to_owned(),
            parameter: "p".to_owned(),
        }),
        declared_crs: Vec::new(),
    };
    let no_parameters = ParameterBag::default();
    let mut stream = provider
        .read(
            &secret,
            &read(DERIVED_TABLE, &["ID", "G"], None),
            &no_parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("lettura di G senza schema");
    let mut rows = 0;
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .expect("lettura di G senza schema")
    {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    drop(stream);
    // La lettura del provider: la colonna binaria della vista e della vista
    // materializzata si rifiuta.
    let binary = ParameterBag::new(BTreeMap::from([(
        "p".to_owned(),
        ParameterValue::Bytes(wkb),
    )]));
    for (object, field) in [
        ("SONDE_DERIVATE_V", "R"),
        ("SONDE_DERIVATE_V", "W2"),
        ("SONDE_DERIVATE_MV", "R"),
        (DERIVED_TABLE, "G"),
    ] {
        let error = provider
            .read(
                &secret,
                &read(object, &["ID"], Some(field)),
                &binary,
                &budget,
                &cancellation,
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("lettura {object}.{field} ammessa"));
        assert!(
            judgement_refusal(&error, GEOMETRY_SCALAR_OPERAND),
            "{object}.{field}: {error}"
        );
    }
    drop_derived(&provider, &secret, &cancellation).await;
}

/// Il piano reso dal renderer del repository attraverso il percorso giudicato
/// dell'esecuzione (`facade::query_judged`) con i **nomi dei bind** resi, da
/// cui il giudizio legge le famiglie dei valori legati (regola F), e lo
/// stesso SQL senza giudizio, nella stessa transazione.
async fn run_bound_plan(
    provider: &OracleProvider,
    secret: &SecretString,
    budget: &ResourceBudget,
    plan: &serde_json::Value,
    values: &[ParameterValue],
    cancellation: &CancellationToken,
) -> (
    String,
    plenora_database_core::Result<Vec<String>>,
    plenora_database_core::Result<Vec<String>>,
) {
    let operation: QueryOperation = serde_json::from_value(plan.clone()).expect("piano");
    let rendered = Renderer::new(
        Dialect::Oracle,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(ProviderKind::Oracle)
    .render_query(&operation)
    .expect("resa Oracle");
    let binds = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let statement = Statement::new(rendered.sql.clone()).with_params(values.to_vec());
    let mut transaction = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione regola F Oracle");
    let judged = plenora_database_core::facade::query_judged(
        transaction.as_mut(),
        &statement,
        Some(&plenora_database_core::spatial_operand::OperandPlan::Query(
            &operation,
        )),
        &binds,
        cancellation,
    )
    .await
    .map(|rows| guard_ids(&rows));
    let direct = transaction
        .query(&statement, cancellation)
        .await
        .map(|rows| guard_ids(&rows));
    let _ = transaction.rollback(cancellation).await;
    (rendered.sql, judged, direct)
}

/// Il rifiuto della regola F: `invalid_plan`, fase di preparazione.
fn family_refusal(error: &plenora_database_core::DatabaseError) -> bool {
    error.category == plenora_database_core::ErrorCategory::InvalidPlan
        && error.phase == plenora_database_core::ErrorPhase::Prepare
        && error.message == plenora_database_core::spatial_operand::OPERAND_FAMILY_MISMATCH
}

fn untyped_equal(left: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({"filter": {"kind": "compare", "left": left, "operator": "eq",
        "right": {"kind": "parameter", "name": "p"}}})
}

/// Una tabella con `G SDO_GEOMETRY` e due poligoni uguali per
/// `SDO_GEOM.RELATE` con i vertici in ordine diverso; il WKB del primo.
async fn equal_polygons(
    provider: &OracleProvider,
    secret: &SecretString,
    budget: &ResourceBudget,
    table: &str,
    cancellation: &CancellationToken,
) -> Vec<u8> {
    let mut setup = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancellation)
        .await
        .expect("transazione preparazione Oracle");
    for sql in [
        format!("DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '{table}'"),
        format!(
            "INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES ('{table}', 'G', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 4326)"
        ),
        format!("INSERT INTO {table} (ID, B, G) VALUES (3, HEXTORAW('03'), SDO_UTIL.FROM_WKTGEOMETRY('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326))"),
        format!("INSERT INTO {table} (ID, B, G) VALUES (4, HEXTORAW('04'), SDO_UTIL.FROM_WKTGEOMETRY('POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))', 4326))"),
    ] {
        setup
            .execute(&Statement::new(sql), cancellation)
            .await
            .expect("preparazione Oracle");
    }
    let equal = setup
        .query(
            &Statement::new(format!(
                "SELECT A.ID FROM {table} A, {table} B WHERE A.ID = 3 AND B.ID = 4 \
                 AND SDO_GEOM.RELATE(A.G, 'EQUAL', B.G, 0.005) = 'EQUAL'"
            )),
            cancellation,
        )
        .await
        .expect("RELATE Oracle");
    assert_eq!(guard_ids(&equal), vec!["3".to_owned()]);
    let wkb = setup
        .query(
            &Statement::new(format!(
                "SELECT DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(G), 2000, 1) AS W \
                 FROM {table} WHERE ID = 3"
            )),
            cancellation,
        )
        .await
        .expect("WKB Oracle");
    let wkb = match wkb.first().and_then(|row| row.get("W")) {
        Some(ParameterValue::Bytes(bytes)) => bytes.clone(),
        other => panic!("WKB Oracle: {other:?}"),
    };
    setup
        .commit(cancellation)
        .await
        .expect("commit preparazione Oracle");
    wkb
}

/// `SELECT ID FROM PLENORA.<object>` con un filtro `field = :p`.
fn bound_read(object: &str, field: &str) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: object.to_owned(),
        },
        projection: vec!["ID".to_owned()],
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: field.to_owned(),
            parameter: "p".to_owned(),
        }),
        declared_crs: Vec::new(),
    }
}

async fn read_count(
    provider: &OracleProvider,
    secret: &SecretString,
    budget: &ResourceBudget,
    reading: &ReadOperation,
    value: ParameterValue,
    cancellation: &CancellationToken,
) -> plenora_database_core::Result<usize> {
    let parameters = ParameterBag::new(BTreeMap::from([("p".to_owned(), value)]));
    let mut stream = provider
        .read(secret, reading, &parameters, budget, cancellation)
        .await?;
    let mut rows = 0;
    while let Some(batch) = stream.next_batch(cancellation).await? {
        rows += batch.num_rows();
    }
    Ok(rows)
}

/// Regola B per colonna (rilettura 13, R13-1), sull'Oracle reale: una
/// colonna **virtuale** di una tabella
/// (`W RAW(2000) AS (DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(G), 2000, 1))`, il
/// WKB della geometria; una colonna virtuale `BLOB` il server non la ammette,
/// ORA-54003, misurato il 25/10) e `VIRTUAL_COLUMN` `YES` in `ALL_TAB_COLS`. Due
/// poligoni uguali per `SDO_GEOM.RELATE` con i vertici in ordine diverso:
/// lo SQL senza giudizio `W = :1` con il WKB del primo rende soltanto il
/// primo, un risultato sbagliato senza errore. Fino al 25/10 l'origine era
/// della relazione e `W = ?` passava; ora si rifiuta nella transazione e
/// nella lettura. La colonna `RAW` memorizzata e la colonna virtuale
/// numerica restano.
#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_server_computed_binary_columns_are_judged_geometric() {
    use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget R13-1 Oracle");
    let table = "SONDE_CALCOLATE";
    let _ = provider
        .execute_ddl(&secret, "DROP TABLE SONDE_CALCOLATE PURGE", &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            "CREATE TABLE SONDE_CALCOLATE (ID NUMBER(10), B RAW(16), G SDO_GEOMETRY, \
             W RAW(2000) GENERATED ALWAYS AS \
             (DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(G), 2000, 1)) VIRTUAL, \
             N NUMBER GENERATED ALWAYS AS (ID + 1) VIRTUAL)",
            &cancellation,
        )
        .await
        .expect("tabella R13-1 Oracle");
    let wkb = equal_polygons(&provider, &secret, &budget, table, &cancellation).await;
    let bytes = [ParameterValue::Bytes(wkb.clone())];

    let (sql, judged, direct) = run_bound_plan(
        &provider,
        &secret,
        &budget,
        &derived_plan(table, &derived_equal(&derived_column("W"), "binary")),
        &bytes,
        &cancellation,
    )
    .await;
    assert!(sql.contains("\"V\".\"W\" = CAST(:1 AS RAW(2000))"), "{sql}");
    assert_eq!(
        direct.expect("senza giudizio"),
        vec!["3".to_owned()],
        "il server confronta i byte: il secondo poligono, uguale, manca"
    );
    let error = judged.expect_err("W = ? giudicato");
    assert!(
        judgement_refusal(&error, GEOMETRY_SCALAR_OPERAND),
        "{error}"
    );
    let refused = [
        (
            "ORDER BY W",
            derived_plan(
                table,
                &serde_json::json!({"order_by": [{"expression": derived_column("W"), "direction": "asc"}]}),
            ),
            Vec::new(),
        ),
        (
            "W = ? (valore Bytes, bind non tipato)",
            derived_plan(table, &untyped_equal(&derived_column("W"))),
            bytes.to_vec(),
        ),
    ];
    for (name, plan, values) in refused {
        let (_, judged, _) =
            run_bound_plan(&provider, &secret, &budget, &plan, &values, &cancellation).await;
        let error = judged.expect_err(name);
        assert!(
            judgement_refusal(&error, GEOMETRY_SCALAR_OPERAND),
            "{name}: {error}"
        );
    }
    let admitted = [
        (
            "B = ? (memorizzata)",
            derived_plan(table, &derived_equal(&derived_column("B"), "binary")),
            vec![ParameterValue::Bytes(vec![4])],
            vec!["4"],
        ),
        (
            "N = ? (virtuale numerica)",
            derived_plan(table, &derived_equal(&derived_column("N"), "integer")),
            vec![ParameterValue::I64(4)],
            vec!["3"],
        ),
    ];
    for (name, plan, values, expected) in admitted {
        let (_, judged, direct) =
            run_bound_plan(&provider, &secret, &budget, &plan, &values, &cancellation).await;
        let rows = judged.unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(rows, expected, "{name}");
        assert_eq!(Ok(rows), direct, "{name}");
    }
    // La lettura del provider.
    let error = read_count(
        &provider,
        &secret,
        &budget,
        &bound_read(table, "W"),
        ParameterValue::Bytes(wkb.clone()),
        &cancellation,
    )
    .await
    .expect_err("lettura W = ?");
    assert!(
        judgement_refusal(&error, GEOMETRY_SCALAR_OPERAND),
        "lettura: {error}"
    );
    assert_eq!(
        read_count(
            &provider,
            &secret,
            &budget,
            &bound_read(table, "B"),
            ParameterValue::Bytes(vec![3]),
            &cancellation,
        )
        .await
        .expect("lettura B = ?"),
        1
    );
    let _ = provider
        .execute_ddl(&secret, "DROP TABLE SONDE_CALCOLATE PURGE", &cancellation)
        .await;
}

/// Regola F (rilettura 13, R13-2), sull'Oracle reale: una vista rende il WKB
/// come testo (`RAWTOHEX(...)` in `X`, `CAST(raw AS VARCHAR2(4000))` in `Y`),
/// e lo SQL senza giudizio `"V"."X" = CAST(:1 AS RAW(2000))` con il WKB del
/// primo poligono converte da se e rende soltanto il primo, mentre i due sono
/// uguali per `SDO_GEOM.RELATE`. Il giudizio rifiuta il confronto fra il
/// testo e il binario (`invalid_plan`, `OPERAND_FAMILY_MISMATCH`) con il
/// bind tipato, con il `CAST` binario del bind e con un bind non tipato di
/// valore `Bytes`, nella transazione e nella lettura; il `CAST` binario della
/// colonna testo e un binario derivato (`GEOMETRY_SCALAR_OPERAND`). Il testo
/// confrontato con un testo resta: il confine dichiarato del mondo chiuso.
#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_text_columns_compared_with_binary_values_are_refused() {
    use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget R13-2 Oracle");
    let table = "SONDE_TESTO";
    let view = "SONDE_TESTO_V";
    for sql in ["DROP VIEW SONDE_TESTO_V", "DROP TABLE SONDE_TESTO PURGE"] {
        let _ = provider.execute_ddl(&secret, sql, &cancellation).await;
    }
    provider
        .execute_ddl(
            &secret,
            "CREATE TABLE SONDE_TESTO (ID NUMBER(10), B RAW(16), G SDO_GEOMETRY)",
            &cancellation,
        )
        .await
        .expect("tabella R13-2 Oracle");
    let wkb = equal_polygons(&provider, &secret, &budget, table, &cancellation).await;
    provider
        .execute_ddl(
            &secret,
            "CREATE OR REPLACE VIEW SONDE_TESTO_V AS SELECT T.ID AS ID, \
             RAWTOHEX(DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(T.G), 2000, 1)) AS X, \
             CAST(DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(T.G), 2000, 1) AS VARCHAR2(4000)) AS Y \
             FROM SONDE_TESTO T",
            &cancellation,
        )
        .await
        .expect("vista R13-2 Oracle");
    let bytes = [ParameterValue::Bytes(wkb.clone())];
    let cast_bind = serde_json::json!({"kind": "cast",
        "expression": {"kind": "parameter", "name": "p"}, "target_type": "binary"});
    for field in ["X", "Y"] {
        let (sql, judged, direct) = run_bound_plan(
            &provider,
            &secret,
            &budget,
            &derived_plan(
                view,
                &serde_json::json!({"filter": {"kind": "compare", "left": derived_column(field),
                    "operator": "eq", "right": cast_bind}}),
            ),
            &bytes,
            &cancellation,
        )
        .await;
        assert!(
            sql.contains(&format!("\"V\".\"{field}\" = CAST(:1 AS RAW(2000))")),
            "{sql}"
        );
        assert_eq!(
            direct.expect("senza giudizio"),
            vec!["3".to_owned()],
            "{field}: il server converte da se, il secondo poligono uguale manca"
        );
        let error = judged.expect_err(field);
        assert!(family_refusal(&error), "{field} = CAST(? AS RAW): {error}");
        for (name, plan) in [
            (
                "bind binario",
                derived_plan(view, &derived_equal(&derived_column(field), "binary")),
            ),
            (
                "valore Bytes, bind non tipato",
                derived_plan(view, &untyped_equal(&derived_column(field))),
            ),
        ] {
            let (_, judged, _) =
                run_bound_plan(&provider, &secret, &budget, &plan, &bytes, &cancellation).await;
            let error = judged.expect_err(name);
            assert!(family_refusal(&error), "{field} {name}: {error}");
        }
        let (_, judged, _) = run_bound_plan(
            &provider,
            &secret,
            &budget,
            &derived_plan(
                view,
                &derived_equal(&derived_binary(&derived_column(field)), "binary"),
            ),
            &bytes,
            &cancellation,
        )
        .await;
        let error = judged.expect_err("CAST(testo AS RAW) = ?");
        assert!(
            judgement_refusal(&error, GEOMETRY_SCALAR_OPERAND),
            "CAST({field}) = ?: {error}"
        );
    }
    // Il testo confrontato con un testo resta (il confine dichiarato).
    let hex = wkb.iter().fold(String::new(), |mut hex, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02X}");
        hex
    });
    let (_, judged, direct) = run_bound_plan(
        &provider,
        &secret,
        &budget,
        &derived_plan(view, &derived_equal(&derived_column("X"), "string")),
        &[ParameterValue::String(hex)],
        &cancellation,
    )
    .await;
    let rows = judged.expect("X = ? con un testo");
    assert_eq!(rows, vec!["3".to_owned()]);
    assert_eq!(Ok(rows), direct);
    // La lettura del provider.
    let error = read_count(
        &provider,
        &secret,
        &budget,
        &bound_read(view, "X"),
        ParameterValue::Bytes(wkb.clone()),
        &cancellation,
    )
    .await
    .expect_err("lettura X = ? con Bytes");
    assert!(family_refusal(&error), "lettura: {error}");
    for sql in ["DROP VIEW SONDE_TESTO_V", "DROP TABLE SONDE_TESTO PURGE"] {
        let _ = provider.execute_ddl(&secret, sql, &cancellation).await;
    }
}

/// Regola T (quattordicesima lettura del 25/10, difetto 1) su Oracle: un
/// parametro dichiarato `string` legato al valore `I64` 1. Lo SQL senza
/// giudizio `"NAME" = :1` converte la colonna nel numero e incontra `'1'`,
/// `'01'` e `'1.0'`; su d5d7fa1 il lowering delle mutazioni rendeva quel
/// segnaposto nudo, e il giudizio prendeva la famiglia dal tipo dichiarato.
/// Ora il valore si rifiuta prima dell'esecuzione nella mutazione e nella
/// query della transazione; un testo per il testo cancella una riga sola.
#[tokio::test]
#[ignore = "richiede Oracle Spatial live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_typed_parameters_refuse_values_of_another_type() {
    use plenora_database_core::facade::execute_judged;
    use plenora_database_core::relational::MutationOperation;
    use plenora_database_core::spatial_operand::OperandPlan;
    use plenora_database_core::typed_parameter::TYPED_PARAMETER_MISMATCH;
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget regola T Oracle");
    let _ = provider
        .execute_ddl(&secret, "DROP TABLE TIPI_T PURGE", &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            "CREATE TABLE TIPI_T (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(10) NOT NULL)",
            &cancellation,
        )
        .await
        .expect("tabella regola T Oracle");
    let mut setup = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione preparazione");
    for (id, name) in [(1, "1"), (2, "01"), (3, "1.0")] {
        setup
            .execute(
                &Statement::new("INSERT INTO TIPI_T (ID, NAME) VALUES (:1, :2)").with_params(vec![
                    ParameterValue::I32(id),
                    ParameterValue::String(name.to_owned()),
                ]),
                &cancellation,
            )
            .await
            .expect("riga");
    }
    setup
        .commit(&cancellation)
        .await
        .expect("commit preparazione");
    let mutation: MutationOperation = serde_json::from_value(serde_json::json!({
        "type": "delete",
        "target": {"catalog": null, "schema": "PLENORA", "object": "TIPI_T"},
        "filter": {"kind": "compare",
            "left": {"kind": "column", "column": {"relation": null, "field": "NAME"}},
            "operator": "eq",
            "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}}
    }))
    .expect("mutazione");
    let lowered =
        plenora_database_core::compile_relational_mutation(ProviderKind::Oracle, &mutation)
            .expect("lowering");
    let mut tx = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione regola T");
    let silent = tx
        .query(
            &Statement::new("SELECT ID FROM TIPI_T WHERE NAME = :1 ORDER BY ID")
                .with_params(vec![ParameterValue::I64(1)]),
            &cancellation,
        )
        .await
        .expect("senza giudizio");
    assert_eq!(
        guard_ids(&silent),
        vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
        "il server converte la colonna nel numero"
    );
    let statement =
        |value: ParameterValue| Statement::new(lowered.sql.clone()).with_params(vec![value]);
    let plan = OperandPlan::Mutation(&mutation);
    let error = execute_judged(
        tx.as_mut(),
        &statement(ParameterValue::I64(1)),
        Some(&plan),
        &lowered.bind_names,
        &cancellation,
    )
    .await
    .expect_err("intero per un testo dichiarato");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan,
        "{error:?}"
    );
    assert_eq!(error.message, TYPED_PARAMETER_MISMATCH, "{error:?}");
    let deleted = execute_judged(
        tx.as_mut(),
        &statement(ParameterValue::String("1".to_owned())),
        Some(&plan),
        &lowered.bind_names,
        &cancellation,
    )
    .await
    .expect("testo per il testo");
    // La forma del renderer delle query.
    assert!(
        lowered.sql.ends_with("= CAST(:1 AS VARCHAR2(4000))"),
        "{}",
        lowered.sql
    );
    assert_eq!(deleted, 1, "il CAST confronta il testo");
    let _ = tx.rollback(&cancellation).await;
    // La query della transazione (lo SDK), con lo stesso predicato.
    let query = derived_plan("TIPI_T", &derived_equal(&derived_column("NAME"), "string"));
    let (_, judged, _) = run_bound_plan(
        &provider,
        &secret,
        &budget,
        &query,
        &[ParameterValue::I64(1)],
        &cancellation,
    )
    .await;
    let error = judged.expect_err("query con un intero");
    assert_eq!(error.message, TYPED_PARAMETER_MISMATCH, "{error:?}");
    let (_, judged, _) = run_bound_plan(
        &provider,
        &secret,
        &budget,
        &query,
        &[ParameterValue::String("1".to_owned())],
        &cancellation,
    )
    .await;
    assert_eq!(judged.expect("query con un testo"), vec!["1".to_owned()]);
    provider
        .execute_ddl(&secret, "DROP TABLE TIPI_T PURGE", &cancellation)
        .await
        .expect("pulizia regola T Oracle");
}

/// Quindicesima lettura del 25/10 sull'Oracle reale, regole C e A.
///
/// - **Regola C.** `CAST(:1 AS NUMBER(38, 10))` arrotonda `0.12345678901` e
///   ritrova `NUMBER(30, 12)` `0.123456789` (misurato, anche con un
///   `BINARY_DOUBLE`). `D = CAST(:p AS decimal)` oltre la scala, con un
///   `Decimal` o un `F64`, e il `CAST` di una colonna `NUMBER(30, 12)` si
///   rifiutano (`CAST_NOT_EXACT`) nel percorso giudicato (`query_judged`, lo
///   SDK); entro la scala e da `NUMBER(20, 4)` restano.
/// - **Regola A.** Il renderer quota: `"k"` non lega la colonna `"K"`, e si
///   rifiuta anche senza geometrie (`NAME_UNRESOLVED`).
#[tokio::test]
#[ignore = "richiede Oracle live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_rilettura_quindici_cast_e_nomi() {
    use plenora_database_core::cast_table::CAST_NOT_EXACT;
    use plenora_database_core::facade::query_judged;
    use plenora_database_core::spatial_operand::{OperandPlan, NAME_UNRESOLVED};
    use serde_json::{json, Value};
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let _ = provider
        .execute_ddl(&secret, "DROP TABLE R15_T PURGE", &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            "CREATE TABLE R15_T (ID NUMBER(10) PRIMARY KEY, D NUMBER(30, 12) NOT NULL, \
             E NUMBER(20, 4) NOT NULL, K NUMBER(10) NOT NULL)",
            &cancellation,
        )
        .await
        .expect("tabella");
    let mut setup = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione preparazione");
    for sql in [
        "INSERT INTO R15_T VALUES (1, 0.123456789, 1.5, 1)",
        "INSERT INTO R15_T VALUES (2, 0.5, 2.5, 2)",
    ] {
        setup
            .execute(&Statement::new(sql), &cancellation)
            .await
            .expect("riga");
    }
    setup.commit(&cancellation).await.expect("commit");
    let mut tx = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione");
    // La tabella del modulo `cast_table`, rimisurata.
    for value in [
        ParameterValue::String("0.12345678901".to_owned()),
        ParameterValue::F64(0.123_456_789_01),
    ] {
        let silent = tx
            .query(
                &Statement::new("SELECT ID FROM R15_T WHERE D = CAST(:1 AS NUMBER(38, 10))")
                    .with_params(vec![value.clone()]),
                &cancellation,
            )
            .await
            .expect("senza giudizio");
        assert_eq!(guard_ids(&silent), vec!["1".to_owned()], "{value:?}");
    }
    let column =
        |field: &str| json!({"kind": "column", "column": {"relation": "t", "field": field}});
    let typed = |parameter_type: &str| json!({"kind": "typed_parameter", "name": "p", "parameter_type": parameter_type});
    let parameter = || json!({"kind": "parameter", "name": "p"});
    let cast = |expression: Value| json!({"kind": "cast", "expression": expression, "target_type": "decimal"});
    let plan = |left: Value, right: Value| -> QueryOperation {
        serde_json::from_value(json!({
            "source": {"object": {"catalog": null, "schema": "PLENORA", "object": "R15_T"}, "alias": "t"},
            "projection": [{"expression": column("ID"), "alias": "ID"}],
            "filter": {"kind": "compare", "left": left, "operator": "eq", "right": right},
            "having": null, "row_limit": null
        }))
        .expect("piano")
    };
    let renderer = Renderer::new(
        Dialect::Oracle,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(ProviderKind::Oracle);
    let refused = [
        (
            "D = CAST(:p AS decimal) oltre la scala",
            plan(column("D"), cast(parameter())),
            ParameterValue::Decimal("0.12345678901".to_owned()),
            CAST_NOT_EXACT,
        ),
        (
            "D = CAST(F64 AS decimal)",
            plan(column("D"), cast(parameter())),
            ParameterValue::F64(0.123_456_789_01),
            CAST_NOT_EXACT,
        ),
        (
            "CAST(D NUMBER(30, 12) AS decimal) = ?",
            plan(cast(column("D")), typed("decimal")),
            ParameterValue::Decimal("0.5".to_owned()),
            CAST_NOT_EXACT,
        ),
        (
            "\"k\" non lega \"K\"",
            plan(column("k"), typed("integer")),
            ParameterValue::I32(1),
            NAME_UNRESOLVED,
        ),
    ];
    let admitted = [
        (
            "D = CAST(:p AS decimal) entro la scala",
            plan(column("D"), cast(parameter())),
            ParameterValue::Decimal("0.5".to_owned()),
        ),
        (
            "CAST(E NUMBER(20, 4) AS decimal) = ?",
            plan(cast(column("E")), typed("decimal")),
            ParameterValue::Decimal("2.5".to_owned()),
        ),
    ];
    for (form, operation, value, message) in refused {
        let resa = renderer.render_query(&operation).expect("resa");
        let names = resa
            .binds
            .iter()
            .map(|bind| bind.name.clone())
            .collect::<Vec<_>>();
        let error = query_judged(
            tx.as_mut(),
            &Statement::new(resa.sql.clone()).with_params(vec![value; names.len()]),
            Some(&OperandPlan::Query(&operation)),
            &names,
            &cancellation,
        )
        .await
        .expect_err(form);
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan,
            "{form}: {error:?}"
        );
        assert_eq!(error.message, message, "{form}: {error:?}");
    }
    for (form, operation, value) in admitted {
        let resa = renderer.render_query(&operation).expect("resa");
        let names = resa
            .binds
            .iter()
            .map(|bind| bind.name.clone())
            .collect::<Vec<_>>();
        let rows = query_judged(
            tx.as_mut(),
            &Statement::new(resa.sql.clone()).with_params(vec![value; names.len()]),
            Some(&OperandPlan::Query(&operation)),
            &names,
            &cancellation,
        )
        .await
        .unwrap_or_else(|error| panic!("{form}: {error:?}"));
        assert_eq!(rows.len(), 1, "{form}");
    }
    let _ = tx.rollback(&cancellation).await;
    let _ = provider
        .execute_ddl(&secret, "DROP TABLE R15_T PURGE", &cancellation)
        .await;
}

/// Sedicesima lettura del 25/10 sull'Oracle reale, mondo chiuso: una tabella
/// temporanea privata (`ORA$PTT_*`) non e in `ALL_TAB_COLS`, ma il server la
/// legge nella sessione che l'ha creata.
///
/// `N NUMBER = '01'` rende la riga per conversione implicita (misurato qui).
/// Su c0f82ba il catalogo la diceva assente (`RelationColumns::Absent`), il
/// riferimento si assumeva scalare e il piano passava senza la regola F; ora
/// un riferimento a una relazione assente non si lega e si rifiuta in ogni
/// piano (`NAME_UNRESOLVED`).
#[tokio::test]
#[ignore = "richiede Oracle live esplicito"]
async fn live_rilettura_sedici_tabella_temporanea_privata() {
    use plenora_database_core::facade::query_judged;
    use plenora_database_core::spatial_operand::{OperandPlan, NAME_UNRESOLVED};
    use serde_json::json;
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione");
    for sql in [
        "CREATE PRIVATE TEMPORARY TABLE ORA$PTT_R16 (ID NUMBER(10), N NUMBER(10)) \
         ON COMMIT PRESERVE DEFINITION",
        "INSERT INTO ORA$PTT_R16 VALUES (1, 1)",
    ] {
        tx.execute(&Statement::new(sql), &cancellation)
            .await
            .expect("tabella temporanea privata");
    }
    let catalogo = tx
        .query(
            &Statement::new(
                "SELECT COUNT(*) AS ID FROM ALL_TAB_COLS WHERE TABLE_NAME = 'ORA$PTT_R16'",
            ),
            &cancellation,
        )
        .await
        .expect("ALL_TAB_COLS");
    assert_eq!(
        guard_ids(&catalogo),
        vec!["0".to_owned()],
        "fuori dal catalogo"
    );
    let silent = tx
        .query(
            &Statement::new("SELECT ID FROM ORA$PTT_R16 WHERE N = :1")
                .with_params(vec![ParameterValue::String("01".to_owned())]),
            &cancellation,
        )
        .await
        .expect("senza giudizio");
    assert_eq!(
        guard_ids(&silent),
        vec!["1".to_owned()],
        "conversione implicita"
    );
    let operation: QueryOperation = serde_json::from_value(json!({
        "source": {"object": {"catalog": null, "schema": "PLENORA", "object": "ORA$PTT_R16"}, "alias": "t"},
        "projection": [{"expression": {"kind": "column", "column": {"relation": "t", "field": "ID"}}, "alias": "ID"}],
        "filter": {"kind": "compare", "operator": "eq",
            "left": {"kind": "column", "column": {"relation": "t", "field": "N"}},
            "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "string"}},
        "having": null, "row_limit": null
    }))
    .expect("piano");
    let renderer = Renderer::new(
        Dialect::Oracle,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_provider(ProviderKind::Oracle);
    let resa = renderer.render_query(&operation).expect("resa");
    let names = resa
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let error = query_judged(
        tx.as_mut(),
        &Statement::new(resa.sql.clone())
            .with_params(vec![ParameterValue::String("01".to_owned()); names.len()]),
        Some(&OperandPlan::Query(&operation)),
        &names,
        &cancellation,
    )
    .await
    .expect_err("N = testo su una tabella temporanea privata");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan,
        "{error:?}"
    );
    assert_eq!(error.message, NAME_UNRESOLVED, "{error:?}");
    let _ = tx.rollback(&cancellation).await;
}
