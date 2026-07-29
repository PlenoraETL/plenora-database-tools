use crate::{
    describe_object, list_objects, list_schemas, prepare_write, probe_server, read_object,
    write_prepared, CertificatePolicy, SqlServerConfig, SqlServerPool, SqlServerSession,
};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, Decimal128Array, Int32Array, StringArray,
};
use plenora_database_core::arrow::{DataType, Field, RecordBatch, Schema, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteStatus;
use plenora_database_core::plan::{
    ObjectRef, SridPolicy, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::protocol;
use plenora_database_core::provider::{BatchStream, ProviderFuture, SecretString};
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, RemoteEffect, ResourceBudget, ResourceLimits,
    RetryDisposition,
};
use std::collections::VecDeque;
use std::sync::Arc;
use tiberius::Query;

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
