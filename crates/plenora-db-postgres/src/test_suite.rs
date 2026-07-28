use super::*;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use plenora_database_core::geometry::GEOARROW_WKB_EXTENSION_NAME;
use plenora_database_core::RemoteEffect;

#[cfg(test)]
impl PostgresProvider {
    fn test_budget() -> Result<ResourceBudget> {
        ResourceBudget::new(plenora_database_core::resource::ResourceLimits::default())
    }

    fn read_with_test_budget<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            let budget = Self::test_budget()?;
            Provider::read(self, secret, operation, parameters, &budget, cancellation).await
        })
    }

    fn query_with_test_budget<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a QueryOperation,
        parameters: &'a ParameterBag,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            let budget = Self::test_budget()?;
            Provider::query(self, secret, operation, parameters, &budget, cancellation).await
        })
    }

    fn prepare_write_with_test_budget<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            let budget = Self::test_budget()?;
            Provider::prepare_write(self, secret, operation, input_schema, &budget, cancellation)
                .await
        })
    }

    fn write_with_prepared_budget<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async move {
            let budget = prepared.budget.clone();
            Provider::write(self, secret, prepared, input, &budget, cancellation).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::geometry::SpatialSemantics;
    use plenora_database_core::loss::MappingPolicy;
    use plenora_database_core::outcome::WriteStatus;
    use plenora_database_core::plan::{
        ComparisonOperator, LayerId, OrderBy, SridPolicy, TransactionProfile, WriteMode,
    };
    use plenora_database_core::query::{
        ColumnRef, JoinKind, QueryDerivedSource, QueryExpression, QueryJoin, QueryOperation,
        QueryOrdering, QueryProjection, QuerySetOperation, QuerySetOperator, QuerySource,
        ScalarFunction, SpatialOperator,
    };
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::OnceLock;

    async fn wait_for_query_state(
        client: &tokio_postgres::Client,
        marker: &str,
        expected_active: bool,
    ) {
        let pattern = format!("%{marker}%");
        tokio::time::timeout(StdDuration::from_secs(2), async {
            loop {
                let active: i64 = client
                    .query_one(
                        "SELECT count(*)
                         FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND state = 'active'
                           AND query LIKE $1
                           AND pid <> pg_backend_pid()",
                        &[&pattern],
                    )
                    .await
                    .expect("query backend cancellation state")
                    .get(0);
                if (active > 0) == expected_active {
                    break;
                }
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "query containing {marker:?} did not become {} within the bounded observation",
                if expected_active {
                    "active"
                } else {
                    "inactive"
                }
            )
        });
    }

    async fn wait_for_active_query(client: &tokio_postgres::Client, marker: &str) {
        wait_for_query_state(client, marker, true).await;
    }

    async fn wait_for_no_active_query(client: &tokio_postgres::Client, marker: &str) {
        wait_for_query_state(client, marker, false).await;
    }

    #[test]
    fn postgres_arrow_schema_declares_contract_version() {
        let schema = contract_schema(vec![Field::new("value", DataType::Int64, false)]);
        assert_eq!(
            schema
                .metadata()
                .get(protocol::CONTRACT_VERSION_KEY)
                .map(String::as_str),
            Some(protocol::CONTRACT_VERSION)
        );
    }

    struct NeverCancelled;
    struct AlwaysCancelled;
    struct NoBatchStream {
        schema: SchemaRef,
    }

    impl BatchStream for NoBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
            Box::pin(std::future::ready(Ok(None)))
        }
    }

    impl Deref for NeverCancelled {
        type Target = CancellationToken;

        fn deref(&self) -> &Self::Target {
            static TOKEN: OnceLock<CancellationToken> = OnceLock::new();
            TOKEN.get_or_init(CancellationToken::new)
        }
    }

    impl Deref for AlwaysCancelled {
        type Target = CancellationToken;

        fn deref(&self) -> &Self::Target {
            static TOKEN: OnceLock<CancellationToken> = OnceLock::new();
            TOKEN.get_or_init(|| {
                let token = CancellationToken::new();
                token.cancel();
                token
            })
        }
    }

    #[test]
    fn decimal_parser_preserves_scale() {
        assert_eq!(parse_decimal128("123.45", 4).expect("decimal"), 1_234_500);
        assert_eq!(parse_decimal128("-0.01", 2).expect("decimal"), -1);
        assert_eq!(parse_decimal128("12300", -2).expect("negative scale"), 123);
    }

    #[tokio::test]
    async fn write_rejects_budget_substitution_before_connecting() {
        let prepared_budget = PostgresProvider::test_budget().expect("prepared budget");
        let other_budget = PostgresProvider::test_budget().expect("other budget");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_id",
            DataType::Int64,
            false,
        )]));
        let prepared = PreparedWrite {
            operation: WriteOperation {
                target: ObjectRef {
                    catalog: None,
                    schema: Some("public".to_owned()),
                    object: "never_reached".to_owned(),
                    layer_id: None,
                },
                mode: WriteMode::Append,
                mapping_policy: MappingPolicy::Strict,
                transaction_profile: TransactionProfile::SingleTransaction,
                keys: Vec::new(),
                update_columns: Vec::new(),
                srid_policy: None,
                create_spatial_index: false,
                allow_partial: false,
            },
            loss_report: plenora_database_core::loss::LossReport {
                schema_version: 1,
                policy: MappingPolicy::Strict,
                losses: Vec::new(),
            },
            budget: prepared_budget.clone(),
            operation_lease: prepared_budget
                .try_lease(ResourceKind::ConcurrentOperations, 1)
                .expect("operation lease"),
            columns_lease: prepared_budget
                .try_lease(ResourceKind::Columns, 1)
                .expect("columns lease"),
        };
        let error = Provider::write(
            &PostgresProvider::default(),
            &SecretString::new("host=must-not-connect.invalid"),
            prepared,
            Box::new(NoBatchStream { schema }),
            &other_budget,
            &CancellationToken::new(),
        )
        .await
        .expect_err("budget substitution");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Validate);
    }

    #[test]
    fn network_options_override_dsn_and_partition_the_pool() {
        let secret = SecretString::new("host=localhost user=fixture");
        let options = PostgresNetworkOptions {
            connect_timeout_ms: 1_500,
            tcp_user_timeout_ms: 9_000,
            keepalive_idle_secs: 11,
            keepalive_interval_secs: 4,
            keepalive_retries: 7,
        };
        let config = connection_config(&secret, options).expect("network config");
        assert_eq!(
            config.get_connect_timeout(),
            Some(&StdDuration::from_millis(1_500))
        );
        assert_eq!(
            config.get_tcp_user_timeout(),
            Some(&StdDuration::from_millis(9_000))
        );
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), StdDuration::from_secs(11));
        assert_eq!(
            config.get_keepalives_interval(),
            Some(StdDuration::from_secs(4))
        );
        assert_eq!(config.get_keepalives_retries(), Some(7));
        assert_eq!(
            connection_config_for_mode(&secret, options, PostgresTlsMode::Disabled)
                .expect("disabled TLS config")
                .get_ssl_mode(),
            SslMode::Disable
        );
        assert_eq!(
            connection_config_for_mode(&secret, options, PostgresTlsMode::Require)
                .expect("required TLS config")
                .get_ssl_mode(),
            SslMode::Require
        );
        let tls_config = PostgresTlsConfig::webpki();
        assert_ne!(
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                options,
                30_000,
                5_000,
            ),
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                PostgresNetworkOptions::default(),
                30_000,
                5_000,
            )
        );
        assert_ne!(
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                options,
                30_000,
                5_000,
            ),
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                options,
                1_000,
                500,
            )
        );
        let mut session_config =
            connection_config_for_mode(&secret, options, PostgresTlsMode::Disabled)
                .expect("session config");
        configure_session_startup(&mut session_config, 1_234, 567).expect("session defaults");
        assert_eq!(
            session_config.get_application_name(),
            Some("plenora-database-tools")
        );
        let startup_options = session_config.get_options().expect("startup options");
        assert!(startup_options.contains("statement_timeout=1234ms"));
        assert!(startup_options.contains("lock_timeout=567ms"));
    }

    #[test]
    fn performance_profiles_are_stable_and_composable() {
        assert_eq!(PostgresPerformanceProfile::LowLatency.batch_rows(), 1_024);
        assert_eq!(
            PostgresPerformanceProfile::LowLatency.insert_mode(),
            PostgresInsertMode::CopyText
        );
        assert_eq!(
            PostgresPerformanceProfile::LowLatency.target_batch_bytes(),
            1024 * 1024
        );
        assert_eq!(PostgresPerformanceProfile::BalancedBulk.batch_rows(), 8_192);
        assert_eq!(
            PostgresPerformanceProfile::BalancedBulk.insert_mode(),
            PostgresInsertMode::CopyBinary
        );
        assert_eq!(
            PostgresPerformanceProfile::BalancedBulk.target_batch_bytes(),
            4 * 1024 * 1024
        );
        assert_eq!(
            serde_json::to_string(&PostgresPerformanceProfile::BalancedBulk)
                .expect("serialize profile"),
            "\"balanced_bulk\""
        );

        let low_latency = PostgresProvider::default();
        assert_eq!(low_latency.batch_rows, 1_024);
        assert_eq!(low_latency.insert_mode, PostgresInsertMode::CopyText);
        assert_eq!(low_latency.target_batch_bytes, Some(1024 * 1024));

        let bulk = PostgresProvider::new(7)
            .with_timeouts(321, 123)
            .with_performance_profile(PostgresPerformanceProfile::BalancedBulk);
        assert_eq!(bulk.batch_rows, 8_192);
        assert_eq!(bulk.insert_mode, PostgresInsertMode::CopyBinary);
        assert_eq!(bulk.target_batch_bytes, Some(4 * 1024 * 1024));
        assert_eq!(bulk.statement_timeout_ms, 321);
        assert_eq!(bulk.lock_timeout_ms, 123);

        let manual = bulk
            .with_target_batch_bytes(123_456)
            .without_target_batch_bytes();
        assert_eq!(manual.target_batch_bytes, None);
    }

    #[test]
    fn poisoned_internal_mutex_is_recovered() {
        let state = Arc::new(Mutex::new(7_u8));
        let poisoned = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.lock().expect("initial lock");
            panic!("intentional poison");
        })
        .join();
        assert_eq!(*lock_recover(&state), 7);
    }

    #[test]
    fn parameterized_fast_path_planning_is_typed_and_conservative() {
        fn column(name: &str, native_type: &str, type_kind: &str) -> ColumnSpec {
            ColumnSpec {
                name: name.to_owned(),
                native_type: native_type.to_owned(),
                nullable: true,
                numeric_precision: None,
                numeric_scale: None,
                spatial_srid: None,
                spatial_dimensions: None,
                spatial_type: None,
                spatial_crs_id: None,
                default_expression: None,
                identity_kind: None,
                generated_kind: None,
                native_declaration: None,
                type_kind: Some(type_kind.to_owned()),
                composite_fields: Vec::new(),
                enum_labels: Vec::new(),
                domain_base_type: None,
                domain_constraints: Vec::new(),
                collation: None,
                kind: ColumnKind::Utf8,
            }
        }

        let integer_filter = FilterExpression::Eq {
            field: "event_id".to_owned(),
            parameter: "event_id".to_owned(),
        };
        let integer_parameters = ParameterBag::new(BTreeMap::from([(
            "event_id".to_owned(),
            ParameterValue::I64(42),
        )]));
        assert_eq!(
            typed_filter_parameter_types(
                Some(&integer_filter),
                &["event_id".to_owned()],
                &integer_parameters,
                &[column("event_id", "int8", "b")],
            ),
            Some(vec![Type::INT8])
        );

        let custom_parameters = ParameterBag::new(BTreeMap::from([(
            "status".to_owned(),
            ParameterValue::String("ready".to_owned()),
        )]));
        let custom_filter = FilterExpression::Eq {
            field: "status".to_owned(),
            parameter: "status".to_owned(),
        };
        assert_eq!(
            typed_filter_parameter_types(
                Some(&custom_filter),
                &["status".to_owned()],
                &custom_parameters,
                &[column("status", "job_status", "e")],
            ),
            None
        );

        let spatial_filter = FilterExpression::Spatial {
            function: SpatialFunction::DWithin,
            field: "geom".to_owned(),
            geometry_parameter: Some("probe".to_owned()),
            distance_parameter: Some("radius".to_owned()),
        };
        let spatial_parameters = ParameterBag::new(BTreeMap::from([
            (
                "probe".to_owned(),
                ParameterValue::Wkb {
                    bytes: vec![1, 2, 3],
                    srid: Some(4326),
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ),
            ("radius".to_owned(), ParameterValue::F64(100.0)),
        ]));
        assert_eq!(
            typed_filter_parameter_types(
                Some(&spatial_filter),
                &["probe".to_owned(), "radius".to_owned()],
                &spatial_parameters,
                &[column("geom", "geometry", "b")],
            ),
            Some(vec![Type::BYTEA, Type::FLOAT8])
        );
    }

    #[tokio::test]
    async fn live_adaptive_read_batches_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(10_000)
            .with_target_batch_bytes(64 * 1024)
            .with_byte_limits(256 * 1024, 64 * 1024 * 1024);
        let secret = SecretString::new(dsn);
        let mut stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection: Vec::new(),
                    order_by: Vec::new(),
                    row_limit: Some(10_000),
                    filter: None,
                },
                &ParameterBag::default(),
                &NeverCancelled,
            )
            .await
            .expect("adaptive stream");
        let mut rows = 0_usize;
        let mut batches = 0_usize;
        let mut max_rows = 0_usize;
        while let Some(batch) = stream.next_batch().await.expect("adaptive batch") {
            assert!(batch_memory_bytes(&batch) <= 256 * 1024);
            rows += batch.num_rows();
            batches += 1;
            max_rows = max_rows.max(batch.num_rows());
        }
        assert_eq!(rows, 10_000);
        assert!(batches > 1);
        assert!(max_rows < 10_000);
        assert!(provider.metrics_snapshot().read_target_limited_batches > 0);
    }

    #[tokio::test]
    async fn live_read_budget_fails_closed_before_exceeding_rows() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let limits = plenora_database_core::resource::ResourceLimits {
            memory_bytes: 256 * 1024,
            rows: 3,
            output_bytes: 256 * 1024,
            cell_bytes: 64 * 1024,
            ..plenora_database_core::resource::ResourceLimits::default()
        };
        let budget = ResourceBudget::new(limits).expect("budget");
        let provider = PostgresProvider::new(10);
        let secret = SecretString::new(dsn);
        let cancellation = CancellationToken::new();
        let operation = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned()],
            order_by: Vec::new(),
            row_limit: Some(10),
            filter: None,
        };
        let mut stream = Provider::read(
            &provider,
            &secret,
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("budgeted stream");
        let first = stream
            .next_batch()
            .await
            .expect("first batch")
            .expect("rows");
        assert_eq!(first.num_rows(), 3);
        let error = stream
            .next_batch()
            .await
            .expect_err("row budget must be exhausted");
        assert_eq!(error.category, ErrorCategory::ResourceLimit);
        assert_eq!(budget.remaining(ResourceKind::Rows), 0);
        drop(stream);
        assert_eq!(
            budget.remaining(ResourceKind::ConcurrentOperations),
            budget.limits().concurrent_operations
        );
        assert_eq!(
            budget.remaining(ResourceKind::Columns),
            budget.limits().columns
        );
    }

    #[tokio::test]
    async fn live_geometry_component_budget_rejects_before_emission() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let limits = plenora_database_core::resource::ResourceLimits {
            memory_bytes: 256 * 1024,
            rows: 1,
            geometry_components: 1,
            output_bytes: 256 * 1024,
            cell_bytes: 64 * 1024,
            ..plenora_database_core::resource::ResourceLimits::default()
        };
        let budget = ResourceBudget::new(limits).expect("geometry budget");
        let provider = PostgresProvider::new(1);
        let secret = SecretString::new(dsn);
        let cancellation = CancellationToken::new();
        let operation = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            projection: vec!["geom".to_owned()],
            order_by: Vec::new(),
            row_limit: Some(1),
            filter: None,
        };
        let mut stream = Provider::read(
            &provider,
            &secret,
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("geometry stream");
        let error = stream
            .next_batch()
            .await
            .expect_err("point needs geometry plus coordinate component");
        assert_eq!(error.category, ErrorCategory::ResourceLimit);
        assert_eq!(
            budget.remaining(ResourceKind::GeometryComponents),
            budget.limits().geometry_components
        );
    }

    #[tokio::test]
    async fn live_read_duration_budget_cancels_backend() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let secret = SecretString::new(dsn);
        let setup = PostgresProvider::connect(&secret).await.expect("setup");
        setup
            .batch_execute(
                "CREATE OR REPLACE VIEW plenora_fixture.deadline_slow_events AS
                 SELECT value::bigint AS event_id
                 FROM generate_series(1, 10) AS value
                 CROSS JOIN LATERAL
                    pg_sleep((value * 0 + 100)::double precision / 1000)",
            )
            .await
            .expect("slow deadline view");
        let limits = plenora_database_core::resource::ResourceLimits {
            duration_ms: 50,
            ..plenora_database_core::resource::ResourceLimits::default()
        };
        let budget = ResourceBudget::new(limits).expect("duration budget");
        let provider = PostgresProvider::new(10);
        let cancellation = CancellationToken::new();
        let operation = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "deadline_slow_events".to_owned(),
                layer_id: None,
            },
            projection: Vec::new(),
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        };
        let result = Provider::read(
            &provider,
            &secret,
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await;
        let error = match result {
            Ok(mut stream) => stream
                .next_batch()
                .await
                .expect_err("read duration deadline"),
            Err(error) => error,
        };
        assert_eq!(error.category, ErrorCategory::Timeout);
        wait_for_no_active_query(&setup, "\"plenora_fixture\".\"deadline_slow_events\"").await;
    }

    #[tokio::test]
    async fn live_resolved_crs_must_match_spatial_ref_sys() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let metadata = HashMap::from([
            (
                protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                GEOARROW_WKB_EXTENSION_NAME.to_owned(),
            ),
            (
                protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                "geometry".to_owned(),
            ),
            (protocol::GEOMETRY_ENCODING.to_owned(), "ewkb".to_owned()),
            (
                protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                "exact".to_owned(),
            ),
            (protocol::GEOMETRY_TYPES.to_owned(), "point".to_owned()),
            (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
            (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
            (
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "resolved".to_owned(),
            ),
            (protocol::GEOMETRY_CRS_ID.to_owned(), "EPSG:3857".to_owned()),
            (
                protocol::GEOMETRY_AXIS_ORDER.to_owned(),
                "unknown".to_owned(),
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "geom",
            DataType::Binary,
            false,
        )
        .with_metadata(metadata)]));
        let operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "crs_validation_never_created".to_owned(),
                layer_id: None,
            },
            mode: WriteMode::Create,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: Some(SridPolicy::RequireMatch),
            create_spatial_index: false,
            allow_partial: false,
        };
        let error = PostgresProvider::default()
            .prepare_write_with_test_budget(
                &SecretString::new(dsn),
                &operation,
                schema,
                &CancellationToken::new(),
            )
            .await
            .err()
            .expect("mismatched resolved CRS");
        assert_eq!(error.category, ErrorCategory::Crs);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    #[test]
    fn tls_material_is_validated_and_redacted() {
        let config = PostgresTlsConfig::webpki();
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("PRIVATE KEY"));
        assert!(PostgresTlsConfig::private_ca_pem(b"not a certificate").is_err());
        assert!(
            PostgresTlsConfig::from_pem(false, &[], None, None).is_err(),
            "empty trust store accepted"
        );
        assert!(
            PostgresTlsConfig::from_pem(true, &[], Some(b"certificate"), None).is_err(),
            "partial client identity accepted"
        );
    }

    #[tokio::test]
    async fn live_private_ca_mtls_and_cancellation_when_configured() {
        let (Ok(dsn), Ok(ca_path), Ok(client_certificate_path), Ok(client_private_key_path)) = (
            std::env::var("PLENORA_TEST_POSTGRES_TLS_DSN"),
            std::env::var("PLENORA_TEST_POSTGRES_TLS_CA"),
            std::env::var("PLENORA_TEST_POSTGRES_TLS_CLIENT_CERT"),
            std::env::var("PLENORA_TEST_POSTGRES_TLS_CLIENT_KEY"),
        ) else {
            return;
        };
        let ca = std::fs::read(ca_path).expect("read private CA");
        let client_certificate =
            std::fs::read(client_certificate_path).expect("read client certificate");
        let client_private_key =
            std::fs::read(client_private_key_path).expect("read client private key");
        let tls_config = PostgresTlsConfig::private_ca_with_client_identity_pem(
            &ca,
            &client_certificate,
            &client_private_key,
        )
        .expect("build mTLS config");
        let provider = PostgresProvider::new(100)
            .with_pool_size(2, 5_000)
            .with_tls_config(tls_config.clone());
        let secret = SecretString::new(dsn);
        let info = provider
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect("mTLS connection");
        assert_eq!(info.provider, ProviderKind::Postgres);

        let setup = PostgresProvider::connect_with_tls(
            &secret,
            PostgresTlsMode::Require,
            &tls_config,
            PostgresNetworkOptions::default(),
            30_000,
            5_000,
        )
        .await
        .expect("mTLS setup connection");
        setup
            .batch_execute(
                "CREATE OR REPLACE VIEW plenora_fixture.mtls_slow_events AS
                 SELECT value::bigint AS event_id
                 FROM generate_series(1, 100) AS value
                 CROSS JOIN LATERAL
                    pg_sleep((value * 0 + 50)::double precision / 1000)",
            )
            .await
            .expect("mTLS slow view");

        let inflight_cancellation = CancellationToken::new();
        let toggle = inflight_cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(StdDuration::from_millis(75)).await;
            toggle.cancel();
        });
        let error = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "mtls_slow_events".to_owned(),
                        layer_id: None,
                    },
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: None,
                },
                &ParameterBag::default(),
                &inflight_cancellation,
            )
            .await
            .err()
            .expect("mTLS server-side cancellation");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        provider
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect("mTLS recovery");
        let metrics = provider.metrics_snapshot();
        assert!(metrics.cancellations >= 1);
        assert!(metrics.invalidated_sessions >= 1);

        let untrusted_error = PostgresProvider::new(1)
            .with_tls_mode(PostgresTlsMode::Require)
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect_err("private CA accepted by WebPKI");
        assert!(!untrusted_error.to_string().contains("PRIVATE KEY"));
        let missing_identity = PostgresProvider::new(1)
            .with_tls_config(PostgresTlsConfig::private_ca_pem(&ca).expect("private CA"))
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect_err("mTLS accepted without client identity");
        assert!(!missing_identity
            .to_string()
            .contains("dataflow_tls_test_2026"));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    // Un'unica sessione live condivide setup e credenziali fra tutte le
    // asserzioni di conformità read, quoting e redazione.
    async fn live_postgis_read_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(777);
        let secret = SecretString::new(dsn);
        let cancellation = NeverCancelled;
        let info = provider
            .test_connection(&secret, &cancellation)
            .await
            .expect("connection");
        assert_eq!(info.provider, ProviderKind::Postgres);
        assert!(provider.pool_idle_connections() >= 1);
        let capabilities = provider
            .probe_capabilities(&secret, &cancellation)
            .await
            .expect("capabilities");
        assert!(capabilities.spatial.read_wkb);
        let operation = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None::<LayerId>,
            },
            projection: vec![
                "event_id".to_owned(),
                "amount".to_owned(),
                "occurred_at".to_owned(),
                "geom".to_owned(),
                "geog".to_owned(),
            ],
            order_by: vec![],
            row_limit: None,
            filter: None,
        };
        let mut stream = provider
            .read_with_test_budget(&secret, &operation, &ParameterBag::default(), &cancellation)
            .await
            .expect("read");
        let stream_schema = stream.schema();
        let geom_metadata = stream_schema
            .field_with_name("geom")
            .expect("geom")
            .metadata();
        assert_eq!(
            geom_metadata
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_WKB_EXTENSION_NAME)
        );
        assert_eq!(
            geom_metadata
                .get(protocol::GEOMETRY_CRS_RESOLUTION)
                .map(String::as_str),
            Some("resolved")
        );
        assert_eq!(
            geom_metadata
                .get(protocol::GEOMETRY_CRS_ID)
                .map(String::as_str),
            Some("EPSG:4326")
        );
        assert_eq!(
            geom_metadata
                .get(protocol::GEOMETRY_AXIS_ORDER)
                .map(String::as_str),
            Some("unknown")
        );
        let mut rows = 0;
        let mut batches = 0;
        while let Some(batch) = stream.next_batch().await.expect("batch") {
            rows += batch.num_rows();
            batches += 1;
        }
        assert_eq!(rows, 10_000);
        assert_eq!(batches, 13);

        let filtered = ReadOperation {
            source: operation.source,
            projection: vec!["event_id".to_owned(), "geom".to_owned()],
            order_by: vec![OrderBy {
                field: "event_id".to_owned(),
                direction: SortDirection::Asc,
            }],
            row_limit: Some(10),
            filter: Some(FilterExpression::Eq {
                field: "region_id".to_owned(),
                parameter: "region_id".to_owned(),
            }),
        };
        let mut values = BTreeMap::new();
        values.insert("region_id".to_owned(), ParameterValue::I32(11));
        let parameters = ParameterBag::new(values);
        let mut filtered_stream = provider
            .read_with_test_budget(&secret, &filtered, &parameters, &cancellation)
            .await
            .expect("filtered read");
        let filtered_batch = filtered_stream
            .next_batch()
            .await
            .expect("filtered batch")
            .expect("rows");
        assert_eq!(filtered_batch.num_rows(), 10);
        assert!(filtered_stream
            .next_batch()
            .await
            .expect("filtered end")
            .is_none());

        let client = PostgresProvider::connect(&secret).await.expect("client");
        let probe_wkb: Vec<u8> = client
            .query_one(
                "SELECT ST_AsEWKB(geom) FROM plenora_fixture.events WHERE event_id = 1",
                &[],
            )
            .await
            .expect("spatial probe")
            .get(0);
        let spatial_read = ReadOperation {
            source: filtered.source.clone(),
            projection: vec!["event_id".to_owned(), "geom".to_owned()],
            order_by: vec![OrderBy {
                field: "event_id".to_owned(),
                direction: SortDirection::Asc,
            }],
            row_limit: Some(3),
            filter: Some(FilterExpression::Spatial {
                function: SpatialFunction::Intersects,
                field: "geom".to_owned(),
                geometry_parameter: Some("probe".to_owned()),
                distance_parameter: None,
            }),
        };
        let mut spatial_values = BTreeMap::new();
        spatial_values.insert(
            "probe".to_owned(),
            ParameterValue::Wkb {
                bytes: probe_wkb.clone(),
                srid: Some(4326),
                dimensions: Dimensions::Xyz,
                semantics: SpatialSemantics::Geometry,
            },
        );
        let mut spatial_stream = provider
            .read_with_test_budget(
                &secret,
                &spatial_read,
                &ParameterBag::new(spatial_values),
                &cancellation,
            )
            .await
            .expect("spatial read");
        assert_eq!(
            spatial_stream
                .next_batch()
                .await
                .expect("spatial batch")
                .expect("spatial rows")
                .num_rows(),
            3
        );

        let indexed_query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: spatial_read.source.clone(),
                alias: Some("e".to_owned()),
            }),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("e".to_owned()),
                            field: "event_id".to_owned(),
                        },
                    },
                    alias: Some("event_id".to_owned()),
                },
                QueryProjection {
                    expression: QueryExpression::Spatial {
                        function: SpatialFunction::X,
                        arguments: vec![QueryExpression::Column {
                            column: ColumnRef {
                                relation: Some("e".to_owned()),
                                field: "geom".to_owned(),
                            },
                        }],
                    },
                    alias: Some("x".to_owned()),
                },
                QueryProjection {
                    expression: QueryExpression::Spatial {
                        function: SpatialFunction::AsGeoJson,
                        arguments: vec![QueryExpression::Column {
                            column: ColumnRef {
                                relation: Some("e".to_owned()),
                                field: "geom".to_owned(),
                            },
                        }],
                    },
                    alias: Some("geojson".to_owned()),
                },
            ],
            joins: Vec::new(),
            filter: Some(QueryExpression::SpatialOperator {
                operator: SpatialOperator::BoundingBoxIntersects,
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("e".to_owned()),
                        field: "geom".to_owned(),
                    },
                }),
                right: Box::new(QueryExpression::Parameter {
                    name: "probe".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: QueryExpression::SpatialOperator {
                    operator: SpatialOperator::KnnDistance,
                    left: Box::new(QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("e".to_owned()),
                            field: "geom".to_owned(),
                        },
                    }),
                    right: Box::new(QueryExpression::Parameter {
                        name: "probe".to_owned(),
                    }),
                },
                direction: SortDirection::Asc,
            }],
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(3),
            row_offset: None,
            locking: None,
        };
        let indexed_parameters = ParameterBag::new(BTreeMap::from([(
            "probe".to_owned(),
            ParameterValue::Wkb {
                bytes: probe_wkb.clone(),
                srid: Some(4326),
                dimensions: Dimensions::Xyz,
                semantics: SpatialSemantics::Geometry,
            },
        )]));
        let mut indexed_stream = provider
            .query_with_test_budget(&secret, &indexed_query, &indexed_parameters, &cancellation)
            .await
            .expect("indexed spatial query");
        let indexed_batch = indexed_stream
            .next_batch()
            .await
            .expect("indexed spatial batch")
            .expect("indexed spatial rows");
        assert_eq!(indexed_batch.num_rows(), 3);
        assert_eq!(
            indexed_stream
                .schema()
                .field_with_name("x")
                .expect("x accessor")
                .data_type(),
            &DataType::Float64
        );
        let explain: serde_json::Value = client
            .query_one(
                r"
                EXPLAIN (FORMAT JSON)
                SELECT event_id
                FROM plenora_fixture.events
                WHERE geom && ST_Expand(ST_GeomFromEWKB($1), 0.01)
                ORDER BY geom <-> ST_GeomFromEWKB($1)
                LIMIT 3
                ",
                &[&probe_wkb],
            )
            .await
            .expect("spatial index explain")
            .get(0);
        let explain_text = explain.to_string();
        assert!(explain_text.contains("events_geom_gix"), "{explain_text}");

        let query_operation = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: spatial_read.source.clone(),
                alias: Some("e".to_owned()),
            }),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("e".to_owned()),
                            field: "event_id".to_owned(),
                        },
                    },
                    alias: Some("event_id".to_owned()),
                },
                QueryProjection {
                    expression: QueryExpression::Spatial {
                        function: SpatialFunction::Centroid,
                        arguments: vec![QueryExpression::Column {
                            column: ColumnRef {
                                relation: Some("e".to_owned()),
                                field: "geom".to_owned(),
                            },
                        }],
                    },
                    alias: Some("center".to_owned()),
                },
            ],
            joins: Vec::new(),
            filter: Some(QueryExpression::Compare {
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("e".to_owned()),
                        field: "event_id".to_owned(),
                    },
                }),
                operator: ComparisonOperator::Gt,
                right: Box::new(QueryExpression::Parameter {
                    name: "minimum_id".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("e".to_owned()),
                        field: "event_id".to_owned(),
                    },
                },
                direction: SortDirection::Asc,
            }],
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(2),
            row_offset: None,
            locking: None,
        };
        let mut query_parameters = BTreeMap::new();
        query_parameters.insert("minimum_id".to_owned(), ParameterValue::I64(100));
        let mut query_stream = provider
            .query_with_test_budget(
                &secret,
                &query_operation,
                &ParameterBag::new(query_parameters),
                &cancellation,
            )
            .await
            .expect("query AST");
        assert_eq!(
            query_stream
                .schema()
                .field_with_name("center")
                .expect("spatial projection")
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_WKB_EXTENSION_NAME)
        );
        assert_eq!(
            query_stream
                .next_batch()
                .await
                .expect("query batch")
                .expect("query rows")
                .num_rows(),
            2
        );
        drop(query_stream);
        let mut cached_query_stream = provider
            .query_with_test_budget(
                &secret,
                &query_operation,
                &ParameterBag::new(BTreeMap::from([(
                    "minimum_id".to_owned(),
                    ParameterValue::I64(100),
                )])),
                &cancellation,
            )
            .await
            .expect("query AST cached plan");
        assert_eq!(
            cached_query_stream
                .next_batch()
                .await
                .expect("cached query batch")
                .expect("cached query rows")
                .num_rows(),
            2
        );
        drop(cached_query_stream);
        let mut empty_query_stream = provider
            .query_with_test_budget(
                &secret,
                &query_operation,
                &ParameterBag::new(BTreeMap::from([(
                    "minimum_id".to_owned(),
                    ParameterValue::I64(i64::MAX),
                )])),
                &cancellation,
            )
            .await
            .expect("query AST empty result");
        assert_eq!(
            empty_query_stream
                .schema()
                .field_with_name("center")
                .expect("empty spatial projection")
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_WKB_EXTENSION_NAME)
        );
        assert!(empty_query_stream
            .next_batch()
            .await
            .expect("empty query batch")
            .is_none());
        drop(empty_query_stream);

        let window_query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: spatial_read.source.clone(),
                alias: Some("e".to_owned()),
            }),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("e".to_owned()),
                            field: "event_id".to_owned(),
                        },
                    },
                    alias: Some("event_id".to_owned()),
                },
                QueryProjection {
                    expression: QueryExpression::Window {
                        function: ScalarFunction::RowNumber,
                        arguments: Vec::new(),
                        partition_by: vec![QueryExpression::Column {
                            column: ColumnRef {
                                relation: Some("e".to_owned()),
                                field: "region_id".to_owned(),
                            },
                        }],
                        order_by: vec![QueryOrdering {
                            expression: QueryExpression::Column {
                                column: ColumnRef {
                                    relation: Some("e".to_owned()),
                                    field: "event_id".to_owned(),
                                },
                            },
                            direction: SortDirection::Asc,
                        }],
                        frame: None,
                    },
                    alias: Some("ordinal".to_owned()),
                },
            ],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("e".to_owned()),
                        field: "event_id".to_owned(),
                    },
                },
                direction: SortDirection::Asc,
            }],
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(3),
            row_offset: Some(2),
            locking: None,
        };
        let mut window_stream = provider
            .query_with_test_budget(
                &secret,
                &window_query,
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("window query");
        assert_eq!(
            window_stream
                .next_batch()
                .await
                .expect("window batch")
                .expect("window rows")
                .num_rows(),
            3
        );
        drop(window_stream);

        let lateral_body = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: spatial_read.source.clone(),
                alias: Some("candidate".to_owned()),
            }),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("candidate".to_owned()),
                        field: "event_id".to_owned(),
                    },
                },
                alias: Some("related_id".to_owned()),
            }],
            joins: Vec::new(),
            filter: Some(QueryExpression::Compare {
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("candidate".to_owned()),
                        field: "region_id".to_owned(),
                    },
                }),
                operator: ComparisonOperator::Eq,
                right: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("e".to_owned()),
                        field: "region_id".to_owned(),
                    },
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("candidate".to_owned()),
                        field: "event_id".to_owned(),
                    },
                },
                direction: SortDirection::Desc,
            }],
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(1),
            row_offset: None,
            locking: None,
        };
        let lateral_query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: spatial_read.source.clone(),
                alias: Some("e".to_owned()),
            }),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("e".to_owned()),
                            field: "event_id".to_owned(),
                        },
                    },
                    alias: Some("event_id".to_owned()),
                },
                QueryProjection {
                    expression: QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("latest".to_owned()),
                            field: "related_id".to_owned(),
                        },
                    },
                    alias: Some("related_id".to_owned()),
                },
            ],
            joins: vec![QueryJoin {
                kind: JoinKind::Cross,
                source: None,
                derived_source: Some(QueryDerivedSource {
                    query: Box::new(lateral_body),
                    alias: "latest".to_owned(),
                }),
                lateral: true,
                on: None,
            }],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(2),
            row_offset: None,
            locking: None,
        };
        let mut lateral_stream = provider
            .query_with_test_budget(
                &secret,
                &lateral_query,
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("lateral query");
        assert_eq!(
            lateral_stream
                .next_batch()
                .await
                .expect("lateral batch")
                .expect("lateral rows")
                .num_rows(),
            2
        );
        drop(lateral_stream);

        let mut set_query = query_operation.clone();
        set_query.filter = Some(QueryExpression::Compare {
            left: Box::new(QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("e".to_owned()),
                    field: "event_id".to_owned(),
                },
            }),
            operator: ComparisonOperator::Lt,
            right: Box::new(QueryExpression::Parameter {
                name: "lower_cut".to_owned(),
            }),
        });
        set_query.order_by = Vec::new();
        set_query.row_limit = Some(4);
        let mut set_rhs = query_operation.clone();
        set_rhs.filter = Some(QueryExpression::Compare {
            left: Box::new(QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("e".to_owned()),
                    field: "event_id".to_owned(),
                },
            }),
            operator: ComparisonOperator::Gt,
            right: Box::new(QueryExpression::Parameter {
                name: "upper_cut".to_owned(),
            }),
        });
        set_rhs.row_limit = None;
        set_query.set_operations = vec![QuerySetOperation {
            operator: QuerySetOperator::Union,
            all: true,
            query: Box::new(set_rhs),
        }];
        let mut set_stream = provider
            .query_with_test_budget(
                &secret,
                &set_query,
                &ParameterBag::new(BTreeMap::from([
                    ("lower_cut".to_owned(), ParameterValue::I64(3)),
                    ("upper_cut".to_owned(), ParameterValue::I64(9_998)),
                ])),
                &cancellation,
            )
            .await
            .expect("set operation query");
        assert_eq!(
            set_stream
                .next_batch()
                .await
                .expect("set batch")
                .expect("set rows")
                .num_rows(),
            4
        );
        drop(set_stream);

        let advanced_source = ObjectRef {
            catalog: None,
            schema: Some("plenora_fixture".to_owned()),
            object: "advanced_types".to_owned(),
            layer_id: None,
        };
        let advanced_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: advanced_source.clone(),
                },
                &cancellation,
            )
            .await
            .expect("advanced introspection");
        assert_eq!(advanced_description.document["relation"]["kind"], "table");
        assert!(advanced_description.document["constraints"]
            .as_array()
            .is_some_and(|constraints| !constraints.is_empty()));
        assert!(advanced_description.document["indexes"]
            .as_array()
            .is_some_and(|indexes| !indexes.is_empty()));
        let described_columns = advanced_description.document["columns"]
            .as_array()
            .expect("described columns");
        assert!(described_columns
            .iter()
            .any(|column| { column["name"] == "id" && column["identity_kind"] == "a" }));
        assert!(described_columns
            .iter()
            .any(|column| { column["name"] == "doubled" && column["generated_kind"] == "s" }));
        assert!(described_columns.iter().any(|column| {
            column["name"] == "status"
                && column["enum_labels"]
                    .as_array()
                    .is_some_and(|labels| labels.len() == 3)
        }));
        assert!(described_columns.iter().any(|column| {
            column["name"] == "domain_value"
                && column["domain_base_type"] == "integer"
                && column["domain_constraints"]
                    .as_array()
                    .is_some_and(|constraints| !constraints.is_empty())
        }));

        let dimensions_source = ObjectRef {
            catalog: None,
            schema: Some("plenora_fixture".to_owned()),
            object: "spatial_dimensions".to_owned(),
            layer_id: None,
        };
        let dimensions_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: dimensions_source.clone(),
                },
                &cancellation,
            )
            .await
            .expect("spatial dimension introspection");
        let dimension_columns = dimensions_description.document["columns"]
            .as_array()
            .expect("spatial dimension columns");
        for (name, dimensions, geometry_type) in [
            ("point_xy", "xy", "Point"),
            ("point_z", "xyz", "Point"),
            ("point_m", "xym", "Point"),
            ("point_zm", "xyzm", "Point"),
            ("collection", "xy", "GeometryCollection"),
            ("curve", "xy", "CircularString"),
            ("tin", "xyz", "TIN"),
        ] {
            assert!(dimension_columns.iter().any(|column| {
                column["name"] == name
                    && column["spatial_dimensions"] == dimensions
                    && column["spatial_type"] == geometry_type
            }));
        }
        let mut dimensions_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: dimensions_source,
                    projection: Vec::new(),
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("spatial dimension read");
        assert_eq!(
            dimensions_stream
                .schema()
                .field_with_name("point_m")
                .expect("point M")
                .metadata()
                .get(protocol::GEOMETRY_DIMENSIONS)
                .map(String::as_str),
            Some("xym")
        );
        assert_eq!(
            dimensions_stream
                .schema()
                .field_with_name("geog")
                .expect("geography")
                .metadata()
                .get(protocol::GEOMETRY_SPATIAL_SEMANTICS)
                .map(String::as_str),
            Some("geography")
        );
        assert_eq!(
            dimensions_stream
                .next_batch()
                .await
                .expect("spatial dimension batch")
                .expect("spatial dimension row")
                .num_rows(),
            1
        );

        let secure_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "secure_events".to_owned(),
                        layer_id: None,
                    },
                },
                &cancellation,
            )
            .await
            .expect("secure partitioned introspection");
        assert_eq!(
            secure_description.document["relation"]["kind"],
            "partitioned_table"
        );
        assert_eq!(
            secure_description.document["relation"]["row_security"],
            true
        );
        assert_eq!(
            secure_description.document["relation"]["force_row_security"],
            true
        );
        assert_eq!(
            secure_description.document["relation"]["partitions"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        assert!(secure_description.document["indexes"]
            .as_array()
            .is_some_and(|indexes| indexes.iter().any(|index| index["spatial"] == true)));
        assert!(secure_description.document["policies"]
            .as_array()
            .is_some_and(|policies| policies.iter().any(|policy| {
                policy["name"] == "secure_events_tenant_policy"
                    && policy["command"] == "all"
                    && policy["roles"]
                        .as_array()
                        .is_some_and(|roles| roles.iter().any(|role| role == "PUBLIC"))
            })));
        assert!(secure_description.document["privileges"]
            .as_array()
            .is_some_and(|privileges| privileges.iter().any(|privilege| {
                privilege["grantee"] == "plenora_reader" && privilege["privilege"] == "SELECT"
            })));
        let materialized_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "event_region_summary".to_owned(),
                        layer_id: None,
                    },
                },
                &cancellation,
            )
            .await
            .expect("materialized view introspection");
        assert_eq!(
            materialized_description.document["relation"]["kind"],
            "materialized_view"
        );
        assert_eq!(
            materialized_description.document["relation"]["is_populated"],
            true
        );
        let mut advanced_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: advanced_source,
                    projection: Vec::new(),
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("advanced read");
        assert!(matches!(
            advanced_stream
                .schema()
                .field_with_name("integer_values")
                .expect("array field")
                .data_type(),
            DataType::List(item) if item.data_type() == &DataType::Int32
        ));
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("local_time")
                .expect("time field")
                .data_type(),
            &DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("duration")
                .expect("interval field")
                .data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert!(matches!(
            advanced_stream
                .schema()
                .field_with_name("integer_window")
                .expect("range field")
                .data_type(),
            DataType::Struct(fields)
                if fields.iter().any(|field| field.name() == "lower")
                    && fields.iter().any(|field| field.name() == "empty")
        ));
        assert!(matches!(
            advanced_stream
                .schema()
                .field_with_name("profile")
                .expect("composite field")
                .data_type(),
            DataType::Struct(fields)
                if fields.iter().map(|field| field.name().as_str()).collect::<Vec<_>>()
                    == ["label", "priority", "enabled"]
        ));
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("rounded_amount")
                .expect("negative scale decimal")
                .data_type(),
            if info
                .server_version
                .split('.')
                .next()
                .and_then(|major| major.parse::<u16>().ok())
                .is_some_and(|major| major >= 15)
            {
                &DataType::Decimal128(6, -2)
            } else {
                &DataType::Decimal128(8, 0)
            }
        );
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("integer_values")
                .expect("array field")
                .metadata()
                .get(protocol::POSTGRES_NATIVE_DECLARATION)
                .map(String::as_str),
            Some("integer[]")
        );
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("status")
                .expect("enum field")
                .metadata()
                .get(protocol::POSTGRES_TYPE_KIND)
                .map(String::as_str),
            Some("e")
        );
        assert_eq!(
            advanced_stream
                .next_batch()
                .await
                .expect("advanced batch")
                .expect("advanced row")
                .num_rows(),
            1
        );
        drop(advanced_stream);

        let mut typed_values = BTreeMap::new();
        typed_values.insert(
            "external_id".to_owned(),
            ParameterValue::Uuid("123e4567-e89b-12d3-a456-426614174000".to_owned()),
        );
        typed_values.insert(
            "rounded_amount".to_owned(),
            ParameterValue::Decimal("12300".to_owned()),
        );
        let mut typed_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec!["id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: Some(FilterExpression::And {
                        args: vec![
                            FilterExpression::Eq {
                                field: "external_id".to_owned(),
                                parameter: "external_id".to_owned(),
                            },
                            FilterExpression::Eq {
                                field: "rounded_amount".to_owned(),
                                parameter: "rounded_amount".to_owned(),
                            },
                        ],
                    }),
                },
                &ParameterBag::new(typed_values),
                &cancellation,
            )
            .await
            .expect("typed parameters");
        assert_eq!(
            typed_stream
                .next_batch()
                .await
                .expect("typed parameter batch")
                .expect("typed parameter row")
                .num_rows(),
            1
        );
        drop(typed_stream);
        let mut null_values = BTreeMap::new();
        null_values.insert(
            "duration".to_owned(),
            ParameterValue::Null {
                type_name: "interval".to_owned(),
            },
        );
        let mut null_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec!["id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: Some(FilterExpression::Eq {
                        field: "duration".to_owned(),
                        parameter: "duration".to_owned(),
                    }),
                },
                &ParameterBag::new(null_values),
                &cancellation,
            )
            .await
            .expect("typed null parameter");
        assert!(null_stream
            .next_batch()
            .await
            .expect("typed null batch")
            .is_none());
        drop(null_stream);

        let limited_provider = PostgresProvider::new(10).with_byte_limits(1, 1);
        let mut limited_stream = limited_provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: spatial_read.source.clone(),
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("limited stream");
        let limited_error = limited_stream.next_batch().await.expect_err("byte budget");
        assert_eq!(limited_error.category, ErrorCategory::ResourceLimit);

        let mut cancelled_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: spatial_read.source.clone(),
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("cancel stream");
        let cancelled_error = cancelled_stream
            .next_batch_with_cancellation(&AlwaysCancelled)
            .await
            .expect_err("cancelled stream");
        assert_eq!(cancelled_error.category, ErrorCategory::Cancelled);
        drop(cancelled_stream);

        client
            .batch_execute(
                "CREATE OR REPLACE VIEW plenora_fixture.slow_events AS
                 SELECT event_id
                 FROM plenora_fixture.events
                 CROSS JOIN LATERAL pg_sleep((event_id * 0 + 50)::double precision / 1000)
                 LIMIT 100",
            )
            .await
            .expect("slow view");
        let inflight_cancellation = CancellationToken::new();
        let toggle = inflight_cancellation.clone();
        let observe_then_cancel = async {
            wait_for_active_query(&client, "\"plenora_fixture\".\"slow_events\"").await;
            toggle.cancel();
        };
        let started = std::time::Instant::now();
        let slow_read = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "slow_events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned()],
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        };
        let slow_parameters = ParameterBag::default();
        let read = provider.read_with_test_budget(
            &secret,
            &slow_read,
            &slow_parameters,
            &inflight_cancellation,
        );
        let (read_result, ()) = tokio::join!(read, observe_then_cancel);
        let inflight_error = read_result.err().expect("in-flight cancellation");
        assert_eq!(inflight_error.category, ErrorCategory::Cancelled);
        assert!(started.elapsed() < StdDuration::from_secs(2));
        wait_for_no_active_query(&client, "\"plenora_fixture\".\"slow_events\"").await;

        let single_connection_provider = PostgresProvider::new(10).with_pool_size(1, 25);
        let held_stream = single_connection_provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: spatial_read.source.clone(),
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("held pool stream");
        let pool_error = single_connection_provider
            .test_connection(&secret, &cancellation)
            .await
            .expect_err("pool acquisition timeout");
        assert_eq!(pool_error.category, ErrorCategory::Timeout);
        assert_eq!(
            single_connection_provider.metrics_snapshot().pool_timeouts,
            1
        );
        drop(held_stream);

        let quoted = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "Quoted Table".to_owned(),
                layer_id: None,
            },
            projection: vec![
                "select".to_owned(),
                "spaced column".to_owned(),
                "a\"b".to_owned(),
            ],
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        };
        let mut quoted_stream = provider
            .read_with_test_budget(&secret, &quoted, &ParameterBag::default(), &cancellation)
            .await
            .expect("quoted read");
        assert_eq!(
            quoted_stream
                .next_batch()
                .await
                .expect("quoted batch")
                .expect("quoted row")
                .num_rows(),
            1
        );

        let marker = "must-not-leak-2026";
        let invalid_secret = SecretString::new(format!(
            "host=dataflow-postgres user=dataflow password={marker} dbname=dataflow_test"
        ));
        let error = provider
            .test_connection(&invalid_secret, &cancellation)
            .await
            .expect_err("invalid authentication");
        assert!(!error.to_string().contains(marker));
        let metrics = provider.metrics_snapshot();
        assert!(metrics.pool_checkouts > 0);
        assert!(metrics.pool_new_connections > 0);
        assert!(metrics.pool_reuses > 0);
        assert!(metrics.session_resets > 0);
        assert!(metrics.catalog_introspections > 0);
        assert!(metrics.read_typed_fast_paths > 0);
        assert!(metrics.read_parameterized_typed_fast_paths >= 4);
        assert_eq!(metrics.read_prepared_fallbacks, 0);
        assert!(metrics.query_typed_fast_paths >= 3);
        assert_eq!(metrics.query_prepared_fallbacks, 0);
        assert!(metrics.read_batches > 0);
        assert!(metrics.read_rows > 0);
        assert!(metrics.read_bytes > 0);
        assert!(metrics.cancellations >= 2);
        assert!(metrics.invalidated_sessions >= 2);
    }

    async fn fixture_stream(
        provider: &PostgresProvider,
        secret: &SecretString,
        cancellation: &NeverCancelled,
        projection: Vec<String>,
        row_limit: u64,
    ) -> Box<dyn BatchStream> {
        provider
            .read_with_test_budget(
                secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection,
                    order_by: vec![OrderBy {
                        field: "event_id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    row_limit: Some(row_limit),
                    filter: None,
                },
                &ParameterBag::default(),
                cancellation,
            )
            .await
            .expect("fixture stream")
    }

    async fn fixture_stream_after(
        provider: &PostgresProvider,
        secret: &SecretString,
        cancellation: &NeverCancelled,
        event_id: i64,
        row_limit: u64,
    ) -> Box<dyn BatchStream> {
        let mut values = BTreeMap::new();
        values.insert("event_id".to_owned(), ParameterValue::I64(event_id));
        provider
            .read_with_test_budget(
                secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection: Vec::new(),
                    order_by: vec![OrderBy {
                        field: "event_id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    row_limit: Some(row_limit),
                    filter: Some(FilterExpression::Gt {
                        field: "event_id".to_owned(),
                        parameter: "event_id".to_owned(),
                    }),
                },
                &ParameterBag::new(values),
                cancellation,
            )
            .await
            .expect("filtered fixture stream")
    }

    fn write_operation(mode: WriteMode) -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "write_reference".to_owned(),
                layer_id: None,
            },
            mode,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: vec!["event_id".to_owned()],
            update_columns: if mode == WriteMode::Update {
                vec!["name".to_owned(), "amount".to_owned()]
            } else {
                Vec::new()
            },
            srid_policy: Some(SridPolicy::RequireMatch),
            create_spatial_index: matches!(mode, WriteMode::Create | WriteMode::Replace),
            allow_partial: false,
        }
    }

    async fn execute_fixture_write(
        provider: &PostgresProvider,
        secret: &SecretString,
        cancellation: &NeverCancelled,
        mode: WriteMode,
        stream: Box<dyn BatchStream>,
    ) -> WriteOutcome {
        let operation = write_operation(mode);
        let prepared = provider
            .prepare_write_with_test_budget(secret, &operation, stream.schema(), cancellation)
            .await
            .expect("prepare write");
        provider
            .write_with_prepared_budget(secret, prepared, stream, cancellation)
            .await
            .expect("execute write")
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn live_postgis_write_modes_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(7);
        let secret = SecretString::new(dsn);
        let cancellation = NeverCancelled;
        let client = PostgresProvider::connect(&secret).await.expect("client");
        client
            .batch_execute(
                "DROP TABLE IF EXISTS plenora_fixture.write_reference;
                 DROP TABLE IF EXISTS plenora_fixture.advanced_roundtrip;
                 DROP TABLE IF EXISTS plenora_fixture.advanced_binary_roundtrip;
                 DROP TABLE IF EXISTS plenora_fixture.evolution_target;
                 DROP TABLE IF EXISTS plenora_fixture.slow_write_target",
            )
            .await
            .expect("cleanup");

        let advanced_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec![
                        "status".to_owned(),
                        "domain_value".to_owned(),
                        "rounded_amount".to_owned(),
                        "integer_values".to_owned(),
                        "text_values".to_owned(),
                        "integer_window".to_owned(),
                        "timestamp_window".to_owned(),
                        "duration".to_owned(),
                        "local_time".to_owned(),
                        "profile".to_owned(),
                    ],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("advanced source");
        let advanced_operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "advanced_roundtrip".to_owned(),
                layer_id: None,
            },
            mode: WriteMode::Create,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        };
        let advanced_prepared = provider
            .prepare_write_with_test_budget(
                &secret,
                &advanced_operation,
                advanced_stream.schema(),
                &cancellation,
            )
            .await
            .expect("advanced prepare");
        let advanced_outcome = provider
            .write_with_prepared_budget(&secret, advanced_prepared, advanced_stream, &cancellation)
            .await
            .expect("advanced write");
        assert_eq!(advanced_outcome.rows.confirmed, 1);
        let advanced_matches: bool = client
            .query_one(
                "SELECT
                    target.status = source.status
                    AND target.domain_value = source.domain_value
                    AND target.rounded_amount = source.rounded_amount
                    AND target.integer_values = source.integer_values
                    AND target.text_values = source.text_values
                    AND target.integer_window = source.integer_window
                    AND target.timestamp_window = source.timestamp_window
                    AND target.duration = source.duration
                    AND target.local_time = source.local_time
                    AND target.profile = source.profile
                 FROM plenora_fixture.advanced_roundtrip AS target
                 CROSS JOIN plenora_fixture.advanced_types AS source",
                &[],
            )
            .await
            .expect("advanced roundtrip")
            .get(0);
        assert!(advanced_matches);

        let advanced_binary_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec![
                        "external_id".to_owned(),
                        "rounded_amount".to_owned(),
                        "integer_values".to_owned(),
                        "text_values".to_owned(),
                        "integer_window".to_owned(),
                        "timestamp_window".to_owned(),
                        "duration".to_owned(),
                        "local_time".to_owned(),
                        "profile".to_owned(),
                    ],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("advanced binary source");
        let mut advanced_binary_operation = advanced_operation.clone();
        advanced_binary_operation.target.object = "advanced_binary_roundtrip".to_owned();
        let binary_provider = provider
            .clone()
            .with_insert_mode(PostgresInsertMode::CopyBinary);
        let advanced_binary_prepared = binary_provider
            .prepare_write_with_test_budget(
                &secret,
                &advanced_binary_operation,
                advanced_binary_stream.schema(),
                &cancellation,
            )
            .await
            .expect("advanced binary prepare");
        let advanced_binary_outcome = binary_provider
            .write_with_prepared_budget(
                &secret,
                advanced_binary_prepared,
                advanced_binary_stream,
                &cancellation,
            )
            .await
            .expect("advanced binary write");
        assert_eq!(advanced_binary_outcome.rows.confirmed, 1);
        let advanced_binary_matches: bool = client
            .query_one(
                "SELECT
                    target.external_id = source.external_id
                    AND target.rounded_amount = source.rounded_amount
                    AND target.integer_values = source.integer_values
                    AND target.text_values = source.text_values
                    AND target.integer_window = source.integer_window
                    AND target.timestamp_window = source.timestamp_window
                    AND target.duration = source.duration
                    AND target.local_time = source.local_time
                    AND target.profile = source.profile
                 FROM plenora_fixture.advanced_binary_roundtrip AS target
                 CROSS JOIN plenora_fixture.advanced_types AS source",
                &[],
            )
            .await
            .expect("advanced binary roundtrip")
            .get(0);
        assert!(advanced_binary_matches);

        client
            .batch_execute(
                "CREATE TABLE plenora_fixture.evolution_target (
                    event_id bigint PRIMARY KEY
                 )",
            )
            .await
            .expect("evolution target");
        let evolution_stream = provider
            .read_with_test_budget(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection: vec![
                        "event_id".to_owned(),
                        "name".to_owned(),
                        "region_id".to_owned(),
                    ],
                    order_by: vec![OrderBy {
                        field: "event_id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    row_limit: Some(2),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("evolution source");
        let evolution_operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "evolution_target".to_owned(),
                layer_id: None,
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
        let strict_error = provider
            .prepare_write_with_test_budget(
                &secret,
                &evolution_operation,
                evolution_stream.schema(),
                &cancellation,
            )
            .await
            .err()
            .expect("strict schema evolution");
        assert_eq!(strict_error.category, ErrorCategory::DataMapping);
        let evolution_provider = provider
            .clone()
            .with_schema_evolution(PostgresSchemaEvolution::AddNullableColumns);
        let evolution_prepared = evolution_provider
            .prepare_write_with_test_budget(
                &secret,
                &evolution_operation,
                evolution_stream.schema(),
                &cancellation,
            )
            .await
            .expect("additive evolution prepare");
        assert_eq!(evolution_prepared.loss_report.losses.len(), 2);
        assert!(evolution_prepared
            .loss_report
            .losses
            .iter()
            .all(|loss| loss.severity == LossSeverity::Information));
        let evolution_outcome = evolution_provider
            .write_with_prepared_budget(
                &secret,
                evolution_prepared,
                evolution_stream,
                &cancellation,
            )
            .await
            .expect("additive evolution write");
        assert_eq!(evolution_outcome.rows.confirmed, 2);
        let evolution_state = client
            .query_one(
                "SELECT
                    (SELECT count(*) FROM plenora_fixture.evolution_target),
                    (
                        SELECT count(*)
                        FROM information_schema.columns
                        WHERE table_schema = 'plenora_fixture'
                          AND table_name = 'evolution_target'
                    )",
                &[],
            )
            .await
            .expect("evolution state");
        assert_eq!(evolution_state.get::<_, i64>(0), 2);
        assert_eq!(evolution_state.get::<_, i64>(1), 3);

        client
            .batch_execute(
                "CREATE OR REPLACE FUNCTION plenora_fixture.slow_write()
                 RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                   PERFORM pg_sleep(0.05);
                   RETURN NEW;
                 END
                 $$;
                 CREATE TABLE plenora_fixture.slow_write_target (
                   event_id bigint,
                   name text
                 );
                 CREATE TRIGGER slow_write_row
                 BEFORE INSERT ON plenora_fixture.slow_write_target
                 FOR EACH ROW EXECUTE FUNCTION plenora_fixture.slow_write()",
            )
            .await
            .expect("slow write target");
        let slow_write_stream = fixture_stream(
            &provider,
            &secret,
            &cancellation,
            vec!["event_id".to_owned(), "name".to_owned()],
            100,
        )
        .await;
        let slow_write_operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "slow_write_target".to_owned(),
                layer_id: None,
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
        let slow_write_prepared = provider
            .prepare_write_with_test_budget(
                &secret,
                &slow_write_operation,
                slow_write_stream.schema(),
                &cancellation,
            )
            .await
            .expect("slow write prepare");
        let write_cancellation = CancellationToken::new();
        let write_toggle = write_cancellation.clone();
        let observe_then_cancel = async {
            wait_for_active_query(&client, "COPY \"plenora_fixture\".\"slow_write_target\"").await;
            write_toggle.cancel();
        };
        let started = std::time::Instant::now();
        let write = provider.write_with_prepared_budget(
            &secret,
            slow_write_prepared,
            slow_write_stream,
            &write_cancellation,
        );
        let (write_result, ()) = tokio::join!(write, observe_then_cancel);
        let slow_write_error = write_result.expect_err("in-flight write cancellation");
        assert_eq!(slow_write_error.category, ErrorCategory::Cancelled);
        assert!(started.elapsed() < StdDuration::from_secs(2));
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        let slow_write_rows: i64 = client
            .query_one(
                "SELECT count(*) FROM plenora_fixture.slow_write_target",
                &[],
            )
            .await
            .expect("slow write rollback")
            .get(0);
        assert_eq!(slow_write_rows, 0);

        let deadline_stream = fixture_stream(
            &provider,
            &secret,
            &cancellation,
            vec!["event_id".to_owned(), "name".to_owned()],
            100,
        )
        .await;
        let deadline_budget =
            ResourceBudget::new(plenora_database_core::resource::ResourceLimits {
                duration_ms: 1_000,
                ..plenora_database_core::resource::ResourceLimits::default()
            })
            .expect("write deadline budget");
        let deadline_prepared = Provider::prepare_write(
            &provider,
            &secret,
            &slow_write_operation,
            deadline_stream.schema(),
            &deadline_budget,
            &cancellation,
        )
        .await
        .expect("deadline write prepare");
        let deadline_error = Provider::write(
            &provider,
            &secret,
            deadline_prepared,
            deadline_stream,
            &deadline_budget,
            &cancellation,
        )
        .await
        .expect_err("write deadline");
        assert_eq!(deadline_error.category, ErrorCategory::Timeout);
        assert_eq!(deadline_error.remote_effect, RemoteEffect::RolledBack);
        let deadline_rows: i64 = client
            .query_one(
                "SELECT count(*) FROM plenora_fixture.slow_write_target",
                &[],
            )
            .await
            .expect("deadline write rollback")
            .get(0);
        assert_eq!(deadline_rows, 0);

        client
            .batch_execute(
                "CREATE OR REPLACE FUNCTION plenora_fixture.reject_second_row()
                 RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                   IF NEW.event_id = 2 THEN
                     RAISE EXCEPTION 'intentional write rejection';
                   END IF;
                   RETURN NEW;
                 END
                 $$;
                 DROP TABLE IF EXISTS plenora_fixture.failing_write_target;
                 CREATE TABLE plenora_fixture.failing_write_target (
                   event_id bigint,
                   name text
                 );
                 CREATE TRIGGER reject_second_row
                 BEFORE INSERT ON plenora_fixture.failing_write_target
                 FOR EACH ROW EXECUTE FUNCTION plenora_fixture.reject_second_row()",
            )
            .await
            .expect("failing write target");
        let failing_stream = fixture_stream(
            &provider,
            &secret,
            &cancellation,
            vec!["event_id".to_owned(), "name".to_owned()],
            3,
        )
        .await;
        let failing_operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "failing_write_target".to_owned(),
                layer_id: None,
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
        let failing_prepared = provider
            .prepare_write_with_test_budget(
                &secret,
                &failing_operation,
                failing_stream.schema(),
                &cancellation,
            )
            .await
            .expect("failing write prepare");
        let failing_error = provider
            .write_with_prepared_budget(&secret, failing_prepared, failing_stream, &cancellation)
            .await
            .expect_err("trigger rejection");
        assert_eq!(failing_error.remote_effect, RemoteEffect::RolledBack);
        assert!(failing_error.execution_id.is_some());
        let failing_rows: i64 = client
            .query_one(
                "SELECT count(*) FROM plenora_fixture.failing_write_target",
                &[],
            )
            .await
            .expect("failing write rollback")
            .get(0);
        assert_eq!(failing_rows, 0);

        let created = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Create,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 20).await,
        )
        .await;
        assert_eq!(created.status, WriteStatus::Committed);
        assert_eq!(created.rows.confirmed, 20);

        let appended = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Append,
            fixture_stream_after(&provider, &secret, &cancellation, 20, 5).await,
        )
        .await;
        assert_eq!(appended.rows.confirmed, 5);

        let upserted = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Upsert,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 10).await,
        )
        .await;
        assert_eq!(upserted.rows.confirmed, 10);

        let updated = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Update,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 5).await,
        )
        .await;
        assert_eq!(updated.rows.confirmed, 5);

        let deleted = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::DeleteByKeys,
            fixture_stream(
                &provider,
                &secret,
                &cancellation,
                vec!["event_id".to_owned()],
                5,
            )
            .await,
        )
        .await;
        assert_eq!(deleted.rows.confirmed, 5);

        let truncated = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::TruncateInsert,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 8).await,
        )
        .await;
        assert_eq!(truncated.rows.confirmed, 8);

        let replaced = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Replace,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 12).await,
        )
        .await;
        assert_eq!(replaced.rows.confirmed, 12);
        let row = client
            .query_one(
                r"
                SELECT
                    count(*),
                    (SELECT ST_SRID(geom) FROM plenora_fixture.write_reference LIMIT 1),
                    (
                        SELECT
                            target.amount = source.amount
                            AND target.payload = source.payload
                            AND target.raw_bytes = source.raw_bytes
                            AND target.occurred_at = source.occurred_at
                            AND ST_Equals(target.geom, source.geom)
                            AND ST_Equals(target.geog::geometry, source.geog::geometry)
                        FROM plenora_fixture.write_reference AS target
                        JOIN plenora_fixture.events AS source USING (event_id)
                        WHERE target.event_id = 1
                    )
                FROM plenora_fixture.write_reference
                ",
                &[],
            )
            .await
            .expect("remote state");
        assert_eq!(row.get::<_, i64>(0), 12);
        assert_eq!(row.get::<_, i32>(1), 4326);
        assert!(row.get::<_, bool>(2));

        let mut fault_operation = write_operation(WriteMode::Create);
        fault_operation.target.object = "write_fault_reference".to_owned();
        client
            .batch_execute("DROP TABLE IF EXISTS plenora_fixture.write_fault_reference")
            .await
            .expect("fault cleanup");
        let rollback_provider =
            PostgresProvider::new(7).with_fault_injection(PostgresFaultPoint::BeforeCommit);
        let rollback_stream =
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 2).await;
        let rollback_prepared = rollback_provider
            .prepare_write_with_test_budget(
                &secret,
                &fault_operation,
                rollback_stream.schema(),
                &cancellation,
            )
            .await
            .expect("fault prepare");
        let rollback_error = rollback_provider
            .write_with_prepared_budget(&secret, rollback_prepared, rollback_stream, &cancellation)
            .await
            .expect_err("fault before commit");
        assert_eq!(rollback_error.remote_effect, RemoteEffect::RolledBack);
        assert_eq!(rollback_error.provider, Some(ProviderKind::Postgres));
        assert!(rollback_error.execution_id.is_some());
        let rolled_back: bool = client
            .query_one(
                "SELECT to_regclass('plenora_fixture.write_fault_reference') IS NULL",
                &[],
            )
            .await
            .expect("rollback state")
            .get(0);
        assert!(rolled_back);

        let unknown_provider = PostgresProvider::new(7)
            .with_fault_injection(PostgresFaultPoint::AfterCommitAcknowledgement);
        let unknown_stream = fixture_stream(&provider, &secret, &cancellation, Vec::new(), 2).await;
        let unknown_prepared = unknown_provider
            .prepare_write_with_test_budget(
                &secret,
                &fault_operation,
                unknown_stream.schema(),
                &cancellation,
            )
            .await
            .expect("unknown prepare");
        let unknown = unknown_provider
            .write_with_prepared_budget(&secret, unknown_prepared, unknown_stream, &cancellation)
            .await
            .expect("unknown outcome");
        assert_eq!(unknown.status, WriteStatus::OutcomeUnknown);
        assert!(unknown.recovery.is_some());
        let committed_rows: i64 = client
            .query_one(
                "SELECT count(*) FROM plenora_fixture.write_fault_reference",
                &[],
            )
            .await
            .expect("unknown remote state")
            .get(0);
        assert_eq!(committed_rows, 2);
        let write_metrics = provider.metrics_snapshot();
        assert!(write_metrics.writes_committed > 0);
        assert!(write_metrics.write_rows > 0);
        assert!(write_metrics.schema_cache_invalidations > 0);
        assert_eq!(rollback_provider.metrics_snapshot().invalidated_sessions, 1);
        let unknown_metrics = unknown_provider.metrics_snapshot();
        assert_eq!(unknown_metrics.writes_outcome_unknown, 1);
        assert_eq!(unknown_metrics.invalidated_sessions, 1);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn live_postgres_schema_cache_detects_external_ddl_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let secret = SecretString::new(dsn);
        let setup = PostgresProvider::connect(&secret)
            .await
            .expect("schema cache setup connection");
        setup
            .batch_execute(
                "DROP TABLE IF EXISTS plenora_fixture.schema_cache_probe;
                 CREATE TABLE plenora_fixture.schema_cache_probe (
                    id bigint NOT NULL,
                    label text DEFAULT 'a',
                    geom geometry(Point, 4326)
                 );
                 INSERT INTO plenora_fixture.schema_cache_probe
                 VALUES (1, 'one', ST_SetSRID(ST_MakePoint(1, 2), 4326))",
            )
            .await
            .expect("schema cache fixture");

        let provider = PostgresProvider::new(16)
            .with_pool_size(1, 5_000)
            .with_schema_cache_capacity(1);
        let source = ObjectRef {
            catalog: None,
            schema: Some("plenora_fixture".to_owned()),
            object: "schema_cache_probe".to_owned(),
            layer_id: None,
        };
        let operation = ReadOperation {
            source: source.clone(),
            projection: Vec::new(),
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        };
        for _ in 0..2 {
            let mut stream = provider
                .read_with_test_budget(
                    &secret,
                    &operation,
                    &ParameterBag::default(),
                    &NeverCancelled,
                )
                .await
                .expect("cached schema read");
            assert_eq!(
                stream
                    .next_batch()
                    .await
                    .expect("cached schema batch")
                    .expect("cached schema row")
                    .num_rows(),
                1
            );
            assert!(stream
                .next_batch()
                .await
                .expect("cached schema end")
                .is_none());
        }
        let first_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: source.clone(),
                },
                &NeverCancelled,
            )
            .await
            .expect("first schema token");
        let first_fingerprint = first_description.document["schema_token"]
            ["structural_fingerprint"]
            .as_str()
            .expect("first structural fingerprint")
            .to_owned();
        assert_eq!(first_fingerprint.len(), 64);
        let warm = provider.metrics_snapshot();
        assert_eq!(warm.schema_cache_misses, 1);
        assert_eq!(warm.catalog_introspections, 1);
        assert_eq!(warm.schema_cache_hits, 2);
        assert_eq!(warm.schema_token_checks, 2);
        assert_eq!(provider.schema_cache_entries(), 1);
        let provider_debug = format!("{provider:?}");
        assert!(!provider_debug.contains("schema_cache_probe"));
        assert!(!provider_debug.contains("DEFAULT 'a'"));

        setup
            .batch_execute(
                "ALTER TABLE plenora_fixture.schema_cache_probe
                    ALTER COLUMN label TYPE varchar(64),
                    ALTER COLUMN label SET DEFAULT 'b',
                    ADD COLUMN extra integer",
            )
            .await
            .expect("external schema evolution");

        let mut evolved = provider
            .read_with_test_budget(
                &secret,
                &operation,
                &ParameterBag::default(),
                &NeverCancelled,
            )
            .await
            .expect("read after external DDL");
        assert_eq!(
            evolved
                .schema()
                .field_with_name("label")
                .expect("evolved label")
                .metadata()
                .get(protocol::POSTGRES_NATIVE_TYPE)
                .map(String::as_str),
            Some("varchar")
        );
        assert!(evolved.schema().field_with_name("extra").is_ok());
        while evolved
            .next_batch()
            .await
            .expect("evolved schema batch")
            .is_some()
        {}
        drop(evolved);

        let second_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: source.clone(),
                },
                &NeverCancelled,
            )
            .await
            .expect("second schema token");
        let second_fingerprint = second_description.document["schema_token"]
            ["structural_fingerprint"]
            .as_str()
            .expect("second structural fingerprint");
        assert_ne!(first_fingerprint, second_fingerprint);
        let evolved_metrics = provider.metrics_snapshot();
        assert_eq!(evolved_metrics.schema_cache_misses, 2);
        assert_eq!(evolved_metrics.catalog_introspections, 2);
        assert_eq!(evolved_metrics.schema_cache_invalidations, 1);
        assert_eq!(evolved_metrics.schema_cache_hits, 3);
        assert_eq!(evolved_metrics.schema_token_checks, 4);

        let other = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned()],
            order_by: Vec::new(),
            row_limit: Some(1),
            filter: None,
        };
        let mut other_stream = provider
            .read_with_test_budget(&secret, &other, &ParameterBag::default(), &NeverCancelled)
            .await
            .expect("LRU second object");
        while other_stream
            .next_batch()
            .await
            .expect("LRU second object batch")
            .is_some()
        {}
        drop(other_stream);
        assert_eq!(provider.schema_cache_entries(), 1);
        assert_eq!(provider.metrics_snapshot().schema_cache_evictions, 1);
    }

    #[tokio::test]
    async fn live_postgres_startup_defaults_and_single_reset_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(8)
            .with_timeouts(1_234, 567)
            .with_pool_size(1, 5_000);
        let secret = SecretString::new(dsn);

        let first = provider
            .connect_session(&secret)
            .await
            .expect("fresh configured session");
        let initial = first
            .client()
            .expect("pooled client")
            .query_one(
                "SELECT current_setting('statement_timeout'),
                        current_setting('lock_timeout'),
                        current_setting('application_name')",
                &[],
            )
            .await
            .expect("startup defaults");
        assert_eq!(initial.get::<_, String>(0), "1234ms");
        assert_eq!(initial.get::<_, String>(1), "567ms");
        assert_eq!(initial.get::<_, String>(2), "plenora-database-tools");
        first
            .client()
            .expect("pooled client")
            .batch_execute(
                "SET statement_timeout = 0;
                 SET lock_timeout = 0;
                 SET application_name = 'contaminated';
                 CREATE TEMP TABLE plenora_pool_contamination(value integer);
                 PREPARE plenora_pool_statement AS SELECT 1",
            )
            .await
            .expect("contaminate checked-out session");
        drop(first);

        let after_fresh = provider.metrics_snapshot();
        assert_eq!(after_fresh.pool_new_connections, 1);
        assert_eq!(after_fresh.session_resets, 0);

        let reused = provider
            .connect_session(&secret)
            .await
            .expect("strictly reset reused session");
        let restored = reused
            .client()
            .expect("pooled client")
            .query_one(
                "SELECT current_setting('statement_timeout'),
                        current_setting('lock_timeout'),
                        current_setting('application_name'),
                        to_regclass('pg_temp.plenora_pool_contamination')::text,
                        (SELECT count(*) FROM pg_prepared_statements
                         WHERE name = 'plenora_pool_statement')",
                &[],
            )
            .await
            .expect("restored session defaults");
        assert_eq!(restored.get::<_, String>(0), "1234ms");
        assert_eq!(restored.get::<_, String>(1), "567ms");
        assert_eq!(restored.get::<_, String>(2), "plenora-database-tools");
        assert_eq!(restored.get::<_, Option<String>>(3), None);
        assert_eq!(restored.get::<_, i64>(4), 0);
        drop(reused);

        let metrics = provider.metrics_snapshot();
        assert_eq!(metrics.pool_checkouts, 2);
        assert_eq!(metrics.pool_new_connections, 1);
        assert_eq!(metrics.pool_reuses, 1);
        assert_eq!(metrics.session_resets, 1);
        assert_eq!(metrics.invalidated_sessions, 0);
    }

    #[tokio::test]
    async fn live_postgres_concurrent_pool_stress_when_dsn_is_available() {
        const WORKERS: u64 = 12;
        const ROUNDS: u64 = 10;
        const ROWS_PER_READ: u64 = 5;
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };

        let provider = Arc::new(PostgresProvider::new(13).with_pool_size(4, 5_000));
        let secret = Arc::new(SecretString::new(dsn));
        let operation = Arc::new(ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned(), "name".to_owned()],
            order_by: vec![OrderBy {
                field: "event_id".to_owned(),
                direction: SortDirection::Asc,
            }],
            row_limit: Some(ROWS_PER_READ),
            filter: None,
        });

        let mut tasks = Vec::new();
        for _ in 0..WORKERS {
            let provider = Arc::clone(&provider);
            let secret = Arc::clone(&secret);
            let operation = Arc::clone(&operation);
            tasks.push(tokio::spawn(async move {
                let cancellation = NeverCancelled;
                let mut rows = 0_u64;
                for _ in 0..ROUNDS {
                    let mut stream = provider
                        .read_with_test_budget(
                            &secret,
                            &operation,
                            &ParameterBag::default(),
                            &cancellation,
                        )
                        .await
                        .expect("concurrent read");
                    while let Some(batch) = stream.next_batch().await.expect("concurrent batch") {
                        rows += u64::try_from(batch.num_rows()).expect("row count");
                    }
                }
                rows
            }));
        }
        let mut observed_rows = 0_u64;
        for task in tasks {
            observed_rows += task.await.expect("stress worker");
        }
        assert_eq!(observed_rows, WORKERS * ROUNDS * ROWS_PER_READ);

        let metrics = provider.metrics_snapshot();
        assert_eq!(metrics.pool_checkouts, WORKERS * ROUNDS);
        assert!((1..=4).contains(&metrics.pool_new_connections));
        assert_eq!(
            metrics.pool_reuses + metrics.pool_new_connections,
            metrics.pool_checkouts
        );
        assert_eq!(metrics.session_resets, metrics.pool_reuses);
        assert_eq!(
            metrics.schema_cache_hits + metrics.schema_cache_misses,
            metrics.pool_checkouts
        );
        assert_eq!(metrics.catalog_introspections, metrics.schema_cache_misses);
        assert_eq!(metrics.schema_token_checks, metrics.schema_cache_hits);
        assert_eq!(metrics.schema_cache_invalidations, 0);
        assert_eq!(metrics.schema_cache_evictions, 0);
        assert_eq!(metrics.read_typed_fast_paths, metrics.pool_checkouts);
        assert_eq!(metrics.pool_timeouts, 0);
        assert_eq!(metrics.invalidated_sessions, 0);
        assert_eq!(metrics.read_batches, WORKERS * ROUNDS);
        assert_eq!(metrics.read_rows, observed_rows);
        assert!(metrics.read_bytes > 0);
        assert!(provider.pool_idle_connections() <= 4);
    }

    #[tokio::test]
    async fn live_postgres_concurrent_cancellation_recovers_pool() {
        const WORKERS: usize = 4;
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let secret = Arc::new(SecretString::new(dsn));
        let setup = PostgresProvider::connect(&secret)
            .await
            .expect("setup client");
        setup
            .batch_execute(
                "CREATE OR REPLACE VIEW plenora_fixture.hardening_slow_events AS
                 SELECT value::bigint AS event_id
                 FROM generate_series(1, 100) AS value
                 CROSS JOIN LATERAL
                    pg_sleep((value * 0 + 50)::double precision / 1000)",
            )
            .await
            .expect("slow hardening view");

        let provider = Arc::new(PostgresProvider::new(100).with_pool_size(WORKERS, 5_000));
        let operation = Arc::new(ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "hardening_slow_events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned()],
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        });
        let mut tasks = Vec::new();
        let cancellation = CancellationToken::new();
        for _ in 0..WORKERS {
            let provider = Arc::clone(&provider);
            let secret = Arc::clone(&secret);
            let operation = Arc::clone(&operation);
            let cancellation = cancellation.clone();
            tasks.push(tokio::spawn(async move {
                let error = match provider
                    .read_with_test_budget(
                        &secret,
                        &operation,
                        &ParameterBag::default(),
                        &cancellation,
                    )
                    .await
                {
                    Ok(mut stream) => stream
                        .next_batch_with_cancellation(&cancellation)
                        .await
                        .expect_err("cancelled slow stream"),
                    Err(error) => error,
                };
                assert_eq!(error.category, ErrorCategory::Cancelled);
            }));
        }
        tokio::time::timeout(StdDuration::from_secs(2), async {
            while provider.metrics_snapshot().pool_new_connections
                < u64::try_from(WORKERS).expect("workers")
            {
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        })
        .await
        .expect("all cancellation sessions connected");
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        cancellation.cancel();
        for task in tasks {
            task.await.expect("cancellation worker");
        }

        provider
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect("pool recovery");
        wait_for_no_active_query(&setup, "\"plenora_fixture\".\"hardening_slow_events\"").await;

        let metrics = provider.metrics_snapshot();
        assert!(metrics.cancellations >= u64::try_from(WORKERS).expect("workers"));
        assert!(metrics.invalidated_sessions >= u64::try_from(WORKERS).expect("workers"));
        assert!(metrics.pool_new_connections >= u64::try_from(WORKERS).expect("workers"));
        assert!(provider.pool_idle_connections() <= WORKERS);
    }

    #[tokio::test]
    #[ignore = "benchmark live esplicito"]
    async fn live_copy_vs_prepared_benchmark() {
        let dsn = std::env::var("PLENORA_TEST_POSTGRES_DSN").expect("live DSN");
        let secret = SecretString::new(dsn);
        let cancellation = NeverCancelled;
        let reader = PostgresProvider::new(1_000);
        let client = PostgresProvider::connect(&secret).await.expect("client");
        client
            .batch_execute(
                "DROP TABLE IF EXISTS plenora_fixture.bench_copy;
                 DROP TABLE IF EXISTS plenora_fixture.bench_binary;
                 DROP TABLE IF EXISTS plenora_fixture.bench_prepared",
            )
            .await
            .expect("benchmark cleanup");

        let mut copy_operation = write_operation(WriteMode::Create);
        copy_operation.target.object = "bench_copy".to_owned();
        let copy_provider =
            PostgresProvider::new(1_000).with_insert_mode(PostgresInsertMode::CopyText);
        let copy_stream = fixture_stream(&reader, &secret, &cancellation, Vec::new(), 1_000).await;
        let copy_prepared = copy_provider
            .prepare_write_with_test_budget(
                &secret,
                &copy_operation,
                copy_stream.schema(),
                &cancellation,
            )
            .await
            .expect("copy prepare");
        let started = std::time::Instant::now();
        copy_provider
            .write_with_prepared_budget(&secret, copy_prepared, copy_stream, &cancellation)
            .await
            .expect("copy write");
        let copy_micros = started.elapsed().as_micros();

        let mut binary_operation = write_operation(WriteMode::Create);
        binary_operation.target.object = "bench_binary".to_owned();
        let binary_provider =
            PostgresProvider::new(1_000).with_insert_mode(PostgresInsertMode::CopyBinary);
        let binary_stream =
            fixture_stream(&reader, &secret, &cancellation, Vec::new(), 1_000).await;
        let binary_prepared = binary_provider
            .prepare_write_with_test_budget(
                &secret,
                &binary_operation,
                binary_stream.schema(),
                &cancellation,
            )
            .await
            .expect("binary prepare");
        let started = std::time::Instant::now();
        binary_provider
            .write_with_prepared_budget(&secret, binary_prepared, binary_stream, &cancellation)
            .await
            .expect("binary write");
        let binary_micros = started.elapsed().as_micros();

        let mut prepared_operation = write_operation(WriteMode::Create);
        prepared_operation.target.object = "bench_prepared".to_owned();
        let prepared_provider =
            PostgresProvider::new(1_000).with_insert_mode(PostgresInsertMode::Prepared);
        let prepared_stream =
            fixture_stream(&reader, &secret, &cancellation, Vec::new(), 1_000).await;
        let prepared = prepared_provider
            .prepare_write_with_test_budget(
                &secret,
                &prepared_operation,
                prepared_stream.schema(),
                &cancellation,
            )
            .await
            .expect("prepared prepare");
        let started = std::time::Instant::now();
        prepared_provider
            .write_with_prepared_budget(&secret, prepared, prepared_stream, &cancellation)
            .await
            .expect("prepared write");
        let prepared_micros = started.elapsed().as_micros();

        let differences: i64 = client
            .query_one(
                "SELECT count(*) FROM (
                    (SELECT * FROM plenora_fixture.bench_copy
                     EXCEPT SELECT * FROM plenora_fixture.bench_prepared)
                    UNION ALL
                    (SELECT * FROM plenora_fixture.bench_prepared
                     EXCEPT SELECT * FROM plenora_fixture.bench_copy)
                    UNION ALL
                    (SELECT * FROM plenora_fixture.bench_binary
                     EXCEPT SELECT * FROM plenora_fixture.bench_copy)
                    UNION ALL
                    (SELECT * FROM plenora_fixture.bench_copy
                     EXCEPT SELECT * FROM plenora_fixture.bench_binary)
                ) AS differences",
                &[],
            )
            .await
            .expect("differential")
            .get(0);
        assert_eq!(differences, 0);
        println!(
            "{{\"rows\":1000,\"copy_text_micros\":{copy_micros},\"copy_binary_micros\":{binary_micros},\"prepared_micros\":{prepared_micros},\"differences\":0}}"
        );
    }

    #[tokio::test]
    #[ignore = "benchmark spatial live esplicito"]
    async fn live_spatial_index_benchmark() {
        let dsn = std::env::var("PLENORA_TEST_POSTGRES_DSN").expect("live DSN");
        let secret = SecretString::new(dsn);
        let client = PostgresProvider::connect(&secret).await.expect("client");
        let probe_wkb: Vec<u8> = client
            .query_one(
                "SELECT ST_AsEWKB(geom) FROM plenora_fixture.events WHERE event_id = 1",
                &[],
            )
            .await
            .expect("spatial benchmark probe")
            .get(0);
        let statement = client
            .prepare(
                r"
                SELECT event_id
                FROM plenora_fixture.events
                WHERE geom && ST_Expand(ST_GeomFromEWKB($1), $2)
                ORDER BY geom <-> ST_GeomFromEWKB($1)
                LIMIT 100
                ",
            )
            .await
            .expect("spatial benchmark statement");
        for _ in 0..5 {
            assert_eq!(
                client
                    .query(&statement, &[&probe_wkb, &0.2_f64])
                    .await
                    .expect("spatial warmup")
                    .len(),
                100
            );
        }
        let mut samples = Vec::with_capacity(50);
        for _ in 0..50 {
            let started = std::time::Instant::now();
            let rows = client
                .query(&statement, &[&probe_wkb, &0.2_f64])
                .await
                .expect("spatial benchmark sample");
            assert_eq!(rows.len(), 100);
            samples.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        }
        samples.sort_unstable();
        let median = samples[samples.len() / 2];
        let p95 = samples[(samples.len() * 95).div_ceil(100) - 1];
        let explain: serde_json::Value = client
            .query_one(
                r"
                EXPLAIN (FORMAT JSON)
                SELECT event_id
                FROM plenora_fixture.events
                WHERE geom && ST_Expand(ST_GeomFromEWKB($1), 0.2)
                ORDER BY geom <-> ST_GeomFromEWKB($1)
                LIMIT 100
                ",
                &[&probe_wkb],
            )
            .await
            .expect("spatial benchmark explain")
            .get(0);
        println!(
            "{}",
            json!({
                "rows": 100,
                "samples": samples.len(),
                "median_micros": median,
                "p95_micros": p95,
                "index_used": explain.to_string().contains("events_geom_gix")
            })
        );
    }
}
