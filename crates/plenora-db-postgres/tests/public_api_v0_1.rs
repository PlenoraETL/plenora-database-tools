use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::Provider;
use plenora_db_postgres::{
    PostgresFaultPoint, PostgresInsertMode, PostgresMetricsSnapshot, PostgresNetworkOptions,
    PostgresPerformanceProfile, PostgresProvider, PostgresSchemaEvolution, PostgresSchemaToken,
    PostgresTlsConfig, PostgresTlsMode,
};

const fn assert_provider<T: Provider>() {}

#[test]
fn provider_and_performance_profiles_are_frozen_at_v0_1() {
    assert_provider::<PostgresProvider>();

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
        Provider::kind(&PostgresProvider::insecure_local()),
        ProviderKind::Postgres
    );
    assert_eq!(
        Provider::kind(&PostgresProvider::for_profile(
            PostgresPerformanceProfile::LowLatency
        )),
        ProviderKind::Postgres
    );
}

#[test]
fn provider_builders_are_frozen_at_v0_1() {
    let network = PostgresNetworkOptions {
        connect_timeout_ms: 10_000,
        tcp_user_timeout_ms: 30_000,
        keepalive_idle_secs: 30,
        keepalive_interval_secs: 10,
        keepalive_retries: 3,
    };
    let provider = PostgresProvider::new(512)
        .with_performance_profile(PostgresPerformanceProfile::BalancedBulk)
        .with_target_batch_bytes(2 * 1024 * 1024)
        .without_target_batch_bytes()
        .with_timeouts(30_000, 5_000)
        .with_fault_injection(PostgresFaultPoint::BeforeCommit)
        .with_insert_mode(PostgresInsertMode::Prepared)
        .with_parameterized_read_fast_path(false)
        .with_byte_limits(16 * 1024 * 1024, 64 * 1024 * 1024)
        .with_tls_mode(PostgresTlsMode::Disabled)
        .with_tls_config(PostgresTlsConfig::webpki())
        .with_network_options(network)
        .with_schema_evolution(PostgresSchemaEvolution::AddNullableColumns)
        .with_pool_size(4, 30_000)
        .with_schema_cache_capacity(128);

    assert_eq!(Provider::kind(&provider), ProviderKind::Postgres);
    assert_eq!(provider.pool_idle_connections(), 0);
    assert_eq!(provider.schema_cache_entries(), 0);
    assert_eq!(
        provider.metrics_snapshot(),
        PostgresMetricsSnapshot::default()
    );
}

#[test]
fn public_configuration_variants_and_tls_constructors_are_frozen_at_v0_1() {
    let fault_points = [
        PostgresFaultPoint::BeforeCommit,
        PostgresFaultPoint::AfterCommitAcknowledgement,
    ];
    let insert_modes = [
        PostgresInsertMode::CopyText,
        PostgresInsertMode::CopyBinary,
        PostgresInsertMode::Prepared,
    ];
    let tls_modes = [PostgresTlsMode::Disabled, PostgresTlsMode::Require];
    let schema_evolution = [
        PostgresSchemaEvolution::Disabled,
        PostgresSchemaEvolution::AddNullableColumns,
    ];
    assert_eq!(fault_points.len(), 2);
    assert_eq!(insert_modes.len(), 3);
    assert_eq!(tls_modes.len(), 2);
    assert_eq!(schema_evolution.len(), 2);

    assert!(PostgresTlsConfig::private_ca_pem(b"").is_err());
    assert!(PostgresTlsConfig::private_ca_with_client_identity_pem(b"", b"", b"").is_err());
    assert!(PostgresTlsConfig::from_pem(true, b"", None, None).is_ok());
}

#[test]
fn public_metrics_and_schema_token_are_frozen_at_v0_1() {
    let metrics = PostgresMetricsSnapshot::default();
    let PostgresMetricsSnapshot {
        pool_checkouts,
        pool_reuses,
        pool_new_connections,
        pool_timeouts,
        invalidated_sessions,
        session_resets,
        catalog_introspections,
        schema_token_checks,
        schema_cache_hits,
        schema_cache_misses,
        schema_cache_evictions,
        schema_cache_invalidations,
        cancellations,
        read_typed_fast_paths,
        read_parameterized_typed_fast_paths,
        read_prepared_fallbacks,
        query_typed_fast_paths,
        query_prepared_fallbacks,
        read_batches,
        read_target_limited_batches,
        read_rows,
        read_bytes,
        writes_committed,
        writes_outcome_unknown,
        write_rows,
    } = metrics;
    assert_eq!(
        pool_checkouts
            + pool_reuses
            + pool_new_connections
            + pool_timeouts
            + invalidated_sessions
            + session_resets
            + catalog_introspections
            + schema_token_checks
            + schema_cache_hits
            + schema_cache_misses
            + schema_cache_evictions
            + schema_cache_invalidations
            + cancellations
            + read_typed_fast_paths
            + read_parameterized_typed_fast_paths
            + read_prepared_fallbacks
            + query_typed_fast_paths
            + query_prepared_fallbacks
            + read_batches
            + read_target_limited_batches
            + read_rows
            + read_bytes
            + writes_committed
            + writes_outcome_unknown
            + write_rows,
        0
    );

    let token = PostgresSchemaToken {
        schema_version: 1,
        database_oid: 1,
        namespace_oid: 2,
        relation_oid: 3,
        structural_fingerprint: "sha256:test".to_owned(),
    };
    assert_eq!(token.schema_version, 1);
    assert_eq!(token.database_oid, 1);
    assert_eq!(token.namespace_oid, 2);
    assert_eq!(token.relation_oid, 3);
    assert_eq!(token.structural_fingerprint, "sha256:test");
}
