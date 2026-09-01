use crate::provider::db2_capabilities;
use crate::{Db2Config, Db2Provider, Db2TlsMode};
use plenora_database_core::capabilities::TransactionScope;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::query::SpatialFunction;
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};

fn provider() -> Db2Provider {
    Db2Provider::new(
        Db2Config::new("db2.example.test", "warehouse", "loader")
            .with_tls_mode(Db2TlsMode::Disable),
    )
    .expect("provider Db2")
}

#[test]
fn implements_the_common_provider_contract() {
    const fn assert_provider<T: Provider>() {}
    assert_provider::<Db2Provider>();
    assert_eq!(provider().kind(), ProviderKind::Db2);
}

#[test]
fn qualified_surfaces_are_open_and_unmeasured_features_stay_fail_closed() {
    let capabilities = db2_capabilities("12.1.5.0".to_owned(), false);

    assert_eq!(capabilities.provider, ProviderKind::Db2);
    assert!(capabilities.reads.streaming);
    assert!(capabilities.reads.pagination);
    assert!(capabilities.reads.projection);
    assert!(capabilities.reads.filter);
    assert!(capabilities.reads.ordering);
    assert!(!capabilities.reads.server_cursor);
    assert!(capabilities.reads.resumable);
    assert!(capabilities.writes.create);
    assert!(capabilities.writes.append);
    assert!(capabilities.writes.update);
    assert!(capabilities.writes.upsert);
    assert!(capabilities.writes.replace);
    assert!(capabilities.writes.delete_by_keys);
    assert!(capabilities.writes.rollback_on_failure);
    assert!(!capabilities.writes.truncate_insert);
    assert!(capabilities.writes.bulk);
    assert!(capabilities.writes.array_binding);
    assert!(!capabilities.writes.returning);
    assert!(capabilities.transactions.single_transaction);
    assert!(capabilities.transactions.savepoints);
    assert!(capabilities.transactions.transactional_ddl);
    assert_eq!(
        capabilities.transactions.scope,
        TransactionScope::Transaction
    );
    assert!(!capabilities.spatial.geometry);
    assert!(capabilities.spatial.functions.is_empty());
    assert!(capabilities.published().is_ok());
}

#[test]
fn spatial_surfaces_open_only_after_the_semantic_probe() {
    let capabilities = db2_capabilities("12.1.5.0".to_owned(), true);

    assert!(capabilities.spatial.read_wkb);
    assert!(capabilities.spatial.write_wkb);
    assert!(capabilities.spatial.geometry);
    assert!(!capabilities.spatial.geography);
    assert!(!capabilities.spatial.spatial_index);
    assert!(capabilities.spatial.mixed_geometry_types);
    assert_eq!(
        capabilities.spatial.dimensions,
        vec![Dimensions::Xy, Dimensions::Xyz]
    );
    assert_eq!(
        capabilities
            .spatial
            .functions_by_semantics
            .get(&SpatialSemantics::Geometry),
        Some(&vec![
            SpatialFunction::Srid,
            SpatialFunction::Dimensions,
            SpatialFunction::Intersects,
            SpatialFunction::Contains,
            SpatialFunction::Within,
        ])
    );
    assert_eq!(
        capabilities.spatial.functions,
        capabilities.spatial.functions_by_semantics[&SpatialSemantics::Geometry]
    );
    assert!(capabilities.spatial.requires_declared_crs);
    assert!(capabilities.published().is_ok());
}

#[tokio::test]
async fn pre_cancelled_connection_never_reaches_odbc() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = provider()
        .test_connection(&SecretString::new("runtime-secret"), &cancellation)
        .await
        .expect_err("connessione cancellata");

    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert_eq!(error.phase, ErrorPhase::Connect);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(error.provider, Some(ProviderKind::Db2));
}
