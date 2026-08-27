use super::*;
use crate::CertificatePolicy;

/// Una sonda con o senza i due UDT, e nient'altro di variabile.
fn probe_with(geometry: Option<i32>, geography: Option<i32>) -> crate::catalog::SqlServerProbe {
    crate::catalog::SqlServerProbe {
        product_version: "16.0.4255.1".to_owned(),
        product_level: "RTM".to_owned(),
        edition: "Developer Edition (64-bit)".to_owned(),
        engine_edition: 3,
        hadr_enabled: false,
        database: "dataflow_test".to_owned(),
        compatibility_level: 160,
        collation: "Latin1_General_100_CI_AS_SC".to_owned(),
        read_committed_snapshot: false,
        snapshot_isolation_state: 0,
        geometry_type_id: geometry,
        geography_type_id: geography,
        polybase_installed: false,
    }
}

#[test]
fn the_guaranteed_list_is_the_intersection_of_what_each_semantics_offers() {
    // L'invariante che rende il campo nuovo leggibile: `functions` non e
    // una terza lista scritta a mano accanto alle due, e cio che vale
    // ovunque. La calcola il core da `functions_by_semantics`, e questa
    // prova pretende che il conto torni con cio che le due liste dicono.
    let both = spatial_capabilities(&probe_with(Some(240), Some(241)));
    assert_eq!(both.functions_by_semantics.len(), 2);
    let on_geometry = &both.functions_by_semantics[&SpatialSemantics::Geometry];
    let on_geography = &both.functions_by_semantics[&SpatialSemantics::Geography];
    assert_eq!(
        on_geometry.len(),
        crate::query::VERIFIED_SPATIAL_FUNCTIONS.len()
            + crate::query::GEOMETRY_ONLY_SPATIAL_FUNCTIONS.len()
    );
    assert_eq!(
        on_geography.len(),
        crate::query::VERIFIED_SPATIAL_FUNCTIONS.len()
    );
    // L'intersezione **e** la lista garantita, e nessuna delle sette la
    // raggiunge.
    assert_eq!(
        both.functions,
        crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec()
    );
    for function in crate::query::GEOMETRY_ONLY_SPATIAL_FUNCTIONS {
        assert!(on_geometry.contains(function), "{function:?}");
        assert!(!on_geography.contains(function), "{function:?}");
        assert!(!both.functions.contains(function), "{function:?}");
    }
}

#[test]
fn one_semantics_alone_publishes_only_its_own_list() {
    // Una chiave per una semantica non dichiarata sarebbe una promessa su
    // un tipo che il prodotto dice di non avere.
    let only_geometry = spatial_capabilities(&probe_with(Some(240), None));
    assert_eq!(only_geometry.functions_by_semantics.len(), 1);
    assert!(only_geometry
        .functions_by_semantics
        .contains_key(&SpatialSemantics::Geometry));
    // Con una semantica sola, l'intersezione **e** quella lista: le sette
    // diventano garantite, perche non c'e un secondo tipo su cui possano
    // mancare.
    for function in crate::query::GEOMETRY_ONLY_SPATIAL_FUNCTIONS {
        assert!(only_geometry.functions.contains(function), "{function:?}");
    }
}

#[test]
fn without_the_spatial_types_the_whole_spatial_block_closes() {
    // Nessuna capability derivata puo restare aperta senza gli UDT che la
    // rendono utilizzabile.
    let closed = spatial_capabilities(&probe_with(None, None));
    assert!(!closed.geometry && !closed.geography);
    assert!(!closed.read_wkb, "so leggere WKB di quale geometria?");
    assert!(!closed.write_wkb);
    assert!(!closed.spatial_index);
    assert!(!closed.mixed_geometry_types);
    assert!(closed.dimensions.is_empty());
    assert!(closed.functions.is_empty());
}

#[test]
fn one_spatial_type_is_enough_to_transport_wkb_and_not_enough_to_index() {
    // Le due condizioni sono diverse e vanno tenute diverse: il trasporto
    // WKB appartiene al tipo, quindi uno solo basta; l'indice spaziale il
    // provider lo emette per **entrambe** le semantiche nella stessa DDL,
    // quindi ne pretende due.
    let only_geometry = spatial_capabilities(&probe_with(Some(240), None));
    assert!(only_geometry.read_wkb && only_geometry.write_wkb);
    assert!(only_geometry.geometry && !only_geometry.geography);
    assert!(!only_geometry.spatial_index);
    assert!(!only_geometry.functions.is_empty());

    let both = spatial_capabilities(&probe_with(Some(240), Some(241)));
    assert!(both.spatial_index);
    assert_eq!(
        both.functions,
        crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec()
    );
    assert_eq!(both.dimensions.len(), 4);
}

fn provider() -> SqlServerProvider {
    let config = SqlServerConfig::new(
        "sql.example.test",
        "warehouse",
        "loader",
        SecretString::new("constructor-secret"),
    )
    .with_certificate_policy(CertificatePolicy::TrustServerCertificate);
    SqlServerProvider::new(config, 1_024, 4).expect("provider")
}

#[test]
fn implements_common_provider_contract_type() {
    const fn assert_provider<T: Provider>() {}
    assert_provider::<SqlServerProvider>();
    assert_eq!(provider().kind(), ProviderKind::Sqlserver);
}

#[tokio::test]
async fn pre_cancelled_connection_fails_without_network() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = provider()
        .test_connection(&SecretString::new("runtime-secret"), &cancellation)
        .await
        .expect_err("cancelled");
    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert_eq!(error.phase, ErrorPhase::Connect);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(error.provider, Some(ProviderKind::Sqlserver));
}

#[tokio::test]
async fn pre_cancelled_ddl_fails_without_network_and_without_remote_effect() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = provider()
        .execute_ddl(
            &SecretString::new("runtime-secret"),
            "CREATE TABLE should_not_run (id int)",
            &cancellation,
        )
        .await
        .expect_err("cancelled");
    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert_eq!(error.phase, ErrorPhase::Write);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
}

#[tokio::test]
async fn unsupported_transaction_options_fail_before_pool_checkout() {
    use plenora_database_core::resource::ResourceLimits;
    use plenora_database_core::transaction::{AccessMode, TransactionOptions};

    let options = TransactionOptions {
        access_mode: Some(AccessMode::ReadOnly),
        ..TransactionOptions::default()
    };
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let outcome = provider()
        .begin_transaction(
            &SecretString::new("runtime-secret"),
            &options,
            &budget,
            &CancellationToken::new(),
        )
        .await;
    let Err(error) = outcome else {
        panic!("l'opzione deve fallire senza tentare la rete");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert_eq!(error.remote_effect, RemoteEffect::None);
}

#[test]
fn ddl_failure_after_dispatch_requires_remote_recovery() {
    let error = provider_error(
        ErrorCategory::Execution,
        ErrorPhase::Write,
        "DDL SQL Server rifiutato",
    );
    let classified = ddl_execution_error(error);
    assert_eq!(classified.remote_effect, RemoteEffect::Unknown);
    assert_eq!(classified.retry, RetryDisposition::RequiresRecovery);
    assert_eq!(classified.message, "DDL SQL Server rifiutato");
}

#[test]
fn provider_debug_redacts_constructor_and_runtime_state() {
    let rendered = format!("{:?}", provider());
    assert!(!rendered.contains("constructor-secret"));
    assert!(!rendered.contains("loader"));
    assert!(rendered.contains("[REDACTED]"));
}
