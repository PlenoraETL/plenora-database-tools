use crate::{OracleConfig, OracleTlsMode};
use plenora_database_core::provider::SecretString;
use std::time::Duration;

#[test]
fn debug_redacts_the_username_and_never_contains_a_secret() {
    let config = OracleConfig::new("db.internal", "FREEPDB1", "app_user");
    let debug = format!("{config:?}");
    assert!(!debug.contains("app_user"));
    let driver = config
        .driver_config(&SecretString::new("top-secret"))
        .expect("driver config");
    let driver_debug = format!("{driver:?}");
    assert!(!driver_debug.contains("top-secret"));
}

#[test]
fn plaintext_with_a_private_ca_is_rejected_before_the_driver() {
    let config = OracleConfig::new("db.internal", "FREEPDB1", "app")
        .with_tls_mode(OracleTlsMode::Disable)
        .with_private_ca_certificate("relative.pem");
    let error = config.validate().expect_err("configurazione incoerente");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidConfiguration
    );
}

#[test]
fn zero_port_and_control_characters_are_rejected() {
    assert!(OracleConfig::new("host", "service", "user")
        .with_port(0)
        .validate()
        .is_err());
    assert!(OracleConfig::new("host\nsecond", "service", "user")
        .validate()
        .is_err());
    assert!(OracleConfig::new("host", "service", "user")
        .with_acquire_timeout(Duration::ZERO)
        .validate()
        .is_err());
}
