use crate::{Db2Config, Db2TlsMode};
use plenora_database_core::provider::SecretString;
use plenora_database_core::{ErrorCategory, ErrorPhase};
use std::path::PathBuf;
use std::time::Duration;

#[test]
fn secure_defaults_are_explicit_and_stable() {
    let config = Db2Config::new("db2.example.test", "warehouse", "loader");

    assert_eq!(config.port(), 50_000);
    assert_eq!(config.tls_mode(), Db2TlsMode::Verify);
    assert_eq!(config.connect_timeout(), Duration::from_secs(10));
    assert_eq!(config.operation_timeout(), Duration::from_secs(30));
    assert!(config.private_ca_certificate().is_none());
}

#[test]
fn debug_redacts_the_principal() {
    let rendered = format!(
        "{:?}",
        Db2Config::new("db2.example.test", "warehouse", "private-user")
    );

    assert!(!rendered.contains("private-user"));
    assert!(rendered.contains("[REDACTED]"));
}

#[test]
fn connection_attributes_are_escaped_and_tls_is_required() {
    let config = Db2Config::new("db2.example.test", "warehouse", "user;admin")
        .with_application_name("plenora;tests");
    let connection = config
        .connection_string(&SecretString::new("secret};PWD=leaked"))
        .expect("connection string Db2");

    assert!(connection.contains("UID={user;admin};"));
    assert!(connection.contains("PWD={secret}};PWD=leaked};"));
    assert!(connection.contains("CLIENTAPPLNAME={plenora;tests};"));
    assert!(connection.contains("SECURITY=SSL;"));
}

#[test]
fn plaintext_requires_an_explicit_opt_out() {
    let connection = Db2Config::new("db2.example.test", "warehouse", "loader")
        .with_tls_mode(Db2TlsMode::Disable)
        .connection_string(&SecretString::new("secret"))
        .expect("connection string plaintext esplicita");

    assert!(!connection.contains("SECURITY=SSL"));
    assert!(!connection.contains("SSLSERVERCERTIFICATE"));
}

#[test]
fn invalid_values_fail_before_the_driver_sees_them() {
    for config in [
        Db2Config::new("", "warehouse", "loader"),
        Db2Config::new("db2.example.test\nHOSTNAME=other", "warehouse", "loader"),
        Db2Config::new("db2.example.test", "warehouse", "loader").with_port(0),
        Db2Config::new("db2.example.test", "warehouse", "loader")
            .with_timeouts(Duration::ZERO, Duration::from_secs(1)),
        Db2Config::new("db2.example.test", "warehouse", "loader")
            .with_private_ca_certificate(PathBuf::from("relative-ca.pem")),
    ] {
        let error = config.validate().expect_err("configurazione non valida");
        assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
        assert_eq!(error.phase, ErrorPhase::Validate);
    }
}

#[test]
fn plaintext_and_a_private_ca_are_rejected_as_contradictory() {
    let executable = std::env::current_exe().expect("percorso test assoluto");
    let error = Db2Config::new("db2.example.test", "warehouse", "loader")
        .with_tls_mode(Db2TlsMode::Disable)
        .with_private_ca_certificate(executable)
        .validate()
        .expect_err("plaintext e CA non possono coesistere");

    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
}
