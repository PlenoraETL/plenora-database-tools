use super::*;

fn config(password: &str) -> MysqlConfig {
    MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new(password),
    )
}

#[test]
fn tls_verification_is_the_default() {
    assert_eq!(
        config("secret").certificate_policy(),
        MysqlCertificatePolicy::Verify
    );
}

#[test]
fn debug_redacts_credentials() {
    let rendered = format!("{:?}", config("unique-password-sentinel"));
    assert!(!rendered.contains("unique-password-sentinel"));
    assert!(!rendered.contains("loader"));
    assert!(rendered.contains("[REDACTED]"));
}

#[test]
fn invalid_configuration_fails_before_io() {
    let invalid = config("secret").with_timeouts(
        Duration::ZERO,
        Duration::from_secs(1),
        Duration::from_secs(1),
    );
    assert_eq!(
        invalid.validate().expect_err("zero timeout").category,
        ErrorCategory::InvalidConfiguration
    );
    assert_eq!(
        config("secret")
            .with_port(0)
            .validate()
            .expect_err("zero port")
            .category,
        ErrorCategory::InvalidConfiguration
    );
}

#[test]
fn driver_opts_require_tls_even_for_explicit_trust_opt_out() {
    let opts = config("secret")
        .with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate)
        .driver_opts("MySQL")
        .expect("driver opts");
    let ssl = opts.ssl_opts().expect("TLS must remain required");
    assert!(ssl.accept_invalid_certs());
    assert!(ssl.skip_domain_validation());
}

#[test]
fn pooled_driver_opts_reapply_bootstrap_after_connection_reset() {
    let opts = config("secret")
        .pooled_driver_opts(2, "MySQL")
        .expect("driver opts pooled");
    assert!(opts.pool_opts().reset_connection());
    assert_eq!(opts.setup(), &[crate::SESSION_BOOTSTRAP_SQL]);
}

#[tokio::test]
async fn in_memory_private_ca_reaches_the_driver_without_a_path() {
    let pem = b"test-only-public-ca-material".to_vec();
    let configured = config("secret").with_private_ca_certificate_pem(pem.clone());
    assert_eq!(configured.private_ca_certificate(), None);
    let opts = configured.driver_opts("MySQL").expect("driver opts");
    let roots = opts.ssl_opts().expect("TLS required").root_certs();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].read().await.expect("buffered CA").as_ref(), pem);
}

#[test]
fn non_secret_validation_does_not_require_a_password() {
    config("")
        .validate_without_password()
        .expect("password is deliberately unresolved");
    let error = MysqlConfig::new("bad\0host", "warehouse", "loader", SecretString::new(""))
        .validate_without_password()
        .expect_err("NUL host");
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
}
