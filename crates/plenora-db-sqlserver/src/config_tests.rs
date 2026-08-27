use super::*;

fn config(password: &str) -> SqlServerConfig {
    SqlServerConfig::new(
        "sql.example.test",
        "warehouse",
        "loader",
        SecretString::new(password),
    )
}

#[test]
fn certificate_verification_is_the_default() {
    assert_eq!(
        config("not-logged").certificate_policy(),
        CertificatePolicy::Verify
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
fn rejects_zero_timeout_before_io() {
    let invalid = config("secret").with_timeouts(
        Duration::ZERO,
        Duration::from_secs(1),
        Duration::from_secs(1),
    );
    assert_eq!(
        invalid.validate().expect_err("zero timeout").category,
        ErrorCategory::InvalidConfiguration
    );
}

#[test]
fn trust_server_certificate_requires_explicit_policy() {
    let unsafe_config =
        config("secret").with_certificate_policy(CertificatePolicy::TrustServerCertificate);
    assert_eq!(
        unsafe_config.certificate_policy(),
        CertificatePolicy::TrustServerCertificate
    );
    unsafe_config.driver_config().expect("valid driver config");
}

#[test]
fn private_ca_requires_a_readable_supported_file() {
    let missing = std::env::temp_dir().join(format!(
        "plenora-sqlserver-missing-ca-{}.pem",
        std::process::id()
    ));
    let error = config("secret")
        .with_private_ca_certificate(&missing)
        .validate()
        .expect_err("missing private CA must fail before network I/O");
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);

    let empty = std::env::temp_dir().join(format!(
        "plenora-sqlserver-empty-ca-{}.pem",
        std::process::id()
    ));
    std::fs::write(&empty, []).expect("write empty CA fixture");
    let error = config("secret")
        .with_private_ca_certificate(&empty)
        .validate()
        .expect_err("empty private CA must fail before network I/O");
    std::fs::remove_file(&empty).expect("remove empty CA fixture");
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);

    let unsupported =
        std::env::temp_dir().join(format!("plenora-sqlserver-ca-{}.txt", std::process::id()));
    std::fs::write(&unsupported, b"not-a-certificate").expect("write unsupported CA fixture");
    let error = config("secret")
        .with_private_ca_certificate(&unsupported)
        .validate()
        .expect_err("unsupported private CA extension");
    std::fs::remove_file(&unsupported).expect("remove unsupported CA fixture");
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
}

#[test]
fn private_ca_cannot_disable_certificate_verification() {
    let fixture = std::env::temp_dir().join(format!(
        "plenora-sqlserver-private-ca-{}.pem",
        std::process::id()
    ));
    std::fs::write(&fixture, b"test-only-ca").expect("write private CA fixture");
    let verified = config("secret").with_private_ca_certificate(&fixture);
    assert_eq!(verified.private_ca_certificate(), Some(fixture.as_path()));
    verified
        .driver_config()
        .expect("private CA verification config");

    let unsafe_config = verified.with_certificate_policy(CertificatePolicy::TrustServerCertificate);
    let error = unsafe_config
        .validate()
        .expect_err("private CA and trust-all must be mutually exclusive");
    std::fs::remove_file(&fixture).expect("remove private CA fixture");
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
}

#[test]
fn owned_private_ca_has_clone_scoped_lifecycle_and_no_public_path() {
    let configured = config("")
        .with_private_ca_certificate_pem(b"test-only-public-ca-material")
        .expect("owned CA staging");
    configured
        .validate_without_password()
        .expect("password deliberately unresolved");
    assert_eq!(configured.private_ca_certificate(), None);
    let path = match configured.private_ca_certificate.as_ref() {
        Some(PrivateCaCertificate::Owned(owned)) => owned.path.clone(),
        _ => panic!("expected owned CA"),
    };
    assert!(path.is_file());
    let expected_temp_root = {
        #[cfg(windows)]
        {
            std::fs::canonicalize(std::env::temp_dir()).expect("canonical temp root")
        }
        #[cfg(not(windows))]
        {
            std::env::temp_dir()
        }
    };
    assert_eq!(
        path.parent(),
        Some(expected_temp_root.as_path()),
        "owned CA must not introduce a replaceable intermediate directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path)
                .expect("owned CA file metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    #[cfg(windows)]
    {
        assert!(
            std::fs::OpenOptions::new().write(true).open(&path).is_err(),
            "owned CA must deny a second writer"
        );
        assert!(
            std::fs::remove_file(&path).is_err(),
            "owned CA must deny delete/replacement while in use"
        );
    }
    let clone = configured.clone();
    drop(configured);
    assert!(path.is_file());
    drop(clone);
    assert!(!path.exists());
}

#[test]
fn non_secret_validation_rejects_nul_without_a_password() {
    let error = SqlServerConfig::new("bad\0host", "warehouse", "loader", SecretString::new(""))
        .validate_without_password()
        .expect_err("NUL host");
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
}
