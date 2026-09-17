use oracle_rs::transport::TlsConfig;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const CERT: &str = include_str!("../../../tests/fixtures/tls/parser-ca.pem");
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct PemDirectory(PathBuf);

impl PemDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "plenora-pem-test-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("create isolated PEM directory");
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self(path)
    }

    fn wallet(&self, contents: &str) -> TlsConfig {
        std::fs::write(self.0.join("ewallet.pem"), contents).expect("write fixture");
        TlsConfig::new().with_wallet(self.0.to_str().expect("UTF-8 path"), None)
    }
}

impl Drop for PemDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_certificate_only_wallet_does_not_require_a_client_key() {
    let directory = PemDirectory::new();
    assert!(directory.wallet(CERT).build_client_config().is_ok());
}

#[test]
fn a_malformed_certificate_after_a_valid_one_is_not_ignored() {
    let directory = PemDirectory::new();
    let contents =
        format!("{CERT}\n-----BEGIN CERTIFICATE-----\n!invalid!\n-----END CERTIFICATE-----\n");
    let error = directory
        .wallet(&contents)
        .build_client_config()
        .expect_err("invalid PEM must fail");
    assert!(!error.to_string().contains("!invalid!"));
}

#[test]
fn a_malformed_wallet_key_cannot_downgrade_to_server_only_tls() {
    let directory = PemDirectory::new();
    let contents =
        format!("{CERT}\n-----BEGIN PRIVATE KEY-----\n!invalid!\n-----END PRIVATE KEY-----\n");
    let error = directory
        .wallet(&contents)
        .build_client_config()
        .expect_err("invalid key must fail");
    assert!(!error.to_string().contains("!invalid!"));
}

#[test]
fn an_encrypted_wallet_key_requires_its_password() {
    let directory = PemDirectory::new();
    let contents = format!(
        "{CERT}\n-----BEGIN ENCRYPTED PRIVATE KEY-----\nAQID\n-----END ENCRYPTED PRIVATE KEY-----\n"
    );
    assert!(directory.wallet(&contents).build_client_config().is_err());
}
