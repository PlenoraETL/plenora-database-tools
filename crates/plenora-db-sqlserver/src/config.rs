use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tiberius::{AuthMethod, Config, EncryptionLevel};

/// Policy di verifica del certificato server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CertificatePolicy {
    /// Verifica catena e nome host. È il comportamento di produzione.
    #[default]
    Verify,
    /// Accetta qualsiasi certificato. Opt-out esplicito e diagnosticabile.
    TrustServerCertificate,
}

/// Configurazione strutturata, senza connection string da propagare nei log.
#[derive(Clone)]
pub struct SqlServerConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: SecretString,
    application_name: String,
    certificate_policy: CertificatePolicy,
    private_ca_certificate: Option<PathBuf>,
    connect_timeout: Duration,
    operation_timeout: Duration,
    acquire_timeout: Duration,
}

impl std::fmt::Debug for SqlServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlServerConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &"[REDACTED]")
            .field("password", &"[REDACTED]")
            .field("application_name", &self.application_name)
            .field("certificate_policy", &self.certificate_policy)
            .field("private_ca_certificate", &self.private_ca_certificate)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish()
    }
}

impl SqlServerConfig {
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: SecretString,
    ) -> Self {
        Self {
            host: host.into(),
            port: 1_433,
            database: database.into(),
            username: username.into(),
            password,
            application_name: "plenora-database-tools".to_owned(),
            certificate_policy: CertificatePolicy::Verify,
            private_ca_certificate: None,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(10),
        }
    }

    #[must_use]
    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    #[must_use]
    pub fn with_application_name(mut self, application_name: impl Into<String>) -> Self {
        self.application_name = application_name.into();
        self
    }

    #[must_use]
    pub const fn with_certificate_policy(mut self, policy: CertificatePolicy) -> Self {
        self.certificate_policy = policy;
        self
    }

    /// Aggiunge una singola CA privata alla trust store della connessione.
    ///
    /// La verifica della catena e del nome host resta obbligatoria. Il file
    /// deve essere una CA PEM, CRT o DER leggibile; non è compatibile con
    /// [`CertificatePolicy::TrustServerCertificate`].
    #[must_use]
    pub fn with_private_ca_certificate(mut self, path: impl Into<PathBuf>) -> Self {
        self.private_ca_certificate = Some(path.into());
        self
    }

    /// Sostituisce la password senza modificare endpoint o policy.
    ///
    /// È usato dall'adattatore [`crate::SqlServerProvider`] per applicare il
    /// secret risolto a runtime senza conservarlo in piani o log.
    #[must_use]
    pub fn with_password(mut self, password: SecretString) -> Self {
        self.password = password;
        self
    }

    #[must_use]
    pub const fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        operation_timeout: Duration,
        acquire_timeout: Duration,
    ) -> Self {
        self.connect_timeout = connect_timeout;
        self.operation_timeout = operation_timeout;
        self.acquire_timeout = acquire_timeout;
        self
    }

    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    #[must_use]
    pub const fn certificate_policy(&self) -> CertificatePolicy {
        self.certificate_policy
    }

    #[must_use]
    pub fn private_ca_certificate(&self) -> Option<&Path> {
        self.private_ca_certificate.as_deref()
    }

    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    #[must_use]
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    #[must_use]
    pub const fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// Verifica tutti i campi prima di qualsiasi I/O.
    ///
    /// # Errors
    ///
    /// Fallisce per campi vuoti, NUL, porta zero o timeout nulli.
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("database", self.database.as_str()),
            ("username", self.username.as_str()),
            ("password", self.password.expose()),
            ("application_name", self.application_name.as_str()),
        ] {
            if value.is_empty() || value.contains('\0') {
                return Err(invalid_configuration(format!(
                    "configurazione SQL Server: {name} vuoto o contenente NUL"
                )));
            }
        }
        if self.port == 0 {
            return Err(invalid_configuration(
                "configurazione SQL Server: porta zero",
            ));
        }
        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.acquire_timeout.is_zero()
        {
            return Err(invalid_configuration(
                "configurazione SQL Server: timeout nullo",
            ));
        }
        if let Some(path) = &self.private_ca_certificate {
            if self.certificate_policy == CertificatePolicy::TrustServerCertificate {
                return Err(invalid_configuration(
                    "configurazione SQL Server: CA privata incompatibile con TrustServerCertificate",
                ));
            }
            let extension = path
                .extension()
                .and_then(|value| value.to_str())
                .map(str::to_ascii_lowercase);
            if !matches!(extension.as_deref(), Some("pem" | "crt" | "der")) {
                return Err(invalid_configuration(
                    "configurazione SQL Server: estensione CA privata non supportata",
                ));
            }
            let metadata = std::fs::File::open(path)
                .and_then(|file| file.metadata())
                .map_err(|_| {
                    invalid_configuration(
                        "configurazione SQL Server: file CA privata assente o non leggibile",
                    )
                })?;
            if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 1_048_576 {
                return Err(invalid_configuration(
                    "configurazione SQL Server: file CA privata vuoto o oltre 1 MiB",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn driver_config(&self) -> Result<Config> {
        self.validate()?;
        let mut config = Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.application_name(&self.application_name);
        config.authentication(AuthMethod::sql_server(
            &self.username,
            self.password.expose(),
        ));
        config.encryption(EncryptionLevel::Required);
        if self.certificate_policy == CertificatePolicy::TrustServerCertificate {
            config.trust_cert();
        } else if let Some(path) = &self.private_ca_certificate {
            config.trust_cert_ca(path.to_string_lossy());
        }
        Ok(config)
    }
}

fn invalid_configuration(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidConfiguration,
        phase: ErrorPhase::Validate,
        remote_effect: RemoteEffect::None,
        retry: plenora_database_core::RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
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

        let unsafe_config =
            verified.with_certificate_policy(CertificatePolicy::TrustServerCertificate);
        let error = unsafe_config
            .validate()
            .expect_err("private CA and trust-all must be mutually exclusive");
        std::fs::remove_file(&fixture).expect("remove private CA fixture");
        assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
    }
}
