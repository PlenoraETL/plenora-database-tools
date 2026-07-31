use mysql_async::{Opts, OptsBuilder, PoolConstraints, PoolOpts, SslOpts};
use plenora_database_core::provider::SecretString;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Policy TLS `MySQL`. Anche l'opt-out richiede cifratura: cambia soltanto la
/// verifica del certificato e deve essere scelto esplicitamente.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MysqlCertificatePolicy {
    #[default]
    Verify,
    TrustServerCertificate,
}

#[derive(Clone)]
pub struct MysqlConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: SecretString,
    certificate_policy: MysqlCertificatePolicy,
    private_ca_certificate: Option<PathBuf>,
    connect_timeout: Duration,
    operation_timeout: Duration,
    acquire_timeout: Duration,
}

impl std::fmt::Debug for MysqlConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &"[REDACTED]")
            .field("password", &"[REDACTED]")
            .field("certificate_policy", &self.certificate_policy)
            .field("private_ca_certificate", &self.private_ca_certificate)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish()
    }
}

impl MysqlConfig {
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: SecretString,
    ) -> Self {
        Self {
            host: host.into(),
            port: 3_306,
            database: database.into(),
            username: username.into(),
            password,
            certificate_policy: MysqlCertificatePolicy::Verify,
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
    pub const fn with_certificate_policy(mut self, policy: MysqlCertificatePolicy) -> Self {
        self.certificate_policy = policy;
        self
    }

    #[must_use]
    pub fn with_private_ca_certificate(mut self, path: impl Into<PathBuf>) -> Self {
        self.private_ca_certificate = Some(path.into());
        self
    }

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
    pub const fn certificate_policy(&self) -> MysqlCertificatePolicy {
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

    /// Valida la configurazione prima di qualsiasi I/O.
    /// Valida limiti, endpoint e configurazione TLS.
    ///
    /// # Errors
    ///
    /// Restituisce un errore fail-closed se la configurazione non e sicura o
    /// non e rappresentabile dal driver.
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("database", self.database.as_str()),
            ("username", self.username.as_str()),
            ("password", self.password.expose()),
        ] {
            if value.is_empty() || value.contains('\0') {
                return Err(invalid_configuration(format!(
                    "configurazione MySQL: {name} vuoto o contenente NUL"
                )));
            }
        }
        if self.port == 0 {
            return Err(invalid_configuration("configurazione MySQL: porta zero"));
        }
        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.acquire_timeout.is_zero()
        {
            return Err(invalid_configuration("configurazione MySQL: timeout nullo"));
        }
        if let Some(path) = &self.private_ca_certificate {
            if self.certificate_policy == MysqlCertificatePolicy::TrustServerCertificate {
                return Err(invalid_configuration(
                    "configurazione MySQL: CA privata incompatibile con TrustServerCertificate",
                ));
            }
            let extension = path
                .extension()
                .and_then(|value| value.to_str())
                .map(str::to_ascii_lowercase);
            if !matches!(extension.as_deref(), Some("pem" | "crt" | "der")) {
                return Err(invalid_configuration(
                    "configurazione MySQL: estensione CA privata non supportata",
                ));
            }
            let metadata = std::fs::File::open(path)
                .and_then(|file| file.metadata())
                .map_err(|_| {
                    invalid_configuration(
                        "configurazione MySQL: file CA privata assente o non leggibile",
                    )
                })?;
            if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 1_048_576 {
                return Err(invalid_configuration(
                    "configurazione MySQL: file CA privata vuoto o oltre 1 MiB",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn driver_opts(&self) -> Result<Opts> {
        self.driver_opts_with_pool(None)
    }

    pub(crate) fn driver_opts_with_pool(&self, max_connections: Option<usize>) -> Result<Opts> {
        self.validate()?;
        let mut ssl = SslOpts::default();
        if let Some(path) = &self.private_ca_certificate {
            ssl = ssl.with_root_certs(vec![path.clone().into()]);
        }
        if self.certificate_policy == MysqlCertificatePolicy::TrustServerCertificate {
            ssl = ssl
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }
        let mut builder = OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .db_name(Some(self.database.clone()))
            .user(Some(self.username.clone()))
            .pass(Some(self.password.expose().to_owned()))
            .prefer_socket(Some(false))
            .secure_auth(true)
            .tcp_nodelay(true)
            .stmt_cache_size(Some(128))
            .ssl_opts(Some(ssl))
            .init(vec![crate::SESSION_BOOTSTRAP_SQL]);
        if let Some(max_connections) = max_connections {
            let constraints = PoolConstraints::new(0, max_connections).ok_or_else(|| {
                invalid_configuration("configurazione MySQL: vincoli pool non validi")
            })?;
            builder = builder.pool_opts(Some(
                PoolOpts::default()
                    .with_constraints(constraints)
                    .with_reset_connection(true),
            ));
        }
        Ok(builder.into())
    }
}

fn invalid_configuration(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidConfiguration,
        phase: ErrorPhase::Validate,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
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
            .driver_opts()
            .expect("driver opts");
        let ssl = opts.ssl_opts().expect("TLS must remain required");
        assert!(ssl.accept_invalid_certs());
        assert!(ssl.skip_domain_validation());
    }
}
