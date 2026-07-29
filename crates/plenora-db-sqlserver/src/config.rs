use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result};
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
}
