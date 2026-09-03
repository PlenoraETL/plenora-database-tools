use oracle_rs::{Config, TlsConfig};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Wrapper che impedisce alla `Debug` del driver di esporre la password.
pub struct DriverConfig(Config);

impl std::fmt::Debug for DriverConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DriverConfig([REDACTED])")
    }
}

impl DriverConfig {
    pub fn into_inner(self) -> Config {
        self.0
    }
}

/// Politica di trasporto Oracle. La modalità sicura è il default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OracleTlsMode {
    /// Richiede TCPS e verifica il certificato server.
    #[default]
    Verify,
    /// Usa TCP in chiaro; deve essere richiesto esplicitamente.
    Disable,
}

/// Configurazione strutturata Oracle. La password non viene conservata qui.
#[derive(Clone)]
pub struct OracleConfig {
    host: String,
    port: u16,
    service_name: String,
    username: String,
    tls_mode: OracleTlsMode,
    private_ca_certificate: Option<PathBuf>,
    connect_timeout: Duration,
    operation_timeout: Duration,
    acquire_timeout: Duration,
    statement_cache_size: usize,
}

impl std::fmt::Debug for OracleConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OracleConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("service_name", &self.service_name)
            .field("username", &"[REDACTED]")
            .field("tls_mode", &self.tls_mode)
            .field("private_ca_certificate", &self.private_ca_certificate)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("statement_cache_size", &self.statement_cache_size)
            .finish()
    }
}

impl OracleConfig {
    /// Crea una configurazione TCPS sulla porta Oracle predefinita.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        service_name: impl Into<String>,
        username: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 1521,
            service_name: service_name.into(),
            username: username.into(),
            tls_mode: OracleTlsMode::Verify,
            private_ca_certificate: None,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(10),
            statement_cache_size: 20,
        }
    }

    #[must_use]
    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    #[must_use]
    pub const fn with_tls_mode(mut self, mode: OracleTlsMode) -> Self {
        self.tls_mode = mode;
        self
    }

    #[must_use]
    pub fn with_private_ca_certificate(mut self, path: impl Into<PathBuf>) -> Self {
        self.private_ca_certificate = Some(path.into());
        self
    }

    #[must_use]
    pub const fn with_timeouts(mut self, connect: Duration, operation: Duration) -> Self {
        self.connect_timeout = connect;
        self.operation_timeout = operation;
        self
    }

    /// Imposta il timeout distinto per l'acquisizione di un lease dal pool.
    #[must_use]
    pub const fn with_acquire_timeout(mut self, acquire: Duration) -> Self {
        self.acquire_timeout = acquire;
        self
    }

    #[must_use]
    pub const fn with_statement_cache_size(mut self, size: usize) -> Self {
        self.statement_cache_size = size;
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
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    #[must_use]
    pub const fn tls_mode(&self) -> OracleTlsMode {
        self.tls_mode
    }

    #[must_use]
    pub fn private_ca_certificate(&self) -> Option<&Path> {
        self.private_ca_certificate.as_deref()
    }

    #[must_use]
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    #[must_use]
    pub const fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// Valida tutto ciò che è verificabile senza aprire una connessione.
    ///
    /// # Errors
    ///
    /// `InvalidConfiguration` per campi vuoti o di controllo, porta e
    /// timeout nulli, cache eccessiva o configurazione TLS incoerente.
    pub fn validate(&self) -> Result<()> {
        for (value, message) in [
            (&self.host, "configurazione Oracle senza host"),
            (
                &self.service_name,
                "configurazione Oracle senza service name",
            ),
            (&self.username, "configurazione Oracle senza username"),
        ] {
            if value.is_empty() || value.contains(['\0', '\r', '\n']) {
                return Err(invalid_configuration(message));
            }
        }
        if self.port == 0 {
            return Err(invalid_configuration(
                "configurazione Oracle con porta zero",
            ));
        }
        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.acquire_timeout.is_zero()
        {
            return Err(invalid_configuration(
                "configurazione Oracle con timeout nullo",
            ));
        }
        if self.statement_cache_size > 10_000 {
            return Err(invalid_configuration(
                "configurazione Oracle con statement cache eccessiva",
            ));
        }
        if self.tls_mode == OracleTlsMode::Disable && self.private_ca_certificate.is_some() {
            return Err(invalid_configuration(
                "configurazione Oracle plaintext incompatibile con CA privata",
            ));
        }
        if let Some(path) = &self.private_ca_certificate {
            if !path.is_absolute() || !path.is_file() {
                return Err(invalid_configuration(
                    "configurazione Oracle con CA privata non leggibile",
                ));
            }
        }
        Ok(())
    }

    /// Costruisce la configurazione effimera consumata dalla connessione.
    ///
    /// # Errors
    ///
    /// Propaga la validazione locale; non espone mai la configurazione raw.
    pub fn driver_config(&self, secret: &SecretString) -> Result<DriverConfig> {
        self.validate()?;
        let mut config = Config::new(
            self.host.clone(),
            self.port,
            self.service_name.clone(),
            self.username.clone(),
            secret.expose(),
        )
        .connect_timeout(self.connect_timeout)
        .with_statement_cache_size(self.statement_cache_size);
        if self.tls_mode == OracleTlsMode::Verify {
            let mut tls = TlsConfig::new().with_server_name(self.host.clone());
            if let Some(path) = &self.private_ca_certificate {
                tls = tls.with_ca_cert(path.to_string_lossy());
            }
            config = config.tls_config(tls);
        }
        Ok(DriverConfig(config))
    }
}

fn invalid_configuration(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidConfiguration,
        ErrorPhase::Validate,
        Some(ProviderKind::Oracle),
        message,
    )
}
