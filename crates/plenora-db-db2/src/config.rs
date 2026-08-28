use odbc_api::escape_attribute_value;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_DRIVER: &str = "IBM DB2 ODBC DRIVER";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Db2TlsMode {
    /// Richiede TLS e usa le root del client IBM o la CA privata indicata.
    #[default]
    Verify,
    /// Disabilita TLS. Deve essere selezionato esplicitamente dal chiamante.
    Disable,
}

/// Configurazione strutturata DB2; credenziali e connection string non sono
/// mai incluse nella rappresentazione `Debug`.
#[derive(Clone)]
pub struct Db2Config {
    host: String,
    port: u16,
    database: String,
    username: String,
    driver: String,
    application_name: String,
    tls_mode: Db2TlsMode,
    private_ca_certificate: Option<PathBuf>,
    connect_timeout: Duration,
    operation_timeout: Duration,
}

impl std::fmt::Debug for Db2Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Db2Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &"[REDACTED]")
            .field("driver", &self.driver)
            .field("application_name", &self.application_name)
            .field("tls_mode", &self.tls_mode)
            .field("private_ca_certificate", &self.private_ca_certificate)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}

impl Db2Config {
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 50_000,
            database: database.into(),
            username: username.into(),
            driver: DEFAULT_DRIVER.to_owned(),
            application_name: "plenora-database-tools".to_owned(),
            tls_mode: Db2TlsMode::Verify,
            private_ca_certificate: None,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
        }
    }

    #[must_use]
    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    #[must_use]
    pub fn with_driver(mut self, driver: impl Into<String>) -> Self {
        self.driver = driver.into();
        self
    }

    #[must_use]
    pub fn with_application_name(mut self, application_name: impl Into<String>) -> Self {
        self.application_name = application_name.into();
        self
    }

    #[must_use]
    pub const fn with_tls_mode(mut self, tls_mode: Db2TlsMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    #[must_use]
    pub fn with_private_ca_certificate(mut self, path: impl Into<PathBuf>) -> Self {
        self.private_ca_certificate = Some(path.into());
        self
    }

    #[must_use]
    pub const fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        operation_timeout: Duration,
    ) -> Self {
        self.connect_timeout = connect_timeout;
        self.operation_timeout = operation_timeout;
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
    pub fn username(&self) -> &str {
        &self.username
    }

    #[must_use]
    pub const fn tls_mode(&self) -> Db2TlsMode {
        self.tls_mode
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

    /// Verifica i vincoli locali prima che configurazione o segreti raggiungano ODBC.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidConfiguration` per campi vuoti o di controllo,
    /// timeout e porta non validi, oppure configurazioni TLS incoerenti.
    pub fn validate(&self) -> Result<()> {
        for (value, message) in [
            (&self.host, "configurazione Db2 senza host"),
            (&self.database, "configurazione Db2 senza database"),
            (&self.username, "configurazione Db2 senza username"),
            (&self.driver, "configurazione Db2 senza driver ODBC"),
            (
                &self.application_name,
                "configurazione Db2 senza application name",
            ),
        ] {
            if value.is_empty() || value.contains(['\0', '\r', '\n']) {
                return Err(invalid_configuration(message));
            }
        }
        if self.port == 0 {
            return Err(invalid_configuration("configurazione Db2 con porta zero"));
        }
        if self.connect_timeout.as_secs() == 0 || self.operation_timeout.as_secs() == 0 {
            return Err(invalid_configuration(
                "configurazione Db2 con timeout inferiore a un secondo",
            ));
        }
        if self.connect_timeout.as_secs() > u64::from(u32::MAX) {
            return Err(invalid_configuration(
                "configurazione Db2 con timeout connessione eccessivo",
            ));
        }
        if self.tls_mode == Db2TlsMode::Disable && self.private_ca_certificate.is_some() {
            return Err(invalid_configuration(
                "configurazione Db2 plaintext incompatibile con CA privata",
            ));
        }
        if let Some(path) = &self.private_ca_certificate {
            if !path.is_absolute() || !path.is_file() {
                return Err(invalid_configuration(
                    "configurazione Db2 con CA privata non leggibile",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn connection_string(&self, secret: &SecretString) -> Result<String> {
        self.validate()?;
        let mut connection = format!(
            "DRIVER={};DATABASE={};HOSTNAME={};PORT={};PROTOCOL=TCPIP;UID={};PWD={};\
             CLIENTAPPLNAME={};CONNECTTIMEOUT={};RECEIVETIMEOUT={};",
            escape_attribute_value(&self.driver),
            escape_attribute_value(&self.database),
            escape_attribute_value(&self.host),
            self.port,
            escape_attribute_value(&self.username),
            escape_attribute_value(secret.expose()),
            escape_attribute_value(&self.application_name),
            self.connect_timeout.as_secs(),
            self.operation_timeout.as_secs(),
        );
        if self.tls_mode == Db2TlsMode::Verify {
            connection.push_str("SECURITY=SSL;");
            if let Some(path) = &self.private_ca_certificate {
                connection.push_str("SSLSERVERCERTIFICATE=");
                connection.push_str(&escape_attribute_value(&path.to_string_lossy()));
                connection.push(';');
            }
        }
        Ok(connection)
    }
}

fn invalid_configuration(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidConfiguration,
        ErrorPhase::Validate,
        Some(ProviderKind::Db2),
        message,
    )
}
