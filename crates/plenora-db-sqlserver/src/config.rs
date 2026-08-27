use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
#[cfg(unix)]
use std::fs::Permissions;
use std::fs::{File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
#[cfg(windows)]
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

static OWNED_CA_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct OwnedPrivateCaFile {
    path: PathBuf,
    file: Option<File>,
    #[cfg(windows)]
    temp_root: Option<File>,
}

impl OwnedPrivateCaFile {
    fn create(pem: &[u8]) -> Result<Self> {
        if pem.is_empty() || pem.len() > 1_048_576 {
            return Err(invalid_configuration(
                "configurazione SQL Server: CA privata in memoria vuota o oltre 1 MiB",
            ));
        }
        let sequence = OWNED_CA_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| invalid_configuration("configurazione SQL Server: clock non valido"))?
            .as_nanos();
        let temp_root = {
            #[cfg(windows)]
            {
                std::fs::canonicalize(std::env::temp_dir()).map_err(|_| {
                    invalid_configuration(
                        "configurazione SQL Server: temp root CA privata non disponibile",
                    )
                })?
            }
            #[cfg(not(windows))]
            {
                std::env::temp_dir()
            }
        };
        #[cfg(windows)]
        let temp_root_handle = {
            const FILE_SHARE_READ: u32 = 0x0000_0001;
            const FILE_SHARE_WRITE: u32 = 0x0000_0002;
            const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
            const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

            let mut options = OpenOptions::new();
            options
                .read(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
            let handle = options.open(&temp_root).map_err(|_| {
                invalid_configuration(
                    "configurazione SQL Server: temp root CA privata non bloccabile",
                )
            })?;
            if handle
                .metadata()
                .map_err(|_| {
                    invalid_configuration(
                        "configurazione SQL Server: temp root CA privata non verificabile",
                    )
                })?
                .file_attributes()
                & FILE_ATTRIBUTE_REPARSE_POINT
                != 0
            {
                return Err(invalid_configuration(
                    "configurazione SQL Server: temp root CA privata non può essere un reparse point",
                ));
            }
            handle
        };
        let path = temp_root.join(format!(
            "plenora-sqlserver-ca-{}-{timestamp}-{sequence}.pem",
            std::process::id()
        ));
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).read(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            #[cfg(windows)]
            options.share_mode(0x0000_0001);
            let mut file = options.open(&path).map_err(|_| {
                invalid_configuration(
                    "configurazione SQL Server: staging CA privata non disponibile",
                )
            })?;
            #[cfg(unix)]
            std::fs::set_permissions(&path, Permissions::from_mode(0o600)).map_err(|_| {
                invalid_configuration(
                    "configurazione SQL Server: permessi CA privata non applicabili",
                )
            })?;
            file.write_all(pem).map_err(|_| {
                invalid_configuration("configurazione SQL Server: scrittura CA privata fallita")
            })?;
            file.sync_all().map_err(|_| {
                invalid_configuration("configurazione SQL Server: sync CA privata fallita")
            })?;
            Ok(Self {
                path: path.clone(),
                file: Some(file),
                #[cfg(windows)]
                temp_root: Some(temp_root_handle),
            })
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&path);
        }
        result
    }
}

impl Drop for OwnedPrivateCaFile {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
        #[cfg(windows)]
        drop(self.temp_root.take());
    }
}

#[derive(Clone)]
enum PrivateCaCertificate {
    Path(PathBuf),
    Owned(Arc<OwnedPrivateCaFile>),
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
    private_ca_certificate: Option<PrivateCaCertificate>,
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
            .field(
                "private_ca_certificate",
                &self
                    .private_ca_certificate
                    .as_ref()
                    .map(|source| match source {
                        PrivateCaCertificate::Path(path) => path.as_os_str(),
                        PrivateCaCertificate::Owned(_) => std::ffi::OsStr::new("[OWNED]"),
                    }),
            )
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
        self.private_ca_certificate = Some(PrivateCaCertificate::Path(path.into()));
        self
    }

    /// Copia una CA PEM gia validata in uno staging owned dalla configurazione.
    ///
    /// # Errors
    ///
    /// Restituisce un errore se lo staging bounded non può essere creato o sincronizzato.
    pub fn with_private_ca_certificate_pem(mut self, pem: &[u8]) -> Result<Self> {
        self.private_ca_certificate = Some(PrivateCaCertificate::Owned(Arc::new(
            OwnedPrivateCaFile::create(pem)?,
        )));
        Ok(self)
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
        match self.private_ca_certificate.as_ref() {
            Some(PrivateCaCertificate::Path(path)) => Some(path),
            Some(PrivateCaCertificate::Owned(_)) | None => None,
        }
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
        self.validate_without_password()?;
        if self.password.expose().is_empty() || self.password.expose().contains('\0') {
            return Err(invalid_configuration(
                "configurazione SQL Server: password vuoto o contenente NUL",
            ));
        }
        Ok(())
    }

    /// Valida endpoint, limiti e trust material senza leggere o richiedere la password.
    ///
    /// # Errors
    ///
    /// Restituisce un errore fail-closed per campi non secret o trust material non validi.
    pub fn validate_without_password(&self) -> Result<()> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("database", self.database.as_str()),
            ("username", self.username.as_str()),
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
        if let Some(source) = &self.private_ca_certificate {
            if self.certificate_policy == CertificatePolicy::TrustServerCertificate {
                return Err(invalid_configuration(
                    "configurazione SQL Server: CA privata incompatibile con TrustServerCertificate",
                ));
            }
            if let PrivateCaCertificate::Path(path) = source {
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
        } else if let Some(source) = &self.private_ca_certificate {
            let path = match source {
                PrivateCaCertificate::Path(path) => path,
                PrivateCaCertificate::Owned(owned) => &owned.path,
            };
            config.trust_cert_ca(path.to_string_lossy());
        }
        Ok(config)
    }
}

fn invalid_configuration(message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidConfiguration,
        ErrorPhase::Validate,
        Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        message,
    )
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
