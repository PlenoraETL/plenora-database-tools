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
enum PrivateCaCertificate {
    Path(PathBuf),
    Pem(Vec<u8>),
}

#[derive(Clone)]
pub struct MysqlConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: SecretString,
    certificate_policy: MysqlCertificatePolicy,
    private_ca_certificate: Option<PrivateCaCertificate>,
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
            .field(
                "private_ca_certificate",
                &self
                    .private_ca_certificate
                    .as_ref()
                    .map(|source| match source {
                        PrivateCaCertificate::Path(path) => path.as_os_str(),
                        PrivateCaCertificate::Pem(_) => std::ffi::OsStr::new("[IN-MEMORY]"),
                    }),
            )
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
        self.private_ca_certificate = Some(PrivateCaCertificate::Path(path.into()));
        self
    }

    /// Usa una CA privata PEM gia acquisita e validata dal chiamante.
    #[must_use]
    pub fn with_private_ca_certificate_pem(mut self, pem: Vec<u8>) -> Self {
        self.private_ca_certificate = Some(PrivateCaCertificate::Pem(pem));
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
        match self.private_ca_certificate.as_ref() {
            Some(PrivateCaCertificate::Path(path)) => Some(path),
            Some(PrivateCaCertificate::Pem(_)) | None => None,
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

    /// Valida limiti, endpoint e configurazione TLS prima dell'I/O di rete.
    /// Se è configurata una CA privata, esegue anche un controllo locale bounded
    /// di esistenza, tipo e dimensione del file.
    ///
    /// # Errors
    ///
    /// Restituisce un errore fail-closed se la configurazione non e sicura o
    /// non e rappresentabile dal driver.
    pub fn validate(&self) -> Result<()> {
        self.validate_for_product("MySQL")
    }

    /// La validazione, con il nome del prodotto che comparira nei messaggi.
    ///
    /// La forma pubblica resta legata a `MySQL` perche il tipo si chiama
    /// `MysqlConfig`: chi la chiama sta configurando quello. Il provider
    /// passa invece il nome del proprio profilo, cosi l'errore che il
    /// consumatore legge non contraddice l'attribuzione che porta.
    ///
    /// # Errors
    ///
    /// Come `validate`.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn validate_for_product(&self, product: &str) -> Result<()> {
        self.validate_without_password_for_product(product)?;
        if self.password.expose().is_empty() || self.password.expose().contains('\0') {
            return Err(invalid_configuration(format!(
                "configurazione {product}: password vuoto o contenente NUL"
            )));
        }
        Ok(())
    }

    /// Valida endpoint, limiti e trust material senza leggere o richiedere la password.
    ///
    /// # Errors
    ///
    /// Restituisce un errore fail-closed per campi non secret o trust material non validi.
    pub fn validate_without_password(&self) -> Result<()> {
        self.validate_without_password_for_product("MySQL")
    }

    /// Vedi `validate_for_product`.
    ///
    /// # Errors
    ///
    /// Come `validate_without_password`.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn validate_without_password_for_product(&self, product: &str) -> Result<()> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("database", self.database.as_str()),
            ("username", self.username.as_str()),
        ] {
            if value.is_empty() || value.contains('\0') {
                return Err(invalid_configuration(format!(
                    "configurazione {product}: {name} vuoto o contenente NUL"
                )));
            }
        }
        if self.port == 0 {
            return Err(invalid_configuration(format!(
                "configurazione {product}: porta zero"
            )));
        }
        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.acquire_timeout.is_zero()
        {
            return Err(invalid_configuration(format!(
                "configurazione {product}: timeout nullo"
            )));
        }
        if let Some(source) = &self.private_ca_certificate {
            if self.certificate_policy == MysqlCertificatePolicy::TrustServerCertificate {
                return Err(invalid_configuration(format!(
                    "configurazione {product}: CA privata incompatibile con TrustServerCertificate",
                )));
            }
            match source {
                PrivateCaCertificate::Path(path) => {
                    let extension = path
                        .extension()
                        .and_then(|value| value.to_str())
                        .map(str::to_ascii_lowercase);
                    if !matches!(extension.as_deref(), Some("pem" | "crt" | "der")) {
                        return Err(invalid_configuration(format!(
                            "configurazione {product}: estensione CA privata non supportata",
                        )));
                    }
                    let metadata = std::fs::File::open(path)
                        .and_then(|file| file.metadata())
                        .map_err(|_| {
                            invalid_configuration(format!(
                                "configurazione {product}: file CA privata assente o non leggibile",
                            ))
                        })?;
                    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 1_048_576 {
                        return Err(invalid_configuration(format!(
                            "configurazione {product}: file CA privata vuoto o oltre 1 MiB",
                        )));
                    }
                }
                PrivateCaCertificate::Pem(pem) => {
                    if pem.is_empty() || pem.len() > 1_048_576 {
                        return Err(invalid_configuration(format!(
                            "configurazione {product}: CA privata in memoria vuota o oltre 1 MiB",
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn driver_opts(&self, product: &str) -> Result<Opts> {
        Ok(self.driver_opts_builder(product)?.into())
    }

    pub(crate) fn pooled_driver_opts(&self, max_connections: usize, product: &str) -> Result<Opts> {
        let constraints = PoolConstraints::new(0, max_connections).ok_or_else(|| {
            invalid_configuration(format!("configurazione {product}: vincoli pool non validi"))
        })?;
        let builder = self.driver_opts_builder(product)?.pool_opts(Some(
            PoolOpts::default()
                .with_constraints(constraints)
                .with_reset_connection(true),
        ));
        Ok(builder.into())
    }

    fn driver_opts_builder(&self, product: &str) -> Result<OptsBuilder> {
        // La validazione si ripete qui, ed e la seconda volta che il prodotto
        // deve arrivarci: il provider valida in costruzione, il pool
        // ricostruisce le opzioni al primo checkout. Fra i due momenti la CA
        // puo diventare illeggibile, e l'errore che ne esce e il primo che il
        // consumatore vede davvero.
        self.validate_for_product(product)?;
        let mut ssl = SslOpts::default();
        if let Some(source) = &self.private_ca_certificate {
            let root = match source {
                PrivateCaCertificate::Path(path) => path.clone().into(),
                PrivateCaCertificate::Pem(pem) => pem.clone().into(),
            };
            ssl = ssl.with_root_certs(vec![root]);
        }
        if self.certificate_policy == MysqlCertificatePolicy::TrustServerCertificate {
            ssl = ssl
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }
        let builder = OptsBuilder::default()
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
            .setup(vec![crate::SESSION_BOOTSTRAP_SQL]);
        Ok(builder)
    }
}

fn invalid_configuration(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidConfiguration,
        phase: ErrorPhase::Validate,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(crate::profile::PROVISIONAL_KIND),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
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
}
