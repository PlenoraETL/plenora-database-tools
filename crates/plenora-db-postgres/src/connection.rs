use crate::error::{classify_error, public_error};
use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::time::Duration;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresTlsMode {
    Disabled,
    Require,
}

#[derive(Clone)]
pub struct PostgresTlsConfig {
    connector: MakeRustlsConnect,
    fingerprint: [u8; 32],
    include_webpki_roots: bool,
    additional_root_count: usize,
    client_identity: bool,
}

impl std::fmt::Debug for PostgresTlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresTlsConfig")
            .field("include_webpki_roots", &self.include_webpki_roots)
            .field("additional_root_count", &self.additional_root_count)
            .field("client_identity", &self.client_identity)
            .field("credential_material", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl Default for PostgresTlsConfig {
    fn default() -> Self {
        Self::webpki()
    }
}

impl PostgresTlsConfig {
    /// Trust store pubblico `WebPKI`, senza certificato client.
    #[must_use]
    pub fn webpki() -> Self {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let mut hasher = Sha256::new();
        hasher.update(b"plenora-postgres-tls-v1");
        hasher.update([1]);
        hash_certificates(&mut hasher, &[]);
        hash_certificates(&mut hasher, &[]);
        Self {
            connector: MakeRustlsConnect::new(client_config),
            fingerprint: hasher.finalize().into(),
            include_webpki_roots: true,
            additional_root_count: 0,
            client_identity: false,
        }
    }

    /// Usa esclusivamente una o più CA private codificate PEM.
    ///
    /// # Errors
    ///
    /// Restituisce errore se il PEM non contiene CA X.509 valide.
    pub fn private_ca_pem(ca_certificates_pem: &[u8]) -> Result<Self> {
        Self::build(false, ca_certificates_pem, None, None)
    }

    /// Usa CA private e autenticazione client mTLS, tutte codificate PEM.
    ///
    /// # Errors
    ///
    /// Restituisce errore per CA, catena client, chiave privata o coppia
    /// certificato/chiave non valide.
    pub fn private_ca_with_client_identity_pem(
        ca_certificates_pem: &[u8],
        client_certificate_chain_pem: &[u8],
        client_private_key_pem: &[u8],
    ) -> Result<Self> {
        Self::build(
            false,
            ca_certificates_pem,
            Some(client_certificate_chain_pem),
            Some(client_private_key_pem),
        )
    }

    /// Costruttore completo per combinare `WebPKI`, CA aggiuntive e `mTLS`.
    ///
    /// # Errors
    ///
    /// Restituisce errore se il trust store è vuoto, un PEM non è valido,
    /// certificato e chiave client non sono entrambi presenti o non combaciano.
    pub fn from_pem(
        include_webpki_roots: bool,
        additional_ca_pem: &[u8],
        client_certificate_chain_pem: Option<&[u8]>,
        client_private_key_pem: Option<&[u8]>,
    ) -> Result<Self> {
        Self::build(
            include_webpki_roots,
            additional_ca_pem,
            client_certificate_chain_pem,
            client_private_key_pem,
        )
    }

    fn build(
        include_webpki_roots: bool,
        additional_ca_pem: &[u8],
        client_certificate_chain_pem: Option<&[u8]>,
        client_private_key_pem: Option<&[u8]>,
    ) -> Result<Self> {
        let additional_roots = parse_certificates(
            additional_ca_pem,
            include_webpki_roots,
            "CA TLS PostgreSQL non valida",
        )?;
        let mut roots = rustls::RootCertStore::empty();
        if include_webpki_roots {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        for certificate in &additional_roots {
            roots
                .add(certificate.clone())
                .map_err(|_| invalid_tls_configuration("CA TLS PostgreSQL non valida"))?;
        }
        if roots.is_empty() {
            return Err(invalid_tls_configuration(
                "TLS PostgreSQL richiede almeno una CA",
            ));
        }

        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        let (client_config, client_certificates, private_key) =
            match (client_certificate_chain_pem, client_private_key_pem) {
                (None, None) => (builder.with_no_client_auth(), Vec::new(), None),
                (Some(certificates), Some(private_key)) => {
                    let certificates = parse_certificates(
                        certificates,
                        false,
                        "catena certificato client PostgreSQL non valida",
                    )?;
                    let private_key = PrivateKeyDer::from_pem_slice(private_key).map_err(|_| {
                        invalid_tls_configuration("chiave privata client PostgreSQL non valida")
                    })?;
                    let client_config = builder
                        .with_client_auth_cert(certificates.clone(), private_key.clone_key())
                        .map_err(|_| {
                            invalid_tls_configuration("identità client TLS PostgreSQL non valida")
                        })?;
                    (client_config, certificates, Some(private_key))
                }
                _ => {
                    return Err(invalid_tls_configuration(
                        "certificato e chiave client PostgreSQL devono essere forniti insieme",
                    ));
                }
            };

        let mut hasher = Sha256::new();
        hasher.update(b"plenora-postgres-tls-v1");
        hasher.update([u8::from(include_webpki_roots)]);
        hash_certificates(&mut hasher, &additional_roots);
        hash_certificates(&mut hasher, &client_certificates);
        if let Some(private_key) = &private_key {
            hash_bytes(&mut hasher, private_key.secret_der());
        }
        Ok(Self {
            connector: MakeRustlsConnect::new(client_config),
            fingerprint: hasher.finalize().into(),
            include_webpki_roots,
            additional_root_count: additional_roots.len(),
            client_identity: private_key.is_some(),
        })
    }

    pub(crate) const fn connector(&self) -> &MakeRustlsConnect {
        &self.connector
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresNetworkOptions {
    pub connect_timeout_ms: u64,
    pub tcp_user_timeout_ms: u64,
    pub keepalive_idle_secs: u64,
    pub keepalive_interval_secs: u64,
    pub keepalive_retries: u32,
}

impl Default for PostgresNetworkOptions {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            tcp_user_timeout_ms: 30_000,
            keepalive_idle_secs: 30,
            keepalive_interval_secs: 10,
            keepalive_retries: 3,
        }
    }
}

pub fn connection_fingerprint(
    secret: &SecretString,
    tls_mode: PostgresTlsMode,
    tls_config: &PostgresTlsConfig,
    network_options: PostgresNetworkOptions,
    statement_timeout_ms: u64,
    lock_timeout_ms: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(secret.expose().as_bytes());
    hasher.update([u8::from(tls_mode == PostgresTlsMode::Require)]);
    if tls_mode == PostgresTlsMode::Require {
        hasher.update(tls_config.fingerprint);
    }
    hasher.update(network_options.connect_timeout_ms.to_le_bytes());
    hasher.update(network_options.tcp_user_timeout_ms.to_le_bytes());
    hasher.update(network_options.keepalive_idle_secs.to_le_bytes());
    hasher.update(network_options.keepalive_interval_secs.to_le_bytes());
    hasher.update(network_options.keepalive_retries.to_le_bytes());
    hasher.update(statement_timeout_ms.to_le_bytes());
    hasher.update(lock_timeout_ms.to_le_bytes());
    hasher.finalize().into()
}

pub fn validate_session_timeouts(statement_timeout_ms: u64, lock_timeout_ms: u64) -> Result<()> {
    if statement_timeout_ms == 0 || lock_timeout_ms == 0 {
        return Err(DatabaseError::invalid_plan(
            "timeout PostgreSQL deve essere maggiore di zero",
        ));
    }
    Ok(())
}

pub fn configure_session_startup(
    config: &mut Config,
    statement_timeout_ms: u64,
    lock_timeout_ms: u64,
) -> Result<()> {
    validate_session_timeouts(statement_timeout_ms, lock_timeout_ms)?;
    let mut options = config.get_options().unwrap_or_default().trim().to_owned();
    if !options.is_empty() {
        options.push(' ');
    }
    write!(
        options,
        "-c statement_timeout={statement_timeout_ms}ms -c lock_timeout={lock_timeout_ms}ms"
    )
    .map_err(|_| {
        public_error(
            ErrorCategory::Internal,
            ErrorPhase::Connect,
            false,
            "impossibile costruire le opzioni di sessione PostgreSQL",
        )
    })?;
    config
        .options(options)
        .application_name("plenora-database-tools");
    Ok(())
}

pub fn connection_config(secret: &SecretString, options: PostgresNetworkOptions) -> Result<Config> {
    if options.connect_timeout_ms == 0
        || options.tcp_user_timeout_ms == 0
        || options.keepalive_idle_secs == 0
        || options.keepalive_interval_secs == 0
        || options.keepalive_retries == 0
    {
        return Err(DatabaseError::invalid_plan(
            "timeout e keepalive PostgreSQL devono essere maggiori di zero",
        ));
    }
    let mut config = secret.expose().parse::<Config>().map_err(|_| {
        public_error(
            ErrorCategory::InvalidConfiguration,
            ErrorPhase::Connect,
            false,
            "configurazione PostgreSQL non valida",
        )
    })?;
    config
        .connect_timeout(Duration::from_millis(options.connect_timeout_ms))
        .tcp_user_timeout(Duration::from_millis(options.tcp_user_timeout_ms))
        .keepalives(true)
        .keepalives_idle(Duration::from_secs(options.keepalive_idle_secs))
        .keepalives_interval(Duration::from_secs(options.keepalive_interval_secs))
        .keepalives_retries(options.keepalive_retries);
    Ok(config)
}

pub fn connection_config_for_mode(
    secret: &SecretString,
    options: PostgresNetworkOptions,
    tls_mode: PostgresTlsMode,
) -> Result<Config> {
    let mut config = connection_config(secret, options)?;
    config.ssl_mode(match tls_mode {
        PostgresTlsMode::Disabled => SslMode::Disable,
        PostgresTlsMode::Require => SslMode::Require,
    });
    Ok(config)
}

#[cfg(test)]
pub async fn connect(secret: &SecretString) -> Result<Client> {
    let config = connection_config(secret, PostgresNetworkOptions::default())?;
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|error| classify_error(ErrorPhase::Connect, &error))?;
    tokio::spawn(async move {
        let _connection_result = connection.await;
    });
    Ok(client)
}

pub async fn connect_with_tls(
    secret: &SecretString,
    tls_mode: PostgresTlsMode,
    tls_config: &PostgresTlsConfig,
    network_options: PostgresNetworkOptions,
    statement_timeout_ms: u64,
    lock_timeout_ms: u64,
) -> Result<Client> {
    if tls_mode == PostgresTlsMode::Disabled {
        let mut config =
            connection_config_for_mode(secret, network_options, PostgresTlsMode::Disabled)?;
        configure_session_startup(&mut config, statement_timeout_ms, lock_timeout_ms)?;
        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|error| classify_error(ErrorPhase::Connect, &error))?;
        tokio::spawn(async move {
            let _connection_result = connection.await;
        });
        return Ok(client);
    }
    let mut config = connection_config_for_mode(secret, network_options, PostgresTlsMode::Require)?;
    configure_session_startup(&mut config, statement_timeout_ms, lock_timeout_ms)?;
    let (client, connection) = config
        .connect(tls_config.connector.clone())
        .await
        .map_err(|error| classify_error(ErrorPhase::Connect, &error))?;
    tokio::spawn(async move {
        let _connection_result = connection.await;
    });
    Ok(client)
}

fn parse_certificates(
    pem: &[u8],
    allow_empty: bool,
    message: &str,
) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid_tls_configuration(message))?;
    if certificates.is_empty() && (!allow_empty || !pem.is_empty()) {
        return Err(invalid_tls_configuration(message));
    }
    Ok(certificates)
}

fn invalid_tls_configuration(message: &str) -> DatabaseError {
    public_error(
        ErrorCategory::InvalidConfiguration,
        ErrorPhase::Connect,
        false,
        message,
    )
}

fn hash_certificates(hasher: &mut Sha256, certificates: &[CertificateDer<'_>]) {
    hasher.update(
        u64::try_from(certificates.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for certificate in certificates {
        hash_bytes(hasher, certificate.as_ref());
    }
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
}
