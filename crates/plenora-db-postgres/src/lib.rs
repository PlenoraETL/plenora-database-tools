//! Driver pilota PostgreSQL/PostGIS.
//!
//! Il driver di riferimento copre connessione, capability, introspezione, read
//! batch-bounded verso Arrow/GeoArrow-WKB e tutte le modalità di scrittura
//! definite dal core.

mod arrow;
mod catalog;
mod control;
mod error;
mod field_contract;
mod metrics;
mod parameter_codec;
mod read_stream;
mod types;
mod write;

pub use metrics::PostgresMetricsSnapshot;

use arrow_schema::{Field, Schema, SchemaRef};
use catalog::{describe_object_metadata, relation_kind, CatalogSchemaToken};
use control::select_with_cancellation;
use error::{check_cancelled, classify_error, public_error};
use futures_util::StreamExt;
use metrics::PostgresMetrics;
use parameter_codec::{bind_parameters, typed_filter_parameter_types, typed_query_parameter_types};
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::loss::{LossCategory, LossReport, LossSeverity, MappingLoss};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, ProviderKind, ReadOperation, SortDirection,
    WriteOperation,
};
use plenora_database_core::protocol;
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::{QueryExpression, QueryOperation, SpatialFunction};
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    Dialect, DialectCapabilities, Expression, Identifier, ObjectName, Renderer,
};
#[cfg(test)]
use read_stream::batch_memory_bytes;
use read_stream::{
    cancel_and_invalidate, cancelled_read_error, BudgetCancellation, PostgresBatchStream,
    PostgresRows, ReadStreamSource,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration as StdDuration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Config, NoTls, RowStream};
use tokio_postgres_rustls::MakeRustlsConnect;
use types::{ColumnKind, ColumnSpec};

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn contract_schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

#[derive(Debug, Clone)]
pub struct PostgresProvider {
    batch_rows: usize,
    statement_timeout_ms: u64,
    lock_timeout_ms: u64,
    fault_point: Option<PostgresFaultPoint>,
    insert_mode: PostgresInsertMode,
    parameterized_read_fast_path: bool,
    target_batch_bytes: Option<u64>,
    max_batch_bytes: u64,
    max_wkb_cell_bytes: u64,
    tls_mode: PostgresTlsMode,
    tls_config: PostgresTlsConfig,
    network_options: PostgresNetworkOptions,
    schema_evolution: PostgresSchemaEvolution,
    pool: Arc<PostgresPool>,
    schema_cache: Arc<PostgresSchemaCache>,
    metrics: Arc<PostgresMetrics>,
    pool_acquire_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresFaultPoint {
    BeforeCommit,
    AfterCommitAcknowledgement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresInsertMode {
    CopyText,
    CopyBinary,
    Prepared,
}

/// Configurazione prestazionale coerente e misurata del data path `PostgreSQL`.
///
/// `LowLatency` mantiene piccoli i batch per ridurre il time to first batch.
/// `BalancedBulk` privilegia throughput e WAL usando COPY binario e batch più
/// grandi, senza adottare il profilo da 32K che aumenta sensibilmente RSS e
/// latenza iniziale sui dati wide/spatial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPerformanceProfile {
    LowLatency,
    BalancedBulk,
}

impl PostgresPerformanceProfile {
    #[must_use]
    pub const fn batch_rows(self) -> usize {
        match self {
            Self::LowLatency => 1_024,
            Self::BalancedBulk => 8_192,
        }
    }

    #[must_use]
    pub const fn insert_mode(self) -> PostgresInsertMode {
        match self {
            Self::LowLatency => PostgresInsertMode::CopyText,
            Self::BalancedBulk => PostgresInsertMode::CopyBinary,
        }
    }

    #[must_use]
    pub const fn target_batch_bytes(self) -> u64 {
        match self {
            Self::LowLatency => 1024 * 1024,
            Self::BalancedBulk => 4 * 1024 * 1024,
        }
    }
}

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
    ///
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSchemaEvolution {
    Disabled,
    AddNullableColumns,
}

/// Identità strutturale `PostgreSQL` di un oggetto introspezionato.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSchemaToken {
    pub schema_version: u32,
    pub database_oid: u32,
    pub namespace_oid: u32,
    pub relation_oid: u32,
    pub structural_fingerprint: String,
}

struct PostgresPool {
    idle: Mutex<HashMap<[u8; 32], Vec<Client>>>,
    semaphore: Arc<Semaphore>,
    max_idle_per_key: usize,
    metrics: Arc<PostgresMetrics>,
}

#[derive(Debug, Clone, Copy)]
struct PostgresSessionOptions {
    statement_timeout_ms: u64,
    lock_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SchemaCacheKey {
    connection: [u8; 32],
    schema: String,
    object: String,
}

#[derive(Clone)]
struct SchemaCacheEntry {
    token: CatalogSchemaToken,
    columns: Arc<Vec<ColumnSpec>>,
    last_used: u64,
}

#[derive(Default)]
struct SchemaCacheState {
    entries: HashMap<SchemaCacheKey, SchemaCacheEntry>,
    clock: u64,
}

struct PostgresSchemaCache {
    state: Mutex<SchemaCacheState>,
    max_entries: usize,
}

impl std::fmt::Debug for PostgresSchemaCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresSchemaCache")
            .field("max_entries", &self.max_entries)
            .field("entries", &self.len())
            .finish_non_exhaustive()
    }
}

impl PostgresSchemaCache {
    fn new(max_entries: usize) -> Self {
        Self {
            state: Mutex::new(SchemaCacheState::default()),
            max_entries,
        }
    }

    fn candidate(&self, key: &SchemaCacheKey) -> Option<SchemaCacheEntry> {
        lock_recover(&self.state).entries.get(key).cloned()
    }

    fn touch(&self, key: &SchemaCacheKey) {
        let mut state = lock_recover(&self.state);
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        if let Some(entry) = state.entries.get_mut(key) {
            entry.last_used = clock;
        }
    }

    fn insert(
        &self,
        key: SchemaCacheKey,
        token: CatalogSchemaToken,
        columns: Arc<Vec<ColumnSpec>>,
    ) -> bool {
        if self.max_entries == 0 {
            return false;
        }
        let mut state = lock_recover(&self.state);
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        state.entries.insert(
            key,
            SchemaCacheEntry {
                token,
                columns,
                last_used: clock,
            },
        );
        let evicted = if state.entries.len() > self.max_entries {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            oldest.is_some_and(|oldest| state.entries.remove(&oldest).is_some())
        } else {
            false
        };
        drop(state);
        evicted
    }

    fn invalidate(&self, key: &SchemaCacheKey) -> bool {
        lock_recover(&self.state).entries.remove(key).is_some()
    }

    fn len(&self) -> usize {
        lock_recover(&self.state).entries.len()
    }
}

impl std::fmt::Debug for PostgresPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresPool")
            .field("max_idle_per_key", &self.max_idle_per_key)
            .finish_non_exhaustive()
    }
}

impl PostgresPool {
    fn new(max_connections: usize, metrics: Arc<PostgresMetrics>) -> Self {
        Self {
            idle: Mutex::new(HashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_idle_per_key: max_connections,
            metrics,
        }
    }

    async fn checkout(
        self: &Arc<Self>,
        secret: &SecretString,
        tls_mode: PostgresTlsMode,
        tls_config: &PostgresTlsConfig,
        network_options: PostgresNetworkOptions,
        session_options: PostgresSessionOptions,
        acquire_timeout_ms: u64,
    ) -> Result<PooledClient> {
        if self.max_idle_per_key == 0 || acquire_timeout_ms == 0 {
            return Err(DatabaseError::invalid_plan(
                "pool PostgreSQL richiede dimensione e timeout maggiori di zero",
            ));
        }
        self.metrics.checkout();
        let permit = tokio::time::timeout(
            StdDuration::from_millis(acquire_timeout_ms),
            Arc::clone(&self.semaphore).acquire_owned(),
        )
        .await
        .map_err(|_| {
            self.metrics.pool_timeout();
            public_error(
                ErrorCategory::Timeout,
                ErrorPhase::Connect,
                true,
                "timeout acquisizione connessione PostgreSQL",
            )
        })?
        .map_err(|_| {
            public_error(
                ErrorCategory::Internal,
                ErrorPhase::Connect,
                false,
                "pool PostgreSQL chiuso",
            )
        })?;
        validate_session_timeouts(
            session_options.statement_timeout_ms,
            session_options.lock_timeout_ms,
        )?;
        let key = connection_fingerprint(
            secret,
            tls_mode,
            tls_config,
            network_options,
            session_options.statement_timeout_ms,
            session_options.lock_timeout_ms,
        );
        let mut client = lock_recover(&self.idle).get_mut(&key).and_then(Vec::pop);
        if client.as_ref().is_some_and(Client::is_closed) {
            self.metrics.invalidate();
            client = None;
        }
        let (client, reused) = if let Some(client) = client {
            self.metrics.reuse();
            (client, true)
        } else {
            let client = PostgresProvider::connect_with_tls(
                secret,
                tls_mode,
                tls_config,
                network_options,
                session_options.statement_timeout_ms,
                session_options.lock_timeout_ms,
            )
            .await?;
            self.metrics.new_connection();
            (client, false)
        };
        Ok(PooledClient {
            client: Some(client),
            key,
            pool: Arc::clone(self),
            reused,
            reusable: true,
            _permit: permit,
        })
    }
}

struct PooledClient {
    client: Option<Client>,
    key: [u8; 32],
    pool: Arc<PostgresPool>,
    reused: bool,
    reusable: bool,
    _permit: OwnedSemaphorePermit,
}

impl PooledClient {
    const fn invalidate(&mut self) {
        self.reusable = false;
    }

    const fn mark_reusable(&mut self) {
        self.reusable = true;
    }

    const fn was_reused(&self) -> bool {
        self.reused
    }

    fn client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or_else(|| {
            public_error(
                ErrorCategory::Internal,
                ErrorPhase::Connect,
                false,
                "client PostgreSQL del pool non disponibile",
            )
        })
    }

    fn client_mut(&mut self) -> Result<&mut Client> {
        self.client.as_mut().ok_or_else(|| {
            public_error(
                ErrorCategory::Internal,
                ErrorPhase::Connect,
                false,
                "client PostgreSQL del pool non disponibile",
            )
        })
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        if !self.reusable || client.is_closed() {
            self.pool.metrics.invalidate();
            return;
        }
        let mut idle = lock_recover(&self.pool.idle);
        let clients = idle.entry(self.key).or_default();
        if clients.len() < self.pool.max_idle_per_key {
            clients.push(client);
        }
        drop(idle);
    }
}

fn connection_fingerprint(
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

fn validate_session_timeouts(statement_timeout_ms: u64, lock_timeout_ms: u64) -> Result<()> {
    if statement_timeout_ms == 0 || lock_timeout_ms == 0 {
        return Err(DatabaseError::invalid_plan(
            "timeout PostgreSQL deve essere maggiore di zero",
        ));
    }
    Ok(())
}

fn configure_session_startup(
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

fn connection_config(secret: &SecretString, options: PostgresNetworkOptions) -> Result<Config> {
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
        .connect_timeout(StdDuration::from_millis(options.connect_timeout_ms))
        .tcp_user_timeout(StdDuration::from_millis(options.tcp_user_timeout_ms))
        .keepalives(true)
        .keepalives_idle(StdDuration::from_secs(options.keepalive_idle_secs))
        .keepalives_interval(StdDuration::from_secs(options.keepalive_interval_secs))
        .keepalives_retries(options.keepalive_retries);
    Ok(config)
}

fn connection_config_for_mode(
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

impl Default for PostgresProvider {
    fn default() -> Self {
        Self::for_profile(PostgresPerformanceProfile::LowLatency)
    }
}

impl PostgresProvider {
    fn build(batch_rows: usize) -> Self {
        let metrics = Arc::new(PostgresMetrics::default());
        Self {
            batch_rows,
            statement_timeout_ms: 30_000,
            lock_timeout_ms: 5_000,
            fault_point: None,
            insert_mode: PostgresInsertMode::CopyText,
            parameterized_read_fast_path: true,
            target_batch_bytes: None,
            max_batch_bytes: 16 * 1024 * 1024,
            max_wkb_cell_bytes: 64 * 1024 * 1024,
            tls_mode: PostgresTlsMode::Disabled,
            tls_config: PostgresTlsConfig::default(),
            network_options: PostgresNetworkOptions::default(),
            schema_evolution: PostgresSchemaEvolution::Disabled,
            pool: Arc::new(PostgresPool::new(8, Arc::clone(&metrics))),
            schema_cache: Arc::new(PostgresSchemaCache::new(256)),
            metrics,
            pool_acquire_timeout_ms: 30_000,
        }
    }

    #[must_use]
    pub fn new(batch_rows: usize) -> Self {
        Self::build(batch_rows)
    }

    /// Costruisce il provider con un profilo prestazionale misurato.
    ///
    /// I setter successivi possono ancora sovrascrivere le singole scelte.
    #[must_use]
    pub fn for_profile(profile: PostgresPerformanceProfile) -> Self {
        Self::build(profile.batch_rows()).with_performance_profile(profile)
    }

    /// Applica un profilo a un provider già configurato, conservando pool,
    /// timeout, TLS, limiti in byte ed evoluzione schema.
    #[must_use]
    pub const fn with_performance_profile(mut self, profile: PostgresPerformanceProfile) -> Self {
        self.batch_rows = profile.batch_rows();
        self.insert_mode = profile.insert_mode();
        self.target_batch_bytes = Some(profile.target_batch_bytes());
        self
    }

    /// Imposta un target soft in byte per i batch di lettura.
    ///
    /// Il batch viene chiuso dopo la riga che raggiunge il target; una singola
    /// riga può quindi superarlo. `max_batch_bytes` resta il limite hard.
    #[must_use]
    pub const fn with_target_batch_bytes(mut self, target_batch_bytes: u64) -> Self {
        self.target_batch_bytes = Some(target_batch_bytes);
        self
    }

    /// Disabilita il target adattivo e usa soltanto il tetto in righe.
    #[must_use]
    pub const fn without_target_batch_bytes(mut self) -> Self {
        self.target_batch_bytes = None;
        self
    }

    #[must_use]
    pub const fn with_timeouts(mut self, statement_timeout_ms: u64, lock_timeout_ms: u64) -> Self {
        self.statement_timeout_ms = statement_timeout_ms;
        self.lock_timeout_ms = lock_timeout_ms;
        self
    }

    #[must_use]
    pub const fn with_fault_injection(mut self, fault_point: PostgresFaultPoint) -> Self {
        self.fault_point = Some(fault_point);
        self
    }

    #[must_use]
    pub const fn with_insert_mode(mut self, insert_mode: PostgresInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }

    /// Abilita o disabilita il protocollo one-shot per read con bind tipizzabili.
    #[must_use]
    pub const fn with_parameterized_read_fast_path(mut self, enabled: bool) -> Self {
        self.parameterized_read_fast_path = enabled;
        self
    }

    #[must_use]
    pub const fn with_byte_limits(mut self, max_batch_bytes: u64, max_wkb_cell_bytes: u64) -> Self {
        self.max_batch_bytes = max_batch_bytes;
        self.max_wkb_cell_bytes = max_wkb_cell_bytes;
        self
    }

    #[must_use]
    pub const fn with_tls_mode(mut self, tls_mode: PostgresTlsMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    #[must_use]
    pub fn with_tls_config(mut self, tls_config: PostgresTlsConfig) -> Self {
        self.tls_mode = PostgresTlsMode::Require;
        self.tls_config = tls_config;
        self
    }

    #[must_use]
    pub const fn with_network_options(mut self, network_options: PostgresNetworkOptions) -> Self {
        self.network_options = network_options;
        self
    }

    #[must_use]
    pub const fn with_schema_evolution(
        mut self,
        schema_evolution: PostgresSchemaEvolution,
    ) -> Self {
        self.schema_evolution = schema_evolution;
        self
    }

    #[must_use]
    pub fn with_pool_size(mut self, max_connections: usize, acquire_timeout_ms: u64) -> Self {
        self.pool = Arc::new(PostgresPool::new(
            max_connections,
            Arc::clone(&self.metrics),
        ));
        self.pool_acquire_timeout_ms = acquire_timeout_ms;
        self
    }

    /// Imposta il numero massimo di schemi `PostgreSQL` conservati nella cache.
    ///
    /// La cache resta strict: ogni hit viene validato contro un token di
    /// catalogo. Una capacità pari a zero la disabilita.
    #[must_use]
    pub fn with_schema_cache_capacity(mut self, max_entries: usize) -> Self {
        self.schema_cache = Arc::new(PostgresSchemaCache::new(max_entries));
        self
    }

    /// Restituisce contatori bounded e privi di label sensibili o cardinalità dinamica.
    #[must_use]
    pub fn metrics_snapshot(&self) -> PostgresMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn pool_idle_connections(&self) -> usize {
        lock_recover(&self.pool.idle).values().map(Vec::len).sum()
    }

    #[must_use]
    pub fn schema_cache_entries(&self) -> usize {
        self.schema_cache.len()
    }

    #[cfg(test)]
    async fn connect(secret: &SecretString) -> Result<Client> {
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

    async fn connect_session(&self, secret: &SecretString) -> Result<PooledClient> {
        let mut client = self
            .pool
            .checkout(
                secret,
                self.tls_mode,
                &self.tls_config,
                self.network_options,
                PostgresSessionOptions {
                    statement_timeout_ms: self.statement_timeout_ms,
                    lock_timeout_ms: self.lock_timeout_ms,
                },
                self.pool_acquire_timeout_ms,
            )
            .await?;
        if client.was_reused() {
            if let Err(error) = client.client()?.batch_execute("DISCARD ALL").await {
                client.invalidate();
                return Err(classify_error(ErrorPhase::Connect, &error));
            }
            self.metrics.session_reset();
        }
        Ok(client)
    }

    pub(crate) async fn connect_with_tls(
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
        let mut config =
            connection_config_for_mode(secret, network_options, PostgresTlsMode::Require)?;
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

    #[allow(clippy::too_many_lines)]
    async fn load_columns_and_token(
        client: &Client,
        source: &ObjectRef,
    ) -> Result<(Arc<Vec<ColumnSpec>>, CatalogSchemaToken)> {
        let schema = source.schema.as_deref().unwrap_or("public");
        let rows = client
            .query(
                r"
                SELECT
                    a.attname,
                    t.typname,
                    NOT a.attnotnull AS nullable,
                    CASE
                      WHEN t.typname = 'numeric' AND a.atttypmod >= 0
                      THEN ((a.atttypmod - 4) >> 16) & 65535
                    END AS numeric_precision,
                    CASE
                      WHEN t.typname = 'numeric' AND a.atttypmod >= 0
                      THEN CASE
                        WHEN ((a.atttypmod - 4) & 2047) >= 1024
                        THEN ((a.atttypmod - 4) & 2047) - 2048
                        ELSE (a.atttypmod - 4) & 2047
                      END
                    END AS numeric_scale,
                    CASE
                      WHEN t.typname IN ('geometry', 'geography')
                           AND a.atttypmod >= 0
                      THEN postgis_typmod_srid(a.atttypmod)
                    END AS spatial_srid,
                    CASE
                      WHEN t.typname IN ('geometry', 'geography')
                           AND a.atttypmod >= 0
                      THEN postgis_typmod_dims(a.atttypmod)
                    END AS spatial_dims,
                    CASE
                      WHEN t.typname IN ('geometry', 'geography')
                           AND a.atttypmod >= 0
                      THEN postgis_typmod_type(a.atttypmod)
                    END AS spatial_type,
                    srs.auth_name AS spatial_crs_auth_name,
                    srs.auth_srid AS spatial_crs_auth_srid,
                    pg_get_expr(ad.adbin, ad.adrelid) AS default_expression,
                    NULLIF(a.attidentity, '')::text AS identity_kind,
                    NULLIF(a.attgenerated, '')::text AS generated_kind,
                    format_type(a.atttypid, a.atttypmod) AS native_declaration,
                    t.typtype::text AS type_kind,
                    CASE WHEN t.typtype = 'c' THEN (
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'name', ca.attname,
                                'declaration', format_type(ca.atttypid, ca.atttypmod)
                            )
                            ORDER BY ca.attnum
                        )::text
                        FROM pg_catalog.pg_attribute ca
                        WHERE ca.attrelid = t.typrelid
                          AND ca.attnum > 0
                          AND NOT ca.attisdropped
                    ) END AS composite_fields,
                    d.oid::bigint AS database_oid,
                    n.oid::bigint AS namespace_oid,
                    c.oid::bigint AS relation_oid,
                    signature.structural_signature,
                    CASE WHEN t.typtype = 'e' THEN (
                        SELECT jsonb_agg(e.enumlabel ORDER BY e.enumsortorder)::text
                        FROM pg_catalog.pg_enum e
                        WHERE e.enumtypid = t.oid
                    ) END AS enum_labels,
                    CASE WHEN t.typtype = 'd'
                         THEN format_type(t.typbasetype, t.typtypmod)
                    END AS domain_base_type,
                    CASE WHEN t.typtype = 'd' THEN (
                        SELECT jsonb_agg(
                            pg_get_constraintdef(dc.oid, true)
                            ORDER BY dc.conname
                        )::text
                        FROM pg_catalog.pg_constraint dc
                        WHERE dc.contypid = t.oid
                    ) END AS domain_constraints,
                    CASE WHEN a.attcollation <> t.typcollation
                         THEN coll.collname
                    END AS collation
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_database d ON d.datname = current_database()
                JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                LEFT JOIN pg_catalog.pg_attrdef ad
                  ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                LEFT JOIN pg_catalog.pg_collation coll ON coll.oid = a.attcollation
                LEFT JOIN spatial_ref_sys srs
                  ON t.typname IN ('geometry', 'geography')
                 AND a.atttypmod >= 0
                 AND srs.srid = postgis_typmod_srid(a.atttypmod)
                CROSS JOIN LATERAL (
                    SELECT jsonb_build_object(
                        'relation',
                        jsonb_build_array(c.relname, c.relkind, c.xmin::text),
                        'columns',
                        COALESCE((
                            SELECT jsonb_agg(
                                jsonb_build_array(
                                    sa.attnum,
                                    sa.attname,
                                    sa.atttypid::text,
                                    sa.atttypmod,
                                    sa.attnotnull,
                                    sa.attidentity,
                                    sa.attgenerated,
                                    sa.attcollation::text,
                                    sa.xmin::text,
                                    st.typname,
                                    st.typtype,
                                    st.xmin::text,
                                    pg_get_expr(sad.adbin, sad.adrelid),
                                    CASE WHEN st.typtype = 'e' THEN (
                                        SELECT jsonb_agg(
                                            jsonb_build_array(
                                                se.enumlabel,
                                                se.enumsortorder,
                                                se.xmin::text
                                            )
                                            ORDER BY se.enumsortorder
                                        )
                                        FROM pg_catalog.pg_enum se
                                        WHERE se.enumtypid = st.oid
                                    ) END,
                                    CASE WHEN st.typtype = 'd' THEN (
                                        SELECT jsonb_agg(
                                            jsonb_build_array(
                                                sdc.conname,
                                                sdc.xmin::text,
                                                pg_get_constraintdef(sdc.oid, true)
                                            )
                                            ORDER BY sdc.conname
                                        )
                                        FROM pg_catalog.pg_constraint sdc
                                        WHERE sdc.contypid = st.oid
                                    ) END,
                                    CASE WHEN st.typtype = 'c' THEN (
                                        SELECT jsonb_agg(
                                            jsonb_build_array(
                                                sca.attnum,
                                                sca.attname,
                                                sca.atttypid::text,
                                                sca.atttypmod,
                                                sca.xmin::text,
                                                format_type(
                                                    sca.atttypid,
                                                    sca.atttypmod
                                                )
                                            )
                                            ORDER BY sca.attnum
                                        )
                                        FROM pg_catalog.pg_attribute sca
                                        WHERE sca.attrelid = st.typrelid
                                          AND sca.attnum > 0
                                          AND NOT sca.attisdropped
                                    ) END,
                                    CASE
                                      WHEN st.typname IN ('geometry', 'geography')
                                           AND sa.atttypmod >= 0
                                      THEN (
                                        SELECT jsonb_build_array(
                                            ssrs.auth_name,
                                            ssrs.auth_srid,
                                            ssrs.xmin::text
                                        )
                                        FROM spatial_ref_sys ssrs
                                        WHERE ssrs.srid =
                                            postgis_typmod_srid(sa.atttypmod)
                                      )
                                    END
                                )
                                ORDER BY sa.attnum
                            )
                            FROM pg_catalog.pg_attribute sa
                            JOIN pg_catalog.pg_type st ON st.oid = sa.atttypid
                            LEFT JOIN pg_catalog.pg_attrdef sad
                              ON sad.adrelid = sa.attrelid
                             AND sad.adnum = sa.attnum
                            WHERE sa.attrelid = c.oid
                              AND sa.attnum > 0
                              AND NOT sa.attisdropped
                        ), '[]'::jsonb)
                    )::text AS structural_signature
                ) signature
                WHERE n.nspname = $1
                  AND c.relname = $2
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attnum
                ",
                &[&schema, &source.object],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
        if rows.is_empty() {
            return Err(public_error(
                ErrorCategory::NotFound,
                ErrorPhase::Probe,
                false,
                "oggetto PostgreSQL non trovato",
            ));
        }
        let token = CatalogSchemaToken::from_catalog_row(&rows[0])?;
        let columns = Arc::new(
            rows.iter()
                .map(ColumnSpec::from_catalog_row)
                .collect::<Result<Vec<_>>>()?,
        );
        Ok((columns, token))
    }

    #[allow(clippy::too_many_lines)]
    async fn schema_token(client: &Client, source: &ObjectRef) -> Result<CatalogSchemaToken> {
        let schema = source.schema.as_deref().unwrap_or("public");
        let row = client
            .query_opt(
                r"
                SELECT
                    d.oid::bigint AS database_oid,
                    n.oid::bigint AS namespace_oid,
                    c.oid::bigint AS relation_oid,
                    jsonb_build_object(
                        'relation',
                        jsonb_build_array(c.relname, c.relkind, c.xmin::text),
                        'columns',
                        COALESCE((
                            SELECT jsonb_agg(
                                jsonb_build_array(
                                    a.attnum,
                                    a.attname,
                                    a.atttypid::text,
                                    a.atttypmod,
                                    a.attnotnull,
                                    a.attidentity,
                                    a.attgenerated,
                                    a.attcollation::text,
                                    a.xmin::text,
                                    t.typname,
                                    t.typtype,
                                    t.xmin::text,
                                    pg_get_expr(ad.adbin, ad.adrelid),
                                    CASE WHEN t.typtype = 'e' THEN (
                                        SELECT jsonb_agg(
                                            jsonb_build_array(
                                                e.enumlabel,
                                                e.enumsortorder,
                                                e.xmin::text
                                            )
                                            ORDER BY e.enumsortorder
                                        )
                                        FROM pg_catalog.pg_enum e
                                        WHERE e.enumtypid = t.oid
                                    ) END,
                                    CASE WHEN t.typtype = 'd' THEN (
                                        SELECT jsonb_agg(
                                            jsonb_build_array(
                                                dc.conname,
                                                dc.xmin::text,
                                                pg_get_constraintdef(dc.oid, true)
                                            )
                                            ORDER BY dc.conname
                                        )
                                        FROM pg_catalog.pg_constraint dc
                                        WHERE dc.contypid = t.oid
                                    ) END,
                                    CASE WHEN t.typtype = 'c' THEN (
                                        SELECT jsonb_agg(
                                            jsonb_build_array(
                                                ca.attnum,
                                                ca.attname,
                                                ca.atttypid::text,
                                                ca.atttypmod,
                                                ca.xmin::text,
                                                format_type(ca.atttypid, ca.atttypmod)
                                            )
                                            ORDER BY ca.attnum
                                        )
                                        FROM pg_catalog.pg_attribute ca
                                        WHERE ca.attrelid = t.typrelid
                                          AND ca.attnum > 0
                                          AND NOT ca.attisdropped
                                    ) END,
                                    CASE
                                      WHEN t.typname IN ('geometry', 'geography')
                                           AND a.atttypmod >= 0
                                      THEN (
                                        SELECT jsonb_build_array(
                                            srs.auth_name,
                                            srs.auth_srid,
                                            srs.xmin::text
                                        )
                                        FROM spatial_ref_sys srs
                                        WHERE srs.srid =
                                            postgis_typmod_srid(a.atttypmod)
                                      )
                                    END
                                )
                                ORDER BY a.attnum
                            )
                            FROM pg_catalog.pg_attribute a
                            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                            LEFT JOIN pg_catalog.pg_attrdef ad
                              ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                            WHERE a.attrelid = c.oid
                              AND a.attnum > 0
                              AND NOT a.attisdropped
                        ), '[]'::jsonb)
                    )::text AS structural_signature
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_database d ON d.datname = current_database()
                WHERE n.nspname = $1 AND c.relname = $2
                ",
                &[&schema, &source.object],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Probe, &error))?
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::NotFound,
                    ErrorPhase::Probe,
                    false,
                    "oggetto PostgreSQL non trovato",
                )
            })?;
        CatalogSchemaToken::from_catalog_row(&row)
    }

    fn schema_cache_key(client: &PooledClient, source: &ObjectRef) -> SchemaCacheKey {
        SchemaCacheKey {
            connection: client.key,
            schema: source.schema.as_deref().unwrap_or("public").to_owned(),
            object: source.object.clone(),
        }
    }

    async fn cached_columns(
        &self,
        client: &PooledClient,
        source: &ObjectRef,
    ) -> Result<(Arc<Vec<ColumnSpec>>, PostgresSchemaToken)> {
        let key = Self::schema_cache_key(client, source);
        if let Some(candidate) = self.schema_cache.candidate(&key) {
            self.metrics.schema_token_check();
            let current = match Self::schema_token(client.client()?, source).await {
                Ok(token) => token,
                Err(error) => {
                    if self.schema_cache.invalidate(&key) {
                        self.metrics.schema_cache_invalidation();
                    }
                    return Err(error);
                }
            };
            if candidate.token.structurally_equals(&current) {
                self.schema_cache.touch(&key);
                self.metrics.schema_cache_hit();
                return Ok((candidate.columns, current.public));
            }
            if self.schema_cache.invalidate(&key) {
                self.metrics.schema_cache_invalidation();
            }
        }
        self.metrics.schema_cache_miss();
        self.metrics.catalog_introspection();
        let (columns, token) = Self::load_columns_and_token(client.client()?, source).await?;
        if self
            .schema_cache
            .insert(key, token.clone(), Arc::clone(&columns))
        {
            self.metrics.schema_cache_eviction();
        }
        Ok((columns, token.public))
    }

    fn invalidate_cached_schema(&self, secret: &SecretString, source: &ObjectRef) {
        let connection = connection_fingerprint(
            secret,
            self.tls_mode,
            &self.tls_config,
            self.network_options,
            self.statement_timeout_ms,
            self.lock_timeout_ms,
        );
        let key = SchemaCacheKey {
            connection,
            schema: source.schema.as_deref().unwrap_or("public").to_owned(),
            object: source.object.clone(),
        };
        if self.schema_cache.invalidate(&key) {
            self.metrics.schema_cache_invalidation();
        }
    }

    async fn capability_document(client: &Client) -> Result<ProviderCapabilities> {
        let row = client
            .query_one(
                r"
                SELECT
                    current_setting('server_version'),
                    COALESCE(
                      (SELECT extversion FROM pg_extension WHERE extname = 'postgis'),
                      ''
                    )
                ",
                &[],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
        let server_version: String = row.get(0);
        let postgis_version: String = row.get(1);
        let spatial = !postgis_version.is_empty();
        let mut extensions = BTreeMap::new();
        if spatial {
            extensions.insert("postgis".to_owned(), postgis_version);
        }
        Ok(ProviderCapabilities {
            schema_version: 1,
            provider: ProviderKind::Postgres,
            provider_version: server_version,
            extension_versions: extensions,
            reads: ReadCapabilities {
                streaming: true,
                server_cursor: true,
                pagination: true,
                object_id_windows: false,
                projection: true,
                filter: true,
                ordering: true,
                resumable: false,
            },
            writes: WriteCapabilities {
                create: true,
                append: true,
                update: true,
                upsert: true,
                replace: true,
                delete_by_keys: true,
                bulk: true,
                array_binding: false,
                returning: true,
                apply_edits: false,
                rollback_on_failure: true,
                use_global_ids: false,
            },
            transactions: TransactionCapabilities {
                single_transaction: true,
                savepoints: true,
                transactional_ddl: true,
                staged_swap: true,
                scope: TransactionScope::Transaction,
            },
            spatial: SpatialCapabilities {
                read_wkb: spatial,
                write_wkb: spatial,
                geometry: spatial,
                geography: spatial,
                spatial_index: spatial,
                mixed_geometry_types: spatial,
                dimensions: if spatial {
                    vec![
                        Dimensions::Xy,
                        Dimensions::Xyz,
                        Dimensions::Xym,
                        Dimensions::Xyzm,
                    ]
                } else {
                    Vec::new()
                },
                functions: if spatial {
                    SpatialFunction::ALL.to_vec()
                } else {
                    Vec::new()
                },
            },
            limits: ProviderLimits {
                max_identifier_bytes: Some(63),
                max_bind_parameters: Some(65_535),
                max_statement_bytes: None,
                max_batch_rows: None,
                max_payload_bytes: None,
                max_record_count: None,
            },
        })
    }

    async fn inspection(&self, client: &PooledClient, operation: &Operation) -> Result<Inspection> {
        match operation {
            Operation::DatabaseListCatalogs => {
                let rows = client
                    .client()?
                    .query(
                        "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname",
                        &[],
                    )
                    .await
                    .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
                Ok(Inspection {
                    operation: "database.list_catalogs".to_owned(),
                    document: json!({
                        "catalogs": rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>()
                    }),
                })
            }
            Operation::DatabaseListSchemas { .. } => {
                let rows = client
                    .client()?
                    .query(
                        r"
                        SELECT schema_name
                        FROM information_schema.schemata
                        ORDER BY schema_name
                        ",
                        &[],
                    )
                    .await
                    .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
                Ok(Inspection {
                    operation: "database.list_schemas".to_owned(),
                    document: json!({
                        "schemas": rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>()
                    }),
                })
            }
            Operation::DatabaseListObjects { source } => {
                let schema = source
                    .as_ref()
                    .and_then(|value| value.schema.as_deref())
                    .unwrap_or("public");
                let rows = client
                    .client()?
                    .query(
                        r"
                        SELECT c.relname, c.relkind::text, c.relispartition
                        FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = $1
                          AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
                        ORDER BY c.relname
                        ",
                        &[&schema],
                    )
                    .await
                    .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
                let objects = rows
                    .iter()
                    .map(|row| {
                        json!({
                            "name": row.get::<_, String>(0),
                            "kind": relation_kind(&row.get::<_, String>(1)),
                            "is_partition": row.get::<_, bool>(2)
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(Inspection {
                    operation: "database.list_objects".to_owned(),
                    document: json!({"schema": schema, "objects": objects}),
                })
            }
            Operation::DatabaseDescribeObject { source } => {
                let (columns, schema_token) = self.cached_columns(client, source).await?;
                let metadata = describe_object_metadata(client.client()?, source).await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!({
                        "columns": columns.as_ref(),
                        "schema_token": schema_token,
                        "relation": metadata.relation,
                        "constraints": metadata.constraints,
                        "indexes": metadata.indexes,
                        "policies": metadata.policies,
                        "privileges": metadata.privileges
                    }),
                })
            }
            _ => Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                ErrorPhase::Probe,
                "operazione di introspezione PostgreSQL non supportata",
            )),
        }
    }

    async fn start_read_query(
        &self,
        client: &mut PooledClient,
        sql: &str,
        parameter_refs: Vec<&(dyn ToSql + Sync)>,
        parameter_types: Option<Vec<Type>>,
        cancellation: &CancellationToken,
    ) -> Result<RowStream> {
        let parameter_count = parameter_refs.len();
        let (result, error_phase) = if let Some(parameter_types) = parameter_types {
            let query = client.client()?.query_typed_raw(
                sql,
                parameter_refs.into_iter().zip(parameter_types.into_iter()),
            );
            (
                select_with_cancellation(query, cancellation)
                    .await
                    .map(|result| {
                        result.inspect(|_| {
                            self.metrics.read_typed_fast_path();
                            if parameter_count > 0 {
                                self.metrics.read_parameterized_typed_fast_path();
                            }
                        })
                    }),
                ErrorPhase::Prepare,
            )
        } else {
            self.metrics.read_prepared_fallback();
            let statement = client
                .client()?
                .prepare(sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
            (
                select_with_cancellation(
                    client.client()?.query_raw(&statement, parameter_refs),
                    cancellation,
                )
                .await,
                ErrorPhase::Read,
            )
        };
        if let Some(result) = result {
            return result.map_err(|error| classify_error(error_phase, &error));
        }
        cancel_and_invalidate(
            client,
            self.tls_mode,
            &self.tls_config.connector,
            self.network_options.connect_timeout_ms,
        )
        .await;
        Err(cancelled_read_error(cancellation))
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn read_stream(
        &self,
        secret: &SecretString,
        operation: &ReadOperation,
        parameters: &ParameterBag,
        budget: &ResourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn BatchStream>> {
        if cancellation.is_cancelled() {
            self.metrics.cancellation();
            return Err(cancelled_read_error(cancellation));
        }
        if self.batch_rows == 0 || self.max_batch_bytes == 0 || self.target_batch_bytes == Some(0) {
            return Err(DatabaseError::invalid_plan(
                "batch_rows e budget byte PostgreSQL devono essere maggiori di zero",
            ));
        }
        let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
        let mut client = if let Some(result) =
            select_with_cancellation(self.connect_session(secret), cancellation).await
        {
            result?
        } else {
            self.metrics.cancellation();
            return Err(cancelled_read_error(cancellation));
        };
        if let Some(catalog) = &operation.source.catalog {
            let current_database: String = client
                .client()?
                .query_one("SELECT current_database()", &[])
                .await
                .map_err(|error| classify_error(ErrorPhase::Probe, &error))?
                .get(0);
            if catalog != &current_database {
                return Err(public_error(
                    ErrorCategory::NotFound,
                    ErrorPhase::Prepare,
                    false,
                    "catalog PostgreSQL diverso dalla connessione corrente",
                ));
            }
        }
        let (available, _schema_token) = self.cached_columns(&client, &operation.source).await?;
        let selected = select_columns(&available, &operation.projection)?;
        let columns = u64::try_from(selected.len())
            .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
        let columns_lease = budget.try_lease(ResourceKind::Columns, columns)?;
        let (sql, bind_names) = build_read_sql(operation, &selected, &available)?;
        let owned_parameters = bind_parameters(parameters, &bind_names)?;
        let parameter_refs = owned_parameters
            .iter()
            .map(|value| value.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let parameter_types = if self.parameterized_read_fast_path || bind_names.is_empty() {
            typed_filter_parameter_types(
                operation.filter.as_ref(),
                &bind_names,
                parameters,
                &available,
            )
        } else {
            None
        };
        let rows = self
            .start_read_query(
                &mut client,
                &sql,
                parameter_refs,
                parameter_types,
                cancellation,
            )
            .await?;
        let cancel_token = client.client()?.cancel_token();
        let schema = contract_schema(
            selected
                .iter()
                .map(ColumnSpec::arrow_field)
                .collect::<Vec<_>>(),
        );
        Ok(Box::new(PostgresBatchStream::new(
            self,
            ReadStreamSource::new(client, cancel_token, Box::pin(rows), selected, schema),
            budget,
            operation_lease,
            columns_lease,
        )))
    }

    #[allow(clippy::significant_drop_tightening)]
    #[allow(clippy::too_many_lines)]
    async fn query_stream(
        &self,
        secret: &SecretString,
        operation: &QueryOperation,
        parameters: &ParameterBag,
        budget: &ResourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn BatchStream>> {
        if cancellation.is_cancelled() {
            self.metrics.cancellation();
            return Err(cancelled_read_error(cancellation));
        }
        if self.batch_rows == 0 || self.max_batch_bytes == 0 || self.target_batch_bytes == Some(0) {
            return Err(DatabaseError::invalid_plan(
                "batch_rows e budget byte PostgreSQL devono essere maggiori di zero",
            ));
        }
        let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
        let rendered = postgres_renderer().render_query(operation)?;
        let mut client = if let Some(result) =
            select_with_cancellation(self.connect_session(secret), cancellation).await
        {
            result?
        } else {
            self.metrics.cancellation();
            return Err(cancelled_read_error(cancellation));
        };
        let bind_names = rendered
            .binds
            .iter()
            .map(|bind| bind.name.clone())
            .collect::<Vec<_>>();
        let owned_parameters = bind_parameters(parameters, &bind_names)?;
        let parameter_refs = owned_parameters
            .iter()
            .map(|value| value.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let typed_parameter_types = self
            .parameterized_read_fast_path
            .then(|| typed_query_parameter_types(&bind_names, parameters))
            .flatten();
        let mut typed_rows = None;
        if let Some(parameter_types) = typed_parameter_types {
            let typed_parameters = parameter_refs
                .iter()
                .copied()
                .zip(parameter_types.into_iter());
            if let Some(result) = select_with_cancellation(
                client
                    .client()?
                    .query_typed_raw(&rendered.sql, typed_parameters),
                cancellation,
            )
            .await
            {
                if let Ok(rows) = result {
                    typed_rows = Some(rows);
                }
            } else {
                cancel_and_invalidate(
                    &mut client,
                    self.tls_mode,
                    &self.tls_config.connector,
                    self.network_options.connect_timeout_ms,
                )
                .await;
                return Err(cancelled_read_error(cancellation));
            }
        }
        let (rows, columns): (PostgresRows, Vec<ColumnSpec>) = if let Some(raw_rows) = typed_rows {
            let mut raw_rows = Box::pin(raw_rows);
            let first = if let Some(result) =
                select_with_cancellation(raw_rows.as_mut().next(), cancellation).await
            {
                result.transpose().map_err(|error| {
                    client.invalidate();
                    classify_error(ErrorPhase::Read, &error)
                })?
            } else {
                cancel_and_invalidate(
                    &mut client,
                    self.tls_mode,
                    &self.tls_config.connector,
                    self.network_options.connect_timeout_ms,
                )
                .await;
                return Err(cancelled_read_error(cancellation));
            };
            self.metrics.query_typed_fast_path();
            if let Some(first) = first {
                let mut columns = first
                    .columns()
                    .iter()
                    .map(ColumnSpec::from_statement_column)
                    .collect::<Result<Vec<_>>>()?;
                mark_query_spatial_columns(operation, &mut columns);
                let rows = futures_util::stream::once(async move { Ok(first) }).chain(raw_rows);
                (Box::pin(rows), columns)
            } else {
                let statement = client
                    .client()?
                    .prepare(&rendered.sql)
                    .await
                    .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
                let mut columns = statement
                    .columns()
                    .iter()
                    .map(ColumnSpec::from_statement_column)
                    .collect::<Result<Vec<_>>>()?;
                mark_query_spatial_columns(operation, &mut columns);
                (Box::pin(futures_util::stream::empty()), columns)
            }
        } else {
            self.metrics.query_prepared_fallback();
            let statement = client
                .client()?
                .prepare(&rendered.sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
            let mut columns = statement
                .columns()
                .iter()
                .map(ColumnSpec::from_statement_column)
                .collect::<Result<Vec<_>>>()?;
            mark_query_spatial_columns(operation, &mut columns);
            let rows = if let Some(result) = select_with_cancellation(
                client.client()?.query_raw(&statement, parameter_refs),
                cancellation,
            )
            .await
            {
                result.map_err(|error| classify_error(ErrorPhase::Read, &error))?
            } else {
                cancel_and_invalidate(
                    &mut client,
                    self.tls_mode,
                    &self.tls_config.connector,
                    self.network_options.connect_timeout_ms,
                )
                .await;
                return Err(cancelled_read_error(cancellation));
            };
            (Box::pin(rows), columns)
        };
        let schema = contract_schema(
            columns
                .iter()
                .map(ColumnSpec::arrow_field)
                .collect::<Vec<_>>(),
        );
        let column_count = u64::try_from(columns.len())
            .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
        let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
        let cancel_token = client.client()?.cancel_token();
        Ok(Box::new(PostgresBatchStream::new(
            self,
            ReadStreamSource::new(client, cancel_token, rows, columns, schema),
            budget,
            operation_lease,
            columns_lease,
        )))
    }

    #[allow(clippy::too_many_lines)]
    async fn preflight_write(
        &self,
        secret: &SecretString,
        operation: &WriteOperation,
        input_schema: &SchemaRef,
    ) -> Result<LossReport> {
        let client = self.connect_session(secret).await?;
        validate_resolved_crs_against_postgis(&client, input_schema).await?;
        let schema_name = operation.target.schema.as_deref().unwrap_or("public");
        let exists: bool = client
            .client()?
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )",
                &[&schema_name, &operation.target.object],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
            .get(0);
        if operation.mode == plenora_database_core::plan::WriteMode::Create && exists {
            return Err(public_error(
                ErrorCategory::Conflict,
                ErrorPhase::Prepare,
                false,
                "target PostgreSQL già esistente",
            ));
        }
        if !matches!(
            operation.mode,
            plenora_database_core::plan::WriteMode::Create
                | plenora_database_core::plan::WriteMode::Replace
        ) && !exists
        {
            return Err(public_error(
                ErrorCategory::NotFound,
                ErrorPhase::Prepare,
                false,
                "target PostgreSQL non esistente",
            ));
        }
        let mut losses = Vec::new();
        if exists
            && !matches!(
                operation.mode,
                plenora_database_core::plan::WriteMode::Create
                    | plenora_database_core::plan::WriteMode::Replace
            )
        {
            let (target_columns, _schema_token) =
                self.cached_columns(&client, &operation.target).await?;
            for (field_id, source) in input_schema.fields().iter().enumerate() {
                let Some(target) = target_columns
                    .iter()
                    .find(|column| column.name == source.name().as_str())
                else {
                    if self.schema_evolution == PostgresSchemaEvolution::AddNullableColumns
                        && supports_additive_evolution(operation.mode)
                    {
                        losses.push(MappingLoss {
                            field_id: u32::try_from(field_id).map_err(|_| {
                                DatabaseError::invalid_plan(
                                    "numero colonne oltre il contratto LossReport",
                                )
                            })?,
                            category: LossCategory::NativeType,
                            severity: LossSeverity::Information,
                            reason: "colonna aggiunta al target come nullable".to_owned(),
                            source_type: Some(source.data_type().to_string()),
                            target_type: Some("nuova colonna nullable".to_owned()),
                        });
                    } else {
                        losses.push(mapping_loss(
                            field_id,
                            LossCategory::NativeType,
                            "colonna sorgente assente nel target",
                            Some(source.data_type().to_string()),
                            None,
                        )?);
                    }
                    continue;
                };
                let target_field = target.arrow_field();
                if source.data_type() != target_field.data_type() {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::NativeType,
                        "tipo Arrow non equivalente al target PostgreSQL",
                        Some(source.data_type().to_string()),
                        Some(target_field.data_type().to_string()),
                    )?);
                }
                if source.is_nullable() && !target.nullable {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::Nullability,
                        "sorgente nullable verso colonna NOT NULL",
                        None,
                        None,
                    )?);
                }
                let source_srid = source
                    .metadata()
                    .get(protocol::GEOMETRY_SRID)
                    .or_else(|| source.metadata().get("plenora.srid"));
                let target_srid = target_field
                    .metadata()
                    .get(protocol::GEOMETRY_SRID)
                    .or_else(|| target_field.metadata().get("plenora.srid"));
                if source_srid != target_srid && (source_srid.is_some() || target_srid.is_some()) {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::Srid,
                        "SRID sorgente e target differenti",
                        source_srid.cloned(),
                        target_srid.cloned(),
                    )?);
                }
                let source_crs = source.metadata().get(protocol::GEOMETRY_CRS_ID);
                let target_crs = target_field.metadata().get(protocol::GEOMETRY_CRS_ID);
                if source_crs != target_crs && (source_crs.is_some() || target_crs.is_some()) {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::Crs,
                        "identificatore CRS sorgente e target differente",
                        source_crs.cloned(),
                        target_crs.cloned(),
                    )?);
                }
            }
            for key in &operation.keys {
                if !target_columns.iter().any(|column| &column.name == key) {
                    return Err(DatabaseError::invalid_plan(
                        "chiave write assente nel target PostgreSQL",
                    ));
                }
            }
        }
        let report = LossReport {
            schema_version: 1,
            policy: operation.mapping_policy,
            losses,
        };
        if !report.permits_execution() {
            return Err(public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                false,
                "preflight PostgreSQL rileva conversioni non ammesse",
            ));
        }
        drop(client);
        Ok(report)
    }
}

async fn validate_resolved_crs_against_postgis(
    client: &PooledClient,
    input_schema: &SchemaRef,
) -> Result<()> {
    for field in input_schema.fields() {
        if field
            .metadata()
            .get(protocol::GEOMETRY_CRS_RESOLUTION)
            .is_none_or(|value| value != "resolved")
        {
            continue;
        }
        let srid = field
            .metadata()
            .get(protocol::GEOMETRY_SRID)
            .and_then(|value| value.parse::<i32>().ok())
            .ok_or_else(|| DatabaseError::invalid_plan("CRS resolved senza SRID valido"))?;
        let declared_id = field
            .metadata()
            .get(protocol::GEOMETRY_CRS_ID)
            .ok_or_else(|| DatabaseError::invalid_plan("CRS resolved senza identificatore"))?;
        let row = client
            .client()?
            .query_opt(
                "SELECT auth_name, auth_srid
                 FROM spatial_ref_sys
                 WHERE srid = $1",
                &[&srid],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Prepare,
                    false,
                    "SRID resolved assente da spatial_ref_sys",
                )
            })?;
        let authority: Option<String> = row.get(0);
        let authority_code: Option<i32> = row.get(1);
        let observed_id = authority
            .filter(|value| !value.is_empty())
            .zip(authority_code)
            .map(|(name, code)| format!("{name}:{code}"))
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Prepare,
                    false,
                    "spatial_ref_sys non risolve un authority ID",
                )
            })?;
        if !declared_id.eq_ignore_ascii_case(&observed_id) {
            return Err(public_error(
                ErrorCategory::Crs,
                ErrorPhase::Prepare,
                false,
                "identificatore CRS non coerente con spatial_ref_sys",
            ));
        }
    }
    Ok(())
}

fn mapping_loss(
    field_id: usize,
    category: LossCategory,
    reason: &str,
    source_type: Option<String>,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    Ok(MappingLoss {
        field_id: u32::try_from(field_id).map_err(|_| {
            DatabaseError::invalid_plan("numero colonne oltre il contratto LossReport")
        })?,
        category,
        severity: LossSeverity::DataLoss,
        reason: reason.to_owned(),
        source_type,
        target_type,
    })
}

const fn supports_additive_evolution(mode: plenora_database_core::plan::WriteMode) -> bool {
    matches!(
        mode,
        plenora_database_core::plan::WriteMode::Append
            | plenora_database_core::plan::WriteMode::TruncateInsert
            | plenora_database_core::plan::WriteMode::Update
            | plenora_database_core::plan::WriteMode::Upsert
    )
}

impl Provider for PostgresProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Connect)?;
            let client = self.connect_session(secret).await?;
            let row = client
                .client()?
                .query_one(
                    "SELECT current_setting('server_version'), current_database(), current_user",
                    &[],
                )
                .await
                .map_err(|error| classify_error(ErrorPhase::Connect, &error))?;
            let info = ConnectionInfo {
                provider: ProviderKind::Postgres,
                server_version: row.get(0),
                connection_identity: Some(format!(
                    "{}/{}",
                    row.get::<_, String>(1),
                    row.get::<_, String>(2)
                )),
            };
            drop(client);
            Ok(info)
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Probe)?;
            let client = self.connect_session(secret).await?;
            Self::capability_document(client.client()?).await
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Probe)?;
            let client = self.connect_session(secret).await?;
            self.inspection(&client, operation).await
        })
    }

    fn read<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            let control = BudgetCancellation::new(cancellation, budget)?;
            self.read_stream(secret, operation, parameters, budget, control.token())
                .await
        })
    }

    fn query<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a QueryOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            let control = BudgetCancellation::new(cancellation, budget)?;
            self.query_stream(secret, operation, parameters, budget, control.token())
                .await
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            let control = BudgetCancellation::new(cancellation, budget)?;
            check_cancelled(control.token(), ErrorPhase::Prepare)?;
            write::validate_schema(&input_schema, operation)?;
            let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
            let column_count = u64::try_from(input_schema.fields().len())
                .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
            let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
            let loss_report = self
                .preflight_write(secret, operation, &input_schema)
                .await?;
            Ok(PreparedWrite {
                operation: operation.clone(),
                loss_report,
                budget: budget.clone(),
                operation_lease,
                columns_lease,
            })
        })
    }

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async move {
            if !prepared.budget.is_same_budget(budget) {
                return Err(DatabaseError::invalid_plan(
                    "il budget di write non coincide con quello usato in prepare_write",
                ));
            }
            let control = BudgetCancellation::new(cancellation, budget)?;
            let target = prepared.operation.target.clone();
            let result = write::execute(
                secret,
                prepared,
                input,
                budget,
                control.token(),
                write::WriteRuntime {
                    statement_timeout_ms: self.statement_timeout_ms,
                    lock_timeout_ms: self.lock_timeout_ms,
                    fault_point: self.fault_point,
                    insert_mode: self.insert_mode,
                    max_batch_bytes: self.max_batch_bytes,
                    max_wkb_cell_bytes: self.max_wkb_cell_bytes,
                    tls_mode: self.tls_mode,
                    tls_config: self.tls_config.clone(),
                    network_options: self.network_options,
                    schema_evolution: self.schema_evolution,
                    pool: Arc::clone(&self.pool),
                    metrics: Arc::clone(&self.metrics),
                    pool_acquire_timeout_ms: self.pool_acquire_timeout_ms,
                },
            )
            .await;
            if result.is_ok() {
                self.invalidate_cached_schema(secret, &target);
            }
            result
        })
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn select_columns(available: &[ColumnSpec], projection: &[String]) -> Result<Vec<ColumnSpec>> {
    if projection.is_empty() {
        return Ok(available.to_vec());
    }
    let by_name = available
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();
    projection
        .iter()
        .map(|name| {
            by_name
                .get(name.as_str())
                .map(|column| (*column).clone())
                .ok_or_else(|| {
                    public_error(
                        ErrorCategory::NotFound,
                        ErrorPhase::Prepare,
                        false,
                        "colonna PostgreSQL richiesta non trovata",
                    )
                })
        })
        .collect()
}

fn mark_query_spatial_columns(operation: &QueryOperation, columns: &mut [ColumnSpec]) {
    for (column, projection) in columns.iter_mut().zip(&operation.projection) {
        if matches!(
            projection.expression,
            QueryExpression::Spatial { function, .. } if function.returns_geometry()
        ) {
            column.kind = ColumnKind::Geometry;
            "geometry".clone_into(&mut column.native_type);
            column.spatial_type = Some("Geometry".to_owned());
        }
    }
}

fn build_read_sql(
    operation: &ReadOperation,
    columns: &[ColumnSpec],
    available_columns: &[ColumnSpec],
) -> Result<(String, Vec<String>)> {
    let renderer = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    );
    let source = ObjectName {
        // PostgreSQL non supporta nomi cross-database a tre componenti. Il
        // catalogo è verificato contro current_database prima del rendering.
        catalog: None,
        schema: operation
            .source
            .schema
            .as_ref()
            .map(|value| Identifier::new(value.clone()))
            .transpose()?,
        object: Identifier::new(operation.source.object.clone())?,
    };
    let projection = columns
        .iter()
        .map(|column| column.projection_sql(&renderer))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let mut sql = format!(
        "SELECT {projection} FROM {}",
        renderer.quote_object(&source)
    );
    let mut bind_names = Vec::new();
    if let Some(filter) = &operation.filter {
        ensure_filter_columns(filter, available_columns)?;
        let rendered_filter = renderer.render_filter(&convert_filter(filter)?)?;
        sql.push_str(" WHERE ");
        sql.push_str(&rendered_filter.sql);
        bind_names.extend(rendered_filter.binds.into_iter().map(|bind| bind.name));
    }
    if !operation.order_by.is_empty() {
        let available = available_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut orders = Vec::with_capacity(operation.order_by.len());
        for order in &operation.order_by {
            if !available.contains(order.field.as_str()) {
                return Err(public_error(
                    ErrorCategory::NotFound,
                    ErrorPhase::Prepare,
                    false,
                    "colonna ORDER BY non presente nella projection",
                ));
            }
            let quoted = renderer.quote_identifier(&Identifier::new(order.field.clone())?);
            let direction = match order.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            orders.push(format!("{quoted} {direction}"));
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&orders.join(", "));
    }
    if let Some(limit) = operation.row_limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }
    Ok((sql, bind_names))
}

const fn postgres_renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}

fn convert_filter(expression: &FilterExpression) -> Result<Expression> {
    match expression {
        FilterExpression::And { args } => Ok(Expression::And(
            args.iter()
                .map(convert_filter)
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Or { args } => Ok(Expression::Or(
            args.iter()
                .map(convert_filter)
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Eq { field, parameter } => comparison(
            field,
            plenora_database_core::plan::ComparisonOperator::Eq,
            parameter,
        ),
        FilterExpression::Ne { field, parameter } => comparison(
            field,
            plenora_database_core::plan::ComparisonOperator::Ne,
            parameter,
        ),
        FilterExpression::Lt { field, parameter } => comparison(
            field,
            plenora_database_core::plan::ComparisonOperator::Lt,
            parameter,
        ),
        FilterExpression::Lte { field, parameter } => comparison(
            field,
            plenora_database_core::plan::ComparisonOperator::Lte,
            parameter,
        ),
        FilterExpression::Gt { field, parameter } => comparison(
            field,
            plenora_database_core::plan::ComparisonOperator::Gt,
            parameter,
        ),
        FilterExpression::Gte { field, parameter } => comparison(
            field,
            plenora_database_core::plan::ComparisonOperator::Gte,
            parameter,
        ),
        FilterExpression::IsNull { field } => {
            Ok(Expression::IsNull(Identifier::new(field.clone())?))
        }
        FilterExpression::IsNotNull { field } => {
            Ok(Expression::IsNotNull(Identifier::new(field.clone())?))
        }
        FilterExpression::In { field, parameters } => Ok(Expression::In {
            field: Identifier::new(field.clone())?,
            parameters: parameters.clone(),
        }),
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => Ok(Expression::Between {
            field: Identifier::new(field.clone())?,
            lower_parameter: lower_parameter.clone(),
            upper_parameter: upper_parameter.clone(),
        }),
        FilterExpression::Like {
            field,
            parameter,
            case_insensitive,
        } => Ok(Expression::Like {
            field: Identifier::new(field.clone())?,
            parameter: parameter.clone(),
            case_insensitive: *case_insensitive,
        }),
        FilterExpression::Spatial {
            function,
            field,
            geometry_parameter,
            distance_parameter,
        } => Ok(Expression::SpatialPredicate {
            function: *function,
            field: Identifier::new(field.clone())?,
            geometry_parameter: geometry_parameter.clone(),
            distance_parameter: distance_parameter.clone(),
        }),
    }
}

fn comparison(
    field: &str,
    operator: plenora_database_core::plan::ComparisonOperator,
    parameter: &str,
) -> Result<Expression> {
    Ok(Expression::Compare {
        field: Identifier::new(field.to_owned())?,
        operator,
        parameter: parameter.to_owned(),
    })
}

fn ensure_filter_columns(expression: &FilterExpression, columns: &[ColumnSpec]) -> Result<()> {
    fn visit(expression: &FilterExpression, available: &std::collections::BTreeSet<&str>) -> bool {
        match expression {
            FilterExpression::And { args } | FilterExpression::Or { args } => {
                args.iter().all(|arg| visit(arg, available))
            }
            FilterExpression::Eq { field, .. }
            | FilterExpression::Ne { field, .. }
            | FilterExpression::Lt { field, .. }
            | FilterExpression::Lte { field, .. }
            | FilterExpression::Gt { field, .. }
            | FilterExpression::Gte { field, .. }
            | FilterExpression::IsNull { field }
            | FilterExpression::IsNotNull { field }
            | FilterExpression::In { field, .. }
            | FilterExpression::Between { field, .. }
            | FilterExpression::Like { field, .. }
            | FilterExpression::Spatial { field, .. } => available.contains(field.as_str()),
        }
    }
    let available = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if visit(expression, &available) {
        Ok(())
    } else {
        Err(public_error(
            ErrorCategory::NotFound,
            ErrorPhase::Prepare,
            false,
            "colonna filtro non presente nella projection",
        ))
    }
}

fn parse_decimal128(value: &str, scale: i8) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches(['-', '+']);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(if integer.is_empty() { "0" } else { integer });
    digits.push_str(fraction);
    let mut parsed = digits.parse::<i128>().map_err(|_| {
        public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            false,
            "decimal oltre Decimal128",
        )
    })?;
    let exponent = i32::from(scale)
        - i32::try_from(fraction.len()).map_err(|_| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "scala decimal non valida",
            )
        })?;
    if exponent >= 0 {
        let factor = 10_i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    false,
                    "decimal oltre Decimal128",
                )
            })?;
        parsed = parsed.checked_mul(factor).ok_or_else(|| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "decimal oltre Decimal128",
            )
        })?;
    } else {
        let divisor = 10_i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    false,
                    "scala decimal non valida",
                )
            })?;
        if parsed % divisor != 0 {
            return Err(public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "decimal non rappresentabile alla scala Arrow dichiarata",
            ));
        }
        parsed /= divisor;
    }
    Ok(if negative { -parsed } else { parsed })
}

#[cfg(test)]
mod test_suite;
