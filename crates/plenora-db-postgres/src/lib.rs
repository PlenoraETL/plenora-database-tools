//! Driver pilota PostgreSQL/PostGIS.
//!
//! Il driver di riferimento copre connessione, capability, introspezione, read
//! batch-bounded verso Arrow/GeoArrow-WKB e tutte le modalità di scrittura
//! definite dal core.

mod metrics;
mod write;

pub use metrics::PostgresMetricsSnapshot;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, IntervalMonthDayNanoBuilder, ListBuilder,
    StringBuilder, StructBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::IntervalMonthDayNano;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use bytes::{Buf, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use futures_util::{Stream, StreamExt};
use metrics::PostgresMetrics;
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::{Dimensions, GEOARROW_WKB_EXTENSION_NAME};
use plenora_database_core::loss::{LossCategory, LossReport, LossSeverity, MappingLoss};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, ProviderKind, ReadOperation, SortDirection,
    WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, Cancellation, ConnectionInfo, Inspection, ParameterBag, ParameterValue,
    PreparedWrite, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::query::{QueryExpression, QueryOperation, SpatialFunction};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    Dialect, DialectCapabilities, Expression, Identifier, ObjectName, Renderer,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use tokio_postgres::{CancelToken, Client, Config, NoTls, Row, RowStream};
use tokio_postgres_rustls::MakeRustlsConnect;

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
    /// # Panics
    ///
    /// Solo se la configurazione statica `WebPKI` incorporata da Rustls non
    /// può essere costruita, condizione che viola un'invariante del crate.
    #[must_use]
    pub fn webpki() -> Self {
        Self::build(true, &[], None, None).expect("WebPKI TLS configuration")
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

#[derive(Clone)]
struct CatalogSchemaToken {
    public: PostgresSchemaToken,
    exact_signature: String,
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
        self.state
            .lock()
            .expect("schema cache lock")
            .entries
            .get(key)
            .cloned()
    }

    fn touch(&self, key: &SchemaCacheKey) {
        let mut state = self.state.lock().expect("schema cache lock");
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
        let mut state = self.state.lock().expect("schema cache lock");
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
        self.state
            .lock()
            .expect("schema cache lock")
            .entries
            .remove(key)
            .is_some()
    }

    fn len(&self) -> usize {
        self.state.lock().map_or(0, |state| state.entries.len())
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
        let mut client = self
            .idle
            .lock()
            .expect("pool lock")
            .get_mut(&key)
            .and_then(Vec::pop);
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
}

impl Deref for PooledClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("pooled client available")
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("pooled client available")
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
        let mut idle = self.pool.idle.lock().expect("pool lock");
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
    .expect("writing session options to String");
    config
        .options(options)
        .application_name("plenora-database-tools");
    Ok(())
}

impl CatalogSchemaToken {
    fn from_catalog_row(row: &Row, offset: usize) -> Result<Self> {
        let database_oid = catalog_oid(row.get::<_, i64>(offset))?;
        let namespace_oid = catalog_oid(row.get::<_, i64>(offset + 1))?;
        let relation_oid = catalog_oid(row.get::<_, i64>(offset + 2))?;
        let exact_signature = row.get::<_, String>(offset + 3);
        let digest = Sha256::digest(exact_signature.as_bytes());
        let mut structural_fingerprint = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(structural_fingerprint, "{byte:02x}")
                .expect("writing schema fingerprint to String");
        }
        Ok(Self {
            public: PostgresSchemaToken {
                schema_version: 1,
                database_oid,
                namespace_oid,
                relation_oid,
                structural_fingerprint,
            },
            exact_signature,
        })
    }

    fn structurally_equals(&self, other: &Self) -> bool {
        self.public.database_oid == other.public.database_oid
            && self.public.namespace_oid == other.public.namespace_oid
            && self.public.relation_oid == other.public.relation_oid
            && self.exact_signature == other.exact_signature
    }
}

fn catalog_oid(value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Probe,
            false,
            "OID catalogo PostgreSQL fuori intervallo",
        )
    })
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
        self.pool
            .idle
            .lock()
            .map_or(0, |idle| idle.values().map(Vec::len).sum())
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
            if let Err(error) = client.batch_execute("DISCARD ALL").await {
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
                    signature.structural_signature
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_database d ON d.datname = current_database()
                JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                LEFT JOIN pg_catalog.pg_attrdef ad
                  ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
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
                                    sa.xmin::text,
                                    st.typname,
                                    st.typtype,
                                    st.xmin::text,
                                    pg_get_expr(sad.adbin, sad.adrelid),
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
                                    ) END
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
        let token = CatalogSchemaToken::from_catalog_row(&rows[0], 14)?;
        let columns = Arc::new(rows.iter().map(ColumnSpec::from_catalog_row).collect());
        Ok((columns, token))
    }

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
                                    a.xmin::text,
                                    t.typname,
                                    t.typtype,
                                    t.xmin::text,
                                    pg_get_expr(ad.adbin, ad.adrelid),
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
                                    ) END
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
        CatalogSchemaToken::from_catalog_row(&row, 0)
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
            let current = match Self::schema_token(client, source).await {
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
        let (columns, token) = Self::load_columns_and_token(client, source).await?;
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
                pagination: false,
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
                let metadata = describe_object_metadata(client, source).await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!({
                        "columns": columns.as_ref(),
                        "schema_token": schema_token,
                        "relation": metadata.relation,
                        "constraints": metadata.constraints,
                        "indexes": metadata.indexes
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
        cancellation: &dyn Cancellation,
    ) -> Result<RowStream> {
        let parameter_count = parameter_refs.len();
        let (result, error_phase) = if let Some(parameter_types) = parameter_types {
            let query = client.query_typed_raw(
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
                .prepare(sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
            (
                select_with_cancellation(
                    client.query_raw(&statement, parameter_refs),
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
        Err(cancelled_read_error())
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn read_stream(
        &self,
        secret: &SecretString,
        operation: &ReadOperation,
        parameters: &ParameterBag,
        cancellation: &dyn Cancellation,
    ) -> Result<Box<dyn BatchStream>> {
        if cancellation.is_cancelled() {
            self.metrics.cancellation();
            return Err(cancelled_read_error());
        }
        if self.batch_rows == 0 || self.max_batch_bytes == 0 || self.target_batch_bytes == Some(0) {
            return Err(DatabaseError::invalid_plan(
                "batch_rows e budget byte PostgreSQL devono essere maggiori di zero",
            ));
        }
        let mut client = if let Some(result) =
            select_with_cancellation(self.connect_session(secret), cancellation).await
        {
            result?
        } else {
            self.metrics.cancellation();
            return Err(cancelled_read_error());
        };
        if let Some(catalog) = &operation.source.catalog {
            let current_database: String = client
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
        let cancel_token = client.cancel_token();
        let schema = Arc::new(Schema::new(
            selected
                .iter()
                .map(ColumnSpec::arrow_field)
                .collect::<Vec<_>>(),
        ));
        Ok(Box::new(PostgresBatchStream {
            client,
            cancel_token,
            tls_mode: self.tls_mode,
            tls_connector: self.tls_config.connector.clone(),
            cancel_timeout_ms: self.network_options.connect_timeout_ms,
            rows: Box::pin(rows),
            columns: selected,
            schema,
            batch_rows: self.batch_rows,
            target_batch_bytes: self
                .target_batch_bytes
                .map(|target| target.min(self.max_batch_bytes)),
            max_batch_bytes: self.max_batch_bytes,
            max_wkb_cell_bytes: self.max_wkb_cell_bytes,
            metrics: Arc::clone(&self.metrics),
            byte_estimate_scale_permille: 1_500,
            track_byte_estimate: true,
            batches_since_byte_estimate: 0,
            finished: false,
        }))
    }

    #[allow(clippy::significant_drop_tightening)]
    #[allow(clippy::too_many_lines)]
    async fn query_stream(
        &self,
        secret: &SecretString,
        operation: &QueryOperation,
        parameters: &ParameterBag,
        cancellation: &dyn Cancellation,
    ) -> Result<Box<dyn BatchStream>> {
        if cancellation.is_cancelled() {
            self.metrics.cancellation();
            return Err(cancelled_read_error());
        }
        if self.batch_rows == 0 || self.max_batch_bytes == 0 || self.target_batch_bytes == Some(0) {
            return Err(DatabaseError::invalid_plan(
                "batch_rows e budget byte PostgreSQL devono essere maggiori di zero",
            ));
        }
        let rendered = postgres_renderer().render_query(operation)?;
        let mut client = if let Some(result) =
            select_with_cancellation(self.connect_session(secret), cancellation).await
        {
            result?
        } else {
            self.metrics.cancellation();
            return Err(cancelled_read_error());
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
                client.query_typed_raw(&rendered.sql, typed_parameters),
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
                return Err(cancelled_read_error());
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
                return Err(cancelled_read_error());
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
                .prepare(&rendered.sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
            let mut columns = statement
                .columns()
                .iter()
                .map(ColumnSpec::from_statement_column)
                .collect::<Result<Vec<_>>>()?;
            mark_query_spatial_columns(operation, &mut columns);
            let rows = if let Some(result) =
                select_with_cancellation(client.query_raw(&statement, parameter_refs), cancellation)
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
                return Err(cancelled_read_error());
            };
            (Box::pin(rows), columns)
        };
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(ColumnSpec::arrow_field)
                .collect::<Vec<_>>(),
        ));
        let cancel_token = client.cancel_token();
        Ok(Box::new(PostgresBatchStream {
            client,
            cancel_token,
            tls_mode: self.tls_mode,
            tls_connector: self.tls_config.connector.clone(),
            cancel_timeout_ms: self.network_options.connect_timeout_ms,
            rows,
            columns,
            schema,
            batch_rows: self.batch_rows,
            target_batch_bytes: self
                .target_batch_bytes
                .map(|target| target.min(self.max_batch_bytes)),
            max_batch_bytes: self.max_batch_bytes,
            max_wkb_cell_bytes: self.max_wkb_cell_bytes,
            metrics: Arc::clone(&self.metrics),
            byte_estimate_scale_permille: 1_500,
            track_byte_estimate: true,
            batches_since_byte_estimate: 0,
            finished: false,
        }))
    }

    #[allow(clippy::too_many_lines)]
    async fn preflight_write(
        &self,
        secret: &SecretString,
        operation: &WriteOperation,
        input_schema: &SchemaRef,
    ) -> Result<LossReport> {
        let client = self.connect_session(secret).await?;
        let schema_name = operation.target.schema.as_deref().unwrap_or("public");
        let exists: bool = client
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
                let source_srid = source.metadata().get("plenora.srid");
                let target_srid = target_field.metadata().get("plenora.srid");
                if source_srid != target_srid && (source_srid.is_some() || target_srid.is_some()) {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::Srid,
                        "SRID sorgente e target differenti",
                        source_srid.cloned(),
                        target_srid.cloned(),
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
        cancellation: &'a dyn Cancellation,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Connect)?;
            let client = self.connect_session(secret).await?;
            let row = client
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
        cancellation: &'a dyn Cancellation,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Probe)?;
            let client = self.connect_session(secret).await?;
            Self::capability_document(&client).await
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a dyn Cancellation,
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
        cancellation: &'a dyn Cancellation,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            self.read_stream(secret, operation, parameters, cancellation)
                .await
        })
    }

    fn query<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a QueryOperation,
        parameters: &'a ParameterBag,
        cancellation: &'a dyn Cancellation,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            self.query_stream(secret, operation, parameters, cancellation)
                .await
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        cancellation: &'a dyn Cancellation,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Prepare)?;
            write::validate_schema(&input_schema, operation)?;
            let loss_report = self
                .preflight_write(secret, operation, &input_schema)
                .await?;
            Ok(PreparedWrite {
                operation: operation.clone(),
                loss_report,
            })
        })
    }

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        cancellation: &'a dyn Cancellation,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async move {
            let target = prepared.operation.target.clone();
            let result = write::execute(
                secret,
                prepared,
                input,
                cancellation,
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

struct ObjectMetadata {
    relation: serde_json::Value,
    constraints: Vec<serde_json::Value>,
    indexes: Vec<serde_json::Value>,
}

async fn describe_object_metadata(client: &Client, source: &ObjectRef) -> Result<ObjectMetadata> {
    let schema = source.schema.as_deref().unwrap_or("public");
    let relation = client
        .query_one(
            r"
            SELECT
                c.relkind::text,
                c.relispartition,
                pg_get_partkeydef(c.oid),
                CASE WHEN c.relkind IN ('v', 'm') THEN pg_get_viewdef(c.oid, true) END,
                obj_description(c.oid, 'pg_class')
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let relation_document = json!({
        "kind": relation_kind(&relation.get::<_, String>(0)),
        "is_partition": relation.get::<_, bool>(1),
        "partition_key": relation.get::<_, Option<String>>(2),
        "view_definition": relation.get::<_, Option<String>>(3),
        "comment": relation.get::<_, Option<String>>(4)
    });
    let constraint_rows = client
        .query(
            r"
            SELECT
                con.conname,
                con.contype::text,
                pg_get_constraintdef(con.oid, true),
                con.convalidated,
                con.condeferrable,
                con.condeferred
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
            ORDER BY con.conname
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let constraints = constraint_rows
        .iter()
        .map(|row| {
            json!({
                "name": row.get::<_, String>(0),
                "kind": constraint_kind(&row.get::<_, String>(1)),
                "definition": row.get::<_, String>(2),
                "validated": row.get::<_, bool>(3),
                "deferrable": row.get::<_, bool>(4),
                "initially_deferred": row.get::<_, bool>(5)
            })
        })
        .collect();
    let index_rows = client
        .query(
            r"
            SELECT
                i.relname,
                ix.indisprimary,
                ix.indisunique,
                ix.indisvalid,
                am.amname,
                pg_get_indexdef(i.oid)
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_am am ON am.oid = i.relam
            WHERE n.nspname = $1 AND t.relname = $2
            ORDER BY i.relname
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let indexes = index_rows
        .iter()
        .map(|row| {
            json!({
                "name": row.get::<_, String>(0),
                "primary": row.get::<_, bool>(1),
                "unique": row.get::<_, bool>(2),
                "valid": row.get::<_, bool>(3),
                "method": row.get::<_, String>(4),
                "definition": row.get::<_, String>(5)
            })
        })
        .collect();
    Ok(ObjectMetadata {
        relation: relation_document,
        constraints,
        indexes,
    })
}

fn relation_kind(kind: &str) -> &'static str {
    match kind {
        "r" => "table",
        "p" => "partitioned_table",
        "v" => "view",
        "m" => "materialized_view",
        "f" => "foreign_table",
        _ => "other",
    }
}

fn constraint_kind(kind: &str) -> &'static str {
    match kind {
        "p" => "primary_key",
        "u" => "unique",
        "f" => "foreign_key",
        "c" => "check",
        "x" => "exclusion",
        _ => "other",
    }
}

#[derive(Debug, Clone, Serialize)]
struct ColumnSpec {
    name: String,
    native_type: String,
    nullable: bool,
    numeric_precision: Option<u8>,
    numeric_scale: Option<i8>,
    spatial_srid: Option<u32>,
    spatial_dimensions: Option<String>,
    spatial_type: Option<String>,
    default_expression: Option<String>,
    identity_kind: Option<String>,
    generated_kind: Option<String>,
    native_declaration: Option<String>,
    type_kind: Option<String>,
    composite_fields: Vec<CompositeFieldSpec>,
    #[serde(skip)]
    kind: ColumnKind,
}

#[derive(Debug, Clone, Copy)]
enum ColumnKind {
    Bool,
    I32,
    I64,
    F32,
    F64,
    Utf8,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    Interval,
    Range,
    Composite,
    Decimal { precision: u8, scale: i8 },
    BoolArray,
    I32Array,
    I64Array,
    F32Array,
    F64Array,
    Utf8Array,
    Geometry,
    Geography,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompositeFieldSpec {
    name: String,
    declaration: String,
}

impl ColumnSpec {
    fn from_statement_column(column: &tokio_postgres::Column) -> Result<Self> {
        let native_type = column.type_().name().to_owned();
        let kind = match native_type.as_str() {
            "bool" => ColumnKind::Bool,
            "int2" | "int4" => ColumnKind::I32,
            "int8" => ColumnKind::I64,
            "float4" => ColumnKind::F32,
            "float8" => ColumnKind::F64,
            "bytea" => ColumnKind::Binary,
            "date" => ColumnKind::Date,
            "time" => ColumnKind::Time,
            "interval" => ColumnKind::Interval,
            "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
                ColumnKind::Range
            }
            "timestamp" => ColumnKind::Timestamp,
            "timestamptz" => ColumnKind::TimestampTz,
            "text" | "varchar" | "bpchar" | "name" | "json" | "jsonb" | "uuid" => ColumnKind::Utf8,
            "_bool" => ColumnKind::BoolArray,
            "_int2" | "_int4" => ColumnKind::I32Array,
            "_int8" => ColumnKind::I64Array,
            "_float4" => ColumnKind::F32Array,
            "_float8" => ColumnKind::F64Array,
            "_text" | "_varchar" | "_bpchar" => ColumnKind::Utf8Array,
            "geometry" | "geography" => {
                return Err(DatabaseError::unsupported(
                    ProviderKind::Postgres,
                    ErrorPhase::Prepare,
                    "projection geometry raw: usare una funzione spatial che produca EWKB",
                ));
            }
            _ => {
                return Err(DatabaseError::unsupported(
                    ProviderKind::Postgres,
                    ErrorPhase::Prepare,
                    "tipo risultato query PostgreSQL non ancora mappato",
                ));
            }
        };
        Ok(Self {
            name: column.name().to_owned(),
            native_type,
            nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            spatial_srid: None,
            spatial_dimensions: None,
            spatial_type: None,
            default_expression: None,
            identity_kind: None,
            generated_kind: None,
            native_declaration: None,
            type_kind: None,
            composite_fields: Vec::new(),
            kind,
        })
    }

    fn from_catalog_row(row: &Row) -> Self {
        let name: String = row.get(0);
        let native_type: String = row.get(1);
        let nullable: bool = row.get(2);
        let precision_i32: Option<i32> = row.get(3);
        let scale_i32: Option<i32> = row.get(4);
        let srid_i32: Option<i32> = row.get(5);
        let spatial_dimensions: Option<i32> = row.get(6);
        let spatial_type: Option<String> = row.get(7);
        let default_expression: Option<String> = row.get(8);
        let identity_kind: Option<String> = row.get(9);
        let generated_kind: Option<String> = row.get(10);
        let native_declaration: Option<String> = row.get(11);
        let type_kind: Option<String> = row.get(12);
        let composite_fields = row
            .get::<_, Option<String>>(13)
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default();
        let numeric_precision = precision_i32.and_then(|value| u8::try_from(value).ok());
        let numeric_scale = scale_i32.and_then(|value| i8::try_from(value).ok());
        let kind = if type_kind.as_deref() == Some("c") {
            ColumnKind::Composite
        } else {
            match native_type.as_str() {
                "bool" => ColumnKind::Bool,
                "int2" | "int4" => ColumnKind::I32,
                "int8" => ColumnKind::I64,
                "float4" => ColumnKind::F32,
                "float8" => ColumnKind::F64,
                "bytea" => ColumnKind::Binary,
                "date" => ColumnKind::Date,
                "time" => ColumnKind::Time,
                "interval" => ColumnKind::Interval,
                "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
                    ColumnKind::Range
                }
                "timestamp" => ColumnKind::Timestamp,
                "timestamptz" => ColumnKind::TimestampTz,
                "numeric" => ColumnKind::Decimal {
                    precision: numeric_precision.unwrap_or(38),
                    scale: numeric_scale.unwrap_or(18),
                },
                "_bool" => ColumnKind::BoolArray,
                "_int2" | "_int4" => ColumnKind::I32Array,
                "_int8" => ColumnKind::I64Array,
                "_float4" => ColumnKind::F32Array,
                "_float8" => ColumnKind::F64Array,
                "_text" | "_varchar" | "_bpchar" => ColumnKind::Utf8Array,
                "geometry" => ColumnKind::Geometry,
                "geography" => ColumnKind::Geography,
                _ => ColumnKind::Utf8,
            }
        };
        Self {
            name,
            native_type,
            nullable,
            numeric_precision,
            numeric_scale,
            spatial_srid: srid_i32.and_then(|value| u32::try_from(value).ok()),
            spatial_dimensions: spatial_dimensions.map(|value| value.to_string()),
            spatial_type,
            default_expression,
            identity_kind,
            generated_kind,
            native_declaration,
            type_kind,
            composite_fields,
            kind,
        }
    }

    fn arrow_field(&self) -> Field {
        let data_type = match self.kind {
            ColumnKind::Bool => DataType::Boolean,
            ColumnKind::I32 => DataType::Int32,
            ColumnKind::I64 => DataType::Int64,
            ColumnKind::F32 => DataType::Float32,
            ColumnKind::F64 => DataType::Float64,
            ColumnKind::Utf8 => DataType::Utf8,
            ColumnKind::Binary | ColumnKind::Geometry | ColumnKind::Geography => DataType::Binary,
            ColumnKind::Date => DataType::Date32,
            ColumnKind::Time => DataType::Time64(TimeUnit::Microsecond),
            ColumnKind::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            ColumnKind::Range => DataType::Struct(range_fields().into()),
            ColumnKind::Composite => DataType::Struct(
                self.composite_fields
                    .iter()
                    .map(|item| {
                        let mut metadata = HashMap::new();
                        metadata.insert(
                            "plenora.native_declaration".to_owned(),
                            item.declaration.clone(),
                        );
                        Arc::new(
                            Field::new(&item.name, DataType::Utf8, true).with_metadata(metadata),
                        )
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnKind::TimestampTz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            ColumnKind::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
            ColumnKind::BoolArray => {
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)))
            }
            ColumnKind::I32Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
            }
            ColumnKind::I64Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
            }
            ColumnKind::F32Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
            }
            ColumnKind::F64Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
            }
            ColumnKind::Utf8Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
        };
        let mut metadata = HashMap::new();
        metadata.insert("plenora.native_type".to_owned(), self.native_type.clone());
        if let Some(declaration) = &self.native_declaration {
            metadata.insert("plenora.native_declaration".to_owned(), declaration.clone());
        }
        if let Some(type_kind) = &self.type_kind {
            metadata.insert("plenora.postgres_type_kind".to_owned(), type_kind.clone());
        }
        if matches!(self.kind, ColumnKind::Geometry | ColumnKind::Geography) {
            metadata.insert(
                "ARROW:extension:name".to_owned(),
                GEOARROW_WKB_EXTENSION_NAME.to_owned(),
            );
            metadata.insert(
                "plenora.spatial_semantics".to_owned(),
                if matches!(self.kind, ColumnKind::Geography) {
                    "geography"
                } else {
                    "geometry"
                }
                .to_owned(),
            );
            if let Some(srid) = self.spatial_srid {
                metadata.insert("plenora.srid".to_owned(), srid.to_string());
            }
            if let Some(dimensions) = &self.spatial_dimensions {
                metadata.insert("plenora.dimensions".to_owned(), dimensions.clone());
            }
            if let Some(spatial_type) = &self.spatial_type {
                metadata.insert("plenora.geometry_type".to_owned(), spatial_type.clone());
            }
        }
        Field::new(&self.name, data_type, self.nullable).with_metadata(metadata)
    }

    fn projection_sql(&self, renderer: &Renderer) -> Result<String> {
        let identifier = Identifier::new(self.name.clone())?;
        let quoted = renderer.quote_identifier(&identifier);
        let expression = match self.kind {
            ColumnKind::Decimal { .. } => format!("{quoted}::text"),
            ColumnKind::Range => format!(
                "CASE WHEN {quoted} IS NULL THEN NULL \
                 WHEN isempty({quoted}) THEN \
                   jsonb_build_object('lower', NULL, 'upper', NULL, \
                     'lower_inclusive', false, 'upper_inclusive', false, \
                     'lower_unbounded', false, 'upper_unbounded', false, \
                     'empty', true)::text \
                 ELSE jsonb_build_object(\
                     'lower', lower({quoted})::text, \
                     'upper', upper({quoted})::text, \
                     'lower_inclusive', lower_inc({quoted}), \
                     'upper_inclusive', upper_inc({quoted}), \
                     'lower_unbounded', lower_inf({quoted}), \
                     'upper_unbounded', upper_inf({quoted}), \
                     'empty', false)::text END"
            ),
            ColumnKind::Composite => format!("to_jsonb({quoted})::text"),
            ColumnKind::Utf8 if matches!(self.native_type.as_str(), "json" | "jsonb" | "uuid") => {
                format!("{quoted}::text")
            }
            ColumnKind::Utf8
                if !matches!(
                    self.native_type.as_str(),
                    "text" | "varchar" | "bpchar" | "name"
                ) =>
            {
                format!("{quoted}::text")
            }
            ColumnKind::Geometry => format!("ST_AsEWKB({quoted})"),
            ColumnKind::Geography => {
                format!("ST_AsEWKB({quoted}::geometry)")
            }
            _ => quoted.clone(),
        };
        Ok(format!("{expression} AS {quoted}"))
    }

    fn initial_arrow_bytes_per_row(&self) -> u64 {
        match self.kind {
            ColumnKind::Bool => 2,
            ColumnKind::I32 | ColumnKind::F32 | ColumnKind::Date => 5,
            ColumnKind::I64
            | ColumnKind::F64
            | ColumnKind::Time
            | ColumnKind::Timestamp
            | ColumnKind::TimestampTz => 9,
            ColumnKind::Interval | ColumnKind::Decimal { .. } => 17,
            ColumnKind::Utf8 => 21,
            ColumnKind::Binary | ColumnKind::Geometry | ColumnKind::Geography => 37,
            ColumnKind::Range => 32,
            ColumnKind::Composite => usize_to_u64(self.composite_fields.len())
                .saturating_mul(21)
                .saturating_add(1),
            ColumnKind::BoolArray => 13,
            ColumnKind::I32Array | ColumnKind::F32Array => 25,
            ColumnKind::I64Array | ColumnKind::F64Array => 41,
            ColumnKind::Utf8Array => 137,
        }
    }
}

struct PostgresBatchStream {
    client: PooledClient,
    cancel_token: CancelToken,
    tls_mode: PostgresTlsMode,
    tls_connector: MakeRustlsConnect,
    cancel_timeout_ms: u64,
    rows: PostgresRows,
    columns: Vec<ColumnSpec>,
    schema: SchemaRef,
    batch_rows: usize,
    target_batch_bytes: Option<u64>,
    max_batch_bytes: u64,
    max_wkb_cell_bytes: u64,
    metrics: Arc<PostgresMetrics>,
    byte_estimate_scale_permille: u64,
    track_byte_estimate: bool,
    batches_since_byte_estimate: u8,
    finished: bool,
}

type PostgresRows =
    Pin<Box<dyn Stream<Item = std::result::Result<Row, tokio_postgres::Error>> + Send>>;

fn range_fields() -> Vec<Arc<Field>> {
    vec![
        Arc::new(Field::new("lower", DataType::Utf8, true)),
        Arc::new(Field::new("upper", DataType::Utf8, true)),
        Arc::new(Field::new("lower_inclusive", DataType::Boolean, false)),
        Arc::new(Field::new("upper_inclusive", DataType::Boolean, false)),
        Arc::new(Field::new("lower_unbounded", DataType::Boolean, false)),
        Arc::new(Field::new("upper_unbounded", DataType::Boolean, false)),
        Arc::new(Field::new("empty", DataType::Boolean, false)),
    ]
}

fn adaptive_builder_capacity(
    columns: &[ColumnSpec],
    batch_rows: usize,
    target_batch_bytes: Option<u64>,
) -> usize {
    let Some(target) = target_batch_bytes else {
        return batch_rows;
    };
    let row_bytes = columns.iter().fold(0_u64, |total, column| {
        total.saturating_add(column.initial_arrow_bytes_per_row())
    });
    let rows = target / row_bytes.max(1);
    batch_rows.min(usize::try_from(rows.max(1)).unwrap_or(usize::MAX))
}

impl PostgresBatchStream {
    fn observe_batch_size(&mut self, actual_bytes: u64, estimated_bytes: u64) {
        let Some(target) = self.target_batch_bytes else {
            return;
        };
        if self.track_byte_estimate && estimated_bytes > 0 {
            self.byte_estimate_scale_permille = actual_bytes
                .saturating_mul(1_100)
                .checked_div(estimated_bytes)
                .unwrap_or(8_000)
                .clamp(1_000, 8_000);
            if actual_bytes.saturating_mul(4) < target {
                self.track_byte_estimate = false;
                self.batches_since_byte_estimate = 0;
            }
        } else if !self.track_byte_estimate {
            self.batches_since_byte_estimate = self.batches_since_byte_estimate.saturating_add(1);
            if self.batches_since_byte_estimate >= 8 {
                self.track_byte_estimate = true;
                self.batches_since_byte_estimate = 0;
            }
        }
    }
}

impl BatchStream for PostgresBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            let builder_capacity =
                adaptive_builder_capacity(&self.columns, self.batch_rows, self.target_batch_bytes);
            let mut buffers = self
                .columns
                .iter()
                .map(|column| ColumnBuffer::new(column, builder_capacity))
                .collect::<Vec<_>>();
            let mut row_count = 0;
            let mut estimated_bytes = 0_u64;
            let mut target_limited = false;
            while row_count < self.batch_rows {
                match self.rows.as_mut().next().await {
                    Some(Ok(row)) => {
                        for (index, buffer) in buffers.iter_mut().enumerate() {
                            match buffer.append(&row, index) {
                                Ok(bytes) if self.track_byte_estimate => {
                                    estimated_bytes = estimated_bytes.saturating_add(bytes);
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    self.client.invalidate();
                                    return Err(error);
                                }
                            }
                        }
                        row_count += 1;
                        if row_count < self.batch_rows
                            && self.track_byte_estimate
                            && self.target_batch_bytes.is_some_and(|target| {
                                estimated_bytes.saturating_mul(self.byte_estimate_scale_permille)
                                    >= target.saturating_mul(1_000)
                            })
                        {
                            target_limited = true;
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        self.client.invalidate();
                        return Err(classify_error(ErrorPhase::Read, &error));
                    }
                    None => {
                        self.finished = true;
                        break;
                    }
                }
            }
            if row_count == 0 {
                return Ok(None);
            }
            let arrays = buffers
                .iter_mut()
                .map(ColumnBuffer::finish)
                .collect::<Vec<_>>();
            let batch = match RecordBatch::try_new(Arc::clone(&self.schema), arrays) {
                Ok(batch) => batch,
                Err(error) => {
                    self.client.invalidate();
                    return Err(DatabaseError::from(error));
                }
            };
            if let Err(error) = enforce_batch_limits(
                &batch,
                &self.columns,
                self.max_batch_bytes,
                self.max_wkb_cell_bytes,
            ) {
                self.client.invalidate();
                return Err(error);
            }
            let actual_bytes = batch_memory_bytes(&batch);
            self.observe_batch_size(actual_bytes, estimated_bytes);
            self.metrics.read_batch(
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                actual_bytes,
            );
            if target_limited {
                self.metrics.target_limited_batch();
            }
            Ok(Some(batch))
        })
    }

    fn next_batch_with_cancellation<'a>(
        &'a mut self,
        cancellation: &'a dyn Cancellation,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                self.metrics.cancellation();
                self.client.invalidate();
                self.finished = true;
                return Err(cancelled_read_error());
            }
            let token = self.cancel_token.clone();
            let tls_mode = self.tls_mode;
            let tls_connector = self.tls_connector.clone();
            let cancel_timeout_ms = self.cancel_timeout_ms;
            let completed = {
                let next = self.next_batch();
                tokio::pin!(next);
                tokio::select! {
                    result = &mut next => Some(result),
                    () = wait_for_cancellation(cancellation) => None,
                }
            };
            if let Some(result) = completed {
                result
            } else {
                self.metrics.cancellation();
                self.client.invalidate();
                self.finished = true;
                let _cancel_result = tokio::time::timeout(
                    StdDuration::from_millis(cancel_timeout_ms),
                    cancel_query(&token, tls_mode, &tls_connector),
                )
                .await;
                Err(cancelled_read_error())
            }
        })
    }
}

impl Drop for PostgresBatchStream {
    fn drop(&mut self) {
        if !self.finished {
            self.client.invalidate();
        }
    }
}

async fn wait_for_cancellation(cancellation: &dyn Cancellation) {
    while !cancellation.is_cancelled() {
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

async fn select_with_cancellation<T, F>(future: F, cancellation: &dyn Cancellation) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => Some(result),
        () = wait_for_cancellation(cancellation) => None,
    }
}

async fn cancel_and_invalidate(
    client: &mut PooledClient,
    tls_mode: PostgresTlsMode,
    tls_connector: &MakeRustlsConnect,
    timeout_ms: u64,
) {
    let token = client.cancel_token();
    client.pool.metrics.cancellation();
    client.invalidate();
    let _cancel_result = tokio::time::timeout(
        StdDuration::from_millis(timeout_ms),
        cancel_query(&token, tls_mode, tls_connector),
    )
    .await;
}

async fn cancel_query(
    token: &CancelToken,
    tls_mode: PostgresTlsMode,
    tls_connector: &MakeRustlsConnect,
) -> std::result::Result<(), tokio_postgres::Error> {
    match tls_mode {
        PostgresTlsMode::Disabled => token.cancel_query(NoTls).await,
        PostgresTlsMode::Require => token.cancel_query(tls_connector.clone()).await,
    }
}

fn cancelled_read_error() -> DatabaseError {
    public_error(
        ErrorCategory::Cancelled,
        ErrorPhase::Read,
        false,
        "query PostgreSQL cancellata sul server",
    )
}

fn enforce_batch_limits(
    batch: &RecordBatch,
    columns: &[ColumnSpec],
    max_batch_bytes: u64,
    max_wkb_cell_bytes: u64,
) -> Result<()> {
    let bytes = batch_memory_bytes(batch);
    if bytes > max_batch_bytes {
        return Err(public_error(
            ErrorCategory::ResourceLimit,
            ErrorPhase::Read,
            false,
            "RecordBatch PostgreSQL oltre max_batch_bytes",
        ));
    }
    for (index, column) in columns.iter().enumerate() {
        if matches!(column.kind, ColumnKind::Geometry | ColumnKind::Geography) {
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .expect("colonna spatial Binary");
            for row in 0..array.len() {
                if !array.is_null(row)
                    && u64::try_from(array.value(row).len()).unwrap_or(u64::MAX)
                        > max_wkb_cell_bytes
                {
                    return Err(public_error(
                        ErrorCategory::ResourceLimit,
                        ErrorPhase::Read,
                        false,
                        "cella WKB oltre max_wkb_cell_bytes",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn batch_memory_bytes(batch: &RecordBatch) -> u64 {
    batch.columns().iter().fold(0_u64, |total, array| {
        total.saturating_add(u64::try_from(array.get_array_memory_size()).unwrap_or(u64::MAX))
    })
}

enum ColumnBuffer {
    Bool(BooleanBuilder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
    Interval(IntervalMonthDayNanoBuilder),
    Range(StructBuilder),
    Composite(StructBuilder, Vec<String>),
    Timestamp(TimestampMicrosecondBuilder),
    TimestampTz(TimestampMicrosecondBuilder),
    Decimal(Decimal128Builder, i8),
    BoolArray(ListBuilder<BooleanBuilder>),
    I32Array(ListBuilder<Int32Builder>),
    I64Array(ListBuilder<Int64Builder>),
    F32Array(ListBuilder<Float32Builder>),
    F64Array(ListBuilder<Float64Builder>),
    Utf8Array(ListBuilder<StringBuilder>),
}

#[derive(Debug)]
struct PostgresInterval {
    microseconds: i64,
    days: i32,
    months: i32,
}

impl<'a> FromSql<'a> for PostgresInterval {
    fn from_sql(
        _ty: &Type,
        mut raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.remaining() != 16 {
            return Err("invalid PostgreSQL interval payload".into());
        }
        Ok(Self {
            microseconds: raw.get_i64(),
            days: raw.get_i32(),
            months: raw.get_i32(),
        })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INTERVAL
    }
}

impl ColumnBuffer {
    fn new(column: &ColumnSpec, capacity: usize) -> Self {
        match column.kind {
            ColumnKind::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnKind::I32 => Self::I32(Int32Builder::with_capacity(capacity)),
            ColumnKind::I64 => Self::I64(Int64Builder::with_capacity(capacity)),
            ColumnKind::F32 => Self::F32(Float32Builder::with_capacity(capacity)),
            ColumnKind::F64 => Self::F64(Float64Builder::with_capacity(capacity)),
            ColumnKind::Utf8 => Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            )),
            ColumnKind::Binary | ColumnKind::Geometry | ColumnKind::Geography => Self::Binary(
                BinaryBuilder::with_capacity(capacity, capacity.saturating_mul(32)),
            ),
            ColumnKind::Date => Self::Date(Date32Builder::with_capacity(capacity)),
            ColumnKind::Time => Self::Time(Time64MicrosecondBuilder::with_capacity(capacity)),
            ColumnKind::Interval => {
                Self::Interval(IntervalMonthDayNanoBuilder::with_capacity(capacity))
            }
            ColumnKind::Range => Self::Range(StructBuilder::from_fields(range_fields(), capacity)),
            ColumnKind::Composite => {
                let names = column
                    .composite_fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect::<Vec<_>>();
                let fields = names
                    .iter()
                    .zip(&column.composite_fields)
                    .map(|(name, composite_field)| {
                        let mut metadata = HashMap::new();
                        metadata.insert(
                            "plenora.native_declaration".to_owned(),
                            composite_field.declaration.clone(),
                        );
                        Arc::new(Field::new(name, DataType::Utf8, true).with_metadata(metadata))
                    })
                    .collect::<Vec<_>>();
                Self::Composite(StructBuilder::from_fields(fields, capacity), names)
            }
            ColumnKind::Timestamp => {
                Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            ColumnKind::TimestampTz => Self::TimestampTz(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_data_type(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ),
            ),
            ColumnKind::Decimal { precision, scale } => Self::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(precision, scale)),
                scale,
            ),
            ColumnKind::BoolArray => Self::BoolArray(ListBuilder::new(
                BooleanBuilder::with_capacity(capacity.saturating_mul(4)),
            )),
            ColumnKind::I32Array => Self::I32Array(ListBuilder::new(Int32Builder::with_capacity(
                capacity.saturating_mul(4),
            ))),
            ColumnKind::I64Array => Self::I64Array(ListBuilder::new(Int64Builder::with_capacity(
                capacity.saturating_mul(4),
            ))),
            ColumnKind::F32Array => Self::F32Array(ListBuilder::new(
                Float32Builder::with_capacity(capacity.saturating_mul(4)),
            )),
            ColumnKind::F64Array => Self::F64Array(ListBuilder::new(
                Float64Builder::with_capacity(capacity.saturating_mul(4)),
            )),
            ColumnKind::Utf8Array => {
                Self::Utf8Array(ListBuilder::new(StringBuilder::with_capacity(
                    capacity.saturating_mul(4),
                    capacity.saturating_mul(32),
                )))
            }
        }
    }

    fn append(&mut self, row: &Row, index: usize) -> Result<u64> {
        match self {
            Self::Bool(builder) => append_option(builder, row.try_get(index)),
            Self::I32(builder) => append_option(builder, row.try_get(index)),
            Self::I64(builder) => append_option(builder, row.try_get(index)),
            Self::F32(builder) => append_option(builder, row.try_get(index)),
            Self::F64(builder) => append_option(builder, row.try_get(index)),
            Self::Utf8(builder) => append_option(builder, row.try_get(index)),
            Self::Binary(builder) => append_option(builder, row.try_get(index)),
            Self::Date(builder) => {
                let value: Option<NaiveDate> = row.try_get(index).map_err(row_decode_error)?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date constant");
                builder.append_option(value.map(|date| {
                    i32::try_from(date.signed_duration_since(epoch).num_days())
                        .expect("PostgreSQL date fits Arrow Date32")
                }));
                Ok(5)
            }
            Self::Time(builder) => {
                let value: Option<chrono::NaiveTime> =
                    row.try_get(index).map_err(row_decode_error)?;
                builder.append_option(value.map(|time| {
                    i64::from(time.num_seconds_from_midnight()) * 1_000_000
                        + i64::from(time.nanosecond() / 1_000)
                }));
                Ok(9)
            }
            Self::Interval(builder) => {
                let value: Option<PostgresInterval> =
                    row.try_get(index).map_err(row_decode_error)?;
                let value = value
                    .map(|interval| {
                        interval
                            .microseconds
                            .checked_mul(1_000)
                            .map(|nanoseconds| {
                                IntervalMonthDayNano::new(
                                    interval.months,
                                    interval.days,
                                    nanoseconds,
                                )
                            })
                            .ok_or_else(|| {
                                public_error(
                                    ErrorCategory::DataMapping,
                                    ErrorPhase::Read,
                                    false,
                                    "interval PostgreSQL oltre il range Arrow nanosecond",
                                )
                            })
                    })
                    .transpose()?;
                builder.append_option(value);
                Ok(17)
            }
            Self::Range(builder) => append_range(builder, row.try_get(index)),
            Self::Composite(builder, names) => append_composite(builder, names, row.try_get(index)),
            Self::Timestamp(builder) => {
                let value: Option<NaiveDateTime> = row.try_get(index).map_err(row_decode_error)?;
                builder
                    .append_option(value.map(|timestamp| timestamp.and_utc().timestamp_micros()));
                Ok(9)
            }
            Self::TimestampTz(builder) => {
                let value: Option<DateTime<Utc>> = row.try_get(index).map_err(row_decode_error)?;
                builder.append_option(value.map(|timestamp| timestamp.timestamp_micros()));
                Ok(9)
            }
            Self::Decimal(builder, scale) => {
                let value: Option<String> = row.try_get(index).map_err(row_decode_error)?;
                let parsed = value
                    .as_deref()
                    .map(|text| parse_decimal128(text, *scale))
                    .transpose()?;
                builder.append_option(parsed);
                Ok(17)
            }
            Self::BoolArray(builder) => append_list(builder, row.try_get(index)),
            Self::I32Array(builder) => append_list(builder, row.try_get(index)),
            Self::I64Array(builder) => append_list(builder, row.try_get(index)),
            Self::F32Array(builder) => append_list(builder, row.try_get(index)),
            Self::F64Array(builder) => append_list(builder, row.try_get(index)),
            Self::Utf8Array(builder) => append_string_list(builder, row.try_get(index)),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::F32(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::Time(builder) => Arc::new(builder.finish()),
            Self::Interval(builder) => Arc::new(builder.finish()),
            Self::Range(builder) | Self::Composite(builder, _) => Arc::new(builder.finish()),
            Self::Timestamp(builder) | Self::TimestampTz(builder) => Arc::new(builder.finish()),
            Self::Decimal(builder, _) => Arc::new(builder.finish()),
            Self::BoolArray(builder) => Arc::new(builder.finish()),
            Self::I32Array(builder) => Arc::new(builder.finish()),
            Self::I64Array(builder) => Arc::new(builder.finish()),
            Self::F32Array(builder) => Arc::new(builder.finish()),
            Self::F64Array(builder) => Arc::new(builder.finish()),
            Self::Utf8Array(builder) => Arc::new(builder.finish()),
        }
    }
}

fn append_range(
    builder: &mut StructBuilder,
    value: std::result::Result<Option<String>, tokio_postgres::Error>,
) -> Result<u64> {
    let value = value.map_err(row_decode_error)?;
    let estimated_bytes = value
        .as_ref()
        .map_or(16, |text| 16_u64.saturating_add(usize_to_u64(text.len())));
    let document = value
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()
        .map_err(|_| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "range PostgreSQL strutturato non valido",
            )
        })?;
    let lower = document
        .as_ref()
        .and_then(|item| item.get("lower"))
        .and_then(serde_json::Value::as_str);
    let upper = document
        .as_ref()
        .and_then(|item| item.get("upper"))
        .and_then(serde_json::Value::as_str);
    builder
        .field_builder::<StringBuilder>(0)
        .expect("range lower builder")
        .append_option(lower);
    builder
        .field_builder::<StringBuilder>(1)
        .expect("range upper builder")
        .append_option(upper);
    for (index, key) in [
        (2, "lower_inclusive"),
        (3, "upper_inclusive"),
        (4, "lower_unbounded"),
        (5, "upper_unbounded"),
        (6, "empty"),
    ] {
        builder
            .field_builder::<BooleanBuilder>(index)
            .expect("range boolean builder")
            .append_value(
                document
                    .as_ref()
                    .and_then(|item| item.get(key))
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            );
    }
    builder.append(document.is_some());
    Ok(estimated_bytes)
}

fn append_composite(
    builder: &mut StructBuilder,
    names: &[String],
    value: std::result::Result<Option<String>, tokio_postgres::Error>,
) -> Result<u64> {
    let value = value.map_err(row_decode_error)?;
    let estimated_bytes = value.as_ref().map_or_else(
        || usize_to_u64(names.len()).saturating_mul(5),
        |text| usize_to_u64(text.len()).saturating_add(usize_to_u64(names.len()).saturating_mul(5)),
    );
    let document = value
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()
        .map_err(|_| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "composite PostgreSQL JSON non valido",
            )
        })?;
    for (index, name) in names.iter().enumerate() {
        let value = document
            .as_ref()
            .and_then(|item| item.get(name))
            .filter(|item| !item.is_null())
            .map(|item| match item {
                serde_json::Value::String(value) => value.clone(),
                _ => item.to_string(),
            });
        builder
            .field_builder::<StringBuilder>(index)
            .expect("composite field builder")
            .append_option(value);
    }
    builder.append(document.is_some());
    Ok(estimated_bytes)
}

fn append_list<T, B>(
    builder: &mut ListBuilder<B>,
    value: std::result::Result<Option<Vec<Option<T>>>, tokio_postgres::Error>,
) -> Result<u64>
where
    B: arrow_array::builder::ArrayBuilder + AppendOption<T>,
    T: EstimatedArrowBytes,
{
    let estimated_bytes = if let Some(values) = value.map_err(row_decode_error)? {
        let estimated = values.iter().fold(5_u64, |total, value| {
            total.saturating_add(
                1 + value
                    .as_ref()
                    .map_or(T::NULL_BYTES, EstimatedArrowBytes::estimated_arrow_bytes),
            )
        });
        for value in values {
            builder.values().append_optional(value);
        }
        builder.append(true);
        estimated
    } else {
        builder.append(false);
        5
    };
    Ok(estimated_bytes)
}

fn append_string_list(
    builder: &mut ListBuilder<StringBuilder>,
    value: std::result::Result<Option<Vec<Option<String>>>, tokio_postgres::Error>,
) -> Result<u64> {
    let estimated_bytes = if let Some(values) = value.map_err(row_decode_error)? {
        let estimated = values.iter().fold(5_u64, |total, value| {
            total.saturating_add(
                1 + value.as_ref().map_or(
                    String::NULL_BYTES,
                    EstimatedArrowBytes::estimated_arrow_bytes,
                ),
            )
        });
        for value in values {
            builder.values().append_option(value);
        }
        builder.append(true);
        estimated
    } else {
        builder.append(false);
        5
    };
    Ok(estimated_bytes)
}

trait EstimatedArrowBytes {
    const NULL_BYTES: u64;

    fn estimated_arrow_bytes(&self) -> u64;
}

macro_rules! impl_fixed_arrow_bytes {
    ($value:ty, $bytes:expr) => {
        impl EstimatedArrowBytes for $value {
            const NULL_BYTES: u64 = $bytes;

            fn estimated_arrow_bytes(&self) -> u64 {
                $bytes
            }
        }
    };
}

impl_fixed_arrow_bytes!(bool, 1);
impl_fixed_arrow_bytes!(i32, 4);
impl_fixed_arrow_bytes!(i64, 8);
impl_fixed_arrow_bytes!(f32, 4);
impl_fixed_arrow_bytes!(f64, 8);

impl EstimatedArrowBytes for String {
    const NULL_BYTES: u64 = 4;

    fn estimated_arrow_bytes(&self) -> u64 {
        4_u64.saturating_add(usize_to_u64(self.len()))
    }
}

impl EstimatedArrowBytes for Vec<u8> {
    const NULL_BYTES: u64 = 4;

    fn estimated_arrow_bytes(&self) -> u64 {
        4_u64.saturating_add(usize_to_u64(self.len()))
    }
}

trait AppendOption<T> {
    fn append_optional(&mut self, value: Option<T>);
}

macro_rules! impl_append_option {
    ($builder:ty, $value:ty) => {
        impl AppendOption<$value> for $builder {
            fn append_optional(&mut self, value: Option<$value>) {
                self.append_option(value);
            }
        }
    };
}

impl_append_option!(BooleanBuilder, bool);
impl_append_option!(Int32Builder, i32);
impl_append_option!(Int64Builder, i64);
impl_append_option!(Float32Builder, f32);
impl_append_option!(Float64Builder, f64);
impl_append_option!(StringBuilder, String);
impl_append_option!(BinaryBuilder, Vec<u8>);

fn append_option<B, T>(
    builder: &mut B,
    value: std::result::Result<Option<T>, tokio_postgres::Error>,
) -> Result<u64>
where
    B: AppendOption<T>,
    T: EstimatedArrowBytes,
{
    let value = value.map_err(row_decode_error)?;
    let estimated_bytes = 1 + value
        .as_ref()
        .map_or(T::NULL_BYTES, EstimatedArrowBytes::estimated_arrow_bytes);
    builder.append_optional(value);
    Ok(estimated_bytes)
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

#[derive(Clone, Copy)]
enum FilterBindTarget<'a> {
    Column(&'a str),
    SpatialGeometry,
    SpatialDistance,
}

#[derive(Clone, Copy)]
struct FilterBind<'a> {
    name: &'a str,
    target: FilterBindTarget<'a>,
}

fn collect_filter_binds<'a>(expression: &'a FilterExpression, binds: &mut Vec<FilterBind<'a>>) {
    match expression {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            for argument in args {
                collect_filter_binds(argument, binds);
            }
        }
        FilterExpression::Eq { field, parameter }
        | FilterExpression::Ne { field, parameter }
        | FilterExpression::Lt { field, parameter }
        | FilterExpression::Lte { field, parameter }
        | FilterExpression::Gt { field, parameter }
        | FilterExpression::Gte { field, parameter }
        | FilterExpression::Like {
            field, parameter, ..
        } => binds.push(FilterBind {
            name: parameter,
            target: FilterBindTarget::Column(field),
        }),
        FilterExpression::In { field, parameters } => {
            binds.extend(parameters.iter().map(|parameter| FilterBind {
                name: parameter,
                target: FilterBindTarget::Column(field),
            }));
        }
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => {
            binds.push(FilterBind {
                name: lower_parameter,
                target: FilterBindTarget::Column(field),
            });
            binds.push(FilterBind {
                name: upper_parameter,
                target: FilterBindTarget::Column(field),
            });
        }
        FilterExpression::Spatial {
            function,
            geometry_parameter,
            distance_parameter,
            ..
        } => {
            if !matches!(
                function,
                SpatialFunction::IsEmpty | SpatialFunction::IsValid
            ) {
                if let Some(parameter) = geometry_parameter {
                    binds.push(FilterBind {
                        name: parameter,
                        target: FilterBindTarget::SpatialGeometry,
                    });
                }
                if *function == SpatialFunction::DWithin {
                    if let Some(parameter) = distance_parameter {
                        binds.push(FilterBind {
                            name: parameter,
                            target: FilterBindTarget::SpatialDistance,
                        });
                    }
                }
            }
        }
        FilterExpression::IsNull { .. } | FilterExpression::IsNotNull { .. } => {}
    }
}

fn typed_filter_parameter_types(
    filter: Option<&FilterExpression>,
    bind_names: &[String],
    parameters: &ParameterBag,
    columns: &[ColumnSpec],
) -> Option<Vec<Type>> {
    let Some(filter) = filter else {
        return bind_names.is_empty().then(Vec::new);
    };
    let mut binds = Vec::new();
    collect_filter_binds(filter, &mut binds);
    if binds.len() != bind_names.len()
        || binds
            .iter()
            .zip(bind_names)
            .any(|(bind, rendered)| bind.name != rendered)
    {
        return None;
    }
    binds
        .into_iter()
        .map(|bind| {
            let value = parameters.get(bind.name)?;
            match bind.target {
                FilterBindTarget::SpatialGeometry => match value {
                    ParameterValue::Wkb { .. } | ParameterValue::Bytes(_) => Some(Type::BYTEA),
                    _ => None,
                },
                FilterBindTarget::SpatialDistance => {
                    matches!(value, ParameterValue::F64(_)).then_some(Type::FLOAT8)
                }
                FilterBindTarget::Column(field) => {
                    let column = columns.iter().find(|column| column.name == field)?;
                    typed_column_parameter_type(column, value)
                }
            }
        })
        .collect()
}

fn typed_column_parameter_type(column: &ColumnSpec, value: &ParameterValue) -> Option<Type> {
    if column.type_kind.as_deref().is_some_and(|kind| kind != "b") {
        return None;
    }
    let native = column.native_type.as_str();
    match value {
        ParameterValue::Bool(_) if native == "bool" => Some(Type::BOOL),
        ParameterValue::I32(_) if matches!(native, "int2" | "int4" | "int8" | "numeric") => {
            Some(Type::INT4)
        }
        ParameterValue::I64(_) if matches!(native, "int2" | "int4" | "int8" | "numeric") => {
            Some(Type::INT8)
        }
        ParameterValue::F64(_) if matches!(native, "float4" | "float8" | "numeric") => {
            Some(Type::FLOAT8)
        }
        ParameterValue::String(_) if matches!(native, "text" | "varchar" | "bpchar" | "name") => {
            Some(Type::TEXT)
        }
        ParameterValue::Bytes(_) | ParameterValue::Wkb { .. } if native == "bytea" => {
            Some(Type::BYTEA)
        }
        ParameterValue::Date(_) if matches!(native, "date" | "timestamp" | "timestamptz") => {
            Some(Type::DATE)
        }
        ParameterValue::Timestamp(_) if matches!(native, "date" | "timestamp" | "timestamptz") => {
            Some(Type::TIMESTAMP)
        }
        ParameterValue::TimestampTz(_)
            if matches!(native, "date" | "timestamp" | "timestamptz") =>
        {
            Some(Type::TIMESTAMPTZ)
        }
        ParameterValue::Json(_) if native == "json" => Some(Type::JSON),
        ParameterValue::Json(_) if native == "jsonb" => Some(Type::JSONB),
        ParameterValue::Decimal(_) if native == "numeric" => Some(Type::NUMERIC),
        ParameterValue::Uuid(_) if native == "uuid" => Some(Type::UUID),
        ParameterValue::Null { type_name } => {
            let declared = builtin_type_name(type_name)?;
            (declared.name() == native
                || null_type_matches(type_name, &declared)
                    && null_type_matches(type_name, &native_type(native)?))
            .then_some(declared)
        }
        _ => None,
    }
}

fn builtin_type_name(type_name: &str) -> Option<Type> {
    match type_name.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Some(Type::BOOL),
        "int2" | "smallint" => Some(Type::INT2),
        "int4" | "integer" => Some(Type::INT4),
        "int8" | "bigint" => Some(Type::INT8),
        "float4" | "real" => Some(Type::FLOAT4),
        "float8" | "double precision" => Some(Type::FLOAT8),
        "text" => Some(Type::TEXT),
        "varchar" | "character varying" => Some(Type::VARCHAR),
        "bpchar" | "character" => Some(Type::BPCHAR),
        "bytea" => Some(Type::BYTEA),
        "date" => Some(Type::DATE),
        "time" => Some(Type::TIME),
        "timestamp" => Some(Type::TIMESTAMP),
        "timestamptz" | "timestamp with time zone" => Some(Type::TIMESTAMPTZ),
        "interval" => Some(Type::INTERVAL),
        "numeric" | "decimal" => Some(Type::NUMERIC),
        "json" => Some(Type::JSON),
        "jsonb" => Some(Type::JSONB),
        "uuid" => Some(Type::UUID),
        _ => None,
    }
}

fn native_type(type_name: &str) -> Option<Type> {
    builtin_type_name(type_name)
}

fn parameter_value_type(value: &ParameterValue) -> Option<Type> {
    match value {
        ParameterValue::Null { type_name } => builtin_type_name(type_name),
        ParameterValue::Bool(_) => Some(Type::BOOL),
        ParameterValue::I32(_) => Some(Type::INT4),
        ParameterValue::I64(_) => Some(Type::INT8),
        ParameterValue::F64(_) => Some(Type::FLOAT8),
        ParameterValue::String(_) => Some(Type::TEXT),
        ParameterValue::Bytes(_) | ParameterValue::Wkb { .. } => Some(Type::BYTEA),
        ParameterValue::Date(_) => Some(Type::DATE),
        ParameterValue::Timestamp(_) => Some(Type::TIMESTAMP),
        ParameterValue::TimestampTz(_) => Some(Type::TIMESTAMPTZ),
        ParameterValue::Json(_) => Some(Type::JSONB),
        ParameterValue::Decimal(_) => Some(Type::NUMERIC),
        ParameterValue::Uuid(_) => Some(Type::UUID),
    }
}

fn typed_query_parameter_types(
    bind_names: &[String],
    parameters: &ParameterBag,
) -> Option<Vec<Type>> {
    bind_names
        .iter()
        .map(|name| parameter_value_type(parameters.get(name)?))
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

#[derive(Debug)]
struct DecimalParameter {
    value: i128,
    scale: i8,
}

impl DecimalParameter {
    fn parse(value: &str) -> Result<Self> {
        let negative = value.starts_with('-');
        let unsigned = value.trim_start_matches(['-', '+']);
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let scale = i8::try_from(fraction.len())
            .map_err(|_| DatabaseError::invalid_plan("scala parametro decimal troppo grande"))?;
        let mut digits = String::with_capacity(integer.len() + fraction.len());
        digits.push_str(if integer.is_empty() { "0" } else { integer });
        digits.push_str(fraction);
        let parsed = digits
            .parse::<i128>()
            .map_err(|_| DatabaseError::invalid_plan("parametro decimal non valido"))?;
        Ok(Self {
            value: if negative { -parsed } else { parsed },
            scale,
        })
    }
}

impl ToSql for DecimalParameter {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non numeric".into());
        }
        write::encode_numeric_binary(self.value, self.scale, output)?;
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        *target_type == Type::NUMERIC
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct UuidParameter([u8; 16]);

impl UuidParameter {
    fn parse(value: &str) -> Result<Self> {
        let compact = value
            .chars()
            .filter(|character| *character != '-')
            .collect::<String>();
        if compact.len() != 32 {
            return Err(DatabaseError::invalid_plan("parametro UUID non valido"));
        }
        let mut bytes = [0_u8; 16];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16)
                .map_err(|_| DatabaseError::invalid_plan("parametro UUID non valido"))?;
        }
        Ok(Self(bytes))
    }
}

impl ToSql for UuidParameter {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non UUID".into());
        }
        output.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        *target_type == Type::UUID
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct TypedNull(String);

impl ToSql for TypedNull {
    fn to_sql(
        &self,
        target_type: &Type,
        _output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !null_type_matches(&self.0, target_type) {
            return Err("tipo NULL dichiarato non coerente col parametro PostgreSQL".into());
        }
        Ok(IsNull::Yes)
    }

    fn accepts(_target_type: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

fn null_type_matches(declared: &str, target: &Type) -> bool {
    declared.eq_ignore_ascii_case(target.name())
        || matches!(
            (declared.to_ascii_lowercase().as_str(), target.name()),
            ("boolean", "bool")
                | ("smallint", "int2")
                | ("integer", "int4")
                | ("bigint", "int8")
                | ("real", "float4")
                | ("double precision", "float8")
                | ("decimal", "numeric")
                | ("timestamp", "timestamp")
                | ("timestamptz", "timestamptz")
                | ("varchar", "varchar")
                | ("text", "text")
        )
}

fn bind_parameters(
    parameters: &ParameterBag,
    bind_names: &[String],
) -> Result<Vec<Box<dyn ToSql + Sync + Send>>> {
    bind_names
        .iter()
        .map(|name| {
            let value = parameters
                .get(name)
                .ok_or_else(|| DatabaseError::invalid_plan("parametro filtro mancante"))?;
            let boxed: Box<dyn ToSql + Sync + Send> = match value {
                ParameterValue::Bool(value) => Box::new(*value),
                ParameterValue::I32(value) => Box::new(*value),
                ParameterValue::I64(value) => Box::new(*value),
                ParameterValue::F64(value) => Box::new(*value),
                ParameterValue::String(value) => Box::new(value.clone()),
                ParameterValue::Bytes(value) => Box::new(value.clone()),
                ParameterValue::Date(value) => Box::new(
                    NaiveDate::parse_from_str(value, "%Y-%m-%d")
                        .map_err(|_| DatabaseError::invalid_plan("parametro date non valido"))?,
                ),
                ParameterValue::Timestamp(value) => Box::new(
                    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").map_err(|_| {
                        DatabaseError::invalid_plan("parametro timestamp non valido")
                    })?,
                ),
                ParameterValue::TimestampTz(value) => Box::new(
                    DateTime::parse_from_rfc3339(value)
                        .map_err(|_| {
                            DatabaseError::invalid_plan("parametro timestamptz non valido")
                        })?
                        .with_timezone(&Utc),
                ),
                ParameterValue::Json(value) => Box::new(value.clone()),
                ParameterValue::Wkb { bytes, .. } => Box::new(bytes.clone()),
                ParameterValue::Decimal(value) => Box::new(DecimalParameter::parse(value)?),
                ParameterValue::Uuid(value) => Box::new(UuidParameter::parse(value)?),
                ParameterValue::Null { type_name } => Box::new(TypedNull(type_name.clone())),
            };
            Ok(boxed)
        })
        .collect()
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

fn check_cancelled(cancellation: &dyn Cancellation, phase: ErrorPhase) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(public_error(
            ErrorCategory::Cancelled,
            phase,
            false,
            "operazione cancellata",
        ))
    } else {
        Ok(())
    }
}

fn row_decode_error(_: tokio_postgres::Error) -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        false,
        "valore PostgreSQL non convertibile nel tipo Arrow",
    )
}

fn classify_error(phase: ErrorPhase, error: &tokio_postgres::Error) -> DatabaseError {
    let (category, retryable, message) =
        match error.code().map(tokio_postgres::error::SqlState::code) {
            Some("28P01") => (
                ErrorCategory::Authentication,
                false,
                "autenticazione PostgreSQL fallita",
            ),
            Some("42501") => (
                ErrorCategory::Authorization,
                false,
                "permesso PostgreSQL insufficiente",
            ),
            Some("42P01" | "42703" | "3F000") => (
                ErrorCategory::NotFound,
                false,
                "oggetto PostgreSQL non trovato",
            ),
            Some("40001" | "40P01" | "55P03") => (
                ErrorCategory::Transient,
                true,
                "conflitto PostgreSQL transitorio",
            ),
            Some("57014") => (
                ErrorCategory::Cancelled,
                false,
                "operazione PostgreSQL cancellata",
            ),
            _ if error.is_closed() => (
                ErrorCategory::Transient,
                true,
                "connessione PostgreSQL chiusa",
            ),
            _ => (
                ErrorCategory::Protocol,
                false,
                "operazione PostgreSQL fallita",
            ),
        };
    public_error(category, phase, retryable, message)
}

pub(crate) fn public_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    retryable: bool,
    message: &str,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        provider: Some(ProviderKind::Postgres),
        retryable,
        execution_id: None,
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::geometry::SpatialSemantics;
    use plenora_database_core::loss::MappingPolicy;
    use plenora_database_core::outcome::WriteStatus;
    use plenora_database_core::plan::{
        ComparisonOperator, LayerId, OrderBy, SridPolicy, TransactionProfile, WriteMode,
    };
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
    };
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    struct NeverCancelled;
    struct AlwaysCancelled;
    struct AtomicCancellation(Arc<AtomicBool>);

    impl Cancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    impl Cancellation for AlwaysCancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    impl Cancellation for AtomicCancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(AtomicOrdering::Acquire)
        }
    }

    #[test]
    fn decimal_parser_preserves_scale() {
        assert_eq!(parse_decimal128("123.45", 4).expect("decimal"), 1_234_500);
        assert_eq!(parse_decimal128("-0.01", 2).expect("decimal"), -1);
        assert_eq!(parse_decimal128("12300", -2).expect("negative scale"), 123);
    }

    #[test]
    fn network_options_override_dsn_and_partition_the_pool() {
        let secret = SecretString::new("host=localhost user=fixture");
        let options = PostgresNetworkOptions {
            connect_timeout_ms: 1_500,
            tcp_user_timeout_ms: 9_000,
            keepalive_idle_secs: 11,
            keepalive_interval_secs: 4,
            keepalive_retries: 7,
        };
        let config = connection_config(&secret, options).expect("network config");
        assert_eq!(
            config.get_connect_timeout(),
            Some(&StdDuration::from_millis(1_500))
        );
        assert_eq!(
            config.get_tcp_user_timeout(),
            Some(&StdDuration::from_millis(9_000))
        );
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), StdDuration::from_secs(11));
        assert_eq!(
            config.get_keepalives_interval(),
            Some(StdDuration::from_secs(4))
        );
        assert_eq!(config.get_keepalives_retries(), Some(7));
        assert_eq!(
            connection_config_for_mode(&secret, options, PostgresTlsMode::Disabled)
                .expect("disabled TLS config")
                .get_ssl_mode(),
            SslMode::Disable
        );
        assert_eq!(
            connection_config_for_mode(&secret, options, PostgresTlsMode::Require)
                .expect("required TLS config")
                .get_ssl_mode(),
            SslMode::Require
        );
        let tls_config = PostgresTlsConfig::webpki();
        assert_ne!(
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                options,
                30_000,
                5_000,
            ),
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                PostgresNetworkOptions::default(),
                30_000,
                5_000,
            )
        );
        assert_ne!(
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                options,
                30_000,
                5_000,
            ),
            connection_fingerprint(
                &secret,
                PostgresTlsMode::Disabled,
                &tls_config,
                options,
                1_000,
                500,
            )
        );
        let mut session_config =
            connection_config_for_mode(&secret, options, PostgresTlsMode::Disabled)
                .expect("session config");
        configure_session_startup(&mut session_config, 1_234, 567).expect("session defaults");
        assert_eq!(
            session_config.get_application_name(),
            Some("plenora-database-tools")
        );
        let startup_options = session_config.get_options().expect("startup options");
        assert!(startup_options.contains("statement_timeout=1234ms"));
        assert!(startup_options.contains("lock_timeout=567ms"));
    }

    #[test]
    fn performance_profiles_are_stable_and_composable() {
        assert_eq!(PostgresPerformanceProfile::LowLatency.batch_rows(), 1_024);
        assert_eq!(
            PostgresPerformanceProfile::LowLatency.insert_mode(),
            PostgresInsertMode::CopyText
        );
        assert_eq!(
            PostgresPerformanceProfile::LowLatency.target_batch_bytes(),
            1024 * 1024
        );
        assert_eq!(PostgresPerformanceProfile::BalancedBulk.batch_rows(), 8_192);
        assert_eq!(
            PostgresPerformanceProfile::BalancedBulk.insert_mode(),
            PostgresInsertMode::CopyBinary
        );
        assert_eq!(
            PostgresPerformanceProfile::BalancedBulk.target_batch_bytes(),
            4 * 1024 * 1024
        );
        assert_eq!(
            serde_json::to_string(&PostgresPerformanceProfile::BalancedBulk)
                .expect("serialize profile"),
            "\"balanced_bulk\""
        );

        let low_latency = PostgresProvider::default();
        assert_eq!(low_latency.batch_rows, 1_024);
        assert_eq!(low_latency.insert_mode, PostgresInsertMode::CopyText);
        assert_eq!(low_latency.target_batch_bytes, Some(1024 * 1024));

        let bulk = PostgresProvider::new(7)
            .with_timeouts(321, 123)
            .with_performance_profile(PostgresPerformanceProfile::BalancedBulk);
        assert_eq!(bulk.batch_rows, 8_192);
        assert_eq!(bulk.insert_mode, PostgresInsertMode::CopyBinary);
        assert_eq!(bulk.target_batch_bytes, Some(4 * 1024 * 1024));
        assert_eq!(bulk.statement_timeout_ms, 321);
        assert_eq!(bulk.lock_timeout_ms, 123);

        let manual = bulk
            .with_target_batch_bytes(123_456)
            .without_target_batch_bytes();
        assert_eq!(manual.target_batch_bytes, None);
    }

    #[test]
    fn parameterized_fast_path_planning_is_typed_and_conservative() {
        fn column(name: &str, native_type: &str, type_kind: &str) -> ColumnSpec {
            ColumnSpec {
                name: name.to_owned(),
                native_type: native_type.to_owned(),
                nullable: true,
                numeric_precision: None,
                numeric_scale: None,
                spatial_srid: None,
                spatial_dimensions: None,
                spatial_type: None,
                default_expression: None,
                identity_kind: None,
                generated_kind: None,
                native_declaration: None,
                type_kind: Some(type_kind.to_owned()),
                composite_fields: Vec::new(),
                kind: ColumnKind::Utf8,
            }
        }

        let integer_filter = FilterExpression::Eq {
            field: "event_id".to_owned(),
            parameter: "event_id".to_owned(),
        };
        let integer_parameters = ParameterBag::new(BTreeMap::from([(
            "event_id".to_owned(),
            ParameterValue::I64(42),
        )]));
        assert_eq!(
            typed_filter_parameter_types(
                Some(&integer_filter),
                &["event_id".to_owned()],
                &integer_parameters,
                &[column("event_id", "int8", "b")],
            ),
            Some(vec![Type::INT8])
        );

        let custom_parameters = ParameterBag::new(BTreeMap::from([(
            "status".to_owned(),
            ParameterValue::String("ready".to_owned()),
        )]));
        let custom_filter = FilterExpression::Eq {
            field: "status".to_owned(),
            parameter: "status".to_owned(),
        };
        assert_eq!(
            typed_filter_parameter_types(
                Some(&custom_filter),
                &["status".to_owned()],
                &custom_parameters,
                &[column("status", "job_status", "e")],
            ),
            None
        );

        let spatial_filter = FilterExpression::Spatial {
            function: SpatialFunction::DWithin,
            field: "geom".to_owned(),
            geometry_parameter: Some("probe".to_owned()),
            distance_parameter: Some("radius".to_owned()),
        };
        let spatial_parameters = ParameterBag::new(BTreeMap::from([
            (
                "probe".to_owned(),
                ParameterValue::Wkb {
                    bytes: vec![1, 2, 3],
                    srid: Some(4326),
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ),
            ("radius".to_owned(), ParameterValue::F64(100.0)),
        ]));
        assert_eq!(
            typed_filter_parameter_types(
                Some(&spatial_filter),
                &["probe".to_owned(), "radius".to_owned()],
                &spatial_parameters,
                &[column("geom", "geometry", "b")],
            ),
            Some(vec![Type::BYTEA, Type::FLOAT8])
        );
    }

    #[tokio::test]
    async fn live_adaptive_read_batches_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(10_000)
            .with_target_batch_bytes(64 * 1024)
            .with_byte_limits(256 * 1024, 64 * 1024 * 1024);
        let secret = SecretString::new(dsn);
        let mut stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection: Vec::new(),
                    order_by: Vec::new(),
                    row_limit: Some(10_000),
                    filter: None,
                },
                &ParameterBag::default(),
                &NeverCancelled,
            )
            .await
            .expect("adaptive stream");
        let mut rows = 0_usize;
        let mut batches = 0_usize;
        let mut max_rows = 0_usize;
        while let Some(batch) = stream.next_batch().await.expect("adaptive batch") {
            assert!(batch_memory_bytes(&batch) <= 256 * 1024);
            rows += batch.num_rows();
            batches += 1;
            max_rows = max_rows.max(batch.num_rows());
        }
        assert_eq!(rows, 10_000);
        assert!(batches > 1);
        assert!(max_rows < 10_000);
        assert!(provider.metrics_snapshot().read_target_limited_batches > 0);
    }

    #[test]
    fn tls_material_is_validated_and_redacted() {
        let config = PostgresTlsConfig::webpki();
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("PRIVATE KEY"));
        assert!(PostgresTlsConfig::private_ca_pem(b"not a certificate").is_err());
        assert!(
            PostgresTlsConfig::from_pem(false, &[], None, None).is_err(),
            "empty trust store accepted"
        );
        assert!(
            PostgresTlsConfig::from_pem(true, &[], Some(b"certificate"), None).is_err(),
            "partial client identity accepted"
        );
    }

    #[tokio::test]
    async fn live_private_ca_mtls_and_cancellation_when_configured() {
        let (Ok(dsn), Ok(ca_path), Ok(client_certificate_path), Ok(client_private_key_path)) = (
            std::env::var("PLENORA_TEST_POSTGRES_TLS_DSN"),
            std::env::var("PLENORA_TEST_POSTGRES_TLS_CA"),
            std::env::var("PLENORA_TEST_POSTGRES_TLS_CLIENT_CERT"),
            std::env::var("PLENORA_TEST_POSTGRES_TLS_CLIENT_KEY"),
        ) else {
            return;
        };
        let ca = std::fs::read(ca_path).expect("read private CA");
        let client_certificate =
            std::fs::read(client_certificate_path).expect("read client certificate");
        let client_private_key =
            std::fs::read(client_private_key_path).expect("read client private key");
        let tls_config = PostgresTlsConfig::private_ca_with_client_identity_pem(
            &ca,
            &client_certificate,
            &client_private_key,
        )
        .expect("build mTLS config");
        let provider = PostgresProvider::new(100)
            .with_pool_size(2, 5_000)
            .with_tls_config(tls_config.clone());
        let secret = SecretString::new(dsn);
        let info = provider
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect("mTLS connection");
        assert_eq!(info.provider, ProviderKind::Postgres);

        let setup = PostgresProvider::connect_with_tls(
            &secret,
            PostgresTlsMode::Require,
            &tls_config,
            PostgresNetworkOptions::default(),
            30_000,
            5_000,
        )
        .await
        .expect("mTLS setup connection");
        setup
            .batch_execute(
                "CREATE OR REPLACE VIEW plenora_fixture.mtls_slow_events AS
                 SELECT value::bigint AS event_id
                 FROM generate_series(1, 100) AS value
                 CROSS JOIN LATERAL
                    pg_sleep((value * 0 + 50)::double precision / 1000)",
            )
            .await
            .expect("mTLS slow view");

        let flag = Arc::new(AtomicBool::new(false));
        let toggle = Arc::clone(&flag);
        tokio::spawn(async move {
            tokio::time::sleep(StdDuration::from_millis(75)).await;
            toggle.store(true, AtomicOrdering::Release);
        });
        let error = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "mtls_slow_events".to_owned(),
                        layer_id: None,
                    },
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: None,
                },
                &ParameterBag::default(),
                &AtomicCancellation(flag),
            )
            .await
            .err()
            .expect("mTLS server-side cancellation");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        provider
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect("mTLS recovery");
        let metrics = provider.metrics_snapshot();
        assert!(metrics.cancellations >= 1);
        assert!(metrics.invalidated_sessions >= 1);

        let untrusted_error = PostgresProvider::new(1)
            .with_tls_mode(PostgresTlsMode::Require)
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect_err("private CA accepted by WebPKI");
        assert!(!untrusted_error.to_string().contains("PRIVATE KEY"));
        let missing_identity = PostgresProvider::new(1)
            .with_tls_config(PostgresTlsConfig::private_ca_pem(&ca).expect("private CA"))
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect_err("mTLS accepted without client identity");
        assert!(!missing_identity
            .to_string()
            .contains("dataflow_tls_test_2026"));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    // Un'unica sessione live condivide setup e credenziali fra tutte le
    // asserzioni di conformità read, quoting e redazione.
    async fn live_postgis_read_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(777);
        let secret = SecretString::new(dsn);
        let cancellation = NeverCancelled;
        let info = provider
            .test_connection(&secret, &cancellation)
            .await
            .expect("connection");
        assert_eq!(info.provider, ProviderKind::Postgres);
        assert!(provider.pool_idle_connections() >= 1);
        let capabilities = provider
            .probe_capabilities(&secret, &cancellation)
            .await
            .expect("capabilities");
        assert!(capabilities.spatial.read_wkb);
        let operation = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None::<LayerId>,
            },
            projection: vec![
                "event_id".to_owned(),
                "amount".to_owned(),
                "occurred_at".to_owned(),
                "geom".to_owned(),
                "geog".to_owned(),
            ],
            order_by: vec![],
            row_limit: None,
            filter: None,
        };
        let mut stream = provider
            .read(&secret, &operation, &ParameterBag::default(), &cancellation)
            .await
            .expect("read");
        assert_eq!(
            stream
                .schema()
                .field_with_name("geom")
                .expect("geom")
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_WKB_EXTENSION_NAME)
        );
        let mut rows = 0;
        let mut batches = 0;
        while let Some(batch) = stream.next_batch().await.expect("batch") {
            rows += batch.num_rows();
            batches += 1;
        }
        assert_eq!(rows, 10_000);
        assert_eq!(batches, 13);

        let filtered = ReadOperation {
            source: operation.source,
            projection: vec!["event_id".to_owned(), "geom".to_owned()],
            order_by: vec![OrderBy {
                field: "event_id".to_owned(),
                direction: SortDirection::Asc,
            }],
            row_limit: Some(10),
            filter: Some(FilterExpression::Eq {
                field: "region_id".to_owned(),
                parameter: "region_id".to_owned(),
            }),
        };
        let mut values = BTreeMap::new();
        values.insert("region_id".to_owned(), ParameterValue::I32(11));
        let parameters = ParameterBag::new(values);
        let mut filtered_stream = provider
            .read(&secret, &filtered, &parameters, &cancellation)
            .await
            .expect("filtered read");
        let filtered_batch = filtered_stream
            .next_batch()
            .await
            .expect("filtered batch")
            .expect("rows");
        assert_eq!(filtered_batch.num_rows(), 10);
        assert!(filtered_stream
            .next_batch()
            .await
            .expect("filtered end")
            .is_none());

        let client = PostgresProvider::connect(&secret).await.expect("client");
        let probe_wkb: Vec<u8> = client
            .query_one(
                "SELECT ST_AsEWKB(geom) FROM plenora_fixture.events WHERE event_id = 1",
                &[],
            )
            .await
            .expect("spatial probe")
            .get(0);
        let spatial_read = ReadOperation {
            source: filtered.source.clone(),
            projection: vec!["event_id".to_owned(), "geom".to_owned()],
            order_by: vec![OrderBy {
                field: "event_id".to_owned(),
                direction: SortDirection::Asc,
            }],
            row_limit: Some(3),
            filter: Some(FilterExpression::Spatial {
                function: SpatialFunction::Intersects,
                field: "geom".to_owned(),
                geometry_parameter: Some("probe".to_owned()),
                distance_parameter: None,
            }),
        };
        let mut spatial_values = BTreeMap::new();
        spatial_values.insert(
            "probe".to_owned(),
            ParameterValue::Wkb {
                bytes: probe_wkb,
                srid: Some(4326),
                dimensions: Dimensions::Xyz,
                semantics: SpatialSemantics::Geometry,
            },
        );
        let mut spatial_stream = provider
            .read(
                &secret,
                &spatial_read,
                &ParameterBag::new(spatial_values),
                &cancellation,
            )
            .await
            .expect("spatial read");
        assert_eq!(
            spatial_stream
                .next_batch()
                .await
                .expect("spatial batch")
                .expect("spatial rows")
                .num_rows(),
            3
        );

        let query_operation = QueryOperation {
            common_table_expressions: Vec::new(),
            source: QuerySource {
                object: spatial_read.source.clone(),
                alias: Some("e".to_owned()),
            },
            projection: vec![
                QueryProjection {
                    expression: QueryExpression::Column {
                        column: ColumnRef {
                            relation: Some("e".to_owned()),
                            field: "event_id".to_owned(),
                        },
                    },
                    alias: Some("event_id".to_owned()),
                },
                QueryProjection {
                    expression: QueryExpression::Spatial {
                        function: SpatialFunction::Centroid,
                        arguments: vec![QueryExpression::Column {
                            column: ColumnRef {
                                relation: Some("e".to_owned()),
                                field: "geom".to_owned(),
                            },
                        }],
                    },
                    alias: Some("center".to_owned()),
                },
            ],
            joins: Vec::new(),
            filter: Some(QueryExpression::Compare {
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("e".to_owned()),
                        field: "event_id".to_owned(),
                    },
                }),
                operator: ComparisonOperator::Gt,
                right: Box::new(QueryExpression::Parameter {
                    name: "minimum_id".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("e".to_owned()),
                        field: "event_id".to_owned(),
                    },
                },
                direction: SortDirection::Asc,
            }],
            distinct: false,
            row_limit: Some(2),
        };
        let mut query_parameters = BTreeMap::new();
        query_parameters.insert("minimum_id".to_owned(), ParameterValue::I64(100));
        let mut query_stream = provider
            .query(
                &secret,
                &query_operation,
                &ParameterBag::new(query_parameters),
                &cancellation,
            )
            .await
            .expect("query AST");
        assert_eq!(
            query_stream
                .schema()
                .field_with_name("center")
                .expect("spatial projection")
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_WKB_EXTENSION_NAME)
        );
        assert_eq!(
            query_stream
                .next_batch()
                .await
                .expect("query batch")
                .expect("query rows")
                .num_rows(),
            2
        );
        drop(query_stream);
        let mut cached_query_stream = provider
            .query(
                &secret,
                &query_operation,
                &ParameterBag::new(BTreeMap::from([(
                    "minimum_id".to_owned(),
                    ParameterValue::I64(100),
                )])),
                &cancellation,
            )
            .await
            .expect("query AST cached plan");
        assert_eq!(
            cached_query_stream
                .next_batch()
                .await
                .expect("cached query batch")
                .expect("cached query rows")
                .num_rows(),
            2
        );
        drop(cached_query_stream);
        let mut empty_query_stream = provider
            .query(
                &secret,
                &query_operation,
                &ParameterBag::new(BTreeMap::from([(
                    "minimum_id".to_owned(),
                    ParameterValue::I64(i64::MAX),
                )])),
                &cancellation,
            )
            .await
            .expect("query AST empty result");
        assert_eq!(
            empty_query_stream
                .schema()
                .field_with_name("center")
                .expect("empty spatial projection")
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_WKB_EXTENSION_NAME)
        );
        assert!(empty_query_stream
            .next_batch()
            .await
            .expect("empty query batch")
            .is_none());
        drop(empty_query_stream);

        let advanced_source = ObjectRef {
            catalog: None,
            schema: Some("plenora_fixture".to_owned()),
            object: "advanced_types".to_owned(),
            layer_id: None,
        };
        let advanced_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: advanced_source.clone(),
                },
                &cancellation,
            )
            .await
            .expect("advanced introspection");
        assert_eq!(advanced_description.document["relation"]["kind"], "table");
        assert!(advanced_description.document["constraints"]
            .as_array()
            .is_some_and(|constraints| !constraints.is_empty()));
        assert!(advanced_description.document["indexes"]
            .as_array()
            .is_some_and(|indexes| !indexes.is_empty()));
        let described_columns = advanced_description.document["columns"]
            .as_array()
            .expect("described columns");
        assert!(described_columns
            .iter()
            .any(|column| { column["name"] == "id" && column["identity_kind"] == "a" }));
        assert!(described_columns
            .iter()
            .any(|column| { column["name"] == "doubled" && column["generated_kind"] == "s" }));
        let mut advanced_stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: advanced_source,
                    projection: Vec::new(),
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("advanced read");
        assert!(matches!(
            advanced_stream
                .schema()
                .field_with_name("integer_values")
                .expect("array field")
                .data_type(),
            DataType::List(item) if item.data_type() == &DataType::Int32
        ));
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("local_time")
                .expect("time field")
                .data_type(),
            &DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("duration")
                .expect("interval field")
                .data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert!(matches!(
            advanced_stream
                .schema()
                .field_with_name("integer_window")
                .expect("range field")
                .data_type(),
            DataType::Struct(fields)
                if fields.iter().any(|field| field.name() == "lower")
                    && fields.iter().any(|field| field.name() == "empty")
        ));
        assert!(matches!(
            advanced_stream
                .schema()
                .field_with_name("profile")
                .expect("composite field")
                .data_type(),
            DataType::Struct(fields)
                if fields.iter().map(|field| field.name().as_str()).collect::<Vec<_>>()
                    == ["label", "priority", "enabled"]
        ));
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("rounded_amount")
                .expect("negative scale decimal")
                .data_type(),
            if info
                .server_version
                .split('.')
                .next()
                .and_then(|major| major.parse::<u16>().ok())
                .is_some_and(|major| major >= 15)
            {
                &DataType::Decimal128(6, -2)
            } else {
                &DataType::Decimal128(8, 0)
            }
        );
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("integer_values")
                .expect("array field")
                .metadata()
                .get("plenora.native_declaration")
                .map(String::as_str),
            Some("integer[]")
        );
        assert_eq!(
            advanced_stream
                .schema()
                .field_with_name("status")
                .expect("enum field")
                .metadata()
                .get("plenora.postgres_type_kind")
                .map(String::as_str),
            Some("e")
        );
        assert_eq!(
            advanced_stream
                .next_batch()
                .await
                .expect("advanced batch")
                .expect("advanced row")
                .num_rows(),
            1
        );
        drop(advanced_stream);

        let mut typed_values = BTreeMap::new();
        typed_values.insert(
            "external_id".to_owned(),
            ParameterValue::Uuid("123e4567-e89b-12d3-a456-426614174000".to_owned()),
        );
        typed_values.insert(
            "rounded_amount".to_owned(),
            ParameterValue::Decimal("12300".to_owned()),
        );
        let mut typed_stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec!["id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: Some(FilterExpression::And {
                        args: vec![
                            FilterExpression::Eq {
                                field: "external_id".to_owned(),
                                parameter: "external_id".to_owned(),
                            },
                            FilterExpression::Eq {
                                field: "rounded_amount".to_owned(),
                                parameter: "rounded_amount".to_owned(),
                            },
                        ],
                    }),
                },
                &ParameterBag::new(typed_values),
                &cancellation,
            )
            .await
            .expect("typed parameters");
        assert_eq!(
            typed_stream
                .next_batch()
                .await
                .expect("typed parameter batch")
                .expect("typed parameter row")
                .num_rows(),
            1
        );
        drop(typed_stream);
        let mut null_values = BTreeMap::new();
        null_values.insert(
            "duration".to_owned(),
            ParameterValue::Null {
                type_name: "interval".to_owned(),
            },
        );
        let mut null_stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec!["id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: Some(FilterExpression::Eq {
                        field: "duration".to_owned(),
                        parameter: "duration".to_owned(),
                    }),
                },
                &ParameterBag::new(null_values),
                &cancellation,
            )
            .await
            .expect("typed null parameter");
        assert!(null_stream
            .next_batch()
            .await
            .expect("typed null batch")
            .is_none());
        drop(null_stream);

        let limited_provider = PostgresProvider::new(10).with_byte_limits(1, 1);
        let mut limited_stream = limited_provider
            .read(
                &secret,
                &ReadOperation {
                    source: spatial_read.source.clone(),
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("limited stream");
        let limited_error = limited_stream.next_batch().await.expect_err("byte budget");
        assert_eq!(limited_error.category, ErrorCategory::ResourceLimit);

        let mut cancelled_stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: spatial_read.source.clone(),
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("cancel stream");
        let cancelled_error = cancelled_stream
            .next_batch_with_cancellation(&AlwaysCancelled)
            .await
            .expect_err("cancelled stream");
        assert_eq!(cancelled_error.category, ErrorCategory::Cancelled);
        drop(cancelled_stream);

        client
            .batch_execute(
                "CREATE OR REPLACE VIEW plenora_fixture.slow_events AS
                 SELECT event_id
                 FROM plenora_fixture.events
                 CROSS JOIN LATERAL pg_sleep((event_id * 0 + 50)::double precision / 1000)
                 LIMIT 100",
            )
            .await
            .expect("slow view");
        let cancellation_flag = Arc::new(AtomicBool::new(false));
        let toggle = Arc::clone(&cancellation_flag);
        tokio::spawn(async move {
            tokio::time::sleep(StdDuration::from_millis(50)).await;
            toggle.store(true, AtomicOrdering::Release);
        });
        let started = std::time::Instant::now();
        let inflight_error = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "slow_events".to_owned(),
                        layer_id: None,
                    },
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: None,
                    filter: None,
                },
                &ParameterBag::default(),
                &AtomicCancellation(cancellation_flag),
            )
            .await
            .err()
            .expect("in-flight cancellation");
        assert_eq!(inflight_error.category, ErrorCategory::Cancelled);
        assert!(started.elapsed() < StdDuration::from_secs(2));
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        let slow_queries: i64 = client
            .query_one(
                "SELECT count(*)
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND state = 'active'
                   AND query LIKE '%plenora_fixture.slow_events%'
                   AND pid <> pg_backend_pid()",
                &[],
            )
            .await
            .expect("cancel state")
            .get(0);
        assert_eq!(slow_queries, 0);

        let single_connection_provider = PostgresProvider::new(10).with_pool_size(1, 25);
        let held_stream = single_connection_provider
            .read(
                &secret,
                &ReadOperation {
                    source: spatial_read.source.clone(),
                    projection: vec!["event_id".to_owned()],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("held pool stream");
        let pool_error = single_connection_provider
            .test_connection(&secret, &cancellation)
            .await
            .expect_err("pool acquisition timeout");
        assert_eq!(pool_error.category, ErrorCategory::Timeout);
        assert_eq!(
            single_connection_provider.metrics_snapshot().pool_timeouts,
            1
        );
        drop(held_stream);

        let quoted = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "Quoted Table".to_owned(),
                layer_id: None,
            },
            projection: vec![
                "select".to_owned(),
                "spaced column".to_owned(),
                "a\"b".to_owned(),
            ],
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        };
        let mut quoted_stream = provider
            .read(&secret, &quoted, &ParameterBag::default(), &cancellation)
            .await
            .expect("quoted read");
        assert_eq!(
            quoted_stream
                .next_batch()
                .await
                .expect("quoted batch")
                .expect("quoted row")
                .num_rows(),
            1
        );

        let marker = "must-not-leak-2026";
        let invalid_secret = SecretString::new(format!(
            "host=dataflow-postgres user=dataflow password={marker} dbname=dataflow_test"
        ));
        let error = provider
            .test_connection(&invalid_secret, &cancellation)
            .await
            .expect_err("invalid authentication");
        assert!(!error.to_string().contains(marker));
        let metrics = provider.metrics_snapshot();
        assert!(metrics.pool_checkouts > 0);
        assert!(metrics.pool_new_connections > 0);
        assert!(metrics.pool_reuses > 0);
        assert!(metrics.session_resets > 0);
        assert!(metrics.catalog_introspections > 0);
        assert!(metrics.read_typed_fast_paths > 0);
        assert!(metrics.read_parameterized_typed_fast_paths >= 4);
        assert_eq!(metrics.read_prepared_fallbacks, 0);
        assert!(metrics.query_typed_fast_paths >= 3);
        assert_eq!(metrics.query_prepared_fallbacks, 0);
        assert!(metrics.read_batches > 0);
        assert!(metrics.read_rows > 0);
        assert!(metrics.read_bytes > 0);
        assert!(metrics.cancellations >= 2);
        assert!(metrics.invalidated_sessions >= 2);
    }

    async fn fixture_stream(
        provider: &PostgresProvider,
        secret: &SecretString,
        cancellation: &NeverCancelled,
        projection: Vec<String>,
        row_limit: u64,
    ) -> Box<dyn BatchStream> {
        provider
            .read(
                secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection,
                    order_by: vec![OrderBy {
                        field: "event_id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    row_limit: Some(row_limit),
                    filter: None,
                },
                &ParameterBag::default(),
                cancellation,
            )
            .await
            .expect("fixture stream")
    }

    async fn fixture_stream_after(
        provider: &PostgresProvider,
        secret: &SecretString,
        cancellation: &NeverCancelled,
        event_id: i64,
        row_limit: u64,
    ) -> Box<dyn BatchStream> {
        let mut values = BTreeMap::new();
        values.insert("event_id".to_owned(), ParameterValue::I64(event_id));
        provider
            .read(
                secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection: Vec::new(),
                    order_by: vec![OrderBy {
                        field: "event_id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    row_limit: Some(row_limit),
                    filter: Some(FilterExpression::Gt {
                        field: "event_id".to_owned(),
                        parameter: "event_id".to_owned(),
                    }),
                },
                &ParameterBag::new(values),
                cancellation,
            )
            .await
            .expect("filtered fixture stream")
    }

    fn write_operation(mode: WriteMode) -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "write_reference".to_owned(),
                layer_id: None,
            },
            mode,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: vec!["event_id".to_owned()],
            update_columns: if mode == WriteMode::Update {
                vec!["name".to_owned(), "amount".to_owned()]
            } else {
                Vec::new()
            },
            srid_policy: Some(SridPolicy::RequireMatch),
            create_spatial_index: matches!(mode, WriteMode::Create | WriteMode::Replace),
            allow_partial: false,
        }
    }

    async fn execute_fixture_write(
        provider: &PostgresProvider,
        secret: &SecretString,
        cancellation: &NeverCancelled,
        mode: WriteMode,
        stream: Box<dyn BatchStream>,
    ) -> WriteOutcome {
        let operation = write_operation(mode);
        let prepared = provider
            .prepare_write(secret, &operation, stream.schema(), cancellation)
            .await
            .expect("prepare write");
        provider
            .write(secret, prepared, stream, cancellation)
            .await
            .expect("execute write")
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn live_postgis_write_modes_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(7);
        let secret = SecretString::new(dsn);
        let cancellation = NeverCancelled;
        let client = PostgresProvider::connect(&secret).await.expect("client");
        client
            .batch_execute(
                "DROP TABLE IF EXISTS plenora_fixture.write_reference;
                 DROP TABLE IF EXISTS plenora_fixture.advanced_roundtrip;
                 DROP TABLE IF EXISTS plenora_fixture.advanced_binary_roundtrip;
                 DROP TABLE IF EXISTS plenora_fixture.evolution_target;
                 DROP TABLE IF EXISTS plenora_fixture.slow_write_target",
            )
            .await
            .expect("cleanup");

        let advanced_stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec![
                        "status".to_owned(),
                        "domain_value".to_owned(),
                        "rounded_amount".to_owned(),
                        "integer_values".to_owned(),
                        "text_values".to_owned(),
                        "integer_window".to_owned(),
                        "timestamp_window".to_owned(),
                        "duration".to_owned(),
                        "local_time".to_owned(),
                        "profile".to_owned(),
                    ],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("advanced source");
        let advanced_operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "advanced_roundtrip".to_owned(),
                layer_id: None,
            },
            mode: WriteMode::Create,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        };
        let advanced_prepared = provider
            .prepare_write(
                &secret,
                &advanced_operation,
                advanced_stream.schema(),
                &cancellation,
            )
            .await
            .expect("advanced prepare");
        let advanced_outcome = provider
            .write(&secret, advanced_prepared, advanced_stream, &cancellation)
            .await
            .expect("advanced write");
        assert_eq!(advanced_outcome.rows.confirmed, 1);
        let advanced_matches: bool = client
            .query_one(
                "SELECT
                    target.status = source.status
                    AND target.domain_value = source.domain_value
                    AND target.rounded_amount = source.rounded_amount
                    AND target.integer_values = source.integer_values
                    AND target.text_values = source.text_values
                    AND target.integer_window = source.integer_window
                    AND target.timestamp_window = source.timestamp_window
                    AND target.duration = source.duration
                    AND target.local_time = source.local_time
                    AND target.profile = source.profile
                 FROM plenora_fixture.advanced_roundtrip AS target
                 CROSS JOIN plenora_fixture.advanced_types AS source",
                &[],
            )
            .await
            .expect("advanced roundtrip")
            .get(0);
        assert!(advanced_matches);

        let advanced_binary_stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "advanced_types".to_owned(),
                        layer_id: None,
                    },
                    projection: vec![
                        "external_id".to_owned(),
                        "rounded_amount".to_owned(),
                        "integer_values".to_owned(),
                        "text_values".to_owned(),
                        "integer_window".to_owned(),
                        "timestamp_window".to_owned(),
                        "duration".to_owned(),
                        "local_time".to_owned(),
                        "profile".to_owned(),
                    ],
                    order_by: Vec::new(),
                    row_limit: Some(1),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("advanced binary source");
        let mut advanced_binary_operation = advanced_operation.clone();
        advanced_binary_operation.target.object = "advanced_binary_roundtrip".to_owned();
        let binary_provider = provider
            .clone()
            .with_insert_mode(PostgresInsertMode::CopyBinary);
        let advanced_binary_prepared = binary_provider
            .prepare_write(
                &secret,
                &advanced_binary_operation,
                advanced_binary_stream.schema(),
                &cancellation,
            )
            .await
            .expect("advanced binary prepare");
        let advanced_binary_outcome = binary_provider
            .write(
                &secret,
                advanced_binary_prepared,
                advanced_binary_stream,
                &cancellation,
            )
            .await
            .expect("advanced binary write");
        assert_eq!(advanced_binary_outcome.rows.confirmed, 1);
        let advanced_binary_matches: bool = client
            .query_one(
                "SELECT
                    target.external_id = source.external_id
                    AND target.rounded_amount = source.rounded_amount
                    AND target.integer_values = source.integer_values
                    AND target.text_values = source.text_values
                    AND target.integer_window = source.integer_window
                    AND target.timestamp_window = source.timestamp_window
                    AND target.duration = source.duration
                    AND target.local_time = source.local_time
                    AND target.profile = source.profile
                 FROM plenora_fixture.advanced_binary_roundtrip AS target
                 CROSS JOIN plenora_fixture.advanced_types AS source",
                &[],
            )
            .await
            .expect("advanced binary roundtrip")
            .get(0);
        assert!(advanced_binary_matches);

        client
            .batch_execute(
                "CREATE TABLE plenora_fixture.evolution_target (
                    event_id bigint PRIMARY KEY
                 )",
            )
            .await
            .expect("evolution target");
        let evolution_stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: ObjectRef {
                        catalog: None,
                        schema: Some("plenora_fixture".to_owned()),
                        object: "events".to_owned(),
                        layer_id: None,
                    },
                    projection: vec![
                        "event_id".to_owned(),
                        "name".to_owned(),
                        "region_id".to_owned(),
                    ],
                    order_by: vec![OrderBy {
                        field: "event_id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    row_limit: Some(2),
                    filter: None,
                },
                &ParameterBag::default(),
                &cancellation,
            )
            .await
            .expect("evolution source");
        let evolution_operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "evolution_target".to_owned(),
                layer_id: None,
            },
            mode: WriteMode::Append,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        };
        let strict_error = provider
            .prepare_write(
                &secret,
                &evolution_operation,
                evolution_stream.schema(),
                &cancellation,
            )
            .await
            .err()
            .expect("strict schema evolution");
        assert_eq!(strict_error.category, ErrorCategory::DataMapping);
        let evolution_provider = provider
            .clone()
            .with_schema_evolution(PostgresSchemaEvolution::AddNullableColumns);
        let evolution_prepared = evolution_provider
            .prepare_write(
                &secret,
                &evolution_operation,
                evolution_stream.schema(),
                &cancellation,
            )
            .await
            .expect("additive evolution prepare");
        assert_eq!(evolution_prepared.loss_report.losses.len(), 2);
        assert!(evolution_prepared
            .loss_report
            .losses
            .iter()
            .all(|loss| loss.severity == LossSeverity::Information));
        let evolution_outcome = evolution_provider
            .write(&secret, evolution_prepared, evolution_stream, &cancellation)
            .await
            .expect("additive evolution write");
        assert_eq!(evolution_outcome.rows.confirmed, 2);
        let evolution_state = client
            .query_one(
                "SELECT
                    (SELECT count(*) FROM plenora_fixture.evolution_target),
                    (
                        SELECT count(*)
                        FROM information_schema.columns
                        WHERE table_schema = 'plenora_fixture'
                          AND table_name = 'evolution_target'
                    )",
                &[],
            )
            .await
            .expect("evolution state");
        assert_eq!(evolution_state.get::<_, i64>(0), 2);
        assert_eq!(evolution_state.get::<_, i64>(1), 3);

        client
            .batch_execute(
                "CREATE OR REPLACE FUNCTION plenora_fixture.slow_write()
                 RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                   PERFORM pg_sleep(0.05);
                   RETURN NEW;
                 END
                 $$;
                 CREATE TABLE plenora_fixture.slow_write_target (
                   event_id bigint,
                   name text
                 );
                 CREATE TRIGGER slow_write_row
                 BEFORE INSERT ON plenora_fixture.slow_write_target
                 FOR EACH ROW EXECUTE FUNCTION plenora_fixture.slow_write()",
            )
            .await
            .expect("slow write target");
        let slow_write_stream = fixture_stream(
            &provider,
            &secret,
            &cancellation,
            vec!["event_id".to_owned(), "name".to_owned()],
            100,
        )
        .await;
        let slow_write_operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "slow_write_target".to_owned(),
                layer_id: None,
            },
            mode: WriteMode::Append,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        };
        let slow_write_prepared = provider
            .prepare_write(
                &secret,
                &slow_write_operation,
                slow_write_stream.schema(),
                &cancellation,
            )
            .await
            .expect("slow write prepare");
        let write_cancellation_flag = Arc::new(AtomicBool::new(false));
        let write_toggle = Arc::clone(&write_cancellation_flag);
        tokio::spawn(async move {
            tokio::time::sleep(StdDuration::from_millis(50)).await;
            write_toggle.store(true, AtomicOrdering::Release);
        });
        let started = std::time::Instant::now();
        let slow_write_error = provider
            .write(
                &secret,
                slow_write_prepared,
                slow_write_stream,
                &AtomicCancellation(write_cancellation_flag),
            )
            .await
            .expect_err("in-flight write cancellation");
        assert_eq!(slow_write_error.category, ErrorCategory::Cancelled);
        assert!(started.elapsed() < StdDuration::from_secs(2));
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        let slow_write_rows: i64 = client
            .query_one(
                "SELECT count(*) FROM plenora_fixture.slow_write_target",
                &[],
            )
            .await
            .expect("slow write rollback")
            .get(0);
        assert_eq!(slow_write_rows, 0);

        let created = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Create,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 20).await,
        )
        .await;
        assert_eq!(created.status, WriteStatus::Committed);
        assert_eq!(created.rows.confirmed, 20);

        let appended = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Append,
            fixture_stream_after(&provider, &secret, &cancellation, 20, 5).await,
        )
        .await;
        assert_eq!(appended.rows.confirmed, 5);

        let upserted = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Upsert,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 10).await,
        )
        .await;
        assert_eq!(upserted.rows.confirmed, 10);

        let updated = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Update,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 5).await,
        )
        .await;
        assert_eq!(updated.rows.confirmed, 5);

        let deleted = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::DeleteByKeys,
            fixture_stream(
                &provider,
                &secret,
                &cancellation,
                vec!["event_id".to_owned()],
                5,
            )
            .await,
        )
        .await;
        assert_eq!(deleted.rows.confirmed, 5);

        let truncated = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::TruncateInsert,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 8).await,
        )
        .await;
        assert_eq!(truncated.rows.confirmed, 8);

        let replaced = execute_fixture_write(
            &provider,
            &secret,
            &cancellation,
            WriteMode::Replace,
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 12).await,
        )
        .await;
        assert_eq!(replaced.rows.confirmed, 12);
        let row = client
            .query_one(
                r"
                SELECT
                    count(*),
                    (SELECT ST_SRID(geom) FROM plenora_fixture.write_reference LIMIT 1),
                    (
                        SELECT
                            target.amount = source.amount
                            AND target.payload = source.payload
                            AND target.raw_bytes = source.raw_bytes
                            AND target.occurred_at = source.occurred_at
                            AND ST_Equals(target.geom, source.geom)
                            AND ST_Equals(target.geog::geometry, source.geog::geometry)
                        FROM plenora_fixture.write_reference AS target
                        JOIN plenora_fixture.events AS source USING (event_id)
                        WHERE target.event_id = 1
                    )
                FROM plenora_fixture.write_reference
                ",
                &[],
            )
            .await
            .expect("remote state");
        assert_eq!(row.get::<_, i64>(0), 12);
        assert_eq!(row.get::<_, i32>(1), 4326);
        assert!(row.get::<_, bool>(2));

        let mut fault_operation = write_operation(WriteMode::Create);
        fault_operation.target.object = "write_fault_reference".to_owned();
        client
            .batch_execute("DROP TABLE IF EXISTS plenora_fixture.write_fault_reference")
            .await
            .expect("fault cleanup");
        let rollback_provider =
            PostgresProvider::new(7).with_fault_injection(PostgresFaultPoint::BeforeCommit);
        let rollback_stream =
            fixture_stream(&provider, &secret, &cancellation, Vec::new(), 2).await;
        let rollback_prepared = rollback_provider
            .prepare_write(
                &secret,
                &fault_operation,
                rollback_stream.schema(),
                &cancellation,
            )
            .await
            .expect("fault prepare");
        rollback_provider
            .write(&secret, rollback_prepared, rollback_stream, &cancellation)
            .await
            .expect_err("fault before commit");
        let rolled_back: bool = client
            .query_one(
                "SELECT to_regclass('plenora_fixture.write_fault_reference') IS NULL",
                &[],
            )
            .await
            .expect("rollback state")
            .get(0);
        assert!(rolled_back);

        let unknown_provider = PostgresProvider::new(7)
            .with_fault_injection(PostgresFaultPoint::AfterCommitAcknowledgement);
        let unknown_stream = fixture_stream(&provider, &secret, &cancellation, Vec::new(), 2).await;
        let unknown_prepared = unknown_provider
            .prepare_write(
                &secret,
                &fault_operation,
                unknown_stream.schema(),
                &cancellation,
            )
            .await
            .expect("unknown prepare");
        let unknown = unknown_provider
            .write(&secret, unknown_prepared, unknown_stream, &cancellation)
            .await
            .expect("unknown outcome");
        assert_eq!(unknown.status, WriteStatus::OutcomeUnknown);
        assert!(unknown.recovery.is_some());
        let committed_rows: i64 = client
            .query_one(
                "SELECT count(*) FROM plenora_fixture.write_fault_reference",
                &[],
            )
            .await
            .expect("unknown remote state")
            .get(0);
        assert_eq!(committed_rows, 2);
        let write_metrics = provider.metrics_snapshot();
        assert!(write_metrics.writes_committed > 0);
        assert!(write_metrics.write_rows > 0);
        assert!(write_metrics.schema_cache_invalidations > 0);
        assert_eq!(rollback_provider.metrics_snapshot().invalidated_sessions, 1);
        let unknown_metrics = unknown_provider.metrics_snapshot();
        assert_eq!(unknown_metrics.writes_outcome_unknown, 1);
        assert_eq!(unknown_metrics.invalidated_sessions, 1);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn live_postgres_schema_cache_detects_external_ddl_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let secret = SecretString::new(dsn);
        let setup = PostgresProvider::connect(&secret)
            .await
            .expect("schema cache setup connection");
        setup
            .batch_execute(
                "DROP TABLE IF EXISTS plenora_fixture.schema_cache_probe;
                 CREATE TABLE plenora_fixture.schema_cache_probe (
                    id bigint NOT NULL,
                    label text DEFAULT 'a',
                    geom geometry(Point, 4326)
                 );
                 INSERT INTO plenora_fixture.schema_cache_probe
                 VALUES (1, 'one', ST_SetSRID(ST_MakePoint(1, 2), 4326))",
            )
            .await
            .expect("schema cache fixture");

        let provider = PostgresProvider::new(16)
            .with_pool_size(1, 5_000)
            .with_schema_cache_capacity(1);
        let source = ObjectRef {
            catalog: None,
            schema: Some("plenora_fixture".to_owned()),
            object: "schema_cache_probe".to_owned(),
            layer_id: None,
        };
        let operation = ReadOperation {
            source: source.clone(),
            projection: Vec::new(),
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        };
        for _ in 0..2 {
            let mut stream = provider
                .read(
                    &secret,
                    &operation,
                    &ParameterBag::default(),
                    &NeverCancelled,
                )
                .await
                .expect("cached schema read");
            assert_eq!(
                stream
                    .next_batch()
                    .await
                    .expect("cached schema batch")
                    .expect("cached schema row")
                    .num_rows(),
                1
            );
            assert!(stream
                .next_batch()
                .await
                .expect("cached schema end")
                .is_none());
        }
        let first_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: source.clone(),
                },
                &NeverCancelled,
            )
            .await
            .expect("first schema token");
        let first_fingerprint = first_description.document["schema_token"]
            ["structural_fingerprint"]
            .as_str()
            .expect("first structural fingerprint")
            .to_owned();
        assert_eq!(first_fingerprint.len(), 64);
        let warm = provider.metrics_snapshot();
        assert_eq!(warm.schema_cache_misses, 1);
        assert_eq!(warm.catalog_introspections, 1);
        assert_eq!(warm.schema_cache_hits, 2);
        assert_eq!(warm.schema_token_checks, 2);
        assert_eq!(provider.schema_cache_entries(), 1);
        let provider_debug = format!("{provider:?}");
        assert!(!provider_debug.contains("schema_cache_probe"));
        assert!(!provider_debug.contains("DEFAULT 'a'"));

        setup
            .batch_execute(
                "ALTER TABLE plenora_fixture.schema_cache_probe
                    ALTER COLUMN label TYPE varchar(64),
                    ALTER COLUMN label SET DEFAULT 'b',
                    ADD COLUMN extra integer",
            )
            .await
            .expect("external schema evolution");

        let mut evolved = provider
            .read(
                &secret,
                &operation,
                &ParameterBag::default(),
                &NeverCancelled,
            )
            .await
            .expect("read after external DDL");
        assert_eq!(
            evolved
                .schema()
                .field_with_name("label")
                .expect("evolved label")
                .metadata()
                .get("plenora.native_type")
                .map(String::as_str),
            Some("varchar")
        );
        assert!(evolved.schema().field_with_name("extra").is_ok());
        while evolved
            .next_batch()
            .await
            .expect("evolved schema batch")
            .is_some()
        {}
        drop(evolved);

        let second_description = provider
            .inspect(
                &secret,
                &Operation::DatabaseDescribeObject {
                    source: source.clone(),
                },
                &NeverCancelled,
            )
            .await
            .expect("second schema token");
        let second_fingerprint = second_description.document["schema_token"]
            ["structural_fingerprint"]
            .as_str()
            .expect("second structural fingerprint");
        assert_ne!(first_fingerprint, second_fingerprint);
        let evolved_metrics = provider.metrics_snapshot();
        assert_eq!(evolved_metrics.schema_cache_misses, 2);
        assert_eq!(evolved_metrics.catalog_introspections, 2);
        assert_eq!(evolved_metrics.schema_cache_invalidations, 1);
        assert_eq!(evolved_metrics.schema_cache_hits, 3);
        assert_eq!(evolved_metrics.schema_token_checks, 4);

        let other = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned()],
            order_by: Vec::new(),
            row_limit: Some(1),
            filter: None,
        };
        let mut other_stream = provider
            .read(&secret, &other, &ParameterBag::default(), &NeverCancelled)
            .await
            .expect("LRU second object");
        while other_stream
            .next_batch()
            .await
            .expect("LRU second object batch")
            .is_some()
        {}
        drop(other_stream);
        assert_eq!(provider.schema_cache_entries(), 1);
        assert_eq!(provider.metrics_snapshot().schema_cache_evictions, 1);
    }

    #[tokio::test]
    async fn live_postgres_startup_defaults_and_single_reset_when_dsn_is_available() {
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let provider = PostgresProvider::new(8)
            .with_timeouts(1_234, 567)
            .with_pool_size(1, 5_000);
        let secret = SecretString::new(dsn);

        let first = provider
            .connect_session(&secret)
            .await
            .expect("fresh configured session");
        let initial = first
            .query_one(
                "SELECT current_setting('statement_timeout'),
                        current_setting('lock_timeout'),
                        current_setting('application_name')",
                &[],
            )
            .await
            .expect("startup defaults");
        assert_eq!(initial.get::<_, String>(0), "1234ms");
        assert_eq!(initial.get::<_, String>(1), "567ms");
        assert_eq!(initial.get::<_, String>(2), "plenora-database-tools");
        first
            .batch_execute(
                "SET statement_timeout = 0;
                 SET lock_timeout = 0;
                 SET application_name = 'contaminated';
                 CREATE TEMP TABLE plenora_pool_contamination(value integer);
                 PREPARE plenora_pool_statement AS SELECT 1",
            )
            .await
            .expect("contaminate checked-out session");
        drop(first);

        let after_fresh = provider.metrics_snapshot();
        assert_eq!(after_fresh.pool_new_connections, 1);
        assert_eq!(after_fresh.session_resets, 0);

        let reused = provider
            .connect_session(&secret)
            .await
            .expect("strictly reset reused session");
        let restored = reused
            .query_one(
                "SELECT current_setting('statement_timeout'),
                        current_setting('lock_timeout'),
                        current_setting('application_name'),
                        to_regclass('pg_temp.plenora_pool_contamination')::text,
                        (SELECT count(*) FROM pg_prepared_statements
                         WHERE name = 'plenora_pool_statement')",
                &[],
            )
            .await
            .expect("restored session defaults");
        assert_eq!(restored.get::<_, String>(0), "1234ms");
        assert_eq!(restored.get::<_, String>(1), "567ms");
        assert_eq!(restored.get::<_, String>(2), "plenora-database-tools");
        assert_eq!(restored.get::<_, Option<String>>(3), None);
        assert_eq!(restored.get::<_, i64>(4), 0);
        drop(reused);

        let metrics = provider.metrics_snapshot();
        assert_eq!(metrics.pool_checkouts, 2);
        assert_eq!(metrics.pool_new_connections, 1);
        assert_eq!(metrics.pool_reuses, 1);
        assert_eq!(metrics.session_resets, 1);
        assert_eq!(metrics.invalidated_sessions, 0);
    }

    #[tokio::test]
    async fn live_postgres_concurrent_pool_stress_when_dsn_is_available() {
        const WORKERS: u64 = 12;
        const ROUNDS: u64 = 10;
        const ROWS_PER_READ: u64 = 5;
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };

        let provider = Arc::new(PostgresProvider::new(13).with_pool_size(4, 5_000));
        let secret = Arc::new(SecretString::new(dsn));
        let operation = Arc::new(ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned(), "name".to_owned()],
            order_by: vec![OrderBy {
                field: "event_id".to_owned(),
                direction: SortDirection::Asc,
            }],
            row_limit: Some(ROWS_PER_READ),
            filter: None,
        });

        let mut tasks = Vec::new();
        for _ in 0..WORKERS {
            let provider = Arc::clone(&provider);
            let secret = Arc::clone(&secret);
            let operation = Arc::clone(&operation);
            tasks.push(tokio::spawn(async move {
                let cancellation = NeverCancelled;
                let mut rows = 0_u64;
                for _ in 0..ROUNDS {
                    let mut stream = provider
                        .read(&secret, &operation, &ParameterBag::default(), &cancellation)
                        .await
                        .expect("concurrent read");
                    while let Some(batch) = stream.next_batch().await.expect("concurrent batch") {
                        rows += u64::try_from(batch.num_rows()).expect("row count");
                    }
                }
                rows
            }));
        }
        let mut observed_rows = 0_u64;
        for task in tasks {
            observed_rows += task.await.expect("stress worker");
        }
        assert_eq!(observed_rows, WORKERS * ROUNDS * ROWS_PER_READ);

        let metrics = provider.metrics_snapshot();
        assert_eq!(metrics.pool_checkouts, WORKERS * ROUNDS);
        assert!((1..=4).contains(&metrics.pool_new_connections));
        assert_eq!(
            metrics.pool_reuses + metrics.pool_new_connections,
            metrics.pool_checkouts
        );
        assert_eq!(metrics.session_resets, metrics.pool_reuses);
        assert_eq!(
            metrics.schema_cache_hits + metrics.schema_cache_misses,
            metrics.pool_checkouts
        );
        assert_eq!(metrics.catalog_introspections, metrics.schema_cache_misses);
        assert_eq!(metrics.schema_token_checks, metrics.schema_cache_hits);
        assert_eq!(metrics.schema_cache_invalidations, 0);
        assert_eq!(metrics.schema_cache_evictions, 0);
        assert_eq!(metrics.read_typed_fast_paths, metrics.pool_checkouts);
        assert_eq!(metrics.pool_timeouts, 0);
        assert_eq!(metrics.invalidated_sessions, 0);
        assert_eq!(metrics.read_batches, WORKERS * ROUNDS);
        assert_eq!(metrics.read_rows, observed_rows);
        assert!(metrics.read_bytes > 0);
        assert!(provider.pool_idle_connections() <= 4);
    }

    #[tokio::test]
    async fn live_postgres_concurrent_cancellation_recovers_pool() {
        const WORKERS: usize = 4;
        let Ok(dsn) = std::env::var("PLENORA_TEST_POSTGRES_DSN") else {
            return;
        };
        let secret = Arc::new(SecretString::new(dsn));
        let setup = PostgresProvider::connect(&secret)
            .await
            .expect("setup client");
        setup
            .batch_execute(
                "CREATE OR REPLACE VIEW plenora_fixture.hardening_slow_events AS
                 SELECT value::bigint AS event_id
                 FROM generate_series(1, 100) AS value
                 CROSS JOIN LATERAL
                    pg_sleep((value * 0 + 50)::double precision / 1000)",
            )
            .await
            .expect("slow hardening view");

        let provider = Arc::new(PostgresProvider::new(100).with_pool_size(WORKERS, 5_000));
        let operation = Arc::new(ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("plenora_fixture".to_owned()),
                object: "hardening_slow_events".to_owned(),
                layer_id: None,
            },
            projection: vec!["event_id".to_owned()],
            order_by: Vec::new(),
            row_limit: None,
            filter: None,
        });
        let mut tasks = Vec::new();
        let cancellation_flag = Arc::new(AtomicBool::new(false));
        for _ in 0..WORKERS {
            let provider = Arc::clone(&provider);
            let secret = Arc::clone(&secret);
            let operation = Arc::clone(&operation);
            let flag = Arc::clone(&cancellation_flag);
            tasks.push(tokio::spawn(async move {
                let cancellation = AtomicCancellation(flag);
                let error = match provider
                    .read(&secret, &operation, &ParameterBag::default(), &cancellation)
                    .await
                {
                    Ok(mut stream) => stream
                        .next_batch_with_cancellation(&cancellation)
                        .await
                        .expect_err("cancelled slow stream"),
                    Err(error) => error,
                };
                assert_eq!(error.category, ErrorCategory::Cancelled);
            }));
        }
        tokio::time::timeout(StdDuration::from_secs(2), async {
            while provider.metrics_snapshot().pool_new_connections
                < u64::try_from(WORKERS).expect("workers")
            {
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        })
        .await
        .expect("all cancellation sessions connected");
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        cancellation_flag.store(true, AtomicOrdering::Release);
        for task in tasks {
            task.await.expect("cancellation worker");
        }

        provider
            .test_connection(&secret, &NeverCancelled)
            .await
            .expect("pool recovery");
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        let active: i64 = setup
            .query_one(
                "SELECT count(*)
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND state = 'active'
                   AND query LIKE '%hardening_slow_events%'
                   AND pid <> pg_backend_pid()",
                &[],
            )
            .await
            .expect("server cancellation state")
            .get(0);
        assert_eq!(active, 0);

        let metrics = provider.metrics_snapshot();
        assert!(metrics.cancellations >= u64::try_from(WORKERS).expect("workers"));
        assert!(metrics.invalidated_sessions >= u64::try_from(WORKERS).expect("workers"));
        assert!(metrics.pool_new_connections >= u64::try_from(WORKERS).expect("workers"));
        assert!(provider.pool_idle_connections() <= WORKERS);
    }

    #[tokio::test]
    #[ignore = "benchmark live esplicito"]
    async fn live_copy_vs_prepared_benchmark() {
        let dsn = std::env::var("PLENORA_TEST_POSTGRES_DSN").expect("live DSN");
        let secret = SecretString::new(dsn);
        let cancellation = NeverCancelled;
        let reader = PostgresProvider::new(1_000);
        let client = PostgresProvider::connect(&secret).await.expect("client");
        client
            .batch_execute(
                "DROP TABLE IF EXISTS plenora_fixture.bench_copy;
                 DROP TABLE IF EXISTS plenora_fixture.bench_binary;
                 DROP TABLE IF EXISTS plenora_fixture.bench_prepared",
            )
            .await
            .expect("benchmark cleanup");

        let mut copy_operation = write_operation(WriteMode::Create);
        copy_operation.target.object = "bench_copy".to_owned();
        let copy_provider =
            PostgresProvider::new(1_000).with_insert_mode(PostgresInsertMode::CopyText);
        let copy_stream = fixture_stream(&reader, &secret, &cancellation, Vec::new(), 1_000).await;
        let copy_prepared = copy_provider
            .prepare_write(
                &secret,
                &copy_operation,
                copy_stream.schema(),
                &cancellation,
            )
            .await
            .expect("copy prepare");
        let started = std::time::Instant::now();
        copy_provider
            .write(&secret, copy_prepared, copy_stream, &cancellation)
            .await
            .expect("copy write");
        let copy_micros = started.elapsed().as_micros();

        let mut binary_operation = write_operation(WriteMode::Create);
        binary_operation.target.object = "bench_binary".to_owned();
        let binary_provider =
            PostgresProvider::new(1_000).with_insert_mode(PostgresInsertMode::CopyBinary);
        let binary_stream =
            fixture_stream(&reader, &secret, &cancellation, Vec::new(), 1_000).await;
        let binary_prepared = binary_provider
            .prepare_write(
                &secret,
                &binary_operation,
                binary_stream.schema(),
                &cancellation,
            )
            .await
            .expect("binary prepare");
        let started = std::time::Instant::now();
        binary_provider
            .write(&secret, binary_prepared, binary_stream, &cancellation)
            .await
            .expect("binary write");
        let binary_micros = started.elapsed().as_micros();

        let mut prepared_operation = write_operation(WriteMode::Create);
        prepared_operation.target.object = "bench_prepared".to_owned();
        let prepared_provider =
            PostgresProvider::new(1_000).with_insert_mode(PostgresInsertMode::Prepared);
        let prepared_stream =
            fixture_stream(&reader, &secret, &cancellation, Vec::new(), 1_000).await;
        let prepared = prepared_provider
            .prepare_write(
                &secret,
                &prepared_operation,
                prepared_stream.schema(),
                &cancellation,
            )
            .await
            .expect("prepared prepare");
        let started = std::time::Instant::now();
        prepared_provider
            .write(&secret, prepared, prepared_stream, &cancellation)
            .await
            .expect("prepared write");
        let prepared_micros = started.elapsed().as_micros();

        let differences: i64 = client
            .query_one(
                "SELECT count(*) FROM (
                    (SELECT * FROM plenora_fixture.bench_copy
                     EXCEPT SELECT * FROM plenora_fixture.bench_prepared)
                    UNION ALL
                    (SELECT * FROM plenora_fixture.bench_prepared
                     EXCEPT SELECT * FROM plenora_fixture.bench_copy)
                    UNION ALL
                    (SELECT * FROM plenora_fixture.bench_binary
                     EXCEPT SELECT * FROM plenora_fixture.bench_copy)
                    UNION ALL
                    (SELECT * FROM plenora_fixture.bench_copy
                     EXCEPT SELECT * FROM plenora_fixture.bench_binary)
                ) AS differences",
                &[],
            )
            .await
            .expect("differential")
            .get(0);
        assert_eq!(differences, 0);
        println!(
            "{{\"rows\":1000,\"copy_text_micros\":{copy_micros},\"copy_binary_micros\":{binary_micros},\"prepared_micros\":{prepared_micros},\"differences\":0}}"
        );
    }
}
