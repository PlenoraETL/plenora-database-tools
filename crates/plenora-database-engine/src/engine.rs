//! Engine e sessione applicativa sopra il pool posseduto dal provider.

use crate::result::QueryResult;
use crate::BoundStatement;
use crate::MetaData;
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::metrics_recorder::{
    noop_recorder, MetricEvent, MetricName, MetricTags, MetricValue, OperationKind, SharedRecorder,
};
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind};
use plenora_database_core::provider::{ConnectionInfo, Provider, ProviderFuture, SecretString};
use plenora_database_core::transaction::{
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, ResourceBudget, Result,
    RetryDisposition, Row,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

/// Opzioni di cache del layer applicativo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOptions {
    metadata_cache_ttl: Duration,
}

impl EngineOptions {
    #[must_use]
    pub const fn new(metadata_cache_ttl: Duration) -> Self {
        Self { metadata_cache_ttl }
    }

    #[must_use]
    pub const fn metadata_cache_ttl(self) -> Duration {
        self.metadata_cache_ttl
    }
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineStatistics {
    pub sessions_opened: u64,
    pub active_sessions: u64,
    pub disposed: bool,
}

#[derive(Debug, Default)]
struct Lifecycle {
    sessions_opened: u64,
    active_sessions: u64,
    disposed: bool,
}

struct Credentials {
    secret: SecretString,
    generation: u64,
    capabilities: Option<ProviderCapabilities>,
    metadata: HashMap<MetadataCacheKey, CachedMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetadataCacheKey {
    catalog: Option<String>,
    schema: Option<String>,
    object: String,
}

impl From<&ObjectRef> for MetadataCacheKey {
    fn from(source: &ObjectRef) -> Self {
        Self {
            catalog: source.catalog.clone(),
            schema: source.schema.clone(),
            object: source.object.clone(),
        }
    }
}

struct CachedMetadata {
    observed_at: Instant,
    value: Arc<MetaData>,
}

struct EngineInner {
    provider: Arc<dyn Provider>,
    credentials: RwLock<Credentials>,
    lifecycle: Mutex<Lifecycle>,
    cancellation: CancellationToken,
    recorder: SharedRecorder,
    options: EngineOptions,
}

fn mutex<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read<T>(value: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    value.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(value: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    value.write().unwrap_or_else(PoisonError::into_inner)
}

/// Punto d'ingresso condivisibile per un provider e il pool che possiede.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("provider", &self.provider_kind())
            .field("statistics", &self.statistics())
            .finish_non_exhaustive()
    }
}

impl Engine {
    #[must_use]
    pub fn new(provider: Arc<dyn Provider>, secret: SecretString) -> Self {
        Self::with_options(provider, secret, noop_recorder(), EngineOptions::default())
    }

    #[must_use]
    pub fn with_recorder(
        provider: Arc<dyn Provider>,
        secret: SecretString,
        recorder: SharedRecorder,
    ) -> Self {
        Self::with_options(provider, secret, recorder, EngineOptions::default())
    }

    #[must_use]
    pub fn with_options(
        provider: Arc<dyn Provider>,
        secret: SecretString,
        recorder: SharedRecorder,
        options: EngineOptions,
    ) -> Self {
        Self {
            inner: Arc::new(EngineInner {
                provider,
                credentials: RwLock::new(Credentials {
                    secret,
                    generation: 0,
                    capabilities: None,
                    metadata: HashMap::new(),
                }),
                lifecycle: Mutex::new(Lifecycle::default()),
                cancellation: CancellationToken::new(),
                recorder,
                options,
            }),
        }
    }

    #[must_use]
    pub fn provider_kind(&self) -> ProviderKind {
        self.inner.provider.kind()
    }

    #[must_use]
    pub fn statistics(&self) -> EngineStatistics {
        let state = mutex(&self.inner.lifecycle);
        EngineStatistics {
            sessions_opened: state.sessions_opened,
            active_sessions: state.active_sessions,
            disposed: state.disposed,
        }
    }

    #[must_use]
    pub fn metadata_cache_entries(&self) -> usize {
        read(&self.inner.credentials).metadata.len()
    }

    /// Impedisce nuovi lavori e cancella sessioni e operazioni in corso.
    ///
    /// Il pool viene rilasciato quando cade l'ultimo handle provider ancora
    /// usato da una transazione gia aperta.
    pub fn dispose(&self) {
        let changed = {
            let mut state = mutex(&self.inner.lifecycle);
            let changed = !state.disposed;
            state.disposed = true;
            changed
        };
        if changed {
            self.inner.cancellation.cancel();
            let mut credentials = write(&self.inner.credentials);
            credentials.capabilities.take();
            credentials.metadata.clear();
        }
    }

    /// Sostituisce il secret senza inserirlo nell'identita dell'engine.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidConfiguration` se l'engine e gia chiuso.
    pub fn rotate_secret(&self, secret: SecretString) -> Result<()> {
        self.ensure_open(ErrorPhase::Connect)?;
        let mut credentials = write(&self.inner.credentials);
        let generation = credentials
            .generation
            .checked_add(1)
            .ok_or_else(|| DatabaseError::resource_limit("generazione secret esaurita"))?;
        credentials.secret = secret;
        credentials.generation = generation;
        credentials.capabilities.take();
        credentials.metadata.clear();
        drop(credentials);
        Ok(())
    }

    /// Verifica la connessione usando il secret corrente.
    ///
    /// # Errors
    ///
    /// Propaga l'errore redatto del provider o il dispose dell'engine.
    pub async fn health_check(&self, cancellation: &CancellationToken) -> Result<ConnectionInfo> {
        self.ensure_open(ErrorPhase::Connect)?;
        let linked = CancellationToken::linked(&[&self.inner.cancellation, cancellation]);
        let secret = read(&self.inner.credentials).secret.clone();
        let started = Instant::now();
        let result = self.inner.provider.test_connection(&secret, &linked).await;
        self.record_duration(OperationKind::Connect, started);
        result
    }

    /// Legge le capability, riusando la misura precedente salvo `refresh`.
    ///
    /// # Errors
    ///
    /// Propaga l'errore redatto del provider o il dispose dell'engine.
    pub async fn capabilities(
        &self,
        refresh: bool,
        cancellation: &CancellationToken,
    ) -> Result<ProviderCapabilities> {
        const MAX_ATTEMPTS: usize = 3;
        for _ in 0..MAX_ATTEMPTS {
            self.ensure_open(ErrorPhase::Probe)?;
            let (secret, generation, cached) = {
                let credentials = read(&self.inner.credentials);
                (
                    credentials.secret.clone(),
                    credentials.generation,
                    credentials.capabilities.clone(),
                )
            };
            if !refresh {
                if let Some(cached) = cached {
                    return Ok(cached);
                }
            }
            let linked = CancellationToken::linked(&[&self.inner.cancellation, cancellation]);
            let started = Instant::now();
            let result = self
                .inner
                .provider
                .probe_capabilities(&secret, &linked)
                .await;
            self.record_duration(OperationKind::Probe, started);
            self.ensure_open(ErrorPhase::Probe)?;
            let mut credentials = write(&self.inner.credentials);
            if credentials.generation != generation {
                continue;
            }
            match result {
                Ok(capabilities) => {
                    credentials.capabilities = Some(capabilities.clone());
                    drop(credentials);
                    return Ok(capabilities);
                }
                Err(error) => {
                    drop(credentials);
                    self.inner.recorder.record(
                        MetricEvent::new(MetricName::DbCapabilityFailure, MetricValue::Count(1))
                            .with_tags(self.tags(OperationKind::Probe)),
                    );
                    return Err(error);
                }
            }
        }
        let mut error = DatabaseError::new(
            ErrorCategory::Transient,
            ErrorPhase::Probe,
            Some(self.provider_kind()),
            "secret ruotato ripetutamente durante il probe capability",
        );
        error.retry = RetryDisposition::Safe;
        Err(error)
    }

    /// Riflette un oggetto nel catalogo tipizzato, con cache a TTL.
    ///
    /// `refresh` forza una nuova osservazione. Una rotazione del secret
    /// invalida la cache e scarta ogni risposta iniziata con il secret vecchio.
    ///
    /// # Errors
    ///
    /// Propaga errori redatti del provider e documenti incompatibili.
    pub async fn reflect_table(
        &self,
        source: &ObjectRef,
        refresh: bool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<MetaData>> {
        const MAX_ATTEMPTS: usize = 3;
        let key = MetadataCacheKey::from(source);
        for _ in 0..MAX_ATTEMPTS {
            self.ensure_open(ErrorPhase::Probe)?;
            let (secret, generation, cached) = {
                let credentials = read(&self.inner.credentials);
                let cached = credentials.metadata.get(&key).and_then(|entry| {
                    (entry.observed_at.elapsed() < self.inner.options.metadata_cache_ttl)
                        .then(|| Arc::clone(&entry.value))
                });
                (credentials.secret.clone(), credentials.generation, cached)
            };
            if !refresh {
                if let Some(cached) = cached {
                    return Ok(cached);
                }
            }
            let linked = CancellationToken::linked(&[&self.inner.cancellation, cancellation]);
            let operation = Operation::DatabaseDescribeObject {
                source: source.clone(),
            };
            let started = Instant::now();
            let inspection = self
                .inner
                .provider
                .inspect(&secret, &operation, &linked)
                .await;
            self.record_duration(OperationKind::Probe, started);
            self.ensure_open(ErrorPhase::Probe)?;
            if read(&self.inner.credentials).generation != generation {
                continue;
            }
            let metadata = Arc::new(MetaData::from_inspection(
                self.provider_kind(),
                source,
                inspection?,
            )?);
            let mut credentials = write(&self.inner.credentials);
            if credentials.generation != generation {
                continue;
            }
            credentials.metadata.insert(
                key.clone(),
                CachedMetadata {
                    observed_at: Instant::now(),
                    value: Arc::clone(&metadata),
                },
            );
            drop(credentials);
            return Ok(metadata);
        }
        let mut error = DatabaseError::new(
            ErrorCategory::Transient,
            ErrorPhase::Probe,
            Some(self.provider_kind()),
            "secret ruotato ripetutamente durante la reflection",
        );
        error.retry = RetryDisposition::Safe;
        Err(error)
    }

    /// Invalida una voce o l'intera cache metadata e restituisce quante voci
    /// sono state rimosse.
    #[must_use]
    pub fn invalidate_metadata(&self, source: Option<&ObjectRef>) -> usize {
        let mut credentials = write(&self.inner.credentials);
        if let Some(source) = source {
            usize::from(
                credentials
                    .metadata
                    .remove(&MetadataCacheKey::from(source))
                    .is_some(),
            )
        } else {
            let removed = credentials.metadata.len();
            credentials.metadata.clear();
            removed
        }
    }

    /// Crea un confine di lavoro non condivisibile fra task concorrenti.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidConfiguration` dopo `dispose`.
    pub fn session(&self) -> Result<Session> {
        let mut state = mutex(&self.inner.lifecycle);
        if state.disposed {
            return Err(self.closed_error(ErrorPhase::Connect));
        }
        state.sessions_opened = state
            .sessions_opened
            .checked_add(1)
            .ok_or_else(|| DatabaseError::resource_limit("contatore sessioni esaurito"))?;
        state.active_sessions = state
            .active_sessions
            .checked_add(1)
            .ok_or_else(|| DatabaseError::resource_limit("contatore sessioni attive esaurito"))?;
        drop(state);
        Ok(Session {
            inner: Arc::clone(&self.inner),
            cancellation: self.inner.cancellation.child_token(),
            closed: false,
        })
    }

    fn ensure_open(&self, phase: ErrorPhase) -> Result<()> {
        if mutex(&self.inner.lifecycle).disposed {
            Err(self.closed_error(phase))
        } else {
            Ok(())
        }
    }

    fn closed_error(&self, phase: ErrorPhase) -> DatabaseError {
        DatabaseError::new(
            ErrorCategory::InvalidConfiguration,
            phase,
            Some(self.provider_kind()),
            "engine chiuso",
        )
    }

    fn tags(&self, operation: OperationKind) -> MetricTags {
        MetricTags::new()
            .with_provider(self.provider_kind())
            .with_operation(operation)
    }

    fn record_duration(&self, operation: OperationKind, started: Instant) {
        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.inner.recorder.record(
            MetricEvent::new(
                MetricName::DbOperationDuration,
                MetricValue::DurationMs(elapsed),
            )
            .with_tags(self.tags(operation)),
        );
    }
}

/// Sequenza di lavoro esclusiva creata da [`Engine`].
pub struct Session {
    inner: Arc<EngineInner>,
    cancellation: CancellationToken,
    closed: bool,
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("provider", &self.inner.provider.kind())
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl Session {
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Token figlio collegato alla chiusura della sessione e dell'engine.
    ///
    /// Gli adapter possono usarlo per operazioni specializzate che non
    /// attraversano un `SessionTransaction`, mantenendo lo stesso lifecycle.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.child_token()
    }

    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.cancellation.cancel();
            let mut state = mutex(&self.inner.lifecycle);
            state.active_sessions = state.active_sessions.saturating_sub(1);
        }
    }

    /// Apre una transazione che prende in prestito esclusivamente la sessione.
    ///
    /// # Errors
    ///
    /// Propaga gli errori di lifecycle, budget, cancellazione e provider.
    pub async fn begin_transaction<'session>(
        &'session mut self,
        options: &TransactionOptions,
        budget: &ResourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<SessionTransaction<'session>> {
        self.ensure_open()?;
        let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
        let secret = read(&self.inner.credentials).secret.clone();
        let scope = self
            .inner
            .provider
            .begin_transaction(&secret, options, budget, &linked)
            .await?;
        Ok(SessionTransaction {
            scope,
            cancellation: linked,
            _session: std::marker::PhantomData,
        })
    }

    /// Consuma la sessione e apre una transazione posseduta.
    ///
    /// Questa forma serve ai binding e agli executor che devono conservare la
    /// transazione oltre lo stack frame che l'ha aperta. Mantiene le stesse
    /// garanzie della forma borrowed: una sola transazione per sessione,
    /// cancellazione collegata all'engine e chiusura del lifecycle al drop.
    ///
    /// # Errors
    ///
    /// Propaga gli errori di lifecycle, budget, cancellazione e provider.
    pub async fn begin_owned_transaction(
        self,
        options: &TransactionOptions,
        budget: &ResourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<OwnedSessionTransaction> {
        self.ensure_open()?;
        let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
        let secret = read(&self.inner.credentials).secret.clone();
        let scope = self
            .inner
            .provider
            .begin_transaction(&secret, options, budget, &linked)
            .await?;
        Ok(OwnedSessionTransaction {
            scope,
            cancellation: linked.child_token(),
            stream_cancellation: linked,
            _session: self,
        })
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed || mutex(&self.inner.lifecycle).disposed {
            Err(DatabaseError::new(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Connect,
                Some(self.inner.provider.kind()),
                "sessione chiusa",
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

/// Transazione `'static` che possiede il lifecycle della propria [`Session`].
///
/// Implementa il contratto provider-neutral [`TransactionScope`], quindi i
/// binding possono riusare gli stessi executor delle transazioni native. La
/// sessione viene chiusa soltanto dopo commit, rollback o quarantena al drop.
pub struct OwnedSessionTransaction {
    scope: Box<dyn TransactionScope>,
    cancellation: CancellationToken,
    stream_cancellation: CancellationToken,
    _session: Session,
}

impl TransactionScope for OwnedSessionTransaction {
    fn provider_kind(&self) -> ProviderKind {
        self.scope.provider_kind()
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'a, u64> {
        Box::pin(async move {
            let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
            self.scope.execute(statement, &linked).await
        })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
            self.scope.query(statement, &linked).await
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        statement: &'a Statement,
        batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        self.stream_cancellation = CancellationToken::linked(&[&self.cancellation, cancellation]);
        self.scope
            .query_stream(statement, batch_size, &self.stream_cancellation)
    }

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'a, ()> {
        Box::pin(async move {
            let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
            self.scope.savepoint(name, &linked).await
        })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'a, ()> {
        Box::pin(async move {
            let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
            self.scope.rollback_to_savepoint(name, &linked).await
        })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'a, ()> {
        Box::pin(async move {
            let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
            self.scope.release_savepoint(name, &linked).await
        })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'a, ()> {
        Box::pin(async move {
            let linked = CancellationToken::linked(&[&self.cancellation, cancellation]);
            self.scope
                .execute_conditional_update(request, &linked)
                .await
        })
    }

    fn commit(
        self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            let Self {
                scope,
                cancellation: internal,
                stream_cancellation: _,
                _session,
            } = *self;
            let linked = CancellationToken::linked(&[&internal, cancellation]);
            scope.commit(&linked).await
        })
    }

    fn rollback(
        self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<'_, ()> {
        Box::pin(async move {
            let Self {
                scope,
                cancellation: internal,
                stream_cancellation: _,
                _session,
            } = *self;
            let linked = CancellationToken::linked(&[&internal, cancellation]);
            scope.rollback(&linked).await
        })
    }
}

/// Transazione vincolata al lifecycle esclusivo di una [`Session`].
pub struct SessionTransaction<'session> {
    scope: Box<dyn TransactionScope>,
    cancellation: CancellationToken,
    _session: std::marker::PhantomData<&'session mut Session>,
}

impl SessionTransaction<'_> {
    #[must_use]
    pub fn provider_kind(&self) -> ProviderKind {
        self.scope.provider_kind()
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Esegue un'operazione generica sullo scope mantenendo la cancellazione
    /// collegata al lifecycle della sessione e dell'engine.
    ///
    /// Serve agli adapter che condividono executor provider-neutral senza
    /// esporre direttamente ne lo scope ne il token interno.
    ///
    /// # Errors
    ///
    /// Propaga l'errore redatto restituito dall'operazione.
    pub async fn run<R, F>(&mut self, work: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a mut dyn TransactionScope,
            &'a CancellationToken,
        ) -> ProviderFuture<'a, R>,
        R: Send,
    {
        work(self.scope.as_mut(), &self.cancellation).await
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn execute(&mut self, statement: &Statement) -> Result<u64> {
        self.scope.execute(statement, &self.cancellation).await
    }

    /// Esegue uno statement immutabile con bind separati dal template.
    ///
    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn execute_bound(&mut self, statement: &BoundStatement) -> Result<u64> {
        self.execute(statement.legacy()).await
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn query(&mut self, statement: &Statement) -> Result<Vec<Row>> {
        self.scope.query(statement, &self.cancellation).await
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn query_bound(&mut self, statement: &BoundStatement) -> Result<Vec<Row>> {
        self.query(statement.legacy()).await
    }

    /// Apre uno stream che mantiene il prestito esclusivo della transazione.
    ///
    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn query_stream<'stream>(
        &'stream mut self,
        statement: &'stream Statement,
        batch_size: u32,
    ) -> Result<SessionRowStream<'stream>> {
        let cancellation = &self.cancellation;
        let inner = self
            .scope
            .query_stream(statement, batch_size, cancellation)
            .await?;
        Ok(SessionRowStream {
            inner,
            cancellation,
        })
    }

    /// Apre uno stream sopra un template immutabile e i suoi bind.
    ///
    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn query_stream_bound<'stream>(
        &'stream mut self,
        statement: &'stream BoundStatement,
        batch_size: u32,
    ) -> Result<SessionRowStream<'stream>> {
        self.query_stream(statement.legacy(), batch_size).await
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn savepoint(&mut self, name: &str) -> Result<()> {
        self.scope.savepoint(name, &self.cancellation).await
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        self.scope
            .rollback_to_savepoint(name, &self.cancellation)
            .await
    }

    /// Esegue una query e applica il protocollo uniforme di consumo.
    ///
    /// # Errors
    ///
    /// Propaga l'errore del provider o metadata incoerenti fra le righe.
    pub async fn query_result(&mut self, statement: &Statement) -> Result<QueryResult> {
        QueryResult::from_rows(self.query(statement).await?)
    }

    /// # Errors
    ///
    /// Propaga l'errore del provider o metadata incoerenti fra le righe.
    pub async fn query_result_bound(&mut self, statement: &BoundStatement) -> Result<QueryResult> {
        QueryResult::from_rows(self.query_bound(statement).await?)
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto del transaction scope.
    pub async fn release_savepoint(&mut self, name: &str) -> Result<()> {
        self.scope.release_savepoint(name, &self.cancellation).await
    }

    /// # Errors
    ///
    /// Propaga `NotFound`, `ConcurrentModification` o l'errore del provider.
    pub async fn execute_conditional_update(
        &mut self,
        request: ConditionalUpdate<'_>,
    ) -> Result<()> {
        self.scope
            .execute_conditional_update(request, &self.cancellation)
            .await
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto o un `OutcomeUnknown` nel valore di ritorno.
    pub async fn commit(self) -> Result<CommitOutcome> {
        let Self {
            scope,
            cancellation,
            _session: _,
        } = self;
        scope.commit(&cancellation).await
    }

    /// # Errors
    ///
    /// Propaga l'errore redatto del rollback.
    pub async fn rollback(self) -> Result<()> {
        let Self {
            scope,
            cancellation,
            _session: _,
        } = self;
        scope.rollback(&cancellation).await
    }
}

pub struct SessionRowStream<'stream> {
    inner: Box<dyn RowStream + Send + 'stream>,
    cancellation: &'stream CancellationToken,
}

impl SessionRowStream<'_> {
    /// # Errors
    ///
    /// Propaga l'errore redatto del cursor o la cancellazione collegata.
    pub async fn next_batch(&mut self) -> Result<Option<Vec<Row>>> {
        self.inner.next_batch(self.cancellation).await
    }

    /// Legge un batch attraverso lo stesso protocollo del risultato
    /// bufferizzato, senza caricare lo stream completo in memoria.
    ///
    /// # Errors
    ///
    /// Propaga l'errore del cursor o metadata incoerenti nel batch.
    pub async fn next_result(&mut self) -> Result<Option<QueryResult>> {
        self.next_batch()
            .await?
            .map(QueryResult::from_rows)
            .transpose()
    }
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
