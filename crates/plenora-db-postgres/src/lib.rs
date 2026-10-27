//! Driver pilota PostgreSQL/`PostGIS`.
//!
//! Il driver di riferimento copre connessione, capability, introspezione, read
//! batch-bounded verso Arrow/GeoArrow-WKB e tutte le modalità di scrittura
//! definite dal core.

mod age;
mod arrow;
mod catalog;
mod connection;
mod control;
mod error;
mod field_contract;
mod field_identity;
mod metrics;
mod parameter_codec;
mod pool;
mod preflight;
mod query_execution;
mod query_plan;
mod read_stream;
mod schema_cache;
mod spatial;
mod spatial_probe;
mod transaction;
mod types;
mod write;

pub use age::parse_agtype;
pub use spatial::{build_spatial_select, spatial_reference};

pub use connection::{PostgresNetworkOptions, PostgresTlsConfig, PostgresTlsMode};
pub use metrics::PostgresMetricsSnapshot;
pub use schema_cache::PostgresSchemaToken;

/// La compilazione offline del piano di scrittura: la prima cosa che
/// `prepare_write` esegue, senza server.
///
/// Giudica lo schema (REJ-001), le chiavi del piano e ogni colonna come la
/// scrittura (SRID di colonna, limiti di supporto decisi dallo schema). Un dry
/// run che la salta direbbe `dry_run` su ingressi che la scrittura
/// rifiuterebbe. Non legge il catalogo del bersaglio: i rifiuti decisi da
/// `spatial_ref_sys` restano della scrittura (fase `prepare`).
///
/// # Errors
///
/// Gli stessi di `prepare_write` prima del contatto con il server.
pub fn compile_write_plan(input_schema: &SchemaRef, operation: &WriteOperation) -> Result<()> {
    write::prepare_state(input_schema, operation).map(|_| ())
}

/// Che cosa un dry run ha deciso, e che cosa no.
#[derive(Debug, Clone)]
pub struct PostgresWriteDryRun {
    /// Batch dell'ingresso giudicati, ogni valore compreso.
    pub batches: u64,
    /// Righe dell'ingresso giudicate.
    pub rows: u64,
    /// `Some` se il catalogo del bersaglio e stato letto (con una DSN): il
    /// `LossReport` che la scrittura avrebbe. `None` se il dry run e
    /// offline, e le classi che decide il catalogo non sono state decise.
    pub loss_report: Option<plenora_database_core::loss::LossReport>,
    /// Se una policy di row-level security decide sull'ingresso: in quel
    /// caso il dry run non decide l'esito, e lo dice. Offline e
    /// `NotPerformed`: il catalogo non e stato letto.
    pub row_security: write::target::RowSecurity,
}

impl PostgresProvider {
    /// Il dry run della scrittura: decide cio che `prepare_write` e `write`
    /// deciderebbero sullo stesso ingresso, senza effetti.
    ///
    /// Con `secret` esegue `prepare_write` stesso — compilazione del piano e
    /// letture del catalogo (`spatial_ref_sys`, esistenza del target, colonne,
    /// perdite di mapping), tutte in sola lettura — poi legge sul catalogo
    /// cio che la scrittura chiede al target (`write::target`: schema,
    /// privilegi, proprieta, colonne dello statement, indice unico
    /// dell'upsert) e confronta gli id dei campi con quelli registrati. Ogni
    /// rifiuto esce nel punto in cui la scrittura lo darebbe. Senza `secret`
    /// compila soltanto il piano: il risultato lo dichiara (`loss_report:
    /// None`) e non vale per le classi del catalogo. In entrambi i casi legge
    /// ogni batch e ne giudica ogni valore con le funzioni della scrittura
    /// (`write::dry_run`).
    ///
    /// Decide la regola R come la scrittura (`write::visibility`): Replace,
    /// Update e `DeleteByKeys` che non vedrebbero tutte le righe si
    /// rifiutano qui, con lo stesso errore.
    ///
    /// Non decide cio che decide il server scrivendo (l'elenco chiuso e in
    /// `write::target`): gli altri trigger, vincoli `CHECK` ed esclusioni, unicita e
    /// foreign key sui valori, vincoli differiti, conversioni della colonna,
    /// timeout e lock concorrenti; e, dove una policy RLS decide sui valori,
    /// l'esito della policy (`row_security: NotPerformed`).
    ///
    /// # Errors
    ///
    /// Il rifiuto che la scrittura darebbe, con categoria e fase della
    /// scrittura e `remote_effect: none`.
    pub async fn dry_run_write(
        &self,
        secret: Option<&SecretString>,
        operation: &WriteOperation,
        mut input: Box<dyn BatchStream>,
        budget: &ResourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<PostgresWriteDryRun> {
        let schema = input.schema();
        let mut gate = write::StatementGate::default();
        let mut row_security = write::target::RowSecurity::NotPerformed;
        let (state, loss_report, prepared) = if let Some(secret) = secret {
            let mut prepared = self
                .prepare_write(secret, operation, Arc::clone(&schema), budget, cancellation)
                .await?;
            let state = prepared
                .take_driver_state::<write::PreparedPostgresWrite>()
                .ok_or_else(|| {
                    DatabaseError::invalid_plan("stato prepared write PostgreSQL assente")
                })?;
            // Cio che la scrittura chiede al target, sul catalogo e nel suo
            // ordine: DDL e svuotamento, poi la registrazione degli id dei
            // campi, poi lo statement e la prima riga (`gate`).
            let client = self.connect_session(secret).await?;
            let row_scoped = operation.mode == plenora_database_core::plan::WriteMode::Append
                && operation.transaction_profile
                    == plenora_database_core::plan::TransactionProfile::SingleTransaction
                && input.declared_input_rows().is_some();
            let verdict = write::target::inspect(
                client.client()?,
                operation,
                &schema,
                write::target::WritePath::of(operation, row_scoped, self.insert_mode),
                self.schema_evolution,
            )
            .await?;
            if let Some(error) = verdict.before_input {
                return Err(error);
            }
            field_identity::check(
                client.client()?,
                operation,
                &schema,
                verdict.may_lock,
                verdict.owner,
            )
            .await?;
            drop(client);
            gate.at_statement = verdict.at_statement;
            gate.at_first_row = verdict.at_first_row;
            row_security = verdict.row_security;
            let report = prepared.loss_report.clone();
            (state, Some(report), Some(prepared))
        } else {
            (write::prepare_state(&schema, operation)?, None, None)
        };
        let control = BudgetCancellation::new(cancellation, budget)?;
        let verified = write::verify_input(
            &state,
            operation,
            input.as_mut(),
            budget,
            control.token(),
            write::InputLimits {
                insert_mode: self.insert_mode,
                max_batch_bytes: self.max_batch_bytes,
                max_wkb_cell_bytes: self.max_wkb_cell_bytes,
            },
            gate,
        )
        .await?;
        // Le prenotazioni del piano restano vive mentre si giudica
        // l'ingresso, come nella scrittura.
        drop(prepared);
        Ok(PostgresWriteDryRun {
            batches: verified.batches,
            rows: verified.rows,
            loss_report,
            row_security,
        })
    }
}

use arrow_schema::SchemaRef;
#[cfg(test)]
use arrow_schema::{Field, Schema};
use catalog::{
    capability_document, describe_object_metadata, list_catalogs, list_objects, list_schemas,
    load_columns_and_token, schema_token,
};
use connection::connection_fingerprint;
#[cfg(test)]
use connection::{
    configure_session_startup, connect as open_connection,
    connect_with_tls as open_connection_with_tls, connection_config, connection_config_for_mode,
};
use error::{check_cancelled, classify_error, public_error};
use metrics::PostgresMetrics;
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::graph::{validate_graph_name, AgeAdminCapabilities, AgeCapabilities};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    ObjectRef, Operation, ProviderKind, ReadOperation, WriteOperation,
};
#[cfg(test)]
use plenora_database_core::protocol;
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::resource::ResourceBudget;
#[cfg(test)]
use plenora_database_core::resource::ResourceKind;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_engine::{validate_prepared_budget, ContractLeases};
use pool::{PooledClient, PostgresPool, PostgresSessionOptions};
#[cfg(test)]
use read_stream::batch_memory_bytes;
use read_stream::BudgetCancellation;
use schema_cache::{PostgresSchemaCache, SchemaCacheKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(test)]
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
#[cfg(test)]
use std::time::Duration as StdDuration;
#[cfg(test)]
use tokio_postgres::config::SslMode;
#[cfg(test)]
use tokio_postgres::Client;
use types::ColumnSpec;

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
    RollbackAcknowledgementLost,
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
pub enum PostgresSchemaEvolution {
    Disabled,
    AddNullableColumns,
}

impl Default for PostgresProvider {
    /// TLS `Require` + trust store `WebPKI` pubblico (secure-by-default).
    ///
    /// Per test/dev locali senza TLS usare esplicitamente
    /// [`PostgresProvider::insecure_local`].
    ///
    /// Per CA private / mTLS: `default().with_tls_config(custom)`.
    fn default() -> Self {
        Self::for_profile(PostgresPerformanceProfile::LowLatency)
    }
}

impl PostgresProvider {
    /// Sonda il contratto AGE separato dal documento capability relazionale.
    ///
    /// # Errors
    ///
    /// Restituisce un errore classificato se connessione o probe non sono
    /// completabili. Un AGE assente o non qualificato produce invece un
    /// documento fail-closed valido.
    pub async fn age_capabilities(
        &self,
        secret: &SecretString,
        cancellation: &CancellationToken,
    ) -> Result<plenora_database_core::graph::AgeCapabilities> {
        check_cancelled(cancellation, ErrorPhase::Probe)?;
        let client = self.connect_session(secret).await?;
        age::probe_age_capabilities(client.client()?).await
    }

    /// Capability amministrative AGE additive, separate dal documento graph
    /// v1 per non cambiarne il wire contract.
    ///
    /// # Errors
    ///
    /// Restituisce un errore classificato se connessione o probe falliscono.
    pub async fn age_admin_capabilities(
        &self,
        secret: &SecretString,
        cancellation: &CancellationToken,
    ) -> Result<AgeAdminCapabilities> {
        Ok(AgeAdminCapabilities::from_age(
            &self.age_capabilities(secret, cancellation).await?,
        ))
    }

    async fn qualified_age_session(
        &self,
        secret: &SecretString,
        cancellation: &CancellationToken,
    ) -> Result<PooledClient> {
        check_cancelled(cancellation, ErrorPhase::Prepare)?;
        let mut client = self.connect_session(secret).await?;
        let capabilities: AgeCapabilities = age::probe_age_capabilities(client.client()?).await?;
        if !capabilities.qualified() {
            client.invalidate();
            return Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                ErrorPhase::Prepare,
                "Apache AGE 1.7.0 su PostgreSQL 18 non e disponibile o qualificato",
            ));
        }
        // Le funzioni amministrative AGE risolvono internamente operatori e
        // opclass non qualificati. Questa e una sessione dedicata presa dal
        // pool: prima del riuso `connect_session` esegue sempre `DISCARD ALL`,
        // quindi il search_path non attraversa il confine verso il chiamante.
        client
            .client()?
            .execute(
                "SELECT set_config('search_path', $1, false)",
                &[&"ag_catalog,\"$user\",public"],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
        Ok(client)
    }

    /// Elenca i grafi AGE in ordine stabile.
    ///
    /// # Errors
    ///
    /// Restituisce `Unsupported` fuori dalla coppia qualificata, oppure un
    /// errore classificato di connessione, cancellazione o lettura.
    pub async fn list_graphs(
        &self,
        secret: &SecretString,
        cancellation: &CancellationToken,
    ) -> Result<Vec<String>> {
        let mut client = self.qualified_age_session(secret, cancellation).await?;
        let Some(result) = control::select_with_cancellation(
            client.client()?.query(
                "SELECT name::text FROM ag_catalog.ag_graph ORDER BY name",
                &[],
            ),
            cancellation,
        )
        .await
        else {
            client.invalidate();
            drop(client);
            return Err(DatabaseError::interrupted(
                cancellation,
                Some(ProviderKind::Postgres),
                ErrorPhase::Read,
                "elenco grafi AGE interrotto",
            ));
        };
        let rows = result.map_err(|error| classify_error(ErrorPhase::Read, &error));
        drop(client);
        let graphs = rows?.into_iter().map(|row| row.get(0)).collect();
        Ok(graphs)
    }

    /// Crea un grafo AGE usando un parametro PostgreSQL, mai interpolazione.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per un nome non valido, `Unsupported` fuori
    /// dalla coppia qualificata, oppure un errore classificato del server.
    pub async fn create_graph(
        &self,
        secret: &SecretString,
        graph: &str,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        validate_graph_name(graph)?;
        let mut client = self.qualified_age_session(secret, cancellation).await?;
        let Some(result) = control::select_with_cancellation(
            client
                .client()?
                .query("SELECT ag_catalog.create_graph($1::name)", &[&graph]),
            cancellation,
        )
        .await
        else {
            client.invalidate();
            drop(client);
            return Err(DatabaseError::interrupted(
                cancellation,
                Some(ProviderKind::Postgres),
                ErrorPhase::Write,
                "creazione grafo AGE interrotta",
            )
            // Il comando e in volo: l'effetto non e provato.
            .after_interrupted_send());
        };
        let outcome = result
            .map(|_| ())
            .map_err(|error| classify_error(ErrorPhase::Write, &error));
        drop(client);
        outcome
    }

    /// Elimina un grafo AGE. `cascade` resta una scelta esplicita del
    /// chiamante e viene bindata separatamente dal nome.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per un nome non valido, `Unsupported` fuori
    /// dalla coppia qualificata, oppure un errore classificato del server.
    pub async fn drop_graph(
        &self,
        secret: &SecretString,
        graph: &str,
        cascade: bool,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        validate_graph_name(graph)?;
        let mut client = self.qualified_age_session(secret, cancellation).await?;
        let Some(result) = control::select_with_cancellation(
            client.client()?.query(
                "SELECT ag_catalog.drop_graph($1::name, $2)",
                &[&graph, &cascade],
            ),
            cancellation,
        )
        .await
        else {
            client.invalidate();
            drop(client);
            return Err(DatabaseError::interrupted(
                cancellation,
                Some(ProviderKind::Postgres),
                ErrorPhase::Write,
                "eliminazione grafo AGE interrotta",
            )
            // Il comando e in volo: l'effetto non e provato.
            .after_interrupted_send());
        };
        let outcome = result
            .map(|_| ())
            .map_err(|error| classify_error(ErrorPhase::Write, &error));
        drop(client);
        outcome
    }
    /// Costruzione base condivisa da `default()`, `new()`,
    /// `for_profile()`, `insecure_local()`.
    ///
    /// TLS secure-by-default: il baseline è `Require` +
    /// `WebPKI`. `insecure_local()` esplicito sovrascrive dopo.
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
            tls_mode: PostgresTlsMode::Require,
            tls_config: PostgresTlsConfig::webpki(),
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

    /// Provider **senza TLS** (`PostgresTlsMode::Disabled`).
    ///
    /// **Solo per test/dev locali** contro `docker-compose` o setup
    /// dev-env noti. In produzione l'operatore ha responsabilità di
    /// non usare questo costruttore: il nome è esplicito per non
    /// nasconderlo dietro un default.
    ///
    /// Scorciatoia esplicita per ambienti locali che non offrono TLS. In ogni
    /// altro caso il default [`TlsMode::Require`] resta intenzionalmente
    /// sicuro.
    #[must_use]
    pub fn insecure_local() -> Self {
        // Parte dal baseline secure e disattiva TLS esplicitamente.
        Self::for_profile(PostgresPerformanceProfile::LowLatency)
            .with_tls_mode(PostgresTlsMode::Disabled)
    }

    /// Variante di [`Self::insecure_local`] con `batch_rows` custom
    /// (come `new(batch_rows)` ma TLS `Disabled`). Usato dai test
    /// live in `test_suite` che devono controllare la dimensione del
    /// batch reader/writer contro Docker plaintext.
    ///
    /// **Solo per test/dev**. In produzione: `new(batch_rows)` con
    /// TLS `Require` default.
    #[must_use]
    pub fn insecure_local_with_batch_rows(batch_rows: usize) -> Self {
        Self::new(batch_rows).with_tls_mode(PostgresTlsMode::Disabled)
    }

    /// Alias deprecato di [`Self::default`], con TLS obbligatorio.
    #[must_use]
    #[deprecated(since = "1.2.0", note = "usa Self::default(), con TLS obbligatorio")]
    pub fn default_secure() -> Self {
        Self::default()
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
        self.pool.idle_connections()
    }

    #[must_use]
    pub fn schema_cache_entries(&self) -> usize {
        self.schema_cache.len()
    }

    #[cfg(test)]
    async fn connect(secret: &SecretString) -> Result<Client> {
        open_connection(secret).await
    }

    async fn connect_session(&self, secret: &SecretString) -> Result<PooledClient> {
        let mut client = self
            .pool
            .checkout(
                secret,
                self.tls_mode,
                &self.tls_config,
                self.network_options,
                PostgresSessionOptions::new(self.statement_timeout_ms, self.lock_timeout_ms),
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

    #[cfg(test)]
    async fn connect_with_tls(
        secret: &SecretString,
        tls_mode: PostgresTlsMode,
        tls_config: &PostgresTlsConfig,
        network_options: PostgresNetworkOptions,
        statement_timeout_ms: u64,
        lock_timeout_ms: u64,
    ) -> Result<Client> {
        open_connection_with_tls(
            secret,
            tls_mode,
            tls_config,
            network_options,
            statement_timeout_ms,
            lock_timeout_ms,
        )
        .await
    }

    fn schema_cache_key(client: &PooledClient, source: &ObjectRef) -> SchemaCacheKey {
        SchemaCacheKey::new(
            client.key(),
            source.schema.as_deref().unwrap_or("public").to_owned(),
            source.object.clone(),
        )
    }

    async fn cached_columns(
        &self,
        client: &PooledClient,
        source: &ObjectRef,
    ) -> Result<(Arc<Vec<ColumnSpec>>, PostgresSchemaToken)> {
        let key = Self::schema_cache_key(client, source);
        if let Some((candidate_token, candidate_columns)) = self.schema_cache.candidate(&key) {
            self.metrics.schema_token_check();
            let current = match schema_token(client.client()?, source).await {
                Ok(token) => token,
                Err(error) => {
                    if self.schema_cache.invalidate(&key) {
                        self.metrics.schema_cache_invalidation();
                    }
                    return Err(error);
                }
            };
            if candidate_token.structurally_equals(&current) {
                self.schema_cache.touch(&key);
                self.metrics.schema_cache_hit();
                return Ok((candidate_columns, current.public));
            }
            if self.schema_cache.invalidate(&key) {
                self.metrics.schema_cache_invalidation();
            }
        }
        self.metrics.schema_cache_miss();
        self.metrics.catalog_introspection();
        let (columns, token) = load_columns_and_token(client.client()?, source).await?;
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
        let key = SchemaCacheKey::new(
            connection,
            source.schema.as_deref().unwrap_or("public").to_owned(),
            source.object.clone(),
        );
        if self.schema_cache.invalidate(&key) {
            self.metrics.schema_cache_invalidation();
        }
    }

    async fn inspection(&self, client: &PooledClient, operation: &Operation) -> Result<Inspection> {
        match operation {
            Operation::DatabaseListCatalogs => Ok(Inspection {
                operation: "database.list_catalogs".to_owned(),
                document: json!({"catalogs": list_catalogs(client.client()?).await?}),
            }),
            Operation::DatabaseListSchemas { .. } => Ok(Inspection {
                operation: "database.list_schemas".to_owned(),
                document: json!({"schemas": list_schemas(client.client()?).await?}),
            }),
            Operation::DatabaseListObjects { source } => {
                let schema = source
                    .as_ref()
                    .and_then(|value| value.schema.as_deref())
                    .unwrap_or("public");
                let objects = list_objects(client.client()?, schema).await?;
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
            capability_document(client.client()?).await?.published()
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
            query_execution::read_stream(
                self,
                secret,
                operation,
                parameters,
                budget,
                control.token(),
            )
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
            query_execution::query_stream(
                self,
                secret,
                operation,
                parameters,
                budget,
                control.token(),
            )
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
            // La stessa compilazione di `compile_write_plan`, che il dry run
            // della CLI esegue senza server.
            let driver_state = write::prepare_state(&input_schema, operation)?;
            let (operation_lease, columns_lease) =
                ContractLeases::acquire(budget, input_schema.fields().len())?.into_parts();
            let loss_report = preflight::write(self, secret, operation, &input_schema).await?;
            Ok(PreparedWrite::new(
                operation.clone(),
                input_schema,
                loss_report,
                budget.clone(),
                operation_lease,
                columns_lease,
            )
            .with_driver_state(driver_state))
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
            validate_prepared_budget(&prepared.budget, budget)?;
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

    fn open_transaction<'a>(
        &'a self,
        secret: &'a SecretString,
        options: &'a plenora_database_core::transaction::TransactionOptions,
        _budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn plenora_database_core::transaction::TransactionScope>> {
        plenora_database_core::caller_sql::open_transaction({
            Box::pin(async move {
                let client = self.connect_session(secret).await?;
                let transaction =
                    transaction::PostgresTransaction::begin(client, options, cancellation).await?;
                Ok(Box::new(transaction)
                    as Box<
                        dyn plenora_database_core::transaction::TransactionScope,
                    >)
            })
        })
    }

    fn run_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        plenora_database_core::caller_sql::run_ddl(self.kind(), cancellation, {
            Box::pin(async move {
                error::check_cancelled(cancellation, ErrorPhase::Prepare)?;
                let mut client = self.connect_session(secret).await?;
                let result = client
                    .client_mut()?
                    .batch_execute(sql)
                    .await
                    .map_err(|error| classify_error(ErrorPhase::Write, &error));
                if result.is_err() {
                    client.invalidate();
                }
                result
            })
        })
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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

#[cfg(test)]
#[path = "write_visibility_live_tests.rs"]
mod write_visibility_live_tests;
