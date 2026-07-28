use super::{error::public_error, lock_recover, metrics::PostgresMetrics};
use crate::connection::{
    connect_with_tls, connection_fingerprint, validate_session_timeouts, PostgresNetworkOptions,
    PostgresTlsConfig, PostgresTlsMode,
};
use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::Client;

pub struct PostgresPool {
    idle: Mutex<HashMap<[u8; 32], Vec<Client>>>,
    semaphore: Arc<Semaphore>,
    max_idle_per_key: usize,
    metrics: Arc<PostgresMetrics>,
}

#[derive(Debug, Clone, Copy)]
pub struct PostgresSessionOptions {
    statement_timeout_ms: u64,
    lock_timeout_ms: u64,
}

impl PostgresSessionOptions {
    pub const fn new(statement_timeout_ms: u64, lock_timeout_ms: u64) -> Self {
        Self {
            statement_timeout_ms,
            lock_timeout_ms,
        }
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
    pub fn new(max_connections: usize, metrics: Arc<PostgresMetrics>) -> Self {
        Self {
            idle: Mutex::new(HashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_idle_per_key: max_connections,
            metrics,
        }
    }

    pub async fn checkout(
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
            let client = connect_with_tls(
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

    pub fn idle_connections(&self) -> usize {
        lock_recover(&self.idle).values().map(Vec::len).sum()
    }
}

pub struct PooledClient {
    client: Option<Client>,
    key: [u8; 32],
    pool: Arc<PostgresPool>,
    reused: bool,
    reusable: bool,
    _permit: OwnedSemaphorePermit,
}

impl PooledClient {
    pub const fn invalidate(&mut self) {
        self.reusable = false;
    }

    pub const fn mark_reusable(&mut self) {
        self.reusable = true;
    }

    pub const fn was_reused(&self) -> bool {
        self.reused
    }

    pub const fn key(&self) -> [u8; 32] {
        self.key
    }

    pub fn record_cancellation(&self) {
        self.pool.metrics.cancellation();
    }

    pub fn client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or_else(|| {
            public_error(
                ErrorCategory::Internal,
                ErrorPhase::Connect,
                false,
                "client PostgreSQL del pool non disponibile",
            )
        })
    }

    pub fn client_mut(&mut self) -> Result<&mut Client> {
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
