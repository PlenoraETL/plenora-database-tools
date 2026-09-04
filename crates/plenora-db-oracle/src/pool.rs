use crate::connection::connect;
use crate::OracleConfig;
use oracle_rs::Connection;
use plenora_database_core::provider::SecretString;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Pool Oracle bounded e lazy. Le credenziali restano nel solo pool associato
/// alla loro impronta e non entrano mai nella configurazione o nel `Debug`.
pub struct OraclePool {
    config: OracleConfig,
    secret: SecretString,
    idle: Mutex<Vec<Connection>>,
    semaphore: Arc<Semaphore>,
    max_idle: usize,
}

impl std::fmt::Debug for OraclePool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OraclePool")
            .field("config", &self.config)
            .field("max_idle", &self.max_idle)
            .field("idle", &self.idle_connections())
            .finish_non_exhaustive()
    }
}

impl OraclePool {
    /// Costruisce un pool lazy con capacita strettamente positiva.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidConfiguration` per configurazione o capacita non valide.
    pub fn new(
        config: OracleConfig,
        secret: SecretString,
        max_connections: usize,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        if max_connections == 0 {
            return Err(pool_error(
                ErrorCategory::InvalidConfiguration,
                RetryDisposition::Never,
                "pool Oracle con capacita zero",
            ));
        }
        Ok(Arc::new(Self {
            config,
            secret,
            idle: Mutex::new(Vec::new()),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_idle: max_connections,
        }))
    }

    /// Acquisisce un lease rispettando backpressure, timeout e cancellazione.
    ///
    /// # Errors
    ///
    /// Restituisce un errore tipizzato se il pool e saturo, cancellato o se
    /// l'apertura della connessione fallisce.
    pub async fn checkout(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<PooledOracleConnection> {
        let acquire = tokio::time::timeout(
            self.config.acquire_timeout(),
            Arc::clone(&self.semaphore).acquire_owned(),
        );
        let permit = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(DatabaseError::interrupted(
                    cancellation,
                    Some(plenora_database_core::plan::ProviderKind::Oracle),
                    ErrorPhase::Connect,
                    "acquisizione pool Oracle interrotta",
                ));
            }
            result = acquire => match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err(pool_error(
                    ErrorCategory::Internal,
                    RetryDisposition::Never,
                    "pool Oracle chiuso",
                )),
                Err(_) => return Err(pool_error(
                    ErrorCategory::Timeout,
                    RetryDisposition::Safe,
                    "timeout acquisizione pool Oracle",
                )),
            }
        };
        let idle = { lock_recover(&self.idle).pop() };
        let connection = match idle {
            Some(connection) => connection,
            None => connect(&self.config, &self.secret, cancellation).await?,
        };
        Ok(PooledOracleConnection {
            connection: Some(connection),
            pool: Arc::clone(self),
            // Una connessione entra nell'idle pool solo dopo che il chiamante
            // ha osservato il completamento dell'intera operazione. Un errore
            // del protocollo, un timeout o una cancellazione la scartano senza
            // dover ricordare un `quarantine()` su ogni ramo anticipato.
            reusable: false,
            _permit: permit,
        })
    }

    #[must_use]
    pub fn idle_connections(&self) -> usize {
        lock_recover(&self.idle).len()
    }
}

pub struct PooledOracleConnection {
    connection: Option<Connection>,
    pool: Arc<OraclePool>,
    reusable: bool,
    _permit: OwnedSemaphorePermit,
}

impl PooledOracleConnection {
    /// Accede alla connessione contenuta nel lease.
    ///
    /// # Errors
    ///
    /// Restituisce `Internal` se il lease non contiene piu la connessione.
    pub fn connection(&self) -> Result<&Connection> {
        self.connection.as_ref().ok_or_else(|| {
            pool_error(
                ErrorCategory::Internal,
                RetryDisposition::Never,
                "connessione Oracle assente dal lease",
            )
        })
    }

    pub const fn quarantine(&mut self) {
        self.reusable = false;
    }

    pub const fn disallow_reuse(&mut self) {
        self.reusable = false;
    }

    pub const fn allow_reuse(&mut self) {
        self.reusable = true;
    }
}

impl Drop for PooledOracleConnection {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        if !self.reusable {
            return;
        }
        let mut idle = lock_recover(&self.pool.idle);
        if idle.len() < self.pool.max_idle {
            idle.push(connection);
        }
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn pool_error(
    category: ErrorCategory,
    retry: RetryDisposition,
    message: &'static str,
) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Connect,
        remote_effect: RemoteEffect::None,
        retry,
        provider: Some(plenora_database_core::plan::ProviderKind::Oracle),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}
