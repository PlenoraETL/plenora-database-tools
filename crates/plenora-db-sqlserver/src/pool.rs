use crate::{SqlServerConfig, SqlServerSession};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct SqlServerPool {
    config: SqlServerConfig,
    idle: Mutex<Vec<SqlServerSession>>,
    semaphore: Arc<Semaphore>,
    max_idle: usize,
}

impl std::fmt::Debug for SqlServerPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlServerPool")
            .field("max_idle", &self.max_idle)
            .field("idle", &self.idle_connections())
            .finish_non_exhaustive()
    }
}

impl SqlServerPool {
    /// Costruisce un pool bounded per una singola configurazione.
    ///
    /// # Errors
    ///
    /// Fallisce prima dell'I/O per configurazione non valida o capacità zero.
    pub fn new(config: SqlServerConfig, max_connections: usize) -> Result<Arc<Self>> {
        config.validate()?;
        if max_connections == 0 {
            return Err(pool_error(
                ErrorCategory::InvalidConfiguration,
                "pool SQL Server con capacità zero",
            ));
        }
        Ok(Arc::new(Self {
            config,
            idle: Mutex::new(Vec::new()),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_idle: max_connections,
        }))
    }

    /// Acquisisce o apre una sessione rispettando timeout e cancellazione.
    ///
    /// # Errors
    ///
    /// Fallisce per saturazione, cancellazione o apertura TDS.
    pub async fn checkout(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<PooledSqlServerSession> {
        let acquire = tokio::time::timeout(
            self.config.acquire_timeout(),
            Arc::clone(&self.semaphore).acquire_owned(),
        );
        let permit = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(DatabaseError::cancelled(
                    Some(plenora_database_core::plan::ProviderKind::Sqlserver),
                    ErrorPhase::Connect,
                    "acquisizione pool SQL Server cancellata",
                ));
            }
            result = acquire => match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    return Err(pool_error(
                        ErrorCategory::Internal,
                        "pool SQL Server chiuso",
                    ));
                }
                Err(_) => {
                    return Err(pool_error(
                        ErrorCategory::Timeout,
                        "timeout acquisizione pool SQL Server",
                    ));
                }
            }
        };

        let session = lock_recover(&self.idle)
            .pop()
            .filter(SqlServerSession::is_reusable);
        let session = match session {
            Some(session) => session,
            None => SqlServerSession::open(&self.config, cancellation).await?,
        };
        Ok(PooledSqlServerSession {
            session: Some(session),
            pool: Arc::clone(self),
            reusable: true,
            _permit: permit,
        })
    }

    #[must_use]
    pub fn idle_connections(&self) -> usize {
        lock_recover(&self.idle).len()
    }
}

pub struct PooledSqlServerSession {
    session: Option<SqlServerSession>,
    pool: Arc<SqlServerPool>,
    reusable: bool,
    _permit: OwnedSemaphorePermit,
}

impl PooledSqlServerSession {
    /// Disabilita preventivamente il rientro nel pool senza distruggere il
    /// client. È il guard fail-closed per operazioni TDS ancora da drenare.
    pub(crate) const fn disallow_reuse(&mut self) {
        self.reusable = false;
    }

    /// Riabilita il rientro soltanto dopo una prova positiva dello stato.
    ///
    /// # Errors
    ///
    /// Fallisce se la sessione non è presente o non è nello stato `Ready`.
    pub(crate) fn allow_reuse_after_drain(&mut self) -> Result<()> {
        if !self.session()?.is_reusable() {
            return Err(pool_error(
                ErrorCategory::Internal,
                "sessione SQL Server non riusabile dopo il drain",
            ));
        }
        self.reusable = true;
        Ok(())
    }

    /// Impedisce il rientro nel pool anche se lo stato locale appare pronto.
    pub fn quarantine(&mut self) {
        self.reusable = false;
        if let Some(session) = &mut self.session {
            session.quarantine();
        }
    }

    /// Accesso fallibile alla sessione, senza assunzioni che possano causare
    /// panic dopo una quarantena o un errore interno.
    ///
    /// # Errors
    ///
    /// Fallisce se il lease non contiene più una sessione.
    pub fn session(&self) -> Result<&SqlServerSession> {
        self.session.as_ref().ok_or_else(|| {
            pool_error(
                ErrorCategory::Internal,
                "sessione SQL Server assente dal lease",
            )
        })
    }

    /// Variante mutabile di [`Self::session`].
    ///
    /// # Errors
    ///
    /// Fallisce se il lease non contiene più una sessione.
    pub fn session_mut(&mut self) -> Result<&mut SqlServerSession> {
        self.session.as_mut().ok_or_else(|| {
            pool_error(
                ErrorCategory::Internal,
                "sessione SQL Server assente dal lease",
            )
        })
    }
}

impl Drop for PooledSqlServerSession {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        if !self.reusable || !session.is_reusable() {
            return;
        }
        let mut idle = lock_recover(&self.pool.idle);
        if idle.len() < self.pool.max_idle {
            idle.push(session);
        }
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn pool_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Connect,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::provider::SecretString;

    #[test]
    fn rejects_zero_capacity_without_network() {
        let config = SqlServerConfig::new(
            "sql.example.test",
            "warehouse",
            "loader",
            SecretString::new("secret"),
        );
        let error = SqlServerPool::new(config, 0).expect_err("zero capacity");
        assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
    }

    #[test]
    fn poisoned_idle_lock_is_recovered() {
        let config = SqlServerConfig::new(
            "sql.example.test",
            "warehouse",
            "loader",
            SecretString::new("secret"),
        );
        let pool = SqlServerPool::new(config, 1).expect("pool fixture");
        let poisoned = std::panic::catch_unwind({
            let pool = Arc::clone(&pool);
            move || {
                let _guard = pool.idle.lock().unwrap_or_else(PoisonError::into_inner);
                panic!("poison fixture");
            }
        });
        assert!(poisoned.is_err());
        assert_eq!(pool.idle_connections(), 0);
    }
}
