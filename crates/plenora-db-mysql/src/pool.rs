use crate::error::{driver_error, interruption_error, timeout_error};
use crate::{MysqlConfig, MysqlSession};
use mysql_async::Pool;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct MysqlPool {
    pool: Pool,
    checkout_timeout: std::time::Duration,
    connect_timeout: std::time::Duration,
    operation_timeout: std::time::Duration,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for MysqlPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlPool")
            .field("checkout_timeout", &self.checkout_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("metrics", &self.pool.metrics())
            .finish_non_exhaustive()
    }
}

impl MysqlPool {
    /// Costruisce un pool lazy con reset obbligatorio delle connessioni.
    ///
    /// # Errors
    ///
    /// Fallisce se configurazione o limiti del pool non sono validi.
    pub fn new(config: &MysqlConfig, max_connections: usize) -> Result<Self> {
        if max_connections == 0 {
            return Err(plenora_database_core::DatabaseError {
                category: plenora_database_core::ErrorCategory::InvalidConfiguration,
                phase: ErrorPhase::Validate,
                remote_effect: RemoteEffect::None,
                retry: plenora_database_core::RetryDisposition::Never,
                provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
                execution_id: None,
                message: "pool MySQL con capacita zero".to_owned(),
            });
        }
        Ok(Self {
            pool: Pool::new(config.pooled_driver_opts(max_connections)?),
            checkout_timeout: config.acquire_timeout(),
            connect_timeout: config.connect_timeout(),
            operation_timeout: config.operation_timeout(),
            permits: Arc::new(Semaphore::new(max_connections)),
        })
    }

    /// Acquisisce una sessione entro il budget temporale configurato.
    ///
    /// # Errors
    ///
    /// Propaga cancellazione, timeout e fallimenti del driver.
    pub async fn checkout(&self, cancellation: &CancellationToken) -> Result<MysqlSession> {
        let acquire_permit = Arc::clone(&self.permits).acquire_owned();
        let permit = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(interruption_error(
                    cancellation,
                    ErrorPhase::Connect,
                    RemoteEffect::None,
                ));
            }
            result = tokio::time::timeout(self.checkout_timeout, acquire_permit) => {
                match result {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => return Err(semaphore_closed_error()),
                    Err(_) => return Err(timeout_error(ErrorPhase::Connect, RemoteEffect::None)),
                }
            }
        };
        let acquire = self.pool.get_conn();
        let connection = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(interruption_error(
                    cancellation,
                    ErrorPhase::Connect,
                    RemoteEffect::None,
                ));
            }
            result = tokio::time::timeout(self.connect_timeout, acquire) => {
                match result {
                    Ok(Ok(connection)) => connection,
                    Ok(Err(error)) => {
                        return Err(driver_error(&error, ErrorPhase::Connect, RemoteEffect::None));
                    }
                    Err(_) => return Err(timeout_error(ErrorPhase::Connect, RemoteEffect::None)),
                }
            }
        };
        Ok(MysqlSession::from_connection(
            connection,
            self.operation_timeout,
            permit,
        ))
    }
}

fn semaphore_closed_error() -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::Internal,
        phase: ErrorPhase::Connect,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: "semaphore pool MySQL chiuso".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::provider::SecretString;
    use std::time::Duration;

    #[test]
    fn zero_capacity_is_rejected_without_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("secret"),
        );
        let error = MysqlPool::new(&config, 0).expect_err("zero pool capacity");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidConfiguration
        );
    }

    #[test]
    fn checkout_preserves_the_independent_acquire_budget() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("secret"),
        )
        .with_timeouts(
            Duration::from_secs(2),
            Duration::from_secs(30),
            Duration::from_secs(11),
        );
        let pool = MysqlPool::new(&config, 1).expect("pool");
        assert_eq!(pool.checkout_timeout, Duration::from_secs(11));
        assert_eq!(pool.connect_timeout, Duration::from_secs(2));
    }
}
