use crate::error::{cancellation_error, driver_error, timeout_error};
use crate::{MysqlConfig, MysqlSession};
use mysql_async::Pool;
use plenora_database_core::{CancellationToken, ErrorPhase, RemoteEffect, Result};

#[derive(Clone)]
pub struct MysqlPool {
    pool: Pool,
    acquire_timeout: std::time::Duration,
    operation_timeout: std::time::Duration,
}

impl std::fmt::Debug for MysqlPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlPool")
            .field("acquire_timeout", &self.acquire_timeout)
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
            pool: Pool::new(config.driver_opts_with_pool(Some(max_connections))?),
            acquire_timeout: config.acquire_timeout(),
            operation_timeout: config.operation_timeout(),
        })
    }

    /// Acquisisce una sessione entro il budget temporale configurato.
    ///
    /// # Errors
    ///
    /// Propaga cancellazione, timeout e fallimenti del driver.
    pub async fn checkout(&self, cancellation: &CancellationToken) -> Result<MysqlSession> {
        let acquire = self.pool.get_conn();
        let connection = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(cancellation_error(ErrorPhase::Connect, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.acquire_timeout, acquire) => {
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
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::provider::SecretString;

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
}
