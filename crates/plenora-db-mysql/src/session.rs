use crate::config::MysqlConfig;
use crate::error::{cancellation_error, driver_error, timeout_error};
use futures_util::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Row};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

pub const SESSION_BOOTSTRAP_SQL: &str = "SET SESSION autocommit = 1, time_zone = '+00:00', sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlSessionState {
    Ready,
    Quarantined,
}

pub struct MysqlSession {
    connection: Option<Conn>,
    state: MysqlSessionState,
    operation_timeout: std::time::Duration,
}

impl std::fmt::Debug for MysqlSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlSession")
            .field("state", &self.state)
            .field("connected", &self.connection.is_some())
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}

impl MysqlSession {
    /// Apre una connessione TLS e applica il bootstrap deterministico.
    ///
    /// # Errors
    ///
    /// Propaga configurazione, autenticazione, cancellazione e timeout.
    pub async fn open(config: &MysqlConfig, cancellation: &CancellationToken) -> Result<Self> {
        let opts = config.driver_opts()?;
        let connect = Conn::new(opts);
        let connection = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(cancellation_error(ErrorPhase::Connect, RemoteEffect::None));
            }
            result = tokio::time::timeout(config.connect_timeout(), connect) => {
                match result {
                    Ok(Ok(connection)) => connection,
                    Ok(Err(error)) => {
                        return Err(driver_error(&error, ErrorPhase::Connect, RemoteEffect::None));
                    }
                    Err(_) => return Err(timeout_error(ErrorPhase::Connect, RemoteEffect::None)),
                }
            }
        };
        Ok(Self {
            connection: Some(connection),
            state: MysqlSessionState::Ready,
            operation_timeout: config.operation_timeout(),
        })
    }

    pub(crate) const fn from_connection(
        connection: Conn,
        operation_timeout: std::time::Duration,
    ) -> Self {
        Self {
            connection: Some(connection),
            state: MysqlSessionState::Ready,
            operation_timeout,
        }
    }

    #[must_use]
    pub const fn state(&self) -> MysqlSessionState {
        self.state
    }

    #[must_use]
    pub const fn is_reusable(&self) -> bool {
        matches!(self.state, MysqlSessionState::Ready) && self.connection.is_some()
    }

    pub(crate) async fn query_rows(
        &mut self,
        sql: &str,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Row>> {
        self.require_ready(phase)?;
        let connection = self.connection.as_mut().ok_or_else(|| state_error(phase))?;
        let query = connection.query::<Row, _>(sql);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(cancellation_error(phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, query) => result,
        };
        match outcome {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(&error, phase, RemoteEffect::None))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(phase, RemoteEffect::None))
            }
        }
    }

    fn require_ready(&self, phase: ErrorPhase) -> Result<()> {
        if self.is_reusable() {
            Ok(())
        } else {
            Err(state_error(phase))
        }
    }

    pub(crate) async fn exec_rows(
        &mut self,
        sql: &str,
        parameters: Params,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Row>> {
        self.require_ready(phase)?;
        let connection = self.connection.as_mut().ok_or_else(|| state_error(phase))?;
        let query = connection.exec::<Row, _, _>(sql, parameters);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(cancellation_error(phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, query) => result,
        };
        match outcome {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(&error, phase, RemoteEffect::None))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(phase, RemoteEffect::None))
            }
        }
    }

    pub(crate) async fn pump_exec_rows(
        &mut self,
        sql: String,
        parameters: Params,
        sender: tokio::sync::mpsc::Sender<Result<Row>>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.require_ready(ErrorPhase::Read)?;
        let operation_timeout = self.operation_timeout;
        let outcome = {
            let connection = self
                .connection
                .as_mut()
                .ok_or_else(|| state_error(ErrorPhase::Read))?;
            let open = connection.exec_stream::<Row, _, _>(sql, parameters);
            let stream = tokio::select! {
                _ = cancellation.cancelled() => {
                    None
                }
                result = tokio::time::timeout(operation_timeout, open) => {
                    match result {
                        Ok(Ok(stream)) => Some(Ok(stream)),
                        Ok(Err(error)) => Some(Err(driver_error(
                            &error,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ))),
                        Err(_) => Some(Err(timeout_error(
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ))),
                    }
                }
            };
            match stream {
                None => PumpOutcome::Cancelled,
                Some(Err(error)) => PumpOutcome::Failed(error),
                Some(Ok(mut stream)) => loop {
                    let next = tokio::select! {
                        _ = cancellation.cancelled() => break PumpOutcome::Cancelled,
                        result = tokio::time::timeout(operation_timeout, stream.next()) => result,
                    };
                    match next {
                        Ok(Some(Ok(row))) => {
                            let sent = tokio::select! {
                                _ = cancellation.cancelled() => {
                                    break PumpOutcome::Cancelled;
                                }
                                result = sender.send(Ok(row)) => result,
                            };
                            if sent.is_err() {
                                break PumpOutcome::Abandoned;
                            }
                        }
                        Ok(Some(Err(error))) => {
                            break PumpOutcome::Failed(driver_error(
                                &error,
                                ErrorPhase::Read,
                                RemoteEffect::None,
                            ));
                        }
                        Ok(None) => break PumpOutcome::Drained,
                        Err(_) => {
                            break PumpOutcome::Failed(timeout_error(
                                ErrorPhase::Read,
                                RemoteEffect::None,
                            ));
                        }
                    }
                },
            }
        };
        match outcome {
            PumpOutcome::Drained => Ok(()),
            PumpOutcome::Cancelled => {
                self.quarantine().await;
                Err(cancellation_error(ErrorPhase::Read, RemoteEffect::None))
            }
            PumpOutcome::Abandoned => {
                self.quarantine().await;
                Ok(())
            }
            PumpOutcome::Failed(error) => {
                self.quarantine().await;
                Err(error)
            }
        }
    }

    async fn quarantine(&mut self) {
        self.state = MysqlSessionState::Quarantined;
        if let Some(connection) = self.connection.take() {
            let _ = connection.disconnect().await;
        }
    }
}

enum PumpOutcome {
    Drained,
    Cancelled,
    Abandoned,
    Failed(DatabaseError),
}

fn state_error(phase: ErrorPhase) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidPlan,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: "sessione MySQL non riusabile".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_is_explicit_and_deterministic() {
        assert!(SESSION_BOOTSTRAP_SQL.contains("autocommit = 1"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("time_zone = '+00:00'"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("STRICT_TRANS_TABLES"));
    }
}
