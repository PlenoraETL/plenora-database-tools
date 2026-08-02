use crate::config::MysqlConfig;
use crate::error::{driver_error, interruption_error, timeout_error};
use futures_util::StreamExt;
use mysql_async::prelude::{Queryable, StatementLike};
use mysql_async::{Conn, Params, Row, Statement};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

pub const SESSION_BOOTSTRAP_SQL: &str = "SET SESSION autocommit = 1, time_zone = '+00:00', sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'";

#[cfg(test)]
static TEST_ROW_PULLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub fn reset_test_row_pulls() {
    TEST_ROW_PULLS.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub fn test_row_pulls() -> u64 {
    TEST_ROW_PULLS.load(std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlSessionState {
    Ready,
    Quarantined,
}

#[derive(Debug, Clone, Copy)]
pub enum MysqlTransactionCommand {
    Start,
    Commit,
    Rollback,
}

impl MysqlTransactionCommand {
    const fn sql(self) -> &'static str {
        match self {
            Self::Start => "START TRANSACTION",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }
}

pub struct MysqlSession {
    connection: Option<Conn>,
    state: MysqlSessionState,
    operation_timeout: std::time::Duration,
    pool_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl std::fmt::Debug for MysqlSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlSession")
            .field("state", &self.state)
            .field("connected", &self.connection.is_some())
            .field("pooled", &self.pool_permit.is_some())
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
                return Err(interruption_error(cancellation, ErrorPhase::Connect, RemoteEffect::None));
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
            pool_permit: None,
        })
    }

    pub(crate) const fn from_connection(
        connection: Conn,
        operation_timeout: std::time::Duration,
        pool_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            connection: Some(connection),
            state: MysqlSessionState::Ready,
            operation_timeout,
            pool_permit: Some(pool_permit),
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
                return Err(interruption_error(cancellation, phase, RemoteEffect::None));
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
                return Err(interruption_error(cancellation, phase, RemoteEffect::None));
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

    /// Esegue una DML preparata e restituisce le righe dichiarate dall'OK
    /// packet del server.
    ///
    /// A differenza di `exec_rows` il risultato non e un result set: il numero
    /// di righe interessate e l'unica conferma che il server produce per un
    /// INSERT, e va letto dalla stessa connessione che lo ha eseguito.
    pub(crate) async fn exec_write(
        &mut self,
        sql: &str,
        parameters: Params,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        self.require_ready(phase)?;
        let connection = self.connection.as_mut().ok_or_else(|| state_error(phase))?;
        let execution = connection.exec_drop(sql, parameters);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(())) => self
                .connection
                .as_ref()
                .map(Conn::affected_rows)
                .ok_or_else(|| state_error(phase)),
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

    pub(crate) async fn exec_transaction(
        &mut self,
        command: MysqlTransactionCommand,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.require_ready(phase)?;
        let connection = self.connection.as_mut().ok_or_else(|| state_error(phase))?;
        let execution = connection.query_drop(command.sql());
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(cancellation, phase, RemoteEffect::Unknown));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(&error, phase, RemoteEffect::None))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(phase, RemoteEffect::Unknown))
            }
        }
    }

    /// Chiude la sessione e la rende inutilizzabile.
    ///
    /// Il path di scrittura la invoca anche dopo esiti che il driver non
    /// considera fatali: una connessione con una transazione di stato ignoto
    /// non puo tornare nel pool.
    pub(crate) async fn discard(&mut self) {
        self.quarantine().await;
    }

    /// Prepara lo statement e restituisce i metadati di colonna del server.
    ///
    /// `MySQL` non descrive un result set senza preparare: i metadati di
    /// `COM_STMT_PREPARE` sono l'unica fonte autoritativa dello schema di
    /// output di una `QueryOperation`.
    pub(crate) async fn prepare_statement(
        &mut self,
        sql: &str,
        cancellation: &CancellationToken,
    ) -> Result<Statement> {
        self.require_ready(ErrorPhase::Prepare)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(ErrorPhase::Prepare))?;
        let prepare = connection.prep(sql);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(cancellation, ErrorPhase::Prepare, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, prepare) => result,
        };
        match outcome {
            Ok(Ok(statement)) => Ok(statement),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    &error,
                    ErrorPhase::Prepare,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(ErrorPhase::Prepare, RemoteEffect::None))
            }
        }
    }

    pub(crate) async fn pump_exec_rows<S>(
        &mut self,
        statement: S,
        parameters: Params,
        sender: tokio::sync::mpsc::Sender<Result<Row>>,
        mut demand: tokio::sync::mpsc::Receiver<()>,
        cancellation: &CancellationToken,
    ) -> Result<()>
    where
        S: StatementLike + 'static,
    {
        self.require_ready(ErrorPhase::Read)?;
        let operation_timeout = self.operation_timeout;
        let outcome = {
            let connection = self
                .connection
                .as_mut()
                .ok_or_else(|| state_error(ErrorPhase::Read))?;
            let open = connection.exec_stream::<Row, _, _>(statement, parameters);
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
                    let requested = tokio::select! {
                        _ = cancellation.cancelled() => break PumpOutcome::Cancelled,
                        requested = demand.recv() => requested,
                    };
                    if requested.is_none() {
                        break PumpOutcome::Abandoned;
                    }
                    #[cfg(test)]
                    TEST_ROW_PULLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                Err(interruption_error(
                    cancellation,
                    ErrorPhase::Read,
                    RemoteEffect::None,
                ))
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
