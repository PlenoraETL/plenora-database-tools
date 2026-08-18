use crate::config::MysqlConfig;
use crate::error::{
    driver_error, interruption_error, row_rejection_cause, server_code, timeout_error,
};
use crate::profile::ProductProfile;
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
    /// Il prodotto servito dalla connessione, ereditato dal pool.
    profile: &'static dyn crate::profile::ProductProfile,
    connection: Option<Conn>,
    state: MysqlSessionState,
    operation_timeout: std::time::Duration,
    pool_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

// Il profilo non compare nel `Debug`: quell'output e superficie
// osservata, e questa fase non ne cambia nemmeno una riga. Il
// prodotto servito si legge dal provider, non da qui.
#[allow(clippy::missing_fields_in_debug)]
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
                return Err(interruption_error(crate::profile::MYSQL_PROFILE.kind(), cancellation, ErrorPhase::Connect, RemoteEffect::None));
            }
            result = tokio::time::timeout(config.connect_timeout(), connect) => {
                match result {
                    Ok(Ok(connection)) => connection,
                    Ok(Err(error)) => {
                        return Err(driver_error(crate::profile::MYSQL_PROFILE.kind(), &error, ErrorPhase::Connect, RemoteEffect::None));
                    }
                    Err(_) => return Err(timeout_error(crate::profile::MYSQL_PROFILE.kind(), ErrorPhase::Connect, RemoteEffect::None)),
                }
            }
        };
        Ok(Self {
            profile: &crate::profile::MYSQL_PROFILE,
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
        profile: &'static dyn crate::profile::ProductProfile,
    ) -> Self {
        Self {
            profile,
            connection: Some(connection),
            state: MysqlSessionState::Ready,
            operation_timeout,
            pool_permit: Some(pool_permit),
        }
    }

    /// Il prodotto servito dalla connessione.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn kind(&self) -> plenora_database_core::plan::ProviderKind {
        self.profile.kind()
    }

    #[must_use]
    pub const fn state(&self) -> MysqlSessionState {
        self.state
    }

    #[must_use]
    pub const fn is_reusable(&self) -> bool {
        matches!(self.state, MysqlSessionState::Ready) && self.connection.is_some()
    }

    /// Accessor mutabile alla `Conn` sottostante — per il transaction
    /// module che usa direttamente `mysql_async` API (exec + query
    /// tipizzati).
    #[must_use]
    pub const fn connection_mut(&mut self) -> Option<&mut Conn> {
        self.connection.as_mut()
    }

    /// Timeout di operazione configurato al connect.
    #[must_use]
    pub const fn operation_timeout(&self) -> std::time::Duration {
        self.operation_timeout
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
                return Err(interruption_error(self.profile.kind(), cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, query) => result,
        };
        match outcome {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile.kind(),
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile.kind(),
                    phase,
                    RemoteEffect::None,
                ))
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
                return Err(interruption_error(self.profile.kind(), cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, query) => result,
        };
        match outcome {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile.kind(),
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile.kind(),
                    phase,
                    RemoteEffect::None,
                ))
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
                return Err(interruption_error(self.profile.kind(), cancellation, phase, RemoteEffect::None));
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
                Err(driver_error(
                    self.profile.kind(),
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile.kind(),
                    phase,
                    RemoteEffect::None,
                ))
            }
        }
    }

    /// Esegue lo statement di **una sola riga** del percorso diagnostico.
    ///
    /// Restituisce `Ok(None)` quando la riga è stata applicata e
    /// `Ok(Some(causa))` quando il server l'ha rifiutata per un vincolo: è
    /// l'unico esito che autorizza ad attribuire il rifiuto a un indice
    /// sorgente, perché lo statement conteneva quella riga e nessun'altra.
    ///
    /// Un rifiuto di vincolo non sporca la sessione — la transazione resta
    /// aperta e annullabile — quindi la connessione non viene quarantinata.
    /// Ogni altro errore mantiene il comportamento di `exec_write`.
    pub(crate) async fn exec_row_write(
        &mut self,
        sql: &str,
        parameters: Params,
        cancellation: &CancellationToken,
    ) -> Result<Option<&'static str>> {
        let phase = ErrorPhase::Write;
        self.require_ready(phase)?;
        let connection = self.connection.as_mut().ok_or_else(|| state_error(phase))?;
        let execution = connection.exec_drop(sql, parameters);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile.kind(), cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(())) => {
                let affected = connection.affected_rows();
                match validate_row_write_affected_rows(affected) {
                    Ok(()) => Ok(None),
                    Err(error) => {
                        self.quarantine().await;
                        Err(error)
                    }
                }
            }
            Ok(Err(error)) => {
                if let Some(cause) = server_code(&error).and_then(row_rejection_cause) {
                    return Ok(Some(cause));
                }
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile.kind(),
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile.kind(),
                    phase,
                    RemoteEffect::None,
                ))
            }
        }
    }

    /// Esegue SQL senza parametri via **text protocol** (`query_drop`).
    ///
    /// Uso: comandi di controllo di sessione (`SET`, `SAVEPOINT`,
    /// `ROLLBACK TO`, `START TRANSACTION`, DDL raw) che `MySQL` rifiuta
    /// nel prepared statement protocol (errore 1295).
    pub(crate) async fn exec_control(
        &mut self,
        sql: &str,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.require_ready(phase)?;
        let connection = self.connection.as_mut().ok_or_else(|| state_error(phase))?;
        let execution = connection.query_drop(sql);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile.kind(), cancellation, phase, RemoteEffect::Unknown));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile.kind(),
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile.kind(),
                    phase,
                    RemoteEffect::Unknown,
                ))
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
                return Err(interruption_error(self.profile.kind(), cancellation, phase, RemoteEffect::Unknown));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile.kind(),
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile.kind(),
                    phase,
                    RemoteEffect::Unknown,
                ))
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
                return Err(interruption_error(self.profile.kind(), cancellation, ErrorPhase::Prepare, RemoteEffect::None));
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
                    self.profile.kind(),
                    &error,
                    ErrorPhase::Prepare,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile.kind(),
                    ErrorPhase::Prepare,
                    RemoteEffect::None,
                ))
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
                        Ok(Err(error)) => Some(Err(driver_error(self.profile.kind(),
                            &error,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ))),
                        Err(_) => Some(Err(timeout_error(self.profile.kind(),
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
                                self.profile.kind(),
                                &error,
                                ErrorPhase::Read,
                                RemoteEffect::None,
                            ));
                        }
                        Ok(None) => break PumpOutcome::Drained,
                        Err(_) => {
                            break PumpOutcome::Failed(timeout_error(
                                self.profile.kind(),
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
                    self.profile.kind(),
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

fn row_count_mismatch_error() -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::Protocol,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::Quarantine,
        provider: Some(crate::profile::PROVISIONAL_KIND),
        execution_id: None,
        message: "conteggio righe MySQL incoerente per statement row-scoped".to_owned(),
        diagnostics: None,
    }
}

fn validate_row_write_affected_rows(affected_rows: u64) -> Result<()> {
    if affected_rows == 1 {
        Ok(())
    } else {
        Err(row_count_mismatch_error())
    }
}

fn state_error(phase: ErrorPhase) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidPlan,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(crate::profile::PROVISIONAL_KIND),
        execution_id: None,
        message: "sessione MySQL non riusabile".to_owned(),
        diagnostics: None,
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

    #[test]
    fn exactly_one_affected_row_is_required_for_row_scoped_success() {
        validate_row_write_affected_rows(1).expect("una riga confermata");
        for affected in [0, 2] {
            let error = validate_row_write_affected_rows(affected)
                .expect_err("conteggio diverso da uno ambiguo");
            assert_eq!(error.phase, ErrorPhase::Write);
            assert_eq!(error.remote_effect, RemoteEffect::Unknown);
            assert_eq!(error.retry, RetryDisposition::Quarantine);
            assert!(error.diagnostics.is_none());
        }
    }
}
