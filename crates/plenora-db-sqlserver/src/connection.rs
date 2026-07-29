use crate::config::SqlServerConfig;
use crate::error::{cancellation_error, driver_error, timeout_error};
use crate::recovery::{TransactionEvent, TransactionState};
use crate::session::{SessionState, SESSION_BOOTSTRAP_SQL};
use futures_util::TryStreamExt;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use tiberius::Client;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type TdsClient = Client<Compat<TcpStream>>;

/// Sessione TDS non clonabile con stato transazionale esplicito.
pub struct SqlServerSession {
    client: Option<TdsClient>,
    transaction: TransactionState,
    operation_timeout: std::time::Duration,
}

impl std::fmt::Debug for SqlServerSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlServerSession")
            .field("state", &self.state())
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl SqlServerSession {
    /// Apre TDS con TLS richiesto ed esegue il bootstrap di sessione.
    ///
    /// # Errors
    ///
    /// Restituisce un errore pubblico redatto per configurazione, rete,
    /// TLS, login, timeout o cancellazione.
    pub async fn open(config: &SqlServerConfig, cancellation: &CancellationToken) -> Result<Self> {
        config.validate()?;
        if cancellation.is_cancelled() {
            return Err(cancellation_error(ErrorPhase::Connect, RemoteEffect::None));
        }
        let driver_config = config.driver_config()?;
        let address = driver_config.get_addr();
        let connect = async move {
            let tcp =
                TcpStream::connect(address)
                    .await
                    .map_err(|error| tiberius::error::Error::Io {
                        kind: error.kind(),
                        message: "connessione TCP SQL Server fallita".to_owned(),
                    })?;
            tcp.set_nodelay(true)
                .map_err(|error| tiberius::error::Error::Io {
                    kind: error.kind(),
                    message: "configurazione TCP SQL Server fallita".to_owned(),
                })?;
            Client::connect(driver_config, tcp.compat_write()).await
        };
        let client = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(cancellation_error(ErrorPhase::Connect, RemoteEffect::None));
            }
            result = tokio::time::timeout(config.connect_timeout(), connect) => {
                match result {
                    Ok(Ok(client)) => client,
                    Ok(Err(error)) => {
                        return Err(driver_error(
                            &error,
                            ErrorPhase::Connect,
                            RemoteEffect::None,
                        ));
                    }
                    Err(_) => {
                        return Err(timeout_error(ErrorPhase::Connect, RemoteEffect::None));
                    }
                }
            }
        };

        let mut session = Self {
            client: Some(client),
            transaction: TransactionState::default(),
            operation_timeout: config.operation_timeout(),
        };
        session
            .run_control(
                SESSION_BOOTSTRAP_SQL,
                ErrorPhase::Prepare,
                RemoteEffect::None,
                cancellation,
            )
            .await?;
        Ok(session)
    }

    #[must_use]
    pub const fn state(&self) -> SessionState {
        self.transaction.state()
    }

    #[must_use]
    pub const fn is_reusable(&self) -> bool {
        self.client.is_some() && self.state().is_reusable()
    }

    /// Distrugge il client TDS locale. Dopo questa chiamata il pool non può
    /// riusare la connessione.
    pub fn quarantine(&mut self) {
        let _ = self.transaction.apply(TransactionEvent::Cancelled);
        self.client.take();
    }

    /// Inizia una transazione esplicita.
    ///
    /// # Errors
    ///
    /// Fallisce se la sessione non è pronta o il batch non viene drenato.
    pub async fn begin(&mut self, cancellation: &CancellationToken) -> Result<()> {
        self.require_state(SessionState::Ready, ErrorPhase::Write)?;
        self.run_control(
            "BEGIN TRANSACTION;",
            ErrorPhase::Write,
            RemoteEffect::None,
            cancellation,
        )
        .await?;
        let _ = self.transaction.apply(TransactionEvent::BeginSucceeded);
        Ok(())
    }

    /// Esegue il commit. Un errore di trasporto produce sempre esito ignoto.
    ///
    /// # Errors
    ///
    /// Fallisce se non c'è una transazione o se il server non conferma il
    /// completamento del batch.
    pub async fn commit(&mut self, cancellation: &CancellationToken) -> Result<()> {
        self.require_state(SessionState::Transaction, ErrorPhase::Commit)?;
        self.run_control(
            "COMMIT TRANSACTION;",
            ErrorPhase::Commit,
            RemoteEffect::Unknown,
            cancellation,
        )
        .await?;
        let _ = self.transaction.apply(TransactionEvent::CommitSucceeded);
        Ok(())
    }

    #[cfg(test)]
    /// Apre esclusivamente nei test una finestra tra commit server e risposta.
    ///
    /// # Errors
    ///
    /// Fallisce se la sessione non è in transazione o se il batch di controllo
    /// non viene confermato integralmente.
    pub async fn commit_with_delayed_response(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.require_state(SessionState::Transaction, ErrorPhase::Commit)?;
        self.run_control(
            "COMMIT TRANSACTION; WAITFOR DELAY '00:00:10';",
            ErrorPhase::Commit,
            RemoteEffect::Unknown,
            cancellation,
        )
        .await?;
        let _ = self.transaction.apply(TransactionEvent::CommitSucceeded);
        Ok(())
    }

    /// Esegue rollback e rende la connessione riusabile solo dopo conferma.
    ///
    /// # Errors
    ///
    /// Fallisce e mette in quarantena se il rollback non è confermato.
    pub async fn rollback(&mut self, cancellation: &CancellationToken) -> Result<()> {
        if !matches!(
            self.state(),
            SessionState::Transaction | SessionState::Uncommittable
        ) {
            return Err(state_error(ErrorPhase::Rollback));
        }
        self.run_control(
            "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;",
            ErrorPhase::Rollback,
            RemoteEffect::Unknown,
            cancellation,
        )
        .await?;
        let _ = self.transaction.apply(TransactionEvent::RollbackSucceeded);
        Ok(())
    }

    #[cfg(test)]
    /// Apre esclusivamente nei test una finestra tra rollback server e risposta.
    ///
    /// # Errors
    ///
    /// Fallisce se la sessione non è in transazione o se il batch di controllo
    /// non viene confermato integralmente.
    pub async fn rollback_with_delayed_response(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if !matches!(
            self.state(),
            SessionState::Transaction | SessionState::Uncommittable
        ) {
            return Err(state_error(ErrorPhase::Rollback));
        }
        self.run_control(
            "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; WAITFOR DELAY '00:00:10';",
            ErrorPhase::Rollback,
            RemoteEffect::Unknown,
            cancellation,
        )
        .await?;
        let _ = self.transaction.apply(TransactionEvent::RollbackSucceeded);
        Ok(())
    }

    fn require_state(&self, expected: SessionState, phase: ErrorPhase) -> Result<()> {
        if self.state() != expected || self.client.is_none() {
            return Err(state_error(phase));
        }
        Ok(())
    }

    async fn run_control(
        &mut self,
        sql: &'static str,
        phase: ErrorPhase,
        remote_effect: RemoteEffect,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let Some(client) = self.client.as_mut() else {
            return Err(state_error(phase));
        };
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => ControlOutcome::Cancelled,
            result = tokio::time::timeout(
                self.operation_timeout,
                execute_and_drain(client, sql),
            ) => match result {
                Ok(Ok(())) => ControlOutcome::Completed,
                Ok(Err(error)) => ControlOutcome::Driver(error),
                Err(_) => ControlOutcome::Timeout,
            },
        };
        match outcome {
            ControlOutcome::Completed => Ok(()),
            ControlOutcome::Cancelled => {
                self.quarantine();
                Err(cancellation_error(phase, remote_effect))
            }
            ControlOutcome::Timeout => {
                self.quarantine();
                Err(timeout_error(phase, remote_effect))
            }
            ControlOutcome::Driver(error) => {
                let public = driver_error(&error, phase, remote_effect);
                self.quarantine();
                Err(public)
            }
        }
    }

    pub(crate) async fn execute_query(
        &mut self,
        query: tiberius::Query<'static>,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Vec<tiberius::Row>>> {
        let in_transaction = self.state() == SessionState::Transaction;
        if !in_transaction {
            self.require_state(SessionState::Ready, phase)?;
        }
        let Some(client) = self.client.as_mut() else {
            return Err(state_error(phase));
        };
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => QueryOutcome::Cancelled,
            result = tokio::time::timeout(
                self.operation_timeout,
                query_and_drain(client, query),
            ) => match result {
                Ok(Ok(rows)) => QueryOutcome::Completed(rows),
                Ok(Err(error)) => QueryOutcome::Driver(error),
                Err(_) => QueryOutcome::Timeout,
            },
        };
        match outcome {
            QueryOutcome::Completed(rows) => Ok(rows),
            QueryOutcome::Cancelled => {
                self.quarantine();
                Err(cancellation_error(phase, RemoteEffect::None))
            }
            QueryOutcome::Timeout => {
                self.quarantine();
                Err(timeout_error(phase, RemoteEffect::None))
            }
            QueryOutcome::Driver(error) => {
                let requested_effect = if in_transaction && error.code().is_none() {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                };
                let public = driver_error(&error, phase, requested_effect);
                if in_transaction && error.code().is_some() {
                    let _ = self.transaction.apply(TransactionEvent::StatementFailed);
                } else {
                    self.quarantine();
                }
                Err(public)
            }
        }
    }

    pub(crate) async fn execute_write_query(
        &mut self,
        query: tiberius::Query<'static>,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        self.require_state(SessionState::Transaction, ErrorPhase::Write)?;
        let Some(client) = self.client.as_mut() else {
            return Err(state_error(ErrorPhase::Write));
        };
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => WriteQueryOutcome::Cancelled,
            result = tokio::time::timeout(self.operation_timeout, query.execute(client)) => {
                match result {
                    Ok(Ok(result)) => WriteQueryOutcome::Completed(result.total()),
                    Ok(Err(error)) => WriteQueryOutcome::Driver(error),
                    Err(_) => WriteQueryOutcome::Timeout,
                }
            }
        };
        match outcome {
            WriteQueryOutcome::Completed(rows) => Ok(rows),
            WriteQueryOutcome::Cancelled => {
                self.quarantine();
                Err(cancellation_error(ErrorPhase::Write, RemoteEffect::Unknown))
            }
            WriteQueryOutcome::Timeout => {
                self.quarantine();
                Err(timeout_error(ErrorPhase::Write, RemoteEffect::Unknown))
            }
            WriteQueryOutcome::Driver(error) => {
                let requested_effect = if error.code().is_none() {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                };
                let public = driver_error(&error, ErrorPhase::Write, requested_effect);
                if error.code().is_some() {
                    let _ = self.transaction.apply(TransactionEvent::StatementFailed);
                } else {
                    self.quarantine();
                }
                Err(public)
            }
        }
    }

    pub(crate) async fn pump_query_rows(
        &mut self,
        query: tiberius::Query<'static>,
        sender: mpsc::Sender<Result<tiberius::Row>>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.require_state(SessionState::Ready, ErrorPhase::Read)?;
        let Some(client) = self.client.as_mut() else {
            return Err(state_error(ErrorPhase::Read));
        };
        let outcome = pump_rows(client, query, sender, cancellation, self.operation_timeout).await;
        match outcome {
            PumpOutcome::Completed => Ok(()),
            PumpOutcome::Cancelled | PumpOutcome::ReceiverDropped => {
                self.quarantine();
                Err(cancellation_error(ErrorPhase::Read, RemoteEffect::None))
            }
            PumpOutcome::Timeout => {
                self.quarantine();
                Err(timeout_error(ErrorPhase::Read, RemoteEffect::None))
            }
            PumpOutcome::Driver(error) => {
                let public = driver_error(&error, ErrorPhase::Read, RemoteEffect::None);
                self.quarantine();
                Err(public)
            }
        }
    }
}

enum ControlOutcome {
    Completed,
    Cancelled,
    Timeout,
    Driver(tiberius::error::Error),
}

enum QueryOutcome {
    Completed(Vec<Vec<tiberius::Row>>),
    Cancelled,
    Timeout,
    Driver(tiberius::error::Error),
}

enum PumpOutcome {
    Completed,
    Cancelled,
    ReceiverDropped,
    Timeout,
    Driver(tiberius::error::Error),
}

enum WriteQueryOutcome {
    Completed(u64),
    Cancelled,
    Timeout,
    Driver(tiberius::error::Error),
}

async fn execute_and_drain(client: &mut TdsClient, sql: &'static str) -> tiberius::Result<()> {
    client.simple_query(sql).await?.into_results().await?;
    Ok(())
}

async fn query_and_drain(
    client: &mut TdsClient,
    query: tiberius::Query<'static>,
) -> tiberius::Result<Vec<Vec<tiberius::Row>>> {
    query.query(client).await?.into_results().await
}

async fn pump_rows(
    client: &mut TdsClient,
    query: tiberius::Query<'static>,
    sender: mpsc::Sender<Result<tiberius::Row>>,
    cancellation: &CancellationToken,
    operation_timeout: std::time::Duration,
) -> PumpOutcome {
    let stream = tokio::select! {
        _ = cancellation.cancelled() => return PumpOutcome::Cancelled,
        result = tokio::time::timeout(operation_timeout, query.query(client)) => {
            match result {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => return PumpOutcome::Driver(error),
                Err(_) => return PumpOutcome::Timeout,
            }
        }
    };
    let mut rows = stream.into_row_stream();
    loop {
        let next = tokio::select! {
            _ = cancellation.cancelled() => return PumpOutcome::Cancelled,
            result = tokio::time::timeout(operation_timeout, rows.try_next()) => {
                match result {
                    Ok(Ok(row)) => row,
                    Ok(Err(error)) => return PumpOutcome::Driver(error),
                    Err(_) => return PumpOutcome::Timeout,
                }
            }
        };
        let Some(row) = next else {
            return PumpOutcome::Completed;
        };
        let sent = tokio::select! {
            _ = cancellation.cancelled() => return PumpOutcome::Cancelled,
            result = sender.send(Ok(row)) => result,
        };
        if sent.is_err() {
            return PumpOutcome::ReceiverDropped;
        }
    }
}

fn state_error(phase: ErrorPhase) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidPlan,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: "stato sessione SQL Server incompatibile con l'operazione".to_owned(),
    }
}
