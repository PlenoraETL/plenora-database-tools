use crate::config::SqlServerConfig;
use crate::error::{driver_error, interruption_error, timeout_error};
use crate::recovery::{TransactionEvent, TransactionState};
use crate::session::{SessionState, SESSION_BOOTSTRAP_SQL};
use crate::types::SqlServerColumnSpec;
use futures_util::future::FutureExt;
use futures_util::TryStreamExt;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use tiberius::{Client, TokenRow};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub type TdsClient = Client<Compat<TcpStream>>;

pub enum RowQueryResult {
    Applied(Vec<Vec<tiberius::Row>>),
    ServerRejected { code: u32, error: DatabaseError },
}

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
            return Err(interruption_error(
                cancellation,
                ErrorPhase::Connect,
                RemoteEffect::None,
            ));
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
                return Err(interruption_error(
                cancellation,
                ErrorPhase::Connect,
                RemoteEffect::None,
            ));
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

    /// Il client TDS, per chi deve tenerlo prestato piu a lungo di una query.
    ///
    /// Esiste per lo **stream** della transazione. Ogni altro percorso passa da
    /// `execute_query` o `execute_write_query`, che aprono e chiudono il
    /// prestito dentro una chiamata sola; uno stream server-side no — vive
    /// finche il chiamante consuma le righe, e in quel tempo nessun altro deve
    /// poter usare la stessa sessione.
    ///
    /// Il vincolo non e affidato alla buona volonta: il prestito e `&mut`, e il
    /// borrow checker rifiuta un secondo uso mentre lo stream e vivo. E' la
    /// stessa garanzia che il contratto di `query_stream` descrive a parole —
    /// «finche e vivo, nessun altro execute/query sulla stessa transazione e
    /// consentito» — e qui e il compilatore a farla rispettare.
    ///
    /// # Errors
    ///
    /// Se la sessione non e in transazione, o il client e assente perche la
    /// connessione e stata messa in quarantena.
    pub fn client_mut(&mut self, phase: ErrorPhase) -> Result<&mut TdsClient> {
        self.require_state(SessionState::Transaction, phase)?;
        self.client.as_mut().ok_or_else(|| state_error(phase))
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
                Err(interruption_error(cancellation, phase, remote_effect))
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
                // Il driver **va in panico** su due famiglie di tipo, e un
                // panico non e un errore: attraversa lo stack e lascia la
                // connessione a meta protocollo. Se tornasse al pool cosi, il
                // prestito successivo la troverebbe con i pacchetti di
                // qualcun altro in coda.
                //
                // `tiberius` 0.12.3 ha un `todo!()` in `TypeInfo::decode` per
                // `Udt` e `SSVariant`: basta che una SELECT renda una colonna
                // `geometry`, `geography` o `sql_variant`, e succede sui
                // **metadati**, prima di ogni riga — quindi nessun controllo
                // sul valore potrebbe prevenirlo.
                //
                // Catturarlo qui e la sola forma che lasci il pool sano.
                std::panic::AssertUnwindSafe(query_and_drain(client, query)).catch_unwind(),
            ) => match result {
                Ok(Ok(Ok(rows))) => QueryOutcome::Completed(rows),
                Ok(Ok(Err(error))) => QueryOutcome::Driver(error),
                Ok(Err(_)) => QueryOutcome::DriverPanic,
                Err(_) => QueryOutcome::Timeout,
            },
        };
        match outcome {
            QueryOutcome::Completed(rows) => Ok(rows),
            // Il messaggio nomina le famiglie e la via d'uscita — per una
            // geometria, `.AsBinaryZM()` nella proiezione — invece di
            // riportare il testo del panico, che e interno al driver e non
            // aiuta chi legge.
            QueryOutcome::DriverPanic => {
                self.quarantine();
                Err(DatabaseError::unsupported(
                    plenora_database_core::plan::ProviderKind::Sqlserver,
                    phase,
                    "il driver SQL Server non decodifica le colonne UDT e sql_variant: \
                     per una geometria usare .AsBinaryZM() nella proiezione",
                ))
            }
            QueryOutcome::Cancelled => {
                self.quarantine();
                Err(interruption_error(cancellation, phase, RemoteEffect::None))
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

    pub(super) async fn execute_row_query(
        &mut self,
        query: tiberius::Query<'static>,
        cancellation: &CancellationToken,
    ) -> Result<RowQueryResult> {
        self.require_state(SessionState::Transaction, ErrorPhase::Write)?;
        let Some(client) = self.client.as_mut() else {
            return Err(state_error(ErrorPhase::Write));
        };
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => QueryOutcome::Cancelled,
            result = tokio::time::timeout(
                self.operation_timeout,
                // Il driver **va in panico** su due famiglie di tipo, e un
                // panico non e un errore: attraversa lo stack e lascia la
                // connessione a meta protocollo. Se tornasse al pool cosi, il
                // prestito successivo la troverebbe con i pacchetti di
                // qualcun altro in coda.
                //
                // `tiberius` 0.12.3 ha un `todo!()` in `TypeInfo::decode` per
                // `Udt` e `SSVariant`: basta che una SELECT renda una colonna
                // `geometry`, `geography` o `sql_variant`, e succede sui
                // **metadati**, prima di ogni riga — quindi nessun controllo
                // sul valore potrebbe prevenirlo.
                //
                // Catturarlo qui e la sola forma che lasci il pool sano.
                std::panic::AssertUnwindSafe(query_and_drain(client, query)).catch_unwind(),
            ) => match result {
                Ok(Ok(Ok(rows))) => QueryOutcome::Completed(rows),
                Ok(Ok(Err(error))) => QueryOutcome::Driver(error),
                Ok(Err(_)) => QueryOutcome::DriverPanic,
                Err(_) => QueryOutcome::Timeout,
            },
        };
        match outcome {
            QueryOutcome::Completed(rows) => Ok(RowQueryResult::Applied(rows)),
            // Come nell'altro percorso: il panico del driver diventa un
            // rifiuto, e la connessione non torna al pool.
            QueryOutcome::DriverPanic => {
                self.quarantine();
                Err(DatabaseError::unsupported(
                    plenora_database_core::plan::ProviderKind::Sqlserver,
                    ErrorPhase::Write,
                    "il driver SQL Server non decodifica le colonne UDT e sql_variant:                      per una geometria usare .AsBinaryZM() nella proiezione",
                ))
            }
            QueryOutcome::Cancelled => {
                self.quarantine();
                Err(interruption_error(
                    cancellation,
                    ErrorPhase::Write,
                    RemoteEffect::Unknown,
                ))
            }
            QueryOutcome::Timeout => {
                self.quarantine();
                Err(timeout_error(ErrorPhase::Write, RemoteEffect::Unknown))
            }
            QueryOutcome::Driver(error) => {
                let Some(code) = error.code() else {
                    let public = driver_error(&error, ErrorPhase::Write, RemoteEffect::Unknown);
                    self.quarantine();
                    return Err(public);
                };
                let _ = self.transaction.apply(TransactionEvent::StatementFailed);
                Ok(RowQueryResult::ServerRejected {
                    code,
                    error: driver_error(&error, ErrorPhase::Write, RemoteEffect::None),
                })
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
                Err(interruption_error(
                    cancellation,
                    ErrorPhase::Write,
                    RemoteEffect::Unknown,
                ))
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

    pub(crate) async fn execute_bulk_rows<'a>(
        &'a mut self,
        table: &'a str,
        rows: Vec<TokenRow<'a>>,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        self.require_state(SessionState::Transaction, ErrorPhase::Write)?;
        let operation_timeout = self.operation_timeout;
        let Some(client) = self.client.as_mut() else {
            return Err(state_error(ErrorPhase::Write));
        };
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => WriteQueryOutcome::Cancelled,
            result = tokio::time::timeout(
                operation_timeout,
                bulk_insert_rows(client, table, rows),
            ) => {
                match result {
                    Ok(Ok(inserted)) => WriteQueryOutcome::Completed(inserted),
                    Ok(Err(error)) => WriteQueryOutcome::Driver(error),
                    Err(_) => WriteQueryOutcome::Timeout,
                }
            }
        };
        match outcome {
            WriteQueryOutcome::Completed(rows) => Ok(rows),
            WriteQueryOutcome::Cancelled => {
                self.quarantine();
                Err(interruption_error(
                    cancellation,
                    ErrorPhase::Write,
                    RemoteEffect::Unknown,
                ))
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
                    // Un errore locale di encoding può arrivare dopo che il
                    // request bulk ha già emesso pacchetti: il protocollo non
                    // è più drenabile con certezza.
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
        expected_columns: &[SqlServerColumnSpec],
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.require_state(SessionState::Ready, ErrorPhase::Read)?;
        let Some(client) = self.client.as_mut() else {
            return Err(state_error(ErrorPhase::Read));
        };
        let outcome = pump_rows(
            client,
            query,
            sender,
            expected_columns,
            cancellation,
            self.operation_timeout,
        )
        .await;
        match outcome {
            PumpOutcome::Completed => Ok(()),
            PumpOutcome::Cancelled | PumpOutcome::ReceiverDropped => {
                self.quarantine();
                Err(interruption_error(
                    cancellation,
                    ErrorPhase::Read,
                    RemoteEffect::None,
                ))
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
            PumpOutcome::Public(error) => {
                self.quarantine();
                Err(error)
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
    /// Il driver e morto in un `todo!()` invece di rendere un errore.
    ///
    /// Non e una condizione del server: e una famiglia di tipo che tiberius
    /// 0.12.3 non sa decodificare, e la scopre sui metadati. La connessione
    /// resta a meta protocollo, quindi va in quarantena e non torna al pool.
    DriverPanic,
}

enum PumpOutcome {
    Completed,
    Cancelled,
    ReceiverDropped,
    Timeout,
    Driver(tiberius::error::Error),
    Public(DatabaseError),
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

async fn bulk_insert_rows<'a>(
    client: &'a mut TdsClient,
    table: &'a str,
    rows: Vec<TokenRow<'a>>,
) -> tiberius::Result<u64> {
    let mut request = client.bulk_insert(table).await?;
    for row in rows {
        request.send(row).await?;
    }
    Ok(request.finalize().await?.total())
}

async fn pump_rows(
    client: &mut TdsClient,
    query: tiberius::Query<'static>,
    sender: mpsc::Sender<Result<tiberius::Row>>,
    expected_columns: &[SqlServerColumnSpec],
    cancellation: &CancellationToken,
    operation_timeout: std::time::Duration,
) -> PumpOutcome {
    let mut stream = tokio::select! {
        _ = cancellation.cancelled() => return PumpOutcome::Cancelled,
        result = tokio::time::timeout(operation_timeout, query.query(client)) => {
            match result {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => return PumpOutcome::Driver(error),
                Err(_) => return PumpOutcome::Timeout,
            }
        }
    };
    let metadata = tokio::select! {
        _ = cancellation.cancelled() => return PumpOutcome::Cancelled,
        result = tokio::time::timeout(operation_timeout, stream.columns()) => {
            match result {
                Ok(Ok(columns)) => columns,
                Ok(Err(error)) => return PumpOutcome::Driver(error),
                Err(_) => return PumpOutcome::Timeout,
            }
        }
    };
    let Some(metadata) = metadata else {
        return PumpOutcome::Public(read_protocol_error(
            "QueryOperation SQL Server senza COLMETADATA",
        ));
    };
    if let Err(error) = validate_result_metadata(metadata, expected_columns) {
        return PumpOutcome::Public(error);
    }
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

fn validate_result_metadata(
    actual: &[tiberius::Column],
    expected: &[SqlServerColumnSpec],
) -> Result<()> {
    if actual.len() != expected.len() {
        return Err(read_protocol_error(
            "numero colonne TDS diverso dalla descrizione QueryOperation",
        ));
    }
    for (actual, expected) in actual.iter().zip(expected) {
        if actual.name() != expected.name.as_str() {
            return Err(read_protocol_error(
                "nome colonna TDS diverso dalla descrizione QueryOperation",
            ));
        }
        if !expected.accepts_tds_column_type(actual.column_type()) {
            return Err(read_protocol_error(
                "tipo colonna TDS diverso dalla descrizione QueryOperation",
            ));
        }
    }
    Ok(())
}

fn read_protocol_error(message: &'static str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::Protocol,
        phase: ErrorPhase::Read,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
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
        diagnostics: None,
    }
}
