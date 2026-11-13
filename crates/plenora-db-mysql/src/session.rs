//! Limiti dei diagnostici: SHOW WARNINGS osserva solo lo statement esterno.
//! CALL non garantisce visibilita delle conversioni interne alla procedura.
//! Limite intenzionale: `lc_time_names`, `default_week_format`, `block_encryption_mode`,
//! `old_mode`, `timestamp` e `insert_id` non sono fissate per lo SQL nativo del chiamante.
//! Funzioni e trigger usano lo `sql_mode` registrato alla CREATE. Una variabile
//! cambiata e ripristinata nella stessa istruzione (trigger BEFORE/AFTER) non e
//! osservabile dalla rilettura del profilo. `collation_connection` usa
//! `utf8mb4_general_ci`: PAD SPACE anche nei confronti fra letterali del chiamante.
//! Con `lower_case_table_names=1` il database configurato deve essere gia minuscolo:
//! il confronto esatto rifiuta esplicitamente database diversi, senza assumere
//! che il case folding Unicode del client coincida con quello del server.
//! `max_sort_length` copre 8 MiB: oltre tale prefisso TEXT/BLOB non hanno una
//! garanzia di ordinamento completo; valori equivalenti richiedono un tie-breaker.
//! Uno stream abbandonato prima di EOF non certifica i batch gia consegnati:
//! il drop chiude il canale Arrow, senza poter restituire un errore al consumer.

use crate::config::MysqlConfig;
use crate::error::{driver_error, interruption_error, server_code, timeout_error};
use crate::profile::ProductProfile;
use futures_util::StreamExt;
use mysql_async::prelude::{Queryable, StatementLike};
use mysql_async::{Conn, Params, Row, Statement};
use plenora_database_core::transaction::after_commit_sent;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

/// Il bootstrap di ogni sessione del pool, rieseguito dopo ogni reset.
///
/// La regola W2 (sessione fissata): ogni impostazione di sessione che
/// spegnerebbe in silenzio una garanzia del target ha qui il valore che la
/// applica. `foreign_key_checks = 0` (anche soltanto globale: la sessione
/// nuova lo eredita) lasciava committare un orfano, misurato il 25/10;
/// `unique_checks = 0` permette a `InnoDB` di non verificare l'unicita degli
/// indici secondari; `sql_mode` senza `STRICT_TRANS_TABLES` troncherebbe e
/// convertirebbe valori con un avviso invece di un errore (il target e
/// sempre `InnoDB`, transazionale: la modalita stretta vale per ogni riga);
/// `sql_select_limit` sotto il massimo troncherebbe ogni `SELECT` della
/// sessione, le letture del catalogo comprese (`DEFAULT` darebbe il valore
/// globale, non il massimo).
///
/// `NO_AUTO_VALUE_ON_ZERO`: senza, uno 0 scritto in una colonna
/// `AUTO_INCREMENT` diventa il prossimo valore della sequenza. Misurato il
/// 25/10 (seconda rilettura, `MySQL` 9.7 e `MariaDB` 12.3): Append e Upsert di
/// `(0,'zero')` su `(1,a),(2,b),(3,a)` committavano la riga `4:zero`. Con la
/// modalita lo 0 si scrive come 0, come ogni altro valore; vale anche per lo
/// SQL del chiamante sulla stessa connessione, che per ottenere il valore
/// della sequenza scrive `NULL` o omette la colonna. `sql_mode` viene
/// confrontato come insieme, indipendentemente dall'ordine del server.
/// I tre modi zero-date/divisione insieme a strict evitano il warning 3135.
/// `max_error_count` si legge senza SET: richiede privilegi amministrativi in
/// MySQL. I diagnostici incompleti vengono comunque rifiutati da `check_warnings`.
/// `div_precision_increment = 4` rende esplicito il default dei server:
/// cambia la scala di `(a / b)` generato da plenora-database-sql.
/// `max_sort_length = 8388608` e il massimo comune MySQL/MariaDB: copre
/// interamente CHAR/VARCHAR e TEXT/BLOB fino a 8 MiB, non LONGTEXT arbitrari.
/// Le tre nuove variabili comuni e `check_constraint_checks` di MariaDB sono
/// SET SESSION non amministrativi, verificati dal gate con account minimo.
/// La scrittura rilegge i valori (`crate::write::verify_write_session`).
pub const SESSION_BOOTSTRAP_SQL: &str = "SET SESSION autocommit = 1, time_zone = '+00:00', sql_mode = 'NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION', foreign_key_checks = 1, unique_checks = 1, sql_notes = 1, sql_select_limit = 18446744073709551615, character_set_client = 'utf8mb4', character_set_connection = 'utf8mb4', character_set_results = 'utf8mb4', collation_connection = 'utf8mb4_general_ci', div_precision_increment = 4, max_sort_length = 8388608, sql_auto_is_null = 0";

pub const MARIADB_BOOTSTRAP_SQL: &str = "SET SESSION check_constraint_checks = 1";

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
    // `const` fuori dai test, dove `commit_sql` e una costante; dentro no,
    // perche legge l'interruttore. La firma resta una sola: due firme
    // vorrebbero dire due percorsi, e il punto della misura e che il percorso
    // sia lo stesso.
    #[cfg_attr(not(test), allow(clippy::missing_const_for_fn))]
    fn sql(self) -> &'static str {
        match self {
            Self::Start => "START TRANSACTION",
            // Nei soli test, la risposta al commit puo essere trattenuta dopo che
            // la transazione e conclusa sul server. La cancellazione in questa
            // finestra verifica deterministicamente un esito di commit ambiguo.
            Self::Commit => commit_sql(),
            Self::Rollback => ROLLBACK_SQL,
        }
    }
}

const COMMIT_SQL: &str = "COMMIT";
const ROLLBACK_SQL: &str = "ROLLBACK";

#[cfg(not(test))]
const fn commit_sql() -> &'static str {
    COMMIT_SQL
}

/// Il commit, con la risposta trattenuta se la guardia e viva.
///
/// `DO SLEEP(n)` invece di `SELECT SLEEP(n)`: non produce un result set, e
/// quindi non cambia cosa il client deve leggere oltre al ritardo.
#[cfg(test)]
fn commit_sql() -> &'static str {
    if DELAYED_COMMIT.load(std::sync::atomic::Ordering::SeqCst) {
        "COMMIT; DO SLEEP(5)"
    } else {
        COMMIT_SQL
    }
}

#[cfg(test)]
static DELAYED_COMMIT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Trattiene la risposta del commit finche la guardia resta viva.
///
/// Globale e non per-thread, per la stessa ragione del bypass `MariaDB`: il
/// provider gira su un runtime multi-thread, e un interruttore legato al
/// thread del test non sarebbe visibile dentro il task che apre la
/// connessione. E si spegne al `Drop`, perche un interruttore che si accende e
/// basta lascerebbe ogni commit successivo del binario di test in attesa di
/// cinque secondi.
#[allow(clippy::redundant_pub_crate)]
#[cfg(test)]
pub(crate) struct DelayedCommitResponse;

#[allow(clippy::redundant_pub_crate)]
#[cfg(test)]
impl DelayedCommitResponse {
    pub(crate) fn engage() -> Self {
        DELAYED_COMMIT.store(true, std::sync::atomic::Ordering::SeqCst);
        Self
    }
}

#[cfg(test)]
impl Drop for DelayedCommitResponse {
    fn drop(&mut self) {
        DELAYED_COMMIT.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

pub struct MysqlSession {
    /// Il prodotto servito dalla connessione, ereditato dal pool.
    profile: &'static dyn crate::profile::ProductProfile,
    connection: Option<Conn>,
    expected_database: String,
    expected_profile: Option<crate::session_profile::SessionProfile>,
    #[cfg(test)]
    pub(crate) raw_write_probe: bool,
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
        let opts = config.driver_opts(crate::profile::MYSQL_PROFILE.product())?;
        let connect = Conn::new(opts);
        let connection = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(interruption_error(&crate::profile::MYSQL_PROFILE, cancellation, ErrorPhase::Connect, RemoteEffect::None));
            }
            result = tokio::time::timeout(config.connect_timeout(), connect) => {
                match result {
                    Ok(Ok(connection)) => connection,
                    Ok(Err(error)) => {
                        return Err(driver_error(&crate::profile::MYSQL_PROFILE, &error, ErrorPhase::Connect, RemoteEffect::None));
                    }
                    Err(_) => return Err(timeout_error(&crate::profile::MYSQL_PROFILE, ErrorPhase::Connect, RemoteEffect::None)),
                }
            }
        };
        let mut session = Self {
            profile: &crate::profile::MYSQL_PROFILE,
            expected_database: config.database().to_owned(),
            expected_profile: None,
            #[cfg(test)]
            raw_write_probe: false,
            connection: Some(connection),
            state: MysqlSessionState::Ready,
            operation_timeout: config.operation_timeout(),
            pool_permit: None,
        };
        session.capture_session_profile(cancellation).await?;
        Ok(session)
    }

    pub(crate) const fn from_connection(
        connection: Conn,
        operation_timeout: std::time::Duration,
        pool_permit: tokio::sync::OwnedSemaphorePermit,
        profile: &'static dyn crate::profile::ProductProfile,
        expected_database: String,
    ) -> Self {
        Self {
            profile,
            expected_database,
            expected_profile: None,
            #[cfg(test)]
            raw_write_probe: false,
            connection: Some(connection),
            state: MysqlSessionState::Ready,
            operation_timeout,
            pool_permit: Some(pool_permit),
        }
    }

    /// Chiamato esclusivamente dopo il setup del driver, mai dopo SQL applicativo.
    pub(crate) async fn capture_session_profile(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if self.expected_profile.is_some() {
            return self
                .verify_session_profile(ErrorPhase::Connect, cancellation)
                .await;
        }
        let result = self
            .query_rows(
                &crate::session_profile::select_sql(
                    self.kind() == plenora_database_core::plan::ProviderKind::Mariadb,
                ),
                ErrorPhase::Connect,
                cancellation,
            )
            .await
            .and_then(|rows| {
                crate::session_profile::SessionProfile::capture(&rows, &self.expected_database)
                    .map_err(|name| self.profile_error(ErrorPhase::Connect, name))
            });
        match result {
            Ok(profile) => {
                self.expected_profile = Some(profile);
                Ok(())
            }
            Err(error) => {
                self.quarantine().await;
                Err(error)
            }
        }
    }

    fn profile_error(&self, phase: ErrorPhase, name: &'static str) -> DatabaseError {
        DatabaseError::new(
            ErrorCategory::Execution,
            phase,
            Some(self.kind()),
            format!("profilo di sessione diverso da quello configurato: {name}"),
        )
    }

    /// Rilegge il profilo dopo i diagnostici dello statement, senza riparare drift.
    pub(crate) async fn verify_session_profile(
        &mut self,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        #[cfg(test)]
        if self.raw_write_probe {
            return Ok(());
        }
        let rows = self
            .query_rows(
                &crate::session_profile::select_sql(
                    self.kind() == plenora_database_core::plan::ProviderKind::Mariadb,
                ),
                phase,
                cancellation,
            )
            .await;
        let result = rows.and_then(|rows| {
            let divergence = self
                .expected_profile
                .as_ref()
                .map_or(Some("profile_missing"), |profile| {
                    profile.divergence(&rows, &self.expected_database)
                });
            divergence.map_or(Ok(()), |name| Err(self.profile_error(phase, name)))
        });
        if result.is_err() {
            self.quarantine().await;
        }
        result
    }

    /// Il prodotto servito dalla connessione.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn kind(&self) -> plenora_database_core::plan::ProviderKind {
        self.profile.kind()
    }

    /// Il profilo della connessione, per chi deve classificare e non solo
    /// attribuire.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn profile(&self) -> &'static dyn crate::profile::ProductProfile {
        self.profile
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

    /// Legge i diagnostici prima di qualunque altro statement sulla sessione.
    /// Il testo del server puo contenere valori: si classificano solo i codici.
    pub(crate) async fn check_warnings(
        &mut self,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.check_warnings_for_sql(phase, cancellation, "").await
    }

    pub(crate) async fn check_warnings_for_sql(
        &mut self,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
        sql: &str,
    ) -> Result<()> {
        #[cfg(test)]
        if self.raw_write_probe {
            return Ok(());
        }
        let profile = self.profile;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(phase, profile))?;
        let count = connection.get_warnings();
        if count == 0 {
            return Ok(());
        }
        let query = connection.query::<Row, _>("SHOW WARNINGS");
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(profile, cancellation, phase, RemoteEffect::Unknown));
            }
            result = tokio::time::timeout(self.operation_timeout, query) => result,
        };
        match outcome {
            Ok(Ok(rows)) => {
                // max_error_count puo limitare SHOW WARNINGS: diagnostici mancanti
                // non sono una prova di innocuita.
                let complete = warning_count_complete(count, rows.len());
                if complete
                    && rows.iter().all(|row| {
                        let level = row
                            .get_opt::<String, _>(0)
                            .and_then(std::result::Result::ok);
                        let code = row.get_opt::<u16, _>(1).and_then(std::result::Result::ok);
                        informative_warning_for_sql(level.as_deref(), code, sql)
                    })
                {
                    Ok(())
                } else {
                    Err(warning_error(profile.kind(), phase))
                }
            }
            Ok(Err(error)) => {
                self.quarantine().await;
                Err(driver_error(profile, &error, phase, RemoteEffect::Unknown))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(profile, phase, RemoteEffect::Unknown))
            }
        }
    }

    pub(crate) async fn query_rows(
        &mut self,
        sql: &str,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Row>> {
        let profile = self.profile;
        self.require_ready(phase)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(phase, profile))?;
        let query = connection.query::<Row, _>(sql);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, query) => result,
        };
        match outcome {
            Ok(Ok(rows)) => {
                self.check_warnings_for_sql(phase, cancellation, sql)
                    .await?;
                Ok(rows)
            }
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile,
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(self.profile, phase, RemoteEffect::None))
            }
        }
    }

    fn require_ready(&self, phase: ErrorPhase) -> Result<()> {
        let profile = self.profile;
        if self.is_reusable() {
            Ok(())
        } else {
            Err(state_error(phase, profile))
        }
    }

    pub(crate) async fn exec_rows(
        &mut self,
        sql: &str,
        parameters: Params,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Row>> {
        let profile = self.profile;
        self.require_ready(phase)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(phase, profile))?;
        let query = connection.exec::<Row, _, _>(sql, parameters);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, query) => result,
        };
        match outcome {
            Ok(Ok(rows)) => {
                self.check_warnings_for_sql(phase, cancellation, sql)
                    .await?;
                Ok(rows)
            }
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile,
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(self.profile, phase, RemoteEffect::None))
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
        self.exec_write_tracked(
            sql,
            parameters,
            phase,
            cancellation,
            &plenora_database_core::caller_sql::SendMarker::default(),
        )
        .await
    }

    pub(crate) async fn exec_write_tracked(
        &mut self,
        sql: &str,
        parameters: Params,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
        sent: &plenora_database_core::caller_sql::SendMarker,
    ) -> Result<u64> {
        let profile = self.profile;
        self.require_ready(phase)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(phase, profile))?;
        let execution = sent.sending(connection.exec_drop(sql, parameters));
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(())) => {
                let affected = self
                    .connection
                    .as_ref()
                    .map(Conn::affected_rows)
                    .ok_or_else(|| state_error(phase, profile))?;
                self.check_warnings_for_sql(phase, cancellation, sql)
                    .await?;
                Ok(affected)
            }
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile,
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(self.profile, phase, RemoteEffect::None))
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
        self.exec_row_write_tracked(
            sql,
            parameters,
            cancellation,
            &plenora_database_core::caller_sql::SendMarker::default(),
        )
        .await
    }

    pub(crate) async fn exec_row_write_tracked(
        &mut self,
        sql: &str,
        parameters: Params,
        cancellation: &CancellationToken,
        sent: &plenora_database_core::caller_sql::SendMarker,
    ) -> Result<Option<&'static str>> {
        let profile = self.profile;
        let phase = ErrorPhase::Write;
        self.require_ready(phase)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(phase, profile))?;
        let execution = sent.sending(connection.exec_drop(sql, parameters));
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, phase, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(())) => {
                let affected = connection.affected_rows();
                self.check_warnings(phase, cancellation).await?;
                match validate_row_write_affected_rows(affected, profile) {
                    Ok(()) => Ok(None),
                    Err(error) => {
                        self.quarantine().await;
                        Err(error)
                    }
                }
            }
            Ok(Err(error)) => {
                if let Some(cause) =
                    server_code(&error).and_then(|code| self.profile.row_rejection_cause(code))
                {
                    return Ok(Some(cause));
                }
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile,
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(self.profile, phase, RemoteEffect::None))
            }
        }
    }

    /// Esegue SQL senza parametri via **text protocol**, controllando i confini dei risultati.
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
        self.exec_control_tracked(
            sql,
            phase,
            cancellation,
            &plenora_database_core::caller_sql::SendMarker::default(),
        )
        .await
    }

    pub(crate) async fn exec_control_tracked(
        &mut self,
        sql: &str,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
        sent: &plenora_database_core::caller_sql::SendMarker,
    ) -> Result<()> {
        plenora_database_core::native_query_policy::enforce_single_mysql_statement(sql)?;
        let profile = self.profile;
        self.require_ready(phase)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(phase, profile))?;
        let execution = sent.sending(checked_text_results(connection, sql));
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, phase, RemoteEffect::Unknown));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(false)) => self.check_warnings_for_sql(phase, cancellation, sql).await,
            Ok(Ok(true)) => {
                self.quarantine().await;
                Err(warning_error(self.kind(), phase))
            }
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                Err(driver_error(
                    self.profile,
                    &error,
                    phase,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(self.profile, phase, RemoteEffect::Unknown))
            }
        }
    }

    pub(crate) async fn exec_transaction(
        &mut self,
        command: MysqlTransactionCommand,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let profile = self.profile;
        self.require_ready(phase)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(phase, profile))?;
        let execution = checked_text_results(connection, command.sql());
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, phase, RemoteEffect::Unknown));
            }
            result = tokio::time::timeout(self.operation_timeout, execution) => result,
        };
        match outcome {
            Ok(Ok(unexpected_results)) => {
                let result = if unexpected_results {
                    Err(warning_error(self.kind(), phase))
                } else {
                    self.check_warnings(phase, cancellation).await
                };
                if result.is_err() {
                    self.quarantine().await;
                }
                result.map_err(|error| match command {
                    MysqlTransactionCommand::Commit => after_commit_sent(error, false),
                    MysqlTransactionCommand::Rollback => error.after_unconfirmed_rollback(),
                    MysqlTransactionCommand::Start => error,
                })
            }
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                let public = driver_error(self.profile, &error, phase, RemoteEffect::None);
                // Il comando e partito: regola unica dopo il COMMIT, e un
                // ROLLBACK che fallisce non e confermato.
                Err(match command {
                    MysqlTransactionCommand::Commit => commit_failure(self.profile, &error),
                    MysqlTransactionCommand::Rollback => public.after_unconfirmed_rollback(),
                    MysqlTransactionCommand::Start => public,
                })
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(self.profile, phase, RemoteEffect::Unknown))
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

    /// Impedisce il riciclo quando una transazione viene abbandonata.
    /// Il task possiede la connessione e non puo quindi rientrare nel pool.
    pub(crate) fn quarantine_on_drop(&mut self) {
        self.state = MysqlSessionState::Quarantined;
        let Some(connection) = self.connection.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = connection.disconnect().await;
            });
        } else {
            // Senza executor non esiste una chiusura async possibile. Perdere
            // il lease e preferibile a restituire al pool una transazione
            // aperta: il processo rilascera il socket alla propria uscita.
            std::mem::forget(connection);
        }
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
        let profile = self.profile;
        self.require_ready(ErrorPhase::Prepare)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(ErrorPhase::Prepare, profile))?;
        let prepare = connection.prep(sql);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, ErrorPhase::Prepare, RemoteEffect::None));
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
                    self.profile,
                    &error,
                    ErrorPhase::Prepare,
                    RemoteEffect::None,
                ))
            }
            Err(_) => {
                self.quarantine().await;
                Err(timeout_error(
                    self.profile,
                    ErrorPhase::Prepare,
                    RemoteEffect::None,
                ))
            }
        }
    }

    /// Le colonne del catalogo che il server lega alla projection di `sql`
    /// (`org_name` dei metadati del prepare, senza eseguire), nell'ordine;
    /// `None` se il server non lega un nome (1054, colonna sconosciuta). La
    /// regola A di `spatial_operand`: il nome lo lega il server.
    pub(crate) async fn prepared_column_origins(
        &mut self,
        sql: &str,
        cancellation: &CancellationToken,
    ) -> Result<Option<Vec<String>>> {
        /// `ER_BAD_FIELD_ERROR`.
        const UNKNOWN_COLUMN: u16 = 1054;
        let profile = self.profile;
        self.require_ready(ErrorPhase::Prepare)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| state_error(ErrorPhase::Prepare, profile))?;
        let prepare = connection.prep(sql);
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                self.quarantine().await;
                return Err(interruption_error(self.profile, cancellation, ErrorPhase::Prepare, RemoteEffect::None));
            }
            result = tokio::time::timeout(self.operation_timeout, prepare) => result,
        };
        let statement = match outcome {
            Ok(Ok(statement)) => statement,
            Ok(Err(error)) if server_code(&error) == Some(UNKNOWN_COLUMN) => return Ok(None),
            Ok(Err(error)) => {
                if error.is_fatal() {
                    self.quarantine().await;
                }
                return Err(driver_error(
                    self.profile,
                    &error,
                    ErrorPhase::Prepare,
                    RemoteEffect::None,
                ));
            }
            Err(_) => {
                self.quarantine().await;
                return Err(timeout_error(
                    self.profile,
                    ErrorPhase::Prepare,
                    RemoteEffect::None,
                ));
            }
        };
        let origins = statement
            .columns()
            .iter()
            .map(|column| column.org_name_str().into_owned())
            .collect();
        Ok(Some(origins))
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
        let profile = self.profile;
        let operation_timeout = self.operation_timeout;
        let outcome = {
            let connection = self
                .connection
                .as_mut()
                .ok_or_else(|| state_error(ErrorPhase::Read, profile))?;
            let open = connection.exec_stream::<Row, _, _>(statement, parameters);
            let stream = tokio::select! {
                _ = cancellation.cancelled() => {
                    None
                }
                result = tokio::time::timeout(operation_timeout, open) => {
                    match result {
                        Ok(Ok(stream)) => Some(Ok(stream)),
                        Ok(Err(error)) => Some(Err(driver_error(self.profile,
                            &error,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ))),
                        Err(_) => Some(Err(timeout_error(self.profile,
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
                                self.profile,
                                &error,
                                ErrorPhase::Read,
                                RemoteEffect::None,
                            ));
                        }
                        Ok(None) => break PumpOutcome::Drained,
                        Err(_) => {
                            break PumpOutcome::Failed(timeout_error(
                                self.profile,
                                ErrorPhase::Read,
                                RemoteEffect::None,
                            ));
                        }
                    }
                },
            }
        };
        match outcome {
            PumpOutcome::Drained => self.check_warnings(ErrorPhase::Read, cancellation).await,
            PumpOutcome::Cancelled => {
                self.quarantine().await;
                Err(interruption_error(
                    self.profile,
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

fn row_count_mismatch_error(profile: &dyn crate::profile::ProductProfile) -> DatabaseError {
    DatabaseError {
        read_loss: None,
        category: ErrorCategory::Protocol,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::Quarantine,
        provider: Some(profile.kind()),
        execution_id: None,
        message: format!(
            "conteggio righe {} incoerente per statement row-scoped",
            profile.product()
        ),
        loss_report: None,
        diagnostics: None,
    }
}

fn validate_row_write_affected_rows(
    affected_rows: u64,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<()> {
    if affected_rows == 1 {
        Ok(())
    } else {
        Err(row_count_mismatch_error(profile))
    }
}

#[allow(clippy::redundant_pub_crate)]
pub(crate) fn state_error(
    phase: ErrorPhase,
    profile: &dyn crate::profile::ProductProfile,
) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        phase,
        Some(profile.kind()),
        format!("sessione {} non riusabile", profile.product()),
    )
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

/// L'errore di un COMMIT inviato: regola unica di `after_commit_sent`, con
/// l'elenco chiuso del prodotto (`ProductProfile::commit_rollback_codes`).
pub fn commit_failure(
    profile: &dyn ProductProfile,
    error: &mysql_async::Error,
) -> plenora_database_core::DatabaseError {
    let public = driver_error(profile, error, ErrorPhase::Commit, RemoteEffect::None);
    after_commit_sent(
        public,
        server_code(error).is_some_and(|code| profile.commit_rollback_codes().contains(&code)),
    )
}

#[cfg(test)]
#[path = "caller_sql_session_tests.rs"]
mod caller_sql_session_tests;

/// Nessun messaggio del server attraversa il confine pubblico.
/// L'effetto resta ignoto finche il chiamante non conferma il rollback.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn warning_error(
    kind: plenora_database_core::plan::ProviderKind,
    phase: ErrorPhase,
) -> DatabaseError {
    let mut error = DatabaseError::new(
        ErrorCategory::DataMapping,
        phase,
        Some(kind),
        "diagnostico server non qualificato come informativo: risultato rifiutato",
    );
    error.remote_effect = RemoteEffect::Unknown;
    error.retry = RetryDisposition::RequiresRecovery;
    error
}

// Note DDL IF [NOT] EXISTS: nessuna conversione di valori, solo oggetto
// gia presente (1007/1050) o assente (1008/1051). 1287/1681 segnalano
// soltanto sintassi deprecata (anche VALUES nell'upsert), senza alterazione.
// Fonti: MySQL server-error-reference, ER_WARN_DEPRECATED_SYNTAX e
// ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT; stessi codici nel catalogo MariaDB.
// Ogni altro codice resta chiuso, inclusi conversioni e troncamenti.
fn informative_warning(level: Option<&str>, code: Option<u16>) -> bool {
    matches!((level, code), (Some("Warning"), Some(1287 | 1681)))
}

// Il contatore OK e saturante: 65535 non prova la completezza dei diagnostici.
fn warning_count_complete(count: u16, rows: usize) -> bool {
    count != u16::MAX && rows == usize::from(count)
}

// Le note di oggetto assente sono innocue solo nella DDL condizionale.
// 1305: routine assente; 1360: trigger assente; 1003: piano EXPLAIN riscritto.
// 1060/1061: colonna/indice gia presente; 1091: colonna/indice assente.
// Si ammettono solo le relative clausole IF [NOT] EXISTS, che saltano la
// sotto-operazione (MariaDB ALTER TABLE reference); altri warning restano
// bloccanti anche in ALTER misti. Utenti/eventi restano non qualificati.
// 1329 resta rifiutato: SELECT INTO puo lasciare il valore precedente.
// Fonti: https://mariadb.com/docs/server/reference/sql-statements/data-definition/alter/alter-table
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
// 1356 non e informativo: metadati di una vista non risolvibile non sono validi.
fn informative_warning_for_sql(level: Option<&str>, code: Option<u16>, sql: &str) -> bool {
    if informative_warning(level, code) {
        return true;
    }
    if level != Some("Note") {
        return false;
    }
    let words: Vec<_> = sql
        .split_whitespace()
        .take(9)
        .map(str::to_ascii_uppercase)
        .collect();
    let prefix = |expected: &[&str]| {
        words.len() >= expected.len()
            && words
                .iter()
                .zip(expected)
                .all(|(actual, expected)| actual == expected)
    };
    let alter = |clause: &[&str]| {
        prefix(&["ALTER", "TABLE"])
            && words.len() >= 3 + clause.len()
            && words[3..]
                .iter()
                .zip(clause)
                .all(|(actual, expected)| actual == expected)
    };
    match code {
        Some(1060) => {
            alter(&["ADD", "COLUMN", "IF", "NOT", "EXISTS"])
                || alter(&["ADD", "IF", "NOT", "EXISTS"])
        }
        Some(1061) => {
            alter(&["ADD", "INDEX", "IF", "NOT", "EXISTS"])
                || alter(&["ADD", "KEY", "IF", "NOT", "EXISTS"])
                || prefix(&["CREATE", "INDEX", "IF", "NOT", "EXISTS"])
        }
        Some(1091) => {
            alter(&["DROP", "COLUMN", "IF", "EXISTS"])
                || alter(&["DROP", "INDEX", "IF", "EXISTS"])
                || alter(&["DROP", "KEY", "IF", "EXISTS"])
                || prefix(&["DROP", "INDEX", "IF", "EXISTS"])
        }
        Some(1007) => {
            prefix(&["CREATE", "DATABASE", "IF", "NOT", "EXISTS"])
                || prefix(&["CREATE", "SCHEMA", "IF", "NOT", "EXISTS"])
        }
        Some(1008) => {
            prefix(&["DROP", "DATABASE", "IF", "EXISTS"])
                || prefix(&["DROP", "SCHEMA", "IF", "EXISTS"])
        }
        Some(1050) => prefix(&["CREATE", "TABLE", "IF", "NOT", "EXISTS"]),
        Some(1051) => prefix(&["DROP", "TABLE", "IF", "EXISTS"]),
        Some(1305) => {
            prefix(&["DROP", "PROCEDURE", "IF", "EXISTS"])
                || prefix(&["DROP", "FUNCTION", "IF", "EXISTS"])
        }
        Some(1360) => prefix(&["DROP", "TRIGGER", "IF", "EXISTS"]),
        Some(1003) => prefix(&["EXPLAIN"]),
        _ => false,
    }
}

/// Ogni comando ammesso dal lexer contiene uno statement. Il driver avanza
/// al set successivo quando legge EOF: controlliamo l'OK prima di avanzare,
/// e contiamo anche i set senza colonne (`is_empty` non basta per quelli).
#[allow(clippy::redundant_pub_crate)]
pub(super) async fn checked_text_results(
    connection: &mut Conn,
    sql: &str,
) -> mysql_async::Result<bool> {
    let mut result = connection.query_iter(sql).await?;
    let mut count = 0;
    loop {
        if result.columns_ref().is_empty() && !result.is_empty() && result.warnings() != 0 {
            return Ok(true);
        }
        let Some(mut rows) = result.stream::<Row>().await? else {
            break;
        };
        count += 1;
        if count > 1 {
            // Qualsiasi risultato aggiuntivo viola il numero del lexer;
            // anche un OK intermedio con warning non puo essere perso.
            return Ok(true);
        }
        while let Some(row) = rows.next().await {
            row?;
        }
    }
    Ok(false)
}
