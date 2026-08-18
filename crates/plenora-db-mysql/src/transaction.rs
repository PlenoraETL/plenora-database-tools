//! Transaction scope MySQL — implementa `TransactionScope` del core.
//!
//! Copre begin con opzioni (isolation, access mode, statement_timeout),
//! savepoint annidati con quoting sicuro, commit/rollback disambiguato
//! (`OutcomeUnknown` in caso di canale compromesso in fase Commit).
//!
//! Non implementa (deferito a minor future):
//! - `query_stream` — richiede cursor MySQL (mysql_async non ha API
//!   nativa: bisogna implementare via SELECT + LIMIT/OFFSET chunked)
//!
//! Riuso: sfrutta `MysqlSession::exec_write` / `query_rows` /
//! `exec_transaction` già presenti. Aggiunge parsing di Row al formato
//! canonico `plenora_database_core::Row`.

#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_const_for_fn,
    clippy::option_if_let_else,
    clippy::future_not_send,
    clippy::significant_drop_tightening
)]

use crate::error::driver_error;
use crate::parameter::bind_positional_params;
use crate::session::{MysqlSession, MysqlTransactionCommand};
use mysql_async::prelude::Queryable;
use mysql_async::{Row as MyRow, Value};
use plenora_database_core::provider::{ParameterValue, ProviderFuture};
use plenora_database_core::row::Row;
use plenora_database_core::transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name,
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{
    plan::ProviderKind, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect,
    Result, RetryDisposition,
};
use std::sync::Arc;

pub struct MysqlTransaction {
    session: MysqlSession,
    open: bool,
    /// Policy di ammissione statement (Allow default, Deny per PFM).
    /// Persistito da `TransactionOptions::native_query_policy`; parity
    /// con `PostgresTransaction`. Fix P1 review MySQL 2026-08-15.
    native_query_policy: plenora_database_core::native_query_policy::NativeQueryPolicy,
}

impl MysqlTransaction {
    /// Apre la transazione emettendo `START TRANSACTION` con opzioni.
    ///
    /// # Errors
    ///
    /// Errore se la sessione non è pronta, il canale è cancellato, o il
    /// server rigetta le opzioni.
    pub async fn begin(
        mut session: MysqlSession,
        options: &TransactionOptions,
        profile: &dyn crate::profile::ProductProfile,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        // 0. Il context si valida **prima** di qualunque statement.
        //    Validarlo al passo 4, dopo START TRANSACTION, significava
        //    aprire una transazione e un isolamento di sessione per poi
        //    scoprire che una chiave non era scrivibile: il chiamante
        //    riceveva `InvalidPlan` con una transazione gia aperta sulla
        //    connessione, che il pool avrebbe poi dovuto ripulire.
        validate_context_keys(options)?;

        // 1. Isolation level (SET TRANSACTION prima di START).
        //    MySQL non supporta "deferrable" (skip); "read only" è opzione
        //    di START TRANSACTION, non di SET.
        if let Some(isolation) = options.isolation {
            let iso_sql = match isolation {
                plenora_database_core::transaction::IsolationLevel::ReadUncommitted => {
                    "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
                }
                plenora_database_core::transaction::IsolationLevel::ReadCommitted => {
                    "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
                }
                plenora_database_core::transaction::IsolationLevel::RepeatableRead => {
                    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                }
                plenora_database_core::transaction::IsolationLevel::Serializable => {
                    "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                }
            };
            raw_exec(&mut session, iso_sql, ErrorPhase::Prepare, cancellation).await?;
        }

        // 2. Statement timeout: quale variabile, e in quale unita, lo decide
        //    il profilo. Qui resta il quando — dopo l'isolamento e prima di
        //    START TRANSACTION — che dipende dalla sequenza, non dal prodotto.
        if let Some(timeout_ms) = options.statement_timeout_ms {
            let sql = profile.statement_timeout_statement(timeout_ms);
            raw_exec(&mut session, &sql, ErrorPhase::Prepare, cancellation).await?;
        }

        // 3. START TRANSACTION [READ ONLY | READ WRITE].
        let start_sql = match options.access_mode {
            Some(plenora_database_core::transaction::AccessMode::ReadOnly) => {
                "START TRANSACTION READ ONLY"
            }
            Some(plenora_database_core::transaction::AccessMode::ReadWrite) => {
                "START TRANSACTION READ WRITE"
            }
            None => "START TRANSACTION",
        };
        raw_exec(&mut session, start_sql, ErrorPhase::Prepare, cancellation).await?;

        // 4. Session context (SET @`plenora_ctx_namespace.name` = value).
        //    MySQL user variables sono session-scoped, resettati alla
        //    disconnessione; non participano al rollback ma è OK: sono
        //    context info, non state applicativo.
        //
        //    A rifiutare le chiavi con il punto eravamo noi, non il server:
        //    `is_safe_context_name` teneva una regola locale che ammetteva
        //    solo alfanumerici e `_`, mentre il core impone `namespace.name`.
        //    Le due validazioni erano mutuamente esclusive, e
        //    `begin(context=...)` non poteva riuscire con un context non
        //    vuoto. A sbloccarlo e stata la delega al core, non il quoting.
        //
        //    I backtick restano perche un nome di variabile utente accetta
        //    piu caratteri di quanti il core ne ammetta oggi (`$`, per dire).
        //    Ma non rendono la resa indipendente da quella regola, ed e
        //    l'affermazione da correggere: qui il backtick nel nome non
        //    viene raddoppiato, quindi una chiave che ne contenesse uno
        //    chiuderebbe la quotatura invece di finirci dentro. A impedirlo
        //    e `validate_context_keys`, che delega al core: oggi il core
        //    ammette `namespace.name` con soli `[a-z0-9_]`, e nessuna chiave
        //    valida contiene un backtick. La sicurezza di questa `format!`
        //    dipende da quella validazione: se il core allargasse la regola,
        //    qui servirebbe il raddoppio.
        //
        //    Verificato che senza backtick il server accetta ugualmente
        //    `@plenora_ctx_app.tenant`.
        for (name, entry) in options.context.iter() {
            let value = entry.value.as_provider_string();
            let sql = format!(
                "SET @`{CONTEXT_VARIABLE_PREFIX}{name}` = {}",
                mysql_string_literal(&value)
            );
            raw_exec(&mut session, &sql, ErrorPhase::Prepare, cancellation).await?;
        }

        Ok(Self {
            session,
            open: true,
            native_query_policy: options.native_query_policy,
        })
    }
}

/// Lunghezza massima di un nome di variabile utente `MySQL`.
const MAX_USER_VARIABLE_NAME: usize = 64;

/// Prefisso applicato a ogni chiave di context.
const CONTEXT_VARIABLE_PREFIX: &str = "plenora_ctx_";

/// Chiave di context piu lunga che il prefisso lascia scrivere.
///
/// Il core ne ammette fino a 63, `MySQL` fino a 64 **incluso il prefisso**:
/// le due soglie non coincidono, e la differenza e proprio la fascia dove il
/// piano sembra valido e il server rifiuta.
const MAX_CONTEXT_KEY: usize = MAX_USER_VARIABLE_NAME - CONTEXT_VARIABLE_PREFIX.len();

/// La chiave di context che `MySQL` accetta di scrivere.
///
/// La regola e quella del core, non una seconda regola locale: il core
/// impone `namespace.name`, quindi un punto, e una versione locale che
/// vietava il punto rifiutava **ogni** chiave valida. Il controllo resta
/// come difesa in profondita — un context puo arrivare da deserializzazione
/// e non solo dai costruttori che gia validano — ma delega la definizione di
/// "sicuro" a chi la possiede.
fn is_safe_context_name(name: &str) -> bool {
    plenora_database_core::session_context::validate_context_key(name).is_ok()
}

/// Verifica ogni chiave di context prima che parta un solo statement.
///
/// E anche cio che rende sana la quotatura del nome in `begin`: quel
/// `format!` non raddoppia i backtick, quindi e questa validazione — non la
/// quotatura — a garantire che nessuna chiave possa chiudere la resa.
///
/// # Errors
///
/// `InvalidPlan` per una chiave che il core non riconosce, o piu lunga di
/// [`MAX_CONTEXT_KEY`]: oltre quella soglia il nome della variabile utente
/// supererebbe i 64 caratteri e il server risponderebbe con un errore di
/// sintassi, a transazione gia aperta.
fn validate_context_keys(options: &TransactionOptions) -> Result<()> {
    for (name, _) in options.context.iter() {
        if !is_safe_context_name(name.as_str()) {
            return Err(DatabaseError::invalid_plan(format!(
                "session context MySQL: nome non sicuro '{name}'"
            )));
        }
        if name.len() > MAX_CONTEXT_KEY {
            return Err(DatabaseError::invalid_plan(format!(
                "session context MySQL: chiave '{name}' di {} caratteri; con il \
                 prefisso '{CONTEXT_VARIABLE_PREFIX}' la variabile utente \
                 supererebbe i {MAX_USER_VARIABLE_NAME} caratteri ammessi, \
                 quindi al massimo {MAX_CONTEXT_KEY}",
                name.len()
            )));
        }
    }
    Ok(())
}

fn mysql_string_literal(value: &str) -> String {
    // Escape single quotes + backslashes. Nessun altro carattere richiede
    // escape con `NO_BACKSLASH_ESCAPES` disabilitato (default MySQL).
    let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

fn quote_savepoint_name(name: &str) -> Result<String> {
    validate_savepoint_name(name)?;
    // Savepoint identifiers vanno backtick-quoted; validate_savepoint_name
    // già rifiuta backtick/nul.
    Ok(format!("`{name}`"))
}

/// Esegue un SQL raw (nessun parametro) sulla sessione via **text protocol**.
///
/// MySQL rifiuta `SET`, `SAVEPOINT`, `START TRANSACTION` ecc. nel prepared
/// statement protocol (errore 1295). `exec_control` usa `query_drop`.
async fn raw_exec(
    session: &mut MysqlSession,
    sql: &str,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
) -> Result<()> {
    session.exec_control(sql, phase, cancellation).await
}

/// Converte una `mysql_async::Row` in `plenora_database_core::Row`.
///
/// I tipi non nativi (Date/Time/Decimal) sono estratti come stringhe UTF-8
/// (formato server-side); il consumer riconverte con `p.date(str)` etc.
fn decode_row(mut row: MyRow, columns: &Arc<[String]>, kind: ProviderKind) -> Result<Row> {
    let mut values = Vec::with_capacity(columns.len());
    for idx in 0..columns.len() {
        let value = row.take_opt::<Value, _>(idx).unwrap_or(Ok(Value::NULL));
        let raw = value.map_err(|error| DatabaseError {
            category: ErrorCategory::DataMapping,
            phase: ErrorPhase::Read,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(kind),
            execution_id: None,
            diagnostics: None,
            message: format!("decode colonna MySQL idx={idx}: {error}"),
        })?;
        values.push(convert_value(raw, idx, kind)?);
    }
    Ok(Row::new(Arc::clone(columns), values))
}

fn convert_value(value: Value, idx: usize, kind: ProviderKind) -> Result<ParameterValue> {
    Ok(match value {
        Value::NULL => ParameterValue::Null {
            type_name: "unknown".to_owned(),
        },
        Value::Int(v) => ParameterValue::I64(v),
        Value::UInt(v) => {
            // MySQL UInt64 può eccedere I64 (>2^63); il decoder canonico non
            // ha un tipo unsigned. Falliamo esplicito piuttosto che overflow.
            i64::try_from(v)
                .map(ParameterValue::I64)
                .map_err(|_| DatabaseError {
                    category: ErrorCategory::DataMapping,
                    phase: ErrorPhase::Read,
                    remote_effect: RemoteEffect::None,
                    retry: RetryDisposition::Never,
                    provider: Some(kind),
                    execution_id: None,
                    diagnostics: None,
                    message: format!("colonna MySQL idx={idx} UInt eccede i64"),
                })?
        }
        Value::Float(v) => ParameterValue::F64(f64::from(v)),
        Value::Double(v) => ParameterValue::F64(v),
        Value::Bytes(bytes) => match std::str::from_utf8(&bytes) {
            Ok(s) => ParameterValue::String(s.to_owned()),
            Err(_) => ParameterValue::Bytes(bytes),
        },
        Value::Date(y, mo, d, h, mi, s, us) => {
            if h == 0 && mi == 0 && s == 0 && us == 0 {
                ParameterValue::Date(format!("{y:04}-{mo:02}-{d:02}"))
            } else {
                ParameterValue::Timestamp(format!(
                    "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}"
                ))
            }
        }
        Value::Time(is_neg, days, h, mi, s, us) => {
            // TIME MySQL può eccedere 24h; rappresentazione canonica:
            // "[-]HHH:MM:SS.uuuuuu"
            let sign = if is_neg { "-" } else { "" };
            let total_hours = u32::from(h) + days * 24;
            ParameterValue::String(format!("{sign}{total_hours:03}:{mi:02}:{s:02}.{us:06}"))
        }
    })
}

// ------------------------------ TransactionScope impl -----------------------

impl TransactionScope for MysqlTransaction {
    fn provider_kind(&self) -> ProviderKind {
        self.session.kind()
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        Box::pin(async move {
            if !self.open {
                return Err(closed_error(ErrorPhase::Write, self.session.kind()));
            }
            // Fix P1 review MySQL: enforcement condiviso del core
            // (parity con `PostgresTransaction::execute`). Se
            // `native_query_policy = Deny`, blocca DDL/session/multi-
            // statement prima del round-trip al server.
            plenora_database_core::native_query_policy::enforce_policy(
                self.native_query_policy,
                &statement.sql,
            )?;
            let params = bind_positional_params(&statement.params)?;
            let result = self
                .session
                .exec_write(&statement.sql, params, ErrorPhase::Write, cancellation)
                .await;
            // Fix P0 review MySQL 2026-08-15: quando MySQL rifiuta lo
            // statement con `RolledBack` (deadlock 1213, ambiguous
            // timeout, ecc.), il server auto-annulla la transazione
            // vittima. Prima `self.open` restava `true` e le scritture
            // successive andavano in **autocommit** (silent write fuori
            // dalla tx supposta). Ora chiudo la tx dopo qualsiasi
            // errore che modifica lo stato server-side.
            if let Err(ref error) = result {
                if matches!(
                    error.remote_effect,
                    RemoteEffect::RolledBack | RemoteEffect::Unknown
                ) {
                    self.open = false;
                }
            }
            result
        })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            if !self.open {
                return Err(closed_error(ErrorPhase::Read, self.session.kind()));
            }
            // Il prodotto della connessione, letto una volta: piu avanti la
            // sessione e prestata in modo esclusivo e non e piu leggibile.
            let kind = self.session.kind();
            // Fix P1 review MySQL: enforcement condiviso (parity Postgres).
            plenora_database_core::native_query_policy::enforce_policy(
                self.native_query_policy,
                &statement.sql,
            )?;
            let params = bind_positional_params(&statement.params)?;
            // Prendo il timeout ORA per evitare borrow conflict con connection_mut sotto.
            let timeout = self.session.operation_timeout();
            let connection = self
                .session
                .connection_mut()
                .ok_or_else(|| closed_error(ErrorPhase::Read, kind))?;
            let execution = connection.exec::<MyRow, _, _>(&statement.sql, params);
            let outcome = tokio::select! {
                _ = cancellation.cancelled() => {
                    self.session.discard().await;
                    self.open = false;
                    return Err(DatabaseError {
                        category: ErrorCategory::Cancelled,
                        phase: ErrorPhase::Read,
                        remote_effect: RemoteEffect::None,
                        retry: RetryDisposition::Never,
                        provider: Some(kind),
                        execution_id: None,
                        diagnostics: None,
                        message: "query MySQL cancellata".to_owned(),
                    });
                }
                result = tokio::time::timeout(timeout, execution) => result,
            };
            let rows = match outcome {
                Ok(Ok(rows)) => rows,
                Ok(Err(error)) => {
                    return Err(driver_error(
                        kind,
                        &error,
                        ErrorPhase::Read,
                        RemoteEffect::None,
                    ));
                }
                Err(_) => {
                    self.session.discard().await;
                    self.open = false;
                    return Err(DatabaseError {
                        category: ErrorCategory::Timeout,
                        phase: ErrorPhase::Read,
                        remote_effect: RemoteEffect::None,
                        retry: RetryDisposition::Never,
                        provider: Some(self.session.kind()),
                        execution_id: None,
                        diagnostics: None,
                        message: "query MySQL timeout".to_owned(),
                    });
                }
            };
            // Extract column names dal primo row (o vuoto se nessun result).
            let columns: Arc<[String]> = if rows.is_empty() {
                Arc::from(Vec::<String>::new())
            } else {
                let names: Vec<String> = rows[0]
                    .columns_ref()
                    .iter()
                    .map(|c| c.name_str().to_string())
                    .collect();
                Arc::from(names)
            };
            rows.into_iter()
                .map(|r| decode_row(r, &columns, kind))
                .collect::<Result<Vec<_>>>()
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async move {
            Err(DatabaseError {
                category: ErrorCategory::Unsupported,
                phase: ErrorPhase::Prepare,
                remote_effect: RemoteEffect::None,
                retry: RetryDisposition::Never,
                provider: Some(self.session.kind()),
                execution_id: None,
                diagnostics: None,
                message: "query_stream MySQL non ancora implementato in v1 \
                          (usa Provider::read per stream Arrow bulk)"
                    .to_owned(),
            })
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            if !self.open {
                return Err(closed_error(ErrorPhase::Prepare, self.session.kind()));
            }
            let quoted = quote_savepoint_name(name)?;
            raw_exec(
                &mut self.session,
                &format!("SAVEPOINT {quoted}"),
                ErrorPhase::Prepare,
                cancellation,
            )
            .await
        })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            if !self.open {
                return Err(closed_error(ErrorPhase::Prepare, self.session.kind()));
            }
            let quoted = quote_savepoint_name(name)?;
            raw_exec(
                &mut self.session,
                &format!("ROLLBACK TO SAVEPOINT {quoted}"),
                ErrorPhase::Write,
                cancellation,
            )
            .await
        })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            if !self.open {
                return Err(closed_error(ErrorPhase::Prepare, self.session.kind()));
            }
            let quoted = quote_savepoint_name(name)?;
            raw_exec(
                &mut self.session,
                &format!("RELEASE SAVEPOINT {quoted}"),
                ErrorPhase::Prepare,
                cancellation,
            )
            .await
        })
    }

    fn commit(
        mut self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            if !self.open {
                return Err(closed_error(ErrorPhase::Commit, self.session.kind()));
            }
            let outcome = self
                .session
                .exec_transaction(
                    MysqlTransactionCommand::Commit,
                    ErrorPhase::Commit,
                    cancellation,
                )
                .await;
            self.open = false;
            match outcome {
                Ok(()) => Ok(CommitOutcome::Committed),
                Err(err)
                    if matches!(
                        err.category,
                        ErrorCategory::Cancelled | ErrorCategory::Timeout | ErrorCategory::Io
                    ) =>
                {
                    // Canale compromesso durante commit: outcome ignoto.
                    Ok(CommitOutcome::OutcomeUnknown {
                        recovery: outcome_unknown_recovery(),
                    })
                }
                Err(err) => Err(err),
            }
        })
    }

    fn rollback(mut self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            if !self.open {
                return Ok(());
            }
            let outcome = self
                .session
                .exec_transaction(
                    MysqlTransactionCommand::Rollback,
                    ErrorPhase::Rollback,
                    cancellation,
                )
                .await;
            self.open = false;
            outcome
        })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            if !self.open {
                return Err(closed_error(ErrorPhase::Write, self.session.kind()));
            }
            // Fix P1 review MySQL: enforcement condiviso su UPDATE
            // (+ probe se presente). Parity con `PostgresTransaction`.
            plenora_database_core::native_query_policy::enforce_policy(
                self.native_query_policy,
                &request.update.sql,
            )?;
            if let Some(probe) = request.key_probe {
                plenora_database_core::native_query_policy::enforce_policy(
                    self.native_query_policy,
                    &probe.sql,
                )?;
            }
            // Pattern:
            //   1. SAVEPOINT __plenora_cu
            //   2. UPDATE ... → verifica affected_rows == expected
            //   3. Se ok, RELEASE; se no, ROLLBACK TO + return ConcurrentModification
            let sp = "__plenora_cu";
            self.savepoint(sp, cancellation).await?;
            let params = bind_positional_params(&request.update.params)?;
            let affected = self
                .session
                .exec_write(&request.update.sql, params, ErrorPhase::Write, cancellation)
                .await?;
            if affected == request.expected_affected_rows {
                self.release_savepoint(sp, cancellation).await?;
                Ok(())
            } else {
                self.rollback_to_savepoint(sp, cancellation).await?;
                Err(concurrent_modification_error(format!(
                    "MySQL: expected {} affected rows, got {}",
                    request.expected_affected_rows, affected
                )))
            }
        })
    }
}

fn closed_error(phase: ErrorPhase, kind: ProviderKind) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidPlan,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(kind),
        execution_id: None,
        diagnostics: None,
        message: "MysqlTransaction già chiusa".to_owned(),
    }
}
#[cfg(test)]
mod tests {
    use super::{validate_context_keys, MAX_CONTEXT_KEY};
    use plenora_database_core::session_context::{SessionEntry, SessionValue};
    use plenora_database_core::transaction::TransactionOptions;
    use plenora_database_core::ErrorCategory;

    /// Una chiave `ns.<riempimento>` lunga esattamente `length` caratteri.
    fn key_of_length(length: usize) -> String {
        let prefix = "ns.";
        format!("{prefix}{}", "a".repeat(length - prefix.len()))
    }

    fn options_with(key: &str) -> TransactionOptions {
        let mut options = TransactionOptions::default();
        options
            .context
            .insert(
                key,
                SessionEntry::public(SessionValue::Text("v".to_owned())),
            )
            .expect("chiave accettata dal core");
        options
    }

    #[test]
    fn the_longest_writable_key_is_fifty_two_characters() {
        assert_eq!(MAX_CONTEXT_KEY, 52, "64 meno il prefisso `plenora_ctx_`");
    }

    #[test]
    fn a_key_of_fifty_two_characters_is_accepted() {
        let key = key_of_length(52);
        assert_eq!(key.len(), 52);
        assert!(validate_context_keys(&options_with(&key)).is_ok());
    }

    #[test]
    fn a_key_of_fifty_three_characters_is_refused_before_any_statement() {
        // Il core la accetta — ne ammette fino a 63 — quindi senza questo
        // controllo il piano sembrerebbe valido e il rifiuto arriverebbe dal
        // server, con la transazione gia aperta.
        let key = key_of_length(53);
        assert_eq!(key.len(), 53);
        let error = validate_context_keys(&options_with(&key))
            .expect_err("chiave da 53 caratteri accettata");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert!(error.message.contains("52"), "{}", error.message);
        assert!(error.message.contains("64"), "{}", error.message);
    }

    #[test]
    fn an_empty_context_is_valid() {
        assert!(validate_context_keys(&TransactionOptions::default()).is_ok());
    }
}
