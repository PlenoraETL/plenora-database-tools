//! Transaction scope `PostgreSQL`: implementa `TransactionScope` del core.
//!
//! Copre begin con opzioni (isolation, access mode, deferrable,
//! `statement_timeout`), savepoint annidati con quoting sicuro, cancellation
//! best-effort e disambiguazione dei commit (`OutcomeUnknown` in caso di
//! canale compromesso in fase `Commit`).
//!
//! Struttura interna:
//!
//! - `sql`: builder puri (BEGIN, quoting, phase classification)
//! - `params`: codec `ParameterValue` → `SqlParam` (impl `ToSql`)
//! - `decode`: decoder `tokio_postgres::Row` → `Vec<Row>` (wrappers custom
//!   per enum e text-encoded exotic types)
//! - `stream`: `PostgresRowStream` (cursor `DECLARE`/`FETCH FORWARD`)
//! - `tests`: `#[cfg(test)]` unit + live (~2200 righe)

mod decode;
mod params;
mod sql;
mod stream;

#[cfg(test)]
mod tests;

use crate::control::select_with_cancellation;
use crate::error::{check_cancelled, classify_error, public_error};
use crate::pool::PooledClient;
use decode::decode_rows;
use params::encode_params;
use plenora_database_core::native_query_policy::{enforce_policy, NativeQueryPolicy};
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::row::Row;
use plenora_database_core::transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name, CommitOutcome,
    ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use sql::{build_begin_sql, phase_of, quote_identifier};
use stream::PostgresRowStream;
use tokio_postgres::types::ToSql;

/// Transazione `PostgreSQL` costruita sopra un `PooledClient`.
pub struct PostgresTransaction {
    client: PooledClient,
    open: bool,
    cursor_counter: u32,
    native_query_policy: NativeQueryPolicy,
}

impl PostgresTransaction {
    /// Apre la transazione emettendo `BEGIN` con le opzioni richieste e
    /// applica il session context via `set_config(name, value, true)` (il
    /// terzo argomento `true` = `is_local`, resettato dal commit/rollback).
    pub async fn begin(
        mut client: PooledClient,
        options: &TransactionOptions,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        check_cancelled(cancellation, ErrorPhase::Prepare)?;
        let sql = build_begin_sql(options);
        if let Err(error) = client
            .client_mut()?
            .batch_execute(&sql)
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))
        {
            client.invalidate();
            return Err(error);
        }
        if !options.context.is_empty() {
            let inner = client.client()?;
            for (name, entry) in options.context.iter() {
                let value = entry.value.as_provider_string();
                if let Err(error) = inner
                    .execute(
                        "SELECT set_config($1, $2, true)",
                        &[&name.as_str(), &value.as_str()],
                    )
                    .await
                    .map_err(|error| classify_error(ErrorPhase::Prepare, &error))
                {
                    // Best-effort rollback per non lasciare la tx orfana;
                    // se anche il rollback fallisce, invalidiamo la sessione.
                    let _ = inner.batch_execute("ROLLBACK").await;
                    client.invalidate();
                    return Err(error);
                }
            }
        }
        Ok(Self {
            client,
            open: true,
            cursor_counter: 0,
            native_query_policy: options.native_query_policy,
        })
    }
}

impl Drop for PostgresTransaction {
    fn drop(&mut self) {
        if self.open {
            // Una transazione droppata senza commit/rollback esplicito lascia
            // la sessione in stato inatteso: la si mette in quarantena.
            self.client.invalidate();
        }
    }
}

impl PostgresTransaction {
    /// Fail-fast se la transazione è stata già chiusa (commit, rollback,
    /// o cancel in-flight che ha invalidato la sessione). Prima del fix
    /// review era possibile chiamare `execute`/`commit` dopo una cancel
    /// mid-flight, con esito ambiguo.
    fn ensure_open(&self, phase: ErrorPhase) -> Result<()> {
        if self.open {
            Ok(())
        } else {
            Err(public_error(
                ErrorCategory::InvalidPlan,
                phase,
                false,
                "transazione già chiusa (commit/rollback/cancel): apri una nuova tx",
            ))
        }
    }

    /// Costruisce l'errore Cancelled con `RemoteEffect::Unknown` nelle
    /// fasi state-mutating (Write/Commit), dove la query può essere
    /// stata già applicata server-side. Le fasi Read/Prepare/Rollback
    /// restano con `None` (nessun effetto).
    fn cancelled_error(phase: ErrorPhase, message: &str) -> DatabaseError {
        let remote_effect = match phase {
            ErrorPhase::Write | ErrorPhase::Commit => RemoteEffect::Unknown,
            _ => RemoteEffect::None,
        };
        DatabaseError {
            category: ErrorCategory::Cancelled,
            phase,
            remote_effect,
            retry: RetryDisposition::Never,
            provider: Some(plenora_database_core::plan::ProviderKind::Postgres),
            execution_id: None,
            message: message.to_owned(),
            diagnostics: None,
        }
    }
}

impl TransactionScope for PostgresTransaction {
    fn provider_kind(&self) -> plenora_database_core::plan::ProviderKind {
        plenora_database_core::plan::ProviderKind::Postgres
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        Box::pin(async move {
            let phase = phase_of(&statement.sql);
            self.ensure_open(phase)?;
            enforce_policy(self.native_query_policy, &statement.sql)?;
            check_cancelled(cancellation, phase)?;
            let encoded = encode_params(&statement.params)?;
            let param_refs: Vec<&(dyn ToSql + Sync)> = encoded
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            let client = self.client.client()?;
            // Fix review #7: la execute era `client.execute().await`
            // senza race col cancellation token — un cancel durante
            // pg_sleep/lock wait/query lunga non veniva onorato.
            // Ora `select_with_cancellation` mette in race la query
            // con `cancellation.cancelled()`. Su cancel il client
            // resta in stato ambiguo (query può essere già
            // completata server-side) → invalida per sicurezza.
            //
            // NB: cancel_query lato Postgres non lo mandiamo qui
            // perché è expensive (nuova connessione + protocol) e
            // non è thread-safe rispetto al client in uso.
            // L'invalidazione del pool è più conservativa.
            let Some(result) = select_with_cancellation(
                client.execute(statement.sql.as_str(), &param_refs),
                cancellation,
            )
            .await
            else {
                self.client.invalidate();
                self.open = false;
                // Fix review: cancel in Write phase → RemoteEffect::Unknown
                // (la query può essere applicata server-side prima del
                // taglio del canale).
                return Err(Self::cancelled_error(
                    phase,
                    "operazione cancellata durante l'esecuzione",
                ));
            };
            match result {
                Ok(affected) => Ok(affected),
                Err(error) => {
                    let mapped = classify_error(phase, &error);
                    if error.is_closed() {
                        self.client.invalidate();
                        self.open = false;
                    }
                    Err(mapped)
                }
            }
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Write)?;
            check_cancelled(cancellation, ErrorPhase::Write)?;
            validate_savepoint_name(name)?;
            let sql = format!("SAVEPOINT {}", quote_identifier(name));
            let client = self.client.client()?;
            if let Some(result) =
                select_with_cancellation(client.batch_execute(&sql), cancellation).await
            {
                result.map_err(|error| classify_error(ErrorPhase::Write, &error))
            } else {
                self.client.invalidate();
                self.open = false;
                Err(Self::cancelled_error(
                    ErrorPhase::Write,
                    "SAVEPOINT cancellato durante l'esecuzione",
                ))
            }
        })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Rollback)?;
            validate_savepoint_name(name)?;
            let sql = format!("ROLLBACK TO SAVEPOINT {}", quote_identifier(name));
            self.client
                .client()?
                .batch_execute(&sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Rollback, &error))
        })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            check_cancelled(cancellation, ErrorPhase::Finalize)?;
            validate_savepoint_name(name)?;
            let sql = format!("RELEASE SAVEPOINT {}", quote_identifier(name));
            self.client
                .client()?
                .batch_execute(&sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Finalize, &error))
        })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Read)?;
            enforce_policy(self.native_query_policy, &statement.sql)?;
            check_cancelled(cancellation, ErrorPhase::Read)?;
            let encoded = encode_params(&statement.params)?;
            let param_refs: Vec<&(dyn ToSql + Sync)> = encoded
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            let client = self.client.client()?;
            // Fix review #7: cancellation con race in-flight (vedi
            // note in `execute`).
            let Some(query_result) = select_with_cancellation(
                client.query(statement.sql.as_str(), &param_refs),
                cancellation,
            )
            .await
            else {
                self.client.invalidate();
                self.open = false;
                // Read phase → RemoteEffect::None (no state-mutating).
                return Err(Self::cancelled_error(
                    ErrorPhase::Read,
                    "operazione cancellata durante l'esecuzione",
                ));
            };
            let rows = query_result
                .map_err(|error| classify_error(ErrorPhase::Read, &error))?;
            decode_rows(&rows)
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        statement: &'a Statement,
        batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async move {
            enforce_policy(self.native_query_policy, &statement.sql)?;
            check_cancelled(cancellation, ErrorPhase::Read)?;
            if batch_size == 0 {
                return Err(public_error(
                    ErrorCategory::InvalidPlan,
                    ErrorPhase::Prepare,
                    false,
                    "batch_size del cursor deve essere > 0",
                ));
            }
            self.cursor_counter = self.cursor_counter.wrapping_add(1);
            let cursor_name = format!("_plenora_stream_{}", self.cursor_counter);

            // DECLARE CURSOR non accetta parametri nel testo del cursor:
            // dobbiamo iniettare i parametri della sorgente in-line come
            // parte del bind. La costruzione della query interna usa i
            // placeholder $1..$n. tokio_postgres.execute() per DECLARE non
            // esiste, ma prepare+bind sì.
            let encoded = encode_params(&statement.params)?;
            let param_refs: Vec<&(dyn ToSql + Sync)> = encoded
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            let declare_sql = format!(
                "DECLARE {cursor_name} NO SCROLL CURSOR FOR {}",
                statement.sql
            );
            self.client
                .client()?
                .execute(declare_sql.as_str(), &param_refs)
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;

            let stream = PostgresRowStream {
                client: self.client.client()?,
                cursor_name,
                batch_size,
                exhausted: false,
            };
            Ok(Box::new(stream) as Box<dyn RowStream + Send + 'a>)
        })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            enforce_policy(self.native_query_policy, &request.update.sql)?;
            if let Some(probe) = request.key_probe {
                enforce_policy(self.native_query_policy, &probe.sql)?;
            }
            let phase = phase_of(&request.update.sql);
            check_cancelled(cancellation, phase)?;

            let update_params = encode_params(&request.update.params)?;
            let update_param_refs: Vec<&(dyn ToSql + Sync)> = update_params
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            let client = self.client.client()?;
            let affected = match client
                .execute(request.update.sql.as_str(), &update_param_refs)
                .await
            {
                Ok(n) => n,
                Err(error) => {
                    let mapped = classify_error(phase, &error);
                    if error.is_closed() {
                        self.client.invalidate();
                        self.open = false;
                    }
                    return Err(mapped);
                }
            };

            if affected == request.expected_affected_rows {
                return Ok(());
            }

            if let Some(probe) = request.key_probe {
                check_cancelled(cancellation, ErrorPhase::Read)?;
                let probe_params = encode_params(&probe.params)?;
                let probe_refs: Vec<&(dyn ToSql + Sync)> = probe_params
                    .iter()
                    .map(|value| value as &(dyn ToSql + Sync))
                    .collect();
                let probe_client = self.client.client()?;
                let rows = probe_client
                    .query(probe.sql.as_str(), &probe_refs)
                    .await
                    .map_err(|error| classify_error(ErrorPhase::Read, &error))?;
                if rows.is_empty() {
                    return Err(public_error(
                        ErrorCategory::NotFound,
                        ErrorPhase::Write,
                        false,
                        "chiave assente per l'update ottimistico",
                    ));
                }
            }

            Err(concurrent_modification_error(
                "versione attesa non allineata: la riga è stata modificata concorrentemente",
            ))
        })
    }

    fn commit(
        mut self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Commit)?;
            check_cancelled(cancellation, ErrorPhase::Commit)?;
            let client = self.client.client()?;
            match client.batch_execute("COMMIT").await {
                Ok(()) => {
                    self.open = false;
                    Ok(CommitOutcome::Committed)
                }
                Err(error) => {
                    let mapped = classify_error(ErrorPhase::Commit, &error);
                    self.open = false;
                    if mapped.remote_effect == RemoteEffect::Unknown {
                        self.client.invalidate();
                        Ok(CommitOutcome::OutcomeUnknown {
                            recovery: outcome_unknown_recovery(),
                        })
                    } else {
                        if error.is_closed() {
                            self.client.invalidate();
                        }
                        Err(mapped)
                    }
                }
            }
        })
    }

    fn rollback(
        mut self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            // Un rollback esplicito non deve fallire sul cancellation: la
            // cancellazione è il motivo per cui stiamo rilasciando lo stato.
            // Riportiamo un errore solo se il rollback SQL fallisce davvero.
            let _ = cancellation;
            let result = self
                .client
                .client()?
                .batch_execute("ROLLBACK")
                .await
                .map_err(|error| classify_error(ErrorPhase::Rollback, &error));
            self.open = false;
            if result.is_err() {
                self.client.invalidate();
            }
            result
        })
    }
}

// Il warning "unused import" verrà segnalato se non serve; teniamo il tipo per
// documentazione della retry disposition applicata al ramo timeout.
#[allow(dead_code)]
const _: fn() -> RetryDisposition = || RetryDisposition::Never;

