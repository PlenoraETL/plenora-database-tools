//! Transaction scope `PostgreSQL`: implementa `TransactionScope` del core.
//!
//! Copre begin con opzioni (isolation, access mode, deferrable,
//! `statement_timeout`), savepoint annidati con quoting sicuro, cancellation
//! best-effort e disambiguazione dei commit (`OutcomeUnknown` in caso di
//! canale compromesso in fase `Commit`).
//!
//! # Semantica di cancellazione
//!
//! Ogni metodo state-mutating (`execute`, `query`, `savepoint`,
//! `rollback_to_savepoint`, `release_savepoint`, `query_stream`,
//! `execute_conditional_update`, `commit`) fa `tokio::select` fra
//! l'await del client Postgres e `cancellation.cancelled()`. Su
//! cancel:
//! - il pool client viene invalidato (`self.client.invalidate()`);
//! - `self.open = false` blocca chiamate successive con `InvalidPlan`
//!   via `ensure_open`;
//! - viene emesso `RemoteEffect::Unknown` per Write/Commit (query
//!   potenzialmente applicata server-side), `None` per Read/Rollback.
//!
//! **Non inviamo `CancelRequest` (Postgres protocol-level cancel)**:
//! richiederebbe una nuova connessione TCP + handshake sub-protocol
//! e non è thread-safe rispetto al client in uso. Il DBA vede la
//! query proseguire fino al `statement_timeout` di sessione (default
//! 30s). Consumer che vuole cancel server-side deve settare
//! `statement_timeout_ms` esplicito nelle `TransactionOptions`.
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

use crate::age::{build_cypher_sql, parse_agtype, probe_age_capabilities, AgeParameter};
use crate::control::select_with_cancellation;
use crate::error::{check_cancelled, classify_error, public_error};
use crate::pool::PooledClient;
use decode::decode_rows;
use params::{encode_params, validate_parameter_targets};
use plenora_database_core::graph::{GraphRow, GraphStatement, GraphValue};
use plenora_database_core::native_query_policy::{enforce_policy, NativeQueryPolicy};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::row::Row;
use plenora_database_core::transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name,
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use sql::{build_begin_sql, phase_of, quote_identifier};
use std::collections::BTreeMap;
use stream::PostgresRowStream;
use tokio_postgres::types::ToSql;
use tokio_postgres::Statement as PreparedStatement;

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
    /// o cancellazione in-flight che ha invalidato la sessione). Dopo una
    /// cancellazione ambigua `execute` e `commit` restano vietati.
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

    /// Costruisce l'errore di interruzione con `RemoteEffect::Unknown` nelle
    /// fasi state-mutating (Write/Commit), dove la query può essere
    /// stata già applicata server-side. Le fasi Read/Prepare/Rollback
    /// restano con `None` (nessun effetto).
    ///
    /// La categoria viene dalla causa: una deadline scaduta è `Timeout`, una
    /// decisione del chiamante è `Cancelled`.
    fn interruption_error(
        cancellation: &CancellationToken,
        phase: ErrorPhase,
        message: &str,
    ) -> DatabaseError {
        let remote_effect = match phase {
            ErrorPhase::Write | ErrorPhase::Commit => RemoteEffect::Unknown,
            _ => RemoteEffect::None,
        };
        DatabaseError {
            category: crate::error::interruption_category(cancellation),
            phase,
            remote_effect,
            retry: RetryDisposition::Never,
            provider: Some(plenora_database_core::plan::ProviderKind::Postgres),
            execution_id: None,
            message: message.to_owned(),
            diagnostics: None,
        }
    }

    async fn prepare_bind(
        &mut self,
        sql: &str,
        params: &[ParameterValue],
        cancellation: &CancellationToken,
        operation_phase: ErrorPhase,
    ) -> Result<PreparedStatement> {
        let client = self.client.client()?;
        let Some(result) = select_with_cancellation(client.prepare(sql), cancellation).await else {
            self.client.invalidate();
            self.open = false;
            return Err(Self::interruption_error(
                cancellation,
                operation_phase,
                "preparazione PostgreSQL cancellata",
            ));
        };
        let prepared = result.map_err(|error| classify_error(operation_phase, &error))?;
        validate_parameter_targets(params, prepared.params())?;
        Ok(prepared)
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
            let prepared = self
                .prepare_bind(&statement.sql, &statement.params, cancellation, phase)
                .await?;
            let client = self.client.client()?;
            // `select_with_cancellation` mette in race la query con il token.
            // Su cancellazione il client
            // resta in stato ambiguo (query può essere già
            // completata server-side) → invalida per sicurezza.
            //
            // NB: cancel_query lato Postgres non lo mandiamo qui
            // perché è expensive (nuova connessione + protocol) e
            // non è thread-safe rispetto al client in uso.
            // L'invalidazione del pool è più conservativa.
            let Some(result) =
                select_with_cancellation(client.execute(&prepared, &param_refs), cancellation)
                    .await
            else {
                self.client.invalidate();
                self.open = false;
                // Una cancellazione in fase Write lascia effetto remoto
                // ignoto: la query può precedere il taglio del canale.
                return Err(Self::interruption_error(
                    cancellation,
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
                Err(Self::interruption_error(
                    cancellation,
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
            self.ensure_open(ErrorPhase::Rollback)?;
            check_cancelled(cancellation, ErrorPhase::Rollback)?;
            validate_savepoint_name(name)?;
            let sql = format!("ROLLBACK TO SAVEPOINT {}", quote_identifier(name));
            let client = self.client.client()?;
            if let Some(result) =
                select_with_cancellation(client.batch_execute(&sql), cancellation).await
            {
                result.map_err(|error| classify_error(ErrorPhase::Rollback, &error))
            } else {
                self.client.invalidate();
                self.open = false;
                Err(Self::interruption_error(
                    cancellation,
                    ErrorPhase::Rollback,
                    "ROLLBACK TO SAVEPOINT cancellato durante l'esecuzione",
                ))
            }
        })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Finalize)?;
            check_cancelled(cancellation, ErrorPhase::Finalize)?;
            validate_savepoint_name(name)?;
            let sql = format!("RELEASE SAVEPOINT {}", quote_identifier(name));
            let client = self.client.client()?;
            if let Some(result) =
                select_with_cancellation(client.batch_execute(&sql), cancellation).await
            {
                result.map_err(|error| classify_error(ErrorPhase::Finalize, &error))
            } else {
                self.client.invalidate();
                self.open = false;
                Err(Self::interruption_error(
                    cancellation,
                    ErrorPhase::Finalize,
                    "RELEASE SAVEPOINT cancellato durante l'esecuzione",
                ))
            }
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
            let prepared = self
                .prepare_bind(
                    &statement.sql,
                    &statement.params,
                    cancellation,
                    ErrorPhase::Read,
                )
                .await?;
            let client = self.client.client()?;
            // Anche la lettura osserva la cancellazione mentre è in flight.
            let Some(query_result) =
                select_with_cancellation(client.query(&prepared, &param_refs), cancellation).await
            else {
                self.client.invalidate();
                self.open = false;
                // Read phase → RemoteEffect::None (no state-mutating).
                return Err(Self::interruption_error(
                    cancellation,
                    ErrorPhase::Read,
                    "operazione cancellata durante l'esecuzione",
                ));
            };
            let rows = query_result.map_err(|error| classify_error(ErrorPhase::Read, &error))?;
            decode_rows(&rows)
        })
    }

    fn execute_graph<'a>(
        &'a mut self,
        statement: &'a GraphStatement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<GraphRow>> {
        Box::pin(async move {
            // Cypher puo contenere scritture anche quando restituisce righe:
            // la fase conservativa e sempre Write.
            self.ensure_open(ErrorPhase::Write)?;
            check_cancelled(cancellation, ErrorPhase::Write)?;
            statement.validate()?;
            let client = self.client.client()?;
            let capabilities = probe_age_capabilities(client).await?;
            if !capabilities.qualified() {
                return Err(DatabaseError::unsupported(
                    plenora_database_core::plan::ProviderKind::Postgres,
                    ErrorPhase::Prepare,
                    "Apache AGE 1.7.0 su PostgreSQL 18 non e disponibile o qualificato",
                ));
            }

            let sql = build_cypher_sql(statement)?;
            let encoded_params = if statement.params.is_empty() {
                None
            } else {
                Some(AgeParameter::new(
                    serde_json::to_string(&statement.params).map_err(|_| {
                        DatabaseError::invalid_plan(
                            "la mappa parametri Cypher non e serializzabile",
                        )
                    })?,
                ))
            };
            let parameter_refs: Vec<&(dyn ToSql + Sync)> = encoded_params
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            // AGE risolve internamente `graphid_ops` e altri oggetti senza
            // qualificarli. Anche con `cypher`/`agtype` qualificati, quindi,
            // ag_catalog deve stare nel search_path durante lo statement.
            // Salviamo e ripristiniamo il valore transaction-local per non
            // alterare le query SQL successive del chiamante.
            let previous_search_path: String = client
                .query_one("SELECT current_setting('search_path')", &[])
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
                .get(0);
            client
                .execute(
                    "SELECT set_config('search_path', $1, true)",
                    &[&"ag_catalog,\"$user\",public"],
                )
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
            let Some(result) =
                select_with_cancellation(client.query(&sql, &parameter_refs), cancellation).await
            else {
                self.client.invalidate();
                self.open = false;
                return Err(Self::interruption_error(
                    cancellation,
                    ErrorPhase::Write,
                    "operazione graph cancellata durante l'esecuzione",
                ));
            };
            let rows = match result {
                Ok(rows) => rows,
                Err(error) => {
                    let mapped = classify_error(ErrorPhase::Write, &error);
                    if error.is_closed() {
                        self.client.invalidate();
                        self.open = false;
                    }
                    return Err(mapped);
                }
            };
            client
                .execute(
                    "SELECT set_config('search_path', $1, true)",
                    &[&previous_search_path],
                )
                .await
                .map_err(|error| classify_error(ErrorPhase::Write, &error))?;

            rows.iter()
                .map(|row| {
                    let mut values = BTreeMap::new();
                    for (index, column) in statement.columns.iter().enumerate() {
                        let raw: Option<String> = row.try_get(index).map_err(|_| {
                            DatabaseError::new(
                                ErrorCategory::DataMapping,
                                ErrorPhase::Read,
                                Some(plenora_database_core::plan::ProviderKind::Postgres),
                                "valore agtype non decodificabile",
                            )
                        })?;
                        let value = raw
                            .as_deref()
                            .map(parse_agtype)
                            .transpose()?
                            .unwrap_or(GraphValue::Null);
                        values.insert(column.clone(), value);
                    }
                    Ok(GraphRow { values })
                })
                .collect()
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        statement: &'a Statement,
        batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Read)?;
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
            let prepared = self
                .prepare_bind(
                    &declare_sql,
                    &statement.params,
                    cancellation,
                    ErrorPhase::Read,
                )
                .await?;
            let client = self.client.client()?;
            let Some(declare_result) =
                select_with_cancellation(client.execute(&prepared, &param_refs), cancellation)
                    .await
            else {
                self.client.invalidate();
                self.open = false;
                return Err(Self::interruption_error(
                    cancellation,
                    ErrorPhase::Prepare,
                    "DECLARE CURSOR cancellato durante l'esecuzione",
                ));
            };
            declare_result.map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;

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
            let phase = phase_of(&request.update.sql);
            self.ensure_open(phase)?;
            enforce_policy(self.native_query_policy, &request.update.sql)?;
            if let Some(probe) = request.key_probe {
                enforce_policy(self.native_query_policy, &probe.sql)?;
            }
            check_cancelled(cancellation, phase)?;

            let update_params = encode_params(&request.update.params)?;
            let update_param_refs: Vec<&(dyn ToSql + Sync)> = update_params
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            let prepared_update = self
                .prepare_bind(
                    &request.update.sql,
                    &request.update.params,
                    cancellation,
                    ErrorPhase::Write,
                )
                .await?;
            let client = self.client.client()?;
            let Some(update_result) = select_with_cancellation(
                client.execute(&prepared_update, &update_param_refs),
                cancellation,
            )
            .await
            else {
                self.client.invalidate();
                self.open = false;
                return Err(Self::interruption_error(
                    cancellation,
                    phase,
                    "UPDATE condizionale cancellato durante l'esecuzione",
                ));
            };
            let affected = match update_result {
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
                let prepared_probe = self
                    .prepare_bind(&probe.sql, &probe.params, cancellation, ErrorPhase::Read)
                    .await?;
                let probe_client = self.client.client()?;
                let Some(probe_result) = select_with_cancellation(
                    probe_client.query(&prepared_probe, &probe_refs),
                    cancellation,
                )
                .await
                else {
                    self.client.invalidate();
                    self.open = false;
                    return Err(Self::interruption_error(
                        cancellation,
                        ErrorPhase::Read,
                        "key probe cancellato durante l'esecuzione",
                    ));
                };
                let rows =
                    probe_result.map_err(|error| classify_error(ErrorPhase::Read, &error))?;
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
            let Some(commit_result) =
                select_with_cancellation(client.batch_execute("COMMIT"), cancellation).await
            else {
                // Cancel mid-COMMIT: server può aver già applicato il
                // commit (out-of-band) → `OutcomeUnknown` con recovery
                // richiesto, coerente con `RemoteEffect::Unknown` in
                // fase Commit.
                self.client.invalidate();
                self.open = false;
                return Ok(CommitOutcome::OutcomeUnknown {
                    recovery: outcome_unknown_recovery(),
                });
            };
            match commit_result {
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

    fn rollback(mut self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
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
