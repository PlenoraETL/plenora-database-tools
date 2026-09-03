use crate::config::OracleConfig;
use crate::connection::{with_timeout, with_timeout_duration};
use crate::decode::{decode_columns, row_from_driver, rows_from_result};
use crate::error::{driver_error, oracle_code_error};
use crate::parameter::{bind_parameters_with_lobs, LobCache};
use crate::{OraclePool, PooledOracleConnection};
use oracle_rs::types::LobValue;
use oracle_rs::{BindDirection, BindParam, ColumnInfo, Connection, QueryResult, Value};
use plenora_database_core::native_query_policy::{enforce_policy, NativeQueryPolicy};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::row::Row;
use plenora_database_core::transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name,
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

pub struct OracleTransaction {
    connection: PooledOracleConnection,
    operation_timeout: Duration,
    open: bool,
    native_query_policy: NativeQueryPolicy,
    lob_cache: LobCache,
}

impl OracleTransaction {
    pub async fn begin(
        config: &OracleConfig,
        pool: &Arc<OraclePool>,
        options: &TransactionOptions,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        validate_options(options)?;
        let mut connection = pool.checkout(cancellation).await?;
        connection.disallow_reuse();
        let begin = begin_statement(options)?;
        if let Some(sql) = begin {
            with_timeout(
                config,
                ErrorPhase::Prepare,
                cancellation,
                connection.connection()?.execute(sql, &[]),
            )
            .await?;
        }
        Ok(Self {
            connection,
            operation_timeout: config.operation_timeout(),
            open: true,
            native_query_policy: options.native_query_policy,
            lob_cache: LobCache::default(),
        })
    }

    async fn execute_inner(
        &mut self,
        statement: &Statement,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        self.execute_dml(statement, false, cancellation).await
    }

    pub(crate) async fn execute_write_dml(
        &mut self,
        statement: &Statement,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        self.execute_dml(statement, true, cancellation).await
    }

    async fn execute_dml(
        &mut self,
        statement: &Statement,
        promote_binary_lobs: bool,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        enforce_policy(self.native_query_policy, &statement.sql)?;
        let params = bind_parameters_with_lobs(
            self.connection.connection()?,
            &statement.params,
            promote_binary_lobs,
            &mut self.lob_cache,
            self.operation_timeout,
            ErrorPhase::Write,
            cancellation,
        )
        .await?;
        let result = timed(
            self.operation_timeout,
            ErrorPhase::Write,
            cancellation,
            self.connection
                .connection()?
                .execute(&statement.sql, &params),
        )
        .await
        .map_err(statement_execution_error)?;
        Ok(result.rows_affected)
    }

    pub(crate) async fn execute_atomic_dml(
        &mut self,
        statement: &Statement,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        enforce_policy(self.native_query_policy, &statement.sql)?;
        let values = bind_parameters_with_lobs(
            self.connection.connection()?,
            &statement.params,
            true,
            &mut self.lob_cache,
            self.operation_timeout,
            ErrorPhase::Write,
            cancellation,
        )
        .await?;
        let sql = format!(
            "DECLARE
               plenora_affected NUMBER := 0;
               plenora_code NUMBER := 0;
             BEGIN
               BEGIN
                 {};
                 plenora_affected := SQL%ROWCOUNT;
               EXCEPTION WHEN OTHERS THEN
                 plenora_code := SQLCODE;
                 ROLLBACK;
               END;
               DBMS_APPLICATION_INFO.SET_CLIENT_INFO(
                 'PLENORA|' || TO_CHAR(plenora_code) || '|' || TO_CHAR(plenora_affected)
               );
             END;",
            statement.sql
        );
        let params = values
            .into_iter()
            .map(|value| match value {
                Value::Lob(LobValue::Locator(locator)) => {
                    let oracle_type = locator.oracle_type();
                    BindParam {
                        value: Some(Value::Lob(LobValue::Locator(locator))),
                        direction: BindDirection::Input,
                        oracle_type,
                        buffer_size: 112,
                    }
                }
                value => BindParam::input(value),
            })
            .collect::<Vec<_>>();
        timed(
            self.operation_timeout,
            ErrorPhase::Write,
            cancellation,
            self.connection.connection()?.execute_plsql(&sql, &params),
        )
        .await
        .map_err(statement_execution_error)?;
        let status = timed(
            self.operation_timeout,
            ErrorPhase::Write,
            cancellation,
            self.connection.connection()?.query(
                "SELECT SYS_CONTEXT('USERENV', 'CLIENT_INFO') AS PLENORA_STATUS FROM DUAL",
                &[],
            ),
        )
        .await
        .map_err(statement_execution_error)?;
        let status = status
            .rows
            .first()
            .and_then(|row| row.get_string(0))
            .ok_or_else(|| {
                DatabaseError::new(
                    ErrorCategory::Protocol,
                    ErrorPhase::Write,
                    Some(ProviderKind::Oracle),
                    "risultato DML protetto Oracle senza stato",
                )
            })?;
        parse_protected_dml_status(status)
    }

    async fn query_inner(
        &mut self,
        statement: &Statement,
        cancellation: &CancellationToken,
    ) -> Result<QueryResult> {
        enforce_policy(self.native_query_policy, &statement.sql)?;
        let params = bind_parameters_with_lobs(
            self.connection.connection()?,
            &statement.params,
            false,
            &mut self.lob_cache,
            self.operation_timeout,
            ErrorPhase::Read,
            cancellation,
        )
        .await?;
        timed(
            self.operation_timeout,
            ErrorPhase::Read,
            cancellation,
            self.connection.connection()?.query(&statement.sql, &params),
        )
        .await
    }
}

fn parse_protected_dml_status(status: &str) -> Result<u64> {
    let mut parts = status.split('|');
    if parts.next() != Some("PLENORA") {
        return Err(DatabaseError::new(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            Some(ProviderKind::Oracle),
            "stato DML protetto Oracle non riconosciuto",
        ));
    }
    let code = parts.next().and_then(|value| value.parse::<i64>().ok());
    let affected = parts.next().and_then(|value| value.parse::<i64>().ok());
    if parts.next().is_some() || code.is_none() || affected.is_none() {
        return Err(DatabaseError::new(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            Some(ProviderKind::Oracle),
            "stato DML protetto Oracle non valido",
        ));
    }
    let code = code.expect("verificato");
    if code != 0 {
        let mut error = oracle_code_error(
            ErrorPhase::Write,
            u32::try_from(code.unsigned_abs()).unwrap_or(u32::MAX),
        );
        error.remote_effect = plenora_database_core::RemoteEffect::RolledBack;
        return Err(error);
    }
    u64::try_from(affected.expect("verificato")).map_err(|_| {
        DatabaseError::new(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            Some(ProviderKind::Oracle),
            "conteggio DML Oracle non rappresentabile",
        )
    })
}

fn statement_execution_error(mut error: DatabaseError) -> DatabaseError {
    if error.category == ErrorCategory::Timeout {
        "esecuzione statement Oracle oltre il timeout configurato".clone_into(&mut error.message);
    }
    error
}

impl Drop for OracleTransaction {
    fn drop(&mut self) {
        // Il lease nasce non riusabile e viene riabilitato soltanto da un
        // commit o rollback confermato. Il drop di una transazione aperta
        // chiude quindi il canale invece di contaminare il prossimo checkout.
        self.open = false;
    }
}

impl TransactionScope for OracleTransaction {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Oracle
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        Box::pin(async move { self.execute_inner(statement, cancellation).await })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            let result = self.query_inner(statement, cancellation).await?;
            let result = drain_result(
                self.connection.connection()?,
                self.operation_timeout,
                cancellation,
                result,
            )
            .await?;
            rows_from_result(
                self.connection.connection()?,
                self.operation_timeout,
                ErrorPhase::Read,
                cancellation,
                result,
            )
            .await
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        statement: &'a Statement,
        batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async move {
            if batch_size == 0 {
                return Err(DatabaseError::invalid_plan(
                    "query_stream Oracle richiede batch_size positivo",
                ));
            }
            let result = self.query_inner(statement, cancellation).await?;
            let stream = OracleRowStream::new(
                self.connection.connection()?,
                self.operation_timeout,
                batch_size,
                result,
            )?;
            Ok(Box::new(stream) as Box<dyn RowStream + Send + 'a>)
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            validate_savepoint_name(name)?;
            timed(
                self.operation_timeout,
                ErrorPhase::Write,
                cancellation,
                self.connection.connection()?.savepoint(name),
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
            validate_savepoint_name(name)?;
            timed(
                self.operation_timeout,
                ErrorPhase::Rollback,
                cancellation,
                self.connection.connection()?.rollback_to_savepoint(name),
            )
            .await
        })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            validate_savepoint_name(name)?;
            // Oracle non offre RELEASE SAVEPOINT. Il no-op è la stessa
            // semantica usata dagli adapter di prodotti che liberano tutti i
            // savepoint soltanto alla fine della transazione.
            Ok(())
        })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            let affected = self.execute_inner(request.update, cancellation).await?;
            if affected == request.expected_affected_rows {
                return Ok(());
            }
            if let Some(probe) = request.key_probe {
                if self.query_inner(probe, cancellation).await?.rows.is_empty() {
                    return Err(DatabaseError::new(
                        ErrorCategory::NotFound,
                        ErrorPhase::Write,
                        Some(ProviderKind::Oracle),
                        "update condizionale Oracle senza riga corrispondente",
                    ));
                }
            }
            Err(concurrent_modification_error(
                "update condizionale Oracle con versione non più corrente",
            ))
        })
    }

    fn commit(
        mut self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            let operation = self.connection.connection()?.commit();
            tokio::select! {
                result = tokio::time::timeout(self.operation_timeout, operation) => match result {
                    Ok(Ok(())) => {
                        self.open = false;
                        self.connection.allow_reuse();
                        Ok(CommitOutcome::Committed)
                    }
                    Ok(Err(error)) if error.is_connection_error() => {
                        self.open = false;
                        Ok(CommitOutcome::OutcomeUnknown { recovery: outcome_unknown_recovery() })
                    }
                    Ok(Err(error)) => Err(driver_error(ErrorPhase::Commit, &error)),
                    Err(_) => {
                        self.open = false;
                        Ok(CommitOutcome::OutcomeUnknown { recovery: outcome_unknown_recovery() })
                    }
                },
                _ = cancellation.cancelled() => {
                    self.open = false;
                    Ok(CommitOutcome::OutcomeUnknown { recovery: outcome_unknown_recovery() })
                },
            }
        })
    }

    fn rollback(mut self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            let result = timed(
                self.operation_timeout,
                ErrorPhase::Rollback,
                cancellation,
                self.connection.connection()?.rollback(),
            )
            .await;
            self.open = false;
            if result.is_ok() {
                self.connection.allow_reuse();
            }
            result
        })
    }
}

async fn drain_result(
    connection: &Connection,
    timeout: Duration,
    cancellation: &CancellationToken,
    mut result: QueryResult,
) -> Result<QueryResult> {
    while result.has_more_rows {
        let page = timed(
            timeout,
            ErrorPhase::Read,
            cancellation,
            connection.fetch_more(result.cursor_id, &result.columns, 256),
        )
        .await?;
        result.rows.extend(page.rows);
        result.cursor_id = page.cursor_id;
        result.has_more_rows = page.has_more_rows;
    }
    Ok(result)
}

struct OracleRowStream<'a> {
    connection: &'a Connection,
    timeout: Duration,
    columns: Vec<ColumnInfo>,
    decoded_columns: Arc<crate::decode::DecodeColumns>,
    pending: VecDeque<oracle_rs::Row>,
    cursor_id: u16,
    has_more: bool,
    batch_size: u32,
}

impl<'a> OracleRowStream<'a> {
    fn new(
        connection: &'a Connection,
        timeout: Duration,
        batch_size: u32,
        result: QueryResult,
    ) -> Result<Self> {
        let columns = decode_columns(&result.columns)?;
        Ok(Self {
            connection,
            timeout,
            columns: result.columns,
            decoded_columns: columns,
            pending: result.rows.into(),
            cursor_id: result.cursor_id,
            has_more: result.has_more_rows,
            batch_size,
        })
    }
}

impl RowStream for OracleRowStream<'_> {
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<Vec<Row>>> {
        Box::pin(async move {
            if self.pending.is_empty() && self.has_more {
                let result = timed(
                    self.timeout,
                    ErrorPhase::Read,
                    cancellation,
                    self.connection
                        .fetch_more(self.cursor_id, &self.columns, self.batch_size),
                )
                .await?;
                self.cursor_id = result.cursor_id;
                self.has_more = result.has_more_rows;
                self.pending.extend(result.rows);
            }
            if self.pending.is_empty() {
                return Ok(None);
            }
            let take = usize::try_from(self.batch_size)
                .unwrap_or(usize::MAX)
                .min(self.pending.len());
            let mut rows = Vec::with_capacity(take);
            for _ in 0..take {
                if let Some(row) = self.pending.pop_front() {
                    rows.push(
                        row_from_driver(
                            self.connection,
                            Arc::clone(&self.decoded_columns),
                            row,
                            self.timeout,
                            ErrorPhase::Read,
                            cancellation,
                        )
                        .await?,
                    );
                }
            }
            Ok(Some(rows))
        })
    }
}

async fn timed<T>(
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
    operation: impl std::future::Future<Output = oracle_rs::Result<T>>,
) -> Result<T> {
    with_timeout_duration(timeout, phase, cancellation, operation).await
}

fn validate_options(options: &TransactionOptions) -> Result<()> {
    if !options.context.is_empty() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Oracle,
            ErrorPhase::Prepare,
            "session context Oracle non ancora qualificato",
        ));
    }
    if options.statement_timeout_ms.is_some() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Oracle,
            ErrorPhase::Prepare,
            "statement timeout Oracle per-transazione non ancora qualificato",
        ));
    }
    if options.deferrable == Some(true) {
        return Err(DatabaseError::unsupported(
            ProviderKind::Oracle,
            ErrorPhase::Prepare,
            "transazione Oracle deferrable non supportata",
        ));
    }
    Ok(())
}

fn begin_statement(options: &TransactionOptions) -> Result<Option<&'static str>> {
    use plenora_database_core::transaction::{AccessMode, IsolationLevel};
    match (options.isolation, options.access_mode) {
        (Some(IsolationLevel::ReadUncommitted | IsolationLevel::RepeatableRead), _) => {
            Err(DatabaseError::unsupported(
                ProviderKind::Oracle,
                ErrorPhase::Prepare,
                "livello di isolamento Oracle non supportato",
            ))
        }
        (Some(IsolationLevel::Serializable), Some(AccessMode::ReadOnly)) => {
            Err(DatabaseError::invalid_plan(
                "Oracle non combina SERIALIZABLE e READ ONLY nello stesso SET TRANSACTION",
            ))
        }
        (Some(IsolationLevel::Serializable), _) => {
            Ok(Some("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
        }
        (Some(IsolationLevel::ReadCommitted), _) => {
            Ok(Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        }
        (None, Some(AccessMode::ReadOnly)) => Ok(Some("SET TRANSACTION READ ONLY")),
        (None, Some(AccessMode::ReadWrite)) => Ok(Some("SET TRANSACTION READ WRITE")),
        (None, None) => Ok(None),
    }
}

pub async fn execute_ddl(
    config: &OracleConfig,
    pool: &Arc<OraclePool>,
    sql: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    let connection = pool.checkout(cancellation).await?;
    with_timeout(
        config,
        ErrorPhase::Write,
        cancellation,
        connection.connection()?.execute(sql, &[]),
    )
    .await?;
    drop(connection);
    Ok(())
}
