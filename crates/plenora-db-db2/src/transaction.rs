use crate::connection::open_connection;
use crate::error::{driver_error, interruption_error, task_error};
use crate::Db2Config;
use odbc_api::buffers::TextRowSet;
use odbc_api::{Connection, Cursor, DataType, IntoParameter, ResultSetMetadata};
use plenora_database_core::native_query_policy::{enforce_policy, NativeQueryPolicy};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{ParameterValue, ProviderFuture, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name,
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result, Row,
};
use std::sync::Arc;

const QUERY_BATCH_ROWS: usize = 256;
const QUERY_CELL_BYTES: usize = 64 * 1024;

pub struct Db2Transaction {
    connection: Option<Connection<'static>>,
    timeout: usize,
    native_query_policy: NativeQueryPolicy,
    open: bool,
    _operation_lease: ResourceLease,
}

impl Drop for Db2Transaction {
    fn drop(&mut self) {
        if self.open {
            if let Some(connection) = self.connection.as_ref() {
                let _ = connection.rollback();
            }
            self.open = false;
        }
    }
}

impl Db2Transaction {
    pub async fn begin(
        config: &Db2Config,
        secret: &SecretString,
        options: &TransactionOptions,
        budget: &ResourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        validate_options(options)?;
        if cancellation.is_cancelled() {
            return Err(interruption_error(cancellation, ErrorPhase::Prepare));
        }
        budget.ensure_active()?;
        let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
        let config = config.clone();
        let secret = secret.clone();
        let isolation = isolation_statement(options);
        let opened = tokio::task::spawn_blocking(move || {
            let (connection, timeout) = open_connection(&config, &secret)?;
            if let Some(sql) = isolation {
                execute_control(&connection, timeout, sql, ErrorPhase::Prepare)?;
            }
            connection
                .set_autocommit(false)
                .map_err(|error| driver_error(&error, ErrorPhase::Prepare))?;
            Ok::<_, DatabaseError>((connection, timeout))
        })
        .await
        .map_err(|_| task_error(ErrorPhase::Prepare))??;
        Ok(Self {
            connection: Some(opened.0),
            timeout: statement_timeout(options).unwrap_or(opened.1),
            native_query_policy: options.native_query_policy,
            open: true,
            _operation_lease: operation_lease,
        })
    }

    fn ensure_open(&self, phase: ErrorPhase) -> Result<()> {
        if self.open && self.connection.is_some() {
            Ok(())
        } else {
            Err(transaction_error(
                ErrorCategory::InvalidPlan,
                phase,
                "transazione Db2 gia conclusa",
            ))
        }
    }

    async fn run<T, F>(
        &mut self,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection<'static>, usize) -> Result<T> + Send + 'static,
    {
        self.ensure_open(phase)?;
        if cancellation.is_cancelled() {
            return Err(interruption_error(cancellation, phase));
        }
        let connection = self.connection.take().ok_or_else(|| {
            transaction_error(ErrorCategory::Internal, phase, "connessione Db2 assente")
        })?;
        let timeout = self.timeout;
        let outcome = tokio::task::spawn_blocking(move || {
            let result = operation(&connection, timeout);
            (connection, result)
        })
        .await;
        if let Ok((connection, result)) = outcome {
            self.connection = Some(connection);
            result
        } else {
            self.open = false;
            Err(task_error(phase))
        }
    }

    async fn control(
        &mut self,
        sql: String,
        phase: ErrorPhase,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.run(phase, cancellation, move |connection, timeout| {
            execute_control(connection, timeout, &sql, phase)
        })
        .await
    }

    pub async fn execute_control_statement(
        &mut self,
        sql: String,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.control(sql, ErrorPhase::Write, cancellation).await
    }
}

pub fn validate_options(options: &TransactionOptions) -> Result<()> {
    if options.access_mode.is_some() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Db2,
            ErrorPhase::Prepare,
            "access mode transazionale Db2 non ancora qualificato",
        ));
    }
    if options.deferrable.is_some() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Db2,
            ErrorPhase::Prepare,
            "transazione deferrable Db2 non supportata",
        ));
    }
    if !options.context.is_empty() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Db2,
            ErrorPhase::Prepare,
            "session context Db2 non ancora qualificato",
        ));
    }
    if options
        .statement_timeout_ms
        .is_some_and(|timeout| timeout == 0 || !timeout.is_multiple_of(1_000))
    {
        return Err(transaction_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "timeout Db2 deve essere espresso in secondi interi positivi",
        ));
    }
    Ok(())
}

fn statement_timeout(options: &TransactionOptions) -> Option<usize> {
    options
        .statement_timeout_ms
        .and_then(|timeout| usize::try_from(timeout / 1_000).ok())
}

pub fn isolation_statement(options: &TransactionOptions) -> Option<&'static str> {
    use plenora_database_core::transaction::IsolationLevel;
    options.isolation.map(|level| match level {
        IsolationLevel::ReadUncommitted => "SET CURRENT ISOLATION = UR",
        IsolationLevel::ReadCommitted => "SET CURRENT ISOLATION = CS",
        IsolationLevel::RepeatableRead => "SET CURRENT ISOLATION = RS",
        IsolationLevel::Serializable => "SET CURRENT ISOLATION = RR",
    })
}

pub fn encode_parameters(values: &[ParameterValue]) -> Result<Vec<Option<String>>> {
    values
        .iter()
        .map(|value| match value {
            ParameterValue::Bool(value) => Ok(Some(value.to_string())),
            ParameterValue::I32(value) => Ok(Some(value.to_string())),
            ParameterValue::I64(value) => Ok(Some(value.to_string())),
            ParameterValue::F64(value) if value.is_finite() => Ok(Some(value.to_string())),
            ParameterValue::String(value)
            | ParameterValue::Date(value)
            | ParameterValue::Timestamp(value)
            | ParameterValue::Decimal(value)
            | ParameterValue::Uuid(value) => Ok(Some(value.clone())),
            ParameterValue::Enum { label, .. } => Ok(Some(label.clone())),
            ParameterValue::Bytes(value) => Ok(Some(hex_encode(value))),
            ParameterValue::Null { .. } => Ok(None),
            _ => Err(DatabaseError::unsupported(
                ProviderKind::Db2,
                ErrorPhase::Prepare,
                "tipo parametro transazionale Db2 non ancora qualificato",
            )),
        })
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn execute_control(
    connection: &Connection<'_>,
    timeout: usize,
    sql: &str,
    phase: ErrorPhase,
) -> Result<()> {
    let cursor = connection
        .execute(sql, (), Some(timeout))
        .map_err(|error| driver_error(&error, phase))?;
    if cursor.is_some() {
        return Err(transaction_error(
            ErrorCategory::Protocol,
            phase,
            "comando transazionale Db2 ha restituito righe",
        ));
    }
    Ok(())
}

fn execute_statement(
    connection: &Connection<'_>,
    timeout: usize,
    sql: &str,
    parameters: &[Option<String>],
) -> Result<u64> {
    let parameters: Vec<_> = parameters
        .iter()
        .map(|parameter| parameter.as_deref().into_parameter())
        .collect();
    let mut statement = connection
        .preallocate()
        .map_err(|error| driver_error(&error, ErrorPhase::Write))?;
    statement
        .set_query_timeout_sec(timeout)
        .map_err(|error| driver_error(&error, ErrorPhase::Write))?;
    let has_cursor = statement
        .execute(sql, parameters.as_slice())
        .map_err(|error| driver_error(&error, ErrorPhase::Write))?
        .is_some();
    if has_cursor {
        return Err(transaction_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "statement DML Db2 ha restituito un result set",
        ));
    }
    statement
        .row_count()
        .map_err(|error| driver_error(&error, ErrorPhase::Write))?
        .map(|rows| u64::try_from(rows).unwrap_or(u64::MAX))
        .ok_or_else(|| {
            transaction_error(
                ErrorCategory::Protocol,
                ErrorPhase::Write,
                "driver Db2 senza conteggio righe DML",
            )
        })
}

fn query_rows(
    connection: &Connection<'_>,
    timeout: usize,
    sql: &str,
    parameters: &[Option<String>],
) -> Result<Vec<Row>> {
    let parameters: Vec<_> = parameters
        .iter()
        .map(|parameter| parameter.as_deref().into_parameter())
        .collect();
    let mut cursor = connection
        .execute(sql, parameters.as_slice(), Some(timeout))
        .map_err(|error| driver_error(&error, ErrorPhase::Read))?
        .ok_or_else(|| {
            transaction_error(
                ErrorCategory::Protocol,
                ErrorPhase::Read,
                "query transazionale Db2 senza result set",
            )
        })?;
    let column_count = usize::try_from(
        cursor
            .num_result_cols()
            .map_err(|error| driver_error(&error, ErrorPhase::Read))?,
    )
    .map_err(|_| {
        transaction_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            "numero colonne Db2 non rappresentabile",
        )
    })?;
    let mut names = Vec::with_capacity(column_count);
    let mut types = Vec::with_capacity(column_count);
    for index in 1..=column_count {
        let index = u16::try_from(index).map_err(|_| {
            transaction_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                "indice colonna Db2 non rappresentabile",
            )
        })?;
        names.push(
            cursor
                .col_name(index)
                .map_err(|error| driver_error(&error, ErrorPhase::Read))?,
        );
        types.push(
            cursor
                .col_data_type(index)
                .map_err(|error| driver_error(&error, ErrorPhase::Read))?,
        );
    }
    let columns: Arc<[String]> = names.into();
    let buffer = TextRowSet::from_max_str_lens(
        QUERY_BATCH_ROWS,
        std::iter::repeat_n(QUERY_CELL_BYTES, column_count),
    )
    .map_err(|error| driver_error(&error, ErrorPhase::Read))?;
    let mut blocks = cursor
        .bind_buffer(buffer)
        .map_err(|error| driver_error(&error, ErrorPhase::Read))?;
    let mut rows = Vec::new();
    while let Some(batch) = blocks
        .fetch()
        .map_err(|error| driver_error(&error, ErrorPhase::Read))?
    {
        for row_index in 0..batch.num_rows() {
            let values = types
                .iter()
                .enumerate()
                .map(|(column, data_type)| {
                    if batch
                        .indicator_at(column, row_index)
                        .is_truncated(batch.max_len(column))
                    {
                        return Err(transaction_error(
                            ErrorCategory::ResourceLimit,
                            ErrorPhase::Read,
                            "cella query Db2 oltre il limite transazionale",
                        ));
                    }
                    decode_value(batch.at(column, row_index), *data_type)
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(Row::try_new(Arc::clone(&columns), values)?);
        }
    }
    Ok(rows)
}

pub fn decode_value(value: Option<&[u8]>, data_type: DataType) -> Result<ParameterValue> {
    let Some(value) = value else {
        return Ok(ParameterValue::Null {
            type_name: type_name(data_type).to_owned(),
        });
    };
    let raw_text = std::str::from_utf8(value).map_err(|_| {
        transaction_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            "valore query Db2 non UTF-8",
        )
    })?;
    let text = raw_text.trim();
    let parse = |message| transaction_error(ErrorCategory::DataMapping, ErrorPhase::Read, message);
    match data_type {
        DataType::Bit => match text.to_ascii_uppercase().as_str() {
            "1" | "TRUE" => Ok(ParameterValue::Bool(true)),
            "0" | "FALSE" => Ok(ParameterValue::Bool(false)),
            _ => Err(parse("BOOLEAN query Db2 non rappresentabile")),
        },
        DataType::TinyInt | DataType::SmallInt | DataType::Integer => text
            .parse()
            .map(ParameterValue::I32)
            .map_err(|_| parse("intero query Db2 non rappresentabile")),
        DataType::BigInt => text
            .parse()
            .map(ParameterValue::I64)
            .map_err(|_| parse("BIGINT query Db2 non rappresentabile")),
        DataType::Real | DataType::Float { .. } | DataType::Double => text
            .parse::<f64>()
            .map_err(|_| parse("float query Db2 non rappresentabile"))
            .and_then(|value| {
                value
                    .is_finite()
                    .then_some(ParameterValue::F64(value))
                    .ok_or_else(|| parse("float query Db2 non finito"))
            }),
        DataType::Numeric { .. } | DataType::Decimal { .. } => {
            Ok(ParameterValue::Decimal(text.to_owned()))
        }
        DataType::Date => Ok(ParameterValue::Date(text.to_owned())),
        DataType::Timestamp { .. } => Ok(ParameterValue::Timestamp(canonical_timestamp(text)?)),
        DataType::Char { .. }
        | DataType::WChar { .. }
        | DataType::Varchar { .. }
        | DataType::WVarchar { .. }
        | DataType::LongVarchar { .. }
        | DataType::WLongVarchar { .. } => Ok(ParameterValue::String(raw_text.to_owned())),
        _ => Err(DatabaseError::unsupported(
            ProviderKind::Db2,
            ErrorPhase::Read,
            "tipo result set Db2 non ancora qualificato",
        )),
    }
}

pub fn canonical_timestamp(value: &str) -> Result<String> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d-%H.%M.%S%.f"))
        .map(|value| value.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        .map_err(|_| {
            transaction_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                "timestamp query Db2 non rappresentabile",
            )
        })
}

const fn type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Bit => "boolean",
        DataType::TinyInt => "tinyint",
        DataType::SmallInt => "smallint",
        DataType::Integer => "integer",
        DataType::BigInt => "bigint",
        DataType::Real | DataType::Float { .. } => "real",
        DataType::Double => "double",
        DataType::Numeric { .. } | DataType::Decimal { .. } => "decimal",
        DataType::Date => "date",
        DataType::Time { .. } => "time",
        DataType::Timestamp { .. } => "timestamp",
        DataType::Char { .. } | DataType::WChar { .. } => "char",
        DataType::Varchar { .. }
        | DataType::WVarchar { .. }
        | DataType::LongVarchar { .. }
        | DataType::WLongVarchar { .. } => "varchar",
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            "binary"
        }
        DataType::Unknown | DataType::Other { .. } => "unknown",
    }
}

fn transaction_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: &'static str,
) -> DatabaseError {
    DatabaseError::new(category, phase, Some(ProviderKind::Db2), message)
}

impl TransactionScope for Db2Transaction {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Db2
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        Box::pin(async move {
            enforce_policy(self.native_query_policy, &statement.sql)?;
            let sql = statement.sql.clone();
            let parameters = encode_parameters(&statement.params)?;
            self.run(
                ErrorPhase::Write,
                cancellation,
                move |connection, timeout| {
                    execute_statement(connection, timeout, &sql, &parameters)
                },
            )
            .await
        })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            enforce_policy(self.native_query_policy, &statement.sql)?;
            let sql = statement.sql.clone();
            let parameters = encode_parameters(&statement.params)?;
            self.run(
                ErrorPhase::Read,
                cancellation,
                move |connection, timeout| query_rows(connection, timeout, &sql, &parameters),
            )
            .await
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async move {
            Err(DatabaseError::unsupported(
                ProviderKind::Db2,
                ErrorPhase::Read,
                "row stream transazionale Db2 non ancora qualificato",
            ))
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            validate_savepoint_name(name)?;
            self.control(
                format!("SAVEPOINT {name} ON ROLLBACK RETAIN CURSORS"),
                ErrorPhase::Write,
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
            validate_savepoint_name(name)?;
            self.control(
                format!("ROLLBACK TO SAVEPOINT {name}"),
                ErrorPhase::Rollback,
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
            validate_savepoint_name(name)?;
            self.control(
                format!("RELEASE SAVEPOINT {name}"),
                ErrorPhase::Write,
                cancellation,
            )
            .await
        })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            let affected = self.execute(request.update, cancellation).await?;
            if affected == request.expected_affected_rows {
                return Ok(());
            }
            let Some(probe) = request.key_probe else {
                return Err(concurrent_modification_error(
                    "update condizionato Db2 con conteggio inatteso",
                ));
            };
            if self.query(probe, cancellation).await?.is_empty() {
                Err(transaction_error(
                    ErrorCategory::NotFound,
                    ErrorPhase::Write,
                    "update condizionato Db2 con chiave assente",
                ))
            } else {
                Err(concurrent_modification_error(
                    "update condizionato Db2 con versione concorrente",
                ))
            }
        })
    }

    fn commit(
        mut self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Commit)?;
            if cancellation.is_cancelled() {
                return Err(interruption_error(cancellation, ErrorPhase::Commit));
            }
            self.open = false;
            let connection = self.connection.take().ok_or_else(|| {
                transaction_error(
                    ErrorCategory::Internal,
                    ErrorPhase::Commit,
                    "connessione Db2 assente al commit",
                )
            })?;
            let result = tokio::task::spawn_blocking(move || connection.commit()).await;
            match result {
                Ok(Ok(())) => Ok(CommitOutcome::Committed),
                Ok(Err(_)) | Err(_) => Ok(CommitOutcome::OutcomeUnknown {
                    recovery: outcome_unknown_recovery(),
                }),
            }
        })
    }

    fn rollback(mut self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            if !self.open {
                return Ok(());
            }
            if cancellation.is_cancelled() {
                return Err(interruption_error(cancellation, ErrorPhase::Rollback));
            }
            self.open = false;
            let connection = self.connection.take().ok_or_else(|| {
                transaction_error(
                    ErrorCategory::Internal,
                    ErrorPhase::Rollback,
                    "connessione Db2 assente al rollback",
                )
            })?;
            tokio::task::spawn_blocking(move || {
                connection
                    .rollback()
                    .map_err(|error| driver_error(&error, ErrorPhase::Rollback))
            })
            .await
            .map_err(|_| task_error(ErrorPhase::Rollback))?
        })
    }
}
