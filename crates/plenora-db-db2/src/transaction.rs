use crate::connection::open_connection;
use crate::error::{driver_error, interruption_error, task_error};
use crate::read::decode_hex_binary;
use crate::Db2Config;
use odbc_api::buffers::{BufferDesc, ColumnarDynBuffer};
use odbc_api::{
    sys::SqlDataType, BindParamDesc, Connection, Cursor, DataType, Prepared, ResultSetMetadata,
};
use plenora_database_core::caller_sql::SendMarker;
use plenora_database_core::native_query_policy::{
    enforce_policy, statement_head, NativeQueryPolicy,
};
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
// IBM CLI espone BLOB con il proprio discriminator, non SQL_LONGVARBINARY.
const DB2_SQL_BLOB: SqlDataType = SqlDataType(-98);

pub struct Db2Transaction {
    sent: SendMarker,
    #[cfg(test)]
    panic_read_worker: bool,
    connection: Option<Connection<'static>>,
    timeout: usize,
    native_query_policy: NativeQueryPolicy,
    open: bool,
    _operation_lease: ResourceLease,
}

/// Ultima risorsa per uno scope abbandonato senza commit ne rollback.
///
/// Resta raggiungibile solo cosi: commit, rollback esplicito e i percorsi di
/// write chiudono la transazione da se e riportano l'esito del rollback negli
/// assi. Qui non c'e un errore da costruire, quindi nessun asse puo mentire:
/// il chiamante che ha lasciato cadere lo scope non riceve un esito, e la
/// connessione chiusa subito dopo fa annullare a Db2 il lavoro non
/// confermato.
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
        #[cfg(test)]
        let panic_read_worker = config.panic_read_worker;
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
            #[cfg(test)]
            panic_read_worker,
            connection: Some(opened.0),
            timeout: statement_timeout(options).unwrap_or(opened.1),
            native_query_policy: options.native_query_policy,
            sent: SendMarker::default(),
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
        #[cfg(test)]
        let panic_read_worker = self.panic_read_worker && phase == ErrorPhase::Read;
        let outcome = tokio::task::spawn_blocking(move || {
            let result = operation(&connection, timeout);
            #[cfg(test)]
            assert!(
                !(panic_read_worker && result.is_ok()),
                "panic del worker dopo SQL reale"
            );
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

    pub(crate) async fn verify_write_plan(
        &mut self,
        plan: crate::write::Db2WritePlan,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.run(
            ErrorPhase::Prepare,
            cancellation,
            move |connection, timeout| plan.verify_catalog(connection, timeout),
        )
        .await
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

    /// Esegue un DML con parameter-array ODBC/CLI. Ogni placeholder e legato
    /// una volta a una colonna di valori e l'intero gruppo parte con una sola
    /// esecuzione dello statement preparato.
    pub(crate) async fn execute_parameter_array(
        &mut self,
        sql: String,
        rows: Vec<Vec<ParameterValue>>,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let columns = rows[0].len();
        if columns == 0 || rows.iter().any(|row| row.len() != columns) {
            return Err(transaction_error(
                ErrorCategory::InvalidPlan,
                ErrorPhase::Write,
                "parameter-array Db2 con arita incoerente",
            ));
        }
        // Forma e dominio prima di ogni connessione; l'ammissione sul tipo
        // descritto di ogni segnaposto avviene dopo la preparazione.
        ArrayColumns::encode(&rows, columns)?;
        let expected = u64::try_from(rows.len()).map_err(|_| {
            transaction_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                "righe parameter-array Db2 non rappresentabili",
            )
        })?;
        self.run(
            ErrorPhase::Write,
            cancellation,
            move |connection, timeout| {
                let mut prepared = connection
                    .prepare(&sql)
                    .map_err(|error| driver_error(&error, ErrorPhase::Write))?;
                prepared
                    .set_query_timeout_sec(timeout)
                    .map_err(|error| driver_error(&error, ErrorPhase::Write))?;
                // La regola dei parametri che incontrano un operando
                // (`operand_admission`): ogni valore si ammette contro il tipo
                // che `SQLDescribeParam` da al suo segnaposto, precisione
                // temporale e scala comprese. Il testo legato senza tipo
                // lasciava al server la conversione: un timestamp
                // `.654321` in un TIMESTAMP(0) si conservava `.00`.
                let catalog =
                    crate::parameter_catalog::text_operands(connection, timeout, &sql, columns)?;
                let encoded =
                    ArrayColumns::encode(&admit_rows(&mut prepared, &rows, &catalog)?, columns)?;
                let targets = (1..=columns)
                    .map(|index| {
                        let ordinal = u16::try_from(index)
                            .map_err(|_| array_error("troppi parametri Db2"))?;
                        prepared
                            .describe_param(ordinal)
                            .map(|description| description.data_type)
                            .map_err(|error| driver_error(&error, ErrorPhase::Prepare))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let descriptions = encoded.descriptions(&targets)?;
                let mut inserter = prepared
                    .into_column_inserter(encoded.rows, descriptions)
                    .map_err(|error| driver_error(&error, ErrorPhase::Write))?;
                inserter.set_num_rows(encoded.rows);
                encoded.fill(&mut inserter)?;
                if inserter
                    .execute()
                    .map_err(|error| driver_error(&error, ErrorPhase::Write))?
                    .is_some()
                {
                    return Err(transaction_error(
                        ErrorCategory::Protocol,
                        ErrorPhase::Write,
                        "parameter-array Db2 ha restituito un result set",
                    ));
                }
                Ok(expected)
            },
        )
        .await
    }

    /// Annulla la transazione mantenendo disponibile l'oggetto al seam
    /// row-scoped, che deve distinguere un acknowledgement confermato da uno
    /// perso senza consumare un `Box<Self>`.
    pub(crate) async fn rollback_in_place(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if !self.open {
            return Ok(());
        }
        if cancellation.is_cancelled() {
            // Anche qui il rollback si esegue e il suo esito decide gli assi:
            // una cancellazione non lascia la transazione al `Drop`.
            let error = interruption_error(cancellation, ErrorPhase::Rollback);
            let confirmed = self.rollback_connection().await.is_ok();
            return Err(abandoned_error(error, confirmed));
        }
        self.rollback_connection().await
    }

    /// Rollback sulla connessione, atteso fino alla risposta del driver.
    async fn rollback_connection(&mut self) -> Result<()> {
        self.open = false;
        let connection = self.connection.take().ok_or_else(|| {
            transaction_error(
                ErrorCategory::Internal,
                ErrorPhase::Rollback,
                "connessione Db2 assente al rollback",
            )
        })?;
        // Un rollback non confermato non prova che le scritture siano
        // annullate (ERR-003, ERR-014).
        tokio::task::spawn_blocking(move || {
            connection
                .rollback()
                .map_err(|error| driver_error(&error, ErrorPhase::Rollback))
        })
        .await
        .map_err(|_| task_error(ErrorPhase::Rollback))?
        .map_err(DatabaseError::after_unconfirmed_rollback)
    }
}

/// Esegue DDL su una connessione dedicata lasciata in autocommit.
///
/// Il driver Db2 non garantisce un conteggio righe per il DDL: usare il path
/// DML trasformerebbe quindi un'esecuzione valida in un errore di protocollo.
/// Il task bloccante viene atteso anche se la cancellazione arriva dopo l'I/O,
/// perché abbandonarlo farebbe riportare `RemoteEffect::None` mentre lo
/// statement potrebbe essere ancora in esecuzione sul server.
pub async fn execute_ddl(
    config: &Db2Config,
    secret: &SecretString,
    sql: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(interruption_error(cancellation, ErrorPhase::Write));
    }
    let config = config.clone();
    let secret = secret.clone();
    let sql = sql.to_owned();
    tokio::task::spawn_blocking(move || {
        let (connection, timeout) = open_connection(&config, &secret)?;
        execute_control(&connection, timeout, &sql, ErrorPhase::Write)
    })
    .await
    .map_err(|_| task_error(ErrorPhase::Write))?
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

/// Le colonne di un parameter-array, ciascuna con il tipo C del proprio
/// binding: `f64` per i double, che in testo perderebbero il valore, testo
/// per il resto. Le righe arrivano dalla write Arrow, il cui piano ha gia
/// confrontato i tipi con la tabella: qui si sceglie soltanto la forma esatta.
pub struct ArrayColumns {
    rows: usize,
    columns: Vec<ArrayColumn>,
}

enum ArrayColumn {
    Double(Vec<Option<f64>>),
    Text(Vec<Option<String>>),
}

impl ArrayColumns {
    /// # Errors
    ///
    /// I rifiuti di `crate::parameter::validate`, e `DataMapping` per una
    /// colonna che mescola double e altri tipi o per byte non esadecimali.
    pub fn encode(rows: &[Vec<ParameterValue>], columns: usize) -> Result<Self> {
        for row in rows {
            crate::parameter::validate(row)?;
        }
        let columns = (0..columns)
            .map(|column| {
                let doubles = rows
                    .iter()
                    .any(|row| matches!(row[column], ParameterValue::F64(_)));
                if doubles {
                    rows.iter()
                        .map(|row| match &row[column] {
                            ParameterValue::F64(value) => Ok(Some(*value)),
                            ParameterValue::Null { .. } => Ok(None),
                            _ => Err(array_error("colonna parameter-array Db2 con tipi misti")),
                        })
                        .collect::<Result<Vec<_>>>()
                        .map(ArrayColumn::Double)
                } else {
                    rows.iter()
                        .map(|row| array_text(&row[column]))
                        .collect::<Result<Vec<_>>>()
                        .map(ArrayColumn::Text)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            rows: rows.len(),
            columns,
        })
    }

    pub(crate) fn descriptions(&self, targets: &[DataType]) -> Result<Vec<BindParamDesc>> {
        if targets.len() != self.columns.len() {
            return Err(array_error("descrittori parameter-array Db2 incompleti"));
        }
        // Il buffer testuale non puo essere rivestito da un target binario:
        // IBM CLI lo leggerebbe come esadecimale, anche senza ammissione.
        if targets.iter().any(|target| {
            matches!(
                crate::parameter::operand_of(target),
                plenora_database_core::operand_admission::OperandType::Binary { .. }
            )
        }) {
            return Err(array_error(
                "target binario senza buffer binario nel parameter-array Db2",
            ));
        }
        Ok(self
            .columns
            .iter()
            .zip(targets)
            .map(|(column, target)| {
                let buffer_desc = match column {
                    ArrayColumn::Double(_) => BufferDesc::F64 { nullable: true },
                    ArrayColumn::Text(values) => BufferDesc::Text {
                        max_str_len: values
                            .iter()
                            .filter_map(|value| value.as_ref().map(String::len))
                            .max()
                            .unwrap_or(1)
                            .max(1),
                    },
                };
                BindParamDesc {
                    buffer_desc,
                    data_type: *target,
                }
            })
            .collect())
    }

    fn fill<S>(
        &self,
        inserter: &mut odbc_api::ColumnarBulkInserter<
            S,
            odbc_api::parameter::WithDataType<odbc_api::buffers::BoxColumnBuffer>,
        >,
    ) -> Result<()>
    where
        S: odbc_api::handles::AsStatementRef,
    {
        for (index, column) in self.columns.iter().enumerate() {
            match column {
                ArrayColumn::Double(values) => {
                    let mut slice = inserter
                        .column_mut(index)
                        .as_nullable_slice::<f64>()
                        .ok_or_else(|| array_error("buffer double Db2 non disponibile"))?;
                    for (row, value) in values.iter().enumerate() {
                        slice.set_cell(row, *value);
                    }
                }
                ArrayColumn::Text(values) => {
                    let mut slice = inserter
                        .column_mut(index)
                        .as_text()
                        .ok_or_else(|| array_error("buffer testo Db2 non disponibile"))?;
                    for (row, value) in values.iter().enumerate() {
                        slice.set_cell(row, value.as_deref().map(str::as_bytes));
                    }
                }
            }
        }
        Ok(())
    }
}

fn array_error(message: &'static str) -> DatabaseError {
    transaction_error(ErrorCategory::DataMapping, ErrorPhase::Write, message)
}

/// Il testo di un valore del parameter-array, gia validato. I byte non hanno
/// una forma testuale esatta: le colonne spatial arrivano gia come testo
/// esadecimale per `HEXTORAW(CAST(? AS VARCHAR(32672)))`, e una colonna binaria scalare non e
/// ammessa dal piano.
fn array_text(value: &ParameterValue) -> Result<Option<String>> {
    match value {
        ParameterValue::Null { .. } => Ok(None),
        ParameterValue::Bool(value) => Ok(Some(value.to_string())),
        ParameterValue::I32(value) => Ok(Some(value.to_string())),
        ParameterValue::I64(value) => Ok(Some(value.to_string())),
        ParameterValue::String(value)
        | ParameterValue::Date(value)
        | ParameterValue::Timestamp(value)
        | ParameterValue::Decimal(value)
        | ParameterValue::Uuid(value)
        | ParameterValue::Enum { label: value, .. } => Ok(Some(value.clone())),
        _ => Err(array_error(
            "valore senza forma testuale esatta nel parameter-array Db2",
        )),
    }
}

/// Le righe del parameter-array ammesse contro il tipo descritto di ogni
/// segnaposto e riscritte nel suo tipo.
fn admit_rows(
    prepared: &mut Prepared<odbc_api::handles::StatementImpl<'_>>,
    rows: &[Vec<ParameterValue>],
    catalog: &[Option<plenora_database_core::operand_admission::OperandType>],
) -> Result<Vec<Vec<ParameterValue>>> {
    let count = prepared
        .num_params()
        .map_err(|error| driver_error(&error, ErrorPhase::Prepare))?;
    let operands = (1..=count)
        .map(|index| {
            prepared
                .describe_param(index)
                .map(|described| described.data_type)
                .map_err(|error| driver_error(&error, ErrorPhase::Prepare))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.iter()
        .map(|row| {
            if row.len() != operands.len() {
                return Err(transaction_error(
                    ErrorCategory::InvalidPlan,
                    ErrorPhase::Prepare,
                    "numero di parametri Db2 diverso dai segnaposto",
                ));
            }
            row.iter()
                .zip(&operands)
                .zip(catalog)
                .map(|((value, described), kind)| {
                    crate::parameter::admit_with_catalog(value, described, kind.as_ref())
                })
                .collect()
        })
        .collect()
}

/// Lo statement preparato con ogni parametro legato sul tipo che il server
/// descrive per il suo segnaposto (`crate::parameter::bind_with_catalog`).
pub fn bind_described(
    connection: &Connection<'_>,
    timeout: usize,
    sql: &str,
    prepared: &mut Prepared<odbc_api::handles::StatementImpl<'_>>,
    values: &[ParameterValue],
) -> Result<Vec<crate::parameter::Bound>> {
    let count = prepared
        .num_params()
        .map_err(|error| driver_error(&error, ErrorPhase::Prepare))?;
    if usize::from(count) != values.len() {
        return Err(transaction_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            "numero di parametri Db2 diverso dai segnaposto",
        ));
    }
    let described = (1..=count)
        .map(|index| {
            prepared
                .describe_param(index)
                .map(|parameter| parameter.data_type)
                .map_err(|error| driver_error(&error, ErrorPhase::Prepare))
        })
        .collect::<Result<Vec<_>>>()?;
    let catalog = if described.iter().any(|kind| {
        matches!(
            crate::parameter::operand_of(kind),
            plenora_database_core::operand_admission::OperandType::Text { .. }
        )
    }) {
        crate::parameter_catalog::text_operands(connection, timeout, sql, values.len())?
    } else {
        vec![None; values.len()]
    };
    values
        .iter()
        .zip(&described)
        .zip(&catalog)
        .map(|((value, described), kind)| {
            crate::parameter::bind_with_catalog(value, described, kind.as_ref())
        })
        .collect()
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
    values: &[ParameterValue],
    requires_row_count: bool,
    sent: &SendMarker,
) -> Result<u64> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| driver_error(&error, ErrorPhase::Prepare))?;
    let parameters = bind_described(connection, timeout, sql, &mut statement, values)?;
    statement
        .set_query_timeout_sec(timeout)
        .map_err(|error| driver_error(&error, ErrorPhase::Write))?;
    sent.mark_sent();
    let has_cursor = statement
        .execute(parameters.as_slice())
        .map_err(|error| driver_error(&error, ErrorPhase::Write))?
        .is_some();
    if has_cursor {
        return Err(transaction_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "statement Db2 senza righe attese ha restituito un result set",
        ));
    }
    let row_count = statement
        .row_count()
        .map_err(|error| driver_error(&error, ErrorPhase::Write))?;
    match row_count {
        Some(rows) => Ok(u64::try_from(rows).unwrap_or(u64::MAX)),
        None if !requires_row_count => Ok(0),
        None => Err(transaction_error(
            ErrorCategory::Protocol,
            ErrorPhase::Write,
            "driver Db2 senza conteggio righe DML",
        )),
    }
}

pub fn db2_statement_requires_row_count(sql: &str) -> bool {
    !matches!(
        statement_head(sql).as_str(),
        "ALTER" | "COMMENT" | "CREATE" | "DROP" | "GRANT" | "RENAME" | "REVOKE" | "TRUNCATE"
    )
}

fn query_rows(
    connection: &Connection<'_>,
    timeout: usize,
    sql: &str,
    values: &[ParameterValue],
    sent: &SendMarker,
) -> Result<Vec<Row>> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| driver_error(&error, ErrorPhase::Prepare))?;
    let parameters = bind_described(connection, timeout, sql, &mut statement, values)?;
    statement
        .set_query_timeout_sec(timeout)
        .map_err(|error| driver_error(&error, ErrorPhase::Read))?;
    sent.mark_sent();
    let mut cursor = statement
        .execute(parameters.as_slice())
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
    // I floating point si leggono nel loro tipo C: il testo del driver ha
    // 15 cifre significative per un DOUBLE e 7 per un REAL, e ne perdeva il
    // valore esatto (regola di `plenora_database_core::parameter_fidelity`).
    let buffer = ColumnarDynBuffer::try_from_descs(
        QUERY_BATCH_ROWS,
        types.iter().map(|data_type| match floating(*data_type) {
            Some(Floating::Double) => BufferDesc::F64 { nullable: true },
            Some(Floating::Real) => BufferDesc::F32 { nullable: true },
            None => BufferDesc::Text {
                max_str_len: QUERY_CELL_BYTES,
            },
        }),
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
                .map(|(column, data_type)| decode_cell(batch.column(column), row_index, *data_type))
                .collect::<Result<Vec<_>>>()?;
            rows.push(Row::try_new(Arc::clone(&columns), values)?);
        }
    }
    Ok(rows)
}

#[derive(Clone, Copy)]
enum Floating {
    Double,
    Real,
}

const fn floating(data_type: DataType) -> Option<Floating> {
    match data_type {
        DataType::Double => Some(Floating::Double),
        DataType::Float { precision } if precision > 24 => Some(Floating::Double),
        DataType::Real | DataType::Float { .. } => Some(Floating::Real),
        _ => None,
    }
}

fn decode_cell(
    column: odbc_api::buffers::AnyColumnBufferSlice<'_>,
    row: usize,
    data_type: DataType,
) -> Result<ParameterValue> {
    let unavailable = || {
        transaction_error(
            ErrorCategory::Internal,
            ErrorPhase::Read,
            "buffer query Db2 di tipo inatteso",
        )
    };
    let float = |value| decode_float(Some(value), data_type);
    match floating(data_type) {
        Some(Floating::Double) => {
            let slice = column.as_nullable_slice::<f64>().ok_or_else(unavailable)?;
            slice
                .get(row)
                .map_or_else(|| decode_value(None, data_type), |value| float(*value))
        }
        Some(Floating::Real) => {
            let slice = column.as_nullable_slice::<f32>().ok_or_else(unavailable)?;
            slice.get(row).map_or_else(
                || decode_value(None, data_type),
                |value| float(f64::from(*value)),
            )
        }
        None => {
            let slice = column.as_text().ok_or_else(unavailable)?;
            if slice.has_truncated_values().is_some() {
                return Err(transaction_error(
                    ErrorCategory::ResourceLimit,
                    ErrorPhase::Read,
                    "cella query Db2 oltre il limite transazionale",
                ));
            }
            decode_value(slice.get(row), data_type)
        }
    }
}

/// Un float letto come `SQL_C_DOUBLE`: esatto, e soltanto finito.
///
/// # Errors
///
/// `data_mapping` se il valore non e finito.
pub fn decode_float(value: Option<f64>, data_type: DataType) -> Result<ParameterValue> {
    let Some(value) = value else {
        return Ok(ParameterValue::Null {
            type_name: type_name(data_type).to_owned(),
        });
    };
    if value.is_finite() {
        Ok(ParameterValue::F64(value))
    } else {
        Err(transaction_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            "float query Db2 non finito",
        ))
    }
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
        DataType::Binary { .. }
        | DataType::Varbinary { .. }
        | DataType::LongVarbinary { .. }
        | DataType::Other {
            data_type: DB2_SQL_BLOB,
            ..
        } => decode_hex_binary(value).map(ParameterValue::Bytes),
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
    // Il canone e il microsecondo: un TIMESTAMP(9) o (12) con cifre oltre si
    // rifiuta, invece di troncarle nella formattazione.
    if plenora_database_core::parameter_fidelity::has_sub_microsecond(value) {
        return Err(transaction_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            "timestamp query Db2 oltre il microsecondo",
        ));
    }
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
        DataType::Binary { .. }
        | DataType::Varbinary { .. }
        | DataType::LongVarbinary { .. }
        | DataType::Other {
            data_type: DB2_SQL_BLOB,
            ..
        } => "binary",
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
    fn begin_caller_sql(&mut self) -> SendMarker {
        self.sent = SendMarker::before_send();
        self.sent.clone()
    }

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
            crate::parameter::validate(&statement.params)?;
            let parameters = statement.params.clone();
            let requires_row_count = db2_statement_requires_row_count(&sql);
            let sent = self.sent.clone();
            self.run(
                ErrorPhase::Write,
                cancellation,
                move |connection, timeout| {
                    execute_statement(
                        connection,
                        timeout,
                        &sql,
                        &parameters,
                        requires_row_count,
                        &sent,
                    )
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
            crate::parameter::validate(&statement.params)?;
            let parameters = statement.params.clone();
            let sent = self.sent.clone();
            self.run(
                ErrorPhase::Read,
                cancellation,
                move |connection, timeout| {
                    query_rows(connection, timeout, &sql, &parameters, &sent)
                },
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
            // Inviato il ROLLBACK TO SAVEPOINT, un errore non lo conferma.
            self.control(
                format!("ROLLBACK TO SAVEPOINT {name}"),
                ErrorPhase::Rollback,
                cancellation,
            )
            .await
            .map_err(DatabaseError::after_unconfirmed_rollback)
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

    /// Le colonne delle relazioni nominate, con il giudizio del tipo
    /// (`crate::operand_catalog`), lette sulla connessione della transazione:
    /// `CURRENT SCHEMA` e quello con cui il server risolvera la query. Non e
    /// SQL del chiamante: non tocca il marcatore di invio.
    fn operand_catalog<'a>(
        &'a mut self,
        relations: &'a [(Option<String>, String)],
        _names: &'a [String],
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, plenora_database_core::spatial_operand::OperandCatalog> {
        Box::pin(async move {
            let relations = relations.to_vec();
            self.run(
                ErrorPhase::Prepare,
                cancellation,
                move |connection, timeout| {
                    crate::operand_catalog::operand_catalog(
                        &mut crate::operand_catalog::OdbcCatalog::new(connection, timeout),
                        &relations,
                    )
                },
            )
            .await
        })
    }

    fn column_types<'a>(
        &'a mut self,
        relation: &'a (Option<String>, String),
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<plenora_database_core::operand_admission::RelationTypes>> {
        Box::pin(async move {
            let relation = relation.clone();
            self.run(
                ErrorPhase::Prepare,
                cancellation,
                move |connection, timeout| {
                    crate::operand_catalog::OdbcCatalog::new(connection, timeout)
                        .column_types(relation.0.as_deref(), &relation.1)
                },
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
                // Le righe sono gia applicate nella transazione: il commit non
                // parte, e il rollback si tenta qui, in modo esplicito, perche
                // il suo esito decida gli assi. Lasciarlo al `Drop`, che ne
                // ignora l'esito, faceva dichiarare `none` anche quando il
                // rollback falliva.
                let error = interruption_error(cancellation, ErrorPhase::Commit);
                let confirmed = self.rollback_connection().await.is_ok();
                return Err(abandoned_error(error, confirmed));
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
                // La causa del driver diventa la categoria (ERR-001); un task
                // perso non ne ha una dimostrabile.
                // Regola unica dopo l'invio del COMMIT; per Db2 l'elenco dei
                // rifiuti che provano il rollback e vuoto, quindi ogni errore
                // lascia l'esito ignoto.
                Ok(Err(error)) => plenora_database_core::transaction::commit_failure_outcome(
                    driver_error(&error, ErrorPhase::Commit),
                    false,
                ),
                Err(_) => Ok(CommitOutcome::OutcomeUnknown {
                    recovery: outcome_unknown_recovery(),
                }),
            }
        })
    }

    fn rollback(mut self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async move { self.rollback_in_place(cancellation).await })
    }
}

/// Gli assi di un'operazione interrotta prima del commit, dopo il rollback
/// esplicito: annullata se il rollback e confermato, ignota altrimenti.
pub fn abandoned_error(error: DatabaseError, rollback_confirmed: bool) -> DatabaseError {
    if rollback_confirmed {
        DatabaseError {
            remote_effect: plenora_database_core::RemoteEffect::RolledBack,
            retry: plenora_database_core::RetryDisposition::Never,
            ..error
        }
    } else {
        error.after_unconfirmed_rollback()
    }
}

#[cfg(test)]
#[path = "caller_sql_send_tests.rs"]
mod caller_sql_send_tests;
