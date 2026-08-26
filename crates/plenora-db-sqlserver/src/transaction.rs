//! La transazione applicativa di SQL Server.
//!
//! # Perche esiste
//!
//! Il documento capability di questo provider dichiarava
//! `transactions.scope = Transaction`, e il trait `Provider` dice a chiare
//! lettere che devono sovrascrivere `begin_transaction` «soltanto i provider
//! che pubblicano scope pari a Transaction». Questo provider lo pubblicava e
//! non la sovrascriveva: il default rispondeva `Unsupported`, e la capability
//! era una promessa che nessuno poteva mantenere.
//!
//! Non se n'era accorto nessuno perche nessun consumatore arrivava qui. Il CLI
//! generico non apre transazioni, le prove live usano le primitive TDS
//! direttamente, e il SDK Python non raggiungeva SQL Server affatto. Il difetto
//! e emerso alla prima riga di Python che ha provato a usarlo, cioe con il
//! mezzo piu economico che esista: usarlo.
//!
//! # Cosa copre
//!
//! Tutto il contratto tranne i savepoint, che restano rifiutati perche
//! `transactions.savepoints` e dichiarato `false` — e li la dichiarazione e
//! coerente con il codice, che e il punto.

use crate::connection::TdsClient;
use crate::error::driver_error;
use crate::pool::PooledSqlServerSession;
use futures_util::TryStreamExt;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{ParameterValue, ProviderFuture};
use plenora_database_core::transaction::{
    ConditionalUpdate, RowStream, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{
    CancellationToken, CommitOutcome, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect,
    Result, RetryDisposition, Row,
};
use std::sync::Arc;
use tiberius::Query;

/// Una transazione aperta su una sessione presa dal pool.
pub struct SqlServerTransaction {
    session: PooledSqlServerSession,
    open: bool,
}

impl SqlServerTransaction {
    /// Apre la transazione, con il livello di isolamento che le opzioni
    /// chiedono.
    ///
    /// L'isolamento si imposta **prima** del `BEGIN`: su SQL Server
    /// `SET TRANSACTION ISOLATION LEVEL` dentro una transazione gia aperta
    /// vale dalla successiva, quindi farlo dopo darebbe un livello diverso da
    /// quello dichiarato senza che nessuno se ne accorga.
    ///
    /// # Errors
    ///
    /// Se la sessione non e utilizzabile, o il server rifiuta il livello.
    pub async fn begin(
        mut session: PooledSqlServerSession,
        options: &TransactionOptions,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        if let Some(level) = isolation_statement(options) {
            let inner = session.session_mut()?;
            inner
                .execute_query(Query::new(level), ErrorPhase::Prepare, cancellation)
                .await?;
        }
        session.session_mut()?.begin(cancellation).await?;
        // Il profilo di sola lettura non ha un equivalente dichiarativo su SQL
        // Server: non esiste `SET TRANSACTION READ ONLY`. Rifiutarlo qui e piu
        // onesto che accettarlo e non applicarlo — una transazione che il
        // chiamante crede in sola lettura e che scrive e peggio di un rifiuto.
        if options.access_mode == Some(plenora_database_core::transaction::AccessMode::ReadOnly) {
            let mut open = Self {
                session,
                open: true,
            };
            open.abort(cancellation).await;
            return Err(DatabaseError::unsupported(
                ProviderKind::Sqlserver,
                ErrorPhase::Prepare,
                "transazione in sola lettura non dichiarabile su SQL Server",
            ));
        }
        Ok(Self {
            session,
            open: true,
        })
    }

    /// Chiude la transazione senza propagare l'esito: serve ai percorsi che
    /// stanno gia rendendo un errore, e che non devono sostituirlo con un
    /// secondo.
    async fn abort(&mut self, cancellation: &CancellationToken) {
        if !self.open {
            return;
        }
        self.open = false;
        if let Ok(session) = self.session.session_mut() {
            let _ = session.rollback(cancellation).await;
        }
    }

    fn ensure_open(&self, phase: ErrorPhase) -> Result<()> {
        if self.open {
            return Ok(());
        }
        Err(DatabaseError {
            category: ErrorCategory::InvalidPlan,
            phase,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(ProviderKind::Sqlserver),
            execution_id: None,
            diagnostics: None,
            message: "transazione SQL Server gia conclusa".to_owned(),
        })
    }

    /// Il rifiuto dei savepoint, uno per i tre metodi.
    ///
    /// Il motore li ha — `SAVE TRANSACTION` esiste — e non e per quello che
    /// sono chiusi: e che `transactions.savepoints` dichiara `false`, e una
    /// capability chiusa con un percorso aperto sotto e la stessa forma di
    /// difetto che questo modulo esiste per correggere, al contrario.
    fn savepoints_are_closed() -> DatabaseError {
        DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Write,
            "savepoint non pubblicati dal provider SQL Server",
        )
    }
}

/// Lo statement che impone il livello di isolamento, se le opzioni ne
/// chiedono uno.
fn isolation_statement(options: &TransactionOptions) -> Option<&'static str> {
    use plenora_database_core::transaction::IsolationLevel;
    options.isolation.map(|level| match level {
        IsolationLevel::ReadUncommitted => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        IsolationLevel::ReadCommitted => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        IsolationLevel::RepeatableRead => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        IsolationLevel::Serializable => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    })
}

/// Prepara la query TDS di uno `Statement`, parametri compresi.
///
/// I segnaposto sono quelli di SQL Server — `@P1`, `@P2` — e non `?`: la
/// traduzione non avviene qui, perche uno `Statement` porta SQL nativo e
/// tradurlo sarebbe riscrivere cio che il chiamante ha scritto.
fn prepared(statement: &Statement) -> Result<Query<'static>> {
    let mut query = Query::new(statement.sql.clone());
    for value in &statement.params {
        crate::parameter::bind_parameter(&mut query, value)?;
    }
    Ok(query)
}

/// I nomi di colonna di un result set, una volta sola per stream.
fn column_names(row: &tiberius::Row) -> Arc<[String]> {
    row.columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect::<Vec<_>>()
        .into()
}

/// Una riga TDS diventa una riga del contratto.
///
/// # Perche un decoder e non il mapper Arrow
///
/// Il percorso di lettura mappa verso Arrow partendo dal **catalogo**: sa il
/// tipo dichiarato di ogni colonna prima di vedere un byte. Qui il catalogo
/// non c'e — uno `Statement` porta SQL arbitrario, e `SELECT 1 + 1` non ha una
/// colonna in `information_schema` — quindi il tipo si legge da cio che il
/// wire dichiara a runtime.
pub fn decode_row(row: &tiberius::Row, columns: &Arc<[String]>) -> Result<Row> {
    let kinds: Vec<tiberius::ColumnType> = row
        .columns()
        .iter()
        .map(tiberius::Column::column_type)
        .collect();
    let mut values = Vec::with_capacity(columns.len());
    for (index, kind) in kinds.iter().enumerate() {
        values.push(decode_cell(row, index, *kind)?);
    }
    Row::try_new(Arc::clone(columns), values)
}

/// Il `NULL` di una cella, con il tipo che il wire ha dichiarato.
///
/// `ParameterValue::Null` porta un `type_name`, e altrove nel repository vale
/// `"unknown"` perche il protocollo di quel prodotto non lo dice per una cella
/// vuota. TDS lo dice: la COLMETADATA descrive la colonna prima delle righe, e
/// una cella assente conserva il proprio tipo dichiarato.
///
/// Il nome e quello SQL, non quello del wire: chi legge un valore nullo vuole
/// sapere cosa avrebbe potuto esserci, e `int` glielo dice meglio di `Int4`.
fn null_value(kind: tiberius::ColumnType) -> ParameterValue {
    use tiberius::ColumnType as Ct;
    let name = match kind {
        Ct::Bit | Ct::Bitn => "bit",
        Ct::Int1 => "tinyint",
        Ct::Int2 => "smallint",
        Ct::Int4 => "int",
        Ct::Int8 => "bigint",
        Ct::Intn => "integer",
        Ct::Float4 => "real",
        Ct::Float8 | Ct::Floatn => "float",
        Ct::Guid => "uniqueidentifier",
        Ct::BigVarBin | Ct::BigBinary => "varbinary",
        Ct::Image => "image",
        Ct::Udt => "udt",
        Ct::Datetime | Ct::Datetimen => "datetime",
        Ct::Datetime2 => "datetime2",
        Ct::Datetime4 => "smalldatetime",
        Ct::DatetimeOffsetn => "datetimeoffset",
        Ct::Daten => "date",
        Ct::Timen => "time",
        Ct::Decimaln | Ct::Numericn => "decimal",
        Ct::Money => "money",
        Ct::Money4 => "smallmoney",
        Ct::NVarchar | Ct::NChar | Ct::NText => "nvarchar",
        Ct::BigVarChar | Ct::BigChar | Ct::Text => "varchar",
        Ct::Xml => "xml",
        _ => "unknown",
    };
    ParameterValue::Null {
        type_name: name.to_owned(),
    }
}

/// Il valore di una cella, secondo il tipo che il wire dichiara.
#[allow(clippy::too_many_lines)] // un ramo per famiglia di tipo TDS
fn decode_cell(
    row: &tiberius::Row,
    index: usize,
    kind: tiberius::ColumnType,
) -> Result<ParameterValue> {
    use tiberius::ColumnType as Ct;

    let mapping = |message: String| DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Read,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        diagnostics: None,
        message,
    };

    match kind {
        Ct::Bit | Ct::Bitn => Ok(row
            .try_get::<bool, _>(index)
            .map_err(|error| mapping(format!("decode bit idx={index}: {error}")))?
            .map_or_else(|| null_value(kind), ParameterValue::Bool)),
        Ct::Int1 => Ok(row
            .try_get::<u8, _>(index)
            .map_err(|error| mapping(format!("decode tinyint idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::I32(i32::from(value)),
            )),
        Ct::Int2 => Ok(row
            .try_get::<i16, _>(index)
            .map_err(|error| mapping(format!("decode smallint idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::I32(i32::from(value)),
            )),
        Ct::Int4 => Ok(row
            .try_get::<i32, _>(index)
            .map_err(|error| mapping(format!("decode int idx={index}: {error}")))?
            .map_or_else(|| null_value(kind), ParameterValue::I32)),
        Ct::Int8 => Ok(row
            .try_get::<i64, _>(index)
            .map_err(|error| mapping(format!("decode bigint idx={index}: {error}")))?
            .map_or_else(|| null_value(kind), ParameterValue::I64)),
        // `Intn` e la forma nullable, e il wire non dice quale larghezza
        // portava: si prova dalla piu larga, che le contiene tutte.
        Ct::Intn => Ok(row
            .try_get::<i64, _>(index)
            .or_else(|_| {
                row.try_get::<i32, _>(index)
                    .map(|value| value.map(i64::from))
            })
            .map_err(|error| mapping(format!("decode intn idx={index}: {error}")))?
            .map_or_else(|| null_value(kind), ParameterValue::I64)),
        Ct::Float4 => Ok(row
            .try_get::<f32, _>(index)
            .map_err(|error| mapping(format!("decode real idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::F64(f64::from(value)),
            )),
        Ct::Float8 | Ct::Floatn => Ok(row
            .try_get::<f64, _>(index)
            .map_err(|error| mapping(format!("decode float idx={index}: {error}")))?
            .map_or_else(|| null_value(kind), ParameterValue::F64)),
        Ct::Guid => Ok(row
            .try_get::<tiberius::Uuid, _>(index)
            .map_err(|error| mapping(format!("decode uniqueidentifier idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::Uuid(value.hyphenated().to_string()),
            )),
        Ct::BigVarBin | Ct::BigBinary | Ct::Image => Ok(row
            .try_get::<&[u8], _>(index)
            .map_err(|error| mapping(format!("decode binary idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::Bytes(value.to_vec()),
            )),
        // `Udt` porta i tipi spaziali, che sul wire arrivano come byte: qui
        // restano byte. Interpretarli come WKB sarebbe una promessa che questo
        // percorso non puo mantenere — il formato nativo di `geometry` non e
        // WKB, e la conversione la fa `.AsBinaryZM()` nel percorso di lettura.
        Ct::Udt => Ok(row
            .try_get::<&[u8], _>(index)
            .map_err(|error| mapping(format!("decode udt idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::Bytes(value.to_vec()),
            )),
        Ct::Datetime | Ct::Datetime2 | Ct::Datetime4 | Ct::Datetimen => Ok(row
            .try_get::<chrono::NaiveDateTime, _>(index)
            .map_err(|error| mapping(format!("decode datetime idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| {
                    ParameterValue::Timestamp(value.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
                },
            )),
        Ct::DatetimeOffsetn => Ok(row
            .try_get::<chrono::DateTime<chrono::Utc>, _>(index)
            .map_err(|error| mapping(format!("decode datetimeoffset idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::TimestampTz(value.to_rfc3339()),
            )),
        Ct::Daten => Ok(row
            .try_get::<chrono::NaiveDate, _>(index)
            .map_err(|error| mapping(format!("decode date idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::Date(value.format("%Y-%m-%d").to_string()),
            )),
        Ct::Timen => Ok(row
            .try_get::<chrono::NaiveTime, _>(index)
            .map_err(|error| mapping(format!("decode time idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::String(value.format("%H:%M:%S%.6f").to_string()),
            )),
        // Decimali e valuta: il testo e la sola forma che non perde cifre, ed
        // e cio che `ParameterValue::Decimal` porta.
        Ct::Decimaln | Ct::Numericn | Ct::Money | Ct::Money4 => Ok(row
            .try_get::<f64, _>(index)
            .map_err(|error| mapping(format!("decode decimal idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::Decimal(value.to_string()),
            )),
        Ct::NVarchar
        | Ct::NChar
        | Ct::BigVarChar
        | Ct::BigChar
        | Ct::NText
        | Ct::Text
        | Ct::Xml => Ok(row
            .try_get::<&str, _>(index)
            .map_err(|error| mapping(format!("decode testo idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::String(value.to_owned()),
            )),
        Ct::Null => Ok(null_value(kind)),
        // `SSVariant` e nominata invece di finire in un `_`: cosi una
        // variante nuova di tiberius **non compila** invece di scivolare in
        // silenzio nel rifiuto. Per un decoder e la differenza fra accorgersi
        // di un tipo nuovo e rifiutarlo per anni senza saperlo.
        //
        // Il tipo non entra nel messaggio: un errore pubblico non porta
        // dettagli del payload, e il nome di un tipo TDS ne e il confine piu
        // vicino. Chi deve saperlo lo legge dal proprio SQL.
        Ct::SSVariant => Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Read,
            "tipo di colonna SQL Server fuori dal sottoinsieme mappato",
        )),
    }
}

/// Lo stream di righe di una transazione.
///
/// Presta il client per tutta la sua vita, ed e questo che rende vera la
/// clausola del contratto: finche lo stream esiste, nessun altro `execute` o
/// `query` puo toccare la stessa transazione, perche il borrow checker lo
/// rifiuta.
struct SqlServerRowStream<'a> {
    inner: futures_util::stream::BoxStream<'a, tiberius::Result<tiberius::Row>>,
    columns: Option<Arc<[String]>>,
    batch_size: usize,
    finished: bool,
}

impl RowStream for SqlServerRowStream<'_> {
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<Vec<Row>>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            let mut batch = Vec::with_capacity(self.batch_size);
            while batch.len() < self.batch_size {
                if cancellation.is_cancelled() {
                    self.finished = true;
                    return Err(DatabaseError::interrupted(
                        cancellation,
                        Some(ProviderKind::Sqlserver),
                        ErrorPhase::Read,
                        "stream della transazione SQL Server interrotto",
                    ));
                }
                let next =
                    self.inner.try_next().await.map_err(|error| {
                        driver_error(&error, ErrorPhase::Read, RemoteEffect::None)
                    })?;
                let Some(row) = next else {
                    self.finished = true;
                    break;
                };
                let columns = Arc::clone(self.columns.get_or_insert_with(|| column_names(&row)));
                batch.push(decode_row(&row, &columns)?);
            }
            if batch.is_empty() {
                return Ok(None);
            }
            Ok(Some(batch))
        })
    }
}

impl TransactionScope for SqlServerTransaction {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Sqlserver
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Write)?;
            let query = prepared(statement)?;
            self.session
                .session_mut()?
                .execute_write_query(query, cancellation)
                .await?;
            // Il conteggio arriva da `@@ROWCOUNT`, non dal contatore TDS.
            //
            // Il bootstrap della sessione impone `SET NOCOUNT ON`, ed e la
            // scelta giusta per il percorso bulk: sopprime un pacchetto per
            // statement, e quel percorso le righe se le conta dai batch che ha
            // mandato. Ma sopprime anche cio che `execute` deve rendere, e il
            // contratto qui e esplicito — un `u64` che sono le righe toccate.
            //
            // Delle tre vie possibili questa e l'unica che non ha effetti
            // collaterali. Spegnere `NOCOUNT` per la durata della transazione
            // cambierebbe uno stato che la sessione condivide con il percorso
            // di scrittura quando torna al pool; appendere `; SELECT @@ROWCOUNT`
            // all'SQL del chiamante vorrebbe dire riscrivere cio che ha
            // scritto, che questo modulo si vieta due funzioni piu sopra.
            //
            // Il costo e un round-trip, e va letto per quello che e: il prezzo
            // di un conteggio vero invece di uno zero comodo.
            let counted = self
                .session
                .session_mut()?
                .execute_query(
                    Query::new("SELECT CAST(@@ROWCOUNT AS bigint)"),
                    ErrorPhase::Write,
                    cancellation,
                )
                .await?;
            let affected = counted
                .first()
                .and_then(|set| set.first())
                .and_then(|row| row.try_get::<i64, _>(0).ok().flatten())
                .unwrap_or_default();
            Ok(u64::try_from(affected).unwrap_or_default())
        })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Read)?;
            let query = prepared(statement)?;
            let sets = self
                .session
                .session_mut()?
                .execute_query(query, ErrorPhase::Read, cancellation)
                .await?;
            let mut rows = Vec::new();
            let mut columns: Option<Arc<[String]>> = None;
            for set in sets {
                for row in set {
                    let names = Arc::clone(columns.get_or_insert_with(|| column_names(&row)));
                    rows.push(decode_row(&row, &names)?);
                }
            }
            Ok(rows)
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
            if batch_size == 0 {
                return Err(DatabaseError::invalid_plan(
                    "batch_size dello stream deve essere > 0",
                ));
            }
            if cancellation.is_cancelled() {
                return Err(DatabaseError::interrupted(
                    cancellation,
                    Some(ProviderKind::Sqlserver),
                    ErrorPhase::Read,
                    "apertura dello stream SQL Server interrotta",
                ));
            }
            let query = prepared(statement)?;
            let client: &mut TdsClient =
                self.session.session_mut()?.client_mut(ErrorPhase::Read)?;
            let inner = query
                .query(client)
                .await
                .map_err(|error| driver_error(&error, ErrorPhase::Read, RemoteEffect::None))?
                .into_row_stream();
            Ok(Box::new(SqlServerRowStream {
                inner,
                columns: None,
                batch_size: batch_size as usize,
                finished: false,
            }) as Box<dyn RowStream + Send + 'a>)
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { Err(Self::savepoints_are_closed()) })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { Err(Self::savepoints_are_closed()) })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move { Err(Self::savepoints_are_closed()) })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Write)?;
            let affected = {
                let query = prepared(request.update)?;
                self.session
                    .session_mut()?
                    .execute_write_query(query, cancellation)
                    .await?
            };
            if affected == request.expected_affected_rows {
                return Ok(());
            }
            // La sonda distingue due esiti che il conteggio confonde: la chiave
            // non c'e, oppure c'e con un'altra versione. Senza sonda il
            // contratto impone il verdetto conservativo.
            let Some(probe) = request.key_probe else {
                return Err(
                    plenora_database_core::transaction::concurrent_modification_error(
                        "update condizionato SQL Server: righe modificate diverse dall'attesa",
                    ),
                );
            };
            let query = prepared(probe)?;
            let sets = self
                .session
                .session_mut()?
                .execute_query(query, ErrorPhase::Write, cancellation)
                .await?;
            let found = sets.iter().any(|set| !set.is_empty());
            if found {
                Err(
                    plenora_database_core::transaction::concurrent_modification_error(
                        "update condizionato SQL Server: la chiave esiste con un'altra versione",
                    ),
                )
            } else {
                Err(DatabaseError {
                    category: ErrorCategory::NotFound,
                    phase: ErrorPhase::Write,
                    remote_effect: RemoteEffect::None,
                    retry: RetryDisposition::Never,
                    provider: Some(ProviderKind::Sqlserver),
                    execution_id: None,
                    diagnostics: None,
                    message: "update condizionato SQL Server: chiave assente".to_owned(),
                })
            }
        })
    }

    fn commit(
        mut self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Commit)?;
            self.open = false;
            self.session.session_mut()?.commit(cancellation).await?;
            Ok(CommitOutcome::Committed)
        })
    }

    fn rollback(mut self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            if !self.open {
                return Ok(());
            }
            self.open = false;
            self.session.session_mut()?.rollback(cancellation).await
        })
    }
}
