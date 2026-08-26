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
//! Non se n'era accorto nessuno perche i consumer e le prove non attraversavano
//! questo bordo comune: le prove live usavano le primitive TDS direttamente.
//!
//! # Cosa copre
//!
//! Il contratto transazionale comune, inclusi savepoint e rollback al punto.
//! Il rilascio resta rifiutato perche T-SQL non ha `RELEASE SAVEPOINT`; la
//! capability comune non promette quell'operazione distinta.

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
    native_query_policy: plenora_database_core::native_query_policy::NativeQueryPolicy,
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
        validate_options(options)?;
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
        Ok(Self {
            session,
            open: true,
            native_query_policy: options.native_query_policy,
        })
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

    /// Il nome di un savepoint, validato e racchiuso per T-SQL.
    ///
    /// Il validatore comune ammette fino a 63 caratteri, che e il limite di
    /// `PostgreSQL` e `MySQL`. SQL Server ne ammette **32**: un nome piu lungo
    /// viene troncato dal server, e due savepoint che differiscono dopo il
    /// trentaduesimo carattere diventerebbero lo stesso senza che nessuno lo
    /// dica. Un `ROLLBACK` andrebbe al punto sbagliato, e nessun errore
    /// segnalerebbe niente.
    ///
    /// Le parentesi quadre bastano perche il validatore comune ammette
    /// soltanto `[A-Za-z_][A-Za-z0-9_]*`: nessun `]` puo entrare.
    fn quoted_savepoint(name: &str) -> Result<String> {
        plenora_database_core::transaction::validate_savepoint_name(name)?;
        if name.len() > 32 {
            return Err(DatabaseError::invalid_plan(
                "il nome di savepoint su SQL Server non puo superare 32 caratteri",
            ));
        }
        Ok(format!("[{name}]"))
    }

    /// Esegue un comando di controllo della transazione.
    ///
    /// `SAVE TRANSACTION` e `ROLLBACK TRANSACTION` non rendono righe e non
    /// hanno un conteggio: passano da `execute_query`, che li drena, invece
    /// che da `execute_write_query`, che pretenderebbe un numero che il server
    /// non manda.
    async fn control(&mut self, sql: String, cancellation: &CancellationToken) -> Result<()> {
        self.session
            .session_mut()?
            .execute_query(Query::new(sql), ErrorPhase::Write, cancellation)
            .await?;
        Ok(())
    }
}

/// Verifica prima di qualunque statement le opzioni che il provider non puo
/// ancora mantenere. Ignorarle farebbe apparire applicati timeout o contesto
/// che la sessione non ha mai ricevuto.
pub fn validate_options(options: &TransactionOptions) -> Result<()> {
    use plenora_database_core::transaction::AccessMode;

    if options.access_mode == Some(AccessMode::ReadOnly) {
        return Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Prepare,
            "transazione in sola lettura non dichiarabile su SQL Server",
        ));
    }
    if options.statement_timeout_ms.is_some() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Prepare,
            "timeout per-transazione SQL Server non ancora qualificato",
        ));
    }
    if !options.context.is_empty() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Prepare,
            "session context transazionale SQL Server non ancora qualificato",
        ));
    }
    Ok(())
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
        //
        // La prima stesura chiedeva un `f64`, e il driver lo rifiutava:
        // «cannot interpret Numeric(Some(12345.6789)) as an f64 value». Il
        // rifiuto era un favore. Un `decimal(38,10)` non entra in un `f64`
        // senza perdere cifre, e la perdita sarebbe stata silenziosa — il
        // valore sarebbe arrivato al chiamante plausibile e sbagliato.
        //
        // `Numeric` di tiberius porta parte intera, parte decimale e scala, e
        // il suo `Display` le compone: testo esatto, che e cio che il
        // contratto vuole.
        Ct::Decimaln | Ct::Numericn => Ok(row
            .try_get::<tiberius::numeric::Numeric, _>(index)
            .map_err(|error| mapping(format!("decode decimal idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::Decimal(value.to_string()),
            )),
        // Il denaro arriva come `f64`, e non e una scelta di questo codice: il
        // driver lo consegna gia convertito, e chiedergli un `Numeric`
        // risponde «cannot interpret F64 as an Numeric value».
        //
        // Il limite va detto perche e reale. `money` ha scala fissa 4 e arriva
        // a ±922_337_203_685_477.5807, cioe oltre nove miliardi di miliardi di
        // unita di scala: piu di quante un `f64` ne rappresenti esattamente,
        // che si fermano a 2^53. Ai valori estremi la cifra meno significativa
        // puo non tornare.
        //
        // Non e riparabile qui — la conversione e gia avvenuta prima che
        // questo codice veda il valore — e rifiutare `money` sarebbe peggio:
        // renderebbe illeggibile una colonna che nella stragrande maggioranza
        // dei casi e esatta. Chi ha bisogno dell'esattezza su quell'ordine di
        // grandezza usa `decimal(19,4)`, che passa dal ramo sopra e non perde
        // niente.
        Ct::Money | Ct::Money4 => Ok(row
            .try_get::<f64, _>(index)
            .map_err(|error| mapping(format!("decode money idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::Decimal(value.to_string()),
            )),
        Ct::NVarchar | Ct::NChar | Ct::BigVarChar | Ct::BigChar | Ct::NText | Ct::Text => Ok(row
            .try_get::<&str, _>(index)
            .map_err(|error| mapping(format!("decode testo idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::String(value.to_owned()),
            )),
        // `xml` non e una stringa per il driver: consegna un `XmlData`, che
        // porta il documento e, quando c'e, lo schema che lo tipizza.
        // Chiedergli un `&str` risponde «cannot interpret Xml(...) as a String
        // value».
        //
        // Qui esce il documento e lo schema no: il contratto ha una stringa,
        // non una coppia, e inventare una serializzazione che li unisca
        // sarebbe un formato che nessuno ha dichiarato. Chi ha bisogno dello
        // schema lo chiede al catalogo, dove sta.
        Ct::Xml => Ok(row
            .try_get::<&tiberius::xml::XmlData, _>(index)
            .map_err(|error| mapping(format!("decode xml idx={index}: {error}")))?
            .map_or_else(
                || null_value(kind),
                |value| ParameterValue::String(value.to_string()),
            )),
        Ct::Null => Ok(null_value(kind)),
        // Il ramo di scarto c'e, e prima non c'era: `Udt` e `SSVariant`
        // avevano ciascuno il proprio, uno che rendeva byte e uno che
        // rifiutava. Erano **irraggiungibili**: tiberius 0.12.3 muore in un
        // `todo!()` mentre decodifica i loro metadati, quindi il decoder non
        // li vede mai.
        //
        // Il panico e ora catturato dove nasce — in `connection.rs`, che mette
        // la sessione in quarantena e rende un rifiuto — e qui restano soltanto
        // le famiglie che arrivano davvero. Un ramo che dichiara di gestire
        // qualcosa che non gli arriva e una promessa a nessuno.
        //
        // Il tipo non entra nel messaggio: un errore pubblico non porta
        // dettagli del payload, e il nome di un tipo TDS ne e il confine piu
        // vicino.
        _ => Err(DatabaseError::unsupported(
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
            plenora_database_core::native_query_policy::enforce_policy(
                self.native_query_policy,
                &statement.sql,
            )?;
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
            plenora_database_core::native_query_policy::enforce_policy(
                self.native_query_policy,
                &statement.sql,
            )?;
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
            plenora_database_core::native_query_policy::enforce_policy(
                self.native_query_policy,
                &statement.sql,
            )?;
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
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Write)?;
            let quoted = Self::quoted_savepoint(name)?;
            self.control(format!("SAVE TRANSACTION {quoted}"), cancellation)
                .await
        })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Write)?;
            let quoted = Self::quoted_savepoint(name)?;
            self.control(format!("ROLLBACK TRANSACTION {quoted}"), cancellation)
                .await
        })
    }

    /// Il rilascio, che su questo prodotto non esiste.
    ///
    /// `PostgreSQL` e `MySQL` hanno `RELEASE SAVEPOINT`, e dopo il rilascio un
    /// `ROLLBACK` a quel nome fallisce. T-SQL non ha l'istruzione: i savepoint
    /// si liberano da soli al commit, e fino ad allora restano raggiungibili.
    ///
    /// Rispondere `Ok(())` sarebbe la scorciatoia comoda, ed e la ragione per
    /// cui non lo fa: dopo un rilascio finto il chiamante crederebbe che quel
    /// punto non sia piu raggiungibile, e un `ROLLBACK` che invece riesce e
    /// peggio di un rifiuto. Una differenza fra prodotti si dichiara, non si
    /// simula.
    ///
    /// La capability resta coerente: `savepoints` promette `SAVEPOINT` e
    /// `ROLLBACK TO`, che ci sono. Il rilascio non e fra le due.
    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            Err(DatabaseError::unsupported(
                ProviderKind::Sqlserver,
                ErrorPhase::Write,
                "T-SQL non ha RELEASE SAVEPOINT: un savepoint si libera al commit",
            ))
        })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_open(ErrorPhase::Write)?;
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

#[cfg(test)]
mod option_tests {
    use super::validate_options;
    use plenora_database_core::native_query_policy::NativeQueryPolicy;
    use plenora_database_core::session_context::{SessionEntry, SessionValue};
    use plenora_database_core::transaction::{AccessMode, TransactionOptions};
    use plenora_database_core::ErrorCategory;

    #[test]
    fn unsupported_options_fail_closed_before_io() {
        let read_only = TransactionOptions {
            access_mode: Some(AccessMode::ReadOnly),
            ..TransactionOptions::default()
        };
        assert_eq!(
            validate_options(&read_only)
                .expect_err("read-only")
                .category,
            ErrorCategory::Unsupported
        );

        let timeout = TransactionOptions {
            statement_timeout_ms: Some(50),
            ..TransactionOptions::default()
        };
        assert_eq!(
            validate_options(&timeout).expect_err("timeout").category,
            ErrorCategory::Unsupported
        );

        let mut with_context = TransactionOptions::default();
        with_context
            .context
            .insert(
                "app.tenant",
                SessionEntry::public(SessionValue::Text("tenant".to_owned())),
            )
            .expect("context valido");
        assert_eq!(
            validate_options(&with_context)
                .expect_err("context")
                .category,
            ErrorCategory::Unsupported
        );
    }

    #[test]
    fn native_query_policy_is_an_enforced_option() {
        let options = TransactionOptions {
            native_query_policy: NativeQueryPolicy::Deny,
            ..TransactionOptions::default()
        };
        validate_options(&options).expect("la policy e applicata dagli statement");
    }
}
