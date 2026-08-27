//! Transaction scope `MySQL` — implementa `TransactionScope` del core.
//!
//! Copre begin con opzioni (isolation, access mode, statement_timeout),
//! savepoint annidati con quoting sicuro, commit/rollback disambiguato
//! (`OutcomeUnknown` in caso di canale compromesso in fase Commit).
//!
//! `query_stream` c'e, e non e un cursore: `MySQL` non ne ha fuori dalle
//! stored procedure, e cio che offre e il result set che scorre sul filo. Vedi
//! `MysqlRowStream` per cosa questo comporta e — piu utile — per cosa **non**
//! comporta.
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

use crate::error::{driver_error, interruption_error};
use crate::parameter::bind_positional_params;
use crate::profile::BINARY_CHARACTER_SET;
use crate::session::{MysqlSession, MysqlTransactionCommand};
use mysql_async::consts::ColumnType;
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
    Result,
};
use std::sync::Arc;

pub struct MysqlTransaction {
    session: MysqlSession,
    open: bool,
    /// Policy di ammissione statement (Allow default, Deny per PFM).
    /// Persistita da `TransactionOptions::native_query_policy` per tutta la
    /// durata della transazione.
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
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        // Il profilo si ricava dalla sessione, non si riceve a parte: due
        // fonti permetterebbero di aprire una transazione con il timeout di
        // un prodotto e classificarne gli errori come di un altro, e la
        // chiamata sarebbe legittima per il compilatore.
        let profile = session.profile();
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
        //    piu caratteri della grammatica del core (`$`, per dire).
        //    Ma non rendono la resa indipendente da quella regola, ed e
        //    l'affermazione da correggere: qui il backtick nel nome non
        //    viene raddoppiato, quindi una chiave che ne contenesse uno
        //    chiuderebbe la quotatura invece di finirci dentro. A impedirlo
        //    e `validate_context_keys`, che delega al core: la grammatica
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
                "session context: nome non sicuro '{name}'"
            )));
        }
        if name.len() > MAX_CONTEXT_KEY {
            return Err(DatabaseError::invalid_plan(format!(
                "session context: chiave '{name}' di {} caratteri; con il \
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
/// `MySQL` rifiuta `SET`, `SAVEPOINT`, `START TRANSACTION` ecc. nel prepared
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
/// La conversione consulta i **metadati di colonna**, non solo il valore. Sul
/// filo `BLOB` e `TEXT` arrivano entrambi come `Value::Bytes` con lo stesso
/// tipo di colonna: l'unico segnale che li distingue e il character set (63 =
/// binario). Senza consultarlo, un `BLOB` i cui byte formano per caso UTF-8
/// valido — qualunque payload ASCII — diventava una stringa, e un `TEXT` in
/// latin1 con byte non UTF-8 diventava un blob.
/// Lo stream di righe di una query dentro la transazione.
///
/// # Perche non e un cursore
///
/// `PostgreSQL` dichiara un cursore nominato e fa `FETCH FORWARD n`: ogni
/// batch e una query a se, e la connessione fra un batch e l'altro resta
/// libera. `MySQL` non ha cursori fuori dalle stored procedure, e cio che
/// offre e il result set che scorre sul filo: le righe arrivano man mano, e
/// la connessione **resta occupata** finche non sono finite.
///
/// La differenza si vede nel prestito esclusivo, che impedisce di usare la
/// transazione mentre lo stream vive: il compilatore lo impone da se, e non
/// serve altro.
///
/// Una transazione che abbandona uno stream
/// dopo un batch su cinquanta scrive, committa, e la riga si rilegge da
/// un'altra connessione: `mysql_async` drena il result set pendente prima
/// dello statement successivo, e la connessione non e mai stata fuori
/// sincrono. Le sonde `live_query_stream_*_is_reusable` e la gemella cancellata
/// verificano che la transazione resti usabile, rileggendo l'effetto da fuori.
///
/// `reads.server_cursor` resta comunque `false` su questo prodotto: questo e
/// uno stream, non un cursore che qualcuno possa nominare e riprendere.
struct MysqlRowStream<'a> {
    result: mysql_async::QueryResult<'a, 'static, mysql_async::BinaryProtocol>,
    columns: Arc<[String]>,
    batch_size: usize,
    exhausted: bool,
    kind: ProviderKind,
    profile: &'static dyn crate::profile::ProductProfile,
    timeout: std::time::Duration,
}

impl RowStream for MysqlRowStream<'_> {
    fn next_batch<'b>(
        &'b mut self,
        cancellation: &'b CancellationToken,
    ) -> ProviderFuture<'b, Option<Vec<Row>>> {
        Box::pin(async move {
            if self.exhausted {
                return Ok(None);
            }
            let mut batch = Vec::with_capacity(self.batch_size);
            while batch.len() < self.batch_size {
                let next = tokio::select! {
                    _ = cancellation.cancelled() => {
                        // Lo stream si chiude qui, e non riprende: `exhausted`
                        // impedisce che un secondo `next_batch` sullo stesso
                        // token torni a leggere righe di una lettura che il
                        // chiamante ha gia dichiarato di non volere.
                        //
                        // La **transazione**, invece, resta usabile:
                        // `RemoteEffect::None` è sostenuto dalla prova che la
                        // transazione resta usabile dopo la cancellazione.
                        self.exhausted = true;
                        return Err(interruption_error(
                            self.profile,
                            cancellation,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ));
                    }
                    result = tokio::time::timeout(self.timeout, self.result.next()) => result,
                };
                match next {
                    Ok(Ok(Some(row))) => batch.push(decode_row(row, &self.columns, self.kind)?),
                    Ok(Ok(None)) => {
                        // Il result set e finito: la connessione e pulita, e
                        // la transazione puo ancora committare.
                        self.exhausted = true;
                        break;
                    }
                    Ok(Err(error)) => {
                        self.exhausted = true;
                        return Err(driver_error(
                            self.profile,
                            &error,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ));
                    }
                    Err(_) => {
                        self.exhausted = true;
                        return Err(query_timeout_error(self.profile));
                    }
                }
            }
            if batch.is_empty() {
                Ok(None)
            } else {
                Ok(Some(batch))
            }
        })
    }
}

fn decode_row(mut row: MyRow, columns: &Arc<[String]>, kind: ProviderKind) -> Result<Row> {
    // I metadati si leggono prima: `take_opt` prende `&mut row` e non
    // convivrebbe col prestito immutabile di `columns_ref`.
    let specs: Vec<(ColumnType, u16)> = row
        .columns_ref()
        .iter()
        .map(|column| (column.column_type(), column.character_set()))
        .collect();

    let mut values = Vec::with_capacity(columns.len());
    for idx in 0..columns.len() {
        let Some(&spec) = specs.get(idx) else {
            return Err(protocol_mismatch(
                kind,
                idx,
                "il result set dichiara meno colonne dei nomi attesi",
            ));
        };
        // `take_opt` risponde `None` per una cella che il protocollo non ha
        // consegnato. Prima diventava un `NULL` SQL: una riga incompleta —
        // cioe un guasto di protocollo o di mapping — si presentava al
        // chiamante come un dato legittimo, indistinguibile da una colonna
        // davvero vuota.
        let Some(value) = row.take_opt::<Value, _>(idx) else {
            return Err(protocol_mismatch(kind, idx, "cella assente nel result set"));
        };
        let raw = value.map_err(|_| row_decode_error(kind, idx))?;
        values.push(convert_value(raw, idx, kind, spec)?);
    }
    Row::try_new(Arc::clone(columns), values)
}

fn row_decode_error(kind: ProviderKind, index: usize) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        Some(kind),
        format!("decode colonna idx={index} fallito"),
    )
}

/// Byte di una colonna testuale, o l'errore che dice che non lo sono.
fn text_or_error(
    bytes: Vec<u8>,
    kind: ProviderKind,
    idx: usize,
    character_set: u16,
) -> Result<ParameterValue> {
    String::from_utf8(bytes)
        .map(ParameterValue::String)
        .map_err(|_| non_utf8_text(kind, idx, character_set))
}

/// Un tipo wire che questo decoder non sa qualificare.
///
/// Fallisce chiuso invece di indovinare: una famiglia nuova — o una che il
/// server introduce in una versione successiva — deve essere qualificata
/// deliberatamente, non ereditare per caso il tipo pubblico di un'altra.
fn unqualified_wire_type(kind: ProviderKind, idx: usize, column_type: ColumnType) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Read,
        Some(kind),
        format!("colonna idx={idx} di tipo wire non qualificato: {column_type:?}"),
    )
}

/// Una colonna dichiarata testuale porta byte che testo non sono.
///
/// Tipicamente una collation che il client non si aspetta. Il messaggio porta
/// posizione e character set — contesto operativo — e non i byte.
fn non_utf8_text(kind: ProviderKind, idx: usize, character_set: u16) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        Some(kind),
        format!("colonna idx={idx} testuale (charset {character_set}) non e UTF-8"),
    )
}

/// Il result set non ha la forma che dichiara.
///
/// `Protocol` e non `DataMapping`: non e un valore che non si sa convertire, e
/// la conversazione col server che non torna. Il messaggio porta la posizione,
/// mai il contenuto.
fn protocol_mismatch(kind: ProviderKind, idx: usize, what: &str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Protocol,
        ErrorPhase::Read,
        Some(kind),
        format!("{what} (colonna idx={idx})"),
    )
}

fn convert_value(
    value: Value,
    idx: usize,
    kind: ProviderKind,
    spec: (ColumnType, u16),
) -> Result<ParameterValue> {
    Ok(match value {
        Value::NULL => ParameterValue::Null {
            type_name: "unknown".to_owned(),
        },
        Value::Int(v) => ParameterValue::I64(v),
        Value::UInt(v) => {
            // MySQL UInt64 può eccedere I64 (>2^63); il decoder canonico non
            // ha un tipo unsigned. Falliamo esplicito piuttosto che overflow.
            i64::try_from(v).map(ParameterValue::I64).map_err(|_| {
                DatabaseError::new(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    Some(kind),
                    format!("colonna idx={idx} UInt eccede i64"),
                )
            })?
        }
        Value::Float(v) => ParameterValue::F64(f64::from(v)),
        Value::Double(v) => ParameterValue::F64(v),
        // Il tipo pubblico di una cella lo decide il **tipo di colonna**, mai
        // l'aspetto dei byte.
        //
        // Sul filo molte famiglie diverse arrivano tutte come `Value::Bytes`.
        // Indovinare provando a leggerle come UTF-8 fa dipendere il tipo dal
        // contenuto: un `BIT(8)` che vale `0x41` diventava la stringa `"A"` e
        // lo stesso `BIT(8)` che vale `0xff` restava byte — la stessa colonna,
        // due tipi, decisi dal dato.
        //
        // Le tre classi qui sotto sono la stessa partizione che `profile.rs`
        // applica in `text_kind` e nel mapping dei tipi wire, quindi il path
        // transazionale e quello del catalogo dicono la stessa cosa.
        Value::Bytes(bytes) => {
            let (column_type, character_set) = spec;
            match column_type {
                // Hanno entrambe le forme, e solo il charset le distingue.
                ColumnType::MYSQL_TYPE_STRING
                | ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_BLOB => {
                    if character_set == BINARY_CHARACTER_SET {
                        ParameterValue::Bytes(bytes)
                    } else {
                        text_or_error(bytes, kind, idx, character_set)?
                    }
                }
                // Sempre testo, qualunque charset dichiarino. `DECIMAL` viene
                // consegnato come stringa di cifre — e cio che l'intestazione
                // di questo modulo promette — e `JSON`, `ENUM`, `SET` sono
                // testuali per definizione.
                ColumnType::MYSQL_TYPE_DECIMAL
                | ColumnType::MYSQL_TYPE_NEWDECIMAL
                | ColumnType::MYSQL_TYPE_JSON
                | ColumnType::MYSQL_TYPE_ENUM
                | ColumnType::MYSQL_TYPE_SET => text_or_error(bytes, kind, idx, character_set)?,
                // Sempre byte. `BIT` e una maschera, `GEOMETRY` e un WKB:
                // nessuno dei due e testo, nemmeno quando i byte lo sembrano.
                ColumnType::MYSQL_TYPE_BIT | ColumnType::MYSQL_TYPE_GEOMETRY => {
                    ParameterValue::Bytes(bytes)
                }
                // Un tipo che questo decoder non qualifica non viene
                // indovinato: fallisce chiuso, cosi chi lo incontra lo
                // qualifica invece di scoprirlo da un tipo pubblico
                // sbagliato.
                other => {
                    return Err(unqualified_wire_type(kind, idx, other));
                }
            }
        }
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
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
                if !self.open {
                    return Err(closed_error(ErrorPhase::Write, self.session.kind()));
                }
                // La policy condivisa blocca DDL, comandi di sessione e
                // multi-statement prima del round-trip al server.
                plenora_database_core::native_query_policy::enforce_policy(
                    self.native_query_policy,
                    &statement.sql,
                )?;
                let params = bind_positional_params(&statement.params)?;
                let result = self
                    .session
                    .exec_write(&statement.sql, params, ErrorPhase::Write, cancellation)
                    .await;
                // Quando il server restituisce `RolledBack` (per esempio per
                // deadlock o timeout ambiguo), la transazione vittima non e
                // piu attiva. Il client deve quindi chiuderla dopo qualsiasi
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
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        Box::pin(async move {
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
                if !self.open {
                    return Err(closed_error(ErrorPhase::Read, self.session.kind()));
                }
                // Il prodotto della connessione, letto una volta: piu avanti la
                // sessione e prestata in modo esclusivo e non e piu leggibile.
                let kind = self.session.kind();
                let profile = self.session.profile();
                // Applica la stessa policy usata dal percorso `execute`.
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
                        // La causa la porta il token: una deadline scaduta e
                        // un `Timeout`. Questo ramo la costruiva a mano come
                        // `Cancelled`, mentre il resto del provider passa da
                        // `interruption_error` — la stessa scadenza usciva in
                        // due categorie diverse a seconda della superficie.
                        return Err(interruption_error(
                            profile,
                            cancellation,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ));
                    }
                    result = tokio::time::timeout(timeout, execution) => result,
                };
                let rows = match outcome {
                    Ok(Ok(rows)) => rows,
                    Ok(Err(error)) => {
                        return Err(driver_error(
                            profile,
                            &error,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ));
                    }
                    Err(_) => {
                        self.session.discard().await;
                        self.open = false;
                        return Err(query_timeout_error(profile));
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
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        statement: &'a Statement,
        batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async move {
            // Bordo della transazione: cio che nasce prima del prestito
            // esclusivo della sessione non conosce il prodotto, e chi riceve
            // l'errore si'.
            let kind = self.session.kind();
            let outcome = async move {
                if !self.open {
                    return Err(closed_error(ErrorPhase::Read, kind));
                }
                if batch_size == 0 {
                    return Err(DatabaseError::invalid_plan(
                        "batch_size dello stream deve essere > 0",
                    ));
                }
                plenora_database_core::native_query_policy::enforce_policy(
                    self.native_query_policy,
                    &statement.sql,
                )?;
                let params = bind_positional_params(&statement.params)?;
                let profile = self.session.profile();
                let timeout = self.session.operation_timeout();
                let Self { session, .. } = self;
                let connection = session
                    .connection_mut()
                    .ok_or_else(|| closed_error(ErrorPhase::Read, kind))?;
                // Lo statement viaggia **posseduto**, non in prestito: il result
                // set che ne esce vive quanto lo stream, e mysql_async lega la
                // sua vita a quella del testo. E' la stessa forma che il
                // percorso Arrow usa gia in `pump_exec_rows`.
                let sql = statement.sql.clone();
                let open = connection.exec_iter(sql, params);
                // La cancellazione vale anche **all'apertura**: un token gia
                // scaduto non deve far partire una query che nessuno leggera.
                let opened = tokio::select! {
                    _ = cancellation.cancelled() => {
                        return Err(interruption_error(
                            profile,
                            cancellation,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ));
                    }
                    result = tokio::time::timeout(timeout, open) => result,
                };
                let result = match opened {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => {
                        return Err(driver_error(
                            profile,
                            &error,
                            ErrorPhase::Read,
                            RemoteEffect::None,
                        ));
                    }
                    Err(_) => {
                        return Err(query_timeout_error(profile));
                    }
                };
                // I nomi di colonna si leggono **una volta**, dai metadata del
                // result set e non dalla prima riga: uno stream che non rende
                // nemmeno una riga ha comunque uno schema, e prenderlo dalla
                // riga lo renderebbe vuoto proprio nel caso in cui il
                // chiamante non ha modo di ricavarlo altrimenti.
                let columns: Arc<[String]> =
                    Arc::from(result.columns().map_or_else(Vec::new, |columns| {
                        columns
                            .iter()
                            .map(|column| column.name_str().to_string())
                            .collect()
                    }));
                Ok(Box::new(MysqlRowStream {
                    result,
                    columns,
                    batch_size: batch_size as usize,
                    exhausted: false,
                    kind,
                    profile,
                    timeout,
                }) as Box<dyn RowStream + Send + 'a>)
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
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
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
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
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
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
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn commit(
        mut self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async move {
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
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
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn rollback(mut self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
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
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            // Bordo della transazione: la policy nativa, la
            // validazione dei savepoint e il binding costruiscono
            // errori che non conoscono il prodotto — chi li riceve
            // si', e li vedeva senza attribuzione o con il
            // segnaposto.
            let kind = self.session.kind();
            let outcome = async move {
                if !self.open {
                    return Err(closed_error(ErrorPhase::Write, self.session.kind()));
                }
                // La policy copre UPDATE e l'eventuale probe associato.
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
                    Err(conditional_update_mismatch(
                        self.session.profile(),
                        request.expected_affected_rows,
                        affected,
                    ))
                }
            }
            .await;
            crate::profile::attributed_kind(kind, outcome)
        })
    }
}

/// Il timeout di una query dentro una transazione.
///
/// Costruisce l'errore di timeout con il nome del prodotto della sessione.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn query_timeout_error(profile: &dyn crate::profile::ProductProfile) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Timeout,
        ErrorPhase::Read,
        Some(profile.kind()),
        format!("query {} timeout", profile.product()),
    )
}

/// Il mismatch di righe di un update condizionale.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn conditional_update_mismatch(
    profile: &dyn crate::profile::ProductProfile,
    expected: u64,
    affected: u64,
) -> DatabaseError {
    let mut error = concurrent_modification_error(format!(
        "{}: expected {expected} affected rows, got {affected}",
        profile.product()
    ));
    error.provider = Some(profile.kind());
    error
}

fn closed_error(phase: ErrorPhase, kind: ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        phase,
        Some(kind),
        "MysqlTransaction già chiusa",
    )
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

    // ------------------------------------------------------------------
    //  decode_row: i metadati di colonna, non l'aspetto dei byte
    // ------------------------------------------------------------------

    use super::{decode_row, row_decode_error, BINARY_CHARACTER_SET};
    use mysql_async::consts::ColumnType;
    use mysql_async::Value;
    use plenora_database_core::plan::ProviderKind;
    use plenora_database_core::provider::ParameterValue;
    use std::sync::Arc;

    /// Character set non binario qualsiasi (utf8mb4).
    const UTF8MB4: u16 = 255;

    fn row_of(column_type: ColumnType, character_set: u16, value: Value) -> mysql_async::Row {
        let wire = Arc::new([mysql_async::Column::new(column_type)
            .with_name(b"payload")
            .with_character_set(character_set)]);
        mysql_common::row::new_row(vec![value], wire)
    }

    fn decode_one(column_type: ColumnType, character_set: u16, value: Value) -> ParameterValue {
        let names: Arc<[String]> = Arc::from(vec!["payload".to_owned()]);
        let row = decode_row(
            row_of(column_type, character_set, value),
            &names,
            ProviderKind::Mysql,
        )
        .expect("riga decodificabile");
        row.values().first().expect("un valore").clone()
    }

    /// Il caso che il decoder sbagliava: un BLOB i cui byte sono ASCII.
    ///
    /// Sono `Value::Bytes` come un TEXT, e formano UTF-8 valido: interpretarli
    /// come stringa era indistinguibile dal caso giusto finche non si
    /// guardava il character set.
    #[test]
    fn an_ascii_blob_stays_binary() {
        let value = decode_one(
            ColumnType::MYSQL_TYPE_BLOB,
            BINARY_CHARACTER_SET,
            Value::Bytes(b"PNG-like-ascii".to_vec()),
        );
        assert!(
            matches!(value, ParameterValue::Bytes(ref bytes) if bytes == b"PNG-like-ascii"),
            "un BLOB ASCII non deve diventare testo: {value:?}"
        );
    }

    /// La regressione che il charset da solo introduceva.
    ///
    /// `DECIMAL` viaggia come `Value::Bytes` **con charset binario** — come
    /// ogni tipo numerico — ma e una stringa di cifre, e l'intestazione di
    /// questo modulo promette che i tipi non nativi escano come stringhe
    /// UTF-8. Applicare il charset a ogni `Bytes` trasformava ogni DECIMAL in
    /// un blob: il charset distingue binario da testo solo per le famiglie che
    /// hanno entrambe le forme.
    #[test]
    fn a_decimal_stays_a_string_despite_the_binary_charset() {
        let value = decode_one(
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
            BINARY_CHARACTER_SET,
            Value::Bytes(b"12.34".to_vec()),
        );
        assert!(
            matches!(value, ParameterValue::String(ref text) if text == "12.34"),
            "un DECIMAL non deve diventare un blob: {value:?}"
        );
    }

    /// `BIT` e una maschera, non testo — nemmeno quando i byte lo sembrano.
    ///
    /// `0x41` e UTF-8 valido, quindi il fallback precedente lo consegnava come
    /// `"A"`; `0xff` no, e restava byte. La stessa colonna cambiava tipo
    /// pubblico in base al valore.
    #[test]
    fn a_bit_column_stays_binary_whatever_the_bytes_look_like() {
        for payload in [vec![0x41_u8], vec![0xff_u8]] {
            let value = decode_one(
                ColumnType::MYSQL_TYPE_BIT,
                BINARY_CHARACTER_SET,
                Value::Bytes(payload.clone()),
            );
            assert!(
                matches!(value, ParameterValue::Bytes(ref bytes) if *bytes == payload),
                "BIT {payload:?} non deve diventare testo: {value:?}"
            );
        }
    }

    /// Un WKB e byte anche quando per caso e UTF-8 valido.
    #[test]
    fn a_geometry_column_stays_binary() {
        let value = decode_one(
            ColumnType::MYSQL_TYPE_GEOMETRY,
            BINARY_CHARACTER_SET,
            Value::Bytes(b"AAAA".to_vec()),
        );
        assert!(matches!(value, ParameterValue::Bytes(_)), "{value:?}");
    }

    /// Un tipo wire non qualificato non viene indovinato.
    #[test]
    fn an_unqualified_wire_type_fails_closed() {
        let names: Arc<[String]> = Arc::from(vec!["payload".to_owned()]);
        let error = decode_row(
            row_of(
                // `MYSQL_TYPE_NULL` non ha una rappresentazione `Bytes`
                // sensata: e il rappresentante del caso "questo decoder non
                // sa cosa farne".
                ColumnType::MYSQL_TYPE_NULL,
                BINARY_CHARACTER_SET,
                Value::Bytes(vec![0x01]),
            ),
            &names,
            ProviderKind::Mysql,
        )
        .expect_err("tipo wire non qualificato");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn a_json_column_stays_a_string() {
        let value = decode_one(
            ColumnType::MYSQL_TYPE_JSON,
            UTF8MB4,
            Value::Bytes(br#"{"a":1}"#.to_vec()),
        );
        assert!(
            matches!(value, ParameterValue::String(ref text) if text == r#"{"a":1}"#),
            "{value:?}"
        );
    }

    #[test]
    fn a_text_column_stays_text() {
        let value = decode_one(
            ColumnType::MYSQL_TYPE_BLOB,
            UTF8MB4,
            Value::Bytes("però".as_bytes().to_vec()),
        );
        assert!(
            matches!(value, ParameterValue::String(ref text) if text == "però"),
            "{value:?}"
        );
    }

    /// L'altro verso: byte non UTF-8 su una colonna dichiarata testuale non
    /// degradano a blob in silenzio.
    #[test]
    fn a_text_column_with_invalid_utf8_is_an_error() {
        let names: Arc<[String]> = Arc::from(vec!["payload".to_owned()]);
        let error = decode_row(
            row_of(
                ColumnType::MYSQL_TYPE_VAR_STRING,
                UTF8MB4,
                Value::Bytes(vec![0xff, 0xfe]),
            ),
            &names,
            ProviderKind::Mysql,
        )
        .expect_err("byte non UTF-8 su colonna testuale");
        assert_eq!(error.category, ErrorCategory::DataMapping);
    }

    #[test]
    fn public_driver_decode_error_contains_only_the_column_position() {
        let error = row_decode_error(ProviderKind::Mysql, 7);
        assert_eq!(error.message, "decode colonna idx=7 fallito");
    }

    /// Una cella che il protocollo non ha consegnato non e un NULL.
    #[test]
    fn a_missing_cell_is_a_protocol_error() {
        // Due nomi attesi, una sola colonna sul filo.
        let names: Arc<[String]> = Arc::from(vec!["a".to_owned(), "b".to_owned()]);
        let error = decode_row(
            row_of(
                ColumnType::MYSQL_TYPE_LONGLONG,
                BINARY_CHARACTER_SET,
                Value::Int(1),
            ),
            &names,
            ProviderKind::Mysql,
        )
        .expect_err("il result set ha meno colonne dei nomi attesi");
        assert_eq!(error.category, ErrorCategory::Protocol);
        assert!(!error.is_retryable());
    }
}
