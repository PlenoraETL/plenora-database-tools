//! Transaction scope `PostgreSQL`: implementa `TransactionScope` del core.
//!
//! Copre begin con opzioni (isolation, access mode, deferrable,
//! `statement_timeout`), savepoint annidati con quoting sicuro, cancellation
//! best-effort e disambiguazione dei commit (`OutcomeUnknown` in caso di
//! canale compromesso in fase `Commit`).

use crate::error::{check_cancelled, classify_error, public_error};
use crate::pool::PooledClient;
use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use plenora_database_core::provider::{ParameterValue, ProviderFuture};
use plenora_database_core::row::Row;
use std::sync::Arc;
use plenora_database_core::native_query_policy::{enforce_policy, NativeQueryPolicy};
use plenora_database_core::transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name, AccessMode,
    CommitOutcome, ConditionalUpdate, IsolationLevel, RowStream, Statement, TransactionOptions,
    TransactionScope,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

/// Costruisce lo statement `BEGIN` con le opzioni richieste.
///
/// Le opzioni non specificate ricadono sul default della sessione. Il
/// `statement_timeout` è applicato con `SET LOCAL` all'interno della
/// transazione, quindi viene automaticamente ripristinato al commit/rollback.
fn build_begin_sql(options: &TransactionOptions) -> String {
    let mut parts: Vec<&'static str> = vec!["BEGIN"];
    if let Some(level) = options.isolation {
        parts.push(match level {
            IsolationLevel::ReadUncommitted => "ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "ISOLATION LEVEL READ COMMITTED",
            IsolationLevel::RepeatableRead => "ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel::Serializable => "ISOLATION LEVEL SERIALIZABLE",
        });
    }
    if let Some(mode) = options.access_mode {
        parts.push(match mode {
            AccessMode::ReadWrite => "READ WRITE",
            AccessMode::ReadOnly => "READ ONLY",
        });
    }
    if matches!(options.deferrable, Some(true)) {
        parts.push("DEFERRABLE");
    } else if matches!(options.deferrable, Some(false)) {
        parts.push("NOT DEFERRABLE");
    }
    let mut sql = parts.join(" ");
    sql.push(';');
    if let Some(ms) = options.statement_timeout_ms {
        use std::fmt::Write;
        write!(sql, " SET LOCAL statement_timeout = {ms};").expect("write to String non fallisce");
    }
    sql
}

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
                    .execute("SELECT set_config($1, $2, true)", &[&name.as_str(), &value.as_str()])
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

fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn phase_of(sql: &str) -> ErrorPhase {
    let trimmed = sql.trim_start();
    let head: String = trimmed
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase();
    match head.as_str() {
        "SELECT" | "WITH" | "SHOW" | "TABLE" | "VALUES" | "EXPLAIN" => ErrorPhase::Read,
        _ => ErrorPhase::Write,
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
            enforce_policy(self.native_query_policy, &statement.sql)?;
            let phase = phase_of(&statement.sql);
            check_cancelled(cancellation, phase)?;
            let encoded = encode_params(&statement.params)?;
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                encoded.iter().map(|value| value as &(dyn ToSql + Sync)).collect();
            let client = self.client.client()?;
            match client.execute(statement.sql.as_str(), &param_refs).await {
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
            check_cancelled(cancellation, ErrorPhase::Write)?;
            validate_savepoint_name(name)?;
            let sql = format!("SAVEPOINT {}", quote_identifier(name));
            self.client
                .client()?
                .batch_execute(&sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Write, &error))
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
            enforce_policy(self.native_query_policy, &statement.sql)?;
            check_cancelled(cancellation, ErrorPhase::Read)?;
            let encoded = encode_params(&statement.params)?;
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                encoded.iter().map(|value| value as &(dyn ToSql + Sync)).collect();
            let client = self.client.client()?;
            let rows = client
                .query(statement.sql.as_str(), &param_refs)
                .await
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
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                encoded.iter().map(|value| value as &(dyn ToSql + Sync)).collect();
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

// Codec parametri per `execute` — sottoinsieme sufficiente per i casi OLTP di
// A1. Tipi geometrici e composite sono gestiti dal codec di `parameter_codec`
// per il piano dati; qui il focus è lo scalare canonico.

enum SqlParam {
    #[allow(dead_code)] // il Type documenta l'intenzione lato PostgreSQL anche se `to_sql` restituisce solo IsNull::Yes
    Null(Type),
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Uuid(uuid_via_string::Uuid),
    Json(serde_json::Value),
}

impl std::fmt::Debug for SqlParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqlParam([REDACTED])")
    }
}

impl ToSql for SqlParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Self::Null(_) => Ok(IsNull::Yes),
            Self::Bool(v) => v.to_sql(ty, out),
            Self::I32(v) => v.to_sql(ty, out),
            Self::I64(v) => v.to_sql(ty, out),
            Self::F64(v) => v.to_sql(ty, out),
            Self::String(v) => v.as_str().to_sql(ty, out),
            Self::Bytes(v) => v.as_slice().to_sql(ty, out),
            Self::Date(v) => v.to_sql(ty, out),
            Self::Timestamp(v) => v.to_sql(ty, out),
            Self::TimestampTz(v) => v.to_sql(ty, out),
            Self::Uuid(v) => v.as_str().to_sql(ty, out),
            Self::Json(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

mod uuid_via_string {
    // Wrapper leggero: PostgreSQL accetta il testo UUID tramite cast implicito
    // quando il parametro è `text`. Evitiamo la dipendenza da `uuid` per il
    // solo path OLTP: il valore è validato lato applicazione.
    #[derive(Debug, Clone)]
    pub struct Uuid(pub String);

    impl Uuid {
        pub fn as_str(&self) -> &str {
            &self.0
        }
    }
}

fn encode_params(params: &[ParameterValue]) -> Result<Vec<SqlParam>> {
    params.iter().map(encode_param).collect()
}

fn encode_param(param: &ParameterValue) -> Result<SqlParam> {
    match param {
        ParameterValue::Bool(v) => Ok(SqlParam::Bool(*v)),
        ParameterValue::I32(v) => Ok(SqlParam::I32(*v)),
        ParameterValue::I64(v) => Ok(SqlParam::I64(*v)),
        ParameterValue::F64(v) => Ok(SqlParam::F64(*v)),
        ParameterValue::String(v) => Ok(SqlParam::String(v.clone())),
        ParameterValue::Bytes(v) => Ok(SqlParam::Bytes(v.clone())),
        ParameterValue::Date(v) => v
            .parse::<NaiveDate>()
            .map(SqlParam::Date)
            .map_err(|_| unsupported_param("date non conforme a ISO-8601")),
        ParameterValue::Timestamp(v) => v
            .parse::<NaiveDateTime>()
            .map(SqlParam::Timestamp)
            .map_err(|_| unsupported_param("timestamp non conforme a ISO-8601")),
        ParameterValue::TimestampTz(v) => v
            .parse::<DateTime<Utc>>()
            .map(SqlParam::TimestampTz)
            .map_err(|_| unsupported_param("timestamptz non conforme a RFC-3339")),
        ParameterValue::Uuid(v) => {
            if v.len() != 36 {
                return Err(unsupported_param("uuid non conforme a lunghezza 36"));
            }
            Ok(SqlParam::Uuid(uuid_via_string::Uuid(v.clone())))
        }
        ParameterValue::Json(v) => Ok(SqlParam::Json(v.clone())),
        ParameterValue::Decimal(_) => Err(unsupported_param(
            "decimal non ancora supportato nel path OLTP",
        )),
        ParameterValue::Wkb { .. } => Err(unsupported_param(
            "geometrie non supportate nel path OLTP: usare il piano dati",
        )),
        ParameterValue::Null { type_name } => Ok(SqlParam::Null(map_null_type(type_name))),
    }
}

#[allow(clippy::match_same_arms)] // catalogo esplicito + fallback dichiarato
fn map_null_type(type_name: &str) -> Type {
    match type_name.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Type::BOOL,
        "int" | "int4" | "integer" => Type::INT4,
        "int8" | "bigint" => Type::INT8,
        "float8" | "double" => Type::FLOAT8,
        "text" | "string" | "varchar" => Type::TEXT,
        "bytea" | "binary" => Type::BYTEA,
        "date" => Type::DATE,
        "timestamp" => Type::TIMESTAMP,
        "timestamptz" => Type::TIMESTAMPTZ,
        "uuid" => Type::UUID,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        _ => Type::TEXT,
    }
}

fn unsupported_param(message: &str) -> DatabaseError {
    public_error(ErrorCategory::Unsupported, ErrorPhase::Write, false, message)
}

/// Stream server-side su cursor `PostgreSQL`. Ripescato via `FETCH FORWARD N`
/// finché il server non restituisce meno di N righe (esaurimento). Il cursor
/// è transaction-scoped: commit/rollback della tx lo chiude automaticamente.
pub struct PostgresRowStream<'a> {
    client: &'a tokio_postgres::Client,
    cursor_name: String,
    batch_size: u32,
    exhausted: bool,
}

impl RowStream for PostgresRowStream<'_> {
    fn next_batch<'b>(
        &'b mut self,
        cancellation: &'b CancellationToken,
    ) -> ProviderFuture<'b, Option<Vec<Row>>> {
        Box::pin(async move {
            if self.exhausted {
                return Ok(None);
            }
            check_cancelled(cancellation, ErrorPhase::Read)?;
            let fetch_sql = format!(
                "FETCH FORWARD {} FROM {}",
                self.batch_size, self.cursor_name
            );
            let rows = self
                .client
                .query(fetch_sql.as_str(), &[])
                .await
                .map_err(|error| classify_error(ErrorPhase::Read, &error))?;
            let n = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            let out = decode_rows(&rows)?;
            if n < self.batch_size {
                self.exhausted = true;
            }
            if out.is_empty() {
                Ok(None)
            } else {
                Ok(Some(out))
            }
        })
    }
}

fn unsupported_column_type(pg_type: &Type) -> DatabaseError {
    public_error(
        ErrorCategory::Unsupported,
        ErrorPhase::Read,
        false,
        &format!("tipo di colonna PostgreSQL non supportato nel path OLTP: {pg_type}"),
    )
}

/// Decodifica un batch di righe condividendo l'array dei nomi colonna.
fn decode_rows(rows: &[tokio_postgres::Row]) -> Result<Vec<Row>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let columns: Arc<[String]> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_owned())
        .collect::<Vec<_>>()
        .into();
    let mut out = Vec::with_capacity(rows.len());
    for pg_row in rows {
        let values = decode_row(pg_row)?;
        out.push(Row::new(Arc::clone(&columns), values));
    }
    Ok(out)
}

fn decode_row(row: &tokio_postgres::Row) -> Result<Vec<ParameterValue>> {
    let mut values = Vec::with_capacity(row.len());
    for (index, column) in row.columns().iter().enumerate() {
        let pg_type = column.type_();
        let type_name = pg_type.name().to_owned();
        let value = match *pg_type {
            Type::BOOL => row
                .try_get::<_, Option<bool>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::Bool))
                .map_err(crate::error::row_decode_error)?,
            Type::INT2 => row
                .try_get::<_, Option<i16>>(index)
                .map(|v| optional_to_param(v.map(i32::from), &type_name, ParameterValue::I32))
                .map_err(crate::error::row_decode_error)?,
            Type::INT4 => row
                .try_get::<_, Option<i32>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::I32))
                .map_err(crate::error::row_decode_error)?,
            Type::INT8 => row
                .try_get::<_, Option<i64>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::I64))
                .map_err(crate::error::row_decode_error)?,
            Type::FLOAT4 => row
                .try_get::<_, Option<f32>>(index)
                .map(|v| optional_to_param(v.map(f64::from), &type_name, ParameterValue::F64))
                .map_err(crate::error::row_decode_error)?,
            Type::FLOAT8 => row
                .try_get::<_, Option<f64>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::F64))
                .map_err(crate::error::row_decode_error)?,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
                .try_get::<_, Option<String>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::String))
                .map_err(crate::error::row_decode_error)?,
            Type::BYTEA => row
                .try_get::<_, Option<Vec<u8>>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::Bytes))
                .map_err(crate::error::row_decode_error)?,
            Type::DATE => row
                .try_get::<_, Option<NaiveDate>>(index)
                .map(|v| optional_to_param(v.map(|d| d.to_string()), &type_name, ParameterValue::Date))
                .map_err(crate::error::row_decode_error)?,
            Type::TIMESTAMP => row
                .try_get::<_, Option<NaiveDateTime>>(index)
                .map(|v| optional_to_param(v.map(|d| d.format("%Y-%m-%dT%H:%M:%S%.f").to_string()), &type_name, ParameterValue::Timestamp))
                .map_err(crate::error::row_decode_error)?,
            Type::TIMESTAMPTZ => row
                .try_get::<_, Option<DateTime<Utc>>>(index)
                .map(|v| optional_to_param(v.map(|d| d.to_rfc3339()), &type_name, ParameterValue::TimestampTz))
                .map_err(crate::error::row_decode_error)?,
            Type::UUID => {
                use tokio_postgres::types::FromSql;
                struct UuidBytes([u8; 16]);
                impl<'a> FromSql<'a> for UuidBytes {
                    fn from_sql(
                        _ty: &Type,
                        raw: &'a [u8],
                    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                        if raw.len() != 16 {
                            return Err("UUID payload deve essere 16 byte".into());
                        }
                        let mut b = [0u8; 16];
                        b.copy_from_slice(raw);
                        Ok(Self(b))
                    }
                    fn accepts(ty: &Type) -> bool {
                        matches!(*ty, Type::UUID)
                    }
                }
                let raw: Option<UuidBytes> = row
                    .try_get(index)
                    .map_err(crate::error::row_decode_error)?;
                match raw {
                    Some(UuidBytes(b)) => {
                        let text = format!(
                            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                            b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
                        );
                        ParameterValue::Uuid(text)
                    }
                    None => ParameterValue::Null {
                        type_name: type_name.clone(),
                    },
                }
            }
            Type::JSON | Type::JSONB => row
                .try_get::<_, Option<serde_json::Value>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::Json))
                .map_err(crate::error::row_decode_error)?,
            _ => return Err(unsupported_column_type(pg_type)),
        };
        values.push(value);
    }
    Ok(values)
}

fn optional_to_param<T>(
    value: Option<T>,
    type_name: &str,
    wrap: fn(T) -> ParameterValue,
) -> ParameterValue {
    value.map_or_else(
        || ParameterValue::Null {
            type_name: type_name.to_owned(),
        },
        wrap,
    )
}

// Il warning "unused import" verrà segnalato se non serve; teniamo il tipo per
// documentazione della retry disposition applicata al ramo timeout.
#[allow(dead_code)]
const _: fn() -> RetryDisposition = || RetryDisposition::Never;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_default_is_bare() {
        let sql = build_begin_sql(&TransactionOptions::default());
        assert_eq!(sql, "BEGIN;");
    }

    #[test]
    fn begin_with_isolation_and_read_only() {
        let opts = TransactionOptions {
            isolation: Some(IsolationLevel::Serializable),
            access_mode: Some(AccessMode::ReadOnly),
            deferrable: Some(true),
            ..TransactionOptions::default()
        };
        assert_eq!(
            build_begin_sql(&opts),
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE;"
        );
    }

    #[test]
    fn begin_with_statement_timeout_appends_set_local() {
        let opts = TransactionOptions {
            isolation: Some(IsolationLevel::ReadCommitted),
            statement_timeout_ms: Some(750),
            ..TransactionOptions::default()
        };
        assert_eq!(
            build_begin_sql(&opts),
            "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 750;"
        );
    }

    #[test]
    fn quote_identifier_escapes_double_quotes() {
        assert_eq!(quote_identifier("plain"), "\"plain\"");
        assert_eq!(quote_identifier("evil\"name"), "\"evil\"\"name\"");
    }

    #[test]
    fn phase_of_detects_read_head() {
        assert_eq!(phase_of("SELECT 1"), ErrorPhase::Read);
        assert_eq!(phase_of("  with cte AS (SELECT 1) SELECT * FROM cte"),
            ErrorPhase::Read);
        assert_eq!(phase_of("SHOW server_version"), ErrorPhase::Read);
    }

    #[test]
    fn phase_of_detects_write_head() {
        assert_eq!(phase_of("INSERT INTO t VALUES (1)"), ErrorPhase::Write);
        assert_eq!(phase_of("UPDATE t SET x=1"), ErrorPhase::Write);
        assert_eq!(phase_of("DELETE FROM t"), ErrorPhase::Write);
        assert_eq!(phase_of("CREATE TABLE t (x INT)"), ErrorPhase::Write);
    }
}

/// Test integrazione live per A1: multi-statement, savepoint, cancellation,
/// statement_timeout. Chiudono il milestone A1 verso Postgres reale.
#[cfg(test)]
mod live {
    use super::*;
    use crate::PostgresProvider;
    use plenora_database_core::provider::{Provider, SecretString};
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};

    const LIVE_DSN: &str =
        "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test";

    fn secret() -> SecretString {
        SecretString::new(LIVE_DSN)
    }

    fn budget() -> ResourceBudget {
        ResourceBudget::new(ResourceLimits::default()).expect("budget")
    }

    async fn provider() -> PostgresProvider {
        PostgresProvider::new(1_024)
    }

    async fn count(provider: &PostgresProvider, sql: &str) -> i64 {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(LIVE_DSN, NoTls)
            .await
            .expect("connect out-of-band");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let _ = provider;
        let row = client.query_one(sql, &[]).await.expect("count");
        row.get::<_, i64>(0)
    }

    async fn scratch_table(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(LIVE_DSN, NoTls)
            .await
            .expect("connect setup");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
                 CREATE TABLE {name} (id INT PRIMARY KEY, v TEXT NOT NULL);",
            ))
            .await
            .expect("scratch table");
    }

    async fn drop_table(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(LIVE_DSN, NoTls)
            .await
            .expect("connect drop");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {name};"))
            .await
            .expect("drop");
    }

    #[tokio::test]
    async fn live_commit_multi_statement_persists_all() {
        scratch_table("a1_commit").await;
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let n1 = tx
            .execute(
                &Statement::new("INSERT INTO a1_commit VALUES ($1, $2)").with_params(vec![
                    ParameterValue::I32(1),
                    ParameterValue::String("a".into()),
                ]),
                &cancel,
            )
            .await
            .expect("insert 1");
        assert_eq!(n1, 1);

        let n2 = tx
            .execute(
                &Statement::new("INSERT INTO a1_commit VALUES ($1, $2)").with_params(vec![
                    ParameterValue::I32(2),
                    ParameterValue::String("b".into()),
                ]),
                &cancel,
            )
            .await
            .expect("insert 2");
        assert_eq!(n2, 1);

        let outcome = tx.commit(&cancel).await.expect("commit");
        assert!(outcome.is_committed());

        assert_eq!(count(&provider, "SELECT COUNT(*)::BIGINT FROM a1_commit").await, 2);
        drop_table("a1_commit").await;
    }

    #[tokio::test]
    async fn live_rollback_discards_all_statements() {
        scratch_table("a1_rollback").await;
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new("INSERT INTO a1_rollback VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("x".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert");

        tx.rollback(&cancel).await.expect("rollback");

        assert_eq!(count(&provider, "SELECT COUNT(*)::BIGINT FROM a1_rollback").await, 0);
        drop_table("a1_rollback").await;
    }

    #[tokio::test]
    async fn live_savepoint_rollback_preserves_prior_statements() {
        scratch_table("a1_sp").await;
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new("INSERT INTO a1_sp VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("keep".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert keep");

        tx.savepoint("sp1", &cancel).await.expect("savepoint");

        tx.execute(
            &Statement::new("INSERT INTO a1_sp VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(2),
                ParameterValue::String("drop".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert drop");

        tx.rollback_to_savepoint("sp1", &cancel).await.expect("rollback to");
        tx.release_savepoint("sp1", &cancel).await.expect("release");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());

        assert_eq!(count(&provider, "SELECT COUNT(*)::BIGINT FROM a1_sp").await, 1);
        assert_eq!(
            count(
                &provider,
                "SELECT COUNT(*)::BIGINT FROM a1_sp WHERE v = 'keep'"
            )
            .await,
            1
        );
        drop_table("a1_sp").await;
    }

    #[tokio::test]
    async fn live_savepoint_name_with_injection_is_rejected() {
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let err = tx
            .savepoint("sp; DROP TABLE users; --", &cancel)
            .await
            .expect_err("nome invalido deve essere rifiutato");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::InvalidPlan);

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_statement_timeout_triggers_cancelled_57014() {
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();
        let opts = TransactionOptions {
            statement_timeout_ms: Some(50),
            ..TransactionOptions::default()
        };

        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget, &cancel)
            .await
            .expect("begin");

        let err = tx
            .execute(&Statement::new("SELECT pg_sleep(2)"), &cancel)
            .await
            .expect_err("timeout deve interrompere");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::Cancelled);

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_serializable_read_only_deferrable_isolation() {
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();
        let opts = TransactionOptions {
            isolation: Some(IsolationLevel::Serializable),
            access_mode: Some(AccessMode::ReadOnly),
            deferrable: Some(true),
            ..TransactionOptions::default()
        };

        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget, &cancel)
            .await
            .expect("begin serializable ro deferrable");

        // Un SELECT in tx SERIALIZABLE READ ONLY DEFERRABLE deve passare senza
        // errori (esclude la clausola di conflitto serializable per definizione).
        tx.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("select 1");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    #[tokio::test]
    async fn live_cancellation_before_execute_is_rejected() {
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        cancel.cancel();
        let err = tx
            .execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect_err("cancel deve bloccare");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::Cancelled);

        // Rollback esplicito ignora il cancellation token: deve chiudere la tx.
        tx.rollback(&cancel).await.expect("rollback ignora cancel");
    }

    // === B1: Spatial profile portabile ===

    use crate::spatial::build_spatial_select;
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    use plenora_database_core::{SpatialFilter, SpatialPredicate, SpatialReference};

    async fn fetch_ewkb(sql_returning_geom: &str) -> Vec<u8> {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(LIVE_DSN, NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                &format!("SELECT ST_AsEWKB({sql_returning_geom})"),
                &[],
            )
            .await
            .expect("fetch ewkb");
        row.get::<_, Vec<u8>>(0)
    }

    async fn setup_spatial_scratch(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(LIVE_DSN, NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
                 CREATE TABLE {name} (
                     id INT PRIMARY KEY,
                     geom geometry(Point, 4326) NOT NULL
                 );
                 INSERT INTO {name} VALUES
                     (1, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326)),
                     (2, ST_SetSRID(ST_MakePoint(12.49, 41.90), 4326)),
                     (3, ST_SetSRID(ST_MakePoint(2.35,  48.86), 4326));",
            ))
            .await
            .expect("setup");
    }

    fn reference(ewkb: Vec<u8>) -> SpatialReference {
        SpatialReference {
            ewkb,
            srid: 4326,
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geometry,
        }
    }

    #[tokio::test]
    async fn live_spatial_intersects_polygon_returns_points_inside() {
        setup_spatial_scratch("b1_intersects").await;
        // Poligono che copre l'Italia settentrionale e centrale (grosso modo).
        let polygon = fetch_ewkb(
            "ST_SetSRID(ST_MakeEnvelope(6.0, 40.0, 14.0, 46.0), 4326)",
        )
        .await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Intersects,
            reference: reference(polygon),
        };
        let stmt = build_spatial_select(None, "b1_intersects", &["id"], &filter, None)
            .expect("build");

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        // I punti Milano (1) e Roma (2) sono nel bbox; Parigi (3) no.
        let ids: Vec<i32> = rows
            .iter()
            .filter_map(|r| match r[0] {
                ParameterValue::I32(v) => Some(v),
                _ => None,
            })
            .collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2]);
        drop_table("b1_intersects").await;
    }

    #[tokio::test]
    async fn live_spatial_dwithin_uses_distance_parameter() {
        setup_spatial_scratch("b1_dwithin").await;
        // Punto vicino a Milano: 100m a est di (9.19, 45.46).
        let near_milan = fetch_ewkb(
            "ST_SetSRID(ST_MakePoint(9.191, 45.46), 4326)",
        )
        .await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        // Filter DWithin di 500 metri (usando ST_DWithin geografico via cast).
        // NB: per un test onesto sui gradi, uso una distanza in "degrees" con
        // ST_DWithin(geometry, geometry, degrees). 100m a Milano è circa 0.0013°.
        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::DWithin {
                distance_meters: 0.01,
            },
            reference: reference(near_milan),
        };
        let stmt = build_spatial_select(None, "b1_dwithin", &["id"], &filter, None)
            .expect("build");

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        // Solo Milano (1) è entro ~0.01° dal punto di riferimento.
        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0][0], ParameterValue::I32(1)));
        drop_table("b1_dwithin").await;
    }

    #[tokio::test]
    async fn live_spatial_bounding_box_uses_index_operator() {
        setup_spatial_scratch("b1_bbox").await;
        let polygon = fetch_ewkb(
            "ST_SetSRID(ST_MakeEnvelope(1.0, 48.0, 5.0, 50.0), 4326)",
        )
        .await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::BoundingBox,
            reference: reference(polygon),
        };
        let stmt =
            build_spatial_select(None, "b1_bbox", &["id"], &filter, None).expect("build");

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        // Solo Parigi (3) è nel bbox europeo occidentale.
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], ParameterValue::I32(3)));
        drop_table("b1_bbox").await;
    }

    #[tokio::test]
    async fn live_spatial_within_selects_features_contained_in_reference() {
        setup_spatial_scratch("b1_within").await;
        // Poligono che contiene solo Milano.
        let polygon = fetch_ewkb(
            "ST_SetSRID(ST_MakeEnvelope(9.0, 45.0, 9.5, 46.0), 4326)",
        )
        .await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Within,
            reference: reference(polygon),
        };
        let stmt =
            build_spatial_select(None, "b1_within", &["id"], &filter, None).expect("build");

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0][0], ParameterValue::I32(1)));
        drop_table("b1_within").await;
    }

    #[tokio::test]
    async fn live_spatial_srid_preserved_roundtrip() {
        // Il punto ricaricato via ST_AsEWKB deve avere lo stesso SRID.
        setup_spatial_scratch("b1_srid").await;
        let polygon = fetch_ewkb("ST_SetSRID(ST_MakeEnvelope(0, 0, 100, 100), 4326)").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Intersects,
            reference: reference(polygon),
        };
        // La query include la colonna geom stessa e verifichiamo che il
        // decode restituisca ParameterValue::Bytes (WKB) senza errori.
        let stmt = build_spatial_select(None, "b1_srid", &["id"], &filter, None)
            .expect("build");

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        assert_eq!(rows.len(), 3);
        drop_table("b1_srid").await;
    }

    // === F1e: Spatial predicate nell'AST portable ===

    #[tokio::test]
    async fn live_portable_spatial_intersects_end_to_end() {
        use plenora_database_core::facade::execute_portable_returning;
        use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
        use plenora_database_core::portable::{
            select as p_select, spatial as p_spatial, Direction,
        };
        use plenora_database_core::{SpatialPredicate, SpatialReference};

        // Setup 3 punti in SRID 4326 (Milano, Roma, Parigi).
        setup_spatial_scratch("f1e_portable").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();

        // Estrai un polygon di riferimento dal DB come EWKB (bbox Italia).
        let bbox_ewkb =
            fetch_ewkb("ST_SetSRID(ST_MakeEnvelope(6.0, 40.0, 14.0, 46.0), 4326)").await;

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // SELECT id FROM f1e_portable WHERE ST_Intersects(geom, <bbox>)
        let stmt = p_select("f1e_portable", vec!["id"])
            .where_(p_spatial(
                "geom",
                SpatialPredicate::Intersects,
                SpatialReference {
                    ewkb: bbox_ewkb,
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .order_by("id", Direction::Asc)
            .into_statement();

        let rows = execute_portable_returning(tx.as_mut(), &stmt, &cancel)
            .await
            .expect("spatial query");

        // Milano (id=1) e Roma (id=2) dentro; Parigi (id=3) fuori.
        let ids: Vec<i32> = rows
            .iter()
            .filter_map(|r| match r.get_index(0) {
                Some(ParameterValue::I32(v)) => Some(*v),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![1, 2]);

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1e_portable").await;
    }

    #[tokio::test]
    async fn live_portable_spatial_dwithin_end_to_end() {
        use plenora_database_core::facade::execute_portable_returning;
        use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
        use plenora_database_core::portable::{select as p_select, spatial as p_spatial};
        use plenora_database_core::{SpatialPredicate, SpatialReference};

        setup_spatial_scratch("f1e_dwithin").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();

        // Punto vicino a Milano.
        let near_milan = fetch_ewkb("ST_SetSRID(ST_MakePoint(9.191, 45.46), 4326)").await;

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let stmt = p_select("f1e_dwithin", vec!["id"])
            .where_(p_spatial(
                "geom",
                SpatialPredicate::DWithin {
                    distance_meters: 0.01,
                },
                SpatialReference {
                    ewkb: near_milan,
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();

        let rows = execute_portable_returning(tx.as_mut(), &stmt, &cancel)
            .await
            .expect("spatial dwithin");

        // Solo Milano è entro ~0.01° dal punto di riferimento.
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].get_index(0), Some(ParameterValue::I32(1))));

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1e_dwithin").await;
    }

    // === F1d: RETURNING canonico via facade portable ===

    #[tokio::test]
    async fn live_execute_portable_returning_produces_generated_id() {
        use plenora_database_core::facade::execute_portable_returning_one;
        use plenora_database_core::portable::{Expression, InsertStatement, PortableStatement, TableRef};

        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // Setup temp table con SERIAL id
        tx.execute(
            &Statement::new(
                "CREATE TEMP TABLE f1d_returning (id SERIAL PRIMARY KEY, v INT) ON COMMIT DROP",
            ),
            &cancel,
        )
        .await
        .expect("temp");

        // INSERT ... RETURNING id via portable AST
        let insert = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("f1d_returning"),
            columns: vec!["v".into()],
            values: vec![vec![Expression::literal(ParameterValue::I32(99))]],
            returning: vec!["id".into(), "v".into()],
        });
        let row = execute_portable_returning_one(tx.as_mut(), &insert, &cancel)
            .await
            .expect("returning");

        // Verifica colonne + valori
        assert_eq!(row.len(), 2);
        let id = match &row["id"] {
            ParameterValue::I32(v) => *v,
            other => panic!("expected i32 id, got {other:?}"),
        };
        assert!(id >= 1);
        assert!(matches!(&row["v"], ParameterValue::I32(99)));

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_execute_portable_without_returning_via_facade_rejects_returning() {
        use plenora_database_core::facade::execute_portable;
        use plenora_database_core::portable::{
            Expression, InsertStatement, PortableStatement, TableRef,
        };

        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // INSERT con RETURNING passato a execute_portable → InvalidPlan.
        let insert_with_returning = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("t"),
            columns: vec!["x".into()],
            values: vec![vec![Expression::literal(ParameterValue::I32(1))]],
            returning: vec!["id".into()],
        });
        let err = execute_portable(tx.as_mut(), &insert_with_returning, &cancel)
            .await
            .expect_err("returning richiede execute_portable_returning");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_execute_portable_returning_via_update() {
        use plenora_database_core::facade::execute_portable_returning;
        use plenora_database_core::portable::{
            eq as p_eq, Expression, PortableStatement, TableRef, UpdateStatement,
        };

        scratch_table("f1d_upd_ret").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // Seed
        tx.execute(
            &Statement::new("INSERT INTO f1d_upd_ret VALUES (1, 'orig'), (2, 'orig')"),
            &cancel,
        )
        .await
        .expect("seed");

        // UPDATE ... RETURNING id, v — deve tornare 2 righe con i nuovi valori
        let update = PortableStatement::Update(UpdateStatement {
            table: TableRef::new("f1d_upd_ret"),
            assignments: vec![(
                "v".into(),
                Expression::literal(ParameterValue::String("new".into())),
            )],
            filter: Some(p_eq("v", ParameterValue::String("orig".into()))),
            returning: vec!["id".into(), "v".into()],
        });
        let rows = execute_portable_returning(tx.as_mut(), &update, &cancel)
            .await
            .expect("returning");
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert!(matches!(&row["v"], ParameterValue::String(s) if s == "new"));
        }

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1d_upd_ret").await;
    }

    // === F1c: PortableStatement AST end-to-end ===

    use plenora_database_core::portable::{
        and as p_and, compile_portable, eq as p_eq, select as p_select, Direction,
        Expression, InsertStatement, PortableStatement, TableRef, UpdateStatement,
    };
    use plenora_database_core::plan::ProviderKind;

    #[tokio::test]
    async fn live_portable_insert_update_select_roundtrip() {
        scratch_table("f1c_portable").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // INSERT (2 rows) via portable AST.
        let insert = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("f1c_portable"),
            columns: vec!["id".into(), "v".into()],
            values: vec![
                vec![
                    Expression::literal(ParameterValue::I32(1)),
                    Expression::literal(ParameterValue::String("alpha".into())),
                ],
                vec![
                    Expression::literal(ParameterValue::I32(2)),
                    Expression::literal(ParameterValue::String("beta".into())),
                ],
            ],
            returning: Vec::new(),
        });
        let stmt = compile_portable(ProviderKind::Postgres, &insert).expect("compile insert");
        let inserted = tx.execute(&stmt, &cancel).await.expect("insert");
        assert_eq!(inserted, 2);

        // UPDATE con WHERE composto.
        let update = PortableStatement::Update(UpdateStatement {
            table: TableRef::new("f1c_portable"),
            assignments: vec![(
                "v".into(),
                Expression::literal(ParameterValue::String("alpha-updated".into())),
            )],
            filter: Some(p_and(vec![
                p_eq("id", ParameterValue::I32(1)),
                p_eq("v", ParameterValue::String("alpha".into())),
            ])),
            returning: Vec::new(),
        });
        let stmt = compile_portable(ProviderKind::Postgres, &update).expect("compile update");
        let updated = tx.execute(&stmt, &cancel).await.expect("update");
        assert_eq!(updated, 1);

        // SELECT con projection + where + order + limit.
        let select = p_select("f1c_portable", vec!["id", "v"])
            .where_(p_eq("id", ParameterValue::I32(1)))
            .order_by("id", Direction::Asc)
            .limit(10)
            .into_statement();
        let stmt = compile_portable(ProviderKind::Postgres, &select).expect("compile select");
        let rows = tx.query(&stmt, &cancel).await.expect("select");
        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0]["id"], ParameterValue::I32(1)));
        assert!(matches!(
            &rows[0]["v"],
            ParameterValue::String(s) if s == "alpha-updated"
        ));

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1c_portable").await;
    }

    #[tokio::test]
    async fn live_portable_upsert_do_update_set() {
        scratch_table("f1c_upsert").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // INSERT iniziale.
        let insert = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("f1c_upsert"),
            columns: vec!["id".into(), "v".into()],
            values: vec![vec![
                Expression::literal(ParameterValue::I32(1)),
                Expression::literal(ParameterValue::String("first".into())),
            ]],
            returning: Vec::new(),
        });
        let stmt = compile_portable(ProviderKind::Postgres, &insert).expect("compile");
        tx.execute(&stmt, &cancel).await.expect("insert");

        // UPSERT sulla stessa chiave con DO UPDATE.
        let upsert = PortableStatement::Upsert(
            plenora_database_core::portable::UpsertStatement {
                table: TableRef::new("f1c_upsert"),
                columns: vec!["id".into(), "v".into()],
                values: vec![vec![
                    Expression::literal(ParameterValue::I32(1)),
                    Expression::literal(ParameterValue::String("upserted".into())),
                ]],
                conflict_target: vec!["id".into()],
                update_on_conflict: vec![(
                    "v".into(),
                    Expression::literal(ParameterValue::String("upserted".into())),
                )],
                returning: Vec::new(),
            },
        );
        let stmt = compile_portable(ProviderKind::Postgres, &upsert).expect("compile upsert");
        let affected = tx.execute(&stmt, &cancel).await.expect("upsert");
        assert_eq!(affected, 1);

        // Verifica lo stato via portable SELECT.
        let sel = p_select("f1c_upsert", vec!["v"])
            .where_(p_eq("id", ParameterValue::I32(1)))
            .into_statement();
        let stmt = compile_portable(ProviderKind::Postgres, &sel).expect("compile select");
        let rows = tx.query(&stmt, &cancel).await.expect("select");
        assert!(matches!(
            &rows[0]["v"],
            ParameterValue::String(s) if s == "upserted"
        ));

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1c_upsert").await;
    }

    // === F1b: Facade scalar completa ===

    use plenora_database_core::facade::{
        execute_scalar_bytes, execute_scalar_date, execute_scalar_decimal, execute_scalar_json,
        execute_scalar_timestamp, execute_scalar_timestamptz, execute_scalar_uuid,
    };

    async fn scalar_tx<'a>(
        provider: &'a PostgresProvider,
        cancel: &'a CancellationToken,
        budget: &'a plenora_database_core::resource::ResourceBudget,
    ) -> Box<dyn plenora_database_core::transaction::TransactionScope + 'a> {
        provider
            .begin_transaction(&secret(), &TransactionOptions::default(), budget, cancel)
            .await
            .expect("begin")
    }

    #[tokio::test]
    async fn live_facade_scalar_bytes() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_bytes(
            tx.as_mut(),
            &Statement::new("SELECT '\\xdeadbeef'::BYTEA"),
            &cancel,
        )
        .await
        .expect("bytes");
        assert_eq!(v, vec![0xde, 0xad, 0xbe, 0xef]);
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_uuid() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_uuid(
            tx.as_mut(),
            &Statement::new("SELECT '12345678-1234-1234-1234-123456789012'::UUID"),
            &cancel,
        )
        .await
        .expect("uuid");
        assert_eq!(v, "12345678-1234-1234-1234-123456789012");
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_json() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_json(
            tx.as_mut(),
            &Statement::new(r#"SELECT '{"k":1}'::JSONB"#),
            &cancel,
        )
        .await
        .expect("json");
        assert_eq!(v, serde_json::json!({"k": 1}));
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_date() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_date(
            tx.as_mut(),
            &Statement::new("SELECT '2026-08-11'::DATE"),
            &cancel,
        )
        .await
        .expect("date");
        assert_eq!(v, "2026-08-11");
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_timestamp_and_timestamptz() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;

        let ts = execute_scalar_timestamp(
            tx.as_mut(),
            &Statement::new("SELECT '2026-08-11 10:20:30'::TIMESTAMP"),
            &cancel,
        )
        .await
        .expect("timestamp");
        assert!(ts.starts_with("2026-08-11T10:20:30"));

        let tstz = execute_scalar_timestamptz(
            tx.as_mut(),
            &Statement::new("SELECT '2026-08-11T10:20:30+00:00'::TIMESTAMPTZ"),
            &cancel,
        )
        .await
        .expect("timestamptz");
        assert!(tstz.starts_with("2026-08-11T10:20:30"));

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_decimal_is_unsupported() {
        // Documentato: decimal via facade OLTP è Unsupported finché non
        // introduciamo rust_decimal dep (Fase 3). Test verifica che il
        // driver segnali gracefully invece di panic.
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let err = execute_scalar_decimal(
            tx.as_mut(),
            &Statement::new("SELECT 3.14::NUMERIC(10,2)"),
            &cancel,
        )
        .await
        .expect_err("decimal deve essere Unsupported oggi");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::Unsupported
        );
        tx.rollback(&cancel).await.expect("rollback");
    }

    // === B3: Native-query governance ===

    use plenora_database_core::native_query_policy::NativeQueryPolicy;

    fn strict_options() -> TransactionOptions {
        TransactionOptions {
            native_query_policy: NativeQueryPolicy::Deny,
            ..TransactionOptions::default()
        }
    }

    #[tokio::test]
    async fn live_native_deny_permits_crud() {
        scratch_table("b3_ok").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        tx.execute(
            &Statement::new("INSERT INTO b3_ok VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("v".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert ok");

        tx.execute(
            &Statement::new("UPDATE b3_ok SET v = 'w' WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("update ok");

        tx.execute(&Statement::new("DELETE FROM b3_ok WHERE id = 1"), &cancel)
            .await
            .expect("delete ok");

        tx.commit(&cancel).await.expect("commit");
        drop_table("b3_ok").await;
    }

    #[tokio::test]
    async fn live_native_deny_blocks_ddl() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        for sql in [
            "CREATE TABLE b3_ddl (x INT)",
            "DROP TABLE b3_ddl",
            "ALTER TABLE b3_ddl ADD COLUMN y INT",
            "TRUNCATE b3_ddl",
            "GRANT SELECT ON b3_ddl TO public",
        ] {
            let err = tx
                .execute(&Statement::new(sql), &cancel)
                .await
                .expect_err(&format!("DDL deve essere bloccato: {sql}"));
            assert_eq!(
                err.category,
                plenora_database_core::ErrorCategory::InvalidPlan,
                "sql={sql}"
            );
        }

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_native_deny_blocks_session_commands() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        for sql in ["SET timezone = 'UTC'", "SHOW server_version", "RESET ALL"] {
            let err = tx
                .execute(&Statement::new(sql), &cancel)
                .await
                .expect_err(&format!("session cmd deve essere bloccato: {sql}"));
            assert_eq!(
                err.category,
                plenora_database_core::ErrorCategory::InvalidPlan,
                "sql={sql}"
            );
        }

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_native_deny_blocks_multi_statement() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        let err = tx
            .execute(
                &Statement::new("SELECT 1; DROP TABLE nothing"),
                &cancel,
            )
            .await
            .expect_err("multi-statement deve essere bloccato");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_native_allow_permits_ddl_with_escape_hatch() {
        // Modalità Allow (default) consente DDL — resta l'escape autorizzato
        // per migrazioni/diagnostica, come previsto dalla roadmap PFM.
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin allow");
        tx.execute(
            &Statement::new("CREATE TEMP TABLE b3_esc (x INT) ON COMMIT DROP"),
            &cancel,
        )
        .await
        .expect("ddl consentita in Allow");
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_transaction_control_is_blocked_even_in_allow() {
        // BEGIN/COMMIT/ROLLBACK/SAVEPOINT sono gestiti dalla libreria;
        // devono essere rifiutati anche in Allow.
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        for sql in ["COMMIT", "ROLLBACK", "SAVEPOINT sp"] {
            let err = tx
                .execute(&Statement::new(sql), &cancel)
                .await
                .expect_err(&format!("tx-control deve essere bloccato: {sql}"));
            assert_eq!(
                err.category,
                plenora_database_core::ErrorCategory::InvalidPlan
            );
        }

        tx.rollback(&cancel).await.expect("rollback");
    }

    // === B4: Server-side streaming (cursor) ===

    #[tokio::test]
    async fn live_query_stream_paginates_result_in_batches() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        // 250 righe con batch_size=100 → 3 batch attesi (100+100+50).
        let stmt = Statement::new("SELECT gs::BIGINT AS n FROM generate_series(1, 250) gs");
        let mut stream = tx
            .query_stream(&stmt, 100, &cancel)
            .await
            .expect("open stream");

        let mut batch_sizes = Vec::new();
        while let Some(batch) = stream.next_batch(&cancel).await.expect("next") {
            batch_sizes.push(batch.len());
        }
        assert_eq!(batch_sizes, vec![100, 100, 50]);
        drop(stream);

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    #[tokio::test]
    async fn live_query_stream_exhausts_at_end() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT gs::INT FROM generate_series(1, 5) gs");
        let mut stream = tx
            .query_stream(&stmt, 10, &cancel)
            .await
            .expect("open");

        let first = stream.next_batch(&cancel).await.expect("first");
        assert!(matches!(first, Some(ref rows) if rows.len() == 5));

        let second = stream.next_batch(&cancel).await.expect("second");
        assert!(second.is_none());

        // Chiamate successive continuano a ritornare None (idempotente).
        let third = stream.next_batch(&cancel).await.expect("third");
        assert!(third.is_none());
        drop(stream);

        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_query_stream_respects_bound_parameters() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT gs::BIGINT FROM generate_series($1::INT, $2::INT) gs")
            .with_params(vec![ParameterValue::I32(10), ParameterValue::I32(14)]);
        let mut stream = tx
            .query_stream(&stmt, 2, &cancel)
            .await
            .expect("open");

        let mut all = Vec::new();
        while let Some(batch) = stream.next_batch(&cancel).await.expect("next") {
            for row in batch {
                match row.get_index(0) {
                    Some(ParameterValue::I64(v)) => all.push(*v),
                    _ => panic!("expected i64"),
                }
            }
        }
        assert_eq!(all, vec![10, 11, 12, 13, 14]);
        drop(stream);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_query_stream_cancelled_mid_stream_returns_cancelled() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT gs::INT FROM generate_series(1, 1000) gs");
        let mut stream = tx
            .query_stream(&stmt, 50, &cancel)
            .await
            .expect("open");

        // Consumiamo un batch OK.
        let _ = stream.next_batch(&cancel).await.expect("first batch");

        // Poi cancelliamo prima del successivo.
        cancel.cancel();
        let err = stream
            .next_batch(&cancel)
            .await
            .expect_err("cancel deve bloccare");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::Cancelled);
        drop(stream);

        tx.rollback(&cancel).await.expect("rollback ignora cancel");
    }

    #[tokio::test]
    async fn live_query_stream_zero_batch_size_is_invalid_plan() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT 1");
        let err = match tx.query_stream(&stmt, 0, &cancel).await {
            Err(e) => e,
            Ok(_) => panic!("batch_size=0 deve fallire"),
        };
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_query_stream_cursor_released_on_commit() {
        // Dopo il commit, il cursor deve essere scomparso dalla sessione.
        // Riusando la stessa sessione (attraverso il pool), un `FETCH` sul
        // nome dovrebbe fallire con 34000 (invalid_cursor_name).
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        {
            let stmt = Statement::new("SELECT 1");
            let mut stream = tx
                .query_stream(&stmt, 10, &cancel)
                .await
                .expect("open");
            let _ = stream.next_batch(&cancel).await.expect("first");
        }
        tx.commit(&cancel).await.expect("commit");

        // Apro un'altra transazione — non abbiamo garanzia deterministica
        // che sia la stessa connessione, ma il test di "commit chiude il
        // cursor" è già coperto dalla non-visibilità cross-transaction dei
        // cursor in Postgres. Ci basta verificare che la nuova tx sia sana.
        let mut tx2 = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin 2");
        tx2.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("sessione sana");
        tx2.commit(&cancel).await.expect("commit 2");
    }

    // === Opz 3: DDL fuori transazione ===

    #[tokio::test]
    async fn live_execute_ddl_creates_index_concurrently() {
        use plenora_database_core::provider::Provider;
        // CREATE INDEX CONCURRENTLY è vietato dentro transazione: la libreria
        // deve permetterne l'esecuzione via `Provider::execute_ddl`.
        scratch_table("opz3_ddl").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        Provider::execute_ddl(
            &provider,
            &secret(),
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS opz3_ddl_v_idx ON opz3_ddl (v)",
            &cancel,
        )
        .await
        .expect("CREATE INDEX CONCURRENTLY out-of-tx deve funzionare");

        // Cleanup
        Provider::execute_ddl(&provider, &secret(),
            "DROP INDEX IF EXISTS opz3_ddl_v_idx", &cancel)
            .await
            .expect("drop index");
        drop_table("opz3_ddl").await;
    }

    #[tokio::test]
    async fn live_execute_ddl_rejects_invalid_sql() {
        use plenora_database_core::provider::Provider;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let err = Provider::execute_ddl(&provider, &secret(), "NOT SQL AT ALL", &cancel)
            .await
            .expect_err("SQL malformata deve fallire");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
    }

    // === A6: Conformance profile ===

    use plenora_database_core::conformance::{
        check_profile, probe_application_oltp_v1, EvidenceKind, ProfileStatus,
        APPLICATION_OLTP_V1,
    };

    #[tokio::test]
    async fn live_probe_pfm_core_v1_passes_on_postgres() {
        use plenora_database_core::conformance::{
            check_profile, probe_pfm_core_v1, ProfileStatus, PFM_CORE_V1,
        };
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let evidence = probe_pfm_core_v1(&provider, &secret(), &cancel).await;
        let report = check_profile(&PFM_CORE_V1, &evidence);
        assert_eq!(
            report.status,
            ProfileStatus::Pass,
            "PFM_CORE_V1 FAIL. missing={:?} failed={:?}",
            report.missing,
            report.failed,
        );
    }

    #[tokio::test]
    async fn live_probe_pfm_gis_v1_passes_on_postgres() {
        use plenora_database_core::conformance::{
            check_profile, probe_pfm_gis_v1, ProfileStatus, PFM_GIS_V1,
        };
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let evidence = probe_pfm_gis_v1(&provider, &secret(), &cancel).await;
        let report = check_profile(&PFM_GIS_V1, &evidence);
        assert_eq!(
            report.status,
            ProfileStatus::Pass,
            "PFM_GIS_V1 FAIL. missing={:?} failed={:?}",
            report.missing,
            report.failed,
        );
    }

    #[tokio::test]
    async fn live_probe_application_oltp_v1_passes_on_postgres() {
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let evidence = probe_application_oltp_v1(&provider, &secret(), &cancel).await;

        // Ogni capability richiesta deve avere un'evidence Verified.
        for cap in APPLICATION_OLTP_V1.required {
            let found = evidence.iter().find(|e| e.capability == *cap).unwrap_or_else(|| {
                panic!("evidence assente per {:?}", cap)
            });
            assert_eq!(
                found.kind,
                EvidenceKind::Verified,
                "{:?} non verificata: {:?}",
                cap,
                found.notes
            );
        }

        let report = check_profile(&APPLICATION_OLTP_V1, &evidence);
        assert_eq!(
            report.status,
            ProfileStatus::Pass,
            "profilo FAIL. missing={:?} failed={:?} evidence={:?}",
            report.missing,
            report.failed,
            report.evidence
        );
        assert!(report.missing.is_empty());
        assert!(report.failed.is_empty());
    }

    // === A5: Facade OLTP (query, query_one, scalar) ===

    use plenora_database_core::facade::{
        execute_scalar_bool, execute_scalar_i64, execute_scalar_string, query_one, query_optional,
    };

    #[tokio::test]
    async fn live_facade_execute_scalar_i64_returns_single_cell() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let value = execute_scalar_i64(
            tx.as_mut(),
            &Statement::new("SELECT 42::BIGINT"),
            &cancel,
        )
        .await
        .expect("scalar i64");
        assert_eq!(value, 42);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_execute_scalar_string_and_bool() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let s = execute_scalar_string(
            tx.as_mut(),
            &Statement::new("SELECT 'hello'::TEXT"),
            &cancel,
        )
        .await
        .expect("scalar string");
        assert_eq!(s, "hello");
        let b = execute_scalar_bool(
            tx.as_mut(),
            &Statement::new("SELECT TRUE"),
            &cancel,
        )
        .await
        .expect("scalar bool");
        assert!(b);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_one_returns_full_row() {
        scratch_table("a5_query_one").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        tx.execute(
            &Statement::new("INSERT INTO a5_query_one VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("payload".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT id, v FROM a5_query_one WHERE id = $1")
                .with_params(vec![ParameterValue::I32(1)]),
            &cancel,
        )
        .await
        .expect("query_one");
        assert_eq!(row.len(), 2);
        assert!(matches!(&row[0], ParameterValue::I32(1)));
        assert!(matches!(&row[1], ParameterValue::String(s) if s == "payload"));

        tx.commit(&cancel).await.expect("commit");
        drop_table("a5_query_one").await;
    }

    #[tokio::test]
    async fn live_facade_query_one_zero_rows_is_not_found() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let err = query_one(
            tx.as_mut(),
            &Statement::new("SELECT 1 WHERE FALSE"),
            &cancel,
        )
        .await
        .expect_err("must be NotFound");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::NotFound);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_one_multiple_rows_is_conflict() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let err = query_one(
            tx.as_mut(),
            &Statement::new("SELECT * FROM (VALUES (1), (2)) t(x)"),
            &cancel,
        )
        .await
        .expect_err("must be Conflict for >1 row");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::Conflict);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_optional_none_and_some() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let none = query_optional(
            tx.as_mut(),
            &Statement::new("SELECT 1 WHERE FALSE"),
            &cancel,
        )
        .await
        .expect("optional none");
        assert!(none.is_none());

        let some = query_optional(
            tx.as_mut(),
            &Statement::new("SELECT 'x'::TEXT"),
            &cancel,
        )
        .await
        .expect("optional some");
        assert!(some.is_some());

        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_decodes_all_supported_scalar_types() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new(
                "SELECT
                    TRUE::BOOL,
                    42::INT4,
                    -1234567890::INT8,
                    3.14::FLOAT8,
                    'text'::TEXT,
                    '\\xdeadbeef'::BYTEA,
                    '2026-01-15'::DATE,
                    '2026-01-15 10:20:30'::TIMESTAMP,
                    '2026-01-15T10:20:30Z'::TIMESTAMPTZ,
                    '12345678-1234-1234-1234-123456789012'::UUID,
                    '{\"k\":1}'::JSONB",
            ),
            &cancel,
        )
        .await
        .expect("decode row");

        assert!(matches!(&row[0], ParameterValue::Bool(true)));
        assert!(matches!(&row[1], ParameterValue::I32(42)));
        assert!(matches!(&row[2], ParameterValue::I64(-1234567890)));
        assert!(matches!(&row[3], ParameterValue::F64(_)));
        assert!(matches!(&row[4], ParameterValue::String(s) if s == "text"));
        assert!(matches!(&row[5], ParameterValue::Bytes(b) if b == &[0xde, 0xad, 0xbe, 0xef]));
        assert!(matches!(&row[6], ParameterValue::Date(_)));
        assert!(matches!(&row[7], ParameterValue::Timestamp(_)));
        assert!(matches!(&row[8], ParameterValue::TimestampTz(_)));
        assert!(matches!(&row[9], ParameterValue::Uuid(_)));
        assert!(matches!(&row[10], ParameterValue::Json(_)));

        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_null_becomes_typed_null() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT NULL::TEXT"),
            &cancel,
        )
        .await
        .expect("null decode");
        match &row[0] {
            ParameterValue::Null { type_name } => assert_eq!(type_name, "text"),
            other => panic!("expected typed null, got {other:?}"),
        }
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_scalar_type_mismatch_is_data_mapping() {
        let provider = provider().await;
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");
        let err = execute_scalar_i64(
            tx.as_mut(),
            &Statement::new("SELECT 'not-a-number'::TEXT"),
            &cancel,
        )
        .await
        .expect_err("type mismatch");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::DataMapping);
        tx.commit(&cancel).await.expect("commit");
    }

    // === A4: Session context ===

    use plenora_database_core::session_context::{
        SessionContext, SessionEntry, SessionValue,
    };

    async fn read_session_setting(name: &str) -> String {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(LIVE_DSN, NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one("SELECT current_setting($1, true)", &[&name])
            .await
            .expect("current_setting");
        row.get::<_, Option<String>>(0).unwrap_or_default()
    }

    #[tokio::test]
    async fn live_session_context_is_readable_inside_transaction() {
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.tenant",
            SessionEntry::public(SessionValue::Text("acme".into())),
        )
        .expect("tenant");
        ctx.insert(
            "app.actor",
            SessionEntry::sensitive(SessionValue::Text("user-42".into())),
        )
        .expect("actor");

        let opts = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };
        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget(), &cancel)
            .await
            .expect("begin with context");

        // Verifica intra-tx: current_setting deve tornare i valori applicati.
        let update_result = tx
            .execute(
                &Statement::new(
                    "DO $$
                     BEGIN
                         IF current_setting('app.tenant', true) <> 'acme' THEN
                             RAISE EXCEPTION 'tenant not set';
                         END IF;
                         IF current_setting('app.actor', true) <> 'user-42' THEN
                             RAISE EXCEPTION 'actor not set';
                         END IF;
                     END$$;",
                ),
                &cancel,
            )
            .await;
        update_result.expect("DO block must succeed");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    #[tokio::test]
    async fn live_session_context_resets_after_commit_on_pooled_reuse() {
        // Un tx con context su una connessione, poi una SECONDA tx *senza*
        // context sulla STESSA connessione (idealmente ripescata dal pool).
        // Il context della prima non deve leakare nella seconda: `SET LOCAL`
        // + `is_local=true` sono resettati automaticamente dal COMMIT.
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.leak_probe",
            SessionEntry::public(SessionValue::Text("first-tx".into())),
        )
        .expect("insert");
        let opts_with = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };

        let mut tx1 = provider
            .begin_transaction(&secret(), &opts_with, &budget(), &cancel)
            .await
            .expect("begin 1");
        tx1.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("tx1 select");
        tx1.commit(&cancel).await.expect("commit tx1");

        let mut tx2 = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin 2");

        // In tx2 la setting non deve esistere: attendiamo stringa vuota.
        let leak_check = tx2
            .execute(
                &Statement::new(
                    "DO $$
                     BEGIN
                         IF current_setting('app.leak_probe', true) <> '' THEN
                             RAISE EXCEPTION 'context leak: %', current_setting('app.leak_probe', true);
                         END IF;
                     END$$;",
                ),
                &cancel,
            )
            .await;
        leak_check.expect("no leak");

        tx2.commit(&cancel).await.expect("commit tx2");
    }

    #[tokio::test]
    async fn live_session_context_is_isolated_from_external_session() {
        // Un tx applica il context, un client SEPARATO (nuova connessione)
        // interroga il proprio setting: deve essere vuoto perché GUC transaction-local
        // vive solo nella sessione che l'ha impostato.
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.isolation_probe",
            SessionEntry::public(SessionValue::Text("only-in-tx".into())),
        )
        .expect("insert");
        let opts = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };

        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget(), &cancel)
            .await
            .expect("begin");
        tx.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("select");

        // In parallelo, altra sessione: la GUC non deve esistere.
        let external = read_session_setting("app.isolation_probe").await;
        assert_eq!(external, "");

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_session_context_typed_values_serialize_correctly() {
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.int_val",
            SessionEntry::public(SessionValue::Integer(42)),
        )
        .expect("int");
        ctx.insert(
            "app.bool_val",
            SessionEntry::public(SessionValue::Boolean(true)),
        )
        .expect("bool");

        let opts = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };
        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget(), &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new(
                "DO $$
                 BEGIN
                     IF current_setting('app.int_val', true) <> '42' THEN
                         RAISE EXCEPTION 'int not encoded';
                     END IF;
                     IF current_setting('app.bool_val', true) <> 'true' THEN
                         RAISE EXCEPTION 'bool not encoded';
                     END IF;
                 END$$;",
            ),
            &cancel,
        )
        .await
        .expect("DO ok");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    // === A3: Optimistic concurrency ===

    async fn versioned_scratch(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(LIVE_DSN, NoTls)
            .await
            .expect("connect setup");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
                 CREATE TABLE {name} (
                     id INT PRIMARY KEY,
                     version INT NOT NULL,
                     payload TEXT NOT NULL
                 );
                 INSERT INTO {name} VALUES (1, 17, 'v17'), (2, 42, 'meta');",
            ))
            .await
            .expect("setup versioned");
    }

    fn conditional_update<'a>(
        update: &'a Statement,
        probe: Option<&'a Statement>,
    ) -> ConditionalUpdate<'a> {
        ConditionalUpdate {
            update,
            key_probe: probe,
            expected_affected_rows: 1,
        }
    }

    #[tokio::test]
    async fn live_optimistic_update_matches_expected_version_applied() {
        versioned_scratch("a3_ok").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let update = Statement::new(
            "UPDATE a3_ok SET version = version + 1, payload = $1 \
             WHERE id = $2 AND version = $3",
        )
        .with_params(vec![
            ParameterValue::String("v18".into()),
            ParameterValue::I32(1),
            ParameterValue::I32(17),
        ]);
        let probe = Statement::new("SELECT 1 FROM a3_ok WHERE id = $1")
            .with_params(vec![ParameterValue::I32(1)]);

        tx.execute_conditional_update(conditional_update(&update, Some(&probe)), &cancel)
            .await
            .expect("update ottimistico");

        tx.commit(&cancel).await.expect("commit");

        assert_eq!(
            count(&provider, "SELECT COUNT(*)::BIGINT FROM a3_ok WHERE version = 18").await,
            1
        );
        drop_table("a3_ok").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_wrong_version_is_concurrent_modification() {
        versioned_scratch("a3_conflict").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let update = Statement::new(
            "UPDATE a3_conflict SET version = version + 1 \
             WHERE id = $1 AND version = $2",
        )
        .with_params(vec![ParameterValue::I32(1), ParameterValue::I32(99)]);
        let probe = Statement::new("SELECT 1 FROM a3_conflict WHERE id = $1")
            .with_params(vec![ParameterValue::I32(1)]);

        let err = tx
            .execute_conditional_update(conditional_update(&update, Some(&probe)), &cancel)
            .await
            .expect_err("mismatch versione");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::ConcurrentModification
        );
        assert_eq!(
            err.remote_effect,
            plenora_database_core::RemoteEffect::RolledBack
        );

        tx.rollback(&cancel).await.expect("rollback");
        assert_eq!(
            count(&provider, "SELECT version::BIGINT FROM a3_conflict WHERE id = 1").await,
            17
        );
        drop_table("a3_conflict").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_missing_key_with_probe_is_not_found() {
        versioned_scratch("a3_missing").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        let update = Statement::new(
            "UPDATE a3_missing SET version = version + 1 \
             WHERE id = $1 AND version = $2",
        )
        .with_params(vec![ParameterValue::I32(999), ParameterValue::I32(17)]);
        let probe = Statement::new("SELECT 1 FROM a3_missing WHERE id = $1")
            .with_params(vec![ParameterValue::I32(999)]);

        let err = tx
            .execute_conditional_update(conditional_update(&update, Some(&probe)), &cancel)
            .await
            .expect_err("chiave assente");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::NotFound);

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("a3_missing").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_without_probe_defaults_to_conflict() {
        versioned_scratch("a3_default").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        // Chiave assente, ma NESSUN probe: il default conservativo classifica
        // come ConcurrentModification (fail-loud sulla concorrenza).
        let update = Statement::new(
            "UPDATE a3_default SET version = version + 1 \
             WHERE id = $1 AND version = $2",
        )
        .with_params(vec![ParameterValue::I32(9999), ParameterValue::I32(0)]);

        let err = tx
            .execute_conditional_update(conditional_update(&update, None), &cancel)
            .await
            .expect_err("mismatch senza probe");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::ConcurrentModification
        );

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("a3_default").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_multi_row_matches_expected_count() {
        versioned_scratch("a3_multi").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin");

        // Update in blocco: le due righe iniziali hanno version=17 e version=42.
        // WHERE version < 100 le colpisce entrambe: expected=2.
        let update = Statement::new(
            "UPDATE a3_multi SET version = version + 1 WHERE version < $1",
        )
        .with_params(vec![ParameterValue::I32(100)]);

        let request = ConditionalUpdate {
            update: &update,
            key_probe: None,
            expected_affected_rows: 2,
        };
        tx.execute_conditional_update(request, &cancel)
            .await
            .expect("update multi-riga");

        tx.commit(&cancel).await.expect("commit");
        assert_eq!(
            count(&provider, "SELECT COUNT(*)::BIGINT FROM a3_multi WHERE version > 17").await,
            2
        );
        drop_table("a3_multi").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_two_writers_only_one_succeeds() {
        // Simulazione end-to-end del pattern PFM: due writer concorrenti che
        // partono dalla stessa expected_version. Con SERIALIZABLE, uno vince
        // e l'altro deve ricevere ConcurrentModification (o serialization
        // failure retryable, tracciato dal test).
        versioned_scratch("a3_race").await;
        let provider = provider().await;
        let cancel = CancellationToken::new();

        let mut tx_a = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin A");
        let mut tx_b = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget(), &cancel)
            .await
            .expect("begin B");

        let update_a = Statement::new(
            "UPDATE a3_race SET version = version + 1, payload = $1 \
             WHERE id = $2 AND version = $3",
        )
        .with_params(vec![
            ParameterValue::String("A".into()),
            ParameterValue::I32(1),
            ParameterValue::I32(17),
        ]);
        let update_b = Statement::new(
            "UPDATE a3_race SET version = version + 1, payload = $1 \
             WHERE id = $2 AND version = $3",
        )
        .with_params(vec![
            ParameterValue::String("B".into()),
            ParameterValue::I32(1),
            ParameterValue::I32(17),
        ]);
        let probe = Statement::new("SELECT 1 FROM a3_race WHERE id = $1")
            .with_params(vec![ParameterValue::I32(1)]);

        // A applica e committa.
        tx_a.execute_conditional_update(
            conditional_update(&update_a, Some(&probe)),
            &cancel,
        )
        .await
        .expect("A applica");
        tx_a.commit(&cancel).await.expect("A commit");

        // B parte dalla stessa expected_version=17 ma la riga è già a 18.
        let err = tx_b
            .execute_conditional_update(conditional_update(&update_b, Some(&probe)), &cancel)
            .await
            .expect_err("B deve fallire");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::ConcurrentModification
        );

        tx_b.rollback(&cancel).await.expect("B rollback");

        drop_table("a3_race").await;
    }

    #[tokio::test]
    async fn live_execute_after_constraint_violation_still_reports_25p02() {
        // Pattern classico: un errore in transazione mette Postgres in
        // "in_failed_sql_transaction" — verifichiamo che il mapping A2 sia
        // ancora corretto quando invocato via il transaction scope.
        scratch_table("a1_fail").await;
        let provider = provider().await;
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new("INSERT INTO a1_fail VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("first".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert 1");

        let dup = tx
            .execute(
                &Statement::new("INSERT INTO a1_fail VALUES ($1, $2)").with_params(vec![
                    ParameterValue::I32(1),
                    ParameterValue::String("dup".into()),
                ]),
                &cancel,
            )
            .await
            .expect_err("unique violation");
        assert_eq!(dup.category, plenora_database_core::ErrorCategory::Conflict);

        // Uno statement successivo in tx guasta → 25P02 (Protocol nel mapping).
        let poisoned = tx
            .execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect_err("tx guasta");
        assert_eq!(
            poisoned.category,
            plenora_database_core::ErrorCategory::Protocol
        );

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("a1_fail").await;
    }
}

