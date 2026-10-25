use crate::connection::with_timeout_duration;
use oracle_rs::types::{LobLocator, LobValue, OracleDate, OracleNumber, OracleTimestamp};
use oracle_rs::{Connection, OracleType, Value};
use plenora_database_core::parameter_fidelity::{canonical_uuid, reject_sub_microsecond};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::time::Duration;

const BLOB_WRITE_CHUNK_BYTES: usize = 1_024;
const CLOB_WRITE_CHUNK_BYTES: usize = 4_000;

#[derive(Default)]
pub struct LobCache {
    blobs: Vec<LobLocator>,
    clobs: Vec<LobLocator>,
}

struct LobIo<'a> {
    connection: &'a Connection,
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &'a plenora_database_core::CancellationToken,
}

pub fn bind_parameters(parameters: &[ParameterValue]) -> Result<Vec<Value>> {
    parameters.iter().map(bind_parameter).collect()
}

pub async fn bind_parameters_with_lobs(
    connection: &Connection,
    parameters: &[ParameterValue],
    promote_write_lobs: bool,
    cache: &mut LobCache,
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &plenora_database_core::CancellationToken,
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(parameters.len());
    let mut blob_index = 0_usize;
    let mut clob_index = 0_usize;
    let lob_io = LobIo {
        connection,
        timeout,
        phase,
        cancellation,
    };
    for parameter in parameters {
        if !promote_write_lobs {
            reject_empty(parameter)?;
        }
        let blob = match parameter {
            ParameterValue::Wkb { bytes, .. } => Some(bytes.as_slice()),
            ParameterValue::Bytes(bytes) if promote_write_lobs => Some(bytes.as_slice()),
            _ => None,
        };
        let clob = match parameter {
            ParameterValue::String(text) if promote_write_lobs && text.len() > 4_000 => {
                Some(text.as_str())
            }
            _ => None,
        };
        if let Some(bytes) = blob {
            values.push(
                lob_io
                    .bind_blob(bytes, &mut cache.blobs, blob_index)
                    .await?,
            );
            blob_index += 1;
        } else if let Some(text) = clob {
            values.push(lob_io.bind_clob(text, &mut cache.clobs, clob_index).await?);
            clob_index += 1;
        } else {
            values.push(bind_parameter(parameter)?);
        }
    }
    Ok(values)
}

impl LobIo<'_> {
    async fn bind_blob(
        &self,
        bytes: &[u8],
        cache: &mut Vec<LobLocator>,
        index: usize,
    ) -> Result<Value> {
        self.reset_or_create(cache, index, OracleType::Blob).await?;
        for (chunk_index, chunk) in bytes.chunks(BLOB_WRITE_CHUNK_BYTES).enumerate() {
            let offset = chunk_index
                .checked_mul(BLOB_WRITE_CHUNK_BYTES)
                .and_then(|offset| u64::try_from(offset).ok())
                .and_then(|offset| offset.checked_add(1))
                .ok_or_else(|| {
                    DatabaseError::resource_limit("offset temporary BLOB Oracle in overflow")
                })?;
            with_timeout_duration(
                self.timeout,
                self.phase,
                self.cancellation,
                self.connection.write_blob(&cache[index], offset, chunk),
            )
            .await
            .map_err(|error| lob_error(error, "scrittura temporary BLOB Oracle fallita"))?;
        }
        Ok(Value::Lob(LobValue::locator(cache[index].clone())))
    }

    async fn bind_clob(
        &self,
        text: &str,
        cache: &mut Vec<LobLocator>,
        index: usize,
    ) -> Result<Value> {
        self.reset_or_create(cache, index, OracleType::Clob).await?;
        let mut byte_offset = 0_usize;
        let mut character_offset = 1_u64;
        while byte_offset < text.len() {
            let mut end = byte_offset
                .checked_add(CLOB_WRITE_CHUNK_BYTES)
                .unwrap_or(text.len())
                .min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            let chunk = &text[byte_offset..end];
            with_timeout_duration(
                self.timeout,
                self.phase,
                self.cancellation,
                self.connection
                    .write_clob(&cache[index], character_offset, chunk),
            )
            .await
            .map_err(|error| lob_error(error, "scrittura temporary CLOB Oracle fallita"))?;
            character_offset = character_offset
                .checked_add(u64::try_from(chunk.chars().count()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    DatabaseError::resource_limit("offset temporary CLOB Oracle in overflow")
                })?;
            byte_offset = end;
        }
        Ok(Value::Lob(LobValue::locator(cache[index].clone())))
    }

    async fn reset_or_create(
        &self,
        cache: &mut Vec<LobLocator>,
        index: usize,
        oracle_type: OracleType,
    ) -> Result<()> {
        if index == cache.len() {
            cache.push(
                with_timeout_duration(
                    self.timeout,
                    self.phase,
                    self.cancellation,
                    self.connection.create_temp_lob(oracle_type),
                )
                .await
                .map_err(|error| lob_error(error, "creazione temporary LOB Oracle fallita"))?,
            );
        } else {
            with_timeout_duration(
                self.timeout,
                self.phase,
                self.cancellation,
                self.connection.lob_trim(&cache[index], 0),
            )
            .await
            .map_err(|error| lob_error(error, "reset temporary LOB Oracle fallito"))?;
        }
        Ok(())
    }
}

/// Oracle conserva una stringa vuota, e un `RAW` vuoto, come `NULL`: il valore
/// scritto non sarebbe quello bindato, e `col = :n` non lo ritroverebbe mai.
///
/// Vale per lo statement del chiamante e per il filtro della lettura
/// (`promote_write_lobs` falso). Il percorso di scrittura Arrow, che promuove
/// i binari a `BLOB` e binda le righe del parameter array con
/// [`bind_parameters`], resta fuori: la sua regola per `''` e quella della
/// scrittura, non di questo modulo.
pub fn reject_empty(parameter: &ParameterValue) -> Result<()> {
    let empty = match parameter {
        ParameterValue::String(value)
        | ParameterValue::Uuid(value)
        | ParameterValue::Enum { label: value, .. } => value.is_empty(),
        ParameterValue::Bytes(value) => value.is_empty(),
        _ => false,
    };
    if empty {
        return Err(mapping_error(
            "parametro vuoto Oracle: stringa e RAW vuoti si conservano come NULL",
        ));
    }
    Ok(())
}

fn lob_error(mut error: DatabaseError, message: &'static str) -> DatabaseError {
    message.clone_into(&mut error.message);
    error
}

fn bind_parameter(value: &ParameterValue) -> Result<Value> {
    match value {
        ParameterValue::Bool(value) => Ok(Value::Integer(i64::from(*value))),
        ParameterValue::I32(value) => Ok(Value::Integer(i64::from(*value))),
        ParameterValue::I64(value) => Ok(Value::Integer(*value)),
        ParameterValue::F64(value) if value.is_finite() => Ok(Value::Float(*value)),
        ParameterValue::F64(_) => Err(mapping_error("parametro float Oracle non finito")),
        ParameterValue::String(value) | ParameterValue::Enum { label: value, .. } => {
            Ok(Value::String(value.clone()))
        }
        ParameterValue::Uuid(value) => {
            canonical_uuid(ProviderKind::Oracle, value)?;
            Ok(Value::String(value.clone()))
        }
        ParameterValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
        ParameterValue::Json(value) => Ok(Value::Json(value.clone())),
        ParameterValue::Decimal(value) => {
            validate_decimal(value)?;
            Ok(Value::Number(OracleNumber::new(value.clone())))
        }
        ParameterValue::Date(value) => parse_date(value).map(Value::Date),
        ParameterValue::Timestamp(value) => parse_timestamp(value).map(Value::Timestamp),
        ParameterValue::TimestampTz(value) => parse_timestamp_tz(value).map(Value::String),
        ParameterValue::Null { .. } => Ok(Value::Null),
        ParameterValue::Wkb { bytes, .. } => Ok(Value::Bytes(bytes.clone())),
    }
}

/// Il testo nel modello `ORACLE_TIMESTAMP_TZ_FORMAT_MODEL`, per il
/// `TO_TIMESTAMP_TZ(:n, modello)` che lo converte.
///
/// Limite dichiarato (regola di `plenora_database_core::parameter_fidelity`):
/// il binding e esatto soltanto dentro quella conversione esplicita, che il
/// portable scrive da se e che lo statement del chiamante deve scrivere.
/// Ambito: un `:n` nudo confrontato o assegnato a un `TIMESTAMP WITH TIME
/// ZONE`. Hazard: Oracle converte il testo con l'`NLS_TIMESTAMP_TZ_FORMAT`
/// della sessione; con quello di default rifiuta (ORA-01843), con un modello
/// diverso potrebbe leggere altri campi. Rientro: un binding nativo di
/// `TIMESTAMP WITH TIME ZONE` nel fork di `oracle-rs`.
fn parse_timestamp_tz(value: &str) -> Result<String> {
    reject_sub_microsecond(ProviderKind::Oracle, value)?;
    let timestamp = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| mapping_error("parametro timestamptz Oracle non valido"))?;
    Ok(timestamp.format("%Y-%m-%dT%H:%M:%S%.6f%:z").to_string())
}

fn validate_decimal(value: &str) -> Result<()> {
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map_or((unsigned, None), |(mantissa, exponent)| {
            (mantissa, Some(exponent))
        });
    let digits = mantissa.bytes().filter(u8::is_ascii_digit).count();
    let significant_digits = mantissa
        .bytes()
        .filter(u8::is_ascii_digit)
        .skip_while(|byte| *byte == b'0')
        .count();
    let mantissa_valid = !mantissa.is_empty()
        && digits > 0
        && mantissa.bytes().filter(|byte| *byte == b'.').count() <= 1
        && mantissa
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.');
    let exponent_valid = exponent.is_none_or(|exponent| {
        let unsigned = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        !unsigned.is_empty()
            && unsigned.bytes().all(|byte| byte.is_ascii_digit())
            && unsigned.len() <= 4
    });
    if value.is_empty()
        || value.len() > 172
        || !mantissa_valid
        || !exponent_valid
        || significant_digits > 38
    {
        return Err(mapping_error("parametro decimal Oracle non valido"));
    }
    Ok(())
}

fn parse_date(value: &str) -> Result<OracleDate> {
    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| mapping_error("parametro date Oracle non valido"))?;
    Ok(OracleDate::date(
        chrono::Datelike::year(&date),
        u8::try_from(chrono::Datelike::month(&date))
            .map_err(|_| mapping_error("mese Oracle non rappresentabile"))?,
        u8::try_from(chrono::Datelike::day(&date))
            .map_err(|_| mapping_error("giorno Oracle non rappresentabile"))?,
    ))
}

fn parse_timestamp(value: &str) -> Result<OracleTimestamp> {
    reject_sub_microsecond(ProviderKind::Oracle, value)?;
    let timestamp = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|_| mapping_error("parametro timestamp Oracle non valido"))?;
    Ok(OracleTimestamp::new(
        chrono::Datelike::year(&timestamp),
        u8::try_from(chrono::Datelike::month(&timestamp))
            .map_err(|_| mapping_error("mese Oracle non rappresentabile"))?,
        u8::try_from(chrono::Datelike::day(&timestamp))
            .map_err(|_| mapping_error("giorno Oracle non rappresentabile"))?,
        u8::try_from(chrono::Timelike::hour(&timestamp))
            .map_err(|_| mapping_error("ora Oracle non rappresentabile"))?,
        u8::try_from(chrono::Timelike::minute(&timestamp))
            .map_err(|_| mapping_error("minuto Oracle non rappresentabile"))?,
        u8::try_from(chrono::Timelike::second(&timestamp))
            .map_err(|_| mapping_error("secondo Oracle non rappresentabile"))?,
        chrono::Timelike::nanosecond(&timestamp) / 1_000,
    ))
}

fn mapping_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        Some(ProviderKind::Oracle),
        message,
    )
}
