use crate::connection::with_timeout_duration;
use chrono::{FixedOffset, NaiveDate, SecondsFormat, TimeZone};
use oracle_rs::{ColumnInfo, Connection, LobData, LobValue, OracleType, QueryResult, Value};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::row::Row;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use std::sync::Arc;
use std::time::Duration;

/// Metadati necessari a decodificare una pagina. Il tipo Oracle fa parte
/// della decisione: un offset UTC `+00:00` non e distinguibile da un
/// timestamp senza fuso guardando il solo valore restituito dal driver.
pub struct DecodeColumns {
    names: Arc<[String]>,
    types: Arc<[OracleType]>,
    json: Arc<[bool]>,
    charset_forms: Arc<[u8]>,
    number_scales: Arc<[i16]>,
}

pub async fn rows_from_result(
    connection: &Connection,
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
    result: QueryResult,
) -> Result<Vec<Row>> {
    let columns = decode_columns(&result.columns)?;
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        rows.push(
            row_from_driver(
                connection,
                Arc::clone(&columns),
                row,
                timeout,
                phase,
                cancellation,
            )
            .await?,
        );
    }
    Ok(rows)
}

pub fn decode_columns(columns: &[ColumnInfo]) -> Result<Arc<DecodeColumns>> {
    if columns.iter().any(|column| column.name.is_empty()) {
        return Err(mapping_error(
            ErrorPhase::Read,
            "risultato Oracle con colonna senza nome",
        ));
    }
    Ok(Arc::new(DecodeColumns {
        names: columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
            .into(),
        types: columns
            .iter()
            .map(|column| column.oracle_type)
            .collect::<Vec<_>>()
            .into(),
        json: columns
            .iter()
            .map(|column| column.is_json)
            .collect::<Vec<_>>()
            .into(),
        charset_forms: columns
            .iter()
            .map(|column| column.csfrm)
            .collect::<Vec<_>>()
            .into(),
        number_scales: columns
            .iter()
            .map(|column| column.scale)
            .collect::<Vec<_>>()
            .into(),
    }))
}

pub async fn row_from_driver(
    connection: &Connection,
    columns: Arc<DecodeColumns>,
    row: oracle_rs::Row,
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
) -> Result<Row> {
    let driver_values = row.into_values();
    if driver_values.len() != columns.names.len() {
        return Err(mapping_error(
            phase,
            "risultato Oracle con numero di colonne incoerente",
        ));
    }
    let mut values = Vec::with_capacity(driver_values.len());
    for (index, value) in driver_values.into_iter().enumerate() {
        values.push(
            value_from_driver(
                connection,
                value,
                columns.types[index],
                columns.json[index],
                columns.charset_forms[index],
                columns.number_scales[index],
                timeout,
                phase,
                cancellation,
            )
            .await?,
        );
    }
    Row::try_new(Arc::clone(&columns.names), values)
}

#[allow(clippy::too_many_arguments)]
async fn value_from_driver(
    connection: &Connection,
    value: Value,
    oracle_type: OracleType,
    is_json: bool,
    charset_form: u8,
    number_scale: i16,
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
) -> Result<ParameterValue> {
    match value {
        Value::Null => Ok(ParameterValue::Null {
            type_name: oracle_type_name(oracle_type).to_owned(),
        }),
        Value::String(value)
            if matches!(oracle_type, OracleType::Number | OracleType::BinaryInteger) =>
        {
            Ok(decode_number(value, number_scale))
        }
        Value::String(value) if is_json || oracle_type == OracleType::Json => {
            serde_json::from_str(&value)
                .map(ParameterValue::Json)
                .map_err(|_| mapping_error(phase, "JSON Oracle non valido"))
        }
        Value::String(value) => Ok(ParameterValue::String(value)),
        Value::Bytes(value) => Ok(ParameterValue::Bytes(value)),
        Value::Integer(value) => Ok(ParameterValue::I64(value)),
        Value::Float(value) if value.is_finite() => Ok(ParameterValue::F64(value)),
        Value::Float(_) => Err(mapping_error(phase, "risultato float Oracle non finito")),
        Value::Number(value) => Ok(decode_number(value.as_str().to_owned(), number_scale)),
        Value::Date(value) => Ok(ParameterValue::Timestamp(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            value.year, value.month, value.day, value.hour, value.minute, value.second
        ))),
        Value::Timestamp(value) if oracle_type == OracleType::TimestampTz => {
            decode_timestamp_tz(value, phase).map(ParameterValue::TimestampTz)
        }
        Value::Timestamp(_) if oracle_type == OracleType::TimestampLtz => {
            Err(DatabaseError::unsupported(
                ProviderKind::Oracle,
                phase,
                "TIMESTAMP WITH LOCAL TIME ZONE Oracle non qualificato",
            ))
        }
        Value::Timestamp(value) => Ok(ParameterValue::Timestamp(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond
        ))),
        Value::RowId(value) => Ok(value.to_string().map_or_else(
            || ParameterValue::Null {
                type_name: oracle_type_name(oracle_type).to_owned(),
            },
            ParameterValue::String,
        )),
        Value::Boolean(value) => Ok(ParameterValue::Bool(value)),
        Value::Json(value) => Ok(ParameterValue::Json(value)),
        Value::Lob(value) => {
            decode_lob(
                connection,
                value,
                oracle_type,
                is_json,
                charset_form,
                timeout,
                phase,
                cancellation,
            )
            .await
        }
        Value::Vector(_) | Value::Cursor(_) | Value::Collection(_) => {
            Err(DatabaseError::unsupported(
                ProviderKind::Oracle,
                phase,
                "tipo risultato Oracle non ancora qualificato",
            ))
        }
    }
}

fn decode_timestamp_tz(
    value: oracle_rs::types::OracleTimestamp,
    phase: ErrorPhase,
) -> Result<String> {
    // oracle-rs espone l'istante normalizzato a UTC insieme all'offset Oracle.
    // Ricostruire un DateTime dall'UTC evita di sottrarre l'offset una seconda
    // volta e gestisce correttamente anche i cambi di giorno.
    let utc = NaiveDate::from_ymd_opt(value.year, u32::from(value.month), u32::from(value.day))
        .and_then(|date| {
            date.and_hms_micro_opt(
                u32::from(value.hour),
                u32::from(value.minute),
                u32::from(value.second),
                value.microsecond,
            )
        })
        .ok_or_else(|| mapping_error(phase, "TIMESTAMP WITH TIME ZONE Oracle non valido"))?;
    let seconds = i32::from(value.tz_hour_offset) * 3_600 + i32::from(value.tz_minute_offset) * 60;
    let offset = FixedOffset::east_opt(seconds)
        .ok_or_else(|| mapping_error(phase, "offset TIMESTAMP WITH TIME ZONE Oracle non valido"))?;
    Ok(offset
        .from_utc_datetime(&utc)
        .to_rfc3339_opts(SecondsFormat::Micros, false))
}

fn decode_number(value: String, scale: i16) -> ParameterValue {
    if scale == 0 {
        if let Ok(value) = value.parse::<i64>() {
            return ParameterValue::I64(value);
        }
    }
    ParameterValue::Decimal(value)
}

#[allow(clippy::too_many_arguments)]
async fn decode_lob(
    connection: &Connection,
    value: LobValue,
    oracle_type: OracleType,
    is_json: bool,
    charset_form: u8,
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
) -> Result<ParameterValue> {
    let data = match value {
        LobValue::Null => {
            return Ok(ParameterValue::Null {
                type_name: oracle_type_name(oracle_type).to_owned(),
            });
        }
        LobValue::Empty => return empty_lob(oracle_type, is_json, phase),
        LobValue::Inline(bytes) => {
            if matches!(oracle_type, OracleType::Clob | OracleType::Json) || is_json {
                LobData::String(decode_inline_text(&bytes, charset_form, phase)?)
            } else {
                LobData::Bytes(bytes)
            }
        }
        LobValue::Locator(locator) => {
            with_timeout_duration(timeout, phase, cancellation, connection.read_lob(&locator))
                .await?
        }
    };
    match data {
        LobData::String(value) if is_json || oracle_type == OracleType::Json => {
            serde_json::from_str(&value)
                .map(ParameterValue::Json)
                .map_err(|_| mapping_error(phase, "JSON Oracle non valido"))
        }
        LobData::String(value) => Ok(ParameterValue::String(value)),
        LobData::Bytes(value) => Ok(ParameterValue::Bytes(value.to_vec())),
    }
}

fn empty_lob(oracle_type: OracleType, is_json: bool, phase: ErrorPhase) -> Result<ParameterValue> {
    if is_json || oracle_type == OracleType::Json {
        return Err(mapping_error(phase, "JSON Oracle vuoto non valido"));
    }
    if oracle_type == OracleType::Clob {
        Ok(ParameterValue::String(String::new()))
    } else {
        Ok(ParameterValue::Bytes(Vec::new()))
    }
}

fn decode_inline_text(bytes: &[u8], charset_form: u8, phase: ErrorPhase) -> Result<String> {
    if charset_form == oracle_rs::constants::csfrm::NCHAR {
        if !bytes.len().is_multiple_of(2) {
            return Err(mapping_error(phase, "testo Oracle UTF-16 non valido"));
        }
        let (pairs, remainder) = bytes.as_chunks::<2>();
        debug_assert!(remainder.is_empty());
        let units = pairs
            .iter()
            .map(|pair| u16::from_be_bytes([pair[0], pair[1]]));
        return char::decode_utf16(units)
            .collect::<std::result::Result<String, _>>()
            .map_err(|_| mapping_error(phase, "testo Oracle UTF-16 non valido"));
    }
    String::from_utf8(bytes.to_vec())
        .map_err(|_| mapping_error(phase, "testo Oracle UTF-8 non valido"))
}

const fn oracle_type_name(value: OracleType) -> &'static str {
    match value {
        OracleType::Varchar | OracleType::Char | OracleType::Long | OracleType::Clob => "string",
        OracleType::Number | OracleType::BinaryInteger => "decimal",
        OracleType::Raw | OracleType::LongRaw | OracleType::Blob | OracleType::Bfile => "bytes",
        OracleType::Date | OracleType::Timestamp => "timestamp",
        OracleType::TimestampTz | OracleType::TimestampLtz => "timestamptz",
        OracleType::BinaryFloat | OracleType::BinaryDouble => "float64",
        OracleType::Boolean => "bool",
        OracleType::Json => "json",
        OracleType::Rowid | OracleType::Urowid => "rowid",
        OracleType::Cursor => "cursor",
        OracleType::Object => "object",
        OracleType::Vector => "vector",
        OracleType::IntervalYm | OracleType::IntervalDs => "interval",
    }
}

fn mapping_error(phase: ErrorPhase, message: &'static str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Oracle),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

#[cfg(test)]
#[path = "decode_tests.rs"]
mod tests;
