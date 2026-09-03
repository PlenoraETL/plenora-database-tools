//! Decodifica Arrow condivisa dagli adapter di scrittura.

use chrono::{Duration, NaiveDate};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, TimeUnit};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

/// Converte una cella Arrow nel valore portabile usato dai driver.
///
/// `timestamp_separator` preserva la sintassi accettata dal protocollo di
/// destinazione senza duplicare il codec scalare nei provider.
///
/// # Errors
///
/// `DataMapping` per array incoerenti, valori non finiti o fuori range;
/// `Unsupported` per tipi Arrow non inclusi nel contratto condiviso.
pub fn arrow_parameter_value(
    array: &dyn Array,
    field: &Field,
    row: usize,
    timestamp_separator: char,
) -> Result<ParameterValue> {
    if array.is_null(row) {
        return Ok(ParameterValue::Null {
            type_name: format!("{:?}", field.data_type()),
        });
    }
    match field.data_type() {
        DataType::Boolean => Ok(ParameterValue::Bool(
            downcast::<BooleanArray>(array)?.value(row),
        )),
        DataType::Int16 => Ok(ParameterValue::I32(i32::from(
            downcast::<Int16Array>(array)?.value(row),
        ))),
        DataType::Int32 => Ok(ParameterValue::I32(
            downcast::<Int32Array>(array)?.value(row),
        )),
        DataType::Int64 => Ok(ParameterValue::I64(
            downcast::<Int64Array>(array)?.value(row),
        )),
        DataType::Float32 => {
            finite(f64::from(downcast::<Float32Array>(array)?.value(row))).map(ParameterValue::F64)
        }
        DataType::Float64 => {
            finite(downcast::<Float64Array>(array)?.value(row)).map(ParameterValue::F64)
        }
        DataType::Decimal128(_, scale) => Ok(ParameterValue::Decimal(decimal_text(
            downcast::<Decimal128Array>(array)?.value(row),
            *scale,
        )?)),
        DataType::Utf8 => Ok(ParameterValue::String(
            downcast::<StringArray>(array)?.value(row).to_owned(),
        )),
        DataType::Binary | DataType::LargeBinary => Ok(ParameterValue::Bytes(
            arrow_binary_value(array, field.data_type(), row)?.to_vec(),
        )),
        DataType::Date32 => Ok(ParameterValue::Date(date_text(
            downcast::<Date32Array>(array)?.value(row),
        )?)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(ParameterValue::Timestamp(timestamp_text(
                downcast::<TimestampMicrosecondArray>(array)?.value(row),
                timestamp_separator,
            )?))
        }
        _ => Err(write_error(
            ErrorCategory::Unsupported,
            "tipo Arrow non qualificato dal codec write condiviso",
        )),
    }
}

/// Legge Binary/LargeBinary senza cambiare il frame WKB.
///
/// # Errors
///
/// `DataMapping` se il tipo dichiarato non e binario o l'array concreto non
/// coincide con il tipo Arrow.
pub fn arrow_binary_value<'a>(
    array: &'a dyn Array,
    data_type: &DataType,
    row: usize,
) -> Result<&'a [u8]> {
    match data_type {
        DataType::Binary => Ok(downcast::<BinaryArray>(array)?.value(row)),
        DataType::LargeBinary => Ok(downcast::<LargeBinaryArray>(array)?.value(row)),
        _ => Err(write_error(
            ErrorCategory::DataMapping,
            "campo Arrow binario con tipo incompatibile",
        )),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref().ok_or_else(|| {
        write_error(
            ErrorCategory::DataMapping,
            "array Arrow incoerente con il tipo dichiarato",
        )
    })
}

fn finite(value: f64) -> Result<f64> {
    value.is_finite().then_some(value).ok_or_else(|| {
        write_error(
            ErrorCategory::DataMapping,
            "float non finito non qualificato dal codec write",
        )
    })
}

fn date_text(days: i32) -> Result<String> {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(Duration::days(i64::from(days))))
        .map(|value| value.format("%Y-%m-%d").to_string())
        .ok_or_else(|| write_error(ErrorCategory::DataMapping, "data Arrow fuori range"))
}

fn timestamp_text(micros: i64, separator: char) -> Result<String> {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|value| {
            format!(
                "{}{}{}",
                value.naive_utc().format("%Y-%m-%d"),
                separator,
                value.naive_utc().format("%H:%M:%S%.6f")
            )
        })
        .ok_or_else(|| write_error(ErrorCategory::DataMapping, "timestamp Arrow fuori range"))
}

fn decimal_text(value: i128, scale: i8) -> Result<String> {
    if scale < 0 {
        return Err(write_error(
            ErrorCategory::Unsupported,
            "scala DECIMAL negativa non qualificata dal codec write",
        ));
    }
    let scale = usize::try_from(scale).map_err(|_| {
        write_error(
            ErrorCategory::DataMapping,
            "scala DECIMAL non rappresentabile",
        )
    })?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let negative = value.is_negative();
    let digits = value.unsigned_abs().to_string();
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    Ok(format!(
        "{}{}.{}",
        if negative { "-" } else { "" },
        &padded[..split],
        &padded[split..]
    ))
}

fn write_error(category: ErrorCategory, message: &'static str) -> DatabaseError {
    DatabaseError::new(category, ErrorPhase::Write, None, message)
}
