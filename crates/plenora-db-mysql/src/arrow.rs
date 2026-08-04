use crate::{MysqlColumnKind, MysqlColumnSpec};
use chrono::NaiveDate;
use mysql_async::{Row, Value};
use plenora_database_core::arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use plenora_database_core::arrow::array::ArrayRef;
use plenora_database_core::arrow::schema::DataType;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use std::sync::Arc;

pub enum MysqlColumnBuffer {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    U8(UInt8Builder),
    I16(Int16Builder),
    U16(UInt16Builder),
    I32(Int32Builder),
    U32(UInt32Builder),
    I64(Int64Builder),
    U64(UInt64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    TimeText(StringBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Decimal(Decimal128Builder, i8),
}

impl MysqlColumnBuffer {
    #[must_use]
    pub fn new(column: &MysqlColumnSpec, capacity: usize) -> Self {
        match column.kind {
            MysqlColumnKind::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            MysqlColumnKind::I8 => Self::I8(Int8Builder::with_capacity(capacity)),
            MysqlColumnKind::U8 => Self::U8(UInt8Builder::with_capacity(capacity)),
            MysqlColumnKind::I16 => Self::I16(Int16Builder::with_capacity(capacity)),
            MysqlColumnKind::U16 => Self::U16(UInt16Builder::with_capacity(capacity)),
            MysqlColumnKind::I32 => Self::I32(Int32Builder::with_capacity(capacity)),
            MysqlColumnKind::U32 => Self::U32(UInt32Builder::with_capacity(capacity)),
            MysqlColumnKind::I64 => Self::I64(Int64Builder::with_capacity(capacity)),
            MysqlColumnKind::U64 => Self::U64(UInt64Builder::with_capacity(capacity)),
            MysqlColumnKind::F32 => Self::F32(Float32Builder::with_capacity(capacity)),
            MysqlColumnKind::F64 => Self::F64(Float64Builder::with_capacity(capacity)),
            MysqlColumnKind::Utf8 => Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            )),
            MysqlColumnKind::Binary | MysqlColumnKind::Geometry => Self::Binary(
                BinaryBuilder::with_capacity(capacity, capacity.saturating_mul(32)),
            ),
            MysqlColumnKind::Date => Self::Date(Date32Builder::with_capacity(capacity)),
            MysqlColumnKind::Time => Self::TimeText(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            )),
            MysqlColumnKind::Timestamp => {
                Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            MysqlColumnKind::Decimal { precision, scale } => Self::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(precision, scale)),
                scale,
            ),
        }
    }

    /// Aggiunge una cella verificando tipo wire e limite dimensionale.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per conversioni, overflow o celle oltre il limite.
    pub fn append(&mut self, row: &Row, index: usize, cell_limit: u64) -> Result<()> {
        let value = row
            .as_ref(index)
            .ok_or_else(|| mapping_error("riga MySQL con meno colonne del piano dichiarato"))?;
        match self {
            Self::Bool(builder) => append_bool(builder, value),
            Self::I8(builder) => append_signed(builder, value, i8::try_from),
            Self::U8(builder) => append_unsigned(builder, value, u8::try_from),
            Self::I16(builder) => append_signed(builder, value, i16::try_from),
            Self::U16(builder) => append_unsigned(builder, value, u16::try_from),
            Self::I32(builder) => append_signed(builder, value, i32::try_from),
            Self::U32(builder) => append_unsigned(builder, value, u32::try_from),
            Self::I64(builder) => append_signed(builder, value, |value| {
                Ok::<_, std::convert::Infallible>(value)
            }),
            Self::U64(builder) => append_unsigned(builder, value, |value| {
                Ok::<_, std::convert::Infallible>(value)
            }),
            Self::F32(builder) => append_f32(builder, value),
            Self::F64(builder) => append_f64(builder, value),
            Self::Utf8(builder) => append_utf8(builder, value, cell_limit),
            Self::Binary(builder) => append_binary(builder, value, cell_limit),
            Self::Date(builder) => append_date(builder, value),
            Self::TimeText(builder) => append_time(builder, value),
            Self::Timestamp(builder) => append_timestamp(builder, value),
            Self::Decimal(builder, scale) => append_decimal(builder, value, *scale, cell_limit),
        }
    }

    #[must_use]
    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::I8(builder) => Arc::new(builder.finish()),
            Self::U8(builder) => Arc::new(builder.finish()),
            Self::I16(builder) => Arc::new(builder.finish()),
            Self::U16(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::U32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::U64(builder) => Arc::new(builder.finish()),
            Self::F32(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) | Self::TimeText(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::Timestamp(builder) => Arc::new(builder.finish()),
            Self::Decimal(builder, _) => Arc::new(builder.finish()),
        }
    }
}

trait OptionalBuilder<T> {
    fn append_optional(&mut self, value: Option<T>);
}

macro_rules! optional_builder {
    ($builder:ty, $value:ty) => {
        impl OptionalBuilder<$value> for $builder {
            fn append_optional(&mut self, value: Option<$value>) {
                self.append_option(value);
            }
        }
    };
}

optional_builder!(Int8Builder, i8);
optional_builder!(UInt8Builder, u8);
optional_builder!(Int16Builder, i16);
optional_builder!(UInt16Builder, u16);
optional_builder!(Int32Builder, i32);
optional_builder!(UInt32Builder, u32);
optional_builder!(Int64Builder, i64);
optional_builder!(UInt64Builder, u64);

fn append_signed<B, T, E>(
    builder: &mut B,
    value: &Value,
    convert: impl FnOnce(i64) -> std::result::Result<T, E>,
) -> Result<()>
where
    B: OptionalBuilder<T>,
{
    let raw = match value {
        Value::NULL => None,
        Value::Int(value) => Some(*value),
        Value::UInt(value) => Some(i64::try_from(*value).map_err(|_| numeric_overflow())?),
        _ => return Err(wire_type_error()),
    };
    let converted = raw
        .map(|value| convert(value).map_err(|_| numeric_overflow()))
        .transpose()?;
    builder.append_optional(converted);
    Ok(())
}

fn append_unsigned<B, T, E>(
    builder: &mut B,
    value: &Value,
    convert: impl FnOnce(u64) -> std::result::Result<T, E>,
) -> Result<()>
where
    B: OptionalBuilder<T>,
{
    let raw = match value {
        Value::NULL => None,
        Value::UInt(value) => Some(*value),
        Value::Int(value) => Some(u64::try_from(*value).map_err(|_| numeric_overflow())?),
        _ => return Err(wire_type_error()),
    };
    let converted = raw
        .map(|value| convert(value).map_err(|_| numeric_overflow()))
        .transpose()?;
    builder.append_optional(converted);
    Ok(())
}

fn append_bool(builder: &mut BooleanBuilder, value: &Value) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Int(0) | Value::UInt(0) => builder.append_value(false),
        Value::Int(1) | Value::UInt(1) => builder.append_value(true),
        _ => return Err(mapping_error("boolean MySQL diverso da 0 o 1")),
    }
    Ok(())
}

fn append_f32(builder: &mut Float32Builder, value: &Value) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Float(value) if value.is_finite() => builder.append_value(*value),
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn append_f64(builder: &mut Float64Builder, value: &Value) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Double(value) if value.is_finite() => builder.append_value(*value),
        Value::Float(value) if value.is_finite() => builder.append_value(f64::from(*value)),
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn append_utf8(builder: &mut StringBuilder, value: &Value, cell_limit: u64) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Bytes(bytes) => {
            enforce_cell(bytes.len(), cell_limit)?;
            let text =
                std::str::from_utf8(bytes).map_err(|_| mapping_error("testo MySQL non UTF-8"))?;
            builder.append_value(text);
        }
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn append_binary(builder: &mut BinaryBuilder, value: &Value, cell_limit: u64) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Bytes(bytes) => {
            enforce_cell(bytes.len(), cell_limit)?;
            builder.append_value(bytes);
        }
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn append_date(builder: &mut Date32Builder, value: &Value) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Date(year, month, day, 0, 0, 0, 0) => {
            let date =
                NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day))
                    .ok_or_else(|| mapping_error("data MySQL fuori intervallo Arrow"))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| mapping_error("epoch data non rappresentabile"))?;
            let days = i32::try_from(date.signed_duration_since(epoch).num_days())
                .map_err(|_| mapping_error("data MySQL fuori Date32"))?;
            builder.append_value(days);
        }
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn append_timestamp(builder: &mut TimestampMicrosecondBuilder, value: &Value) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            let date =
                NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day))
                    .ok_or_else(|| mapping_error("timestamp MySQL con data non valida"))?;
            let timestamp = date
                .and_hms_micro_opt(
                    u32::from(*hour),
                    u32::from(*minute),
                    u32::from(*second),
                    *micros,
                )
                .ok_or_else(|| mapping_error("timestamp MySQL fuori intervallo"))?;
            builder.append_value(timestamp.and_utc().timestamp_micros());
        }
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn append_time(builder: &mut StringBuilder, value: &Value) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let total_hours = u64::from(*days)
                .checked_mul(24)
                .and_then(|value| value.checked_add(u64::from(*hours)))
                .ok_or_else(|| mapping_error("TIME MySQL in overflow"))?;
            if *minutes > 59 || *seconds > 59 || *micros > 999_999 {
                return Err(mapping_error("TIME MySQL non canonico"));
            }
            let sign = if *negative { "-" } else { "" };
            builder.append_value(format!(
                "{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            ));
        }
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn append_decimal(
    builder: &mut Decimal128Builder,
    value: &Value,
    scale: i8,
    cell_limit: u64,
) -> Result<()> {
    match value {
        Value::NULL => builder.append_null(),
        Value::Bytes(bytes) => {
            enforce_cell(bytes.len(), cell_limit)?;
            let text =
                std::str::from_utf8(bytes).map_err(|_| mapping_error("decimal MySQL non ASCII"))?;
            builder.append_value(parse_decimal128(text, scale)?);
        }
        _ => return Err(wire_type_error()),
    }
    Ok(())
}

fn parse_decimal128(text: &str, scale: i8) -> Result<i128> {
    let scale = usize::try_from(scale).map_err(|_| mapping_error("scala decimal negativa"))?;
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |value| (true, value));
    let (whole, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
        || fractional.len() > scale
    {
        return Err(mapping_error("decimal MySQL non canonico"));
    }
    let mut digits = String::with_capacity(whole.len().saturating_add(scale));
    digits.push_str(whole);
    digits.push_str(fractional);
    digits.extend(std::iter::repeat_n('0', scale - fractional.len()));
    let magnitude = digits
        .parse::<i128>()
        .map_err(|_| mapping_error("decimal MySQL oltre i128"))?;
    if negative {
        magnitude
            .checked_neg()
            .ok_or_else(|| mapping_error("decimal MySQL in overflow"))
    } else {
        Ok(magnitude)
    }
}

fn enforce_cell(length: usize, limit: u64) -> Result<()> {
    let length = u64::try_from(length)
        .map_err(|_| DatabaseError::resource_limit("cella MySQL non rappresentabile"))?;
    if length > limit {
        return Err(DatabaseError::resource_limit(
            "cella MySQL oltre il limite configurato",
        ));
    }
    Ok(())
}

fn numeric_overflow() -> DatabaseError {
    mapping_error("intero MySQL fuori intervallo Arrow")
}

fn wire_type_error() -> DatabaseError {
    mapping_error("tipo wire MySQL diverso dal piano")
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    mapping_error_owned(message.into())
}

const fn mapping_error_owned(message: String) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Read,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message,
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_parser_is_exact_and_checked() {
        assert_eq!(parse_decimal128("1234.5", 4).expect("decimal"), 12_345_000);
        assert_eq!(parse_decimal128("-0.25", 2).expect("negative"), -25);
        assert!(parse_decimal128("1.234", 2).is_err());
        assert!(parse_decimal128("NaN", 2).is_err());
    }

    #[test]
    fn zero_dates_fail_closed_without_panicking() {
        let mut builder = Date32Builder::new();
        assert_eq!(
            append_date(&mut builder, &Value::Date(0, 0, 0, 0, 0, 0, 0))
                .expect_err("zero date")
                .category,
            ErrorCategory::DataMapping
        );
    }
}
