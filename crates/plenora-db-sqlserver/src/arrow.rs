use crate::error::driver_error;
use crate::types::{SqlServerColumnKind, SqlServerColumnSpec};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use plenora_database_core::arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt8Builder,
};
use plenora_database_core::arrow::array::ArrayRef;
use plenora_database_core::arrow::schema::DataType;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use std::sync::Arc;
use tiberius::{FromSql, Row};

pub enum SqlServerColumnBuffer {
    Bool(BooleanBuilder),
    U8(UInt8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    TimestampTz(StringBuilder),
    Decimal(Decimal128Builder, i8),
}

impl SqlServerColumnBuffer {
    pub(super) fn new(column: &SqlServerColumnSpec, capacity: usize) -> Self {
        match column.kind {
            SqlServerColumnKind::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            SqlServerColumnKind::U8 => Self::U8(UInt8Builder::with_capacity(capacity)),
            SqlServerColumnKind::I16 => Self::I16(Int16Builder::with_capacity(capacity)),
            SqlServerColumnKind::I32 => Self::I32(Int32Builder::with_capacity(capacity)),
            SqlServerColumnKind::I64 => Self::I64(Int64Builder::with_capacity(capacity)),
            SqlServerColumnKind::F32 => Self::F32(Float32Builder::with_capacity(capacity)),
            SqlServerColumnKind::F64 => Self::F64(Float64Builder::with_capacity(capacity)),
            SqlServerColumnKind::Utf8 => Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            )),
            SqlServerColumnKind::Binary
            | SqlServerColumnKind::Geometry
            | SqlServerColumnKind::Geography => Self::Binary(BinaryBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(32),
            )),
            SqlServerColumnKind::Date => Self::Date(Date32Builder::with_capacity(capacity)),
            SqlServerColumnKind::Time => {
                Self::Time(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            SqlServerColumnKind::Timestamp => {
                Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            SqlServerColumnKind::TimestampTz => Self::TimestampTz(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(40),
            )),
            SqlServerColumnKind::Decimal { precision, scale } => Self::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(precision, scale)),
                scale,
            ),
        }
    }

    pub(super) fn append(&mut self, row: &Row, index: usize, cell_limit: u64) -> Result<()> {
        match self {
            Self::Bool(builder) => append_direct(builder, row, index),
            Self::U8(builder) => append_direct(builder, row, index),
            Self::I16(builder) => append_direct(builder, row, index),
            Self::I32(builder) => append_direct(builder, row, index),
            Self::I64(builder) => append_direct(builder, row, index),
            Self::F32(builder) => append_direct(builder, row, index),
            Self::F64(builder) => append_direct(builder, row, index),
            Self::Utf8(builder) => {
                let value: Option<&str> = get(row, index)?;
                enforce_cell(value.map_or(0, str::len), cell_limit)?;
                builder.append_option(value);
                Ok(())
            }
            Self::Binary(builder) => {
                let value: Option<&[u8]> = get(row, index)?;
                enforce_cell(value.map_or(0, <[u8]>::len), cell_limit)?;
                builder.append_option(value);
                Ok(())
            }
            Self::Date(builder) => {
                let value: Option<&str> = get(row, index)?;
                let parsed = value.map(parse_date).transpose()?;
                builder.append_option(parsed);
                Ok(())
            }
            Self::Time(builder) => {
                let value: Option<&str> = get(row, index)?;
                let parsed = value.map(parse_time).transpose()?;
                builder.append_option(parsed);
                Ok(())
            }
            Self::Timestamp(builder) => {
                let value: Option<&str> = get(row, index)?;
                let parsed = value.map(parse_timestamp).transpose()?;
                builder.append_option(parsed);
                Ok(())
            }
            Self::TimestampTz(builder) => {
                let value: Option<&str> = get(row, index)?;
                if let Some(text) = value {
                    validate_timestamp_tz(text)?;
                }
                builder.append_option(value);
                Ok(())
            }
            Self::Decimal(builder, scale) => {
                let value: Option<&str> = get(row, index)?;
                let parsed = value
                    .map(|text| parse_decimal128(text, *scale))
                    .transpose()?;
                builder.append_option(parsed);
                Ok(())
            }
        }
    }

    pub(super) fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::U8(builder) => Arc::new(builder.finish()),
            Self::I16(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::F32(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) | Self::TimestampTz(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::Time(builder) => Arc::new(builder.finish()),
            Self::Timestamp(builder) => Arc::new(builder.finish()),
            Self::Decimal(builder, _) => Arc::new(builder.finish()),
        }
    }
}

trait OptionBuilder<T> {
    fn append_optional(&mut self, value: Option<T>);
}

macro_rules! option_builder {
    ($builder:ty, $value:ty) => {
        impl OptionBuilder<$value> for $builder {
            fn append_optional(&mut self, value: Option<$value>) {
                self.append_option(value);
            }
        }
    };
}

option_builder!(BooleanBuilder, bool);
option_builder!(UInt8Builder, u8);
option_builder!(Int16Builder, i16);
option_builder!(Int32Builder, i32);
option_builder!(Int64Builder, i64);
option_builder!(Float32Builder, f32);
option_builder!(Float64Builder, f64);

fn append_direct<'a, B, T>(builder: &mut B, row: &'a Row, index: usize) -> Result<()>
where
    B: OptionBuilder<T>,
    T: FromSql<'a>,
{
    builder.append_optional(get(row, index)?);
    Ok(())
}

fn get<'a, T>(row: &'a Row, index: usize) -> Result<Option<T>>
where
    T: FromSql<'a>,
{
    row.try_get(index)
        .map_err(|error| driver_error(&error, ErrorPhase::Read, RemoteEffect::None))
}

fn enforce_cell(length: usize, limit: u64) -> Result<()> {
    let length = u64::try_from(length)
        .map_err(|_| DatabaseError::resource_limit("dimensione cella non rappresentabile"))?;
    if length > limit {
        return Err(DatabaseError::resource_limit(
            "cella SQL Server oltre il limite configurato",
        ));
    }
    Ok(())
}

fn parse_date(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| mapping_error("data SQL Server non valida"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| mapping_error("epoch Date32 non rappresentabile"))?;
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| mapping_error("data SQL Server oltre Date32"))
}

fn parse_time(value: &str) -> Result<i64> {
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
        .map_err(|_| mapping_error("ora SQL Server non valida"))?;
    ensure_microsecond_exact(time.nanosecond())?;
    Ok(i64::from(time.num_seconds_from_midnight()) * 1_000_000
        + i64::from(time.nanosecond() / 1_000))
}

fn parse_timestamp(value: &str) -> Result<i64> {
    let timestamp = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|_| mapping_error("timestamp SQL Server non valido"))?;
    ensure_microsecond_exact(timestamp.nanosecond())?;
    Ok(timestamp.and_utc().timestamp_micros())
}

fn validate_timestamp_tz(value: &str) -> Result<()> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| mapping_error("datetimeoffset SQL Server non valido"))?;
    Ok(())
}

fn ensure_microsecond_exact(nanoseconds: u32) -> Result<()> {
    if !nanoseconds.is_multiple_of(1_000) {
        return Err(mapping_error(
            "precisione SQL Server a 100 ns non rappresentabile senza perdita in Arrow microsecond",
        ));
    }
    Ok(())
}

fn parse_decimal128(value: &str, scale: i8) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches(['-', '+']);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if integer.is_empty() && fraction.is_empty() {
        return Err(mapping_error("decimal SQL Server vuoto"));
    }
    let mut digits = String::with_capacity(integer.len().saturating_add(fraction.len()));
    digits.push_str(if integer.is_empty() { "0" } else { integer });
    digits.push_str(fraction);
    let mut parsed = digits
        .parse::<i128>()
        .map_err(|_| mapping_error("decimal SQL Server oltre Decimal128"))?;
    let fraction_length = i32::try_from(fraction.len())
        .map_err(|_| mapping_error("scala decimal SQL Server non valida"))?;
    let exponent = i32::from(scale) - fraction_length;
    if exponent >= 0 {
        let factor = 10_i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| mapping_error("decimal SQL Server oltre Decimal128"))?;
        parsed = parsed
            .checked_mul(factor)
            .ok_or_else(|| mapping_error("decimal SQL Server oltre Decimal128"))?;
    } else {
        let divisor = 10_i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| mapping_error("scala decimal SQL Server non valida"))?;
        if parsed % divisor != 0 {
            return Err(mapping_error(
                "decimal SQL Server non rappresentabile alla scala Arrow",
            ));
        }
        parsed /= divisor;
    }
    Ok(if negative { -parsed } else { parsed })
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Read,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_conversion_is_checked_and_exact() {
        assert_eq!(parse_decimal128("123.45", 4).expect("decimal"), 1_234_500);
        assert_eq!(parse_decimal128("-0.01", 2).expect("decimal"), -1);
        assert!(parse_decimal128("0.001", 2).is_err());
        assert!(parse_decimal128("999999999999999999999999999999999999999", 0).is_err());
    }

    #[test]
    fn temporal_projection_parser_has_checked_boundaries() {
        assert_eq!(parse_date("1970-01-01").expect("epoch"), 0);
        assert_eq!(
            parse_time("23:59:59.1234560").expect("time"),
            86_399_123_456
        );
        assert!(validate_timestamp_tz("2026-01-02T03:04:05.1234567+01:00").is_ok());
        assert!(parse_time("23:59:59.1234567").is_err());
        assert!(parse_timestamp("2026-01-02T03:04:05.1234567").is_err());
        assert!(parse_timestamp("not-a-timestamp").is_err());
    }
}
