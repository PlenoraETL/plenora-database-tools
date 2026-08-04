use crate::error::driver_error;
use crate::types::{SqlServerColumnKind, SqlServerColumnSpec, SqlServerWireEncoding};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
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
use std::fmt::Write as _;
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
    Utf8(StringBuilder, Utf8Source),
    Binary(BinaryBuilder),
    Date(Date32Builder, SqlServerWireEncoding),
    Time(Time64MicrosecondBuilder, SqlServerWireEncoding),
    Timestamp(TimestampMicrosecondBuilder, SqlServerWireEncoding),
    TimestampTz(StringBuilder, SqlServerWireEncoding, u8),
    Decimal(Decimal128Builder, i8, SqlServerWireEncoding),
}

#[derive(Debug, Clone, Copy)]
pub enum Utf8Source {
    Text,
    Uuid,
    Xml,
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
            SqlServerColumnKind::Utf8 => {
                let source = match (column.wire_encoding, column.native_type.as_str()) {
                    (SqlServerWireEncoding::Native, "uniqueidentifier") => Utf8Source::Uuid,
                    (SqlServerWireEncoding::Native, "xml") => Utf8Source::Xml,
                    _ => Utf8Source::Text,
                };
                Self::Utf8(
                    StringBuilder::with_capacity(capacity, capacity.saturating_mul(16)),
                    source,
                )
            }
            SqlServerColumnKind::Binary
            | SqlServerColumnKind::Geometry
            | SqlServerColumnKind::Geography => Self::Binary(BinaryBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(32),
            )),
            SqlServerColumnKind::Date => {
                Self::Date(Date32Builder::with_capacity(capacity), column.wire_encoding)
            }
            SqlServerColumnKind::Time => Self::Time(
                Time64MicrosecondBuilder::with_capacity(capacity),
                column.wire_encoding,
            ),
            SqlServerColumnKind::Timestamp => Self::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity),
                column.wire_encoding,
            ),
            SqlServerColumnKind::TimestampTz => Self::TimestampTz(
                StringBuilder::with_capacity(capacity, capacity.saturating_mul(40)),
                column.wire_encoding,
                column.native_scale().unwrap_or(7),
            ),
            SqlServerColumnKind::Decimal { precision, scale } => Self::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(precision, scale)),
                scale,
                column.wire_encoding,
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
            Self::Utf8(builder, source) => append_utf8(builder, *source, row, index, cell_limit),
            Self::Binary(builder) => {
                let value: Option<&[u8]> = get(row, index)?;
                enforce_cell(value.map_or(0, <[u8]>::len), cell_limit)?;
                builder.append_option(value);
                Ok(())
            }
            Self::Date(builder, encoding) => {
                let parsed = match encoding {
                    SqlServerWireEncoding::Projected => {
                        let value: Option<&str> = get(row, index)?;
                        value.map(parse_date).transpose()?
                    }
                    SqlServerWireEncoding::Native => {
                        let value: Option<NaiveDate> = get(row, index)?;
                        value.map(date32).transpose()?
                    }
                };
                builder.append_option(parsed);
                Ok(())
            }
            Self::Time(builder, encoding) => {
                let parsed = match encoding {
                    SqlServerWireEncoding::Projected => {
                        let value: Option<&str> = get(row, index)?;
                        value.map(parse_time).transpose()?
                    }
                    SqlServerWireEncoding::Native => {
                        let value: Option<NaiveTime> = get(row, index)?;
                        value.map(time_micros).transpose()?
                    }
                };
                builder.append_option(parsed);
                Ok(())
            }
            Self::Timestamp(builder, encoding) => {
                let parsed = match encoding {
                    SqlServerWireEncoding::Projected => {
                        let value: Option<&str> = get(row, index)?;
                        value.map(parse_timestamp).transpose()?
                    }
                    SqlServerWireEncoding::Native => {
                        let value: Option<NaiveDateTime> = get(row, index)?;
                        value.map(timestamp_micros).transpose()?
                    }
                };
                builder.append_option(parsed);
                Ok(())
            }
            Self::TimestampTz(builder, encoding, scale) => {
                match encoding {
                    SqlServerWireEncoding::Projected => {
                        let value: Option<&str> = get(row, index)?;
                        if let Some(text) = value {
                            validate_timestamp_tz(text)?;
                        }
                        builder.append_option(value);
                    }
                    SqlServerWireEncoding::Native => {
                        let value: Option<DateTime<FixedOffset>> = get(row, index)?;
                        let encoded = value
                            .map(|value| format_timestamp_tz(value, *scale))
                            .transpose()?;
                        builder.append_option(encoded.as_deref());
                    }
                }
                Ok(())
            }
            Self::Decimal(builder, scale, encoding) => {
                let parsed = match encoding {
                    SqlServerWireEncoding::Projected => {
                        let value: Option<&str> = get(row, index)?;
                        value
                            .map(|text| parse_decimal128(text, *scale))
                            .transpose()?
                    }
                    SqlServerWireEncoding::Native => {
                        let value: Option<tiberius::numeric::Numeric> = get(row, index)?;
                        value
                            .map(|numeric| rescale_numeric(numeric, *scale))
                            .transpose()?
                    }
                };
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
            Self::Utf8(builder, _) | Self::TimestampTz(builder, _, _) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Date(builder, _) => Arc::new(builder.finish()),
            Self::Time(builder, _) => Arc::new(builder.finish()),
            Self::Timestamp(builder, _) => Arc::new(builder.finish()),
            Self::Decimal(builder, _, _) => Arc::new(builder.finish()),
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

fn append_utf8(
    builder: &mut StringBuilder,
    source: Utf8Source,
    row: &Row,
    index: usize,
    cell_limit: u64,
) -> Result<()> {
    match source {
        Utf8Source::Text => {
            let value: Option<&str> = get(row, index)?;
            enforce_cell(value.map_or(0, str::len), cell_limit)?;
            builder.append_option(value);
        }
        Utf8Source::Uuid => {
            let value: Option<tiberius::Uuid> = get(row, index)?;
            let encoded = value.map(|value| value.hyphenated().to_string());
            enforce_cell(encoded.as_deref().map_or(0, str::len), cell_limit)?;
            builder.append_option(encoded.as_deref());
        }
        Utf8Source::Xml => {
            let value: Option<&tiberius::xml::XmlData> = get(row, index)?;
            let text = value.map(AsRef::<str>::as_ref);
            enforce_cell(text.map_or(0, str::len), cell_limit)?;
            builder.append_option(text);
        }
    }
    Ok(())
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
    date32(date)
}

fn date32(date: NaiveDate) -> Result<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| mapping_error("epoch Date32 non rappresentabile"))?;
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| mapping_error("data SQL Server oltre Date32"))
}

fn parse_time(value: &str) -> Result<i64> {
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
        .map_err(|_| mapping_error("ora SQL Server non valida"))?;
    time_micros(time)
}

fn time_micros(time: NaiveTime) -> Result<i64> {
    ensure_microsecond_exact(time.nanosecond())?;
    Ok(i64::from(time.num_seconds_from_midnight()) * 1_000_000
        + i64::from(time.nanosecond() / 1_000))
}

fn parse_timestamp(value: &str) -> Result<i64> {
    let timestamp = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|_| mapping_error("timestamp SQL Server non valido"))?;
    timestamp_micros(timestamp)
}

fn timestamp_micros(timestamp: NaiveDateTime) -> Result<i64> {
    ensure_microsecond_exact(timestamp.nanosecond())?;
    Ok(timestamp.and_utc().timestamp_micros())
}

fn validate_timestamp_tz(value: &str) -> Result<()> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| mapping_error("datetimeoffset SQL Server non valido"))?;
    Ok(())
}

fn format_timestamp_tz(value: DateTime<FixedOffset>, scale: u8) -> Result<String> {
    if scale > 7 {
        return Err(mapping_error(
            "scala datetimeoffset SQL Server oltre il profilo supportato",
        ));
    }
    let mut encoded = value.format("%Y-%m-%dT%H:%M:%S").to_string();
    if scale > 0 {
        let divisor = 10_u32.pow(u32::from(9 - scale));
        let fraction = value.nanosecond() / divisor;
        encoded.push('.');
        write!(encoded, "{fraction:0width$}", width = usize::from(scale))
            .map_err(|_| mapping_error("datetimeoffset SQL Server non serializzabile"))?;
    }
    encoded.push_str(&value.format("%:z").to_string());
    Ok(encoded)
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

fn rescale_numeric(value: tiberius::numeric::Numeric, scale: i8) -> Result<i128> {
    let source_scale = i32::from(value.scale());
    let target_scale = i32::from(scale);
    let exponent = target_scale - source_scale;
    let mut parsed = value.value();
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
    Ok(parsed)
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
        diagnostics: None,
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

    #[test]
    fn native_numeric_rescaling_is_exact_and_checked() {
        let value = tiberius::numeric::Numeric::new_with_scale(12_345, 2);
        assert_eq!(rescale_numeric(value, 4).expect("upscale"), 1_234_500);
        let exact = tiberius::numeric::Numeric::new_with_scale(12_300, 4);
        assert_eq!(rescale_numeric(exact, 2).expect("downscale"), 123);
        let inexact = tiberius::numeric::Numeric::new_with_scale(12_301, 4);
        assert!(rescale_numeric(inexact, 2).is_err());
    }

    #[test]
    fn native_datetimeoffset_formatter_preserves_declared_scale_and_offset() {
        let value = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.1234567+01:00")
            .expect("datetimeoffset");
        assert_eq!(
            format_timestamp_tz(value, 7).expect("format"),
            "2026-01-02T03:04:05.1234567+01:00"
        );
        assert!(format_timestamp_tz(value, 8).is_err());
    }
}
