use super::{parse_decimal128, usize_to_u64};
use crate::error::{public_error, row_decode_error};
use crate::types::{ColumnKind, ColumnSpec};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, IntervalMonthDayNanoBuilder, ListBuilder,
    StringBuilder, StructBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::IntervalMonthDayNano;
use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::Buf;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use plenora_database_core::protocol;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::Row;

pub fn read_mapping_error(message: &'static str) -> DatabaseError {
    public_error(ErrorCategory::DataMapping, ErrorPhase::Read, false, message)
}
pub fn range_fields() -> Vec<Arc<Field>> {
    vec![
        Arc::new(Field::new("lower", DataType::Utf8, true)),
        Arc::new(Field::new("upper", DataType::Utf8, true)),
        Arc::new(Field::new("lower_inclusive", DataType::Boolean, false)),
        Arc::new(Field::new("upper_inclusive", DataType::Boolean, false)),
        Arc::new(Field::new("lower_unbounded", DataType::Boolean, false)),
        Arc::new(Field::new("upper_unbounded", DataType::Boolean, false)),
        Arc::new(Field::new("empty", DataType::Boolean, false)),
    ]
}

pub enum ColumnBuffer {
    Bool(BooleanBuilder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
    Interval(IntervalMonthDayNanoBuilder),
    Range(StructBuilder),
    Composite(StructBuilder, Vec<String>),
    Timestamp(TimestampMicrosecondBuilder),
    TimestampTz(TimestampMicrosecondBuilder),
    Decimal(Decimal128Builder, i8),
    BoolArray(ListBuilder<BooleanBuilder>),
    I32Array(ListBuilder<Int32Builder>),
    I64Array(ListBuilder<Int64Builder>),
    F32Array(ListBuilder<Float32Builder>),
    F64Array(ListBuilder<Float64Builder>),
    Utf8Array(ListBuilder<StringBuilder>),
}

#[derive(Debug)]
struct PostgresInterval {
    microseconds: i64,
    days: i32,
    months: i32,
}

impl<'a> FromSql<'a> for PostgresInterval {
    fn from_sql(
        _ty: &Type,
        mut raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.remaining() != 16 {
            return Err("invalid PostgreSQL interval payload".into());
        }
        Ok(Self {
            microseconds: raw.get_i64(),
            days: raw.get_i32(),
            months: raw.get_i32(),
        })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INTERVAL
    }
}

impl ColumnBuffer {
    pub fn new(column: &ColumnSpec, capacity: usize) -> Self {
        match column.kind {
            ColumnKind::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnKind::I32 => Self::I32(Int32Builder::with_capacity(capacity)),
            ColumnKind::I64 => Self::I64(Int64Builder::with_capacity(capacity)),
            ColumnKind::F32 => Self::F32(Float32Builder::with_capacity(capacity)),
            ColumnKind::F64 => Self::F64(Float64Builder::with_capacity(capacity)),
            ColumnKind::Utf8 => Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            )),
            ColumnKind::Binary | ColumnKind::Geometry | ColumnKind::Geography => Self::Binary(
                BinaryBuilder::with_capacity(capacity, capacity.saturating_mul(32)),
            ),
            ColumnKind::Date => Self::Date(Date32Builder::with_capacity(capacity)),
            ColumnKind::Time => Self::Time(Time64MicrosecondBuilder::with_capacity(capacity)),
            ColumnKind::Interval => {
                Self::Interval(IntervalMonthDayNanoBuilder::with_capacity(capacity))
            }
            ColumnKind::Range => Self::Range(StructBuilder::from_fields(range_fields(), capacity)),
            ColumnKind::Composite => {
                let names = column
                    .composite_fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect::<Vec<_>>();
                let fields = names
                    .iter()
                    .zip(&column.composite_fields)
                    .map(|(name, composite_field)| {
                        let mut metadata = HashMap::new();
                        metadata.insert(
                            protocol::POSTGRES_NATIVE_DECLARATION.to_owned(),
                            composite_field.declaration.clone(),
                        );
                        Arc::new(Field::new(name, DataType::Utf8, true).with_metadata(metadata))
                    })
                    .collect::<Vec<_>>();
                Self::Composite(StructBuilder::from_fields(fields, capacity), names)
            }
            ColumnKind::Timestamp => {
                Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            ColumnKind::TimestampTz => Self::TimestampTz(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_data_type(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ),
            ),
            ColumnKind::Decimal { precision, scale } => Self::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(precision, scale)),
                scale,
            ),
            ColumnKind::BoolArray => Self::BoolArray(ListBuilder::new(
                BooleanBuilder::with_capacity(capacity.saturating_mul(4)),
            )),
            ColumnKind::I32Array => Self::I32Array(ListBuilder::new(Int32Builder::with_capacity(
                capacity.saturating_mul(4),
            ))),
            ColumnKind::I64Array => Self::I64Array(ListBuilder::new(Int64Builder::with_capacity(
                capacity.saturating_mul(4),
            ))),
            ColumnKind::F32Array => Self::F32Array(ListBuilder::new(
                Float32Builder::with_capacity(capacity.saturating_mul(4)),
            )),
            ColumnKind::F64Array => Self::F64Array(ListBuilder::new(
                Float64Builder::with_capacity(capacity.saturating_mul(4)),
            )),
            ColumnKind::Utf8Array => {
                Self::Utf8Array(ListBuilder::new(StringBuilder::with_capacity(
                    capacity.saturating_mul(4),
                    capacity.saturating_mul(32),
                )))
            }
        }
    }

    pub fn append(&mut self, row: &Row, index: usize) -> Result<u64> {
        match self {
            Self::Bool(builder) => append_option(builder, row.try_get(index)),
            Self::I32(builder) => append_option(builder, row.try_get(index)),
            Self::I64(builder) => append_option(builder, row.try_get(index)),
            Self::F32(builder) => append_option(builder, row.try_get(index)),
            Self::F64(builder) => append_option(builder, row.try_get(index)),
            Self::Utf8(builder) => append_option(builder, row.try_get(index)),
            Self::Binary(builder) => append_option(builder, row.try_get(index)),
            Self::Date(builder) => {
                let value: Option<NaiveDate> = row.try_get(index).map_err(row_decode_error)?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| read_mapping_error("epoch PostgreSQL non rappresentabile"))?;
                let arrow_date = value
                    .map(|date| {
                        i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(|_| {
                            read_mapping_error("data PostgreSQL oltre il range Arrow Date32")
                        })
                    })
                    .transpose()?;
                builder.append_option(arrow_date);
                Ok(5)
            }
            Self::Time(builder) => {
                let value: Option<chrono::NaiveTime> =
                    row.try_get(index).map_err(row_decode_error)?;
                builder.append_option(value.map(|time| {
                    i64::from(time.num_seconds_from_midnight()) * 1_000_000
                        + i64::from(time.nanosecond() / 1_000)
                }));
                Ok(9)
            }
            Self::Interval(builder) => {
                let value: Option<PostgresInterval> =
                    row.try_get(index).map_err(row_decode_error)?;
                let value = value
                    .map(|interval| {
                        interval
                            .microseconds
                            .checked_mul(1_000)
                            .map(|nanoseconds| {
                                IntervalMonthDayNano::new(
                                    interval.months,
                                    interval.days,
                                    nanoseconds,
                                )
                            })
                            .ok_or_else(|| {
                                public_error(
                                    ErrorCategory::DataMapping,
                                    ErrorPhase::Read,
                                    false,
                                    "interval PostgreSQL oltre il range Arrow nanosecond",
                                )
                            })
                    })
                    .transpose()?;
                builder.append_option(value);
                Ok(17)
            }
            Self::Range(builder) => append_range(builder, row.try_get(index)),
            Self::Composite(builder, names) => append_composite(builder, names, row.try_get(index)),
            Self::Timestamp(builder) => {
                let value: Option<NaiveDateTime> = row.try_get(index).map_err(row_decode_error)?;
                builder
                    .append_option(value.map(|timestamp| timestamp.and_utc().timestamp_micros()));
                Ok(9)
            }
            Self::TimestampTz(builder) => {
                let value: Option<DateTime<Utc>> = row.try_get(index).map_err(row_decode_error)?;
                builder.append_option(value.map(|timestamp| timestamp.timestamp_micros()));
                Ok(9)
            }
            Self::Decimal(builder, scale) => {
                let value: Option<String> = row.try_get(index).map_err(row_decode_error)?;
                let parsed = value
                    .as_deref()
                    .map(|text| parse_decimal128(text, *scale))
                    .transpose()?;
                builder.append_option(parsed);
                Ok(17)
            }
            Self::BoolArray(builder) => append_list(builder, row.try_get(index)),
            Self::I32Array(builder) => append_list(builder, row.try_get(index)),
            Self::I64Array(builder) => append_list(builder, row.try_get(index)),
            Self::F32Array(builder) => append_list(builder, row.try_get(index)),
            Self::F64Array(builder) => append_list(builder, row.try_get(index)),
            Self::Utf8Array(builder) => append_string_list(builder, row.try_get(index)),
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::F32(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::Time(builder) => Arc::new(builder.finish()),
            Self::Interval(builder) => Arc::new(builder.finish()),
            Self::Range(builder) | Self::Composite(builder, _) => Arc::new(builder.finish()),
            Self::Timestamp(builder) | Self::TimestampTz(builder) => Arc::new(builder.finish()),
            Self::Decimal(builder, _) => Arc::new(builder.finish()),
            Self::BoolArray(builder) => Arc::new(builder.finish()),
            Self::I32Array(builder) => Arc::new(builder.finish()),
            Self::I64Array(builder) => Arc::new(builder.finish()),
            Self::F32Array(builder) => Arc::new(builder.finish()),
            Self::F64Array(builder) => Arc::new(builder.finish()),
            Self::Utf8Array(builder) => Arc::new(builder.finish()),
        }
    }
}

fn append_range(
    builder: &mut StructBuilder,
    value: std::result::Result<Option<String>, tokio_postgres::Error>,
) -> Result<u64> {
    let value = value.map_err(row_decode_error)?;
    let estimated_bytes = value
        .as_ref()
        .map_or(16, |text| 16_u64.saturating_add(usize_to_u64(text.len())));
    let document = value
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()
        .map_err(|_| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "range PostgreSQL strutturato non valido",
            )
        })?;
    let lower = document
        .as_ref()
        .and_then(|item| item.get("lower"))
        .and_then(serde_json::Value::as_str);
    let upper = document
        .as_ref()
        .and_then(|item| item.get("upper"))
        .and_then(serde_json::Value::as_str);
    builder
        .field_builder::<StringBuilder>(0)
        .ok_or_else(|| read_mapping_error("builder range lower incompatibile"))?
        .append_option(lower);
    builder
        .field_builder::<StringBuilder>(1)
        .ok_or_else(|| read_mapping_error("builder range upper incompatibile"))?
        .append_option(upper);
    for (index, key) in [
        (2, "lower_inclusive"),
        (3, "upper_inclusive"),
        (4, "lower_unbounded"),
        (5, "upper_unbounded"),
        (6, "empty"),
    ] {
        builder
            .field_builder::<BooleanBuilder>(index)
            .ok_or_else(|| read_mapping_error("builder range boolean incompatibile"))?
            .append_value(
                document
                    .as_ref()
                    .and_then(|item| item.get(key))
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            );
    }
    builder.append(document.is_some());
    Ok(estimated_bytes)
}

fn append_composite(
    builder: &mut StructBuilder,
    names: &[String],
    value: std::result::Result<Option<String>, tokio_postgres::Error>,
) -> Result<u64> {
    let value = value.map_err(row_decode_error)?;
    let estimated_bytes = value.as_ref().map_or_else(
        || usize_to_u64(names.len()).saturating_mul(5),
        |text| usize_to_u64(text.len()).saturating_add(usize_to_u64(names.len()).saturating_mul(5)),
    );
    let document = value
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()
        .map_err(|_| {
            public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "composite PostgreSQL JSON non valido",
            )
        })?;
    for (index, name) in names.iter().enumerate() {
        let value = document
            .as_ref()
            .and_then(|item| item.get(name))
            .filter(|item| !item.is_null())
            .map(|item| match item {
                serde_json::Value::String(value) => value.clone(),
                _ => item.to_string(),
            });
        builder
            .field_builder::<StringBuilder>(index)
            .ok_or_else(|| read_mapping_error("builder composite incompatibile"))?
            .append_option(value);
    }
    builder.append(document.is_some());
    Ok(estimated_bytes)
}

fn append_list<T, B>(
    builder: &mut ListBuilder<B>,
    value: std::result::Result<Option<Vec<Option<T>>>, tokio_postgres::Error>,
) -> Result<u64>
where
    B: arrow_array::builder::ArrayBuilder + AppendOption<T>,
    T: EstimatedArrowBytes,
{
    let estimated_bytes = if let Some(values) = value.map_err(row_decode_error)? {
        let estimated = values.iter().fold(5_u64, |total, value| {
            total.saturating_add(
                1 + value
                    .as_ref()
                    .map_or(T::NULL_BYTES, EstimatedArrowBytes::estimated_arrow_bytes),
            )
        });
        for value in values {
            builder.values().append_optional(value);
        }
        builder.append(true);
        estimated
    } else {
        builder.append(false);
        5
    };
    Ok(estimated_bytes)
}

fn append_string_list(
    builder: &mut ListBuilder<StringBuilder>,
    value: std::result::Result<Option<Vec<Option<String>>>, tokio_postgres::Error>,
) -> Result<u64> {
    let estimated_bytes = if let Some(values) = value.map_err(row_decode_error)? {
        let estimated = values.iter().fold(5_u64, |total, value| {
            total.saturating_add(
                1 + value.as_ref().map_or(
                    String::NULL_BYTES,
                    EstimatedArrowBytes::estimated_arrow_bytes,
                ),
            )
        });
        for value in values {
            builder.values().append_option(value);
        }
        builder.append(true);
        estimated
    } else {
        builder.append(false);
        5
    };
    Ok(estimated_bytes)
}

trait EstimatedArrowBytes {
    const NULL_BYTES: u64;

    fn estimated_arrow_bytes(&self) -> u64;
}

macro_rules! impl_fixed_arrow_bytes {
    ($value:ty, $bytes:expr) => {
        impl EstimatedArrowBytes for $value {
            const NULL_BYTES: u64 = $bytes;

            fn estimated_arrow_bytes(&self) -> u64 {
                $bytes
            }
        }
    };
}

impl_fixed_arrow_bytes!(bool, 1);
impl_fixed_arrow_bytes!(i32, 4);
impl_fixed_arrow_bytes!(i64, 8);
impl_fixed_arrow_bytes!(f32, 4);
impl_fixed_arrow_bytes!(f64, 8);

impl EstimatedArrowBytes for String {
    const NULL_BYTES: u64 = 4;

    fn estimated_arrow_bytes(&self) -> u64 {
        4_u64.saturating_add(usize_to_u64(self.len()))
    }
}

impl EstimatedArrowBytes for Vec<u8> {
    const NULL_BYTES: u64 = 4;

    fn estimated_arrow_bytes(&self) -> u64 {
        4_u64.saturating_add(usize_to_u64(self.len()))
    }
}

trait AppendOption<T> {
    fn append_optional(&mut self, value: Option<T>);
}

macro_rules! impl_append_option {
    ($builder:ty, $value:ty) => {
        impl AppendOption<$value> for $builder {
            fn append_optional(&mut self, value: Option<$value>) {
                self.append_option(value);
            }
        }
    };
}

impl_append_option!(BooleanBuilder, bool);
impl_append_option!(Int32Builder, i32);
impl_append_option!(Int64Builder, i64);
impl_append_option!(Float32Builder, f32);
impl_append_option!(Float64Builder, f64);
impl_append_option!(StringBuilder, String);
impl_append_option!(BinaryBuilder, Vec<u8>);

fn append_option<B, T>(
    builder: &mut B,
    value: std::result::Result<Option<T>, tokio_postgres::Error>,
) -> Result<u64>
where
    B: AppendOption<T>,
    T: EstimatedArrowBytes,
{
    let value = value.map_err(row_decode_error)?;
    let estimated_bytes = 1 + value
        .as_ref()
        .map_or(T::NULL_BYTES, EstimatedArrowBytes::estimated_arrow_bytes);
    builder.append_optional(value);
    Ok(estimated_bytes)
}
