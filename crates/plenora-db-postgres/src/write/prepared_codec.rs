use super::{
    binary_codec::{decimal_string, postgres_interval},
    mapping_error,
    plan::{ColumnSemantics, WriteColumnPlan},
    temporal_range_error,
    value_codec::{composite_string, list_string, range_string, time_from_microseconds},
};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, IntervalMonthDayNanoArray, ListArray, StringArray, StructArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use plenora_database_core::Result;
use tokio_postgres::types::ToSql;

#[allow(clippy::too_many_lines)]
pub(super) fn arrow_value(
    array: &dyn Array,
    plan: &WriteColumnPlan,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>> {
    macro_rules! scalar {
        ($array:ty, $value:expr) => {{
            let typed = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| $value(typed, row)))
                as Box<dyn ToSql + Sync + Send>
        }};
    }
    Ok(match &plan.data_type {
        DataType::Boolean => scalar!(BooleanArray, |a: &BooleanArray, i| a.value(i)),
        DataType::Int32 => scalar!(Int32Array, |a: &Int32Array, i| a.value(i)),
        DataType::Int64 => scalar!(Int64Array, |a: &Int64Array, i| a.value(i)),
        DataType::Float32 => scalar!(Float32Array, |a: &Float32Array, i| a.value(i)),
        DataType::Float64 => scalar!(Float64Array, |a: &Float64Array, i| a.value(i)),
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| typed.value(row).to_owned()))
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| typed.value(row).to_vec()))
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(mapping_error)?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(temporal_range_error)?;
            let value = (!typed.is_null(row))
                .then(|| {
                    epoch
                        .checked_add_signed(Duration::days(i64::from(typed.value(row))))
                        .ok_or_else(temporal_range_error)
                })
                .transpose()?;
            Box::new(value)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let typed = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| time_from_microseconds(typed.value(row)))
                .transpose()?;
            Box::new(value)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let typed = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| postgres_interval(typed, row))
                .transpose()?;
            Box::new(value)
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(mapping_error)?;
            if timezone.is_some() {
                let value = (!typed.is_null(row))
                    .then(|| {
                        DateTime::<Utc>::from_timestamp_micros(typed.value(row))
                            .ok_or_else(temporal_range_error)
                    })
                    .transpose()?;
                Box::new(value)
            } else {
                let value = (!typed.is_null(row))
                    .then(|| {
                        DateTime::<Utc>::from_timestamp_micros(typed.value(row))
                            .map(|instant| instant.naive_utc())
                            .ok_or_else(temporal_range_error)
                    })
                    .transpose()?;
                Box::new(value)
            }
        }
        DataType::Decimal128(_, scale) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(mapping_error)?;
            Box::new((!typed.is_null(row)).then(|| decimal_string(typed.value(row), *scale)))
        }
        DataType::List(_) => {
            let typed = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| list_string(typed, row))
                .transpose()?;
            Box::new(value)
        }
        DataType::Struct(_) if plan.semantics == ColumnSemantics::Range => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| range_string(typed, row))
                .transpose()?;
            Box::new(value)
        }
        DataType::Struct(_) if plan.semantics == ColumnSemantics::Composite => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| composite_string(typed, row))
                .transpose()?;
            Box::new(value)
        }
        _ => return Err(mapping_error()),
    })
}
