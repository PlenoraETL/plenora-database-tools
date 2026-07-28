use super::{
    decimal_string, interval_text, mapping_error,
    plan::{ColumnSemantics, WriteColumnPlan},
    postgres_interval, temporal_range_error,
};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use plenora_database_core::Result;
use std::fmt::Write as _;

pub(super) fn copy_buffer(batch: &RecordBatch, plans: &[WriteColumnPlan]) -> Result<Vec<u8>> {
    let mut output = String::new();
    for row in 0..batch.num_rows() {
        for (column, plan) in plans.iter().enumerate() {
            if column > 0 {
                output.push('\t');
            }
            let array = batch.column(column).as_ref();
            if array.is_null(row) {
                output.push_str("\\N");
            } else {
                encode_copy_value(&mut output, array, plan, row)?;
            }
        }
        output.push('\n');
    }
    Ok(output.into_bytes())
}

#[allow(clippy::too_many_lines)]
pub(super) fn encode_copy_value(
    output: &mut String,
    array: &dyn Array,
    plan: &WriteColumnPlan,
    row: usize,
) -> Result<()> {
    macro_rules! typed {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(mapping_error)?
        };
    }
    match &plan.data_type {
        DataType::Boolean => output.push_str(if typed!(BooleanArray).value(row) {
            "t"
        } else {
            "f"
        }),
        DataType::Int32 => {
            append_formatted(output, format_args!("{}", typed!(Int32Array).value(row)))?;
        }
        DataType::Int64 => {
            append_formatted(output, format_args!("{}", typed!(Int64Array).value(row)))?;
        }
        DataType::Float32 => {
            encode_float(output, f64::from(typed!(Float32Array).value(row)))?;
        }
        DataType::Float64 => {
            encode_float(output, typed!(Float64Array).value(row))?;
        }
        DataType::Utf8 => escape_copy_text(output, typed!(StringArray).value(row)),
        DataType::Binary => {
            let bytes = typed!(BinaryArray).value(row);
            if !plan.is_spatial() {
                // COPY consumes one escaping layer before the bytea parser sees
                // the canonical PostgreSQL `\x` hexadecimal representation.
                output.push_str("\\\\x");
            }
            encode_hex(output, bytes);
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(temporal_range_error)?;
            let value = epoch
                .checked_add_signed(Duration::days(i64::from(typed!(Date32Array).value(row))))
                .ok_or_else(temporal_range_error)?;
            append_formatted(output, format_args!("{value}"))?;
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let value = time_from_microseconds(typed!(Time64MicrosecondArray).value(row))?;
            append_formatted(output, format_args!("{value}"))?;
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = postgres_interval(typed!(IntervalMonthDayNanoArray), row)?;
            output.push_str(&interval_text(&value));
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let instant = DateTime::<Utc>::from_timestamp_micros(
                typed!(TimestampMicrosecondArray).value(row),
            )
            .ok_or_else(temporal_range_error)?;
            if timezone.is_some() {
                append_formatted(output, format_args!("{}", instant.to_rfc3339()))?;
            } else {
                append_formatted(output, format_args!("{}", instant.naive_utc()))?;
            }
        }
        DataType::Decimal128(_, scale) => {
            output.push_str(&decimal_string(typed!(Decimal128Array).value(row), *scale));
        }
        DataType::List(_) => {
            let value = list_string(typed!(ListArray), row)?;
            escape_copy_text(output, &value);
        }
        DataType::Struct(_) if plan.semantics == ColumnSemantics::Range => {
            let value = range_string(typed!(StructArray), row)?;
            escape_copy_text(output, &value);
        }
        DataType::Struct(_) if plan.semantics == ColumnSemantics::Composite => {
            let value = composite_string(typed!(StructArray), row)?;
            escape_copy_text(output, &value);
        }
        _ => return Err(mapping_error()),
    }
    Ok(())
}

fn append_formatted(output: &mut String, arguments: std::fmt::Arguments<'_>) -> Result<()> {
    output.write_fmt(arguments).map_err(|_| mapping_error())
}

fn encode_float(output: &mut String, value: f64) -> Result<()> {
    if value.is_nan() {
        output.push_str("NaN");
    } else if value == f64::INFINITY {
        output.push_str("Infinity");
    } else if value == f64::NEG_INFINITY {
        output.push_str("-Infinity");
    } else {
        append_formatted(output, format_args!("{value}"))?;
    }
    Ok(())
}

fn escape_copy_text(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\t' => output.push_str("\\t"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            _ => output.push(character),
        }
    }
}

pub(super) fn time_from_microseconds(value: i64) -> Result<NaiveTime> {
    const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
    const SECONDS_PER_DAY: i64 = 86_400;
    if !(0..SECONDS_PER_DAY * MICROSECONDS_PER_SECOND).contains(&value) {
        return Err(mapping_error());
    }
    let seconds = u32::try_from(value / MICROSECONDS_PER_SECOND).map_err(|_| mapping_error())?;
    let nanoseconds =
        u32::try_from(value % MICROSECONDS_PER_SECOND).map_err(|_| mapping_error())? * 1_000;
    NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds).ok_or_else(mapping_error)
}

pub(super) fn list_string(array: &ListArray, row: usize) -> Result<String> {
    let values = array.value(row);
    let mut output = String::from("{");
    for index in 0..values.len() {
        if index > 0 {
            output.push(',');
        }
        if values.is_null(index) {
            output.push_str("NULL");
        } else {
            append_array_item(&mut output, values.as_ref(), index)?;
        }
    }
    output.push('}');
    Ok(output)
}

fn append_array_item(output: &mut String, values: &dyn Array, index: usize) -> Result<()> {
    macro_rules! value {
        ($array:ty) => {
            values
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(mapping_error)?
                .value(index)
        };
    }
    match values.data_type() {
        DataType::Boolean => output.push_str(if value!(BooleanArray) { "t" } else { "f" }),
        DataType::Int32 => append_formatted(output, format_args!("{}", value!(Int32Array)))?,
        DataType::Int64 => append_formatted(output, format_args!("{}", value!(Int64Array)))?,
        DataType::Float32 => encode_float(output, f64::from(value!(Float32Array)))?,
        DataType::Float64 => encode_float(output, value!(Float64Array))?,
        DataType::Utf8 => append_quoted_array_string(output, value!(StringArray)),
        _ => return Err(mapping_error()),
    }
    Ok(())
}

fn append_quoted_array_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        if matches!(character, '\\' | '"') {
            output.push('\\');
        }
        output.push(character);
    }
    output.push('"');
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct RangeValue {
    pub(super) lower: Option<String>,
    pub(super) upper: Option<String>,
    pub(super) lower_inclusive: bool,
    pub(super) upper_inclusive: bool,
    pub(super) lower_unbounded: bool,
    pub(super) upper_unbounded: bool,
    pub(super) empty: bool,
}

pub(super) fn range_value(array: &StructArray, row: usize) -> Result<RangeValue> {
    let string_value = |name: &str| -> Result<Option<String>> {
        let values = array
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(mapping_error)?;
        Ok((!values.is_null(row)).then(|| values.value(row).to_owned()))
    };
    let bool_value = |name: &str| -> Result<bool> {
        let values = array
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(mapping_error)?;
        if values.is_null(row) {
            return Err(mapping_error());
        }
        Ok(values.value(row))
    };
    Ok(RangeValue {
        lower: string_value("lower")?,
        upper: string_value("upper")?,
        lower_inclusive: bool_value("lower_inclusive")?,
        upper_inclusive: bool_value("upper_inclusive")?,
        lower_unbounded: bool_value("lower_unbounded")?,
        upper_unbounded: bool_value("upper_unbounded")?,
        empty: bool_value("empty")?,
    })
}

pub(super) fn range_string(array: &StructArray, row: usize) -> Result<String> {
    let value = range_value(array, row)?;
    if value.empty {
        return Ok("empty".to_owned());
    }
    if (!value.lower_unbounded && value.lower.is_none())
        || (!value.upper_unbounded && value.upper.is_none())
    {
        return Err(mapping_error());
    }
    let mut output = String::new();
    output.push(if value.lower_inclusive { '[' } else { '(' });
    if !value.lower_unbounded {
        append_quoted_range_bound(
            &mut output,
            value.lower.as_deref().ok_or_else(mapping_error)?,
        );
    }
    output.push(',');
    if !value.upper_unbounded {
        append_quoted_range_bound(
            &mut output,
            value.upper.as_deref().ok_or_else(mapping_error)?,
        );
    }
    output.push(if value.upper_inclusive { ']' } else { ')' });
    Ok(output)
}

fn append_quoted_range_bound(output: &mut String, value: &str) {
    append_quoted_postgres_value(output, value);
}

pub(super) fn append_quoted_postgres_value(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        if matches!(character, '\\' | '"') {
            output.push('\\');
        }
        output.push(character);
    }
    output.push('"');
}

pub(super) fn composite_value(array: &StructArray, row: usize) -> Result<Vec<Option<String>>> {
    array
        .columns()
        .iter()
        .map(|column| {
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mapping_error)?;
            Ok((!values.is_null(row)).then(|| values.value(row).to_owned()))
        })
        .collect()
}

pub(super) fn composite_string(array: &StructArray, row: usize) -> Result<String> {
    let values = composite_value(array, row)?;
    let mut output = String::from("(");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        if let Some(value) = value {
            append_quoted_postgres_value(&mut output, value);
        }
    }
    output.push(')');
    Ok(output)
}

fn encode_hex(output: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
}
