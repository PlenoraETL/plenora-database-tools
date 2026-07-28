use super::{
    mapping_error,
    plan::{ColumnSemantics, WriteColumnPlan},
    prepared_codec::arrow_value,
    value_codec::{composite_value, range_value, RangeValue},
};
use crate::error::public_error;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, IntervalMonthDayNanoArray, ListArray, StringArray, StructArray,
};
use arrow_schema::DataType;
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use tokio_postgres::types::{to_sql_checked, IsNull, Kind, ToSql, Type};

#[derive(Debug)]
struct NumericBinary {
    value: i128,
    scale: i8,
}

impl ToSql for NumericBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non numeric".into());
        }
        encode_numeric_binary(self.value, self.scale, output)?;
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        target_type.name() == "numeric"
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct EwkbBinary(Vec<u8>);

impl ToSql for EwkbBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non spatial".into());
        }
        output.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        matches!(target_type.name(), "geometry" | "geography")
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct UuidBinary([u8; 16]);

impl ToSql for UuidBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non UUID".into());
        }
        output.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        *target_type == Type::UUID
    }

    to_sql_checked!();
}

#[derive(Debug)]
pub(super) struct PostgresIntervalBinary {
    pub(super) microseconds: i64,
    pub(super) days: i32,
    pub(super) months: i32,
}

pub(super) fn interval_text(value: &PostgresIntervalBinary) -> String {
    const MICROS_PER_SECOND: u64 = 1_000_000;
    const MICROS_PER_MINUTE: u64 = 60 * MICROS_PER_SECOND;
    const MICROS_PER_HOUR: u64 = 60 * MICROS_PER_MINUTE;

    let negative = value.microseconds < 0;
    let absolute = value.microseconds.unsigned_abs();
    let hours = absolute / MICROS_PER_HOUR;
    let minutes = (absolute % MICROS_PER_HOUR) / MICROS_PER_MINUTE;
    let seconds = (absolute % MICROS_PER_MINUTE) / MICROS_PER_SECOND;
    let microseconds = absolute % MICROS_PER_SECOND;
    format!(
        "{} mons {} days {}{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}",
        value.months,
        value.days,
        if negative { "-" } else { "" }
    )
}

#[derive(Debug)]
struct PostgresRangeBinary(RangeValue);

impl ToSql for PostgresRangeBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let Kind::Range(member) = target_type.kind() else {
            return Err("target non range".into());
        };
        if self.0.empty {
            output.put_u8(0x01);
            return Ok(IsNull::No);
        }
        let mut flags = 0_u8;
        if self.0.lower_inclusive {
            flags |= 0x02;
        }
        if self.0.upper_inclusive {
            flags |= 0x04;
        }
        if self.0.lower_unbounded {
            flags |= 0x08;
        }
        if self.0.upper_unbounded {
            flags |= 0x10;
        }
        output.put_u8(flags);
        if !self.0.lower_unbounded {
            encode_range_bound(
                self.0.lower.as_deref().ok_or("range lower mancante")?,
                member,
                output,
            )?;
        }
        if !self.0.upper_unbounded {
            encode_range_bound(
                self.0.upper.as_deref().ok_or("range upper mancante")?,
                member,
                output,
            )?;
        }
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        matches!(target_type.kind(), Kind::Range(_))
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct PostgresCompositeBinary(Vec<Option<String>>);

impl ToSql for PostgresCompositeBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let Kind::Composite(fields) = target_type.kind() else {
            return Err("target non composite".into());
        };
        if fields.len() != self.0.len() {
            return Err("numero campi composite non coerente".into());
        }
        output.put_i32(i32::try_from(fields.len())?);
        for (field, value) in fields.iter().zip(&self.0) {
            output.put_u32(field.type_().oid());
            if let Some(value) = value {
                let mut encoded = BytesMut::new();
                encode_composite_field(value, field.type_(), &mut encoded)?;
                output.put_i32(i32::try_from(encoded.len())?);
                output.extend_from_slice(&encoded);
            } else {
                output.put_i32(-1);
            }
        }
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        matches!(target_type.kind(), Kind::Composite(_))
    }

    to_sql_checked!();
}

#[allow(clippy::too_many_lines)]
fn encode_composite_field(
    value: &str,
    field_type: &Type,
    output: &mut BytesMut,
) -> std::result::Result<(), Box<dyn std::error::Error + Sync + Send>> {
    match field_type.kind() {
        Kind::Domain(inner) => return encode_composite_field(value, inner, output),
        Kind::Enum(_) => {
            output.extend_from_slice(value.as_bytes());
            return Ok(());
        }
        _ => {}
    }
    match *field_type {
        Type::BOOL => {
            value.parse::<bool>()?.to_sql(field_type, output)?;
        }
        Type::INT2 => {
            value.parse::<i16>()?.to_sql(field_type, output)?;
        }
        Type::INT4 => {
            value.parse::<i32>()?.to_sql(field_type, output)?;
        }
        Type::INT8 => {
            value.parse::<i64>()?.to_sql(field_type, output)?;
        }
        Type::FLOAT4 => {
            value.parse::<f32>()?.to_sql(field_type, output)?;
        }
        Type::FLOAT8 => {
            value.parse::<f64>()?.to_sql(field_type, output)?;
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            output.extend_from_slice(value.as_bytes());
        }
        Type::DATE => {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")?.to_sql(field_type, output)?;
        }
        Type::TIME => {
            NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?.to_sql(field_type, output)?;
        }
        Type::TIMESTAMP => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))?
                .to_sql(field_type, output)?;
        }
        Type::TIMESTAMPTZ => {
            DateTime::parse_from_rfc3339(value)?
                .with_timezone(&Utc)
                .to_sql(field_type, output)?;
        }
        Type::NUMERIC => {
            let (unscaled, scale) = parse_numeric_components(value)?;
            NumericBinary {
                value: unscaled,
                scale,
            }
            .to_sql(field_type, output)?;
        }
        Type::JSON | Type::JSONB => {
            serde_json::from_str::<serde_json::Value>(value)?.to_sql(field_type, output)?;
        }
        Type::UUID => output.extend_from_slice(&parse_uuid_bytes(value)?),
        _ => return Err("campo composite binario non supportato".into()),
    }
    Ok(())
}

fn parse_uuid_bytes(
    value: &str,
) -> std::result::Result<[u8; 16], Box<dyn std::error::Error + Sync + Send>> {
    let compact = value
        .chars()
        .filter(|character| *character != '-')
        .collect::<String>();
    if compact.len() != 32 {
        return Err("UUID non valido".into());
    }
    let mut output = [0_u8; 16];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16)?;
    }
    Ok(output)
}

fn encode_range_bound(
    value: &str,
    member: &Type,
    output: &mut BytesMut,
) -> std::result::Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let mut encoded = BytesMut::new();
    match *member {
        Type::INT4 => {
            value.parse::<i32>()?.to_sql(member, &mut encoded)?;
        }
        Type::INT8 => {
            value.parse::<i64>()?.to_sql(member, &mut encoded)?;
        }
        Type::DATE => {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")?.to_sql(member, &mut encoded)?;
        }
        Type::TIMESTAMP => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")?
                .to_sql(member, &mut encoded)?;
        }
        Type::TIMESTAMPTZ => {
            DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z")?
                .with_timezone(&Utc)
                .to_sql(member, &mut encoded)?;
        }
        Type::NUMERIC => {
            let (unscaled, scale) = parse_numeric_components(value)?;
            NumericBinary {
                value: unscaled,
                scale,
            }
            .to_sql(member, &mut encoded)?;
        }
        _ => return Err("range subtype binario non supportato".into()),
    }
    output.put_i32(i32::try_from(encoded.len())?);
    output.extend_from_slice(&encoded);
    Ok(())
}

pub(super) fn parse_numeric_components(
    value: &str,
) -> std::result::Result<(i128, i8), Box<dyn std::error::Error + Sync + Send>> {
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if unsigned.is_empty() {
        return Err("numeric vuoto".into());
    }
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if fraction.contains('.')
        || (integer.is_empty() && fraction.is_empty())
        || !integer.chars().all(|character| character.is_ascii_digit())
        || !fraction.chars().all(|character| character.is_ascii_digit())
    {
        return Err("numeric non valido".into());
    }
    let scale = i8::try_from(fraction.len())?;
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(if integer.is_empty() { "0" } else { integer });
    digits.push_str(fraction);
    let value = digits.parse::<i128>()?;
    Ok((if negative { -value } else { value }, scale))
}

impl ToSql for PostgresIntervalBinary {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non interval".into());
        }
        output.put_i64(self.microseconds);
        output.put_i32(self.days);
        output.put_i32(self.months);
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        *target_type == Type::INTERVAL
    }

    to_sql_checked!();
}

pub(super) fn postgres_interval(
    array: &IntervalMonthDayNanoArray,
    row: usize,
) -> Result<PostgresIntervalBinary> {
    let value = array.value(row);
    if value.nanoseconds % 1_000 != 0 {
        return Err(public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Write,
            false,
            "interval Arrow richiede precisione massima al microsecondo per PostgreSQL",
        ));
    }
    Ok(PostgresIntervalBinary {
        microseconds: value.nanoseconds / 1_000,
        days: value.days,
        months: value.months,
    })
}

pub(super) fn binary_copy_value(
    array: &dyn Array,
    plan: &WriteColumnPlan,
    target_type: &Type,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>> {
    if matches!(plan.data_type, DataType::Struct(_)) && plan.semantics == ColumnSemantics::Composite
    {
        let typed = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| composite_value(typed, row).map(PostgresCompositeBinary))
            .transpose()?;
        return Ok(Box::new(value));
    }
    if matches!(plan.data_type, DataType::Struct(_)) && plan.semantics == ColumnSemantics::Range {
        let typed = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| range_value(typed, row).map(PostgresRangeBinary))
            .transpose()?;
        return Ok(Box::new(value));
    }
    if matches!(plan.data_type, DataType::List(_)) {
        if !matches!(target_type.kind(), Kind::Array(_)) {
            return Err(mapping_error());
        }
        let typed = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(mapping_error)?;
        return binary_array_value(typed, row);
    }
    if matches!(plan.data_type, DataType::Decimal128(_, _)) {
        let typed = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(mapping_error)?;
        let DataType::Decimal128(_, scale) = &plan.data_type else {
            return Err(mapping_error());
        };
        return Ok(Box::new((!typed.is_null(row)).then(|| NumericBinary {
            value: typed.value(row),
            scale: *scale,
        })));
    }
    if plan.is_spatial() {
        let typed = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(mapping_error)?;
        return Ok(Box::new(
            (!typed.is_null(row)).then(|| EwkbBinary(typed.value(row).to_vec())),
        ));
    }
    if matches!(plan.data_type, DataType::Utf8)
        && matches!(plan.native_type.as_deref(), Some("json" | "jsonb"))
    {
        let typed = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| serde_json::from_str::<serde_json::Value>(typed.value(row)))
            .transpose()
            .map_err(|_| DatabaseError::invalid_plan("JSON Arrow non valido"))?;
        return Ok(Box::new(value));
    }
    if target_type.name() == "uuid" {
        let typed = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(mapping_error)?;
        let value = (!typed.is_null(row))
            .then(|| {
                parse_uuid_bytes(typed.value(row))
                    .map(UuidBinary)
                    .map_err(|_| DatabaseError::invalid_plan("UUID Arrow non valido"))
            })
            .transpose()?;
        return Ok(Box::new(value));
    }
    arrow_value(array, plan, row)
}

#[allow(clippy::too_many_lines)]
fn binary_array_value(array: &ListArray, row: usize) -> Result<Box<dyn ToSql + Sync + Send>> {
    macro_rules! primitive {
        ($array:ty, $value:ty) => {{
            if array.is_null(row) {
                Box::new(None::<Vec<Option<$value>>>) as Box<dyn ToSql + Sync + Send>
            } else {
                let values = array.value(row);
                let typed = values
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(mapping_error)?;
                let result = (0..typed.len())
                    .map(|index| (!typed.is_null(index)).then(|| typed.value(index)))
                    .collect::<Vec<_>>();
                Box::new(Some(result)) as Box<dyn ToSql + Sync + Send>
            }
        }};
    }
    Ok(match array.value_type() {
        DataType::Boolean => primitive!(BooleanArray, bool),
        DataType::Int32 => primitive!(Int32Array, i32),
        DataType::Int64 => primitive!(Int64Array, i64),
        DataType::Float32 => primitive!(Float32Array, f32),
        DataType::Float64 => primitive!(Float64Array, f64),
        DataType::Utf8 => {
            if array.is_null(row) {
                Box::new(None::<Vec<Option<String>>>)
            } else {
                let values = array.value(row);
                let typed = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(mapping_error)?;
                let result = (0..typed.len())
                    .map(|index| (!typed.is_null(index)).then(|| typed.value(index).to_owned()))
                    .collect::<Vec<_>>();
                Box::new(Some(result))
            }
        }
        _ => return Err(mapping_error()),
    })
}

fn parse_base10000_group(
    chunk: &[u8],
) -> std::result::Result<i16, Box<dyn std::error::Error + Sync + Send>> {
    if chunk.len() != 4 {
        return Err("numeric base-10000 group must contain four digits".into());
    }
    let group = std::str::from_utf8(chunk)?.parse::<i16>()?;
    if !(0..10_000).contains(&group) {
        return Err("numeric base-10000 group is outside its wire range".into());
    }
    Ok(group)
}

pub fn encode_numeric_binary(
    value: i128,
    scale: i8,
    output: &mut BytesMut,
) -> std::result::Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let negative = value < 0;
    let mut digits = value.unsigned_abs().to_string();
    let scale = if scale < 0 {
        digits.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
        0
    } else {
        usize::try_from(scale)?
    };
    if digits.len() <= scale {
        digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
    }
    let integer_digits = digits.len() - scale;
    let left_padding = (4 - integer_digits % 4) % 4;
    let right_padding = (4 - scale % 4) % 4;
    let mut padded = String::with_capacity(left_padding + digits.len() + right_padding);
    padded.push_str(&"0".repeat(left_padding));
    padded.push_str(&digits);
    padded.push_str(&"0".repeat(right_padding));
    let integer_groups = (left_padding + integer_digits) / 4;
    let mut groups = padded
        .as_bytes()
        .chunks_exact(4)
        .map(parse_base10000_group)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let leading = groups.iter().take_while(|digit| **digit == 0).count();
    let trailing = groups.iter().rev().take_while(|digit| **digit == 0).count();
    let end = groups.len().saturating_sub(trailing).max(leading);
    groups = groups[leading..end].to_vec();
    let weight = i16::try_from(integer_groups)?
        .checked_sub(1)
        .and_then(|value| value.checked_sub(i16::try_from(leading).ok()?))
        .ok_or("numeric weight overflow")?;
    output.put_i16(i16::try_from(groups.len())?);
    output.put_i16(if groups.is_empty() { 0 } else { weight });
    output.put_u16(if negative { 0x4000 } else { 0x0000 });
    output.put_u16(u16::try_from(scale)?);
    for group in groups {
        output.put_i16(group);
    }
    Ok(())
}

pub(super) fn decimal_string(value: i128, scale: i8) -> String {
    let negative = value < 0;
    let mut digits = value.unsigned_abs().to_string();
    if scale < 0 {
        digits.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
        if negative {
            digits.insert(0, '-');
        }
        return digits;
    }
    let scale = usize::from(scale.unsigned_abs());
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let mut result = if scale == 0 {
        padded
    } else {
        format!("{}.{}", &padded[..split], &padded[split..])
    };
    if negative {
        result.insert(0, '-');
    }
    result
}
