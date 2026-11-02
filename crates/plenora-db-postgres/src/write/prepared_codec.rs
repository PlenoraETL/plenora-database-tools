use super::{
    binary_codec::{decimal_string, postgres_interval, EwkbBinary},
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
use bytes::BytesMut;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use plenora_database_core::Result;
use tokio_postgres::types::{Format, IsNull, Kind, ToSql, Type};

/// Il parametro di uno statement preparato per il valore di `array` alla riga
/// `row`, che attraversa i domini ([`ThroughDomains`]).
///
/// # Errors
///
/// `data_mapping` per un valore che il piano non codifica, o non canonico
/// (regola V).
pub(super) fn arrow_value(
    array: &dyn Array,
    plan: &WriteColumnPlan,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>> {
    Ok(Box::new(ThroughDomains(typed_value(array, plan, row)?)))
}

/// Un parametro che il server ha tipizzato con un dominio si codifica come il
/// tipo base del dominio: e il formato binario che il server legge.
///
/// Fino al 25/10 il percorso preparato rifiutava ogni valore diretto a una
/// colonna con un dominio (`protocol`: i codec del driver riconoscono il tipo
/// base, non il dominio), mentre `COPY` lo scriveva; i vincoli del dominio li
/// applica comunque il server.
#[derive(Debug)]
struct ThroughDomains(Box<dyn ToSql + Sync + Send>);

fn base_type(target_type: &Type) -> &Type {
    let mut current = target_type;
    while let Kind::Domain(base) = current.kind() {
        current = base;
    }
    current
}

impl ToSql for ThroughDomains {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql_checked(base_type(target_type), output)
    }

    fn accepts(_target_type: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql_checked(base_type(target_type), output)
    }

    fn encode_format(&self, target_type: &Type) -> Format {
        self.0.encode_format(base_type(target_type))
    }
}

/// Un valore di testo in formato testo (regola F, `super::sql::placeholder_expression`):
/// il server lo legge con la funzione d'ingresso del tipo della colonna, che
/// ha dato al parametro, come una riga di `COPY` in testo. Nessun passaggio
/// da un altro tipo.
#[derive(Debug)]
pub(super) struct TextParam(pub(super) String);

impl ToSql for TextParam {
    fn to_sql(
        &self,
        _target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        output.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_target_type: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(target_type, output)
    }

    fn encode_format(&self, _target_type: &Type) -> Format {
        Format::Text
    }
}

#[allow(clippy::too_many_lines)]
fn typed_value(
    array: &dyn Array,
    plan: &WriteColumnPlan,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>> {
    if !array.is_null(row) {
        super::canonical::check(plan, array, row)?;
    }
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
            Box::new((!typed.is_null(row)).then(|| TextParam(typed.value(row).to_owned())))
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(mapping_error)?;
            if plan.is_spatial() {
                Box::new((!typed.is_null(row)).then(|| EwkbBinary(typed.value(row).to_vec())))
            } else {
                Box::new((!typed.is_null(row)).then(|| typed.value(row).to_vec()))
            }
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
            Box::new(
                (!typed.is_null(row)).then(|| TextParam(decimal_string(typed.value(row), *scale))),
            )
        }
        DataType::List(_) => {
            let typed = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| list_string(typed, row).map(TextParam))
                .transpose()?;
            Box::new(value)
        }
        DataType::Struct(_) if plan.semantics == ColumnSemantics::Range => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| range_string(typed, row).map(TextParam))
                .transpose()?;
            Box::new(value)
        }
        DataType::Struct(_) if plan.semantics == ColumnSemantics::Composite => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = (!typed.is_null(row))
                .then(|| composite_string(typed, row).map(TextParam))
                .transpose()?;
            Box::new(value)
        }
        _ => return Err(mapping_error()),
    })
}
