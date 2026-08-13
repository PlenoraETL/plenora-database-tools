use super::{types::ColumnSpec, write};
use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use plenora_database_core::plan::FilterExpression;
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::query::SpatialFunction;
use plenora_database_core::{DatabaseError, Result};
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

#[derive(Clone, Copy)]
enum FilterBindTarget<'a> {
    Column(&'a str),
    SpatialGeometry,
    SpatialDistance,
}

#[derive(Clone, Copy)]
struct FilterBind<'a> {
    name: &'a str,
    target: FilterBindTarget<'a>,
}

fn collect_filter_binds<'a>(expression: &'a FilterExpression, binds: &mut Vec<FilterBind<'a>>) {
    match expression {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            for argument in args {
                collect_filter_binds(argument, binds);
            }
        }
        FilterExpression::Eq { field, parameter }
        | FilterExpression::Ne { field, parameter }
        | FilterExpression::Lt { field, parameter }
        | FilterExpression::Lte { field, parameter }
        | FilterExpression::Gt { field, parameter }
        | FilterExpression::Gte { field, parameter }
        | FilterExpression::Like {
            field, parameter, ..
        } => binds.push(FilterBind {
            name: parameter,
            target: FilterBindTarget::Column(field),
        }),
        FilterExpression::In { field, parameters } => {
            binds.extend(parameters.iter().map(|parameter| FilterBind {
                name: parameter,
                target: FilterBindTarget::Column(field),
            }));
        }
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => {
            binds.push(FilterBind {
                name: lower_parameter,
                target: FilterBindTarget::Column(field),
            });
            binds.push(FilterBind {
                name: upper_parameter,
                target: FilterBindTarget::Column(field),
            });
        }
        FilterExpression::Spatial {
            function,
            geometry_parameter,
            distance_parameter,
            ..
        } => {
            if !matches!(
                function,
                SpatialFunction::IsEmpty | SpatialFunction::IsValid
            ) {
                if let Some(parameter) = geometry_parameter {
                    binds.push(FilterBind {
                        name: parameter,
                        target: FilterBindTarget::SpatialGeometry,
                    });
                }
                if *function == SpatialFunction::DWithin {
                    if let Some(parameter) = distance_parameter {
                        binds.push(FilterBind {
                            name: parameter,
                            target: FilterBindTarget::SpatialDistance,
                        });
                    }
                }
            }
        }
        FilterExpression::IsNull { .. } | FilterExpression::IsNotNull { .. } => {}
    }
}

pub fn typed_filter_parameter_types(
    filter: Option<&FilterExpression>,
    bind_names: &[String],
    parameters: &ParameterBag,
    columns: &[ColumnSpec],
) -> Option<Vec<Type>> {
    let Some(filter) = filter else {
        return bind_names.is_empty().then(Vec::new);
    };
    let mut binds = Vec::new();
    collect_filter_binds(filter, &mut binds);
    if binds.len() != bind_names.len()
        || binds
            .iter()
            .zip(bind_names)
            .any(|(bind, rendered)| bind.name != rendered)
    {
        return None;
    }
    binds
        .into_iter()
        .map(|bind| {
            let value = parameters.get(bind.name)?;
            match bind.target {
                FilterBindTarget::SpatialGeometry => match value {
                    ParameterValue::Wkb { .. } | ParameterValue::Bytes(_) => Some(Type::BYTEA),
                    _ => None,
                },
                FilterBindTarget::SpatialDistance => {
                    matches!(value, ParameterValue::F64(_)).then_some(Type::FLOAT8)
                }
                FilterBindTarget::Column(field) => {
                    let column = columns.iter().find(|column| column.name == field)?;
                    typed_column_parameter_type(column, value)
                }
            }
        })
        .collect()
}

fn typed_column_parameter_type(column: &ColumnSpec, value: &ParameterValue) -> Option<Type> {
    if column.type_kind.as_deref().is_some_and(|kind| kind != "b") {
        return None;
    }
    let native = column.native_type.as_str();
    match value {
        ParameterValue::Bool(_) if native == "bool" => Some(Type::BOOL),
        ParameterValue::I32(_) if matches!(native, "int2" | "int4" | "int8" | "numeric") => {
            Some(Type::INT4)
        }
        ParameterValue::I64(_) if matches!(native, "int2" | "int4" | "int8" | "numeric") => {
            Some(Type::INT8)
        }
        ParameterValue::F64(_) if matches!(native, "float4" | "float8" | "numeric") => {
            Some(Type::FLOAT8)
        }
        ParameterValue::String(_) if matches!(native, "text" | "varchar" | "bpchar" | "name") => {
            Some(Type::TEXT)
        }
        ParameterValue::Bytes(_) | ParameterValue::Wkb { .. } if native == "bytea" => {
            Some(Type::BYTEA)
        }
        ParameterValue::Date(_) if matches!(native, "date" | "timestamp" | "timestamptz") => {
            Some(Type::DATE)
        }
        ParameterValue::Timestamp(_) if matches!(native, "date" | "timestamp" | "timestamptz") => {
            Some(Type::TIMESTAMP)
        }
        ParameterValue::TimestampTz(_)
            if matches!(native, "date" | "timestamp" | "timestamptz") =>
        {
            Some(Type::TIMESTAMPTZ)
        }
        ParameterValue::Json(_) if native == "json" => Some(Type::JSON),
        ParameterValue::Json(_) if native == "jsonb" => Some(Type::JSONB),
        ParameterValue::Decimal(_) if native == "numeric" => Some(Type::NUMERIC),
        ParameterValue::Uuid(_) if native == "uuid" => Some(Type::UUID),
        ParameterValue::Null { type_name } => {
            let declared = builtin_type_name(type_name)?;
            (declared.name() == native
                || null_type_matches(type_name, &declared)
                    && null_type_matches(type_name, &native_type(native)?))
            .then_some(declared)
        }
        _ => None,
    }
}

fn builtin_type_name(type_name: &str) -> Option<Type> {
    match type_name.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Some(Type::BOOL),
        "int2" | "smallint" => Some(Type::INT2),
        "int4" | "integer" => Some(Type::INT4),
        "int8" | "bigint" => Some(Type::INT8),
        "float4" | "real" => Some(Type::FLOAT4),
        "float8" | "double precision" => Some(Type::FLOAT8),
        "text" => Some(Type::TEXT),
        "varchar" | "character varying" => Some(Type::VARCHAR),
        "bpchar" | "character" => Some(Type::BPCHAR),
        "bytea" => Some(Type::BYTEA),
        "date" => Some(Type::DATE),
        "time" => Some(Type::TIME),
        "timestamp" => Some(Type::TIMESTAMP),
        "timestamptz" | "timestamp with time zone" => Some(Type::TIMESTAMPTZ),
        "interval" => Some(Type::INTERVAL),
        "numeric" | "decimal" => Some(Type::NUMERIC),
        "json" => Some(Type::JSON),
        "jsonb" => Some(Type::JSONB),
        "uuid" => Some(Type::UUID),
        _ => None,
    }
}

fn native_type(type_name: &str) -> Option<Type> {
    builtin_type_name(type_name)
}

fn parameter_value_type(value: &ParameterValue) -> Option<Type> {
    match value {
        ParameterValue::Null { type_name } => builtin_type_name(type_name),
        ParameterValue::Bool(_) => Some(Type::BOOL),
        ParameterValue::I32(_) => Some(Type::INT4),
        ParameterValue::I64(_) => Some(Type::INT8),
        ParameterValue::F64(_) => Some(Type::FLOAT8),
        ParameterValue::String(_) | ParameterValue::Enum { .. } => Some(Type::TEXT),
        ParameterValue::Bytes(_) | ParameterValue::Wkb { .. } => Some(Type::BYTEA),
        ParameterValue::Date(_) => Some(Type::DATE),
        ParameterValue::Timestamp(_) => Some(Type::TIMESTAMP),
        ParameterValue::TimestampTz(_) => Some(Type::TIMESTAMPTZ),
        ParameterValue::Json(_) => Some(Type::JSONB),
        ParameterValue::Decimal(_) => Some(Type::NUMERIC),
        ParameterValue::Uuid(_) => Some(Type::UUID),
    }
}

pub fn typed_query_parameter_types(
    bind_names: &[String],
    parameters: &ParameterBag,
) -> Option<Vec<Type>> {
    bind_names
        .iter()
        .map(|name| parameter_value_type(parameters.get(name)?))
        .collect()
}

#[derive(Debug)]
pub struct DecimalParameter {
    value: i128,
    scale: i8,
}

impl DecimalParameter {
    pub fn parse(value: &str) -> Result<Self> {
        let (negative, unsigned) = match value.as_bytes().first() {
            Some(b'-') => (true, &value[1..]),
            Some(b'+') => (false, &value[1..]),
            _ => (false, value),
        };
        if unsigned.is_empty() {
            return Err(DatabaseError::invalid_plan("parametro decimal non valido"));
        }
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if fraction.contains('.')
            || (integer.is_empty() && fraction.is_empty())
            || !integer.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(DatabaseError::invalid_plan("parametro decimal non valido"));
        }
        let scale = i8::try_from(fraction.len())
            .map_err(|_| DatabaseError::invalid_plan("scala parametro decimal troppo grande"))?;
        let mut digits = String::with_capacity(integer.len() + fraction.len());
        digits.push_str(if integer.is_empty() { "0" } else { integer });
        digits.push_str(fraction);
        let parsed = digits
            .parse::<i128>()
            .map_err(|_| DatabaseError::invalid_plan("parametro decimal non valido"))?;
        Ok(Self {
            value: if negative { -parsed } else { parsed },
            scale,
        })
    }
}

impl ToSql for DecimalParameter {
    fn to_sql(
        &self,
        target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(target_type) {
            return Err("target non numeric".into());
        }
        write::binary_codec::encode_numeric_binary(self.value, self.scale, output)?;
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        *target_type == Type::NUMERIC
    }

    to_sql_checked!();
}

#[derive(Debug)]
pub struct UuidParameter(pub [u8; 16]);

impl UuidParameter {
    pub fn parse(value: &str) -> Result<Self> {
        let compact = value
            .bytes()
            .filter(|byte| *byte != b'-')
            .collect::<Vec<_>>();
        if compact.len() != 32 {
            return Err(DatabaseError::invalid_plan("parametro UUID non valido"));
        }
        let mut bytes = [0_u8; 16];
        for (output, pair) in bytes.iter_mut().zip(compact.chunks_exact(2)) {
            let high = decode_hex_nibble(pair[0])
                .ok_or_else(|| DatabaseError::invalid_plan("parametro UUID non valido"))?;
            let low = decode_hex_nibble(pair[1])
                .ok_or_else(|| DatabaseError::invalid_plan("parametro UUID non valido"))?;
            *output = high << 4 | low;
        }
        Ok(Self(bytes))
    }
}

const fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

impl ToSql for UuidParameter {
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
struct TypedNull(String);

impl ToSql for TypedNull {
    fn to_sql(
        &self,
        target_type: &Type,
        _output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !null_type_matches(&self.0, target_type) {
            return Err("tipo NULL dichiarato non coerente col parametro PostgreSQL".into());
        }
        Ok(IsNull::Yes)
    }

    fn accepts(_target_type: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

fn null_type_matches(declared: &str, target: &Type) -> bool {
    declared.eq_ignore_ascii_case(target.name())
        || matches!(
            (declared.to_ascii_lowercase().as_str(), target.name()),
            ("boolean", "bool")
                | ("smallint", "int2")
                | ("integer", "int4")
                | ("bigint", "int8")
                | ("real", "float4")
                | ("double precision", "float8")
                | ("decimal", "numeric")
                | ("timestamp", "timestamp")
                | ("timestamptz", "timestamptz")
                | ("varchar", "varchar")
                | ("text", "text")
        )
}

pub fn bind_parameters(
    parameters: &ParameterBag,
    bind_names: &[String],
) -> Result<Vec<Box<dyn ToSql + Sync + Send>>> {
    bind_names
        .iter()
        .map(|name| {
            let value = parameters
                .get(name)
                .ok_or_else(|| DatabaseError::invalid_plan("parametro filtro mancante"))?;
            let boxed: Box<dyn ToSql + Sync + Send> = match value {
                ParameterValue::Bool(value) => Box::new(*value),
                ParameterValue::I32(value) => Box::new(*value),
                ParameterValue::I64(value) => Box::new(*value),
                ParameterValue::F64(value) => Box::new(*value),
                ParameterValue::String(value) => Box::new(value.clone()),
                ParameterValue::Bytes(value) => Box::new(value.clone()),
                ParameterValue::Date(value) => Box::new(
                    NaiveDate::parse_from_str(value, "%Y-%m-%d")
                        .map_err(|_| DatabaseError::invalid_plan("parametro date non valido"))?,
                ),
                ParameterValue::Timestamp(value) => Box::new(
                    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").map_err(|_| {
                        DatabaseError::invalid_plan("parametro timestamp non valido")
                    })?,
                ),
                ParameterValue::TimestampTz(value) => Box::new(
                    DateTime::parse_from_rfc3339(value)
                        .map_err(|_| {
                            DatabaseError::invalid_plan("parametro timestamptz non valido")
                        })?
                        .with_timezone(&Utc),
                ),
                ParameterValue::Json(value) => Box::new(value.clone()),
                ParameterValue::Wkb { bytes, .. } => Box::new(bytes.clone()),
                ParameterValue::Decimal(value) => Box::new(DecimalParameter::parse(value)?),
                ParameterValue::Uuid(value) => Box::new(UuidParameter::parse(value)?),
                ParameterValue::Enum { label, .. } => Box::new(label.clone()),
                ParameterValue::Null { type_name } => Box::new(TypedNull(type_name.clone())),
            };
            Ok(boxed)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{DecimalParameter, UuidParameter};

    #[test]
    fn decimal_parser_rejects_ambiguous_or_non_ascii_input() {
        for invalid in ["", "+", "-", ".", "+.", "-.", "1.2.3", "NaN", "１２"] {
            assert!(
                DecimalParameter::parse(invalid).is_err(),
                "{invalid:?} deve fallire"
            );
        }
        assert_eq!(
            DecimalParameter::parse("-.5")
                .expect("decimal valido")
                .value,
            -5
        );
    }

    #[test]
    fn uuid_parser_is_utf8_safe_and_strictly_hexadecimal() {
        // 32 byte ma indici non allineati ai confini UTF-8: il vecchio
        // slicing della String poteva causare panic su questo input.
        let adversarial_utf8 = format!("{}aa", "€".repeat(10));
        assert_eq!(adversarial_utf8.len(), 32);
        assert!(UuidParameter::parse(&adversarial_utf8).is_err());
        assert!(UuidParameter::parse("gggggggg-gggg-gggg-gggg-gggggggggggg").is_err());
        assert_eq!(
            UuidParameter::parse("123e4567-e89b-12d3-a456-426614174000")
                .expect("UUID valido")
                .0,
            [
                0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
                0x40, 0x00,
            ]
        );
    }
}
