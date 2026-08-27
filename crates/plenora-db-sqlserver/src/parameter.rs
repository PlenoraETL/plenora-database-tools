use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeSet;
use tiberius::Query;

pub fn bind_parameters(
    query: &mut Query<'static>,
    bind_names: &[String],
    parameters: &ParameterBag,
) -> Result<()> {
    validate_parameter_set(bind_names, parameters)?;
    for name in bind_names {
        let value = parameters
            .get(name)
            .ok_or_else(|| parameter_error("parametro SQL Server mancante"))?;
        bind_parameter(query, value)?;
    }
    Ok(())
}

pub fn parameter_declarations(
    bind_names: &[String],
    parameters: &ParameterBag,
) -> Result<Option<String>> {
    validate_parameter_set(bind_names, parameters)?;
    if bind_names.is_empty() {
        return Ok(None);
    }
    bind_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let value = parameters
                .get(name)
                .ok_or_else(|| parameter_error("parametro SQL Server mancante"))?;
            Ok(format!("@p{} {}", index + 1, parameter_type(value)?))
        })
        .collect::<Result<Vec<_>>>()
        .map(|declarations| Some(declarations.join(", ")))
}

fn validate_parameter_set(bind_names: &[String], parameters: &ParameterBag) -> Result<()> {
    let unique = bind_names.iter().collect::<BTreeSet<_>>();
    if unique.len() != parameters.len() {
        return Err(parameter_error(
            "insieme parametri SQL Server diverso dai bind richiesti",
        ));
    }
    for name in bind_names {
        if parameters.get(name).is_none() {
            return Err(parameter_error("parametro SQL Server mancante"));
        }
    }
    Ok(())
}

pub fn bind_parameter(query: &mut Query<'static>, value: &ParameterValue) -> Result<()> {
    match value {
        ParameterValue::Bool(value) => query.bind(*value),
        ParameterValue::I32(value) => query.bind(*value),
        ParameterValue::I64(value) => query.bind(*value),
        ParameterValue::F64(value) => query.bind(*value),
        ParameterValue::String(value) => query.bind(value.clone()),
        ParameterValue::Date(value) => {
            validate_date(value)?;
            query.bind(value.clone());
        }
        ParameterValue::Timestamp(value) => {
            validate_timestamp(value)?;
            query.bind(value.clone());
        }
        ParameterValue::TimestampTz(value) => {
            validate_timestamp_tz(value)?;
            query.bind(value.clone());
        }
        ParameterValue::Decimal(value) => {
            validate_decimal(value)?;
            query.bind(value.clone());
        }
        ParameterValue::Uuid(value) => {
            validate_uuid(value)?;
            query.bind(value.clone());
        }
        ParameterValue::Bytes(value) => query.bind(value.clone()),
        ParameterValue::Json(value) => {
            let encoded = serde_json::to_string(value)
                .map_err(|_| parameter_error("parametro JSON SQL Server non serializzabile"))?;
            query.bind(encoded);
        }
        ParameterValue::Wkb { bytes, .. } => query.bind(bytes.clone()),
        ParameterValue::Enum { label, .. } => query.bind(label.clone()),
        ParameterValue::Null { .. } => {
            return Err(unsupported(
                "NULL bindato SQL Server richiede un tipo target risolto",
            ));
        }
    }
    Ok(())
}

fn parameter_type(value: &ParameterValue) -> Result<&'static str> {
    match value {
        ParameterValue::Bool(_) => Ok("bit"),
        ParameterValue::I32(_) => Ok("int"),
        ParameterValue::I64(_) => Ok("bigint"),
        ParameterValue::F64(_) => Ok("float(53)"),
        ParameterValue::String(value) => Ok(string_parameter_type(value)),
        ParameterValue::Bytes(value) => Ok(if value.len() <= 8_000 {
            "varbinary(8000)"
        } else {
            "varbinary(max)"
        }),
        ParameterValue::Date(value) => {
            validate_date(value)?;
            Ok(string_parameter_type(value))
        }
        ParameterValue::Timestamp(value) => {
            validate_timestamp(value)?;
            Ok(string_parameter_type(value))
        }
        ParameterValue::TimestampTz(value) => {
            validate_timestamp_tz(value)?;
            Ok(string_parameter_type(value))
        }
        ParameterValue::Decimal(value) => {
            validate_decimal(value)?;
            Ok(string_parameter_type(value))
        }
        ParameterValue::Uuid(value) => {
            validate_uuid(value)?;
            Ok(string_parameter_type(value))
        }
        ParameterValue::Json(value) => {
            let encoded = serde_json::to_string(value)
                .map_err(|_| parameter_error("parametro JSON SQL Server non serializzabile"))?;
            Ok(string_parameter_type(&encoded))
        }
        ParameterValue::Wkb { bytes, .. } => Ok(if bytes.len() <= 8_000 {
            "varbinary(8000)"
        } else {
            "varbinary(max)"
        }),
        ParameterValue::Enum { label, .. } => Ok(string_parameter_type(label)),
        ParameterValue::Null { .. } => Err(unsupported(
            "NULL bindato SQL Server richiede un tipo target risolto",
        )),
    }
}

const fn string_parameter_type(value: &str) -> &'static str {
    if value.len() <= 4_000 {
        "nvarchar(4000)"
    } else {
        "nvarchar(max)"
    }
}

fn validate_date(value: &str) -> Result<()> {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map(|_| ())
        .map_err(|_| parameter_error("parametro date SQL Server non valido"))
}

fn validate_timestamp(value: &str) -> Result<()> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .map(|_| ())
        .map_err(|_| parameter_error("parametro timestamp SQL Server non valido"))
}

fn validate_timestamp_tz(value: &str) -> Result<()> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|_| ())
        .map_err(|_| parameter_error("parametro datetimeoffset SQL Server non valido"))
}

fn validate_decimal(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return Err(parameter_error("parametro decimal SQL Server vuoto"));
    }
    let start = usize::from(matches!(bytes[0], b'+' | b'-'));
    let mut digits = 0_usize;
    let mut separators = 0_usize;
    for byte in &bytes[start..] {
        match byte {
            b'0'..=b'9' => digits += 1,
            b'.' if separators == 0 => separators += 1,
            _ => {
                return Err(parameter_error("parametro decimal SQL Server non canonico"));
            }
        }
    }
    if digits == 0 {
        return Err(parameter_error("parametro decimal SQL Server senza cifre"));
    }
    Ok(())
}

fn validate_uuid(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || ![8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| ![8, 13, 18, 23].contains(&index) && !byte.is_ascii_hexdigit())
    {
        return Err(parameter_error("parametro UUID SQL Server non valido"));
    }
    Ok(())
}

fn parameter_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        ErrorPhase::Prepare,
        Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        message,
    )
}

fn unsupported(message: &'static str) -> DatabaseError {
    DatabaseError::unsupported(
        plenora_database_core::plan::ProviderKind::Sqlserver,
        ErrorPhase::Prepare,
        message,
    )
}

#[cfg(test)]
#[path = "parameter_tests.rs"]
mod tests;
