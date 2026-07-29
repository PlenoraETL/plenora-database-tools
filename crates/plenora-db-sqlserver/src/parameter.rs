use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use std::collections::BTreeSet;
use tiberius::Query;

pub fn bind_parameters(
    query: &mut Query<'static>,
    bind_names: &[String],
    parameters: &ParameterBag,
) -> Result<()> {
    let unique = bind_names.iter().collect::<BTreeSet<_>>();
    if unique.len() != parameters.len() {
        return Err(parameter_error(
            "insieme parametri SQL Server diverso dai bind richiesti",
        ));
    }
    for name in bind_names {
        let value = parameters
            .get(name)
            .ok_or_else(|| parameter_error("parametro SQL Server mancante"))?;
        match value {
            ParameterValue::Bool(value) => query.bind(*value),
            ParameterValue::I32(value) => query.bind(*value),
            ParameterValue::I64(value) => query.bind(*value),
            ParameterValue::F64(value) => query.bind(*value),
            ParameterValue::String(value) => query.bind(value.clone()),
            ParameterValue::Bytes(value) => query.bind(value.clone()),
            ParameterValue::Date(value) => {
                chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                    .map_err(|_| parameter_error("parametro date SQL Server non valido"))?;
                query.bind(value.clone());
            }
            ParameterValue::Timestamp(value) => {
                chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                    .map_err(|_| parameter_error("parametro timestamp SQL Server non valido"))?;
                query.bind(value.clone());
            }
            ParameterValue::TimestampTz(value) => {
                chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
                    parameter_error("parametro datetimeoffset SQL Server non valido")
                })?;
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
            ParameterValue::Json(value) => {
                let encoded = serde_json::to_string(value)
                    .map_err(|_| parameter_error("parametro JSON SQL Server non serializzabile"))?;
                query.bind(encoded);
            }
            ParameterValue::Wkb { .. } => {
                return Err(unsupported(
                    "bind WKB SQL Server richiede tipo spatial e SRID risolti",
                ));
            }
            ParameterValue::Null { .. } => {
                return Err(unsupported(
                    "NULL bindato SQL Server richiede un tipo target risolto",
                ));
            }
        }
    }
    Ok(())
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
    DatabaseError {
        category: ErrorCategory::InvalidPlan,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.to_owned(),
    }
}

fn unsupported(message: &'static str) -> DatabaseError {
    DatabaseError::unsupported(
        plenora_database_core::plan::ProviderKind::Sqlserver,
        ErrorPhase::Prepare,
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn rejects_unused_and_missing_parameters_before_io() {
        let parameters = ParameterBag::new(BTreeMap::from([(
            "unused".to_owned(),
            ParameterValue::I32(1),
        )]));
        let mut query = Query::new("SELECT 1 WHERE 1 = @P1");
        assert!(bind_parameters(&mut query, &["wanted".to_owned()], &parameters).is_err());
    }

    #[test]
    fn decimal_and_uuid_validation_is_strict() {
        for invalid in ["", "+", ".", "1.2.3", "NaN", "１２"] {
            assert!(validate_decimal(invalid).is_err(), "{invalid:?}");
        }
        assert!(validate_decimal("-0.125").is_ok());
        assert!(validate_uuid("550e8400-e29b-41d4-a716-446655440000").is_ok());
        assert!(validate_uuid("550e8400e29b41d4a716446655440000").is_err());
    }
}
