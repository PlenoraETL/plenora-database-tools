use plenora_database_core::parameter_fidelity::{reject_sub_microsecond, DecimalLiteral};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeSet;
use tiberius::numeric::Numeric;
use tiberius::Query;

/// La precisione massima di `decimal` in SQL Server.
const DECIMAL_PRECISION: usize = 38;

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
        // `float` non rappresenta NaN ne gli infiniti: il server rifiutava il
        // flusso TDS dopo l'invio (8023); si rifiuta prima.
        ParameterValue::F64(value) => {
            finite(*value)?;
            query.bind(*value);
        }
        ParameterValue::String(value) => query.bind(value.clone()),
        // I temporali e gli UUID si legano nel proprio tipo: in `nvarchar` il
        // server li convertiva nel tipo dell'operando, e `date = @P1` con
        // `2024-02-29T10:00:00` ritrovava la riga del 29, un
        // `smalldatetime` arrotondava i secondi al minuto e ritrovava
        // `10:20:29` cercando `10:20`.
        ParameterValue::Date(value) => query.bind(validate_date(value)?),
        ParameterValue::Timestamp(value) => query.bind(validate_timestamp(value)?),
        ParameterValue::TimestampTz(value) => query.bind(validate_timestamp_tz(value)?),
        // Un decimale in testo veniva convertito dal server nel tipo della
        // colonna con cui lo si confrontava: `12345.67890001` diventava
        // `12345.6789` e `col = @P1` ritrovava la riga. Si lega come
        // `decimal` nativo, con la propria scala.
        ParameterValue::Decimal(value) => query.bind(decimal(value)?),
        ParameterValue::Uuid(value) => query.bind(validate_uuid(value)?),
        ParameterValue::Bytes(value) => query.bind(value.clone()),
        ParameterValue::Json(value) => {
            let encoded = serde_json::to_string(value)
                .map_err(|_| parameter_error("parametro JSON SQL Server non serializzabile"))?;
            query.bind(encoded);
        }
        ParameterValue::Wkb { bytes, .. } => query.bind(bytes.clone()),
        ParameterValue::Enum { label, .. } => query.bind(label.clone()),
        ParameterValue::Null { type_name } => bind_null_parameter(query, type_name)?,
    }
    Ok(())
}

fn parameter_type(value: &ParameterValue) -> Result<String> {
    let fixed = |name: &str| Ok(name.to_owned());
    match value {
        ParameterValue::Bool(_) => fixed("bit"),
        ParameterValue::I32(_) => fixed("int"),
        ParameterValue::I64(_) => fixed("bigint"),
        ParameterValue::F64(value) => finite(*value).map(|()| "float(53)".to_owned()),
        ParameterValue::String(value) => Ok(string_parameter_type(value).to_owned()),
        ParameterValue::Bytes(value) => fixed(if value.len() <= 8_000 {
            "varbinary(8000)"
        } else {
            "varbinary(max)"
        }),
        ParameterValue::Date(value) => validate_date(value).map(|_| "date".to_owned()),
        ParameterValue::Timestamp(value) => {
            validate_timestamp(value).map(|_| "datetime2(7)".to_owned())
        }
        ParameterValue::TimestampTz(value) => {
            validate_timestamp_tz(value).map(|_| "datetimeoffset(7)".to_owned())
        }
        ParameterValue::Decimal(value) => {
            let numeric = decimal(value)?;
            Ok(format!("decimal({DECIMAL_PRECISION}, {})", numeric.scale()))
        }
        ParameterValue::Uuid(value) => validate_uuid(value).map(|_| "uniqueidentifier".to_owned()),
        ParameterValue::Json(value) => {
            let encoded = serde_json::to_string(value)
                .map_err(|_| parameter_error("parametro JSON SQL Server non serializzabile"))?;
            fixed(string_parameter_type(&encoded))
        }
        ParameterValue::Wkb { bytes, .. } => fixed(if bytes.len() <= 8_000 {
            "varbinary(8000)"
        } else {
            "varbinary(max)"
        }),
        ParameterValue::Enum { label, .. } => fixed(string_parameter_type(label)),
        ParameterValue::Null { type_name } => null_parameter_type(type_name).map(str::to_owned),
    }
}

fn finite(value: f64) -> Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            Some(ProviderKind::Sqlserver),
            "parametro float SQL Server non finito",
        ))
    }
}

/// Il decimale come `decimal` nativo: mantissa e scala del testo, che non
/// supera la precisione di SQL Server.
fn decimal(value: &str) -> Result<Numeric> {
    validate_decimal(value)?;
    let literal = DecimalLiteral::parse(ProviderKind::Sqlserver, value)?;
    if literal.integer.len() + literal.significant_fraction().len() > DECIMAL_PRECISION {
        return Err(DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            Some(ProviderKind::Sqlserver),
            "parametro decimal SQL Server oltre 38 cifre",
        ));
    }
    let (mantissa, scale) = literal.mantissa_and_scale(ProviderKind::Sqlserver)?;
    Ok(Numeric::new_with_scale(mantissa, scale))
}

fn bind_null_parameter(query: &mut Query<'static>, type_name: &str) -> Result<()> {
    match type_name {
        "binary" | "varbinary" => {
            query.bind(Option::<Vec<u8>>::None);
            Ok(())
        }
        _ => Err(unsupported(
            "NULL bindato SQL Server richiede un tipo target qualificato",
        )),
    }
}

fn null_parameter_type(type_name: &str) -> Result<&'static str> {
    match type_name {
        "binary" | "varbinary" => Ok("varbinary(max)"),
        _ => Err(unsupported(
            "NULL bindato SQL Server richiede un tipo target qualificato",
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

fn validate_date(value: &str) -> Result<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| parameter_error("parametro date SQL Server non valido"))
}

fn validate_timestamp(value: &str) -> Result<chrono::NaiveDateTime> {
    reject_sub_microsecond(ProviderKind::Sqlserver, value)?;
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|_| parameter_error("parametro timestamp SQL Server non valido"))
}

fn validate_timestamp_tz(value: &str) -> Result<chrono::DateTime<chrono::FixedOffset>> {
    reject_sub_microsecond(ProviderKind::Sqlserver, value)?;
    chrono::DateTime::parse_from_rfc3339(value)
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

fn validate_uuid(value: &str) -> Result<tiberius::Uuid> {
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
    tiberius::Uuid::parse_str(value)
        .map_err(|_| parameter_error("parametro UUID SQL Server non valido"))
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
