use mysql_async::{Params, Value};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use std::collections::BTreeSet;

/// Codifica i parametri nell'ordine prodotto dal renderer `MySQL`.
///
/// # Errors
///
/// Fallisce chiuso per parametri mancanti, eccedenti o non ancora qualificati.
pub fn bind_parameters(names: &[String], parameters: &ParameterBag) -> Result<Params> {
    let used = names.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if parameters
        .iter()
        .any(|(name, _)| !used.contains(name.as_str()))
    {
        return Err(parameter_error(
            ErrorCategory::InvalidPlan,
            "ParameterBag MySQL contiene parametri non usati",
        ));
    }
    let values = names
        .iter()
        .map(|name| {
            parameters
                .get(name)
                .ok_or_else(|| {
                    parameter_error(
                        ErrorCategory::InvalidPlan,
                        format!("parametro MySQL mancante: {name}"),
                    )
                })
                .and_then(parameter_value)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Params::Positional(values))
}

fn parameter_value(value: &ParameterValue) -> Result<Value> {
    match value {
        ParameterValue::Bool(value) => Ok(Value::Int(i64::from(*value))),
        ParameterValue::I32(value) => Ok(Value::Int(i64::from(*value))),
        ParameterValue::I64(value) => Ok(Value::Int(*value)),
        ParameterValue::F64(value) if value.is_finite() => Ok(Value::Double(*value)),
        ParameterValue::F64(_) => Err(parameter_error(
            ErrorCategory::DataMapping,
            "parametro float MySQL non finito",
        )),
        ParameterValue::String(value)
        | ParameterValue::Date(value)
        | ParameterValue::Timestamp(value)
        | ParameterValue::TimestampTz(value)
        | ParameterValue::Decimal(value)
        | ParameterValue::Uuid(value) => {
            if value.contains('\0') {
                return Err(parameter_error(
                    ErrorCategory::DataMapping,
                    "parametro testuale MySQL contiene NUL",
                ));
            }
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        ParameterValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
        ParameterValue::Json(value) => serde_json::to_vec(value).map(Value::Bytes).map_err(|_| {
            parameter_error(
                ErrorCategory::DataMapping,
                "serializzazione parametro JSON MySQL fallita",
            )
        }),
        ParameterValue::Null { type_name } => {
            if type_name.is_empty() || type_name.contains('\0') {
                return Err(parameter_error(
                    ErrorCategory::DataMapping,
                    "tipo NULL MySQL vuoto o contenente NUL",
                ));
            }
            Ok(Value::NULL)
        }
        ParameterValue::Wkb { .. } => Err(parameter_error(
            ErrorCategory::Unsupported,
            "parametro WKB MySQL richiede SRID e preflight spatial qualificato",
        )),
    }
}

fn parameter_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn binding_is_positional_and_rejects_extra_parameters() {
        let parameters = ParameterBag::new(BTreeMap::from([
            ("first".to_owned(), ParameterValue::I32(7)),
            ("second".to_owned(), ParameterValue::String("x".to_owned())),
        ]));
        let bound =
            bind_parameters(&["second".to_owned(), "first".to_owned()], &parameters).expect("bind");
        assert_eq!(
            bound,
            Params::Positional(vec![Value::Bytes(vec![b'x']), Value::Int(7)])
        );
        assert_eq!(
            bind_parameters(&["first".to_owned()], &parameters)
                .expect_err("extra parameter")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    #[test]
    fn wkb_is_rejected_until_srid_preflight_exists() {
        let parameters = ParameterBag::new(BTreeMap::from([(
            "shape".to_owned(),
            ParameterValue::Wkb {
                bytes: vec![
                    1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
                srid: Some(4_326),
                dimensions: plenora_database_core::geometry::Dimensions::Xy,
                semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
            },
        )]));
        assert_eq!(
            bind_parameters(&["shape".to_owned()], &parameters)
                .expect_err("unqualified WKB")
                .category,
            ErrorCategory::Unsupported
        );
    }
}
