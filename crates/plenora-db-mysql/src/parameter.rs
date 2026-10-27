use mysql_async::{Params, Value};
use plenora_database_core::parameter_fidelity::{
    canonical_uuid, reject_sub_microsecond, DecimalLiteral,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeSet;

/// Codifica una sequenza posizionale di `ParameterValue` in `Params`.
///
/// Uso: `TransactionScope::execute` riceve `Statement { sql, params:
/// `Vec<ParameterValue>` }` con placeholder `?` `MySQL` nell'ordine di `params`.
///
/// # Errors
///
/// Propaga `parameter_value` errors (float non finiti, stringhe con NUL, ecc.).
pub fn bind_positional_params(params: &[ParameterValue], kind: ProviderKind) -> Result<Params> {
    let values = params
        .iter()
        .map(|value| parameter_value(value, kind))
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        Ok(Params::Empty)
    } else {
        Ok(Params::Positional(values))
    }
}

/// Codifica i parametri nell'ordine prodotto dal renderer `MySQL`.
///
/// # Errors
///
/// Fallisce chiuso per parametri mancanti, eccedenti o non ancora qualificati.
pub fn bind_parameters(
    names: &[String],
    parameters: &ParameterBag,
    kind: ProviderKind,
) -> Result<Params> {
    let used = names.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if parameters
        .iter()
        .any(|(name, _)| !used.contains(name.as_str()))
    {
        return Err(parameter_error(
            ErrorCategory::InvalidPlan,
            "ParameterBag contiene parametri non usati",
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
                        format!("parametro mancante: {name}"),
                    )
                })
                .and_then(|value| parameter_value(value, kind))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Params::Positional(values))
}

/// Converte un singolo `ParameterValue` in `mysql_async::Value`.
///
/// Esposto per il transaction module (positional binding).
///
/// # Errors
///
/// Errore `DataMapping` per float non finiti, stringhe con NUL,
/// tipi non supportati (Geometry, Interval).
///
/// Il testo dei tipi temporali e decimali viaggia come stringa e il server lo
/// converte: per questo la forma si verifica qui, prima dell'invio, e un
/// testo che il server leggerebbe arrotondato o troncato (una frazione oltre
/// il microsecondo, un esponente, un timestamp malformato che `MySQL` in un
/// `WHERE` tronca con un warning) si rifiuta (regola di
/// `plenora_database_core::parameter_fidelity`).
pub fn parameter_value(value: &ParameterValue, kind: ProviderKind) -> Result<Value> {
    match value {
        ParameterValue::Bool(value) => Ok(Value::Int(i64::from(*value))),
        ParameterValue::I32(value) => Ok(Value::Int(i64::from(*value))),
        ParameterValue::I64(value) => Ok(Value::Int(*value)),
        // Misurato: `MariaDB` conserva `-0.0` in un DOUBLE come `+0.0`.
        ParameterValue::F64(value)
            if kind == ProviderKind::Mariadb && value.to_bits() == (-0.0_f64).to_bits() =>
        {
            Err(parameter_error(
                ErrorCategory::DataMapping,
                "double -0.0: MariaDB conserva lo zero senza segno",
            ))
        }
        ParameterValue::F64(value) if value.is_finite() => Ok(Value::Double(*value)),
        ParameterValue::F64(_) => Err(parameter_error(
            ErrorCategory::DataMapping,
            "parametro float non finito",
        )),
        ParameterValue::Date(_) | ParameterValue::Timestamp(_) | ParameterValue::TimestampTz(_) => {
            temporal_value(value, kind)
        }
        // Un decimale intero si lega come intero: legato come testo, il
        // server lo converte attraverso un double contro una colonna intera
        // (misurato: `9007199254740993` in un `BIGINT` diventava
        // `9007199254740992`, e `BIGINT = '9007199254740992'` ritrovava la
        // riga di `9007199254740993`).
        ParameterValue::Decimal(value) => {
            let literal = DecimalLiteral::parse(kind, value)?;
            if literal.fraction.bytes().all(|digit| digit == b'0') {
                let magnitude = if literal.integer.is_empty() {
                    Some(0_i128)
                } else {
                    literal.integer.parse::<i128>().ok()
                };
                let signed = magnitude.map(|magnitude| {
                    if literal.negative {
                        -magnitude
                    } else {
                        magnitude
                    }
                });
                if let Some(integer) = signed.and_then(|value| i64::try_from(value).ok()) {
                    return Ok(Value::Int(integer));
                }
                if let Some(integer) = signed.and_then(|value| u64::try_from(value).ok()) {
                    return Ok(Value::UInt(integer));
                }
            }
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        ParameterValue::Uuid(value) => {
            canonical_uuid(kind, value)?;
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        ParameterValue::String(value) => {
            if value.contains('\0') {
                return Err(parameter_error(
                    ErrorCategory::DataMapping,
                    "parametro testuale contiene NUL",
                ));
            }
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        ParameterValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
        // Misurato: un JSON spedito come stringa su `MySQL` e una stringa, e
        // `col_json = ?` lo confronta come scalare stringa senza ritrovare il
        // documento. Il portable lo lega come `CAST(? AS JSON)`; nello SQL del
        // chiamante il cast va scritto, con il testo legato come `String`.
        // Su `MariaDB` JSON e testo, e il testo e il valore.
        ParameterValue::Json(_) if kind != ProviderKind::Mariadb => {
            Err(DatabaseError::unsupported(
                kind,
                ErrorPhase::Prepare,
                "parametro JSON MySQL nudo: legare il testo come String dentro CAST(? AS JSON)",
            ))
        }
        ParameterValue::Json(value) => serde_json::to_vec(value).map(Value::Bytes).map_err(|_| {
            parameter_error(
                ErrorCategory::DataMapping,
                "serializzazione parametro JSON fallita",
            )
        }),
        ParameterValue::Null { type_name } => {
            if type_name.is_empty() || type_name.contains('\0') {
                return Err(parameter_error(
                    ErrorCategory::DataMapping,
                    "tipo NULL vuoto o contenente NUL",
                ));
            }
            Ok(Value::NULL)
        }
        ParameterValue::Wkb { .. } => Err(parameter_error(
            ErrorCategory::Unsupported,
            "parametro WKB richiede SRID e preflight spatial qualificato",
        )),
        ParameterValue::Enum { label, .. } => {
            if label.contains('\0') {
                return Err(parameter_error(
                    ErrorCategory::DataMapping,
                    "label ENUM contiene NUL",
                ));
            }
            Ok(Value::Bytes(label.as_bytes().to_vec()))
        }
    }
}

/// Il testo di un valore temporale, dopo averne verificato la forma: il
/// server lo convertirebbe, e una forma che non legge esatta (frazione oltre
/// il microsecondo, data inesistente, offset su `MariaDB`) si rifiuta qui.
fn temporal_value(value: &ParameterValue, kind: ProviderKind) -> Result<Value> {
    match value {
        ParameterValue::Date(value) => {
            chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
                parameter_error(ErrorCategory::DataMapping, "parametro date non valido")
            })?;
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        ParameterValue::Timestamp(value) => {
            reject_sub_microsecond(kind, value)?;
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
                .map_err(|_| {
                    parameter_error(ErrorCategory::DataMapping, "parametro timestamp non valido")
                })?;
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        // Misurato: `MariaDB` non legge l'offset; in un INSERT rifiuta (1292),
        // in un `WHERE` lo tronca con un warning e confronta un altro istante.
        ParameterValue::TimestampTz(_) if kind == ProviderKind::Mariadb => {
            Err(DatabaseError::unsupported(
                kind,
                ErrorPhase::Prepare,
                "timestamp con offset non qualificato su MariaDB",
            ))
        }
        ParameterValue::TimestampTz(value) => {
            reject_sub_microsecond(kind, value)?;
            chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
                parameter_error(
                    ErrorCategory::DataMapping,
                    "parametro timestamptz non valido",
                )
            })?;
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        _ => Err(parameter_error(
            ErrorCategory::Internal,
            "valore non temporale nel codec temporale",
        )),
    }
}

fn parameter_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

#[cfg(test)]
#[path = "parameter_tests.rs"]
mod tests;
