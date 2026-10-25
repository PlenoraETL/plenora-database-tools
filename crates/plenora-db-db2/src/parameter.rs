//! Binding esatto dei parametri Db2 (regola di
//! `plenora_database_core::parameter_fidelity`).
//!
//! Il driver CLI converte un parametro nel tipo che il server ha dato al
//! segnaposto, e la conversione non avvisa quando cambia il valore: un testo
//! `12345.67890001` su un segnaposto `DECIMAL(12, 4)` diventava
//! `12345.6789`, e `col = ?` ritrovava una riga diversa; byte spediti come
//! testo esadecimale finivano in una colonna `FOR BIT DATA` come i codici
//! ASCII delle cifre; un double scritto in cifre perdeva il segno dello zero.
//!
//! Qui ogni valore si lega in due tempi. [`validate`] rifiuta, prima di
//! qualunque connessione, cio che nessun segnaposto Db2 riceve esatto (JSON,
//! `TimestampTz`, WKB, double non finiti o `-0.0`, frazioni oltre il
//! microsecondo, decimali fuori dalla forma posizionale). [`bind`] riceve poi
//! il tipo che il server descrive per il segnaposto (`SQLDescribeParam`) e
//! sceglie il tipo C del binding: binario per i byte, `f64`/`f32` per i
//! floating point, interi nativi, testo soltanto dove il testo e il valore o
//! dove il tipo descritto lo contiene per intero. Tutto il resto si rifiuta.
//!
//! Il testo viaggia nella codepage del client; `crate::connection` rifiuta
//! una connessione la cui codepage non e UTF-8, cosi i byte del testo sono
//! quelli che il database conserva.

use odbc_api::parameter::InputParameter;
use odbc_api::sys::SqlDataType;
use odbc_api::{DataType, IntoParameter};
use plenora_database_core::parameter_fidelity::{
    canonical_uuid, exact_f32, reject_sub_microsecond, DecimalLiteral,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

const PROVIDER: ProviderKind = ProviderKind::Db2;
// Discriminator IBM CLI dei LOB: BLOB, CLOB e DBCLOB.
const SQL_BLOB: SqlDataType = SqlDataType(-98);
const SQL_CLOB: SqlDataType = SqlDataType(-99);
const SQL_DBCLOB: SqlDataType = SqlDataType(-350);
// Il massimo intero che un double, o un real, rappresentano senza buchi.
const DOUBLE_EXACT_INTEGER: u64 = 1 << 53;
const REAL_EXACT_INTEGER: u64 = 1 << 24;

pub type Bound = Box<dyn InputParameter>;

fn mapping(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        Some(PROVIDER),
        message,
    )
}

/// Rifiuta i valori che nessun segnaposto Db2 riceve esatti, senza
/// connessione: l'errore arriva prima di ogni invio.
///
/// # Errors
///
/// `Unsupported` per le varianti senza binding Db2, `DataMapping` per i
/// valori fuori dalla regola.
pub fn validate(values: &[ParameterValue]) -> Result<()> {
    values.iter().try_for_each(validate_one)
}

fn validate_one(value: &ParameterValue) -> Result<()> {
    match value {
        ParameterValue::F64(value) if !value.is_finite() => Err(DatabaseError::unsupported(
            PROVIDER,
            ErrorPhase::Prepare,
            "double non finito senza rappresentazione Db2",
        )),
        // Misurato: fuori dal dominio DOUBLE di Db2 (SQLSTATE 22003).
        ParameterValue::F64(value) if value.is_subnormal() => {
            Err(mapping("double subnormale fuori dal dominio DOUBLE Db2"))
        }
        // Misurato: Db2 conserva `-0.0` come `+0.0` in DOUBLE e REAL.
        ParameterValue::F64(value) if value.to_bits() == (-0.0_f64).to_bits() => {
            Err(mapping("double -0.0: Db2 conserva lo zero senza segno"))
        }
        ParameterValue::Decimal(value) => DecimalLiteral::parse(PROVIDER, value).map(|_| ()),
        ParameterValue::Date(value) => chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(|_| ())
            .map_err(|_| mapping("parametro date Db2 non valido")),
        ParameterValue::Timestamp(value) => timestamp(value).map(|_| ()),
        ParameterValue::Uuid(value) => canonical_uuid(PROVIDER, value),
        ParameterValue::Json(_) | ParameterValue::TimestampTz(_) | ParameterValue::Wkb { .. } => {
            Err(DatabaseError::unsupported(
                PROVIDER,
                ErrorPhase::Prepare,
                "tipo parametro transazionale Db2 non ancora qualificato",
            ))
        }
        _ => Ok(()),
    }
}

fn timestamp(value: &str) -> Result<chrono::NaiveDateTime> {
    reject_sub_microsecond(PROVIDER, value)?;
    // La write Arrow scrive il separatore spazio, il contratto la `T`.
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .map_err(|_| mapping("parametro timestamp Db2 non valido"))
}

const fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Char { .. }
            | DataType::WChar { .. }
            | DataType::Varchar { .. }
            | DataType::WVarchar { .. }
            | DataType::LongVarchar { .. }
            | DataType::WLongVarchar { .. }
            | DataType::Other {
                data_type: SQL_CLOB | SQL_DBCLOB,
                ..
            }
    )
}

const fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary { .. }
            | DataType::Varbinary { .. }
            | DataType::LongVarbinary { .. }
            | DataType::Other {
                data_type: SQL_BLOB,
                ..
            }
    )
}

fn text(value: String) -> Bound {
    Box::new(value.into_parameter())
}

/// Il binding di un valore gia validato sul tipo descritto del segnaposto.
///
/// # Errors
///
/// `DataMapping` in fase `Prepare` se il tipo descritto non riceve il valore
/// esatto.
pub fn bind(value: &ParameterValue, described: &DataType) -> Result<Bound> {
    validate_one(value)?;
    match value {
        ParameterValue::Null { .. } => Ok(if is_binary(described) {
            Box::new(Option::<Vec<u8>>::None.into_parameter())
        } else {
            Box::new(Option::<String>::None.into_parameter())
        }),
        ParameterValue::Bool(value) => match described {
            DataType::Bit => Ok(text(value.to_string())),
            _ => Err(mapping("boolean su un segnaposto Db2 non BOOLEAN")),
        },
        ParameterValue::I32(value) => integer(i64::from(*value), described),
        ParameterValue::I64(value) => integer(*value, described),
        ParameterValue::F64(value) => double(*value, described),
        ParameterValue::Decimal(value) => decimal(value, described),
        ParameterValue::Date(value) => match described {
            DataType::Date => Ok(text(value.clone())),
            data_type if is_text(data_type) => Ok(text(value.clone())),
            _ => Err(mapping("date su un segnaposto Db2 non DATE")),
        },
        ParameterValue::Timestamp(value) => timestamp_parameter(value, described),
        // Su un segnaposto binario (una colonna FOR BIT DATA, un `LIKE` su un
        // CAST di una geometria) il testo arriva come i suoi byte UTF-8, che
        // la codepage verificata alla connessione garantisce: e il valore del
        // testo, non una sua conversione.
        ParameterValue::String(value)
        | ParameterValue::Uuid(value)
        | ParameterValue::Enum { label: value, .. } => {
            if is_text(described) || is_binary(described) {
                Ok(text(value.clone()))
            } else {
                Err(mapping(
                    "testo su un segnaposto Db2 non testuale: usare il tipo del valore",
                ))
            }
        }
        ParameterValue::Bytes(value) => {
            if is_binary(described) {
                Ok(Box::new(value.clone().into_parameter()))
            } else {
                Err(mapping("byte su un segnaposto Db2 non binario"))
            }
        }
        ParameterValue::Json(_) | ParameterValue::TimestampTz(_) | ParameterValue::Wkb { .. } => {
            Err(DatabaseError::unsupported(
                PROVIDER,
                ErrorPhase::Prepare,
                "tipo parametro transazionale Db2 non ancora qualificato",
            ))
        }
    }
}

fn integer(value: i64, described: &DataType) -> Result<Bound> {
    match described {
        DataType::SmallInt | DataType::Integer | DataType::BigInt | DataType::TinyInt => {
            Ok(Box::new(value))
        }
        DataType::Decimal { precision, scale } | DataType::Numeric { precision, scale } => {
            let digits = value
                .unsigned_abs()
                .checked_ilog10()
                .map_or(1, |log| log + 1);
            let integer_digits = precision.saturating_sub(usize::try_from(*scale).unwrap_or(0));
            if usize::try_from(digits).unwrap_or(usize::MAX) > integer_digits {
                return Err(mapping(
                    "intero oltre la precisione del segnaposto DECIMAL Db2",
                ));
            }
            Ok(Box::new(value))
        }
        DataType::Double | DataType::Float { .. } | DataType::Real => {
            let limit = if matches!(described, DataType::Real)
                || matches!(described, DataType::Float { precision } if *precision <= 24)
            {
                REAL_EXACT_INTEGER
            } else {
                DOUBLE_EXACT_INTEGER
            };
            if value.unsigned_abs() > limit {
                return Err(mapping(
                    "intero non rappresentabile esatto nel floating point del segnaposto Db2",
                ));
            }
            Ok(Box::new(value))
        }
        data_type if is_text(data_type) => Ok(text(value.to_string())),
        _ => Err(mapping("intero su un segnaposto Db2 non numerico")),
    }
}

fn double(value: f64, described: &DataType) -> Result<Bound> {
    match described {
        DataType::Double => Ok(Box::new(value)),
        DataType::Float { precision } if *precision > 24 => Ok(Box::new(value)),
        DataType::Real | DataType::Float { .. } => exact_f32(value)
            .map(|narrow| Box::new(narrow) as Bound)
            .ok_or_else(|| mapping("double non rappresentabile esatto nel REAL Db2")),
        _ => Err(mapping(
            "double su un segnaposto Db2 non floating point: la conversione lo arrotonderebbe",
        )),
    }
}

fn decimal(value: &str, described: &DataType) -> Result<Bound> {
    let literal = DecimalLiteral::parse(PROVIDER, value)?;
    let canonical = format!(
        "{}{}.{}",
        if literal.negative { "-" } else { "" },
        if literal.integer.is_empty() {
            "0"
        } else {
            &literal.integer
        },
        if literal.fraction.is_empty() {
            "0"
        } else {
            &literal.fraction
        },
    );
    match described {
        DataType::Decimal { precision, scale } | DataType::Numeric { precision, scale } => {
            if literal.fits(*precision, usize::try_from(*scale).unwrap_or(0)) {
                Ok(text(canonical))
            } else {
                Err(mapping(
                    "decimal oltre precisione o scala del segnaposto Db2: il server lo arrotonderebbe",
                ))
            }
        }
        DataType::SmallInt | DataType::Integer | DataType::BigInt
            if literal.significant_fraction().is_empty() =>
        {
            Ok(text(canonical))
        }
        data_type if is_text(data_type) => Ok(text(value.to_owned())),
        _ => Err(mapping(
            "decimal su un segnaposto Db2 non decimale: la conversione lo arrotonderebbe",
        )),
    }
}

fn timestamp_parameter(value: &str, described: &DataType) -> Result<Bound> {
    let parsed = timestamp(value)?;
    match described {
        DataType::Timestamp { precision } => {
            let micros = chrono::Timelike::nanosecond(&parsed) / 1_000;
            let digits = if micros == 0 {
                0
            } else {
                6 - format!("{micros:06}")
                    .bytes()
                    .rev()
                    .take_while(|b| *b == b'0')
                    .count()
            };
            if i16::try_from(digits).unwrap_or(i16::MAX) > *precision {
                return Err(mapping(
                    "timestamp oltre la precisione del segnaposto Db2: il server lo troncherebbe",
                ));
            }
            Ok(text(parsed.format("%Y-%m-%d-%H.%M.%S%.6f").to_string()))
        }
        data_type if is_text(data_type) => Ok(text(value.to_owned())),
        _ => Err(mapping("timestamp su un segnaposto Db2 non TIMESTAMP")),
    }
}

#[cfg(test)]
#[path = "parameter_tests.rs"]
mod tests;
