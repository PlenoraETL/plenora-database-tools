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
//! microsecondo, decimali fuori dalla forma posizionale). [`bind_with_catalog`] riceve poi
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
use plenora_database_core::admission_gate::described as admit;
use plenora_database_core::operand_admission::{Charset, LengthUnit, OperandType};
use plenora_database_core::parameter_fidelity::{
    canonical_uuid, exact_f32, reject_sub_microsecond, DecimalLiteral,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

const PROVIDER: ProviderKind = ProviderKind::Db2;
// Discriminator IBM CLI dei testi grafici e dei LOB.
const SQL_GRAPHIC: SqlDataType = SqlDataType(-95);
const SQL_VARGRAPHIC: SqlDataType = SqlDataType(-96);
const SQL_LONGVARGRAPHIC: SqlDataType = SqlDataType(-97);
const SQL_BLOB: SqlDataType = SqlDataType(-98);
const SQL_CLOB: SqlDataType = SqlDataType(-99);
const SQL_DBCLOB: SqlDataType = SqlDataType(-350);
const SQL_DECFLOAT: SqlDataType = SqlDataType(-360);

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

/// Il tipo dell'operando descritto da `SQLDescribeParam`, per la regola
/// unica (`plenora_database_core::operand_admission`).
#[must_use]
pub fn operand_of(described: &DataType) -> OperandType {
    let text = |length, padded, unit| OperandType::Text {
        length,
        padded,
        unit,
        charset: Charset::Unicode,
    };
    let length = |length: &Option<std::num::NonZeroUsize>| {
        length.and_then(|length| u64::try_from(length.get()).ok())
    };
    let integer = |min: i128, max: i128| OperandType::Integer { min, max };
    match described {
        DataType::Bit => OperandType::Boolean,
        DataType::TinyInt => integer(i128::from(i8::MIN), i128::from(i8::MAX)),
        DataType::SmallInt => integer(i128::from(i16::MIN), i128::from(i16::MAX)),
        DataType::Integer => integer(i128::from(i32::MIN), i128::from(i32::MAX)),
        DataType::BigInt => integer(i128::from(i64::MIN), i128::from(i64::MAX)),
        DataType::Real => OperandType::Float { mantissa_bits: 24 },
        DataType::Float { precision } if *precision <= 24 => {
            OperandType::Float { mantissa_bits: 24 }
        }
        DataType::Float { .. } | DataType::Double => OperandType::Float { mantissa_bits: 53 },
        DataType::Decimal { precision, scale } | DataType::Numeric { precision, scale } => {
            match (u32::try_from(*precision), u32::try_from(*scale)) {
                (Ok(precision), Ok(scale)) => OperandType::Decimal { precision, scale },
                _ => OperandType::Unknown,
            }
        }
        DataType::Date => OperandType::Date,
        DataType::Timestamp { precision } => u32::try_from(*precision)
            .map_or(OperandType::Unknown, |fraction_digits| {
                OperandType::Timestamp { fraction_digits }
            }),
        // Il segnaposto descrive `CHAR` e `VARCHAR` in byte della codepage
        // 1208 (la connessione la verifica), `GRAPHIC` in unita UTF-16.
        DataType::Char { length: size } => text(length(size), true, LengthUnit::Bytes),
        DataType::WChar { length: size }
        | DataType::Other {
            data_type: SQL_GRAPHIC,
            column_size: size,
            ..
        } => text(length(size), true, LengthUnit::Utf16),
        DataType::Varchar { length: size }
        | DataType::LongVarchar { length: size }
        | DataType::Other {
            data_type: SQL_CLOB,
            column_size: size,
            ..
        } => text(length(size), false, LengthUnit::Bytes),
        DataType::WVarchar { length: size }
        | DataType::WLongVarchar { length: size }
        | DataType::Other {
            data_type: SQL_VARGRAPHIC | SQL_LONGVARGRAPHIC | SQL_DBCLOB,
            column_size: size,
            ..
        } => text(length(size), false, LengthUnit::Utf16),
        DataType::Binary { length: size } => OperandType::Binary {
            length: length(size),
            padded: true,
        },
        DataType::Varbinary { length: size } | DataType::LongVarbinary { length: size } => {
            OperandType::Binary {
                length: length(size),
                padded: false,
            }
        }
        DataType::Other {
            data_type: SQL_BLOB,
            column_size: size,
            ..
        } => OperandType::Binary {
            length: length(size),
            padded: false,
        },
        DataType::Other {
            data_type: SQL_DECFLOAT,
            column_size,
            ..
        } => OperandType::UnboundedDecimal {
            digits: column_size
                .and_then(|size| u32::try_from(size.get()).ok())
                .unwrap_or(34),
        },
        _ => OperandType::Unknown,
    }
}

/// Il valore che un segnaposto descritto riceve, per il binding a colonne dei
/// parameter array: la stessa regola di [`bind_with_catalog`]. Un testo su un segnaposto
/// binario diventa byte UTF-8 prima del bind C binario. Anche lunghezza e
/// padding passano da [`admit`]; il driver non interpreta cifre esadecimali.
///
/// # Errors
///
/// `DataMapping` in fase `Prepare` se il tipo descritto non riceve il valore
/// esatto.
pub fn admit_described(value: &ParameterValue, described: &DataType) -> Result<ParameterValue> {
    validate_one(value)?;
    if is_binary(described) {
        if let ParameterValue::String(text)
        | ParameterValue::Uuid(text)
        | ParameterValue::Enum { label: text, .. } = value
        {
            return admit(
                PROVIDER,
                &ParameterValue::Bytes(text.as_bytes().to_vec()),
                &operand_of(described),
            );
        }
    }
    let operand = operand_of(described);
    let admitted = admit(PROVIDER, value, &operand)?;
    // SQLDescribeParam non espone TYPESTRINGUNITS: la capacita in byte
    // puo descrivere OCTETS oppure quattro byte per CODEUNITS32. Si prova
    // anche il limite minimo di caratteri, senza indovinare l'unita.
    // Per GRAPHIC il limite puo essere espresso in unita UTF-16 (due per
    // carattere) o byte. Il catalogo portable conserva l'unita esatta.
    if let OperandType::Text {
        length: Some(length),
        unit,
        ..
    } = operand
    {
        let divisor = if unit == LengthUnit::Utf16 { 2 } else { 4 };
        let divisor = if length.is_multiple_of(4) { 4 } else { divisor };
        if length.is_multiple_of(divisor) {
            admit(
                PROVIDER,
                &admitted,
                &OperandType::Text {
                    length: Some(length / divisor),
                    padded: false,
                    unit: LengthUnit::Characters,
                    charset: Charset::Unicode,
                },
            )?;
        }
    }
    Ok(admitted)
}

/// Il catalogo risolve l'unita per bind diretti o CAST identici alla colonna
/// testuale; senza questa prova continua a valere il descrittore.
pub fn admit_with_catalog(
    value: &ParameterValue,
    described: &DataType,
    catalog: Option<&OperandType>,
) -> Result<ParameterValue> {
    validate_one(value)?;
    if let (OperandType::Text { .. }, Some(kind @ OperandType::Text { .. })) =
        (operand_of(described), catalog)
    {
        return admit(PROVIDER, value, kind);
    }
    admit_described(value, described)
}

#[cfg(test)]
pub fn bind(value: &ParameterValue, described: &DataType) -> Result<Bound> {
    bind_with_catalog(value, described, None)
}

/// Il binding di un valore sul tipo descritto del segnaposto: prima la regola
/// unica ([`admit`], che lo riscrive nel tipo dell'operando o lo rifiuta),
/// poi il tipo C esatto del valore ammesso.
///
/// # Errors
///
/// `DataMapping` in fase `Prepare` se il tipo descritto non riceve il valore
/// esatto.
pub fn bind_with_catalog(
    value: &ParameterValue,
    described: &DataType,
    catalog: Option<&OperandType>,
) -> Result<Bound> {
    let value = bind_value_with_catalog(value, described, catalog)?;
    Ok(Box::new(odbc_api::parameter::WithDataType {
        value,
        data_type: *described,
    }))
}

fn bind_value_with_catalog(
    value: &ParameterValue,
    described: &DataType,
    catalog: Option<&OperandType>,
) -> Result<Bound> {
    validate_one(value)?;
    let admitted = admit_with_catalog(value, described, catalog)?;
    match admitted {
        ParameterValue::Null { .. } => Ok(if is_binary(described) {
            Box::new(Option::<Vec<u8>>::None.into_parameter())
        } else {
            Box::new(Option::<String>::None.into_parameter())
        }),
        ParameterValue::Bool(flag) => Ok(text(flag.to_string())),
        ParameterValue::I32(number) => Ok(Box::new(i64::from(number))),
        ParameterValue::I64(number) => Ok(Box::new(number)),
        ParameterValue::F64(number) => match described {
            DataType::Real => narrow(number),
            DataType::Float { precision } if *precision <= 24 => narrow(number),
            _ => Ok(Box::new(number)),
        },
        ParameterValue::Decimal(literal) => Ok(text(literal)),
        ParameterValue::Date(date) => Ok(text(date)),
        ParameterValue::Timestamp(stamp) => Ok(text(
            timestamp(&stamp)?
                .format("%Y-%m-%d-%H.%M.%S%.6f")
                .to_string(),
        )),
        ParameterValue::String(text_value) => Ok(text(text_value)),
        ParameterValue::Bytes(bytes) => Ok(Box::new(bytes.into_parameter())),
        _ => Err(DatabaseError::unsupported(
            PROVIDER,
            ErrorPhase::Prepare,
            "tipo parametro transazionale Db2 non ancora qualificato",
        )),
    }
}

fn narrow(value: f64) -> Result<Bound> {
    exact_f32(value)
        .map(|narrow| Box::new(narrow) as Bound)
        .ok_or_else(|| mapping("double non rappresentabile esatto nel REAL Db2"))
}

#[cfg(test)]
#[path = "parameter_tests.rs"]
mod tests;
