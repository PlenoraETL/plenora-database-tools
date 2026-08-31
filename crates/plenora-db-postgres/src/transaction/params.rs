//! Codec parametri OLTP: mapping `ParameterValue` → `SqlParam` che implementa
//! `ToSql` per `tokio_postgres`. Sottoinsieme sufficiente per i tipi scalari
//! canonici; geometrie/composite passano dal codec del piano dati (`parameter_codec`).
//!
//! UUID e Decimal scelgono la rappresentazione in `to_sql` dal tipo target:
//! per target `UUID`/`NUMERIC` inviano il payload binario (16 byte /
//! Postgres NUMERIC wire format), altrimenti fallback al text encoding
//! del testo originale (utile per `SELECT $1::text::uuid` pattern).

use super::sql::unsupported_param;
use crate::error::public_error;
use crate::parameter_codec::{DecimalParameter, IntegerParameter, UuidParameter};
use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

pub(super) enum SqlParam {
    #[allow(dead_code)]
    // il Type documenta l'intenzione lato PostgreSQL anche se `to_sql` restituisce solo IsNull::Yes
    Null(Type),
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    /// UUID: mantiene sia il testo originale (36 char con dash) sia i 16
    /// byte binari. In `to_sql` dispatcha in base al target type.
    Uuid {
        text: String,
        binary: [u8; 16],
    },
    /// Decimal: text originale + rappresentazione binaria (i128 + scale).
    /// In `to_sql` dispatcha in base al target type.
    Decimal {
        text: String,
        binary: DecimalParameter,
    },
    Json(serde_json::Value),
}

impl std::fmt::Debug for SqlParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqlParam([REDACTED])")
    }
}

impl ToSql for SqlParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Self::Null(_) => Ok(IsNull::Yes),
            Self::Bool(v) => v.to_sql(ty, out),
            Self::I32(v) => IntegerParameter(i64::from(*v)).to_sql(ty, out),
            Self::I64(v) => IntegerParameter(*v).to_sql(ty, out),
            // Il target preparato richiede esplicitamente la rappresentazione
            // IEEE-754 a 32 bit; la riduzione di precisione e quindi intenzionale.
            #[allow(clippy::cast_possible_truncation)]
            Self::F64(v) if *ty == Type::FLOAT4 => (*v as f32).to_sql(ty, out),
            Self::F64(v) => v.to_sql(ty, out),
            Self::String(v) => v.as_str().to_sql(ty, out),
            Self::Bytes(v) => v.as_slice().to_sql(ty, out),
            Self::Date(v) => v.to_sql(ty, out),
            Self::Timestamp(v) => v.to_sql(ty, out),
            Self::TimestampTz(v) => v.to_sql(ty, out),
            Self::Uuid { text, binary } => {
                // Se il target è UUID → invio i 16 byte binari (formato
                // wire Postgres). Altrimenti → text (utile per pattern
                // `($1::text)::uuid` e per colonne TEXT che contengono
                // stringhe UUID).
                if *ty == Type::UUID {
                    UuidParameter(*binary).to_sql(ty, out)
                } else {
                    text.as_str().to_sql(ty, out)
                }
            }
            Self::Decimal { text, binary } => {
                if *ty == Type::NUMERIC {
                    binary.to_sql(ty, out)
                } else {
                    text.as_str().to_sql(ty, out)
                }
            }
            Self::Json(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

pub(super) fn encode_params(params: &[ParameterValue]) -> Result<Vec<SqlParam>> {
    params.iter().map(encode_param).collect()
}

pub(super) fn validate_parameter_targets(
    params: &[ParameterValue],
    targets: &[Type],
) -> Result<()> {
    if params.len() != targets.len() {
        return Err(public_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            false,
            "numero di parametri diverso dai target PostgreSQL preparati",
        ));
    }
    for (index, (param, target)) in params.iter().zip(targets).enumerate() {
        if parameter_accepts_target(param, target) {
            continue;
        }
        return Err(public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            false,
            &format!(
                "bind PostgreSQL incompatibile al parametro {}: tipo portabile {}, target {}",
                index + 1,
                portable_type_name(param),
                target.name()
            ),
        ));
    }
    Ok(())
}

fn parameter_accepts_target(param: &ParameterValue, target: &Type) -> bool {
    let textual = matches!(
        *target,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
    );
    match param {
        ParameterValue::Null { type_name } => map_null_type(type_name) == *target,
        ParameterValue::Bool(_) => *target == Type::BOOL,
        ParameterValue::I32(value) => match *target {
            Type::INT2 => i16::try_from(*value).is_ok(),
            Type::INT4 | Type::INT8 | Type::NUMERIC => true,
            _ => false,
        },
        ParameterValue::I64(value) => match *target {
            Type::INT2 => i16::try_from(*value).is_ok(),
            Type::INT4 => i32::try_from(*value).is_ok(),
            Type::INT8 | Type::NUMERIC => true,
            _ => false,
        },
        ParameterValue::F64(_) => matches!(*target, Type::FLOAT4 | Type::FLOAT8),
        ParameterValue::String(_) => textual,
        ParameterValue::Bytes(_) | ParameterValue::Wkb { .. } => *target == Type::BYTEA,
        ParameterValue::Date(_) => *target == Type::DATE,
        ParameterValue::Timestamp(_) => *target == Type::TIMESTAMP,
        ParameterValue::TimestampTz(_) => *target == Type::TIMESTAMPTZ,
        ParameterValue::Uuid(_) => *target == Type::UUID || textual,
        ParameterValue::Decimal(_) => *target == Type::NUMERIC || textual,
        ParameterValue::Json(_) => matches!(*target, Type::JSON | Type::JSONB),
        ParameterValue::Enum { .. } => {
            textual || matches!(target.kind(), tokio_postgres::types::Kind::Enum(_))
        }
    }
}

const fn portable_type_name(param: &ParameterValue) -> &'static str {
    match param {
        ParameterValue::Null { .. } => "null",
        ParameterValue::Bool(_) => "bool",
        ParameterValue::I32(_) => "i32",
        ParameterValue::I64(_) => "i64",
        ParameterValue::F64(_) => "f64",
        ParameterValue::String(_) => "string",
        ParameterValue::Bytes(_) => "bytes",
        ParameterValue::Date(_) => "date",
        ParameterValue::Timestamp(_) => "timestamp",
        ParameterValue::TimestampTz(_) => "timestamp_tz",
        ParameterValue::Json(_) => "json",
        ParameterValue::Wkb { .. } => "wkb",
        ParameterValue::Decimal(_) => "decimal",
        ParameterValue::Uuid(_) => "uuid",
        ParameterValue::Enum { .. } => "enum",
    }
}

fn encode_param(param: &ParameterValue) -> Result<SqlParam> {
    match param {
        ParameterValue::Bool(v) => Ok(SqlParam::Bool(*v)),
        ParameterValue::I32(v) => Ok(SqlParam::I32(*v)),
        ParameterValue::I64(v) => Ok(SqlParam::I64(*v)),
        ParameterValue::F64(v) => Ok(SqlParam::F64(*v)),
        ParameterValue::String(v) => Ok(SqlParam::String(v.clone())),
        ParameterValue::Bytes(v) => Ok(SqlParam::Bytes(v.clone())),
        ParameterValue::Date(v) => v
            .parse::<NaiveDate>()
            .map(SqlParam::Date)
            .map_err(|_| unsupported_param("date non conforme a ISO-8601")),
        ParameterValue::Timestamp(v) => v
            .parse::<NaiveDateTime>()
            .map(SqlParam::Timestamp)
            .map_err(|_| unsupported_param("timestamp non conforme a ISO-8601")),
        ParameterValue::TimestampTz(v) => v
            .parse::<DateTime<Utc>>()
            .map(SqlParam::TimestampTz)
            .map_err(|_| unsupported_param("timestamptz non conforme a RFC-3339")),
        ParameterValue::Uuid(v) => {
            if v.len() != 36 {
                return Err(unsupported_param("uuid non conforme a lunghezza 36"));
            }
            // Parsea a 16 byte per invio binario a Postgres UUID.
            let binary = UuidParameter::parse(v)
                .map_err(|_| unsupported_param("uuid non conforme (hex-digits + dash attesi)"))?;
            Ok(SqlParam::Uuid {
                text: v.clone(),
                binary: binary.0,
            })
        }
        ParameterValue::Json(v) => Ok(SqlParam::Json(v.clone())),
        ParameterValue::Decimal(v) => {
            let binary = DecimalParameter::parse(v)
                .map_err(|_| unsupported_param("decimal non valido (formato numerico atteso)"))?;
            Ok(SqlParam::Decimal {
                text: v.clone(),
                binary,
            })
        }
        ParameterValue::Wkb { .. } => Err(unsupported_param(
            "geometrie non supportate nel path OLTP: usare il piano dati",
        )),
        ParameterValue::Enum { label, .. } => {
            // Il wire format Postgres per enum è la label testuale. Se la
            // colonna target è enum, Postgres applica implicit cast dal
            // text. In altri contesti il consumer deve usare `$1::mood`.
            Ok(SqlParam::String(label.clone()))
        }
        ParameterValue::Null { type_name } => Ok(SqlParam::Null(map_null_type(type_name))),
    }
}

#[allow(clippy::match_same_arms)] // catalogo esplicito + fallback dichiarato
fn map_null_type(type_name: &str) -> Type {
    match type_name.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Type::BOOL,
        "int" | "int4" | "integer" => Type::INT4,
        "int8" | "bigint" => Type::INT8,
        "float8" | "double" => Type::FLOAT8,
        "text" | "string" => Type::TEXT,
        "varchar" | "character varying" => Type::VARCHAR,
        "char" | "character" | "bpchar" => Type::BPCHAR,
        "name" => Type::NAME,
        "bytea" | "binary" => Type::BYTEA,
        "date" => Type::DATE,
        "timestamp" => Type::TIMESTAMP,
        "timestamptz" => Type::TIMESTAMPTZ,
        "uuid" => Type::UUID,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        _ => Type::TEXT,
    }
}

#[cfg(test)]
#[path = "params_tests.rs"]
mod tests;
