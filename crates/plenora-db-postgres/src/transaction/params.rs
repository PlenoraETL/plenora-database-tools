//! Codec parametri OLTP: mapping `ParameterValue` → `SqlParam` che implementa
//! `ToSql` per `tokio_postgres`. Sottoinsieme sufficiente per i tipi scalari
//! canonici; geometrie/composite passano dal codec del piano dati (`parameter_codec`).
//!
//! UUID e Decimal scelgono la rappresentazione in `to_sql` dal tipo target:
//! per target `UUID`/`NUMERIC` inviano il payload binario (16 byte /
//! Postgres NUMERIC wire format), altrimenti fallback al text encoding
//! del testo originale (utile per `SELECT $1::text::uuid` pattern).

use super::sql::unsupported_param;
use crate::parameter_codec::{DecimalParameter, UuidParameter};
use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::Result;
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
            Self::I32(v) => v.to_sql(ty, out),
            Self::I64(v) => v.to_sql(ty, out),
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
        "text" | "string" | "varchar" => Type::TEXT,
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
