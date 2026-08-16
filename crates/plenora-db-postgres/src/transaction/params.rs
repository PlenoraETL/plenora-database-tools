//! Codec parametri OLTP: mapping `ParameterValue` → `SqlParam` che implementa
//! `ToSql` per `tokio_postgres`. Sottoinsieme sufficiente per i tipi scalari
//! canonici; geometrie/composite passano dal codec del piano dati (`parameter_codec`).
//!
//! v0.3 (P0.7): UUID e Decimal dispatchano in `to_sql` sul target type:
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
mod tests {
    use super::*;
    use plenora_database_core::ErrorCategory;

    fn encode(v: &ParameterValue) -> Result<SqlParam> {
        encode_param(v)
    }

    #[test]
    fn scalar_variants_are_encoded_end_to_end() {
        assert!(matches!(
            encode(&ParameterValue::Bool(true)).unwrap(),
            SqlParam::Bool(true)
        ));
        assert!(matches!(
            encode(&ParameterValue::I32(42)).unwrap(),
            SqlParam::I32(42)
        ));
        assert!(matches!(
            encode(&ParameterValue::I64(-42)).unwrap(),
            SqlParam::I64(-42)
        ));
        assert!(matches!(
            encode(&ParameterValue::F64(3.5)).unwrap(),
            SqlParam::F64(_)
        ));
        assert!(matches!(
            encode(&ParameterValue::String("s".into())).unwrap(),
            SqlParam::String(_)
        ));
        assert!(matches!(
            encode(&ParameterValue::Bytes(vec![1, 2])).unwrap(),
            SqlParam::Bytes(_)
        ));
        assert!(matches!(
            encode(&ParameterValue::Json(serde_json::json!({"k": "v"}))).unwrap(),
            SqlParam::Json(_)
        ));
    }

    #[test]
    fn temporal_scalars_require_iso8601_or_rfc3339() {
        assert!(matches!(
            encode(&ParameterValue::Date("2026-08-12".into())).unwrap(),
            SqlParam::Date(_)
        ));
        assert!(matches!(
            encode(&ParameterValue::Timestamp("2026-08-12T10:00:00".into())).unwrap(),
            SqlParam::Timestamp(_)
        ));
        assert!(matches!(
            encode(&ParameterValue::TimestampTz("2026-08-12T10:00:00Z".into())).unwrap(),
            SqlParam::TimestampTz(_)
        ));

        assert_eq!(
            encode(&ParameterValue::Date("12/08/2026".into()))
                .unwrap_err()
                .category,
            ErrorCategory::Unsupported
        );
        assert_eq!(
            encode(&ParameterValue::Timestamp("nope".into()))
                .unwrap_err()
                .category,
            ErrorCategory::Unsupported
        );
        assert_eq!(
            encode(&ParameterValue::TimestampTz("2026-08-12 10:00:00".into()))
                .unwrap_err()
                .category,
            ErrorCategory::Unsupported
        );
    }

    #[test]
    fn uuid_validates_length_36() {
        let ok = "11111111-2222-3333-4444-555555555555";
        assert!(matches!(
            encode(&ParameterValue::Uuid(ok.into())).unwrap(),
            SqlParam::Uuid { .. }
        ));

        let short = "not-a-uuid";
        assert_eq!(
            encode(&ParameterValue::Uuid(short.into()))
                .unwrap_err()
                .category,
            ErrorCategory::Unsupported
        );
    }

    #[test]
    fn enum_is_encoded_as_text_label() {
        let encoded = encode(&ParameterValue::Enum {
            type_name: "mood".into(),
            label: "sad".into(),
        })
        .unwrap();
        match encoded {
            SqlParam::String(s) => assert_eq!(s, "sad"),
            _ => panic!("enum deve essere encoded come text"),
        }
    }

    #[test]
    fn wkb_is_rejected_from_oltp_path() {
        use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
        let wkb_err = encode(&ParameterValue::Wkb {
            bytes: vec![1, 2, 3],
            srid: Some(4326),
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geometry,
        })
        .unwrap_err();
        assert_eq!(wkb_err.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn decimal_is_encoded_with_dual_representation() {
        // v0.3 (P0.7): Decimal ora è supportato nel path OLTP.
        let encoded = encode(&ParameterValue::Decimal("1234.56".into())).unwrap();
        match encoded {
            SqlParam::Decimal { text, .. } => assert_eq!(text, "1234.56"),
            _ => panic!("Decimal deve essere encoded come SqlParam::Decimal"),
        }
    }

    #[test]
    fn decimal_invalid_format_is_rejected() {
        let err = encode(&ParameterValue::Decimal("non-numerico".into())).unwrap_err();
        assert_eq!(err.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn uuid_is_encoded_with_dual_representation() {
        // v0.3 (P0.7): UUID ora invia binary sui target Type::UUID.
        let encoded = encode(&ParameterValue::Uuid(
            "550e8400-e29b-41d4-a716-446655440000".into(),
        ))
        .unwrap();
        match encoded {
            SqlParam::Uuid { text, binary } => {
                assert_eq!(text, "550e8400-e29b-41d4-a716-446655440000");
                assert_eq!(binary.len(), 16);
                // Primo byte del UUID di test: 0x55.
                assert_eq!(binary[0], 0x55);
                assert_eq!(binary[15], 0x00);
            }
            _ => panic!("Uuid deve essere encoded come SqlParam::Uuid"),
        }
    }

    #[test]
    fn uuid_invalid_hex_is_rejected() {
        let err = encode(&ParameterValue::Uuid(
            "ZZZe8400-e29b-41d4-a716-446655440000".into(),
        ))
        .unwrap_err();
        assert_eq!(err.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn null_type_hint_maps_to_pg_type_with_text_fallback() {
        assert_eq!(map_null_type("bool"), Type::BOOL);
        assert_eq!(map_null_type("BOOLEAN"), Type::BOOL);
        assert_eq!(map_null_type("integer"), Type::INT4);
        assert_eq!(map_null_type("int8"), Type::INT8);
        assert_eq!(map_null_type("float8"), Type::FLOAT8);
        assert_eq!(map_null_type("text"), Type::TEXT);
        assert_eq!(map_null_type("varchar"), Type::TEXT);
        assert_eq!(map_null_type("bytea"), Type::BYTEA);
        assert_eq!(map_null_type("date"), Type::DATE);
        assert_eq!(map_null_type("timestamp"), Type::TIMESTAMP);
        assert_eq!(map_null_type("timestamptz"), Type::TIMESTAMPTZ);
        assert_eq!(map_null_type("uuid"), Type::UUID);
        assert_eq!(map_null_type("json"), Type::JSON);
        assert_eq!(map_null_type("jsonb"), Type::JSONB);
        // fallback dichiarato: qualsiasi type hint sconosciuto → TEXT.
        assert_eq!(map_null_type("hstore"), Type::TEXT);
        assert_eq!(map_null_type(""), Type::TEXT);
    }

    #[test]
    fn null_variant_is_encoded_with_the_declared_type() {
        let encoded = encode(&ParameterValue::Null {
            type_name: "uuid".into(),
        })
        .unwrap();
        match encoded {
            SqlParam::Null(t) => assert_eq!(t, Type::UUID),
            _ => panic!("Null deve essere encoded come SqlParam::Null"),
        }
    }

    #[test]
    fn encode_params_preserves_order_and_length() {
        let vs = vec![
            ParameterValue::I32(1),
            ParameterValue::String("two".into()),
            ParameterValue::Bool(false),
        ];
        let encoded = encode_params(&vs).unwrap();
        assert_eq!(encoded.len(), 3);
        assert!(matches!(encoded[0], SqlParam::I32(1)));
        assert!(matches!(&encoded[1], SqlParam::String(s) if s == "two"));
        assert!(matches!(encoded[2], SqlParam::Bool(false)));
    }

    #[test]
    fn encode_params_short_circuits_on_first_error() {
        let vs = vec![
            ParameterValue::I32(1),
            ParameterValue::Uuid("bad".into()),
            ParameterValue::I32(2),
        ];
        assert!(encode_params(&vs).is_err());
    }

    #[test]
    fn debug_impl_redacts_the_value() {
        let s = format!(
            "{:?}",
            SqlParam::String("segreto-che-non-deve-comparire".into())
        );
        assert!(
            !s.contains("segreto"),
            "Debug non deve rivelare i valori: {s}"
        );
        assert!(s.contains("REDACTED"));
    }
}
