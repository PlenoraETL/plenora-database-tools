//! Decoder Row: da `tokio_postgres::Row` a `Vec<Row>` con `ParameterValue`
//! canonici. Wrapper `FromSql` custom per tipi che tokio-postgres non mappa
//! a `String` di default (enum, tsvector/tsquery/xml).

use super::sql::unsupported_column_type;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::row::Row;
use plenora_database_core::Result;
use std::sync::Arc;
use tokio_postgres::types::Type;

/// Decodifica un batch di righe condividendo l'array dei nomi colonna.
pub(super) fn decode_rows(rows: &[tokio_postgres::Row]) -> Result<Vec<Row>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let columns: Arc<[String]> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_owned())
        .collect::<Vec<_>>()
        .into();
    let mut out = Vec::with_capacity(rows.len());
    for pg_row in rows {
        let values = decode_row(pg_row)?;
        out.push(Row::new(Arc::clone(&columns), values));
    }
    Ok(out)
}

/// Wrapper `FromSql` che accetta un valore di qualsiasi enum e ne estrae
/// il label testuale (che è il wire format nativo Postgres per gli enum).
struct EnumLabel(String);

impl<'a> tokio_postgres::types::FromSql<'a> for EnumLabel {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(std::str::from_utf8(raw)?.to_owned()))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), tokio_postgres::types::Kind::Enum(_))
    }
}

/// Wrapper `FromSql` per tipi Postgres il cui wire format è UTF-8 testuale
/// ma che non hanno un mapping `FromSql<String>` nativo in `tokio_postgres`.
/// Copre: `tsvector`, `tsquery`, `xml`.
///
/// **Non** copre `cidr`, `inet`, `macaddr`, `money`: questi hanno wire
/// format binario Postgres-specifico (byte header + payload); per ora il
/// consumer deve fare cast esplicito lato SQL (`column::text`) o usare
/// il data plane Arrow.
struct PostgresTextRepr(String);

impl<'a> tokio_postgres::types::FromSql<'a> for PostgresTextRepr {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(std::str::from_utf8(raw)?.to_owned()))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TS_VECTOR | Type::TSQUERY | Type::XML)
    }
}

#[allow(clippy::too_many_lines)] // catalogo Postgres type → ParameterValue intenzionalmente lineare
fn decode_row(row: &tokio_postgres::Row) -> Result<Vec<ParameterValue>> {
    use tokio_postgres::types::Kind;

    let mut values = Vec::with_capacity(row.len());
    for (index, column) in row.columns().iter().enumerate() {
        let pg_type = column.type_();
        let type_name = pg_type.name().to_owned();

        // Enum: intercettato via kind() perché il type_name è custom e non
        // matcha nella tabella Type::* sotto. Domain: tokio_postgres di
        // solito espone direttamente il base type sulla colonna, quindi
        // cade nel match Type::* sotto senza handling speciale. Composite:
        // out-of-scope, produce Unsupported.
        if let Kind::Enum(_) = pg_type.kind() {
            let raw: Option<EnumLabel> = row
                .try_get(index)
                .map_err(crate::error::row_decode_error)?;
            values.push(match raw {
                Some(EnumLabel(label)) => ParameterValue::Enum { type_name, label },
                None => ParameterValue::Null { type_name },
            });
            continue;
        }
        if let Kind::Composite(_) = pg_type.kind() {
            return Err(unsupported_column_type(pg_type));
        }

        let value = match *pg_type {
            Type::BOOL => row
                .try_get::<_, Option<bool>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::Bool))
                .map_err(crate::error::row_decode_error)?,
            Type::INT2 => row
                .try_get::<_, Option<i16>>(index)
                .map(|v| optional_to_param(v.map(i32::from), &type_name, ParameterValue::I32))
                .map_err(crate::error::row_decode_error)?,
            Type::INT4 => row
                .try_get::<_, Option<i32>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::I32))
                .map_err(crate::error::row_decode_error)?,
            Type::INT8 => row
                .try_get::<_, Option<i64>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::I64))
                .map_err(crate::error::row_decode_error)?,
            Type::FLOAT4 => row
                .try_get::<_, Option<f32>>(index)
                .map(|v| optional_to_param(v.map(f64::from), &type_name, ParameterValue::F64))
                .map_err(crate::error::row_decode_error)?,
            Type::FLOAT8 => row
                .try_get::<_, Option<f64>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::F64))
                .map_err(crate::error::row_decode_error)?,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
                .try_get::<_, Option<String>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::String))
                .map_err(crate::error::row_decode_error)?,
            // Tipi estesi con wire format testuale: tsvector/tsquery per
            // full-text search, xml. Decodificati come ParameterValue::String
            // con rappresentazione canonica Postgres.
            //
            // cidr/inet/macaddr/money hanno wire binario: se il consumer
            // vuole leggerli deve usare cast esplicito `column::text`.
            Type::TS_VECTOR | Type::TSQUERY | Type::XML => row
                .try_get::<_, Option<PostgresTextRepr>>(index)
                .map(|v| optional_to_param(v.map(|t| t.0), &type_name, ParameterValue::String))
                .map_err(crate::error::row_decode_error)?,
            Type::BYTEA => row
                .try_get::<_, Option<Vec<u8>>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::Bytes))
                .map_err(crate::error::row_decode_error)?,
            Type::DATE => row
                .try_get::<_, Option<NaiveDate>>(index)
                .map(|v| optional_to_param(v.map(|d| d.to_string()), &type_name, ParameterValue::Date))
                .map_err(crate::error::row_decode_error)?,
            Type::TIMESTAMP => row
                .try_get::<_, Option<NaiveDateTime>>(index)
                .map(|v| optional_to_param(v.map(|d| d.format("%Y-%m-%dT%H:%M:%S%.f").to_string()), &type_name, ParameterValue::Timestamp))
                .map_err(crate::error::row_decode_error)?,
            Type::TIMESTAMPTZ => row
                .try_get::<_, Option<DateTime<Utc>>>(index)
                .map(|v| optional_to_param(v.map(|d| d.to_rfc3339()), &type_name, ParameterValue::TimestampTz))
                .map_err(crate::error::row_decode_error)?,
            Type::UUID => {
                use tokio_postgres::types::FromSql;
                struct UuidBytes([u8; 16]);
                impl<'a> FromSql<'a> for UuidBytes {
                    fn from_sql(
                        _ty: &Type,
                        raw: &'a [u8],
                    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                        if raw.len() != 16 {
                            return Err("UUID payload deve essere 16 byte".into());
                        }
                        let mut b = [0u8; 16];
                        b.copy_from_slice(raw);
                        Ok(Self(b))
                    }
                    fn accepts(ty: &Type) -> bool {
                        matches!(*ty, Type::UUID)
                    }
                }
                let raw: Option<UuidBytes> = row
                    .try_get(index)
                    .map_err(crate::error::row_decode_error)?;
                match raw {
                    Some(UuidBytes(b)) => {
                        let text = format!(
                            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                            b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
                        );
                        ParameterValue::Uuid(text)
                    }
                    None => ParameterValue::Null {
                        type_name: type_name.clone(),
                    },
                }
            }
            Type::JSON | Type::JSONB => row
                .try_get::<_, Option<serde_json::Value>>(index)
                .map(|v| optional_to_param(v, &type_name, ParameterValue::Json))
                .map_err(crate::error::row_decode_error)?,
            _ => return Err(unsupported_column_type(pg_type)),
        };
        values.push(value);
    }
    Ok(values)
}

fn optional_to_param<T>(
    value: Option<T>,
    type_name: &str,
    wrap: fn(T) -> ParameterValue,
) -> ParameterValue {
    value.map_or_else(
        || ParameterValue::Null {
            type_name: type_name.to_owned(),
        },
        wrap,
    )
}
