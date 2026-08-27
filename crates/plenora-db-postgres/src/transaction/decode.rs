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
        out.push(Row::try_new(Arc::clone(&columns), values)?);
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

/// Wrapper `FromSql` per `Type::NUMERIC`. Il wire format
/// binary Postgres NUMERIC è:
///
///   * `ndigits: i16` — numero di digit "chunks" (ogni chunk = 4 cifre
///     decimali, base-10000).
///   * `weight: i16` — posizione del primo digit in base-10000
///     (positiva = parte intera, negativa = solo frazione).
///   * `sign: u16` — 0x0000 positivo, 0x4000 negativo, 0xC000 NaN.
///   * `dscale: i16` — cifre decimali significative dichiarate (0..=127).
///   * `digits: [i16; ndigits]` — ogni digit ∈ [0, 9999], big-endian.
///
/// Ricostruisce la stringa decimale canonica. Non usa `bigdecimal` o
/// `rust_decimal` per mantenere il set di dipendenze del driver minimale.
struct NumericDecoded(String);

impl<'a> tokio_postgres::types::FromSql<'a> for NumericDecoded {
    #[allow(clippy::too_many_lines)] // catalogo casi + assemblaggio stringa esplicito
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        use std::fmt::Write;
        if raw.len() < 8 {
            return Err(format!("NUMERIC payload troppo corto: {} byte < 8", raw.len()).into());
        }
        let ndigits = i16::from_be_bytes([raw[0], raw[1]]);
        let weight = i16::from_be_bytes([raw[2], raw[3]]);
        let sign = u16::from_be_bytes([raw[4], raw[5]]);
        let dscale = i16::from_be_bytes([raw[6], raw[7]]);

        // NaN e infiniti: emessi come stringhe canoniche Postgres.
        match sign {
            0xC000 => return Ok(Self("NaN".to_owned())),
            0xD000 => return Ok(Self("Infinity".to_owned())),
            0xF000 => return Ok(Self("-Infinity".to_owned())),
            _ => {}
        }

        if ndigits < 0 {
            return Err(format!("NUMERIC ndigits negativo: {ndigits}").into());
        }
        let ndigits = usize::try_from(ndigits)?;
        let dscale = usize::try_from(dscale.max(0))?;
        let expected_bytes = 8 + ndigits * 2;
        if raw.len() < expected_bytes {
            return Err(format!(
                "NUMERIC payload {}b < atteso {expected_bytes}b per ndigits={ndigits}",
                raw.len()
            )
            .into());
        }
        let mut digits = Vec::with_capacity(ndigits);
        for i in 0..ndigits {
            let hi = raw[8 + i * 2];
            let lo = raw[8 + i * 2 + 1];
            digits.push(i16::from_be_bytes([hi, lo]));
        }

        // Caso zero puro: ndigits = 0. La stringa è "0" (o "0.000..." se dscale > 0).
        if ndigits == 0 {
            let text = if dscale > 0 {
                format!("0.{}", "0".repeat(dscale))
            } else {
                "0".to_owned()
            };
            return Ok(Self(text));
        }

        // Ricostruzione parte intera: percorriamo le posizioni base-10000 da
        // `weight` giù fino a 0. Il primo digit non ha padding di zeri; i
        // successivi sì (sempre 4 cifre).
        let mut integer_part = String::new();
        if weight >= 0 {
            for pos in (0..=weight).rev() {
                let digit_idx = i32::from(weight) - i32::from(pos);
                let value = usize::try_from(digit_idx).ok().and_then(|i| digits.get(i));
                let chunk = value.copied().unwrap_or(0);
                if integer_part.is_empty() {
                    integer_part.push_str(&chunk.to_string());
                } else {
                    write!(integer_part, "{chunk:04}").expect("String write");
                }
            }
        }
        if integer_part.is_empty() {
            integer_part.push('0');
        }

        // Ricostruzione parte frazionaria: posizioni -1, -2, ...
        // Prendiamo dscale cifre totali (arrotondando su di 4 e poi truncate).
        let mut fraction_part = String::new();
        if dscale > 0 {
            let fraction_chunks = dscale.div_ceil(4);
            for chunk_i in 0..fraction_chunks {
                let pos = -1_i32 - i32::try_from(chunk_i)?;
                let digit_idx = i32::from(weight) - pos;
                let value = usize::try_from(digit_idx).ok().and_then(|i| digits.get(i));
                let chunk = value.copied().unwrap_or(0);
                write!(fraction_part, "{chunk:04}").expect("String write");
            }
            fraction_part.truncate(dscale);
            while fraction_part.len() < dscale {
                fraction_part.push('0');
            }
        }

        let mut result = String::new();
        if sign == 0x4000 {
            result.push('-');
        }
        result.push_str(&integer_part);
        if dscale > 0 {
            result.push('.');
            result.push_str(&fraction_part);
        }
        Ok(Self(result))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

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
            let raw: Option<EnumLabel> =
                row.try_get(index).map_err(crate::error::row_decode_error)?;
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
                .map(|v| {
                    optional_to_param(v.map(|d| d.to_string()), &type_name, ParameterValue::Date)
                })
                .map_err(crate::error::row_decode_error)?,
            Type::TIMESTAMP => row
                .try_get::<_, Option<NaiveDateTime>>(index)
                .map(|v| {
                    optional_to_param(
                        v.map(|d| d.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
                        &type_name,
                        ParameterValue::Timestamp,
                    )
                })
                .map_err(crate::error::row_decode_error)?,
            Type::TIMESTAMPTZ => row
                .try_get::<_, Option<DateTime<Utc>>>(index)
                .map(|v| {
                    optional_to_param(
                        v.map(|d| d.to_rfc3339()),
                        &type_name,
                        ParameterValue::TimestampTz,
                    )
                })
                .map_err(crate::error::row_decode_error)?,
            Type::UUID => {
                use tokio_postgres::types::FromSql;
                struct UuidBytes([u8; 16]);
                impl<'a> FromSql<'a> for UuidBytes {
                    fn from_sql(
                        _ty: &Type,
                        raw: &'a [u8],
                    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>>
                    {
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
                let raw: Option<UuidBytes> =
                    row.try_get(index).map_err(crate::error::row_decode_error)?;
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
            Type::NUMERIC => row
                .try_get::<_, Option<NumericDecoded>>(index)
                .map(|v| optional_to_param(v.map(|n| n.0), &type_name, ParameterValue::Decimal))
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::FromSql;

    /// Costruisce il payload NUMERIC binary Postgres per unit test.
    fn build_numeric(ndigits: i16, weight: i16, sign: u16, dscale: i16, digits: &[i16]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + digits.len() * 2);
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    fn decode(raw: &[u8]) -> String {
        NumericDecoded::from_sql(&Type::NUMERIC, raw)
            .expect("decode")
            .0
    }

    #[test]
    fn numeric_zero_no_scale() {
        assert_eq!(decode(&build_numeric(0, 0, 0, 0, &[])), "0");
    }

    #[test]
    fn numeric_zero_with_scale_pads_fraction() {
        assert_eq!(decode(&build_numeric(0, 0, 0, 3, &[])), "0.000");
    }

    #[test]
    fn numeric_positive_integer() {
        // 999.99 → chunks: 999, 9900. weight=0, dscale=2.
        assert_eq!(decode(&build_numeric(2, 0, 0, 2, &[999, 9900])), "999.99");
    }

    #[test]
    fn numeric_multi_chunk_integer_pads_lower_chunks() {
        // 1234567.89 → chunks: 123, 4567, 8900. weight=1, dscale=2.
        assert_eq!(
            decode(&build_numeric(3, 1, 0, 2, &[123, 4567, 8900])),
            "1234567.89"
        );
    }

    #[test]
    fn numeric_negative_sign() {
        // -1.5 → chunks: 1, 5000. weight=0, dscale=1, sign=0x4000.
        assert_eq!(decode(&build_numeric(2, 0, 0x4000, 1, &[1, 5000])), "-1.5");
    }

    #[test]
    fn numeric_only_fraction_negative_weight() {
        // 0.001 → chunks: [10]. weight=-1, dscale=3.
        assert_eq!(decode(&build_numeric(1, -1, 0, 3, &[10])), "0.001");
    }

    #[test]
    fn numeric_integer_only_no_scale() {
        // 100 → chunks: [100]. weight=0, dscale=0.
        assert_eq!(decode(&build_numeric(1, 0, 0, 0, &[100])), "100");
    }

    #[test]
    fn numeric_trailing_integer_zeros() {
        // 10000 → chunks: [1, 0]. weight=1, dscale=0.
        assert_eq!(decode(&build_numeric(2, 1, 0, 0, &[1, 0])), "10000");
    }

    #[test]
    fn numeric_nan() {
        assert_eq!(decode(&build_numeric(0, 0, 0xC000, 0, &[])), "NaN");
    }

    #[test]
    fn numeric_infinity() {
        assert_eq!(decode(&build_numeric(0, 0, 0xD000, 0, &[])), "Infinity");
        assert_eq!(decode(&build_numeric(0, 0, 0xF000, 0, &[])), "-Infinity");
    }

    #[test]
    fn numeric_short_payload_is_rejected() {
        // < 8 byte di header.
        assert!(NumericDecoded::from_sql(&Type::NUMERIC, &[0, 0]).is_err());
    }

    #[test]
    fn numeric_truncated_digits_is_rejected() {
        // ndigits=2 dichiarati ma solo 1 digit nel payload.
        let mut buf = build_numeric(2, 0, 0, 0, &[123]);
        buf.truncate(10); // 8 header + 2 = 10 (invece di 12).
        assert!(NumericDecoded::from_sql(&Type::NUMERIC, &buf).is_err());
    }
}
