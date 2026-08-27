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
