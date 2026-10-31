use super::decode_mapping_error;

#[test]
fn public_decode_error_contains_only_operational_context() {
    let error = decode_mapping_error(7, "decimal");
    assert_eq!(error.message, "decode decimal idx=7 fallito");
}
#[test]
fn query_temporals_preserve_submicroseconds_and_existing_microsecond_format() {
    for (fraction, expected) in [
        ("1234567", "123456700"),
        ("9999999", "999999900"),
        ("0000005", "000000500"),
        ("0000010", "000001"),
        ("0000000", "000000"),
    ] {
        let value = chrono::NaiveDateTime::parse_from_str(
            &format!("1969-12-31T23:59:59.{fraction}"),
            "%Y-%m-%dT%H:%M:%S%.f",
        )
        .expect("timestamp");
        assert_eq!(
            super::format_query_timestamp(value),
            format!("1969-12-31T23:59:59.{expected}")
        );
        assert_eq!(
            super::format_query_time(value.time()),
            format!("23:59:59.{expected}")
        );
    }
}
