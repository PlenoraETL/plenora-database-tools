use super::*;

#[test]
fn decimal_conversion_is_checked_and_exact() {
    assert_eq!(parse_decimal128("123.45", 4).expect("decimal"), 1_234_500);
    assert_eq!(parse_decimal128("-0.01", 2).expect("decimal"), -1);
    assert!(parse_decimal128("0.001", 2).is_err());
    assert!(parse_decimal128("999999999999999999999999999999999999999", 0).is_err());
}

#[test]
fn temporal_projection_parser_has_checked_boundaries() {
    assert_eq!(parse_date("1970-01-01").expect("epoch"), 0);
    assert_eq!(
        parse_time("23:59:59.1234560").expect("time"),
        86_399_123_456
    );
    assert!(validate_timestamp_tz("2026-01-02T03:04:05.1234567+01:00").is_ok());
    assert!(parse_time("23:59:59.1234567").is_err());
    assert!(parse_timestamp("2026-01-02T03:04:05.1234567").is_err());
    assert!(parse_timestamp("not-a-timestamp").is_err());
}

#[test]
fn native_numeric_rescaling_is_exact_and_checked() {
    let value = tiberius::numeric::Numeric::new_with_scale(12_345, 2);
    assert_eq!(rescale_numeric(value, 4).expect("upscale"), 1_234_500);
    let exact = tiberius::numeric::Numeric::new_with_scale(12_300, 4);
    assert_eq!(rescale_numeric(exact, 2).expect("downscale"), 123);
    let inexact = tiberius::numeric::Numeric::new_with_scale(12_301, 4);
    assert!(rescale_numeric(inexact, 2).is_err());
}

#[test]
fn native_datetimeoffset_formatter_preserves_declared_scale_and_offset() {
    let value =
        DateTime::parse_from_rfc3339("2026-01-02T03:04:05.1234567+01:00").expect("datetimeoffset");
    assert_eq!(
        format_timestamp_tz(value, 7).expect("format"),
        "2026-01-02T03:04:05.1234567+01:00"
    );
    assert!(format_timestamp_tz(value, 8).is_err());
}
