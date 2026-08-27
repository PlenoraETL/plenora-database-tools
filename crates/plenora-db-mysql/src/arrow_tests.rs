use super::*;

#[test]
fn decimal_parser_is_exact_and_checked() {
    assert_eq!(parse_decimal128("1234.5", 4).expect("decimal"), 12_345_000);
    assert_eq!(parse_decimal128("-0.25", 2).expect("negative"), -25);
    assert!(parse_decimal128("1.234", 2).is_err());
    assert!(parse_decimal128("NaN", 2).is_err());
}

#[test]
fn zero_dates_fail_closed_without_panicking() {
    let mut builder = Date32Builder::new();
    assert_eq!(
        append_date(&mut builder, &Value::Date(0, 0, 0, 0, 0, 0, 0))
            .expect_err("zero date")
            .category,
        ErrorCategory::DataMapping
    );
}
