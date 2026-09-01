use crate::read::{bind_parameters, decode_hex_binary, parse_date, parse_decimal, parse_timestamp};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::ErrorCategory;
use std::collections::BTreeMap;

#[test]
fn decimal_codec_preserves_the_declared_scale_exactly() {
    assert_eq!(parse_decimal("123.45", 4).expect("decimal"), 1_234_500);
    assert_eq!(parse_decimal("-0.0001", 4).expect("decimal"), -1);
    assert!(parse_decimal("1.23456", 4).is_err());
    assert!(parse_decimal("+", 4).is_err());
    assert!(parse_decimal(".", 4).is_err());
}

#[test]
fn temporal_codecs_use_the_arrow_epoch_and_microseconds() {
    assert_eq!(parse_date("1970-01-01").expect("epoch"), 0);
    assert_eq!(parse_date("1969-12-31").expect("pre-epoch"), -1);
    assert_eq!(
        parse_timestamp("1970-01-01 00:00:00.000001").expect("timestamp"),
        1
    );
    assert_eq!(
        parse_timestamp("1970-01-01-00.00.00.000001").expect("formato Db2"),
        1
    );
}

#[test]
fn repeated_binds_reuse_one_declared_parameter() {
    let names = vec!["id".to_owned(), "id".to_owned()];
    let parameters = ParameterBag::new(BTreeMap::from([("id".to_owned(), ParameterValue::I32(7))]));

    assert_eq!(
        bind_parameters(&names, &parameters).expect("bind ripetuto"),
        ["7", "7"]
    );
}

#[test]
fn extra_parameters_fail_before_the_network() {
    let parameters = ParameterBag::new(BTreeMap::from([(
        "unused".to_owned(),
        ParameterValue::String("dato che non deve apparire nell'errore".to_owned()),
    )]));

    let error = bind_parameters(&[], &parameters).expect_err("parametro extra");

    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert!(!error.message.contains("dato che non deve apparire"));
}

#[test]
fn db2_cli_hex_wkb_decodes_without_accepting_malformed_payloads() {
    let point = decode_hex_binary(b"0101000000000000000000F03F0000000000000040")
        .expect("WKB point esadecimale");
    assert_eq!(point.len(), 21);
    assert_eq!(&point[..5], &[1, 1, 0, 0, 0]);
    assert!(decode_hex_binary(b"010").is_err());
    assert!(decode_hex_binary(b"01GG").is_err());
}
