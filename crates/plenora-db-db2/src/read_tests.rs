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
        [ParameterValue::I32(7), ParameterValue::I32(7)]
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

#[test]
fn un_timestamp_oltre_il_microsecondo_non_si_tronca_in_lettura() {
    use plenora_database_core::parameter_fidelity::has_sub_microsecond as has_sub_microsecond_digits;
    assert!(has_sub_microsecond_digits("2026-01-01-00.00.00.123456789"));
    assert!(has_sub_microsecond_digits(
        "2026-01-01 00:00:00.000000000001"
    ));
    assert!(!has_sub_microsecond_digits("2026-01-01-00.00.00.123456000"));
    assert!(!has_sub_microsecond_digits("2026-01-01-00.00.00.123"));
    assert!(!has_sub_microsecond_digits("2026-01-01-00.00.00"));
    let error = parse_timestamp("2026-01-01-00.00.00.123456789").expect_err("cifre perse");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::DataMapping
    );
    assert_eq!(
        parse_timestamp("1970-01-01-00.00.00.000001000").expect("zeri oltre il microsecondo"),
        1
    );
    assert_eq!(
        crate::transaction::canonical_timestamp("2026-08-27-12.34.56.123456789").unwrap(),
        "2026-08-27T12:34:56.123456789"
    );
}

#[test]
fn un_float_della_query_si_legge_esatto_e_finito() {
    use crate::transaction::decode_float;
    use plenora_database_core::provider::ParameterValue;
    let value = 0.1 + 0.2;
    assert_eq!(
        decode_float(Some(value), odbc_api::DataType::Double).expect("float"),
        ParameterValue::F64(value)
    );
    assert_eq!(
        decode_float(Some(f64::MAX), odbc_api::DataType::Double).expect("massimo"),
        ParameterValue::F64(f64::MAX)
    );
    assert!(matches!(
        decode_float(None, odbc_api::DataType::Real).expect("NULL"),
        ParameterValue::Null { .. }
    ));
    assert!(decode_float(Some(f64::NAN), odbc_api::DataType::Double).is_err());
}

#[test]
fn timestamp_arrow_requires_exact_microseconds_before_chrono_parsing() {
    for precision in 7..=12 {
        for fraction in [
            format!("123456{}1", "0".repeat(precision - 7)),
            "9".repeat(precision),
        ] {
            let value = format!("2026-10-31-12.34.56.{fraction}");
            let error = parse_timestamp(&value).expect_err("perdita di precisione");
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert!(!error.message.contains(&fraction));
        }
        let exact = format!("1969-12-31-23.59.59.999999{}", "0".repeat(precision - 6));
        assert_eq!(
            parse_timestamp(&exact).expect("esatto prima dell'epoch"),
            -1
        );
    }
}
