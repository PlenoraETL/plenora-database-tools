use crate::parameter::bind_parameters;
use oracle_rs::Value;
use plenora_database_core::provider::ParameterValue;

#[test]
fn primitive_parameters_keep_their_types() {
    let values = bind_parameters(&[
        ParameterValue::I64(42),
        ParameterValue::String("hello".to_owned()),
        ParameterValue::Bytes(vec![0, 1]),
        ParameterValue::Bool(true),
    ])
    .expect("bind");
    assert!(matches!(values[0], Value::Integer(42)));
    assert!(matches!(&values[1], Value::String(value) if value == "hello"));
    assert!(matches!(&values[2], Value::Bytes(value) if value == &[0, 1]));
    assert!(matches!(values[3], Value::Integer(1)));
}

#[test]
fn invalid_temporal_decimal_and_float_values_fail_without_echo() {
    for value in [
        ParameterValue::Date("not-a-date SECRET".to_owned()),
        ParameterValue::Decimal("12; DROP TABLE secret".to_owned()),
        ParameterValue::TimestampTz("2026-03-19T10:11:12+00:00 SECRET".to_owned()),
        ParameterValue::F64(f64::NAN),
    ] {
        let error = bind_parameters(&[value]).expect_err("valore non valido");
        assert!(!error.message.contains("SECRET"));
        assert!(!error.message.contains("DROP"));
    }
}

#[test]
fn decimal_validation_is_exact_and_does_not_round_through_float() {
    let exact = "12345678901234567890123456789012345678";
    assert!(bind_parameters(&[ParameterValue::Decimal(exact.to_owned())]).is_ok());
    for invalid in [
        "+",
        ".",
        "1e",
        "1e+",
        "1.2.3",
        "123456789012345678901234567890123456789",
    ] {
        let error = bind_parameters(&[ParameterValue::Decimal(invalid.to_owned())])
            .expect_err("decimal non valido");
        assert!(!error.message.contains(invalid));
    }
}

#[test]
fn timestamptz_is_validated_and_normalized_for_explicit_oracle_conversion() {
    let values = bind_parameters(&[ParameterValue::TimestampTz(
        "2026-09-03T10:11:12.123+02:30".to_owned(),
    )])
    .expect("timestamptz valido");
    assert!(matches!(
        &values[0],
        Value::String(value) if value == "2026-09-03T10:11:12.123000+02:30"
    ));
}

/// Il codec `NUMBER` del driver: con un esponente negativo dispari la cifra si
/// spostava di una posizione decimale (`0.01` si salvava come `0.1`, con
/// `Strict` e `committed`; misurato il 25/10 dalla sonda della regola A).
#[test]
fn number_con_esponente_negativo_dispari_conserva_la_posizione_delle_cifre() {
    use oracle_rs::types::{decode_oracle_number, encode_oracle_number};
    assert_eq!(encode_oracle_number("0.01").expect("0.01"), vec![192, 2]);
    assert_eq!(
        encode_oracle_number("-0.01").expect("-0.01"),
        vec![63, 100, 102]
    );
    for value in [
        "0.01",
        "0.001",
        "0.0001",
        "0.000123",
        "-0.01",
        "-0.000123",
        "1e-3",
        "1.5e-5",
        "0.1",
        "12.34",
        "100",
    ] {
        let decoded = decode_oracle_number(&encode_oracle_number(value).expect("codifica"))
            .expect("decodifica");
        let expected: f64 = value.parse().expect("numero");
        assert!(
            (decoded.to_f64().expect("f64") - expected).abs() <= expected.abs() * 1e-15,
            "{value}"
        );
    }
}

/// Seconda rilettura del round 8: un anno fuori da 1-9999 mandava in panic la
/// codifica del driver; una frazione sotto il microsecondo si perdeva.
#[test]
fn date_e_istanti_fuori_dall_intervallo_di_oracle_si_rifiutano() {
    let date = |value: &str| bind_parameters(&[ParameterValue::Date(value.to_owned())]);
    let instant = |value: &str| bind_parameters(&[ParameterValue::Timestamp(value.to_owned())]);
    for value in ["0000-01-01", "-0001-12-31", "10000-01-01"] {
        assert!(date(value).is_err(), "{value}");
    }
    assert!(date("0001-01-01").is_ok());
    assert!(date("9999-12-31").is_ok());
    assert!(instant("0000-06-01T00:00:00").is_err());
    assert!(instant("2026-01-01T00:00:00.123456789").is_err());
    assert!(instant("2026-01-01T00:00:00.123456").is_ok());
}

/// Oracle non conserva `-0.0` in nessun tipo (PR #136, misurato): si rifiuta
/// al bind invece di salvarsi in una forma non canonica o come `+0`.
#[test]
fn lo_zero_negativo_si_rifiuta_al_bind() {
    assert!(bind_parameters(&[ParameterValue::F64(-0.0)]).is_err());
    assert!(bind_parameters(&[ParameterValue::F64(0.0)]).is_ok());
}
