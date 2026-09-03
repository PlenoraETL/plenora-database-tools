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
