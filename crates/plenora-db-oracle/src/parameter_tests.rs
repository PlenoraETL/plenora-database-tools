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

/// La precisione bindabile e il microsecondo (regola di
/// `plenora_database_core::parameter_fidelity`): la decima cifra non si tronca.
#[test]
fn sub_microsecond_timestamps_and_non_canonical_uuids_are_refused() {
    for value in [
        ParameterValue::Timestamp("2026-03-19T10:11:12.1234567".to_owned()),
        ParameterValue::TimestampTz("2026-03-19T10:11:12.1234567+02:00".to_owned()),
        ParameterValue::Uuid("6f1c2a2e7-d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
    ] {
        let error = bind_parameters(std::slice::from_ref(&value)).expect_err("rifiuto");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::DataMapping
        );
    }
    assert!(bind_parameters(&[ParameterValue::Timestamp(
        "2026-03-19T10:11:12.123456000".to_owned()
    )])
    .is_ok());
}

/// Oracle conserva `''` e un `RAW` vuoto come `NULL`: sul percorso dei
/// parametri si rifiutano, prima dell'invio.
#[test]
fn empty_text_and_bytes_are_refused_on_the_parameter_path() {
    for value in [
        ParameterValue::String(String::new()),
        ParameterValue::Bytes(Vec::new()),
        ParameterValue::Enum {
            type_name: "mood".to_owned(),
            label: String::new(),
        },
    ] {
        let error = crate::parameter::reject_empty(&value).expect_err("vuoto");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::DataMapping
        );
    }
    assert!(crate::parameter::reject_empty(&ParameterValue::String(" ".to_owned())).is_ok());
}
