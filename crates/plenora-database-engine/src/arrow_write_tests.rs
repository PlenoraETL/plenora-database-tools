use crate::arrow_write::arrow_parameter_value;
use plenora_database_core::arrow::array::{Decimal128Array, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::{DataType, Field, TimeUnit};
use plenora_database_core::provider::ParameterValue;

#[test]
fn public_arrow_helpers_reject_row_bounds_without_panicking() {
    use crate::arrow_write::{arrow_binary_value, ValidatedWriteArray};
    use plenora_database_core::arrow::array::BinaryArray;
    let values = BinaryArray::from(vec![Some(b"PAYLOAD_MUST_NOT_LEAK".as_slice())]);
    let field = Field::new("v", DataType::Binary, true);
    let error = arrow_parameter_value(&values, &field, 1, 'T').expect_err("indice");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::DataMapping
    );
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Write);
    assert!(!error.message.contains("PAYLOAD"));
    assert!(arrow_binary_value(&values, &DataType::Binary, 1).is_err());
    assert_eq!(
        ValidatedWriteArray::new(&values)
            .expect("array")
            .binary_value(&DataType::Binary, 0)
            .expect("valore"),
        b"PAYLOAD_MUST_NOT_LEAK"
    );
}

#[test]
fn decimal_and_timestamp_are_canonical_and_payload_free_on_error() {
    let decimal = Decimal128Array::from(vec![12345_i128])
        .with_precision_and_scale(5, 2)
        .expect("decimal fixture");
    let field = Field::new("amount", DataType::Decimal128(5, 2), false);
    assert_eq!(
        arrow_parameter_value(&decimal, &field, 0, 'T').expect("decimal"),
        ParameterValue::Decimal("123.45".to_owned())
    );
    let timestamp = TimestampMicrosecondArray::from(vec![0_i64]);
    let field = Field::new(
        "observed_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    );
    assert_eq!(
        arrow_parameter_value(&timestamp, &field, 0, 'T').expect("timestamp"),
        ParameterValue::Timestamp("1970-01-01T00:00:00.000000".to_owned())
    );
}
