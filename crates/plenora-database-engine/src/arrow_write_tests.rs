use crate::arrow_write::arrow_parameter_value;
use plenora_database_core::arrow::array::{Decimal128Array, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::{DataType, Field, TimeUnit};
use plenora_database_core::provider::ParameterValue;

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
