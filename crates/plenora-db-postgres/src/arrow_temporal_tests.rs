use super::*;

#[test]
fn temporal_interval_wire_extremes_and_exact_precision() {
    for micros in [
        i64::MIN,
        i64::MIN / 1000 - 1,
        i64::MIN / 1000,
        -1,
        0,
        1,
        i64::MAX / 1000,
        i64::MAX / 1000 + 1,
        i64::MAX,
    ] {
        let mut raw = micros.to_be_bytes().to_vec();
        raw.extend(i32::MIN.to_be_bytes());
        raw.extend(i32::MAX.to_be_bytes());
        let value = PostgresInterval::from_sql(&Type::INTERVAL, &raw).unwrap();
        assert_eq!(value.days, i32::MIN);
        assert_eq!(value.months, i32::MAX);
        assert_eq!(
            crate::temporal::interval_nanoseconds(value.microseconds).ok(),
            micros.checked_mul(1000)
        );
    }
    for len in [0, 8, 15, 17] {
        assert!(PostgresInterval::from_sql(&Type::INTERVAL, &vec![0; len]).is_err());
    }
}
