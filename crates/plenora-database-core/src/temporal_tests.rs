use super::*;

#[test]
fn temporal_operational_boundaries() {
    for ms in [0, MAX_OPERATIONAL_MS + 1, u64::MAX] {
        assert!(timeout_ms(ms).is_err());
    }
    for ms in [1, MAX_OPERATIONAL_MS - 1, MAX_OPERATIONAL_MS] {
        assert_eq!(timeout_ms(ms).unwrap().as_millis(), u128::from(ms));
    }
    assert!(timeout(Duration::MAX).is_err());
    assert!(retry_ms(0).is_ok());
    assert!(retry_ms(MAX_OPERATIONAL_MS).is_ok());
    assert!(retry_ms(MAX_OPERATIONAL_MS + 1).is_err());
    assert!(keepalive_seconds(u64::MAX).is_err());
    assert!(deadline(Duration::ZERO).is_ok());
    assert!(deadline(Duration::MAX).is_err());
}

#[test]
fn temporal_odbc_no_fallback_on_narrow_target() {
    assert_eq!(odbc_timeout_ms(None).unwrap(), None);
    assert_eq!(odbc_timeout_ms(Some(1000)).unwrap(), Some(1));
    for ms in [
        0,
        999,
        1001,
        (u64::from(u32::MAX) + 1) * 1000,
        u64::MAX / 1000 * 1000,
    ] {
        assert!(odbc_timeout_ms(Some(ms)).is_err());
    }
}
