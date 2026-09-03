use super::decode_timestamp_tz;
use oracle_rs::types::OracleTimestamp;
use plenora_database_core::ErrorPhase;

#[test]
fn timestamp_tz_restores_the_wall_clock_from_the_driver_utc_instant() {
    let positive = OracleTimestamp::with_timezone(2026, 3, 19, 7, 41, 12, 123_456, 2, 30);
    assert_eq!(
        decode_timestamp_tz(positive, ErrorPhase::Read).expect("offset positivo"),
        "2026-03-19T10:11:12.123456+02:30"
    );
    let negative = OracleTimestamp::with_timezone(2026, 3, 20, 1, 15, 0, 0, -3, -30);
    assert_eq!(
        decode_timestamp_tz(negative, ErrorPhase::Read).expect("offset negativo"),
        "2026-03-19T21:45:00.000000-03:30"
    );
}
