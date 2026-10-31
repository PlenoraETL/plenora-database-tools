use super::decode_mapping_error;

#[test]
fn public_decode_error_contains_only_operational_context() {
    let error = decode_mapping_error(7, "decimal");
    assert_eq!(error.message, "decode decimal idx=7 fallito");
}
#[test]
fn query_temporals_preserve_submicroseconds_and_existing_microsecond_format() {
    for (fraction, expected) in [
        ("1234567", "123456700"),
        ("9999999", "999999900"),
        ("0000005", "000000500"),
        ("0000010", "000001"),
        ("0000000", "000000"),
    ] {
        let value = chrono::NaiveDateTime::parse_from_str(
            &format!("1969-12-31T23:59:59.{fraction}"),
            "%Y-%m-%dT%H:%M:%S%.f",
        )
        .expect("timestamp");
        assert_eq!(
            super::format_query_timestamp(value),
            format!("1969-12-31T23:59:59.{expected}")
        );
        assert_eq!(
            super::format_query_time(value.time()),
            format!("23:59:59.{expected}")
        );
    }
}

// Il payload TDS e costruito da campi UTC noti, senza roundtrip nel codec
// di scrittura: due errori complementari non devono annullarsi nel test.
#[test]
fn datetimeoffset_wire_preserves_complete_instant_for_all_offset_signs() {
    use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
    use tiberius::{
        time::{Date, DateTime2, DateTimeOffset, Time},
        ColumnData, FromSql,
    };
    let days = u32::try_from(
        NaiveDate::from_ymd_opt(2026, 10, 31)
            .expect("data")
            .signed_duration_since(NaiveDate::from_ymd_opt(1, 1, 1).expect("epoca"))
            .num_days(),
    )
    .expect("giorni");
    for offset in [120, -330, 0, 840, -720] {
        let data = ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
            DateTime2::new(Date::new(days), Time::new(381_001_234_567, 7)),
            offset,
        )));
        let value = DateTime::<FixedOffset>::from_sql(&data)
            .expect("decode")
            .expect("valore");
        assert_eq!(
            value.with_timezone(&Utc).to_rfc3339(),
            "2026-10-31T10:35:00.123456700+00:00"
        );
    }
    assert_eq!(
        DateTime::<FixedOffset>::from_sql(&ColumnData::DateTimeOffset(None)).expect("NULL"),
        None
    );
}
