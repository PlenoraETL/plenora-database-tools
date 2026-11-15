use super::*;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use tiberius::time::{Date, DateTime2, DateTimeOffset, SmallDateTime, Time};

#[test]
fn temporal_smalldatetime_dependency_panic_is_blocked() {
    let value = ColumnData::SmallDateTime(Some(SmallDateTime::new(0, u16::MAX)));
    assert!(std::panic::catch_unwind(|| NaiveDateTime::from_sql(&value)).is_err());
    assert!(Checked::<NaiveDateTime>::from_sql(&value).is_err());
}

#[test]
fn temporal_tds_raw_fields() {
    for days in [0, 3_652_058, 3_652_059, 0xff_ffff] {
        let value = ColumnData::Date(Some(Date::new(days)));
        assert_eq!(
            Checked::<NaiveDate>::from_sql(&value).is_ok(),
            days <= 3_652_058
        );
    }
    for scale in [0, 3, 7, 8, 255] {
        for ticks in [0, 1, 864_000_000_000, u64::MAX] {
            let value = ColumnData::Time(Some(Time::new(ticks, scale)));
            let expected = scale <= 7 && ticks < 86400 * 10_u64.pow(u32::from(scale));
            assert_eq!(Checked::<NaiveTime>::from_sql(&value).is_ok(), expected);
        }
    }
    for days in [i32::MIN, -53691, -53690, 0, 2958463, 2958464, i32::MAX] {
        for ticks in [0, 25_919_999, 25_920_000, u32::MAX] {
            let value = ColumnData::DateTime(Some(tiberius::time::DateTime::new(days, ticks)));
            assert_eq!(
                Checked::<NaiveDateTime>::from_sql(&value).is_ok(),
                (-53690..=2958463).contains(&days) && ticks < 25_920_000
            );
        }
    }
    for offset in [i16::MIN, -841, -840, 0, 840, 841, i16::MAX] {
        let value = ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
            DateTime2::new(Date::new(1), Time::new(1, 7)),
            offset,
        )));
        assert_eq!(
            Checked::<DateTime<FixedOffset>>::from_sql(&value).is_ok(),
            (-840..=840).contains(&offset)
        );
    }
    for (days, offset) in [(0, -1), (3_652_058, 1)] {
        let ticks = if days == 0 { 0 } else { 863_999_999_999 };
        let value = ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
            DateTime2::new(Date::new(days), Time::new(ticks, 7)),
            offset,
        )));
        assert!(Checked::<DateTime<FixedOffset>>::from_sql(&value).is_err());
    }
}
