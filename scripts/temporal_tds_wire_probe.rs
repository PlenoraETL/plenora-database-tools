/// Test hook added only to the disposable pinned dependency copy.
pub async fn plenora_temporal_wire_probe() {
    use crate::plenora_checked::Checked;
    use crate::tds::time::{Date, Time, DateTime, DateTime2, DateTimeOffset, SmallDateTime};
    use crate::{ColumnData, FromSql};
    use chrono::{NaiveDate, NaiveTime, NaiveDateTime, FixedOffset};
    use futures_util::io::AsyncRead;
    use std::{pin::Pin, task::{Context, Poll}};
    struct Reader { bytes: futures_util::io::Cursor<Vec<u8>>, ctx: crate::tds::Context }
    impl AsyncRead for Reader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.bytes).poll_read(cx, bytes)
        }
    }
    impl crate::SqlReadBytes for Reader {
        fn debug_buffer(&self) {}
        fn context(&self) -> &crate::tds::Context { &self.ctx }
        fn context_mut(&mut self) -> &mut crate::tds::Context { &mut self.ctx }
    }
    fn reader(bytes: Vec<u8>) -> Reader { Reader { bytes: futures_util::io::Cursor::new(bytes), ctx: crate::tds::Context::new() } }
    // Passa anche dal dispatcher reale: include il byte di lunghezza del campo.
    use crate::tds::codec::{TypeInfo, VarLenContext, VarLenType};
    for (kind, scale, valid_len) in [(VarLenType::Daten,0,3), (VarLenType::Timen,7,5),
        (VarLenType::Datetime2,7,8), (VarLenType::DatetimeOffsetn,7,10)] {
        let info = TypeInfo::VarLenSized(VarLenContext::new(kind,scale,None));
        for length in [1,2,3,4,5,6,7,8,9,10,11,255] {
            let mut bytes = vec![0;usize::from(length)+1]; bytes[0] = length;
            let result = ColumnData::decode(&mut reader(bytes), &info).await;
            assert_eq!(result.is_ok(), length == valid_len);
        }
        assert!(ColumnData::decode(&mut reader(vec![0]), &info).await.is_ok());
        assert!(ColumnData::decode(&mut reader(vec![valid_len]), &info).await.is_err());
    }
    for days in [0_u32, 3_652_058, 3_652_059, 0xffffff] {
        let raw = days.to_le_bytes()[..3].to_vec();
        let value = ColumnData::Date(Some(Date::decode(&mut reader(raw)).await.unwrap()));
        assert_eq!(Checked::<NaiveDate>::from_sql(&value).is_ok(), days <= 3_652_058);
    }
    for mins in [0_u16, 1439, 1440, u16::MAX] {
        let mut raw = vec![0,0]; raw.extend(mins.to_le_bytes());
        let value = ColumnData::SmallDateTime(Some(SmallDateTime::decode(&mut reader(raw)).await.unwrap()));
        assert_eq!(Checked::<NaiveDateTime>::from_sql(&value).is_ok(), mins < 1440);
    }
    for (days, ticks) in [(0_i32,0_u32), (i32::MIN,u32::MAX), (i32::MAX,0), (0,25_920_000)] {
        let mut raw = days.to_le_bytes().to_vec(); raw.extend(ticks.to_le_bytes());
        let value = ColumnData::DateTime(Some(DateTime::decode(&mut reader(raw)).await.unwrap()));
        assert_eq!(Checked::<NaiveDateTime>::from_sql(&value).is_ok(), days == 0 && ticks == 0);
    }
    for (scale,len) in [(0,3), (3,4), (7,5), (8,5)] {
        for full in [false,true] {
            let raw = vec![if full {255} else {0};len];
            let value = Time::decode(&mut reader(raw.clone()),scale,len).await;
            if scale > 7 { assert!(value.is_err()); continue; }
            assert_eq!(Checked::<NaiveTime>::from_sql(&ColumnData::Time(Some(value.unwrap()))).is_ok(), !full);
            let mut raw = raw; raw.extend([0,0,0]);
            let value = DateTime2::decode(&mut reader(raw.clone()),scale,len).await.unwrap();
            assert_eq!(Checked::<NaiveDateTime>::from_sql(&ColumnData::DateTime2(Some(value))).is_ok(), !full);
            raw.extend(0_i16.to_le_bytes());
            let value = DateTimeOffset::decode(&mut reader(raw),scale,u8::try_from(len).unwrap()).await.unwrap();
            assert_eq!(Checked::<chrono::DateTime<FixedOffset>>::from_sql(&ColumnData::DateTimeOffset(Some(value))).is_ok(), !full);
        }
    }
    for len in 0..8 { assert!(DateTime::decode(&mut reader(vec![0;len])).await.is_err()); }
    for len in 0..3 { assert!(Date::decode(&mut reader(vec![0;len])).await.is_err()); }
    for offset in [i16::MIN, -841, -840, 0, 840, 841, i16::MAX] {
        let mut raw = vec![0;5]; raw.extend([1,0,0]); raw.extend(offset.to_le_bytes());
        let value = DateTimeOffset::decode(&mut reader(raw),7,5).await.unwrap();
        let data = ColumnData::DateTimeOffset(Some(value));
        let decoded = Checked::<chrono::DateTime<FixedOffset>>::from_sql(&data);
        assert_eq!(decoded.is_ok(), (-840..=840).contains(&offset));
        if let Ok(Some(value)) = decoded {
            assert_eq!(value.0.naive_utc().to_string(), "0001-01-02 00:00:00");
            assert_eq!(value.0.offset().local_minus_utc(), i32::from(offset) * 60);
        }
    }
}
