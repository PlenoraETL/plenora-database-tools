use super::*;
use mysql_common::{
    constants::{ColumnFlags, ColumnType},
    io::ParseBuf,
    proto::MyDeserialize,
    value::{BinValue, ValueDeserializer},
};

fn wire(kind: ColumnType, bytes: &[u8]) -> std::io::Result<Value> {
    ValueDeserializer::<BinValue>::deserialize((kind, ColumnFlags::empty()), &mut ParseBuf(bytes))
        .map(|v| v.0)
}

#[test]
fn temporal_mysql_internal_wire_types_are_refused() {
    for kind in [
        ColumnType::MYSQL_TYPE_TIMESTAMP2,
        ColumnType::MYSQL_TYPE_DATETIME2,
        ColumnType::MYSQL_TYPE_TIME2,
    ] {
        assert_eq!(
            wire(kind, &[0; 16]).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}

#[test]
fn temporal_mysql_wire_dates() {
    use ColumnType::MYSQL_TYPE_DATETIME as DT;
    for (year, month, day, seconds, micros, valid) in [
        (0_u16, 0, 0, 0, 0_u32, false),
        (2024, 2, 29, 59, 999_999, true),
        (2023, 2, 29, 0, 0, false),
        (65535, 1, 1, 0, 0, false),
        (9999, 12, 31, 59, 999_999, true),
        (2024, 1, 1, 59, 1_000_000, false),
        (2024, 1, 1, 59, u32::MAX, false),
        (2024, 1, 1, 60, 0, false),
    ] {
        let mut bytes = vec![11];
        bytes.extend(year.to_le_bytes());
        bytes.extend([month, day, 23, 59, seconds]);
        bytes.extend(micros.to_le_bytes());
        let decoded = wire(DT, &bytes).unwrap();
        assert_eq!(
            decoded,
            Value::Date(year, month, day, 23, 59, seconds, micros)
        );
        assert_eq!(validate(&decoded).is_ok(), valid);
        for length in 0..bytes.len() {
            assert!(wire(DT, &bytes[..length]).is_err());
        }
    }
    for length in [1, 2, 3, 5, 6, 8, 9, 10, 12, 255] {
        let mut bytes = vec![0; usize::from(length) + 1];
        bytes[0] = length;
        assert!(wire(DT, &bytes).is_err());
    }
}

#[test]
fn temporal_mysql_wire_time() {
    use ColumnType::MYSQL_TYPE_TIME as TM;
    for (days, hours, micros, valid) in [
        (0_u32, 0, 999_999_u32, true),
        (34, 22, 0, true),
        (34, 22, 1, false),
        (34, 22, 999_999, false),
        (34, 23, 0, false),
        (u32::MAX, 0, 0, false),
        (0, 255, 0, false),
    ] {
        let mut bytes = vec![12, 1];
        bytes.extend(days.to_le_bytes());
        bytes.extend([hours, 59, 59]);
        bytes.extend(micros.to_le_bytes());
        assert_eq!(validate(&wire(TM, &bytes).unwrap()).is_ok(), valid);
        bytes[1] = 2;
        assert!(wire(TM, &bytes).is_err());
    }
    for length in [1, 7, 9, 11, 13, 255] {
        let mut bytes = vec![0; usize::from(length) + 1];
        bytes[0] = length;
        assert!(wire(TM, &bytes).is_err());
    }
    assert!(validate(&wire(TM, &[0]).unwrap()).is_ok());
}
