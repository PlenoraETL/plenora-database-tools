use oracle_rs::types::{decode_oracle_date, decode_oracle_timestamp};

#[test]
fn temporal_oracle_row_wire_does_not_replace_truncation_with_null() {
    use oracle_rs::{buffer::ReadBuffer, row::RowDataDecoder};
    use oracle_rs::{ColumnInfo, OracleType};
    for kind in [
        OracleType::Date,
        OracleType::Timestamp,
        OracleType::TimestampTz,
    ] {
        let mut column = ColumnInfo::new("T", kind);
        column.buffer_size = 13;
        let columns = [column];
        let decoder = RowDataDecoder::new(&columns);
        assert!(decoder
            .decode_row(&mut ReadBuffer::from_slice(&[]), None)
            .is_err());
        assert!(decoder
            .decode_row(&mut ReadBuffer::from_slice(&[7, 120]), None)
            .is_err());
        assert!(decoder
            .decode_row(&mut ReadBuffer::from_slice(&[0]), None)
            .unwrap()
            .is_null(0));
    }
    for kind in [OracleType::IntervalYm, OracleType::IntervalDs] {
        let mut column = ColumnInfo::new("T", kind);
        column.buffer_size = 13;
        let columns = [column];
        let decoder = RowDataDecoder::new(&columns);
        for raw in [vec![255; 12], vec![5, 0, 0, 0, 0, 0]] {
            assert!(decoder
                .decode_row(&mut ReadBuffer::from_slice(&raw), None)
                .is_err());
        }
    }
}

#[test]
fn temporal_oracle_wire_boundaries() {
    let good = [120, 124, 2, 29, 24, 60, 60];
    assert_eq!(decode_oracle_date(&good).unwrap().year, 2024);
    for (raw, year) in [
        ([53, 88, 1, 1, 1, 1, 1], -4712),
        ([199, 199, 12, 31, 24, 60, 60], 9999),
    ] {
        assert_eq!(decode_oracle_date(&raw).unwrap().year, year);
    }
    for raw in [
        [100, 100, 1, 1, 1, 1, 1],
        [200, 100, 1, 1, 1, 1, 1],
        [120, 123, 2, 29, 1, 1, 1],
        [120, 124, 1, 1, 0, 1, 1],
        [255; 7],
    ] {
        assert!(decode_oracle_date(&raw).is_err());
    }
    for length in [0, 6, 8, 10, 12, 14] {
        assert!(decode_oracle_timestamp(&vec![0; length]).is_err());
    }
    for nanos in [0_u32, 1000, 999_999_000, 1, 1_000_000_000, u32::MAX] {
        let mut raw = good.to_vec();
        raw.extend(nanos.to_be_bytes());
        assert_eq!(
            decode_oracle_timestamp(&raw).is_ok(),
            nanos < 1_000_000_000 && nanos.is_multiple_of(1000)
        );
    }
    for (hour, minute, valid) in [
        (8, 60, true),
        (34, 60, true),
        (20, 119, true),
        (20, 1, true),
        (255, 255, false),
    ] {
        let mut raw = good.to_vec();
        raw.extend([0; 4]);
        raw.extend([hour, minute]);
        assert_eq!(decode_oracle_timestamp(&raw).is_ok(), valid);
    }
    assert!(
        crate::decode::require_gregorian(1582, 10, 4, plenora_database_core::ErrorPhase::Read)
            .is_err()
    );
    assert!(crate::decode::require_gregorian(
        1582,
        10,
        15,
        plenora_database_core::ErrorPhase::Read
    )
    .is_ok());
}
