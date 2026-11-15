#[test]
fn temporal_db2_odbc_text_boundaries() {
    // Il bordo posseduto dal repository e SQL_C_CHAR; il decoder IBM non e Rust.
    for bytes in [b"0001-01-01".as_slice(), b"9999-12-31", b"1970-01-01"] {
        assert!(crate::read::parse_date(std::str::from_utf8(bytes).unwrap()).is_ok());
    }
    for text in [
        "0000-01-01",
        "+10000-01-01",
        "2023-02-29",
        "infinity",
        "\0",
        "9999-99-99",
    ] {
        assert!(crate::read::parse_date(text).is_err());
    }
    for text in [
        "0001-01-01 00:00:00",
        "9999-12-31 23:59:59.999999000000",
        "1970-01-01 00:00:00.000000000000",
    ] {
        assert!(crate::read::parse_timestamp(text).is_ok());
    }
    for text in [
        "0000-01-01 00:00:00",
        "9999-12-31 23:59:59.999999000001",
        "infinity",
        "2024-01-01 23:59:60",
        "2024-01-01 00:00:00.0000000000001",
    ] {
        assert!(crate::read::parse_timestamp(text).is_err());
    }
}

#[test]
fn temporal_db2_unqualified_time_is_refused_before_decoding() {
    for raw in [b"23:59:59".as_slice(), b"99:99:99", &[255; 12]] {
        let error =
            crate::transaction::decode_value(Some(raw), odbc_api::DataType::Time { precision: 6 })
                .unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported
        );
    }
}
