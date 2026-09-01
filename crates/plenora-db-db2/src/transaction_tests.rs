use crate::transaction::{
    canonical_timestamp, decode_value, encode_parameters, isolation_statement, validate_options,
};
use odbc_api::DataType;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::transaction::{AccessMode, IsolationLevel, TransactionOptions};
use plenora_database_core::ErrorCategory;

#[test]
fn db2_isolation_levels_map_to_their_native_semantics() {
    for (level, expected) in [
        (
            IsolationLevel::ReadUncommitted,
            "SET CURRENT ISOLATION = UR",
        ),
        (IsolationLevel::ReadCommitted, "SET CURRENT ISOLATION = CS"),
        (IsolationLevel::RepeatableRead, "SET CURRENT ISOLATION = RS"),
        (IsolationLevel::Serializable, "SET CURRENT ISOLATION = RR"),
    ] {
        let options = TransactionOptions {
            isolation: Some(level),
            ..TransactionOptions::default()
        };
        assert_eq!(isolation_statement(&options), Some(expected));
    }
}

#[test]
fn unsupported_transaction_options_fail_before_the_network() {
    let access_mode = TransactionOptions {
        access_mode: Some(AccessMode::ReadOnly),
        ..TransactionOptions::default()
    };
    assert_eq!(
        validate_options(&access_mode)
            .expect_err("access mode")
            .category,
        ErrorCategory::Unsupported
    );

    let fractional_timeout = TransactionOptions {
        statement_timeout_ms: Some(1_500),
        ..TransactionOptions::default()
    };
    assert_eq!(
        validate_options(&fractional_timeout)
            .expect_err("timeout non rappresentabile")
            .category,
        ErrorCategory::InvalidPlan
    );
}

#[test]
fn binary_parameters_are_encoded_as_uppercase_hex() {
    assert_eq!(
        encode_parameters(&[ParameterValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef])])
            .expect("binary bind Db2"),
        vec![Some("DEADBEEF".to_owned())]
    );
}

#[test]
fn null_parameters_remain_null_instead_of_becoming_text() {
    assert_eq!(
        encode_parameters(&[ParameterValue::Null {
            type_name: "varchar".to_owned(),
        }])
        .expect("bind NULL Db2"),
        vec![None]
    );
}

#[test]
fn db2_timestamp_separators_become_canonical_iso() {
    assert_eq!(
        canonical_timestamp("2026-08-27-12.34.56.123456").expect("timestamp Db2"),
        "2026-08-27T12:34:56.123456"
    );
    assert_eq!(
        canonical_timestamp("2026-08-27 12:34:56.123456").expect("timestamp ODBC"),
        "2026-08-27T12:34:56.123456"
    );
}

#[test]
fn transaction_text_decoder_preserves_significant_spaces() {
    assert_eq!(
        decode_value(
            Some(b"  significant text  "),
            DataType::Varchar {
                length: std::num::NonZeroUsize::new(64)
            }
        )
        .expect("testo Db2"),
        ParameterValue::String("  significant text  ".to_owned())
    );
}

#[test]
fn transaction_binary_decoder_recovers_the_driver_hex_representation() {
    let data_type = DataType::Other {
        data_type: odbc_api::sys::SqlDataType(-98),
        column_size: std::num::NonZeroUsize::new(2_147_483_647),
        decimal_digits: 0,
    };
    assert_eq!(
        decode_value(Some(b"010203FEFF"), data_type).expect("BLOB Db2"),
        ParameterValue::Bytes(vec![1, 2, 3, 0xfe, 0xff])
    );
    let error = decode_value(Some(b"01GG"), data_type).expect_err("BLOB Db2 non valido");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(!error.message.contains("01GG"));
}
