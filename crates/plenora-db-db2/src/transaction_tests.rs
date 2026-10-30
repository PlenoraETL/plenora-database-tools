use crate::transaction::{
    canonical_timestamp, db2_statement_requires_row_count, decode_value, isolation_statement,
    validate_options, ArrayColumns,
};
use odbc_api::DataType;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::transaction::{AccessMode, IsolationLevel, TransactionOptions};
use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};

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

/// Il parameter-array non ha una forma testuale esatta per i byte: le
/// colonne spatial arrivano gia esadecimali per `HEXTORAW(CAST(? AS VARCHAR(32672)))`, ogni altro
/// binario si rifiuta invece di diventare il testo delle sue cifre.
#[test]
fn parameter_arrays_refuse_bytes_and_mixed_double_columns() {
    let bytes = vec![vec![ParameterValue::Bytes(vec![0xde, 0xad])]];
    assert_eq!(
        ArrayColumns::encode(&bytes, 1)
            .err()
            .map(|error| error.category),
        Some(ErrorCategory::DataMapping)
    );
    let mixed = vec![
        vec![ParameterValue::F64(0.5)],
        vec![ParameterValue::String("0.5".to_owned())],
    ];
    assert_eq!(
        ArrayColumns::encode(&mixed, 1)
            .err()
            .map(|error| error.category),
        Some(ErrorCategory::DataMapping)
    );
    let negative_zero = vec![vec![ParameterValue::F64(-0.0)]];
    assert!(ArrayColumns::encode(&negative_zero, 1).is_err());
    let doubles = vec![
        vec![ParameterValue::F64(0.1)],
        vec![ParameterValue::Null {
            type_name: "Float64".to_owned(),
        }],
    ];
    assert!(ArrayColumns::encode(&doubles, 1).is_ok());
}

#[test]
fn only_confirmed_db2_ddl_may_omit_the_affected_row_count() {
    for sql in [
        "CREATE TABLE t (id INTEGER)",
        "-- migration\nDROP TABLE t",
        "/* migration */ ALTER TABLE t ADD COLUMN name VARCHAR(20)",
        "RENAME TABLE t TO t_old",
        "TRUNCATE TABLE t IMMEDIATE",
        "COMMENT ON TABLE t IS 'test'",
        "GRANT SELECT ON TABLE t TO USER test",
        "REVOKE SELECT ON TABLE t FROM USER test",
    ] {
        assert!(!db2_statement_requires_row_count(sql), "{sql}");
    }
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "UPDATE t SET id = 2",
        "DELETE FROM t",
        "MERGE INTO t USING source ON t.id = source.id WHEN MATCHED THEN UPDATE SET id = source.id",
        "WITH source AS (SELECT 1 AS id FROM sysibm.sysdummy1) UPDATE t SET id = 1",
        "CALL procedure()",
    ] {
        assert!(db2_statement_requires_row_count(sql), "{sql}");
    }
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

/// Una cancellazione all'ingresso del commit, dopo che le righe sono state
/// applicate: l'esito del rollback esplicito decide gli assi (ERR-005,
/// ERR-014), non un `none` scritto prima di provarlo.
#[test]
fn an_abandoned_commit_reports_what_the_rollback_proved() {
    let cancellation = plenora_database_core::CancellationToken::new();
    cancellation.cancel();
    let interrupted = || crate::error::interruption_error(&cancellation, ErrorPhase::Commit);
    let confirmed = crate::transaction::abandoned_error(interrupted(), true);
    assert_eq!(confirmed.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(confirmed.retry, RetryDisposition::Never);
    let unconfirmed = crate::transaction::abandoned_error(interrupted(), false);
    assert_eq!(unconfirmed.remote_effect, RemoteEffect::Unknown);
    assert_eq!(unconfirmed.retry, RetryDisposition::RequiresRecovery);
    assert_eq!(unconfirmed.phase, ErrorPhase::Rollback);
}

#[test]
fn array_bind_descriptions_preserve_target_types_and_reject_missing_metadata() {
    use odbc_api::DataType;
    use plenora_database_core::provider::ParameterValue;
    let rows = vec![vec![ParameterValue::I32(1), ParameterValue::F64(1.5)]];
    let columns = ArrayColumns::encode(&rows, 2).unwrap();
    let targets = [DataType::SmallInt, DataType::Real];
    let descriptors = columns.descriptions(&targets).unwrap();
    assert_eq!(
        descriptors.iter().map(|d| d.data_type).collect::<Vec<_>>(),
        targets
    );
    assert!(columns.descriptions(&targets[..1]).is_err());
}

#[test]
fn parameter_array_never_sends_character_buffers_to_binary_targets() {
    let rows = vec![vec![ParameterValue::String("AB01".into())]];
    let columns = ArrayColumns::encode(&rows, 1).unwrap();
    for target in [
        DataType::Binary {
            length: std::num::NonZeroUsize::new(4),
        },
        DataType::Varbinary {
            length: std::num::NonZeroUsize::new(64),
        },
        DataType::LongVarbinary { length: None },
        DataType::Other {
            data_type: odbc_api::sys::SqlDataType(-98),
            column_size: None,
            decimal_digits: 0,
        },
    ] {
        assert!(columns.descriptions(&[target]).is_err());
        let admitted = crate::parameter::admit_described(&rows[0][0], &target).unwrap();
        assert!(ArrayColumns::encode(&[vec![admitted]], 1).is_err());
    }
    let target = DataType::Varchar {
        length: std::num::NonZeroUsize::new(32672),
    };
    let admitted = crate::parameter::admit_described(&rows[0][0], &target).unwrap();
    assert!(ArrayColumns::encode(&[vec![admitted]], 1)
        .unwrap()
        .descriptions(&[target])
        .is_ok());
}
