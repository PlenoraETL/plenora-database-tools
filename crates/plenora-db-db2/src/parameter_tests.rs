use super::{bind, validate};
use odbc_api::sys::SqlDataType;
use odbc_api::DataType;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::ErrorCategory;
use std::num::NonZeroUsize;

fn varchar() -> DataType {
    DataType::Varchar {
        length: NonZeroUsize::new(64),
    }
}

fn varbinary() -> DataType {
    DataType::Varbinary {
        length: NonZeroUsize::new(64),
    }
}

fn refused(value: &ParameterValue, described: &DataType) -> ErrorCategory {
    match bind(value, described) {
        Ok(_) => ErrorCategory::Internal,
        Err(error) => {
            assert!(!error.message.contains("SECRET"));
            error.category
        }
    }
}

#[test]
fn values_without_an_exact_db2_binding_are_refused_before_any_connection() {
    for (value, category) in [
        (ParameterValue::F64(f64::NAN), ErrorCategory::Unsupported),
        (
            ParameterValue::F64(f64::INFINITY),
            ErrorCategory::Unsupported,
        ),
        (ParameterValue::F64(-0.0), ErrorCategory::DataMapping),
        (
            ParameterValue::Decimal("1.5E2".to_owned()),
            ErrorCategory::DataMapping,
        ),
        (
            ParameterValue::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            ErrorCategory::DataMapping,
        ),
        (
            ParameterValue::Uuid("SECRET-not-a-uuid".to_owned()),
            ErrorCategory::DataMapping,
        ),
        (
            ParameterValue::TimestampTz("2024-01-01T10:20:30+02:00".to_owned()),
            ErrorCategory::Unsupported,
        ),
        (
            ParameterValue::Json(serde_json::json!({"a": 1})),
            ErrorCategory::Unsupported,
        ),
    ] {
        let error = validate(std::slice::from_ref(&value)).expect_err("rifiuto");
        assert_eq!(error.category, category, "{value:?}");
    }
    assert!(validate(&[ParameterValue::F64(0.0), ParameterValue::Bytes(vec![1])]).is_ok());
}

/// Il difetto misurato: i byte arrivavano come testo esadecimale. Ora sono un
/// binario, soltanto su un segnaposto binario.
#[test]
fn bytes_bind_only_as_binary() {
    let bytes = ParameterValue::Bytes(vec![0xAB, 0x01]);
    assert!(bind(&bytes, &varbinary()).is_ok());
    assert!(bind(
        &bytes,
        &DataType::Other {
            data_type: SqlDataType(-98),
            column_size: NonZeroUsize::new(1024),
            decimal_digits: 0,
        }
    )
    .is_ok());
    assert_eq!(refused(&bytes, &varchar()), ErrorCategory::DataMapping);
    // Il testo su un segnaposto binario e il testo: i suoi byte UTF-8.
    assert!(bind(&ParameterValue::String("AB01".to_owned()), &varbinary()).is_ok());
}

#[test]
fn decimals_bind_only_where_they_fit_exactly() {
    let described = DataType::Decimal {
        precision: 12,
        scale: 4,
    };
    let exact = ParameterValue::Decimal("12345.6789".to_owned());
    let trailing = ParameterValue::Decimal("12345.678900".to_owned());
    let longer = ParameterValue::Decimal("12345.67890001".to_owned());
    let wider = ParameterValue::Decimal("123456789.1".to_owned());
    assert!(bind(&exact, &described).is_ok());
    assert!(bind(&trailing, &described).is_ok());
    assert_eq!(refused(&longer, &described), ErrorCategory::DataMapping);
    assert_eq!(refused(&wider, &described), ErrorCategory::DataMapping);
    assert_eq!(
        refused(&exact, &DataType::Double),
        ErrorCategory::DataMapping
    );
    assert!(bind(&exact, &varchar()).is_ok());
    assert!(bind(
        &ParameterValue::Decimal("12.00".to_owned()),
        &DataType::Integer
    )
    .is_ok());
    assert_eq!(
        refused(
            &ParameterValue::Decimal("12.5".to_owned()),
            &DataType::Integer
        ),
        ErrorCategory::DataMapping
    );
}

#[test]
fn floating_point_binds_in_its_own_c_type() {
    assert!(bind(&ParameterValue::F64(0.1), &DataType::Double).is_ok());
    assert!(bind(&ParameterValue::F64(0.5), &DataType::Real).is_ok());
    assert_eq!(
        refused(&ParameterValue::F64(0.1), &DataType::Real),
        ErrorCategory::DataMapping
    );
    assert_eq!(
        refused(
            &ParameterValue::F64(0.1),
            &DataType::Decimal {
                precision: 12,
                scale: 4
            }
        ),
        ErrorCategory::DataMapping
    );
    assert_eq!(
        refused(&ParameterValue::I64((1 << 53) + 1), &DataType::Double),
        ErrorCategory::DataMapping
    );
    assert!(bind(&ParameterValue::I64(1 << 53), &DataType::Double).is_ok());
    assert_eq!(
        refused(
            &ParameterValue::I64(1_000_000_000),
            &DataType::Decimal {
                precision: 9,
                scale: 2
            }
        ),
        ErrorCategory::DataMapping
    );
}

#[test]
fn timestamps_bind_within_the_described_precision() {
    let value = ParameterValue::Timestamp("2024-01-01T10:20:30.123456".to_owned());
    assert!(bind(&value, &DataType::Timestamp { precision: 6 }).is_ok());
    assert_eq!(
        refused(&value, &DataType::Timestamp { precision: 3 }),
        ErrorCategory::DataMapping
    );
    assert!(bind(
        &ParameterValue::Timestamp("2024-01-01T10:20:30.100".to_owned()),
        &DataType::Timestamp { precision: 1 }
    )
    .is_ok());
    assert!(bind(
        &ParameterValue::Timestamp("2024-01-01T10:20:30".to_owned()),
        &DataType::Timestamp { precision: 0 }
    )
    .is_ok());
    assert_eq!(refused(&value, &DataType::Date), ErrorCategory::DataMapping);
    // La write Arrow scrive il separatore spazio.
    assert!(bind(
        &ParameterValue::Timestamp("2024-01-01 10:20:30.123456".to_owned()),
        &DataType::Timestamp { precision: 6 }
    )
    .is_ok());
}

#[test]
fn text_binds_only_on_text_markers() {
    let text = ParameterValue::String("12".to_owned());
    assert!(bind(&text, &varchar()).is_ok());
    assert!(bind(
        &text,
        &DataType::Other {
            data_type: SqlDataType(-99),
            column_size: NonZeroUsize::new(1024),
            decimal_digits: 0,
        }
    )
    .is_ok());
    for described in [
        DataType::Integer,
        DataType::Decimal {
            precision: 12,
            scale: 4,
        },
        DataType::Date,
        DataType::Double,
    ] {
        assert_eq!(refused(&text, &described), ErrorCategory::DataMapping);
    }
    assert!(bind(&ParameterValue::Bool(true), &DataType::Bit).is_ok());
    // Un booleano in un intero e 0 o 1, esatto.
    assert!(bind(&ParameterValue::Bool(true), &DataType::Integer).is_ok());
    assert_eq!(
        refused(&ParameterValue::Bool(true), &DataType::Date),
        ErrorCategory::DataMapping
    );
}

#[test]
fn zero_fits_a_pure_fraction_decimal() {
    // Il caso non silenzioso del secondo giro: `checked_ilog10(0)`.
    assert!(bind(
        &ParameterValue::I64(0),
        &DataType::Decimal {
            precision: 5,
            scale: 5
        }
    )
    .is_ok());
}

#[test]
fn nulls_follow_the_described_marker() {
    let null = ParameterValue::Null {
        type_name: "unknown".to_owned(),
    };
    assert!(bind(&null, &varbinary()).is_ok());
    assert!(bind(&null, &DataType::Integer).is_ok());
}

#[test]
fn server_conversion_probes_must_keep_the_marker_type_exact() {
    // I tre test live degli operandi devono chiedere conversioni al SQL,
    // senza affidarle al driver: nessuna deroga al binding esatto.
    let decimal = DataType::Decimal {
        precision: 31,
        scale: 10,
    };
    for value in [
        ParameterValue::String("0.12345678901".to_owned()),
        ParameterValue::Decimal("0.12345678901".to_owned()),
    ] {
        assert_eq!(refused(&value, &decimal), ErrorCategory::DataMapping);
    }
    assert!(bind(
        &ParameterValue::Decimal("0.1234567890".to_owned()),
        &decimal
    )
    .is_ok());
    assert_eq!(
        refused(&ParameterValue::String("01".to_owned()), &DataType::Integer),
        ErrorCategory::DataMapping
    );
    assert_eq!(
        refused(&ParameterValue::I64(1), &varchar()),
        ErrorCategory::DataMapping
    );
    assert!(bind(
        &ParameterValue::String("0.12345678901".to_owned()),
        &varchar()
    )
    .is_ok());
    assert!(bind(&ParameterValue::String("01".to_owned()), &varchar()).is_ok());
    assert!(bind(&ParameterValue::I64(1), &DataType::BigInt).is_ok());
}

#[test]
fn described_text_capacity_cannot_prove_a_larger_codeunits32_limit() {
    for described in [
        DataType::Varchar {
            length: NonZeroUsize::new(12),
        },
        DataType::WVarchar {
            length: NonZeroUsize::new(6),
        },
        DataType::WVarchar {
            length: NonZeroUsize::new(12),
        },
        DataType::Other {
            data_type: SqlDataType(-99),
            column_size: NonZeroUsize::new(12),
            decimal_digits: 0,
        },
        DataType::Other {
            data_type: SqlDataType(-350),
            column_size: NonZeroUsize::new(6),
            decimal_digits: 0,
        },
    ] {
        for value in ["abc ", "abc   ", "xxx "] {
            let value = ParameterValue::String(value.to_owned());
            assert_eq!(refused(&value, &described), ErrorCategory::DataMapping);
            assert!(super::admit_described(&value, &described).is_err());
        }
        for value in ["abc", "a ", "\u{1f600}\u{1f600}\u{1f600}"] {
            assert!(bind(&ParameterValue::String(value.to_owned()), &described).is_ok());
        }
    }
}

#[test]
fn catalog_units_disambiguate_identical_cli_capacities_without_truncation() {
    use super::{admit_with_catalog, bind_with_catalog};
    use crate::operand_catalog::catalog_type;
    use plenora_database_core::operand_admission::operand_type;
    use plenora_database_core::plan::ProviderKind;

    for type_name in ["CHAR", "VARCHAR", "CLOB", "GRAPHIC", "VARGRAPHIC", "DBCLOB"] {
        for (units, length, accepted, rejected) in [
            ("OCTETS", 12, "abc         ", "abc          "),
            (
                "CODEUNITS16",
                6,
                "\u{1f600}\u{1f600}\u{1f600}",
                "\u{1f600}\u{1f600}\u{1f600} ",
            ),
            ("CODEUNITS32", 3, "\u{1f600}\u{1f600}\u{1f600}", "abc "),
        ] {
            let kind = operand_type(
                ProviderKind::Db2,
                &catalog_type(type_name, length, 0, 1208, units),
            );
            let described = match type_name {
                "CHAR" => DataType::Char {
                    length: NonZeroUsize::new(12),
                },
                "VARCHAR" => DataType::Varchar {
                    length: NonZeroUsize::new(12),
                },
                other => DataType::Other {
                    data_type: match other {
                        "CLOB" => SqlDataType(-99),
                        "GRAPHIC" => SqlDataType(-95),
                        "VARGRAPHIC" => SqlDataType(-96),
                        "DBCLOB" => SqlDataType(-350),
                        _ => unreachable!("matrice chiusa"),
                    },
                    column_size: NonZeroUsize::new(12),
                    decimal_digits: 0,
                },
            };
            for (value, expected) in [(accepted, true), (rejected, false)] {
                let value = ParameterValue::String(value.to_owned());
                assert_eq!(
                    admit_with_catalog(&value, &described, Some(&kind)).is_ok(),
                    expected,
                    "{type_name} {units}"
                );
                assert_eq!(
                    bind_with_catalog(&value, &described, Some(&kind)).is_ok(),
                    expected,
                    "{type_name} {units}"
                );
            }
        }
    }
    assert!(!matches!(
        operand_type(ProviderKind::Db2, &catalog_type("CLOB", 12, 0, 0, "OCTETS")),
        plenora_database_core::operand_admission::OperandType::Text { .. }
    ));
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    let kind = operand_type(
        ProviderKind::Db2,
        &catalog_type("CHAR", 36, 0, 1208, "OCTETS"),
    );
    let described = DataType::Char {
        length: NonZeroUsize::new(36),
    };
    for value in [
        ParameterValue::Uuid(uuid.to_owned()),
        ParameterValue::String(uuid.to_owned()),
    ] {
        assert!(bind_with_catalog(&value, &described, Some(&kind)).is_ok());
        assert!(admit_with_catalog(&value, &described, Some(&kind)).is_ok());
    }
}

#[test]
fn sql_bind_type_is_the_described_target_not_the_c_buffer() {
    for (value, target) in [
        (ParameterValue::I32(1), DataType::SmallInt),
        (
            ParameterValue::Decimal("1.25".into()),
            DataType::Decimal {
                precision: 12,
                scale: 4,
            },
        ),
        (ParameterValue::String("abc".into()), varchar()),
        (
            ParameterValue::Null {
                type_name: "string".into(),
            },
            varchar(),
        ),
    ] {
        let bound = bind(&value, &target).unwrap();
        assert_eq!(bound.data_type(), target);
    }
}
