use super::{
    admit, admit_filter, admit_portable, exact_decimal_of_f64, operand_type, CatalogType, Charset,
    LengthUnit, OperandType, RelationTypes,
};
use crate::plan::{FilterExpression, ProviderKind};
use crate::portable::{eq, select, Expression, InsertStatement, PortableStatement, TableRef};
use crate::provider::{ParameterBag, ParameterValue as V};
use crate::ErrorCategory;
use std::collections::BTreeMap;

const P: ProviderKind = ProviderKind::Mysql;
const BIGINT: OperandType = OperandType::Integer {
    min: i64::MIN as i128,
    max: i64::MAX as i128,
};
const DOUBLE: OperandType = OperandType::Float { mantissa_bits: 53 };
const REAL: OperandType = OperandType::Float { mantissa_bits: 24 };
const DECIMAL_12_4: OperandType = OperandType::Decimal {
    precision: 12,
    scale: 4,
};

fn refused(value: &V, operand: &OperandType) -> bool {
    matches!(admit(P, value, operand), Err(error) if error.category == ErrorCategory::DataMapping)
}

#[test]
fn negative_zero_is_preserved_or_refused_before_numeric_rewriting() {
    for operand in [
        BIGINT,
        DECIMAL_12_4,
        OperandType::UnboundedDecimal { digits: 38 },
        OperandType::RoundedDecimal {
            precision: 38,
            rounding: 2,
        },
    ] {
        assert!(refused(&V::F64(-0.0), &operand));
        assert!(admit(P, &V::F64(0.0), &operand).is_ok());
    }
    for operand in [DOUBLE, REAL] {
        let V::F64(number) = admit(P, &V::F64(-0.0), &operand).expect("zero con segno") else {
            panic!("tipo floating point");
        };
        assert_eq!(number.to_bits(), (-0.0_f64).to_bits());
    }
}

#[test]
fn exact_decimal_expansion_of_doubles() {
    assert_eq!(
        exact_decimal_of_f64(0.1).map(|value| value.text()),
        Some("0.1000000000000000055511151231257827021181583404541015625".to_owned())
    );
    assert_eq!(
        exact_decimal_of_f64(-2.5).map(|value| value.text()),
        Some("-2.5".to_owned())
    );
    assert_eq!(
        exact_decimal_of_f64(-0.0).map(|value| value.text()),
        Some("0".to_owned())
    );
    assert_eq!(
        exact_decimal_of_f64(9_007_199_254_740_992.0).map(|value| value.text()),
        Some("9007199254740992".to_owned())
    );
    let tiny = exact_decimal_of_f64(5e-324).expect("subnormale");
    assert!(tiny.text().starts_with("0.000"));
    assert!(tiny.text().ends_with('5'));
    assert!(exact_decimal_of_f64(f64::NAN).is_none());
}

/// Il difetto 3 del secondo giro: confronti fra tipi numerici che
/// ritrovavano un vicino.
#[test]
fn integers_floats_and_decimals_meet_by_exact_representability() {
    let two_53 = 1_i64 << 53;
    assert!(refused(&V::I64(two_53 + 1), &DOUBLE));
    assert_eq!(
        admit(P, &V::I64(two_53), &DOUBLE).ok(),
        Some(V::F64(9_007_199_254_740_992.0))
    );
    #[allow(clippy::cast_precision_loss)]
    let float_two_53 = V::F64(two_53 as f64);
    assert_eq!(admit(P, &float_two_53, &BIGINT).ok(), Some(V::I64(two_53)));
    assert!(refused(&V::F64(0.5), &BIGINT));
    assert!(refused(
        &V::Decimal("0.1000000000000000055511151231257827".to_owned()),
        &DOUBLE
    ));
    assert_eq!(
        admit(
            P,
            &V::Decimal("0.1000000000000000055511151231257827021181583404541015625".to_owned()),
            &DOUBLE
        )
        .ok(),
        Some(V::F64(0.1))
    );
    assert_eq!(
        admit(P, &V::Decimal("0.5".to_owned()), &REAL).ok(),
        Some(V::F64(0.5))
    );
    assert!(refused(&V::F64(0.1), &DECIMAL_12_4));
    assert_eq!(
        admit(P, &V::F64(0.5), &DECIMAL_12_4).ok(),
        Some(V::Decimal("0.5".to_owned()))
    );
    assert!(refused(&V::F64(0.1), &REAL));
    assert!(admit(P, &V::F64(0.1), &DOUBLE).is_ok());
    assert!(refused(
        &V::Decimal("12345.67890001".to_owned()),
        &DECIMAL_12_4
    ));
    assert!(refused(
        &V::Decimal("123456789.5".to_owned()),
        &DECIMAL_12_4
    ));
    assert_eq!(
        admit(P, &V::Decimal("012345.678900".to_owned()), &DECIMAL_12_4).ok(),
        Some(V::Decimal("12345.6789".to_owned()))
    );
    assert_eq!(
        admit(P, &V::Decimal("42.000".to_owned()), &BIGINT).ok(),
        Some(V::I64(42))
    );
    assert!(refused(&V::I64(1_000_000_000), &DECIMAL_12_4));
    // Il caso non silenzioso del secondo giro: `0` in DECIMAL(5, 5).
    assert!(admit(
        P,
        &V::I64(0),
        &OperandType::Decimal {
            precision: 5,
            scale: 5
        }
    )
    .is_ok());
    assert!(refused(
        &V::I32(300),
        &OperandType::Integer {
            min: -128,
            max: 127
        }
    ));
}

/// Il difetto 4 (data contro timestamp) e la precisione della colonna (1,
/// 2): un valore che il server arrotonderebbe o troncherebbe si rifiuta.
#[test]
fn temporal_values_meet_the_operand_precision() {
    let timestamp = V::Timestamp("2024-02-29T10:00:00".to_owned());
    assert!(refused(&timestamp, &OperandType::Date));
    assert_eq!(
        admit(
            P,
            &V::Timestamp("2024-02-29T00:00:00.000".to_owned()),
            &OperandType::Date
        )
        .ok(),
        Some(V::Date("2024-02-29".to_owned()))
    );
    assert_eq!(
        admit(
            P,
            &V::Date("2024-02-29".to_owned()),
            &OperandType::Timestamp { fraction_digits: 0 }
        )
        .ok(),
        Some(V::Timestamp("2024-02-29T00:00:00".to_owned()))
    );
    let micros = V::Timestamp("2024-01-01T10:20:30.654321".to_owned());
    assert!(refused(
        &micros,
        &OperandType::Timestamp { fraction_digits: 0 }
    ));
    assert!(refused(
        &micros,
        &OperandType::Timestamp { fraction_digits: 3 }
    ));
    assert!(admit(P, &micros, &OperandType::Timestamp { fraction_digits: 6 }).is_ok());
    assert!(admit(
        P,
        &V::Timestamp("2024-01-01T10:20:30.400000".to_owned()),
        &OperandType::Timestamp { fraction_digits: 1 }
    )
    .is_ok());
    let zone = V::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned());
    assert!(refused(
        &zone,
        &OperandType::TimestampTz { fraction_digits: 3 }
    ));
    assert!(admit(P, &zone, &OperandType::TimestampTz { fraction_digits: 6 }).is_ok());
    assert!(refused(
        &zone,
        &OperandType::Timestamp { fraction_digits: 6 }
    ));
}

#[test]
fn text_binary_and_unknown_operands() {
    let padded = OperandType::Text {
        length: Some(4),
        padded: true,
        unit: LengthUnit::Characters,
        charset: Charset::Unicode,
    };
    // Il padding di `CHAR(n)` e semantica della collation, dichiarata.
    assert!(admit(P, &V::String("ab".to_owned()), &padded).is_ok());
    assert!(admit(P, &V::String("abcd".to_owned()), &padded).is_ok());
    assert!(refused(&V::String("12".to_owned()), &BIGINT));
    assert!(refused(
        &V::I64(12),
        &OperandType::Text {
            length: None,
            padded: false,
            unit: LengthUnit::Characters,
            charset: Charset::Unicode,
        }
    ));
    let fixed = OperandType::Binary {
        length: Some(4),
        padded: true,
    };
    assert!(refused(&V::Bytes(vec![0xAB, 0x01]), &fixed));
    assert!(admit(P, &V::Bytes(vec![1, 2, 3, 4]), &fixed).is_ok());
    assert!(refused(
        &V::Bytes(vec![1]),
        &OperandType::Text {
            length: None,
            padded: false,
            unit: LengthUnit::Characters,
            charset: Charset::Unicode,
        }
    ));
    assert!(refused(&V::I64(1), &OperandType::Unknown));
    assert!(admit(
        P,
        &V::Null {
            type_name: "x".to_owned()
        },
        &OperandType::Unknown
    )
    .is_ok());
}

#[test]
fn catalog_types_classify_per_provider() {
    let catalog = |name: &str| CatalogType {
        name: name.to_owned(),
        ..CatalogType::default()
    };
    assert_eq!(
        operand_type(
            ProviderKind::Mysql,
            &CatalogType {
                declaration: Some("bigint unsigned".to_owned()),
                ..catalog("bigint")
            }
        ),
        OperandType::Integer {
            min: 0,
            max: (1_i128 << 64) - 1
        }
    );
    assert_eq!(
        operand_type(ProviderKind::Postgres, &catalog("numeric")),
        OperandType::UnboundedDecimal { digits: u32::MAX }
    );
    assert_eq!(
        operand_type(
            ProviderKind::Sqlserver,
            &CatalogType {
                scale: Some(3),
                ..catalog("datetime2")
            }
        ),
        OperandType::Timestamp { fraction_digits: 3 }
    );
    assert_eq!(
        operand_type(ProviderKind::Sqlserver, &catalog("datetime")),
        OperandType::Unknown
    );
    assert_eq!(
        operand_type(
            ProviderKind::Oracle,
            &CatalogType {
                scale: Some(9),
                ..catalog("TIMESTAMP(9) WITH TIME ZONE")
            }
        ),
        OperandType::TimestampTz { fraction_digits: 9 }
    );
    assert_eq!(
        operand_type(ProviderKind::Oracle, &catalog("DATE")),
        OperandType::Timestamp { fraction_digits: 0 }
    );
    assert_eq!(
        operand_type(
            ProviderKind::Db2,
            &CatalogType {
                precision: Some(5),
                scale: Some(2),
                ..catalog("DECIMAL")
            }
        ),
        OperandType::Decimal {
            precision: 5,
            scale: 2
        }
    );
}

fn types() -> RelationTypes {
    BTreeMap::from([
        (
            "d".to_owned(),
            CatalogType {
                name: "decimal".to_owned(),
                precision: Some(12),
                scale: Some(4),
                ..CatalogType::default()
            },
        ),
        (
            "f".to_owned(),
            CatalogType {
                name: "double".to_owned(),
                ..CatalogType::default()
            },
        ),
    ])
}

#[test]
fn portable_literals_are_admitted_against_the_catalog() {
    let insert = PortableStatement::Insert(InsertStatement {
        table: TableRef::new("t"),
        columns: vec!["d".to_owned(), "f".to_owned()],
        values: vec![vec![
            Expression::literal(V::F64(0.5)),
            Expression::literal(V::Decimal("0.25".to_owned())),
        ]],
        returning: Vec::new(),
    });
    let PortableStatement::Insert(admitted) =
        admit_portable(P, &insert, &types()).expect("ammesso")
    else {
        panic!("insert atteso");
    };
    assert_eq!(
        admitted.values[0],
        vec![
            Expression::literal(V::Decimal("0.5".to_owned())),
            Expression::literal(V::F64(0.25))
        ]
    );
    let lossy = select("t", vec!["d"])
        .where_(eq("d", V::Decimal("12345.67890001".to_owned())))
        .into_statement();
    assert!(admit_portable(P, &lossy, &types()).is_err());
    let untyped = select("t", vec!["d"])
        .where_(eq("missing", V::I64(1)))
        .into_statement();
    assert!(admit_portable(P, &untyped, &types()).is_err());
}

#[test]
fn filter_parameters_are_admitted_against_their_fields() {
    let filter = FilterExpression::And {
        args: vec![
            FilterExpression::Eq {
                field: "f".to_owned(),
                parameter: "value".to_owned(),
            },
            FilterExpression::Eq {
                field: "d".to_owned(),
                parameter: "value".to_owned(),
            },
        ],
    };
    let operand = |field: &str| types().get(field).map(|catalog| operand_type(P, catalog));
    let parameters = ParameterBag::new(BTreeMap::from([("value".to_owned(), V::I64(2))]));
    let admitted = admit_filter(P, Some(&filter), &parameters, operand);
    assert!(admitted.is_err(), "lo stesso parametro in due forme");
    let single = FilterExpression::Eq {
        field: "f".to_owned(),
        parameter: "value".to_owned(),
    };
    let admitted = admit_filter(P, Some(&single), &parameters, operand).expect("ammesso");
    assert_eq!(admitted.get("value"), Some(&V::F64(2.0)));
}

#[test]
fn negative_scale_decimals_admit_only_values_they_keep() {
    // `numeric(6, -2)` PostgreSQL, `NUMBER(6, -2)` Oracle: multipli di 100
    // con al piu sei cifre prima degli zeri arrotondati.
    let rounded = OperandType::RoundedDecimal {
        precision: 6,
        rounding: 2,
    };
    let catalog = CatalogType {
        name: "numeric".to_owned(),
        declaration: Some("numeric(6,-2)".to_owned()),
        length: None,
        precision: Some(6),
        scale: Some(-2),
        datetime_precision: None,
        ..CatalogType::default()
    };
    assert_eq!(operand_type(ProviderKind::Postgres, &catalog), rounded);
    assert_eq!(
        admit(P, &V::I64(12_300), &rounded).ok(),
        Some(V::I64(12_300))
    );
    assert_eq!(admit(P, &V::I64(0), &rounded).ok(), Some(V::I64(0)));
    assert_eq!(
        admit(P, &V::Decimal("-99999900".to_owned()), &rounded).ok(),
        Some(V::Decimal("-99999900".to_owned()))
    );
    assert_eq!(
        admit(P, &V::F64(12_300.0), &rounded).ok(),
        Some(V::Decimal("12300".to_owned()))
    );
    assert!(refused(&V::I64(12_345), &rounded));
    assert!(refused(&V::I64(50), &rounded));
    assert!(refused(&V::I64(100_000_000), &rounded));
    assert!(refused(&V::Decimal("12300.5".to_owned()), &rounded));
    assert!(refused(&V::F64(0.5), &rounded));
}

#[test]
fn text_meets_the_column_length_unit_and_charset() {
    let text = |length, padded, unit, charset| OperandType::Text {
        length: Some(length),
        padded,
        unit,
        charset,
    };
    let s = |value: &str| V::String(value.to_owned());
    // `varchar(3)` di PostgreSQL e Db2: gli spazi oltre la lunghezza si
    // troncavano in silenzio.
    let varchar3 = text(3, false, LengthUnit::Characters, Charset::Unicode);
    assert!(refused(&s("abc   "), &varchar3));
    assert!(refused(&s("abcd"), &varchar3));
    assert_eq!(admit(P, &s("abc"), &varchar3).ok(), Some(s("abc")));
    assert_eq!(admit(P, &s(""), &varchar3).ok(), Some(s("")));
    assert!(refused(
        &s("abc "),
        &text(3, true, LengthUnit::Characters, Charset::Unicode)
    ));
    // I byte UTF-8 di `VARCHAR(3)` di Db2: `'è'` sono due byte.
    let bytes3 = text(3, false, LengthUnit::Bytes, Charset::Unicode);
    assert!(admit(P, &s("aè"), &bytes3).is_ok());
    assert!(refused(&s("aaè"), &bytes3));
    // Le unita UTF-16 di `nvarchar(2)`: un carattere fuori dal piano di base
    // ne occupa due.
    let utf16 = text(2, false, LengthUnit::Utf16, Charset::Unicode);
    assert!(admit(P, &s("\u{1F600}"), &utf16).is_ok());
    assert!(refused(&s("a\u{1F600}"), &utf16));
    // Il charset: `'łę日x'` in un `varchar(20)` CP1252 diventava `'l?x'`.
    let cp1252 = text(20, false, LengthUnit::Bytes, Charset::Windows1252);
    assert!(refused(&s("\u{142}\u{119}\u{65E5}x"), &cp1252));
    assert!(refused(&s("\u{142}"), &cp1252));
    assert!(admit(P, &s("caff\u{E8} \u{20AC}"), &cp1252).is_ok());
    let ascii = text(20, false, LengthUnit::Bytes, Charset::Ascii);
    assert!(refused(&s("\u{E8}"), &ascii));
    assert!(admit(P, &s("abc"), &ascii).is_ok());
    let bmp = text(20, false, LengthUnit::Characters, Charset::UnicodeBmp);
    assert!(refused(&s("\u{1F600}"), &bmp));
    assert!(admit(P, &s("\u{E8}\u{65E5}"), &bmp).is_ok());
    // Gli altri valori testuali seguono la stessa regola.
    assert!(refused(&V::Date("2024-01-01".to_owned()), &varchar3));
    assert!(refused(&V::Json(serde_json::json!({"a": 1})), &varchar3));
    // I binari oltre la lunghezza.
    let varbinary2 = OperandType::Binary {
        length: Some(2),
        padded: false,
    };
    assert!(refused(&V::Bytes(vec![1, 2, 3]), &varbinary2));
    assert!(admit(P, &V::Bytes(vec![1, 2]), &varbinary2).is_ok());
}

#[test]
fn mysql_scaled_floats_admit_only_values_their_scale_keeps() {
    let float71 = OperandType::ScaledFloat {
        mantissa_bits: 24,
        precision: 7,
        scale: 1,
    };
    let double52 = OperandType::ScaledFloat {
        mantissa_bits: 53,
        precision: 5,
        scale: 2,
    };
    let catalog = CatalogType {
        name: "float".to_owned(),
        declaration: Some("float(7,1)".to_owned()),
        ..CatalogType::default()
    };
    assert_eq!(operand_type(ProviderKind::Mysql, &catalog), float71);
    assert_eq!(
        operand_type(
            ProviderKind::Mysql,
            &CatalogType {
                name: "float".to_owned(),
                declaration: Some("float".to_owned()),
                ..CatalogType::default()
            }
        ),
        REAL
    );
    // `1.25` in `FLOAT(7, 1)` diventava `1.2`; `2.75` diventava `2.8`.
    assert!(refused(&V::F64(1.25), &float71));
    assert!(refused(&V::Decimal("2.75".to_owned()), &float71));
    assert_eq!(admit(P, &V::F64(1.5), &float71).ok(), Some(V::F64(1.5)));
    assert_eq!(
        admit(P, &V::Decimal("2.5".to_owned()), &float71).ok(),
        Some(V::F64(2.5))
    );
    assert_eq!(
        admit(P, &V::I64(123_456), &float71).ok(),
        Some(V::F64(123_456.0))
    );
    assert!(refused(&V::I64(1_234_567), &float71));
    // 0.1 non e un float a 24 bit esatto.
    assert!(refused(&V::F64(0.1), &float71));
    assert_eq!(admit(P, &V::F64(0.25), &double52).ok(), Some(V::F64(0.25)));
    assert!(refused(&V::F64(0.125), &double52));
    assert!(refused(&V::F64(1000.0), &double52));
    // Il valore esatto di 0.1 non ha due cifre: misurato su `MySQL`, `-0.1`
    // in un `DOUBLE(10, 2)` diventava `-0.09999999999999998`.
    assert!(refused(&V::F64(0.1), &double52));
    assert!(refused(&V::F64(-0.1), &double52));
    assert!(refused(&V::Decimal("0.1".to_owned()), &double52));
    assert_eq!(
        admit(P, &V::F64(-0.75), &double52).ok(),
        Some(V::F64(-0.75))
    );
}

#[test]
fn a_scale_without_precision_keeps_the_scale() {
    // `NUMBER(*, -2)` e `NUMBER(*, 2)` di Oracle: precisione assente nel
    // catalogo, scala presente.
    let catalog = |scale| CatalogType {
        name: "NUMBER".to_owned(),
        precision: None,
        scale: Some(scale),
        ..CatalogType::default()
    };
    let rounded = operand_type(ProviderKind::Oracle, &catalog(-2));
    assert_eq!(
        rounded,
        OperandType::RoundedDecimal {
            precision: 38,
            rounding: 2
        }
    );
    assert!(refused(&V::I64(123), &rounded));
    assert!(admit(P, &V::I64(12_300), &rounded).is_ok());
    let scaled = operand_type(ProviderKind::Oracle, &catalog(2));
    assert!(refused(&V::Decimal("1.234".to_owned()), &scaled));
    assert!(admit(P, &V::Decimal("1.23".to_owned()), &scaled).is_ok());
}

/// Il catalogo impedisce conversioni implicite testo/binario su tutti i
/// provider, anche quando lo SQL nativo delega conversioni al server.
#[test]
fn catalog_admission_never_reinterprets_text_as_binary() {
    let binary = OperandType::Binary {
        length: Some(64),
        padded: false,
    };
    for provider in [
        ProviderKind::Db2,
        ProviderKind::Oracle,
        ProviderKind::Sqlserver,
        ProviderKind::Postgres,
        ProviderKind::Mysql,
        ProviderKind::Mariadb,
    ] {
        for text in ["AB01", "not hex", "\u{e9}", "\u{1f600}"] {
            assert_eq!(
                admit(provider, &V::String(text.into()), &binary)
                    .unwrap_err()
                    .category,
                ErrorCategory::DataMapping
            );
            let bytes = V::Bytes(text.as_bytes().to_vec());
            assert_eq!(admit(provider, &bytes, &binary).unwrap(), bytes);
        }
    }
}
