use super::*;
use std::collections::BTreeMap;

#[test]
fn rejects_unused_and_missing_parameters_before_io() {
    let parameters = ParameterBag::new(BTreeMap::from([(
        "unused".to_owned(),
        ParameterValue::I32(1),
    )]));
    let mut query = Query::new("SELECT 1 WHERE 1 = @P1");
    assert!(bind_parameters(&mut query, &["wanted".to_owned()], &parameters).is_err());
}

#[test]
fn decimal_and_uuid_validation_is_strict() {
    for invalid in ["", "+", ".", "1.2.3", "NaN", "１２"] {
        assert!(validate_decimal(invalid).is_err(), "{invalid:?}");
    }
    assert!(validate_decimal("-0.125").is_ok());
    assert!(validate_uuid("550e8400-e29b-41d4-a716-446655440000").is_ok());
    assert!(validate_uuid("550e8400e29b41d4a716446655440000").is_err());
}

#[test]
fn describe_declarations_match_tiberius_wire_types_and_repeated_binds() {
    let parameters = ParameterBag::new(BTreeMap::from([
        ("bytes".to_owned(), ParameterValue::Bytes(vec![0; 8_001])),
        ("minimum".to_owned(), ParameterValue::I32(3)),
        (
            "text".to_owned(),
            ParameterValue::String("2026-01-01".to_owned()),
        ),
    ]));
    let declarations = parameter_declarations(
        &[
            "minimum".to_owned(),
            "text".to_owned(),
            "minimum".to_owned(),
            "bytes".to_owned(),
        ],
        &parameters,
    )
    .expect("declarations")
    .expect("non-empty");
    assert_eq!(
        declarations,
        "@p1 int, @p2 nvarchar(4000), @p3 int, @p4 varbinary(max)"
    );
}

#[test]
fn typed_binary_null_is_bound_and_declared_without_opening_unknown_types() {
    let parameters = ParameterBag::new(BTreeMap::from([(
        "payload".to_owned(),
        ParameterValue::Null {
            type_name: "varbinary".to_owned(),
        },
    )]));
    let names = ["payload".to_owned()];
    let mut query = Query::new("SELECT @P1");

    bind_parameters(&mut query, &names, &parameters).expect("typed binary NULL");
    assert_eq!(
        parameter_declarations(&names, &parameters).expect("declarations"),
        Some("@p1 varbinary(max)".to_owned())
    );

    let unknown = ParameterBag::new(BTreeMap::from([(
        "payload".to_owned(),
        ParameterValue::Null {
            type_name: "unknown".to_owned(),
        },
    )]));
    assert!(parameter_declarations(&names, &unknown).is_err());
}
