use super::*;
use std::collections::BTreeMap;

#[test]
fn binding_is_positional_and_rejects_extra_parameters() {
    let parameters = ParameterBag::new(BTreeMap::from([
        ("first".to_owned(), ParameterValue::I32(7)),
        ("second".to_owned(), ParameterValue::String("x".to_owned())),
    ]));
    let bound =
        bind_parameters(&["second".to_owned(), "first".to_owned()], &parameters).expect("bind");
    assert_eq!(
        bound,
        Params::Positional(vec![Value::Bytes(vec![b'x']), Value::Int(7)])
    );
    assert_eq!(
        bind_parameters(&["first".to_owned()], &parameters)
            .expect_err("extra parameter")
            .category,
        ErrorCategory::InvalidPlan
    );
}

#[test]
fn wkb_is_rejected_until_srid_preflight_exists() {
    let parameters = ParameterBag::new(BTreeMap::from([(
        "shape".to_owned(),
        ParameterValue::Wkb {
            bytes: vec![
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            srid: Some(4_326),
            dimensions: plenora_database_core::geometry::Dimensions::Xy,
            semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
        },
    )]));
    assert_eq!(
        bind_parameters(&["shape".to_owned()], &parameters)
            .expect_err("unqualified WKB")
            .category,
        ErrorCategory::Unsupported
    );
}
