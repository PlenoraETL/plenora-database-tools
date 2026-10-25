use super::*;
use std::collections::BTreeMap;

const MYSQL: ProviderKind = crate::profile::PROVISIONAL_KIND;
const MARIADB: ProviderKind = ProviderKind::Mariadb;

#[test]
fn binding_is_positional_and_rejects_extra_parameters() {
    let parameters = ParameterBag::new(BTreeMap::from([
        ("first".to_owned(), ParameterValue::I32(7)),
        ("second".to_owned(), ParameterValue::String("x".to_owned())),
    ]));
    let bound = bind_parameters(
        &["second".to_owned(), "first".to_owned()],
        &parameters,
        MYSQL,
    )
    .expect("bind");
    assert_eq!(
        bound,
        Params::Positional(vec![Value::Bytes(vec![b'x']), Value::Int(7)])
    );
    assert_eq!(
        bind_parameters(&["first".to_owned()], &parameters, MYSQL)
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
        bind_parameters(&["shape".to_owned()], &parameters, MYSQL)
            .expect_err("unqualified WKB")
            .category,
        ErrorCategory::Unsupported
    );
}

/// La regola di `plenora_database_core::parameter_fidelity`: cio che il
/// server convertirebbe cambiando il valore si rifiuta prima dell'invio.
#[test]
fn values_the_server_would_change_are_refused_before_sending() {
    for (value, kind) in [
        (
            ParameterValue::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            MYSQL,
        ),
        (
            ParameterValue::TimestampTz("2024-01-01T10:20:30.1234567+02:00".to_owned()),
            MYSQL,
        ),
        (
            ParameterValue::Timestamp("2024-13-01T10:20:30".to_owned()),
            MYSQL,
        ),
        (ParameterValue::Date("2024-02-30".to_owned()), MYSQL),
        (ParameterValue::Decimal("1.5E2".to_owned()), MYSQL),
        (
            ParameterValue::Uuid("6f1c2a2e7-d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            MYSQL,
        ),
        (ParameterValue::F64(-0.0), MARIADB),
    ] {
        assert_eq!(
            parameter_value(&value, kind).expect_err("rifiuto").category,
            ErrorCategory::DataMapping,
            "{value:?}"
        );
    }
    assert_eq!(
        parameter_value(&ParameterValue::Json(serde_json::json!({"a": 1})), MYSQL)
            .expect_err("JSON nudo")
            .category,
        ErrorCategory::Unsupported
    );
    assert_eq!(
        parameter_value(
            &ParameterValue::TimestampTz("2024-01-01T10:20:30+02:00".to_owned()),
            MARIADB
        )
        .expect_err("offset MariaDB")
        .category,
        ErrorCategory::Unsupported
    );
    for (value, kind) in [
        (ParameterValue::Json(serde_json::json!({"a": 1})), MARIADB),
        (ParameterValue::F64(-0.0), MYSQL),
        (
            ParameterValue::Timestamp("2024-01-01 10:20:30.123456".to_owned()),
            MYSQL,
        ),
        (
            ParameterValue::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            MYSQL,
        ),
    ] {
        assert!(parameter_value(&value, kind).is_ok(), "{value:?}");
    }
}
