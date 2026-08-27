use super::*;

fn identifier(value: &str) -> Result<Identifier> {
    Identifier::new(value)
}

#[test]
fn policy_closes_unqualified_filter_forms() {
    let expression = FilterExpression::Like {
        field: "name".to_owned(),
        parameter: "pattern".to_owned(),
        case_insensitive: true,
    };
    let error = lower_filter(
        &expression,
        FilterLowering {
            provider: ProviderKind::Mysql,
            case_insensitive_like: false,
            spatial: false,
        },
        identifier,
    )
    .expect_err("forma non qualificata");
    assert_eq!(error.provider, Some(ProviderKind::Mysql));
    assert_eq!(error.message, CASE_INSENSITIVE_LIKE_REFUSAL);

    let spatial = FilterExpression::Spatial {
        function: plenora_database_core::query::SpatialFunction::Intersects,
        field: "shape".to_owned(),
        geometry_parameter: Some("geometry".to_owned()),
        distance_parameter: None,
    };
    let error = lower_filter(
        &spatial,
        FilterLowering {
            provider: ProviderKind::Mysql,
            case_insensitive_like: false,
            spatial: false,
        },
        identifier,
    )
    .expect_err("filtro spatial non qualificato");
    assert_eq!(error.provider, Some(ProviderKind::Mysql));
    assert_eq!(error.message, SPATIAL_FILTER_REFUSAL);
}

#[test]
fn projection_keeps_requested_order_and_concrete_values() {
    let available = vec![("a".to_owned(), 1_u8), ("b".to_owned(), 2_u8)];
    let selected = select_columns_by_name(
        &available,
        &["b".to_owned(), "a".to_owned()],
        |column| column.0.as_str(),
        || DatabaseError::invalid_plan("colonna assente"),
    )
    .expect("projection");
    assert_eq!(selected, vec![("b".to_owned(), 2), ("a".to_owned(), 1)]);
}
