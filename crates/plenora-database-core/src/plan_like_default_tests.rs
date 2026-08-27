use super::FilterExpression;

/// `$defs` del filtro `like` non richiede `case_insensitive`; Serde deve
/// quindi applicare lo stesso default ammesso dal contratto.
#[test]
fn a_like_filter_without_case_insensitive_is_read_as_case_sensitive() {
    let filter: FilterExpression =
        serde_json::from_str(r#"{"op":"like","field":"nome","parameter":"needle"}"#)
            .expect("il contratto ammette l'omissione");
    assert_eq!(
        filter,
        FilterExpression::Like {
            field: "nome".to_owned(),
            parameter: "needle".to_owned(),
            case_insensitive: false,
        }
    );
}
