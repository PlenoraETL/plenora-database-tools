use super::{validate_create_primary_key, PrimaryKeyViolation};
use arrow_schema::{DataType, Field, Schema};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant", DataType::Int64, false),
        Field::new("nota", DataType::Utf8, true),
    ])
}

#[test]
fn a_composite_key_of_non_nullable_columns_is_accepted() {
    let keys = vec!["id".to_owned(), "tenant".to_owned()];
    assert_eq!(validate_create_primary_key(&schema(), &keys), Ok(()));
}

#[test]
fn no_keys_is_not_a_violation() {
    assert_eq!(validate_create_primary_key(&schema(), &[]), Ok(()));
}

#[test]
fn a_key_outside_the_schema_is_refused() {
    let keys = vec!["assente".to_owned()];
    assert_eq!(
        validate_create_primary_key(&schema(), &keys),
        Err(PrimaryKeyViolation::Missing("assente".to_owned()))
    );
}

#[test]
fn a_nullable_key_is_refused_before_any_provider_coerces_it() {
    let keys = vec!["nota".to_owned()];
    assert_eq!(
        validate_create_primary_key(&schema(), &keys),
        Err(PrimaryKeyViolation::Nullable("nota".to_owned()))
    );
}

#[test]
fn a_repeated_key_is_refused() {
    let keys = vec!["id".to_owned(), "id".to_owned()];
    assert_eq!(
        validate_create_primary_key(&schema(), &keys),
        Err(PrimaryKeyViolation::Repeated("id".to_owned()))
    );
}

#[test]
fn the_message_names_the_provider_and_the_column() {
    let violation = PrimaryKeyViolation::Nullable("nota".to_owned());
    let message = violation.message("MySQL");
    assert!(message.contains("MySQL"), "{message}");
    assert!(message.contains("'nota'"), "{message}");
    assert_eq!(violation.key(), "nota");
}
