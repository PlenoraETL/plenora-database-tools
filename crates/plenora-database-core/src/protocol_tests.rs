use super::*;
use crate::arrow::DataType;

fn field(name: &str, id: Option<&str>) -> Field {
    let field = Field::new(name, DataType::Int64, true);
    match id {
        Some(id) => field.with_metadata(HashMap::from([(FIELD_ID.to_owned(), id.to_owned())])),
        None => field,
    }
}

fn ids(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.metadata()[FIELD_ID].clone())
        .collect()
}

/// ARROW-VOCABULARY §2: ogni campo pubblicato ha un field id unico.
#[test]
fn every_published_field_carries_a_field_id() {
    let schema = contract_schema(vec![field("a", None), field("b", None)]);
    assert_eq!(ids(&schema), ["0", "1"]);
    assert_eq!(schema.metadata()[CONTRACT_VERSION_KEY], CONTRACT_VERSION);
}

#[test]
fn declared_field_ids_are_kept_and_new_ones_do_not_collide() {
    let schema = contract_schema(vec![
        field("a", Some("7")),
        field("b", None),
        field("c", None),
    ]);
    assert_eq!(ids(&schema), ["7", "8", "9"]);
}
