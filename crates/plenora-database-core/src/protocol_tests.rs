use super::*;
use crate::arrow::DataType;

fn field(name: &str, id: Option<&str>) -> Field {
    let field = Field::new(name, DataType::Int64, true);
    match id {
        Some(id) => field.with_metadata(HashMap::from([(FIELD_ID.to_owned(), id.to_owned())])),
        None => field,
    }
}

fn id_of(schema: &Schema, name: &str) -> String {
    schema.field_with_name(name).expect("campo").metadata()[FIELD_ID].clone()
}

/// ARROW-VOCABULARY §2: ogni campo pubblicato ha un field id.
#[test]
fn every_published_field_carries_a_field_id() {
    let schema = contract_schema(vec![field("a", None), field("b", None)]).expect("schema");
    assert!(schema
        .fields()
        .iter()
        .all(|f| f.metadata().contains_key(FIELD_ID)));
    assert_ne!(id_of(&schema, "a"), id_of(&schema, "b"));
    assert_eq!(schema.metadata()[CONTRACT_VERSION_KEY], CONTRACT_VERSION);
}

/// L'identita non dipende da posizione o proiezione: `b` ha lo stesso id
/// letto da solo o dopo `a`.
#[test]
fn the_field_id_does_not_depend_on_the_position() {
    let both = contract_schema(vec![field("a", None), field("b", None)]).expect("schema");
    let alone = contract_schema(vec![field("b", None)]).expect("schema");
    let reversed = contract_schema(vec![field("b", None), field("a", None)]).expect("schema");
    assert_eq!(id_of(&both, "b"), id_of(&alone, "b"));
    assert_eq!(id_of(&both, "a"), id_of(&reversed, "a"));
}

/// Un id dichiarato resta; due campi con lo stesso id sono un errore, non
/// una scelta.
#[test]
fn declared_ids_are_kept_and_duplicates_fail() {
    let schema = contract_schema(vec![field("a", Some("7"))]).expect("schema");
    assert_eq!(id_of(&schema, "a"), "7");
    let error = contract_schema(vec![field("a", Some("7")), field("b", Some("7"))])
        .expect_err("id duplicati");
    assert_eq!(error.category, crate::ErrorCategory::Schema);
    assert!(contract_schema(vec![field("a", None), field("a", None)]).is_err());
    // Un valore che non e un intero decimale non negativo e rifiutato.
    for invalid in ["-1", "1.5", "", "0x10", " 7"] {
        assert!(
            contract_schema(vec![field("a", Some(invalid))]).is_err(),
            "{invalid:?}"
        );
    }
    // Lo stesso id scritto con zeri iniziali e lo stesso id.
    assert!(contract_schema(vec![field("a", Some("7")), field("b", Some("007"))]).is_err());
}

/// ARROW-VOCABULARY §2 non pone un massimo: un id dichiarato oltre `u32`, e
/// oltre `u64`, resta com'e.
#[test]
fn declared_ids_beyond_u32_are_kept() {
    for id in ["4294967296", "18446744073709551616"] {
        let schema = contract_schema(vec![field("a", Some(id))]).expect(id);
        assert_eq!(id_of(&schema, "a"), id);
    }
}

/// Due nomi diversi con lo stesso FNV-1a a 31 bit: errore, mai due campi
/// con lo stesso id.
#[test]
fn a_hash_collision_between_different_names_fails() {
    assert_eq!(name_field_id("c268724"), name_field_id("c698200"));
    let error = contract_schema(vec![field("c268724", None), field("c698200", None)])
        .expect_err("collisione");
    assert_eq!(error.category, crate::ErrorCategory::Schema);
}

/// L'identita di una colonna sorgente non dipende dal nome: due alias della
/// stessa colonna hanno lo stesso id, due colonne diverse id diversi.
#[test]
fn an_origin_field_id_survives_a_rename() {
    let column = origin_field_id(&16_384_u32.to_le_bytes(), &3_i16.to_le_bytes());
    assert_eq!(
        column,
        origin_field_id(&16_384_u32.to_le_bytes(), &3_i16.to_le_bytes())
    );
    assert_ne!(
        column,
        origin_field_id(&16_384_u32.to_le_bytes(), &4_i16.to_le_bytes())
    );
}
