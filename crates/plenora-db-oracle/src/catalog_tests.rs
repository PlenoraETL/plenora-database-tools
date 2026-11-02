//! Identita dei nomi VARCHAR2 letti dal dizionario Oracle.
use crate::catalog::{optional_string, required_string};
use plenora_database_core::{provider::ParameterValue, row::Row};

#[test]
fn dictionary_names_keep_leading_and_trailing_spaces() {
    for name in [" C", "C ", " C ", "C"] {
        let row = Row::try_new(
            vec!["name".to_owned()].into(),
            vec![ParameterValue::String(name.into())],
        )
        .unwrap();
        assert_eq!(required_string(&row, 0, "nome assente").unwrap(), name);
        assert_eq!(optional_string(&row, 0).unwrap().as_deref(), Some(name));
    }
}
