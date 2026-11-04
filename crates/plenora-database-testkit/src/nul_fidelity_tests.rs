use super::exact_native_value;
use plenora_database_core::provider::ParameterValue as V;
use plenora_database_core::Row;
use std::sync::Arc;

fn row(text: &str) -> Row {
    Row::try_new(Arc::from(["V".to_owned()]), vec![V::String(text.into())]).expect("riga")
}

#[test]
fn native_nul_requires_the_exact_written_value() {
    let values = ["abc", "a\0b", "\0ab", "ab\0", "\0"];
    for expected in values {
        assert!(exact_native_value(&[row(expected)], expected).is_ok());
        for changed in values.into_iter().chain(["", "a", "ab", "abc "]) {
            if changed != expected {
                assert_eq!(
                    exact_native_value(&[row(changed)], expected).unwrap_err(),
                    "alterato: decoder nativo non conserva il valore scritto"
                );
            }
        }
        assert!(exact_native_value(&[], expected).is_err());
        assert!(exact_native_value(&[row(expected), row(expected)], expected).is_err());
        let wrong_type = Row::try_new(
            Arc::from(["V".to_owned()]),
            vec![V::Bytes(expected.as_bytes().to_vec())],
        )
        .expect("riga");
        assert!(exact_native_value(&[wrong_type], expected).is_err());
    }
}
