use super::*;
use serde_json::Value;
use std::collections::BTreeMap;

#[test]
fn a_repeated_key_is_rejected_where_serde_json_keeps_the_last() {
    for document in [
        br#"{"a": 1, "a": 2}"#.as_slice(),
        br#"{"outer": {"a": 1, "a": 2}}"#.as_slice(),
        br#"[{"a": 1}, {"b": 1, "b": 1}]"#.as_slice(),
        // La stessa chiave scritta con un escape e la stessa chiave.
        br#"{"a": 1, "\u0061": 2}"#.as_slice(),
    ] {
        // Il comportamento che questa funzione corregge: nessun errore.
        assert!(serde_json::from_slice::<Value>(document).is_ok());
        let error = from_slice::<Value>(document).unwrap_err();
        assert!(
            error.to_string().contains("chiave JSON ripetuta"),
            "{error}"
        );
        assert!(!error.to_string().contains("\"a\""));
    }
    assert!(serde_json::from_slice::<BTreeMap<String, u8>>(br#"{"k": 1, "k": 2}"#).is_ok());
    assert!(from_slice::<BTreeMap<String, u8>>(br#"{"k": 1, "k": 2}"#).is_err());
}

#[test]
fn distinct_keys_and_trailing_garbage_keep_their_usual_outcome() {
    let value: Value = from_slice(br#"{"a": [1, 2.5, "x", null, true], "b": {"a": 1}}"#).unwrap();
    assert_eq!(value["b"]["a"], 1);
    assert!(from_slice::<Value>(br#"{"a": 1} {"a": 2}"#).is_err());
    assert!(from_slice::<Value>(b"{").is_err());
}
