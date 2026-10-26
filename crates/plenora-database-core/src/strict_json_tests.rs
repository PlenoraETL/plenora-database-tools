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

#[test]
fn public_error_summary_never_repeats_document_data() {
    let duplicate =
        from_slice::<Value>(br#"{"SECRET_KEY":1,"SECRET_KEY":"SECRET_VALUE"}"#).unwrap_err();
    assert!(error_message("test", &duplicate).contains("chiave JSON ripetuta"));
    let mismatch = from_slice::<BTreeMap<String, u8>>(br#"{"a":"SECRET_VALUE"}"#).unwrap_err();
    assert!(mismatch.to_string().contains("SECRET_VALUE"));
    assert!(!error_message("test", &mismatch).contains("SECRET"));
}

/// splitmix64: generatore deterministico, nessuna dipendenza nuova.
fn next_bits(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn assert_parses_like_std(text: &str, failures: &mut Vec<String>) {
    let expected = text.parse::<f64>().expect("testo f64 valido");
    let scalar = from_slice::<f64>(text.as_bytes()).expect("JSON f64");
    let nested = from_slice::<Value>(format!("{{\"x\":[{text}]}}").as_bytes())
        .expect("JSON annidato")["x"][0]
        .as_f64()
        .expect("numero");
    if scalar.to_bits() != expected.to_bits() || nested.to_bits() != expected.to_bits() {
        failures.push(text.to_owned());
    }
}

/// Senza la feature `float_roundtrip` di `serde_json` circa un testo su quattro
/// era letto un ULP lontano da `str::parse` (-1e-130 come
/// -9.999999999999999e-131): ogni f64 di un piano o di un letterale CLI
/// arrivava al database diverso da quello scritto.
#[test]
fn every_float_text_is_parsed_correctly_rounded() {
    let mut failures = Vec::new();
    for text in [
        "5e-324",
        "4.9406564584124654e-324",
        "2.2250738585072014e-308",
        "2.225073858507201e-308",
        "1.7976931348623157e308",
        "-1e-130",
        "1.0715660391465826e-75",
        "0.1",
        "0.30000000000000004",
        "1e23",
        "9007199254740993",
        "123456789.12345679",
        "-0.0",
        "0",
    ] {
        assert_parses_like_std(text, &mut failures);
    }
    let mut state = 0x5EED_u64;
    let mut checked = 0_usize;
    while checked < 100_000 {
        let value = f64::from_bits(next_bits(&mut state));
        if !value.is_finite() {
            continue;
        }
        checked += 1;
        // Forma breve, forma posizionale e 17 cifre significative.
        for text in [format!("{value:e}"), format!("{value:.16e}")] {
            assert_parses_like_std(&text, &mut failures);
        }
        if value.abs() > 1e-30 && value.abs() < 1e30 {
            assert_parses_like_std(&format!("{value}"), &mut failures);
        }
    }
    assert!(
        failures.is_empty(),
        "{} testi letti diversi da str::parse, per esempio {:?}",
        failures.len(),
        &failures[..failures.len().min(5)]
    );
}
