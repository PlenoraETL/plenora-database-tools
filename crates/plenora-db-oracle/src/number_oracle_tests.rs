//! Oracolo del formato NUMBER Oracle, indipendente dal codec del driver.
//!
//! Il driver codifica un decimale allineando la posizione del punto alle
//! coppie in base 100. Nel fork di `oracle-rs` 0.1.7 l'allineamento usava il
//! `%` di Rust come se fosse quello di Python: ogni valore in (-1, 1) con un
//! numero dispari di zeri dopo il punto arrivava al server moltiplicato per
//! dieci (0.05 salvato come 0.5).
//!
//! Il riferimento qui sotto non riusa quell'algoritmo: assegna a ogni cifra
//! il proprio peso assoluto `10^w`, la colloca nella coppia `w / 2` su indici
//! resi non negativi da un offset, e costruisce i byte che Oracle scrive in
//! `DUMP()` (verificati sul server per i casi di confine). Il generatore copre
//! ogni ordine di grandezza fra 1e-130 e 1e125, entrambi i segni, da 1 a 40
//! cifre significative, ogni conteggio di zeri iniziali e finali e gli interi.
//! I test live riusano lo stesso generatore contro il server.

use oracle_rs::types::{decode_oracle_number, encode_oracle_number};
use std::collections::BTreeMap;

/// Posizione minima del punto: 1e-130 e `0.1 * 10^-129`.
pub const MIN_POINT: i32 = -129;
/// Posizione massima del punto: 9.99...e125 e `0.999... * 10^126`.
pub const MAX_POINT: i32 = 126;
/// Lunghezza massima del testo accettata dal driver e dal provider.
const MAX_TEXT: usize = 172;
/// Offset che rende non negativo ogni peso decimale generato.
const WEIGHT_OFFSET: i32 = 400;

/// Un decimale finito in forma canonica: `0.d1 d2 ... dn * 10^point`, senza
/// zeri iniziali o finali nelle cifre. Lo zero ha cifre vuote e segno positivo.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Canon {
    pub negative: bool,
    pub digits: Vec<u8>,
    pub point: i32,
}

impl Canon {
    pub fn zero() -> Self {
        Self {
            negative: false,
            digits: Vec::new(),
            point: 0,
        }
    }

    fn new(negative: bool, digits: Vec<u8>, point: i32) -> Self {
        let mut canon = Self {
            negative,
            digits,
            point,
        };
        while canon.digits.last() == Some(&0) {
            canon.digits.pop();
        }
        while canon.digits.first() == Some(&0) {
            canon.digits.remove(0);
            canon.point -= 1;
        }
        if canon.digits.is_empty() {
            return Self::zero();
        }
        canon
    }

    /// Notazione posizionale senza esponente, eventualmente con zeri in piu
    /// davanti e dietro (`pad`), che il codec deve ignorare.
    pub fn plain(&self, pad: usize) -> String {
        let mut text = String::new();
        if self.negative {
            text.push('-');
        }
        let digits = self.digit_text();
        let lead = "0".repeat(pad);
        let point = usize::try_from(self.point).unwrap_or(0);
        if self.digits.is_empty() {
            text.push('0');
        } else if self.point <= 0 {
            text.push_str(&lead);
            text.push_str("0.");
            text.push_str(&"0".repeat(self.point.unsigned_abs() as usize));
            text.push_str(&digits);
            text.push_str(&lead);
        } else if point >= digits.len() {
            text.push_str(&lead);
            text.push_str(&digits);
            text.push_str(&"0".repeat(point - digits.len()));
            if pad > 0 {
                text.push('.');
                text.push_str(&lead);
            }
        } else {
            let (integer, fraction) = digits.split_at(point);
            text.push_str(&lead);
            text.push_str(integer);
            text.push('.');
            text.push_str(fraction);
            text.push_str(&lead);
        }
        text
    }

    /// Notazione scientifica `d.ddd e±x`.
    pub fn scientific(&self, upper: bool) -> String {
        if self.digits.is_empty() {
            return "0E0".to_owned();
        }
        let digits = self.digit_text();
        let mut text = String::new();
        if self.negative {
            text.push('-');
        }
        text.push_str(&digits[..1]);
        if digits.len() > 1 {
            text.push('.');
            text.push_str(&digits[1..]);
        }
        text.push(if upper { 'E' } else { 'e' });
        text.push_str(&(self.point - 1).to_string());
        text
    }

    /// Un testo che il provider accetta: posizionale quando richiesto e
    /// possibile (al massimo 172 byte e 38 cifre dopo gli zeri iniziali, come
    /// valida `parameter.rs`), altrimenti scientifico.
    pub fn bind_text(&self, prefer_plain: bool) -> String {
        let plain = self.plain(0);
        let significant = plain
            .bytes()
            .filter(u8::is_ascii_digit)
            .skip_while(|byte| *byte == b'0')
            .count();
        if prefer_plain && plain.len() <= MAX_TEXT && significant <= 38 {
            plain
        } else {
            self.scientific(false)
        }
    }

    fn digit_text(&self) -> String {
        self.digits.iter().map(|d| char::from(b'0' + d)).collect()
    }

    pub fn negated(&self) -> Self {
        Self {
            negative: !self.negative && !self.digits.is_empty(),
            ..self.clone()
        }
    }
}

/// Byte NUMBER attesi, oppure `None` se il valore richiede piu di 20 cifre
/// in base 100 o cade fuori dal dominio Oracle.
pub fn reference_bytes(value: &Canon) -> Option<Vec<u8>> {
    if value.digits.is_empty() {
        return Some(vec![128]);
    }
    if !(MIN_POINT..=MAX_POINT).contains(&value.point) {
        return None;
    }
    // Peso della cifra i: 10^(point - 1 - i). Coppia: floor(peso / 2), con
    // aritmetica non negativa grazie all'offset.
    let mut pairs = BTreeMap::<i32, u8>::new();
    for (index, digit) in value.digits.iter().enumerate() {
        let weight = value.point - 1 - i32::try_from(index).expect("indice") + WEIGHT_OFFSET;
        assert!(weight >= 0, "offset insufficiente");
        let pair = weight / 2 - WEIGHT_OFFSET / 2;
        let tens = weight % 2 == 1;
        *pairs.entry(pair).or_default() += if tens { digit * 10 } else { *digit };
    }
    let high = *pairs.keys().next_back().expect("almeno una coppia");
    let low = *pairs.keys().next().expect("almeno una coppia");
    let count = usize::try_from(high - low + 1).expect("coppie");
    if count > 20 {
        return None;
    }
    let positive_exponent = u8::try_from(193 + high).expect("esponente nel dominio");
    let mut bytes = vec![if value.negative {
        255 - positive_exponent
    } else {
        positive_exponent
    }];
    for pair in (low..=high).rev() {
        let digit = pairs.get(&pair).copied().unwrap_or(0);
        bytes.push(if value.negative {
            101 - digit
        } else {
            digit + 1
        });
    }
    if value.negative && count < 20 {
        bytes.push(102);
    }
    Some(bytes)
}

/// Interpreta un testo numerico (incluso `TO_CHAR(x, 'TM9')`: `.05`,
/// `-1.5E+125`) nella forma canonica.
pub fn parse_canon(text: &str) -> Option<Canon> {
    let text = text.trim();
    let (negative, unsigned) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => (
            &unsigned[..index],
            unsigned[index + 1..].parse::<i32>().ok()?,
        ),
        None => (unsigned, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    let mut digits = Vec::new();
    for byte in integer.bytes().chain(fraction.bytes()) {
        if !byte.is_ascii_digit() {
            return None;
        }
        digits.push(byte - b'0');
    }
    let point = i32::try_from(integer.len()).ok()? + exponent;
    let canon = Canon::new(negative, digits, point);
    Some(if canon.digits.is_empty() {
        Canon::zero()
    } else {
        canon
    })
}

/// Interpreta `DUMP(x)`: `Typ=2 Len=3: 192,6`.
pub fn parse_dump(text: &str) -> Option<Vec<u8>> {
    let (_, bytes) = text.split_once(": ")?;
    bytes
        .split(',')
        .map(|byte| byte.trim().parse::<u8>().ok())
        .collect()
}

fn pattern_digits(pattern: usize, count: usize, seed: u64) -> Vec<u8> {
    let mut digits = match pattern {
        0 => vec![9; count],
        1 => {
            let mut digits = vec![0; count];
            digits[0] = 1;
            digits[count - 1] = 1;
            digits
        }
        _ => {
            let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (0..count)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    u8::try_from((state >> 33) % 10).expect("cifra")
                })
                .collect()
        }
    };
    if digits[0] == 0 {
        digits[0] = 7;
    }
    if digits[count - 1] == 0 {
        digits[count - 1] = 3;
    }
    digits
}

/// Ogni posizione del punto del dominio Oracle per ogni numero di cifre
/// significative in `counts`, con tre forme delle cifre e i due segni.
pub fn generated(counts: &[usize], patterns: usize) -> Vec<Canon> {
    let mut values = std::collections::BTreeSet::new();
    values.insert(Canon::zero());
    for point in MIN_POINT..=MAX_POINT {
        for &count in counts {
            for pattern in 0..patterns {
                let seed = u64::try_from(point - MIN_POINT).expect("seme") * 1_000
                    + u64::try_from(count * 10 + pattern).expect("seme");
                let positive = Canon::new(false, pattern_digits(pattern, count, seed), point);
                values.insert(positive.negated());
                values.insert(positive);
            }
        }
    }
    values.into_iter().collect()
}

/// Interi di confine: potenze di dieci e vicini, i limiti di `i64`.
pub fn boundary_integers() -> Vec<String> {
    let mut values = vec![
        i64::MIN.to_string(),
        i64::MAX.to_string(),
        "1".to_owned(),
        "-1".to_owned(),
    ];
    for exponent in 1..=38_usize {
        let power = format!("1{}", "0".repeat(exponent));
        let below = "9".repeat(exponent);
        let above = format!("1{}1", "0".repeat(exponent - 1));
        for text in [power, below, above] {
            values.push(format!("-{text}"));
            values.push(text);
        }
    }
    values
}

fn check_text(expected: &Canon, text: &str) -> Result<(), String> {
    let reference = reference_bytes(expected);
    match (encode_oracle_number(text), reference) {
        (Ok(bytes), Some(reference)) => {
            if bytes != reference {
                return Err(format!("{text}: byte {bytes:?} attesi {reference:?}"));
            }
            let decoded = decode_oracle_number(&bytes).map_err(|e| format!("{text}: {e}"))?;
            if parse_canon(&decoded.value).as_ref() != Some(expected) {
                return Err(format!("{text}: decodificato {}", decoded.value));
            }
            Ok(())
        }
        (Err(_), None) => Ok(()),
        (Ok(bytes), None) => Err(format!("{text}: accettato {bytes:?} fuori dominio")),
        (Err(error), Some(_)) => Err(format!("{text}: rifiutato ({error})")),
    }
}

#[test]
fn every_magnitude_sign_and_digit_count_matches_the_oracle_byte_format() {
    let counts = (1..=40).collect::<Vec<_>>();
    let values = generated(&counts, 3);
    assert!(values.len() > 60_000, "{}", values.len());
    let mut failures = Vec::new();
    let mut checked = 0_usize;
    for value in &values {
        let mut texts = vec![value.scientific(false), value.scientific(true)];
        for pad in [0, 3] {
            let plain = value.plain(pad);
            if plain.len() <= MAX_TEXT {
                texts.push(plain);
            }
        }
        for text in texts {
            checked += 1;
            if let Err(failure) = check_text(value, &text) {
                failures.push(failure);
            }
        }
        // Il decoder, da solo, sui byte del riferimento.
        if let Some(bytes) = reference_bytes(value) {
            let decoded = decode_oracle_number(&bytes).expect("byte di riferimento decodificabili");
            if parse_canon(&decoded.value).as_ref() != Some(value) {
                failures.push(format!("{bytes:?}: decodificato {}", decoded.value));
            }
        }
    }
    assert!(checked > 200_000, "{checked}");
    assert!(
        failures.is_empty(),
        "{} su {checked}: {:?}",
        failures.len(),
        &failures[..failures.len().min(10)]
    );
}

#[test]
fn integers_and_domain_boundaries_match_the_oracle_byte_format() {
    let mut failures = Vec::new();
    for text in boundary_integers() {
        let expected = parse_canon(&text).expect("intero");
        if let Err(failure) = check_text(&expected, &text) {
            failures.push(failure);
        }
    }
    // 1e-130 e 9.99...e125 (38 cifre) sono i confini; un passo oltre e rifiuto.
    let largest = format!("{}e125", "9.".to_owned() + &"9".repeat(37));
    for (text, accepted) in [
        ("1e-130", true),
        ("-1e-130", true),
        (largest.as_str(), true),
        ("1e126", false),
        ("-1e126", false),
        ("1e-131", false),
        ("9.9e-131", false),
        ("0", true),
        ("-0", true),
        ("0.000", true),
    ] {
        assert_eq!(encode_oracle_number(text).is_ok(), accepted, "{text}");
        if accepted {
            let expected = parse_canon(text).expect("testo valido");
            if let Err(failure) = check_text(&expected, text) {
                failures.push(failure);
            }
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}

#[test]
fn the_reported_values_are_no_longer_scaled_by_ten() {
    for (text, dump) in [
        ("0.05", "Typ=2 Len=2: 192,6"),
        ("-0.05", "Typ=2 Len=3: 63,96,102"),
        ("0.0005", "Typ=2 Len=2: 191,6"),
        ("0.07", "Typ=2 Len=2: 192,8"),
        ("5E-130", "Typ=2 Len=2: 128,6"),
    ] {
        assert_eq!(
            encode_oracle_number(text).expect("valore valido"),
            parse_dump(dump).expect("dump")
        );
    }
}

#[test]
fn the_reference_is_itself_checked_against_known_dumps() {
    // Byte letti con DUMP() su Oracle Free: il riferimento non deve dipendere
    // dal codec che giudica.
    for (text, dump) in [
        ("123", "Typ=2 Len=3: 194,2,24"),
        ("-123", "Typ=2 Len=4: 61,100,78,102"),
        ("1.5", "Typ=2 Len=3: 193,2,51"),
        ("0.0123", "Typ=2 Len=3: 192,2,24"),
        ("-1E-130", "Typ=2 Len=3: 127,100,102"),
        ("9.9E125", "Typ=2 Len=2: 255,100"),
        ("100", "Typ=2 Len=2: 194,2"),
    ] {
        let canon = parse_canon(text).expect("testo valido");
        assert_eq!(reference_bytes(&canon), parse_dump(dump), "{text}");
    }
}

#[test]
fn infinities_and_the_overflow_digit_are_refused_by_the_decoder() {
    // -infinito ([0], DUMP `Typ=2 Len=1: 0`), +infinito ([255, 101], TO_CHAR
    // `~`), la cifra 100 fuori e dentro la prima posizione: prima erano "",
    // 1e126 e "11".
    for bytes in [
        &[0_u8][..],
        &[255, 101],
        &[193, 2, 101],
        &[193, 101],
        &[62, 1, 102],
        &[62, 100, 1, 102],
        &[129],
    ] {
        assert!(decode_oracle_number(bytes).is_err(), "{bytes:?}");
    }
    assert_eq!(decode_oracle_number(&[128]).expect("zero").value, "0");
}

#[test]
fn decimal_text_without_digits_or_beyond_precision_is_not_read_as_a_value() {
    use crate::read::parse_decimal;
    for (text, precision, scale) in [
        ("", 38, 0),
        ("-", 38, 0),
        (".", 38, 2),
        ("-.", 38, 2),
        ("1e3", 38, 0),
        ("0.5", 38, 0),
        ("123", 2, 0),
        ("10.5", 2, 1),
        ("1000000000000000000000000000000000000000", 38, 0),
    ] {
        assert!(
            parse_decimal(text, precision, scale).is_err(),
            "{text} ({precision}, {scale})"
        );
    }
    for (text, precision, scale, expected) in [
        ("0", 38, 0, 0_i128),
        ("-0.5", 38, 1, -5),
        ("99", 2, 0, 99),
        ("1.5", 2, 1, 15),
        ("0.05", 2, 2, 5),
        (".05", 2, 2, 5),
        (
            "-12345678901234567890123456789012345678",
            38,
            0,
            -12_345_678_901_234_567_890_123_456_789_012_345_678,
        ),
    ] {
        assert_eq!(
            parse_decimal(text, precision, scale).expect("decimale valido"),
            expected,
            "{text}"
        );
    }
}

#[test]
fn a_decimal_float_becomes_f64_only_when_it_is_the_shortest_form_of_its_f64() {
    use crate::read::decimal_as_f64;
    for (text, expected) in [
        ("0.1", 0.1_f64),
        ("1.0000000000000002", 1.000_000_000_000_000_2),
        ("100", 100.0),
        ("9007199254740992", 9_007_199_254_740_992.0),
        ("123456789.12345679", 123_456_789.123_456_79),
        ("-1.5", -1.5),
        ("0", 0.0),
        ("1E-130", 1e-130),
        ("1.7976931348623157e308", f64::MAX),
    ] {
        assert_eq!(
            decimal_as_f64(text).expect("ricostruibile").to_bits(),
            expected.to_bits(),
            "{text}"
        );
    }
    // Gli esempi del secondo lettore: f64 renderebbe un altro numero. Poi
    // quelli del terzo: decimali distinti con gli stessi bit di uno sopra,
    // che la conversione renderebbe uguali (non iniettiva).
    for text in [
        ".10000000000000001",
        "0.1000000000000000055511151231257827021181583404541015625",
        "1.0000000000000002220446049250313080847",
        "12345678901234567890123",
        "9007199254740993",
        "123456789.123456789",
        ".3333333333333333333333333333333333333333",
        "0.12345678901234567891",
        "",
        "-",
        "1e400",
    ] {
        assert!(decimal_as_f64(text).is_err(), "{text}");
    }
}

#[test]
fn binary_floats_are_decoded_from_the_stored_bytes_and_negative_zero_is_refused() {
    use crate::read::{binary_double_from_dump, binary_float_from_dump};
    for (dump, expected) in [
        ("Typ=101 Len=8: 128,0,0,0,0,0,0,0", Some(0.0_f64)),
        ("Typ=101 Len=8: 191,248,0,0,0,0,0,0", Some(1.5)),
        ("Typ=101 Len=8: 64,7,255,255,255,255,255,255", Some(-1.5)),
        ("NULL", None),
    ] {
        assert_eq!(
            binary_double_from_dump(dump)
                .expect("BINARY_DOUBLE valido")
                .map(f64::to_bits),
            expected.map(f64::to_bits),
            "{dump}"
        );
    }
    // -0.0 salvato non canonico dalle versioni precedenti, non finiti,
    // DUMP di un altro tipo o malformato.
    for dump in [
        "Typ=101 Len=8: 127,255,255,255,255,255,255,255",
        "Typ=101 Len=8: 255,240,0,0,0,0,0,0",
        "Typ=100 Len=4: 128,0,0,0",
        "Typ=101 Len=8: 128,0,0",
        "Typ=101 Len=8: 128,0,0,0,0,0,0,x",
        "",
    ] {
        assert!(binary_double_from_dump(dump).is_err(), "{dump}");
    }
    assert_eq!(
        binary_float_from_dump("Typ=100 Len=4: 191,192,0,0")
            .expect("BINARY_FLOAT valido")
            .map(f32::to_bits),
        Some(1.5_f32.to_bits())
    );
    assert!(binary_float_from_dump("Typ=100 Len=4: 127,255,255,255").is_err());
}

#[test]
fn negative_zero_inside_json_is_refused_by_the_bind_and_by_the_oson_encoder() {
    use crate::parameter::bind_parameters;
    use plenora_database_core::provider::ParameterValue;
    for document in [
        serde_json::json!({"a": -0.0}),
        serde_json::json!([1, {"b": [-0.0]}]),
        serde_json::from_str::<serde_json::Value>(r#"{"a": -0e5}"#).expect("json"),
    ] {
        assert!(
            bind_parameters(&[ParameterValue::Json(document.clone())]).is_err(),
            "{document}"
        );
        assert!(
            oracle_rs::OsonEncoder::encode(&document).is_err(),
            "{document}"
        );
    }
    for document in [
        serde_json::json!({"a": 0.0}),
        serde_json::json!({"a": -0.5, "b": 0}),
        serde_json::json!({"a": -10}),
    ] {
        assert!(
            bind_parameters(&[ParameterValue::Json(document.clone())]).is_ok(),
            "{document}"
        );
        assert!(
            oracle_rs::OsonEncoder::encode(&document).is_ok(),
            "{document}"
        );
    }
}
