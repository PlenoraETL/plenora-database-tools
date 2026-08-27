use super::{DecimalParameter, UuidParameter};

#[test]
fn decimal_parser_rejects_ambiguous_or_non_ascii_input() {
    for invalid in ["", "+", "-", ".", "+.", "-.", "1.2.3", "NaN", "１２"] {
        assert!(
            DecimalParameter::parse(invalid).is_err(),
            "{invalid:?} deve fallire"
        );
    }
    assert_eq!(
        DecimalParameter::parse("-.5")
            .expect("decimal valido")
            .value,
        -5
    );
}

#[test]
fn uuid_parser_is_utf8_safe_and_strictly_hexadecimal() {
    // 32 byte ma indici non allineati ai confini UTF-8: il vecchio
    // slicing della String poteva causare panic su questo input.
    let adversarial_utf8 = format!("{}aa", "€".repeat(10));
    assert_eq!(adversarial_utf8.len(), 32);
    assert!(UuidParameter::parse(&adversarial_utf8).is_err());
    assert!(UuidParameter::parse("gggggggg-gggg-gggg-gggg-gggggggggggg").is_err());
    assert_eq!(
        UuidParameter::parse("123e4567-e89b-12d3-a456-426614174000")
            .expect("UUID valido")
            .0,
        [
            0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
            0x40, 0x00,
        ]
    );
}
