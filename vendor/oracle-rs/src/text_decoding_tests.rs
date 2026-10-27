use super::decode_column_text;

#[test]
fn national_and_database_text_decode_exactly() {
    // NVARCHAR2 'ab' in AL16UTF16: prima si leggeva "\0a\0b".
    assert_eq!(
        decode_column_text(&[0, b'a', 0, b'b'], 2, 2000).expect("utf16"),
        "ab"
    );
    assert_eq!(decode_column_text(b"ab", 1, 873).expect("utf8"), "ab");
    // Il charset nazionale UTF8 (871) non e misurato: si rifiuta invece di
    // indovinare (terzo giro della #135).
    assert!(decode_column_text(b"ab", 2, 871).is_err());
    // Un NCLOB senza il charset della colonna (locator temporaneo) neppure.
    assert!(decode_column_text(&[0, b'a'], 2, 0).is_err());
    assert!(decode_column_text(&[0x61, 0xFF, 0x62], 1, 873).is_err());
    assert!(decode_column_text(&[0, b'a', 0], 2, 2000).is_err());
    assert!(decode_column_text(&[0xD8, 0x00], 2, 2000).is_err());
    assert!(decode_column_text(b"ab", 2, 1).is_err());
}
