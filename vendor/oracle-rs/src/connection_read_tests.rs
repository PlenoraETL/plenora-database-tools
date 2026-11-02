// Decodifica dei valori letti: testo nazionale, binari IEEE, JSON NULL,
// colonne ripetute fra pagine, nanosecondi. Nessun server Oracle.

fn length_prefixed(bytes: &[u8]) -> Vec<u8> {
    let mut buf = WriteBuffer::new();
    buf.write_bytes_with_length(Some(bytes)).unwrap();
    buf.as_slice().to_vec()
}

fn column_value(column: &ColumnInfo, wire: &[u8]) -> Result<Value> {
    let mut buf = ReadBuffer::from_slice(wire);
    let caps = Capabilities::new();
    Connection::parse_column_value(&mut buf, column, &caps)
}

fn utf16_be(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_be_bytes).collect()
}

// Difetto del 25/10 (sonda): NCHAR/NVARCHAR2 arrivano in AL16UTF16 e si
// decodificavano come UTF-8 con sostituzioni.
#[test]
fn national_text_is_utf16_and_invalid_text_is_an_error() {
    for text in ["a", "\u{e8}\u{e0}", "\u{1F600}x", "a\0b", "a   "] {
        let mut column = ColumnInfo::new("N", OracleType::Varchar);
        column.csfrm = crate::constants::csfrm::NCHAR;
        column.charset_id = 2000;
        let value = column_value(&column, &length_prefixed(&utf16_be(text))).unwrap();
        assert!(
            matches!(&value, Value::String(read) if read == text),
            "{text:?}"
        );
        let plain = ColumnInfo::new("S", OracleType::Char);
        let value = column_value(&plain, &length_prefixed(text.as_bytes())).unwrap();
        assert!(matches!(&value, Value::String(read) if read == text));
    }
    // Lunghezza dispari, surrogato isolato, UTF-8 non valido: errori.
    let nchar = crate::constants::csfrm::NCHAR;
    assert!(decode_column_text(&[0, 0x61, 0], nchar, 2000).is_err());
    assert!(decode_column_text(&[0xD8, 0x3D], nchar, 2000).is_err());
    assert!(decode_column_text(&[0xC3, 0x28], crate::constants::csfrm::IMPLICIT, 873).is_err());
    let plain = ColumnInfo::new("S", OracleType::Varchar);
    assert!(column_value(&plain, &length_prefixed(&[0xFF, 0x61])).is_err());
}

// Difetto del 25/10 (sonda): BINARY_DOUBLE e BINARY_FLOAT passavano dal ramo
// testo con byte qualunque; la read del provider li convertiva in NUMBER.
#[test]
fn binary_double_and_float_decode_every_bit_pattern() {
    let column = ColumnInfo::new("D", OracleType::BinaryDouble);
    for bits in [
        0_u64,
        1,
        0x000F_FFFF_FFFF_FFFF,
        0x0010_0000_0000_0000,
        0x7FEF_FFFF_FFFF_FFFF,
        0xFFEF_FFFF_FFFF_FFFF,
        0x3FB9_9999_9999_999A,
        0x8000_0000_0000_0001,
    ] {
        let value = f64::from_bits(bits);
        let wire = length_prefixed(&crate::types::encode_binary_double(value));
        match column_value(&column, &wire).unwrap() {
            Value::Float(read) => assert_eq!(read.to_bits(), bits),
            other => panic!("BINARY_DOUBLE come {other:?}"),
        }
    }
    let column = ColumnInfo::new("F", OracleType::BinaryFloat);
    for bits in [0_u32, 1, 0x007F_FFFF, 0x7F7F_FFFF, 0xFF7F_FFFF, 0x3DCC_CCCD] {
        let value = f32::from_bits(bits);
        let wire = length_prefixed(&crate::types::encode_binary_float(value));
        match column_value(&column, &wire).unwrap() {
            Value::Float(read) => assert_eq!(read.to_bits(), f64::from(value).to_bits()),
            other => panic!("BINARY_FLOAT come {other:?}"),
        }
    }
    // Una lunghezza diversa non diventa 0.0.
    let column = ColumnInfo::new("D", OracleType::BinaryDouble);
    assert!(column_value(&column, &length_prefixed(&[0x80; 7])).is_err());
}

// Difetto del 25/10 (sonda): un JSON SQL NULL si rileggeva come JSON `null`.
#[test]
fn sql_null_json_and_vector_stay_null() {
    for oracle_type in [OracleType::Json, OracleType::Vector] {
        let column = ColumnInfo::new("J", oracle_type);
        assert!(matches!(column_value(&column, &[0]).unwrap(), Value::Null));
    }
    let mut column = ColumnInfo::new("J", OracleType::Clob);
    column.is_json = true;
    assert!(matches!(column_value(&column, &[0]).unwrap(), Value::Null));
}

// Difetto del 25/10 (sonda): la prima riga di una pagina puo ripetere colonne
// dell'ultima riga della pagina precedente; senza quella riga diventavano NULL.
#[test]
fn repeated_columns_come_from_the_previous_page_or_fail() {
    let columns = vec![
        ColumnInfo::new("A", OracleType::Varchar),
        ColumnInfo::new("B", OracleType::Varchar),
    ];
    let wire = length_prefixed(b"nuovo");
    let previous = vec![Value::String("a".to_owned()), Value::String("b".to_owned())];
    let caps = Capabilities::new();
    // Bit 0 presente, bit 1 ripetuto dalla riga precedente.
    let bits = [0b01_u8];
    let mut buf = ReadBuffer::from_slice(&wire);
    let row = Connection::parse_row_data_with_bitvector(
        &mut buf,
        &columns,
        &caps,
        Some(&bits),
        Some(&previous),
    )
    .unwrap();
    assert!(matches!(
        row.values(),
        [Value::String(first), Value::String(second)] if first == "nuovo" && second == "b"
    ));
    let mut buf = ReadBuffer::from_slice(&wire);
    assert!(Connection::parse_row_data_with_bitvector(
        &mut buf,
        &columns,
        &caps,
        Some(&bits),
        None
    )
    .is_err());
}

#[test]
fn timestamp_nanoseconds_are_not_truncated() {
    let mut data = vec![120, 126, 1, 1, 1, 1, 1];
    data.extend_from_slice(&123_456_000_u32.to_be_bytes());
    assert_eq!(
        crate::types::decode_oracle_timestamp(&data)
            .unwrap()
            .microsecond,
        123_456
    );
    data.truncate(7);
    data.extend_from_slice(&123_456_789_u32.to_be_bytes());
    assert!(crate::types::decode_oracle_timestamp(&data).is_err());
}
