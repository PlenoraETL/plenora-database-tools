//! Plenora fork: il describe porta precisione e scala con il segno (SB1),
//! rifiuta un tipo senza decoder e una colonna senza nome.

use super::Connection;

/// Describe di una colonna nel formato che `parse_describe_info` legge con
/// la versione TTC 16 (oaccolid presente, niente domini, annotazioni o
/// vettori).
fn describe(ora_type: u8, precision: u8, scale: u8, name: Option<&[u8]>) -> Vec<u8> {
    let mut bytes = vec![0, 1, 1, 0]; // max row size, una colonna, byte saltato
    bytes.extend_from_slice(&[ora_type, 0, precision, scale]);
    bytes.extend_from_slice(&[1, 22]); // buffer size
    bytes.extend_from_slice(&[0, 0, 0]); // array elements, cont flags, OID vuoto
    bytes.extend_from_slice(&[0, 0, 0, 0]); // version, charset id, csfrm, max size
    bytes.push(0); // oaccolid
    bytes.extend_from_slice(&[1, 0]); // nulls allowed, v7 length
    match name {
        Some(name) => {
            bytes.extend_from_slice(&[1, u8::try_from(name.len()).unwrap()]);
            bytes.push(u8::try_from(name.len()).unwrap());
            bytes.extend_from_slice(name);
        }
        None => bytes.push(0),
    }
    bytes.extend_from_slice(&[0, 0, 0, 0]); // schema, type name, position, uds flags
    bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // current date, dcb*, dcbqcky
    bytes
}

fn parse(bytes: &[u8]) -> crate::Result<Vec<crate::ColumnInfo>> {
    Connection::parse_describe_info(&mut crate::buffer::ReadBuffer::from_slice(bytes), 16)
}

#[test]
fn describe_scale_is_signed_and_unknown_types_or_names_are_refused() {
    // NUMBER senza scala: -127 (0x81); NUMBER(5,-2): -2 (0xFE). Il fork li
    // leggeva come 129 e 254.
    for (precision, scale, expected) in [(0_u8, 0x81_u8, -127_i16), (5, 0xFE, -2), (10, 2, 2)] {
        let columns = parse(&describe(2, precision, scale, Some(b"N"))).unwrap();
        assert_eq!(columns[0].scale, expected);
        assert_eq!(columns[0].precision, i16::from(precision));
    }
    // REF (111) non ha decoder: era descritto come VARCHAR2.
    assert!(parse(&describe(111, 0, 0, Some(b"R"))).is_err());
    // Una colonna senza nome: era il nome "".
    assert!(parse(&describe(1, 0, 0, None)).is_err());
}
