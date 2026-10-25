//! Oracoli dei codec del fork `oracle-rs`, indipendenti dal codice che
//! verificano: ogni valore generato fa il giro esatto (byte attesi calcolati
//! qui, decodifica uguale all'originale) oppure e rifiutato con un errore,
//! mai con un panic e mai con un valore diverso.
//!
//! I test usano soltanto l'API pubblica che esisteva gia in d978250 (base
//! della PR): il tratto [`Outcome`] accetta sia le firme infallibili di
//! allora sia le `Result` di oggi, cosi lo stesso sorgente misura entrambi.

use oracle_rs::buffer::ReadBuffer;
use oracle_rs::serde_json::{self, json, Value as Json};
use oracle_rs::types::{
    decode_oracle_date, decode_oracle_number, decode_oracle_timestamp, decode_vector,
    encode_oracle_number, OracleDate, OracleTimestamp, OsonDecoder, OsonEncoder,
};
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Esito di un encoder con la firma di allora (valore) o di oggi (`Result`).
pub trait Outcome<T> {
    fn outcome(self) -> Option<T>;
}

impl<const N: usize> Outcome<Vec<u8>> for [u8; N] {
    fn outcome(self) -> Option<Vec<u8>> {
        Some(self.to_vec())
    }
}

impl<const N: usize> Outcome<Vec<u8>> for oracle_rs::Result<[u8; N]> {
    fn outcome(self) -> Option<Vec<u8>> {
        self.ok().map(|bytes| bytes.to_vec())
    }
}

/// Esegue `f` trasformando un panic in un fallimento leggibile del test.
fn no_panic<T>(what: &str, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|payload| {
        drop(payload);
        panic!("{what}: panic invece di un valore o di un errore")
    })
}

fn encode_date(date: OracleDate) -> Option<Vec<u8>> {
    no_panic("encode DATE", || date.to_oracle_bytes().outcome())
}

fn encode_timestamp(timestamp: OracleTimestamp) -> Option<Vec<u8>> {
    no_panic("encode TIMESTAMP", || timestamp.to_oracle_bytes().outcome())
}

/// Anno bisestile nel calendario di Oracle, misurato sul server con
/// `VALIDATE_CONVERSION` sul 29 febbraio: giuliano sul numero d'anno di
/// Oracle fino al 1582 (-4 e -4708 si, -1 e -5 no) tranne il primo anno,
/// -4712, che non lo e; poi gregoriano.
pub fn oracle_leap(year: i32) -> bool {
    if year > 1582 {
        year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
    } else {
        year.rem_euclid(4) == 0 && year != -4712
    }
}

/// Date del calendario di Oracle: anni -4712..=9999 senza lo zero, mesi
/// della lunghezza giusta, e il salto del 1582-10-05..=14.
pub fn oracle_calendar_date(year: i32, month: u8, day: u8) -> bool {
    let length = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if oracle_leap(year) => 29,
        2 => 28,
        _ => return false,
    };
    (-4712..=9999).contains(&year)
        && year != 0
        && (1..=length).contains(&day)
        && !(year == 1582 && month == 10 && (5..=14).contains(&day))
}

/// I sette byte che Oracle scrive per una data (`DUMP`, Typ=12): secolo e
/// anno nel secolo con il segno dell'anno, ciascuno spostato di 100, poi
/// mese, giorno e ora, minuto, secondo piu uno. Misurato: -4712 e (53, 88).
pub fn reference_date_bytes(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
) -> [u8; 7] {
    let sign = if year < 0 { -1 } else { 1 };
    let magnitude = year.abs();
    let century = 100 + sign * (magnitude / 100);
    let in_century = 100 + sign * (magnitude % 100);
    [
        u8::try_from(century).unwrap(),
        u8::try_from(in_century).unwrap(),
        month,
        day,
        hour + 1,
        minute + 1,
        second + 1,
    ]
}

fn all_years() -> impl Iterator<Item = i32> {
    (-4712..=9999).filter(|year| *year != 0)
}

#[test]
fn date_encoder_and_decoder_are_exact_on_every_year_of_the_oracle_calendar() {
    for year in all_years() {
        let last_february = if oracle_leap(year) { 29 } else { 28 };
        for (month, day) in [(1, 1), (2, last_february), (3, 1), (12, 31)] {
            for (hour, minute, second) in [(0, 0, 0), (23, 59, 59), (12, 30, 1)] {
                let date = OracleDate::new(year, month, day, hour, minute, second);
                let expected = reference_date_bytes(year, month, day, hour, minute, second);
                let encoded = encode_date(date);
                assert_eq!(encoded.as_deref(), Some(&expected[..]), "{date:?}");
                assert_eq!(decode_oracle_date(&expected).unwrap(), date, "{date:?}");
            }
        }
    }
}

#[test]
fn dates_outside_the_oracle_calendar_are_refused_not_wrapped() {
    // Base: 10000 e 25601 passavano come altri anni, gli anni negativi e lo
    // zero andavano in panic per overflow di `as u8 + 100`.
    for year in [0, -4713, -4800, 10_000, 25_601, 99_999, i32::MIN, i32::MAX] {
        let date = OracleDate::new(year, 1, 1, 0, 0, 0);
        assert_eq!(encode_date(date), None, "anno {year}");
    }
    // Ogni giorno 0..=32 di ogni mese 0..=13 per gli anni di confine:
    // accettato se e solo se esiste nel calendario di Oracle.
    for year in [
        -4712, -4709, -4708, -101, -100, -5, -4, -1, 1, 4, 99, 100, 1500, 1580, 1582, 1583, 1600,
        1700, 1900, 2000, 2024, 2100, 9996, 9999,
    ] {
        for month in 0..=13_u8 {
            for day in 0..=32_u8 {
                let date = OracleDate::new(year, month, day, 0, 0, 0);
                let accepted = encode_date(date).is_some();
                assert_eq!(
                    accepted,
                    oracle_calendar_date(year, month, day),
                    "{year}-{month}-{day}"
                );
            }
        }
    }
    for (hour, minute, second) in [(24, 0, 0), (0, 60, 0), (0, 0, 60), (255, 255, 255)] {
        let date = OracleDate::new(2024, 1, 1, hour, minute, second);
        assert_eq!(encode_date(date), None, "{hour}:{minute}:{second}");
    }
}

#[test]
fn date_decoder_accepts_only_bytes_it_would_write() {
    // Ogni coppia secolo/anno con giorno e ora fissi, e ogni byte di ora,
    // minuto e secondo: una decodifica accettata deve riscrivere gli stessi
    // byte (base: byte di ora 0 diventava ora 0, anni incoerenti passavano).
    for century in 0..=255_u8 {
        for in_century in 0..=255_u8 {
            let bytes = [century, in_century, 1, 1, 1, 1, 1];
            check_date_bytes(bytes);
        }
    }
    for field in 4..7 {
        for value in 0..=255_u8 {
            let mut bytes = [120, 124, 3, 15, 15, 31, 46];
            bytes[field] = value;
            check_date_bytes(bytes);
        }
    }
    for length in [0, 1, 6, 8, 11, 13] {
        let bytes = vec![120; length];
        assert!(
            no_panic("decode DATE", || decode_oracle_date(&bytes)).is_err(),
            "{length} byte"
        );
    }
}

fn check_date_bytes(bytes: [u8; 7]) {
    let decoded = no_panic("decode DATE", || decode_oracle_date(&bytes));
    if let Ok(date) = decoded {
        let year = date.year;
        let valid = oracle_calendar_date(year, date.month, date.day)
            && date.hour < 24
            && date.minute < 60
            && date.second < 60;
        assert!(valid, "{bytes:?} decodificato in {date:?}");
        assert_eq!(
            reference_date_bytes(
                year,
                date.month,
                date.day,
                date.hour,
                date.minute,
                date.second
            ),
            bytes,
            "{bytes:?}"
        );
    }
}

/// Microsecondi di ogni precisione frazionaria 0..=6, con le cifre di
/// confine di ciascuna.
fn fractions() -> Vec<u32> {
    let mut values = vec![0, 1, 9, 10, 99, 100, 999_999];
    // Precisione 0: soltanto lo zero; precisione p: multipli di 10^(6-p).
    for digits in 1..=6_u32 {
        let unit = 10_u32.pow(6 - digits);
        for count in [1, 5, 9] {
            values.push(unit * count);
        }
        values.push(999_999 - (999_999 % unit));
    }
    values.sort_unstable();
    values.dedup();
    values
}

#[test]
fn timestamp_fraction_round_trips_at_every_precision_or_is_refused() {
    for micro in fractions() {
        let timestamp = OracleTimestamp::new(2024, 2, 29, 23, 59, 59, micro);
        let encoded = encode_timestamp(timestamp).expect("frazione valida");
        let mut expected = reference_date_bytes(2024, 2, 29, 23, 59, 59).to_vec();
        expected.extend_from_slice(&(micro * 1000).to_be_bytes());
        assert_eq!(encoded, expected, "{micro}");
        assert_eq!(decode_oracle_timestamp(&encoded).unwrap(), timestamp);
    }
    // Un secondo intero o piu (un leap second di chrono porta 1_000_000
    // microsecondi) e un valore che traboccherebbe `* 1000` si rifiutano.
    for micro in [1_000_000, 1_999_999, 4_294_968, u32::MAX] {
        let timestamp = OracleTimestamp::new(2024, 1, 1, 0, 0, 0, micro);
        assert_eq!(encode_timestamp(timestamp), None, "{micro}");
    }
    // Lettura: nanosecondi non multipli del microsecondo e frazioni da un
    // secondo in su si rifiutano invece di troncarsi.
    for nanos in [1_u32, 999, 123_456_789, 1_000_000_000, u32::MAX] {
        let mut bytes = reference_date_bytes(2024, 1, 1, 0, 0, 0).to_vec();
        bytes.extend_from_slice(&nanos.to_be_bytes());
        assert!(
            no_panic("decode TIMESTAMP", || decode_oracle_timestamp(&bytes)).is_err(),
            "{nanos}"
        );
    }
    for length in [0, 6, 8, 9, 10, 12, 14] {
        let bytes = vec![120; length];
        assert!(
            no_panic("decode TIMESTAMP", || decode_oracle_timestamp(&bytes)).is_err(),
            "{length} byte"
        );
    }
}

#[test]
fn timestamp_offsets_are_oracle_offsets_or_refused() {
    let base = {
        let mut bytes = reference_date_bytes(2024, 6, 1, 12, 0, 0).to_vec();
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        bytes
    };
    for hour in -12_i8..=14 {
        for minute in [0_i8, 30, 45, 59] {
            let minute = if hour < 0 { -minute } else { minute };
            let mut bytes = base.clone();
            bytes.push(u8::try_from(hour + 20).unwrap());
            bytes.push(u8::try_from(minute + 60).unwrap());
            let decoded = decode_oracle_timestamp(&bytes).unwrap();
            assert_eq!(
                (decoded.tz_hour_offset, decoded.tz_minute_offset),
                (hour, minute)
            );
        }
    }
    // Una regione con il byte basso a zero era letta come UTC; un solo byte
    // di offset a zero era ignorato; un'ora fuori da -12..=14 passava.
    for (high, low) in [(0x80, 0), (0x81, 0x10), (0, 60), (20, 0), (40, 60), (1, 60)] {
        let mut bytes = base.clone();
        bytes.push(high);
        bytes.push(low);
        assert!(
            no_panic("decode TIMESTAMP WITH TIME ZONE", || {
                decode_oracle_timestamp(&bytes)
            })
            .is_err(),
            "{high:#x} {low:#x}"
        );
    }
}

fn oson_round_trip(value: &Json) -> Option<Json> {
    no_panic("OSON", || {
        let encoded = OsonEncoder::encode(value).ok()?;
        Some(OsonDecoder::decode(encoded).expect("OSON scritto dal driver e illeggibile"))
    })
}

#[test]
fn oson_field_names_of_every_length_round_trip_or_are_refused() {
    for length in 0..=255_usize {
        let key: String = (0..length)
            .map(|i| char::from(b'a' + u8::try_from(i % 26).unwrap()))
            .collect();
        let value = json!({ key.clone(): length, "z": [1, {"k": key}] });
        assert_eq!(
            oson_round_trip(&value),
            Some(value),
            "chiave di {length} byte"
        );
    }
    // Base: la lunghezza era scritta `as u8`, una chiave di 300 byte
    // diventava di 44 byte. Oltre 255 byte la forma a nomi lunghi (versione
    // 3) non e scritta dal fork: si rifiuta.
    for length in [256_usize, 300, 1_000, 65_535, 65_536] {
        let value = json!({ "x".repeat(length): 1 });
        assert_eq!(oson_round_trip(&value), None, "chiave di {length} byte");
    }
    // Byte multibyte: la lunghezza conta i byte UTF-8, non i caratteri.
    let accented = "\u{e8}".repeat(127); // 254 byte
    let value = json!({ accented: true });
    assert_eq!(oson_round_trip(&value), Some(value));
    let value = json!({ "\u{e8}".repeat(128): true }); // 256 byte
    assert_eq!(oson_round_trip(&value), None);
}

#[test]
fn oson_field_counts_beyond_255_round_trip() {
    for count in [
        1_usize, 2, 254, 255, 256, 257, 300, 511, 1_000, 5_000, 65_535,
    ] {
        let object: serde_json::Map<String, Json> =
            (0..count).map(|i| (format!("k{i}"), json!(i))).collect();
        let value = Json::Object(object);
        assert_eq!(oson_round_trip(&value), Some(value), "{count} campi");
    }
    // Nomi distinti ripartiti su oggetti annidati contano insieme.
    let nested: Vec<Json> = (0..400).map(|i| json!({ format!("n{i}"): i })).collect();
    let value = Json::Array(nested);
    assert_eq!(oson_round_trip(&value), Some(value));
    let object: serde_json::Map<String, Json> =
        (0..65_536).map(|i| (format!("k{i}"), json!(i))).collect();
    assert_eq!(oson_round_trip(&Json::Object(object)), None, "65536 campi");
}

#[test]
fn oson_nesting_depth_is_bounded_explicitly() {
    let nest = |depth: usize, object: bool| {
        let mut value = json!(1);
        for _ in 0..depth {
            value = if object {
                json!({ "a": value })
            } else {
                json!([value])
            };
        }
        value
    };
    for depth in [1, 2, 100, 126, 127] {
        for object in [false, true] {
            let value = nest(depth, object);
            assert_eq!(oson_round_trip(&value), Some(value), "profondita {depth}");
        }
    }
    for object in [false, true] {
        assert_eq!(oson_round_trip(&nest(128, object)), None, "profondita 128");
        assert_eq!(oson_round_trip(&nest(200, object)), None, "profondita 200");
    }
}

#[test]
fn oson_strings_around_every_length_threshold_round_trip() {
    for length in [0_usize, 1, 31, 32, 255, 256, 65_535, 65_536, 70_000] {
        let value = json!({ "s": "y".repeat(length), "t": ["x".repeat(length)] });
        assert_eq!(oson_round_trip(&value), Some(value), "{length}");
        let scalar = json!("q".repeat(length));
        assert_eq!(oson_round_trip(&scalar), Some(scalar), "scalare {length}");
    }
}

/// Un documento OSON scalare con il nodo `node` (byte del tipo e dati).
fn oson_scalar(node: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0xFF, 0x4A, 0x5A, 0x01];
    bytes.extend_from_slice(&0x0012_u16.to_be_bytes()); // IS_SCALAR | INLINE_LEAF
    bytes.extend_from_slice(&u16::try_from(node.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(node);
    bytes
}

fn oson_number(text: &str) -> Vec<u8> {
    let number = encode_oracle_number(text).unwrap();
    let mut node = vec![0x34, u8::try_from(number.len()).unwrap()];
    node.extend_from_slice(&number);
    oson_scalar(&node)
}

fn decode_oson(bytes: Vec<u8>) -> Option<Json> {
    no_panic("decode OSON", || OsonDecoder::decode(bytes.into()).ok())
}

#[test]
fn oson_numbers_decode_exactly_or_are_refused() {
    for (text, expected) in [
        ("0", json!(0)),
        ("-7", json!(-7)),
        ("9223372036854775807", json!(i64::MAX)),
        ("18446744073709551615", json!(u64::MAX)),
        ("0.1", json!(0.1)),
        ("-2.5", json!(-2.5)),
        ("1e125", json!(1e125)),
        ("1.5e-130", json!(1.5e-130)),
    ] {
        assert_eq!(decode_oson(oson_number(text)), Some(expected), "{text}");
    }
    // Base: passavano da f64 e perdevano cifre in silenzio.
    for text in [
        "0.30000000000000001",
        "1.234567890123456789",
        "123456789012345678901234567890",
        "18446744073709551616",
        "-9223372036854775809",
        "0.1000000000000000000000000000000000001",
    ] {
        assert_eq!(decode_oson(oson_number(text)), None, "{text}");
    }
}

#[test]
fn oson_decoding_errors_are_errors_not_values() {
    // NUMBER non decodificabile: era `null`.
    assert_eq!(decode_oson(oson_scalar(&[0x34, 1, 0xC1])), None);
    // BINARY_DOUBLE NaN: era `null`.
    let nan = oracle_rs::types::encode_binary_double(f64::NAN);
    let mut node = vec![0x36];
    node.extend_from_slice(&nan);
    assert_eq!(decode_oson(oson_scalar(&node)), None);
    // VECTOR esteso: era un array vuoto.
    assert_eq!(
        decode_oson(oson_scalar(&[0x7B, 0x01, 0, 0, 0, 1, 0xAB])),
        None
    );
    // TIMESTAMP con nanosecondi: le cifre sotto il microsecondo si
    // conservano (erano troncate).
    let mut node = vec![0x39];
    node.extend_from_slice(&reference_date_bytes(2024, 3, 15, 14, 30, 45));
    node.extend_from_slice(&123_456_789_u32.to_be_bytes());
    assert_eq!(
        decode_oson(oson_scalar(&node)),
        Some(json!("2024-03-15T14:30:45.123456789"))
    );
    // DATE avanti Cristo: anno con segno e quattro cifre, come JSON_SERIALIZE.
    let mut node = vec![0x3C];
    node.extend_from_slice(&reference_date_bytes(-44, 3, 15, 0, 0, 0));
    assert_eq!(
        decode_oson(oson_scalar(&node)),
        Some(json!("-0044-03-15T00:00:00"))
    );
    // Binario: esadecimale come JSON_SERIALIZE (era base64).
    assert_eq!(
        decode_oson(oson_scalar(&[0x3A, 0, 2, 0x0A, 0x0B])),
        Some(json!("0A0B"))
    );
    // Un id di campo senza nome: era il nome inventato "field_<id>".
    let value = json!({"a": 1});
    let mut bytes = OsonEncoder::encode(&value).unwrap().to_vec();
    let position = bytes.len() - 9; // id del campo: poi offset (4) e valore (4)
    bytes[position] = 9;
    assert_eq!(decode_oson(bytes), None);
}

#[test]
fn oson_decoder_never_panics_on_malformed_documents() {
    let documents = [
        json!({"a": [1, 2.5, "x", null, true], "b": {"c": "d"}}),
        Json::Array((0..300).map(|i| json!({ format!("k{i}"): i })).collect()),
        json!("scalare"),
    ];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for document in &documents {
        let encoded = OsonEncoder::encode(document).unwrap().to_vec();
        for cut in 0..encoded.len() {
            let truncated = encoded[..cut].to_vec();
            let _ = decode_oson(truncated);
        }
        for _ in 0..4_000 {
            let mut mutated = encoded.clone();
            let index = usize::try_from(next() % mutated.len() as u64).unwrap();
            mutated[index] = next().to_le_bytes()[0];
            let _ = decode_oson(mutated);
        }
    }
}

#[test]
fn number_infinities_and_bare_exponents_are_refused() {
    // Base: [0x00] (-~) diventava "", [0xFF, 0x65] (+~) un 1e126 finito,
    // [0xC1] senza mantissa "0".
    for bytes in [
        &[0x00_u8][..],
        &[0xFF, 0x65],
        &[0xC1],
        &[0x3E],
        &[0x01, 0x66],
    ] {
        assert!(
            no_panic("decode NUMBER", || decode_oracle_number(bytes)).is_err(),
            "{bytes:?}"
        );
    }
    assert_eq!(decode_oracle_number(&[0x80]).unwrap().value, "0");
}

#[test]
fn unsigned_and_signed_wire_integers_keep_their_sign() {
    // UB4 con il bit di segno: era la magnitudine positiva.
    let mut buffer = ReadBuffer::from_slice(&[0x81, 0x05]);
    assert!(buffer.read_ub4().is_err());
    let mut buffer = ReadBuffer::from_slice(&[0x82, 0x01, 0x00]);
    assert!(buffer.read_ub2().is_err());
    // SB2 negativo: era il valore positivo, e 200 diventava -56.
    let mut buffer = ReadBuffer::from_slice(&[0x81, 0x05]);
    assert_eq!(buffer.read_sb2().unwrap(), -5);
    let mut buffer = ReadBuffer::from_slice(&[0x01, 200]);
    assert_eq!(buffer.read_sb2().unwrap(), 200);
    // Intero Oracle oltre i64: era avvolto.
    let mut buffer =
        ReadBuffer::from_slice(&[0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    assert!(buffer.read_oracle_int().is_err());
    let mut buffer = ReadBuffer::from_slice(&[0x88, 0x80, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(buffer.read_oracle_int().unwrap(), i64::MIN);
}

#[test]
fn vector_headers_and_payloads_are_checked() {
    // Magic, versione, flag NORM, formato FLOAT32, una dimensione: dieci byte
    // di intestazione senza la norma annunciata (era un panic di slicing).
    let header = |flags: u16, format: u8, count: u32, payload: &[u8]| {
        let mut bytes = vec![0xDB, 0];
        bytes.extend_from_slice(&flags.to_be_bytes());
        bytes.push(format);
        bytes.extend_from_slice(&count.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    };
    for extra in 1..=7 {
        let bytes = header(0x0002, 2, 1, &vec![0; extra]);
        assert!(
            no_panic("decode VECTOR", || decode_vector(&bytes)).is_err(),
            "{extra}"
        );
    }
    // BINARY con 12 bit: gli ultimi 4 erano persi.
    let bytes = header(0, 5, 12, &[0xFF, 0xF0]);
    assert!(no_panic("decode VECTOR", || decode_vector(&bytes)).is_err());
    // Byte in coda oltre le dimensioni: erano ignorati.
    let bytes = header(0, 4, 2, &[1, 2, 3]);
    assert!(no_panic("decode VECTOR", || decode_vector(&bytes)).is_err());
    // Un conteggio enorme senza dati non alloca prima di fallire.
    let bytes = header(0, 3, u32::MAX, &[]);
    assert!(no_panic("decode VECTOR", || decode_vector(&bytes)).is_err());
}

#[test]
fn protocol_response_without_the_national_charset_is_refused() {
    use oracle_rs::messages::ProtocolMessage;
    use oracle_rs::Capabilities;
    let response = |fdo: &[u8]| {
        let mut payload = vec![0, 0, 1, 6, 0];
        payload.extend_from_slice(b"Oracle\0");
        payload.extend_from_slice(&873_u16.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&0_u16.to_le_bytes());
        payload.extend_from_slice(&u16::try_from(fdo.len()).unwrap().to_be_bytes());
        payload.extend_from_slice(fdo);
        payload.push(255);
        payload.push(255);
        payload
    };
    // Senza FDO restava il default AL16UTF16 senza alcuna verifica.
    let mut caps = Capabilities::new();
    assert!(ProtocolMessage::new()
        .parse_response(&response(&[]), &mut caps)
        .is_err());
    let mut fdo = vec![0_u8; 9];
    fdo.extend_from_slice(&2000_u16.to_be_bytes());
    let mut caps = Capabilities::new();
    ProtocolMessage::new()
        .parse_response(&response(&fdo), &mut caps)
        .unwrap();
    assert_eq!(caps.ncharset_id, 2000);
}

#[test]
fn provider_dates_bind_only_where_chrono_and_oracle_calendars_agree() {
    use crate::parameter::bind_parameters;
    use plenora_database_core::provider::ParameterValue;
    // Base: l'etichetta passava, cioe un altro giorno per Oracle (giuliano) e
    // per Arrow (gregoriano prolettico); gli anni negativi andavano in panic.
    for value in ["1500-03-01", "1582-10-14", "0000-01-01", "-0044-03-15"] {
        let bound = no_panic("bind DATE", || {
            bind_parameters(&[ParameterValue::Date(value.to_owned())])
        });
        assert!(bound.is_err(), "{value}");
    }
    for value in ["1582-10-15", "1970-01-01", "9999-12-31"] {
        assert!(
            bind_parameters(&[ParameterValue::Date(value.to_owned())]).is_ok(),
            "{value}"
        );
    }
    // Gli anni oltre 9999 passano il bind e sono rifiutati dall'encoder del
    // driver all'esecuzione (`dates_outside_the_oracle_calendar_...`).
    // Un leap second di chrono porta 1_000_000 microsecondi: rifiutato, non
    // scritto come frazione invalida.
    // Il bind lo accetta o lo rifiuta; se lo accetta, l'encoder del driver
    // deve rifiutarlo prima che arrivi al server.
    for value in [
        "2016-12-31T23:59:60",
        "2016-12-31T23:59:60.5",
        "1500-01-01T00:00:00",
    ] {
        let bound = no_panic("bind TIMESTAMP", || {
            bind_parameters(&[ParameterValue::Timestamp(value.to_owned())])
        });
        let written = match bound.as_deref() {
            Ok([oracle_rs::Value::Timestamp(timestamp)]) => encode_timestamp(*timestamp),
            Ok(other) => panic!("{value}: legato come {other:?}"),
            Err(_) => None,
        };
        assert_eq!(written, None, "{value}");
    }
}
