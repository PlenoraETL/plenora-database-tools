use super::{validate_context_keys, MAX_CONTEXT_KEY};
use plenora_database_core::session_context::{SessionEntry, SessionValue};
use plenora_database_core::transaction::TransactionOptions;
use plenora_database_core::ErrorCategory;

/// Una chiave `ns.<riempimento>` lunga esattamente `length` caratteri.
fn key_of_length(length: usize) -> String {
    let prefix = "ns.";
    format!("{prefix}{}", "a".repeat(length - prefix.len()))
}

fn options_with(key: &str) -> TransactionOptions {
    let mut options = TransactionOptions::default();
    options
        .context
        .insert(
            key,
            SessionEntry::public(SessionValue::Text("v".to_owned())),
        )
        .expect("chiave accettata dal core");
    options
}

#[test]
fn the_longest_writable_key_is_fifty_two_characters() {
    assert_eq!(MAX_CONTEXT_KEY, 52, "64 meno il prefisso `plenora_ctx_`");
}

#[test]
fn a_key_of_fifty_two_characters_is_accepted() {
    let key = key_of_length(52);
    assert_eq!(key.len(), 52);
    assert!(validate_context_keys(&options_with(&key)).is_ok());
}

#[test]
fn a_key_of_fifty_three_characters_is_refused_before_any_statement() {
    // Il core la accetta — ne ammette fino a 63 — quindi senza questo
    // controllo il piano sembrerebbe valido e il rifiuto arriverebbe dal
    // server, con la transazione gia aperta.
    let key = key_of_length(53);
    assert_eq!(key.len(), 53);
    let error =
        validate_context_keys(&options_with(&key)).expect_err("chiave da 53 caratteri accettata");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert!(error.message.contains("52"), "{}", error.message);
    assert!(error.message.contains("64"), "{}", error.message);
}

#[test]
fn an_empty_context_is_valid() {
    assert!(validate_context_keys(&TransactionOptions::default()).is_ok());
}

// ------------------------------------------------------------------
//  decode_row: i metadati di colonna, non l'aspetto dei byte
// ------------------------------------------------------------------

use super::{decode_row, row_decode_error, BINARY_CHARACTER_SET};
use mysql_async::consts::ColumnType;
use mysql_async::Value;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterValue;
use std::sync::Arc;

/// Character set non binario qualsiasi (utf8mb4).
const UTF8MB4: u16 = 255;

fn row_of(column_type: ColumnType, character_set: u16, value: Value) -> mysql_async::Row {
    let wire = Arc::new([mysql_async::Column::new(column_type)
        .with_name(b"payload")
        .with_character_set(character_set)]);
    mysql_common::row::new_row(vec![value], wire)
}

fn decode_one(column_type: ColumnType, character_set: u16, value: Value) -> ParameterValue {
    let names: Arc<[String]> = Arc::from(vec!["payload".to_owned()]);
    let row = decode_row(
        row_of(column_type, character_set, value),
        &names,
        ProviderKind::Mysql,
    )
    .expect("riga decodificabile");
    row.values().first().expect("un valore").clone()
}

/// Il caso che il decoder sbagliava: un BLOB i cui byte sono ASCII.
///
/// Sono `Value::Bytes` come un TEXT, e formano UTF-8 valido: interpretarli
/// come stringa era indistinguibile dal caso giusto finche non si
/// guardava il character set.
#[test]
fn an_ascii_blob_stays_binary() {
    let value = decode_one(
        ColumnType::MYSQL_TYPE_BLOB,
        BINARY_CHARACTER_SET,
        Value::Bytes(b"PNG-like-ascii".to_vec()),
    );
    assert!(
        matches!(value, ParameterValue::Bytes(ref bytes) if bytes == b"PNG-like-ascii"),
        "un BLOB ASCII non deve diventare testo: {value:?}"
    );
}

/// La regressione che il charset da solo introduceva.
///
/// `DECIMAL` viaggia come `Value::Bytes` **con charset binario** — come
/// ogni tipo numerico — ma e una stringa di cifre, e l'intestazione di
/// questo modulo promette che i tipi non nativi escano come stringhe
/// UTF-8. Applicare il charset a ogni `Bytes` trasformava ogni DECIMAL in
/// un blob: il charset distingue binario da testo solo per le famiglie che
/// hanno entrambe le forme.
#[test]
fn a_decimal_stays_a_string_despite_the_binary_charset() {
    let value = decode_one(
        ColumnType::MYSQL_TYPE_NEWDECIMAL,
        BINARY_CHARACTER_SET,
        Value::Bytes(b"12.34".to_vec()),
    );
    assert!(
        matches!(value, ParameterValue::String(ref text) if text == "12.34"),
        "un DECIMAL non deve diventare un blob: {value:?}"
    );
}

/// `BIT` e una maschera, non testo — nemmeno quando i byte lo sembrano.
///
/// `0x41` e UTF-8 valido, quindi il fallback precedente lo consegnava come
/// `"A"`; `0xff` no, e restava byte. La stessa colonna cambiava tipo
/// pubblico in base al valore.
#[test]
fn a_bit_column_stays_binary_whatever_the_bytes_look_like() {
    for payload in [vec![0x41_u8], vec![0xff_u8]] {
        let value = decode_one(
            ColumnType::MYSQL_TYPE_BIT,
            BINARY_CHARACTER_SET,
            Value::Bytes(payload.clone()),
        );
        assert!(
            matches!(value, ParameterValue::Bytes(ref bytes) if *bytes == payload),
            "BIT {payload:?} non deve diventare testo: {value:?}"
        );
    }
}

/// Un WKB e byte anche quando per caso e UTF-8 valido.
#[test]
fn a_geometry_column_stays_binary() {
    let value = decode_one(
        ColumnType::MYSQL_TYPE_GEOMETRY,
        BINARY_CHARACTER_SET,
        Value::Bytes(b"AAAA".to_vec()),
    );
    assert!(matches!(value, ParameterValue::Bytes(_)), "{value:?}");
}

/// Un tipo wire non qualificato non viene indovinato.
#[test]
fn an_unqualified_wire_type_fails_closed() {
    let names: Arc<[String]> = Arc::from(vec!["payload".to_owned()]);
    let error = decode_row(
        row_of(
            // `MYSQL_TYPE_NULL` non ha una rappresentazione `Bytes`
            // sensata: e il rappresentante del caso "questo decoder non
            // sa cosa farne".
            ColumnType::MYSQL_TYPE_NULL,
            BINARY_CHARACTER_SET,
            Value::Bytes(vec![0x01]),
        ),
        &names,
        ProviderKind::Mysql,
    )
    .expect_err("tipo wire non qualificato");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn a_json_column_stays_a_string() {
    let value = decode_one(
        ColumnType::MYSQL_TYPE_JSON,
        UTF8MB4,
        Value::Bytes(br#"{"a":1}"#.to_vec()),
    );
    assert!(
        matches!(value, ParameterValue::String(ref text) if text == r#"{"a":1}"#),
        "{value:?}"
    );
}

#[test]
fn a_text_column_stays_text() {
    let value = decode_one(
        ColumnType::MYSQL_TYPE_BLOB,
        UTF8MB4,
        Value::Bytes("però".as_bytes().to_vec()),
    );
    assert!(
        matches!(value, ParameterValue::String(ref text) if text == "però"),
        "{value:?}"
    );
}

/// L'altro verso: byte non UTF-8 su una colonna dichiarata testuale non
/// degradano a blob in silenzio.
#[test]
fn a_text_column_with_invalid_utf8_is_an_error() {
    let names: Arc<[String]> = Arc::from(vec!["payload".to_owned()]);
    let error = decode_row(
        row_of(
            ColumnType::MYSQL_TYPE_VAR_STRING,
            UTF8MB4,
            Value::Bytes(vec![0xff, 0xfe]),
        ),
        &names,
        ProviderKind::Mysql,
    )
    .expect_err("byte non UTF-8 su colonna testuale");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}

#[test]
fn public_driver_decode_error_contains_only_the_column_position() {
    let error = row_decode_error(ProviderKind::Mysql, 7);
    assert_eq!(error.message, "decode colonna idx=7 fallito");
}

/// Una cella che il protocollo non ha consegnato non e un NULL.
#[test]
fn a_missing_cell_is_a_protocol_error() {
    // Due nomi attesi, una sola colonna sul filo.
    let names: Arc<[String]> = Arc::from(vec!["a".to_owned(), "b".to_owned()]);
    let error = decode_row(
        row_of(
            ColumnType::MYSQL_TYPE_LONGLONG,
            BINARY_CHARACTER_SET,
            Value::Int(1),
        ),
        &names,
        ProviderKind::Mysql,
    )
    .expect_err("il result set ha meno colonne dei nomi attesi");
    assert_eq!(error.category, ErrorCategory::Protocol);
    assert!(!error.is_retryable());
}
