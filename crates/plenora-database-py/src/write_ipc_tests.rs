use super::{check_stream_framing, decode_ipc_stream};

#[test]
fn pyarrow_dictionary_stream_preserves_nulls_and_metadata() {
    let bytes = include_bytes!("../../../tests/fixtures/arrow/dictionary.stream");
    let (schema, batches, rows) = decode_ipc_stream(bytes).expect("PyArrow stream");
    assert_eq!(rows, 3);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].column(0).null_count(), 1);
    assert_eq!(
        schema
            .metadata()
            .get("plenora.contract.version")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        schema
            .field(0)
            .metadata()
            .get("test.unit")
            .map(String::as_str),
        Some("label")
    );
}

#[test]
fn dictionary_without_data_returns_redacted_error() {
    let bytes = include_bytes!("../../../tests/fixtures/arrow/dictionary-missing-data.stream");
    let error = decode_ipc_stream(bytes).expect_err("malformed dictionary");
    assert_eq!(error.message, "record batch Arrow IPC non valido: il 1o");
    assert!(!error.message.contains("PAYLOAD_MUST_NOT_LEAK"));
}

/// Uno stream senza il marcatore di fine e troncato: tagliato al confine
/// fra due messaggi si leggerebbe come uno stream piu corto, senza errore.
#[test]
fn a_stream_without_its_end_marker_is_rejected() {
    let bytes = include_bytes!("../../../tests/fixtures/arrow/dictionary.stream");
    let truncated = &bytes[..bytes.len() - 8];
    let error = decode_ipc_stream(truncated).expect_err("stream troncato");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
}

/// Byte dopo la fine dello stream non si ignorano.
#[test]
fn bytes_after_the_end_marker_are_rejected() {
    let bytes = include_bytes!("../../../tests/fixtures/arrow/dictionary.stream");
    let mut doubled = bytes.to_vec();
    doubled.extend_from_slice(bytes);
    assert!(
        decode_ipc_stream(&doubled).is_err(),
        "secondo stream ignorato"
    );
}

/// Le lunghezze dichiarate dall'inquadramento non governano le allocazioni:
/// un corpo o dei metadati piu lunghi dell'input sono un errore prima di
/// arrivare al decoder, che allocherebbe la lunghezza dichiarata.
#[test]
fn declared_lengths_beyond_the_input_are_rejected_before_decoding() {
    let mut forged = vec![0xFF, 0xFF, 0xFF, 0xFF];
    forged.extend_from_slice(&0x7FFF_FFF0_u32.to_le_bytes());
    forged.extend_from_slice(&[0_u8; 32]);
    let error = check_stream_framing(&forged).expect_err("metadati oltre l'input");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
    let bytes = include_bytes!("../../../tests/fixtures/arrow/dictionary.stream");
    assert!(check_stream_framing(bytes).is_ok());
}

/// Il valore di un metadato che porta i due byte `01 00` (endianness `Big`)
/// in posizione pari, qualunque sia l'allineamento della stringa.
const BIG_ENDIAN_CARRIER: &str = "\u{1}\u{0}\u{1}\u{0}\u{1}\u{1}\u{0}";

/// Uno stream il cui schema dichiara l'ordine dei byte big-endian: lo slot
/// `endianness` della vtable dello schema, assente perche al valore
/// predefinito, punta a due byte `01 00` di un metadato (Arrow non permette
/// di scriverlo).
fn big_endian_stream() -> Vec<u8> {
    use plenora_database_core::arrow::array::{Int32Array, RecordBatch};
    use plenora_database_core::arrow::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int32, false)],
        std::collections::HashMap::from([("portatore".to_owned(), BIG_ENDIAN_CARRIER.to_owned())]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .expect("batch");
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    writer.write(&batch).expect("write");
    let mut bytes = writer.into_inner().expect("stream");
    let metadata = &mut bytes[8..];
    let (table, carrier) = {
        let message = arrow_ipc::root_as_message(metadata).expect("schema");
        let table = message.header_as_schema().expect("schema")._tab.loc();
        let pattern = BIG_ENDIAN_CARRIER.as_bytes();
        let start = metadata
            .windows(pattern.len())
            .position(|window| window == pattern)
            .expect("portatore");
        (table, if start % 2 == 0 { start } else { start + 5 })
    };
    let soffset = i32::from_le_bytes(metadata[table..table + 4].try_into().expect("soffset"));
    let vtable =
        usize::try_from(i64::try_from(table).expect("loc") - i64::from(soffset)).expect("vtable");
    let relative = u16::try_from(carrier - table).expect("offset");
    metadata[vtable + 4..vtable + 6].copy_from_slice(&relative.to_le_bytes());
    assert_eq!(
        arrow_ipc::root_as_message(metadata)
            .expect("valido")
            .header_as_schema()
            .expect("schema")
            .endianness(),
        arrow_ipc::Endianness::Big
    );
    bytes
}

/// Un'endianness diversa da quella della piattaforma si rifiuta: il lettore
/// dello stream di Arrow non la confronta, e i valori si leggerebbero con i
/// byte invertiti.
#[test]
fn a_foreign_endianness_is_rejected() {
    let error = decode_ipc_stream(&big_endian_stream()).expect_err("endianness estranea");
    assert!(error.message.contains("endianness"), "{}", error.message);
}
