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
