use super::decode_ipc_stream;

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
