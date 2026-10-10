use super::*;
use arrow_ipc::writer::{FileWriter, StreamWriter};
use plenora_database_core::arrow::array::Int64Array;
use plenora_database_core::arrow::schema::{DataType, Field, Schema};

struct Scratch(std::path::PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn scratch(name: &str, bytes: &[u8]) -> Scratch {
    let path =
        std::env::temp_dir().join(format!("plenora-ipc-input-{}-{name}", std::process::id()));
    std::fs::write(&path, bytes).expect("scrittura di prova");
    Scratch(path)
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
        .expect("batch")
}

fn file_bytes() -> Vec<u8> {
    let batch = batch();
    let mut writer = FileWriter::try_new(Vec::new(), &batch.schema()).expect("writer");
    writer.write(&batch).expect("write");
    writer.into_inner().expect("finish")
}

fn stream_bytes() -> Vec<u8> {
    let batch = batch();
    let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema()).expect("writer");
    writer.write(&batch).expect("write");
    writer.into_inner().expect("finish")
}

fn rows(path: &Scratch) -> CliResult<u64> {
    let stream = IpcFileBatchStream::open(path.0.to_str().expect("UTF-8"))?;
    Ok(stream.declared_input_rows().expect("righe"))
}

#[test]
fn the_file_format_is_read() {
    let file = scratch("file.arrow", &file_bytes());
    assert_eq!(rows(&file).expect("file"), 3);
}

/// Il catalogo dichiara `application/vnd.apache.arrow.stream` in ingresso a
/// `database.write`, e data-tools2 e IO-tools lo producono.
#[test]
fn the_stream_format_is_read() {
    let stream = scratch("stream.arrows", &stream_bytes());
    assert_eq!(rows(&stream).expect("stream"), 3);
}

/// Un inizio che non e ne file ne stream non si prova "a tentativi".
#[test]
fn an_unrecognised_or_legacy_start_is_rejected() {
    // Stream legacy, precedente ad Arrow 0.15: lunghezza senza continuazione.
    let mut legacy = stream_bytes();
    legacy.drain(..4);
    for (name, bytes) in [
        ("empty.arrow", Vec::new()),
        ("text.arrow", b"id\n1\n2\n".to_vec()),
        ("legacy.arrows", legacy),
    ] {
        let input = scratch(name, &bytes);
        let error = rows(&input).expect_err(name);
        assert!(
            error.database_error().message.contains("non riconosciuto"),
            "{name}: {}",
            error.database_error().message
        );
    }
}

/// Uno stream troncato fra due messaggi non e uno stream piu corto.
#[test]
fn a_stream_without_its_end_marker_is_rejected() {
    let mut truncated = stream_bytes();
    truncated.truncate(truncated.len() - END_OF_STREAM.len());
    let input = scratch("truncated.arrows", &truncated);
    let error = rows(&input).expect_err("troncato");
    assert!(error.database_error().message.contains("marcatore di fine"));
}

#[test]
fn the_format_is_recognised_from_the_first_bytes() {
    assert_eq!(detect(&file_bytes()[..8]).expect("file"), IpcFormat::File);
    assert_eq!(
        detect(&stream_bytes()[..8]).expect("stream"),
        IpcFormat::Stream
    );
    assert!(detect(b"ARROW").is_err());
}
