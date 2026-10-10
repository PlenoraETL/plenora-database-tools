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

/// Due stream concatenati: il lettore si fermerebbe al primo EOS e il
/// secondo stream andrebbe perso con un successo parziale.
#[test]
fn bytes_after_the_end_of_stream_are_rejected() {
    let mut twice = stream_bytes();
    twice.extend_from_slice(&stream_bytes());
    let input = scratch("twice.arrows", &twice);
    let error = rows(&input).expect_err("due stream");
    assert!(
        error.database_error().message.contains("dopo la fine"),
        "{}",
        error.database_error().message
    );
}

/// Uno stream senza EOS i cui ultimi otto byte sembrano il marcatore di
/// fine: il framing, non la coda del file, decide dove finisce.
#[test]
fn a_tail_that_looks_like_the_end_marker_is_not_the_end() {
    let mut forged = stream_bytes();
    forged.truncate(forged.len() - END_OF_STREAM.len());
    // Un messaggio dichiarato ma incompleto, che termina con otto byte
    // uguali al marcatore di fine.
    forged.extend_from_slice(&CONTINUATION);
    forged.extend_from_slice(&64_u32.to_le_bytes());
    forged.extend_from_slice(&END_OF_STREAM);
    let input = scratch("forged.arrows", &forged);
    assert!(rows(&input).is_err(), "coda ambigua accettata");
}

/// Un messaggio oltre il limite si rifiuta prima di allocarne il corpo.
#[test]
fn a_message_beyond_the_limit_is_rejected_before_decoding() {
    let input = scratch("limited.arrows", &stream_bytes());
    let error = open_batches(input.0.to_str().expect("UTF-8"), 16)
        .err()
        .expect("oltre il limite");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
    let file = scratch("limited.arrow", &file_bytes());
    let error = open_batches(file.0.to_str().expect("UTF-8"), 16)
        .err()
        .expect("oltre il limite");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
}

/// Il file cambia dopo la validazione e prima della decodifica: i byte
/// letti nella seconda passata non sono quelli validati, e la lettura
/// fallisce invece di consegnare il dato nuovo. Il corpo e piu grande del
/// buffer del lettore, quindi alla modifica non e ancora stato letto.
#[test]
fn bytes_changed_between_the_two_passes_are_rejected() {
    let large = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(
            (0..200_000_i64)
                .map(|value| value * 10)
                .chain([3])
                .collect::<Vec<_>>(),
        ))],
    )
    .expect("batch");
    let mut stream = StreamWriter::try_new(Vec::new(), &large.schema()).expect("writer");
    stream.write(&large).expect("write");
    let mut file = FileWriter::try_new(Vec::new(), &large.schema()).expect("writer");
    file.write(&large).expect("write");
    for (name, bytes) in [
        ("toctou.arrows", stream.into_inner().expect("stream")),
        ("toctou.arrow", file.into_inner().expect("file")),
    ] {
        let input = scratch(name, &bytes);
        let path = input.0.to_str().expect("UTF-8");
        let opened = open_batches(path, 1 << 24).expect("validato");
        // Il valore 3 dell'ultima riga diventa 4, stessa lunghezza.
        let three = 3_i64.to_le_bytes();
        let offset = bytes
            .windows(8)
            .rposition(|window| window == three)
            .expect("valore nel corpo");
        assert!(
            offset > 64 * 1024,
            "{name}: il valore deve stare oltre il buffer"
        );
        let mut changed = bytes.clone();
        changed[offset] = 4;
        std::fs::write(&input.0, &changed).expect("modifica sul posto");
        let decoded: Result<Vec<_>, _> = opened.batches.collect();
        assert!(decoded.is_err(), "{name}: byte diversi da quelli validati");
    }
}
