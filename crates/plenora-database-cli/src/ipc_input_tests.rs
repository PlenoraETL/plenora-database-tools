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

/// Il prefisso di un messaggio cambia dopo la validazione: una fine
/// anticipata o una lunghezza diversa non devono arrivare al decoder. Il
/// messaggio alterato segue un batch piu grande dei buffer di lettura,
/// quindi alla modifica non e ancora stato letto.
#[test]
fn a_prefix_changed_between_the_two_passes_is_rejected() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let large = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(
            (0..200_000_i64).collect::<Vec<_>>(),
        ))],
    )
    .expect("batch grande");
    let small = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch piccolo");
    let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    writer.write(&large).expect("write");
    writer.write(&small).expect("write");
    let bytes = writer.into_inner().expect("stream");
    // Il prefisso dell'ultimo batch: tre messaggi prima (schema, grande).
    let mut position = 0_usize;
    for _ in 0..2 {
        let metadata =
            u32::from_le_bytes(bytes[position + 4..position + 8].try_into().expect("u32")) as usize;
        let message = root_as_message(&bytes[position + 8..position + 8 + metadata]).expect("msg");
        position += 8 + metadata + usize::try_from(message.bodyLength()).expect("corpo");
    }
    assert!(
        position > 128 * 1024,
        "il prefisso deve stare oltre i buffer"
    );
    let declared = u32::from_le_bytes(bytes[position + 4..position + 8].try_into().expect("u32"));
    for (name, value) in [("eos", 0_u32), ("length", declared + 8)] {
        let mut forged = bytes.clone();
        forged[position + 4..position + 8].copy_from_slice(&value.to_le_bytes());
        let input = scratch(&format!("prefix-{name}.arrows"), &bytes);
        let opened = open_batches(input.0.to_str().expect("UTF-8"), 1 << 24).expect("validato");
        std::fs::write(&input.0, &forged).expect("modifica sul posto");
        let decoded: Result<Vec<_>, _> = opened.batches.collect();
        assert!(
            decoded.is_err(),
            "{name}: prefisso alterato consegnato al decoder"
        );
    }
}

fn two_large_batches() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    for start in [0_i64, 100_000] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(
                (start..start + 100_000).collect::<Vec<_>>(),
            ))],
        )
        .expect("batch");
        writer.write(&batch).expect("write");
    }
    writer.into_inner().expect("stream")
}

/// Due messaggi consecutivi vicini al limite: il verificatore libera il
/// precedente prima di allocare il successivo, e non tiene mai piu di un
/// messaggio.
#[test]
fn the_verifier_holds_one_message_at_a_time() {
    let bytes = two_large_batches();
    let input = scratch("two-large.arrows", &bytes);
    let mut file = File::open(&input.0).expect("apertura");
    let length = bytes.len() as u64;
    let (segments, _) = scan_stream(&mut file, 0, length, 1 << 24).expect("validato");
    let largest = segments
        .iter()
        .map(Segment::length)
        .max()
        .expect("segmenti");
    file.seek(SeekFrom::Start(0)).expect("seek");
    let mut verified = VerifiedRange::new(BufReader::new(file), segments);
    std::io::copy(&mut verified, &mut std::io::sink()).expect("lettura verificata");
    assert!(
        verified.peak <= usize::try_from(largest).expect("usize"),
        "picco {} oltre un messaggio ({largest})",
        verified.peak
    );
}

/// Il limite conta tutto l'inquadramento: un messaggio che lo rispetta solo
/// senza il prefisso di otto byte e oltre il limite.
#[test]
fn the_limit_counts_the_message_prefix() {
    let bytes = stream_bytes();
    let input = scratch("prefix-limit.arrows", &bytes);
    let mut file = File::open(&input.0).expect("apertura");
    let (segments, _) = scan_stream(&mut file, 0, bytes.len() as u64, 1 << 24).expect("validato");
    let without_prefix = segments
        .iter()
        .map(|segment| segment.metadata_length + segment.body_length)
        .max()
        .expect("segmenti");
    let error = open_batches(input.0.to_str().expect("UTF-8"), without_prefix)
        .err()
        .expect("il prefisso deve contare");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
}

/// Molti batch vuoti: ogni messaggio sta nel limite, ma l'indice dei
/// segmenti cresce con il loro numero e deve stare nello stesso budget.
#[test]
fn many_empty_batches_beyond_the_index_budget_are_rejected() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let empty = RecordBatch::new_empty(Arc::clone(&schema));
    let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    for _ in 0..2_000 {
        writer.write(&empty).expect("write");
    }
    let bytes = writer.into_inner().expect("stream");
    let input = scratch("many-empty.arrows", &bytes);
    let error = open_batches(input.0.to_str().expect("UTF-8"), 16 * 1024)
        .err()
        .expect("indice oltre il budget");
    let error = error.database_error();
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
    assert!(!error.message.contains("2000"), "{}", error.message);
}

/// Dopo un messaggio non verificato il lettore non riprende dal segmento
/// successivo: ogni lettura seguente fallisce.
#[test]
fn after_a_failed_message_every_read_fails() {
    let bytes = two_large_batches();
    let input = scratch("poisoned.arrows", &bytes);
    let mut file = File::open(&input.0).expect("apertura");
    let (segments, _) = scan_stream(&mut file, 0, bytes.len() as u64, 1 << 24).expect("validato");
    // Il primo batch cambia: lo schema passa, il batch no.
    let first_batch = usize::try_from(segments[1].offset).expect("usize") + 64;
    let mut changed = bytes;
    changed[first_batch] ^= 0xFF;
    let mut verified = VerifiedRange::new(std::io::Cursor::new(changed), segments);
    let mut sink = Vec::new();
    assert!(verified.read_to_end(&mut sink).is_err(), "primo errore");
    let mut buffer = [0_u8; 16];
    assert!(verified.read(&mut buffer).is_err(), "ripresa dopo l'errore");
}
