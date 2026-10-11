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
    let segments = scan_stream(&mut file, 0, length, 1 << 24)
        .expect("validato")
        .segments;
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
    let segments = scan_stream(&mut file, 0, bytes.len() as u64, 1 << 24)
        .expect("validato")
        .segments;
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
    let segments = scan_stream(&mut file, 0, bytes.len() as u64, 1 << 24)
        .expect("validato")
        .segments;
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

/// Il footer si legge mentre l'indice dei segmenti e vivo: conta la sua
/// capacita allocata, non la lunghezza. 513 batch vuoti con un limite di
/// 60.000 byte stanno nella prima passata, ma indice (capacita 1024),
/// footer e blocchi dichiarati insieme lo superano.
#[test]
fn the_footer_budget_counts_the_allocated_index() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let empty = RecordBatch::new_empty(Arc::clone(&schema));
    let mut writer = FileWriter::try_new(Vec::new(), &schema).expect("writer");
    for _ in 0..513 {
        writer.write(&empty).expect("write");
    }
    let bytes = writer.into_inner().expect("file");
    let input = scratch("footer-budget.arrow", &bytes);
    let error = open_batches(input.0.to_str().expect("UTF-8"), 60_000)
        .err()
        .expect("indice, footer e blocchi oltre il budget");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
}

/// Sostituisce l'ultima occorrenza: nel file Arrow e quella del footer.
fn replace_last(bytes: &mut [u8], from: &[u8], to: &[u8]) {
    assert_eq!(from.len(), to.len());
    let at = bytes
        .windows(from.len())
        .rposition(|window| window == from)
        .expect("occorrenza nel footer");
    bytes[at..at + to.len()].copy_from_slice(to);
}

/// Lo schema del footer deve coincidere con quello dello stream, metadati
/// compresi: un lettore di file Arrow userebbe il footer, lo stream l'altro.
#[test]
fn a_footer_schema_different_from_the_stream_is_rejected() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("colonna_a", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([("chiave".to_owned(), "campo_1".to_owned())]),
            ),
        ],
        std::collections::HashMap::from([("versione".to_owned(), "schema_1".to_owned())]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch");
    let mut writer = FileWriter::try_new(Vec::new(), &schema).expect("writer");
    writer.write(&batch).expect("write");
    let original = writer.into_inner().expect("file");
    for (name, from, to) in [
        ("nome", b"colonna_a".as_slice(), b"colonna_b".as_slice()),
        (
            "metadati del campo",
            b"campo_1".as_slice(),
            b"campo_2".as_slice(),
        ),
        (
            "metadati dello schema",
            b"schema_1".as_slice(),
            b"schema_2".as_slice(),
        ),
    ] {
        let mut forged = original.clone();
        replace_last(&mut forged, from, to);
        let input = scratch(
            &format!("footer-schema-{}.arrow", from[from.len() - 1]),
            &forged,
        );
        let result = open_batches(input.0.to_str().expect("UTF-8"), 1 << 20);
        assert!(
            result.is_err(),
            "{name}: schema del footer discordante accettato"
        );
    }
    let input = scratch("footer-schema-same.arrow", &original);
    open_batches(input.0.to_str().expect("UTF-8"), 1 << 20).expect("schema coerente");
}

/// Uno schema con metadati grandi resta vivo per tutta la lettura (lo
/// materializza il lettore dello stream) e, per il file, anche come schema
/// del footer: conta nel budget. Un valore di 20.000 byte con un limite di
/// 64 KiB sta nel limite per messaggio, ma non con le copie dello schema.
#[test]
fn a_large_schema_counts_in_the_budget_while_it_is_alive() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        std::collections::HashMap::from([("grande".to_owned(), "x".repeat(20_000))]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch");
    let mut stream = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    stream.write(&batch).expect("write");
    let mut file = FileWriter::try_new(Vec::new(), &schema).expect("writer");
    file.write(&batch).expect("write");
    for (name, bytes) in [
        ("large-schema.arrows", stream.into_inner().expect("stream")),
        ("large-schema.arrow", file.into_inner().expect("file")),
    ] {
        let input = scratch(name, &bytes);
        let error = open_batches(input.0.to_str().expect("UTF-8"), 64 * 1024)
            .err()
            .unwrap_or_else(|| panic!("{name}: schema oltre il budget accettato"));
        assert_eq!(
            error.database_error().category,
            plenora_database_core::ErrorCategory::ResourceLimit,
            "{name}"
        );
        // Con un limite largo lo stesso input si legge.
        open_batches(input.0.to_str().expect("UTF-8"), 1 << 20).expect(name);
    }
}

/// Gli id dei dizionari governano la decodifica: un footer con gli id
/// scambiati darebbe a un lettore di file Arrow i valori di un'altra
/// colonna. Si rifiuta anche se nomi, tipi e metadati coincidono.
#[test]
fn a_footer_with_swapped_dictionary_ids_is_rejected() {
    use plenora_database_core::arrow::array::types::Int32Type;
    use plenora_database_core::arrow::array::{DictionaryArray, StringArray};
    let dictionary = |values: &[&str]| -> Arc<dyn plenora_database_core::arrow::array::Array> {
        Arc::new(DictionaryArray::<Int32Type>::new(
            vec![0_i32, 1].into(),
            Arc::new(StringArray::from(values.to_vec())),
        ))
    };
    let field = |name: &str| {
        Field::new(
            name,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )
    };
    // Tre colonne: gli id 1 e 2 non sono il valore di default, quindi
    // compaiono nel footer e si possono scambiare.
    let schema = Arc::new(Schema::new(vec![field("c0"), field("c1"), field("c2")]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            dictionary(&["a", "a"]),
            dictionary(&["b", "b"]),
            dictionary(&["c", "c"]),
        ],
    )
    .expect("batch");
    let mut writer = FileWriter::try_new(Vec::new(), &schema).expect("writer");
    writer.write(&batch).expect("write");
    let original = writer.into_inner().expect("file");
    let footer_length = u32::from_le_bytes(
        original[original.len() - 10..original.len() - 6]
            .try_into()
            .expect("u32"),
    ) as usize;
    let footer_start = original.len() - 10 - footer_length;
    let mut forged = original.clone();
    let footer = &mut forged[footer_start..original.len() - 10];
    let one = 1_i64.to_le_bytes();
    let two = 2_i64.to_le_bytes();
    let find = |bytes: &[u8], needle: &[u8; 8]| {
        let found: Vec<usize> = (0..=bytes.len() - 8)
            .step_by(8)
            .filter(|at| bytes[*at..*at + 8] == *needle)
            .collect();
        assert_eq!(found.len(), 1, "id non univoco nel footer");
        found[0]
    };
    let (at_one, at_two) = (find(footer, &one), find(footer, &two));
    footer[at_one..at_one + 8].copy_from_slice(&two);
    footer[at_two..at_two + 8].copy_from_slice(&one);
    let input = scratch("footer-dictionary-ids.arrow", &forged);
    assert!(
        open_batches(input.0.to_str().expect("UTF-8"), 1 << 20).is_err(),
        "id dei dizionari scambiati nel footer accettati"
    );
    let input = scratch("footer-dictionary-ids-same.arrow", &original);
    open_batches(input.0.to_str().expect("UTF-8"), 1 << 20).expect("footer coerente");
}

/// Il valore della chiave di schema che porta i due byte `01 00` (una
/// endianness `Big`) in posizione pari, qualunque sia l'allineamento della
/// stringa nel flatbuffer.
const BIG_ENDIAN_CARRIER: &str = "\u{1}\u{0}\u{1}\u{0}\u{1}\u{1}\u{0}";

/// Uno stream il cui schema dichiara l'ordine dei byte big-endian, mentre i
/// dati restano quelli scritti dalla piattaforma. Arrow non permette di
/// scriverlo, quindi lo slot `endianness` della vtable dello schema, assente
/// perche al valore predefinito, si fa puntare a due byte `01 00` di un
/// metadato.
fn big_endian_stream() -> Vec<u8> {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int32, false)],
        std::collections::HashMap::from([("portatore".to_owned(), BIG_ENDIAN_CARRIER.to_owned())]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            plenora_database_core::arrow::array::Int32Array::from(vec![1]),
        )],
    )
    .expect("batch");
    let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    writer.write(&batch).expect("write");
    let mut bytes = writer.into_inner().expect("stream");
    declare_big_endian(&mut bytes[8..]);
    bytes
}

/// Riscrive in place il primo messaggio (lo schema) di `metadata`.
fn declare_big_endian(metadata: &mut [u8]) {
    let (table, carrier) = {
        let message = root_as_message(metadata).expect("messaggio di schema");
        let schema = message.header_as_schema().expect("schema");
        let table = schema._tab.loc();
        let pattern = BIG_ENDIAN_CARRIER.as_bytes();
        let start = metadata
            .windows(pattern.len())
            .position(|window| window == pattern)
            .expect("portatore");
        // `01 00` in posizione pari: all'inizio o dal sesto byte.
        let carrier = if start % 2 == 0 { start } else { start + 5 };
        (table, carrier)
    };
    let soffset = i32::from_le_bytes(metadata[table..table + 4].try_into().expect("soffset"));
    let vtable =
        usize::try_from(i64::try_from(table).expect("loc") - i64::from(soffset)).expect("vtable");
    let relative = u16::try_from(carrier - table).expect("offset nel tavolo");
    // Lo slot 0 della vtable (dopo le due lunghezze) e `endianness`.
    metadata[vtable + 4..vtable + 6].copy_from_slice(&relative.to_le_bytes());
    let message = root_as_message(metadata).expect("ancora valido");
    assert_eq!(
        message.header_as_schema().expect("schema").endianness(),
        arrow_ipc::Endianness::Big
    );
}

/// Un'endianness diversa da quella della piattaforma si rifiuta: il lettore
/// dello stream di Arrow non la confronta, e un Int32 con valore 1 scritto
/// big-endian si leggerebbe 16777216.
#[test]
fn a_foreign_endianness_is_rejected() {
    let bytes = big_endian_stream();
    let input = scratch("big-endian.arrows", &bytes);
    let error = open_batches(input.0.to_str().expect("UTF-8"), 1 << 20)
        .err()
        .expect("endianness estranea");
    assert!(
        error.database_error().message.contains("endianness"),
        "{}",
        error.database_error().message
    );
}

/// Una colonna dizionario con valori lunghi: il dizionario resta in memoria
/// per tutta la lettura.
fn dictionary_column(
    name: &str,
    seed: char,
) -> (Field, plenora_database_core::arrow::array::ArrayRef) {
    let values = (0..10)
        .map(|index| format!("{seed}{index}{}", "x".repeat(1_000)))
        .collect::<Vec<_>>();
    let keys = plenora_database_core::arrow::array::Int32Array::from(vec![0, 1]);
    let array = plenora_database_core::arrow::array::DictionaryArray::new(
        keys,
        Arc::new(plenora_database_core::arrow::array::StringArray::from(
            values,
        )),
    );
    (
        Field::new(
            name,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Arc::new(array),
    )
}

/// I dizionari restano tutti residenti nel lettore, per id: il budget li
/// conta insieme. Ciascuno sta nel limite, i tre insieme no.
#[test]
fn resident_dictionaries_count_together_in_the_budget() {
    let (fields, columns): (Vec<_>, Vec<_>) = [("a", 'a'), ("b", 'b'), ("c", 'c')]
        .into_iter()
        .map(|(name, seed)| dictionary_column(name, seed))
        .unzip();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
    let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    writer.write(&batch).expect("write");
    let bytes = writer.into_inner().expect("stream");
    let input = scratch("resident-dictionaries.arrows", &bytes);
    // Un dizionario e circa 10 KB: due copie del messaggio piu grande
    // stanno in 40 KB, i tre dizionari residenti in piu no.
    let error = open_batches(input.0.to_str().expect("UTF-8"), 40_000)
        .err()
        .expect("dizionari residenti oltre il budget");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
    open_batches(input.0.to_str().expect("UTF-8"), 1 << 20).expect("nel budget");
}

/// Un dizionario sostituito prende il posto del precedente: la memoria
/// residente e quella dell'ultimo (con il precedente vivo durante la
/// sostituzione), non la somma di tutti quelli arrivati.
#[test]
fn a_replaced_dictionary_does_not_add_up() {
    let (field, first) = dictionary_column("a", 'a');
    let (_, second) = dictionary_column("a", 'b');
    let (_, third) = dictionary_column("a", 'c');
    let schema = Arc::new(Schema::new(vec![field]));
    let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    for column in [first, second, third] {
        writer
            .write(&RecordBatch::try_new(Arc::clone(&schema), vec![column]).expect("batch"))
            .expect("write");
    }
    let bytes = writer.into_inner().expect("stream");
    let input = scratch("replaced-dictionary.arrows", &bytes);
    // Due copie del messaggio piu grande e due dizionari vivi durante una
    // sostituzione stanno in 48 KB; la somma dei tre dizionari no.
    let batches = open_batches(input.0.to_str().expect("UTF-8"), 48_000)
        .expect("un dizionario residente alla volta")
        .batches
        .collect::<Result<Vec<_>, _>>()
        .expect("decodifica");
    assert_eq!(batches.len(), 3);
}

/// Uno schema con molti campi e metadati piccoli: la memoria di ogni mappa
/// materializzata (tabella hash di costruzione, vettore ordinato, nodo del
/// `BTreeMap`, `Arc`) supera di molto i byte del flatbuffer, e conta.
#[test]
fn many_fields_with_small_metadata_count_their_maps() {
    let fields = (0..2_000)
        .map(|index| {
            Field::new(format!("c{index}"), DataType::Int32, false).with_metadata(
                std::collections::HashMap::from([("k".to_owned(), "v".to_owned())]),
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let writer = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
    let bytes = writer.into_inner().expect("stream");
    assert!(
        bytes.len() < 512 * 1024,
        "flatbuffer piccolo: {}",
        bytes.len()
    );
    let input = scratch("small-maps.arrows", &bytes);
    let error = open_batches(input.0.to_str().expect("UTF-8"), 3 << 19)
        .err()
        .expect("mappe materializzate oltre il budget");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
}

/// Un delta si concatena al dizionario precedente: la memoria residente
/// cresce a ogni delta, e durante la concatenazione il precedente e il
/// nuovo sono vivi insieme.
#[test]
fn dictionary_deltas_accumulate_in_the_budget() {
    use arrow_ipc::writer::{DictionaryHandling, IpcWriteOptions};
    let field = Field::new(
        "a",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    );
    let schema = Arc::new(Schema::new(vec![field]));
    let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    let mut writer =
        StreamWriter::try_new_with_options(Vec::new(), &schema, options).expect("writer");
    let mut values = Vec::new();
    for seed in ['a', 'b', 'c'] {
        values.extend((0..10).map(|index| format!("{seed}{index}{}", "x".repeat(1_000))));
        let keys = plenora_database_core::arrow::array::Int32Array::from(vec![0]);
        let array = plenora_database_core::arrow::array::DictionaryArray::new(
            keys,
            Arc::new(plenora_database_core::arrow::array::StringArray::from(
                values.clone(),
            )),
        );
        writer
            .write(
                &RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).expect("batch"),
            )
            .expect("write");
    }
    let bytes = writer.into_inner().expect("stream");
    let input = scratch("dictionary-deltas.arrows", &bytes);
    // Ogni messaggio e circa 10 KB: con i delta il dizionario arriva a 30 KB,
    // e la terza concatenazione tiene vivi 20 KB e 30 KB.
    let error = open_batches(input.0.to_str().expect("UTF-8"), 60_000)
        .err()
        .expect("delta accumulati oltre il budget");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::ResourceLimit
    );
    let batches = open_batches(input.0.to_str().expect("UTF-8"), 1 << 20)
        .expect("nel budget")
        .batches
        .collect::<Result<Vec<_>, _>>()
        .expect("decodifica");
    assert_eq!(batches.len(), 3);
}
