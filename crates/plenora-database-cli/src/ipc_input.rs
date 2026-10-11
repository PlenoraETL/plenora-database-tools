//! Input Arrow IPC comune ai provider: formato file o stream.
//!
//! Il formato si riconosce dai primi byte, non dall'estensione: il file
//! comincia con `ARROW1`, lo stream con il marcatore di continuazione
//! `0xFFFFFFFF`. Qualunque altro inizio — compreso lo stream legacy senza
//! marcatore, precedente ad Arrow 0.15 — e un errore esplicito: provare un
//! lettore dopo l'altro accetterebbe per caso cio che nessuno ha dichiarato.
//!
//! # Due passate, nessun accumulo
//!
//! La prima passata legge soltanto l'inquadramento: le intestazioni dei
//! messaggi dello stream, o il footer e i blocchi del file. Verifica che ogni
//! messaggio stia sotto il limite **prima** che qualcuno ne allochi il corpo,
//! conta le righe che la sorgente dichiara, e per lo stream pretende il
//! messaggio di fine e nessun byte dopo. La seconda passata decodifica un
//! batch alla volta, sullo stesso file: la memoria e quella del batch
//! corrente, non dell'input.
//!
//! # Il budget
//!
//! Il limite si confronta con tutto cio che la lettura tiene in memoria per
//! colpa dell'input, non con il solo corpo di un messaggio:
//!
//! - ogni messaggio conta intero, prefisso di otto byte compreso (il padding
//!   sta gia nelle lunghezze di metadati e corpo);
//! - il messaggio piu grande conta **due volte**: la copia verificata e la
//!   copia del decoder esistono insieme;
//! - l'indice dei segmenti conta per la capacita allocata, quindi molti
//!   messaggi piccoli o vuoti non lo fanno crescere fuori dal limite;
//! - il footer di un file conta insieme all'indice e ai blocchi che dichiara.
//!
//! Il batch decodificato, una volta consegnato, e del consumatore: lo conta
//! il budget della scrittura.

use crate::{CliResult, File};
use arrow_ipc::reader::{read_footer_length, StreamReader};
use arrow_ipc::{root_as_footer, root_as_message, MessageHeader};
use plenora_database_core::arrow::array::RecordBatch;
use plenora_database_core::arrow::schema::ArrowError;
use plenora_database_core::arrow::{Schema, SchemaRef};
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::DatabaseError;
use sha2::{Digest, Sha256};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

/// I due formati Arrow IPC che la CLI accetta in ingresso.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpcFormat {
    /// `application/vnd.apache.arrow.file`: magic `ARROW1` in testa e in coda.
    File,
    /// `application/vnd.apache.arrow.stream`: messaggi con marcatore di
    /// continuazione, chiusi dal marcatore di fine stream.
    Stream,
}

const FILE_MAGIC: &[u8] = b"ARROW1";
const CONTINUATION: [u8; 4] = [0xFF; 4];
/// Continuazione seguita da lunghezza zero: la fine esplicita dello stream.
#[cfg(test)]
const END_OF_STREAM: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0];

/// Il formato dai primi byte del contenuto.
///
/// # Errors
///
/// Un inizio che non e ne il magic del file ne il marcatore dello stream.
pub(crate) fn detect(prefix: &[u8]) -> CliResult<IpcFormat> {
    if prefix.starts_with(FILE_MAGIC) {
        Ok(IpcFormat::File)
    } else if prefix.starts_with(&CONTINUATION) {
        Ok(IpcFormat::Stream)
    } else {
        Err(
            "input Arrow IPC di formato non riconosciuto: attesi il file (ARROW1) o lo \
             stream con marcatore di continuazione"
                .into(),
        )
    }
}

/// Schema, righe dichiarate e batch di un input Arrow IPC su disco.
pub(crate) struct IpcBatches {
    pub(crate) schema: SchemaRef,
    pub(crate) rows: u64,
    pub(crate) batches: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send>,
}

fn malformed() -> crate::CliError {
    "input Arrow IPC malformato".into()
}

fn beyond_limit() -> crate::CliError {
    DatabaseError::resource_limit("messaggio Arrow IPC oltre il limite di memoria").into()
}

fn truncated() -> crate::CliError {
    "stream Arrow IPC senza marcatore di fine: troncato o incompleto".into()
}

fn foreign_endianness() -> crate::CliError {
    "endianness dello schema Arrow IPC diversa da quella della piattaforma: i valori si \
     leggerebbero con i byte invertiti"
        .into()
}

fn unreadable() -> crate::CliError {
    "input Arrow IPC non leggibile".into()
}

/// Un messaggio dello stream come la prima passata l'ha visto: dove comincia,
/// quanto e lungo (prefisso, metadati e corpo) e l'impronta dei suoi byte.
struct Segment {
    offset: u64,
    metadata_length: u64,
    body_length: u64,
    digest: [u8; 32],
}

impl Segment {
    const fn length(&self) -> u64 {
        8 + self.metadata_length + self.body_length
    }
}

/// La memoria di una voce dell'indice dei segmenti.
const SEGMENT_BYTES: u64 = std::mem::size_of::<Segment>() as u64;

/// La memoria che la lettura tiene per l'input: l'indice con la capacita
/// data, due copie del messaggio piu grande (verificatore e decoder) e la
/// parte residente per tutta la lettura (lo schema, vedi [`Scan::resident`]).
fn footprint(index_capacity: usize, largest: u64, resident: u64) -> Option<u64> {
    u64::try_from(index_capacity)
        .ok()?
        .checked_mul(SEGMENT_BYTES)?
        .checked_add(largest.checked_mul(2)?)?
        .checked_add(resident)
}

/// Memoria di un campo materializzato oltre ai suoi testi: `Field`, il suo
/// `DataType` e i contenitori che li tengono (`Arc`, `Box`, vettori dei
/// figli). Stima per eccesso.
const FIELD_BYTES: u64 = (std::mem::size_of::<plenora_database_core::arrow::Field>()
    + std::mem::size_of::<plenora_database_core::arrow::DataType>()
    + 64) as u64;

/// Uno slot `(String, String)` di una mappa di metadati.
const KEY_VALUE_BYTES: usize = 2 * std::mem::size_of::<String>();

/// Memoria di una voce di metadati materializzata oltre a chiave e valore.
///
/// Arrow 60 (`convert.rs`) raccoglie i metadati di schema e campi in una
/// `HashMap<String, String>` temporanea e la converte in `Metadata`, cioe un
/// `Arc<BTreeMap<String, String>>`: la conversione raccoglie le voci in un
/// vettore, lo ordina e costruisce l'albero, mentre la tabella hash consumata
/// e ancora allocata. Per ogni voce, insieme: la tabella hash (fino a tre
/// slot con il byte di controllo, per il fattore di carico e la capacita a
/// potenze di due), lo slot del vettore, e i nodi dell'albero (fino a tre
/// slot per voce: le foglie della costruzione ordinata sono piene almeno a
/// meta, piu i nodi interni). Stima per eccesso.
const ENTRY_BYTES: u64 = (3 * (KEY_VALUE_BYTES + 1) + KEY_VALUE_BYTES + 3 * KEY_VALUE_BYTES) as u64;

/// Memoria fissa di una mappa di metadati non vuota, anche con una sola
/// voce: la tabella hash minima (quattro slot e il gruppo dei byte di
/// controllo), una foglia del `BTreeMap` (undici slot, il puntatore al
/// padre e le due lunghezze) e l'intestazione dell'`Arc`. Stima per eccesso.
const MAP_BYTES: u64 = (4 * (KEY_VALUE_BYTES + 1) + 16 + 11 * KEY_VALUE_BYTES + 16 + 16) as u64;

/// La memoria dello schema che `try_fb_to_schema` (o il lettore dello
/// stream) materializza da questo flatbuffer: nomi, metadati e campi
/// annidati, misurati **prima** di materializzarli. La ricorsione e limitata
/// dalla profondita che il verificatore del flatbuffer accetta.
fn schema_footprint(schema: arrow_ipc::Schema<'_>) -> Option<u64> {
    let mut total = metadata_footprint(schema.custom_metadata().into_iter().flatten())?;
    for field in schema.fields().into_iter().flatten() {
        total = total.checked_add(field_footprint(field)?)?;
    }
    Some(total)
}

fn field_footprint(field: arrow_ipc::Field<'_>) -> Option<u64> {
    let mut total = FIELD_BYTES
        .checked_add(field.name().map_or(0, str::len) as u64)?
        .checked_add(metadata_footprint(
            field.custom_metadata().into_iter().flatten(),
        )?)?;
    // I soli testi e vettori che il tipo porta con se.
    if let Some(timezone) = field.type_as_timestamp().and_then(|t| t.timezone()) {
        total = total.checked_add(timezone.len() as u64)?;
    }
    if let Some(ids) = field.type_as_union().and_then(|u| u.typeIds()) {
        total = total.checked_add((ids.len() as u64).checked_mul(4)?)?;
    }
    for child in field.children().into_iter().flatten() {
        total = total.checked_add(field_footprint(child)?)?;
    }
    Some(total)
}

fn metadata_footprint<'a>(entries: impl Iterator<Item = arrow_ipc::KeyValue<'a>>) -> Option<u64> {
    let mut entries = entries.peekable();
    // Una mappa vuota non alloca (`Metadata` la rappresenta con `None`).
    let fixed = if entries.peek().is_some() {
        MAP_BYTES
    } else {
        0
    };
    entries.try_fold(fixed, |total, entry| {
        total
            .checked_add(ENTRY_BYTES)?
            .checked_add(entry.key().map_or(0, str::len) as u64)?
            .checked_add(entry.value().map_or(0, str::len) as u64)
    })
}

/// Un campo a dizionario dentro i valori di un altro dizionario.
///
/// # Limite dichiarato
///
/// **Regola**: un input IPC con un dizionario annidato nei valori di un
/// altro dizionario si rifiuta prima del decoder. **Ambito**: ogni campo a
/// dizionario che compare, a qualunque profondita, nel tipo dei valori di
/// un campo a dizionario; i dizionari affiancati o dentro struct e liste
/// restano ammessi. **Hazard**: il lettore di Arrow trattiene le
/// generazioni vecchie del dizionario interno attraverso il dizionario
/// esterno che le contiene, mentre il budget dei dizionari residenti le
/// sottrae alla sostituzione: con l'annidamento la memoria reale supera
/// quella contata. **Rientro**: un conteggio delle dipendenze fra
/// dizionari, o un lettore che non trattenga le generazioni sostituite.
fn nests_a_dictionary(field: arrow_ipc::Field<'_>, inside_dictionary: bool) -> bool {
    let encoded = field.dictionary().is_some();
    if encoded && inside_dictionary {
        return true;
    }
    field
        .children()
        .into_iter()
        .flatten()
        .any(|child| nests_a_dictionary(child, inside_dictionary || encoded))
}

fn nested_dictionary() -> crate::CliError {
    "dizionario Arrow IPC annidato nei valori di un altro dizionario: non supportato".into()
}

/// Memoria di un dizionario residente oltre ai suoi dati: la voce della
/// tabella per id del lettore, l'`ArrayRef` e la struttura dell'array
/// (tipo, buffer, figli). Stima per eccesso.
const DICTIONARY_ENTRY_BYTES: u64 = 512;

/// La memoria che resta viva per tutta la lettura, oltre a indice e
/// messaggi: lo schema (vedi [`schema_resident`]) e i dizionari che il
/// lettore dello stream tiene per id (`dictionaries_by_id` di Arrow). Un
/// dizionario sostituisce il precedente con lo stesso id, un delta si
/// concatena a lui. I dati di un dizionario restano nel corpo del suo
/// messaggio, o nella concatenazione: la sua memoria e al piu la lunghezza
/// dei corpi che lo compongono.
#[derive(Default)]
struct Residency {
    schema: u64,
    sizes: std::collections::HashMap<i64, u64>,
    total: u64,
}

impl Residency {
    /// Schema e dizionari residenti.
    fn resident(&self) -> CliResult<u64> {
        self.schema.checked_add(self.total).ok_or_else(beyond_limit)
    }

    /// Registra un messaggio di dizionario. Rende il picco durante la sua
    /// decodifica: il dizionario precedente con lo stesso id e ancora vivo
    /// mentre nasce il nuovo (sostituzione o concatenazione).
    fn apply(&mut self, id: i64, is_delta: bool, body_length: u64) -> Option<u64> {
        let previous = self.sizes.get(&id).copied();
        let size = match (is_delta, previous) {
            (true, Some(previous)) => previous.checked_add(body_length)?,
            _ => body_length,
        };
        let entry = if previous.is_some() {
            0
        } else {
            DICTIONARY_ENTRY_BYTES
        };
        let peak = self.total.checked_add(size)?.checked_add(entry)?;
        self.total = peak.checked_sub(previous.unwrap_or(0))?;
        self.sizes.insert(id, size);
        Some(peak)
    }

    /// Se `message` e un dizionario, lo registra e controlla nel budget il
    /// picco della sua decodifica, con lo schema, l'indice e le due copie
    /// del messaggio piu grande.
    fn admit_dictionary(
        &mut self,
        message: &arrow_ipc::Message<'_>,
        index_capacity: usize,
        largest: u64,
        limit: u64,
    ) -> CliResult<()> {
        let Some(dictionary) = message.header_as_dictionary_batch() else {
            return Ok(());
        };
        let body_length = u64::try_from(message.bodyLength()).map_err(|_| malformed())?;
        let schema = self.schema;
        if self
            .apply(dictionary.id(), dictionary.isDelta(), body_length)
            .and_then(|peak| peak.checked_add(schema))
            .and_then(|peak| footprint(index_capacity, largest, peak))
            .is_none_or(|bytes| bytes > limit)
        {
            return Err(beyond_limit());
        }
        Ok(())
    }
}

/// Le proprieta IPC che governano la decodifica e che il confronto degli
/// schemi Arrow materializzati non vede: ordine dei byte, feature, e per
/// ogni campo, ricorsivamente, la codifica a dizionario (id, ordinamento,
/// tipo e segno degli indici, tipo di dizionario) e il tipo IPC. Gli id dei
/// dizionari collegano le colonne ai messaggi di dizionario: con id diversi
/// lo stesso file darebbe valori diversi al lettore del file e a quello
/// dello stream (`Field::eq` di Arrow ignora `dict_id` e `dict_is_ordered`).
fn same_ipc_encoding(left: arrow_ipc::Schema<'_>, right: arrow_ipc::Schema<'_>) -> bool {
    left.endianness() == right.endianness()
        && left.features().map(|list| list.iter().collect::<Vec<_>>())
            == right.features().map(|list| list.iter().collect::<Vec<_>>())
        && same_fields(
            left.fields().into_iter().flatten(),
            left.fields().map_or(0, |list| list.len()),
            right.fields().into_iter().flatten(),
            right.fields().map_or(0, |list| list.len()),
        )
}

fn same_fields<'a>(
    left: impl Iterator<Item = arrow_ipc::Field<'a>>,
    left_len: usize,
    right: impl Iterator<Item = arrow_ipc::Field<'a>>,
    right_len: usize,
) -> bool {
    left_len == right_len && left.zip(right).all(|(left, right)| same_field(left, right))
}

fn same_field(left: arrow_ipc::Field<'_>, right: arrow_ipc::Field<'_>) -> bool {
    let dictionary = |field: arrow_ipc::Field<'_>| {
        field.dictionary().map(|encoding| {
            (
                encoding.id(),
                encoding.isOrdered(),
                encoding.dictionaryKind(),
                encoding
                    .indexType()
                    .map(|index| (index.bitWidth(), index.is_signed())),
            )
        })
    };
    left.type_type() == right.type_type()
        && dictionary(left) == dictionary(right)
        && same_fields(
            left.children().into_iter().flatten(),
            left.children().map_or(0, |list| list.len()),
            right.children().into_iter().flatten(),
            right.children().map_or(0, |list| list.len()),
        )
}

/// La prima passata: i segmenti, le righe dichiarate, la memoria residente
/// e i metadati del messaggio di schema.
struct Scan {
    segments: Vec<Segment>,
    rows: u64,
    /// La memoria tenuta per tutta la lettura oltre a indice e messaggi: lo
    /// schema che il lettore dello stream materializza, e i byte del
    /// messaggio di schema conservati per il confronto con il footer. Per lo
    /// stream questi ultimi servono solo fino alla fine della prima passata:
    /// contarli per tutta la lettura e una stima per eccesso.
    resident: u64,
    schema_message: Vec<u8>,
}

/// Registra un segmento se l'indice, con la sua nuova capacita, e il
/// messaggio piu grande stanno nel limite. La capacita cresce in modo
/// esplicito, raddoppiando, e si controlla prima di allocarla.
fn admit(
    segments: &mut Vec<Segment>,
    largest: &mut u64,
    segment: Segment,
    resident: u64,
    limit: u64,
) -> CliResult<()> {
    *largest = (*largest).max(segment.length());
    let capacity = if segments.len() == segments.capacity() {
        segments.capacity().saturating_mul(2).max(4)
    } else {
        segments.capacity()
    };
    if footprint(capacity, *largest, resident).is_none_or(|bytes| bytes > limit) {
        return Err(beyond_limit());
    }
    segments.reserve_exact(capacity - segments.len());
    segments.push(segment);
    Ok(())
}

/// Apre `path` come file o stream Arrow IPC, secondo i suoi primi byte.
///
/// `max_message_bytes` limita ogni messaggio (intestazione e corpo): si
/// controlla sull'inquadramento, prima di allocare.
///
/// Un file Arrow e lo stesso stream fra il magic iniziale e il footer: si
/// legge come tale, e i blocchi del footer devono coincidere con i messaggi
/// dello stream. La seconda passata decodifica i byte attraverso
/// [`VerifiedRange`], che confronta ogni messaggio con l'impronta registrata
/// nella prima: un file cambiato fra le due passate e un errore, non un dato
/// diverso da quello validato.
///
/// # Errors
///
/// Contenuto non leggibile, formato non riconosciuto, stream senza fine
/// esplicita o con byte dopo la fine, messaggio oltre il limite, footer
/// discordante dallo stream, schema non decodificabile.
pub(crate) fn open_batches(path: &str, max_message_bytes: u64) -> CliResult<IpcBatches> {
    let mut file = File::open(path).map_err(|_| unreadable())?;
    let length = file.metadata().map_err(|_| unreadable())?.len();
    let mut prefix = [0_u8; 8];
    let read = read_prefix(&mut file, &mut prefix)?;
    let format = detect(&prefix[..read])?;
    let (start, end) = match format {
        IpcFormat::Stream => (0, length),
        IpcFormat::File => (
            file_data_start(&mut file, length)?,
            file_data_end(&mut file, length, max_message_bytes)?,
        ),
    };
    let scan = scan_stream(&mut file, start, end, max_message_bytes)?;
    let (footer_schema, footer_resident) = if format == IpcFormat::File {
        let (schema, resident) =
            check_footer_blocks(&mut file, length, end, &scan, max_message_bytes)?;
        (Some(schema), resident)
    } else {
        (None, 0)
    };
    let Scan {
        segments,
        rows,
        resident,
        schema_message,
    } = scan;
    drop(schema_message);
    // Lo schema del footer resta vivo mentre il lettore dello stream legge il
    // primo messaggio e materializza il proprio: conta con loro.
    if footprint(segments.capacity(), largest_segment(&segments), resident)
        .and_then(|bytes| bytes.checked_add(footer_resident))
        .is_none_or(|bytes| bytes > max_message_bytes)
    {
        return Err(beyond_limit());
    }
    file.seek(SeekFrom::Start(start))
        .map_err(|_| unreadable())?;
    let verified = VerifiedRange::new(BufReader::new(file), segments);
    let reader = StreamReader::try_new(verified, None).map_err(|_| malformed())?;
    // Lo schema del footer e quello che un lettore di file Arrow userebbe:
    // deve essere lo schema dello stream, metadati compresi, o lo stesso
    // file darebbe colonne diverse a seconda del lettore. Le proprieta IPC
    // che lo schema materializzato non porta le confronta il footer.
    if footer_schema.is_some_and(|footer| footer != *reader.schema()) {
        return Err("schema del footer Arrow IPC discordante dallo stream del file".into());
    }
    Ok(IpcBatches {
        schema: reader.schema(),
        rows,
        batches: Box::new(reader),
    })
}

/// Le righe di un batch dai metadati del suo messaggio, senza il corpo.
fn message_rows(metadata: &[u8]) -> CliResult<Option<u64>> {
    let message = root_as_message(metadata).map_err(|_| malformed())?;
    match message.header_type() {
        MessageHeader::RecordBatch => {
            let batch = message.header_as_record_batch().ok_or_else(malformed)?;
            u64::try_from(batch.length())
                .map(Some)
                .map_err(|_| malformed())
        }
        MessageHeader::Schema | MessageHeader::DictionaryBatch => Ok(None),
        _ => Err(malformed()),
    }
}

/// L'inquadramento dello stream fra `start` e `end`: messaggi entro il
/// limite, la fine esplicita al confine fra due messaggi, nessun byte dopo.
/// Rende i segmenti con l'impronta di ogni messaggio e le righe dichiarate.
fn scan_stream(file: &mut File, start: u64, end: u64, limit: u64) -> CliResult<Scan> {
    file.seek(SeekFrom::Start(start))
        .map_err(|_| unreadable())?;
    let mut reader = BufReader::new(file);
    let mut position = start;
    let mut rows = 0_u64;
    let mut schema = false;
    let mut segments = Vec::new();
    let mut largest = 0_u64;
    let mut resident = 0_u64;
    let mut residency = Residency::default();
    let mut schema_message = Vec::new();
    loop {
        let (prefix, metadata_length) = read_frame_prefix(&mut reader, position, end)?;
        let mut hasher = Sha256::new();
        hasher.update(prefix);
        if metadata_length == 0 {
            let end_of_stream = Segment {
                offset: position,
                metadata_length: 0,
                body_length: 0,
                digest: hasher.finalize().into(),
            };
            admit(&mut segments, &mut largest, end_of_stream, resident, limit)?;
            position += 8;
            break;
        }
        // Prima di allocare i metadati: il loro inquadramento, da solo, deve
        // gia stare nel budget.
        if footprint(
            segments.capacity(),
            largest.max(8 + metadata_length),
            resident,
        )
        .is_none_or(|bytes| bytes > limit)
        {
            return Err(beyond_limit());
        }
        let after_metadata = position
            .checked_add(8)
            .and_then(|value| value.checked_add(metadata_length))
            .ok_or_else(malformed)?;
        if after_metadata > end {
            return Err(truncated());
        }
        let mut metadata =
            vec![0_u8; usize::try_from(metadata_length).map_err(|_| beyond_limit())?];
        reader.read_exact(&mut metadata).map_err(|_| truncated())?;
        hasher.update(&metadata);
        let message = root_as_message(&metadata).map_err(|_| malformed())?;
        let body_length = u64::try_from(message.bodyLength()).map_err(|_| malformed())?;
        let schema_resident = schema_resident(&message, &mut schema, metadata_length)?;
        residency.schema = schema_resident.unwrap_or(residency.schema);
        let length = 8 + metadata_length + body_length;
        residency.admit_dictionary(&message, segments.capacity(), largest.max(length), limit)?;
        resident = residency.resident()?;
        if (8 + metadata_length)
            .checked_add(body_length)
            .and_then(|length| footprint(segments.capacity(), largest.max(length), resident))
            .is_none_or(|bytes| bytes > limit)
        {
            return Err(beyond_limit());
        }
        if let Some(batch_rows) = message_rows(&metadata)? {
            rows = rows.checked_add(batch_rows).ok_or_else(beyond_limit)?;
        }
        if schema_resident.is_some() {
            schema_message = metadata;
        }
        let after_body = after_metadata
            .checked_add(body_length)
            .ok_or_else(malformed)?;
        if after_body > end {
            return Err(truncated());
        }
        hash_body(&mut reader, &mut hasher, body_length)?;
        let segment = Segment {
            offset: position,
            metadata_length,
            body_length,
            digest: hasher.finalize().into(),
        };
        admit(&mut segments, &mut largest, segment, resident, limit)?;
        position = after_body;
    }
    if !schema {
        return Err(malformed());
    }
    if position != end {
        return Err(
            "byte dopo la fine dello stream Arrow IPC: un secondo stream o dati \
                    estranei non si ignorano"
                .into(),
        );
    }
    Ok(Scan {
        segments,
        rows,
        resident,
        schema_message,
    })
}

/// L'ordine dei messaggi (schema per primo, poi batch e dizionari) e, per
/// il messaggio di schema, la memoria che resta viva per tutta la lettura:
/// lo schema materializzato e i byte del messaggio.
fn schema_resident(
    message: &arrow_ipc::Message<'_>,
    schema: &mut bool,
    metadata_length: u64,
) -> CliResult<Option<u64>> {
    match message.header_type() {
        MessageHeader::Schema if !*schema => {
            *schema = true;
            let fields = message.header_as_schema().ok_or_else(malformed)?;
            // Il lettore dello stream di Arrow non confronta l'ordine dei
            // byte: dati di un'altra endianness si leggerebbero invertiti
            // (un Int32 con valore 1 diventerebbe 16777216), senza errore.
            if !fields.endianness().equals_to_target_endianness() {
                return Err(foreign_endianness());
            }
            if fields
                .fields()
                .into_iter()
                .flatten()
                .any(|field| nests_a_dictionary(field, false))
            {
                return Err(nested_dictionary());
            }
            schema_footprint(fields)
                .and_then(|bytes| bytes.checked_add(metadata_length))
                .map(Some)
                .ok_or_else(beyond_limit)
        }
        MessageHeader::RecordBatch | MessageHeader::DictionaryBatch if *schema => Ok(None),
        _ => Err(malformed()),
    }
}

/// Il prefisso di otto byte di un messaggio in `position`: marcatore di
/// continuazione e lunghezza dei metadati.
fn read_frame_prefix(reader: &mut impl Read, position: u64, end: u64) -> CliResult<([u8; 8], u64)> {
    let mut prefix = [0_u8; 8];
    if position.checked_add(8).is_none_or(|next| next > end) {
        return Err(truncated());
    }
    reader.read_exact(&mut prefix).map_err(|_| truncated())?;
    if prefix[..4] != CONTINUATION {
        return Err(malformed());
    }
    let metadata_length = u64::from(u32::from_le_bytes([
        prefix[4], prefix[5], prefix[6], prefix[7],
    ]));
    Ok((prefix, metadata_length))
}

/// Il messaggio piu lungo registrato.
fn largest_segment(segments: &[Segment]) -> u64 {
    segments.iter().map(Segment::length).max().unwrap_or(0)
}

/// Il corpo di un messaggio passa per l'impronta a blocchi fissi: la
/// memoria resta quella del buffer, qualunque sia la lunghezza.
fn hash_body(reader: &mut impl Read, hasher: &mut Sha256, body_length: u64) -> CliResult<()> {
    let mut remaining = body_length;
    let mut buffer = vec![0_u8; 64 * 1024];
    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(buffer.len() as u64)).map_err(|_| malformed())?;
        reader
            .read_exact(&mut buffer[..chunk])
            .map_err(|_| truncated())?;
        hasher.update(&buffer[..chunk]);
        remaining -= chunk as u64;
    }
    Ok(())
}

/// L'inizio dello stream incapsulato in un file Arrow: il magic e seguito
/// da byte nulli fino all'allineamento del writer (8 byte nella specifica,
/// fino a 64 con arrow-rs). Byte non nulli prima del primo messaggio sono un
/// file malformato.
fn file_data_start(file: &mut File, length: u64) -> CliResult<u64> {
    let mut header = [0_u8; 72];
    let available = usize::try_from(length.min(72)).map_err(|_| malformed())?;
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.read_exact(&mut header[..available]))
        .map_err(|_| malformed())?;
    let start = header[FILE_MAGIC.len()..available]
        .iter()
        .position(|byte| *byte != 0)
        .map(|offset| offset + FILE_MAGIC.len())
        .ok_or_else(malformed)?;
    if start % 8 != 0 || start > 64 {
        return Err(malformed());
    }
    u64::try_from(start).map_err(|_| malformed())
}

/// La fine dello stream incapsulato in un file Arrow: dove comincia il
/// footer.
fn file_data_end(file: &mut File, length: u64, limit: u64) -> CliResult<u64> {
    if length < 16 {
        return Err(malformed());
    }
    let mut trailer = [0_u8; 10];
    file.seek(SeekFrom::End(-10))
        .and_then(|_| file.read_exact(&mut trailer))
        .map_err(|_| malformed())?;
    let footer_length = u64::try_from(read_footer_length(trailer).map_err(|_| malformed())?)
        .map_err(|_| malformed())?;
    if footer_length > limit {
        return Err(beyond_limit());
    }
    length
        .checked_sub(10)
        .and_then(|value| value.checked_sub(footer_length))
        .filter(|end| *end >= 8)
        .ok_or_else(malformed)
}

/// I blocchi del footer devono essere esattamente i messaggi di dizionario
/// e di batch dello stream, con le stesse posizioni e lunghezze: un footer
/// che indica altro non si segue. Le proprieta IPC dello schema del footer
/// si confrontano con il messaggio di schema dello stream; rende lo schema
/// del footer materializzato, che il chiamante confronta con quello dello
/// stream, e la sua memoria.
///
/// Mentre il footer si legge restano vivi l'indice, per la sua capacita
/// allocata, e la parte residente della prima passata.
fn check_footer_blocks(
    file: &mut File,
    length: u64,
    data_end: u64,
    scan: &Scan,
    limit: u64,
) -> CliResult<(Schema, u64)> {
    let segments = &scan.segments;
    let footer_length = length - 10 - data_end;
    // Il footer si legge mentre l'indice e la parte residente sono vivi.
    let index_bytes = footprint(segments.capacity(), 0, scan.resident).ok_or_else(beyond_limit)?;
    if index_bytes
        .checked_add(footer_length)
        .is_none_or(|bytes| bytes > limit)
    {
        return Err(beyond_limit());
    }
    let mut footer = vec![0_u8; usize::try_from(footer_length).map_err(|_| beyond_limit())?];
    file.seek(SeekFrom::Start(data_end))
        .and_then(|_| file.read_exact(&mut footer))
        .map_err(|_| malformed())?;
    let footer = root_as_footer(&footer).map_err(|_| malformed())?;
    // I blocchi dichiarati si copiano per ordinarli: anche loro nel budget.
    let blocks = footer.dictionaries().map_or(0, |list| list.len())
        + footer.recordBatches().map_or(0, |list| list.len());
    if u64::try_from(blocks)
        .ok()
        .and_then(|count| count.checked_mul(std::mem::size_of::<(u64, u64, u64)>() as u64))
        .and_then(|bytes| bytes.checked_add(index_bytes))
        .and_then(|bytes| bytes.checked_add(footer_length))
        .is_none_or(|bytes| bytes > limit)
    {
        return Err(beyond_limit());
    }
    // La capacita e quella contata sopra: niente crescita per raddoppio.
    let mut declared = Vec::with_capacity(blocks);
    for block in footer
        .dictionaries()
        .into_iter()
        .flatten()
        .chain(footer.recordBatches().into_iter().flatten())
    {
        let offset = u64::try_from(block.offset()).map_err(|_| malformed())?;
        let metadata = u64::try_from(block.metaDataLength()).map_err(|_| malformed())?;
        let body = u64::try_from(block.bodyLength()).map_err(|_| malformed())?;
        if metadata.checked_add(body).is_none_or(|size| size > limit) {
            return Err(beyond_limit());
        }
        declared.push((offset, metadata, body));
    }
    declared.sort_unstable();
    // Nello stream: schema per primo, fine per ultima; i blocchi del footer
    // sono i messaggi in mezzo. `metaDataLength` include il prefisso di otto
    // byte.
    let observed = segments
        .get(1..segments.len().saturating_sub(1))
        .ok_or_else(malformed)?
        .iter()
        .map(|segment| {
            (
                segment.offset,
                segment.metadata_length + 8,
                segment.body_length,
            )
        });
    if !declared.iter().copied().eq(observed) {
        return Err("footer Arrow IPC discordante dallo stream del file".into());
    }
    drop(declared);
    let footer_schema = footer.schema().ok_or_else(malformed)?;
    let stream_schema = root_as_message(&scan.schema_message)
        .ok()
        .and_then(|message| message.header_as_schema())
        .ok_or_else(malformed)?;
    if !same_ipc_encoding(footer_schema, stream_schema) {
        return Err("schema del footer Arrow IPC discordante dallo stream del file".into());
    }
    // Lo schema del footer si materializza mentre il footer e vivo.
    let footer_resident = schema_footprint(footer_schema).ok_or_else(beyond_limit)?;
    if index_bytes
        .checked_add(footer_length)
        .and_then(|bytes| bytes.checked_add(footer_resident))
        .is_none_or(|bytes| bytes > limit)
    {
        return Err(beyond_limit());
    }
    let schema = arrow_ipc::convert::try_fb_to_schema(footer_schema).map_err(|_| malformed())?;
    Ok((schema, footer_resident))
}

/// Lettore dei soli messaggi validati nella prima passata.
///
/// Per ogni segmento registrato legge il messaggio **intero** — prefisso,
/// metadati e corpo, nella lunghezza registrata, che la prima passata ha
/// gia confrontato con il limite — ne verifica l'impronta e solo dopo lo
/// consegna al decoder da un buffer che nessuno modifica piu. Il messaggio
/// precedente si libera prima di allocare il successivo: il verificatore
/// tiene un messaggio alla volta. Prima della
/// verifica il decoder non vede un byte: un prefisso alterato (una fine
/// anticipata, una lunghezza diversa) non arriva a governare il framing ne
/// le allocazioni. Dopo l'ultimo segmento rende la fine del file.
struct VerifiedRange<R> {
    inner: R,
    segments: std::collections::VecDeque<Segment>,
    current: Vec<u8>,
    served: usize,
    /// I byte di messaggio tenuti insieme nel momento di massima occupazione.
    peak: usize,
    /// Un messaggio non e stato verificato: da qui ogni lettura fallisce,
    /// invece di riprendere dal segmento successivo.
    failed: bool,
}

impl<R> VerifiedRange<R> {
    fn new(inner: R, segments: Vec<Segment>) -> Self {
        Self {
            inner,
            segments: segments.into(),
            current: Vec::new(),
            served: 0,
            peak: 0,
            failed: false,
        }
    }
}

fn changed() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "input Arrow IPC cambiato dopo la validazione",
    )
}

impl<R: Read> VerifiedRange<R> {
    /// Carica e verifica il prossimo segmento; `false` dopo l'ultimo.
    fn load_next(&mut self) -> std::io::Result<bool> {
        let Some(segment) = self.segments.pop_front() else {
            return Ok(false);
        };
        let length = usize::try_from(segment.length()).map_err(|_| changed())?;
        self.current = Vec::new();
        self.served = 0;
        let mut message = vec![0_u8; length];
        self.peak = self.peak.max(self.current.capacity() + message.capacity());
        self.inner.read_exact(&mut message).map_err(|_| changed())?;
        let digest: [u8; 32] = Sha256::digest(&message).into();
        if digest != segment.digest {
            return Err(changed());
        }
        self.current = message;
        self.served = 0;
        Ok(true)
    }
}

impl<R: Read> Read for VerifiedRange<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.failed {
            return Err(changed());
        }
        if self.served == self.current.len() {
            match self.load_next() {
                Ok(true) => {}
                Ok(false) => return Ok(0),
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            }
        }
        let available = &self.current[self.served..];
        let count = available.len().min(buffer.len());
        buffer[..count].copy_from_slice(&available[..count]);
        self.served += count;
        Ok(count)
    }
}

/// Legge fino a `buffer.len()` byte, fermandosi solo alla fine del file.
fn read_prefix(file: &mut File, buffer: &mut [u8]) -> CliResult<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return Err("input Arrow IPC non leggibile".into()),
        }
    }
    Ok(filled)
}

/// Stream lazy su un input Arrow IPC: un batch alla volta, letto quando il
/// provider lo chiede, dopo che l'inquadramento e stato verificato.
pub(crate) struct IpcFileBatchStream {
    schema: SchemaRef,
    batches: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send>,
    declared_rows: u64,
}

impl IpcFileBatchStream {
    pub(crate) fn open(path: &str) -> CliResult<Self> {
        let limit = plenora_database_core::resource::ResourceLimits::default().memory_bytes;
        let input = open_batches(path, limit)?;
        Ok(Self {
            schema: input.schema,
            batches: input.batches,
            declared_rows: input.rows,
        })
    }
}

impl BatchStream for IpcFileBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a plenora_database_core::CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        let next = self.batches.next().transpose().map_err(|_| {
            DatabaseError::new(
                plenora_database_core::ErrorCategory::DataMapping,
                plenora_database_core::ErrorPhase::Read,
                None,
                "batch Arrow non leggibile",
            )
        });
        Box::pin(std::future::ready(next))
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.declared_rows)
    }
}

#[cfg(test)]
#[path = "ipc_input_tests.rs"]
mod tests;
