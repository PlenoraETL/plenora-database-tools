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

use crate::{CliResult, File};
use arrow_ipc::reader::{read_footer_length, StreamReader};
use arrow_ipc::{root_as_footer, root_as_message, MessageHeader};
use plenora_database_core::arrow::array::RecordBatch;
use plenora_database_core::arrow::schema::ArrowError;
use plenora_database_core::arrow::SchemaRef;
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
    let (segments, rows) = scan_stream(&mut file, start, end, max_message_bytes)?;
    if format == IpcFormat::File {
        check_footer_blocks(&mut file, length, end, &segments, max_message_bytes)?;
    }
    file.seek(SeekFrom::Start(start))
        .map_err(|_| unreadable())?;
    let verified = VerifiedRange::new(BufReader::new(file), segments);
    let reader = StreamReader::try_new(verified, None).map_err(|_| malformed())?;
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
fn scan_stream(
    file: &mut File,
    start: u64,
    end: u64,
    limit: u64,
) -> CliResult<(Vec<Segment>, u64)> {
    file.seek(SeekFrom::Start(start))
        .map_err(|_| unreadable())?;
    let mut reader = BufReader::new(file);
    let mut position = start;
    let mut rows = 0_u64;
    let mut schema = false;
    let mut segments = Vec::new();
    loop {
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
        let mut hasher = Sha256::new();
        hasher.update(prefix);
        if metadata_length == 0 {
            segments.push(Segment {
                offset: position,
                metadata_length: 0,
                body_length: 0,
                digest: hasher.finalize().into(),
            });
            position += 8;
            break;
        }
        if metadata_length > limit {
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
        if metadata_length
            .checked_add(body_length)
            .is_none_or(|size| size > limit)
        {
            return Err(beyond_limit());
        }
        match message.header_type() {
            MessageHeader::Schema if !schema => schema = true,
            MessageHeader::RecordBatch | MessageHeader::DictionaryBatch if schema => {}
            _ => return Err(malformed()),
        }
        if let Some(batch_rows) = message_rows(&metadata)? {
            rows = rows.checked_add(batch_rows).ok_or_else(beyond_limit)?;
        }
        let after_body = after_metadata
            .checked_add(body_length)
            .ok_or_else(malformed)?;
        if after_body > end {
            return Err(truncated());
        }
        // Il corpo passa per l'impronta a blocchi fissi: la memoria resta
        // quella del buffer, qualunque sia la lunghezza.
        let mut remaining = body_length;
        let mut buffer = vec![0_u8; 64 * 1024];
        while remaining > 0 {
            let chunk =
                usize::try_from(remaining.min(buffer.len() as u64)).map_err(|_| malformed())?;
            reader
                .read_exact(&mut buffer[..chunk])
                .map_err(|_| truncated())?;
            hasher.update(&buffer[..chunk]);
            remaining -= chunk as u64;
        }
        segments.push(Segment {
            offset: position,
            metadata_length,
            body_length,
            digest: hasher.finalize().into(),
        });
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
    Ok((segments, rows))
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
/// che indica altro non si segue.
fn check_footer_blocks(
    file: &mut File,
    length: u64,
    data_end: u64,
    segments: &[Segment],
    limit: u64,
) -> CliResult<()> {
    let footer_length = length - 10 - data_end;
    let mut footer = vec![0_u8; usize::try_from(footer_length).map_err(|_| beyond_limit())?];
    file.seek(SeekFrom::Start(data_end))
        .and_then(|_| file.read_exact(&mut footer))
        .map_err(|_| malformed())?;
    let footer = root_as_footer(&footer).map_err(|_| malformed())?;
    let mut declared = footer
        .dictionaries()
        .into_iter()
        .flatten()
        .chain(footer.recordBatches().into_iter().flatten())
        .map(|block| {
            let offset = u64::try_from(block.offset()).map_err(|_| malformed())?;
            let metadata = u64::try_from(block.metaDataLength()).map_err(|_| malformed())?;
            let body = u64::try_from(block.bodyLength()).map_err(|_| malformed())?;
            if metadata.checked_add(body).is_none_or(|size| size > limit) {
                return Err(beyond_limit());
            }
            Ok((offset, metadata, body))
        })
        .collect::<CliResult<Vec<_>>>()?;
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
        })
        .collect::<Vec<_>>();
    if declared != observed {
        return Err("footer Arrow IPC discordante dallo stream del file".into());
    }
    Ok(())
}

/// Lettore dei soli messaggi validati nella prima passata.
///
/// Per ogni segmento registrato legge il messaggio **intero** — prefisso,
/// metadati e corpo, nella lunghezza registrata, che la prima passata ha
/// gia confrontato con il limite — ne verifica l'impronta e solo dopo lo
/// consegna al decoder da un buffer che nessuno modifica piu. Prima della
/// verifica il decoder non vede un byte: un prefisso alterato (una fine
/// anticipata, una lunghezza diversa) non arriva a governare il framing ne
/// le allocazioni. Dopo l'ultimo segmento rende la fine del file.
struct VerifiedRange<R> {
    inner: R,
    segments: std::collections::VecDeque<Segment>,
    current: Vec<u8>,
    served: usize,
}

impl<R> VerifiedRange<R> {
    fn new(inner: R, segments: Vec<Segment>) -> Self {
        Self {
            inner,
            segments: segments.into(),
            current: Vec::new(),
            served: 0,
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
        let mut message = vec![0_u8; length];
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
        if self.served == self.current.len() && !self.load_next()? {
            return Ok(0);
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
