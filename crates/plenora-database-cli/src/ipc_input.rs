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
use arrow_ipc::reader::{read_footer_length, FileReader, StreamReader};
use arrow_ipc::{root_as_footer, root_as_message, MessageHeader};
use plenora_database_core::arrow::array::RecordBatch;
use plenora_database_core::arrow::schema::ArrowError;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::DatabaseError;
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

/// Apre `path` come file o stream Arrow IPC, secondo i suoi primi byte.
///
/// `max_message_bytes` limita ogni messaggio (intestazione e corpo): si
/// controlla sull'inquadramento, prima di allocare.
///
/// # Errors
///
/// Contenuto non leggibile, formato non riconosciuto, stream senza fine
/// esplicita o con byte dopo la fine, messaggio oltre il limite, schema non
/// decodificabile.
pub(crate) fn open_batches(path: &str, max_message_bytes: u64) -> CliResult<IpcBatches> {
    let mut file = File::open(path).map_err(|_| "input Arrow IPC non leggibile")?;
    let length = file
        .metadata()
        .map_err(|_| "input Arrow IPC non leggibile")?
        .len();
    let mut prefix = [0_u8; 8];
    let read = read_prefix(&mut file, &mut prefix)?;
    let format = detect(&prefix[..read])?;
    let rows = match format {
        IpcFormat::File => scan_file(&mut file, length, max_message_bytes)?,
        IpcFormat::Stream => scan_stream(&mut file, length, max_message_bytes)?,
    };
    file.seek(SeekFrom::Start(0))
        .map_err(|_| "input Arrow IPC non leggibile")?;
    match format {
        IpcFormat::File => {
            let reader = FileReader::try_new(file, None).map_err(|_| malformed())?;
            Ok(IpcBatches {
                schema: reader.schema(),
                rows,
                batches: Box::new(reader),
            })
        }
        IpcFormat::Stream => {
            let reader =
                StreamReader::try_new(BufReader::new(file), None).map_err(|_| malformed())?;
            Ok(IpcBatches {
                schema: reader.schema(),
                rows,
                batches: Box::new(reader),
            })
        }
    }
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

/// L'inquadramento dello stream: messaggi entro il limite, la fine esplicita,
/// nessun byte dopo.
///
/// La fine e il messaggio di lunghezza zero letto **al confine fra due
/// messaggi**: otto byte uguali al marcatore dentro un corpo non lo sono, e
/// uno stream che si chiude senza di esso e troncato.
fn scan_stream(file: &mut File, length: u64, limit: u64) -> CliResult<u64> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| "input Arrow IPC non leggibile")?;
    let mut reader = BufReader::new(file);
    let mut position = 0_u64;
    let mut rows = 0_u64;
    let mut schema = false;
    let truncated = || -> crate::CliError {
        "stream Arrow IPC senza marcatore di fine: troncato o incompleto".into()
    };
    loop {
        let mut word = [0_u8; 4];
        reader.read_exact(&mut word).map_err(|_| truncated())?;
        if word != CONTINUATION {
            return Err(malformed());
        }
        reader.read_exact(&mut word).map_err(|_| truncated())?;
        position += 8;
        let metadata_length = u64::from(u32::from_le_bytes(word));
        if metadata_length == 0 {
            break;
        }
        if metadata_length > limit {
            return Err(beyond_limit());
        }
        if position + metadata_length > length {
            return Err(truncated());
        }
        let mut metadata =
            vec![0_u8; usize::try_from(metadata_length).map_err(|_| beyond_limit())?];
        reader.read_exact(&mut metadata).map_err(|_| truncated())?;
        position += metadata_length;
        let message = root_as_message(&metadata).map_err(|_| malformed())?;
        let body = u64::try_from(message.bodyLength()).map_err(|_| malformed())?;
        if metadata_length.saturating_add(body) > limit {
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
        if position + body > length {
            return Err(truncated());
        }
        reader
            .seek_relative(i64::try_from(body).map_err(|_| malformed())?)
            .map_err(|_| truncated())?;
        position += body;
    }
    if !schema {
        return Err(malformed());
    }
    if position != length {
        return Err(
            "byte dopo la fine dello stream Arrow IPC: un secondo stream o dati \
                    estranei non si ignorano"
                .into(),
        );
    }
    Ok(rows)
}

/// L'inquadramento del file: footer, blocchi dentro il file e sotto il
/// limite, righe dai metadati dei batch.
fn scan_file(file: &mut File, length: u64, limit: u64) -> CliResult<u64> {
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
    let data_end = length
        .checked_sub(10 + footer_length)
        .ok_or_else(malformed)?;
    let mut footer = vec![0_u8; usize::try_from(footer_length).map_err(|_| beyond_limit())?];
    file.seek(SeekFrom::Start(data_end))
        .and_then(|_| file.read_exact(&mut footer))
        .map_err(|_| malformed())?;
    let footer = root_as_footer(&footer).map_err(|_| malformed())?;
    let mut rows = 0_u64;
    let dictionaries = footer.dictionaries().into_iter().flatten();
    let batches = footer.recordBatches().into_iter().flatten();
    for (block, is_batch) in dictionaries
        .map(|block| (block, false))
        .chain(batches.map(|block| (block, true)))
    {
        let offset = u64::try_from(block.offset()).map_err(|_| malformed())?;
        let metadata_length = u64::try_from(block.metaDataLength()).map_err(|_| malformed())?;
        let body = u64::try_from(block.bodyLength()).map_err(|_| malformed())?;
        let size = metadata_length.checked_add(body).ok_or_else(malformed)?;
        if size > limit {
            return Err(beyond_limit());
        }
        if offset.checked_add(size).is_none_or(|end| end > data_end) {
            return Err(malformed());
        }
        if !is_batch {
            continue;
        }
        let mut metadata =
            vec![0_u8; usize::try_from(metadata_length).map_err(|_| beyond_limit())?];
        file.seek(SeekFrom::Start(offset))
            .and_then(|_| file.read_exact(&mut metadata))
            .map_err(|_| malformed())?;
        // Il blocco comincia con la continuazione e la lunghezza, oppure, nel
        // formato precedente ad Arrow 0.15, con la sola lunghezza.
        let skip = if metadata.starts_with(&CONTINUATION) {
            8
        } else {
            4
        };
        let message = metadata.get(skip..).ok_or_else(malformed)?;
        let batch_rows = message_rows(message)?.ok_or_else(malformed)?;
        rows = rows.checked_add(batch_rows).ok_or_else(beyond_limit)?;
    }
    Ok(rows)
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
