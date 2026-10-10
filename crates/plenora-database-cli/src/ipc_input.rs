//! Input Arrow IPC comune ai provider: formato file o stream.
//!
//! Il formato si riconosce dai primi byte, non dall'estensione: il file
//! comincia con `ARROW1`, lo stream con il marcatore di continuazione
//! `0xFFFFFFFF`. Qualunque altro inizio — compreso lo stream legacy senza
//! marcatore, precedente ad Arrow 0.15 — e un errore esplicito: provare un
//! lettore dopo l'altro accetterebbe per caso cio che nessuno ha dichiarato.

use crate::{CliResult, File};
use arrow_ipc::reader::{FileReader, StreamReader};
use plenora_database_core::arrow::array::RecordBatch;
use plenora_database_core::arrow::schema::ArrowError;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use std::collections::VecDeque;
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

/// Schema e batch di un input Arrow IPC su disco, nel formato che dichiara.
pub(crate) struct IpcBatches {
    pub(crate) schema: SchemaRef,
    pub(crate) batches: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>,
}

/// Apre `path` come file o stream Arrow IPC, secondo i suoi primi byte.
///
/// Uno stream deve finire con il marcatore di fine: senza, uno stream
/// troncato fra due messaggi si leggerebbe come uno stream piu corto, cioe
/// righe perse senza errore.
///
/// # Errors
///
/// Contenuto non leggibile, formato non riconosciuto, stream senza fine
/// esplicita, schema non decodificabile.
pub(crate) fn open_batches(path: &str) -> CliResult<IpcBatches> {
    let mut file = File::open(path).map_err(|_| "input Arrow IPC non leggibile")?;
    let mut prefix = [0_u8; 8];
    let read = read_prefix(&mut file, &mut prefix)?;
    match detect(&prefix[..read])? {
        IpcFormat::File => {
            file.seek(SeekFrom::Start(0))
                .map_err(|_| "input Arrow IPC non leggibile")?;
            let reader =
                FileReader::try_new(file, None).map_err(|_| "input Arrow IPC malformato")?;
            Ok(IpcBatches {
                schema: reader.schema(),
                batches: Box::new(reader),
            })
        }
        IpcFormat::Stream => {
            let mut tail = [0_u8; 8];
            let ends = file.seek(SeekFrom::End(-8)).is_ok()
                && file.read_exact(&mut tail).is_ok()
                && tail == END_OF_STREAM;
            if !ends {
                return Err(
                    "stream Arrow IPC senza marcatore di fine: troncato o incompleto".into(),
                );
            }
            file.seek(SeekFrom::Start(0))
                .map_err(|_| "input Arrow IPC non leggibile")?;
            let reader = StreamReader::try_new(BufReader::new(file), None)
                .map_err(|_| "input Arrow IPC malformato")?;
            Ok(IpcBatches {
                schema: reader.schema(),
                batches: Box::new(reader),
            })
        }
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

/// Stream bounded costruito da un input Arrow IPC gia materializzato.
pub(crate) struct IpcFileBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    declared_rows: u64,
}

impl IpcFileBatchStream {
    pub(crate) fn open(path: &str) -> CliResult<Self> {
        let input = open_batches(path)?;
        let mut batches = VecDeque::new();
        let mut declared_rows = 0_u64;
        for maybe_batch in input.batches {
            let batch = maybe_batch.map_err(|_| "batch Arrow non leggibile")?;
            declared_rows = declared_rows
                .checked_add(
                    u64::try_from(batch.num_rows()).map_err(|_| "numero righe Arrow oltre u64")?,
                )
                .ok_or("numero righe Arrow oltre u64")?;
            batches.push_back(batch);
        }
        Ok(Self {
            schema: input.schema,
            batches,
            declared_rows,
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
        let next = self.batches.pop_front();
        Box::pin(std::future::ready(Ok(next)))
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.declared_rows)
    }
}

#[cfg(test)]
#[path = "ipc_input_tests.rs"]
mod tests;
