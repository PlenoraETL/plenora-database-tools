//! Input Arrow IPC comune ai provider.

use crate::{CliResult, File};
use arrow_ipc::reader::FileReader;
use plenora_database_core::arrow::array::RecordBatch;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use std::collections::VecDeque;
use std::sync::Arc;

/// Stream bounded costruito da un file Arrow IPC gia materializzato.
pub(crate) struct IpcFileBatchStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    declared_rows: u64,
}

impl IpcFileBatchStream {
    pub(crate) fn open(path: &str) -> CliResult<Self> {
        let file = File::open(path).map_err(|_| "input Arrow IPC non leggibile")?;
        let reader = FileReader::try_new(file, None).map_err(|_| "input Arrow IPC malformato")?;
        let schema = reader.schema();
        let mut batches = VecDeque::new();
        let mut declared_rows = 0_u64;
        for maybe_batch in reader {
            let batch = maybe_batch.map_err(|_| "batch Arrow non leggibile")?;
            declared_rows = declared_rows
                .checked_add(
                    u64::try_from(batch.num_rows()).map_err(|_| "numero righe Arrow oltre u64")?,
                )
                .ok_or("numero righe Arrow oltre u64")?;
            batches.push_back(batch);
        }
        Ok(Self {
            schema,
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
