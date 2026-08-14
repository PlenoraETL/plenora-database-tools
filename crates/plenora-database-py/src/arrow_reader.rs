//! Arrow batch reader (F4-3).
//!
//! Espone `Provider::read` come iterator Python (sync + async) che
//! ritorna record batch serializzati in formato Arrow IPC stream.
//!
//! Formato di ogni chunk emesso:
//!   - Arrow IPC stream self-contained: schema header + 1 record batch +
//!     EOS marker. Il consumer parsea con
//!     `pyarrow.ipc.open_stream(io.BytesIO(chunk)).read_all()` →
//!     `pa.Table` con 1 batch.
//!
//! Schema viene ripetuto in ogni chunk (~1KB): trascurabile per batch
//! di 1000+ righe, e permette al consumer di processare i batch
//! indipendentemente (buon fit per streaming).

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::redundant_pub_crate,
    clippy::unused_self,          // __repr__ pyclass richiede &self
)]

use crate::errors::to_py_err;
use crate::runtime;
use arrow_ipc::writer::StreamWriter;
use plenora_database_core::plan::{ObjectRef, ReadOperation};
use plenora_database_core::provider::{BatchStream, ParameterBag, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::{CancellationToken, DatabaseError};
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

fn default_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("default budget")
}

fn internal_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: plenora_database_core::ErrorCategory::Internal,
        phase: plenora_database_core::ErrorPhase::Read,
        remote_effect: plenora_database_core::RemoteEffect::None,
        retry: plenora_database_core::RetryDisposition::Never,
        provider: None,
        execution_id: None,
        diagnostics: None,
        message: message.into(),
    }
}

/// Serializza un `RecordBatch` in un buffer Arrow IPC stream
/// self-contained (schema + batch + EOS marker).
fn batch_to_ipc_bytes(
    batch: &plenora_database_core::arrow::RecordBatch,
) -> Result<Vec<u8>, DatabaseError> {
    let schema = batch.schema();
    let mut buf = Vec::with_capacity(1024);
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| internal_error(format!("arrow-ipc writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| internal_error(format!("arrow-ipc writer write: {e}")))?;
        writer
            .finish()
            .map_err(|e| internal_error(format!("arrow-ipc writer finish: {e}")))?;
    }
    Ok(buf)
}

// ============================ Sync BatchReader ============================

/// Iterator sync sopra `BatchStream`. Ritornato da `Session.read(ref, ...)`.
///
/// Implementa il Python iterator protocol (`__iter__` / `__next__`).
/// Ogni `__next__` ritorna `bytes` Arrow IPC stream (con schema).
///
/// Al termine dello stream, ritorna `PyStopIteration` (che Python
/// interpreta come fine dell'iterazione).
#[pyclass(module = "plenora_database._native", unsendable)]
pub struct BatchReader {
    inner: Box<dyn BatchStream>,
}

impl BatchReader {
    pub(crate) fn new(inner: Box<dyn BatchStream>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl BatchReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let cancel = CancellationToken::new();
        let batch_opt = py
            .allow_threads(|| {
                runtime().block_on(async { self.inner.next_batch(&cancel).await })
            })
            .map_err(to_py_err)?;
        let batch = batch_opt.ok_or_else(|| PyStopIteration::new_err(()))?;
        let bytes = batch_to_ipc_bytes(&batch).map_err(to_py_err)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Ritorna lo schema come bytes Arrow IPC (senza record batch,
    /// solo header + EOS marker vuoto). Utile per costruire un
    /// RecordBatchReader Python-side prima di iterare i batch.
    fn schema_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let schema = self.inner.schema();
        let mut buf = Vec::with_capacity(512);
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(|e| {
                PyRuntimeError::new_err(format!("arrow-ipc schema writer: {e}"))
            })?;
            writer.finish().map_err(|e| {
                PyRuntimeError::new_err(format!("arrow-ipc schema finish: {e}"))
            })?;
        }
        Ok(PyBytes::new(py, &buf))
    }

    fn __repr__(&self) -> String {
        "<BatchReader>".to_owned()
    }
}

/// Apre un BatchReader su una tabella/vista Postgres.
///
/// # Errors
///
/// `PlenoraError` se l'apertura dello stream fallisce.
pub(crate) fn open_reader(
    provider: &Arc<PostgresProvider>,
    secret: &SecretString,
    schema: &str,
    object: &str,
    batch_rows: Option<u32>,
) -> Result<BatchReader, DatabaseError> {
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: object.to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        filter: None,
    };
    let cancel = CancellationToken::new();
    let stream = runtime().block_on(async move {
        let _ = batch_rows; // TODO: passare a Provider::read se supportato
        provider
            .read(secret, &operation, &ParameterBag::default(), &default_budget(), &cancel)
            .await
    })?;
    Ok(BatchReader::new(stream))
}

// ============================ Async BatchReader ============================

pub(crate) type SharedStream = Arc<Mutex<Option<Box<dyn BatchStream>>>>;

fn wrap(stream: Box<dyn BatchStream>) -> SharedStream {
    Arc::new(Mutex::new(Some(stream)))
}

/// Iterator async sopra `BatchStream`. Ritornato da
/// `await AsyncSession.aread(ref, ...)`.
///
/// Implementa il Python async iterator protocol (`__aiter__` /
/// `__anext__`). Ogni `__anext__` ritorna un awaitable che si
/// risolve in `bytes` Arrow IPC stream self-contained.
#[pyclass(module = "plenora_database._native")]
pub struct AsyncBatchReader {
    inner: SharedStream,
}

impl AsyncBatchReader {
    pub(crate) fn new(inner: Box<dyn BatchStream>) -> Self {
        Self { inner: wrap(inner) }
    }
}

#[pymethods]
impl AsyncBatchReader {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("AsyncBatchReader esaurito"))?;
            let cancel = CancellationToken::new();
            let batch_opt = stream.next_batch(&cancel).await.map_err(to_py_err)?;
            match batch_opt {
                Some(batch) => {
                    let bytes = batch_to_ipc_bytes(&batch).map_err(to_py_err)?;
                    Python::with_gil(|py| {
                        Ok(PyBytes::new(py, &bytes).into_any().unbind())
                    })
                }
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }

    fn schema_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let guard = inner.lock().await;
            let stream = guard.as_ref().ok_or_else(|| {
                PyRuntimeError::new_err("AsyncBatchReader chiuso")
            })?;
            let schema = stream.schema();
            let mut buf = Vec::with_capacity(512);
            {
                let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(|e| {
                    PyRuntimeError::new_err(format!("arrow-ipc schema writer: {e}"))
                })?;
                writer.finish().map_err(|e| {
                    PyRuntimeError::new_err(format!("arrow-ipc schema finish: {e}"))
                })?;
            }
            Python::with_gil(|py| Ok(PyBytes::new(py, &buf).into_any().unbind()))
        })
    }

    fn __repr__(&self) -> String {
        "<AsyncBatchReader>".to_owned()
    }
}

/// Apre un AsyncBatchReader (async version di `open_reader`).
pub(crate) async fn open_reader_async(
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    schema: String,
    object: String,
    _batch_rows: Option<u32>,
) -> Result<AsyncBatchReader, DatabaseError> {
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(schema),
            object,
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        filter: None,
    };
    let cancel = CancellationToken::new();
    let stream = provider
        .read(&secret, &operation, &ParameterBag::default(), &default_budget(), &cancel)
        .await?;
    Ok(AsyncBatchReader::new(stream))
}
