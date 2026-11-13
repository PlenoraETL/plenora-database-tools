//! Lettore streaming di batch Arrow esposto a Python.
//!
//! Espone `Provider::read` come iterator Python (sync + async) che
//! ritorna record batch serializzati in formato Arrow IPC stream.
//!
//! Formato di ogni chunk emesso:
//!   - Arrow IPC stream self-contained: schema header + 1 record batch +
//!     EOS marker. Il consumer parsea con
//!     `pyarrow.ipc.open_stream(io.BytesIO(chunk)).read_all()` â†’
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
use plenora_database_core::plan::{ObjectRef, OrderBy, ReadOperation, SortDirection};
use plenora_database_core::provider::{BatchStream, ParameterBag, Provider, SecretString};
use plenora_database_core::{CancellationToken, DatabaseError, ReadCheckpoint};
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

// Re-export interno perche `family_arrow_reader` continua ad accedere via
// `crate::arrow_reader::default_budget`.
pub(crate) use crate::budget::session_budget as default_budget;

fn internal_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        read_loss: None,
        category: plenora_database_core::ErrorCategory::Internal,
        phase: plenora_database_core::ErrorPhase::Read,
        remote_effect: plenora_database_core::RemoteEffect::None,
        retry: plenora_database_core::RetryDisposition::Never,
        provider: None,
        execution_id: None,
        loss_report: None,
        diagnostics: None,
        message: message.into(),
    }
}

/// Serializza un `RecordBatch` in un buffer Arrow IPC stream
/// self-contained (schema + batch + EOS marker).
pub(crate) fn batch_to_ipc_bytes(
    batch: &plenora_database_core::arrow::RecordBatch,
) -> Result<Vec<u8>, DatabaseError> {
    let schema = batch.schema();
    let mut buf = Vec::with_capacity(1024);
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|_| internal_error("arrow-ipc writer init"))?;
        writer
            .write(batch)
            .map_err(|_| internal_error("arrow-ipc writer write"))?;
        writer
            .finish()
            .map_err(|_| internal_error("arrow-ipc writer finish"))?;
    }
    Ok(buf)
}

pub(crate) fn schema_to_ipc_bytes(
    schema: &plenora_database_core::arrow::Schema,
) -> Result<Vec<u8>, DatabaseError> {
    let mut buf = Vec::with_capacity(512);
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|_| internal_error("arrow-ipc schema writer"))?;
    writer
        .finish()
        .map_err(|_| internal_error("arrow-ipc schema finish"))?;
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
///
/// Lo stream sta dietro un `Mutex`: il reader si puo consumare da un thread
/// diverso da quello che l'ha aperto, come fa `acopy_from` per non bloccare
/// il loop asyncio.
#[pyclass(module = "plenora_database._native")]
pub struct BatchReader {
    reader: crate::stream_reader::Reader,
}

impl BatchReader {
    pub(crate) fn new(inner: Box<dyn BatchStream>, cancellation: CancellationToken) -> Self {
        Self {
            reader: crate::stream_reader::Reader::new(inner, cancellation),
        }
    }
}
impl Drop for BatchReader {
    fn drop(&mut self) {
        self.reader.close();
    }
}

fn reader_poisoned() -> PyErr {
    to_py_err(internal_error("stato del reader Arrow non disponibile"))
}

#[pymethods]
impl BatchReader {
    fn close(&self, py: Python<'_>) {
        self.reader.close();
        py.detach(|| runtime().block_on(self.reader.released()));
    }

    fn _record_arrow_failure(&self, py: Python<'_>) -> Py<PyAny> {
        to_py_err(self.reader.mapping_failure())
            .value(py)
            .clone()
            .into_any()
            .unbind()
    }

    fn _record_arrow_cancellation(&self, py: Python<'_>) -> Py<PyAny> {
        to_py_err(self.reader.interruption())
            .value(py)
            .clone()
            .into_any()
            .unbind()
    }

    #[getter]
    fn stream_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        self.reader.state.snapshot(py)
    }

    #[getter]
    fn read_loss<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.reader
            .state
            .snapshot(py)?
            .get_item("read_loss")?
            .ok_or_else(reader_poisoned)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = py
            .detach(|| runtime().block_on(self.reader.next()))
            .map_err(to_py_err)?;
        let bytes = bytes.ok_or_else(|| PyStopIteration::new_err(()))?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Ritorna lo schema come bytes Arrow IPC (senza record batch,
    /// solo header + EOS marker vuoto). Utile per costruire un
    /// RecordBatchReader Python-side prima di iterare i batch.
    fn schema_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let buf = self.reader.schema_bytes().map_err(to_py_err)?;
        Ok(PyBytes::new(py, &buf))
    }

    /// Arrow PyCapsule Interface: lo stream C dei batch ancora da leggere.
    ///
    /// Qualunque consumatore di `__arrow_c_stream__` â€” `pyarrow`,
    /// `plenora_data.run`, polars, DuckDB â€” legge il reader direttamente,
    /// senza passare dai `bytes`. Il lettore resta lazy: ogni batch si legge
    /// dal database quando il consumatore lo chiede. Consuma il reader come
    /// l'iterazione.
    ///
    /// Ogni batch attraversa ancora l'IPC (una copia): lo zero-copy dalla
    /// memoria Rust richiederebbe la C Data Interface lato Rust, che il
    /// binding non porta.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        slf: Bound<'py, Self>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        py.import("plenora_database._arrow_io")?
            .call_method1("c_stream_from_reader", (slf, requested_schema))
    }

    /// I batch ancora da leggere come un'unica `pyarrow.Table`.
    fn read_all(slf: Bound<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let py = slf.py();
        py.import("plenora_database._arrow_io")?
            .call_method1("table_from_reader", (slf,))
    }

    fn __repr__(&self) -> String {
        "<BatchReader>".to_owned()
    }
}

/// Costruisce un `ReadOperation` da parametri Python.
///
/// - `projection`: lista di nomi di colonna da leggere (vuota = tutte)
/// - `order_by`: lista di `(nome, "asc"|"desc")` per l'ORDER BY
/// - `limit`: numero massimo di righe (None = nessun limite)
///
/// # Errors
///
/// `PlenoraError::invalid_plan` se la direzione ORDER BY non Ã¨
/// `"asc"` o `"desc"`.
pub(crate) fn make_qualified_read_operation(
    catalog: Option<&str>,
    schema: &str,
    object: &str,
    projection: Vec<String>,
    order_by: Vec<(String, String)>,
    limit: Option<u64>,
) -> Result<ReadOperation, DatabaseError> {
    let order_by_parsed: Vec<OrderBy> = order_by
        .into_iter()
        .map(|(field, dir)| {
            let direction = match dir.as_str() {
                "asc" => SortDirection::Asc,
                "desc" => SortDirection::Desc,
                _ => {
                    return Err(DatabaseError::invalid_plan(
                        "order_by direzione sconosciuta: attesi 'asc' o 'desc'",
                    ));
                }
            };
            Ok(OrderBy { field, direction })
        })
        .collect::<Result<Vec<_>, DatabaseError>>()?;
    Ok(ReadOperation {
        source: ObjectRef {
            catalog: catalog.map(str::to_owned),
            schema: Some(schema.to_owned()),
            object: object.to_owned(),
        },
        projection,
        order_by: order_by_parsed,
        row_limit: limit,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    })
}

pub(crate) fn prepare_resumable_read(
    provider: plenora_database_core::plan::ProviderKind,
    resumable: bool,
    operation: ReadOperation,
    checkpoint: Option<&ReadCheckpoint>,
) -> Result<(ReadOperation, ParameterBag), DatabaseError> {
    let parameters = ParameterBag::default();
    match checkpoint {
        None => Ok((operation, parameters)),
        Some(_) if !resumable => Err(DatabaseError::unsupported(
            provider,
            plenora_database_core::ErrorPhase::Read,
            "lettura riprendibile non qualificata per questo provider",
        )),
        Some(checkpoint) => checkpoint.resume(provider, &operation, &parameters),
    }
}

/// Apre un BatchReader su una tabella/vista Postgres.
///
/// La size dei batch prodotti dallo stream Ã¨ decisa internamente dal
/// provider (per Postgres: bounded dal buffer del cursore server-side).
///
/// # Errors
///
/// `PlenoraError` se l'apertura dello stream fallisce.
pub(crate) fn open_reader(
    provider: &Arc<PostgresProvider>,
    secret: &SecretString,
    operation: ReadOperation,
    parameters: ParameterBag,
    cancellation: CancellationToken,
) -> Result<BatchReader, DatabaseError> {
    let cancellation = cancellation.child_token();
    let stream_cancellation = cancellation.clone();
    let stream = runtime().block_on(async move {
        provider
            .read(
                secret,
                &operation,
                &parameters,
                &default_budget(),
                &stream_cancellation,
            )
            .await
    })?;
    Ok(BatchReader::new(stream, cancellation))
}

// ============================ Async BatchReader ============================

/// Iterator async sopra `BatchStream`. Ritornato da
/// `await AsyncSession.aread(ref, ...)`.
///
/// Implementa il Python async iterator protocol (`__aiter__` /
/// `__anext__`). Ogni `__anext__` ritorna un awaitable che si
/// risolve in `bytes` Arrow IPC stream self-contained.
#[pyclass(module = "plenora_database._native")]
pub struct AsyncBatchReader {
    reader: crate::stream_reader::Reader,
}

impl AsyncBatchReader {
    pub(crate) fn new(inner: Box<dyn BatchStream>, cancellation: CancellationToken) -> Self {
        Self {
            reader: crate::stream_reader::Reader::new(inner, cancellation),
        }
    }
}
impl Drop for AsyncBatchReader {
    fn drop(&mut self) {
        self.reader.close();
    }
}

#[pymethods]
impl AsyncBatchReader {
    fn close(&self) {
        self.reader.close();
    }

    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.reader.close();
        let reader = self.reader.clone();
        future_into_py(py, async move {
            reader.released().await;
            Ok(())
        })
    }

    fn _record_arrow_failure(&self, py: Python<'_>) -> Py<PyAny> {
        to_py_err(self.reader.mapping_failure())
            .value(py)
            .clone()
            .into_any()
            .unbind()
    }

    fn _record_arrow_cancellation(&self, py: Python<'_>) -> Py<PyAny> {
        to_py_err(self.reader.interruption())
            .value(py)
            .clone()
            .into_any()
            .unbind()
    }

    #[getter]
    fn stream_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        self.reader.state.snapshot(py)
    }

    #[getter]
    fn read_loss<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.reader
            .state
            .snapshot(py)?
            .get_item("read_loss")?
            .ok_or_else(reader_poisoned)
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut flight = self.reader.flight();
        future_into_py(py, async move {
            let result = flight.reader.next().await;
            flight.finish();
            let bytes = result
                .map_err(to_py_err)?
                .ok_or_else(|| PyStopAsyncIteration::new_err(()))?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    fn schema_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut flight = self.reader.flight();
        future_into_py(py, async move {
            let result = flight.reader.schema_bytes();
            flight.finish();
            let bytes = result.map_err(to_py_err)?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
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
    operation: ReadOperation,
    parameters: ParameterBag,
    cancellation: CancellationToken,
) -> Result<AsyncBatchReader, DatabaseError> {
    let cancellation = cancellation.child_token();
    let stream = provider
        .read(
            &secret,
            &operation,
            &parameters,
            &default_budget(),
            &cancellation,
        )
        .await?;
    Ok(AsyncBatchReader::new(stream, cancellation))
}
