//! AsyncSession + aconnect (F3-7).
//!
//! Bridge asyncio ↔ tokio via `pyo3-async-runtimes`. Le API espongono
//! metodi che ritornano awaitable Python: `await s.execute_scalar(...)`.
//!
//! Ogni metodo apre una transazione auto-commit dedicata. Le transazioni
//! gestite dal chiamante usano `AsyncTransaction`.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]

use crate::arrow_reader::open_reader_async;
use crate::errors::to_py_err;
use crate::py_convert::{portable_from_json, statement_from_python};
use plenora_database_core::plan::Operation;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Sessione Postgres asincrona. Ottenuta da `await aconnect(dsn)`.
///
/// I metodi ritornano awaitable Python; ogni operazione apre una
/// transazione dedicata e la committa (auto-commit). Per transazioni
/// user-managed vedi `AsyncSession.begin()` → `AsyncTransaction`.
#[pyclass(module = "plenora_database._native")]
#[allow(dead_code)] // provider + secret consumati dagli awaitable
pub struct AsyncSession {
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    capabilities: plenora_database_core::capabilities::ProviderCapabilities,
    server_version: String,
    postgis_version: Option<String>,
    closed: bool,
}

impl AsyncSession {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(
                "sessione chiusa: aprine una nuova con plenora_database.aconnect(...)",
            ));
        }
        Ok(())
    }

    fn provider(&self) -> Arc<dyn Provider> {
        Arc::clone(&self.provider) as Arc<dyn Provider>
    }
}

#[pymethods]
impl AsyncSession {
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
    }

    #[getter]
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let value = serde_json::to_value(&self.capabilities)
            .map_err(|_| PyRuntimeError::new_err("capability non serializzabili"))?;
        crate::session::json_value_to_pydict(py, &value)
    }

    #[getter]
    fn postgis_version(&self) -> Option<&str> {
        self.postgis_version.as_deref()
    }

    #[getter]
    fn is_closed(&self) -> bool {
        self.closed
    }

    fn close(&mut self) {
        self.closed = true;
    }

    /// Async context manager entry (Python `__aenter__`). Ritorna un
    /// awaitable che si risolve in `self`.
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj = slf.into_pyobject(py)?.into_any().unbind();
        future_into_py(py, async move { Ok(obj) })
    }

    /// Async context manager exit. Chiude la sessione. Non sopprime eccezioni.
    fn __aexit__(
        slf: Py<Self>,
        py: Python<'_>,
        _exc_type: Py<PyAny>,
        _exc_value: Py<PyAny>,
        _traceback: Py<PyAny>,
    ) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            Python::attach(|py| {
                let mut guard = slf.borrow_mut(py);
                guard.closed = true;
            });
            Ok(false)
        })
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let statement = statement_from_python(sql, params.as_ref())?;
        crate::async_session_ops::execute(py, self.provider(), self.secret.clone(), statement)
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let statement = statement_from_python(sql, params.as_ref())?;
        crate::async_session_ops::execute_scalar(
            py,
            self.provider(),
            self.secret.clone(),
            statement,
        )
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let statement = statement_from_python(sql, params.as_ref())?;
        crate::async_session_ops::execute_rows(py, self.provider(), self.secret.clone(), statement)
    }

    fn execute_ddl<'py>(&self, py: Python<'py>, sql: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::execute_ddl(
            py,
            self.provider(),
            self.secret.clone(),
            sql.to_owned(),
        )
    }

    fn inspect_catalogs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_strings(
            py,
            self.provider(),
            self.secret.clone(),
            Operation::DatabaseListCatalogs,
            "catalogs",
        )
    }

    fn inspect_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_strings(
            py,
            self.provider(),
            self.secret.clone(),
            Operation::DatabaseListSchemas { source: None },
            "schemas",
        )
    }

    fn inspect_tables<'py>(&self, py: Python<'py>, schema: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_objects(
            py,
            self.provider(),
            self.secret.clone(),
            schema.to_owned(),
        )
    }

    fn inspect_describe<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
        object: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_describe(
            py,
            self.provider(),
            self.secret.clone(),
            schema.to_owned(),
            object.to_owned(),
        )
    }

    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast = portable_from_json(ast_json)?;
        crate::async_session_ops::execute_portable_rows(
            py,
            self.provider(),
            self.secret.clone(),
            ast,
        )
    }

    /// Apre uno stream async di record batch Arrow. Ritorna un
    /// awaitable che si risolve in `AsyncBatchReader`, che implementa
    /// il Python async iterator protocol.
    ///
    /// Uso tipico:
    ///
    ///     import io, pyarrow.ipc as ipc
    ///     reader = await s.aread("public.large_table")
    ///     async for chunk in reader:
    ///         batch = ipc.open_stream(io.BytesIO(chunk)).read_all()
    ///
    /// Non carica tutto in memoria; legge batch-by-batch dal cursor
    /// server-side sul runtime tokio (non blocca l'event loop).
    #[pyo3(signature = (schema, object, projection=None, order_by=None, limit=None))]
    fn aread<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
        object: &str,
        projection: Option<Vec<String>>,
        order_by: Option<Vec<(String, String)>>,
        limit: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let schema = schema.to_owned();
        let object = object.to_owned();
        let projection = projection.unwrap_or_default();
        let order_by = order_by.unwrap_or_default();
        future_into_py(py, async move {
            let reader = open_reader_async(
                provider, secret, schema, object, projection, order_by, limit,
            )
            .await
            .map_err(to_py_err)?;
            Python::attach(|py| {
                let obj = Py::new(py, reader)?;
                Ok(obj.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Bulk write async — analogo di `Session.copy_from`. Ritorna un
    /// awaitable che si risolve in dict `WriteOutcome`.
    #[pyo3(signature = (
        schema,
        table,
        ipc_bytes,
        mode="append",
        transaction_profile="single_transaction",
        mapping_policy="compatible",
        keys=None,
        update_columns=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn acopy_from<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
        table: &str,
        ipc_bytes: &[u8],
        mode: &str,
        transaction_profile: &str,
        mapping_policy: &str,
        keys: Option<Vec<String>>,
        update_columns: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let schema_owned = schema.to_owned();
        let table_owned = table.to_owned();
        let ipc_owned = ipc_bytes.to_vec();
        let mode_owned = mode.to_owned();
        let profile_owned = transaction_profile.to_owned();
        let policy_owned = mapping_policy.to_owned();
        let keys_owned = keys.unwrap_or_default();
        let update_columns_owned = update_columns.unwrap_or_default();
        future_into_py(py, async move {
            let outcome = crate::write::copy_from_async(
                provider,
                secret,
                schema_owned,
                table_owned,
                ipc_owned,
                mode_owned,
                profile_owned,
                policy_owned,
                keys_owned,
                update_columns_owned,
            )
            .await
            .map_err(crate::errors::to_py_err)?;
            Python::attach(|py| {
                let d = crate::write::outcome_into_py(py, &outcome)?;
                Ok(d.into_any().unbind())
            })
        })
    }

    /// Apre una nuova transazione async user-managed. Ritorna un
    /// awaitable che si risolve in `AsyncTransaction`.
    ///
    /// Uso:
    ///     async with await s.begin(isolation="serializable") as tx:
    ///         await tx.execute(...)
    #[pyo3(signature = (
        isolation=None,
        read_only=None,
        deferrable=None,
        statement_timeout_ms=None,
        context=None,
        native_query_policy=None,
    ))]
    #[allow(clippy::too_many_arguments)] // API PyO3 keyword — non fattibile compressione
    fn begin<'py>(
        &self,
        py: Python<'py>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
        statement_timeout_ms: Option<u64>,
        context: Option<crate::session_context_py::PySessionContext>,
        native_query_policy: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let opts = crate::session_tx::transaction_options(
            isolation,
            read_only,
            deferrable,
            statement_timeout_ms,
            context,
            native_query_policy,
        )?;
        crate::async_session_ops::begin(py, self.provider(), self.secret.clone(), opts)
    }

    fn execute_portable_count<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast = portable_from_json(ast_json)?;
        crate::async_session_ops::execute_portable_count(
            py,
            self.provider(),
            self.secret.clone(),
            ast,
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "<AsyncSession server_version={:?} postgis_version={:?} closed={}>",
            self.server_version, self.postgis_version, self.closed
        )
    }
}

/// Apre una sessione Postgres asincrona. La DSN è nel formato libpq.
/// Ritorna un awaitable che si risolve in `AsyncSession`.
///
/// # Errors
///
/// L'awaitable rifiuta con `PlenoraError` (categoria mappata) se il
/// probe iniziale fallisce.
///
/// # TLS
///
/// Come `connect()` sync: default `tls_mode="require"`; per dev/test
/// senza TLS passare `tls_mode="insecure_local"`.
#[pyfunction]
#[pyo3(signature = (dsn, tls_mode="require"))]
pub fn aconnect<'py>(py: Python<'py>, dsn: &str, tls_mode: &str) -> PyResult<Bound<'py, PyAny>> {
    let provider_built = crate::session::build_provider(tls_mode)?;
    let secret_for_result = SecretString::new(dsn.to_owned());
    let secret_for_probe = SecretString::new(dsn.to_owned());
    future_into_py(py, async move {
        let provider = Arc::new(provider_built);
        let provider_for_probe = Arc::clone(&provider);
        let cancel = CancellationToken::new();
        let caps = provider_for_probe
            .probe_capabilities(&secret_for_probe, &cancel)
            .await
            .map_err(to_py_err)?;
        let postgis_version = caps.extension_versions.get("postgis").cloned();
        let server_version = caps.provider_version.clone();
        let session = AsyncSession {
            provider,
            secret: secret_for_result,
            capabilities: caps,
            server_version,
            postgis_version,
            closed: false,
        };
        Python::attach(|py| {
            let obj = Py::new(py, session)?;
            Ok(obj.into_pyobject(py)?.into_any().unbind())
        })
    })
}

/// Runtime tokio inizializzato per pyo3-async-runtimes al primo uso.
///
/// Chiamato dall'inizializzazione del pymodule per garantire che il
/// runtime multi-thread condiviso con la Session sync sia usato anche
/// per gli awaitable async.
///
/// # Errors
///
/// Restituisce il motivo se il runtime non e avviabile (limiti di thread o di
/// descrittori) o se un altro runtime globale risulta gia registrato, senza
/// propagare un panic attraverso l'import Python.
pub fn init_async_runtime() -> std::result::Result<(), String> {
    let rt = crate::build_runtime()?;
    pyo3_async_runtimes::tokio::init_with_runtime(rt)
        .map_err(|()| "runtime globale pyo3-async-runtimes gia registrato".to_owned())
}
