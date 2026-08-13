//! AsyncSession + aconnect (F3-7).
//!
//! Bridge asyncio ↔ tokio via `pyo3-async-runtimes`. Le API espongono
//! metodi che ritornano awaitable Python: `await s.execute_scalar(...)`.
//!
//! Ogni metodo apre una transazione auto-commit dedicata (come la
//! Session sync in F3-3). Per transazioni user-managed → F3-7b
//! AsyncTransaction.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
)]

use crate::async_transaction::AsyncTransaction;
use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use crate::runtime;
use crate::transaction::parse_isolation;
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{
    AccessMode, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, Row};
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

fn default_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("default budget")
}

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

    /// Esegue una closure async in una transazione auto-commit dedicata.
    ///
    /// Il pattern è identico a Session::run_tx ma la closure ritorna un
    /// future generico che poi entra nell'awaitable Python.
    async fn run_tx<R>(
        provider: Arc<PostgresProvider>,
        secret: SecretString,
        work: impl for<'a> FnOnce(
            &'a mut dyn TransactionScope,
            &'a CancellationToken,
        ) -> plenora_database_core::provider::ProviderFuture<'a, R>,
    ) -> plenora_database_core::Result<R> {
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret, &TransactionOptions::default(), &default_budget(), &cancel)
            .await?;
        let result = work(tx.as_mut(), &cancel).await;
        match result {
            Ok(value) => {
                let outcome = Box::new(tx).commit(&cancel).await?;
                if !outcome.is_committed() {
                    return Err(DatabaseError {
                        category: plenora_database_core::ErrorCategory::Internal,
                        phase: plenora_database_core::ErrorPhase::Write,
                        remote_effect: plenora_database_core::RemoteEffect::Unknown,
                        retry: plenora_database_core::RetryDisposition::Never,
                        provider: None,
                        execution_id: None,
                        message: "commit outcome unknown: verificare stato del target out-of-band"
                            .to_owned(),
                        diagnostics: None,
                    });
                }
                Ok(value)
            }
            Err(e) => {
                let _ = Box::new(tx).rollback(&cancel).await;
                Err(e)
            }
        }
    }
}

#[pymethods]
impl AsyncSession {
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
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
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            Python::with_gil(|py| {
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
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            Self::run_tx(provider, secret, move |tx, cancel| {
                Box::pin(async move { tx.execute(&statement, cancel).await })
            })
            .await
            .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            let rows: Vec<Row> = Self::run_tx(provider, secret, move |tx, cancel| {
                Box::pin(async move { tx.query(&statement, cancel).await })
            })
            .await
            .map_err(to_py_err)?;
            Python::with_gil(|py| {
                rows.first().and_then(|r| r.get_index(0)).map_or_else(
                    || Ok(py.None()),
                    |v| param_to_python(py, v).map(Bound::unbind),
                )
            })
        })
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            let rows: Vec<Row> = Self::run_tx(provider, secret, move |tx, cancel| {
                Box::pin(async move { tx.query(&statement, cancel).await })
            })
            .await
            .map_err(to_py_err)?;
            rows_to_pyobject(rows)
        })
    }

    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            PyRuntimeError::new_err(format!("AST portable non valida: {e}"))
        })?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            let rows: Vec<Row> = Self::run_tx(provider, secret, move |tx, cancel| {
                Box::pin(async move { execute_portable_returning(tx, &ast, cancel).await })
            })
            .await
            .map_err(to_py_err)?;
            rows_to_pyobject(rows)
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
    ))]
    fn begin<'py>(
        &self,
        py: Python<'py>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
        statement_timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let mut opts = TransactionOptions::default();
        if let Some(iso) = isolation {
            opts.isolation = Some(parse_isolation(iso)?);
        }
        if let Some(ro) = read_only {
            opts.access_mode = Some(if ro {
                AccessMode::ReadOnly
            } else {
                AccessMode::ReadWrite
            });
        }
        opts.deferrable = deferrable;
        opts.statement_timeout_ms = statement_timeout_ms;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            let cancel = CancellationToken::new();
            let tx = provider
                .begin_transaction(&secret, &opts, &default_budget(), &cancel)
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| {
                let atx = AsyncTransaction::new(tx);
                let obj = Py::new(py, atx)?;
                Ok(obj.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    fn execute_portable_count<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            PyRuntimeError::new_err(format!("AST portable non valida: {e}"))
        })?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            Self::run_tx(provider, secret, move |tx, cancel| {
                Box::pin(async move { execute_portable(tx, &ast, cancel).await })
            })
            .await
            .map_err(to_py_err)
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "<AsyncSession server_version={:?} postgis_version={:?} closed={}>",
            self.server_version, self.postgis_version, self.closed
        )
    }
}

/// Costruisce una list[dict] Python dalle Row (chiamato dentro
/// `Python::with_gil`).
fn rows_to_pyobject(rows: Vec<Row>) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let out = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            for (col, val) in row.columns().iter().zip(row.values().iter()) {
                dict.set_item(col.as_str(), param_to_python(py, val)?)?;
            }
            out.append(dict)?;
        }
        Ok(out.into_any().unbind())
    })
}

/// Apre una sessione Postgres asincrona. La DSN è nel formato libpq.
/// Ritorna un awaitable che si risolve in `AsyncSession`.
///
/// # Errors
///
/// L'awaitable rifiuta con `PlenoraError` (categoria mappata) se il
/// probe iniziale fallisce.
#[pyfunction]
pub fn aconnect<'py>(py: Python<'py>, dsn: &str) -> PyResult<Bound<'py, PyAny>> {
    let secret_for_result = SecretString::new(dsn.to_owned());
    let secret_for_probe = SecretString::new(dsn.to_owned());
    future_into_py(py, async move {
        let provider = Arc::new(PostgresProvider::default());
        let provider_for_probe = Arc::clone(&provider);
        let cancel = CancellationToken::new();
        let caps = provider_for_probe
            .probe_capabilities(&secret_for_probe, &cancel)
            .await
            .map_err(to_py_err)?;
        let session = AsyncSession {
            provider,
            secret: secret_for_result,
            server_version: caps.provider_version,
            postgis_version: caps.extension_versions.get("postgis").cloned(),
            closed: false,
        };
        Python::with_gil(|py| {
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
pub fn init_async_runtime() {
    let rt = runtime();
    pyo3_async_runtimes::tokio::init_with_runtime(rt)
        .expect("pyo3-async-runtimes init");
}
