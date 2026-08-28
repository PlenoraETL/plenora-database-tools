//! Engine applicativo PostgreSQL esposto al binding Python.

#![allow(clippy::missing_const_for_fn, clippy::unused_self)]

use crate::errors::to_py_err;
use crate::runtime;
use crate::session::{build_provider, Session};
use crate::AsyncSession;
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
use plenora_database_engine::Engine as CoreEngine;
use plenora_db_postgres::PostgresProvider;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Engine PostgreSQL condivisibile fra richieste e sessioni applicative.
#[pyclass(name = "Engine", module = "plenora_database._native")]
pub struct PyEngine {
    core: CoreEngine,
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    capabilities: ProviderCapabilities,
}

/// Variante asyncio dell'Engine PostgreSQL.
#[pyclass(name = "AsyncEngine", module = "plenora_database._native")]
pub struct PyAsyncEngine {
    core: CoreEngine,
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    capabilities: ProviderCapabilities,
}

impl PyEngine {
    fn new(
        core: CoreEngine,
        provider: Arc<PostgresProvider>,
        secret: SecretString,
        capabilities: ProviderCapabilities,
    ) -> Self {
        Self {
            core,
            provider,
            secret,
            capabilities,
        }
    }
}

impl PyAsyncEngine {
    fn new(
        core: CoreEngine,
        provider: Arc<PostgresProvider>,
        secret: SecretString,
        capabilities: ProviderCapabilities,
    ) -> Self {
        Self {
            core,
            provider,
            secret,
            capabilities,
        }
    }
}

#[pymethods]
impl PyEngine {
    /// Apre una sessione logica governata dal lifecycle dell'engine.
    fn session(&self) -> PyResult<Session> {
        Session::from_engine(
            &self.core,
            Arc::clone(&self.provider),
            self.secret.clone(),
            self.capabilities.clone(),
        )
        .map_err(to_py_err)
    }

    /// Snapshot dei contatori di lifecycle dell'engine.
    fn statistics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let statistics = self.core.statistics();
        let value = PyDict::new(py);
        value.set_item("sessions_opened", statistics.sessions_opened)?;
        value.set_item("active_sessions", statistics.active_sessions)?;
        value.set_item("disposed", statistics.disposed)?;
        Ok(value)
    }

    #[getter]
    fn provider_kind(&self) -> &'static str {
        "postgres"
    }

    #[getter]
    fn is_disposed(&self) -> bool {
        self.core.statistics().disposed
    }

    /// Impedisce nuove sessioni e cancella il lavoro governato dall'engine.
    fn dispose(&self) {
        self.core.dispose();
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(&self, _exc_type: Py<PyAny>, _exc_value: Py<PyAny>, _traceback: Py<PyAny>) -> bool {
        self.dispose();
        false
    }

    fn __repr__(&self) -> String {
        let statistics = self.core.statistics();
        format!(
            "<Engine provider='postgres' active_sessions={} disposed={}>",
            statistics.active_sessions, statistics.disposed
        )
    }
}

#[pymethods]
impl PyAsyncEngine {
    /// Apre una sessione asyncio governata dal lifecycle dell'engine.
    fn session(&self) -> PyResult<AsyncSession> {
        AsyncSession::from_engine(
            &self.core,
            Arc::clone(&self.provider),
            self.secret.clone(),
            self.capabilities.clone(),
        )
        .map_err(to_py_err)
    }

    fn statistics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let statistics = self.core.statistics();
        let value = PyDict::new(py);
        value.set_item("sessions_opened", statistics.sessions_opened)?;
        value.set_item("active_sessions", statistics.active_sessions)?;
        value.set_item("disposed", statistics.disposed)?;
        Ok(value)
    }

    #[getter]
    fn provider_kind(&self) -> &'static str {
        "postgres"
    }

    #[getter]
    fn is_disposed(&self) -> bool {
        self.core.statistics().disposed
    }

    fn dispose(&self) {
        self.core.dispose();
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let object = slf.into_pyobject(py)?.into_any().unbind();
        future_into_py(py, async move { Ok(object) })
    }

    fn __aexit__(
        slf: Py<Self>,
        py: Python<'_>,
        _exc_type: Py<PyAny>,
        _exc_value: Py<PyAny>,
        _traceback: Py<PyAny>,
    ) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            Python::attach(|py| slf.borrow(py).dispose());
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        let statistics = self.core.statistics();
        format!(
            "<AsyncEngine provider='postgres' active_sessions={} disposed={}>",
            statistics.active_sessions, statistics.disposed
        )
    }
}

/// Crea un Engine PostgreSQL e misura le capability prima di pubblicarlo.
#[pyfunction]
#[pyo3(signature = (dsn, tls_mode="require"))]
pub fn create_engine(py: Python<'_>, dsn: &str, tls_mode: &str) -> PyResult<PyEngine> {
    let provider = Arc::new(build_provider(tls_mode)?);
    let secret = SecretString::new(dsn.to_owned());
    let provider_for_core: Arc<dyn Provider> = Arc::clone(&provider) as Arc<dyn Provider>;
    let core = CoreEngine::new(provider_for_core, secret.clone());
    let core_for_probe = core.clone();
    let capabilities = py
        .detach(|| {
            runtime().block_on(async move {
                core_for_probe
                    .capabilities(false, &CancellationToken::new())
                    .await
            })
        })
        .map_err(to_py_err)?;
    Ok(PyEngine::new(core, provider, secret, capabilities))
}

/// Crea in modo asincrono un Engine PostgreSQL gia qualificato dalla probe.
#[pyfunction]
#[pyo3(signature = (dsn, tls_mode="require"))]
pub fn create_async_engine<'py>(
    py: Python<'py>,
    dsn: &str,
    tls_mode: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let provider = Arc::new(build_provider(tls_mode)?);
    let secret = SecretString::new(dsn.to_owned());
    let provider_for_core: Arc<dyn Provider> = Arc::clone(&provider) as Arc<dyn Provider>;
    let core = CoreEngine::new(provider_for_core, secret.clone());
    future_into_py(py, async move {
        let capabilities = core
            .capabilities(false, &CancellationToken::new())
            .await
            .map_err(to_py_err)?;
        let engine = PyAsyncEngine::new(core, provider, secret, capabilities);
        Python::attach(|py| Ok(Py::new(py, engine)?.into_pyobject(py)?.into_any().unbind()))
    })
}
