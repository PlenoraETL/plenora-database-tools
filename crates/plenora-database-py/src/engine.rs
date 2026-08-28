//! Engine applicativo provider-neutral esposto al binding Python.

#![allow(clippy::missing_const_for_fn, clippy::unused_self)]

use crate::errors::to_py_err;
use crate::runtime;
use crate::session::{build_provider, Session};
use crate::session_family::{DatabaseSession, Endpoint, ProviderBuilder};
use crate::{AsyncDatabaseSession, AsyncSession};
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
#[cfg(not(feature = "db2"))]
use plenora_database_core::{DatabaseError, ErrorPhase};
use plenora_database_engine::Engine as CoreEngine;
use plenora_db_postgres::PostgresProvider;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use std::path::PathBuf;
use std::sync::Arc;

enum SessionTarget {
    Postgres(Arc<PostgresProvider>),
    Family {
        provider: Arc<dyn Provider>,
        product: &'static str,
        factory: &'static str,
    },
}

struct EngineBinding {
    core: CoreEngine,
    target: SessionTarget,
    secret: SecretString,
    capabilities: ProviderCapabilities,
    provider_kind: &'static str,
}

/// Engine condivisibile fra richieste e sessioni applicative.
#[pyclass(name = "Engine", module = "plenora_database._native")]
pub struct PyEngine {
    binding: Arc<EngineBinding>,
}

/// Variante asyncio dello stesso Engine provider-neutral.
#[pyclass(name = "AsyncEngine", module = "plenora_database._native")]
pub struct PyAsyncEngine {
    binding: Arc<EngineBinding>,
}

impl PyEngine {
    fn new(binding: EngineBinding) -> Self {
        Self {
            binding: Arc::new(binding),
        }
    }
}

impl PyAsyncEngine {
    fn new(binding: EngineBinding) -> Self {
        Self {
            binding: Arc::new(binding),
        }
    }
}

#[pymethods]
impl PyEngine {
    /// Apre una sessione logica governata dal lifecycle dell'engine.
    fn session(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.binding.target {
            SessionTarget::Postgres(provider) => Py::new(
                py,
                Session::from_engine(
                    &self.binding.core,
                    Arc::clone(provider),
                    self.binding.secret.clone(),
                    self.binding.capabilities.clone(),
                )
                .map_err(to_py_err)?,
            )
            .map(Py::into_any),
            SessionTarget::Family {
                provider,
                product,
                factory,
            } => Py::new(
                py,
                DatabaseSession::from_engine(
                    &self.binding.core,
                    Arc::clone(provider),
                    self.binding.secret.clone(),
                    self.binding.capabilities.clone(),
                    product,
                    factory,
                )
                .map_err(to_py_err)?,
            )
            .map(Py::into_any),
        }
    }

    /// Snapshot dei contatori di lifecycle dell'engine.
    fn statistics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let statistics = self.binding.core.statistics();
        let value = PyDict::new(py);
        value.set_item("sessions_opened", statistics.sessions_opened)?;
        value.set_item("active_sessions", statistics.active_sessions)?;
        value.set_item("disposed", statistics.disposed)?;
        Ok(value)
    }

    #[getter]
    fn provider_kind(&self) -> &'static str {
        self.binding.provider_kind
    }

    #[getter]
    fn is_disposed(&self) -> bool {
        self.binding.core.statistics().disposed
    }

    /// Impedisce nuove sessioni e cancella il lavoro governato dall'engine.
    fn dispose(&self) {
        self.binding.core.dispose();
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(&self, _exc_type: Py<PyAny>, _exc_value: Py<PyAny>, _traceback: Py<PyAny>) -> bool {
        self.dispose();
        false
    }

    fn __repr__(&self) -> String {
        let statistics = self.binding.core.statistics();
        format!(
            "<Engine provider='{}' active_sessions={} disposed={}>",
            self.binding.provider_kind, statistics.active_sessions, statistics.disposed
        )
    }
}

#[pymethods]
impl PyAsyncEngine {
    /// Apre una sessione asyncio governata dal lifecycle dell'engine.
    fn session(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.binding.target {
            SessionTarget::Postgres(provider) => Py::new(
                py,
                AsyncSession::from_engine(
                    &self.binding.core,
                    Arc::clone(provider),
                    self.binding.secret.clone(),
                    self.binding.capabilities.clone(),
                )
                .map_err(to_py_err)?,
            )
            .map(Py::into_any),
            SessionTarget::Family {
                provider,
                product,
                factory,
            } => Py::new(
                py,
                AsyncDatabaseSession::from_engine(
                    &self.binding.core,
                    Arc::clone(provider),
                    self.binding.secret.clone(),
                    self.binding.capabilities.clone(),
                    product,
                    factory,
                )
                .map_err(to_py_err)?,
            )
            .map(Py::into_any),
        }
    }

    fn statistics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let statistics = self.binding.core.statistics();
        let value = PyDict::new(py);
        value.set_item("sessions_opened", statistics.sessions_opened)?;
        value.set_item("active_sessions", statistics.active_sessions)?;
        value.set_item("disposed", statistics.disposed)?;
        Ok(value)
    }

    #[getter]
    fn provider_kind(&self) -> &'static str {
        self.binding.provider_kind
    }

    #[getter]
    fn is_disposed(&self) -> bool {
        self.binding.core.statistics().disposed
    }

    fn dispose(&self) {
        self.binding.core.dispose();
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
        let statistics = self.binding.core.statistics();
        format!(
            "<AsyncEngine provider='{}' active_sessions={} disposed={}>",
            self.binding.provider_kind, statistics.active_sessions, statistics.disposed
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
    Ok(PyEngine::new(EngineBinding {
        core,
        target: SessionTarget::Postgres(provider),
        secret,
        capabilities,
        provider_kind: "postgres",
    }))
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
        let engine = PyAsyncEngine::new(EngineBinding {
            core,
            target: SessionTarget::Postgres(provider),
            secret,
            capabilities,
            provider_kind: "postgres",
        });
        Python::attach(|py| Ok(Py::new(py, engine)?.into_pyobject(py)?.into_any().unbind()))
    })
}

const fn provider_kind_name(kind: ProviderKind) -> &'static str {
    match kind {
        ProviderKind::Postgres => "postgres",
        ProviderKind::Mysql => "mysql",
        ProviderKind::Mariadb => "mariadb",
        ProviderKind::Sqlserver => "sqlserver",
        ProviderKind::Oracle => "oracle",
        ProviderKind::Db2 => "db2",
        ProviderKind::Sqlite => "sqlite",
        ProviderKind::Duckdb => "duckdb",
    }
}

async fn qualify_family_engine(
    endpoint: Endpoint,
    build: ProviderBuilder,
    product: &'static str,
    factory: &'static str,
) -> PyResult<EngineBinding> {
    let secret = endpoint.secret.clone();
    let provider = build(endpoint)?;
    let provider_kind = provider_kind_name(provider.kind());
    let core = CoreEngine::new(Arc::clone(&provider), secret.clone());
    let capabilities = core
        .capabilities(false, &CancellationToken::new())
        .await
        .map_err(to_py_err)?;
    Ok(EngineBinding {
        core,
        target: SessionTarget::Family {
            provider,
            product,
            factory,
        },
        secret,
        capabilities,
        provider_kind,
    })
}

#[allow(clippy::too_many_arguments)]
fn family_endpoint(
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_ca_path: Option<PathBuf>,
    tls_mode: &str,
) -> Endpoint {
    Endpoint {
        host: host.to_owned(),
        database: database.to_owned(),
        user: user.to_owned(),
        secret: SecretString::new(password.to_owned()),
        port,
        tls_ca_pem,
        tls_ca_path,
        tls_mode: tls_mode.to_owned(),
    }
}

macro_rules! family_engine_factories {
    ($sync_name:ident, $async_name:ident, $builder:path, $product:literal) => {
        #[pyfunction]
        #[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
        #[allow(clippy::too_many_arguments)]
        pub fn $sync_name(
            py: Python<'_>,
            host: &str,
            database: &str,
            user: &str,
            password: &str,
            port: Option<u16>,
            tls_ca_pem: Option<Vec<u8>>,
            tls_mode: &str,
        ) -> PyResult<PyEngine> {
            let endpoint = family_endpoint(
                host, database, user, password, port, tls_ca_pem, None, tls_mode,
            );
            let binding = py.detach(|| {
                runtime().block_on(qualify_family_engine(
                    endpoint,
                    $builder,
                    $product,
                    stringify!($sync_name),
                ))
            })?;
            Ok(PyEngine::new(binding))
        }

        #[pyfunction]
        #[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
        #[allow(clippy::too_many_arguments)]
        pub fn $async_name<'py>(
            py: Python<'py>,
            host: &str,
            database: &str,
            user: &str,
            password: &str,
            port: Option<u16>,
            tls_ca_pem: Option<Vec<u8>>,
            tls_mode: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            let endpoint = family_endpoint(
                host, database, user, password, port, tls_ca_pem, None, tls_mode,
            );
            future_into_py(py, async move {
                let binding = qualify_family_engine(
                    endpoint,
                    $builder,
                    $product,
                    stringify!($async_name),
                )
                .await?;
                Python::attach(|py| {
                    Ok(Py::new(py, PyAsyncEngine::new(binding))?
                        .into_pyobject(py)?
                        .into_any()
                        .unbind())
                })
            })
        }
    };
}

family_engine_factories!(
    create_mysql_engine,
    create_async_mysql_engine,
    crate::session_family::mysql_provider,
    "MySQL"
);
family_engine_factories!(
    create_mariadb_engine,
    create_async_mariadb_engine,
    crate::session_family::mariadb_provider,
    "MariaDB"
);
family_engine_factories!(
    create_sqlserver_engine,
    create_async_sqlserver_engine,
    crate::session_family::sqlserver_provider,
    "SQL Server"
);

#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_path=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)]
#[cfg(feature = "db2")]
pub fn create_db2_engine(
    py: Python<'_>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_path: Option<PathBuf>,
    tls_mode: &str,
) -> PyResult<PyEngine> {
    let endpoint = family_endpoint(
        host,
        database,
        user,
        password,
        port,
        None,
        tls_ca_path,
        tls_mode,
    );
    let binding = py.detach(|| {
        runtime().block_on(qualify_family_engine(
            endpoint,
            crate::session_family::db2_provider,
            "IBM Db2 LUW",
            "create_db2_engine",
        ))
    })?;
    Ok(PyEngine::new(binding))
}

#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_path=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)]
#[cfg(feature = "db2")]
pub fn create_async_db2_engine<'py>(
    py: Python<'py>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_path: Option<PathBuf>,
    tls_mode: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let endpoint = family_endpoint(
        host,
        database,
        user,
        password,
        port,
        None,
        tls_ca_path,
        tls_mode,
    );
    future_into_py(py, async move {
        let binding = qualify_family_engine(
            endpoint,
            crate::session_family::db2_provider,
            "IBM Db2 LUW",
            "create_async_db2_engine",
        )
        .await?;
        Python::attach(|py| {
            Ok(Py::new(py, PyAsyncEngine::new(binding))?
                .into_pyobject(py)?
                .into_any()
                .unbind())
        })
    })
}

macro_rules! db2_engine_unavailable {
    ($name:ident, $return_type:ty) => {
        #[pyfunction]
        #[pyo3(signature = (host, database, user, password, port=None, tls_ca_path=None, tls_mode="require"))]
        #[allow(clippy::too_many_arguments)]
        #[cfg(not(feature = "db2"))]
        pub fn $name(
            host: &str,
            database: &str,
            user: &str,
            password: &str,
            port: Option<u16>,
            tls_ca_path: Option<PathBuf>,
            tls_mode: &str,
        ) -> PyResult<$return_type> {
            let _ = (host, database, user, password, port, tls_ca_path, tls_mode);
            Err(to_py_err(DatabaseError::unsupported(
                ProviderKind::Db2,
                ErrorPhase::Prepare,
                "supporto Db2 non incluso in questo wheel; usa un artefatto costruito con la feature 'db2'",
            )))
        }
    };
}

db2_engine_unavailable!(create_db2_engine, PyEngine);
db2_engine_unavailable!(create_async_db2_engine, Py<PyAny>);
