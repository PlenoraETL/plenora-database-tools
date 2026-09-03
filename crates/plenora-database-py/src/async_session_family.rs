//! Sessione asincrona provider-neutral esposta dal binding Python.
//!
//! E il bridge asyncio <-> tokio della superficie sincrona in
//! [`crate::session_family`]. Ogni operazione fuori da `begin` usa una
//! transazione dedicata; provider e capability sono determinati dalla probe
//! della sessione.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::redundant_pub_crate,
    clippy::too_many_arguments
)]

use crate::arrow_reader::{
    default_budget as reader_default_budget, make_qualified_read_operation, prepare_resumable_read,
};
use crate::async_session_ops::{SharedEngineSession, TransactionBackend};
use crate::checkpoint::PyReadCheckpoint;
use crate::errors::to_py_err;
use crate::py_convert::{portable_from_json, statement_from_python};
use crate::session_family::ProviderBuilder;
use plenora_database_core::plan::Operation;
#[cfg(not(feature = "db2"))]
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
#[cfg(not(feature = "db2"))]
use plenora_database_core::{DatabaseError, ErrorPhase};
use plenora_database_engine::Engine;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Sessione asincrona comune dei provider non PostgreSQL del binding.
#[pyclass(module = "plenora_database._native")]
#[allow(dead_code)]
pub struct AsyncDatabaseSession {
    provider: Arc<dyn Provider>,
    /// Il prodotto che questa sessione serve, e la factory che l'ha aperta:
    /// una sessione `MariaDB` che si dichiarasse `MySQL` mentirebbe proprio a
    /// chi sta cercando di capire cosa ha in mano.
    product: &'static str,
    factory: &'static str,
    secret: SecretString,
    capabilities: plenora_database_core::capabilities::ProviderCapabilities,
    server_version: String,
    engine: Engine,
    engine_handle: Option<SharedEngineSession>,
    operation_cancellation: CancellationToken,
    transaction_active: Arc<AtomicBool>,
    closed: bool,
}

impl AsyncDatabaseSession {
    pub(crate) fn from_engine(
        engine: &Engine,
        provider: Arc<dyn Provider>,
        secret: SecretString,
        capabilities: plenora_database_core::capabilities::ProviderCapabilities,
        product: &'static str,
        factory: &'static str,
    ) -> plenora_database_core::Result<Self> {
        let session = engine.session()?;
        let operation_cancellation = session.cancellation_token();
        Ok(Self {
            provider,
            product,
            factory,
            secret,
            server_version: capabilities.provider_version.clone(),
            capabilities,
            engine: engine.clone(),
            engine_handle: Some(Arc::new(tokio::sync::Mutex::new(Some(session)))),
            operation_cancellation,
            transaction_active: Arc::new(AtomicBool::new(false)),
            closed: false,
        })
    }

    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(format!(
                "sessione {} chiusa: aprine una nuova con plenora_database.{}(...)",
                self.product, self.factory
            )));
        }
        if self.transaction_active.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err(
                "sessione occupata da una transazione esplicita",
            ));
        }
        Ok(())
    }

    fn cancellation(&self) -> CancellationToken {
        self.operation_cancellation.clone()
    }

    fn transaction_backend(&self) -> TransactionBackend {
        TransactionBackend::Engine(
            Arc::clone(
                self.engine_handle
                    .as_ref()
                    .expect("ensure_open garantisce la sessione Engine"),
            ),
            self.cancellation(),
        )
    }
}

#[pymethods]
impl AsyncDatabaseSession {
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
    fn is_closed(&self) -> bool {
        self.closed
    }

    fn close(&mut self) {
        self.closed = true;
        self.operation_cancellation.cancel();
        self.engine_handle.take();
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj = slf.into_pyobject(py)?.into_any().unbind();
        future_into_py(py, async move { Ok(obj) })
    }

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
                guard.close();
            });
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "<AsyncDatabaseSession server='{}' closed={}>",
            self.server_version, self.closed
        )
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
        crate::async_session_ops::execute_with_backend(py, self.transaction_backend(), statement)
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
        crate::async_session_ops::execute_scalar_with_backend(
            py,
            self.transaction_backend(),
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
        crate::async_session_ops::execute_rows_with_backend(
            py,
            self.transaction_backend(),
            statement,
        )
    }

    fn execute_ddl<'py>(&self, py: Python<'py>, sql: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::execute_ddl(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
            sql.to_owned(),
            self.cancellation(),
        )
    }

    fn inspect_catalogs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_strings(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
            Operation::DatabaseListCatalogs,
            "catalogs",
            self.cancellation(),
        )
    }

    fn inspect_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_strings(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
            Operation::DatabaseListSchemas { source: None },
            "schemas",
            self.cancellation(),
        )
    }

    fn inspect_tables<'py>(&self, py: Python<'py>, schema: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_objects(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
            schema.to_owned(),
            self.cancellation(),
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
            Arc::clone(&self.provider),
            self.secret.clone(),
            schema.to_owned(),
            object.to_owned(),
            self.cancellation(),
        )
    }

    /// Apre una transazione asincrona gestita dal chiamante.
    #[pyo3(signature = (
        isolation=None,
        read_only=None,
        statement_timeout_ms=None,
        context=None,
        native_query_policy=None,
    ))]
    #[allow(clippy::too_many_arguments)] // Firma Python a keyword della sessione comune.
    fn begin<'py>(
        &mut self,
        py: Python<'py>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        statement_timeout_ms: Option<u64>,
        context: Option<crate::session_context_py::PySessionContext>,
        native_query_policy: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let opts = crate::session_tx::transaction_options(
            isolation,
            read_only,
            None,
            statement_timeout_ms,
            context,
            native_query_policy,
        )?;
        if self
            .transaction_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(PyRuntimeError::new_err(
                "sessione occupata da una transazione esplicita",
            ));
        }
        let session = match self.engine.session() {
            Ok(session) => session,
            Err(error) => {
                self.transaction_active.store(false, Ordering::Release);
                return Err(to_py_err(error));
            }
        };
        let awaitable = crate::async_session_ops::begin_engine(
            py,
            Arc::new(tokio::sync::Mutex::new(Some(session))),
            opts,
            self.cancellation(),
            Arc::clone(&self.transaction_active),
        );
        match awaitable {
            Ok(awaitable) => Ok(awaitable),
            Err(error) => {
                self.transaction_active.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Esegue un PortableStatement async e ritorna rows come list[dict].
    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast = portable_from_json(ast_json)?;
        crate::async_session_ops::execute_portable_rows_with_backend(
            py,
            self.transaction_backend(),
            ast,
        )
    }

    /// Esegue un PortableStatement async e ritorna affected_rows.
    fn execute_portable_count<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast = portable_from_json(ast_json)?;
        crate::async_session_ops::execute_portable_count_with_backend(
            py,
            self.transaction_backend(),
            ast,
        )
    }

    /// Streaming Arrow read async. Ritorna awaitable → `AsyncBatchReader`.
    #[pyo3(signature = (
        schema,
        object,
        projection=None,
        order_by=None,
        limit=None,
        *,
        catalog=None,
        checkpoint=None,
    ))]
    fn aread<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
        object: &str,
        projection: Option<Vec<String>>,
        order_by: Option<Vec<(String, String)>>,
        limit: Option<u64>,
        catalog: Option<&str>,
        checkpoint: Option<PyRef<'_, PyReadCheckpoint>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let schema = schema.to_owned();
        let object = object.to_owned();
        let projection = projection.unwrap_or_default();
        let order_by = order_by.unwrap_or_default();
        let catalog = catalog.map(str::to_owned);
        let checkpoint = checkpoint.map(|value| value.inner().clone());
        let resumable = self.capabilities.reads.resumable;
        let cancel = self.cancellation();
        future_into_py(py, async move {
            let operation = make_qualified_read_operation(
                catalog.as_deref(),
                &schema,
                &object,
                projection,
                order_by,
                limit,
            )
            .map_err(to_py_err)?;
            let (operation, parameters) =
                prepare_resumable_read(provider.kind(), resumable, operation, checkpoint.as_ref())
                    .map_err(to_py_err)?;
            let stream = provider
                .read(
                    &secret,
                    &operation,
                    &parameters,
                    &reader_default_budget(),
                    &cancel,
                )
                .await
                .map_err(to_py_err)?;
            Python::attach(|py| {
                let reader = crate::arrow_reader::AsyncBatchReader::new(stream, cancel);
                Ok(Py::new(py, reader)?.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Bulk write async. Awaitable → dict `WriteOutcome`.
    ///
    /// Vedi `DatabaseSession::copy_from` sync per WriteMode disponibili
    /// e `mapping_policy` obbligatorio `"strict"` per default.
    #[pyo3(signature = (
        schema,
        table,
        ipc_bytes,
        mode="append",
        transaction_profile="single_transaction",
        mapping_policy="strict",
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
        let schema = schema.to_owned();
        let table = table.to_owned();
        let ipc = ipc_bytes.to_vec();
        let mode = mode.to_owned();
        let profile = transaction_profile.to_owned();
        let policy = mapping_policy.to_owned();
        let keys = keys.unwrap_or_default();
        let update_columns = update_columns.unwrap_or_default();
        let cancellation = self.cancellation();
        future_into_py(py, async move {
            let outcome = crate::family_write::do_copy_from_async_family(
                provider,
                secret,
                schema,
                table,
                ipc,
                &mode,
                &profile,
                &policy,
                keys,
                update_columns,
                cancellation,
            )
            .await
            .map_err(to_py_err)?;
            Python::attach(|py| {
                let d = crate::write::outcome_into_py(py, &outcome)?;
                Ok(d.into_any().unbind())
            })
        })
    }
}

/// Factory async — apre una `AsyncDatabaseSession`.
///
/// Ritorna un awaitable Python: `s = await aconnect_mysql(...)`.
///
/// # Errors
///
/// `PlenoraError` se la configurazione è invalida o la connessione fallisce.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)] // Firma allineata alla factory sincrona.
pub fn aconnect_mysql<'py>(
    py: Python<'py>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
) -> PyResult<Bound<'py, PyAny>> {
    open_async(
        py,
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        crate::session_family::mysql_provider,
        "MySQL",
        "aconnect_mysql",
    )
}

/// Factory async di `MariaDB` — apre una `AsyncDatabaseSession` sul suo provider.
///
/// Una factory sua e non un parametro di [`aconnect_mysql`], per la stessa
/// ragione del percorso sincrono: ADR 0014 vieta la selezione automatica, e il
/// prodotto lo dichiara il consumatore.
///
/// # Errors
///
/// Come [`aconnect_mysql`], piu il rifiuto della probe se il server non e
/// `MariaDB`.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)] // Firma comune alle factory asincrone.
pub fn aconnect_mariadb<'py>(
    py: Python<'py>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
) -> PyResult<Bound<'py, PyAny>> {
    open_async(
        py,
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        crate::session_family::mariadb_provider,
        "MariaDB",
        "aconnect_mariadb",
    )
}

/// Factory async di `SQL Server` — apre una sessione della famiglia sul suo
/// provider.
///
/// Awaitable analogo di `connect_sqlserver`, e come le altre due della
/// famiglia non fa selezione automatica: il prodotto lo dichiara il
/// consumatore, e la probe verifica quella scelta invece di compierla.
///
/// # Errors
///
/// Come [`aconnect_mysql`], piu il rifiuto della probe se il server non e
/// `SQL Server`.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)] // Firma comune alle factory asincrone.
pub fn aconnect_sqlserver<'py>(
    py: Python<'py>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
) -> PyResult<Bound<'py, PyAny>> {
    open_async(
        py,
        host,
        database,
        user,
        password,
        port,
        tls_ca_pem,
        tls_mode,
        crate::session_family::sqlserver_provider,
        "SQL Server",
        "aconnect_sqlserver",
    )
}

/// Factory async di `IBM Db2 LUW` sulla sessione provider-agnostic.
///
/// # Errors
///
/// Come [`aconnect_mysql`], con `tls_ca_path` persistente e opt-out plaintext
/// disponibile solo tramite `tls_mode="disable"` esplicito.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_path=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)]
#[cfg(feature = "db2")]
pub fn aconnect_db2<'py>(
    py: Python<'py>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_path: Option<std::path::PathBuf>,
    tls_mode: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let secret = SecretString::new(password.to_owned());
    open_async_endpoint(
        py,
        crate::session_family::Endpoint {
            host: host.to_owned(),
            database: database.to_owned(),
            user: user.to_owned(),
            secret,
            port,
            tls_ca_pem: None,
            tls_ca_path,
            tls_mode: tls_mode.to_owned(),
            max_connections: 1,
            acquire_timeout_ms: 10_000,
        },
        crate::session_family::db2_provider,
        "IBM Db2 LUW",
        "aconnect_db2",
    )
}

/// Factory async Oracle thin.
///
/// # Errors
///
/// Come [`aconnect_mysql`], con CA persistente e plaintext solo tramite
/// `tls_mode="disable"` esplicito.
#[pyfunction]
#[pyo3(signature = (host, service, user, password, port=None, tls_ca_path=None, tls_mode="require", max_connections=4, acquire_timeout_ms=10_000))]
#[allow(clippy::too_many_arguments)]
pub fn aconnect_oracle<'py>(
    py: Python<'py>,
    host: &str,
    service: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_path: Option<std::path::PathBuf>,
    tls_mode: &str,
    max_connections: usize,
    acquire_timeout_ms: u64,
) -> PyResult<Bound<'py, PyAny>> {
    let secret = SecretString::new(password.to_owned());
    open_async_endpoint(
        py,
        crate::session_family::Endpoint {
            host: host.to_owned(),
            database: service.to_owned(),
            user: user.to_owned(),
            secret,
            port,
            tls_ca_pem: None,
            tls_ca_path,
            tls_mode: tls_mode.to_owned(),
            max_connections,
            acquire_timeout_ms,
        },
        crate::session_family::oracle_provider,
        "Oracle",
        "aconnect_oracle",
    )
}

/// Stub async fail-closed dei wheel senza feature `db2`.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_path=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "db2"))]
pub fn aconnect_db2<'py>(
    py: Python<'py>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_path: Option<std::path::PathBuf>,
    tls_mode: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let _ = (host, database, user, password, port, tls_ca_path, tls_mode);
    future_into_py(py, async move {
        Err::<AsyncDatabaseSession, _>(to_py_err(DatabaseError::unsupported(
            ProviderKind::Db2,
            ErrorPhase::Prepare,
            "supporto Db2 non incluso in questo wheel; usa un artefatto costruito con la feature 'db2'",
        )))
    })
}

/// Il corpo comune delle factory async.
///
/// La configurazione — TLS compreso — passa da `family_config`, la stessa che
/// usa il percorso sincrono: il fail-close TLS ha gia avuto il suo difetto una
/// volta, e non puo permettersi ne due copie ne quattro.
#[allow(clippy::too_many_arguments)] // gli argomenti sono quelli delle API
fn open_async<'py>(
    py: Python<'py>,
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
    build: ProviderBuilder,
    product: &'static str,
    factory: &'static str,
) -> PyResult<Bound<'py, PyAny>> {
    let secret = SecretString::new(password.to_owned());
    open_async_endpoint(
        py,
        crate::session_family::Endpoint {
            host: host.to_owned(),
            database: database.to_owned(),
            user: user.to_owned(),
            secret,
            port,
            tls_ca_pem,
            tls_ca_path: None,
            tls_mode: tls_mode.to_owned(),
            max_connections: 4,
            acquire_timeout_ms: 10_000,
        },
        build,
        product,
        factory,
    )
}

/// Apre una sessione async a partire da un endpoint gia tipizzato.
fn open_async_endpoint<'py>(
    py: Python<'py>,
    endpoint: crate::session_family::Endpoint,
    build: ProviderBuilder,
    product: &'static str,
    factory: &'static str,
) -> PyResult<Bound<'py, PyAny>> {
    let secret = endpoint.secret.clone();
    future_into_py(py, async move {
        // Il costruttore seleziona il prodotto senza confronti su etichette:
        // un nome errato non deve scegliere per omissione un altro motore.
        let provider = build(endpoint)?;
        let engine = Engine::new(Arc::clone(&provider), secret.clone());
        let capabilities = engine
            .capabilities(false, &CancellationToken::new())
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| {
            let session = AsyncDatabaseSession::from_engine(
                &engine,
                provider,
                secret,
                capabilities,
                product,
                factory,
            )
            .map_err(to_py_err)?;
            Ok(Py::new(py, session)?.into_pyobject(py)?.into_any().unbind())
        })
    })
}
