//! `AsyncDatabaseSession` + `aconnect_mysql`.
//!
//! Bridge asyncio <-> tokio per `MySQL`: i metodi ritornano awaitable Python.
//! Ogni operazione fuori da `begin` apre una transazione in autocommit.
//!
//! La superficie e quella di [`crate::session_family`], con `aread` e
//! `acopy_from` al posto di `read` e `copy_from`:
//!
//! * `aconnect_mysql(host, database, user, password, port, tls_ca_pem,
//!   tls_mode)`
//! * `execute` / `execute_scalar` / `execute_returning_rows` / `execute_ddl`
//! * `begin(...)` -> `AsyncTransaction`, con `SessionContext` e
//!   `native_query_policy`
//! * `aread(...)` -> `AsyncBatchReader`
//! * `acopy_from(...)` -> bulk write
//! * `execute_portable_rows` / `execute_portable_count`, su cui girano i
//!   builder AST del wrapper async
//! * `__aenter__`/`__aexit__`/`close`/`is_closed`/`server_version`
//!
//! Non esposto: spatial predicates e `SpatialReference`.

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

use crate::arrow_reader::{default_budget as reader_default_budget, make_read_operation};
use crate::errors::to_py_err;
use crate::py_convert::{portable_from_json, statement_from_python};
use crate::session_family::ProviderBuilder;
use plenora_database_core::plan::Operation;
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::CancellationToken;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Sessione MySQL asincrona. Ottenuta da `await aconnect_mysql(...)`.
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
    closed: bool,
}

impl AsyncDatabaseSession {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(format!(
                "sessione {} chiusa: aprine una nuova con plenora_database.{}(...)",
                self.product, self.factory
            )));
        }
        Ok(())
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
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj = slf.into_pyobject(py)?.into_any().unbind();
        future_into_py(py, async move { Ok(obj) })
    }

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
        crate::async_session_ops::execute(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
            statement,
        )
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
            Arc::clone(&self.provider),
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
        crate::async_session_ops::execute_rows(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
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
        )
    }

    fn inspect_tables<'py>(&self, py: Python<'py>, schema: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        crate::async_session_ops::inspect_objects(
            py,
            Arc::clone(&self.provider),
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
            Arc::clone(&self.provider),
            self.secret.clone(),
            schema.to_owned(),
            object.to_owned(),
        )
    }

    /// Apre una tx async. Ritorna awaitable → `AsyncTransaction`
    /// (provider-agnostic, riusato dal path Postgres).
    #[pyo3(signature = (
        isolation=None,
        read_only=None,
        statement_timeout_ms=None,
        context=None,
        native_query_policy=None,
    ))]
    #[allow(clippy::too_many_arguments)] // API PyO3 keyword — parity con Postgres
    fn begin<'py>(
        &self,
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
        crate::async_session_ops::begin(py, Arc::clone(&self.provider), self.secret.clone(), opts)
    }

    /// Esegue un PortableStatement async e ritorna rows come list[dict].
    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast = portable_from_json(ast_json)?;
        crate::async_session_ops::execute_portable_rows(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
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
        crate::async_session_ops::execute_portable_count(
            py,
            Arc::clone(&self.provider),
            self.secret.clone(),
            ast,
        )
    }

    /// Streaming Arrow read async. Ritorna awaitable → `AsyncBatchReader`.
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
            let operation = make_read_operation(&schema, &object, projection, order_by, limit)
                .map_err(to_py_err)?;
            let cancel = CancellationToken::new();
            let stream = provider
                .read(
                    &secret,
                    &operation,
                    &ParameterBag::default(),
                    &reader_default_budget(),
                    &cancel,
                )
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| {
                let reader = crate::arrow_reader::AsyncBatchReader::new(stream);
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
            )
            .await
            .map_err(to_py_err)?;
            Python::with_gil(|py| {
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
#[allow(clippy::too_many_arguments)] // API PyO3 keyword — parity con connect_mysql sync
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
#[allow(clippy::too_many_arguments)] // API PyO3 keyword — parity con aconnect_mysql
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
#[allow(clippy::too_many_arguments)] // API PyO3 keyword — parity con aconnect_mysql
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
    let host = host.to_owned();
    let database = database.to_owned();
    let user = user.to_owned();
    let secret = SecretString::new(password.to_owned());
    let tls_mode_owned = tls_mode.to_owned();
    future_into_py(py, async move {
        // Il costruttore seleziona il prodotto senza confronti su etichette:
        // un nome errato non deve scegliere per omissione un altro motore.
        let provider = build(crate::session_family::Endpoint {
            host,
            database,
            user,
            secret: secret.clone(),
            port,
            tls_ca_pem,
            tls_mode: tls_mode_owned,
        })?;
        let cancel = CancellationToken::new();
        let connection = provider
            .test_connection(&secret, &cancel)
            .await
            .map_err(to_py_err)?;
        let capabilities = provider
            .probe_capabilities(&secret, &cancel)
            .await
            .map_err(to_py_err)?;
        Python::with_gil(|py| {
            let session = AsyncDatabaseSession {
                provider,
                product,
                factory,
                secret,
                capabilities,
                server_version: connection.server_version,
                closed: false,
            };
            Ok(Py::new(py, session)?.into_pyobject(py)?.into_any().unbind())
        })
    })
}
