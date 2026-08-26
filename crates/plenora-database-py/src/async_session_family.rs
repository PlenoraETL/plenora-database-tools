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
use crate::async_transaction::AsyncTransaction;
use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use crate::session_family::ProviderBuilder;
use crate::transaction::parse_isolation;
use plenora_database_core::facade::{execute_portable, execute_portable_returning, scalar_opt};
use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
// Fase E: ResourceBudget/ResourceLimits ora consumati solo via `budget` module
use plenora_database_core::transaction::{
    AccessMode, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, Row};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

// Fase E: consolidato in `crate::budget::session_budget`.
use crate::budget::session_budget as default_budget;

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

    fn inspect_strings<'py>(
        &self,
        py: Python<'py>,
        operation: Operation,
        key: &'static str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            let cancel = CancellationToken::new();
            let inspection = provider
                .inspect(&secret, &operation, &cancel)
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| {
                crate::session::json_to_pylist_of_strings(py, &inspection.document, key)
                    .map(|value| value.into_any().unbind())
            })
        })
    }

    async fn run_tx<R>(
        provider: Arc<dyn Provider>,
        secret: SecretString,
        work: impl for<'a> FnOnce(
            &'a mut dyn TransactionScope,
            &'a CancellationToken,
        ) -> plenora_database_core::provider::ProviderFuture<'a, R>,
    ) -> plenora_database_core::Result<R> {
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret,
                &TransactionOptions::default(),
                &default_budget(),
                &cancel,
            )
            .await?;
        let result = work(tx.as_mut(), &cancel).await;
        match result {
            Ok(value) => {
                let provider_kind = tx.provider_kind();
                let outcome = Box::new(tx).commit(&cancel).await?;
                if !outcome.is_committed() {
                    return Err(crate::errors_commit::commit_outcome_unknown(provider_kind));
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
            // Cardinalita imposta, non dedotta: `scalar_opt` rifiuta piu di
            // una riga o piu di una colonna invece di prendere la prima e
            // buttare via il resto. E' la stessa regola dei costruttori
            // scalar tipizzati del core.
            let value = scalar_opt(rows).map_err(to_py_err)?;
            Python::with_gil(|py| {
                value.as_ref().map_or_else(
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
            Python::with_gil(|py| {
                crate::transaction::rows_to_pylist(py, rows).map(|list| list.into_any().unbind())
            })
        })
    }

    fn execute_ddl<'py>(&self, py: Python<'py>, sql: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let sql = sql.to_owned();
        future_into_py(py, async move {
            let cancel = CancellationToken::new();
            provider
                .execute_ddl(&secret, &sql, &cancel)
                .await
                .map_err(to_py_err)
        })
    }

    fn inspect_catalogs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.inspect_strings(py, Operation::DatabaseListCatalogs, "catalogs")
    }

    fn inspect_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.inspect_strings(
            py,
            Operation::DatabaseListSchemas { source: None },
            "schemas",
        )
    }

    fn inspect_tables<'py>(&self, py: Python<'py>, schema: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let operation = Operation::DatabaseListObjects {
            source: Some(ObjectRef {
                catalog: None,
                schema: Some(schema.to_owned()),
                object: String::new(),
            }),
        };
        future_into_py(py, async move {
            let cancel = CancellationToken::new();
            let inspection = provider
                .inspect(&secret, &operation, &cancel)
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| {
                crate::session::json_to_pylist_of_dicts(py, &inspection.document, "objects")
                    .map(|value| value.into_any().unbind())
            })
        })
    }

    fn inspect_describe<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
        object: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let operation = Operation::DatabaseDescribeObject {
            source: ObjectRef {
                catalog: None,
                schema: Some(schema.to_owned()),
                object: object.to_owned(),
            },
        };
        future_into_py(py, async move {
            let cancel = CancellationToken::new();
            let inspection = provider
                .inspect(&secret, &operation, &cancel)
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| {
                crate::session::json_value_to_pydict(py, &inspection.document)
                    .map(|value| value.into_any().unbind())
            })
        })
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
        if let Some(ms) = statement_timeout_ms {
            opts.statement_timeout_ms = Some(ms);
        }
        // Fix P1 review MySQL — parity con AsyncSession Postgres.
        if let Some(ctx) = context {
            opts.context = ctx.inner;
        }
        if let Some(policy) = native_query_policy {
            opts.native_query_policy = crate::transaction::parse_native_query_policy(policy)?;
        }
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            let cancel = CancellationToken::new();
            let scope = provider
                .begin_transaction(&secret, &opts, &default_budget(), &cancel)
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| {
                let tx = AsyncTransaction::new(scope);
                Ok(Py::new(py, tx)?.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Esegue un PortableStatement async e ritorna rows come list[dict].
    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!(
                "AST portable non valida a riga {}, colonna {}",
                e.line(),
                e.column()
            )))
        })?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        future_into_py(py, async move {
            let rows: Vec<Row> = Self::run_tx(provider, secret, move |tx, cancel| {
                Box::pin(async move { execute_portable_returning(tx, &ast, cancel).await })
            })
            .await
            .map_err(to_py_err)?;
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
        })
    }

    /// Esegue un PortableStatement async e ritorna affected_rows.
    fn execute_portable_count<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!(
                "AST portable non valida a riga {}, colonna {}",
                e.line(),
                e.column()
            )))
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
    /// (5 attivi, 2 fail-closed) e `mapping_policy` obbligatorio
    /// `"strict"` (default post py-v0.9.2).
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
        // Il prodotto lo costruisce il **costruttore**, non un confronto fra
        // stringhe. Qui c'era `if product == "MariaDB" { .. } else { .. }`, e
        // il ramo `else` era MySQL: un terzo prodotto sarebbe finito nel ramo
        // sbagliato per omissione, e un refuso nel nome avrebbe costruito in
        // silenzio il provider di un altro motore. Il rifiuto sarebbe poi
        // arrivato dalla probe, con la faccia di un problema di
        // configurazione del server invece che di un difetto qui.
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
