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
    clippy::too_many_lines
)]

use crate::arrow_reader::open_reader_async;
use crate::async_transaction::AsyncTransaction;
use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use crate::transaction::parse_isolation;
use plenora_database_core::facade::{execute_portable, execute_portable_returning, scalar_opt};
use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{Provider, SecretString};
// Fase E: ResourceBudget/ResourceLimits ora consumati solo via `budget` module
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

// Fase E: consolidato in `crate::budget::session_budget`.
use crate::budget::session_budget as default_budget;

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
                let outcome = Box::new(tx).commit(&cancel).await?;
                if !outcome.is_committed() {
                    // Fix review #9: helper unico.
                    return Err(crate::errors_commit::commit_outcome_unknown(
                        plenora_database_core::plan::ProviderKind::Postgres,
                    ));
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
            rows_to_pyobject(rows)
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
            rows_to_pyobject(rows)
        })
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
            Python::with_gil(|py| {
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
            Python::with_gil(|py| {
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
        // PFM CHG-002: SessionContext transaction-local.
        if let Some(ctx) = context {
            opts.context = ctx.inner;
        }
        // PFM CHG-003: policy Allow|Deny come parametro esplicito.
        if let Some(policy) = native_query_policy {
            opts.native_query_policy = crate::transaction::parse_native_query_policy(policy)?;
        }
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
///
/// # TLS (ADR-011, py-v0.9.0)
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
///
/// # Errors
///
/// Restituisce il motivo se il runtime non e avviabile (limiti di thread o di
/// descrittori) o se un altro runtime globale risulta gia registrato. Prima
/// entrambi i casi erano un `expect`, cioe un panico dentro l'import del
/// modulo: il chiamante Python riceveva una `PanicException` invece di un
/// errore classificato.
pub fn init_async_runtime() -> std::result::Result<(), String> {
    let rt = crate::build_runtime()?;
    pyo3_async_runtimes::tokio::init_with_runtime(rt)
        .map_err(|()| "runtime globale pyo3-async-runtimes gia registrato".to_owned())
}
