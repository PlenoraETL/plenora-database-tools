//! AsyncMysqlSession + aconnect_mysql (v0.8).
//!
//! Bridge asyncio ↔ tokio per MySQL. Metodi ritornano awaitable Python.
//! Ogni operazione (senza begin) apre una tx auto-commit.
//!
//! API subset:
//! - `aconnect_mysql(host, database, user, password, port, tls_ca_pem)` async
//! - `AsyncMysqlSession.execute / execute_scalar / execute_returning_rows / execute_ddl` async
//! - `AsyncMysqlSession.begin(...)` → `AsyncTransaction` (provider-agnostic)
//! - `AsyncMysqlSession.aread(...)` → `AsyncBatchReader`
//! - `AsyncMysqlSession.acopy_from(...)` bulk write async
//! - `__aenter__/__aexit__/close/is_closed/server_version`
//!
//! Non incluso: portable AST builders (blocca su cross-crate refactor),
//! spatial predicates, typed params.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::redundant_pub_crate,
    clippy::too_many_arguments,
)]

use crate::arrow_reader::{default_budget as reader_default_budget, make_read_operation};
use crate::async_transaction::AsyncTransaction;
use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use crate::transaction::parse_isolation;
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
// Fase E: ResourceBudget/ResourceLimits ora consumati solo via `budget` module
use plenora_database_core::transaction::{
    AccessMode, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, Row};
use plenora_db_mysql::{MysqlCertificatePolicy, MysqlConfig, MysqlProvider};
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
pub struct AsyncMysqlSession {
    provider: Arc<MysqlProvider>,
    secret: SecretString,
    server_version: String,
    closed: bool,
}

impl AsyncMysqlSession {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(
                "sessione chiusa: aprine una nuova con plenora_database.aconnect_mysql(...)",
            ));
        }
        Ok(())
    }

    async fn run_tx<R>(
        provider: Arc<MysqlProvider>,
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
                    return Err(crate::errors_commit::commit_outcome_unknown(
                        plenora_database_core::plan::ProviderKind::Mysql,
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
impl AsyncMysqlSession {
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
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
            "<AsyncMysqlSession server='{}' closed={}>",
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
            Python::with_gil(|py| {
                let out = PyList::empty(py);
                for row in rows {
                    let dict = PyDict::new(py);
                    let columns: Vec<String> = row.columns().to_vec();
                    for (idx, name) in columns.iter().enumerate() {
                        let value = row
                            .values()
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| {
                                plenora_database_core::provider::ParameterValue::Null {
                                    type_name: "unknown".to_owned(),
                                }
                            });
                        let py_val = param_to_python(py, &value)?;
                        dict.set_item(name.as_str(), py_val)?;
                    }
                    out.append(dict)?;
                }
                Ok(out.into_any().unbind())
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

    /// Apre una tx async. Ritorna awaitable → `AsyncTransaction`
    /// (provider-agnostic, riusato dal path Postgres).
    #[pyo3(signature = (isolation=None, read_only=None, statement_timeout_ms=None))]
    fn begin<'py>(
        &self,
        py: Python<'py>,
        isolation: Option<&str>,
        read_only: Option<bool>,
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
        if let Some(ms) = statement_timeout_ms {
            opts.statement_timeout_ms = Some(ms);
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
                "AST portable non valida: {e}"
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
                "AST portable non valida: {e}"
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
            let operation =
                make_read_operation(&schema, &object, projection, order_by, limit)
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
        let schema = schema.to_owned();
        let table = table.to_owned();
        let ipc = ipc_bytes.to_vec();
        let mode = mode.to_owned();
        let profile = transaction_profile.to_owned();
        let policy = mapping_policy.to_owned();
        let keys = keys.unwrap_or_default();
        let update_columns = update_columns.unwrap_or_default();
        future_into_py(py, async move {
            // Riusa il helper generic write.rs → mysql_write.rs.
            // do_copy_from_async_mysql non è pub — replichiamo la logica
            // qui in modo minimo per non aggiungere altra API pubblica.
            use crate::write::{
                decode_ipc_stream, default_budget as write_budget, make_operation,
                parse_mapping_policy, parse_mode, parse_profile, VecBatchStream,
            };
            let mode_enum = parse_mode(&mode).map_err(to_py_err)?;
            let profile_enum = parse_profile(&profile).map_err(to_py_err)?;
            let policy_enum = parse_mapping_policy(&policy).map_err(to_py_err)?;
            let (input_schema, batches, declared_rows) =
                decode_ipc_stream(&ipc).map_err(to_py_err)?;
            let stream = VecBatchStream {
                schema: Arc::clone(&input_schema),
                batches,
                declared_rows,
            };
            let operation = make_operation(
                &schema,
                &table,
                mode_enum,
                profile_enum,
                policy_enum,
                keys,
                update_columns,
            )
            .map_err(to_py_err)?;
            let budget = write_budget();
            let cancel = CancellationToken::new();
            let prepared = provider
                .prepare_write(&secret, &operation, input_schema, &budget, &cancel)
                .await
                .map_err(to_py_err)?;
            let outcome = provider
                .write(&secret, prepared, Box::new(stream), &budget, &cancel)
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| {
                let d = crate::write::outcome_into_py(py, &outcome)?;
                Ok(d.into_any().unbind())
            })
        })
    }
}

/// Factory async — apre una `AsyncMysqlSession`.
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
    let host = host.to_owned();
    let database = database.to_owned();
    let user = user.to_owned();
    let secret = SecretString::new(password.to_owned());
    // Fix P1 review MySQL: default TLS `require` (Verify webpki),
    // `insecure_trust_server` opt-in esplicito.
    let tls_mode_owned = tls_mode.to_owned();
    future_into_py(py, async move {
        let mut config = MysqlConfig::new(host, database, user, secret.clone());
        if let Some(p) = port {
            config = config.with_port(p);
        }
        if let Some(pem) = tls_ca_pem {
            if pem.len() > 1024 * 1024 {
                return Err(PyRuntimeError::new_err("CA PEM MySQL oltre 1 MiB"));
            }
            config = config.with_private_ca_certificate_pem(pem);
        }
        match tls_mode_owned.as_str() {
            "require" => {}
            "insecure_trust_server" => {
                config = config
                    .with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate);
            }
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "tls_mode non riconosciuto: {other:?}. Valori: \
                     'require' (default) | 'insecure_trust_server'"
                )));
            }
        }
        let provider = Arc::new(MysqlProvider::new(config, 4).map_err(to_py_err)?);
        let cancel = CancellationToken::new();
        let connection = provider
            .test_connection(&secret, &cancel)
            .await
            .map_err(to_py_err)?;
        let _capabilities = provider
            .probe_capabilities(&secret, &cancel)
            .await
            .map_err(to_py_err)?;
        Python::with_gil(|py| {
            let session = AsyncMysqlSession {
                provider,
                secret,
                server_version: connection.server_version,
                closed: false,
            };
            Ok(Py::new(py, session)?.into_pyobject(py)?.into_any().unbind())
        })
    })
}
