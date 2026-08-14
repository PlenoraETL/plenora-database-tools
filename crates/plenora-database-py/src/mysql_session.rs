//! MysqlSession — scaffold Python SDK per MySQL (v0.4-alpha).
//!
//! Prima esposizione MySQL nel SDK. **Scope limitato** (scaffold, non
//! feature parity):
//! - `connect_mysql(host, database, user, password, port=None, tls_ca_pem=None)`
//! - `MysqlSession.execute(sql, params) → affected_rows`
//! - `MysqlSession.execute_scalar(sql, params) → value`
//! - `MysqlSession.execute_returning_rows(sql, params) → list[dict]`
//! - `MysqlSession.execute_ddl(sql) → None`
//! - `MysqlSession.close()`, `__enter__/__exit__`, `__repr__`
//!
//! **Non incluso** (roadmap SDK MySQL):
//! - `begin()` + `Transaction` context manager
//! - `copy_from` bulk write (7 WriteMode via Arrow IPC)
//! - `read()` streaming Arrow
//! - Portable AST builders (`select/insert/update/delete/upsert`)
//! - Spatial predicates + typed params (uuid/decimal/etc.)
//! - Async variant `AsyncMysqlSession`
//!
//! Placeholder syntax MySQL: `?` (non `$1` come Postgres). Il consumer
//! deve fornire SQL provider-compatibile.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::redundant_pub_crate,
    clippy::unused_self,
)]

use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use crate::runtime;
use crate::transaction::{parse_isolation, Transaction};
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{
    AccessMode, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError};
use plenora_db_mysql::{MysqlCertificatePolicy, MysqlConfig, MysqlProvider};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;

fn default_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("default budget")
}

/// Sessione MySQL. Wrapper thin sopra `MysqlProvider`.
///
/// Prodotta da `plenora_database.connect_mysql(...)`. Context-manager
/// friendly (`with connect_mysql(...) as s:`).
#[pyclass(module = "plenora_database._native")]
pub struct MysqlSession {
    provider: Arc<MysqlProvider>,
    secret: SecretString,
    server_version: String,
    closed: bool,
}

impl MysqlSession {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(
                "sessione MySQL chiusa: aprine una nuova con plenora_database.connect_mysql(...)",
            ));
        }
        Ok(())
    }

    /// Esegue uno statement in una transazione dedicata (auto-commit stile).
    fn run_tx<F, R>(&self, py: Python<'_>, work: F) -> PyResult<R>
    where
        F: for<'a> FnOnce(
                &'a mut dyn TransactionScope,
                &'a CancellationToken,
            ) -> plenora_database_core::provider::ProviderFuture<'a, R>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        py.allow_threads(|| {
            runtime().block_on(async move {
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
                            return Err(DatabaseError {
                                category: plenora_database_core::ErrorCategory::Internal,
                                phase: plenora_database_core::ErrorPhase::Write,
                                remote_effect: plenora_database_core::RemoteEffect::None,
                                retry: plenora_database_core::RetryDisposition::Never,
                                provider: Some(
                                    plenora_database_core::plan::ProviderKind::Mysql,
                                ),
                                execution_id: None,
                                message: "commit MySQL outcome unknown: verificare stato del target"
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
            })
        })
        .map_err(to_py_err)
    }
}

#[pymethods]
impl MysqlSession {
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

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> bool {
        self.close();
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<MysqlSession server='{}' closed={}>",
            self.server_version, self.closed
        )
    }

    /// Esegue DML (INSERT/UPDATE/DELETE) senza rows. Ritorna affected_rows.
    ///
    /// SQL usa placeholder `?` (convenzione MySQL). Params in ordine posizionale.
    #[pyo3(signature = (sql, params=None))]
    fn execute(&self, py: Python<'_>, sql: &str, params: Option<Bound<'_, PyList>>) -> PyResult<u64> {
        self.ensure_open()?;
        let params = params_from_python(params.as_ref())?;
        let sql = sql.to_owned();
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move {
                let stmt = Statement { sql, params };
                tx.execute(&stmt, cancel).await
            })
        })
    }

    /// SELECT scalare — 1 riga × 1 colonna.
    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let params = params_from_python(params.as_ref())?;
        let sql = sql.to_owned();
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move {
                let stmt = Statement { sql, params };
                tx.query(&stmt, cancel).await
            })
        })?;
        let value = rows
            .first()
            .and_then(|row| row.values().first())
            .cloned()
            .unwrap_or_else(|| plenora_database_core::provider::ParameterValue::Null {
                type_name: "unknown".to_owned(),
            });
        param_to_python(py, &value)
    }

    /// SELECT con rows → list[dict] (nome colonna → valore Python).
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let params = params_from_python(params.as_ref())?;
        let sql = sql.to_owned();
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move {
                let stmt = Statement { sql, params };
                tx.query(&stmt, cancel).await
            })
        })?;
        let out = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            let columns: Vec<String> = row.columns().to_vec();
            for (idx, name) in columns.iter().enumerate() {
                let value = row.values().get(idx).cloned().unwrap_or_else(|| {
                    plenora_database_core::provider::ParameterValue::Null {
                        type_name: "unknown".to_owned(),
                    }
                });
                let py_val = param_to_python(py, &value)?;
                dict.set_item(name.as_str(), py_val)?;
            }
            out.append(dict)?;
        }
        Ok(out)
    }

    /// Apre una nuova transazione user-managed su MySQL.
    ///
    /// Uso: `with s.begin() as tx: tx.execute(...); tx.commit()`.
    /// `Transaction` è provider-agnostic (wrapper sopra `dyn TransactionScope`)
    /// e supporta savepoints, conditional_update, execute_returning_rows.
    ///
    /// Opzioni:
    /// - `isolation`: "read_uncommitted" / "read_committed" /
    ///   "repeatable_read" / "serializable" (None = default MySQL)
    /// - `read_only`: True/False (default: False)
    /// - `statement_timeout_ms`: MAX_EXECUTION_TIME session-scoped
    ///
    /// Nota: MySQL non ha `deferrable` — parametro non esposto qui.
    #[pyo3(signature = (isolation=None, read_only=None, statement_timeout_ms=None))]
    fn begin(
        &self,
        py: Python<'_>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        statement_timeout_ms: Option<u64>,
    ) -> PyResult<Transaction> {
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
        let scope = py
            .allow_threads(|| {
                runtime().block_on(async move {
                    let cancel = CancellationToken::new();
                    provider
                        .begin_transaction(&secret, &opts, &default_budget(), &cancel)
                        .await
                })
            })
            .map_err(to_py_err)?;
        Ok(Transaction::new(scope))
    }

    /// DDL raw (CREATE/DROP/ALTER). MySQL fa autocommit implicito.
    fn execute_ddl(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let sql = sql.to_owned();
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                provider.execute_ddl(&secret, &sql, &cancel).await
            })
        })
        .map_err(to_py_err)
    }
}

/// Apre una connessione MySQL e produce una `MysqlSession`.
///
/// Parametri:
/// - `host`, `database`, `user`, `password`
/// - `port`: opzionale, default 3306
/// - `tls_ca_pem`: opzionale, bytes del certificato CA privato PEM.
///   Se `None`, usa `TrustServerCertificate` (solo per sviluppo).
///
/// # Errors
///
/// `PlenoraError` se la configurazione è invalida, la connessione fallisce,
/// o il probe capabilities restituisce errore.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None))]
pub fn connect_mysql(
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
) -> PyResult<MysqlSession> {
    let secret = SecretString::new(password.to_owned());
    let mut config = MysqlConfig::new(host, database, user, secret.clone());
    if let Some(p) = port {
        config = config.with_port(p);
    }
    if let Some(pem) = tls_ca_pem {
        if pem.len() > 1024 * 1024 {
            return Err(PyRuntimeError::new_err("CA PEM MySQL oltre 1 MiB"));
        }
        config = config.with_private_ca_certificate_pem(pem);
    } else {
        config = config.with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate);
    }
    let provider = Arc::new(MysqlProvider::new(config, 4).map_err(to_py_err)?);
    let provider_probe = Arc::clone(&provider);
    let secret_probe = secret.clone();
    let (connection, _capabilities) = runtime()
        .block_on(async move {
            let cancel = CancellationToken::new();
            let conn = provider_probe.test_connection(&secret_probe, &cancel).await?;
            let caps = provider_probe
                .probe_capabilities(&secret_probe, &cancel)
                .await?;
            Ok::<_, DatabaseError>((conn, caps))
        })
        .map_err(to_py_err)?;
    Ok(MysqlSession {
        provider,
        secret,
        server_version: connection.server_version,
        closed: false,
    })
}
