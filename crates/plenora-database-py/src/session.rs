//! Session Postgres esposta a Python.
//!
//! Il consumer Python fa:
//!
//! ```python
//! import plenora_database
//! with plenora_database.connect(dsn="host=localhost user=me dbname=app") as s:
//!     print(s.server_version, s.postgis_version)
//!     affected = s.execute("INSERT INTO t(x) VALUES ($1)", [42])
//!     value = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM t")
//!     rows = s.execute_returning_rows("SELECT id, name FROM t WHERE id = $1", [1])
//! ```
//!
//! Runtime tokio globale (`OnceLock`) condiviso da tutte le Session: evita
//! di ricreare un runtime per ogni chiamata e permette di riusare il pool
//! di worker thread di tokio. Non è mai droppato durante la vita del
//! processo Python.
//!
//! Ogni chiamata `execute*` apre una transazione dedicata, esegue lo
//! statement e committa; questo dà semantica auto-commit stile psycopg
//! `autocommit=True`. Le transazioni esplicite gestite dall'utente
//! (`with s.begin() as tx:`) sono milestone F3-5.

// Suppressioni per idiomi PyO3:
// - doc_markdown: firma dei pymethod cita nomi Python (close, __enter__)
//   che non sono item Rust e non vogliamo backtick-are ovunque.
// - missing_const_for_fn: i #[pymethods] non possono essere const per via
//   dei macro attributi di pyo3.
// - needless_pass_by_value: __exit__ deve avere firma esatta (tre PyObject).
#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
)]

use crate::py_convert::{param_to_python, params_from_python};
use crate::runtime;
use crate::transaction::{parse_isolation, Transaction};
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{AccessMode, Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, DatabaseError, Row};
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;

fn default_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("default budget")
}

/// Trasforma un errore Rust in RuntimeError Python con prefisso categoria.
fn to_py_err(e: DatabaseError) -> PyErr {
    let diag = e
        .diagnostics
        .as_ref()
        .map(|v| format!(" [{v:?}]"))
        .unwrap_or_default();
    PyRuntimeError::new_err(format!("{:?}: {}{}", e.category, e.message, diag))
}

/// Sessione Postgres. Wrapper thin sopra `PostgresProvider` + DSN + metadata
/// scoperti in probe. È un context manager: `with connect(...) as s: ...`.
#[pyclass(module = "plenora_database._native")]
pub struct Session {
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    server_version: String,
    postgis_version: Option<String>,
    closed: bool,
}

impl Session {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(
                "sessione chiusa: aprine una nuova con plenora_database.connect(...)",
            ));
        }
        Ok(())
    }

    /// Esegue uno statement in una transazione dedicata e committa.
    fn run_tx<F, R>(&self, py: Python<'_>, work: F) -> PyResult<R>
    where
        F: for<'a> FnOnce(
                &'a mut dyn plenora_database_core::transaction::TransactionScope,
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
                                remote_effect: plenora_database_core::RemoteEffect::None,
                                retry: plenora_database_core::RetryDisposition::Never,
                                provider: None,
                                execution_id: None,
                                message: "commit outcome unknown: verificare stato del target".to_owned(),
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
impl Session {
    /// Versione del server Postgres (stringa piena come da `server_version`).
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Versione dell'estensione PostGIS se installata sul target, altrimenti None.
    #[getter]
    fn postgis_version(&self) -> Option<&str> {
        self.postgis_version.as_deref()
    }

    /// True se la sessione è stata chiusa (via `close()` o uscendo dal
    /// context manager).
    #[getter]
    fn is_closed(&self) -> bool {
        self.closed
    }

    /// Marca la sessione come chiusa. Idempotente. Le risorse di connessione
    /// vengono rilasciate quando l'oggetto Python viene garbage-collected.
    fn close(&mut self) {
        self.closed = true;
    }

    /// Esegue un statement DML/DDL e ritorna il numero di righe modificate.
    ///
    /// I placeholder sono positional-style Postgres: `$1`, `$2`, ...
    /// I parametri sono una `list` Python (o None) con valori serializzabili
    /// per il type mapping (`py_convert`).
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<u64> {
        self.ensure_open()?;
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.execute(&statement, cancel).await })
        })
    }

    /// Esegue una query e ritorna il primo valore (prima riga, prima colonna).
    /// `None` se la query non ritorna righe.
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
        let rows: Vec<Row> = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })?;
        rows.first().and_then(|r| r.get_index(0)).map_or_else(
            || Ok(py.None().into_bound(py)),
            |v| param_to_python(py, v),
        )
    }

    /// Esegue una query e ritorna tutte le righe come lista di dict
    /// (`colonna` → `valore`).
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let rows: Vec<Row> = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })?;
        let out = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            for (col, val) in row.columns().iter().zip(row.values().iter()) {
                dict.set_item(col.as_str(), param_to_python(py, val)?)?;
            }
            out.append(dict)?;
        }
        Ok(out)
    }

    /// Esegue un `PortableStatement` (serializzato come JSON) e ritorna
    /// le righe come `list[dict]`. Usato dal layer di builder Python
    /// (`plenora_database.query`) per Select o statement con RETURNING.
    ///
    /// # Errors
    ///
    /// - `PyRuntimeError` se il JSON non è un AST valido
    /// - `PyRuntimeError` mappato da `DatabaseError` in caso di errore SQL
    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            PyRuntimeError::new_err(format!("AST portable non valida: {e}"))
        })?;
        let rows: Vec<Row> = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { execute_portable_returning(tx, &ast, cancel).await })
        })?;
        let out = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            for (col, val) in row.columns().iter().zip(row.values().iter()) {
                dict.set_item(col.as_str(), param_to_python(py, val)?)?;
            }
            out.append(dict)?;
        }
        Ok(out)
    }

    /// Esegue un `PortableStatement` (serializzato come JSON) senza
    /// RETURNING e ritorna il numero di righe modificate. Solo per
    /// Insert/Update/Delete/Upsert privi di RETURNING.
    ///
    /// # Errors
    ///
    /// Come `execute_portable_rows`.
    fn execute_portable_count(&self, py: Python<'_>, ast_json: &str) -> PyResult<u64> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            PyRuntimeError::new_err(format!("AST portable non valida: {e}"))
        })?;
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { execute_portable(tx, &ast, cancel).await })
        })
    }

    /// Apre una nuova transazione user-managed. Usa `with s.begin() as tx:`
    /// per commit/rollback automatico (rollback su eccezione, commit su
    /// uscita normale).
    ///
    /// Opzioni:
    /// - `isolation`: "read_uncommitted" / "read_committed" /
    ///   "repeatable_read" / "serializable" (None = default sessione)
    /// - `read_only`: True/False (None = default)
    /// - `deferrable`: True/False (solo effettivo con Serializable+ReadOnly)
    /// - `statement_timeout_ms`: timeout per singolo statement
    #[pyo3(signature = (
        isolation=None,
        read_only=None,
        deferrable=None,
        statement_timeout_ms=None,
    ))]
    fn begin(
        &self,
        py: Python<'_>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
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
        opts.deferrable = deferrable;
        opts.statement_timeout_ms = statement_timeout_ms;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let tx = py
            .allow_threads(|| {
                runtime().block_on(async move {
                    let cancel = CancellationToken::new();
                    provider
                        .begin_transaction(&secret, &opts, &default_budget(), &cancel)
                        .await
                })
            })
            .map_err(to_py_err)?;
        Ok(Transaction::new(tx))
    }

    /// Context manager: entrata restituisce self.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager: uscita chiama close(). Non sopprime eccezioni
    /// (ritorna False, secondo il protocollo Python).
    fn __exit__(
        &mut self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> bool {
        self.closed = true;
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<Session server_version={:?} postgis_version={:?} closed={}>",
            self.server_version, self.postgis_version, self.closed
        )
    }
}

/// Apre una nuova sessione Postgres. La DSN è nel formato libpq
/// (`host=... user=... password=... dbname=...`).
///
/// # Errors
///
/// Restituisce `PyRuntimeError` con messaggio "<category>: <message>"
/// se il probe iniziale fallisce.
#[pyfunction]
pub fn connect(py: Python<'_>, dsn: &str) -> PyResult<Session> {
    let provider = Arc::new(PostgresProvider::default());
    let secret = SecretString::new(dsn.to_owned());
    let cancel = CancellationToken::new();
    let provider_for_probe = Arc::clone(&provider);
    let secret_for_probe = SecretString::new(dsn.to_owned());
    let caps_result = py.allow_threads(|| {
        runtime().block_on(async move {
            provider_for_probe
                .probe_capabilities(&secret_for_probe, &cancel)
                .await
        })
    });
    let caps = caps_result.map_err(to_py_err)?;
    Ok(Session {
        provider,
        secret,
        server_version: caps.provider_version,
        postgis_version: caps.extension_versions.get("postgis").cloned(),
        closed: false,
    })
}
