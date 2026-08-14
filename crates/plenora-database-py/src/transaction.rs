//! Transaction esposta a Python (F3-5).
//!
//! Il consumer usa:
//!
//! ```python
//! with s.begin() as tx:
//!     tx.execute("INSERT ...")
//!     row = tx.select("t").where_eq("id", 1).one()
//! # commit su exit senza eccezione, rollback su eccezione
//! ```
//!
//! Wrappa `Box<dyn TransactionScope>` in `Option` perché `commit`/`rollback`
//! consumano il box. Una volta consumato, i metodi successivi ritornano
//! `RuntimeError("transaction non attiva")`.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
)]

use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use crate::runtime;
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::transaction::{ConditionalUpdate, Statement, TransactionScope};
use plenora_database_core::{CancellationToken, DatabaseError, Row};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

fn tx_closed_error() -> PyErr {
    PyRuntimeError::new_err(
        "transaction non attiva: già committata o rollback-ata (o chiusa dal context manager)",
    )
}

/// Transazione applicativa. Ottenuta da `Session.begin(...)`. Espone gli
/// stessi metodi di execute/scalar/rows/portable di Session, ma tutti
/// operano nella stessa transazione (nessun auto-commit per-call).
///
/// `unsendable`: la transazione tiene la connessione Postgres pool-owned
/// e non può migrare fra thread Python. PyO3 lo enforca a runtime.
#[pyclass(module = "plenora_database._native", unsendable)]
pub struct Transaction {
    inner: Option<Box<dyn TransactionScope>>,
}

impl Transaction {
    pub(crate) fn new(inner: Box<dyn TransactionScope>) -> Self {
        Self { inner: Some(inner) }
    }

    fn tx_mut(&mut self) -> PyResult<&mut Box<dyn TransactionScope>> {
        self.inner.as_mut().ok_or_else(tx_closed_error)
    }
}

#[pymethods]
impl Transaction {
    /// True se la transazione è ancora attiva (non committata né rollback-ata).
    #[getter]
    fn is_active(&self) -> bool {
        self.inner.is_some()
    }

    /// Esegue uno statement DML/DDL nella transazione. Ritorna affected_rows.
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &mut self,
        py: Python<'_>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<u64> {
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let tx = self.tx_mut()?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                tx.execute(&statement, &cancel).await
            })
        })
        .map_err(to_py_err)
    }

    /// Esegue una query nella transazione e ritorna la prima cella o None.
    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &mut self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rows = self.execute_returning_rows(py, sql, params)?;
        if rows.is_empty() {
            return Ok(py.None().into_bound(py));
        }
        let first = rows.get_item(0)?.downcast_into::<PyDict>()?;
        first.values().iter().next().map_or_else(
            || Ok(py.None().into_bound(py)),
            Ok,
        )
    }

    /// Esegue una query nella transazione e ritorna tutte le righe come
    /// `list[dict]`.
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rows<'py>(
        &mut self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let tx = self.tx_mut()?;
        let rows: Vec<Row> = py
            .allow_threads(|| {
                runtime().block_on(async move {
                    let cancel = CancellationToken::new();
                    tx.query(&statement, &cancel).await
                })
            })
            .map_err(to_py_err)?;
        rows_to_pylist(py, rows)
    }

    /// Esegue un `PortableStatement` nella transazione, ritorna le righe.
    fn execute_portable_rows<'py>(
        &mut self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!("AST portable non valida: {e}")))
        })?;
        let tx = self.tx_mut()?;
        let rows: Vec<Row> = py
            .allow_threads(|| {
                runtime().block_on(async move {
                    let cancel = CancellationToken::new();
                    execute_portable_returning(&mut **tx, &ast, &cancel).await
                })
            })
            .map_err(to_py_err)?;
        rows_to_pylist(py, rows)
    }

    /// Esegue un `PortableStatement` (senza RETURNING) nella transazione.
    fn execute_portable_count(&mut self, py: Python<'_>, ast_json: &str) -> PyResult<u64> {
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!("AST portable non valida: {e}")))
        })?;
        let tx = self.tx_mut()?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                execute_portable(&mut **tx, &ast, &cancel).await
            })
        })
        .map_err(to_py_err)
    }

    /// Esegue un update ottimistico condizionato. Distingue
    /// esplicitamente NotFound (chiave assente) da
    /// ConcurrentModification (chiave esiste ma versione diversa).
    ///
    /// - `update_sql`: statement UPDATE tipicamente con clausola
    ///   `WHERE key = $... AND version = $...`. Deve modificare
    ///   esattamente `expected_affected_rows` righe (default 1).
    /// - `key_probe_sql`: OPZIONALE — un `SELECT 1 FROM ... WHERE key = ... LIMIT 1`
    ///   eseguito quando l'update NON matcha `expected_affected_rows`.
    ///   Se ritorna almeno una riga → `ConcurrentModification`,
    ///   altrimenti → `NotFound`.
    ///   Senza probe, tutti i mismatch classificati come
    ///   `ConcurrentModification` (default conservativo).
    ///
    /// # Errors
    ///
    /// - `PlenoraNotFoundError` se `key_probe` conferma l'assenza
    /// - `PlenoraConcurrentModificationError` altrimenti
    #[pyo3(signature = (
        update_sql,
        update_params=None,
        expected_affected_rows=1,
        key_probe_sql=None,
        key_probe_params=None,
    ))]
    fn conditional_update(
        &mut self,
        py: Python<'_>,
        update_sql: &str,
        update_params: Option<Bound<'_, PyList>>,
        expected_affected_rows: u64,
        key_probe_sql: Option<&str>,
        key_probe_params: Option<Bound<'_, PyList>>,
    ) -> PyResult<()> {
        let update_values = params_from_python(update_params.as_ref())?;
        let update_stmt =
            Statement::new(update_sql.to_owned()).with_params(update_values);
        let probe_stmt = if let Some(sql) = key_probe_sql {
            let probe_values = params_from_python(key_probe_params.as_ref())?;
            Some(Statement::new(sql.to_owned()).with_params(probe_values))
        } else {
            None
        };
        let tx = self.tx_mut()?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                let request = ConditionalUpdate {
                    update: &update_stmt,
                    key_probe: probe_stmt.as_ref(),
                    expected_affected_rows,
                };
                tx.execute_conditional_update(request, &cancel).await
            })
        })
        .map_err(to_py_err)
    }

    /// Apre un savepoint annidato con il nome dato (solo identificatori
    /// semplici `[A-Za-z_][A-Za-z0-9_]*`).
    fn savepoint(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        let tx = self.tx_mut()?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                tx.savepoint(name, &cancel).await
            })
        })
        .map_err(to_py_err)
    }

    /// Rollback al savepoint indicato. Il savepoint resta aperto (usa
    /// `release_savepoint` per chiuderlo).
    fn rollback_to_savepoint(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        let tx = self.tx_mut()?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                tx.rollback_to_savepoint(name, &cancel).await
            })
        })
        .map_err(to_py_err)
    }

    /// Chiude il savepoint (equivalente a `RELEASE SAVEPOINT`).
    fn release_savepoint(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        let tx = self.tx_mut()?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                tx.release_savepoint(name, &cancel).await
            })
        })
        .map_err(to_py_err)
    }

    /// Committa la transazione. La consuma: chiamate successive sui metodi
    /// ritornano `RuntimeError("transaction non attiva")`.
    ///
    /// Se il commit ha `OutcomeUnknown` (rara, es. disconnessione fra
    /// COMMIT e ACK), viene ritornato `RuntimeError` con messaggio
    /// che segnala l'ambiguità — la sessione va verificata out-of-band.
    fn commit(&mut self, py: Python<'_>) -> PyResult<()> {
        let tx = self.inner.take().ok_or_else(tx_closed_error)?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                let outcome = tx.commit(&cancel).await?;
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
                Ok(())
            })
        })
        .map_err(to_py_err)
    }

    /// Rollback della transazione. La consuma.
    fn rollback(&mut self, py: Python<'_>) -> PyResult<()> {
        let tx = self.inner.take().ok_or_else(tx_closed_error)?;
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                tx.rollback(&cancel).await
            })
        })
        .map_err(to_py_err)
    }

    /// Context manager: entry restituisce self.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager: exit. Se c'è stata un'eccezione (exc_type != None)
    /// fa rollback; altrimenti commit. In entrambi i casi la transazione
    /// diventa inattiva. Non sopprime eccezioni.
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<bool> {
        // Se già chiusa (commit/rollback esplicito dentro il with), no-op.
        if self.inner.is_none() {
            return Ok(false);
        }
        if exc_type.is_none(py) {
            self.commit(py)?;
        } else {
            // Best-effort rollback: se fallisce, l'errore originale
            // dell'utente è più importante. Loggheremmo qui.
            let _ = self.rollback(py);
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!(
            "<Transaction active={}>",
            self.inner.is_some()
        )
    }
}

fn rows_to_pylist(py: Python<'_>, rows: Vec<Row>) -> PyResult<Bound<'_, PyList>> {
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

/// Traduce una stringa `isolation` all'enum core.
pub fn parse_isolation(
    value: &str,
) -> PyResult<plenora_database_core::transaction::IsolationLevel> {
    use plenora_database_core::transaction::IsolationLevel;
    match value {
        "read_uncommitted" => Ok(IsolationLevel::ReadUncommitted),
        "read_committed" => Ok(IsolationLevel::ReadCommitted),
        "repeatable_read" => Ok(IsolationLevel::RepeatableRead),
        "serializable" => Ok(IsolationLevel::Serializable),
        other => Err(PyValueError::new_err(format!(
            "isolation level sconosciuto: {other:?} (attesi: read_uncommitted, read_committed, repeatable_read, serializable)"
        ))),
    }
}
