//! AsyncTransaction (F3-7).
//!
//! Il pattern è identico a `Transaction` sync ma i metodi ritornano
//! awaitable Python. La tx è wrappata in `Arc<tokio::Mutex<Option<...>>>`
//! perché `future_into_py` richiede il future `'static + Send`.
//!
//! - `Arc` per condividere fra le async closure senza vincoli di lifetime
//! - `tokio::Mutex` (async-aware) invece di `std::sync::Mutex` per non
//!   bloccare il worker tokio durante i lock
//! - `Option` perché `commit`/`rollback` consumano il Box<dyn TransactionScope>

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::unused_self,                  // getter è_active() usa self via Arc clone in async block
    clippy::future_not_send,              // future ha guard tokio::Mutex, non serve Send tra worker
    clippy::redundant_pub_crate,          // helper interni condivisi tra async_session/async_transaction
    clippy::significant_drop_tightening,  // MutexGuard va tenuto per l'intero body dell'async block
)]

use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::transaction::{ConditionalUpdate, Statement, TransactionScope};
use plenora_database_core::{CancellationToken, DatabaseError, Row};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

fn tx_closed_error() -> PyErr {
    PyRuntimeError::new_err(
        "transaction non attiva: già committata o rollback-ata (o chiusa dal context manager)",
    )
}

pub(crate) type SharedTx = Arc<Mutex<Option<Box<dyn TransactionScope>>>>;

pub(crate) fn wrap(tx: Box<dyn TransactionScope>) -> SharedTx {
    Arc::new(Mutex::new(Some(tx)))
}

/// Trasforma il tokio::sync::MutexGuard in `&mut dyn TransactionScope`
/// oppure ritorna PyErr se la tx è chiusa.
async fn locked_tx(
    inner: &Mutex<Option<Box<dyn TransactionScope>>>,
) -> PyResult<tokio::sync::MutexGuard<'_, Option<Box<dyn TransactionScope>>>> {
    let guard = inner.lock().await;
    if guard.is_none() {
        return Err(tx_closed_error());
    }
    Ok(guard)
}

// Rimossa `unsendable`: `Arc<tokio::Mutex<Option<Box<dyn TransactionScope>>>>`
// è Send + Sync (Mutex<Send> è Sync, Arc<Send+Sync> è Send+Sync). Necessario
// perché pyo3-async-runtimes può muovere il future tra thread del runtime
// tokio multi-thread.
#[pyclass(module = "plenora_database._native")]
pub struct AsyncTransaction {
    inner: SharedTx,
}

impl AsyncTransaction {
    pub(crate) fn new(tx: Box<dyn TransactionScope>) -> Self {
        Self { inner: wrap(tx) }
    }
}

#[pymethods]
impl AsyncTransaction {
    #[getter]
    fn is_active<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.is_some())
        })
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = locked_tx(&inner).await?;
            let tx = guard.as_mut().expect("guard checked non-None");
            let cancel = CancellationToken::new();
            tx.execute(&statement, &cancel).await.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let rows: Vec<Row> = {
                let mut guard = locked_tx(&inner).await?;
                let tx = guard.as_mut().expect("guard checked non-None");
                let cancel = CancellationToken::new();
                tx.query(&statement, &cancel).await.map_err(to_py_err)?
            };
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
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let rows: Vec<Row> = {
                let mut guard = locked_tx(&inner).await?;
                let tx = guard.as_mut().expect("guard checked non-None");
                let cancel = CancellationToken::new();
                tx.query(&statement, &cancel).await.map_err(to_py_err)?
            };
            rows_to_pyobject(rows)
        })
    }

    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!("AST portable non valida: {e}")))
        })?;
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let rows: Vec<Row> = {
                let mut guard = locked_tx(&inner).await?;
                let tx = guard.as_mut().expect("guard checked non-None");
                let cancel = CancellationToken::new();
                execute_portable_returning(&mut **tx, &ast, &cancel)
                    .await
                    .map_err(to_py_err)?
            };
            rows_to_pyobject(rows)
        })
    }

    fn execute_portable_count<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!("AST portable non valida: {e}")))
        })?;
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = locked_tx(&inner).await?;
            let tx = guard.as_mut().expect("guard checked non-None");
            let cancel = CancellationToken::new();
            execute_portable(&mut **tx, &ast, &cancel)
                .await
                .map_err(to_py_err)
        })
    }

    /// Async equivalente di `Transaction.conditional_update`.
    /// Vedi la docstring sync per la semantica.
    #[pyo3(signature = (
        update_sql,
        update_params=None,
        expected_affected_rows=1,
        key_probe_sql=None,
        key_probe_params=None,
    ))]
    fn conditional_update<'py>(
        &self,
        py: Python<'py>,
        update_sql: &str,
        update_params: Option<Bound<'_, PyList>>,
        expected_affected_rows: u64,
        key_probe_sql: Option<&str>,
        key_probe_params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let update_values = params_from_python(update_params.as_ref())?;
        let update_stmt =
            Statement::new(update_sql.to_owned()).with_params(update_values);
        let probe_stmt = if let Some(sql) = key_probe_sql {
            let probe_values = params_from_python(key_probe_params.as_ref())?;
            Some(Statement::new(sql.to_owned()).with_params(probe_values))
        } else {
            None
        };
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = locked_tx(&inner).await?;
            let tx = guard.as_mut().expect("guard checked non-None");
            let cancel = CancellationToken::new();
            let request = ConditionalUpdate {
                update: &update_stmt,
                key_probe: probe_stmt.as_ref(),
                expected_affected_rows,
            };
            tx.execute_conditional_update(request, &cancel)
                .await
                .map_err(to_py_err)
        })
    }

    fn savepoint<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = locked_tx(&inner).await?;
            let tx = guard.as_mut().expect("guard checked non-None");
            let cancel = CancellationToken::new();
            tx.savepoint(&name, &cancel).await.map_err(to_py_err)
        })
    }

    fn rollback_to_savepoint<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = locked_tx(&inner).await?;
            let tx = guard.as_mut().expect("guard checked non-None");
            let cancel = CancellationToken::new();
            tx.rollback_to_savepoint(&name, &cancel).await.map_err(to_py_err)
        })
    }

    fn release_savepoint<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = locked_tx(&inner).await?;
            let tx = guard.as_mut().expect("guard checked non-None");
            let cancel = CancellationToken::new();
            tx.release_savepoint(&name, &cancel).await.map_err(to_py_err)
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let tx = {
                let mut guard = inner.lock().await;
                guard.take().ok_or_else(tx_closed_error)?
            };
            let cancel = CancellationToken::new();
            let outcome = tx.commit(&cancel).await.map_err(to_py_err)?;
            if !outcome.is_committed() {
                return Err(to_py_err(crate::errors_commit::commit_outcome_unknown(
                    plenora_database_core::plan::ProviderKind::Postgres,
                )));
            }
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let tx = {
                let mut guard = inner.lock().await;
                guard.take().ok_or_else(tx_closed_error)?
            };
            let cancel = CancellationToken::new();
            tx.rollback(&cancel).await.map_err(to_py_err)
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj = slf.into_pyobject(py)?.into_any().unbind();
        future_into_py(py, async move { Ok(obj) })
    }

    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let is_ok = exc_type.is_none(py);
        future_into_py(py, async move {
            let tx_opt = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            // Se già chiusa (commit/rollback esplicito dentro il with), no-op.
            let Some(tx) = tx_opt else {
                return Ok(false);
            };
            let cancel = CancellationToken::new();
            if is_ok {
                let outcome = tx.commit(&cancel).await.map_err(to_py_err)?;
                if !outcome.is_committed() {
                    return Err(to_py_err(crate::errors_commit::commit_outcome_unknown(
                        plenora_database_core::plan::ProviderKind::Postgres,
                    )));
                }
            } else {
                let _ = tx.rollback(&cancel).await;
            }
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        "<AsyncTransaction>".to_owned()
    }
}

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
