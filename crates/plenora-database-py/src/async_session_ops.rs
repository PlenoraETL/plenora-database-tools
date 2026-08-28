//! Operazioni provider-neutral condivise dalle sessioni Python asincrone.

#![allow(clippy::redundant_pub_crate, clippy::significant_drop_tightening)]

use crate::async_transaction::AsyncTransaction;
use crate::errors::to_py_err;
use crate::py_convert::{rows_to_pylist, scalar_to_python};
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{CancellationToken, Row};
use plenora_database_engine::Session as EngineSession;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) type SharedEngineSession = Arc<Mutex<Option<EngineSession>>>;

#[derive(Clone)]
pub(crate) enum TransactionBackend {
    Engine(SharedEngineSession, CancellationToken),
}

async fn run_with_backend<R: Send>(
    backend: TransactionBackend,
    work: impl for<'a> FnOnce(
            &'a mut dyn TransactionScope,
            &'a CancellationToken,
        ) -> plenora_database_core::provider::ProviderFuture<'a, R>
        + Send,
) -> plenora_database_core::Result<R> {
    let TransactionBackend::Engine(session, cancellation) = backend;
    let mut guard = session.lock().await;
    let current = guard.as_mut().ok_or_else(|| {
        plenora_database_core::DatabaseError::new(
            plenora_database_core::ErrorCategory::InvalidConfiguration,
            plenora_database_core::ErrorPhase::Connect,
            None,
            "sessione Engine chiusa o trasferita a una transazione",
        )
    })?;
    crate::session_tx::run_engine_transaction(current, &cancellation, work).await
}

pub(crate) fn execute_with_backend(
    py: Python<'_>,
    backend: TransactionBackend,
    statement: Statement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        run_with_backend(backend, move |tx, cancel| {
            Box::pin(async move { tx.execute(&statement, cancel).await })
        })
        .await
        .map_err(to_py_err)
    })
}

pub(crate) fn execute_scalar_with_backend(
    py: Python<'_>,
    backend: TransactionBackend,
    statement: Statement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let rows = run_with_backend(backend, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })
        .await
        .map_err(to_py_err)?;
        Python::attach(|py| scalar_to_python(py, rows).map(Bound::unbind))
    })
}

pub(crate) fn execute_rows_with_backend(
    py: Python<'_>,
    backend: TransactionBackend,
    statement: Statement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let rows = run_with_backend(backend, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })
        .await
        .map_err(to_py_err)?;
        rows_to_pyobject(rows)
    })
}

pub(crate) fn execute_portable_rows_with_backend(
    py: Python<'_>,
    backend: TransactionBackend,
    statement: PortableStatement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let rows = run_with_backend(backend, move |tx, cancel| {
            Box::pin(async move { execute_portable_returning(tx, &statement, cancel).await })
        })
        .await
        .map_err(to_py_err)?;
        rows_to_pyobject(rows)
    })
}

pub(crate) fn execute_portable_count_with_backend(
    py: Python<'_>,
    backend: TransactionBackend,
    statement: PortableStatement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        run_with_backend(backend, move |tx, cancel| {
            Box::pin(async move { execute_portable(tx, &statement, cancel).await })
        })
        .await
        .map_err(to_py_err)
    })
}

pub(crate) fn begin_engine(
    py: Python<'_>,
    session: SharedEngineSession,
    options: TransactionOptions,
    cancellation: CancellationToken,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let current = session.lock().await.take().ok_or_else(|| {
            PyRuntimeError::new_err("sessione Engine chiusa o gia trasferita a una transazione")
        })?;
        let scope = current
            .begin_owned_transaction(&options, &crate::budget::session_budget(), &cancellation)
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| {
            let transaction = AsyncTransaction::new(Box::new(scope));
            Ok(Py::new(py, transaction)?
                .into_pyobject(py)?
                .into_any()
                .unbind())
        })
    })
}

pub fn execute_ddl(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    sql: String,
    cancellation: CancellationToken,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        provider
            .execute_ddl(&secret, &sql, &cancellation)
            .await
            .map_err(to_py_err)
    })
}

pub fn inspect_strings<'py>(
    py: Python<'py>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    operation: Operation,
    key: &'static str,
    cancellation: CancellationToken,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let inspection = provider
            .inspect(&secret, &operation, &cancellation)
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| {
            crate::session::json_to_pylist_of_strings(py, &inspection.document, key)
                .map(|value| value.into_any().unbind())
        })
    })
}

pub fn inspect_objects(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    schema: String,
    cancellation: CancellationToken,
) -> PyResult<Bound<'_, PyAny>> {
    let operation = Operation::DatabaseListObjects {
        source: Some(ObjectRef {
            catalog: None,
            schema: Some(schema),
            object: String::new(),
        }),
    };
    future_into_py(py, async move {
        let inspection = provider
            .inspect(&secret, &operation, &cancellation)
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| {
            crate::session::json_to_pylist_of_dicts(py, &inspection.document, "objects")
                .map(|value| value.into_any().unbind())
        })
    })
}

pub fn inspect_describe(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    schema: String,
    object: String,
    cancellation: CancellationToken,
) -> PyResult<Bound<'_, PyAny>> {
    let operation = Operation::DatabaseDescribeObject {
        source: ObjectRef {
            catalog: None,
            schema: Some(schema),
            object,
        },
    };
    future_into_py(py, async move {
        let inspection = provider
            .inspect(&secret, &operation, &cancellation)
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| {
            crate::session::json_value_to_pydict(py, &inspection.document)
                .map(|value| value.into_any().unbind())
        })
    })
}

pub fn rows_to_pyobject(rows: Vec<Row>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| rows_to_pylist(py, rows).map(|value| value.into_any().unbind()))
}
