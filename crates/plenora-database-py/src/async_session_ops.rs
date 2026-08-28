//! Operazioni provider-neutral condivise dalle sessioni Python asincrone.

use crate::async_transaction::AsyncTransaction;
use crate::errors::to_py_err;
use crate::py_convert::{rows_to_pylist, scalar_to_python};
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{CancellationToken, Row};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

async fn run_transaction<R: Send>(
    provider: Arc<dyn Provider>,
    secret: SecretString,
    work: impl for<'a> FnOnce(
            &'a mut dyn TransactionScope,
            &'a CancellationToken,
        ) -> plenora_database_core::provider::ProviderFuture<'a, R>
        + Send,
) -> plenora_database_core::Result<R> {
    crate::session_tx::run_transaction(provider, secret, work).await
}

pub fn execute(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    statement: Statement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        run_transaction(provider, secret, move |tx, cancel| {
            Box::pin(async move { tx.execute(&statement, cancel).await })
        })
        .await
        .map_err(to_py_err)
    })
}

pub fn execute_scalar(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    statement: Statement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let rows = run_transaction(provider, secret, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })
        .await
        .map_err(to_py_err)?;
        Python::attach(|py| scalar_to_python(py, rows).map(Bound::unbind))
    })
}

pub fn execute_rows(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    statement: Statement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let rows = run_transaction(provider, secret, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })
        .await
        .map_err(to_py_err)?;
        rows_to_pyobject(rows)
    })
}

pub fn execute_ddl(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    sql: String,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        provider
            .execute_ddl(&secret, &sql, &CancellationToken::new())
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
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let inspection = provider
            .inspect(&secret, &operation, &CancellationToken::new())
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
            .inspect(&secret, &operation, &CancellationToken::new())
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
            .inspect(&secret, &operation, &CancellationToken::new())
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| {
            crate::session::json_value_to_pydict(py, &inspection.document)
                .map(|value| value.into_any().unbind())
        })
    })
}

pub fn execute_portable_rows(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    statement: PortableStatement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let rows = run_transaction(provider, secret, move |tx, cancel| {
            Box::pin(async move { execute_portable_returning(tx, &statement, cancel).await })
        })
        .await
        .map_err(to_py_err)?;
        rows_to_pyobject(rows)
    })
}

pub fn execute_portable_count(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    statement: PortableStatement,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        run_transaction(provider, secret, move |tx, cancel| {
            Box::pin(async move { execute_portable(tx, &statement, cancel).await })
        })
        .await
        .map_err(to_py_err)
    })
}

pub fn begin(
    py: Python<'_>,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    options: TransactionOptions,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let budget = crate::budget::session_budget();
        let cancellation = CancellationToken::new();
        let scope = provider
            .begin_transaction(&secret, &options, &budget, &cancellation)
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| {
            let transaction = AsyncTransaction::new(scope);
            Ok(Py::new(py, transaction)?
                .into_pyobject(py)?
                .into_any()
                .unbind())
        })
    })
}

pub fn rows_to_pyobject(rows: Vec<Row>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| rows_to_pylist(py, rows).map(|value| value.into_any().unbind()))
}
