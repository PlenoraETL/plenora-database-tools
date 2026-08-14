//! Bindings Python (`PyO3`) di `plenora-database-tools`.
//!
//! Milestone corrente: F3-2 (`Session` + `connect()` context manager, Postgres).
//! Le API di query / spatial / transaction arrivano in F3-3..F3-8.
//!
//! Il modulo nativo è compilato come `plenora_database._native`; i
//! wrapper Python idiomatici vivono in `python/plenora_database/__init__.py`.

use pyo3::prelude::*;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

mod arrow_reader;
mod async_session;
mod async_transaction;
mod errors;
mod mysql_session;
mod mysql_write;
mod py_convert;
mod session;
mod transaction;
mod write;

use arrow_reader::{AsyncBatchReader, BatchReader};
use async_session::{aconnect, init_async_runtime, AsyncSession};
use async_transaction::AsyncTransaction;
use mysql_session::{connect_mysql, MysqlSession};
use session::{connect, Session};
use transaction::Transaction;

/// Runtime tokio globale condiviso da Session e Transaction. Inizializzato
/// al primo uso e mai droppato durante la vita del processo Python.
pub(crate) fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("plenora-py")
            .build()
            .expect("build tokio runtime")
    })
}

/// Versione del SDK Python. Coincide con `pyproject.toml::version` e
/// con il filename del wheel prodotto. La versione del Rust workspace
/// (driver core, provider) è indipendente.
#[pyfunction]
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Inizializza il runtime condiviso con pyo3-async-runtimes per bridge
    // asyncio ↔ tokio. Chiamato una sola volta all'import del modulo.
    init_async_runtime();
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect, m)?)?;
    m.add_function(wrap_pyfunction!(connect_mysql, m)?)?;
    m.add_class::<Session>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<AsyncSession>()?;
    m.add_class::<AsyncTransaction>()?;
    m.add_class::<BatchReader>()?;
    m.add_class::<AsyncBatchReader>()?;
    m.add_class::<MysqlSession>()?;
    errors::register(m)?;
    Ok(())
}
