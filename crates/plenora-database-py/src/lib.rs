//! Bindings Python (`PyO3`) di `plenora-database-tools`.
//!
//! Milestone corrente: F3-2 (`Session` + `connect()` context manager, Postgres).
//! Le API di query / spatial / transaction arrivano in F3-3..F3-8.
//!
//! Il modulo nativo è compilato come `plenora_database._native`; i
//! wrapper Python idiomatici vivono in `python/plenora_database/__init__.py`.

use pyo3::prelude::*;

mod py_convert;
mod session;

use session::{connect, Session};

/// Versione del bindings crate. Coincide con la versione del workspace
/// Rust, che è la fonte di verità per compatibilità API.
#[pyfunction]
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Session>()?;
    Ok(())
}
