//! Bindings Python (`PyO3`) di `plenora-database-tools`.
//!
//! F3-1 skeleton: espone solo `version()` per validare la toolchain
//! `maturin` + `PyO3` + abi3 end-to-end. Le API vere (Session, execute,
//! portable AST, spatial, transaction context) arrivano nelle milestone
//! successive F3-2..F3-8.
//!
//! Il modulo nativo è compilato come `plenora_database._native`; i
//! wrapper Python idiomatici vivono in `python/plenora_database/__init__.py`.

use pyo3::prelude::*;

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
    Ok(())
}
