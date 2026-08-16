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
mod async_mysql_session;
mod async_session;
mod async_transaction;
mod budget;
mod errors;
mod errors_commit;
mod mysql_arrow_reader;
mod mysql_session;
mod mysql_write;
mod py_convert;
mod session;
mod session_context_py;
mod transaction;
mod write;

use arrow_reader::{AsyncBatchReader, BatchReader};
use async_mysql_session::{aconnect_mysql, AsyncMysqlSession};
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

/// SRID che rappresentano coordinate geografiche (lat/lon in gradi).
///
/// Single-source-of-truth: la lista è definita in
/// `plenora_database_core::spatial_policy::GEOGRAPHIC_SRIDS` e
/// esposta qui perché `spatial.py` la consulti per fast-fail
/// client-side su `DWithin + Geometry + SRID geografico`.
///
/// Prima di Fase A la lista era duplicata (Rust + Python) con rischio
/// di divergenza silente.
#[pyfunction]
#[must_use]
pub fn geographic_srids() -> Vec<u32> {
    plenora_database_core::spatial_policy::GEOGRAPHIC_SRIDS.to_vec()
}

/// Valida che `srid` e `dimensions` dichiarati coincidano con quelli
/// realmente presenti nel buffer EWKB. Fix review #5.
///
/// - `dimensions` accetta `"xy"|"xyz"|"xym"|"xyzm"|"unknown"`.
///   `"unknown"` bypassa il check dimensioni.
/// - Se l'EWKB non ha SRID embedded (WKB puro), il check SRID è
///   permissivo (accetta qualsiasi srid dichiarato).
///
/// Ritorna `None` su successo, solleva `ValueError` su mismatch.
///
/// # Errors
///
/// `PyValueError` se `dimensions` non è una stringa valida oppure se
/// l'EWKB non è consistente col SRID / dimensioni dichiarate.
#[pyfunction]
pub fn validate_ewkb_reference(ewkb: &[u8], srid: u32, dimensions: &str) -> PyResult<()> {
    use plenora_database_core::geometry::Dimensions;
    use pyo3::exceptions::PyValueError;

    let dims = match dimensions {
        "xy" => Dimensions::Xy,
        "xyz" => Dimensions::Xyz,
        "xym" => Dimensions::Xym,
        "xyzm" => Dimensions::Xyzm,
        "unknown" => Dimensions::Unknown,
        other => {
            return Err(PyValueError::new_err(format!(
                "dimensions non valida: {other:?}"
            )))
        }
    };
    plenora_database_core::spatial_predicate::SpatialReference::new_validated(
        ewkb.to_vec(),
        srid,
        dims,
        plenora_database_core::geometry::SpatialSemantics::Geometry,
    )
    .map(|_| ())
    .map_err(|e| PyValueError::new_err(e.message))
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Inizializza il runtime condiviso con pyo3-async-runtimes per bridge
    // asyncio ↔ tokio. Chiamato una sola volta all'import del modulo.
    init_async_runtime();
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(geographic_srids, m)?)?;
    m.add_function(wrap_pyfunction!(validate_ewkb_reference, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect, m)?)?;
    m.add_function(wrap_pyfunction!(connect_mysql, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect_mysql, m)?)?;
    m.add_class::<Session>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<AsyncSession>()?;
    m.add_class::<AsyncTransaction>()?;
    m.add_class::<BatchReader>()?;
    m.add_class::<AsyncBatchReader>()?;
    m.add_class::<MysqlSession>()?;
    m.add_class::<AsyncMysqlSession>()?;
    m.add_class::<session_context_py::PySessionContext>()?;
    errors::register(m)?;
    Ok(())
}
