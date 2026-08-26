//! Bindings Python (`PyO3`) di `plenora-database-tools`.
//!
//! Due provider esposti, ciascuno sync e async:
//!
//! * `PostgreSQL`/`PostGIS` — `connect` / `aconnect`, con spatial predicates
//!   e `SpatialReference`;
//! * `MySQL` — `connect_mysql` / `aconnect_mysql`, senza spatial.
//! * `MariaDB` — `connect_mariadb` / `aconnect_mariadb`, stessa superficie
//!   e provider distinto: il prodotto lo dichiara il consumatore, e la
//!   probe verifica quella scelta invece di compierla (ADR 0014).
//!
//! Su entrambi: `execute` / `execute_scalar` / `execute_returning_rows` /
//! `execute_ddl`, `begin` con isolamento, savepoint, `SessionContext` e
//! `native_query_policy`, lettura Arrow IPC bounded (`read` / `aread`),
//! bulk write (`copy_from` / `acopy_from`) e builder AST portabili
//! (`select`/`insert`/`update`/`delete`/`upsert`).
//!
//! `SQL Server` resta raggiungibile solo dal driver Rust: nessun binding.
//!
//! Il modulo nativo e compilato come `plenora_database._native`; i wrapper
//! Python idiomatici vivono in `python/plenora_database/__init__.py`, che e
//! anche dove sta la conversione degli input di `copy_from` verso Arrow IPC.

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
use async_mysql_session::{
    aconnect_mariadb, aconnect_mysql, aconnect_sqlserver, AsyncMysqlSession,
};
use async_session::{aconnect, init_async_runtime, AsyncSession};
use async_transaction::AsyncTransaction;
use mysql_session::{connect_mariadb, connect_mysql, connect_sqlserver, MysqlSession};
use session::{connect, Session};
use transaction::Transaction;

/// Runtime tokio globale condiviso da Session e Transaction. Costruito una
/// sola volta all'import del modulo `_native` e mai droppato durante la vita
/// del processo Python.
static RT: OnceLock<Runtime> = OnceLock::new();

/// Costruisce il runtime, oppure descrive perche non e stato possibile.
///
/// Idempotente: la seconda chiamata restituisce quello gia costruito.
///
/// `Builder::build()` fa I/O — apre l'event loop e avvia i worker — e puo
/// fallire per limiti di thread o di descrittori del processo. Prima quel
/// fallimento passava per un `expect`, cioe un panico durante l'import del
/// modulo: Python lo vedeva come `pyo3_runtime.PanicException`, senza classe
/// d'errore stabile su cui un chiamante possa ragionare. Ora l'import
/// restituisce un `ImportError` con il motivo.
fn build_runtime() -> std::result::Result<&'static Runtime, String> {
    if let Some(existing) = RT.get() {
        return Ok(existing);
    }
    let built = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("plenora-py")
        .build()
        .map_err(|error| format!("runtime tokio non avviabile: {error}"))?;
    // `set` fallisce solo se un'altra thread ha vinto la corsa: in quel caso
    // il runtime buono e il suo, e il nostro viene droppato. Va bene.
    drop(RT.set(built));
    RT.get()
        .ok_or_else(|| "runtime tokio non registrato".to_owned())
}

/// Il runtime condiviso.
///
/// # Panics
///
/// Solo se invocata prima che l'import del modulo `_native` sia riuscito, il
/// che non e raggiungibile da Python: `#[pymodule]` costruisce il runtime e,
/// se non ci riesce, l'import fallisce e nessuna di queste funzioni diventa
/// chiamabile.
pub(crate) fn runtime() -> &'static Runtime {
    RT.get()
        .expect("runtime costruito dall'inizializzazione del modulo _native")
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
        _ => {
            return Err(PyValueError::new_err(
                "dimensions non valida: attesi xy, xyz, xym, xyzm, unknown",
            ))
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
    //
    // Fallisce chiuso: se il runtime non parte, l'import del modulo non
    // riesce e nessuna funzione qui sotto diventa raggiungibile. E' anche
    // l'invariante che rende infallibile `runtime()`.
    init_async_runtime().map_err(pyo3::exceptions::PyImportError::new_err)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(geographic_srids, m)?)?;
    m.add_function(wrap_pyfunction!(validate_ewkb_reference, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect, m)?)?;
    m.add_function(wrap_pyfunction!(connect_mysql, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect_mysql, m)?)?;
    // Le due superfici esplicite di `MariaDB`. ADR 0014: il consumatore
    // dichiara il prodotto, la probe verifica quella scelta invece di
    // compierla.
    m.add_function(wrap_pyfunction!(connect_mariadb, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect_mariadb, m)?)?;
    m.add_function(wrap_pyfunction!(connect_sqlserver, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect_sqlserver, m)?)?;
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
