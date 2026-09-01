//! Bindings Python (`PyO3`) di `plenora-database-tools`.
//!
//! Espone sessioni sync e async per `PostgreSQL`, `MySQL`/`MariaDB`, `SQL`
//! Server e `IBM Db2 LUW`, preservando i contratti comuni su transazioni,
//! Arrow e query portabili.
//!
//! Il modulo nativo e compilato come `plenora_database._native`; i wrapper
//! Python idiomatici vivono in `python/plenora_database/__init__.py`, che e
//! anche dove sta la conversione degli input di `copy_from` verso Arrow IPC.

use pyo3::prelude::*;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

mod arrow_reader;
mod async_session;
mod async_session_family;
mod async_session_ops;
mod async_transaction;
mod budget;
mod checkpoint;
mod engine;
mod errors;
mod errors_commit;
mod family_arrow_reader;
mod family_write;
mod py_convert;
mod relational_query;
mod session;
mod session_context_py;
mod session_family;
mod session_tx;
mod transaction;
mod write;

use arrow_reader::{AsyncBatchReader, BatchReader};
use async_session::{aconnect, init_async_runtime, AsyncSession};
use async_session_family::{
    aconnect_db2, aconnect_mariadb, aconnect_mysql, aconnect_sqlserver, AsyncDatabaseSession,
};
use async_transaction::AsyncTransaction;
use checkpoint::PyReadCheckpoint;
use engine::{
    create_async_db2_engine, create_async_engine, create_async_mariadb_engine,
    create_async_mysql_engine, create_async_sqlserver_engine, create_db2_engine, create_engine,
    create_mariadb_engine, create_mysql_engine, create_sqlserver_engine, PyAsyncEngine, PyEngine,
};
use session::{connect, Session};
use session_family::{
    connect_db2, connect_mariadb, connect_mysql, connect_sqlserver, DatabaseSession,
};
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
/// fallire per limiti di thread o descrittori; il binding lo converte in un
/// `ImportError` stabile invece di propagare un panico.
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
#[pyfunction]
#[must_use]
pub fn geographic_srids() -> Vec<u32> {
    plenora_database_core::spatial_policy::GEOGRAPHIC_SRIDS.to_vec()
}

/// Valida che `srid` e `dimensions` dichiarati coincidano con quelli
/// realmente presenti nel buffer EWKB.
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

/// Valida frame EWKB e restituisce il tipo geometrico canonico della radice.
///
/// # Errors
///
/// `PyValueError` se il buffer non e un EWKB valido, il frame non coincide o
/// il tipo radice non appartiene al catalogo geometrico canonico.
#[pyfunction]
pub fn inspect_ewkb_geometry_type(ewkb: &[u8], srid: u32, dimensions: &str) -> PyResult<String> {
    use pyo3::exceptions::PyValueError;

    validate_ewkb_reference(ewkb, srid, dimensions)?;
    let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(ewkb, u64::MAX, u64::MAX)
        .map_err(|error| PyValueError::new_err(error.message))?;
    inspection
        .root
        .geometry_type_name()
        .map(str::to_owned)
        .ok_or_else(|| PyValueError::new_err("tipo geometrico EWKB non riconosciuto"))
}

/// Converte il solo involucro EWKB XY qualificato in WKB per MySQL/MariaDB.
///
/// I due prodotti non accettano il flag SRID EWKB dentro
/// `ST_GeomFromWKB`; ricevono il frame come secondo argomento SQL. La
/// conversione rimuove quindi l'SRID della radice dopo aver validato l'intero
/// payload. Un SRID annidato o una coordinata Z/M restano fail-closed.
///
/// # Errors
///
/// `PyValueError` se il buffer non e valido o contiene una forma non
/// qualificata per il percorso OLTP MySQL/MariaDB.
#[pyfunction]
pub fn geometry_wkb_xy<'py>(
    py: Python<'py>,
    ewkb: &[u8],
) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
    use pyo3::exceptions::PyValueError;

    let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(ewkb, u64::MAX, u64::MAX)
        .map_err(|error| PyValueError::new_err(error.message))?;
    if inspection.has_any_z || inspection.has_any_m {
        return Err(PyValueError::new_err(
            "Geometry ORM MySQL/MariaDB qualifica soltanto coordinate XY",
        ));
    }
    let root_has_srid = inspection.root.srid.is_some();
    if inspection.embedded_srid_count != u64::from(root_has_srid) {
        return Err(PyValueError::new_err(
            "Geometry ORM MySQL/MariaDB non qualifica SRID EWKB annidati",
        ));
    }
    let wkb = geometry_wkb_bytes(ewkb, root_has_srid)?;
    Ok(pyo3::types::PyBytes::new(py, &wkb))
}

/// Converte l'involucro EWKB qualificato nel WKB ISO accettato dai
/// costruttori spatial di SQL Server e Db2.
///
/// Il frame SRID resta separato nell'IR e nel costruttore SQL. Gli SRID
/// annidati restano rifiutati: rimuoverli richiederebbe riscrivere l'intero
/// albero e nessun gate ORM pubblica quella forma.
///
/// # Errors
///
/// `PyValueError` se il buffer non e valido o contiene SRID annidati.
#[pyfunction]
pub fn geometry_wkb<'py>(
    py: Python<'py>,
    ewkb: &[u8],
) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
    use pyo3::exceptions::PyValueError;

    let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(ewkb, u64::MAX, u64::MAX)
        .map_err(|error| PyValueError::new_err(error.message))?;
    let root_has_srid = inspection.root.srid.is_some();
    if inspection.embedded_srid_count != u64::from(root_has_srid) {
        return Err(PyValueError::new_err(
            "Geometry ORM non qualifica SRID EWKB annidati",
        ));
    }
    let wkb = geometry_wkb_bytes(ewkb, root_has_srid)?;
    Ok(pyo3::types::PyBytes::new(py, &wkb))
}

fn geometry_wkb_bytes(ewkb: &[u8], root_has_srid: bool) -> PyResult<Vec<u8>> {
    use pyo3::exceptions::PyValueError;

    let endian = *ewkb
        .first()
        .ok_or_else(|| PyValueError::new_err("payload EWKB troncato"))?;
    let raw: [u8; 4] = ewkb
        .get(1..5)
        .ok_or_else(|| PyValueError::new_err("payload EWKB troncato"))?
        .try_into()
        .map_err(|_| PyValueError::new_err("payload EWKB troncato"))?;
    let mut type_word = match endian {
        0 => u32::from_be_bytes(raw),
        1 => u32::from_le_bytes(raw),
        _ => return Err(PyValueError::new_err("byte order EWKB non valido")),
    };
    let has_z = type_word & 0x8000_0000 != 0;
    let has_m = type_word & 0x4000_0000 != 0;
    type_word &= 0x0fff_ffff;
    if has_z {
        type_word = type_word
            .checked_add(1_000)
            .ok_or_else(|| PyValueError::new_err("tipo EWKB non rappresentabile come WKB ISO"))?;
    }
    if has_m {
        type_word = type_word
            .checked_add(2_000)
            .ok_or_else(|| PyValueError::new_err("tipo EWKB non rappresentabile come WKB ISO"))?;
    }
    let removed = if root_has_srid { 4 } else { 0 };
    let mut wkb = Vec::with_capacity(ewkb.len().saturating_sub(removed));
    wkb.push(endian);
    wkb.extend_from_slice(&match endian {
        0 => type_word.to_be_bytes(),
        _ => type_word.to_le_bytes(),
    });
    let body_offset = if root_has_srid { 9 } else { 5 };
    wkb.extend_from_slice(
        ewkb.get(body_offset..)
            .ok_or_else(|| PyValueError::new_err("payload EWKB troncato"))?,
    );
    Ok(wkb)
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
    m.add_function(wrap_pyfunction!(inspect_ewkb_geometry_type, m)?)?;
    m.add_function(wrap_pyfunction!(geometry_wkb_xy, m)?)?;
    m.add_function(wrap_pyfunction!(geometry_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(
        relational_query::compile_relational_query,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        relational_query::compile_relational_mutation,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(create_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_async_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_mysql_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_async_mysql_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_mariadb_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_async_mariadb_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_sqlserver_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_async_sqlserver_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_db2_engine, m)?)?;
    m.add_function(wrap_pyfunction!(create_async_db2_engine, m)?)?;
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
    m.add_function(wrap_pyfunction!(connect_db2, m)?)?;
    m.add_function(wrap_pyfunction!(aconnect_db2, m)?)?;
    m.add_class::<Session>()?;
    m.add_class::<PyEngine>()?;
    m.add_class::<PyAsyncEngine>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<AsyncSession>()?;
    m.add_class::<AsyncTransaction>()?;
    m.add_class::<BatchReader>()?;
    m.add_class::<AsyncBatchReader>()?;
    m.add_class::<DatabaseSession>()?;
    m.add_class::<AsyncDatabaseSession>()?;
    m.add_class::<PyReadCheckpoint>()?;
    m.add_class::<session_context_py::PySessionContext>()?;
    errors::register(m)?;
    Ok(())
}
