//! Session Postgres esposta a Python.
//!
//! Il consumer Python fa:
//!
//! ```python
//! import plenora_database
//! with plenora_database.connect(dsn="host=localhost user=me dbname=app") as s:
//!     print(s.server_version, s.postgis_version)
//! ```
//!
//! F3-2 espone solo la parte di lifecycle (connect, close, context manager)
//! + i metadata scoperti in probe. Le API di query/exec sono F3-3.
//!
//! Runtime tokio globale (`OnceLock`) condiviso da tutte le Session: evita
//! di ricreare un runtime per ogni chiamata e permette di riusare il pool
//! di worker thread di tokio. Non è mai droppato durante la vita del
//! processo Python.

// Suppressioni per idiomi PyO3:
// - doc_markdown: firma dei pymethod cita nomi Python (close, __enter__)
//   che non sono item Rust e non vogliamo backtick-are ovunque.
// - missing_const_for_fn: i #[pymethods] non possono essere const per via
//   dei macro attributi di pyo3.
// - needless_pass_by_value: __exit__ deve avere firma esatta (tre PyObject).
#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
)]

use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

/// Runtime tokio globale, inizializzato al primo uso.
fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("plenora-py")
            .build()
            .expect("build tokio runtime")
    })
}

/// Sessione Postgres. Wrapper thin sopra `PostgresProvider` + DSN + metadata
/// scoperti in probe. È un context manager: `with connect(...) as s: ...`.
///
/// La versione F3-2 non espone `execute`/`query` ancora — quelli arrivano
/// in F3-3. Qui basta validare la DSN e memorizzare cosa il probe scopre
/// sul server (versione, estensioni).
#[pyclass(module = "plenora_database._native")]
#[allow(dead_code)] // provider + secret usati da F3-3 (execute/query API)
pub struct Session {
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    server_version: String,
    postgis_version: Option<String>,
    closed: bool,
}

#[pymethods]
impl Session {
    /// Versione del server Postgres (stringa piena come da `server_version`).
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Versione dell'estensione PostGIS se installata sul target, altrimenti None.
    #[getter]
    fn postgis_version(&self) -> Option<&str> {
        self.postgis_version.as_deref()
    }

    /// True se la sessione è stata chiusa (via `close()` o uscendo dal
    /// context manager).
    #[getter]
    fn is_closed(&self) -> bool {
        self.closed
    }

    /// Marca la sessione come chiusa. Idempotente. Le risorse di connessione
    /// vengono rilasciate quando l'oggetto Python viene garbage-collected
    /// (Drop di Arc<PostgresProvider>).
    fn close(&mut self) {
        self.closed = true;
    }

    /// Context manager: entrata restituisce self.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager: uscita chiama close(). Non sopprime eccezioni
    /// (ritorna False, secondo il protocollo Python).
    fn __exit__(
        &mut self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> bool {
        self.closed = true;
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<Session server_version={:?} postgis_version={:?} closed={}>",
            self.server_version, self.postgis_version, self.closed
        )
    }
}

/// Apre una nuova sessione Postgres. La DSN è nel formato libpq
/// (`host=... user=... password=... dbname=...`).
///
/// Fa fail-fast: se la DSN è invalida, la rete non risponde o le
/// credenziali sono errate, ritorna `RuntimeError` con un messaggio
/// che include la categoria dell'errore (per orientarsi anche senza
/// stack trace).
///
/// # Errors
///
/// Restituisce `PyRuntimeError` con messaggio "<category>: <message>"
/// se il probe iniziale fallisce.
#[pyfunction]
pub fn connect(py: Python<'_>, dsn: &str) -> PyResult<Session> {
    let provider = Arc::new(PostgresProvider::default());
    let secret = SecretString::new(dsn.to_owned());
    let cancel = CancellationToken::new();
    let provider_for_probe = Arc::clone(&provider);
    let secret_for_probe = SecretString::new(dsn.to_owned());
    let caps_result = py.allow_threads(|| {
        runtime()
            .block_on(async move { provider_for_probe.probe_capabilities(&secret_for_probe, &cancel).await })
    });
    let caps = caps_result.map_err(|e| {
        PyRuntimeError::new_err(format!("{:?}: {}", e.category, e.message))
    })?;
    Ok(Session {
        provider,
        secret,
        server_version: caps.provider_version,
        postgis_version: caps.extension_versions.get("postgis").cloned(),
        closed: false,
    })
}
