//! `SessionContext` esposto a Python (PFM CHG-002).
//!
//! Wraps `plenora_database_core::session_context::SessionContext` con
//! API Python idiomatica. Il consumer PFM può popolare il context
//! prima di aprire una transazione e i valori vengono applicati
//! server-side via `SET LOCAL` (transaction-local, no leak fra riusi
//! della connessione dal pool).
//!
//! Esempio:
//!
//! ```python
//! import plenora_database as p
//! ctx = p.SessionContext()
//! ctx.insert_public("app.tenant_id", "42")
//! ctx.insert_sensitive("app.actor_email", "alice@example.com")
//!
//! with p.connect(dsn).begin(context=ctx) as tx:
//!     rows = tx.execute_returning_rows("...")
//! ```

#![allow(clippy::doc_markdown, clippy::needless_pass_by_value)]

use plenora_database_core::session_context::{
    SessionClassification, SessionContext as CoreContext, SessionEntry, SessionValue,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

/// Session context: mappa chiave (namespace.name) → valore tipizzato
/// + classificazione (public/internal/sensitive per redaction logging).
///
/// Il context è transaction-local server-side: `SET LOCAL` applicato
/// dopo `BEGIN` e resettato automaticamente al commit/rollback.
#[pyclass(module = "plenora_database._native", name = "SessionContext")]
#[derive(Clone, Default)]
pub struct PySessionContext {
    pub(crate) inner: CoreContext,
}

fn to_session_value(value: &Bound<'_, PyAny>) -> PyResult<SessionValue> {
    // Ordine: bool prima di int (bool è sottoclasse di int in Python).
    if let Ok(b) = value.extract::<bool>() {
        return Ok(SessionValue::Boolean(b));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(SessionValue::Integer(i));
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(SessionValue::Text(s));
    }
    Err(PyTypeError::new_err(format!(
        "SessionValue accetta str|int|bool, ricevuto {:?}",
        value.get_type().name()?
    )))
}

fn from_session_value(py: Python<'_>, value: &SessionValue) -> PyObject {
    match value {
        SessionValue::Text(s) => s.clone().into_pyobject(py).unwrap().into_any().unbind(),
        SessionValue::Integer(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        // PyBool ha lifetime GIL: clone del Bound prima di unbind.
        SessionValue::Boolean(b) => b
            .into_pyobject(py)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind(),
    }
}

const fn classification_str(c: SessionClassification) -> &'static str {
    match c {
        SessionClassification::Public => "public",
        SessionClassification::Internal => "internal",
        SessionClassification::Sensitive => "sensitive",
    }
}

#[pymethods]
impl PySessionContext {
    #[new]
    fn py_new() -> Self {
        Self::default()
    }

    /// Inserisce un entry `public` (ok da loggare in output esterni).
    ///
    /// Chiave: `namespace.name` con `[a-z0-9_]`, max 63 caratteri.
    fn insert_public(&mut self, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let v = to_session_value(value)?;
        self.inner
            .insert(name, SessionEntry::public(v))
            .map_err(|e| PyValueError::new_err(e.message))
    }

    /// Entry `internal`: non loggabile in output esterni, ok in metriche
    /// aggregate. Tipico per correlation_id / decision_id.
    fn insert_internal(&mut self, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let v = to_session_value(value)?;
        self.inner
            .insert(name, SessionEntry::internal(v))
            .map_err(|e| PyValueError::new_err(e.message))
    }

    /// Entry `sensitive`: mai loggabile, mai in metriche. PII, token,
    /// email, ecc. Il valore appare come `[REDACTED]` in repr/Debug.
    fn insert_sensitive(&mut self, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let v = to_session_value(value)?;
        self.inner
            .insert(name, SessionEntry::sensitive(v))
            .map_err(|e| PyValueError::new_err(e.message))
    }

    /// Ritorna il valore associato alla chiave (senza classificazione)
    /// o None se assente. Il valore restituito rispetta il tipo
    /// originale (str/int/bool).
    fn get(&self, py: Python<'_>, name: &str) -> Option<PyObject> {
        self.inner
            .get(name)
            .map(|entry| from_session_value(py, &entry.value))
    }

    /// Ritorna la classificazione dell'entry o None se la chiave
    /// non esiste. Utile per test policy di redaction.
    fn classification(&self, name: &str) -> Option<&'static str> {
        self.inner
            .get(name)
            .map(|entry| classification_str(entry.classification))
    }

    /// Numero di entries nel context.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Lista delle chiavi in ordine deterministico (BTreeMap ordering).
    fn keys(&self) -> Vec<String> {
        self.inner.iter().map(|(k, _)| k.clone()).collect()
    }

    fn __repr__(&self) -> String {
        format!("<SessionContext entries={}>", self.inner.len())
    }
}
