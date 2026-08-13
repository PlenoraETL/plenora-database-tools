//! Gerarchia di eccezioni Python (F3-6a).
//!
//! Mappa `DatabaseError.category` a una classe di eccezione Python
//! dedicata. Tutte ereditano da `PlenoraError`, che a sua volta eredita
//! da `RuntimeError` per retro-compat: chi filtrava su `RuntimeError`
//! continua a intercettare tutti i nostri errori.
//!
//! L'istanza dell'errore porta come attributi:
//!   - `category` (str, snake_case della categoria: "schema", "not_found", ...)
//!   - `phase` (str, snake_case della fase: "read", "write", "commit", ...)
//!   - `retry` (str: "safe", "requires_recovery", "never", "quarantine")
//!   - `remote_effect` (str: "committed", "rolled_back", "unknown", "none")
//!   - `provider` (str, "postgres" / "mysql" / "sqlserver" / None)
//!   - `execution_id` (str o None)
//!   - `diagnostics` (dict decodificato dal Value serde) o None
//!
//! Il messaggio testuale (`str(exc)`) è "<category>: <message>".

#![allow(clippy::doc_markdown)]

use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};
use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

// Base + sottoclassi. Il modulo dichiarato deve essere `plenora_database._native`
// per corrispondere al pymodule di lib.rs.
create_exception!(plenora_database._native, PlenoraError, PyRuntimeError);
create_exception!(plenora_database._native, PlenoraInvalidPlanError, PlenoraError);
create_exception!(
    plenora_database._native,
    PlenoraInvalidConfigurationError,
    PlenoraError
);
create_exception!(plenora_database._native, PlenoraSchemaError, PlenoraError);
create_exception!(plenora_database._native, PlenoraDataMappingError, PlenoraError);
create_exception!(plenora_database._native, PlenoraCrsError, PlenoraError);
create_exception!(plenora_database._native, PlenoraUnsupportedError, PlenoraError);
create_exception!(plenora_database._native, PlenoraNotFoundError, PlenoraError);
create_exception!(plenora_database._native, PlenoraConflictError, PlenoraError);
create_exception!(
    plenora_database._native,
    PlenoraConcurrentModificationError,
    PlenoraError
);
create_exception!(
    plenora_database._native,
    PlenoraAuthenticationError,
    PlenoraError
);
create_exception!(
    plenora_database._native,
    PlenoraAuthorizationError,
    PlenoraError
);
create_exception!(plenora_database._native, PlenoraTimeoutError, PlenoraError);
create_exception!(plenora_database._native, PlenoraCancelledError, PlenoraError);
create_exception!(
    plenora_database._native,
    PlenoraResourceLimitError,
    PlenoraError
);
create_exception!(plenora_database._native, PlenoraIoError, PlenoraError);
create_exception!(plenora_database._native, PlenoraProtocolError, PlenoraError);
create_exception!(plenora_database._native, PlenoraTransientError, PlenoraError);
create_exception!(plenora_database._native, PlenoraExecutionError, PlenoraError);
create_exception!(plenora_database._native, PlenoraInternalError, PlenoraError);

/// Registra tutte le classi di eccezione nel pymodule.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PlenoraError", m.py().get_type::<PlenoraError>())?;
    m.add("PlenoraInvalidPlanError", m.py().get_type::<PlenoraInvalidPlanError>())?;
    m.add(
        "PlenoraInvalidConfigurationError",
        m.py().get_type::<PlenoraInvalidConfigurationError>(),
    )?;
    m.add("PlenoraSchemaError", m.py().get_type::<PlenoraSchemaError>())?;
    m.add("PlenoraDataMappingError", m.py().get_type::<PlenoraDataMappingError>())?;
    m.add("PlenoraCrsError", m.py().get_type::<PlenoraCrsError>())?;
    m.add("PlenoraUnsupportedError", m.py().get_type::<PlenoraUnsupportedError>())?;
    m.add("PlenoraNotFoundError", m.py().get_type::<PlenoraNotFoundError>())?;
    m.add("PlenoraConflictError", m.py().get_type::<PlenoraConflictError>())?;
    m.add(
        "PlenoraConcurrentModificationError",
        m.py().get_type::<PlenoraConcurrentModificationError>(),
    )?;
    m.add(
        "PlenoraAuthenticationError",
        m.py().get_type::<PlenoraAuthenticationError>(),
    )?;
    m.add(
        "PlenoraAuthorizationError",
        m.py().get_type::<PlenoraAuthorizationError>(),
    )?;
    m.add("PlenoraTimeoutError", m.py().get_type::<PlenoraTimeoutError>())?;
    m.add("PlenoraCancelledError", m.py().get_type::<PlenoraCancelledError>())?;
    m.add("PlenoraResourceLimitError", m.py().get_type::<PlenoraResourceLimitError>())?;
    m.add("PlenoraIoError", m.py().get_type::<PlenoraIoError>())?;
    m.add("PlenoraProtocolError", m.py().get_type::<PlenoraProtocolError>())?;
    m.add("PlenoraTransientError", m.py().get_type::<PlenoraTransientError>())?;
    m.add("PlenoraExecutionError", m.py().get_type::<PlenoraExecutionError>())?;
    m.add("PlenoraInternalError", m.py().get_type::<PlenoraInternalError>())?;
    Ok(())
}

/// Traduce un `DatabaseError` nella `PyErr` con la sottoclasse giusta.
/// Aggancia inoltre metadata (category, phase, retry, remote_effect,
/// provider, execution_id, diagnostics) come attributi sull'istanza.
pub fn to_py_err(err: DatabaseError) -> PyErr {
    let message = format!("{}: {}", category_name(err.category), err.message);
    let pyerr = match err.category {
        ErrorCategory::InvalidPlan => PlenoraInvalidPlanError::new_err(message),
        ErrorCategory::InvalidConfiguration => PlenoraInvalidConfigurationError::new_err(message),
        ErrorCategory::Schema => PlenoraSchemaError::new_err(message),
        ErrorCategory::DataMapping => PlenoraDataMappingError::new_err(message),
        ErrorCategory::Crs => PlenoraCrsError::new_err(message),
        ErrorCategory::Unsupported => PlenoraUnsupportedError::new_err(message),
        ErrorCategory::NotFound => PlenoraNotFoundError::new_err(message),
        ErrorCategory::Conflict => PlenoraConflictError::new_err(message),
        ErrorCategory::ConcurrentModification => {
            PlenoraConcurrentModificationError::new_err(message)
        }
        ErrorCategory::Authentication => PlenoraAuthenticationError::new_err(message),
        ErrorCategory::Authorization => PlenoraAuthorizationError::new_err(message),
        ErrorCategory::Timeout => PlenoraTimeoutError::new_err(message),
        ErrorCategory::Cancelled => PlenoraCancelledError::new_err(message),
        ErrorCategory::ResourceLimit => PlenoraResourceLimitError::new_err(message),
        ErrorCategory::Io => PlenoraIoError::new_err(message),
        ErrorCategory::Protocol => PlenoraProtocolError::new_err(message),
        ErrorCategory::Transient => PlenoraTransientError::new_err(message),
        ErrorCategory::Execution => PlenoraExecutionError::new_err(message),
        ErrorCategory::Internal => PlenoraInternalError::new_err(message),
    };
    Python::with_gil(|py| {
        let bound = pyerr.value(py);
        // Ignora errori di setattr: sono attributi di comodo, non essenziali
        // per la propagazione dell'errore stesso.
        let _ = bound.setattr("category", category_name(err.category));
        let _ = bound.setattr("phase", phase_name(err.phase));
        let _ = bound.setattr("retry", retry_name(err.retry));
        let _ = bound.setattr("remote_effect", remote_effect_name(err.remote_effect));
        let _ = bound.setattr(
            "provider",
            err.provider.map(|p| format!("{p:?}").to_lowercase()),
        );
        let _ = bound.setattr("execution_id", err.execution_id);
        // Serializza diagnostics via serde_json → string → Python json.loads,
        // così l'utente riceve un dict/list Python.
        let diagnostics_py = err
            .diagnostics
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok())
            .and_then(|json_str| {
                py.import("json")
                    .and_then(|json_mod| json_mod.getattr("loads"))
                    .and_then(|loads| loads.call1((json_str,)))
                    .ok()
            });
        let _ = bound.setattr("diagnostics", diagnostics_py);
    });
    pyerr
}

const fn category_name(c: ErrorCategory) -> &'static str {
    match c {
        ErrorCategory::InvalidPlan => "invalid_plan",
        ErrorCategory::InvalidConfiguration => "invalid_configuration",
        ErrorCategory::Schema => "schema",
        ErrorCategory::DataMapping => "data_mapping",
        ErrorCategory::Crs => "crs",
        ErrorCategory::Unsupported => "unsupported",
        ErrorCategory::NotFound => "not_found",
        ErrorCategory::Conflict => "conflict",
        ErrorCategory::ConcurrentModification => "concurrent_modification",
        ErrorCategory::Authentication => "authentication",
        ErrorCategory::Authorization => "authorization",
        ErrorCategory::Timeout => "timeout",
        ErrorCategory::Cancelled => "cancelled",
        ErrorCategory::ResourceLimit => "resource_limit",
        ErrorCategory::Io => "io",
        ErrorCategory::Protocol => "protocol",
        ErrorCategory::Transient => "transient",
        ErrorCategory::Execution => "execution",
        ErrorCategory::Internal => "internal",
    }
}

const fn phase_name(p: ErrorPhase) -> &'static str {
    match p {
        ErrorPhase::Validate => "validate",
        ErrorPhase::Connect => "connect",
        ErrorPhase::Probe => "probe",
        ErrorPhase::Prepare => "prepare",
        ErrorPhase::Read => "read",
        ErrorPhase::Write => "write",
        ErrorPhase::Commit => "commit",
        ErrorPhase::Finalize => "finalize",
        ErrorPhase::Rollback => "rollback",
        ErrorPhase::Cleanup => "cleanup",
    }
}

fn retry_name(r: RetryDisposition) -> String {
    match r {
        RetryDisposition::Never => "never".to_owned(),
        RetryDisposition::Quarantine => "quarantine".to_owned(),
        RetryDisposition::Safe => "safe".to_owned(),
        RetryDisposition::RequiresIdempotencyKey => "requires_idempotency_key".to_owned(),
        RetryDisposition::RequiresRecovery => "requires_recovery".to_owned(),
        RetryDisposition::After(ms) => format!("after:{ms}"),
    }
}

const fn remote_effect_name(r: RemoteEffect) -> &'static str {
    match r {
        RemoteEffect::None => "none",
        RemoteEffect::RolledBack => "rolled_back",
        RemoteEffect::Partial => "partial",
        RemoteEffect::Committed => "committed",
        RemoteEffect::Unknown => "unknown",
    }
}
