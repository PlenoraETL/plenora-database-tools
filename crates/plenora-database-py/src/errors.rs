//! Gerarchia delle eccezioni Python pubbliche.
//!
//! Mappa `DatabaseError.category` a una classe di eccezione Python
//! dedicata. Tutte ereditano da `PlenoraError`, che a sua volta eredita
//! da `RuntimeError` per retro-compat: chi filtrava su `RuntimeError`
//! continua a intercettare tutti i nostri errori.
//!
//! L'istanza dell'errore porta come attributi:
//!   - `category` (str, snake_case della categoria: "schema", "not_found", ...)
//!   - `phase` (str, snake_case della fase: "read", "write", "commit", ...)
//!   - `retry` (dict conforme all'asse `retry` di `plenora-error-v1`)
//!   - `remote_effect` (str: "committed", "rolled_back", "unknown", "none")
//!   - `provider` (str, "postgres" / "mysql" / "sqlserver" / None)
//!   - `execution_id` (str o None)
//!   - `message` (str redatta e bounded)
//!   - `details` (dict con eventuale `row_diagnostics`) o None
//!   - `diagnostics` (alias compatibile delle sole diagnostiche di riga) o None
//!   - `parameter_index`, `portable_type`, `target_type` per un errore di bind
//!     diagnosticato prima dell'esecuzione; altrimenti None
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
create_exception!(
    plenora_database._native,
    PlenoraInvalidPlanError,
    PlenoraError
);
create_exception!(
    plenora_database._native,
    PlenoraInvalidConfigurationError,
    PlenoraError
);
create_exception!(plenora_database._native, PlenoraSchemaError, PlenoraError);
create_exception!(
    plenora_database._native,
    PlenoraDataMappingError,
    PlenoraError
);
create_exception!(plenora_database._native, PlenoraCrsError, PlenoraError);
create_exception!(
    plenora_database._native,
    PlenoraUnsupportedError,
    PlenoraError
);
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
create_exception!(
    plenora_database._native,
    PlenoraCancelledError,
    PlenoraError
);
create_exception!(
    plenora_database._native,
    PlenoraResourceLimitError,
    PlenoraError
);
create_exception!(plenora_database._native, PlenoraIoError, PlenoraError);
create_exception!(plenora_database._native, PlenoraProtocolError, PlenoraError);
create_exception!(
    plenora_database._native,
    PlenoraTransientError,
    PlenoraError
);
create_exception!(
    plenora_database._native,
    PlenoraExecutionError,
    PlenoraError
);
create_exception!(plenora_database._native, PlenoraInternalError, PlenoraError);
// PFM CHG-004: eccezione dedicata per commit con esito incerto.
// Il consumer che vuole discriminare recovery/quarantine dalla generica
// "internal" filtra qui direttamente.
create_exception!(
    plenora_database._native,
    PlenoraCommitOutcomeUnknownError,
    PlenoraInternalError
);

/// Registra tutte le classi di eccezione nel pymodule.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PlenoraError", m.py().get_type::<PlenoraError>())?;
    m.add(
        "PlenoraInvalidPlanError",
        m.py().get_type::<PlenoraInvalidPlanError>(),
    )?;
    m.add(
        "PlenoraInvalidConfigurationError",
        m.py().get_type::<PlenoraInvalidConfigurationError>(),
    )?;
    m.add(
        "PlenoraSchemaError",
        m.py().get_type::<PlenoraSchemaError>(),
    )?;
    m.add(
        "PlenoraDataMappingError",
        m.py().get_type::<PlenoraDataMappingError>(),
    )?;
    m.add("PlenoraCrsError", m.py().get_type::<PlenoraCrsError>())?;
    m.add(
        "PlenoraUnsupportedError",
        m.py().get_type::<PlenoraUnsupportedError>(),
    )?;
    m.add(
        "PlenoraNotFoundError",
        m.py().get_type::<PlenoraNotFoundError>(),
    )?;
    m.add(
        "PlenoraConflictError",
        m.py().get_type::<PlenoraConflictError>(),
    )?;
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
    m.add(
        "PlenoraTimeoutError",
        m.py().get_type::<PlenoraTimeoutError>(),
    )?;
    m.add(
        "PlenoraCancelledError",
        m.py().get_type::<PlenoraCancelledError>(),
    )?;
    m.add(
        "PlenoraResourceLimitError",
        m.py().get_type::<PlenoraResourceLimitError>(),
    )?;
    m.add("PlenoraIoError", m.py().get_type::<PlenoraIoError>())?;
    m.add(
        "PlenoraProtocolError",
        m.py().get_type::<PlenoraProtocolError>(),
    )?;
    m.add(
        "PlenoraTransientError",
        m.py().get_type::<PlenoraTransientError>(),
    )?;
    m.add(
        "PlenoraExecutionError",
        m.py().get_type::<PlenoraExecutionError>(),
    )?;
    m.add(
        "PlenoraInternalError",
        m.py().get_type::<PlenoraInternalError>(),
    )?;
    m.add(
        "PlenoraCommitOutcomeUnknownError",
        m.py().get_type::<PlenoraCommitOutcomeUnknownError>(),
    )?;
    Ok(())
}

/// Traduce un `DatabaseError` nella `PyErr` con la sottoclasse giusta.
/// Aggancia inoltre metadata (category, phase, retry, remote_effect,
/// provider, execution_id, diagnostics) come attributi sull'istanza.
///
/// PFM CHG-004: se il pattern coincide con "commit outcome unknown"
/// (`Internal` + `Commit` phase + `Unknown` remote_effect +
/// `RequiresRecovery` retry), l'errore ottiene la classe dedicata
/// `PlenoraCommitOutcomeUnknownError` invece di `PlenoraInternalError`
/// generico. Il consumer può filtrare separatamente per retry/quarantine
/// logic senza matching stringhe nel messaggio.
pub fn to_py_err(err: DatabaseError) -> PyErr {
    let public = err.public_projection();
    drop(err);
    let bind_context = bind_error_context(&public.message);
    let message = format!("{}: {}", category_name(public.category), public.message);
    let is_commit_outcome_unknown = public.category == ErrorCategory::Internal
        && public.phase == ErrorPhase::Commit
        && public.remote_effect == RemoteEffect::Unknown
        // La disposizione e `RequiresRecovery`: il commit non e perso, va
        // verificato fuori banda e poi eventualmente ripreso. Riconoscerlo su
        // `Never` faceva dire all'attributo Python `retry` il contrario di cio
        // che il messaggio chiedeva di fare.
        && matches!(public.retry, RetryDisposition::RequiresRecovery);
    let pyerr = if is_commit_outcome_unknown {
        PlenoraCommitOutcomeUnknownError::new_err(message)
    } else {
        match public.category {
            ErrorCategory::InvalidPlan => PlenoraInvalidPlanError::new_err(message),
            ErrorCategory::InvalidConfiguration => {
                PlenoraInvalidConfigurationError::new_err(message)
            }
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
        }
    };
    Python::attach(|py| {
        let bound = pyerr.value(py);
        // Ignora errori di setattr: sono attributi di comodo, non essenziali
        // per la propagazione dell'errore stesso.
        let _ = bound.setattr("category", category_name(public.category));
        let _ = bound.setattr("phase", phase_name(public.phase));
        let _ = bound.setattr("message", &public.message);
        if let Ok(value) = serde_json::to_value(public.retry) {
            if let Ok(value) = crate::py_convert::json_to_python(py, &value) {
                let _ = bound.setattr("retry", value);
            }
        }
        let _ = bound.setattr("remote_effect", remote_effect_name(public.remote_effect));
        let _ = bound.setattr(
            "provider",
            public.provider.map(|p| format!("{p:?}").to_lowercase()),
        );
        let _ = bound.setattr("execution_id", public.execution_id.as_deref());
        // La forma pubblica annida la diagnostica di riga in `details`; il
        // vecchio attributo `diagnostics` resta un alias strutturato.
        let details_py = public
            .details
            .as_ref()
            .and_then(|value| serde_json::to_value(value).ok())
            .and_then(|value| crate::py_convert::json_to_python(py, &value).ok());
        let _ = bound.setattr("details", details_py.as_ref());
        let diagnostics_py = public.details.as_ref().and_then(|details| {
            serde_json::to_value(&details.row_diagnostics)
                .ok()
                .and_then(|value| crate::py_convert::json_to_python(py, &value).ok())
        });
        let _ = bound.setattr("diagnostics", diagnostics_py.as_ref());
        let _ = bound.setattr(
            "parameter_index",
            bind_context.as_ref().map(|context| context.0),
        );
        let _ = bound.setattr(
            "portable_type",
            bind_context.as_ref().map(|context| context.1.as_str()),
        );
        let _ = bound.setattr(
            "target_type",
            bind_context.as_ref().map(|context| context.2.as_str()),
        );
        // PFM CHG-004: attributi extra su commit-outcome-unknown per
        // guidare il recovery lato consumer.
        if is_commit_outcome_unknown {
            let _ = bound.setattr("automatic_retry_allowed", false);
            let _ = bound.setattr(
                "recovery_action",
                "verificare fuori banda lo stato del target prima di ritentare",
            );
        }
    });
    pyerr
}

fn bind_error_context(message: &str) -> Option<(usize, String, String)> {
    let rest = message.strip_prefix("bind PostgreSQL incompatibile al parametro ")?;
    let (index, rest) = rest.split_once(": tipo portabile ")?;
    let (portable, target) = rest.split_once(", target ")?;
    Some((index.parse().ok()?, portable.to_owned(), target.to_owned()))
}

#[cfg(test)]
#[path = "errors_tests.rs"]
mod tests;

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

const fn remote_effect_name(r: RemoteEffect) -> &'static str {
    match r {
        RemoteEffect::None => "none",
        RemoteEffect::RolledBack => "rolled_back",
        RemoteEffect::Partial => "partial",
        RemoteEffect::Committed => "committed",
        RemoteEffect::Unknown => "unknown",
    }
}
