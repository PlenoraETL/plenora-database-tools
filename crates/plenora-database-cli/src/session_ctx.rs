#![allow(clippy::doc_markdown, clippy::match_same_arms)]
//! Parser globale `--session-context KEY=VALUE:TYPE` per iniettare voci
//! nel `SessionContext` di ogni comando che apre una transazione.
//!
//! Sintassi:
//!   --session-context app.tenant_id=t42:string
//!   --session-context app.user_id=42:int --session-context app.request_id=abc-123:string
//!
//! Le voci sono aggiunte come `SessionEntry::public(SessionValue::...)`. Se
//! serve il flavour secret, aggiungeremo `--session-context-secret KEY=...`
//! (non ancora implementato: il tool non deve mai loggare secret).

use crate::typed_params::parse_named_value_type;
use crate::CliResult;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::session_context::{SessionContext, SessionEntry, SessionValue};
use std::sync::{Mutex, OnceLock};

static ACTIVE: OnceLock<Mutex<SessionContext>> = OnceLock::new();

fn store() -> &'static Mutex<SessionContext> {
    ACTIVE.get_or_init(|| Mutex::new(SessionContext::new()))
}

/// Restituisce il session context globale attivo per la sessione CLI
/// (popolato via `strip_session_context` nel dispatcher).
pub(crate) fn active() -> SessionContext {
    store()
        .lock()
        .map_or_else(|_| SessionContext::new(), |g| g.clone())
}

/// Imposta il session context globale attivo.
fn set_active(ctx: SessionContext) {
    if let Ok(mut guard) = store().lock() {
        *guard = ctx;
    }
}

/// Estrae tutti i `--session-context <spec>` presenti in `args`, li rimuove
/// e restituisce il resto + il `SessionContext` popolato.
pub(crate) fn strip_session_context(args: Vec<String>) -> CliResult<Vec<String>> {
    let mut out = Vec::with_capacity(args.len());
    let mut ctx = SessionContext::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--session-context" {
            let spec = iter
                .next()
                .ok_or_else(|| "--session-context richiede NAME=VALUE:TYPE".to_owned())?;
            let (name, value) = parse_named_value_type(&spec)?;
            let session_value = param_to_session_value(value)?;
            ctx.insert(&name, SessionEntry::public(session_value))?;
        } else {
            out.push(arg);
        }
    }
    set_active(ctx);
    Ok(out)
}

fn param_to_session_value(param: ParameterValue) -> CliResult<SessionValue> {
    match param {
        ParameterValue::Bool(v) => Ok(SessionValue::Boolean(v)),
        ParameterValue::I32(v) => Ok(SessionValue::Integer(i64::from(v))),
        ParameterValue::I64(v) => Ok(SessionValue::Integer(v)),
        ParameterValue::F64(v) => Ok(SessionValue::Text(v.to_string())),
        ParameterValue::String(v) => Ok(SessionValue::Text(v)),
        ParameterValue::Uuid(v) => Ok(SessionValue::Text(v)),
        ParameterValue::Json(v) => Ok(SessionValue::Text(v.to_string())),
        ParameterValue::Date(v)
        | ParameterValue::Timestamp(v)
        | ParameterValue::TimestampTz(v)
        | ParameterValue::Decimal(v) => Ok(SessionValue::Text(v)),
        ParameterValue::Null { .. } => Ok(SessionValue::Text(String::new())),
        ParameterValue::Bytes(_) => {
            Err("session context non supporta bytes: usa hex string come text".into())
        }
        ParameterValue::Wkb { .. } => {
            Err("session context non supporta geometry".into())
        }
        ParameterValue::Enum { label, .. } => Ok(SessionValue::Text(label)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // I test manipolano lo store globale; serializzati per non falsare le
    // assertion sotto cargo test parallel.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn strip_extracts_ordered_entries() {
        let _g = TEST_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let rest = strip_session_context(vec![
            "cmd".into(),
            "--session-context".into(),
            "app.tenant_id=t42:string".into(),
            "arg1".into(),
            "--session-context".into(),
            "app.user_id=99:int".into(),
        ])
        .unwrap();
        assert_eq!(rest, vec!["cmd", "arg1"]);
        let ctx = active();
        assert!(ctx.get("app.tenant_id").is_some());
        assert!(ctx.get("app.user_id").is_some());
        let _ = strip_session_context(vec![]);
    }

    #[test]
    fn missing_argument_fails() {
        let err = strip_session_context(vec!["--session-context".into()]).unwrap_err();
        assert!(format!("{err:?}").contains("session-context"));
    }
}
