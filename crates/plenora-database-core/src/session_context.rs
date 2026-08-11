//! Session context generico portabile.
//!
//! Il PFM richiede di poter propagare al database il contesto della richiesta
//! (`tenant`, `actor`, `correlation_id`, `decision_id`, ...) in modo:
//!
//! 1. **Isolato**: il context di una richiesta non deve sopravvivere al
//!    riuso della connessione da parte di una richiesta successiva. La
//!    libreria applica il context con semantica *transaction-local* dove il
//!    provider la supporta (es. Postgres `SET LOCAL` / `set_config(..,true)`),
//!    così che il commit/rollback lo resetti automaticamente.
//! 2. **Tipizzato**: valori booleani, numerici o testuali, non stringhe
//!    opache. Il provider si occupa dell'encoding.
//! 3. **Classificato e reddito**: i valori sensibili non compaiono nel
//!    formato `Debug`, nei log strutturati né nelle metriche.
//! 4. **Namespaced**: le chiavi devono contenere un namespace (formato
//!    `namespace.name`) per non collidere con parametri di sistema del
//!    provider.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Classificazione di un valore di contesto: guida la policy di redaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionClassification {
    /// Ok da loggare/tracciare.
    Public,
    /// Non loggare in output esterni; ok in metriche aggregate.
    Internal,
    /// Non loggare mai; non usare in metriche.
    Sensitive,
}

impl SessionClassification {
    #[must_use]
    pub const fn is_public(self) -> bool {
        matches!(self, Self::Public)
    }
}

/// Valore tipizzato di un entry di contesto.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SessionValue {
    Text(String),
    Integer(i64),
    Boolean(bool),
}

impl SessionValue {
    /// Serializzazione testuale accettata da tutti i provider come stringa
    /// (`Text`) tramite `set_config` / `SET SESSION`.
    #[must_use]
    pub fn as_provider_string(&self) -> String {
        match self {
            Self::Text(v) => v.clone(),
            Self::Integer(v) => v.to_string(),
            Self::Boolean(v) => (if *v { "true" } else { "false" }).to_owned(),
        }
    }
}

impl std::fmt::Debug for SessionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text(_) | Self::Integer(_) | Self::Boolean(_) => f.write_str("SessionValue(_)"),
        }
    }
}

/// Entry singolo di contesto: valore + classificazione.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEntry {
    pub value: SessionValue,
    pub classification: SessionClassification,
}

impl SessionEntry {
    #[must_use]
    pub const fn public(value: SessionValue) -> Self {
        Self {
            value,
            classification: SessionClassification::Public,
        }
    }

    #[must_use]
    pub const fn internal(value: SessionValue) -> Self {
        Self {
            value,
            classification: SessionClassification::Internal,
        }
    }

    #[must_use]
    pub const fn sensitive(value: SessionValue) -> Self {
        Self {
            value,
            classification: SessionClassification::Sensitive,
        }
    }
}

impl std::fmt::Debug for SessionEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.classification.is_public() {
            f.debug_struct("SessionEntry")
                .field("value", &self.value.as_provider_string())
                .field("classification", &self.classification)
                .finish()
        } else {
            f.debug_struct("SessionEntry")
                .field("value", &"[REDACTED]")
                .field("classification", &self.classification)
                .finish()
        }
    }
}

/// Contesto di sessione: mappa chiave namespaced → entry classificato.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionContext {
    entries: BTreeMap<String, SessionEntry>,
}

impl SessionContext {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserisce un entry.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` se il nome non è nel formato
    /// `namespace.name` con caratteri ammessi `[a-z0-9_]`, se lunghezza
    /// eccede 63 caratteri (limite provider), o se il valore contiene
    /// caratteri di controllo.
    pub fn insert(
        &mut self,
        name: impl Into<String>,
        entry: SessionEntry,
    ) -> crate::Result<()> {
        let name = name.into();
        validate_context_key(&name)?;
        validate_context_value(&entry.value)?;
        self.entries.insert(name, entry);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&SessionEntry> {
        self.entries.get(name)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SessionEntry)> {
        self.entries.iter()
    }
}

impl std::fmt::Debug for SessionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.entries.iter()).finish()
    }
}

/// Verifica che una chiave sia nel formato `namespace.name`.
///
/// # Errors
///
/// Ritorna `InvalidPlan` per formato non conforme.
///
/// # Panics
///
/// Non panics: gli `expect()` interni sono protetti da controlli precedenti.
pub fn validate_context_key(name: &str) -> crate::Result<()> {
    if name.is_empty() {
        return Err(crate::DatabaseError::invalid_plan(
            "il nome del context di sessione non può essere vuoto",
        ));
    }
    if name.len() > 63 {
        return Err(crate::DatabaseError::invalid_plan(
            "il nome del context di sessione eccede 63 caratteri",
        ));
    }
    let dot_count = name.chars().filter(|c| *c == '.').count();
    if dot_count != 1 {
        return Err(crate::DatabaseError::invalid_plan(
            "il nome del context di sessione richiede formato `namespace.name`",
        ));
    }
    let (namespace, local) = name.split_once('.').expect("dot presente");
    if namespace.is_empty() || local.is_empty() {
        return Err(crate::DatabaseError::invalid_plan(
            "namespace e nome locale del context di sessione non possono essere vuoti",
        ));
    }
    validate_identifier_segment(namespace)?;
    validate_identifier_segment(local)?;
    Ok(())
}

fn validate_identifier_segment(segment: &str) -> crate::Result<()> {
    let mut chars = segment.chars();
    let first = chars.next().expect("controllato non vuoto");
    if !(first.is_ascii_lowercase() || first == '_') {
        return Err(crate::DatabaseError::invalid_plan(
            "segmento del context deve iniziare con lettera minuscola o underscore",
        ));
    }
    for c in chars {
        if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
            return Err(crate::DatabaseError::invalid_plan(
                "segmento del context può contenere solo [a-z0-9_]",
            ));
        }
    }
    Ok(())
}

/// Rifiuta valori con caratteri di controllo (NUL, CR, LF) che potrebbero
/// rompere il protocollo o essere abusati per iniezione di log.
///
/// # Errors
///
/// Ritorna `InvalidPlan` se il valore contiene caratteri di controllo.
pub fn validate_context_value(value: &SessionValue) -> crate::Result<()> {
    if let SessionValue::Text(text) = value {
        if text.len() > 8_192 {
            return Err(crate::DatabaseError::invalid_plan(
                "valore di context di sessione eccede 8192 byte",
            ));
        }
        if text.chars().any(|c| c == '\0' || c == '\r' || c == '\n') {
            return Err(crate::DatabaseError::invalid_plan(
                "valore di context di sessione contiene caratteri di controllo",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_namespaced_keys_are_accepted() {
        for name in [
            "app.tenant",
            "plenora.actor_id",
            "audit.correlation_id",
            "sec.policy_v1",
            "x.y",
        ] {
            assert!(validate_context_key(name).is_ok(), "atteso ok: {name}");
        }
    }

    #[test]
    fn invalid_keys_are_rejected() {
        for name in [
            "",
            "no_namespace",
            "app.",
            ".name",
            "app..name",
            "App.tenant",
            "app.Tenant",
            "app.name-with-dash",
            "1app.name",
            "app.1name",
            "app.name with space",
            &format!("app.{}", "x".repeat(70)),
        ] {
            assert!(
                validate_context_key(name).is_err(),
                "atteso rifiuto: {name}"
            );
        }
    }

    #[test]
    fn values_with_control_chars_are_rejected() {
        let mut ctx = SessionContext::new();
        assert!(ctx
            .insert(
                "app.actor",
                SessionEntry::public(SessionValue::Text("evil\n".into())),
            )
            .is_err());
        assert!(ctx
            .insert(
                "app.actor",
                SessionEntry::public(SessionValue::Text("bad\0nul".into())),
            )
            .is_err());
    }

    #[test]
    fn debug_of_sensitive_entry_is_redacted() {
        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.token",
            SessionEntry::sensitive(SessionValue::Text("must-not-leak".into())),
        )
        .expect("insert");
        let s = format!("{ctx:?}");
        assert!(!s.contains("must-not-leak"), "atteso redacted: {s}");
        assert!(s.contains("REDACTED"), "atteso marker REDACTED: {s}");
    }

    #[test]
    fn debug_of_public_entry_shows_value() {
        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.tenant",
            SessionEntry::public(SessionValue::Text("acme".into())),
        )
        .expect("insert");
        let s = format!("{ctx:?}");
        assert!(s.contains("acme"), "public deve essere visibile: {s}");
    }

    #[test]
    fn provider_string_encoding_covers_all_variants() {
        assert_eq!(SessionValue::Text("x".into()).as_provider_string(), "x");
        assert_eq!(SessionValue::Integer(42).as_provider_string(), "42");
        assert_eq!(SessionValue::Boolean(true).as_provider_string(), "true");
        assert_eq!(SessionValue::Boolean(false).as_provider_string(), "false");
    }
}
