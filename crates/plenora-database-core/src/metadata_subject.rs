//! Localizzazione redatta di un valore metadata nel rapporto esecutivo v4.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataScope {
    Schema,
    Field,
}

/// `index` distingue le chiavi dello stesso campo/regola; il digest identifica
/// la chiave originale senza pubblicarne testo o valore.
///
/// La posizione del campo
/// resta `RiskEntry::field_id`, come nel rapporto v3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataSubject {
    pub index: u32,
    pub scope: MetadataScope,
    #[serde(deserialize_with = "required_identity")]
    pub field_identity: Option<String>,
    pub property: String,
    pub key_sha256: String,
    pub hop: MetadataHop,
    pub reason: MetadataReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataHop {
    ColumnComment,
}

impl MetadataSubject {
    /// # Errors
    /// Non ammette valori o nomi di chiavi arbitrarie nel selettore pubblico.
    pub fn validate(&self) -> crate::Result<()> {
        if (self.scope == MetadataScope::Field) != self.field_identity.is_some()
            || self.key_sha256.len() != 64
            || !self
                .key_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.property.is_empty()
            || self.property.len() > 32
            || !self
                .property
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_')
            || self
                .field_identity
                .as_ref()
                .is_some_and(|id| crate::protocol::canonical_field_id(id).as_ref() != Some(id))
        {
            return Err(crate::execution_loss::invalid());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataReason {
    ColumnLimit,
    OperationLimit,
}

/// Deserialize a present optional subject, refusing explicit null.
/// # Errors
/// Rejects values outside the subject's wire type.
pub fn present<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}

fn required_identity<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}
