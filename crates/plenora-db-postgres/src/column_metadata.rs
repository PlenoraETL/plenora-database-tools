//! Codec locale dei commenti Arrow. Il digest rileva corruzione, non autentica.
#[cfg(test)]
use arrow_schema::{Field, Schema};
use plenora_database_core::protocol::canonical_field_id;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const ID_PREFIX: &str = "plenora.field_id.v1:";
const ARROW_PREFIX: &str = "plenora.arrow.v1:";
pub const COLUMN_LIMIT: usize = 1024 * 1024;
pub const OPERATION_LIMIT: usize = 16 * COLUMN_LIMIT;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnMetadata {
    pub physical: Option<PhysicalGeometry>,
    pub version: u8,
    pub id: String,
    pub data_type: String,
    pub field: BTreeMap<String, String>,
    pub schema: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted: Vec<plenora_database_core::metadata_subject::MetadataSubject>,
}

use crate::geometry_properties::PhysicalGeometry;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    metadata: ColumnMetadata,
    sha256: String,
}

pub fn invalid() -> DatabaseError {
    crate::error::public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        false,
        "metadati di colonna PostgreSQL non validi",
    )
}

pub fn limit() -> DatabaseError {
    crate::error::public_error(
        ErrorCategory::ResourceLimit,
        ErrorPhase::Validate,
        false,
        "metadati di colonna PostgreSQL oltre il limite di persistenza",
    )
}

impl ColumnMetadata {
    #[cfg(test)]
    pub fn plan(field: &Field, schema: &Schema) -> Result<Self> {
        let isolated = Schema::new_with_metadata(vec![field.clone()], schema.metadata().clone());
        crate::geometry_properties::SchemaPlan::compile(&isolated)?
            .columns
            .into_iter()
            .next()
            .ok_or_else(invalid)
    }

    pub fn encode(&self) -> Result<String> {
        let payload = serde_json::to_vec(self).map_err(|_| invalid())?;
        let sha256 = digest(&payload);
        let text = serde_json::to_string(&Envelope {
            metadata: self.clone(),
            sha256,
        })
        .map_err(|_| invalid())?;
        if text.len() > COLUMN_LIMIT {
            return Err(limit());
        }
        Ok(text)
    }
}

pub fn decode(comment: Option<&str>) -> Result<Option<ColumnMetadata>> {
    let (_, body) = split_identity(comment)?;
    let Some(body) = body.strip_prefix(ARROW_PREFIX) else {
        if body
            .lines()
            .any(|line| line.starts_with("plenora.arrow.") || line.starts_with("plenora.field_id."))
        {
            return Err(invalid());
        }
        return Ok(None);
    };
    let (json, human) = body.split_once('\n').ok_or_else(invalid)?;
    if json.len() > COLUMN_LIMIT
        || human
            .lines()
            .any(|line| line.starts_with("plenora.arrow.") || line.starts_with("plenora.field_id."))
    {
        return Err(invalid());
    }
    bounded_json(json)?;
    let envelope: Envelope =
        plenora_database_core::strict_json::from_slice(json.as_bytes()).map_err(|_| invalid())?;
    let bytes = serde_json::to_vec(&envelope.metadata).map_err(|_| invalid())?;
    if envelope.metadata.version != 1 || digest(&bytes) != envelope.sha256 {
        return Err(invalid());
    }
    let (id, _) = split_identity(comment)?;
    if canonical_field_id(&envelope.metadata.id) != id {
        return Err(invalid());
    }
    if envelope
        .metadata
        .field
        .iter()
        .chain(&envelope.metadata.schema)
        .any(|(key, value)| key.contains('\0') || value.contains('\0'))
    {
        return Err(invalid());
    }
    let mut subjects = std::collections::BTreeSet::new();
    let mut keys = std::collections::BTreeSet::new();
    for subject in &envelope.metadata.omitted {
        subject.validate()?;
        let map = match subject.scope {
            plenora_database_core::metadata_subject::MetadataScope::Schema => {
                &envelope.metadata.schema
            }
            plenora_database_core::metadata_subject::MetadataScope::Field => {
                &envelope.metadata.field
            }
        };
        let expected_identity = match subject.scope {
            plenora_database_core::metadata_subject::MetadataScope::Schema => None,
            plenora_database_core::metadata_subject::MetadataScope::Field => {
                Some(&envelope.metadata.id)
            }
        };
        if subject.field_identity.as_ref() != expected_identity
            || !subjects.insert(subject.index)
            || !keys.insert((subject.scope as u8, &subject.key_sha256))
            || map
                .keys()
                .any(|k| digest(k.as_bytes()) == subject.key_sha256)
            || !crate::geometry_properties::PROPERTY_REGISTRY
                .iter()
                .any(|p| p.name == subject.property)
        {
            return Err(invalid());
        }
    }
    Ok(Some(envelope.metadata))
}

fn bounded_json(json: &str) -> Result<()> {
    let (mut depth, mut quoted, mut escaped) = (0_usize, false, false);
    for byte in json.bytes() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 16 {
                        return Err(invalid());
                    }
                }
                b'}' | b']' => depth = depth.checked_sub(1).ok_or_else(invalid)?,
                _ => (),
            }
        }
    }
    if depth != 0 || quoted {
        return Err(invalid());
    }
    Ok(())
}

pub fn split_identity(comment: Option<&str>) -> Result<(Option<String>, &str)> {
    let text = comment.unwrap_or("");
    if let Some(rest) = text.strip_prefix(ID_PREFIX) {
        let (id, body) = rest.split_once('\n').ok_or_else(invalid)?;
        return Ok((Some(canonical_field_id(id).ok_or_else(invalid)?), body));
    }
    if text
        .lines()
        .any(|line| line.starts_with("plenora.field_id.") || line.starts_with("plenora.arrow."))
    {
        return Err(invalid());
    }
    Ok((None, text))
}

pub fn plan_registration(
    comment: Option<&str>,
    incoming: &ColumnMetadata,
) -> Result<Option<String>> {
    let (old_id, human) = split_identity(comment)?;
    let new_id = canonical_field_id(&incoming.id);
    if old_id.is_some() && old_id != new_id {
        return Err(conflict());
    }
    if let Some(mut existing) = decode(comment)? {
        let mut contract = existing.clone();
        contract.omitted.clone_from(&incoming.omitted);
        if contract != *incoming {
            return Err(conflict());
        }
        let previous = existing.omitted.len();
        merge_omissions(&mut existing, &incoming.omitted)?;
        if existing.omitted.len() == previous {
            return Ok(None);
        }
        let (_, human) = human.split_once('\n').ok_or_else(invalid)?;
        let id = new_id.ok_or_else(invalid)?;
        return Ok(Some(format!(
            "{ID_PREFIX}{id}\n{ARROW_PREFIX}{}\n{human}",
            existing.encode()?
        )));
    }
    let id = new_id.ok_or_else(invalid)?;
    Ok(Some(format!(
        "{ID_PREFIX}{id}\n{ARROW_PREFIX}{}\n{human}",
        incoming.encode()?
    )))
}

/// Size of the new JSON block, excluding identity and untouched human text.
pub fn registration_bytes(comment: &str) -> Result<usize> {
    let (_, body) = split_identity(Some(comment))?;
    let json = body
        .strip_prefix(ARROW_PREFIX)
        .ok_or_else(invalid)?
        .split_once('\n')
        .ok_or_else(invalid)?
        .0;
    Ok(json.len())
}

pub fn merge_omissions(
    target: &mut ColumnMetadata,
    incoming: &[plenora_database_core::metadata_subject::MetadataSubject],
) -> Result<()> {
    for subject in incoming {
        if !target
            .omitted
            .iter()
            .any(|old| old.scope == subject.scope && old.key_sha256 == subject.key_sha256)
        {
            let mut added = subject.clone();
            added.index = target
                .omitted
                .iter()
                .map(|s| s.index)
                .max()
                .map_or(Some(0), |i| i.checked_add(1))
                .ok_or_else(invalid)?;
            target.omitted.push(added);
        }
    }
    Ok(())
}

fn conflict() -> DatabaseError {
    crate::error::public_error(
        ErrorCategory::Conflict,
        ErrorPhase::Write,
        false,
        "metadati Arrow incompatibili con la colonna PostgreSQL registrata",
    )
}

pub fn digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

#[cfg(test)]
#[path = "column_metadata_tests.rs"]
pub mod tests;
