//! Codec locale dei commenti Arrow. Il digest rileva corruzione, non autentica.
use arrow_schema::{Field, Schema};
use plenora_database_core::protocol::{canonical_field_id, FIELD_ID};
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
    version: u8,
    pub id: String,
    pub data_type: String,
    pub field: BTreeMap<String, String>,
    pub schema: BTreeMap<String, String>,
}

/// Physical guarantees required by the registered Arrow spatial declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalGeometry {
    geography: bool,
    dimensions: Option<String>,
    srid: Option<u32>,
    geometry_type: String,
}

impl PhysicalGeometry {
    fn plan(field: &Field) -> Result<Option<Self>> {
        let contract = crate::field_contract::FieldContract::parse(field)?;
        if !contract.canonical.spatial {
            return Ok(None);
        }
        let dimensions = match contract.dimensions {
            Some("unknown") => None,
            Some("xy" | "xyz" | "xym" | "xyzm") => contract.dimensions.map(str::to_owned),
            None => Some("xy".to_owned()),
            _ => return Err(invalid()),
        };
        let geometry_type = if dimensions.is_none() {
            "Geometry"
        } else {
            contract
                .geometry_type
                .filter(|types| !types.contains(','))
                .unwrap_or("Geometry")
        }
        .to_owned();
        if !geometry_type.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(invalid());
        }
        Ok(Some(Self {
            geography: contract.is_geography(),
            dimensions,
            srid: contract.target_srid.filter(|v| *v != 0),
            geometry_type,
        }))
    }

    pub fn declaration(field: &Field) -> Result<String> {
        let physical = Self::plan(field)?.ok_or_else(invalid)?;
        let base = if physical.geography {
            "geography"
        } else {
            "geometry"
        };
        let Some(dimensions) = &physical.dimensions else {
            return Ok(base.to_owned());
        };
        let suffix = match dimensions.as_str() {
            "xy" => "",
            "xyz" => "Z",
            "xym" => "M",
            "xyzm" => "ZM",
            _ => return Err(invalid()),
        };
        Ok(format!(
            "{base}({}{suffix},{})",
            physical.geometry_type,
            physical.srid.unwrap_or(0)
        ))
    }

    pub fn matches(&self, column: &crate::types::ColumnSpec) -> bool {
        use crate::types::ColumnKind;
        matches!(column.kind, ColumnKind::Geometry | ColumnKind::Geography)
            && self.geography == matches!(column.kind, ColumnKind::Geography)
            && self.dimensions
                == column
                    .spatial_dimensions
                    .as_deref()
                    .map(str::to_ascii_lowercase)
            && self.srid == column.spatial_srid.filter(|v| *v != 0)
            && self
                .geometry_type
                .eq_ignore_ascii_case(column.spatial_type.as_deref().unwrap_or("geometry"))
    }
}

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

fn limit() -> DatabaseError {
    crate::error::public_error(
        ErrorCategory::ResourceLimit,
        ErrorPhase::Validate,
        false,
        "metadati di colonna PostgreSQL oltre il limite di persistenza",
    )
}

impl ColumnMetadata {
    pub fn plan(field: &Field, schema: &Schema) -> Result<Self> {
        crate::geometry_properties::validate_dimensions(
            &crate::field_contract::FieldContract::parse(field)?,
        )?;
        // Contabilizzare prima delle copie e del serializzatore; JSON puo solo
        // aumentare questa dimensione, che viene verificata ancora in encode.
        let mut bytes = 0_usize;
        for (key, value) in field.metadata().iter().chain(schema.metadata()) {
            if key.contains('\0') || value.contains('\0') {
                return Err(invalid());
            }
            bytes = bytes
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(limit)?;
            if bytes > COLUMN_LIMIT {
                return Err(limit());
            }
        }
        Ok(Self {
            version: 1,
            physical: PhysicalGeometry::plan(field)?,
            id: field.metadata().get(FIELD_ID).cloned().unwrap_or_else(|| {
                plenora_database_core::protocol::name_field_id(field.name()).to_string()
            }),
            data_type: field.data_type().to_string(),
            field: field.metadata().clone().into_iter().collect(),
            schema: schema.metadata().clone().into_iter().collect(),
        })
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
    Ok(Some(envelope.metadata))
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
    if let Some(existing) = decode(comment)? {
        if existing != *incoming {
            return Err(conflict());
        }
        return Ok(None);
    }
    let id = new_id.ok_or_else(invalid)?;
    Ok(Some(format!(
        "{ID_PREFIX}{id}\n{ARROW_PREFIX}{}\n{human}",
        incoming.encode()?
    )))
}

fn conflict() -> DatabaseError {
    crate::error::public_error(
        ErrorCategory::Conflict,
        ErrorPhase::Write,
        false,
        "metadati Arrow incompatibili con la colonna PostgreSQL registrata",
    )
}

pub fn plan_schema(schema: &Schema) -> Result<()> {
    let mut total = 0_usize;
    for field in schema.fields() {
        let encoded = ColumnMetadata::plan(field, schema)?.encode()?;
        total = total.checked_add(encoded.len()).ok_or_else(limit)?;
        if total > OPERATION_LIMIT {
            return Err(limit());
        }
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
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
