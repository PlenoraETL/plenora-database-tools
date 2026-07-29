//! Adattatore del contratto canonico per i metadati nativi `PostgreSQL`.

use arrow_schema::Field;
use plenora_database_core::field_contract::FieldContract as CanonicalFieldContract;
use plenora_database_core::protocol;
use plenora_database_core::{DatabaseError, Result};
use std::collections::HashMap;

const LEGACY_NATIVE_TYPE: &str = "plenora.native_type";
const LEGACY_NATIVE_DECLARATION: &str = "plenora.native_declaration";
const LEGACY_TYPE_KIND: &str = "plenora.postgres_type_kind";

#[derive(Debug, Clone, Copy)]
pub struct FieldContract<'a> {
    pub canonical: CanonicalFieldContract<'a>,
    pub field: &'a Field,
    pub native_type: Option<&'a str>,
    pub native_declaration: Option<&'a str>,
    pub type_kind: Option<&'a str>,
    pub geometry_type: Option<&'a str>,
    pub dimensions: Option<&'a str>,
    pub srid: Option<u32>,
}

impl<'a> FieldContract<'a> {
    pub fn parse(field: &'a Field) -> Result<Self> {
        let canonical = CanonicalFieldContract::parse(field)?;
        let metadata = field.metadata();
        Ok(Self {
            canonical,
            field,
            native_type: coherent_value(
                metadata,
                protocol::POSTGRES_NATIVE_TYPE,
                LEGACY_NATIVE_TYPE,
            )?,
            native_declaration: coherent_value(
                metadata,
                protocol::POSTGRES_NATIVE_DECLARATION,
                LEGACY_NATIVE_DECLARATION,
            )?,
            type_kind: coherent_value(metadata, protocol::POSTGRES_TYPE_KIND, LEGACY_TYPE_KIND)?,
            geometry_type: canonical.geometry_types,
            dimensions: canonical.dimensions,
            srid: canonical.srid,
        })
    }

    #[must_use]
    pub fn is_geometry(self) -> bool {
        self.canonical.is_geometry()
    }

    #[must_use]
    pub fn is_geography(self) -> bool {
        self.canonical.is_geography()
    }

    #[must_use]
    pub fn is_range(self) -> bool {
        matches!(
            self.native_type,
            Some("int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange")
        )
    }

    #[must_use]
    pub fn is_composite(self) -> bool {
        self.type_kind == Some("c")
    }
}

fn coherent_value<'a>(
    metadata: &'a HashMap<String, String>,
    canonical: &str,
    legacy: &str,
) -> Result<Option<&'a str>> {
    let current = metadata.get(canonical).map(String::as_str);
    let previous = metadata.get(legacy).map(String::as_str);
    if let (Some(current), Some(previous)) = (current, previous) {
        if current != previous {
            return Err(DatabaseError::invalid_plan(
                "metadata PostgreSQL canonico e legacy divergenti",
            ));
        }
    }
    Ok(current.or(previous))
}
