use arrow_schema::Field;
use plenora_database_core::geometry::GEOARROW_WKB_EXTENSION_NAME;
use plenora_database_core::protocol;
use plenora_database_core::{DatabaseError, Result};
use std::collections::HashMap;

const LEGACY_DIMENSIONS: &str = "plenora.dimensions";
const LEGACY_SRID: &str = "plenora.srid";
const LEGACY_SPATIAL_SEMANTICS: &str = "plenora.spatial_semantics";
const LEGACY_GEOMETRY_TYPE: &str = "plenora.geometry_type";
const LEGACY_NATIVE_TYPE: &str = "plenora.native_type";
const LEGACY_NATIVE_DECLARATION: &str = "plenora.native_declaration";
const LEGACY_TYPE_KIND: &str = "plenora.postgres_type_kind";

#[derive(Debug, Clone, Copy)]
pub struct FieldContract<'a> {
    pub field: &'a Field,
    pub native_type: Option<&'a str>,
    pub native_declaration: Option<&'a str>,
    pub type_kind: Option<&'a str>,
    pub geometry_type: Option<&'a str>,
    pub dimensions: Option<&'a str>,
    pub spatial_semantics: Option<&'a str>,
    pub srid: Option<u32>,
    pub crs_resolution: Option<&'a str>,
    pub crs_id: Option<&'a str>,
    pub crs_definition: Option<&'a str>,
    pub crs_definition_format: Option<&'a str>,
    pub axis_order: Option<&'a str>,
    pub spatial: bool,
}

impl<'a> FieldContract<'a> {
    pub fn parse(field: &'a Field) -> Result<Self> {
        let metadata = field.metadata();
        let dimensions = coherent_value(
            metadata,
            protocol::GEOMETRY_DIMENSIONS,
            LEGACY_DIMENSIONS,
            false,
        )?;
        let raw_srid = coherent_value(metadata, protocol::GEOMETRY_SRID, LEGACY_SRID, false)?;
        let spatial_semantics = coherent_value(
            metadata,
            protocol::GEOMETRY_SPATIAL_SEMANTICS,
            LEGACY_SPATIAL_SEMANTICS,
            false,
        )?;
        let geometry_type = coherent_value(
            metadata,
            protocol::GEOMETRY_TYPES,
            LEGACY_GEOMETRY_TYPE,
            true,
        )?;
        let native_type = coherent_value(
            metadata,
            protocol::POSTGRES_NATIVE_TYPE,
            LEGACY_NATIVE_TYPE,
            false,
        )?;
        let native_declaration = coherent_value(
            metadata,
            protocol::POSTGRES_NATIVE_DECLARATION,
            LEGACY_NATIVE_DECLARATION,
            false,
        )?;
        let type_kind = coherent_value(
            metadata,
            protocol::POSTGRES_TYPE_KIND,
            LEGACY_TYPE_KIND,
            false,
        )?;
        let spatial = metadata
            .get(protocol::GEOARROW_EXTENSION_NAME)
            .is_some_and(|value| value == GEOARROW_WKB_EXTENSION_NAME);
        let srid = if spatial {
            raw_srid
                .map(|value| {
                    value
                        .parse::<u32>()
                        .ok()
                        .filter(|value| *value > 0)
                        .ok_or_else(|| {
                            DatabaseError::invalid_plan("SRID CRS deve essere un intero positivo")
                        })
                })
                .transpose()?
        } else {
            None
        };
        let contract = Self {
            field,
            native_type,
            native_declaration,
            type_kind,
            geometry_type,
            dimensions,
            spatial_semantics,
            srid,
            crs_resolution: metadata
                .get(protocol::GEOMETRY_CRS_RESOLUTION)
                .map(String::as_str),
            crs_id: metadata.get(protocol::GEOMETRY_CRS_ID).map(String::as_str),
            crs_definition: metadata
                .get(protocol::GEOMETRY_CRS_DEFINITION)
                .map(String::as_str),
            crs_definition_format: metadata
                .get(protocol::GEOMETRY_CRS_DEFINITION_FORMAT)
                .map(String::as_str),
            axis_order: metadata
                .get(protocol::GEOMETRY_AXIS_ORDER)
                .map(String::as_str),
            spatial,
        };
        contract.validate_crs()?;
        Ok(contract)
    }

    #[must_use]
    pub fn is_geometry(self) -> bool {
        self.spatial
            && self
                .spatial_semantics
                .is_none_or(|value| value == "geometry")
    }

    #[must_use]
    pub fn is_geography(self) -> bool {
        self.spatial && self.spatial_semantics == Some("geography")
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

    fn validate_crs(self) -> Result<()> {
        if !self.spatial {
            return Ok(());
        }
        if self.crs_definition.is_some() != self.crs_definition_format.is_some() {
            return Err(DatabaseError::invalid_plan(
                "definizione CRS e formato devono essere presenti insieme",
            ));
        }
        if self.crs_definition.is_some() && self.axis_order.is_none_or(|axis| axis == "unknown") {
            return Err(DatabaseError::invalid_plan(
                "una definizione CRS richiede un ordine assi esplicito",
            ));
        }
        if self.axis_order.is_some_and(|axis| {
            !matches!(
                axis,
                "lon_lat"
                    | "lat_lon"
                    | "easting_northing"
                    | "northing_easting"
                    | "other"
                    | "unknown"
            )
        }) {
            return Err(DatabaseError::invalid_plan("ordine assi CRS non valido"));
        }
        if self.crs_id.is_some_and(|value| {
            value.is_empty() || value.len() > 1_024 || value.chars().any(char::is_control)
        }) {
            return Err(DatabaseError::invalid_plan("identificatore CRS non valido"));
        }
        match self.crs_resolution {
            Some("resolved") if self.srid.is_none() || self.crs_id.is_none() => {
                Err(DatabaseError::invalid_plan(
                    "CRS resolved PostgreSQL richiede SRID e identificatore",
                ))
            }
            Some("missing")
                if self.srid.is_some()
                    || self.crs_id.is_some()
                    || self.crs_definition.is_some()
                    || self.crs_definition_format.is_some()
                    || self.axis_order.is_some() =>
            {
                Err(DatabaseError::invalid_plan(
                    "CRS missing non ammette metadati CRS dichiarati",
                ))
            }
            Some("resolved" | "declared_unresolved" | "missing") | None => Ok(()),
            Some(_) => Err(DatabaseError::invalid_plan(
                "stato di risoluzione CRS non valido",
            )),
        }
    }
}

fn coherent_value<'a>(
    metadata: &'a HashMap<String, String>,
    canonical: &str,
    legacy: &str,
    case_insensitive: bool,
) -> Result<Option<&'a str>> {
    let current = metadata.get(canonical).map(String::as_str);
    let previous = metadata.get(legacy).map(String::as_str);
    if let (Some(current), Some(previous)) = (current, previous) {
        let coherent = if case_insensitive {
            current.eq_ignore_ascii_case(previous)
        } else {
            current == previous
        };
        if !coherent {
            return Err(DatabaseError::invalid_plan(
                "metadata canonico e legacy divergenti",
            ));
        }
    }
    Ok(current.or(previous))
}
