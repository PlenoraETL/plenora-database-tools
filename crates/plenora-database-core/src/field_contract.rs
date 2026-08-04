//! Validazione del contratto Arrow trasversale.
//!
//! Questo modulo giudica soltanto le chiavi canoniche e `GeoArrow`. I metadati
//! nativi di un provider restano nel relativo crate.

use crate::arrow::schema::{DataType, Field, Schema};
use crate::geometry::GEOARROW_WKB_EXTENSION_NAME;
use crate::protocol;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition};
use std::collections::HashMap;

const LEGACY_DIMENSIONS: &str = "plenora.dimensions";
const LEGACY_SRID: &str = "plenora.srid";
const LEGACY_SPATIAL_SEMANTICS: &str = "plenora.spatial_semantics";
const LEGACY_GEOMETRY_TYPE: &str = "plenora.geometry_type";

const CANONICAL_GEOMETRY_KEYS: [&str; 12] = [
    protocol::GEOMETRY_ENCODING,
    protocol::GEOMETRY_DIMENSIONS,
    protocol::GEOMETRY_TYPES,
    protocol::GEOMETRY_TYPES_DECLARATION,
    protocol::GEOMETRY_SRID,
    protocol::GEOMETRY_CRS_RESOLUTION,
    protocol::GEOMETRY_CRS_ID,
    protocol::GEOMETRY_CRS_DEFINITION,
    protocol::GEOMETRY_CRS_DEFINITION_FORMAT,
    protocol::GEOMETRY_AXIS_ORDER,
    protocol::GEOMETRY_SPATIAL_SEMANTICS,
    protocol::GEOMETRY_PRECISION,
];

const GEOMETRY_TYPES: [&str; 16] = [
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
    "circularstring",
    "compoundcurve",
    "curvepolygon",
    "multicurve",
    "multisurface",
    "polyhedralsurface",
    "tin",
    "triangle",
    "unknown",
];

/// Vista validata dei metadati canonici di un campo Arrow.
#[derive(Debug, Clone, Copy)]
pub struct FieldContract<'a> {
    pub field: &'a Field,
    pub field_id: Option<u32>,
    pub encoding: Option<&'a str>,
    pub geometry_types: Option<&'a str>,
    pub types_declaration: Option<&'a str>,
    pub dimensions: Option<&'a str>,
    pub spatial_semantics: Option<&'a str>,
    pub srid: Option<u32>,
    pub crs_resolution: Option<&'a str>,
    pub crs_id: Option<&'a str>,
    pub crs_definition: Option<&'a str>,
    pub crs_definition_format: Option<&'a str>,
    pub axis_order: Option<&'a str>,
    pub precision: Option<&'a str>,
    pub spatial: bool,
}

impl<'a> FieldContract<'a> {
    /// Analizza un campo senza attribuirgli default impliciti.
    ///
    /// Accetta ingressi legacy incompleti, ma rifiuta valori presenti non
    /// validi e rappresentazioni canoniche/legacy/GeoArrow divergenti.
    ///
    /// # Errors
    ///
    /// Restituisce un errore categorizzato quando tipo Arrow, metadati,
    /// enumerazioni o rappresentazioni CRS sono incoerenti.
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
        let geometry_types = coherent_value(
            metadata,
            protocol::GEOMETRY_TYPES,
            LEGACY_GEOMETRY_TYPE,
            true,
        )?;
        let extension = metadata
            .get(protocol::GEOARROW_EXTENSION_NAME)
            .map(String::as_str);
        let has_canonical_geometry = CANONICAL_GEOMETRY_KEYS
            .iter()
            .any(|key| metadata.contains_key(*key));
        let spatial = has_canonical_geometry
            || dimensions.is_some()
            || raw_srid.is_some()
            || spatial_semantics.is_some()
            || geometry_types.is_some()
            || extension == Some(GEOARROW_WKB_EXTENSION_NAME);

        if spatial && extension.is_some_and(|value| value != GEOARROW_WKB_EXTENSION_NAME) {
            return Err(contract_error(
                ErrorCategory::DataMapping,
                "metadati geometrici divergenti dall'estensione GeoArrow",
            ));
        }
        if spatial && !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Err(contract_error(
                ErrorCategory::DataMapping,
                "il contratto geometrico WKB richiede un campo Arrow binary",
            ));
        }

        let field_id = parse_optional_u32(metadata.get(protocol::FIELD_ID), "field_id", true)?;
        let srid = parse_optional_u32(raw_srid, "SRID", true)?;
        let contract = Self {
            field,
            field_id,
            encoding: metadata
                .get(protocol::GEOMETRY_ENCODING)
                .map(String::as_str),
            geometry_types,
            types_declaration: metadata
                .get(protocol::GEOMETRY_TYPES_DECLARATION)
                .map(String::as_str),
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
            precision: metadata
                .get(protocol::GEOMETRY_PRECISION)
                .map(String::as_str),
            spatial,
        };
        contract.validate_values()?;
        Ok(contract)
    }

    /// Applica anche i requisiti obbligatori per un campo emesso nel protocollo
    /// corrente, dopo che lo schema è stato validato con
    /// [`validate_schema_contract`].
    ///
    /// # Errors
    ///
    /// Restituisce `DataMapping` quando manca un metadato obbligatorio.
    pub fn validate_current(self) -> Result<()> {
        if !self.spatial {
            return Ok(());
        }
        for (value, name) in [
            (self.encoding, protocol::GEOMETRY_ENCODING),
            (self.dimensions, protocol::GEOMETRY_DIMENSIONS),
            (self.types_declaration, protocol::GEOMETRY_TYPES_DECLARATION),
            (self.crs_resolution, protocol::GEOMETRY_CRS_RESOLUTION),
        ] {
            if value.is_none() {
                return Err(contract_error(
                    ErrorCategory::DataMapping,
                    format!("campo geometrico privo del metadato obbligatorio {name}"),
                ));
            }
        }
        Ok(())
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

    fn validate_values(self) -> Result<()> {
        validate_enum(self.encoding, &["wkb", "ewkb"], "encoding geometrico")?;
        validate_enum(
            self.dimensions,
            &["xy", "xyz", "xym", "xyzm", "unknown"],
            "dimensioni geometriche",
        )?;
        validate_enum(
            self.types_declaration,
            &["exact", "mixed", "unresolved"],
            "dichiarazione tipi geometrici",
        )?;
        validate_enum(
            self.spatial_semantics,
            &["geometry", "geography"],
            "semantica spaziale",
        )?;
        validate_enum(
            self.precision,
            &["float64", "float32", "native"],
            "precisione geometrica",
        )?;
        validate_enum(
            self.axis_order,
            &[
                "lon_lat",
                "lat_lon",
                "easting_northing",
                "northing_easting",
                "other",
                "unknown",
            ],
            "ordine assi CRS",
        )?;
        validate_enum(
            self.crs_definition_format,
            &["wkt", "wkt2", "projjson"],
            "formato definizione CRS",
        )?;
        validate_enum(
            self.crs_resolution,
            &["resolved", "declared_unresolved", "missing"],
            "stato di risoluzione CRS",
        )?;
        self.validate_geometry_types()?;
        self.validate_crs()
    }

    fn validate_geometry_types(self) -> Result<()> {
        let Some(value) = self.geometry_types else {
            if self.types_declaration == Some("exact") {
                return Err(contract_error(
                    ErrorCategory::DataMapping,
                    "types_declaration=exact richiede tipi geometrici",
                ));
            }
            return Ok(());
        };
        if value.is_empty() || value.contains(char::is_whitespace) {
            return Err(contract_error(
                ErrorCategory::DataMapping,
                "lista dei tipi geometrici vuota o contenente spazi",
            ));
        }
        if self.types_declaration == Some("unresolved") {
            return Err(contract_error(
                ErrorCategory::DataMapping,
                "types_declaration=unresolved non ammette tipi geometrici",
            ));
        }
        let mut previous = None;
        for item in value.split(',') {
            let Some(index) = GEOMETRY_TYPES
                .iter()
                .position(|candidate| item.eq_ignore_ascii_case(candidate))
            else {
                return Err(contract_error(
                    ErrorCategory::DataMapping,
                    "tipo geometrico fuori dal catalogo canonico",
                ));
            };
            if previous.is_some_and(|old| index <= old) {
                return Err(contract_error(
                    ErrorCategory::DataMapping,
                    "tipi geometrici duplicati o fuori dall'ordine canonico",
                ));
            }
            previous = Some(index);
        }
        Ok(())
    }

    fn validate_crs(self) -> Result<()> {
        if self.crs_definition.is_some() != self.crs_definition_format.is_some() {
            return Err(contract_error(
                ErrorCategory::Crs,
                "definizione CRS e formato devono essere presenti insieme",
            ));
        }
        if (self.crs_id.is_some() || self.crs_definition.is_some()) && self.axis_order.is_none() {
            return Err(contract_error(
                ErrorCategory::Crs,
                "identificatore o definizione CRS richiede un ordine assi esplicito",
            ));
        }
        if self.crs_id.is_some_and(|value| {
            value.is_empty() || value.len() > 1_024 || value.chars().any(char::is_control)
        }) {
            return Err(contract_error(
                ErrorCategory::Crs,
                "identificatore CRS non valido",
            ));
        }
        if self.crs_resolution == Some("missing")
            && (self.srid.is_some()
                || self.crs_id.is_some()
                || self.crs_definition.is_some()
                || self.crs_definition_format.is_some()
                || self.axis_order.is_some())
        {
            return Err(contract_error(
                ErrorCategory::Crs,
                "CRS missing non ammette rappresentazioni CRS",
            ));
        }
        if let (Some(crs_id), Some(srid)) = (self.crs_id, self.srid) {
            if authority_srid(crs_id).is_some_and(|authority| authority != srid) {
                return Err(contract_error(
                    ErrorCategory::Crs,
                    "identificatore CRS e SRID numerico divergenti",
                ));
            }
        }
        Ok(())
    }
}

/// Verifica la versione a livello schema e tutti i campi.
///
/// Uno schema con almeno una chiave canonica deve dichiarare esattamente la
/// versione supportata. Le versioni future non vengono interpretate
/// parzialmente.
///
/// # Errors
///
/// Restituisce un errore categorizzato per versione assente/non supportata o
/// per un campo non conforme.
pub fn validate_schema_contract(schema: &Schema) -> Result<()> {
    let carries_canonical = schema.fields().iter().any(|field| {
        field.metadata().contains_key(protocol::FIELD_ID)
            || CANONICAL_GEOMETRY_KEYS
                .iter()
                .any(|key| field.metadata().contains_key(*key))
    });
    let version = schema
        .metadata()
        .get(protocol::CONTRACT_VERSION_KEY)
        .map(String::as_str);
    if carries_canonical && version.is_none() {
        return Err(contract_error(
            ErrorCategory::DataMapping,
            "schema con metadati canonici privo di plenora.contract.version",
        ));
    }
    if version.is_some_and(|value| value != protocol::CONTRACT_VERSION) {
        return Err(contract_error(
            ErrorCategory::Unsupported,
            "versione del contratto Arrow non supportata",
        ));
    }
    for field in schema.fields() {
        let contract = FieldContract::parse(field)?;
        if carries_canonical {
            contract.validate_current()?;
        }
    }
    Ok(())
}

fn authority_srid(value: &str) -> Option<u32> {
    let (authority, code) = value.split_once(':')?;
    authority
        .eq_ignore_ascii_case("EPSG")
        .then(|| code.parse::<u32>().ok())
        .flatten()
}

fn parse_optional_u32(
    value: Option<impl AsRef<str>>,
    label: &str,
    allow_zero: bool,
) -> Result<Option<u32>> {
    value
        .map(|raw| {
            raw.as_ref()
                .parse::<u32>()
                .ok()
                .filter(|parsed| allow_zero || *parsed > 0)
                .ok_or_else(|| {
                    contract_error(
                        ErrorCategory::DataMapping,
                        format!("{label} deve essere un intero decimale senza segno"),
                    )
                })
        })
        .transpose()
}

fn validate_enum(value: Option<&str>, allowed: &[&str], label: &str) -> Result<()> {
    if value.is_some_and(|candidate| !allowed.contains(&candidate)) {
        Err(contract_error(
            ErrorCategory::DataMapping,
            format!("{label} non valido"),
        ))
    } else {
        Ok(())
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
            return Err(contract_error(
                ErrorCategory::DataMapping,
                "metadati canonici e legacy divergenti",
            ));
        }
    }
    Ok(current.or(previous))
}

fn contract_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Validate,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn spatial_field(extra: &[(&str, &str)]) -> Field {
        let mut metadata = HashMap::from([
            (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
            (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
            (
                protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                "exact".to_owned(),
            ),
            (protocol::GEOMETRY_TYPES.to_owned(), "point".to_owned()),
            (
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "missing".to_owned(),
            ),
        ]);
        metadata.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
        );
        Field::new("geometry", DataType::Binary, true).with_metadata(metadata)
    }

    fn current_schema(field: Field) -> Schema {
        Schema::new_with_metadata(
            vec![field],
            HashMap::from([(
                protocol::CONTRACT_VERSION_KEY.to_owned(),
                protocol::CONTRACT_VERSION.to_owned(),
            )]),
        )
    }

    #[test]
    fn current_contract_requires_schema_version() {
        let schema = Schema::new(vec![spatial_field(&[])]);
        assert!(validate_schema_contract(&schema).is_err());
        assert!(validate_schema_contract(&current_schema(spatial_field(&[]))).is_ok());
    }

    #[test]
    fn future_contract_version_fails_closed() {
        let mut schema = current_schema(spatial_field(&[]));
        schema
            .metadata
            .insert(protocol::CONTRACT_VERSION_KEY.to_owned(), "2".to_owned());
        let error = validate_schema_contract(&schema).expect_err("future version");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn conflicting_crs_representations_are_rejected() {
        let field = spatial_field(&[
            (protocol::GEOMETRY_CRS_RESOLUTION, "resolved"),
            (protocol::GEOMETRY_CRS_ID, "EPSG:4326"),
            (protocol::GEOMETRY_SRID, "3003"),
            (protocol::GEOMETRY_AXIS_ORDER, "lat_lon"),
        ]);
        let error = FieldContract::parse(&field).expect_err("conflicting CRS");
        assert_eq!(error.category, ErrorCategory::Crs);
    }

    #[test]
    fn geometry_type_order_and_unresolved_state_are_closed() {
        let reversed = spatial_field(&[(protocol::GEOMETRY_TYPES, "polygon,point")]);
        assert!(FieldContract::parse(&reversed).is_err());
        let unresolved = spatial_field(&[(protocol::GEOMETRY_TYPES_DECLARATION, "unresolved")]);
        assert!(FieldContract::parse(&unresolved).is_err());
    }

    #[test]
    fn canonical_and_legacy_values_must_agree() {
        let field = spatial_field(&[(LEGACY_DIMENSIONS, "xyz")]);
        assert!(FieldContract::parse(&field).is_err());
    }
}
