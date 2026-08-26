use serde::{Deserialize, Serialize};

pub const GEOARROW_WKB_EXTENSION_NAME: &str = "geoarrow.wkb";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GeometryEncoding {
    Wkb,
    Ewkb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dimensions {
    Xy,
    Xyz,
    Xym,
    Xyzm,
    Unknown,
}

pub type CoordinateDimensions = Dimensions;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GeometryType {
    Point,
    Linestring,
    Polygon,
    Multipoint,
    Multilinestring,
    Multipolygon,
    Geometrycollection,
    Circularstring,
    Compoundcurve,
    Curvepolygon,
    Multicurve,
    Multisurface,
    Polyhedralsurface,
    Tin,
    Triangle,
    Unknown,
}

impl GeometryType {
    pub const CANONICAL_ORDER: [Self; 16] = [
        Self::Point,
        Self::Linestring,
        Self::Polygon,
        Self::Multipoint,
        Self::Multilinestring,
        Self::Multipolygon,
        Self::Geometrycollection,
        Self::Circularstring,
        Self::Compoundcurve,
        Self::Curvepolygon,
        Self::Multicurve,
        Self::Multisurface,
        Self::Polyhedralsurface,
        Self::Tin,
        Self::Triangle,
        Self::Unknown,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypesDeclaration {
    Exact,
    Mixed,
    Unresolved,
}

// `Ord` perche il contratto la usa come chiave di
// `SpatialCapabilities::functions_by_semantics`, e un ordine stabile e cio che
// rende il documento pubblicato confrontabile byte per byte fra due corse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialSemantics {
    Geometry,
    Geography,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatePrecision {
    Float64,
    Float32,
    Native,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrsDefinitionFormat {
    Wkt,
    Wkt2,
    Projjson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxisOrder {
    LonLat,
    LatLon,
    EastingNorthing,
    NorthingEasting,
    Other,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedCrs {
    pub id: Option<String>,
    pub srid: Option<u32>,
    pub definition: Option<String>,
    pub definition_format: Option<CrsDefinitionFormat>,
    pub axis_order: AxisOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredCrs {
    pub id: Option<String>,
    pub srid: Option<u32>,
    pub definition: Option<String>,
    pub definition_format: Option<CrsDefinitionFormat>,
    pub axis_order: AxisOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum CrsResolution {
    Resolved(ResolvedCrs),
    DeclaredUnresolved(DeclaredCrs),
    Missing,
}

/// Il contratto di una colonna geometrica, **in memoria**.
///
/// Non e la forma di un documento JSON: il contratto di colonna viaggia come
/// metadata di campo Arrow, con le chiavi `plenora.geometry.*` dichiarate in
/// [`crate::protocol`] e lette da [`crate::field_contract`]. Fino a poco fa
/// `contracts/v2/common.schema.json` conteneva anche un `$defs`
/// `geometry_contract` che descriveva un JSON con altri campi
/// (`geometry_type` al singolare, `srid`, `crs` come stringa): nessuno lo
/// riferiva, nessun produttore lo emetteva, e diceva una cosa diversa da
/// questo tipo. E stato tolto, e `phase0_validate.py` ora rifiuta le
/// definizioni senza riferimenti perche non ne ricompaiano.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeometryContract {
    pub field_id: FieldId,
    pub encoding: GeometryEncoding,
    pub dimensions: Dimensions,
    pub nullable: bool,
    pub types_declaration: TypesDeclaration,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub geometry_types: Vec<GeometryType>,
    pub crs: CrsResolution,
    pub spatial_semantics: Option<SpatialSemantics>,
    pub precision: Option<CoordinatePrecision>,
}

impl GeometryContract {
    /// Verifica le relazioni fra dichiarazione e insieme dei tipi.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` se la dichiarazione è incoerente o se i tipi
    /// non sono unici e nell'ordine canonico.
    pub fn validate(&self) -> crate::Result<()> {
        match self.types_declaration {
            TypesDeclaration::Exact if self.geometry_types.is_empty() => {
                return Err(crate::DatabaseError::invalid_plan(
                    "types_declaration=exact richiede almeno un tipo geometrico",
                ));
            }
            TypesDeclaration::Unresolved if !self.geometry_types.is_empty() => {
                return Err(crate::DatabaseError::invalid_plan(
                    "types_declaration=unresolved non ammette tipi geometrici",
                ));
            }
            TypesDeclaration::Exact | TypesDeclaration::Mixed | TypesDeclaration::Unresolved => {}
        }

        let mut previous_index = None;
        for geometry_type in &self.geometry_types {
            let Some(index) = GeometryType::CANONICAL_ORDER
                .iter()
                .position(|candidate| candidate == geometry_type)
            else {
                return Err(crate::DatabaseError::invalid_plan(
                    "tipo geometrico fuori dal catalogo canonico",
                ));
            };
            if previous_index.is_some_and(|previous| index <= previous) {
                return Err(crate::DatabaseError::invalid_plan(
                    "i tipi geometrici devono essere unici e in ordine canonico",
                ));
            }
            previous_index = Some(index);
        }

        validate_crs(&self.crs)
    }

    #[must_use]
    pub const fn is_geoarrow_wkb(&self) -> bool {
        matches!(
            self.encoding,
            GeometryEncoding::Wkb | GeometryEncoding::Ewkb
        )
    }
}

fn validate_crs(crs: &CrsResolution) -> crate::Result<()> {
    let (definition, format, axis_order) = match crs {
        CrsResolution::Resolved(value) => (
            value.definition.as_ref(),
            value.definition_format,
            value.axis_order,
        ),
        CrsResolution::DeclaredUnresolved(value) => (
            value.definition.as_ref(),
            value.definition_format,
            value.axis_order,
        ),
        CrsResolution::Missing => return Ok(()),
    };

    if definition.is_some() != format.is_some() {
        return Err(crate::DatabaseError::invalid_plan(
            "definizione CRS e relativo formato devono essere presenti insieme",
        ));
    }
    if definition.is_some() && axis_order == AxisOrder::Unknown {
        return Err(crate::DatabaseError::invalid_plan(
            "una definizione CRS richiede un ordine degli assi esplicito",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_contract() -> GeometryContract {
        GeometryContract {
            field_id: FieldId(7),
            encoding: GeometryEncoding::Ewkb,
            dimensions: Dimensions::Xyzm,
            nullable: true,
            types_declaration: TypesDeclaration::Exact,
            geometry_types: vec![GeometryType::Point, GeometryType::Multipolygon],
            crs: CrsResolution::Resolved(ResolvedCrs {
                id: Some("EPSG:4326".to_owned()),
                srid: Some(4326),
                definition: None,
                definition_format: None,
                axis_order: AxisOrder::LatLon,
            }),
            spatial_semantics: Some(SpatialSemantics::Geometry),
            precision: Some(CoordinatePrecision::Float64),
        }
    }

    #[test]
    fn all_geometry_types_and_dimensions_round_trip() {
        for geometry_type in GeometryType::CANONICAL_ORDER {
            let encoded = serde_json::to_string(&geometry_type).expect("serialize geometry type");
            let decoded: GeometryType =
                serde_json::from_str(&encoded).expect("deserialize geometry type");
            assert_eq!(decoded, geometry_type);
        }
        for dimensions in [
            Dimensions::Xy,
            Dimensions::Xyz,
            Dimensions::Xym,
            Dimensions::Xyzm,
            Dimensions::Unknown,
        ] {
            let encoded = serde_json::to_string(&dimensions).expect("serialize dimensions");
            let decoded: Dimensions =
                serde_json::from_str(&encoded).expect("deserialize dimensions");
            assert_eq!(decoded, dimensions);
        }
    }

    #[test]
    fn exact_types_are_non_empty_unique_and_canonical() {
        assert!(base_contract().validate().is_ok());

        let mut empty = base_contract();
        empty.geometry_types.clear();
        assert!(empty.validate().is_err());

        let mut duplicate = base_contract();
        duplicate.geometry_types = vec![GeometryType::Point, GeometryType::Point];
        assert!(duplicate.validate().is_err());

        let mut reversed = base_contract();
        reversed.geometry_types = vec![GeometryType::Polygon, GeometryType::Point];
        assert!(reversed.validate().is_err());
    }

    #[test]
    fn unresolved_types_cannot_claim_an_observed_type() {
        let mut contract = base_contract();
        contract.types_declaration = TypesDeclaration::Unresolved;
        assert!(contract.validate().is_err());
        contract.geometry_types.clear();
        assert!(contract.validate().is_ok());
    }

    #[test]
    fn crs_definition_requires_format_and_axis_order() {
        let mut contract = base_contract();
        contract.crs = CrsResolution::Resolved(ResolvedCrs {
            id: None,
            srid: None,
            definition: Some("GEOGCRS[...]".to_owned()),
            definition_format: None,
            axis_order: AxisOrder::LatLon,
        });
        assert!(contract.validate().is_err());
    }
}
