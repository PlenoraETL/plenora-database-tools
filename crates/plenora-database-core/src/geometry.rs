use serde::{Deserialize, Serialize};

pub const GEOARROW_WKB_EXTENSION_NAME: &str = "geoarrow.wkb";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dimensions {
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GeometryType {
    Unknown,
    Point,
    Multipoint,
    Linestring,
    Multilinestring,
    Polygon,
    Multipolygon,
    Geometrycollection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialSemantics {
    Geometry,
    Geography,
    FeatureService,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeometryContract {
    pub field_id: u32,
    pub encoding: String,
    pub dimensions: Dimensions,
    pub nullable: bool,
    pub geometry_type: Option<GeometryType>,
    pub srid: Option<u32>,
    pub crs: Option<String>,
    pub spatial_semantics: Option<SpatialSemantics>,
}

impl GeometryContract {
    #[must_use]
    pub fn is_geoarrow_wkb(&self) -> bool {
        self.encoding == GEOARROW_WKB_EXTENSION_NAME
    }
}
