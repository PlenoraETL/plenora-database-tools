//! Profilo spatial portabile.
//!
//! Il consumer PFM invoca semantica canonica (`SpatialPredicate::Intersects`)
//! invece di funzioni provider-specifiche (`ST_Intersects(...)`). La
//! traduzione verso il motore concreto è responsabilità del driver.

use crate::geometry::{Dimensions, SpatialSemantics};
use serde::{Deserialize, Serialize};

/// Predicato spaziale canonico.
///
/// Non è un catalogo esaustivo dei predicati OGC: sono i predicati che la
/// roadmap PFM richiede come minimo (`DBT-PFM-006`) — coprono il read GIS
/// operativo tipico (mappa + filtri applicativi).
#[allow(clippy::derive_partial_eq_without_eq)] // DWithin contiene f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpatialPredicate {
    /// La colonna geometry interseca il riferimento.
    Intersects,
    /// La colonna contiene interamente il riferimento.
    Contains,
    /// La colonna è contenuta nel riferimento.
    Within,
    /// La colonna è entro `distance_meters` dal riferimento.
    DWithin { distance_meters: f64 },
    /// `Bounding-box` overlap (indice-friendly): equivalente a `&&` in
    /// `PostGIS`. Utile per filtri di viewport prima di predicati più stretti.
    BoundingBox,
}

impl SpatialPredicate {
    #[must_use]
    pub const fn requires_distance(&self) -> bool {
        matches!(self, Self::DWithin { .. })
    }
}

/// Riferimento spaziale usato da un `SpatialFilter`: geometria in formato
/// EWKB (WKB con SRID prefixed) + attributi di dimensione e semantica per
/// la coerenza col contratto geometry-arrow del progetto.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialReference {
    pub ewkb: Vec<u8>,
    pub srid: u32,
    pub dimensions: Dimensions,
    pub semantics: SpatialSemantics,
}

/// Filtro spaziale applicato a una colonna geometry.
#[allow(clippy::derive_partial_eq_without_eq)] // contiene SpatialPredicate con f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialFilter {
    /// Nome della colonna geometry sul target. Il driver è responsabile del
    /// quoting.
    pub geometry_column: String,
    pub predicate: SpatialPredicate,
    pub reference: SpatialReference,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicate_requires_distance_only_for_dwithin() {
        assert!(!SpatialPredicate::Intersects.requires_distance());
        assert!(!SpatialPredicate::Contains.requires_distance());
        assert!(!SpatialPredicate::Within.requires_distance());
        assert!(!SpatialPredicate::BoundingBox.requires_distance());
        assert!(SpatialPredicate::DWithin {
            distance_meters: 1.0
        }
        .requires_distance());
    }

    #[test]
    fn predicate_serializes_snake_case() {
        let p = SpatialPredicate::DWithin {
            distance_meters: 500.0,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("d_within"));
        assert!(json.contains("distance_meters"));
    }
}
