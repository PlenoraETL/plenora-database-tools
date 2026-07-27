//! Catalogo dichiarativo incorporato delle funzioni spatial portabili.

use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialFunctionSpec {
    pub id: String,
    pub category: String,
    pub arguments: Vec<String>,
    pub returns: String,
    pub portable: bool,
    pub postgres: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialFunctionCatalog {
    pub schema_version: u32,
    pub functions: Vec<SpatialFunctionSpec>,
}

#[must_use]
/// Restituisce il catalogo spatial incorporato.
///
/// # Panics
///
/// Panica soltanto se il catalogo versionato nel repository non è JSON valido;
/// il gate offline impedisce di pubblicare tale stato.
pub fn spatial_function_catalog() -> &'static SpatialFunctionCatalog {
    static CATALOG: OnceLock<SpatialFunctionCatalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str(include_str!("../../../catalog/spatial-functions.v1.json"))
            .expect("catalogo spatial incorporato valido")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_is_unique_and_complete() {
        let catalog = spatial_function_catalog();
        assert_eq!(catalog.schema_version, 1);
        assert_eq!(catalog.functions.len(), 29);
        let unique = catalog
            .functions
            .iter()
            .map(|function| function.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), catalog.functions.len());
    }
}
