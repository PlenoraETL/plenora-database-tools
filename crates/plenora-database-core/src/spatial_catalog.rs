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

/// Restituisce il catalogo spatial incorporato.
///
/// # Errors
///
/// Restituisce `InvalidPlan` se il catalogo incorporato non è valido; un
/// pacchetto corrotto non può causare un panic nel processo chiamante.
pub fn spatial_function_catalog() -> crate::Result<&'static SpatialFunctionCatalog> {
    static CATALOG: OnceLock<std::result::Result<SpatialFunctionCatalog, String>> = OnceLock::new();
    CATALOG
        .get_or_init(|| {
            serde_json::from_str(include_str!("../../../catalog/spatial-functions.v1.json"))
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_or_else(
            |_| {
                Err(crate::DatabaseError::invalid_plan(
                    "catalogo spatial incorporato non valido",
                ))
            },
            Ok,
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_is_unique_and_complete() {
        let catalog = spatial_function_catalog().expect("catalog fixture");
        assert_eq!(catalog.schema_version, 1);
        assert_eq!(
            catalog.functions.len(),
            crate::query::SpatialFunction::ALL.len()
        );
        let unique = catalog
            .functions
            .iter()
            .map(|function| function.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), catalog.functions.len());
    }

    #[test]
    fn spatial_function_wire_names_match_the_versioned_catalog() {
        let catalog = spatial_function_catalog().expect("catalog fixture");
        let catalog_ids = catalog
            .functions
            .iter()
            .map(|function| function.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let wire_ids = crate::query::SpatialFunction::ALL
            .iter()
            .map(|function| {
                serde_json::to_value(function)
                    .expect("serialize spatial function")
                    .as_str()
                    .expect("spatial function string")
                    .to_owned()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(wire_ids, catalog_ids);
    }
}
