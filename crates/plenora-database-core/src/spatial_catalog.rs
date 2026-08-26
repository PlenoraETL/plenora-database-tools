//! Catalogo dichiarativo incorporato delle funzioni spatial portabili.

use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// Dove sta il risultato di una funzione che rende geometria.
///
/// # Perché il contratto lo dichiara
///
/// Un motore può restituire una geometria calcolata senza dirci in quale
/// sistema di riferimento sta. `MySQL` e `MariaDB` rendono SRID 0 per un envelope
/// costruito su una colonna in 3003: quello 0 non significa «il risultato è
/// altrove», significa che il motore non ha propagato il frame.
///
/// L'informazione però non è perduta, perché su quei due prodotti la colonna
/// d'ingresso ha un CRS dichiarato e verificato. Manca solo la regola che dice
/// se il risultato lo eredita — e quella regola è una proprietà della
/// funzione, non del prodotto: il centroide di una figura sta dov'è la figura,
/// su qualunque motore.
///
/// Il campo dichiara quella regola. Non la deduce: una misura per funzione la
/// verifica prima che la funzione venga aperta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrsRule {
    /// Il risultato sta nel sistema della geometria d'ingresso.
    Preserves,
    /// Il chiamante nomina l'SRID di destinazione, e il risultato ci sta per
    /// costruzione.
    Argument,
    /// Il frame del risultato non è derivabile dal contratto: entrano due
    /// geometrie, e lo conservano solo se lo condividono — condizione che si
    /// dimostra a runtime, non si dichiara qui.
    Undefined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialFunctionSpec {
    pub id: String,
    pub category: String,
    pub arguments: Vec<String>,
    pub returns: String,
    /// Presente esattamente quando `returns` è `geometry`: un risultato
    /// scalare non sta in nessun sistema di riferimento, e attribuirgliene uno
    /// sarebbe un campo privo di significato.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crs: Option<CrsRule>,
    pub portable: bool,
    pub postgres: String,
}

impl SpatialFunctionSpec {
    /// La regola di CRS di una funzione che rende geometria.
    ///
    /// Restituisce `None` per le funzioni che rendono uno scalare, dove la
    /// domanda non si pone.
    #[must_use]
    pub const fn crs_rule(&self) -> Option<CrsRule> {
        self.crs
    }
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
    fn every_geometry_result_declares_where_it_lands_and_no_scalar_does() {
        let catalog = spatial_function_catalog().expect("catalog fixture");
        let disagreeing = catalog
            .functions
            .iter()
            .filter(|function| (function.returns == "geometry") != function.crs.is_some())
            .map(|function| function.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            disagreeing.is_empty(),
            "regola di CRS e tipo di ritorno non coincidono: {disagreeing:?}"
        );
    }

    #[test]
    fn two_geometries_in_means_the_frame_is_not_derivable() {
        let catalog = spatial_function_catalog().expect("catalog fixture");
        // Il risultato conserva il frame soltanto se i due ingressi lo
        // condividono, e il contratto non lo sa: è una condizione da
        // dimostrare a runtime, non una regola da dichiarare.
        let overclaiming = catalog
            .functions
            .iter()
            .filter(|function| {
                function
                    .arguments
                    .iter()
                    .filter(|a| *a == "geometry")
                    .count()
                    > 1
                    && function.crs.is_some_and(|rule| rule != CrsRule::Undefined)
            })
            .map(|function| function.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            overclaiming.is_empty(),
            "prendono due geometrie e dichiarano di sapere dove cade il risultato: {overclaiming:?}"
        );
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
