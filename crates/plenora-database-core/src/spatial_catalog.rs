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
#[path = "spatial_catalog_tests.rs"]
mod tests;
