//! Profilo spatial portabile.
//!
//! Il consumer PFM invoca semantica canonica (`SpatialPredicate::Intersects`)
//! invece di funzioni provider-specifiche (`ST_Intersects(...)`). La
//! traduzione verso il motore concreto è responsabilità del driver.

use crate::ewkb::{inspect_ewkb_detailed, EwkbInspection};
use crate::geometry::{Dimensions, SpatialSemantics};
use crate::{DatabaseError, Result};
use serde::{Deserialize, Serialize};

/// Predicato spaziale canonico.
///
/// Non è un catalogo esaustivo OGC: delimita la semantica portabile supportata
/// dal read GIS operativo.
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
    ///
    /// # ⚠️ Semantica delle unità
    ///
    /// Il nome del campo implica **metri**, ma il significato effettivo
    /// dipende dalla combinazione `SpatialReference.semantics` × `srid`:
    ///
    /// | Semantics  | SRID                       | Unità effettive          | Stato           |
    /// |------------|----------------------------|--------------------------|-----------------|
    /// | Geography  | qualsiasi                  | **metri** (garantito)    | ok              |
    /// | Geometry   | geografico (4326, 4269, …) | gradi                    | **rifiutato**   |
    /// | Geometry   | proiettato in metri (3857, 25832, 32633, …) | metri | ok |
    /// | Geometry   | proiettato **non metri** (es. EPSG:2229 piedi US, EPSG:27700 metri UK ma varia) | unità del CRS (piedi, chains, ecc.) | ⚠️ *silent wrong result* |
    ///
    /// **Terzo caso — attenzione**: il compilatore portable non ha
    /// modo di sapere l'unità di misura di un SRID proiettato arbitrario
    /// (servirebbe accesso al catalogo EPSG lato client). Se dichiari
    /// `Geometry` + SRID proiettato in piedi/miglia/chains, la query
    /// passa e restituisce risultati numericamente sbagliati (un
    /// `distance_meters: 100` viene interpretato come 100 piedi ≈ 30 m).
    ///
    /// **Raccomandazione**: per query `DWithin` fidati SEMPRE di
    /// `SpatialSemantics::Geography` (o riproietta su un SRID
    /// esplicitamente in metri come EPSG:3857 web mercator). Non
    /// affidarsi a nomi SRID per dedurre l'unità.
    ///
    /// # `MySQL`
    ///
    /// `DWithin` non supportato (nessun `ST_DWithin` nativo); il
    /// compilatore fallisce `Unsupported`.
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
///
/// **Costruzione**: preferire [`Self::new_validated`] a costruzione via
/// literal — validata contro l'EWKB reale. Il costruttore literal è
/// mantenuto pubblico per compat / deserializzazione JSON, ma non
/// verifica coerenza fra `srid`/`dimensions` dichiarati e bytes EWKB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialReference {
    pub ewkb: Vec<u8>,
    pub srid: u32,
    pub dimensions: Dimensions,
    pub semantics: SpatialSemantics,
}

impl SpatialReference {
    /// Costruisce un `SpatialReference` validando che i metadati
    /// dichiarati (`srid`, `dimensions`) coincidano con quelli reali
    /// dell'EWKB.
    ///
    /// Il controllo impedisce che un EWKB con SRID embedded contraddica il
    /// riferimento usato da `spatial_policy::validate_predicate`, evitando
    /// risultati calcolati nel sistema di riferimento sbagliato.
    ///
    /// # Errors
    ///
    /// - `InvalidPlan` se l'EWKB non è parsabile.
    /// - `InvalidPlan` se il SRID embedded nell'EWKB differisce da
    ///   quello dichiarato (l'EWKB può omettere il SRID prefix — WKB
    ///   puro — in tal caso il `srid` dichiarato viene accettato senza
    ///   check).
    /// - `InvalidPlan` se le `dimensions` dichiarate divergono da
    ///   quelle dell'EWKB (es. dichiarato `Xy` ma EWKB è Point Z).
    pub fn new_validated(
        ewkb: Vec<u8>,
        srid: u32,
        dimensions: Dimensions,
        semantics: SpatialSemantics,
    ) -> Result<Self> {
        let candidate = Self {
            ewkb,
            srid,
            dimensions,
            semantics,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    /// Ri-esegue la validazione EWKB/SRID/dimensioni su un
    /// `SpatialReference` già costruito. Necessario perché lo struct
    /// ha campi pubblici (compat serde `deny_unknown_fields`) e può
    /// essere costruito literal o via `Deserialize` bypassando
    /// `new_validated`.
    ///
    /// Il compiler portable chiama `validate()` prima di generare
    /// SQL — così anche un `SpatialReference` deserializzato da JSON
    /// non può aggirare la policy con SRID divergenti.
    ///
    /// # Errors
    ///
    /// - `InvalidPlan` se l'EWKB non è parsabile.
    /// - `InvalidPlan` se il SRID embedded nell'EWKB differisce da
    ///   `self.srid` (WKB puro senza SRID embedded è accettato).
    /// - `InvalidPlan` se le `dimensions` dichiarate divergono da
    ///   quelle dell'EWKB. `Dimensions::Unknown` è wildcard.
    pub fn validate(&self) -> Result<()> {
        let inspection: EwkbInspection = inspect_ewkb_detailed(&self.ewkb, u64::MAX, u64::MAX)?;

        if let Some(embedded_srid) = inspection.root.srid {
            if embedded_srid != self.srid {
                return Err(DatabaseError::invalid_plan(format!(
                    "SRID divergente: dichiarato {}, embedded nell'EWKB {embedded_srid}. \
                     Ri-serializza l'EWKB con SRID {} o correggi la dichiarazione.",
                    self.srid, self.srid
                )));
            }
        }

        if self.dimensions != Dimensions::Unknown {
            let embedded_label = inspection.root.dimensions_label();
            let declared_label = dimensions_label(self.dimensions);
            if embedded_label != declared_label {
                return Err(DatabaseError::invalid_plan(format!(
                    "dimensioni EWKB divergenti: dichiarato `{declared_label}`, embedded \
                     `{embedded_label}`. Il consumer deve produrre EWKB coerente col contratto."
                )));
            }
        }
        Ok(())
    }
}

const fn dimensions_label(dimensions: Dimensions) -> &'static str {
    match dimensions {
        Dimensions::Xy => "xy",
        Dimensions::Xyz => "xyz",
        Dimensions::Xym => "xym",
        Dimensions::Xyzm => "xyzm",
        Dimensions::Unknown => "unknown",
    }
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
#[path = "spatial_predicate_tests.rs"]
mod tests;
