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
    /// dell'EWKB. Fix review #5.
    ///
    /// Prima di questo costruttore era possibile dichiarare
    /// `srid: 3857` e passare un EWKB con SRID embedded 4326: il
    /// compiler bindava l'EWKB come `bytea` e `PostGIS` usava il SRID
    /// embedded, aggirando `spatial_policy::validate_predicate` e
    /// producendo silent wrong result.
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
        // Uso limiti "infiniti" perché il caller ha il proprio budget.
        // `inspect_ewkb_detailed` è O(bytes) e stateless.
        let inspection: EwkbInspection = inspect_ewkb_detailed(&ewkb, u64::MAX, u64::MAX)?;

        // SRID embedded: se presente, deve coincidere. Se assente
        // (WKB puro), accettiamo il SRID dichiarato senza check.
        if let Some(embedded_srid) = inspection.root.srid {
            if embedded_srid != srid {
                return Err(DatabaseError::invalid_plan(format!(
                    "SRID divergente: dichiarato {srid}, embedded nell'EWKB {embedded_srid}. \
                     Ri-serializza l'EWKB con SRID {srid} o correggi la dichiarazione."
                )));
            }
        }

        // Dimensions embedded vs dichiarate. `Dimensions::Unknown`
        // accetta qualsiasi cosa (compat consumer che non conosce le
        // dims a priori).
        if dimensions != Dimensions::Unknown {
            let embedded_label = inspection.root.dimensions_label();
            let declared_label = dimensions_label(dimensions);
            if embedded_label != declared_label {
                return Err(DatabaseError::invalid_plan(format!(
                    "dimensioni EWKB divergenti: dichiarato `{declared_label}`, embedded \
                     `{embedded_label}`. Il consumer deve produrre EWKB coerente col contratto."
                )));
            }
        }

        Ok(Self {
            ewkb,
            srid,
            dimensions,
            semantics,
        })
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

    // ---- Fix review #5: SpatialReference::new_validated -------------

    /// Costruisce un EWKB Point 2D con SRID prefixed. Little-endian.
    /// Formato: byte order + `type_with_srid_flag` + srid + x + y.
    fn ewkb_point_xy(srid: u32, x: f64, y: f64) -> Vec<u8> {
        let mut b = Vec::with_capacity(25);
        b.push(0x01); // little-endian
        // Type = 1 (Point) | 0x20000000 (SRID flag)
        b.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
        b.extend_from_slice(&srid.to_le_bytes());
        b.extend_from_slice(&x.to_le_bytes());
        b.extend_from_slice(&y.to_le_bytes());
        b
    }

    /// Come sopra ma Point Z (3D) — flag Z = 0x80000000.
    fn ewkb_point_xyz(srid: u32, x: f64, y: f64, z: f64) -> Vec<u8> {
        let mut b = Vec::with_capacity(33);
        b.push(0x01);
        b.extend_from_slice(&0xA000_0001_u32.to_le_bytes()); // SRID + Z
        b.extend_from_slice(&srid.to_le_bytes());
        b.extend_from_slice(&x.to_le_bytes());
        b.extend_from_slice(&y.to_le_bytes());
        b.extend_from_slice(&z.to_le_bytes());
        b
    }

    #[test]
    fn new_validated_accepts_matching_srid_and_dimensions() {
        let ewkb = ewkb_point_xy(4326, 9.19, 45.46);
        let r = SpatialReference::new_validated(
            ewkb,
            4326,
            Dimensions::Xy,
            SpatialSemantics::Geography,
        )
        .unwrap();
        assert_eq!(r.srid, 4326);
    }

    #[test]
    fn new_validated_rejects_srid_mismatch() {
        let ewkb = ewkb_point_xy(4326, 9.19, 45.46);
        // Dichiaro 3857 ma l'EWKB è WGS84 → deve fallire (attacco
        // di aggiramento della policy DWithin+Geometry+geog_srid).
        let err = SpatialReference::new_validated(
            ewkb,
            3857,
            Dimensions::Xy,
            SpatialSemantics::Geometry,
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
        assert!(err.message.contains("SRID"));
    }

    #[test]
    fn new_validated_rejects_dimensions_mismatch() {
        // EWKB Point Z (3D) ma dichiaro Xy → fail.
        let ewkb = ewkb_point_xyz(4326, 9.19, 45.46, 100.0);
        let err = SpatialReference::new_validated(
            ewkb,
            4326,
            Dimensions::Xy,
            SpatialSemantics::Geometry,
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
        assert!(err.message.contains("dimensioni"));
    }

    #[test]
    fn new_validated_accepts_dimensions_unknown_as_wildcard() {
        // Consumer che non conosce le dims a priori dichiara Unknown.
        let ewkb = ewkb_point_xyz(4326, 9.19, 45.46, 100.0);
        SpatialReference::new_validated(
            ewkb,
            4326,
            Dimensions::Unknown,
            SpatialSemantics::Geometry,
        )
        .unwrap();
    }

    #[test]
    fn new_validated_rejects_malformed_ewkb() {
        // inspect_ewkb propaga la propria categoria (DataMapping per
        // bytes malformati) — non normalizzata a InvalidPlan.
        assert!(SpatialReference::new_validated(
            vec![0x00, 0x01, 0x02],
            4326,
            Dimensions::Xy,
            SpatialSemantics::Geometry,
        )
        .is_err());
    }
}
