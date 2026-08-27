//! Policy spaziale unificata: SRID geografici, semantica di `DWithin`,
//! validazione predicati portable.
//!
//! E la fonte unica per compilatore portable, provider e binding Python. Il
//! binding espone la lista tramite `geographic_srids()` senza replicarla.

use crate::geometry::SpatialSemantics;
use crate::plan::ProviderKind;
use crate::spatial_predicate::{SpatialPredicate, SpatialReference};
use crate::{DatabaseError, Result};

/// SRID che rappresentano coordinate geografiche (lat/lon in gradi).
///
/// **Perché la lista è fissa e non un flag su SRID arbitrari**: PFM e
/// consumer tipici trattano una manciata di SRID globali. Aggiungerne
/// uno è additivo (safe); catturarne uno in più darebbe falsi positivi
/// solo su casi patologici (SRID custom che coincidono con codici
/// standard, non realistico per PFM).
///
/// Riferimento: EPSG registry, categoria "Geographic 2D".
pub const GEOGRAPHIC_SRIDS: &[u32] = &[
    4326, // WGS 84 (GPS)
    4269, // NAD 83
    4267, // NAD 27
    4258, // ETRS89
    4283, // GDA94
];

/// True se il SRID rappresenta un sistema di coordinate geografiche
/// (lat/lon in gradi) — su questi SRID le funzioni `PostGIS` `geometry`
/// producono distanze in gradi, non in metri.
#[must_use]
pub fn is_geographic_srid(srid: u32) -> bool {
    GEOGRAPHIC_SRIDS.contains(&srid)
}

/// Cast SQL da applicare per il dialetto `Postgres` in base a `semantics`.
/// Ritorna il suffisso completo pronto (`"::geometry"` o `"::geography"`).
///
/// Per `MySQL` non c'è distinzione tipo, quindi questa funzione non è
/// applicabile (il compiler `MySQL` passa `Geography` come hint senza cast).
#[must_use]
pub const fn postgres_cast_for(semantics: SpatialSemantics) -> &'static str {
    match semantics {
        SpatialSemantics::Geometry => "::geometry",
        SpatialSemantics::Geography => "::geography",
    }
}

/// Valida la combinazione `(predicate, reference)` prima della compilazione.
///
/// Restituisce `InvalidPlan` per combinazioni che produrrebbero silent
/// wrong result e `Unsupported` per predicati non implementati sul provider.
///
/// Casi coperti:
/// - `DWithin` + `Geometry` + SRID geografico → `InvalidPlan`
///   (silent wrong result: distanza in gradi rispetto al nome
///   `distance_meters`).
/// - `BoundingBox` + `Geography` su `Postgres` → `Unsupported`
///   (operator `&&` esiste solo per `geometry`).
/// - `DWithin` su `MySQL` → `Unsupported` (no `ST_DWithin` nativo).
/// - `DWithin` con distanza non-finita o negativa → `InvalidPlan`.
///
/// # Errors
///
/// Vedi elenco sopra.
pub fn validate_predicate(
    provider: ProviderKind,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
) -> Result<()> {
    // Check universale su DWithin: distanza finita non-negativa.
    if let SpatialPredicate::DWithin { distance_meters } = predicate {
        if !distance_meters.is_finite() || *distance_meters < 0.0 {
            return Err(DatabaseError::invalid_plan(
                "DWithin richiede distanza finita non-negativa",
            ));
        }
    }

    match provider {
        ProviderKind::Postgres => validate_postgres(predicate, reference),
        ProviderKind::Mysql => validate_mysql(predicate),
        ProviderKind::Sqlserver => validate_sqlserver(predicate),
        // Gli altri provider sono rifiutati dal compilatore portable prima di
        // raggiungere questa validazione.
        _ => Ok(()),
    }
}

/// Cosa T-SQL non sa esprimere di un predicato spatial.
///
/// Una sola cosa, e non e una prudenza: `raw` ha chiesto `STDWithin` a SQL
/// Server e il server ha risposto che il metodo non esiste, ne su `geometry` ne
/// su `geography`.
///
/// Si potrebbe scrivere come `STDistance(...) <= d`, e non e la stessa cosa: su
/// `geography` quella distanza e in metri e su `geometry` nelle unita del
/// sistema di riferimento, mentre il contratto porta un `distance_meters`.
/// Emetterlo su una colonna `geometry` in gradi confronterebbe metri con gradi
/// e renderebbe righe sbagliate senza che nessuno se ne accorga — il difetto
/// peggiore di un rifiuto.
fn validate_sqlserver(predicate: &SpatialPredicate) -> Result<()> {
    match predicate {
        SpatialPredicate::DWithin { .. } => Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            crate::ErrorPhase::Prepare,
            "STDWithin non esiste in T-SQL, su nessuna delle due semantiche. La forma con              STDistance confronterebbe i metri del contratto con le unita del sistema di              riferimento su una colonna geometry: usa Intersects su un buffer costruito dal              chiamante.",
        )),
        SpatialPredicate::Intersects
        | SpatialPredicate::Contains
        | SpatialPredicate::Within
        | SpatialPredicate::BoundingBox => Ok(()),
    }
}

fn validate_postgres(predicate: &SpatialPredicate, reference: &SpatialReference) -> Result<()> {
    match predicate {
        SpatialPredicate::BoundingBox if reference.semantics == SpatialSemantics::Geography => {
            Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                crate::ErrorPhase::Prepare,
                "BoundingBox con SpatialSemantics::Geography non supportato \
                 (operator && è solo geometry). Usa Intersects.",
            ))
        }
        // PostGIS definisce ST_Contains e ST_Within solo per geometry.
        // Chiamare
        // ST_Contains(geography, geography) causa "function does not
        // exist" a runtime.
        // Ref: https://postgis.net/docs/manual-dev/ST_Contains.html
        //      https://postgis.net/docs/manual-dev/en/ST_Within.html
        SpatialPredicate::Contains if reference.semantics == SpatialSemantics::Geography => {
            Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                crate::ErrorPhase::Prepare,
                "ST_Contains non supportato per SpatialSemantics::Geography \
                 (PostGIS espone Contains solo per geometry). \
                 Riproietta su geometry o usa ST_Covers/ST_Intersects.",
            ))
        }
        SpatialPredicate::Within if reference.semantics == SpatialSemantics::Geography => {
            Err(DatabaseError::unsupported(
                ProviderKind::Postgres,
                crate::ErrorPhase::Prepare,
                "ST_Within non supportato per SpatialSemantics::Geography \
                 (PostGIS espone Within solo per geometry). \
                 Riproietta su geometry o usa ST_CoveredBy/ST_Intersects.",
            ))
        }
        SpatialPredicate::DWithin { .. }
            if reference.semantics == SpatialSemantics::Geometry
                && is_geographic_srid(reference.srid) =>
        {
            Err(DatabaseError::invalid_plan(format!(
                "DWithin con SpatialSemantics::Geometry su SRID geografico {} \
                 produrrebbe distanza in gradi (fuorviante rispetto al nome \
                 `distance_meters`). Usa SpatialSemantics::Geography per metri \
                 reali, oppure riproietta su un SRID planare.",
                reference.srid
            )))
        }
        _ => Ok(()),
    }
}

fn validate_mysql(predicate: &SpatialPredicate) -> Result<()> {
    match predicate {
        SpatialPredicate::DWithin { .. } => Err(DatabaseError::unsupported(
            ProviderKind::Mysql,
            crate::ErrorPhase::Prepare,
            "DWithin non supportato da MySQL (no ST_DWithin nativo). \
             Usa ST_Distance() < X come workaround via SQL raw.",
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::Dimensions;

    fn ref_with(srid: u32, semantics: SpatialSemantics) -> SpatialReference {
        SpatialReference {
            ewkb: vec![0x01],
            srid,
            dimensions: Dimensions::Xy,
            semantics,
        }
    }

    #[test]
    fn geographic_srids_include_wgs84_and_nad_family() {
        assert!(is_geographic_srid(4326));
        assert!(is_geographic_srid(4269));
        assert!(is_geographic_srid(4267));
        assert!(is_geographic_srid(4258));
        assert!(is_geographic_srid(4283));
    }

    #[test]
    fn projected_srids_are_not_geographic() {
        // Web mercator, UTM 32N, UTM 33N — tutti planari con unità
        // metri, tipici per PFM.
        assert!(!is_geographic_srid(3857));
        assert!(!is_geographic_srid(25832));
        assert!(!is_geographic_srid(32633));
    }

    #[test]
    fn cast_dispatch_matches_semantics() {
        assert_eq!(postgres_cast_for(SpatialSemantics::Geometry), "::geometry");
        assert_eq!(
            postgres_cast_for(SpatialSemantics::Geography),
            "::geography"
        );
    }

    #[test]
    fn dwithin_geometry_on_wgs84_is_rejected_for_postgres() {
        let err = validate_predicate(
            ProviderKind::Postgres,
            &SpatialPredicate::DWithin {
                distance_meters: 100.0,
            },
            &ref_with(4326, SpatialSemantics::Geometry),
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
        assert!(err.message.contains("Geography"));
    }

    #[test]
    fn dwithin_geography_on_wgs84_is_accepted() {
        validate_predicate(
            ProviderKind::Postgres,
            &SpatialPredicate::DWithin {
                distance_meters: 100.0,
            },
            &ref_with(4326, SpatialSemantics::Geography),
        )
        .unwrap();
    }

    #[test]
    fn dwithin_geometry_on_projected_srid_is_accepted() {
        validate_predicate(
            ProviderKind::Postgres,
            &SpatialPredicate::DWithin {
                distance_meters: 100.0,
            },
            &ref_with(3857, SpatialSemantics::Geometry),
        )
        .unwrap();
    }

    #[test]
    fn bounding_box_with_geography_is_rejected_for_postgres() {
        let err = validate_predicate(
            ProviderKind::Postgres,
            &SpatialPredicate::BoundingBox,
            &ref_with(4326, SpatialSemantics::Geography),
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::Unsupported);
    }

    #[test]
    fn contains_and_within_with_geography_are_rejected_for_postgres() {
        // PostGIS non espone ST_Contains/ST_Within per geography.
        for predicate in [SpatialPredicate::Contains, SpatialPredicate::Within] {
            let err = validate_predicate(
                ProviderKind::Postgres,
                &predicate,
                &ref_with(4326, SpatialSemantics::Geography),
            )
            .unwrap_err();
            assert_eq!(err.category, crate::ErrorCategory::Unsupported);
        }
    }

    #[test]
    fn intersects_with_geography_is_accepted() {
        // ST_Intersects è disponibile sia per geometry che per geography.
        validate_predicate(
            ProviderKind::Postgres,
            &SpatialPredicate::Intersects,
            &ref_with(4326, SpatialSemantics::Geography),
        )
        .unwrap();
    }

    #[test]
    fn dwithin_on_mysql_is_unsupported() {
        let err = validate_predicate(
            ProviderKind::Mysql,
            &SpatialPredicate::DWithin {
                distance_meters: 100.0,
            },
            &ref_with(4326, SpatialSemantics::Geography),
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::Unsupported);
    }

    #[test]
    fn dwithin_negative_distance_is_rejected_universally() {
        for provider in [ProviderKind::Postgres, ProviderKind::Mysql] {
            let err = validate_predicate(
                provider,
                &SpatialPredicate::DWithin {
                    distance_meters: -1.0,
                },
                &ref_with(3857, SpatialSemantics::Geometry),
            )
            .unwrap_err();
            assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
        }
    }

    #[test]
    fn dwithin_nan_is_rejected() {
        let err = validate_predicate(
            ProviderKind::Postgres,
            &SpatialPredicate::DWithin {
                distance_meters: f64::NAN,
            },
            &ref_with(3857, SpatialSemantics::Geometry),
        )
        .unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
    }
}
