//! Traduttore `SpatialPredicate` → SQL `PostGIS`.
//!
//! Il consumer PFM invoca semantica canonica (`SpatialPredicate::Intersects`)
//! e non deve mai scrivere `ST_*` a mano. Il builder qui produce uno
//! `Statement` completo (SQL + parametri legati) da passare a
//! `TransactionScope::query` / `query_stream`.

use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::identifier::{quote_identifier as core_quote, IdentifierDialect};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::spatial_policy;
use plenora_database_core::transaction::Statement;
use plenora_database_core::{
    DatabaseError, SpatialFilter, SpatialPredicate, SpatialReference,
};

/// Delega a `plenora-database-core::identifier` (Fase A2). Prima
/// duplicava le stesse regole di validazione + quoting di
/// `compiler.rs` e `sql::Renderer`.
fn quote_identifier(name: &str) -> Result<String, DatabaseError> {
    core_quote(IdentifierDialect::Postgres, name)
}

fn qualify_table(schema: Option<&str>, table: &str) -> Result<String, DatabaseError> {
    let table_q = quote_identifier(table)?;
    match schema {
        None => Ok(table_q),
        Some(s) => {
            let schema_q = quote_identifier(s)?;
            Ok(format!("{schema_q}.{table_q}"))
        }
    }
}

fn ref_expr(index: usize, semantics: SpatialSemantics) -> String {
    // Fase B: cast delegato a `spatial_policy::postgres_cast_for`.
    // Prima era inline replicando le regole di `compiler.rs`.
    format!(
        "ST_GeomFromEWKB(${index}){}",
        spatial_policy::postgres_cast_for(semantics)
    )
}

/// Costruisce un `SELECT projection FROM [schema.]table WHERE <predicate>`
/// con la traduzione `PostGIS` del `SpatialFilter`. La geometria di
/// riferimento viene passata come parametro `bytea` (`EWKB`).
///
/// # Errors
///
/// Ritorna `InvalidPlan` se il nome della tabella/colonna non è valido, se
/// la projection è vuota, o se il predicato è mal-formato (es. `DWithin` con
/// distanza negativa o non finita).
pub fn build_spatial_select(
    schema: Option<&str>,
    table: &str,
    projection: &[&str],
    filter: &SpatialFilter,
    limit: Option<u64>,
) -> Result<Statement, DatabaseError> {
    if projection.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "projection spaziale non può essere vuota",
        ));
    }

    // Fase B: validazione predicato/reference centralizzata in
    // `spatial_policy`. Copre distanza finita+positiva, DWithin+
    // Geometry+SRID_geografico fail-closed, BoundingBox+Geography
    // Unsupported. Prima erano inline duplicati con `compiler.rs`.
    spatial_policy::validate_predicate(ProviderKind::Postgres, &filter.predicate, &filter.reference)?;

    // Fase A2: quote_identifier centralizzato — valida + quota in
    // un'unica funzione, ritorna errore uniforme.
    let projection_sql = projection
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    let mut params: Vec<ParameterValue> = Vec::with_capacity(2);
    let geom_index = 1;
    params.push(ParameterValue::Bytes(filter.reference.ewkb.clone()));

    let column_raw = quote_identifier(&filter.geometry_column)?;
    let column = if filter.reference.semantics == SpatialSemantics::Geography {
        // Cast anche sulla colonna per operator resolution PostGIS
        // (allineato con compiler.rs).
        format!("{column_raw}::geography")
    } else {
        column_raw
    };
    let ref_sql = ref_expr(geom_index, filter.reference.semantics);

    let where_sql = match &filter.predicate {
        SpatialPredicate::Intersects => format!("ST_Intersects({column}, {ref_sql})"),
        SpatialPredicate::Contains => format!("ST_Contains({column}, {ref_sql})"),
        SpatialPredicate::Within => format!("ST_Within({column}, {ref_sql})"),
        SpatialPredicate::BoundingBox => format!("{column} && {ref_sql}"),
        SpatialPredicate::DWithin { distance_meters } => {
            // Distanza già validata (finita, non-negativa) da spatial_policy.
            let dist_index = 2;
            params.push(ParameterValue::F64(*distance_meters));
            format!("ST_DWithin({column}, {ref_sql}, ${dist_index})")
        }
    };

    let mut sql = format!(
        "SELECT {projection_sql} FROM {} WHERE {where_sql}",
        qualify_table(schema, table)?
    );
    if let Some(n) = limit {
        use std::fmt::Write;
        write!(sql, " LIMIT {n}").expect("write to String non fallisce");
    }

    Ok(Statement { sql, params })
}

/// Costruisce un valore `SpatialReference` a partire da un buffer `EWKB` e
/// dai suoi attributi. È un helper puro (nessuna I/O): serve al consumer per
/// non doverli inline.
#[must_use]
pub const fn spatial_reference(
    ewkb: Vec<u8>,
    srid: u32,
    dimensions: plenora_database_core::geometry::Dimensions,
    semantics: plenora_database_core::geometry::SpatialSemantics,
) -> SpatialReference {
    SpatialReference {
        ewkb,
        srid,
        dimensions,
        semantics,
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // matches!() con parametri f64 letterali
mod tests {
    use super::*;
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};

    fn dummy_ewkb() -> Vec<u8> {
        vec![0x01, 0x02, 0x03, 0x04]
    }

    fn dummy_reference() -> SpatialReference {
        SpatialReference {
            ewkb: dummy_ewkb(),
            srid: 4326,
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geometry,
        }
    }

    #[test]
    fn intersects_builds_st_intersects() {
        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Intersects,
            reference: dummy_reference(),
        };
        let stmt = build_spatial_select(
            Some("plenora_fixture"),
            "events",
            &["event_id", "geom"],
            &filter,
            Some(100),
        )
        .expect("build");
        assert_eq!(
            stmt.sql,
            "SELECT \"event_id\", \"geom\" FROM \"plenora_fixture\".\"events\" \
             WHERE ST_Intersects(\"geom\", ST_GeomFromEWKB($1)::geometry) LIMIT 100"
        );
        assert_eq!(stmt.params.len(), 1);
        assert!(matches!(stmt.params[0], ParameterValue::Bytes(_)));
    }

    #[test]
    fn dwithin_binds_distance_parameter() {
        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::DWithin {
                distance_meters: 250.0,
            },
            reference: dummy_reference(),
        };
        let stmt =
            build_spatial_select(None, "poi", &["id"], &filter, None).expect("build");
        assert_eq!(
            stmt.sql,
            "SELECT \"id\" FROM \"poi\" \
             WHERE ST_DWithin(\"geom\", ST_GeomFromEWKB($1)::geometry, $2)"
        );
        assert_eq!(stmt.params.len(), 2);
        assert!(matches!(stmt.params[1], ParameterValue::F64(v) if v == 250.0));
    }

    #[test]
    fn bounding_box_uses_index_friendly_operator() {
        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::BoundingBox,
            reference: dummy_reference(),
        };
        let stmt =
            build_spatial_select(None, "buildings", &["id"], &filter, None).expect("build");
        assert!(stmt.sql.contains("\"geom\" && ST_GeomFromEWKB"));
    }

    #[test]
    fn contains_and_within_are_distinct() {
        let mut filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Contains,
            reference: dummy_reference(),
        };
        let a = build_spatial_select(None, "t", &["id"], &filter, None).unwrap();
        filter.predicate = SpatialPredicate::Within;
        let b = build_spatial_select(None, "t", &["id"], &filter, None).unwrap();
        assert!(a.sql.contains("ST_Contains"));
        assert!(b.sql.contains("ST_Within"));
    }

    #[test]
    fn empty_projection_is_invalid() {
        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Intersects,
            reference: dummy_reference(),
        };
        assert!(build_spatial_select(None, "t", &[], &filter, None).is_err());
    }

    #[test]
    fn dwithin_negative_distance_is_invalid() {
        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::DWithin {
                distance_meters: -1.0,
            },
            reference: dummy_reference(),
        };
        assert!(build_spatial_select(None, "t", &["id"], &filter, None).is_err());
    }

    #[test]
    fn identifiers_are_quoted_and_escaped() {
        let filter = SpatialFilter {
            geometry_column: "geo\"m".into(),
            predicate: SpatialPredicate::Intersects,
            reference: dummy_reference(),
        };
        let stmt = build_spatial_select(None, "e\"vil", &["c\"ol"], &filter, None).unwrap();
        assert!(stmt.sql.contains("\"e\"\"vil\""));
        assert!(stmt.sql.contains("\"c\"\"ol\""));
        assert!(stmt.sql.contains("\"geo\"\"m\""));
    }

    #[test]
    fn control_char_in_identifier_is_rejected() {
        let filter = SpatialFilter {
            geometry_column: "geom\n".into(),
            predicate: SpatialPredicate::Intersects,
            reference: dummy_reference(),
        };
        assert!(build_spatial_select(None, "t", &["id"], &filter, None).is_err());
    }

    #[test]
    fn geography_semantics_casts_reference_to_geography() {
        // v0.2 (fix P0.5 finding): con semantics=Geography il ref è castato
        // a ::geography invece che ::geometry — necessario per query verso
        // colonne geography (PostGIS non fa cast implicito cross-type).
        let filter = SpatialFilter {
            geometry_column: "g".into(),
            predicate: SpatialPredicate::DWithin {
                distance_meters: 500.0,
            },
            reference: SpatialReference {
                ewkb: dummy_ewkb(),
                srid: 4326,
                dimensions: Dimensions::Xy,
                semantics: SpatialSemantics::Geography,
            },
        };
        let stmt = build_spatial_select(None, "poi", &["id"], &filter, None).expect("build");
        assert!(
            stmt.sql.contains("ST_GeomFromEWKB($1)::geography"),
            "atteso cast ::geography, sql: {}",
            stmt.sql
        );
        assert!(
            !stmt.sql.contains("::geometry"),
            "non deve contenere ::geometry con semantics=Geography, sql: {}",
            stmt.sql
        );
    }
}
