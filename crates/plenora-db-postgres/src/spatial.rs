//! Traduttore `SpatialPredicate` → SQL `PostGIS`.
//!
//! Il consumer PFM invoca semantica canonica (`SpatialPredicate::Intersects`)
//! e non deve mai scrivere `ST_*` a mano. Il builder qui produce uno
//! `Statement` completo (SQL + parametri legati) da passare a
//! `TransactionScope::query` / `query_stream`.

use plenora_database_core::provider::ParameterValue;
use plenora_database_core::transaction::Statement;
use plenora_database_core::{
    DatabaseError, SpatialFilter, SpatialPredicate, SpatialReference,
};

/// Quoting sicuro di un identificatore SQL (colonna/tabella/schema).
///
/// `PostgreSQL` usa doppi apici. Gli eventuali doppi apici interni vengono
/// escape-ati raddoppiando.
fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn qualify_table(schema: Option<&str>, table: &str) -> String {
    schema.map_or_else(
        || quote_identifier(table),
        |s| format!("{}.{}", quote_identifier(s), quote_identifier(table)),
    )
}

fn validate_identifier(name: &str) -> Result<(), DatabaseError> {
    if name.is_empty() {
        return Err(DatabaseError::invalid_plan("identificatore vuoto"));
    }
    if name.len() > 63 {
        return Err(DatabaseError::invalid_plan(
            "identificatore eccede 63 caratteri (limite Postgres)",
        ));
    }
    // Non impongo case/underscore: Postgres li accetta con quoting.
    // Blocco solo caratteri di controllo per evitare abuso in log.
    if name.chars().any(char::is_control) {
        return Err(DatabaseError::invalid_plan(
            "identificatore contiene caratteri di controllo",
        ));
    }
    Ok(())
}

fn ref_expr(index: usize) -> String {
    format!("ST_GeomFromEWKB(${index})::geometry")
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
    validate_identifier(table)?;
    if let Some(s) = schema {
        validate_identifier(s)?;
    }
    for col in projection {
        validate_identifier(col)?;
    }
    validate_identifier(&filter.geometry_column)?;

    let projection_sql = projection
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");

    let mut params: Vec<ParameterValue> = Vec::with_capacity(2);
    let geom_index = 1;
    params.push(ParameterValue::Bytes(filter.reference.ewkb.clone()));

    let column = quote_identifier(&filter.geometry_column);
    let ref_sql = ref_expr(geom_index);

    let where_sql = match &filter.predicate {
        SpatialPredicate::Intersects => format!("ST_Intersects({column}, {ref_sql})"),
        SpatialPredicate::Contains => format!("ST_Contains({column}, {ref_sql})"),
        SpatialPredicate::Within => format!("ST_Within({column}, {ref_sql})"),
        SpatialPredicate::BoundingBox => format!("{column} && {ref_sql}"),
        SpatialPredicate::DWithin { distance_meters } => {
            if !distance_meters.is_finite() || *distance_meters < 0.0 {
                return Err(DatabaseError::invalid_plan(
                    "DWithin richiede distanza finita non-negativa",
                ));
            }
            let dist_index = 2;
            params.push(ParameterValue::F64(*distance_meters));
            format!("ST_DWithin({column}, {ref_sql}, ${dist_index})")
        }
    };

    let mut sql = format!(
        "SELECT {projection_sql} FROM {} WHERE {where_sql}",
        qualify_table(schema, table)
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
}
