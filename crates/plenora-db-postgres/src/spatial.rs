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
use plenora_database_core::transaction::Statement;
use plenora_database_core::{spatial_policy, spatial_probe};
use plenora_database_core::{DatabaseError, SpatialFilter, SpatialPredicate, SpatialReference};

/// Delega validazione e quoting all'implementazione condivisa del core.
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

/// Costruisce un `SELECT projection FROM [schema.]table WHERE <predicate>`
/// con la traduzione `PostGIS` del `SpatialFilter`.
///
/// La geometria di riferimento viene passata come parametro `bytea`
/// (`EWKB`), con lo SRID dichiarato; il server lo confronta con quello della
/// colonna (typmod) prima di ogni riga, e uno SRID diverso o una colonna
/// senza SRID dichiarato fanno fallire lo statement con `crs`
/// all'esecuzione.
///
/// # Errors
///
/// Ritorna `InvalidPlan` se il nome della tabella/colonna non è valido, se
/// la projection è vuota, se lo SRID o le dimensioni incorporati nell'EWKB
/// divergono da quelli dichiarati, o se il predicato è mal-formato (es.
/// `DWithin` con distanza negativa o non finita).
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

    // La policy condivisa copre distanza finita e positiva, DWithin geometry
    // con SRID geografico fail-closed e BoundingBox geography non supportato.
    // `validate` confronta lo SRID dichiarato con quello incorporato
    // nell'EWKB e le dimensioni: fino al 25/10 questo builder non lo
    // chiamava, a differenza del compilatore portable.
    filter.reference.validate()?;
    spatial_policy::validate_predicate(
        ProviderKind::Postgres,
        &filter.predicate,
        &filter.reference,
    )?;
    let srid = i32::try_from(filter.reference.srid).map_err(|_| {
        DatabaseError::invalid_plan("SRID oltre il range i32 supportato da PostGIS")
    })?;

    // Validazione e quoting passano da un'unica funzione condivisa.
    let projection_sql = projection
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    let relation = qualify_table(schema, table)?;
    let column_raw = quote_identifier(&filter.geometry_column)?;
    // La sonda porta lo SRID dichiarato, e il server lo confronta con quello
    // della colonna prima di ogni riga (`postgres_guarded_probe`). Fino al
    // 25/10 era `ST_GeomFromEWKB($1)`: un WKB senza SRID arrivava con SRID 0,
    // verso `geography` diventava 4326 per cast implicito, e `&&` non
    // confrontava gli SRID.
    let mut params: Vec<ParameterValue> = vec![
        ParameterValue::Bytes(filter.reference.ewkb.clone()),
        ParameterValue::I32(srid),
        ParameterValue::String(relation.clone()),
        ParameterValue::String(filter.geometry_column.clone()),
    ];
    let ref_sql = format!(
        "{}{}",
        spatial_probe::postgres_guarded_probe("$1", "$2::int4", "$3::text", "$4::text"),
        spatial_policy::postgres_cast_for(filter.reference.semantics)
    );
    let column = if filter.reference.semantics == SpatialSemantics::Geography {
        // Cast anche sulla colonna per operator resolution PostGIS
        // (allineato con compiler.rs).
        format!("{column_raw}::geography")
    } else {
        column_raw
    };

    let where_sql = match &filter.predicate {
        SpatialPredicate::Intersects => format!("ST_Intersects({column}, {ref_sql})"),
        SpatialPredicate::Contains => format!("ST_Contains({column}, {ref_sql})"),
        SpatialPredicate::Within => format!("ST_Within({column}, {ref_sql})"),
        SpatialPredicate::BoundingBox => format!("{column} && {ref_sql}"),
        SpatialPredicate::DWithin { distance_meters } => {
            // Distanza già validata (finita, non-negativa) da spatial_policy.
            params.push(ParameterValue::F64(*distance_meters));
            format!("ST_DWithin({column}, {ref_sql}, $5)")
        }
    };

    let mut sql = format!("SELECT {projection_sql} FROM {relation} WHERE {where_sql}");
    if let Some(n) = limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&n.to_string());
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
#[path = "spatial_tests.rs"]
mod tests;
