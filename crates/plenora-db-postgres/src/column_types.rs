//! I tipi delle colonne di una relazione, con il typmod, per l'ammissione
//! dei letterali portable (`plenora_database_core::operand_admission`).

use plenora_database_core::operand_admission::{CatalogType, RelationTypes};
use plenora_database_core::{ErrorPhase, Result};

/// Le colonne di una relazione risolta come la risolve il server
/// (`to_regclass`), con il tipo base dei domini e il loro typmod; `enum` per
/// un tipo enumerato.
const COLUMN_TYPES_SQL: &str = "\
SELECT a.attname::text, \
    CASE WHEN b.typtype = 'e' THEN 'enum' ELSE b.typname::text END, \
    CASE WHEN t.typbasetype <> 0 THEN t.typtypmod ELSE a.atttypmod END \
FROM pg_catalog.pg_attribute a \
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
JOIN pg_catalog.pg_type b ON b.oid = CASE WHEN t.typbasetype <> 0 THEN t.typbasetype ELSE t.oid END \
WHERE a.attrelid = to_regclass(CASE WHEN $1::text IS NULL \
        THEN quote_ident($2::text) \
        ELSE quote_ident($1::text) || '.' || quote_ident($2::text) END) \
  AND a.attnum > 0 AND NOT a.attisdropped";

/// Il tipo del catalogo dal nome e dal typmod: precisione e scala di
/// `numeric`, lunghezza di `varchar` e `bpchar`, precisione di `timestamp`.
#[must_use]
pub fn catalog_type(name: &str, typmod: i32) -> CatalogType {
    let mut catalog = CatalogType {
        name: name.to_owned(),
        ..CatalogType::default()
    };
    if typmod < 0 {
        return catalog;
    }
    match name {
        "numeric" => {
            let packed = typmod - 4;
            catalog.precision = u64::try_from((packed >> 16) & 0xffff).ok();
            // La scala e un intero a 11 bit con segno (PostgreSQL 15).
            let scale = packed & 0x7ff;
            catalog.scale = Some(i64::from(if scale > 1023 { scale - 2048 } else { scale }));
        }
        "varchar" | "bpchar" => catalog.length = u64::try_from(typmod - 4).ok(),
        "timestamp" | "timestamptz" => catalog.datetime_precision = u64::try_from(typmod).ok(),
        _ => {}
    }
    catalog
}

/// # Errors
///
/// Gli errori del catalogo in fase `prepare`.
pub async fn column_types(
    client: &tokio_postgres::Client,
    schema: Option<&str>,
    object: &str,
) -> Result<Option<RelationTypes>> {
    let rows = client
        .query(COLUMN_TYPES_SQL, &[&schema, &object])
        .await
        .map_err(|error| crate::error::classify_error(ErrorPhase::Prepare, &error))?;
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        rows.iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    catalog_type(&row.get::<_, String>(1), row.get::<_, i32>(2)),
                )
            })
            .collect(),
    ))
}

#[cfg(test)]
#[path = "column_types_tests.rs"]
mod tests;
