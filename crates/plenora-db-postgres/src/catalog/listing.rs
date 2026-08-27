use super::relation_kind;
use crate::error::classify_error;
use plenora_database_core::{ErrorPhase, Result};
use serde_json::{json, Value};
use tokio_postgres::Client;

pub async fn list_catalogs(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname",
            &[],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

pub async fn list_schemas(client: &Client) -> Result<Vec<String>> {
    // Esclude gli schemi di sistema. Il consumer che ha bisogno anche
    // dei system schemas deve interrogare pg_namespace direttamente.
    let rows = client
        .query(
            r"
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND schema_name NOT LIKE 'pg\_temp\_%' ESCAPE '\'
              AND schema_name NOT LIKE 'pg\_toast\_temp\_%' ESCAPE '\'
            ORDER BY schema_name
            ",
            &[],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

pub async fn list_objects(client: &Client, schema: &str) -> Result<Vec<Value>> {
    let rows = client
        .query(
            r"
            SELECT c.relname, c.relkind::text, c.relispartition
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1
              AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
            ORDER BY c.relname
            ",
            &[&schema],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    Ok(rows
        .iter()
        .map(|row| {
            json!({
                "name": row.get::<_, String>(0),
                "kind": relation_kind(&row.get::<_, String>(1)),
                "is_partition": row.get::<_, bool>(2)
            })
        })
        .collect())
}
