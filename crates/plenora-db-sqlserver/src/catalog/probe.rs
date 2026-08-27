use super::{one_result, optional, required, text};
use crate::SqlServerSession;
use plenora_database_core::{CancellationToken, ErrorPhase, Result};
use serde::{Deserialize, Serialize};
use tiberius::Query;

const PROBE_SQL: &str = r"
SELECT
    CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),
    CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128)),
    CAST(SERVERPROPERTY('Edition') AS nvarchar(256)),
    CAST(SERVERPROPERTY('EngineEdition') AS int),
    CAST(SERVERPROPERTY('IsHadrEnabled') AS int),
    DB_NAME(),
    d.compatibility_level,
    d.collation_name,
    d.is_read_committed_snapshot_on,
    d.snapshot_isolation_state,
    TYPE_ID(N'geometry'),
    TYPE_ID(N'geography'),
    CAST(COALESCE(SERVERPROPERTY('IsPolyBaseInstalled'), 0) AS int)
FROM sys.databases AS d
WHERE d.name = DB_NAME();
";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerProbe {
    pub product_version: String,
    pub product_level: String,
    pub edition: String,
    pub engine_edition: i32,
    pub hadr_enabled: bool,
    pub database: String,
    pub compatibility_level: u8,
    pub collation: String,
    pub read_committed_snapshot: bool,
    pub snapshot_isolation_state: u8,
    pub geometry_type_id: Option<i32>,
    pub geography_type_id: Option<i32>,
    pub polybase_installed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerObjectSummary {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub object_id: i32,
    pub kind: String,
    pub temporal_type: u8,
    pub memory_optimized: bool,
}

/// Rileva versione e proprietà del database corrente senza dedurle dal DSN.
///
/// # Errors
///
/// Fallisce e mette in quarantena la sessione per timeout, cancellazione,
/// errore TDS o risposta incompatibile.
pub async fn probe_server(
    session: &mut SqlServerSession,
    cancellation: &CancellationToken,
) -> Result<SqlServerProbe> {
    let rows = one_result(
        session
            .execute_query(Query::new(PROBE_SQL), ErrorPhase::Probe, cancellation)
            .await?,
    )?;
    let row = rows
        .first()
        .ok_or_else(|| super::mapping_error("probe SQL Server senza righe"))?;
    let compatibility_level = required::<u8>(row, 6, "compatibility_level")?;
    let snapshot_isolation_state = required::<u8>(row, 9, "snapshot_isolation_state")?;
    Ok(SqlServerProbe {
        product_version: text(row, 0, "product_version")?,
        product_level: text(row, 1, "product_level")?,
        edition: text(row, 2, "edition")?,
        engine_edition: required(row, 3, "engine_edition")?,
        hadr_enabled: required::<i32>(row, 4, "is_hadr_enabled")? == 1,
        database: text(row, 5, "database")?,
        compatibility_level,
        collation: text(row, 7, "collation")?,
        read_committed_snapshot: required(row, 8, "is_read_committed_snapshot_on")?,
        snapshot_isolation_state,
        geometry_type_id: optional(row, 10, "geometry_type_id")?,
        geography_type_id: optional(row, 11, "geography_type_id")?,
        polybase_installed: required::<i32>(row, 12, "is_polybase_installed")? == 1,
    })
}

/// Elenca gli schemi visibili all'utente corrente in ordine deterministico.
///
/// # Errors
///
/// Propaga errori redatti di sessione o mapping.
pub async fn list_schemas(
    session: &mut SqlServerSession,
    cancellation: &CancellationToken,
) -> Result<Vec<String>> {
    // Il catalogo pubblico esclude gli schemi di sistema. Un consumer che ne
    // ha bisogno deve
    // interrogare sys.schemas direttamente.
    let rows = one_result(
        session
            .execute_query(
                Query::new(
                    r"
SELECT s.name
FROM sys.schemas AS s
WHERE (HAS_PERMS_BY_NAME(QUOTENAME(s.name), 'SCHEMA', 'SELECT') = 1
       OR s.principal_id = DATABASE_PRINCIPAL_ID())
  AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
  AND s.name NOT LIKE 'db\_%' ESCAPE '\'
ORDER BY s.name;
",
                ),
                ErrorPhase::Probe,
                cancellation,
            )
            .await?,
    )?;
    rows.iter().map(|row| text(row, 0, "schema_name")).collect()
}

/// Elenca tabelle e viste visibili, con filtro schema opzionale bindato.
///
/// # Errors
///
/// Propaga errori redatti di sessione o mapping.
pub async fn list_objects(
    session: &mut SqlServerSession,
    schema: Option<&str>,
    cancellation: &CancellationToken,
) -> Result<Vec<SqlServerObjectSummary>> {
    let mut query = Query::new(
        r"
SELECT
    DB_NAME(),
    s.name,
    o.name,
    o.object_id,
    o.type_desc,
    CAST(COALESCE(t.temporal_type, 0) AS tinyint),
    CAST(COALESCE(t.is_memory_optimized, 0) AS bit)
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
LEFT JOIN sys.tables AS t ON t.object_id = o.object_id
WHERE o.type IN ('U', 'V', 'ET')
  AND o.is_ms_shipped = 0
  AND (@P1 IS NULL OR s.name = @P1)
  AND HAS_PERMS_BY_NAME(
        QUOTENAME(s.name) + N'.' + QUOTENAME(o.name),
        'OBJECT',
        'SELECT'
      ) = 1
ORDER BY s.name, o.name;
",
    );
    query.bind(schema.map(ToOwned::to_owned));
    let rows = one_result(
        session
            .execute_query(query, ErrorPhase::Probe, cancellation)
            .await?,
    )?;
    rows.iter()
        .map(|row| {
            Ok(SqlServerObjectSummary {
                catalog: text(row, 0, "catalog")?,
                schema: text(row, 1, "schema")?,
                name: text(row, 2, "name")?,
                object_id: required(row, 3, "object_id")?,
                kind: text(row, 4, "type_desc")?,
                temporal_type: required(row, 5, "temporal_type")?,
                memory_optimized: required(row, 6, "is_memory_optimized")?,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "probe_tests.rs"]
mod tests;
