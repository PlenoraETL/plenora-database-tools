use super::{one_result, optional_text, required, text};
use crate::SqlServerSession;
use plenora_database_core::{CancellationToken, ErrorPhase, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use tiberius::Query;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
// I flag provengono da proprietà indipendenti di sys.columns: accorparli
// perderebbe informazione del catalogo.
pub struct SqlServerColumn {
    pub ordinal: i32,
    pub name: String,
    pub type_schema: String,
    pub native_type: String,
    pub max_length: i16,
    pub precision: u8,
    pub scale: u8,
    pub nullable: bool,
    pub identity: bool,
    pub computed: bool,
    pub generated_always_type: u8,
    pub collation: Option<String>,
    pub default_definition: Option<String>,
    pub computed_definition: Option<String>,
    pub computed_persisted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerConstraint {
    pub name: String,
    pub kind: String,
    pub definition: Option<String>,
    pub columns: Option<String>,
    pub referenced_object: Option<String>,
    pub disabled: bool,
    pub not_trusted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
// I flag di sys.indexes sono combinabili e fanno parte del token strutturale.
pub struct SqlServerIndex {
    pub index_id: i32,
    pub name: Option<String>,
    pub kind: String,
    pub unique: bool,
    pub primary_key: bool,
    pub unique_constraint: bool,
    pub disabled: bool,
    pub filtered: bool,
    pub filter_definition: Option<String>,
    pub columns: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerSchemaToken {
    pub schema_version: u32,
    pub database_id: i32,
    pub object_id: i32,
    pub structural_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerObjectDescription {
    pub database_id: i32,
    pub object_id: i32,
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub temporal_type: u8,
    pub memory_optimized: bool,
    pub durability: Option<String>,
    pub columns: Vec<SqlServerColumn>,
    pub constraints: Vec<SqlServerConstraint>,
    pub indexes: Vec<SqlServerIndex>,
    pub token: SqlServerSchemaToken,
}

/// Descrive struttura, vincoli e indici e produce un token strutturale.
///
/// # Errors
///
/// Fallisce se l'oggetto non esiste/non è visibile o se il catalogo non
/// rispetta il contratto atteso.
pub async fn describe_object(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<SqlServerObjectDescription> {
    let identity = load_identity(session, schema, object, cancellation).await?;
    let columns = load_columns(session, schema, object, cancellation).await?;
    let constraints = load_constraints(session, schema, object, cancellation).await?;
    let indexes = load_indexes(session, schema, object, cancellation).await?;
    let encoded = serde_json::to_vec(&(&identity, &columns, &constraints, &indexes))
        .map_err(|_| super::mapping_error("serializzazione token schema SQL Server fallita"))?;
    let fingerprint = hex_digest(&encoded)?;
    let token = SqlServerSchemaToken {
        schema_version: 1,
        database_id: identity.database_id,
        object_id: identity.object_id,
        structural_fingerprint: fingerprint,
    };
    Ok(SqlServerObjectDescription {
        database_id: identity.database_id,
        object_id: identity.object_id,
        catalog: identity.catalog,
        schema: identity.schema,
        name: identity.name,
        kind: identity.kind,
        temporal_type: identity.temporal_type,
        memory_optimized: identity.memory_optimized,
        durability: identity.durability,
        columns,
        constraints,
        indexes,
        token,
    })
}

#[derive(Debug, Serialize)]
struct ObjectIdentity {
    database_id: i32,
    object_id: i32,
    catalog: String,
    schema: String,
    name: String,
    kind: String,
    temporal_type: u8,
    memory_optimized: bool,
    durability: Option<String>,
}

async fn load_identity(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<ObjectIdentity> {
    let rows = execute_bound(
        session,
        r"
SELECT
    CAST(DB_ID() AS int),
    o.object_id,
    DB_NAME(),
    s.name,
    o.name,
    o.type_desc,
    CAST(COALESCE(t.temporal_type, 0) AS tinyint),
    CAST(COALESCE(t.is_memory_optimized, 0) AS bit),
    t.durability_desc
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
LEFT JOIN sys.tables AS t ON t.object_id = o.object_id
WHERE s.name = @P1
  AND o.name = @P2
  AND o.type IN ('U', 'V')
  AND HAS_PERMS_BY_NAME(
        QUOTENAME(s.name) + N'.' + QUOTENAME(o.name),
        'OBJECT',
        'SELECT'
      ) = 1;
",
        schema,
        object,
        cancellation,
    )
    .await?;
    let row = rows
        .first()
        .ok_or_else(|| super::mapping_error("oggetto SQL Server non trovato o non visibile"))?;
    Ok(ObjectIdentity {
        database_id: required(row, 0, "database_id")?,
        object_id: required(row, 1, "object_id")?,
        catalog: text(row, 2, "catalog")?,
        schema: text(row, 3, "schema")?,
        name: text(row, 4, "name")?,
        kind: text(row, 5, "kind")?,
        temporal_type: required(row, 6, "temporal_type")?,
        memory_optimized: required(row, 7, "memory_optimized")?,
        durability: optional_text(row, 8, "durability")?,
    })
}

async fn load_columns(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<SqlServerColumn>> {
    let rows = execute_bound(
        session,
        r"
SELECT
    c.column_id,
    c.name,
    ts.name,
    ty.name,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    c.is_computed,
    c.generated_always_type,
    c.collation_name,
    dc.definition,
    cc.definition,
    CAST(COALESCE(cc.is_persisted, 0) AS bit)
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
JOIN sys.columns AS c ON c.object_id = o.object_id
JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
JOIN sys.schemas AS ts ON ts.schema_id = ty.schema_id
LEFT JOIN sys.default_constraints AS dc
  ON dc.parent_object_id = c.object_id
 AND dc.parent_column_id = c.column_id
LEFT JOIN sys.computed_columns AS cc
  ON cc.object_id = c.object_id
 AND cc.column_id = c.column_id
WHERE s.name = @P1 AND o.name = @P2
ORDER BY c.column_id;
",
        schema,
        object,
        cancellation,
    )
    .await?;
    rows.iter()
        .map(|row| {
            Ok(SqlServerColumn {
                ordinal: required(row, 0, "column_id")?,
                name: text(row, 1, "column_name")?,
                type_schema: text(row, 2, "type_schema")?,
                native_type: text(row, 3, "native_type")?,
                max_length: required(row, 4, "max_length")?,
                precision: required(row, 5, "precision")?,
                scale: required(row, 6, "scale")?,
                nullable: required(row, 7, "is_nullable")?,
                identity: required(row, 8, "is_identity")?,
                computed: required(row, 9, "is_computed")?,
                generated_always_type: required(row, 10, "generated_always_type")?,
                collation: optional_text(row, 11, "collation")?,
                default_definition: optional_text(row, 12, "default_definition")?,
                computed_definition: optional_text(row, 13, "computed_definition")?,
                computed_persisted: required(row, 14, "is_persisted")?,
            })
        })
        .collect()
}

async fn load_constraints(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<SqlServerConstraint>> {
    let rows = execute_bound(
        session,
        r"
WITH target AS
(
    SELECT o.object_id
    FROM sys.objects AS o
    JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    WHERE s.name = @P1 AND o.name = @P2
)
SELECT
    kc.name,
    kc.type_desc,
    CAST(NULL AS nvarchar(max)),
    STRING_AGG(COL_NAME(ic.object_id, ic.column_id), N',')
      WITHIN GROUP (ORDER BY ic.key_ordinal),
    CAST(NULL AS nvarchar(517)),
    CAST(0 AS bit),
    CAST(0 AS bit)
FROM target
JOIN sys.key_constraints AS kc ON kc.parent_object_id = target.object_id
JOIN sys.index_columns AS ic
  ON ic.object_id = kc.parent_object_id
 AND ic.index_id = kc.unique_index_id
 AND ic.key_ordinal > 0
GROUP BY kc.name, kc.type_desc
UNION ALL
SELECT
    cc.name,
    N'CHECK_CONSTRAINT',
    cc.definition,
    COL_NAME(cc.parent_object_id, cc.parent_column_id),
    CAST(NULL AS nvarchar(517)),
    cc.is_disabled,
    cc.is_not_trusted
FROM target
JOIN sys.check_constraints AS cc ON cc.parent_object_id = target.object_id
UNION ALL
SELECT
    fk.name,
    N'FOREIGN_KEY_CONSTRAINT',
    CAST(NULL AS nvarchar(max)),
    STRING_AGG(COL_NAME(fkc.parent_object_id, fkc.parent_column_id), N',')
      WITHIN GROUP (ORDER BY fkc.constraint_column_id),
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) + N'.'
      + OBJECT_NAME(fk.referenced_object_id),
    fk.is_disabled,
    fk.is_not_trusted
FROM target
JOIN sys.foreign_keys AS fk ON fk.parent_object_id = target.object_id
JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
GROUP BY
    fk.name,
    fk.referenced_object_id,
    fk.is_disabled,
    fk.is_not_trusted
ORDER BY 2, 1;
",
        schema,
        object,
        cancellation,
    )
    .await?;
    rows.iter()
        .map(|row| {
            Ok(SqlServerConstraint {
                name: text(row, 0, "constraint_name")?,
                kind: text(row, 1, "constraint_kind")?,
                definition: optional_text(row, 2, "constraint_definition")?,
                columns: optional_text(row, 3, "constraint_columns")?,
                referenced_object: optional_text(row, 4, "referenced_object")?,
                disabled: required(row, 5, "constraint_disabled")?,
                not_trusted: required(row, 6, "constraint_not_trusted")?,
            })
        })
        .collect()
}

async fn load_indexes(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<SqlServerIndex>> {
    let rows = execute_bound(
        session,
        r"
SELECT
    i.index_id,
    i.name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_disabled,
    i.has_filter,
    i.filter_definition,
    STRING_AGG(
        c.name + N':' + CONVERT(nvarchar(10), ic.key_ordinal)
          + N':' + CONVERT(nvarchar(1), ic.is_descending_key)
          + N':' + CONVERT(nvarchar(1), ic.is_included_column),
        N','
    ) WITHIN GROUP (ORDER BY
        CASE WHEN ic.key_ordinal = 0 THEN 2147483647 ELSE ic.key_ordinal END,
        ic.index_column_id)
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
JOIN sys.indexes AS i ON i.object_id = o.object_id
LEFT JOIN sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
LEFT JOIN sys.columns AS c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
WHERE s.name = @P1 AND o.name = @P2 AND i.is_hypothetical = 0
GROUP BY
    i.index_id,
    i.name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_disabled,
    i.has_filter,
    i.filter_definition
ORDER BY i.index_id;
",
        schema,
        object,
        cancellation,
    )
    .await?;
    rows.iter()
        .map(|row| {
            Ok(SqlServerIndex {
                index_id: required(row, 0, "index_id")?,
                name: optional_text(row, 1, "index_name")?,
                kind: text(row, 2, "index_kind")?,
                unique: required(row, 3, "index_unique")?,
                primary_key: required(row, 4, "index_primary_key")?,
                unique_constraint: required(row, 5, "index_unique_constraint")?,
                disabled: required(row, 6, "index_disabled")?,
                filtered: required(row, 7, "index_filtered")?,
                filter_definition: optional_text(row, 8, "index_filter_definition")?,
                columns: optional_text(row, 9, "index_columns")?,
            })
        })
        .collect()
}

async fn execute_bound(
    session: &mut SqlServerSession,
    sql: &'static str,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<tiberius::Row>> {
    let mut query = Query::new(sql);
    query.bind(schema.to_owned());
    query.bind(object.to_owned());
    one_result(
        session
            .execute_query(query, ErrorPhase::Probe, cancellation)
            .await?,
    )
}

fn hex_digest(value: &[u8]) -> Result<String> {
    let digest = Sha256::digest(value);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(output, "{byte:02x}")
            .map_err(|_| super::mapping_error("codifica token schema SQL Server fallita"))?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_fingerprint_is_stable_and_sensitive() {
        let first = hex_digest(b"schema-a").expect("digest");
        assert_eq!(first, hex_digest(b"schema-a").expect("same digest"));
        assert_ne!(first, hex_digest(b"schema-b").expect("different digest"));
        assert_eq!(first.len(), 64);
    }
}
