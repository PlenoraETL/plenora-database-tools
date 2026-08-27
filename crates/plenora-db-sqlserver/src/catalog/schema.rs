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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlServerSpatialBoundingBox {
    pub xmin: f64,
    pub ymin: f64,
    pub xmax: f64,
    pub ymax: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlServerSpatialIndex {
    pub spatial_type: String,
    pub tessellation_scheme: String,
    pub bounding_box: Option<SqlServerSpatialBoundingBox>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    pub spatial: Option<SqlServerSpatialIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerSchemaToken {
    pub schema_version: u32,
    pub database_id: i32,
    pub object_id: i32,
    pub structural_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerObjectName {
    pub object_id: i32,
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerTemporalMetadata {
    pub kind: String,
    pub history: Option<SqlServerObjectName>,
    pub period_start_column: Option<String>,
    pub period_end_column: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlServerGraphKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerExternalTableMetadata {
    pub data_source: String,
    pub file_format: Option<String>,
    pub location: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerPartitioning {
    pub data_space_id: i32,
    pub scheme: String,
    pub function: String,
    pub partition_column: String,
    pub boundary_value_on_right: bool,
    pub partition_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerSecurityPredicate {
    pub policy_schema: String,
    pub policy_name: String,
    pub policy_enabled: bool,
    pub policy_schema_bound: bool,
    pub predicate_definition: String,
    pub kind: String,
    pub operation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerPermission {
    pub grantee: String,
    pub grantor: String,
    pub permission: String,
    pub state: String,
    pub column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerViewMetadata {
    pub definition: Option<String>,
    pub schema_bound: bool,
    pub uses_ansi_nulls: bool,
    pub uses_quoted_identifier: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlServerObjectDescription {
    pub database_id: i32,
    pub object_id: i32,
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub temporal_type: u8,
    pub temporal: Option<SqlServerTemporalMetadata>,
    pub graph_kind: Option<SqlServerGraphKind>,
    pub external: Option<SqlServerExternalTableMetadata>,
    pub partitioning: Option<SqlServerPartitioning>,
    pub owner: String,
    pub security_predicates: Vec<SqlServerSecurityPredicate>,
    pub permissions: Vec<SqlServerPermission>,
    pub view: Option<SqlServerViewMetadata>,
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
    let partitioning = load_partitioning(session, schema, object, cancellation).await?;
    let security_predicates =
        load_security_predicates(session, schema, object, cancellation).await?;
    let permissions = load_permissions(session, schema, object, cancellation).await?;
    let columns = load_columns(session, schema, object, cancellation).await?;
    let constraints = load_constraints(session, schema, object, cancellation).await?;
    let indexes = load_indexes(session, schema, object, cancellation).await?;
    let encoded = serde_json::to_vec(&(
        &identity,
        &partitioning,
        &security_predicates,
        &permissions,
        &columns,
        &constraints,
        &indexes,
    ))
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
        temporal: identity.temporal,
        graph_kind: identity.graph_kind,
        external: identity.external,
        partitioning,
        owner: identity.owner,
        security_predicates,
        permissions,
        view: identity.view,
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
    temporal: Option<SqlServerTemporalMetadata>,
    graph_kind: Option<SqlServerGraphKind>,
    external: Option<SqlServerExternalTableMetadata>,
    owner: String,
    view: Option<SqlServerViewMetadata>,
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
    t.durability_desc,
    t.temporal_type_desc,
    t.history_table_id,
    OBJECT_SCHEMA_NAME(t.history_table_id),
    OBJECT_NAME(t.history_table_id),
    period_start.name,
    period_end.name,
    CAST(COALESCE(t.is_node, 0) AS bit),
    CAST(COALESCE(t.is_edge, 0) AS bit),
    CAST(CASE WHEN et.object_id IS NULL THEN 0 ELSE 1 END AS bit),
    eds.name,
    eff.name,
    et.location,
    COALESCE(USER_NAME(o.principal_id), USER_NAME(s.principal_id)),
    module.definition,
    module.is_schema_bound,
    module.uses_ansi_nulls,
    module.uses_quoted_identifier
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
LEFT JOIN sys.tables AS t ON t.object_id = o.object_id
LEFT JOIN sys.periods AS p ON p.object_id = o.object_id
LEFT JOIN sys.columns AS period_start
  ON period_start.object_id = p.object_id
 AND period_start.column_id = p.start_column_id
LEFT JOIN sys.columns AS period_end
  ON period_end.object_id = p.object_id
 AND period_end.column_id = p.end_column_id
LEFT JOIN sys.external_tables AS et ON et.object_id = o.object_id
LEFT JOIN sys.external_data_sources AS eds
  ON eds.data_source_id = et.data_source_id
LEFT JOIN sys.external_file_formats AS eff
  ON eff.file_format_id = et.file_format_id
LEFT JOIN sys.sql_modules AS module ON module.object_id = o.object_id
WHERE s.name = @P1
  AND o.name = @P2
  AND o.type IN ('U', 'V', 'ET')
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
    let kind = text(row, 5, "kind")?;
    let temporal_type = required(row, 6, "temporal_type")?;
    let temporal = temporal_metadata(row, temporal_type)?;
    let graph_kind = graph_kind(required(row, 15, "is_node")?, required(row, 16, "is_edge")?)?;
    let external = external_metadata(row)?;
    let view = view_metadata(row, &kind)?;
    Ok(ObjectIdentity {
        database_id: required(row, 0, "database_id")?,
        object_id: required(row, 1, "object_id")?,
        catalog: text(row, 2, "catalog")?,
        schema: text(row, 3, "schema")?,
        name: text(row, 4, "name")?,
        kind,
        temporal_type,
        temporal,
        graph_kind,
        external,
        owner: text(row, 21, "owner")?,
        view,
        memory_optimized: required(row, 7, "memory_optimized")?,
        durability: optional_text(row, 8, "durability")?,
    })
}

fn temporal_metadata(
    row: &tiberius::Row,
    temporal_type: u8,
) -> Result<Option<SqlServerTemporalMetadata>> {
    let kind = optional_text(row, 9, "temporal_type_desc")?;
    let history_id = super::optional::<i32>(row, 10, "history_table_id")?;
    let history_schema = optional_text(row, 11, "history_schema")?;
    let history_name = optional_text(row, 12, "history_table")?;
    let period_start_column = optional_text(row, 13, "period_start_column")?;
    let period_end_column = optional_text(row, 14, "period_end_column")?;
    if temporal_type == 0 {
        return Ok(None);
    }
    let kind = kind.ok_or_else(|| {
        super::mapping_error("tipo temporal SQL Server privo della descrizione di catalogo")
    })?;
    let history = match (history_id, history_schema, history_name) {
        (None, None, None) => None,
        (Some(object_id), Some(schema), Some(name)) => Some(SqlServerObjectName {
            object_id,
            schema,
            name,
        }),
        _ => {
            return Err(super::mapping_error(
                "riferimento history table SQL Server incompleto",
            ))
        }
    };
    if period_start_column.is_some() != period_end_column.is_some() {
        return Err(super::mapping_error(
            "periodo temporal SQL Server con estremi incompleti",
        ));
    }
    if temporal_type == 2 && (history.is_none() || period_start_column.is_none()) {
        return Err(super::mapping_error(
            "system-versioned temporal table SQL Server priva di history o periodo",
        ));
    }
    Ok(Some(SqlServerTemporalMetadata {
        kind,
        history,
        period_start_column,
        period_end_column,
    }))
}

fn graph_kind(is_node: bool, is_edge: bool) -> Result<Option<SqlServerGraphKind>> {
    match (is_node, is_edge) {
        (false, false) => Ok(None),
        (true, false) => Ok(Some(SqlServerGraphKind::Node)),
        (false, true) => Ok(Some(SqlServerGraphKind::Edge)),
        (true, true) => Err(super::mapping_error(
            "tabella graph SQL Server dichiarata sia node sia edge",
        )),
    }
}

fn external_metadata(row: &tiberius::Row) -> Result<Option<SqlServerExternalTableMetadata>> {
    if !required(row, 17, "is_external")? {
        return Ok(None);
    }
    Ok(Some(SqlServerExternalTableMetadata {
        data_source: text(row, 18, "external_data_source")?,
        file_format: optional_text(row, 19, "external_file_format")?,
        location: text(row, 20, "external_location")?,
    }))
}

fn view_metadata(row: &tiberius::Row, kind: &str) -> Result<Option<SqlServerViewMetadata>> {
    if kind != "VIEW" {
        return Ok(None);
    }
    Ok(Some(SqlServerViewMetadata {
        definition: optional_text(row, 22, "view_definition")?,
        schema_bound: required(row, 23, "view_schema_bound")?,
        uses_ansi_nulls: required(row, 24, "view_uses_ansi_nulls")?,
        uses_quoted_identifier: required(row, 25, "view_uses_quoted_identifier")?,
    }))
}

async fn load_partitioning(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Option<SqlServerPartitioning>> {
    let rows = execute_bound(
        session,
        r"
SELECT
    i.data_space_id,
    ps.name,
    pf.name,
    pf.boundary_value_on_right,
    c.name,
    CAST((
        SELECT COUNT_BIG(*)
        FROM sys.partitions AS p
        WHERE p.object_id = i.object_id AND p.index_id = i.index_id
    ) AS int)
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
JOIN sys.indexes AS i ON i.object_id = o.object_id AND i.index_id IN (0, 1)
JOIN sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
JOIN sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
 AND ic.partition_ordinal = 1
JOIN sys.columns AS c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
WHERE s.name = @P1 AND o.name = @P2;
",
        schema,
        object,
        cancellation,
    )
    .await?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => {
            let partition_count = required(row, 5, "partition_count")?;
            if partition_count < 1 {
                return Err(super::mapping_error(
                    "schema di partizionamento SQL Server privo di partizioni",
                ));
            }
            Ok(Some(SqlServerPartitioning {
                data_space_id: required(row, 0, "partition_data_space_id")?,
                scheme: text(row, 1, "partition_scheme")?,
                function: text(row, 2, "partition_function")?,
                boundary_value_on_right: required(row, 3, "boundary_value_on_right")?,
                partition_column: text(row, 4, "partition_column")?,
                partition_count,
            }))
        }
        _ => Err(super::mapping_error(
            "oggetto SQL Server associato a piu strategie di partizionamento",
        )),
    }
}

async fn load_security_predicates(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<SqlServerSecurityPredicate>> {
    let rows = execute_bound(
        session,
        r"
SELECT
    policy_schema.name,
    policy.name,
    policy.is_enabled,
    policy.is_schema_bound,
    predicate.predicate_definition,
    predicate.predicate_type_desc,
    predicate.operation_desc
FROM sys.objects AS target
JOIN sys.schemas AS target_schema ON target_schema.schema_id = target.schema_id
JOIN sys.security_predicates AS predicate ON predicate.target_object_id = target.object_id
JOIN sys.security_policies AS policy ON policy.object_id = predicate.object_id
JOIN sys.schemas AS policy_schema ON policy_schema.schema_id = policy.schema_id
WHERE target_schema.name = @P1 AND target.name = @P2
ORDER BY policy_schema.name, policy.name, predicate.security_predicate_id;
",
        schema,
        object,
        cancellation,
    )
    .await?;
    rows.iter()
        .map(|row| {
            Ok(SqlServerSecurityPredicate {
                policy_schema: text(row, 0, "security_policy_schema")?,
                policy_name: text(row, 1, "security_policy_name")?,
                policy_enabled: required(row, 2, "security_policy_enabled")?,
                policy_schema_bound: required(row, 3, "security_policy_schema_bound")?,
                predicate_definition: text(row, 4, "security_predicate_definition")?,
                kind: text(row, 5, "security_predicate_kind")?,
                operation: optional_text(row, 6, "security_predicate_operation")?,
            })
        })
        .collect()
}

async fn load_permissions(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<SqlServerPermission>> {
    let rows = execute_bound(
        session,
        r"
SELECT
    USER_NAME(permission.grantee_principal_id),
    USER_NAME(permission.grantor_principal_id),
    permission.permission_name,
    permission.state_desc,
    column_metadata.name
FROM sys.objects AS target
JOIN sys.schemas AS target_schema ON target_schema.schema_id = target.schema_id
JOIN sys.database_permissions AS permission
  ON permission.class = 1
 AND permission.major_id = target.object_id
LEFT JOIN sys.columns AS column_metadata
  ON column_metadata.object_id = target.object_id
 AND column_metadata.column_id = permission.minor_id
WHERE target_schema.name = @P1 AND target.name = @P2
ORDER BY
    USER_NAME(permission.grantee_principal_id),
    permission.permission_name,
    permission.state_desc,
    permission.minor_id;
",
        schema,
        object,
        cancellation,
    )
    .await?;
    rows.iter()
        .map(|row| {
            Ok(SqlServerPermission {
                grantee: text(row, 0, "permission_grantee")?,
                grantor: text(row, 1, "permission_grantor")?,
                permission: text(row, 2, "permission_name")?,
                state: text(row, 3, "permission_state")?,
                column: optional_text(row, 4, "permission_column")?,
            })
        })
        .collect()
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
        ic.index_column_id),
    si.spatial_index_type_desc,
    si.tessellation_scheme,
    sit.bounding_box_xmin,
    sit.bounding_box_ymin,
    sit.bounding_box_xmax,
    sit.bounding_box_ymax
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
JOIN sys.indexes AS i ON i.object_id = o.object_id
LEFT JOIN sys.spatial_indexes AS si
  ON si.object_id = i.object_id
 AND si.index_id = i.index_id
LEFT JOIN sys.spatial_index_tessellations AS sit
  ON sit.object_id = i.object_id
 AND sit.index_id = i.index_id
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
    i.filter_definition,
    si.spatial_index_type_desc,
    si.tessellation_scheme,
    sit.bounding_box_xmin,
    sit.bounding_box_ymin,
    sit.bounding_box_xmax,
    sit.bounding_box_ymax
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
                spatial: spatial_index(row)?,
            })
        })
        .collect()
}

fn spatial_index(row: &tiberius::Row) -> Result<Option<SqlServerSpatialIndex>> {
    let spatial_type = optional_text(row, 10, "spatial_index_type")?;
    let tessellation_scheme = optional_text(row, 11, "spatial_tessellation_scheme")?;
    let bounds = [
        super::optional::<f64>(row, 12, "spatial_bounding_box_xmin")?,
        super::optional::<f64>(row, 13, "spatial_bounding_box_ymin")?,
        super::optional::<f64>(row, 14, "spatial_bounding_box_xmax")?,
        super::optional::<f64>(row, 15, "spatial_bounding_box_ymax")?,
    ];
    match (spatial_type, tessellation_scheme, bounds) {
        (None, None, [None, None, None, None]) => Ok(None),
        (Some(spatial_type), Some(tessellation_scheme), [None, None, None, None]) => {
            Ok(Some(SqlServerSpatialIndex {
                spatial_type,
                tessellation_scheme,
                bounding_box: None,
            }))
        }
        (
            Some(spatial_type),
            Some(tessellation_scheme),
            [Some(xmin), Some(ymin), Some(xmax), Some(ymax)],
        ) if [xmin, ymin, xmax, ymax]
            .iter()
            .all(|value| value.is_finite())
            && xmin < xmax
            && ymin < ymax =>
        {
            Ok(Some(SqlServerSpatialIndex {
                spatial_type,
                tessellation_scheme,
                bounding_box: Some(SqlServerSpatialBoundingBox {
                    xmin,
                    ymin,
                    xmax,
                    ymax,
                }),
            }))
        }
        _ => Err(super::mapping_error(
            "metadati indice spatial SQL Server incoerenti",
        )),
    }
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
#[path = "schema_tests.rs"]
mod tests;
