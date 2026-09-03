use crate::config::Db2Config;
use crate::connection::{exactly_one, query_text, required_text, with_connection};
use crate::error::{interruption_error, task_error};
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind};
use plenora_database_core::provider::{Inspection, SecretString};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

const LIST_CATALOGS_SQL: &str = "SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1";
const LIST_SCHEMAS_SQL: &str = "SELECT TRIM(SCHEMANAME) \
    FROM SYSCAT.SCHEMATA \
    WHERE SCHEMANAME NOT LIKE 'SYS%' \
      AND SCHEMANAME NOT IN ('NULLID', 'SQLJ') \
    ORDER BY SCHEMANAME";
const LIST_OBJECTS_SQL: &str = "SELECT TRIM(TABSCHEMA), TRIM(TABNAME), \
    CASE TYPE WHEN 'T' THEN 'TABLE' WHEN 'V' THEN 'VIEW' ELSE TRIM(TYPE) END \
    FROM SYSCAT.TABLES \
    WHERE TABSCHEMA = ? AND TYPE IN ('T', 'V') \
    ORDER BY TABNAME";
const DESCRIBE_RELATION_SQL: &str = "SELECT TRIM(TABSCHEMA), TRIM(TABNAME), \
    CASE TYPE WHEN 'T' THEN 'TABLE' WHEN 'V' THEN 'VIEW' ELSE TRIM(TYPE) END \
    FROM SYSCAT.TABLES \
    WHERE TABSCHEMA = ? AND TABNAME = ? AND TYPE IN ('T', 'V')";
const DESCRIBE_COLUMNS_SQL: &str = "SELECT TRIM(COLNAME), COLNO + 1, \
    TRIM(TYPENAME), LENGTH, SCALE, NULLS, DEFAULT, GENERATED, IDENTITY \
    FROM SYSCAT.COLUMNS \
    WHERE TABSCHEMA = ? AND TABNAME = ? \
    ORDER BY COLNO";
const DESCRIBE_INDEXES_SQL: &str = "SELECT TRIM(i.INDNAME), i.UNIQUERULE, \
    k.COLSEQ, TRIM(k.COLNAME), k.COLORDER \
    FROM SYSCAT.INDEXES AS i \
    JOIN SYSCAT.INDEXCOLUSE AS k \
      ON k.INDSCHEMA = i.INDSCHEMA AND k.INDNAME = i.INDNAME \
    WHERE i.TABSCHEMA = ? AND i.TABNAME = ? \
    ORDER BY i.INDNAME, k.COLSEQ";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Db2ObjectSummary {
    pub schema: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Db2Column {
    pub name: String,
    pub ordinal: u64,
    pub data_type: String,
    pub length: u64,
    pub scale: i64,
    pub nullable: bool,
    pub default_expression: Option<String>,
    pub generated: bool,
    pub identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Db2Index {
    pub name: String,
    pub unique: bool,
    pub primary: bool,
    pub columns: Vec<String>,
    pub descending: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Db2ObjectDescription {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub columns: Vec<Db2Column>,
    pub indexes: Vec<Db2Index>,
    pub schema_token: String,
}

pub async fn inspect(
    config: &Db2Config,
    secret: &SecretString,
    operation: &Operation,
    cancellation: &CancellationToken,
) -> Result<Inspection> {
    validate_operation(config, operation)?;
    if cancellation.is_cancelled() {
        return Err(interruption_error(cancellation, ErrorPhase::Probe));
    }
    let config = config.clone();
    let secret = secret.clone();
    let operation = operation.clone();
    let mut task =
        tokio::task::spawn_blocking(move || inspect_blocking(&config, &secret, &operation));
    tokio::select! {
        outcome = &mut task => outcome.map_err(|_| task_error(ErrorPhase::Probe))?,
        _ = cancellation.cancelled() => Err(interruption_error(cancellation, ErrorPhase::Probe)),
    }
}

fn inspect_blocking(
    config: &Db2Config,
    secret: &SecretString,
    operation: &Operation,
) -> Result<Inspection> {
    with_connection(config, secret, |connection, timeout| match operation {
        Operation::DatabaseListCatalogs => {
            let rows = query_text(connection, LIST_CATALOGS_SQL, &[], timeout, 1, 256)?;
            let catalogs = rows
                .iter()
                .map(|row| required_text(row, 0, "catalogo Db2 privo di nome"))
                .collect::<Result<Vec<_>>>()?;
            Ok(Inspection {
                operation: "database.list_catalogs".to_owned(),
                document: json!({"catalogs": catalogs}),
            })
        }
        Operation::DatabaseListSchemas { .. } => {
            let rows = query_text(connection, LIST_SCHEMAS_SQL, &[], timeout, 1, 256)?;
            let schemas = rows
                .iter()
                .map(|row| required_text(row, 0, "schema Db2 privo di nome"))
                .collect::<Result<Vec<_>>>()?;
            Ok(Inspection {
                operation: "database.list_schemas".to_owned(),
                document: json!({"schemas": schemas}),
            })
        }
        Operation::DatabaseListObjects { source } => {
            let schema = source_schema(config, source.as_ref());
            let rows = query_text(connection, LIST_OBJECTS_SQL, &[&schema], timeout, 3, 512)?;
            let objects = rows
                .iter()
                .map(|row| object_summary(row))
                .collect::<Result<Vec<_>>>()?;
            Ok(Inspection {
                operation: "database.list_objects".to_owned(),
                document: json!({"schema": schema, "objects": objects}),
            })
        }
        Operation::DatabaseDescribeObject { source } => {
            let schema = source_schema(config, Some(source));
            let description = describe_object(connection, timeout, &schema, &source.object)?;
            Ok(Inspection {
                operation: "database.describe_object".to_owned(),
                document: json!(description),
            })
        }
        _ => Err(DatabaseError::unsupported(
            ProviderKind::Db2,
            ErrorPhase::Probe,
            "operazione di introspezione Db2 non supportata",
        )),
    })
}

pub fn describe_object(
    connection: &odbc_api::Connection<'_>,
    timeout: usize,
    schema: &str,
    object: &str,
) -> Result<Db2ObjectDescription> {
    let relation = query_text(
        connection,
        DESCRIBE_RELATION_SQL,
        &[schema, object],
        timeout,
        3,
        512,
    )?;
    if relation.is_empty() {
        return Err(DatabaseError::new(
            ErrorCategory::NotFound,
            ErrorPhase::Probe,
            Some(ProviderKind::Db2),
            "oggetto Db2 richiesto non trovato",
        ));
    }
    let relation = exactly_one(relation, "catalogo Db2 con oggetto non univoco")?;
    let summary = object_summary(&relation)?;
    let column_rows = query_text(
        connection,
        DESCRIBE_COLUMNS_SQL,
        &[schema, object],
        timeout,
        9,
        16 * 1024,
    )?;
    let columns = column_rows
        .iter()
        .map(|row| column_description(row))
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Err(mapping_error("oggetto Db2 descritto senza colonne"));
    }
    let index_rows = query_text(
        connection,
        DESCRIBE_INDEXES_SQL,
        &[schema, object],
        timeout,
        5,
        512,
    )?;
    let indexes = build_indexes(&index_rows)?;
    let schema_token = schema_token(&summary, &columns, &indexes)?;
    Ok(Db2ObjectDescription {
        schema: summary.schema,
        name: summary.name,
        kind: summary.kind,
        columns,
        indexes,
        schema_token,
    })
}

fn object_summary(row: &[Option<String>]) -> Result<Db2ObjectSummary> {
    Ok(Db2ObjectSummary {
        schema: required_text(row, 0, "oggetto Db2 senza schema")?,
        name: required_text(row, 1, "oggetto Db2 senza nome")?,
        kind: required_text(row, 2, "oggetto Db2 senza tipo")?,
    })
}

fn column_description(row: &[Option<String>]) -> Result<Db2Column> {
    Ok(Db2Column {
        name: required_text(row, 0, "colonna Db2 senza nome")?,
        ordinal: parse_number(row, 1, "ordinale colonna Db2 non rappresentabile")?,
        data_type: required_text(row, 2, "colonna Db2 senza tipo")?,
        length: parse_number(row, 3, "lunghezza colonna Db2 non rappresentabile")?,
        scale: parse_number(row, 4, "scala colonna Db2 non rappresentabile")?,
        nullable: flag(row, 5, "NULLS")?,
        default_expression: row.get(6).and_then(Clone::clone),
        generated: flag(row, 7, "GENERATED")?,
        identity: flag(row, 8, "IDENTITY")?,
    })
}

pub fn build_indexes(rows: &[Vec<Option<String>>]) -> Result<Vec<Db2Index>> {
    let mut indexes = Vec::<Db2Index>::new();
    for row in rows {
        let name = required_text(row, 0, "indice Db2 senza nome")?;
        let rule = required_text(row, 1, "indice Db2 senza regola")?;
        let sequence: usize = parse_number(row, 2, "sequenza indice Db2 non rappresentabile")?;
        let column = required_text(row, 3, "indice Db2 senza colonna")?;
        let order = required_text(row, 4, "indice Db2 senza ordinamento")?;
        if !matches!(order.as_str(), "A" | "D") {
            return Err(mapping_error("ordinamento indice Db2 non riconosciuto"));
        }
        if indexes.last().is_none_or(|index| index.name != name) {
            indexes.push(Db2Index {
                name: name.clone(),
                unique: matches!(rule.as_str(), "P" | "U"),
                primary: rule == "P",
                columns: Vec::new(),
                descending: Vec::new(),
            });
        }
        let index = indexes.last_mut().expect("indice appena inserito");
        if sequence != index.columns.len() + 1 {
            return Err(mapping_error("sequenza colonne indice Db2 non contigua"));
        }
        index.columns.push(column);
        index.descending.push(order == "D");
    }
    Ok(indexes)
}

pub fn schema_token(
    summary: &Db2ObjectSummary,
    columns: &[Db2Column],
    indexes: &[Db2Index],
) -> Result<String> {
    let encoded =
        plenora_database_core::fingerprint::canonical_json_sha256(&(summary, columns, indexes))
            .map_err(|_| {
                DatabaseError::new(
                    ErrorCategory::Internal,
                    ErrorPhase::Probe,
                    Some(ProviderKind::Db2),
                    "token schema Db2 non serializzabile",
                )
            })?;
    Ok(format!("sha256:{encoded}"))
}

fn parse_number<T>(row: &[Option<String>], column: usize, message: &'static str) -> Result<T>
where
    T: std::str::FromStr,
{
    required_text(row, column, message)?
        .parse()
        .map_err(|_| mapping_error(message))
}

fn flag(row: &[Option<String>], column: usize, field: &'static str) -> Result<bool> {
    match row
        .get(column)
        .and_then(|value| value.as_deref())
        .map(str::trim)
    {
        Some("Y" | "A" | "D") => Ok(true),
        Some("N" | "") => Ok(false),
        _ => Err(mapping_error(field)),
    }
}

fn source_schema(config: &Db2Config, source: Option<&ObjectRef>) -> String {
    source
        .and_then(|source| source.schema.clone())
        .unwrap_or_else(|| config.username().to_ascii_uppercase())
}

fn validate_operation(config: &Db2Config, operation: &Operation) -> Result<()> {
    let source = match operation {
        Operation::DatabaseListSchemas { source } | Operation::DatabaseListObjects { source } => {
            source.as_ref()
        }
        Operation::DatabaseDescribeObject { source } => Some(source),
        _ => None,
    };
    if source
        .and_then(|source| source.catalog.as_deref())
        .is_some_and(|catalog| !catalog.eq_ignore_ascii_case(config.database()))
    {
        return Err(DatabaseError::unsupported(
            ProviderKind::Db2,
            ErrorPhase::Probe,
            "accesso cross-database Db2 non supportato",
        ));
    }
    Ok(())
}

fn mapping_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Probe,
        Some(ProviderKind::Db2),
        message,
    )
}
