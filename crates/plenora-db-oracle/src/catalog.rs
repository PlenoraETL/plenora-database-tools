use crate::config::OracleConfig;
use crate::connection::{connect, with_timeout};
use crate::decode::rows_from_result;
use crate::parameter::bind_parameters;
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind};
use plenora_database_core::provider::{Inspection, ParameterValue, SecretString};
use plenora_database_core::row::Row;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

const LIST_CATALOGS_SQL: &str =
    "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS CATALOG_NAME FROM DUAL";
const LIST_SCHEMAS_SQL: &str =
    "SELECT USERNAME FROM ALL_USERS WHERE ORACLE_MAINTAINED = 'N' ORDER BY USERNAME";
const LIST_OBJECTS_SQL: &str = "SELECT OWNER, OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS WHERE OWNER = :1 AND OBJECT_TYPE IN ('TABLE', 'VIEW') ORDER BY OBJECT_NAME";
const DESCRIBE_RELATION_SQL: &str = "SELECT OWNER, OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS WHERE OWNER = :1 AND OBJECT_NAME = :2 AND OBJECT_TYPE IN ('TABLE', 'VIEW')";
// `DATA_DEFAULT` è LONG e il driver thin non lo qualifica; non viene letto e
// resta esplicitamente `null` nel documento, invece di convertirlo in modo
// lossy o promettere una colonna `DATA_DEFAULT_VC` non presente su ogni major.
const DESCRIBE_COLUMNS_SQL: &str = "SELECT COLUMN_NAME, COLUMN_ID, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, CAST(NULL AS VARCHAR2(1)) AS DATA_DEFAULT, IDENTITY_COLUMN, CAST('NO' AS VARCHAR2(3)) AS VIRTUAL_COLUMN, CHAR_LENGTH FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID";
const DESCRIBE_INDEXES_SQL: &str = "SELECT i.INDEX_NAME, i.UNIQUENESS, CASE WHEN c.CONSTRAINT_TYPE = 'P' THEN 'Y' ELSE 'N' END, ic.COLUMN_POSITION, ic.COLUMN_NAME, ic.DESCEND, i.INDEX_TYPE, i.ITYP_OWNER, i.ITYP_NAME FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS ic ON ic.INDEX_OWNER = i.OWNER AND ic.INDEX_NAME = i.INDEX_NAME LEFT JOIN ALL_CONSTRAINTS c ON c.OWNER = i.OWNER AND c.INDEX_NAME = i.INDEX_NAME AND c.CONSTRAINT_TYPE = 'P' WHERE i.TABLE_OWNER = :1 AND i.TABLE_NAME = :2 AND i.INDEX_TYPE <> 'LOB' ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION";
const DESCRIBE_SPATIAL_SQL: &str = "SELECT m.COLUMN_NAME, m.SRID, (SELECT COUNT(*) FROM TABLE(m.DIMINFO)) FROM ALL_SDO_GEOM_METADATA m WHERE m.OWNER = :1 AND m.TABLE_NAME = :2 ORDER BY m.COLUMN_NAME";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleObjectSummary {
    pub schema: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleColumn {
    pub name: String,
    pub ordinal: u64,
    pub data_type: String,
    pub data_length: u64,
    pub char_length: u64,
    pub precision: Option<u64>,
    pub scale: Option<i64>,
    pub nullable: bool,
    pub default_expression: Option<String>,
    pub identity: bool,
    pub virtual_column: bool,
    #[serde(default)]
    pub spatial_srid: Option<u32>,
    #[serde(default)]
    pub spatial_dimensions: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleIndex {
    pub name: String,
    pub unique: bool,
    pub primary: bool,
    pub columns: Vec<String>,
    pub descending: Vec<bool>,
    #[serde(default)]
    pub spatial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleObjectDescription {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub columns: Vec<OracleColumn>,
    pub indexes: Vec<OracleIndex>,
    pub schema_token: String,
}

pub async fn inspect(
    config: &OracleConfig,
    secret: &SecretString,
    operation: &Operation,
    cancellation: &CancellationToken,
) -> Result<Inspection> {
    validate_operation(config, operation)?;
    let connection = connect(config, secret, cancellation).await?;
    match operation {
        Operation::DatabaseListCatalogs => {
            let rows = query(config, &connection, LIST_CATALOGS_SQL, &[], cancellation).await?;
            let catalogs = rows
                .iter()
                .map(|row| required_string(row, 0, "catalogo Oracle privo di nome"))
                .collect::<Result<Vec<_>>>()?;
            Ok(Inspection {
                operation: "database.list_catalogs".to_owned(),
                document: json!({"catalogs": catalogs}),
            })
        }
        Operation::DatabaseListSchemas { .. } => {
            let rows = query(config, &connection, LIST_SCHEMAS_SQL, &[], cancellation).await?;
            let schemas = rows
                .iter()
                .map(|row| required_string(row, 0, "schema Oracle privo di nome"))
                .collect::<Result<Vec<_>>>()?;
            Ok(Inspection {
                operation: "database.list_schemas".to_owned(),
                document: json!({"schemas": schemas}),
            })
        }
        Operation::DatabaseListObjects { source } => {
            let schema = source_schema(config, source.as_ref());
            let rows = query(
                config,
                &connection,
                LIST_OBJECTS_SQL,
                &[ParameterValue::String(schema.clone())],
                cancellation,
            )
            .await?;
            let objects = rows
                .iter()
                .map(object_summary)
                .collect::<Result<Vec<_>>>()?;
            Ok(Inspection {
                operation: "database.list_objects".to_owned(),
                document: json!({"schema": schema, "objects": objects}),
            })
        }
        Operation::DatabaseDescribeObject { source } => {
            let schema = source_schema(config, Some(source));
            let description =
                describe_object(config, &connection, &schema, &source.object, cancellation).await?;
            Ok(Inspection {
                operation: "database.describe_object".to_owned(),
                document: json!(description),
            })
        }
        _ => Err(DatabaseError::unsupported(
            ProviderKind::Oracle,
            ErrorPhase::Probe,
            "operazione di introspezione Oracle non supportata",
        )),
    }
}

pub async fn describe_object(
    config: &OracleConfig,
    connection: &oracle_rs::Connection,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<OracleObjectDescription> {
    let parameters = [
        ParameterValue::String(schema.to_owned()),
        ParameterValue::String(object.to_owned()),
    ];
    let relation = query(
        config,
        connection,
        DESCRIBE_RELATION_SQL,
        &parameters,
        cancellation,
    )
    .await
    .map_err(|error| catalog_context(error, "lettura relazione catalogo Oracle fallita"))?;
    let summary = match relation.as_slice() {
        [] => {
            return Err(DatabaseError::new(
                ErrorCategory::NotFound,
                ErrorPhase::Probe,
                Some(ProviderKind::Oracle),
                "oggetto Oracle richiesto non trovato",
            ))
        }
        [row] => object_summary(row)?,
        _ => return Err(mapping_error("catalogo Oracle con oggetto non univoco")),
    };
    let mut columns = query(
        config,
        connection,
        DESCRIBE_COLUMNS_SQL,
        &parameters,
        cancellation,
    )
    .await
    .map_err(|error| catalog_context(error, "lettura colonne catalogo Oracle fallita"))?
    .iter()
    .map(column_description)
    .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Err(mapping_error("oggetto Oracle descritto senza colonne"));
    }
    if columns
        .iter()
        .any(|column| column.data_type.eq_ignore_ascii_case("SDO_GEOMETRY"))
    {
        let rows = query(
            config,
            connection,
            DESCRIBE_SPATIAL_SQL,
            &parameters,
            cancellation,
        )
        .await
        .map_err(|error| catalog_context(error, "lettura metadati spatial Oracle fallita"))?;
        apply_spatial_metadata(&mut columns, &rows)?;
    }
    let index_rows = query(
        config,
        connection,
        DESCRIBE_INDEXES_SQL,
        &parameters,
        cancellation,
    )
    .await
    .map_err(|error| catalog_context(error, "lettura indici catalogo Oracle fallita"))?;
    let indexes = build_indexes(&index_rows)?;
    let digest =
        plenora_database_core::fingerprint::canonical_json_sha256(&(&summary, &columns, &indexes))
            .map_err(|_| mapping_error("token schema Oracle non serializzabile"))?;
    Ok(OracleObjectDescription {
        schema: summary.schema,
        name: summary.name,
        kind: summary.kind,
        columns,
        indexes,
        schema_token: format!("sha256:{digest}"),
    })
}

pub async fn probe_spatial(
    config: &OracleConfig,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Result<bool> {
    let connection = connect(config, secret, cancellation).await?;
    let rows = query(
        config,
        &connection,
        "SELECT COUNT(*) FROM ALL_TYPES WHERE OWNER = 'MDSYS' AND TYPE_NAME = 'SDO_GEOMETRY'",
        &[],
        cancellation,
    )
    .await?;
    let [row] = rows.as_slice() else {
        return Err(mapping_error(
            "sonda Spatial Oracle con cardinalita inattesa",
        ));
    };
    Ok(required_u64(row, 0, "esito sonda Spatial Oracle non rappresentabile")? == 1)
}

async fn query(
    config: &OracleConfig,
    connection: &oracle_rs::Connection,
    sql: &str,
    parameters: &[ParameterValue],
    cancellation: &CancellationToken,
) -> Result<Vec<Row>> {
    let parameters = bind_parameters(parameters)?;
    let mut result = with_timeout(
        config,
        ErrorPhase::Probe,
        cancellation,
        connection.query(sql, &parameters),
    )
    .await?;
    while result.has_more_rows {
        let page = with_timeout(
            config,
            ErrorPhase::Probe,
            cancellation,
            connection.fetch_more(result.cursor_id, &result.columns, 256),
        )
        .await?;
        result.rows.extend(page.rows);
        result.cursor_id = page.cursor_id;
        result.has_more_rows = page.has_more_rows;
    }
    rows_from_result(
        connection,
        config.operation_timeout(),
        ErrorPhase::Probe,
        cancellation,
        result,
    )
    .await
}

fn object_summary(row: &Row) -> Result<OracleObjectSummary> {
    Ok(OracleObjectSummary {
        schema: required_string(row, 0, "oggetto Oracle senza schema")?,
        name: required_string(row, 1, "oggetto Oracle senza nome")?,
        kind: required_string(row, 2, "oggetto Oracle senza tipo")?,
    })
}

fn column_description(row: &Row) -> Result<OracleColumn> {
    Ok(OracleColumn {
        name: required_string(row, 0, "colonna Oracle senza nome")?,
        ordinal: required_u64(row, 1, "ordinale colonna Oracle non rappresentabile")?,
        data_type: required_string(row, 2, "colonna Oracle senza tipo")?,
        data_length: required_u64(row, 3, "lunghezza colonna Oracle non rappresentabile")?,
        precision: optional_u64(row, 4, "precisione colonna Oracle non rappresentabile")?,
        scale: optional_i64(row, 5, "scala colonna Oracle non rappresentabile")?,
        nullable: required_string(row, 6, "nullable colonna Oracle non riconosciuto")? == "Y",
        default_expression: optional_string(row, 7)?,
        identity: required_string(row, 8, "identity colonna Oracle non riconosciuta")? == "YES",
        virtual_column: required_string(row, 9, "virtual column Oracle non riconosciuta")? == "YES",
        char_length: required_u64(row, 10, "char length Oracle non rappresentabile")?,
        spatial_srid: None,
        spatial_dimensions: None,
    })
}

fn apply_spatial_metadata(columns: &mut [OracleColumn], rows: &[Row]) -> Result<()> {
    for row in rows {
        let name = required_string(row, 0, "metadato spatial Oracle senza colonna")?;
        let column = columns
            .iter_mut()
            .find(|column| column.name == name)
            .ok_or_else(|| mapping_error("metadato spatial Oracle su colonna non trovata"))?;
        let srid = required_u64(row, 1, "SRID spatial Oracle assente o non rappresentabile")?;
        column.spatial_srid = Some(
            u32::try_from(srid)
                .map_err(|_| mapping_error("SRID spatial Oracle oltre il contratto u32"))?,
        );
        let dimensions = required_u64(
            row,
            2,
            "dimensionalita metadato spatial Oracle non rappresentabile",
        )?;
        column.spatial_dimensions = Some(
            u8::try_from(dimensions)
                .map_err(|_| mapping_error("dimensionalita spatial Oracle oltre u8"))?,
        );
    }
    for column in columns
        .iter()
        .filter(|column| column.data_type.eq_ignore_ascii_case("SDO_GEOMETRY"))
    {
        if column.spatial_srid.is_none() || column.spatial_dimensions.is_none() {
            return Err(DatabaseError::new(
                ErrorCategory::Crs,
                ErrorPhase::Probe,
                Some(ProviderKind::Oracle),
                "colonna SDO_GEOMETRY Oracle priva di metadati CRS riproducibili",
            ));
        }
    }
    Ok(())
}

fn build_indexes(rows: &[Row]) -> Result<Vec<OracleIndex>> {
    let mut indexes = Vec::<OracleIndex>::new();
    for row in rows {
        let name = required_string(row, 0, "indice Oracle senza nome")?;
        let unique = required_string(row, 1, "indice Oracle senza unicità")? == "UNIQUE";
        let primary = required_string(row, 2, "indice Oracle senza flag primary")? == "Y";
        let position = required_u64(row, 3, "posizione indice Oracle non rappresentabile")?;
        let column = required_string(row, 4, "indice Oracle senza colonna")?;
        let descending = required_string(row, 5, "indice Oracle senza ordinamento")? == "DESC";
        let index_type = required_string(row, 6, "indice Oracle senza tipo")?;
        let type_owner = optional_string(row, 7)?;
        let type_name = optional_string(row, 8)?;
        let spatial = index_type == "DOMAIN"
            && type_owner.as_deref() == Some("MDSYS")
            && matches!(
                type_name.as_deref(),
                Some("SPATIAL_INDEX" | "SPATIAL_INDEX_V2")
            );
        if indexes.last().is_none_or(|index| index.name != name) {
            indexes.push(OracleIndex {
                name: name.clone(),
                unique,
                primary,
                columns: Vec::new(),
                descending: Vec::new(),
                spatial,
            });
        }
        let index = indexes.last_mut().expect("indice appena inserito");
        if index.spatial != spatial {
            return Err(mapping_error(
                "indice Oracle con tipo incoerente fra colonne",
            ));
        }
        if position != u64::try_from(index.columns.len()).unwrap_or(u64::MAX) + 1 {
            return Err(mapping_error("sequenza colonne indice Oracle non contigua"));
        }
        index.columns.push(column);
        index.descending.push(descending);
    }
    Ok(indexes)
}

fn required_string(row: &Row, index: usize, message: &'static str) -> Result<String> {
    match row.get_index(index) {
        Some(ParameterValue::String(value)) => Ok(value.trim().to_owned()),
        _ => Err(mapping_error(message)),
    }
}

fn optional_string(row: &Row, index: usize) -> Result<Option<String>> {
    match row.get_index(index) {
        Some(ParameterValue::String(value)) => Ok(Some(value.trim().to_owned())),
        Some(ParameterValue::Null { .. }) => Ok(None),
        _ => Err(mapping_error(
            "campo testuale catalogo Oracle non rappresentabile",
        )),
    }
}

fn required_u64(row: &Row, index: usize, message: &'static str) -> Result<u64> {
    optional_u64(row, index, message)?.ok_or_else(|| mapping_error(message))
}

fn optional_u64(row: &Row, index: usize, message: &'static str) -> Result<Option<u64>> {
    match row.get_index(index) {
        Some(ParameterValue::I64(value)) => u64::try_from(*value)
            .map(Some)
            .map_err(|_| mapping_error(message)),
        Some(ParameterValue::Decimal(value)) => {
            value.parse().map(Some).map_err(|_| mapping_error(message))
        }
        Some(ParameterValue::String(value)) => value
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| mapping_error(message)),
        Some(ParameterValue::Null { .. }) => Ok(None),
        _ => Err(mapping_error(message)),
    }
}

fn optional_i64(row: &Row, index: usize, message: &'static str) -> Result<Option<i64>> {
    match row.get_index(index) {
        Some(ParameterValue::I64(value)) => Ok(Some(*value)),
        Some(ParameterValue::Decimal(value)) => {
            value.parse().map(Some).map_err(|_| mapping_error(message))
        }
        Some(ParameterValue::String(value)) => value
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| mapping_error(message)),
        Some(ParameterValue::Null { .. }) => Ok(None),
        _ => Err(mapping_error(message)),
    }
}

fn source_schema(config: &OracleConfig, source: Option<&ObjectRef>) -> String {
    source
        .and_then(|source| source.schema.clone())
        .unwrap_or_else(|| config.username().to_ascii_uppercase())
}

fn validate_operation(config: &OracleConfig, operation: &Operation) -> Result<()> {
    let source = match operation {
        Operation::DatabaseListSchemas { source } | Operation::DatabaseListObjects { source } => {
            source.as_ref()
        }
        Operation::DatabaseDescribeObject { source } => Some(source),
        _ => None,
    };
    if source
        .and_then(|source| source.catalog.as_deref())
        .is_some_and(|catalog| !catalog.eq_ignore_ascii_case(config.service_name()))
    {
        return Err(DatabaseError::unsupported(
            ProviderKind::Oracle,
            ErrorPhase::Probe,
            "accesso cross-database Oracle non supportato",
        ));
    }
    Ok(())
}

fn mapping_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Probe,
        Some(ProviderKind::Oracle),
        message,
    )
}

fn catalog_context(mut error: DatabaseError, message: &'static str) -> DatabaseError {
    message.clone_into(&mut error.message);
    error
}
