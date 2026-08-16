use crate::MysqlSession;
use mysql_async::prelude::FromValue;
use mysql_async::{Params, Row, Value};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlProbe {
    pub product_version: String,
    pub database: String,
    pub version_comment: String,
    pub lower_case_table_names: u64,
    pub sql_mode: String,
    pub time_zone: String,
    pub transaction_isolation: String,
    pub tls_cipher: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlObjectSummary {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub engine: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlColumn {
    pub name: String,
    pub ordinal: u64,
    pub data_type: String,
    pub native_declaration: String,
    pub nullable: bool,
    pub default_expression: Option<String>,
    pub character_set: Option<String>,
    pub collation: Option<String>,
    pub numeric_precision: Option<u64>,
    pub numeric_scale: Option<u64>,
    pub datetime_precision: Option<u64>,
    pub spatial_srid: Option<u32>,
    pub extra: String,
    pub generation_expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MysqlSchemaToken(pub String);

/// Un indice (PRIMARY / UNIQUE / non-unique) osservato su una tabella.
///
/// Serve al preflight Upsert: `ON DUPLICATE KEY UPDATE` scatta su ogni
/// PK/unique index, non solo sulle `keys` dichiarate, quindi la policy
/// fail-closed deve poter confrontare le colonne di ciascun unique index
/// con le keys dell'operazione.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlIndex {
    pub name: String,
    /// `true` per PRIMARY e UNIQUE (`NON_UNIQUE` = 0).
    pub unique: bool,
    /// `true` se ogni parte dell'indice è una colonna semplice. `false` se
    /// almeno una parte è un'espressione (functional index, `MySQL` 8.0+):
    /// in quel caso `columns` non descrive l'indice per intero e non è
    /// confrontabile con le keys.
    pub column_backed: bool,
    /// Colonne dell'indice in ordine `SEQ_IN_INDEX`. Parziale/vuoto quando
    /// `column_backed` è `false`.
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlObjectDescription {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub engine: Option<String>,
    pub columns: Vec<MysqlColumn>,
    pub indexes: Vec<MysqlIndex>,
    pub token: MysqlSchemaToken,
}

/// Rileva versione, sessione deterministica e cifratura del server.
///
/// # Errors
///
/// Fallisce se il probe non e univoco, la sessione non e cifrata o il server
/// restituisce valori non rappresentabili.
pub async fn probe_server(
    session: &mut MysqlSession,
    cancellation: &CancellationToken,
) -> Result<MysqlProbe> {
    let mut rows = session
        .query_rows(
            "SELECT VERSION() AS product_version, DATABASE() AS database_name, \
             @@version_comment AS version_comment, \
             @@lower_case_table_names AS lower_case_table_names, \
             @@sql_mode AS sql_mode, @@time_zone AS time_zone, \
             @@transaction_isolation AS transaction_isolation",
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    if rows.len() != 1 {
        return Err(mapping_error("probe MySQL privo di una riga univoca"));
    }
    let row = rows.remove(0);
    let tls_rows = session
        .query_rows(
            "SHOW STATUS LIKE 'Ssl_cipher'",
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    let tls_cipher: String = tls_rows
        .first()
        .map(|value| required::<String, _>(value, 1, "Ssl_cipher"))
        .transpose()?
        .unwrap_or_default();
    if tls_cipher.is_empty() {
        return Err(DatabaseError {
            category: ErrorCategory::Authentication,
            phase: ErrorPhase::Probe,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
            execution_id: None,
            message: "connessione MySQL priva di cifratura TLS negoziata".to_owned(),
            diagnostics: None,
        });
    }
    let product_version: String = required(&row, "product_version", "product_version")?;
    let version_comment: String = required(&row, "version_comment", "version_comment")?;

    // Fix P1 review MySQL 2026-08-15: fail-closed su MariaDB.
    // MariaDB non è testato né qualificato: differenze rilevanti su
    // sequenze, INSERT ... ON DUPLICATE KEY, MERGE syntax, spatial
    // (GEOMETRYCOLLECTION), pool prepared statement cache, e
    // isolation semantics. Il consumer che dichiara MariaDB usa il
    // fork sbagliato — meglio errore chiaro alla probe che silenti
    // divergenze in produzione.
    let looks_like_mariadb = product_version.to_ascii_lowercase().contains("mariadb")
        || version_comment.to_ascii_lowercase().contains("mariadb");
    if looks_like_mariadb {
        return Err(DatabaseError {
            category: ErrorCategory::Unsupported,
            phase: ErrorPhase::Probe,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
            execution_id: None,
            message: format!(
                "MariaDB rilevato (product_version={product_version:?}, \
                 version_comment={version_comment:?}) — provider `mysql` non \
                 qualificato per MariaDB. Un provider dedicato è in roadmap."
            ),
            diagnostics: None,
        });
    }

    Ok(MysqlProbe {
        product_version,
        database: required(&row, "database_name", "database_name")?,
        version_comment,
        lower_case_table_names: required(&row, "lower_case_table_names", "lower_case_table_names")?,
        sql_mode: required(&row, "sql_mode", "sql_mode")?,
        time_zone: required(&row, "time_zone", "time_zone")?,
        transaction_isolation: required(&row, "transaction_isolation", "transaction_isolation")?,
        tls_cipher,
    })
}

/// Elenca gli schemi visibili alla connessione corrente.
///
/// # Errors
///
/// Propaga errori di protocollo, autorizzazione, timeout e cancellazione.
pub async fn list_schemas(
    session: &mut MysqlSession,
    cancellation: &CancellationToken,
) -> Result<Vec<String>> {
    // v0.2 (fix H7.1): esclude system schemas MySQL (information_schema, mysql,
    // performance_schema, sys). Il consumer che ha bisogno anche dei system
    // schemas deve interrogare information_schema.schemata direttamente.
    session
        .query_rows(
            "SELECT SCHEMA_NAME AS schema_name \
             FROM information_schema.schemata \
             WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY SCHEMA_NAME",
            ErrorPhase::Probe,
            cancellation,
        )
        .await?
        .iter()
        .map(|row| required(row, "schema_name", "schema_name"))
        .collect()
}

/// Elenca tabelle e viste di uno schema usando parametri preparati.
///
/// # Errors
///
/// Propaga errori di protocollo, autorizzazione, timeout e cancellazione.
pub async fn list_objects(
    session: &mut MysqlSession,
    schema: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<MysqlObjectSummary>> {
    session
        .exec_rows(
            "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, \
             TABLE_TYPE AS table_type, ENGINE AS engine \
             FROM information_schema.tables WHERE TABLE_SCHEMA = ? \
             ORDER BY TABLE_NAME",
            Params::Positional(vec![Value::from(schema)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(MysqlObjectSummary {
                schema: required(row, "table_schema", "table_schema")?,
                name: required(row, "table_name", "table_name")?,
                kind: required(row, "table_type", "table_type")?,
                engine: optional(row, "engine", "engine")?,
            })
        })
        .collect()
}

/// Descrive un oggetto e produce un token stabile del suo schema.
///
/// # Errors
///
/// Fallisce se l'oggetto non esiste, e ambiguo o contiene metadati non
/// rappresentabili.
pub async fn describe_object(
    session: &mut MysqlSession,
    schema: &str,
    name: &str,
    cancellation: &CancellationToken,
) -> Result<MysqlObjectDescription> {
    let objects = session
        .exec_rows(
            "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, \
             TABLE_TYPE AS table_type, ENGINE AS engine \
             FROM information_schema.tables \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            Params::Positional(vec![Value::from(schema), Value::from(name)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    if objects.len() != 1 {
        return Err(not_found("oggetto MySQL non trovato o ambiguo"));
    }
    let object = &objects[0];
    let column_rows = session
        .exec_rows(
            "SELECT COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position, \
             DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, \
             IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, \
             CHARACTER_SET_NAME AS character_set_name, COLLATION_NAME AS collation_name, \
             NUMERIC_PRECISION AS numeric_precision, NUMERIC_SCALE AS numeric_scale, \
             DATETIME_PRECISION AS datetime_precision, SRS_ID AS srs_id, \
             EXTRA AS extra, GENERATION_EXPRESSION AS generation_expression \
             FROM information_schema.columns \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            Params::Positional(vec![Value::from(schema), Value::from(name)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    if column_rows.is_empty() {
        return Err(mapping_error("oggetto MySQL senza colonne osservabili"));
    }
    let columns = column_rows
        .iter()
        .map(|row| {
            let nullable: String = required(row, "is_nullable", "is_nullable")?;
            Ok(MysqlColumn {
                name: required(row, "column_name", "column_name")?,
                ordinal: required(row, "ordinal_position", "ordinal_position")?,
                data_type: required(row, "data_type", "data_type")?,
                native_declaration: required(row, "column_type", "column_type")?,
                nullable: nullable == "YES",
                default_expression: optional(row, "column_default", "column_default")?,
                character_set: optional(row, "character_set_name", "character_set_name")?,
                collation: optional(row, "collation_name", "collation_name")?,
                numeric_precision: optional(row, "numeric_precision", "numeric_precision")?,
                numeric_scale: optional(row, "numeric_scale", "numeric_scale")?,
                datetime_precision: optional(row, "datetime_precision", "datetime_precision")?,
                spatial_srid: optional(row, "srs_id", "srs_id")?,
                extra: required(row, "extra", "extra")?,
                generation_expression: required(
                    row,
                    "generation_expression",
                    "generation_expression",
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Indici: PRIMARY/UNIQUE/non-unique con le loro colonne. Ordinati per
    // (INDEX_NAME, SEQ_IN_INDEX) così che le parti di ogni indice siano
    // contigue e in ordine. `EXPRESSION` (MySQL 8.0+) è non-NULL per le
    // parti funzionali: in quel caso COLUMN_NAME è NULL e l'indice non è
    // confrontabile per colonne.
    let index_rows = session
        .exec_rows(
            "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, \
             SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, \
             EXPRESSION AS expression \
             FROM information_schema.statistics \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            Params::Positional(vec![Value::from(schema), Value::from(name)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    let indexes = build_indexes(&index_rows)?;
    let schema_name: String = required(object, "table_schema", "table_schema")?;
    let object_name: String = required(object, "table_name", "table_name")?;
    let kind: String = required(object, "table_type", "table_type")?;
    let engine = optional(object, "engine", "engine")?;
    let token = schema_token(
        &schema_name,
        &object_name,
        &kind,
        engine.as_deref(),
        &columns,
        &indexes,
    )?;
    Ok(MysqlObjectDescription {
        schema: schema_name,
        name: object_name,
        kind,
        engine,
        columns,
        indexes,
        token,
    })
}

/// Aggrega le righe di `information_schema.statistics` (una per parte di
/// indice) in una lista di `MysqlIndex`. Le righe arrivano già ordinate per
/// `(INDEX_NAME, SEQ_IN_INDEX)`.
fn build_indexes(rows: &[Row]) -> Result<Vec<MysqlIndex>> {
    let mut indexes: Vec<MysqlIndex> = Vec::new();
    for row in rows {
        let name: String = required(row, "index_name", "index_name")?;
        let non_unique: i64 = required(row, "non_unique", "non_unique")?;
        let column: Option<String> = optional(row, "column_name", "column_name")?;
        let expression: Option<String> = optional(row, "expression", "expression")?;
        // Nuovo indice se il nome cambia rispetto all'ultimo accumulato.
        if indexes.last().map(|last| last.name.as_str()) != Some(name.as_str()) {
            indexes.push(MysqlIndex {
                name,
                unique: non_unique == 0,
                column_backed: true,
                columns: Vec::new(),
            });
        }
        let current = indexes
            .last_mut()
            .ok_or_else(|| mapping_error("aggregazione indici MySQL incoerente"))?;
        match column {
            Some(column_name) => current.columns.push(column_name),
            // Parte funzionale (EXPRESSION non-NULL, COLUMN_NAME NULL):
            // l'indice non è più confrontabile per colonne.
            None if expression.is_some() => current.column_backed = false,
            None => return Err(mapping_error("parte di indice MySQL senza colonna né espressione")),
        }
    }
    Ok(indexes)
}

fn schema_token(
    schema: &str,
    name: &str,
    kind: &str,
    engine: Option<&str>,
    columns: &[MysqlColumn],
    indexes: &[MysqlIndex],
) -> Result<MysqlSchemaToken> {
    // Indici inclusi nel token: una modifica agli indici (es. drop del PK,
    // aggiunta di un unique index) fra prepare ed esecuzione deve cambiare
    // il token e non passare inosservata al preflight Upsert.
    let bytes = serde_json::to_vec(&(schema, name, kind, engine, columns, indexes))
        .map_err(|_| mapping_error("serializzazione token schema MySQL fallita"))?;
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| mapping_error("codifica token schema MySQL fallita"))?;
    }
    Ok(MysqlSchemaToken(encoded))
}

fn required<T, I>(row: &Row, index: I, field: &str) -> Result<T>
where
    T: FromValue,
    I: mysql_async::prelude::ColumnIndex,
{
    match row.get_opt::<T, _>(index) {
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) => Err(mapping_error(format!(
            "campo catalogo MySQL {field} non convertibile"
        ))),
        None => Err(mapping_error(format!(
            "campo catalogo MySQL {field} assente"
        ))),
    }
}

fn optional<T, I>(row: &Row, index: I, field: &str) -> Result<Option<T>>
where
    T: FromValue,
    I: mysql_async::prelude::ColumnIndex,
{
    match row.get_opt::<Option<T>, _>(index) {
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) => Err(mapping_error(format!(
            "campo catalogo MySQL {field} non convertibile"
        ))),
        None => Err(mapping_error(format!(
            "campo catalogo MySQL {field} assente"
        ))),
    }
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Probe,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

fn not_found(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::NotFound,
        phase: ErrorPhase::Probe,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_token_is_stable_and_sensitive() {
        let column = MysqlColumn {
            name: "id".to_owned(),
            ordinal: 1,
            data_type: "int".to_owned(),
            native_declaration: "int".to_owned(),
            nullable: false,
            default_expression: None,
            character_set: None,
            collation: None,
            numeric_precision: Some(10),
            numeric_scale: Some(0),
            datetime_precision: None,
            spatial_srid: None,
            extra: String::new(),
            generation_expression: String::new(),
        };
        let pk = MysqlIndex {
            name: "PRIMARY".to_owned(),
            unique: true,
            column_backed: true,
            columns: vec!["id".to_owned()],
        };
        let first = schema_token(
            "data",
            "items",
            "BASE TABLE",
            Some("InnoDB"),
            std::slice::from_ref(&column),
            std::slice::from_ref(&pk),
        )
        .expect("token");
        let same = schema_token(
            "data",
            "items",
            "BASE TABLE",
            Some("InnoDB"),
            std::slice::from_ref(&column),
            std::slice::from_ref(&pk),
        )
        .expect("same token");
        let changed = schema_token(
            "data",
            "items",
            "BASE TABLE",
            Some("InnoDB"),
            &[MysqlColumn {
                nullable: true,
                ..column.clone()
            }],
            std::slice::from_ref(&pk),
        )
        .expect("changed token");
        // Una modifica agli indici (aggiunta di un unique index) cambia il token.
        let index_changed = schema_token(
            "data",
            "items",
            "BASE TABLE",
            Some("InnoDB"),
            std::slice::from_ref(&column),
            &[
                pk,
                MysqlIndex {
                    name: "uq_code".to_owned(),
                    unique: true,
                    column_backed: true,
                    columns: vec!["code".to_owned()],
                },
            ],
        )
        .expect("index changed token");
        assert_eq!(first, same);
        assert_ne!(first, changed);
        assert_ne!(first, index_changed);
    }
}
