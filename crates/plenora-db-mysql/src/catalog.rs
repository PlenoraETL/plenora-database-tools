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

/// Interruttore **di solo test** sul rifiuto iniziale di `MariaDB`.
///
/// Il provider `MySQL` non e qualificato per `MariaDB`, e lo dichiara alla
/// probe.
/// Quel rifiuto e cio che ADR 0014 chiede di misurare *attraverso*: senza
/// attraversarlo non si puo sapere quali superfici del provider divergano
/// davvero, e la lista delle divergenze resterebbe quella di una review —
/// che, misurata, e risultata sbagliata su tre punti su cinque.
///
/// Tre proprieta lo rendono un bypass e non un supporto:
///
/// * e `#[cfg(test)]`, quindi non esiste nel binario pubblico. Non e una
///   feature, non e una variabile d'ambiente, non e un parametro: non c'e
///   modo di attivarlo da fuori il crate;
/// * salta **solo** il rifiuto. Non tocca SQL, mapping, timeout, transazioni
///   ne classificazione degli errori: cio che succede dopo e esattamente cio
///   che il provider fa oggi, ed e il punto della misura;
/// * fuori dai test la funzione e `false` costante, quindi la condizione
///   resta quella di prima e il codice generato non cambia.
#[cfg(test)]
static MARIADB_REJECTION_BYPASS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Attiva il bypass per il resto del processo di test.
///
/// Globale e non per-thread perche il provider gira su un runtime tokio
/// multi-thread: un interruttore legato al thread del test non sarebbe
/// visibile dentro il task che apre la connessione.
/// Attiva il bypass finche la guardia resta viva, e lo spegne al `Drop`.
///
/// Un interruttore che si accende e basta lascerebbe il rifiuto disattivato
/// per il resto del processo di test: qualunque altro test eseguito dopo, in
/// quel binario, vedrebbe un provider che accetta `MariaDB` senza che nessuno
/// lo abbia chiesto. Lo scope lo rende un fatto locale alla misura.
// `pub(crate)` in un modulo privato e cio che serve: la misura vive in un
// modulo fratello e non deve poterlo chiamare nessun altro.
#[allow(clippy::redundant_pub_crate)]
#[cfg(test)]
pub(crate) struct MariadbRejectionBypass;

#[allow(clippy::redundant_pub_crate)]
#[cfg(test)]
impl MariadbRejectionBypass {
    pub(crate) fn engage() -> Self {
        MARIADB_REJECTION_BYPASS.store(true, std::sync::atomic::Ordering::SeqCst);
        Self
    }
}

#[cfg(test)]
impl Drop for MariadbRejectionBypass {
    fn drop(&mut self) {
        MARIADB_REJECTION_BYPASS.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
fn mariadb_rejection_bypassed() -> bool {
    MARIADB_REJECTION_BYPASS.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(not(test))]
const fn mariadb_rejection_bypassed() -> bool {
    false
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
    probe_server_with_profile(session, &crate::profile::MYSQL_PROFILE, cancellation).await
}

/// La probe, con il profilo che decide quale prodotto e accettabile.
///
/// La forma pubblica sopra resta legata a un solo prodotto perche e API: chi
/// la chiama sta chiedendo la probe `MySQL`. Il provider passa invece il
/// proprio profilo, ed e da qui che un secondo prodotto entrera senza
/// toccare la firma esportata.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn probe_server_with_profile(
    session: &mut MysqlSession,
    profile: &dyn crate::profile::ProductProfile,
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

    // Il riconoscimento del prodotto, e la ragione per cui rifiutarlo,
    // stanno nel profilo. Qui resta il punto in cui il rifiuto scatta, e con
    // esso il bypass di test, che non si sposta: e questo il punto che
    // attraversa, ed e accanto a questo che va letto.
    if !mariadb_rejection_bypassed() {
        if let Some(rejection) =
            profile.foreign_product_rejection(&product_version, &version_comment)
        {
            return Err(rejection);
        }
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
    list_schemas_with_profile(session, &crate::profile::MYSQL_PROFILE, cancellation).await
}

/// Gli schemi, con il profilo che decide come interrogarli.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn list_schemas_with_profile(
    session: &mut MysqlSession,
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
) -> Result<Vec<String>> {
    // v0.2 (fix H7.1): esclude system schemas MySQL (information_schema, mysql,
    // performance_schema, sys). Il consumer che ha bisogno anche dei system
    // schemas deve interrogare information_schema.schemata direttamente.
    session
        .query_rows(profile.schemas_query(), ErrorPhase::Probe, cancellation)
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
    list_objects_with_profile(
        session,
        schema,
        &crate::profile::MYSQL_PROFILE,
        cancellation,
    )
    .await
}

/// Gli oggetti di uno schema, con il profilo che decide come interrogarli.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn list_objects_with_profile(
    session: &mut MysqlSession,
    schema: &str,
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
) -> Result<Vec<MysqlObjectSummary>> {
    session
        .exec_rows(
            profile.objects_query(),
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
    describe_object_with_profile(
        session,
        schema,
        name,
        &crate::profile::MYSQL_PROFILE,
        cancellation,
    )
    .await
}

/// La descrizione di un oggetto, con il profilo che decide come interrogarlo.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn describe_object_with_profile(
    session: &mut MysqlSession,
    schema: &str,
    name: &str,
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
) -> Result<MysqlObjectDescription> {
    let objects = session
        .exec_rows(
            profile.object_query(),
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
            profile.object_columns_query(),
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
            profile.object_indexes_query(),
            Params::Positional(vec![Value::from(schema), Value::from(name)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    let indexes = build_indexes(&index_rows, profile)?;
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
fn build_indexes(
    rows: &[Row],
    profile: &dyn crate::profile::ProductProfile,
) -> Result<Vec<MysqlIndex>> {
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
            // Parte funzionale (espressione non nulla, colonna nulla):
            // l'indice non è più confrontabile per colonne. Che il prodotto
            // pubblichi o meno queste parti lo dice il profilo; che una parte
            // senza colonna né espressione sia un errore no, perché sarebbe
            // un indice di cui non sappiamo dire nulla.
            None if expression.is_some() && profile.reports_functional_index_parts() => {
                current.column_backed = false;
            }
            None => {
                return Err(mapping_error(
                    "parte di indice MySQL senza colonna né espressione",
                ))
            }
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
